"""Bounded, read-only snapshot exports for the standalone wallet collector."""

from __future__ import annotations

import fcntl
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

SNAPSHOT_PAGES = 128
ARTIFACTS = ("wallets.csv", "wallet-evidence.jsonl", "manifest.json")


class ExportCancelled(RuntimeError):
    pass


class SnapshotBudgetExceeded(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_json(path: Path, payload: Any) -> None:
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temporary.open("w") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def snapshot_database(source: Path, destination: Path, *, timeout_s: float = 900,
                      max_wal_bytes: int = 2 * 1024**3,
                      min_free_bytes: int = 10 * 1024**3, pause_s: float = 0.01,
                      cancelled: Callable[[], bool] = lambda: False) -> dict[str, Any]:
    """Copy one pinned WAL snapshot, releasing its live reader before returning.

    ``min_free_bytes`` is headroom remaining after reserving the database copy.
    Limits are checked before opening the reader and after every backup batch.
    WAL size includes existing allocated capacity, making the ceiling conservative.
    """
    if timeout_s <= 0 or max_wal_bytes < 0 or min_free_bytes < 0 or pause_s < 0:
        raise ValueError("Invalid snapshot budgets")
    if destination.exists():
        raise ValueError("Snapshot destination already exists")
    destination.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    started_at = _now()
    wal_path = Path(str(source) + "-wal")
    wal_high_water = 0
    total_pages = 0
    copied_pages = 0
    source_bytes = source.stat().st_size

    def check_budget(remaining: int = 0, page_size: int = 4096) -> None:
        nonlocal wal_high_water
        if cancelled():
            raise ExportCancelled("Snapshot cancelled")
        if time.monotonic() - started >= timeout_s:
            raise SnapshotBudgetExceeded("Snapshot copy deadline exceeded")
        try:
            wal_bytes = wal_path.stat().st_size
        except FileNotFoundError:
            wal_bytes = 0
        wal_high_water = max(wal_high_water, wal_bytes)
        if wal_bytes > max_wal_bytes:
            raise SnapshotBudgetExceeded("Source WAL exceeds snapshot budget")
        if shutil.disk_usage(destination.parent).free < min_free_bytes + remaining * page_size:
            raise SnapshotBudgetExceeded("Insufficient scratch headroom for snapshot")

    check_budget((source_bytes + 4095) // 4096)
    reader: sqlite3.Connection | None = None
    target: sqlite3.Connection | None = None
    try:
        reader = sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
        reader.execute("PRAGMA query_only=ON")
        reader.execute("BEGIN")
        # BEGIN alone does not establish a read snapshot. Never use immutable=1
        # on the live database, or leave this reader open while serializing.
        reader.execute("SELECT name FROM sqlite_master LIMIT 1").fetchone()
        page_size = int(reader.execute("PRAGMA page_size").fetchone()[0])
        target = sqlite3.connect(destination)

        def progress(_status: int, remaining: int, total: int) -> None:
            nonlocal total_pages, copied_pages
            total_pages, copied_pages = total, total - remaining
            check_budget(remaining, page_size)
            if remaining and pause_s:
                time.sleep(pause_s)

        reader.backup(target, pages=SNAPSHOT_PAGES, progress=progress, sleep=0.05)
        check_budget()
        target.close()
        target = None
        reader.rollback()
        reader.close()
        reader = None
        return {"started_at": started_at, "completed_at": _now(),
                "elapsed_s": time.monotonic() - started, "database_bytes": destination.stat().st_size,
                "copied_pages": copied_pages, "total_pages": total_pages,
                "wal_high_water_bytes": wal_high_water}
    except BaseException:
        if target is not None:
            target.close()
            target = None
        destination.unlink(missing_ok=True)
        raise
    finally:
        if reader is not None:
            reader.close()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        result = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return result if isinstance(result, dict) else {}


def _publish_aliases(output: Path, generation: str) -> None:
    # Each root path is a compatibility view. Only reading latest-export.json
    # once, followed by paths in that immutable generation, gives a bundle.
    for name in ARTIFACTS:
        temporary = output / ("." + name + "." + uuid.uuid4().hex + ".tmp")
        try:
            temporary.symlink_to(str(Path(generation) / name))
            temporary.replace(output / name)
        finally:
            temporary.unlink(missing_ok=True)


def _prune_generations(output: Path, current: str, previous: str | None) -> None:
    retained = {current, previous}
    for path in (output / "export-generations").glob("generation-*"):
        if path.is_dir() and str(path.relative_to(output)) not in retained:
            shutil.rmtree(path)


def publish_generation(output: Path, generation_path: Path, snapshot: dict[str, Any],
                       snapshot_status: dict[str, Any]) -> dict[str, Any]:
    """Publish completed artifacts; the caller must hold ``.export.lock``.

    Both synchronous collection and snapshot workers use this protocol. The
    pointer's atomic rename commits publication. Later alias/retention I/O errors
    return ``publication_warning`` while leaving the coherent bundle available.
    Stop signals propagate; callers must preserve a generation already selected
    by the pointer when cleaning up after an exception.
    """
    generation = str(generation_path.relative_to(output))
    parts = Path(generation).parts
    if len(parts) != 2 or parts[0] != "export-generations" or not parts[1].startswith("generation-"):
        raise ValueError("Export generation must be under output/export-generations")
    manifest = _read_json(generation_path / "manifest.json")
    hashes = manifest.get("artifact_sha256", {})
    if (not all((generation_path / name).is_file() for name in ARTIFACTS)
            or not isinstance(hashes, dict) or manifest.get("status") != snapshot_status
            or any(not isinstance(hashes.get(name), str) or len(hashes[name]) != 64
                   or any(letter not in "0123456789abcdef" for letter in hashes[name])
                   for name in ARTIFACTS[:2])):
        raise ValueError("Export generation is incomplete")
    # Hashes come from exact emitted bytes, rather than rereading large files.
    previous = _read_json(output / "latest-export.json").get("generation")
    if not isinstance(previous, str):
        previous = None
    pointer = {"generation": generation, "previous_generation": previous,
               "published_at": _now(), "snapshot": snapshot,
               "snapshot_age_s": (datetime.now(timezone.utc)
                                  - datetime.fromisoformat(snapshot["started_at"])).total_seconds(),
               "snapshot_status": snapshot_status,
               "files": {name: str(Path(generation) / name) for name in ARTIFACTS}}
    published = False
    try:
        _atomic_json(output / "latest-export.json", pointer)
        published = True
        _publish_aliases(output, generation)
        _prune_generations(output, generation, previous)
    except OSError as exc:
        published = published or _read_json(output / "latest-export.json").get("generation") == generation
        if not published:
            raise
        pointer["publication_warning"] = str(exc)
    return pointer


def worker_main(output: Path, scratch: Path, state: str, error: str | None,
                writer_callback: Callable[[Path, Path, str, str | None], dict[str, Any]], *,
                snapshot_timeout_s: float = 900, snapshot_max_wal_bytes: int = 2 * 1024**3,
                snapshot_min_free_bytes: int = 10 * 1024**3,
                snapshot_pause_s: float = 0.01) -> int:
    """Publish one generation, without writing collection status or making RPCs."""
    output.mkdir(parents=True, exist_ok=True)
    scratch.mkdir(parents=True, exist_ok=True)
    generation = "export-generations/generation-" + uuid.uuid4().hex
    generation_path = output / generation
    snapshot = scratch / (f"wallet-registry-snapshot-{os.getpid()}-" + uuid.uuid4().hex + ".sqlite3")
    progress_path = output / "export-progress.json"
    progress: dict[str, Any] = {"generation": generation, "started_at": _now(), "pid": os.getpid()}
    published = False
    old_handlers: dict[int, Any] = {}

    def stage(name: str, **details: Any) -> None:
        progress.update(stage=name, updated_at=_now(), **details)
        _atomic_json(progress_path, progress)

    def cancel(_signum: int, _frame: Any) -> None:
        raise ExportCancelled("Export worker cancelled")

    with (output / ".export.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 75
        try:
            for signum in (signal.SIGTERM, signal.SIGINT):
                old_handlers[signum] = signal.signal(signum, cancel)
            stage("copying")
            timings = snapshot_database(output / "registry.sqlite3", snapshot,
                                        timeout_s=snapshot_timeout_s,
                                        max_wal_bytes=snapshot_max_wal_bytes,
                                        min_free_bytes=snapshot_min_free_bytes,
                                        pause_s=snapshot_pause_s)
            generation_path.mkdir(parents=True)
            stage("exporting", snapshot=timings)
            serialized_at = time.monotonic()
            snapshot_status = writer_callback(snapshot, generation_path, state, error)
            stage("publishing", serialization_s=time.monotonic() - serialized_at,
                  snapshot_status=snapshot_status)
            publication_started = time.monotonic()
            pointer = publish_generation(output, generation_path, timings, snapshot_status)
            published = True
            stage("succeeded", completed_at=_now(), publication_s=time.monotonic() - publication_started,
                  error=pointer.get("publication_warning"))
            return 0
        except BaseException as exc:
            # A signal can arrive immediately after the pointer's atomic rename.
            # Never remove a complete generation already visible to consumers.
            published = published or _read_json(output / "latest-export.json").get("generation") == generation
            try:
                stage("succeeded" if published else "cancelled" if isinstance(exc, ExportCancelled) else "failed",
                      error=str(exc), completed_at=_now())
            except OSError:
                pass
            return 0 if published else 130 if isinstance(exc, ExportCancelled) else 1
        finally:
            for signum, handler in old_handlers.items():
                signal.signal(signum, handler)
            if not published:
                shutil.rmtree(generation_path, ignore_errors=True)
            for path in (snapshot, Path(str(snapshot) + "-wal"), Path(str(snapshot) + "-shm")):
                path.unlink(missing_ok=True)


class ExportManager:
    """One fresh worker interpreter; due exports never accumulate a queue."""

    def __init__(self, output: Path, scratch: Path, collector_path: Path, *,
                 snapshot_timeout_s: float = 900, snapshot_max_wal_bytes: int = 2 * 1024**3,
                 snapshot_min_free_bytes: int = 10 * 1024**3,
                 snapshot_pause_s: float = 0.01):
        self.output, self.scratch, self.collector_path = output, scratch, collector_path
        self.options = {"snapshot-timeout-s": snapshot_timeout_s,
                        "snapshot-max-wal-bytes": snapshot_max_wal_bytes,
                        "snapshot-min-free-bytes": snapshot_min_free_bytes,
                        "snapshot-pause-s": snapshot_pause_s}
        self.process: subprocess.Popen[bytes] | None = None
        self.launch_error: str | None = None
        self.cancel_requested = False

    def start(self, state: str, error: str | None = None) -> bool:
        if self.process is not None and self.process.poll() is None:
            return False
        command = [sys.executable, str(self.collector_path), "--_export-worker",
                   "--output", str(self.output), "--export-scratch-dir", str(self.scratch),
                   "--worker-state", state]
        if error is not None:
            command.extend(["--worker-error", error])
        for key, value in self.options.items():
            command.extend(["--" + key, str(value)])
        # No inherited Python/SQLite objects or file descriptors. OS nice, I/O
        # priority and cgroup membership remain inherited from the collector.
        self.launch_error = None
        self.cancel_requested = False
        try:
            self.process = subprocess.Popen(command, close_fds=True, stdout=subprocess.DEVNULL)
        except OSError as exc:
            self.process = None
            self.launch_error = f"Could not launch export worker: {exc}"
            return False
        return True

    def poll(self) -> dict[str, Any]:
        progress = _read_json(self.output / "export-progress.json")
        code = self.process.poll() if self.process is not None else None
        running = self.process is not None and code is None
        if self.launch_error is not None:
            progress = {"stage": "failed", "error": self.launch_error}
        elif self.process is not None:
            if progress.get("pid") != self.process.pid:
                progress = {"stage": "starting" if running else "failed", "pid": self.process.pid}
            if code is not None and code != 0:
                cancelled = self.cancel_requested or code in (130, -signal.SIGTERM, -signal.SIGKILL)
                progress.update(stage="cancelled" if cancelled else "failed",
                                error=progress.get("error") or ("Export publisher lock is already held" if code == 75
                                                               else f"Export worker exited with status {code}"))
            elif code == 0 and progress.get("stage") != "succeeded":
                progress.update(stage="failed", error="Export worker exited without a successful progress record")
            elif running and self.cancel_requested:
                progress.update(stage="cancelled", error="Export worker cancellation pending")
        pointer = _read_json(self.output / "latest-export.json")
        snapshot = pointer.get("snapshot", {})
        snapshot_updated_at = snapshot.get("started_at") if isinstance(snapshot, dict) else None
        age = None
        if isinstance(snapshot_updated_at, str):
            try:
                age = max(0.0, (datetime.now(timezone.utc)
                               - datetime.fromisoformat(snapshot_updated_at)).total_seconds())
            except (ValueError, TypeError):
                pass
        return {**progress, "worker_running": self.process is not None and code is None,
                "worker_exit_code": code, "worker_pid": self.process.pid if self.process else None,
                "latest_generation": pointer.get("generation"), "snapshot_updated_at": snapshot_updated_at,
                "snapshot_age_s": age}

    def wait(self) -> dict[str, Any]:
        if self.process is not None:
            self.process.wait()
        return self.poll()

    def cancel(self, timeout_s: float = 5) -> dict[str, Any]:
        if timeout_s < 0:
            raise ValueError("Cancellation timeout cannot be negative")
        if self.process is not None and self.process.poll() is None:
            self.cancel_requested = True
            deadline = time.monotonic() + timeout_s
            self.process.terminate()
            try:
                self.process.wait(timeout=timeout_s * 0.8)
            except subprocess.TimeoutExpired:
                self.process.kill()
                try:
                    self.process.wait(timeout=max(0, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    # A task stuck in kernel I/O may not be reaped immediately.
                    # Report its running state without blocking collector stop.
                    return self.poll()
                self._clean_killed_worker()
        return self.poll()

    def _clean_killed_worker(self) -> None:
        assert self.process is not None
        for path in self.scratch.glob(f"wallet-registry-snapshot-{self.process.pid}-*"):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
        progress = _read_json(self.output / "export-progress.json")
        generation = progress.get("generation")
        pointer_path = self.output / "latest-export.json"
        try:
            pointer = _read_json(pointer_path)
            if pointer_path.exists() and not isinstance(pointer.get("generation"), str):
                return  # Unreadable pointer: retain rather than risk its bundle.
        except OSError:
            return
        if (progress.get("pid") == self.process.pid and isinstance(generation, str)
                and len(Path(generation).parts) == 2 and Path(generation).parts[0] == "export-generations"
                and Path(generation).name.startswith("generation-")
                and pointer.get("generation") != generation):
            shutil.rmtree(self.output / generation, ignore_errors=True)
