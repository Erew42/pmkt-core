from __future__ import annotations

import fcntl
import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import wallet_registry_export as exporter  # noqa: E402


@pytest.fixture
def registry(tmp_path):
    output = tmp_path / "output"
    output.mkdir()
    path = output / "registry.sqlite3"
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA wal_autocheckpoint=0")
    db.executescript("""
        CREATE TABLE metadata (key TEXT PRIMARY KEY, value INTEGER);
        CREATE TABLE wallets (address TEXT PRIMARY KEY, generation INTEGER, evidence BLOB);
        CREATE TABLE requests (id INTEGER PRIMARY KEY, generation INTEGER);
        CREATE TABLE ranges (generation INTEGER);
        CREATE TABLE streams (next_block INTEGER);
        INSERT INTO metadata VALUES ('generation', 0);
        INSERT INTO requests VALUES (1, 0);
        INSERT INTO ranges VALUES (0);
        INSERT INTO streams VALUES (1);
    """)
    db.executemany("INSERT INTO wallets VALUES (?,0,?)", [(str(n), bytes(8192)) for n in range(100)])
    db.commit()
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    yield output, db
    db.close()


def snapshot_options():
    return {"min_free_bytes": 0, "pause_s": 0}


def worker_options():
    return {"snapshot_min_free_bytes": 0, "snapshot_pause_s": 0}


def callback(snapshot, generation, state, error):
    db = sqlite3.connect(snapshot.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        db.execute("PRAGMA query_only=ON")
        count = db.execute("SELECT count(*) FROM wallets").fetchone()[0]
        version = db.execute("SELECT value FROM metadata WHERE key='generation'").fetchone()[0]
    finally:
        db.close()
    status = {"state": state, "error": error, "wallet_count": count, "generation": version}
    hashes = {}
    for name in exporter.ARTIFACTS[:2]:
        payload = json.dumps(status).encode()
        (generation / name).write_bytes(payload)
        hashes[name] = hashlib.sha256(payload).hexdigest()
    (generation / "manifest.json").write_text(json.dumps({"status": status, "artifact_sha256": hashes}))
    return status


def test_writer_commits_during_pinned_copy_and_reader_releases_wal(registry, tmp_path, monkeypatch):
    output, writer = registry
    begin, done = threading.Event(), threading.Event()
    results = []
    errors = []
    monkeypatch.setattr(exporter, "SNAPSHOT_PAGES", 8)

    def write():
        try:
            assert begin.wait(5)
            db = sqlite3.connect(output / "registry.sqlite3")
            try:
                db.execute("PRAGMA wal_autocheckpoint=0")
                for number in range(1, 9):
                    with db:
                        db.execute("UPDATE metadata SET value=?", (number,))
                        db.execute("UPDATE wallets SET generation=?", (number,))
                        db.execute("UPDATE requests SET generation=?", (number,))
                        db.execute("UPDATE ranges SET generation=?", (number,))
                        db.execute("UPDATE streams SET next_block=?", (number + 1,))
                results.append(db.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone())
            finally:
                db.close()
        except BaseException as exc:
            errors.append(exc)
        finally:
            done.set()

    thread = threading.Thread(target=write)
    thread.start()

    def between_batches(_seconds):
        begin.set()
        assert done.wait(5)

    monkeypatch.setattr(exporter.time, "sleep", between_batches)
    snapshot = tmp_path / "copy.sqlite3"
    try:
        timings = exporter.snapshot_database(output / "registry.sqlite3", snapshot,
                                            min_free_bytes=0, pause_s=0.001)
    finally:
        thread.join(6)
    assert not errors and not thread.is_alive()
    assert results[0][2] < results[0][1]  # Active reader held back checkpointing.
    copied = sqlite3.connect(snapshot)
    try:
        assert copied.execute("SELECT value FROM metadata").fetchone() == (0,)
        assert copied.execute("SELECT DISTINCT generation FROM wallets").fetchall() == [(0,)]
        assert copied.execute("SELECT generation FROM requests").fetchone() == (0,)
        assert copied.execute("SELECT generation FROM ranges").fetchone() == (0,)
        assert copied.execute("SELECT next_block FROM streams").fetchone() == (1,)
    finally:
        copied.close()
    assert writer.execute("SELECT value FROM metadata").fetchone() == (8,)
    assert writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone() == (0, 0, 0)
    assert Path(str(output / "registry.sqlite3") + "-wal").stat().st_size == 0
    assert timings["copied_pages"] == timings["total_pages"] > 8
    assert timings["wal_high_water_bytes"] > 0


@pytest.mark.parametrize("kind", ["time", "wal", "space", "cancel"])
def test_snapshot_budget_abort_releases_reader_and_removes_copy(registry, tmp_path, monkeypatch, kind):
    output, db = registry
    monkeypatch.setattr(exporter, "SNAPSHOT_PAGES", 8)
    # Keep preflight valid and exceed a limit after the reader opens.
    calls = 0

    def cancelled():
        nonlocal calls
        calls += 1
        if calls == 2:
            with db:
                db.execute("UPDATE wallets SET generation=1")
        return kind == "cancel" and calls >= 2

    options = {**snapshot_options(), "cancelled": cancelled}
    if kind == "time":
        monkeypatch.setattr(exporter.time, "monotonic", iter([0, 0, 1000]).__next__)
    elif kind == "wal":
        options["max_wal_bytes"] = 0
    elif kind == "space":
        free = iter([10**9, 0])
        monkeypatch.setattr(exporter.shutil, "disk_usage", lambda _: SimpleNamespace(free=next(free)))
    snapshot = tmp_path / "copy.sqlite3"
    expected = exporter.ExportCancelled if kind == "cancel" else exporter.SnapshotBudgetExceeded
    with pytest.raises(expected):
        exporter.snapshot_database(output / "registry.sqlite3", snapshot, **options)
    assert not snapshot.exists()
    assert db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone() == (0, 0, 0)


def test_space_budget_reserves_copy_in_addition_to_headroom(registry, tmp_path, monkeypatch):
    output, _ = registry
    size = (output / "registry.sqlite3").stat().st_size
    monkeypatch.setattr(exporter.shutil, "disk_usage", lambda _: SimpleNamespace(free=size + 999))
    with pytest.raises(exporter.SnapshotBudgetExceeded, match="headroom"):
        exporter.snapshot_database(output / "registry.sqlite3", tmp_path / "copy.sqlite3",
                                   min_free_bytes=1000)


def test_refuses_existing_snapshot_destination(registry, tmp_path):
    output, _ = registry
    destination = tmp_path / "copy.sqlite3"
    destination.write_text("preserved")
    with pytest.raises(ValueError, match="already exists"):
        exporter.snapshot_database(output / "registry.sqlite3", destination, **snapshot_options())
    assert destination.read_text() == "preserved"


def test_worker_publishes_one_snapshot_without_overwriting_live_status(registry, tmp_path):
    output, db = registry
    live = {"next_block": 999, "state": "running"}
    (output / "status.json").write_text(json.dumps(live))
    scratch = tmp_path / "scratch"

    def exporting(snapshot, generation, state, error):
        # The live snapshot reader must be gone before serialization starts.
        with db:
            db.execute("UPDATE metadata SET value=3")
            db.execute("INSERT INTO wallets VALUES ('new',3,X'')")
        assert db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone() == (0, 0, 0)
        return callback(snapshot, generation, state, error)

    assert exporter.worker_main(output, scratch, "running", None, exporting, **worker_options()) == 0
    pointer = json.loads((output / "latest-export.json").read_text())
    assert pointer["snapshot_status"]["wallet_count"] == 100
    assert pointer["snapshot_status"]["generation"] == 0
    assert json.loads((output / "status.json").read_text()) == live
    assert json.loads((output / "export-progress.json").read_text())["stage"] == "succeeded"
    for name in exporter.ARTIFACTS:
        assert (output / name).is_symlink()
        assert (output / name).read_bytes() == (output / pointer["files"][name]).read_bytes()
    assert not list(scratch.iterdir())


@pytest.mark.parametrize("failure", ["callback", "incomplete", "publication", "cancel"])
def test_failed_export_preserves_previous_generation(registry, tmp_path, monkeypatch, failure):
    output, _ = registry
    scratch = tmp_path / "scratch"
    assert exporter.worker_main(output, scratch, "running", None, callback, **worker_options()) == 0
    previous = (output / "latest-export.json").read_bytes()
    good_generation = json.loads(previous)["generation"]

    def fail(snapshot, generation, state, error):
        if failure == "callback":
            (generation / "wallets.csv").write_text("partial")
            raise OSError("disk full")
        if failure == "incomplete":
            return {"wallet_count": 0}
        if failure == "cancel":
            raise exporter.ExportCancelled("Stopped during serialization")
        return callback(snapshot, generation, state, error)

    original_atomic = exporter._atomic_json
    if failure == "publication":
        def fail_pointer(path, value):
            if path.name == "latest-export.json":
                raise OSError("publication failed")
            original_atomic(path, value)
        monkeypatch.setattr(exporter, "_atomic_json", fail_pointer)
    assert exporter.worker_main(output, scratch, "running", None, fail, **worker_options()) == (
        130 if failure == "cancel" else 1)
    assert (output / "latest-export.json").read_bytes() == previous
    assert [str(p.relative_to(output)) for p in (output / "export-generations").iterdir()] == [good_generation]
    assert not list(scratch.iterdir())
    for name in exporter.ARTIFACTS:
        assert (output / name).is_file()


def test_publisher_lock_does_not_overwrite_other_worker_progress(registry, tmp_path):
    output, _ = registry
    progress = b'{"stage":"exporting","pid":123}'
    (output / "export-progress.json").write_bytes(progress)
    with (output / ".export.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert exporter.worker_main(output, tmp_path / "scratch", "running", None,
                                    callback, **worker_options()) == 75
    assert (output / "export-progress.json").read_bytes() == progress


def test_worker_retains_current_and_previous_generation(registry, tmp_path):
    output, _ = registry
    generations = []
    for _ in range(3):
        assert exporter.worker_main(output, tmp_path / "scratch", "running", None,
                                    callback, **worker_options()) == 0
        generations.append(json.loads((output / "latest-export.json").read_text())["generation"])
    pointer = json.loads((output / "latest-export.json").read_text())
    assert pointer["previous_generation"] == generations[-2]
    assert not (output / generations[0]).exists()
    assert all((output / generation).is_dir() for generation in generations[-2:])


def test_publication_is_complete_if_compatibility_aliases_fail(registry, tmp_path, monkeypatch):
    output, _ = registry
    monkeypatch.setattr(exporter, "_publish_aliases", lambda *_: (_ for _ in ()).throw(OSError("alias failure")))
    assert exporter.worker_main(output, tmp_path / "scratch", "running", None,
                                callback, **worker_options()) == 0
    pointer = json.loads((output / "latest-export.json").read_text())
    assert all((output / file).is_file() for file in pointer["files"].values())
    assert json.loads((output / "export-progress.json").read_text())["error"] == "alias failure"


def test_manager_has_one_child_no_backlog_and_bounded_cancellation(tmp_path, monkeypatch):
    calls = []

    class Child:
        pid = 123
        code = None
        terminated = False
        killed = False

        def poll(self):
            return self.code

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.killed = True
            self.code = -9

        def wait(self, timeout=None):
            if timeout is not None and not self.killed:
                raise subprocess.TimeoutExpired("child", timeout)
            self.code = 0 if self.code is None else self.code
            return self.code

    def launch(command, **kwargs):
        child = Child()
        calls.append((command, kwargs, child))
        return child

    monkeypatch.setattr(exporter.subprocess, "Popen", launch)
    manager = exporter.ExportManager(tmp_path / "output", tmp_path / "scratch", Path("collector.py"))
    assert manager.start("running", "reason")
    assert not manager.start("running")
    assert manager.poll()["worker_running"] is True
    result = manager.cancel(timeout_s=0.01)
    assert calls[0][2].terminated and calls[0][2].killed
    assert result["worker_exit_code"] == -9
    command, kwargs, _ = calls[0]
    assert command[:3] == [sys.executable, "collector.py", "--_export-worker"]
    assert kwargs["close_fds"] is True
    assert "preexec_fn" not in kwargs
    assert "--worker-error" in command and "reason" in command
    assert manager.start("done")
    assert len(calls) == 2
    assert manager.wait()["worker_running"] is False


def test_manager_cancels_real_child_with_bounded_deadline(tmp_path):
    stub = tmp_path / "child.py"
    stub.write_text("import time\ntime.sleep(60)\n")
    manager = exporter.ExportManager(tmp_path / "output", tmp_path / "scratch", stub)
    try:
        assert manager.start("running")
        assert not manager.start("running")
        result = manager.cancel(timeout_s=0.1)
        assert result["worker_running"] is False
        assert result["worker_exit_code"] in (-signal.SIGTERM, -signal.SIGKILL)
    finally:
        manager.cancel(timeout_s=0.1)


@pytest.mark.parametrize("phase", ["copying", "exporting"])
def test_signal_cancellation_releases_real_worker_reader_and_partial_files(registry, tmp_path, phase):
    output, db = registry
    stub = tmp_path / "worker.py"
    stub.write_text(
        "import argparse, sys, time\n"
        "from pathlib import Path\n"
        f"sys.path.insert(0, {str(SCRIPTS_DIR)!r})\n"
        "import wallet_registry_export as helper\n"
        "helper.SNAPSHOT_PAGES = 1\n"
        "parser = argparse.ArgumentParser()\n"
        "parser.add_argument('--output', type=Path)\n"
        "parser.add_argument('--export-scratch-dir', type=Path)\n"
        "args, _ = parser.parse_known_args()\n"
        "def callback(snapshot, generation, state, error):\n"
        "    (generation / 'partial').write_text('not publishable')\n"
        "    time.sleep(30)\n"
        "    raise AssertionError('test must cancel the worker')\n"
        f"sys.exit(helper.worker_main(args.output, args.export_scratch_dir, 'running', None, callback, "
        f"snapshot_min_free_bytes=0, snapshot_pause_s={0.01 if phase == 'copying' else 0}))\n"
    )
    scratch = tmp_path / "scratch"
    manager = exporter.ExportManager(output, scratch, stub, snapshot_min_free_bytes=0)
    try:
        assert manager.start("running")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            progress = manager.poll()
            if progress.get("stage") == phase:
                if phase == "exporting" or list(scratch.glob("*.sqlite3")):
                    break
            assert progress["worker_running"]
            time.sleep(0.005)
        else:
            pytest.fail(f"Worker did not reach {phase}")
        assert os.getpriority(os.PRIO_PROCESS, manager.process.pid) == os.getpriority(os.PRIO_PROCESS, 0)
        with db:
            db.execute("UPDATE metadata SET value=5")
        if phase == "copying":
            _, log_frames, checkpointed = db.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            assert checkpointed < log_frames
        result = manager.cancel(timeout_s=0.5)
        assert result["worker_exit_code"] == 130
        assert result["stage"] == "cancelled"
        assert db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone() == (0, 0, 0)
        assert not list(scratch.iterdir())
        assert not list((output / "export-generations").glob("generation-*"))
        assert not (output / "latest-export.json").exists()
    finally:
        manager.cancel(timeout_s=0.1)


def test_cancel_immediately_after_pointer_commit_retains_visible_generation(registry, tmp_path, monkeypatch):
    output, _ = registry
    original_atomic = exporter._atomic_json

    def cancel_after_rename(path, payload):
        original_atomic(path, payload)
        if path.name == "latest-export.json":
            signal.raise_signal(signal.SIGTERM)

    monkeypatch.setattr(exporter, "_atomic_json", cancel_after_rename)
    assert exporter.worker_main(output, tmp_path / "scratch", "running", None,
                                callback, **worker_options()) == 0
    pointer = json.loads((output / "latest-export.json").read_text())
    assert all((output / path).is_file() for path in pointer["files"].values())


@pytest.mark.parametrize("code,stage", [(75, "failed"), (1, "failed"), (-9, "cancelled"), (0, "failed")])
@pytest.mark.parametrize("progress_pid", [999, 123])
def test_manager_never_claims_success_from_old_or_nonzero_worker_progress(tmp_path, code, stage, progress_pid):
    output = tmp_path / "output"
    output.mkdir()
    old = {"stage": "succeeded", "pid": progress_pid, "snapshot_status": {"wallet_count": 99}}
    (output / "export-progress.json").write_text(json.dumps(old))
    (output / "latest-export.json").write_text(json.dumps(
        {"generation": "export-generations/generation-good", "snapshot": {"started_at": "2026-01-01T00:00:00+00:00"}}))
    manager = exporter.ExportManager(output, tmp_path / "scratch", Path("collector.py"))
    manager.process = SimpleNamespace(pid=123, poll=lambda: code)
    result = manager.poll()
    if code == 0 and progress_pid == 123:
        assert result["stage"] == "succeeded"
    else:
        assert result["stage"] == stage
        assert result["error"]
    assert result["latest_generation"] == "export-generations/generation-good"
    assert result["snapshot_updated_at"] == "2026-01-01T00:00:00+00:00"
    assert result["snapshot_age_s"] > 0
    if progress_pid != 123:
        assert "snapshot_status" not in result


def test_manager_start_failure_reports_health_and_preserves_historic_freshness(tmp_path, monkeypatch):
    output = tmp_path / "output"
    output.mkdir()
    (output / "export-progress.json").write_text('{"stage":"succeeded","pid":999}')
    (output / "latest-export.json").write_text(json.dumps(
        {"generation": "export-generations/generation-good", "snapshot": {"started_at": "2026-01-01T00:00:00+00:00"}}))

    def fail_launch(*args, **kwargs):
        raise OSError("fork unavailable")

    monkeypatch.setattr(exporter.subprocess, "Popen", fail_launch)
    manager = exporter.ExportManager(output, tmp_path / "scratch", Path("collector.py"))
    assert manager.start("running") is False
    result = manager.poll()
    assert result["stage"] == "failed"
    assert "fork unavailable" in result["error"]
    assert result["latest_generation"] == "export-generations/generation-good"
    assert result["snapshot_age_s"] > 0
    assert (output / "export-progress.json").read_text() == '{"stage":"succeeded","pid":999}'


def test_manager_new_child_is_starting_while_old_success_progress_remains(tmp_path):
    output = tmp_path / "output"
    output.mkdir()
    (output / "export-progress.json").write_text('{"stage":"succeeded","pid":999}')
    manager = exporter.ExportManager(output, tmp_path / "scratch", Path("collector.py"))
    manager.process = SimpleNamespace(pid=123, poll=lambda: None)
    assert manager.poll()["stage"] == "starting"


def test_manager_force_kill_cleans_only_its_unpublished_files(tmp_path):
    output, scratch = tmp_path / "output", tmp_path / "scratch"
    output.mkdir()
    scratch.mkdir()
    partial = output / "export-generations/generation-partial"
    good = output / "export-generations/generation-good"
    partial.mkdir(parents=True)
    good.mkdir()
    (output / "latest-export.json").write_text(json.dumps({"generation": str(good.relative_to(output))}))
    (output / "export-progress.json").write_text(json.dumps(
        {"pid": 123, "stage": "copying", "generation": str(partial.relative_to(output))}))
    snapshot = scratch / "wallet-registry-snapshot-123-copy.sqlite3"
    unrelated = scratch / "wallet-registry-snapshot-999-other.sqlite3"
    snapshot.write_bytes(b"partial")
    unrelated.write_bytes(b"another worker")

    class Child:
        pid = 123
        code = None

        def poll(self):
            return self.code

        def terminate(self):
            pass

        def kill(self):
            self.code = -9

        def wait(self, timeout):
            if self.code is None:
                raise subprocess.TimeoutExpired("child", timeout)
            return self.code

    manager = exporter.ExportManager(output, scratch, Path("collector.py"))
    manager.process = Child()
    result = manager.cancel(timeout_s=0.1)
    assert result["stage"] == "cancelled"
    assert not snapshot.exists() and not partial.exists()
    assert unrelated.is_file() and good.is_dir()


def test_manager_does_not_wait_indefinitely_for_kernel_stalled_child(tmp_path):
    waits = []

    class Child:
        pid = 123

        def poll(self):
            return None

        def terminate(self):
            pass

        def kill(self):
            pass

        def wait(self, timeout):
            waits.append(timeout)
            raise subprocess.TimeoutExpired("child", timeout)

    manager = exporter.ExportManager(tmp_path / "output", tmp_path / "scratch", Path("collector.py"))
    manager.process = Child()
    result = manager.cancel(timeout_s=0.01)
    assert len(waits) == 2 and all(0 <= value <= 0.01 for value in waits)
    assert result["stage"] == "cancelled" and result["worker_running"]


def test_force_kill_cleanup_retains_generation_when_pointer_cannot_be_read(tmp_path):
    generation = tmp_path / "output/export-generations/generation-visible"
    generation.mkdir(parents=True)
    output = tmp_path / "output"
    (output / "latest-export.json").write_text("unreadable or incomplete pointer")
    (output / "export-progress.json").write_text(json.dumps(
        {"pid": 123, "generation": str(generation.relative_to(output)), "stage": "publishing"}))
    manager = exporter.ExportManager(output, tmp_path / "scratch", Path("collector.py"))
    manager.process = SimpleNamespace(pid=123)
    manager._clean_killed_worker()
    assert generation.is_dir()


def test_shared_publication_keeps_pointer_current_across_export_modes(registry, tmp_path):
    output, db = registry
    scratch = tmp_path / "scratch"
    assert exporter.worker_main(output, scratch, "running", None, callback, **worker_options()) == 0
    first = json.loads((output / "latest-export.json").read_text())
    with db:
        db.execute("UPDATE metadata SET value=7")
        db.execute("INSERT INTO wallets VALUES ('new',7,X'')")
    synchronous_generation = output / "export-generations/generation-sync"
    synchronous_generation.mkdir()
    with (output / ".export.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        status = callback(output / "registry.sqlite3", synchronous_generation, "running", None)
        now = exporter._now()
        result = exporter.publish_generation(output, synchronous_generation,
                                             {"started_at": now, "completed_at": now, "mode": "sync", "elapsed_s": 0},
                                             status)
    second = json.loads((output / "latest-export.json").read_text())
    assert second == result
    assert second["previous_generation"] == first["generation"]
    assert second["snapshot_status"]["generation"] == 7
    assert second["snapshot_status"]["wallet_count"] == 101
    assert json.loads((output / "wallets.csv").read_text()) == second["snapshot_status"]
    with db:
        db.execute("UPDATE metadata SET value=9")
    assert exporter.worker_main(output, scratch, "running", None, callback, **worker_options()) == 0
    third = json.loads((output / "latest-export.json").read_text())
    assert third["previous_generation"] == second["generation"]
    assert third["snapshot_status"]["generation"] == 9
    assert not (output / first["generation"]).exists()


def test_shared_publisher_reports_postcommit_cleanup_warning(registry, tmp_path, monkeypatch):
    output, _ = registry
    generation = output / "export-generations/generation-sync"
    generation.mkdir(parents=True)
    status = callback(output / "registry.sqlite3", generation, "exported", None)
    monkeypatch.setattr(exporter, "_prune_generations", lambda *_: (_ for _ in ()).throw(OSError("cleanup denied")))
    with (output / ".export.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = exporter.publish_generation(output, generation, {"started_at": exporter._now()}, status)
    assert result["publication_warning"] == "cleanup denied"
    pointer = json.loads((output / "latest-export.json").read_text())
    assert all((output / path).is_file() for path in pointer["files"].values())


def test_shared_publisher_propagates_stop_after_commit_preserving_bundle(registry, tmp_path, monkeypatch):
    output, _ = registry
    generation = output / "export-generations/generation-sync"
    generation.mkdir(parents=True)
    status = callback(output / "registry.sqlite3", generation, "exported", None)
    original_atomic = exporter._atomic_json

    def stop_after_commit(path, payload):
        original_atomic(path, payload)
        if path.name == "latest-export.json":
            raise KeyboardInterrupt

    monkeypatch.setattr(exporter, "_atomic_json", stop_after_commit)
    with (output / ".export.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(KeyboardInterrupt):
            exporter.publish_generation(output, generation, {"started_at": exporter._now()}, status)
    pointer = json.loads((output / "latest-export.json").read_text())
    assert all((output / path).is_file() for path in pointer["files"].values())
