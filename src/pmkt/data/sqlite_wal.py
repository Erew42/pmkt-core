"""Standard-library SQLite WAL connections, file leases and WAL maintenance.

These helpers configure local SQLite files for durable single-writer use. They
know nothing about the tables a caller stores; schema, transactions and
retention stay with the caller.
"""

from __future__ import annotations

import sqlite3
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal

__all__ = ["file_lease", "maintain_wal", "open_sqlite_readonly", "open_sqlite_wal"]

DEFAULT_BUSY_TIMEOUT_MS = 5_000
DEFAULT_WAL_MAINTENANCE_BYTES = 64 * 1024**2


def _non_negative_int(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def open_sqlite_wal(
    path: Path | str,
    *,
    isolation_level: Literal["DEFERRED"] | None = "DEFERRED",
    busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
    foreign_keys: bool = False,
    journal_size_limit_bytes: int | None = None,
    cache_size_kib: int | None = None,
) -> sqlite3.Connection:
    """Open a writable connection in WAL mode with ``synchronous=FULL``.

    The parent directory must already exist. ``isolation_level="DEFERRED"``
    keeps Python's implicit deferred transactions for batch writers; pass
    ``None`` to manage every transaction explicitly. ``wal_autocheckpoint``
    keeps SQLite's default. The connection is closed if any setting fails.
    """
    if isolation_level not in ("DEFERRED", None):
        raise ValueError("isolation_level must be 'DEFERRED' or None")
    timeout_ms = _non_negative_int("busy_timeout_ms", busy_timeout_ms)
    if journal_size_limit_bytes is not None:
        _non_negative_int("journal_size_limit_bytes", journal_size_limit_bytes)
    if cache_size_kib is not None and (
        _non_negative_int("cache_size_kib", cache_size_kib) == 0
    ):
        raise ValueError("cache_size_kib must be positive")
    connection = sqlite3.connect(
        Path(path), timeout=timeout_ms / 1000, isolation_level=isolation_level
    )
    try:
        connection.execute(f"PRAGMA busy_timeout={timeout_ms}")
        mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        if str(mode).lower() != "wal":
            raise RuntimeError(f"SQLite WAL mode is unavailable (journal_mode={mode})")
        connection.execute("PRAGMA synchronous=FULL")
        if foreign_keys:
            connection.execute("PRAGMA foreign_keys=ON")
        if journal_size_limit_bytes is not None:
            connection.execute(f"PRAGMA journal_size_limit={journal_size_limit_bytes}")
        if cache_size_kib is not None:
            connection.execute(f"PRAGMA cache_size=-{cache_size_kib}")
    except BaseException:
        connection.close()
        raise
    return connection


def open_sqlite_readonly(
    path: Path | str,
    *,
    busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
    immutable: bool = False,
) -> sqlite3.Connection:
    """Open an existing database read-only in autocommit mode.

    A missing file fails without creating a database or directory. Readers
    manage their own snapshots with explicit ``BEGIN``. ``immutable=True``
    skips locking and WAL lookups and is only safe for files nothing writes.
    """
    timeout_ms = _non_negative_int("busy_timeout_ms", busy_timeout_ms)
    # as_uri percent-encodes spaces, '?', '#' and '%' in the path.
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    if immutable:
        uri += "&immutable=1"
    connection = sqlite3.connect(
        uri, uri=True, timeout=timeout_ms / 1000, isolation_level=None
    )
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute(f"PRAGMA busy_timeout={timeout_ms}")
    except BaseException:
        connection.close()
        raise
    return connection


@contextmanager
def file_lease(
    path: Path | str, *, busy_message: str = "file lease is held"
) -> Iterator[None]:
    """Hold a nonblocking exclusive OS lock on ``path`` for the block.

    The operating system releases the lock when the process dies. The lock
    file is kept on release: unlinking it would let a later process lock a new
    inode while an older holder still locks the old one.
    """
    with Path(path).open("a+b") as handle:
        handle.seek(0, 2)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if sys.platform == "win32":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError(busy_message) from exc
        try:
            yield
        finally:
            handle.seek(0)
            if sys.platform == "win32":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _wal_size(wal: Path) -> int:
    try:
        return wal.stat().st_size
    except FileNotFoundError:
        return 0


def maintain_wal(
    connection: sqlite3.Connection,
    path: Path | str,
    *,
    max_bytes: int = DEFAULT_WAL_MAINTENANCE_BYTES,
    mode: Literal["PASSIVE", "TRUNCATE"] = "TRUNCATE",
) -> dict[str, int | str]:
    """Checkpoint the WAL of ``path`` once it exceeds ``max_bytes``.

    Call this on the writer connection outside its transactions; an open
    transaction defers the work. The checkpoint never waits on other
    connections' locks: ``busy_timeout`` is 0 for the call and then restored.

    ``TRUNCATE`` reports ``reclaimed`` once the sidecar is back under
    ``max_bytes`` and ``deferred`` otherwise. ``PASSIVE`` copies the frames
    that no reader still needs and reports ``checkpointed`` or ``partial``
    with ``outstanding_frames``; it leaves the sidecar size to later resets.
    """
    if mode not in ("PASSIVE", "TRUNCATE"):
        raise ValueError("mode must be 'PASSIVE' or 'TRUNCATE'")
    _non_negative_int("max_bytes", max_bytes)
    wal = Path(str(path) + "-wal")
    before = _wal_size(wal)
    result: dict[str, int | str] = {
        "state": "not_needed",
        "wal_before_bytes": before,
        "wal_after_bytes": before,
    }
    if before <= max_bytes:
        return result
    if connection.in_transaction:
        return {**result, "state": "deferred", "reason": "Active writer transaction"}
    previous_timeout = connection.execute("PRAGMA busy_timeout").fetchone()[0]
    try:
        connection.execute("PRAGMA busy_timeout=0")
        busy, frames, checkpointed = connection.execute(
            f"PRAGMA wal_checkpoint({mode})"
        ).fetchone()
        after = _wal_size(wal)
        counts: dict[str, int | str] = {
            "wal_before_bytes": before,
            "wal_after_bytes": after,
            "checkpoint_busy": busy,
            "log_frames": frames,
            "checkpointed_frames": checkpointed,
        }
        if mode == "TRUNCATE":
            state = "reclaimed" if not busy and after <= max_bytes else "deferred"
            return {"state": state, **counts}
        outstanding = max(0, frames - checkpointed) if frames >= 0 else 0
        state = "checkpointed" if not busy and outstanding == 0 else "partial"
        return {"state": state, **counts, "outstanding_frames": outstanding}
    except sqlite3.OperationalError as exc:
        return {**result, "state": "deferred", "reason": str(exc)}
    finally:
        connection.execute(f"PRAGMA busy_timeout={int(previous_timeout)}")
