from __future__ import annotations

import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

import pmkt.data.sqlite_wal as sqlite_wal
from pmkt.data.sqlite_wal import (
    file_lease,
    maintain_wal,
    open_sqlite_readonly,
    open_sqlite_wal,
)


def _pragma(connection: sqlite3.Connection, name: str) -> object:
    return connection.execute(f"PRAGMA {name}").fetchone()[0]


def _writer(path: Path, **kwargs: object) -> sqlite3.Connection:
    connection = open_sqlite_wal(path, **kwargs)  # type: ignore[arg-type]
    connection.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)")
    connection.commit()
    return connection


def test_writer_configures_wal_full_sync_and_options(tmp_path: Path) -> None:
    connection = open_sqlite_wal(
        tmp_path / "a.sqlite3",
        busy_timeout_ms=1234,
        foreign_keys=True,
        journal_size_limit_bytes=65536,
        cache_size_kib=2048,
    )
    try:
        assert _pragma(connection, "journal_mode") == "wal"
        assert _pragma(connection, "synchronous") == 2  # FULL
        assert _pragma(connection, "busy_timeout") == 1234
        assert _pragma(connection, "foreign_keys") == 1
        assert _pragma(connection, "journal_size_limit") == 65536
        assert _pragma(connection, "cache_size") == -2048
        assert connection.isolation_level == "DEFERRED"
    finally:
        connection.close()


def test_writer_defaults_leave_foreign_keys_and_autocheckpoint(tmp_path: Path) -> None:
    connection = open_sqlite_wal(tmp_path / "a.sqlite3", isolation_level=None)
    try:
        assert _pragma(connection, "foreign_keys") == 0
        assert _pragma(connection, "busy_timeout") == 5000
        assert _pragma(connection, "wal_autocheckpoint") == 1000
        assert connection.isolation_level is None
        connection.execute("CREATE TABLE t (id INTEGER)")
        assert not connection.in_transaction
    finally:
        connection.close()


def test_writer_requires_existing_parent(tmp_path: Path) -> None:
    with pytest.raises(sqlite3.OperationalError):
        open_sqlite_wal(tmp_path / "missing" / "a.sqlite3")
    assert not (tmp_path / "missing").exists()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"isolation_level": "IMMEDIATE"},
        {"busy_timeout_ms": -1},
        {"busy_timeout_ms": True},
        {"journal_size_limit_bytes": -1},
        {"cache_size_kib": 0},
    ],
)
def test_writer_rejects_invalid_options_before_opening(
    tmp_path: Path, kwargs: dict[str, object]
) -> None:
    with pytest.raises(ValueError):
        open_sqlite_wal(tmp_path / "a.sqlite3", **kwargs)  # type: ignore[arg-type]
    assert not (tmp_path / "a.sqlite3").exists()


def test_memory_database_is_rejected() -> None:
    with pytest.raises(RuntimeError, match="WAL"):
        open_sqlite_wal(":memory:")


class _Tracking(sqlite3.Connection):
    closed = False

    def execute(self, sql: str, *args: object) -> sqlite3.Cursor:  # type: ignore[override]
        if "journal_size_limit" in sql:
            raise sqlite3.OperationalError("injected setup failure")
        return super().execute(sql, *args)  # type: ignore[arg-type]

    def close(self) -> None:
        type(self).closed = True
        super().close()


def test_setup_failure_closes_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_connect = sqlite3.connect

    def connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        return real_connect(*args, factory=_Tracking, **kwargs)  # type: ignore[arg-type,call-overload,no-any-return]

    monkeypatch.setattr(sqlite_wal.sqlite3, "connect", connect)
    path = tmp_path / "a.sqlite3"
    with pytest.raises(sqlite3.OperationalError, match="injected"):
        open_sqlite_wal(path, journal_size_limit_bytes=1024)
    assert _Tracking.closed
    # A closed handle lets Windows delete and rename the directory at once.
    for sidecar in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        sidecar.unlink(missing_ok=True)
    tmp_path.rename(tmp_path.with_name(tmp_path.name + "-moved"))


def test_deferred_batches_stay_invisible_until_commit(tmp_path: Path) -> None:
    path = tmp_path / "a.sqlite3"
    writer = _writer(path)
    reader = open_sqlite_readonly(path)
    try:
        writer.execute("INSERT INTO t VALUES (1, 'a')")
        assert writer.in_transaction  # implicit deferred batch
        assert reader.execute("SELECT count(*) FROM t").fetchone() == (0,)
        writer.commit()
        assert reader.execute("SELECT count(*) FROM t").fetchone() == (1,)
        writer.execute("INSERT INTO t VALUES (2, 'b')")
        writer.rollback()
        assert reader.execute("SELECT count(*) FROM t").fetchone() == (1,)
    finally:
        reader.close()
        writer.close()


def test_foreign_keys_are_enforced_when_requested(tmp_path: Path) -> None:
    connection = open_sqlite_wal(tmp_path / "a.sqlite3", foreign_keys=True)
    try:
        connection.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)")
        connection.execute("CREATE TABLE c (p INTEGER REFERENCES p(id))")
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("INSERT INTO c VALUES (7)")
    finally:
        connection.close()


@pytest.mark.parametrize(
    "name",
    [
        "with space.sqlite3",
        "hash#name.sqlite3",
        "percent%41.sqlite3",
        pytest.param(
            "question?name.sqlite3",
            marks=pytest.mark.skipif(
                sys.platform == "win32", reason="Windows forbids '?' in file names"
            ),
        ),
    ],
)
def test_readonly_opens_paths_with_uri_metacharacters(tmp_path: Path, name: str) -> None:
    directory = tmp_path / "dir #? %20"
    if sys.platform == "win32":
        directory = tmp_path / "dir # %20"
    directory.mkdir()
    path = directory / name
    writer = _writer(path)
    writer.execute("INSERT INTO t VALUES (1, 'x')")
    writer.commit()
    reader = open_sqlite_readonly(path)
    try:
        assert reader.execute("SELECT v FROM t").fetchall() == [("x",)]
        assert reader.isolation_level is None
        assert _pragma(reader, "query_only") == 1
        with pytest.raises(sqlite3.OperationalError):
            reader.execute("INSERT INTO t VALUES (2, 'y')")
    finally:
        reader.close()
        writer.close()
    # A sibling literally named like the decoded URI must not be created.
    assert sorted(p.name for p in directory.iterdir() if p.suffix == ".sqlite3") == [name]


def test_readonly_missing_file_creates_nothing(tmp_path: Path) -> None:
    with pytest.raises(sqlite3.OperationalError):
        open_sqlite_readonly(tmp_path / "missing" / "a.sqlite3")
    with pytest.raises(sqlite3.OperationalError):
        open_sqlite_readonly(tmp_path / "a.sqlite3")
    assert list(tmp_path.iterdir()) == []


def test_readonly_immutable_and_timeout(tmp_path: Path) -> None:
    path = tmp_path / "a.sqlite3"
    writer = _writer(path)
    writer.execute("INSERT INTO t VALUES (1, 'x')")
    writer.commit()
    maintain_wal(writer, path, max_bytes=0)
    writer.close()
    reader = open_sqlite_readonly(path, busy_timeout_ms=250, immutable=True)
    try:
        assert _pragma(reader, "busy_timeout") == 250
        assert reader.execute("SELECT count(*) FROM t").fetchone() == (1,)
    finally:
        reader.close()


_LEASE_CHILD = textwrap.dedent(
    """
    import sys, time
    from pmkt.data.sqlite_wal import file_lease

    path, mode = sys.argv[1], sys.argv[2]
    if mode == "try":
        try:
            with file_lease(path, busy_message="busy-child"):
                print("acquired", flush=True)
        except RuntimeError as exc:
            print(f"refused:{exc}", flush=True)
        sys.exit(0)
    with file_lease(path):
        print("held", flush=True)
        time.sleep(60)
    """
)


def _child(path: Path, mode: str) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "-c", _LEASE_CHILD, str(path), mode],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def test_lease_contention_across_processes(tmp_path: Path) -> None:
    lock = tmp_path / ".lease"
    with file_lease(lock, busy_message="busy-parent"):
        child = _child(lock, "try")
        out, err = child.communicate(timeout=60)
        assert child.returncode == 0, err
        assert out.strip() == "refused:busy-child"
        with pytest.raises(RuntimeError, match="busy-parent"):
            with file_lease(lock, busy_message="busy-parent"):
                pass
    child = _child(lock, "try")
    out, err = child.communicate(timeout=60)
    assert out.strip() == "acquired", err
    assert lock.read_bytes() == b"0"


def test_lease_is_released_when_holder_dies(tmp_path: Path) -> None:
    lock = tmp_path / ".lease"
    holder = _child(lock, "hold")
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "held"
        with pytest.raises(RuntimeError, match="file lease is held"):
            with file_lease(lock):
                pass
    finally:
        holder.kill()
        holder.communicate(timeout=60)
    with file_lease(lock):
        pass
    assert lock.exists()


def test_lease_keeps_existing_lock_file_contents(tmp_path: Path) -> None:
    lock = tmp_path / ".lease"
    lock.write_bytes(b"existing")
    with file_lease(lock):
        pass
    assert lock.read_bytes() == b"existing"


def _grow_wal(connection: sqlite3.Connection, rows: int = 400) -> None:
    connection.executemany(
        "INSERT INTO t (v) VALUES (?)", [("x" * 1000,) for _ in range(rows)]
    )
    connection.commit()


def test_maintain_not_needed_and_missing_sidecar(tmp_path: Path) -> None:
    path = tmp_path / "a.sqlite3"
    writer = _writer(path)
    try:
        assert maintain_wal(writer, path)["state"] == "not_needed"
        assert maintain_wal(writer, tmp_path / "absent.sqlite3", max_bytes=0) == {
            "state": "not_needed",
            "wal_before_bytes": 0,
            "wal_after_bytes": 0,
        }
        with pytest.raises(ValueError):
            maintain_wal(writer, path, mode="FULL")  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            maintain_wal(writer, path, max_bytes=-1)
    finally:
        writer.close()


def test_maintain_defers_inside_transaction(tmp_path: Path) -> None:
    path = tmp_path / "a.sqlite3"
    writer = _writer(path)
    try:
        _grow_wal(writer)
        writer.execute("INSERT INTO t (v) VALUES ('pending')")
        assert writer.in_transaction
        result = maintain_wal(writer, path, max_bytes=1)
        assert result["state"] == "deferred"
        assert result["reason"] == "Active writer transaction"
        writer.commit()
    finally:
        writer.close()


def test_truncate_reclaims_and_restores_timeout(tmp_path: Path) -> None:
    path = tmp_path / "a.sqlite3"
    writer = _writer(path, busy_timeout_ms=7000)
    try:
        _grow_wal(writer)
        result = maintain_wal(writer, path, max_bytes=1)
        assert result["state"] == "reclaimed"
        assert result["wal_after_bytes"] == 0
        assert int(result["wal_before_bytes"]) > 0
        assert result["log_frames"] == result["checkpointed_frames"]
        assert _pragma(writer, "busy_timeout") == 7000
    finally:
        writer.close()


def test_truncate_defers_behind_reader_without_waiting(tmp_path: Path) -> None:
    path = tmp_path / "a.sqlite3"
    writer = _writer(path, busy_timeout_ms=10_000)
    reader = open_sqlite_readonly(path)
    try:
        _grow_wal(writer)
        reader.execute("BEGIN")
        reader.execute("SELECT count(*) FROM t").fetchone()
        _grow_wal(writer)
        result = maintain_wal(writer, path, max_bytes=1)
        assert result["state"] == "deferred"
        assert _pragma(writer, "busy_timeout") == 10_000
        reader.rollback()
        assert maintain_wal(writer, path, max_bytes=1)["state"] == "reclaimed"
    finally:
        reader.close()
        writer.close()


def test_passive_reports_outstanding_frames(tmp_path: Path) -> None:
    path = tmp_path / "a.sqlite3"
    writer = _writer(path, isolation_level=None)
    writer.execute("PRAGMA wal_autocheckpoint=0")
    reader = open_sqlite_readonly(path)
    try:
        writer.execute("BEGIN IMMEDIATE")
        _grow_wal(writer, 50)
        reader.execute("BEGIN")
        reader.execute("SELECT count(*) FROM t").fetchone()
        writer.execute("BEGIN IMMEDIATE")
        _grow_wal(writer, 50)
        partial = maintain_wal(writer, path, max_bytes=1, mode="PASSIVE")
        assert partial["state"] == "partial"
        assert int(partial["outstanding_frames"]) > 0
        assert partial["outstanding_frames"] == int(partial["log_frames"]) - int(
            partial["checkpointed_frames"]
        )
        reader.rollback()
        done = maintain_wal(writer, path, max_bytes=1, mode="PASSIVE")
        assert done["state"] == "checkpointed"
        assert done["outstanding_frames"] == 0
        # PASSIVE leaves the sidecar for later resets.
        assert int(done["wal_after_bytes"]) > 0
        assert _pragma(writer, "busy_timeout") == 5000
    finally:
        reader.close()
        writer.close()


class _FailingCheckpoint(sqlite3.Connection):
    def execute(self, sql: str, *args: object) -> sqlite3.Cursor:  # type: ignore[override]
        if sql.startswith("PRAGMA wal_checkpoint"):
            raise sqlite3.OperationalError("database table is locked")
        return super().execute(sql, *args)  # type: ignore[arg-type]


def test_checkpoint_error_defers_and_restores_timeout(tmp_path: Path) -> None:
    path = tmp_path / "a.sqlite3"
    _writer(path).close()
    connection = sqlite3.connect(path, factory=_FailingCheckpoint)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.execute("PRAGMA busy_timeout=4321")
        _grow_wal(connection)
        result = maintain_wal(connection, path, max_bytes=1)
        assert result["state"] == "deferred"
        assert result["reason"] == "database table is locked"
        assert _pragma(connection, "busy_timeout") == 4321
    finally:
        connection.close()


def test_closed_files_can_be_deleted_and_renamed_immediately(tmp_path: Path) -> None:
    directory = tmp_path / "store"
    directory.mkdir()
    path = directory / "a.sqlite3"
    writer = _writer(path)
    reader = open_sqlite_readonly(path)
    reader.execute("SELECT count(*) FROM t").fetchone()
    reader.close()
    writer.close()
    renamed = tmp_path / "renamed"
    directory.rename(renamed)
    for child in list(renamed.iterdir()):
        child.unlink()
    renamed.rmdir()
