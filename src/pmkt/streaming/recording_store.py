"""SQLite is the recording authority; Parquet is a repeatable, verified export."""

from __future__ import annotations

import hashlib
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
from typing import Any, Iterator, Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from pmkt.data.recording_schema import RECORDING_FIELDS, RECORDING_KEYS
from pmkt.data.registry import arrow_schema, get_table_spec

FORMAT = "recording.v1"
BATCH_ROWS = 4096
BATCH_SECONDS = 1.0


@contextmanager
def recording_lock(root: Path) -> Iterator[None]:
    """An OS-held lease survives neither process death nor a closed recorder."""
    with (root / ".recording.lock").open("a+b") as handle:
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
            raise RuntimeError(
                "recording is active or another export is running"
            ) from exc
        try:
            yield
        finally:
            handle.seek(0)
            if sys.platform == "win32":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def json_text(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def write_json(path: Path, value: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(json_text(value) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


class RecordingStore:
    def __init__(self, run_dir: Path, metadata: dict[str, Any]) -> None:
        self.run_dir = run_dir
        self.lease = recording_lock(run_dir)
        self.lease.__enter__()
        try:
            self.connection = sqlite3.connect(run_dir / "recording.sqlite")
        except BaseException:
            self.lease.__exit__(None, None, None)
            raise
        self.metadata = metadata
        self.tables = tuple(
            name
            for name in RECORDING_FIELDS
            if metadata["options"]["mode"] == "full" or not name.startswith("book_")
        )
        try:
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA synchronous=FULL")
            self.connection.execute("PRAGMA foreign_keys=ON")
            self.connection.execute(
                "CREATE TABLE metadata (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL)"
            )
            for name in self.tables:
                types = {
                    "string": "TEXT",
                    "int64": "INTEGER",
                    "bool": "INTEGER",
                    "float64": "REAL",
                }
                columns = [
                    f'"{field}" {types[dtype]}' + ("" if nullable else " NOT NULL")
                    for field, dtype, nullable in RECORDING_FIELDS[name]
                ]
                columns.append("PRIMARY KEY (" + ",".join(RECORDING_KEYS[name]) + ")")
                if name == "book_levels":
                    columns.append(
                        "FOREIGN KEY(run_id,snapshot_id) REFERENCES book_snapshots(run_id,snapshot_id)"
                    )
                    columns.extend(
                        (
                            "CHECK(side IN ('bid','ask'))",
                            "CHECK(quantity > 0)",
                            "CHECK(price >= 0 AND price <= 1)",
                        )
                    )
                self.connection.execute(f'CREATE TABLE "{name}" ({",".join(columns)})')
            self.connection.execute(
                "CREATE UNIQUE INDEX trade_identity ON trades(venue,venue_trade_id) WHERE venue_trade_id IS NOT NULL"
            )
            self.connection.execute(
                "INSERT INTO metadata VALUES (1, ?)", (json_text(metadata),)
            )
            self.connection.commit()
        except BaseException:
            self.connection.close()
            self.lease.__exit__(None, None, None)
            raise
        self.pending_rows = 0
        self.last_commit = time.monotonic()
        self.failed = False

    def append(self, table: str, row: Mapping[str, Any]) -> None:
        if self.failed:
            raise RuntimeError("recording store has failed")
        fields = RECORDING_FIELDS[table]
        values = [
            f"recording_{table}.v1" if field == "schema_version" else row.get(field)
            for field, _, _ in fields
        ]
        placeholders = ",".join("?" for _ in fields)
        try:
            self.connection.execute(
                f'INSERT INTO "{table}" VALUES ({placeholders})', values
            )
            self.pending_rows += 1
        except BaseException:
            self.connection.rollback()
            self.failed = True
            raise

    def flush_due(self) -> bool:
        return (
            self.pending_rows >= BATCH_ROWS
            or time.monotonic() - self.last_commit >= BATCH_SECONDS
        )

    def flush(self, metadata: dict[str, Any], *, force: bool = False) -> None:
        if self.failed:
            raise RuntimeError("recording store has failed")
        if not force and not self.flush_due():
            return
        try:
            self.connection.execute(
                "UPDATE metadata SET value=? WHERE id=1", (json_text(metadata),)
            )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            self.failed = True
            raise
        self.metadata = metadata
        self.pending_rows = 0
        self.last_commit = time.monotonic()

    def close(self) -> None:
        # A failed batch is never implicitly committed by cleanup.
        self.connection.rollback()
        self.connection.close()
        self.lease.__exit__(None, None, None)


def export_recording(run_dir: str | Path) -> dict[str, Any]:
    """Export a stopped recorder or recover committed rows after a process crash.

    Never invoke against a running recorder. The database is retained. Repeating
    this operation replaces exports rather than appending observations.
    """
    root = Path(run_dir).resolve()
    with recording_lock(root):
        return _export_recording(root)


def _export_recording(root: Path) -> dict[str, Any]:
    db = sqlite3.connect(f"{(root / 'recording.sqlite').as_uri()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute("BEGIN")  # one consistent database snapshot for all tables
        metadata = json.loads(
            db.execute("SELECT value FROM metadata WHERE id=1").fetchone()[0]
        )
        if metadata.get("recording_format") != FORMAT:
            raise ValueError("unsupported recording format")
        (root / "manifest.json").unlink(missing_ok=True)
        tables = tuple(
            name
            for name in RECORDING_FIELDS
            if metadata["options"]["mode"] == "full" or not name.startswith("book_")
        )
        if "book_levels" in tables:
            invalid = db.execute(
                "SELECT 1 FROM book_snapshots s LEFT JOIN book_levels l "
                "ON s.run_id=l.run_id AND s.snapshot_id=l.snapshot_id "
                "GROUP BY s.run_id,s.snapshot_id HAVING "
                "sum(CASE WHEN l.side='bid' THEN 1 ELSE 0 END) != s.bid_level_count OR "
                "sum(CASE WHEN l.side='ask' THEN 1 ELSE 0 END) != s.ask_level_count LIMIT 1"
            ).fetchone()
            if invalid or db.execute("PRAGMA foreign_key_check").fetchone():
                raise ValueError("snapshot headers and levels disagree")
        artifacts = {}
        for name in tables:
            schema_id = f"recording_{name}.v1"
            schema = arrow_schema(get_table_spec(schema_id))
            target = root / f"{name}.parquet"
            temporary = root / f"{name}.parquet.tmp"
            cursor = db.execute(
                f'SELECT * FROM "{name}" ORDER BY {",".join(RECORDING_KEYS[name])}'
            )
            count = 0
            try:
                with pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
                    while rows := cursor.fetchmany(BATCH_ROWS):
                        records = [dict(row) for row in rows]
                        for record in records:
                            for field, dtype, _ in RECORDING_FIELDS[name]:
                                if dtype == "bool" and record[field] is not None:
                                    record[field] = bool(record[field])
                        writer.write_table(pa.Table.from_pylist(records, schema=schema))
                        count += len(rows)
                parquet = pq.ParquetFile(temporary)
                try:
                    if (
                        parquet.metadata.num_rows != count
                        or not parquet.schema_arrow.equals(schema)
                    ):
                        raise ValueError(f"export verification failed: {name}")
                finally:
                    parquet.close()
                temporary.replace(target)
            finally:
                temporary.unlink(missing_ok=True)
            digest = hashlib.sha256()
            with target.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            artifacts[name] = {
                "path": target.name,
                "schema": schema_id,
                "rows": count,
                "sha256": digest.hexdigest(),
            }
        metadata["artifacts"] = artifacts
        metadata["counts"] = {
            name: artifact["rows"] for name, artifact in artifacts.items()
        }
        if metadata.get("status") == "recording":
            metadata["status"] = (
                "partial" if metadata.get("ever_intact_instruments") else "failed"
            )
            metadata["stop_reason"] = "process_interrupted"
        failure_path = root / "failure.json"
        if failure_path.exists():
            failure = json.loads(failure_path.read_text(encoding="utf-8"))
            if failure.get("phase") == "persistence":
                metadata.update(
                    status="failed",
                    stop_reason="persistence_error",
                    error=failure.get("error"),
                )
        metadata["export_status"] = "complete"
        write_json(root / "manifest.json", metadata)
        return metadata
    except BaseException as exc:
        # An old successful manifest must not describe a failed re-export.
        (root / "manifest.json").unlink(missing_ok=True)
        failure_path = root / "failure.json"
        previous = (
            json.loads(failure_path.read_text(encoding="utf-8"))
            if failure_path.exists()
            else {}
        )
        if previous.get("phase") != "persistence":
            write_json(
                failure_path, {"status": "failed", "phase": "export", "error": str(exc)}
            )
        raise
    finally:
        db.close()


def inspect_recording(run_dir: str | Path) -> dict[str, Any]:
    """Inspect committed metadata/counts without finalizing or changing files."""
    root = Path(run_dir).resolve()
    db = sqlite3.connect(f"{(root / 'recording.sqlite').as_uri()}?mode=ro", uri=True)
    try:
        db.execute("BEGIN")
        result = json.loads(
            db.execute("SELECT value FROM metadata WHERE id=1").fetchone()[0]
        )
        if result.get("recording_format") != FORMAT:
            raise ValueError("unsupported recording format")
        result["counts"] = {
            name: db.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
            for name in RECORDING_FIELDS
            if result["options"]["mode"] == "full" or not name.startswith("book_")
        }
        return result
    finally:
        db.close()


def validate_recording_manifest(path: str | Path) -> tuple[str, ...]:
    """Validate the declared new-format exports without requiring retained SQLite."""
    manifest_path = Path(path)
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return (f"cannot read recording manifest: {exc}",)
    if not isinstance(payload, dict):
        return ("recording manifest must be an object",)
    errors = []
    if payload.get("recording_format") != FORMAT:
        return ("unsupported recording format",)
    if (
        payload.get("status") not in ("complete", "partial", "failed")
        or payload.get("export_status") != "complete"
    ):
        errors.append("invalid recording/export status")
    options = payload.get("options")
    mode = options.get("mode") if isinstance(options, dict) else None
    if mode not in ("topbook", "full"):
        return ("invalid recording mode",)
    counts = payload.get("counts")
    if not isinstance(counts, dict):
        return ("recording counts must be an object",)
    expected = {
        name
        for name in RECORDING_FIELDS
        if mode == "full" or not name.startswith("book_")
    }
    artifacts = payload.get("artifacts", {})
    if not isinstance(artifacts, dict) or set(artifacts) != expected:
        return ("recording artifact roles do not match mode",)
    for name in sorted(expected):
        artifact = artifacts[name]
        if (
            not isinstance(artifact, dict)
            or artifact.get("path") != f"{name}.parquet"
            or artifact.get("schema") != f"recording_{name}.v1"
        ):
            errors.append(f"{name}: invalid artifact path/schema")
            continue
        file = manifest_path.parent / f"{name}.parquet"
        if file.resolve().parent != manifest_path.parent.resolve():
            errors.append(f"{name}: artifact escapes recording directory")
            continue
        try:
            digest = hashlib.sha256()
            with file.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() != artifact.get("sha256"):
                errors.append(f"{name}: SHA-256 mismatch")
            with pq.ParquetFile(file) as parquet:
                if (
                    parquet.metadata.num_rows != artifact.get("rows")
                    or type(artifact.get("rows")) is not int
                ):
                    errors.append(f"{name}: row count mismatch")
                if counts.get(name) != artifact.get("rows"):
                    errors.append(f"{name}: report count mismatch")
                if not parquet.schema_arrow.equals(
                    arrow_schema(get_table_spec(artifact["schema"]))
                ):
                    errors.append(f"{name}: schema mismatch")
                for batch in parquet.iter_batches(columns=["run_id", "schema_version"]):
                    for row in batch.to_pylist():
                        if (
                            row["run_id"] != payload.get("run_id")
                            or row["schema_version"] != artifact["schema"]
                        ):
                            errors.append(
                                f"{name}: row identity/schema disagrees with manifest"
                            )
                            break
                    else:
                        continue
                    break
        except (OSError, ValueError, pa.ArrowException) as exc:
            errors.append(f"{name}: {exc}")
    return tuple(errors)
