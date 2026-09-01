from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import sqlite3
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping

import pandas as pd

from pmkt.data.registry import arrow_schema, get_table_spec
from pmkt.data.validation import coerce_frame, validate_frame
from pmkt.streaming.durability import (
    COMMIT_JOURNAL_NAME,
    COMMIT_JOURNAL_V1_NAME,
    SCHEMA_MAP_NAME,
    DurableCaptureCoordinator,
    _checked_column,
    _fsync_directory,
    _role_sort_key,
    _utc_now,
    file_sha256,
    normalize_capture_value,
    validate_committed_capture_group,
)
from pmkt.streaming.durability_settings import CaptureDurabilitySettings
from pmkt.streaming.recovery_contracts import (
    CaptureCommitArtifactV1,
    CaptureCommitCause,
    CaptureCommitRecordV2,
    RunStateV1,
    resolve_run_relative_path,
)
from pmkt.streaming.storage_backends import (
    CaptureStorageBackend,
    CaptureStorageSettings,
    sample_summary,
)
from pmkt.streaming.tape import canonical_json_bytes, semantic_hash

SQLITE_CAPTURE_FORMAT = "pmkt.sqlite_capture.v1"
SQLITE_SCHEMA_VERSION = 1
SQLITE_GROUP_FORMAT = "pmkt.sqlite_capture_group.v1"
PROMOTION_NAMESPACE = "sqlite-capture-promotion.v1"


@dataclass(frozen=True)
class SQLiteCaptureInspection:
    group_count: int
    role_row_counts: Mapping[str, int]
    cause_counts: Mapping[str, int]
    errors: tuple[str, ...] = ()

    @property
    def recoverable(self) -> bool:
        return self.group_count > 0 and not self.errors


class SQLiteCaptureCoordinator(DurableCaptureCoordinator):
    """Persist logical capture groups transactionally, then promote once.

    SQLite is authoritative while the run is recording.  Parquet and the existing
    capture journal are materialized only after a clean seal or explicit recovery.
    """

    storage_backend: CaptureStorageBackend = CaptureStorageBackend.SQLITE_WAL

    def __init__(self, **kwargs: Any) -> None:
        external_roles = tuple(kwargs.get("external_file_roles") or ())
        if external_roles:
            raise ValueError(
                "sqlite_wal_v1 does not yet support external file roles; disable "
                "raw_jsonl or use parquet_segments"
            )
        super().__init__(**kwargs)
        for role, version in self.role_schema_versions.items():
            if role not in self.role_schemas:
                try:
                    self.role_schemas[role] = arrow_schema(get_table_spec(version))
                except KeyError as exc:
                    raise ValueError(
                        f"sqlite storage requires an explicit Arrow schema for "
                        f"role {role!r} with unregistered version {version!r}"
                    ) from exc
        if set(self.role_schemas) != set(self.role_schema_versions):
            raise ValueError(
                "sqlite storage requires Arrow schemas for every capture role"
            )
        self._logical_groups_committed = 0
        self._logical_cause_counts: Counter[str] = Counter()
        self._logical_commit_latencies_ms: list[float] = []
        self._logical_latencies_by_cause_ms: dict[str, list[float]] = defaultdict(list)
        self._logical_group_rows: list[int] = []
        self._logical_group_input_bytes: list[int] = []
        self._wal_peak_bytes = 0
        self._resource_metrics_error_count = 0
        self._last_resource_metrics_error: str | None = None
        self._promotion_attempt_count = 0
        self._promotion_failure_count = 0
        self._promotion_latencies_ms: list[float] = []
        self._promotion_input_bytes = 0
        self._promotion_output_bytes = 0
        self._promotion_output_files = 0
        self._database_status = "recording"
        self._connection: sqlite3.Connection | None = self._open_connection(create=True)
        self._initialize_database()

    @classmethod
    def open_existing(
        cls, run_dir: str | Path, state: RunStateV1
    ) -> "SQLiteCaptureCoordinator":
        root = Path(run_dir).resolve()
        storage = CaptureStorageSettings.from_mapping(state.capture_storage or {})
        if storage.backend is not CaptureStorageBackend.SQLITE_WAL:
            raise ValueError("run state does not declare sqlite_wal_v1 storage")
        schema_payload = json.loads(
            (root / SCHEMA_MAP_NAME).read_text(encoding="utf-8")
        )
        raw_versions = (
            schema_payload.get("roles") if isinstance(schema_payload, Mapping) else None
        )
        if not isinstance(raw_versions, Mapping):
            raise ValueError("capture schema version map is invalid")

        instance = object.__new__(cls)
        instance.run_dir = root
        instance.state = state
        instance.role_schema_versions = {
            str(role): str(version) for role, version in raw_versions.items()
        }
        if set(instance.role_schema_versions) != set(state.expected_role_paths):
            raise ValueError(
                "capture schema version map does not match run state roles"
            )
        instance.adapter_settings_by_venue = {
            str(venue): dict(settings)
            for venue, settings in (state.adapter_settings_by_venue or {}).items()
        }
        instance.role_schemas = {}
        instance.external_file_roles = frozenset()
        instance.durability_settings = CaptureDurabilitySettings.from_mapping(
            state.capture_durability or {}
        )
        instance.storage_settings = storage
        instance.segment_row_limit = instance.durability_settings.effective_segment_rows
        instance.commit_interval_seconds = (
            instance.durability_settings.effective_segment_seconds
        )
        instance._buffers = defaultdict(list)
        instance._buffered_rows = 0
        instance._group_index = 0
        instance._last_commit_monotonic = time.monotonic()
        instance._first_pending_monotonic = None
        instance._groups_published = 0
        instance._groups_accepted = 0
        instance._cause_counts = Counter()
        instance._publication_latencies_ms = []
        instance._publication_latencies_by_cause_ms = defaultdict(list)
        instance._group_row_counts = []
        instance._group_file_counts = []
        instance._group_output_bytes = []
        instance._group_canonical_input_bytes = []
        instance._minimum_disk_free_bytes = None
        instance._maximum_uncommitted_age_seconds = 0.0
        instance._role_row_counts = Counter()
        instance._committed_roles = set()
        instance._segment_manifests_written = False
        instance._tape_books = {}
        instance._logical_groups_committed = 0
        instance._logical_cause_counts = Counter()
        instance._logical_commit_latencies_ms = []
        instance._logical_latencies_by_cause_ms = defaultdict(list)
        instance._logical_group_rows = []
        instance._logical_group_input_bytes = []
        instance._wal_peak_bytes = 0
        instance._resource_metrics_error_count = 0
        instance._last_resource_metrics_error = None
        instance._promotion_attempt_count = 0
        instance._promotion_failure_count = 0
        instance._promotion_latencies_ms = []
        instance._promotion_input_bytes = 0
        instance._promotion_output_bytes = 0
        instance._promotion_output_files = 0
        instance._database_status = "recording"
        instance._connection = instance._open_connection(create=False)
        try:
            metadata = _metadata(instance._require_connection())
            instance.role_schemas = _deserialize_arrow_schemas(
                metadata.get("arrow_schemas") or ""
            )
            if set(instance.role_schemas) != set(instance.role_schema_versions):
                raise ValueError(
                    "sqlite capture Arrow schemas do not match run-state roles"
                )
            instance._load_persisted_metrics()
            instance._update_resource_metrics_best_effort()
        except BaseException:
            instance.close()
            raise
        return instance

    @property
    def database_path(self) -> Path:
        relative = self.storage_settings.authoritative_path
        if relative is None:
            raise RuntimeError("sqlite storage has no authoritative path")
        return resolve_run_relative_path(
            self.run_dir, relative, key="capture_storage.authoritative_path"
        )

    def _open_connection(self, *, create: bool) -> sqlite3.Connection:
        path = self.database_path
        path.parent.mkdir(parents=True, exist_ok=True)
        if create:
            connection = sqlite3.connect(
                path, isolation_level=None, check_same_thread=False
            )
        else:
            connection = sqlite3.connect(
                f"file:{path.as_posix()}?mode=rw",
                uri=True,
                isolation_level=None,
                check_same_thread=False,
            )
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        journal_mode = str(
            connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
        )
        if journal_mode.lower() != "wal":
            connection.close()
            raise RuntimeError("sqlite capture could not enable WAL mode")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize_database(self) -> None:
        connection = self._require_connection()
        connection.executescript(
            """
            BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) STRICT;
            CREATE TABLE IF NOT EXISTS capture_groups (
                group_index INTEGER PRIMARY KEY,
                group_id TEXT NOT NULL UNIQUE,
                cause TEXT NOT NULL,
                accepted_at_utc TEXT NOT NULL,
                committed_at_utc TEXT NOT NULL,
                row_count INTEGER NOT NULL CHECK (row_count > 0),
                canonical_bytes INTEGER NOT NULL CHECK (canonical_bytes > 0),
                commit_latency_ms REAL NOT NULL CHECK (commit_latency_ms >= 0),
                checksum_sha256 TEXT NOT NULL
            ) STRICT;
            CREATE TABLE IF NOT EXISTS capture_rows (
                group_index INTEGER NOT NULL,
                role TEXT NOT NULL,
                row_index INTEGER NOT NULL,
                local_sequence INTEGER NOT NULL CHECK (local_sequence >= 0),
                payload BLOB NOT NULL,
                PRIMARY KEY (group_index, role, row_index),
                FOREIGN KEY (group_index) REFERENCES capture_groups(group_index)
                    ON DELETE RESTRICT
            ) STRICT;
            CREATE INDEX IF NOT EXISTS capture_rows_role_order
                ON capture_rows(role, group_index, row_index);
            COMMIT;
            """
        )
        connection.execute(f"PRAGMA user_version = {SQLITE_SCHEMA_VERSION}")
        expected = {
            "format": SQLITE_CAPTURE_FORMAT,
            "run_id": self.state.run_id,
            "status": "recording",
            "capture_storage": json.dumps(
                self.storage_settings.to_mapping(),
                separators=(",", ":"),
                sort_keys=True,
            ),
            "schema_versions": json.dumps(
                self.role_schema_versions, separators=(",", ":"), sort_keys=True
            ),
            "arrow_schemas": _serialize_arrow_schemas(self.role_schemas),
            "wal_peak_bytes": "0",
            "minimum_disk_free_bytes": "",
            "promotion_attempt_count": "0",
            "promotion_failure_count": "0",
            "promotion_latencies_ms": "[]",
            "promotion_input_bytes": "0",
            "promotion_output_bytes": "0",
            "promotion_output_files": "0",
        }
        connection.execute("BEGIN IMMEDIATE")
        try:
            for key, value in expected.items():
                connection.execute(
                    "INSERT INTO metadata(key, value) VALUES (?, ?)", (key, value)
                )
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise

    def _require_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("sqlite capture database is closed")
        return self._connection

    def commit(
        self,
        *,
        cause: CaptureCommitCause | str | None = None,
        force: bool = False,
    ) -> CaptureCommitRecordV2 | None:
        if not self.has_pending_rows:
            return None
        threshold_cause = self.due_cause()
        if force:
            if cause is None:
                raise ValueError("forced commits require a canonical cause")
            normalized_cause = CaptureCommitCause(cause)
        else:
            if threshold_cause is None:
                return None
            normalized_cause = threshold_cause

        frozen = {role: tuple(rows) for role, rows in self._buffers.items() if rows}
        proposed_tape_books = self._validate_group(frozen)
        accepted_at_utc = _utc_now()
        accepted_monotonic = time.monotonic()
        if self._first_pending_monotonic is not None:
            self._maximum_uncommitted_age_seconds = max(
                self._maximum_uncommitted_age_seconds,
                accepted_monotonic - self._first_pending_monotonic,
            )
        encoded: list[tuple[str, int, int, bytes]] = []
        hashes_by_role: dict[str, list[str]] = {}
        canonical_bytes = 0
        for role in sorted(frozen, key=_role_sort_key):
            role_hashes: list[str] = []
            for row_index, row in enumerate(frozen[role]):
                payload = canonical_json_bytes(row)
                canonical_bytes += len(payload) + 1
                role_hashes.append(hashlib.sha256(payload).hexdigest())
                sequence = int(row.get("local_sequence") or row.get("sequence") or 0)
                encoded.append((role, row_index, sequence, payload))
            hashes_by_role[role] = role_hashes
        group_index = self._logical_groups_committed
        group_id = semantic_hash(
            [
                SQLITE_GROUP_FORMAT,
                self.state.run_id,
                group_index,
                normalized_cause.value,
                hashes_by_role,
            ]
        )
        committed_at_utc = _utc_now()
        precommit_latency_ms = (time.monotonic() - accepted_monotonic) * 1_000.0
        checksum = _sqlite_group_checksum(
            group_id=group_id,
            group_index=group_index,
            cause=normalized_cause.value,
            accepted_at_utc=accepted_at_utc,
            committed_at_utc=committed_at_utc,
            row_count=len(encoded),
            canonical_bytes=canonical_bytes,
        )
        connection = self._require_connection()
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                """
                INSERT INTO capture_groups(
                    group_index, group_id, cause, accepted_at_utc,
                    committed_at_utc, row_count, canonical_bytes,
                    commit_latency_ms, checksum_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    group_index,
                    group_id,
                    normalized_cause.value,
                    accepted_at_utc,
                    committed_at_utc,
                    len(encoded),
                    canonical_bytes,
                    precommit_latency_ms,
                    checksum,
                ),
            )
            connection.executemany(
                """
                INSERT INTO capture_rows(
                    group_index, role, row_index, local_sequence, payload
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    (group_index, role, row_index, sequence, payload)
                    for role, row_index, sequence, payload in encoded
                ),
            )
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise

        latency_ms = (time.monotonic() - accepted_monotonic) * 1_000.0
        # SQL COMMIT is the authoritative persistence boundary. Reconcile the
        # in-process state before any optional observability work so a metrics
        # failure cannot report a durable group as failed or leave it buffered
        # for replay under the same group index.
        self._tape_books = proposed_tape_books
        self._logical_groups_committed += 1
        self._logical_cause_counts[normalized_cause.value] += 1
        self._logical_commit_latencies_ms.append(latency_ms)
        self._logical_latencies_by_cause_ms[normalized_cause.value].append(latency_ms)
        self._logical_group_rows.append(len(encoded))
        self._logical_group_input_bytes.append(canonical_bytes)
        for role, rows in frozen.items():
            self._role_row_counts[role] += len(rows)
            self._committed_roles.add(role)
        self._buffers.clear()
        self._buffered_rows = 0
        self._first_pending_monotonic = None
        self._last_commit_monotonic = time.monotonic()
        self._update_resource_metrics_best_effort()
        return None

    def _update_resource_metrics(self) -> None:
        wal = self.database_path.with_name(f"{self.database_path.name}-wal")
        self._wal_peak_bytes = max(
            self._wal_peak_bytes,
            wal.stat().st_size if wal.exists() else 0,
        )
        free = shutil.disk_usage(self.run_dir).free
        self._minimum_disk_free_bytes = (
            free
            if self._minimum_disk_free_bytes is None
            else min(self._minimum_disk_free_bytes, free)
        )

    def _update_resource_metrics_best_effort(self) -> None:
        try:
            self._update_resource_metrics()
        except Exception as exc:
            self._resource_metrics_error_count += 1
            self._last_resource_metrics_error = f"{type(exc).__name__}: {exc}"

    def inspect(self, *, validate_payloads: bool = True) -> SQLiteCaptureInspection:
        errors: list[str] = []
        cause_counts: Counter[str] = Counter()
        role_counts: Counter[str] = Counter()
        tape_books: dict[Any, Any] = {}
        expected_index = 0
        try:
            self._validate_database_authority()
            for group in self._iter_group_records():
                if group["group_index"] != expected_index:
                    raise ValueError(
                        "sqlite capture group indexes are not contiguous: "
                        f"expected {expected_index}, found {group['group_index']}"
                    )
                rows_by_role, hashes_by_role, canonical_bytes = self._read_group_rows(
                    expected_index
                )
                unknown_roles = sorted(
                    set(rows_by_role) - set(self.role_schema_versions)
                )
                if unknown_roles:
                    raise ValueError(
                        f"sqlite capture group {expected_index} has unknown roles: "
                        + ", ".join(unknown_roles)
                    )
                CaptureCommitCause(str(group["cause"]))
                row_count = sum(len(rows) for rows in rows_by_role.values())
                expected_group_id = semantic_hash(
                    [
                        SQLITE_GROUP_FORMAT,
                        self.state.run_id,
                        expected_index,
                        group["cause"],
                        hashes_by_role,
                    ]
                )
                if group["group_id"] != expected_group_id:
                    raise ValueError(
                        f"sqlite capture group {expected_index} identity mismatch"
                    )
                if group["row_count"] != row_count:
                    raise ValueError(
                        f"sqlite capture group {expected_index} row count mismatch"
                    )
                if group["canonical_bytes"] != canonical_bytes:
                    raise ValueError(
                        f"sqlite capture group {expected_index} byte count mismatch"
                    )
                expected_checksum = _sqlite_group_checksum(
                    group_id=group["group_id"],
                    group_index=expected_index,
                    cause=group["cause"],
                    accepted_at_utc=group["accepted_at_utc"],
                    committed_at_utc=group["committed_at_utc"],
                    row_count=row_count,
                    canonical_bytes=canonical_bytes,
                )
                if group["checksum_sha256"] != expected_checksum:
                    raise ValueError(
                        f"sqlite capture group {expected_index} checksum mismatch"
                    )
                if validate_payloads:
                    tape_books = validate_committed_capture_group(
                        run_state=self.state,
                        role_schema_versions=self.role_schema_versions,
                        rows_by_role=rows_by_role,
                        adapter_settings_by_venue=self.adapter_settings_by_venue,
                        prior_tape_books=tape_books,
                    )
                cause_counts[group["cause"]] += 1
                for role, rows in rows_by_role.items():
                    role_counts[role] += len(rows)
                expected_index += 1
        except (
            KeyError,
            OSError,
            TypeError,
            sqlite3.DatabaseError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            errors.append(str(exc))
        return SQLiteCaptureInspection(
            group_count=expected_index if not errors else 0,
            role_row_counts=dict(sorted(role_counts.items())),
            cause_counts=dict(sorted(cause_counts.items())),
            errors=tuple(errors),
        )

    def _validate_database_authority(self) -> None:
        connection = self._require_connection()
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version != SQLITE_SCHEMA_VERSION:
            raise ValueError(
                f"sqlite capture schema version must equal {SQLITE_SCHEMA_VERSION}"
            )
        metadata = _metadata(connection)
        if metadata.get("format") != SQLITE_CAPTURE_FORMAT:
            raise ValueError("sqlite capture format is invalid")
        if metadata.get("run_id") != self.state.run_id:
            raise ValueError("sqlite capture run_id does not match run state")
        if metadata.get("status") not in {"recording", "sealed", "promoted"}:
            raise ValueError("sqlite capture status is invalid")
        if json.loads(metadata.get("capture_storage") or "null") != (
            self.storage_settings.to_mapping()
        ):
            raise ValueError("sqlite capture storage settings do not match run state")
        if json.loads(metadata.get("schema_versions") or "null") != (
            self.role_schema_versions
        ):
            raise ValueError("sqlite capture schema versions do not match run state")
        if metadata.get("arrow_schemas") != _serialize_arrow_schemas(self.role_schemas):
            raise ValueError("sqlite capture Arrow schemas do not match authority")
        integrity = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        if integrity != "ok":
            raise ValueError(f"sqlite capture quick_check failed: {integrity}")

    def _iter_group_records(self) -> Iterator[dict[str, Any]]:
        cursor = self._require_connection().execute(
            """
            SELECT group_index, group_id, cause, accepted_at_utc,
                   committed_at_utc, row_count, canonical_bytes,
                   commit_latency_ms, checksum_sha256
            FROM capture_groups ORDER BY group_index
            """
        )
        columns = tuple(item[0] for item in cursor.description or ())
        for row in cursor:
            yield dict(zip(columns, row, strict=True))

    def _read_group_rows(
        self, group_index: int
    ) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[str]], int]:
        rows_by_role: dict[str, list[dict[str, Any]]] = defaultdict(list)
        hashes_by_role: dict[str, list[str]] = defaultdict(list)
        canonical_bytes = 0
        cursor = self._require_connection().execute(
            """
            SELECT role, row_index, payload FROM capture_rows
            WHERE group_index = ? ORDER BY role, row_index
            """,
            (group_index,),
        )
        previous: dict[str, int] = {}
        for role, row_index, raw_payload in cursor:
            role_text = str(role)
            expected = previous.get(role_text, -1) + 1
            if int(row_index) != expected:
                raise ValueError(
                    f"sqlite capture group {group_index} role {role_text!r} has "
                    "non-contiguous row indexes"
                )
            previous[role_text] = expected
            payload = bytes(raw_payload)
            canonical_bytes += len(payload) + 1
            hashes_by_role[role_text].append(hashlib.sha256(payload).hexdigest())
            decoded = json.loads(payload)
            if not isinstance(decoded, Mapping):
                raise ValueError("sqlite capture row payload must be an object")
            if canonical_json_bytes(decoded) != payload:
                raise ValueError(
                    f"sqlite capture group {group_index} role {role_text!r} "
                    f"row {row_index} is not canonical JSON"
                )
            rows_by_role[role_text].append(dict(decoded))
        return dict(rows_by_role), dict(hashes_by_role), canonical_bytes

    def finalize_segments(self) -> None:
        if self._segment_manifests_written:
            return
        self.commit(cause=CaptureCommitCause.CLEAN_SHUTDOWN, force=True)
        inspection = self.inspect(validate_payloads=True)
        if inspection.errors:
            raise ValueError(
                "cannot seal invalid sqlite capture: " + "; ".join(inspection.errors)
            )
        self._set_database_status("sealed")
        self._checkpoint_wal()
        self._promote()
        self._segment_manifests_written = True

    def mark_finalized(self) -> None:
        self._checkpoint_wal()
        super().mark_finalized()
        self.close()

    def _set_database_status(self, status: str) -> None:
        connection = self._require_connection()
        connection.execute("BEGIN IMMEDIATE")
        try:
            _set_metadata(connection, "status", status)
            _set_metadata(connection, "wal_peak_bytes", str(self._wal_peak_bytes))
            _set_metadata(
                connection,
                "minimum_disk_free_bytes",
                ""
                if self._minimum_disk_free_bytes is None
                else str(self._minimum_disk_free_bytes),
            )
            connection.executemany(
                "UPDATE capture_groups SET commit_latency_ms = ? WHERE group_index = ?",
                (
                    (latency_ms, group_index)
                    for group_index, latency_ms in enumerate(
                        self._logical_commit_latencies_ms
                    )
                ),
            )
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        self._database_status = status

    def _checkpoint_wal(self) -> None:
        result = (
            self._require_connection()
            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .fetchone()
        )
        if result is None or len(result) != 3:
            raise RuntimeError("sqlite capture WAL checkpoint returned no status")
        busy, log_frames, checkpointed_frames = (int(value) for value in result)
        if busy or log_frames != checkpointed_frames:
            raise RuntimeError(
                "sqlite capture WAL checkpoint did not complete: "
                f"busy={busy}, log_frames={log_frames}, "
                f"checkpointed_frames={checkpointed_frames}"
            )

    def _promote(self) -> None:
        started = time.monotonic()
        accepted_at = _utc_now()
        self._promotion_attempt_count += 1
        self._persist_promotion_metrics()
        try:
            artifacts = self._write_promoted_parquet()
            self._validate_promoted_parquet(artifacts)
            group_id = semantic_hash(
                [
                    PROMOTION_NAMESPACE,
                    self.state.run_id,
                    [artifact.to_mapping() for artifact in artifacts],
                ]
            )
            record = CaptureCommitRecordV2.create(
                group_id=group_id,
                group_index=0,
                cause=CaptureCommitCause.CLEAN_SHUTDOWN,
                accepted_at_utc=accepted_at,
                committed_at_utc=_utc_now(),
                artifacts=artifacts,
            )
            _write_journal_atomic(self.run_dir / COMMIT_JOURNAL_NAME, record)
            elapsed_ms = (time.monotonic() - started) * 1_000.0
            self._promotion_latencies_ms.append(elapsed_ms)
            self._promotion_input_bytes = self.database_path.stat().st_size
            self._promotion_output_bytes = sum(
                resolve_run_relative_path(
                    self.run_dir, artifact.path, key="commit artifact path"
                )
                .stat()
                .st_size
                for artifact in artifacts
            )
            self._promotion_output_files = len(artifacts)
            self._install_promoted_journal_metrics(record, elapsed_ms)
            self._write_segment_manifests()
            self._set_database_status("promoted")
            self._persist_promotion_metrics()
        except BaseException:
            self._promotion_failure_count += 1
            self._persist_promotion_metrics()
            raise

    def _write_promoted_parquet(self) -> list[CaptureCommitArtifactV1]:
        import pyarrow as pa
        import pyarrow.parquet as pq

        artifacts: list[CaptureCommitArtifactV1] = []
        connection = self._require_connection()
        for role in sorted(self.role_schema_versions, key=_role_sort_key):
            output_dir = resolve_run_relative_path(
                self.run_dir,
                self.state.expected_role_paths[role],
                key=f"expected_role_paths.{role}",
            )
            output_dir.mkdir(parents=True, exist_ok=True)
            unexpected = sorted(
                path.name
                for path in output_dir.glob("*.parquet")
                if path.name != "part-000000.parquet"
            )
            if unexpected:
                raise ValueError(
                    f"sqlite promotion found unexpected {role} parquet files: "
                    + ", ".join(unexpected)
                )
            final = output_dir / "part-000000.parquet"
            temp = output_dir / ".part-000000.parquet.tmp"
            schema = self.role_schemas.get(role)
            if schema is None:
                schema = arrow_schema(get_table_spec(self.role_schema_versions[role]))
            writer = pq.ParquetWriter(temp, schema)
            row_count = 0
            first_sequence: int | None = None
            last_sequence: int | None = None
            try:
                cursor = connection.execute(
                    """
                    SELECT payload, local_sequence FROM capture_rows
                    WHERE role = ? ORDER BY group_index, row_index
                    """,
                    (role,),
                )
                while batch := cursor.fetchmany(10_000):
                    rows = []
                    for raw_payload, sequence in batch:
                        decoded = json.loads(bytes(raw_payload))
                        if not isinstance(decoded, Mapping):
                            raise ValueError(
                                "sqlite capture row payload must be an object"
                            )
                        rows.append(dict(decoded))
                        numeric_sequence = int(sequence)
                        first_sequence = (
                            numeric_sequence
                            if first_sequence is None
                            else min(first_sequence, numeric_sequence)
                        )
                        last_sequence = (
                            numeric_sequence
                            if last_sequence is None
                            else max(last_sequence, numeric_sequence)
                        )
                    # Canonical JSON deliberately encodes finite floats and
                    # Decimals as fixed-point strings. Reuse the registry's
                    # physical conversion boundary before Arrow construction.
                    # The rows were already validated, by logical group, during
                    # the inspection immediately preceding promotion. ``coerce``
                    # is intentional here because feed_health.v1 permits an
                    # empty detail sentinel in a field whose registry dtype is
                    # otherwise JSON; the stricter conversion helper rejects
                    # that established representation.
                    try:
                        spec = get_table_spec(self.role_schema_versions[role])
                    except KeyError:
                        converted_rows = _restore_canonical_numeric_values(rows, schema)
                    else:
                        # Parse canonical numeric strings with Python's exact
                        # string-to-IEEE conversion before pandas sees them.
                        # ``pd.to_numeric`` may choose a slightly different
                        # decimal parser for long recurring values, which used
                        # to make separately persisted evidence IDs disagree.
                        restored_rows = _restore_canonical_numeric_values(rows, schema)
                        frame = coerce_frame(
                            pd.DataFrame(restored_rows, columns=spec.columns), spec
                        )
                        converted_rows = frame.to_dict(orient="records")
                    arrays = [
                        pa.array(
                            _checked_column(role, field, converted_rows),
                            type=field.type,
                            from_pandas=True,
                        )
                        for field in schema
                    ]
                    writer.write_table(pa.Table.from_arrays(arrays, schema=schema))
                    row_count += len(rows)
                if row_count == 0:
                    arrays = [pa.array([], type=field.type) for field in schema]
                    writer.write_table(pa.Table.from_arrays(arrays, schema=schema))
            finally:
                writer.close()
            with temp.open("r+b") as handle:
                os.fsync(handle.fileno())
            parquet = pq.ParquetFile(temp)
            try:
                if parquet.metadata.num_rows != row_count:
                    raise ValueError(
                        f"sqlite promotion row count mismatch for {role}: "
                        f"expected {row_count}, found {parquet.metadata.num_rows}"
                    )
                try:
                    validation_spec = get_table_spec(self.role_schema_versions[role])
                except KeyError:
                    pass
                else:
                    for batch in parquet.iter_batches(batch_size=10_000):
                        frame = batch.to_pandas()
                        report = validate_frame(frame, validation_spec, strict=True)
                        if not report.ok:
                            raise ValueError(
                                f"invalid promoted {role} parquet: "
                                + "; ".join(report.errors)
                            )
            finally:
                parquet.close()
            os.replace(temp, final)
            _fsync_directory(output_dir)
            artifacts.append(
                CaptureCommitArtifactV1(
                    role=role,
                    path=final.relative_to(self.run_dir).as_posix(),
                    sha256=file_sha256(final),
                    row_count=row_count,
                    first_local_sequence=first_sequence or 0,
                    last_local_sequence=last_sequence or 0,
                )
            )
        return artifacts

    def _validate_promoted_parquet(
        self, artifacts: list[CaptureCommitArtifactV1]
    ) -> None:
        """Apply the full recovery boundary before publishing the journal.

        Per-role validation in ``_write_promoted_parquet`` cannot detect broken
        control-to-evidence links or tape continuity.  The promoted journal is
        downstream authority once the run is finalized, so publication must be
        gated on the same cross-role validation used by recovery.
        """
        rows_by_role: dict[str, list[dict[str, Any]]] = {}
        for artifact in artifacts:
            path = resolve_run_relative_path(
                self.run_dir, artifact.path, key="commit artifact path"
            )
            frame = pd.read_parquet(path)
            rows_by_role[artifact.role] = [
                {str(key): normalize_capture_value(value) for key, value in row.items()}
                for row in frame.to_dict(orient="records")
            ]
        validate_committed_capture_group(
            run_state=self.state,
            role_schema_versions=self.role_schema_versions,
            rows_by_role=rows_by_role,
            adapter_settings_by_venue=self.adapter_settings_by_venue,
        )

    def _install_promoted_journal_metrics(
        self, record: CaptureCommitRecordV2, latency_ms: float
    ) -> None:
        self._groups_accepted = 1
        self._groups_published = 1
        self._cause_counts = Counter({CaptureCommitCause.CLEAN_SHUTDOWN.value: 1})
        self._publication_latencies_ms = [latency_ms]
        self._publication_latencies_by_cause_ms = defaultdict(
            list, {CaptureCommitCause.CLEAN_SHUTDOWN.value: [latency_ms]}
        )
        self._group_row_counts = [sum(item.row_count for item in record.artifacts)]
        self._group_file_counts = [len(record.artifacts)]
        self._group_output_bytes = [self._promotion_output_bytes]
        self._group_canonical_input_bytes = [sum(self._logical_group_input_bytes)]
        self._group_index = 1
        self._committed_roles = set(self.role_schema_versions)

    def _persist_promotion_metrics(self) -> None:
        connection = self._require_connection()
        connection.execute("BEGIN IMMEDIATE")
        try:
            values = {
                "promotion_attempt_count": str(self._promotion_attempt_count),
                "promotion_failure_count": str(self._promotion_failure_count),
                "promotion_latencies_ms": json.dumps(
                    self._promotion_latencies_ms, separators=(",", ":")
                ),
                "promotion_input_bytes": str(self._promotion_input_bytes),
                "promotion_output_bytes": str(self._promotion_output_bytes),
                "promotion_output_files": str(self._promotion_output_files),
            }
            for key, value in values.items():
                _set_metadata(connection, key, value)
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise

    def _load_persisted_metrics(self) -> None:
        connection = self._require_connection()
        metadata = _metadata(connection)
        self._database_status = metadata.get("status", "recording")
        self._wal_peak_bytes = int(metadata.get("wal_peak_bytes") or 0)
        raw_disk_free = metadata.get("minimum_disk_free_bytes") or ""
        self._minimum_disk_free_bytes = int(raw_disk_free) if raw_disk_free else None
        self._promotion_attempt_count = int(
            metadata.get("promotion_attempt_count") or 0
        )
        self._promotion_failure_count = int(
            metadata.get("promotion_failure_count") or 0
        )
        self._promotion_latencies_ms = [
            float(value)
            for value in json.loads(metadata.get("promotion_latencies_ms") or "[]")
        ]
        self._promotion_input_bytes = int(metadata.get("promotion_input_bytes") or 0)
        self._promotion_output_bytes = int(metadata.get("promotion_output_bytes") or 0)
        self._promotion_output_files = int(metadata.get("promotion_output_files") or 0)
        groups = list(self._iter_group_records())
        self._logical_groups_committed = len(groups)
        self._group_index = len(groups)
        for group in groups:
            cause = str(group["cause"])
            latency = float(group["commit_latency_ms"])
            self._logical_cause_counts[cause] += 1
            self._logical_commit_latencies_ms.append(latency)
            self._logical_latencies_by_cause_ms[cause].append(latency)
            self._logical_group_rows.append(int(group["row_count"]))
            self._logical_group_input_bytes.append(int(group["canonical_bytes"]))
        for role, count in connection.execute(
            "SELECT role, COUNT(*) FROM capture_rows GROUP BY role"
        ):
            self._role_row_counts[str(role)] = int(count)
            self._committed_roles.add(str(role))
        if (self.run_dir / COMMIT_JOURNAL_NAME).exists():
            record = _read_promoted_record(self.run_dir / COMMIT_JOURNAL_NAME)
            self._install_promoted_journal_metrics(
                record,
                self._promotion_latencies_ms[-1]
                if self._promotion_latencies_ms
                else 0.0,
            )
            self._segment_manifests_written = all(
                self._segment_manifest_path(role, relative).exists()
                for role, relative in self.state.expected_role_paths.items()
            )

    def storage_manifest(self) -> dict[str, Any]:
        database_bytes = self.database_path.stat().st_size
        unpromoted = self._database_status != "promoted"
        return {
            "configuration": self.storage_settings.to_mapping(),
            "metrics": {
                "logical_groups_committed": self._logical_groups_committed,
                "cause_counts": dict(sorted(self._logical_cause_counts.items())),
                "committed_rows": sum(self._role_row_counts.values()),
                "group_rows": sample_summary(self._logical_group_rows),
                "canonical_input_bytes": sample_summary(
                    self._logical_group_input_bytes
                ),
                "durable_files": sample_summary([0] * self._logical_groups_committed),
                "durable_bytes": sample_summary(()),
                "commit_latency_ms": sample_summary(self._logical_commit_latencies_ms),
                "commit_latency_ms_by_cause": {
                    cause: sample_summary(values)
                    for cause, values in sorted(
                        self._logical_latencies_by_cause_ms.items()
                    )
                },
                "minimum_disk_free_bytes": self._minimum_disk_free_bytes,
                "database_bytes": database_bytes,
                "wal_peak_bytes": self._wal_peak_bytes,
                "resource_metrics": {
                    "status": (
                        "available"
                        if self._resource_metrics_error_count == 0
                        else "degraded"
                    ),
                    "error_count": self._resource_metrics_error_count,
                    "last_error": self._last_resource_metrics_error,
                },
                "retained_database_bytes_after_promotion": (
                    database_bytes if self._database_status == "promoted" else 0
                ),
                "unpromoted_sealed_database_count": int(
                    unpromoted and self._database_status == "sealed"
                ),
                "unpromoted_sealed_bytes": (
                    database_bytes
                    if unpromoted and self._database_status == "sealed"
                    else 0
                ),
                "promotion": {
                    "attempt_count": self._promotion_attempt_count,
                    "failure_count": self._promotion_failure_count,
                    "latency_ms": sample_summary(self._promotion_latencies_ms),
                    "input_bytes": self._promotion_input_bytes,
                    "output_bytes": self._promotion_output_bytes,
                    "output_files": self._promotion_output_files,
                },
            },
        }

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None


def inspect_sqlite_capture(
    run_dir: str | Path,
    state: RunStateV1,
    *,
    validate_payloads: bool = True,
) -> SQLiteCaptureInspection:
    coordinator = SQLiteCaptureCoordinator.open_existing(run_dir, state)
    try:
        return coordinator.inspect(validate_payloads=validate_payloads)
    finally:
        coordinator.close()


def promote_sqlite_capture(
    run_dir: str | Path,
    state: RunStateV1,
) -> SQLiteCaptureCoordinator:
    coordinator = SQLiteCaptureCoordinator.open_existing(run_dir, state)
    inspection = coordinator.inspect(validate_payloads=True)
    if inspection.errors:
        coordinator.close()
        raise ValueError(
            "cannot promote invalid sqlite capture: " + "; ".join(inspection.errors)
        )
    if not coordinator._segment_manifests_written:
        if (coordinator.run_dir / COMMIT_JOURNAL_NAME).exists():
            # The journal loader above has already verified every artifact hash.
            # A crash after journal publication therefore resumes by completing
            # only derived manifests; rewriting the journaled Parquet would open
            # a second crash window where its hashes temporarily disagree.
            coordinator._write_segment_manifests()
            coordinator._set_database_status("promoted")
            coordinator._persist_promotion_metrics()
        else:
            coordinator._set_database_status("sealed")
            coordinator._checkpoint_wal()
            coordinator._promote()
        coordinator._segment_manifests_written = True
    return coordinator


def sqlite_storage_manifest(run_dir: str | Path, state: RunStateV1) -> dict[str, Any]:
    coordinator = SQLiteCaptureCoordinator.open_existing(run_dir, state)
    try:
        return coordinator.storage_manifest()
    finally:
        coordinator.close()


def _sqlite_group_checksum(
    *,
    group_id: str,
    group_index: int,
    cause: str,
    accepted_at_utc: str,
    committed_at_utc: str,
    row_count: int,
    canonical_bytes: int,
) -> str:
    return semantic_hash(
        [
            SQLITE_GROUP_FORMAT,
            group_id,
            group_index,
            cause,
            accepted_at_utc,
            committed_at_utc,
            row_count,
            canonical_bytes,
        ]
    )


def _metadata(connection: sqlite3.Connection) -> dict[str, str]:
    return {
        str(key): str(value)
        for key, value in connection.execute("SELECT key, value FROM metadata")
    }


def _set_metadata(connection: sqlite3.Connection, key: str, value: str) -> None:
    connection.execute(
        "INSERT INTO metadata(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def _serialize_arrow_schemas(schemas: Mapping[str, Any]) -> str:
    encoded = {
        role: base64.b64encode(schema.serialize().to_pybytes()).decode("ascii")
        for role, schema in sorted(schemas.items())
    }
    return json.dumps(encoded, separators=(",", ":"), sort_keys=True)


def _deserialize_arrow_schemas(payload: str) -> dict[str, Any]:
    import pyarrow as pa

    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError("sqlite capture Arrow schema metadata is invalid") from exc
    if not isinstance(decoded, Mapping) or any(
        not isinstance(role, str) or not isinstance(value, str)
        for role, value in decoded.items()
    ):
        raise ValueError("sqlite capture Arrow schema metadata must be an object")
    schemas: dict[str, Any] = {}
    for role, value in decoded.items():
        try:
            raw = base64.b64decode(value, validate=True)
            schemas[role] = pa.ipc.read_schema(pa.BufferReader(raw))
        except (ValueError, TypeError, pa.ArrowException) as exc:
            raise ValueError(
                f"sqlite capture Arrow schema for role {role!r} is invalid"
            ) from exc
    return schemas


def _write_journal_atomic(path: Path, record: CaptureCommitRecordV2) -> None:
    if (path.parent / COMMIT_JOURNAL_V1_NAME).exists():
        raise ValueError("sqlite promotion cannot coexist with a v1 capture journal")
    temp = path.with_name(f".{path.name}.tmp")
    with temp.open("wb") as handle:
        handle.write(canonical_json_bytes(record.to_mapping()))
        handle.write(b"\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)
    _fsync_directory(path.parent)


def _read_promoted_record(path: Path) -> CaptureCommitRecordV2:
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line]
    if len(lines) != 1:
        raise ValueError("sqlite promotion journal must contain exactly one record")
    payload = json.loads(lines[0])
    if not isinstance(payload, Mapping):
        raise ValueError("sqlite promotion journal row must be an object")
    record = CaptureCommitRecordV2.from_mapping(payload)
    for artifact in record.artifacts:
        artifact_path = resolve_run_relative_path(
            path.parent, artifact.path, key="commit artifact path"
        )
        if not artifact_path.exists() or file_sha256(artifact_path) != artifact.sha256:
            raise ValueError("sqlite promotion journal artifact is missing or corrupt")
    return record


def _restore_canonical_numeric_values(
    rows: list[dict[str, Any]], schema: Any
) -> list[dict[str, Any]]:
    """Restore only numeric types changed by canonical JSON encoding.

    Legacy collector schemas intentionally live outside the canonical registry.
    Fixed-point strings produced by ``canonical_json_bytes`` must therefore be
    converted back using their explicit Arrow schema. Other shapes remain
    untouched so Arrow still rejects incompatible producer values.
    """
    import pyarrow as pa

    converted: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for field in schema:
            value = item.get(field.name)
            if value is None or not isinstance(value, str):
                continue
            if pa.types.is_floating(field.type):
                item[field.name] = float(value)
            elif pa.types.is_integer(field.type):
                if not value or value.strip() != value:
                    raise ValueError(
                        f"invalid integer representation for {field.name}: {value!r}"
                    )
                item[field.name] = int(value)
        converted.append(item)
    return converted


__all__ = [
    "SQLITE_CAPTURE_FORMAT",
    "SQLiteCaptureCoordinator",
    "SQLiteCaptureInspection",
    "inspect_sqlite_capture",
    "promote_sqlite_capture",
    "sqlite_storage_manifest",
]
