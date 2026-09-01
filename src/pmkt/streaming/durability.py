from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from collections import Counter, defaultdict
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import pandas as pd

from pmkt.data.registry import arrow_schema, get_table_spec
from pmkt.data.validation import (
    validate_book_control_evidence,
    validate_book_tape_bundle,
    validate_frame,
)
from pmkt.streaming.durability_settings import (
    CaptureDurabilitySettings,
    PublicationMode,
)
from pmkt.streaming.recovery_contracts import (
    CaptureCommitArtifactV1,
    CaptureCommitCause,
    CaptureCommitRecordV2,
    RunStateV1,
    resolve_run_relative_path,
)
from pmkt.streaming.tape import (
    NativeBookLevel,
    canonical_json_bytes,
    post_book_hash,
    recompute_tape_event_id,
    recompute_tape_event_payload_hash,
    semantic_hash,
)
from pmkt.streaming.storage_backends import (
    CaptureStorageBackend,
    CaptureStorageSettings,
    sample_summary,
)

RUN_STATE_NAME = "run_state.v1.json"
COMMIT_JOURNAL_V1_NAME = "capture_commit_journal.v1.jsonl"
COMMIT_JOURNAL_V2_NAME = "capture_commit_journal.v2.jsonl"
COMMIT_JOURNAL_NAME = COMMIT_JOURNAL_V2_NAME
SCHEMA_MAP_NAME = "capture_schema_versions.v1.json"
DEFAULT_COMMIT_INTERVAL_SECONDS = 30.0

_ROLE_ORDER = (
    "tape_level",
    "tape_event",
    "tape_control",
    "topbook_main",
    "topbook_checkpoint",
    "trade",
    "lifecycle",
    "health",
    "depth_main",
    "parsed_event",
    "legacy_snapshot",
    "legacy_level",
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_capture_value(value: Any) -> Any:
    """Normalize Parquet materializations into validator-safe Python values."""
    if isinstance(value, Mapping):
        return {str(key): normalize_capture_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_capture_value(item) for item in value]
    if not isinstance(value, (str, bytes, bool, int, float)) and hasattr(
        value, "tolist"
    ):
        converted = value.tolist()
        if converted is not value:
            return normalize_capture_value(converted)
    try:
        missing = pd.isna(value)
        if type(missing).__name__ in {"bool", "bool_"} and bool(missing):
            return None
    except (TypeError, ValueError):
        pass
    if not isinstance(value, (str, bytes, bool, int, float)) and hasattr(value, "item"):
        converted = value.item()
        if converted is not value:
            return normalize_capture_value(converted)
    return value


def read_committed_capture_rows(
    run_dir: str | Path, artifacts: Sequence[CaptureCommitArtifactV1]
) -> dict[str, list[dict[str, Any]]]:
    """Read the exact persisted Parquet rows authorized by journal artifacts."""
    root = Path(run_dir).resolve()
    rows_by_role: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for artifact in artifacts:
        path = resolve_run_relative_path(
            root, artifact.path, key="commit artifact path"
        )
        if path.suffix.lower() != ".parquet":
            continue
        rows_by_role[artifact.role].extend(
            {str(key): normalize_capture_value(value) for key, value in row.items()}
            for row in pd.read_parquet(path).to_dict(orient="records")
        )
    return dict(rows_by_role)


def capture_segment_manifest_path(
    run_dir: str | Path,
    relative_path: str,
    *,
    external_file: bool,
) -> Path:
    path = resolve_run_relative_path(run_dir, relative_path, key="capture dataset path")
    if external_file:
        return path.with_name(f"{path.name}.segments.json")
    return path / "_segments.json"


def write_json_atomic_fsync(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp")
    with temp.open("wb") as handle:
        handle.write(
            json.dumps(
                payload,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        )
        handle.write(b"\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)
    _fsync_directory(path.parent)


def write_run_state(run_dir: Path, state: RunStateV1) -> Path:
    output = run_dir / RUN_STATE_NAME
    write_json_atomic_fsync(output, state.to_mapping())
    return output


class DurableCaptureCoordinator:
    """Commit bounded, cross-role Parquet segment groups behind an fsynced journal."""

    storage_backend: CaptureStorageBackend = CaptureStorageBackend.PARQUET_SEGMENTS

    def __init__(
        self,
        *,
        run_dir: str | Path,
        run_state: RunStateV1,
        role_schema_versions: Mapping[str, str],
        role_schemas: Mapping[str, Any] | None = None,
        external_file_roles: Sequence[str] = (),
        segment_row_limit: int,
        adapter_settings_by_venue: Mapping[str, Mapping[str, Any]] | None = None,
        commit_interval_seconds: float = DEFAULT_COMMIT_INTERVAL_SECONDS,
        durability_settings: CaptureDurabilitySettings | None = None,
    ) -> None:
        if segment_row_limit <= 0:
            raise ValueError("segment_row_limit must be positive")
        if commit_interval_seconds <= 0:
            raise ValueError("commit_interval_seconds must be positive")
        provided_durability = durability_settings or CaptureDurabilitySettings.resolve(
            requested_segment_rows=segment_row_limit,
            requested_segment_seconds=commit_interval_seconds,
        )
        persisted_durability = (
            CaptureDurabilitySettings.from_mapping(run_state.capture_durability)
            if run_state.capture_durability is not None
            else None
        )
        if (
            persisted_durability is not None
            and durability_settings is not None
            and persisted_durability != durability_settings
        ):
            raise ValueError(
                "durability settings must match the persisted run-state authority"
            )
        effective_durability = persisted_durability or provided_durability
        if effective_durability.publication_mode is not PublicationMode.INLINE:
            raise ValueError(
                "async publication is disabled until its canary acceptance passes"
            )
        if effective_durability.effective_segment_rows != int(segment_row_limit):
            raise ValueError(
                "segment_row_limit must match effective durability settings"
            )
        if effective_durability.effective_segment_seconds != float(
            commit_interval_seconds
        ):
            raise ValueError(
                "commit_interval_seconds must match effective durability settings"
            )
        if run_state.capture_durability is None:
            run_state = replace(
                run_state,
                capture_durability=effective_durability.to_mapping(),
            )
        storage_settings = (
            CaptureStorageSettings.from_mapping(run_state.capture_storage)
            if run_state.capture_storage is not None
            else CaptureStorageSettings.for_backend(self.storage_backend)
        )
        if storage_settings.backend is not self.storage_backend:
            raise ValueError(
                "capture storage backend does not match coordinator implementation: "
                f"expected {self.storage_backend.value}, found "
                f"{CaptureStorageBackend(storage_settings.backend).value}"
            )
        self.run_dir = Path(run_dir).resolve()
        persisted_adapter_settings = {
            str(venue): dict(settings)
            for venue, settings in (run_state.adapter_settings_by_venue or {}).items()
        }
        provided_adapter_settings = {
            str(venue): dict(settings)
            for venue, settings in (adapter_settings_by_venue or {}).items()
        }
        if run_state.adapter_settings_by_venue is not None and (
            adapter_settings_by_venue is not None
            and persisted_adapter_settings != provided_adapter_settings
        ):
            raise ValueError(
                "adapter settings must match the persisted run-state authority"
            )
        effective_adapter_settings = (
            persisted_adapter_settings
            if run_state.adapter_settings_by_venue is not None
            else provided_adapter_settings
        )
        if run_state.adapter_settings_by_venue is None:
            run_state = replace(
                run_state,
                adapter_settings_by_venue=effective_adapter_settings,
            )
        self.state = run_state
        self.role_schema_versions = dict(role_schema_versions)
        expected = set(run_state.expected_role_paths)
        if set(self.role_schema_versions) != expected:
            raise ValueError(
                "role schema versions must exactly match expected role paths"
            )
        self.adapter_settings_by_venue = effective_adapter_settings
        self.role_schemas = dict(role_schemas or {})
        self.external_file_roles = frozenset(external_file_roles)
        if not set(self.role_schemas) <= expected:
            raise ValueError("role schemas must be a subset of expected role paths")
        if not self.external_file_roles <= expected:
            raise ValueError(
                "external file roles must be a subset of expected role paths"
            )
        if set(self.role_schemas) & self.external_file_roles:
            raise ValueError("external file roles cannot have Parquet schemas")
        for role, relative_path in run_state.expected_role_paths.items():
            resolve_run_relative_path(
                self.run_dir,
                relative_path,
                key=f"expected_role_paths.{role}",
            )
        self.durability_settings = effective_durability
        self.storage_settings = storage_settings
        self.segment_row_limit = effective_durability.effective_segment_rows
        self.commit_interval_seconds = effective_durability.effective_segment_seconds
        self._buffers: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._buffered_rows = 0
        self._group_index = 0
        self._last_commit_monotonic = time.monotonic()
        self._first_pending_monotonic: float | None = None
        self._groups_published = 0
        self._groups_accepted = 0
        self._cause_counts: Counter[str] = Counter()
        self._publication_latencies_ms: list[float] = []
        self._publication_latencies_by_cause_ms: dict[str, list[float]] = (
            defaultdict(list)
        )
        self._group_row_counts: list[int] = []
        self._group_file_counts: list[int] = []
        self._group_output_bytes: list[int] = []
        self._group_canonical_input_bytes: list[int] = []
        self._minimum_disk_free_bytes: int | None = None
        self._maximum_uncommitted_age_seconds = 0.0
        self._role_row_counts: Counter[str] = Counter()
        self._committed_roles: set[str] = set()
        self._segment_manifests_written = False
        self._tape_books: dict[
            tuple[str, str], dict[tuple[str, str], NativeBookLevel]
        ] = {}
        self.run_dir.mkdir(parents=True, exist_ok=True)
        existing_journals = [
            name
            for name in (COMMIT_JOURNAL_V1_NAME, COMMIT_JOURNAL_V2_NAME)
            if (self.run_dir / name).exists()
        ]
        if existing_journals:
            raise ValueError(
                "capture run directory already contains commit journal evidence: "
                + ", ".join(existing_journals)
            )
        write_run_state(self.run_dir, run_state)
        write_json_atomic_fsync(
            self.run_dir / SCHEMA_MAP_NAME,
            {
                "format": "capture_schema_versions.v1",
                "roles": self.role_schema_versions,
            },
        )

    def add(self, role: str, row: Mapping[str, Any]) -> None:
        if role not in self.role_schema_versions:
            raise ValueError(f"role {role!r} is not enabled for this capture")
        if not self.has_pending_rows:
            self._first_pending_monotonic = time.monotonic()
        self._buffers[role].append(dict(row))
        self._buffered_rows += 1

    def add_rows_bounded(
        self,
        role: str,
        rows: Iterable[Mapping[str, Any]],
        *,
        max_rows_per_commit: int,
        cause: CaptureCommitCause | str,
    ) -> int:
        """Stage an iterable while forcing bounded durable groups.

        The final partial batch remains pending so a caller can bind it to the
        terminal control emitted immediately afterwards.  Full batches are
        already journaled and therefore do not need to coexist in memory with
        the rest of a large terminal dataset.
        """

        if max_rows_per_commit <= 0:
            raise ValueError("max_rows_per_commit must be positive")
        normalized_cause = CaptureCommitCause(cause)
        added = 0
        batch_rows = 0
        for row in rows:
            self.add(role, row)
            added += 1
            batch_rows += 1
            if batch_rows >= max_rows_per_commit:
                self.commit(cause=normalized_cause, force=True)
                batch_rows = 0
        return added

    @property
    def has_pending_rows(self) -> bool:
        return self._buffered_rows > 0

    def due_cause(self) -> CaptureCommitCause | None:
        if not self.has_pending_rows:
            return None
        if self._buffered_rows >= self.segment_row_limit:
            return CaptureCommitCause.THRESHOLD_ROWS
        if (
            time.monotonic() - self._last_commit_monotonic
            >= self.commit_interval_seconds
        ):
            return CaptureCommitCause.THRESHOLD_TIME
        return None

    def barrier_due(self) -> bool:
        return self.due_cause() is not None

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
            # The row threshold is evaluated first and is authoritative when
            # row and time thresholds become due on the same observation.
            normalized_cause = threshold_cause
        frozen = {role: tuple(rows) for role, rows in self._buffers.items() if rows}
        self._validate_group(frozen)
        accepted_at_utc, accepted_monotonic = self._accept_group(normalized_cause)
        artifacts: list[CaptureCommitArtifactV1] = []
        for role in sorted(frozen, key=_role_sort_key):
            artifacts.append(self._write_role_segment(role, frozen[role]))
        persisted = read_committed_capture_rows(self.run_dir, artifacts)
        proposed_tape_books = self._validate_group(persisted)
        record = self._publish_artifacts(
            artifacts,
            cause=normalized_cause,
            accepted_at_utc=accepted_at_utc,
            accepted_monotonic=accepted_monotonic,
            namespace="capture-segment-group.v2",
        )
        self._tape_books = proposed_tape_books
        for artifact in artifacts:
            self._role_row_counts[artifact.role] += artifact.row_count
            self._committed_roles.add(artifact.role)
        self._buffers.clear()
        self._buffered_rows = 0
        self._first_pending_monotonic = None
        return record

    def _accept_group(self, cause: CaptureCommitCause) -> tuple[str, float]:
        accepted_monotonic = time.monotonic()
        self._groups_accepted += 1
        if self._first_pending_monotonic is not None:
            self._maximum_uncommitted_age_seconds = max(
                self._maximum_uncommitted_age_seconds,
                accepted_monotonic - self._first_pending_monotonic,
            )
        return _utc_now(), accepted_monotonic

    def _publish_artifacts(
        self,
        artifacts: Sequence[CaptureCommitArtifactV1],
        *,
        cause: CaptureCommitCause,
        accepted_at_utc: str,
        accepted_monotonic: float,
        namespace: str,
        canonical_input_bytes: int | None = None,
    ) -> CaptureCommitRecordV2:
        group_id = semantic_hash(
            [
                namespace,
                self.state.run_id,
                self._group_index,
                cause.value,
                [artifact.to_mapping() for artifact in artifacts],
            ]
        )
        record = CaptureCommitRecordV2.create(
            group_id=group_id,
            group_index=self._group_index,
            cause=cause,
            accepted_at_utc=accepted_at_utc,
            committed_at_utc=_utc_now(),
            artifacts=artifacts,
        )
        self._append_journal_record(record)
        self._cause_counts[cause.value] += 1
        latency_ms = (time.monotonic() - accepted_monotonic) * 1_000.0
        self._publication_latencies_ms.append(latency_ms)
        self._publication_latencies_by_cause_ms[cause.value].append(latency_ms)
        self._group_row_counts.append(sum(item.row_count for item in artifacts))
        self._group_file_counts.append(len(artifacts))
        self._group_output_bytes.append(
            sum(
                resolve_run_relative_path(
                    self.run_dir,
                    item.path,
                    key="commit artifact path",
                ).stat().st_size
                for item in artifacts
            )
        )
        if canonical_input_bytes is not None:
            self._group_canonical_input_bytes.append(int(canonical_input_bytes))
        disk_free = shutil.disk_usage(self.run_dir).free
        self._minimum_disk_free_bytes = (
            disk_free
            if self._minimum_disk_free_bytes is None
            else min(self._minimum_disk_free_bytes, disk_free)
        )
        self._groups_published += 1
        self._group_index += 1
        self._last_commit_monotonic = time.monotonic()
        return record

    def finalize_segments(self) -> None:
        if self._segment_manifests_written:
            return
        self.commit(cause="clean_shutdown", force=True)
        self._commit_external_files()
        missing_roles = sorted(set(self.role_schema_versions) - self._committed_roles)
        if missing_roles:
            accepted_at_utc, accepted_monotonic = self._accept_group(
                CaptureCommitCause.CLEAN_SHUTDOWN
            )
            artifacts = [self._write_role_segment(role, ()) for role in missing_roles]
            self._publish_artifacts(
                artifacts,
                cause=CaptureCommitCause.CLEAN_SHUTDOWN,
                accepted_at_utc=accepted_at_utc,
                accepted_monotonic=accepted_monotonic,
                namespace="capture-empty-roles.v2",
            )
            self._committed_roles.update(missing_roles)
        self._write_segment_manifests()
        self._segment_manifests_written = True

    def mark_finalized(self) -> None:
        if not self._segment_manifests_written:
            raise RuntimeError("capture segments must be finalized before run state")
        finalized = RunStateV1(
            run_id=self.state.run_id,
            profile_name=self.state.profile_name,
            profile_version=self.state.profile_version,
            expected_role_paths=self.state.expected_role_paths,
            shard_plan=self.state.shard_plan,
            started_at_utc=self.state.started_at_utc,
            status="finalized",
            adapter_settings_by_venue=self.state.adapter_settings_by_venue,
            storage_profile=self.state.storage_profile,
            capture_durability=self.state.capture_durability,
            capture_storage=self.state.capture_storage,
        )
        write_run_state(self.run_dir, finalized)
        self.state = finalized

    def finalize(self) -> None:
        self.finalize_segments()
        self.mark_finalized()

    @property
    def row_counts(self) -> dict[str, int]:
        return {
            role: int(self._role_row_counts.get(role, 0))
            for role in sorted(self.role_schema_versions)
        }

    @property
    def committed_roles(self) -> frozenset[str]:
        return frozenset(self._committed_roles)

    @property
    def segments_finalized(self) -> bool:
        return self._segment_manifests_written

    def durability_manifest(self) -> dict[str, Any]:
        latencies = self._publication_latencies_ms
        return {
            "configuration": self.durability_settings.to_mapping(),
            "metrics": {
                "groups_accepted": self._groups_accepted,
                "groups_published": self._groups_published,
                "groups_discarded": self._groups_accepted - self._groups_published,
                "cause_counts": dict(sorted(self._cause_counts.items())),
                "maximum_queue_depth": 0,
                "queue_full_wait_count": 0,
                "acceptance_to_journal_latency_ms": {
                    "sample_count": len(latencies),
                    "p50": _percentile(latencies, 0.50),
                    "p95": _percentile(latencies, 0.95),
                    "p99": _percentile(latencies, 0.99),
                    "maximum": max(latencies, default=None),
                },
                "maximum_observed_uncommitted_age_seconds": (
                    self._maximum_uncommitted_age_seconds
                ),
            },
        }

    def storage_manifest(self) -> dict[str, Any]:
        return {
            "configuration": self.storage_settings.to_mapping(),
            "metrics": {
                "logical_groups_committed": self._groups_published,
                "cause_counts": dict(sorted(self._cause_counts.items())),
                "committed_rows": sum(self._role_row_counts.values()),
                "group_rows": sample_summary(self._group_row_counts),
                "canonical_input_bytes": sample_summary(
                    self._group_canonical_input_bytes
                ),
                "durable_files": sample_summary(self._group_file_counts),
                "durable_bytes": sample_summary(self._group_output_bytes),
                "commit_latency_ms": sample_summary(
                    self._publication_latencies_ms
                ),
                "commit_latency_ms_by_cause": {
                    cause: sample_summary(values)
                    for cause, values in sorted(
                        self._publication_latencies_by_cause_ms.items()
                    )
                },
                "minimum_disk_free_bytes": self._minimum_disk_free_bytes,
                "database_bytes": None,
                "wal_peak_bytes": None,
                "retained_database_bytes_after_promotion": 0,
                "unpromoted_sealed_database_count": 0,
                "unpromoted_sealed_bytes": 0,
                "promotion": {
                    "attempt_count": 0,
                    "failure_count": 0,
                    "latency_ms": sample_summary(()),
                    "input_bytes": 0,
                    "output_bytes": 0,
                    "output_files": 0,
                },
            },
        }

    def dataset_artifacts(self) -> dict[str, dict[str, Any]]:
        artifacts: dict[str, dict[str, Any]] = {}
        for role, relative_path in self.state.expected_role_paths.items():
            segment_manifest = self._segment_manifest_path(role, relative_path)
            schema_version = self.role_schema_versions[role]
            artifacts[role] = {
                "path": relative_path,
                "dataset_key": _dataset_key(role, schema_version),
                "schema_version": schema_version,
                "row_count": int(self._role_row_counts.get(role, 0)),
                "segment_manifest_path": segment_manifest.relative_to(
                    self.run_dir
                ).as_posix(),
                "segment_manifest_hash": file_sha256(segment_manifest),
                "completion_status": "closed"
                if role in self._committed_roles
                else "failed",
            }
        return artifacts

    def _validate_group(
        self, rows_by_role: Mapping[str, Sequence[dict[str, Any]]]
    ) -> dict[tuple[str, str], dict[tuple[str, str], NativeBookLevel]]:
        frames: dict[str, pd.DataFrame] = {}
        event_rows = rows_by_role.get("tape_event")
        level_rows = rows_by_role.get("tape_level")
        raw_controls = rows_by_role.get("tape_control")
        relationally_validated_roles: set[str] = set()
        if event_rows is not None or level_rows is not None:
            relationally_validated_roles.update(("tape_event", "tape_level"))
        for role, rows in rows_by_role.items():
            for row in rows:
                if (
                    "collector_run_id" in row
                    and str(row.get("collector_run_id") or "") != self.state.run_id
                ):
                    raise ValueError(
                        f"invalid {role} capture segment: collector_run_id must equal "
                        f"{self.state.run_id!r}"
                    )
            version = self.role_schema_versions[role]
            try:
                spec = get_table_spec(version)
            except KeyError:
                continue
            allowed_columns = set(spec.columns)
            for row in rows:
                unknown_columns = sorted(set(row) - allowed_columns)
                if unknown_columns:
                    raise ValueError(
                        f"invalid {role} capture segment: unknown fields "
                        + ", ".join(unknown_columns)
                    )
            frame = pd.DataFrame(rows, columns=spec.columns)
            frames[role] = frame
            if role in relationally_validated_roles:
                continue
            report = validate_frame(frame, spec, strict=True)
            if not report.ok:
                raise ValueError(
                    f"invalid {role} capture segment: {'; '.join(report.errors)}"
                )

        if event_rows is not None or level_rows is not None:
            event_columns = get_table_spec("book_tape_event.v1").columns
            level_columns = get_table_spec("book_tape_level.v1").columns
            bundle = validate_book_tape_bundle(
                pd.DataFrame(event_rows or [], columns=event_columns),
                pd.DataFrame(level_rows or [], columns=level_columns),
            )
            if not bundle.ok:
                raise ValueError(f"invalid tape group: {'; '.join(bundle.errors)}")

        if raw_controls is not None:
            control_columns = get_table_spec("book_tape_control.v1").columns
            evidence_report = validate_book_control_evidence(
                pd.DataFrame(raw_controls, columns=control_columns),
                tape_events=frames.get("tape_event"),
                topbook_main=frames.get("topbook_main"),
                topbook_checkpoint=frames.get("topbook_checkpoint"),
            )
            if not evidence_report.ok:
                raise ValueError(
                    "invalid tape control evidence: "
                    + "; ".join(evidence_report.errors)
                )

        proposed = {
            book_key: dict(levels) for book_key, levels in self._tape_books.items()
        }
        if event_rows is not None:
            self._validate_tape_identities(
                event_rows,
                level_rows or (),
                proposed,
            )
        return proposed

    def _validate_tape_identities(
        self,
        event_rows: Sequence[dict[str, Any]],
        level_rows: Sequence[dict[str, Any]],
        proposed: dict[tuple[str, str], dict[tuple[str, str], NativeBookLevel]],
    ) -> None:
        levels_by_event: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in level_rows:
            levels_by_event[
                (
                    str(row.get("collector_run_id") or ""),
                    str(row.get("event_id") or ""),
                )
            ].append(row)

        for event in sorted(
            event_rows,
            key=lambda row: (
                str(row.get("received_at_utc") or ""),
                int(row.get("local_sequence") or 0),
                int(row.get("subsequence") or 0),
                str(row.get("event_id") or ""),
            ),
        ):
            run_id = str(event.get("collector_run_id") or "")
            event_id = str(event.get("event_id") or "")
            event_levels = levels_by_event.get((run_id, event_id), [])
            shard_id = self._resolve_tape_shard(event)
            payload_hash = recompute_tape_event_payload_hash(event, event_levels)
            if payload_hash != str(event.get("event_payload_hash") or ""):
                raise ValueError(
                    f"invalid tape_event capture segment: event_payload_hash mismatch "
                    f"for {event_id!r}"
                )
            expected_event_id = recompute_tape_event_id(
                event,
                shard_id=shard_id,
                payload_hash=payload_hash,
            )
            if expected_event_id != event_id:
                raise ValueError(
                    f"invalid tape_event capture segment: event_id mismatch for "
                    f"{event_id!r}"
                )

            venue = str(event.get("venue") or "")
            venue_book_id = str(event.get("venue_book_id") or "")
            book_key = (venue, venue_book_id)
            if str(event.get("event_kind") or "") == "checkpoint":
                book: dict[tuple[str, str], NativeBookLevel] = {}
            else:
                existing = proposed.get(book_key)
                if existing is None:
                    raise ValueError(
                        "invalid tape_event capture segment: delta has no committed "
                        f"checkpoint for {book_key}"
                    )
                book = dict(existing)
            for row in event_levels:
                level = NativeBookLevel(
                    source_side=str(row.get("source_side") or ""),
                    price=row.get("price_key"),
                    size_after_contracts=row.get("size_after_contracts"),
                    size_delta_contracts=row.get("size_delta_contracts"),
                    level_ordinal=int(row.get("level_ordinal") or 0),
                )
                level_key = (level.source_side, level.price_key)
                if float(level.size_after_contracts) > 0:
                    book[level_key] = level
                else:
                    book.pop(level_key, None)
            adapter_settings = self._adapter_settings(venue)
            expected_post_hash = post_book_hash(
                venue=venue,
                venue_book_id=venue_book_id,
                levels=book.values(),
                adapter_settings=adapter_settings,
            )
            if expected_post_hash != str(event.get("post_book_hash") or ""):
                raise ValueError(
                    f"invalid tape_event capture segment: post_book_hash mismatch "
                    f"for {event_id!r}"
                )
            proposed[book_key] = book

    def _resolve_tape_shard(self, event: Mapping[str, Any]) -> str:
        venue_book_id = str(event.get("venue_book_id") or "")
        matches = self._tape_shard_index().get(venue_book_id, ())
        if len(matches) == 1:
            return matches[0]
        if not matches and len(self.state.shard_plan) == 1:
            return str(next(iter(self.state.shard_plan)))
        if not matches:
            raise ValueError(
                f"tape event {venue_book_id!r} is not bound to a capture shard"
            )
        raise ValueError(
            f"tape event {venue_book_id!r} is bound to multiple capture shards"
        )

    def _tape_shard_index(self) -> dict[str, tuple[str, ...]]:
        """Return the shard bindings cached for the current run-state object."""
        state = self.state
        shard_plan = state.shard_plan
        cache = getattr(self, "_tape_shard_index_cache", None)
        if (
            cache is not None
            and cache[0] is state
            and cache[1] is shard_plan
        ):
            return cache[2]

        index: defaultdict[str, list[str]] = defaultdict(list)
        for shard_id, raw_members in shard_plan.items():
            normalized_shard_id = str(shard_id)
            for venue_book_id in _shard_members(raw_members):
                index[venue_book_id].append(normalized_shard_id)
        normalized_index = {
            venue_book_id: tuple(shard_ids)
            for venue_book_id, shard_ids in index.items()
        }
        self._tape_shard_index_cache = (state, shard_plan, normalized_index)
        return normalized_index

    def _adapter_settings(self, venue: str) -> Mapping[str, Any]:
        settings = self.adapter_settings_by_venue.get(venue)
        if settings is not None:
            return settings
        if venue == "kalshi":
            raise ValueError(
                "kalshi tape validation requires explicit adapter settings"
            )
        return {}

    def _write_role_segment(
        self, role: str, rows: Sequence[dict[str, Any]]
    ) -> CaptureCommitArtifactV1:
        import pyarrow as pa
        import pyarrow.parquet as pq

        output_dir = resolve_run_relative_path(
            self.run_dir,
            self.state.expected_role_paths[role],
            key=f"expected_role_paths.{role}",
        )
        output_dir.mkdir(parents=True, exist_ok=True)
        final = output_dir / f"part-{self._group_index:06d}.parquet"
        temp = output_dir / f".part-{self._group_index:06d}.parquet.tmp"
        schema = self.role_schemas.get(role)
        if schema is None:
            schema = arrow_schema(get_table_spec(self.role_schema_versions[role]))
        arrays = [
            pa.array(_checked_column(role, field, rows), type=field.type)
            for field in schema
        ]
        pq.write_table(pa.Table.from_arrays(arrays, schema=schema), temp)
        with temp.open("r+b") as handle:
            os.fsync(handle.fileno())
        os.replace(temp, final)
        _fsync_directory(output_dir)
        sequences = [
            int(row.get("local_sequence") or row.get("sequence") or 0) for row in rows
        ]
        return CaptureCommitArtifactV1(
            role=role,
            path=final.relative_to(self.run_dir).as_posix(),
            sha256=file_sha256(final),
            row_count=len(rows),
            first_local_sequence=min(sequences, default=0),
            last_local_sequence=max(sequences, default=0),
        )

    def _commit_external_files(self) -> None:
        for role in sorted(self.external_file_roles - self._committed_roles):
            relative_text = self.state.expected_role_paths[role]
            relative_path = Path(relative_text)
            path = resolve_run_relative_path(
                self.run_dir,
                relative_text,
                key=f"expected_role_paths.{role}",
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            if not path.exists():
                path.touch()
            with path.open("r+b") as handle:
                os.fsync(handle.fileno())
            _fsync_directory(path.parent)
            row_count = 0
            with path.open("rb") as handle:
                for line in handle:
                    if line.strip():
                        row_count += 1
            artifact = CaptureCommitArtifactV1(
                role=role,
                path=relative_path.as_posix(),
                sha256=file_sha256(path),
                row_count=row_count,
                first_local_sequence=0 if row_count == 0 else 1,
                last_local_sequence=row_count,
            )
            accepted_at_utc, accepted_monotonic = self._accept_group(
                CaptureCommitCause.CLEAN_SHUTDOWN
            )
            self._publish_artifacts(
                [artifact],
                cause=CaptureCommitCause.CLEAN_SHUTDOWN,
                accepted_at_utc=accepted_at_utc,
                accepted_monotonic=accepted_monotonic,
                namespace="capture-external-file.v2",
            )
            self._role_row_counts[role] += row_count
            self._committed_roles.add(role)

    def _append_journal_record(self, record: CaptureCommitRecordV2) -> None:
        path = self.run_dir / COMMIT_JOURNAL_NAME
        with path.open("ab") as handle:
            handle.write(canonical_json_bytes(record.to_mapping()))
            handle.write(b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_directory(path.parent)

    def _write_segment_manifests(self) -> None:
        writers: dict[str, dict[str, Any]] = {}
        try:
            for role, relative_path in self.state.expected_role_paths.items():
                final = self._segment_manifest_path(role, relative_path)
                final.parent.mkdir(parents=True, exist_ok=True)
                temp = final.with_name(f".{final.name}.tmp")
                handle = temp.open("wb")
                handle.write(b'{"completed_segments":[')
                writers[role] = {
                    "final": final,
                    "temp": temp,
                    "handle": handle,
                    "first": True,
                    "index": 0,
                    "row_count": 0,
                }

            for record in self._iter_published_records():
                for artifact in record.artifacts:
                    writer = writers.get(artifact.role)
                    if writer is None:
                        raise ValueError(
                            "journal artifact role is absent from expected role paths: "
                            f"{artifact.role}"
                        )
                    handle = writer["handle"]
                    if not writer["first"]:
                        handle.write(b",")
                    item = {
                        "index": writer["index"],
                        "path": Path(artifact.path).name,
                        "row_count": artifact.row_count,
                        "sha256": artifact.sha256,
                    }
                    handle.write(
                        json.dumps(
                            item,
                            allow_nan=False,
                            ensure_ascii=False,
                            separators=(",", ":"),
                            sort_keys=True,
                        ).encode("utf-8")
                    )
                    writer["first"] = False
                    writer["index"] += 1
                    writer["row_count"] += artifact.row_count

            for writer in writers.values():
                handle = writer["handle"]
                handle.write(
                    (
                        '],"format":"pmkt.capture_segments.v1",'
                        f'"journal_path":"{COMMIT_JOURNAL_NAME}",'
                        f'"row_count":{writer["row_count"]},'
                        '"status":"closed"}\n'
                    ).encode("utf-8")
                )
                handle.flush()
                os.fsync(handle.fileno())
                handle.close()

            for writer in writers.values():
                os.replace(writer["temp"], writer["final"])
                _fsync_directory(writer["final"].parent)
        finally:
            for writer in writers.values():
                handle = writer["handle"]
                if not handle.closed:
                    handle.close()

    def _iter_published_records(self) -> Iterator[CaptureCommitRecordV2]:
        journal = self.run_dir / COMMIT_JOURNAL_NAME
        with journal.open("rb") as handle:
            for raw_line in handle:
                if not raw_line.strip():
                    continue
                payload = json.loads(raw_line)
                if not isinstance(payload, Mapping):
                    raise ValueError("capture commit journal rows must be objects")
                yield CaptureCommitRecordV2.from_mapping(payload)

    def _segment_manifest_path(self, role: str, relative_path: str) -> Path:
        return capture_segment_manifest_path(
            self.run_dir,
            relative_path,
            external_file=role in self.external_file_roles,
        )


def validate_committed_capture_group(
    *,
    run_state: RunStateV1,
    role_schema_versions: Mapping[str, str],
    rows_by_role: Mapping[str, Sequence[dict[str, Any]]],
    adapter_settings_by_venue: Mapping[str, Mapping[str, Any]] | None = None,
    prior_tape_books: Mapping[
        tuple[str, str], Mapping[tuple[str, str], NativeBookLevel]
    ]
    | None = None,
) -> dict[tuple[str, str], dict[tuple[str, str], NativeBookLevel]]:
    """Apply the live final-boundary authority to one recovered journal group."""
    validator = object.__new__(DurableCaptureCoordinator)
    validator.state = run_state
    validator.role_schema_versions = dict(role_schema_versions)
    validator.adapter_settings_by_venue = {
        str(venue): dict(settings)
        for venue, settings in (
            adapter_settings_by_venue or run_state.adapter_settings_by_venue or {}
        ).items()
    }
    validator._tape_books = {
        book_key: dict(levels) for book_key, levels in (prior_tape_books or {}).items()
    }
    return validator._validate_group(rows_by_role)


def _percentile(values: Sequence[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _shard_members(value: Any) -> set[str]:
    if isinstance(value, Mapping):
        for key in ("instrument_ids", "venue_book_ids", "instruments", "markets"):
            if key in value:
                return _shard_members(value[key])
        return set()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return {str(item) for item in value}
    return set()


def _role_sort_key(role: str) -> tuple[int, str]:
    try:
        return (_ROLE_ORDER.index(role), role)
    except ValueError:
        return (len(_ROLE_ORDER), role)


def _dataset_key(role: str, schema_version: str) -> str:
    try:
        return get_table_spec(schema_version).name
    except KeyError:
        return role


def _checked_column(
    role: str, field: Any, rows: Sequence[Mapping[str, Any]]
) -> list[Any]:
    """Project one column, rejecting values whose Python shape cannot match the
    registered Arrow type.

    ``pa.array(["a;b"], type=list_(string()))`` silently yields ``['a', ';', 'b']``
    rather than raising, so a producer that emits a delimited string for a
    list-typed field persists a character array that passes strict validation.
    This guard converts that class of defect into a fail-closed capture error for
    every list-typed field in the registry, not just ``quality_flags``.
    """
    # pyarrow is an optional extra and is imported lazily by the writer.
    import pyarrow as pa

    values = [row.get(field.name) for row in rows]
    if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
        for value in values:
            if isinstance(value, (str, bytes, bytearray)):
                raise ValueError(
                    f"invalid {role} capture segment: {field.name} expects a "
                    f"sequence for {field.type}, got "
                    f"{type(value).__name__} {value!r:.60}"
                )
    return values


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


__all__ = [
    "COMMIT_JOURNAL_NAME",
    "DEFAULT_COMMIT_INTERVAL_SECONDS",
    "capture_segment_manifest_path",
    "DurableCaptureCoordinator",
    "RUN_STATE_NAME",
    "normalize_capture_value",
    "read_committed_capture_rows",
    "validate_committed_capture_group",
    "SCHEMA_MAP_NAME",
    "file_sha256",
    "write_json_atomic_fsync",
    "write_run_state",
]
