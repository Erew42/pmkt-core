from __future__ import annotations

import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Collection, Literal, Mapping, Sequence

import pandas as pd

from pmkt.data.manifests import build_run_manifest, validate_run_manifest
from pmkt.data.registry import arrow_schema, get_table_spec
from pmkt.streaming.durability import (
    COMMIT_JOURNAL_V1_NAME,
    COMMIT_JOURNAL_V2_NAME,
    RUN_STATE_NAME,
    SCHEMA_MAP_NAME,
    capture_segment_manifest_path,
    file_sha256,
    normalize_capture_value,
    validate_committed_capture_group,
    write_json_atomic_fsync,
    write_run_state,
)
from pmkt.streaming.durability_settings import CaptureDurabilitySettings
from pmkt.streaming.recovery_contracts import (
    CAPTURE_COMMIT_JOURNAL_V1_FORMAT,
    CAPTURE_COMMIT_JOURNAL_V2_FORMAT,
    CaptureCommitCause,
    CaptureCommitRecord,
    CaptureCommitRecordV2,
    RunStateV1,
    canonical_run_relative_path,
    parse_capture_commit_record,
    resolve_run_relative_path,
)
from pmkt.streaming.profiles import (
    build_storage_profile_manifest_mapping,
    select_storage_profile,
)
from pmkt.streaming.tape import NativeBookLevel
from pmkt.streaming.storage_backends import sample_summary
from pmkt.streaming.storage_backends import (
    CaptureStorageBackend,
    CaptureStorageSettings,
)

ArtifactStatFingerprint = tuple[int, int, int, int, int]

_SEGMENT_NAME_RE = re.compile(r"^part-(\d{6,})\.parquet$")


@dataclass(frozen=True)
class RecoveryReport:
    run_dir: str
    run_id: str | None
    state_status: str | None
    valid_group_count: int
    committed_role_counts: Mapping[str, int]
    journal_errors: tuple[str, ...]
    orphan_paths: tuple[str, ...]
    journal_version: str | None
    commit_cause_counts: Mapping[str, int]
    validated_artifact_fingerprints: Mapping[
        str, ArtifactStatFingerprint
    ] = field(default_factory=dict)
    finalized_manifest_path: str | None = None
    validated_records: tuple[CaptureCommitRecord, ...] = ()

    @property
    def recoverable(self) -> bool:
        return self.valid_group_count > 0 and not self.journal_errors

    def to_mapping(self) -> dict[str, Any]:
        return {
            "run_dir": self.run_dir,
            "run_id": self.run_id,
            "state_status": self.state_status,
            "valid_group_count": self.valid_group_count,
            "committed_role_counts": dict(self.committed_role_counts),
            "validated_artifact_count": len(self.validated_artifact_fingerprints),
            "journal_errors": list(self.journal_errors),
            "orphan_paths": list(self.orphan_paths),
            "journal_version": self.journal_version,
            "commit_cause_counts": dict(self.commit_cause_counts),
            "recoverable": self.recoverable,
            "finalized_manifest_path": self.finalized_manifest_path,
        }


def recover_stream_run(
    run_dir: str | Path,
    *,
    finalize: bool = False,
    payload_validation: Literal["full", "integrity"] = "full",
    artifact_roles: Collection[str] | None = None,
) -> RecoveryReport:
    if payload_validation not in {"full", "integrity"}:
        raise ValueError("payload_validation must be either 'full' or 'integrity'")
    if finalize and payload_validation != "full":
        raise ValueError(
            "recovery finalization requires full committed-payload validation"
        )
    if artifact_roles is not None and payload_validation != "integrity":
        raise ValueError(
            "artifact_roles requires integrity-only committed-payload validation"
        )
    root = Path(run_dir).resolve()
    state = _read_state(root)
    if state.capture_storage is not None:
        storage_settings = CaptureStorageSettings.from_mapping(state.capture_storage)
        # SQLite is the crash-recovery authority until promotion and run
        # finalization complete.  Afterwards the promoted v2 journal is the
        # downstream read authority; retaining SQLite provenance in run state
        # must not route reconstruction back to the pre-promotion reader.
        if (
            storage_settings.backend is CaptureStorageBackend.SQLITE_WAL
            and state.status != "finalized"
        ):
            return _recover_sqlite_stream_run(
                root,
                state,
                finalize=finalize,
                payload_validation=payload_validation,
                artifact_roles=artifact_roles,
            )
    scoped_roles: frozenset[str] | None = None
    if artifact_roles is not None:
        scoped_roles = frozenset(
            role.strip() for role in artifact_roles if isinstance(role, str)
        )
        if not scoped_roles or len(scoped_roles) != len(artifact_roles):
            raise ValueError("artifact_roles must contain unique non-empty strings")
        unknown_roles = sorted(scoped_roles - set(state.expected_role_paths))
        if unknown_roles:
            raise ValueError(
                "artifact_roles are absent from the run state: "
                + ", ".join(unknown_roles)
            )
    validated_fingerprints: dict[str, ArtifactStatFingerprint] = {}
    records, errors = _read_valid_records(
        root,
        state,
        payload_validation=payload_validation,
        artifact_roles=scoped_roles,
        validated_artifact_fingerprints=validated_fingerprints,
    )
    _, journal_version, selection_errors = _select_journal(root)
    for error in selection_errors:
        if error not in errors:
            errors.append(error)
    journaled = {artifact.path for record in records for artifact in record.artifacts}
    validated_fingerprints = {
        path: fingerprint
        for path, fingerprint in validated_fingerprints.items()
        if path in journaled
    }
    all_parquet = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*.parquet")
        if "_orphans" not in path.parts and path.is_file()
    }
    external_files: set[str] = set()
    for role, relative in state.expected_role_paths.items():
        path = resolve_run_relative_path(
            root,
            relative,
            key=f"expected_role_paths.{role}",
        )
        if path.is_file():
            external_files.add(relative)
    orphans = tuple(sorted((all_parquet | external_files) - journaled))
    counts: Counter[str] = Counter()
    for record in records:
        for artifact in record.artifacts:
            counts[artifact.role] += artifact.row_count
    cause_counts = Counter(_record_cause(record) for record in records)
    report = RecoveryReport(
        run_dir=str(root),
        run_id=state.run_id,
        state_status=state.status,
        valid_group_count=len(records),
        committed_role_counts=dict(sorted(counts.items())),
        journal_errors=tuple(errors),
        orphan_paths=orphans,
        journal_version=journal_version,
        commit_cause_counts=dict(sorted(cause_counts.items())),
        validated_records=tuple(records),
        validated_artifact_fingerprints=validated_fingerprints,
    )
    if not finalize:
        return report
    if state.status == "finalized":
        raise ValueError("cannot recover an already finalized stream run")
    if report.journal_errors:
        raise ValueError(
            "cannot finalize recovery with invalid journal evidence: "
            + "; ".join(report.journal_errors)
        )
    manifest_path = _finalize_recovery(root, state, records, report)
    return RecoveryReport(
        **{**report.__dict__, "finalized_manifest_path": str(manifest_path)}
    )


def validate_commit_journal(
    run_dir: str | Path,
) -> tuple[CaptureCommitRecord, ...]:
    """Return records bound to the run state, or reject the journal as a unit."""
    root = Path(run_dir).resolve()
    journal_path, _, selection_errors = _select_journal(root)
    if selection_errors:
        raise ValueError("invalid commit journal: " + "; ".join(selection_errors))
    if journal_path is None:
        raise ValueError("commit journal does not exist")
    state = _read_state(root)
    records, errors = _read_valid_records(root, state)
    if errors:
        raise ValueError("invalid commit journal: " + "; ".join(errors))
    if not records:
        raise ValueError("commit journal contains no records")
    return tuple(records)


def _recover_sqlite_stream_run(
    root: Path,
    state: RunStateV1,
    *,
    finalize: bool,
    payload_validation: Literal["full", "integrity"],
    artifact_roles: Collection[str] | None,
) -> RecoveryReport:
    if artifact_roles is not None:
        raise ValueError(
            "artifact_roles is unavailable before sqlite capture promotion"
        )
    from pmkt.streaming.sqlite_durability import (
        SQLITE_CAPTURE_FORMAT,
        inspect_sqlite_capture,
        promote_sqlite_capture,
    )

    inspection = inspect_sqlite_capture(
        root,
        state,
        validate_payloads=payload_validation == "full",
    )
    orphan_paths = tuple(
        sorted(
            path.relative_to(root).as_posix()
            for path in root.rglob("*.parquet")
            if "_orphans" not in path.parts and path.is_file()
        )
    )
    report = RecoveryReport(
        run_dir=str(root),
        run_id=state.run_id,
        state_status=state.status,
        valid_group_count=inspection.group_count,
        committed_role_counts=inspection.role_row_counts,
        journal_errors=inspection.errors,
        orphan_paths=orphan_paths,
        journal_version=SQLITE_CAPTURE_FORMAT,
        commit_cause_counts=inspection.cause_counts,
    )
    if not finalize:
        return report
    if state.status == "finalized":
        raise ValueError("cannot recover an already finalized stream run")
    if report.journal_errors:
        raise ValueError(
            "cannot finalize recovery with invalid sqlite evidence: "
            + "; ".join(report.journal_errors)
        )

    coordinator = promote_sqlite_capture(root, state)
    coordinator.close()
    records, errors = _read_valid_records(root, state)
    if errors:
        raise ValueError(
            "cannot finalize sqlite promotion with invalid parquet evidence: "
            + "; ".join(errors)
        )
    promoted_report = RecoveryReport(
        run_dir=str(root),
        run_id=state.run_id,
        state_status=state.status,
        valid_group_count=len(records),
        committed_role_counts={
            role: sum(
                artifact.row_count
                for record in records
                for artifact in record.artifacts
                if artifact.role == role
            )
            for role in state.expected_role_paths
        },
        journal_errors=(),
        orphan_paths=(),
        journal_version=CAPTURE_COMMIT_JOURNAL_V2_FORMAT,
        commit_cause_counts=dict(
            sorted(Counter(_record_cause(record) for record in records).items())
        ),
        validated_records=tuple(records),
    )
    manifest_path = _finalize_recovery(root, state, records, promoted_report)
    return RecoveryReport(
        **{**report.__dict__, "finalized_manifest_path": str(manifest_path)}
    )


def resolve_commit_journal_path(run_dir: str | Path) -> Path:
    """Return the one versioned journal present in a run, or fail closed."""
    root = Path(run_dir).resolve()
    journal_path, _, selection_errors = _select_journal(root)
    if selection_errors:
        raise ValueError("invalid commit journal: " + "; ".join(selection_errors))
    if journal_path is None:
        raise ValueError("commit journal does not exist")
    return journal_path


def _read_state(root: Path) -> RunStateV1:
    path = root / RUN_STATE_NAME
    if not path.exists():
        raise ValueError(f"run state does not exist: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("run state root must be a JSON object")
    return RunStateV1.from_mapping(payload)


def _read_valid_records(
    root: Path,
    state: RunStateV1,
    *,
    payload_validation: Literal["full", "integrity"] = "full",
    artifact_roles: frozenset[str] | None = None,
    validated_artifact_fingerprints: dict[
        str, ArtifactStatFingerprint
    ] | None = None,
) -> tuple[list[CaptureCommitRecord], list[str]]:
    path, expected_format, selection_errors = _select_journal(root)
    if selection_errors:
        return [], list(selection_errors)
    if path is None or expected_format is None:
        return [], []
    if (
        expected_format == CAPTURE_COMMIT_JOURNAL_V2_FORMAT
        and state.capture_durability is None
    ):
        return [], ["capture_commit_journal.v2 requires persisted capture_durability"]
    try:
        durability_settings = (
            CaptureDurabilitySettings.from_mapping(state.capture_durability)
            if state.capture_durability is not None
            else None
        )
    except (KeyError, TypeError, ValueError) as exc:
        return [], [f"persisted capture_durability is invalid: {exc}"]
    configured_version = (
        durability_settings.journal_version if durability_settings is not None else None
    )
    if configured_version is not None and configured_version != expected_format:
        return [], [
            "journal version disagrees with persisted durability configuration: "
            f"expected {configured_version}, found {expected_format}"
        ]
    raw_lines = path.read_text(encoding="utf-8").splitlines()
    observed_formats: set[str] = set()
    for line in raw_lines:
        if not line.strip():
            continue
        try:
            raw_probe = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(raw_probe, Mapping):
            observed_formats.add(str(raw_probe.get("format") or "missing"))
    if any(format_name != expected_format for format_name in observed_formats):
        return [], [
            "mixed journal versions are not allowed: "
            f"expected {expected_format}, found {', '.join(sorted(observed_formats))}"
        ]
    records: list[CaptureCommitRecord] = []
    errors: list[str] = []
    seen_group_ids: set[str] = set()
    seen_artifact_paths: set[str] = set()
    last_segment_index: dict[str, int] = {}
    last_committed_at: datetime | None = None
    try:
        schema_versions = _read_schema_versions(root)
        expected_role_roots = {
            role: resolve_run_relative_path(
                root,
                relative,
                key=f"expected_role_paths.{role}",
            )
            for role, relative in state.expected_role_paths.items()
        }
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return [], [f"capture schema version map: {exc}"]
    tape_books: dict[tuple[str, str], dict[tuple[str, str], NativeBookLevel]] = {}
    for line_number, line in enumerate(raw_lines, 1):
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
            if not isinstance(raw, Mapping):
                raise ValueError("record must be an object")
            raw_format = str(raw.get("format") or "")
            if raw_format != expected_format:
                raise ValueError(
                    "mixed journal versions are not allowed: "
                    f"expected {expected_format}, found {raw_format or 'missing'}"
                )
            record = parse_capture_commit_record(raw)
            if record.group_id in seen_group_ids:
                raise ValueError(f"duplicate group_id: {record.group_id}")
            if isinstance(record, CaptureCommitRecordV2):
                if record.group_index != len(records):
                    raise ValueError(
                        f"group_index {record.group_index} does not match journal "
                        f"position {len(records)}"
                    )
            rows_by_role = _validate_artifacts(
                root,
                state,
                record,
                role_schema_versions=schema_versions,
                expected_role_roots=expected_role_roots,
                materialize_payloads=payload_validation == "full",
                artifact_roles=artifact_roles,
                validated_artifact_fingerprints=validated_artifact_fingerprints,
            )
            committed_at = _parse_utc(record.committed_at_utc)
            record_paths = {artifact.path for artifact in record.artifacts}
            duplicate_paths = sorted(record_paths & seen_artifact_paths)
            if duplicate_paths:
                raise ValueError(f"duplicate artifact path: {duplicate_paths[0]}")
            if last_committed_at is not None and committed_at < last_committed_at:
                raise ValueError("commit timestamps are not monotonic")
            pending_indexes: dict[str, int] = {}
            for artifact in record.artifacts:
                match = _SEGMENT_NAME_RE.fullmatch(Path(artifact.path).name)
                if match is None:
                    continue
                index = int(match.group(1))
                expected_index = len(records)
                if index != expected_index:
                    raise ValueError(
                        f"segment index {index} does not match journal group {expected_index}"
                    )
                previous = last_segment_index.get(artifact.role)
                if previous is not None and index <= previous:
                    raise ValueError(
                        f"non-monotonic segment index for {artifact.role}: {index}"
                    )
                pending_indexes[artifact.role] = index
            proposed_tape_books = tape_books
            if payload_validation == "full":
                proposed_tape_books = validate_committed_capture_group(
                    run_state=state,
                    role_schema_versions=schema_versions,
                    rows_by_role=rows_by_role,
                    adapter_settings_by_venue=state.adapter_settings_by_venue,
                    prior_tape_books=tape_books,
                )
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"journal line {line_number}: {exc}")
            continue
        records.append(record)
        seen_group_ids.add(record.group_id)
        seen_artifact_paths.update(record_paths)
        last_segment_index.update(pending_indexes)
        last_committed_at = committed_at
        tape_books = proposed_tape_books
    return records, errors


def _select_journal(
    root: Path,
) -> tuple[Path | None, str | None, list[str]]:
    candidates = (
        (root / COMMIT_JOURNAL_V1_NAME, CAPTURE_COMMIT_JOURNAL_V1_FORMAT),
        (root / COMMIT_JOURNAL_V2_NAME, CAPTURE_COMMIT_JOURNAL_V2_FORMAT),
    )
    existing = [
        (path, format_name) for path, format_name in candidates if path.exists()
    ]
    if len(existing) > 1:
        return None, None, ["mixed journal versions are not allowed in one run"]
    if not existing:
        return None, None, []
    path, format_name = existing[0]
    return path, format_name, []


def _validate_artifacts(
    root: Path,
    state: RunStateV1,
    record: CaptureCommitRecord,
    *,
    role_schema_versions: Mapping[str, str],
    expected_role_roots: Mapping[str, Path],
    materialize_payloads: bool = True,
    artifact_roles: frozenset[str] | None = None,
    validated_artifact_fingerprints: dict[
        str, ArtifactStatFingerprint
    ] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    rows_by_role: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for artifact in record.artifacts:
        relative = canonical_run_relative_path(
            artifact.path,
            key="commit artifact path",
        )
        path = root.joinpath(*Path(relative).parts)
        try:
            path.relative_to(root)
        except ValueError:
            raise ValueError(
                f"commit artifact path escapes run directory: {artifact.path}"
            ) from None
        if artifact.role not in state.expected_role_paths:
            raise ValueError(
                f"artifact role is not present in run state: {artifact.role}"
            )
        matches: list[tuple[str, Path]] = []
        for role, authority in expected_role_roots.items():
            if path == authority:
                matches.append((role, authority))
                continue
            try:
                path.relative_to(authority)
            except ValueError:
                continue
            matches.append((role, authority))
        if not matches:
            raise ValueError(
                f"artifact path is outside expected root for role {artifact.role}: "
                f"{artifact.path}"
            )
        if len(matches) != 1:
            roles = ", ".join(sorted(role for role, _ in matches))
            raise ValueError(
                f"artifact path matches ambiguous expected role roots ({roles}): "
                f"{artifact.path}"
            )
        matched_role, expected_path = matches[0]
        if matched_role != artifact.role:
            raise ValueError(
                f"artifact role {artifact.role!r} disagrees with expected path role "
                f"{matched_role!r}: {artifact.path}"
            )
        if path != expected_path:
            relative_segment = path.relative_to(expected_path)
            if (
                len(relative_segment.parts) != 1
                or _SEGMENT_NAME_RE.fullmatch(path.name) is None
            ):
                raise ValueError(
                    f"artifact segment path is not canonical for role {artifact.role}: "
                    f"{artifact.path}"
                )
        if not path.is_file():
            raise ValueError(f"artifact missing: {artifact.path}")
        if artifact_roles is not None and artifact.role not in artifact_roles:
            continue
        fingerprint_before = _artifact_stat_fingerprint(path)
        if file_sha256(path) != artifact.sha256:
            raise ValueError(f"artifact hash mismatch: {artifact.path}")
        if path.suffix.lower() == ".parquet":
            if materialize_payloads:
                frame = pd.read_parquet(path)
                row_count = len(frame)
                sequence_column = (
                    "local_sequence"
                    if "local_sequence" in frame.columns
                    else "sequence"
                    if "sequence" in frame.columns
                    else None
                )
                sequences = (
                    [int(value) for value in frame[sequence_column].dropna().tolist()]
                    if sequence_column is not None
                    else []
                )
                first_sequence = min(sequences, default=0)
                last_sequence = max(sequences, default=0)
                persisted_rows = [
                    {
                        str(key): normalize_capture_value(value)
                        for key, value in row.items()
                    }
                    for row in frame.to_dict(orient="records")
                ]
                if state.profile_version == "1":
                    _normalize_profile_v1_capture_flags(
                        persisted_rows,
                        schema_version=role_schema_versions[artifact.role],
                    )
                rows_by_role[artifact.role].extend(persisted_rows)
            else:
                import pyarrow.parquet as pq

                parquet_file = pq.ParquetFile(path)
                row_count = parquet_file.metadata.num_rows
                physical_schema = parquet_file.schema_arrow
                try:
                    expected_spec = get_table_spec(
                        role_schema_versions[artifact.role]
                    )
                except KeyError:
                    # Profile-v1 compatibility includes explicitly declared
                    # legacy schemas that predate the canonical registry.
                    expected_spec = None
                if expected_spec is not None:
                    expected_schema = arrow_schema(expected_spec)
                    if not physical_schema.equals(
                        expected_schema,
                        check_metadata=False,
                    ):
                        raise ValueError(
                            "artifact Arrow schema mismatch: "
                            f"{artifact.path}"
                        )
                columns = set(physical_schema.names)
                sequence_column = (
                    "local_sequence"
                    if "local_sequence" in columns
                    else "sequence"
                    if "sequence" in columns
                    else None
                )
                if sequence_column is None:
                    first_sequence = 0
                    last_sequence = 0
                else:
                    first_sequence, last_sequence = _parquet_sequence_bounds(
                        parquet_file,
                        sequence_column,
                    )
        elif path.suffix.lower() == ".jsonl":
            with path.open("rb") as handle:
                row_count = sum(1 for line in handle if line.strip())
            first_sequence = 0 if row_count == 0 else 1
            last_sequence = row_count
        else:
            raise ValueError(f"unsupported journaled artifact: {artifact.path}")
        if row_count != artifact.row_count:
            raise ValueError(f"artifact row count mismatch: {artifact.path}")
        if (
            first_sequence != artifact.first_local_sequence
            or last_sequence != artifact.last_local_sequence
        ):
            raise ValueError(f"artifact sequence bounds mismatch: {artifact.path}")
        fingerprint_after = _artifact_stat_fingerprint(path)
        if fingerprint_after != fingerprint_before:
            raise ValueError(
                "artifact changed during integrity validation: "
                f"{artifact.path}"
            )
        if validated_artifact_fingerprints is not None:
            validated_artifact_fingerprints[artifact.path] = fingerprint_after
    return dict(rows_by_role)


def _parquet_sequence_bounds(
    parquet_file: Any,
    sequence_column: str,
) -> tuple[int, int]:
    """Read exact sequence bounds from statistics, falling back to the column."""
    try:
        physical_index = parquet_file.schema.names.index(sequence_column)
    except ValueError:
        physical_index = -1
    minima: list[int] = []
    maxima: list[int] = []
    metadata = parquet_file.metadata
    if physical_index >= 0 and metadata.num_rows:
        for row_group_index in range(metadata.num_row_groups):
            statistics = metadata.row_group(row_group_index).column(
                physical_index
            ).statistics
            if (
                statistics is None
                or not statistics.has_min_max
                or statistics.min is None
                or statistics.max is None
            ):
                minima = []
                maxima = []
                break
            minima.append(int(statistics.min))
            maxima.append(int(statistics.max))
    if minima and maxima:
        return min(minima), max(maxima)
    if not metadata.num_rows:
        return 0, 0

    import pyarrow.compute as pc

    sequence_values = parquet_file.read(columns=[sequence_column]).column(0)
    bounds = pc.min_max(sequence_values).as_py()
    return int(bounds["min"] or 0), int(bounds["max"] or 0)

def _artifact_stat_fingerprint(path: Path) -> ArtifactStatFingerprint:
    stat = path.stat()
    return (
        int(stat.st_size),
        int(stat.st_mtime_ns),
        int(stat.st_ctime_ns),
        int(stat.st_ino),
        int(stat.st_dev),
    )


def _normalize_profile_v1_capture_flags(
    rows: list[dict[str, Any]],
    *,
    schema_version: str,
) -> None:
    """Repair the known profile-v1 Arrow string-to-character-list defect.

    The legacy writer allowed a scalar flag string to reach a ``list<string>``
    Arrow field, which persisted one character per list element. Recovery may
    join that exact historical shape at this explicit compatibility boundary.
    Other malformed values, and every profile-v2 value, remain untouched so
    strict schema validation continues to fail closed.
    """
    try:
        spec = get_table_spec(schema_version)
    except KeyError:
        return
    flag_columns = {
        field.name
        for field in spec.fields
        if field.dtype == "list[string]"
        and field.name
        in {"quality_flags", "data_quality_flags", "book_quality_flags"}
    }
    for row in rows:
        for column in flag_columns:
            value = row.get(column)
            if not isinstance(value, list) or len(value) < 2:
                continue
            if not all(isinstance(item, str) and len(item) == 1 for item in value):
                continue
            joined = "".join(value)
            tokens = [
                token.strip()
                for token in re.split(r"[;,]", joined)
                if token.strip()
            ]
            if tokens and all(
                2 <= len(token) <= 64
                and token == token.strip()
                and not any(character.isspace() for character in token)
                for token in tokens
            ):
                row[column] = tokens


def _finalize_recovery(
    root: Path,
    state: RunStateV1,
    records: list[CaptureCommitRecord],
    report: RecoveryReport,
) -> Path:
    journaled = {artifact.path for record in records for artifact in record.artifacts}
    journal_path, _, selection_errors = _select_journal(root)
    if selection_errors:
        raise ValueError("cannot finalize recovery with mixed journal versions")
    # A crashed run may have accepted no group and therefore have no journal file.
    if journal_path is not None:
        journal_name = journal_path.name
    elif state.capture_durability is not None:
        journal_version = str(state.capture_durability.get("journal_version") or "")
        journal_name = (
            COMMIT_JOURNAL_V2_NAME
            if journal_version == CAPTURE_COMMIT_JOURNAL_V2_FORMAT
            else COMMIT_JOURNAL_V1_NAME
        )
    else:
        journal_name = COMMIT_JOURNAL_V1_NAME
    orphan_dir = root / "_orphans"
    for relative in report.orphan_paths:
        source = root / relative
        target = orphan_dir / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(source, target)
    schema_versions = _read_schema_versions(root)
    by_role: dict[str, list[Any]] = defaultdict(list)
    for record in records:
        for artifact in record.artifacts:
            by_role[artifact.role].append(artifact)
    artifacts: dict[str, dict[str, Any]] = {}
    dataset_paths: dict[str, str] = {}
    row_counts: dict[str, int] = {}
    for role, relative_path in state.expected_role_paths.items():
        role_artifacts = by_role.get(role, [])
        dataset_path = resolve_run_relative_path(
            root,
            relative_path,
            key=f"expected_role_paths.{role}",
        )
        external_file = schema_versions.get(role) == "legacy.raw_jsonl.v1"
        if external_file:
            dataset_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            dataset_path.mkdir(parents=True, exist_ok=True)
        segment_manifest_path = capture_segment_manifest_path(
            root,
            relative_path,
            external_file=external_file,
        )
        completed = [
            {
                "index": index,
                "path": Path(artifact.path).name,
                "row_count": artifact.row_count,
                "sha256": artifact.sha256,
            }
            for index, artifact in enumerate(role_artifacts)
        ]
        write_json_atomic_fsync(
            segment_manifest_path,
            {
                "format": "pmkt.capture_segments.v1",
                "status": "closed" if role_artifacts else "failed",
                "row_count": sum(item["row_count"] for item in completed),
                "completed_segments": completed,
                "journal_path": journal_name,
            },
        )
        count = sum(artifact.row_count for artifact in role_artifacts)
        completion = "closed" if role_artifacts else "failed"
        artifacts[role] = {
            "path": relative_path,
            "dataset_key": _dataset_key(role, schema_versions.get(role, "unknown")),
            "schema_version": schema_versions.get(role, "unknown"),
            "row_count": count,
            "segment_manifest_path": segment_manifest_path.relative_to(root).as_posix(),
            "segment_manifest_hash": file_sha256(segment_manifest_path),
            "completion_status": completion,
        }
        dataset_paths[role] = relative_path
        row_counts[role] = count
    committed_roles = sorted(by_role)
    profile_authority = state.storage_profile
    if profile_authority is None:
        profile_authority = select_storage_profile(
            state.profile_name,
            profile_version=state.profile_version,
        ).to_manifest_mapping()
    terminal_completeness = "partial" if records else "failed"
    manifest_status = terminal_completeness
    storage_profile = build_storage_profile_manifest_mapping(
        profile_authority,
        successfully_committed_roles=committed_roles,
        terminal_completeness=terminal_completeness,
    )
    recovered_completeness = _recovered_capture_completeness(
        root,
        state,
        artifacts,
    )

    manifest = build_run_manifest(
        run_id=state.run_id,
        run_dir=root,
        started_at_utc=state.started_at_utc,
        ended_at_utc=_utc_now(),
        status=manifest_status,
        command="recover-stream-run --finalize",
        dataset_paths=dataset_paths,
        schema_versions=schema_versions,
        row_counts=row_counts,
        error_type="CaptureCrash",
        error_message="stream capture did not reach clean finalization",
        extra={
            "dataset_artifacts": artifacts,
            "storage_profile": storage_profile,
            "capture_termination": "crashed",
            "capture_adapter_settings": state.adapter_settings_by_venue or {},
            "capture_commit_journal": journal_name,
            "journaled_group_count": len(records),
            "ignored_orphan_paths": list(report.orphan_paths),
            "journaled_artifact_paths": sorted(journaled),
            "capture_durability": _recovered_durability_mapping(state, records),
            **(
                {"capture_completeness": recovered_completeness}
                if recovered_completeness is not None
                else {}
            ),
            **(
                {
                    "capture_storage": _recovered_storage_mapping(
                        root, state, records
                    )
                }
                if state.capture_storage is not None
                else {}
            ),
        },
    )
    manifest_path = root / "run_manifest.v1.json"
    validation_path = root / ".run_manifest.v1.validation.json"
    write_json_atomic_fsync(validation_path, manifest)
    validation = validate_run_manifest(validation_path)
    if not validation.ok:
        validation_path.unlink(missing_ok=True)
        raise ValueError(
            "recovery manifest validation failed: " + "; ".join(validation.all_errors)
        )
    try:
        write_json_atomic_fsync(manifest_path, manifest)
    finally:
        validation_path.unlink(missing_ok=True)
    finalized_state = RunStateV1(
        run_id=state.run_id,
        profile_name=state.profile_name,
        profile_version=state.profile_version,
        expected_role_paths=state.expected_role_paths,
        shard_plan=state.shard_plan,
        started_at_utc=state.started_at_utc,
        status="finalized",
        storage_profile=storage_profile,
        adapter_settings_by_venue=state.adapter_settings_by_venue,
        capture_durability=state.capture_durability,
        capture_storage=state.capture_storage,
    )
    write_run_state(root, finalized_state)
    return manifest_path


def _recovered_capture_completeness(
    root: Path,
    state: RunStateV1,
    artifacts: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any] | None:
    if state.profile_version != "2":
        return None
    from pmkt.streaming.instrument_evidence import (
        CAPTURE_INSTRUMENT_EVIDENCE_POLICY_VERSION,
        CAPTURE_INSTRUMENT_EVIDENCE_ROLE,
        summarize_capture_instrument_evidence,
    )

    artifact = artifacts.get(CAPTURE_INSTRUMENT_EVIDENCE_ROLE)
    if not isinstance(artifact, Mapping):
        return None
    relative = artifact.get("path")
    if not isinstance(relative, str):
        return None
    dataset_path = resolve_run_relative_path(
        root,
        relative,
        key=f"dataset_artifacts.{CAPTURE_INSTRUMENT_EVIDENCE_ROLE}.path",
    )
    rows = pd.read_parquet(dataset_path).to_dict("records")
    summary = summarize_capture_instrument_evidence(rows)
    return {
        "policy_version": CAPTURE_INSTRUMENT_EVIDENCE_POLICY_VERSION,
        "policy_status": "provisional",
        "ok": False,
        "evaluated": True,
        "acceptance_eligible": False,
        "execution_status": "failed",
        "capture_status": "partial",
        "legacy_status": "partial",
        **summary.as_manifest_mapping(),
        "evidence_artifact_role": CAPTURE_INSTRUMENT_EVIDENCE_ROLE,
        "evidence_artifact_hash": artifact.get("segment_manifest_hash"),
        "evidence_artifact_reconciled": True,
        "terminal_reason": "stream_error",
        "reasons": ["recovered_after_process_loss"],
    }


def _record_cause(record: CaptureCommitRecord) -> str:
    cause = record.cause
    return cause.value if isinstance(cause, CaptureCommitCause) else str(cause)


def _recovered_durability_mapping(
    state: RunStateV1,
    records: list[CaptureCommitRecord],
) -> dict[str, Any]:
    causes = Counter(_record_cause(record) for record in records)
    return {
        "configuration": dict(state.capture_durability or {}),
        "metrics": {
            "groups_accepted": None,
            "groups_published": len(records),
            "groups_discarded": None,
            "cause_counts": dict(sorted(causes.items())),
            "maximum_queue_depth": None,
            "queue_full_wait_count": None,
            "acceptance_to_journal_latency_ms": {
                "sample_count": 0,
                "p50": None,
                "p95": None,
                "p99": None,
                "maximum": None,
            },
            "maximum_observed_uncommitted_age_seconds": None,
            "recovered_from_journal": True,
        },
    }


def _recovered_storage_mapping(
    root: Path,
    state: RunStateV1,
    records: Sequence[CaptureCommitRecord],
) -> dict[str, Any]:
    settings = CaptureStorageSettings.from_mapping(state.capture_storage or {})
    if settings.backend is CaptureStorageBackend.SQLITE_WAL:
        from pmkt.streaming.sqlite_durability import sqlite_storage_manifest

        return sqlite_storage_manifest(root, state)
    row_counts = [sum(item.row_count for item in record.artifacts) for record in records]
    file_counts = [len(record.artifacts) for record in records]
    output_bytes = [
        sum(
            resolve_run_relative_path(
                root, artifact.path, key="commit artifact path"
            ).stat().st_size
            for artifact in record.artifacts
        )
        for record in records
    ]
    return {
        "configuration": dict(state.capture_storage or {}),
        "metrics": {
            "logical_groups_committed": len(records),
            "cause_counts": dict(
                sorted(Counter(_record_cause(record) for record in records).items())
            ),
            "committed_rows": sum(row_counts),
            "group_rows": sample_summary(row_counts),
            "canonical_input_bytes": sample_summary(()),
            "durable_files": sample_summary(file_counts),
            "durable_bytes": sample_summary(output_bytes),
            "commit_latency_ms": sample_summary(()),
            "commit_latency_ms_by_cause": {},
            "minimum_disk_free_bytes": None,
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
            "recovered_from_journal": True,
        },
    }


def _read_schema_versions(root: Path) -> dict[str, str]:
    path = root / SCHEMA_MAP_NAME
    payload = json.loads(path.read_text(encoding="utf-8"))
    roles = payload.get("roles") if isinstance(payload, Mapping) else None
    if not isinstance(roles, Mapping):
        raise ValueError("capture schema version map is invalid")
    return {str(role): str(version) for role, version in roles.items()}


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(
        value[:-1] + "+00:00" if value.endswith("Z") else value
    )


def _dataset_key(role: str, schema_version: str) -> str:
    try:
        return get_table_spec(schema_version).name
    except KeyError:
        return role


__all__ = [
    "RecoveryReport",
    "recover_stream_run",
    "resolve_commit_journal_path",
    "validate_commit_journal",
]
