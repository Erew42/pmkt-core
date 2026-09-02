from __future__ import annotations

import hashlib
import importlib.metadata
import json
import re
import subprocess
from collections import Counter
from dataclasses import dataclass, replace
from math import isfinite
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Iterable, Literal, Mapping, Sequence

import pandas as pd

from pmkt.data.canonical import RUN_MANIFEST_SCHEMA_VERSION, run_manifest_row
from pmkt.data.registry import get_table_spec
from pmkt.data.types import parse_int as parse_exact_int


_MANIFEST_INTEGER_TEXT_RE = re.compile(r"^[+-]?[0-9](?:_?[0-9])*$")


@dataclass(frozen=True)
class ManifestDatasetValidation:
    dataset_key: str
    path: str | None = None
    exists: bool = False
    row_count: int | None = None
    expected_row_count: int | None = None
    schema_version: str | None = None
    expected_sha256: str | None = None
    actual_sha256: str | None = None
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass(frozen=True)
class ManifestValidationReport:
    manifest_path: str
    datasets: tuple[ManifestDatasetValidation, ...] = ()
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.errors and all(dataset.ok for dataset in self.datasets)

    @property
    def all_errors(self) -> tuple[str, ...]:
        messages = list(self.errors)
        for dataset in self.datasets:
            messages.extend(
                f"{dataset.dataset_key}: {error}" for error in dataset.errors
            )
        return tuple(messages)

    @property
    def all_warnings(self) -> tuple[str, ...]:
        messages = list(self.warnings)
        for dataset in self.datasets:
            messages.extend(
                f"{dataset.dataset_key}: {warning}" for warning in dataset.warnings
            )
        return tuple(messages)


def current_git_commit(cwd: Path | None = None) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    commit = result.stdout.strip()
    return commit or None


def core_implementation_provenance() -> dict[str, str]:
    """Return the installed core version and immutable source commit when known."""
    provenance: dict[str, str] = {}
    try:
        distribution = importlib.metadata.distribution("pmkt")
    except importlib.metadata.PackageNotFoundError:
        distribution = None
    if distribution is not None:
        provenance["pmkt_core_version"] = distribution.version
        raw_direct_url = distribution.read_text("direct_url.json")
        if raw_direct_url:
            try:
                direct_url = json.loads(raw_direct_url)
            except json.JSONDecodeError:
                direct_url = {}
            vcs_info = direct_url.get("vcs_info")
            if isinstance(vcs_info, Mapping):
                commit = vcs_info.get("commit_id")
                if isinstance(commit, str) and commit.strip():
                    provenance["pmkt_core_commit"] = commit.strip()
    source_root = _source_project_root(Path(__file__).resolve(), "pmkt")
    if "pmkt_core_commit" not in provenance and source_root is not None:
        commit = current_git_commit(source_root)
        if commit:
            provenance["pmkt_core_commit"] = commit
    return provenance


def count_quality_flags(rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        value = row.get("quality_flags")
        if isinstance(value, str):
            flags = [
                item.strip()
                for item in value.replace(",", ";").split(";")
                if item.strip()
            ]
        elif isinstance(value, Iterable):
            flags = [str(item).strip() for item in value if str(item).strip()]
        else:
            flags = []
        counter.update(flags)
    return dict(sorted(counter.items()))


def build_run_manifest(
    *,
    run_id: str,
    run_dir: str | Path,
    started_at_utc: str,
    ended_at_utc: str,
    status: str,
    command: str,
    dataset_paths: Mapping[str, str],
    schema_versions: Mapping[str, str],
    row_counts: Mapping[str, int],
    quality_flag_counts: Mapping[str, int] | None = None,
    venue_counts: Mapping[str, int] | None = None,
    instrument_counts: Mapping[str, int] | None = None,
    reconnect_count: int = 0,
    sequence_gap_count: int = 0,
    resync_event_count: int = 0,
    git_commit: str | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    notes: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = run_manifest_row(
        run_id=run_id,
        started_at_utc=started_at_utc,
        ended_at_utc=ended_at_utc,
        status=status,
        command=command,
        git_commit=git_commit,
        dataset_paths=dict(dataset_paths),
        schema_versions=dict(schema_versions),
        row_counts=dict(row_counts),
        quality_flag_counts=dict(quality_flag_counts or {}),
        venue_counts=dict(venue_counts or {}),
        instrument_counts=dict(instrument_counts or {}),
        reconnect_count=int(reconnect_count),
        sequence_gap_count=int(sequence_gap_count),
        resync_event_count=int(resync_event_count),
        error_type=error_type,
        error_message=error_message,
        notes=notes,
    )
    manifest["run_dir"] = str(run_dir)
    if extra:
        manifest.update(dict(extra))
    for key, value in core_implementation_provenance().items():
        manifest.setdefault(key, value)
    if git_commit:
        manifest.setdefault("caller_git_commit", git_commit)
    return manifest


def _source_project_root(start: Path, expected_name: str) -> Path | None:
    current = start if start.is_dir() else start.parent
    for candidate in (current, *current.parents):
        pyproject = candidate / "pyproject.toml"
        if not pyproject.is_file():
            continue
        name = _pyproject_name(pyproject)
        if name != expected_name:
            continue
        source_package = candidate / "src" / expected_name.replace("-", "_")
        try:
            start.relative_to(source_package)
        except ValueError:
            continue
        return candidate
    return None


def _pyproject_name(path: Path) -> str | None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    in_project = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            in_project = line == "[project]"
            continue
        if not in_project:
            continue
        key, separator, value = line.partition("=")
        if separator and key.strip() == "name":
            return value.strip().strip("\"'")
    return None


def write_manifest(path: str | Path, manifest: Mapping[str, Any]) -> Path:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return output


def validate_run_manifest(
    path: str | Path,
    *,
    exact_artifact_validation: Literal["full", "structure"] = "full",
) -> ManifestValidationReport:
    if exact_artifact_validation not in {"full", "structure"}:
        raise ValueError(
            "exact_artifact_validation must be either 'full' or 'structure'"
        )
    manifest_path = Path(path)
    errors: list[str] = []
    warnings: list[str] = []
    if not manifest_path.exists():
        return ManifestValidationReport(
            manifest_path=str(manifest_path),
            errors=(f"manifest does not exist: {manifest_path}",),
        )
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return ManifestValidationReport(
            manifest_path=str(manifest_path),
            errors=(f"manifest JSON is invalid: {exc}",),
        )
    if not isinstance(payload, Mapping):
        return ManifestValidationReport(
            manifest_path=str(manifest_path),
            errors=("manifest root must be a JSON object",),
        )

    if payload.get("schema_version") == RUN_MANIFEST_SCHEMA_VERSION:
        from pmkt.data.validation import validate_frame

        report = validate_frame(pd.DataFrame([payload]), RUN_MANIFEST_SCHEMA_VERSION)
        errors.extend(report.errors)

    exact_authority_declared = (
        "storage_profile" in payload or "dataset_artifacts" in payload
    )
    raw_dataset_artifacts = payload.get("dataset_artifacts")
    if isinstance(raw_dataset_artifacts, Mapping):
        dataset_artifacts = dict(raw_dataset_artifacts)
    else:
        dataset_artifacts = {}
    if exact_authority_declared and not isinstance(raw_dataset_artifacts, Mapping):
        errors.append(
            "dataset_artifacts must be an object when exact profile authority is declared"
        )
    elif exact_authority_declared and not dataset_artifacts:
        errors.append(
            "dataset_artifacts must be a non-empty object when exact profile authority is declared"
        )
    dataset_paths = _manifest_mapping(payload, "dataset_paths", warnings)
    schema_versions = _manifest_mapping(payload, "schema_versions", warnings)
    row_counts = _manifest_mapping(payload, "row_counts", warnings)
    hashes = _hash_mapping(payload, warnings)
    run_dir = _manifest_run_dir(payload, manifest_path)

    datasets: list[ManifestDatasetValidation] = []
    if exact_authority_declared:
        exact_run_dir = manifest_path.parent.resolve()
        errors.extend(_declared_run_dir_errors(payload, exact_run_dir))
        exact_datasets: list[ManifestDatasetValidation] = []
        if dataset_artifacts:
            exact_datasets, artifact_errors = _validate_dataset_artifacts(
                dataset_artifacts,
                manifest_path=manifest_path,
                run_dir=exact_run_dir,
                materialize=exact_artifact_validation == "full",
            )
            datasets.extend(exact_datasets)
            errors.extend(artifact_errors)
        raw_run_id = payload.get("run_id")
        if (
            not isinstance(raw_run_id, str)
            or not raw_run_id
            or raw_run_id != raw_run_id.strip()
        ):
            errors.append(
                "run_id must be a non-empty string for exact profile authority"
            )
        else:
            journal_bound_roles: frozenset[str] = frozenset()
            if exact_artifact_validation == "full":
                journal_bound_roles, journal_errors = _journal_artifact_run_binding(
                    payload,
                    dataset_artifacts,
                    run_dir=exact_run_dir,
                    run_id=raw_run_id,
                )
                errors.extend(journal_errors)
                errors.extend(
                    _artifact_bundle_errors(
                        exact_datasets,
                        run_id=raw_run_id,
                        journal_bound_roles=journal_bound_roles,
                    )
                )
        errors.extend(_storage_profile_errors(payload, dataset_artifacts))
        errors.extend(
            _capture_instrument_evidence_errors(
                payload, dataset_artifacts, exact_datasets
            )
        )
    elif dataset_paths:
        for dataset_key, value in dataset_paths.items():
            dataset_entry = value if isinstance(value, Mapping) else {}
            dataset_path = _dataset_entry_path(value)
            expected_schema = _dataset_entry_text(dataset_entry, "schema_version")
            expected_schema = expected_schema or _lookup_manifest_value(
                schema_versions, dataset_key
            )
            inline_count = (
                _dataset_entry_value(dataset_entry, "row_count")
                if dataset_entry
                else None
            )
            raw_expected_count = inline_count
            if raw_expected_count is None:
                raw_expected_count = _lookup_manifest_raw_value(
                    row_counts, dataset_key
                )
            expected_count = _parse_int(raw_expected_count)
            expected_hash = _dataset_entry_text(dataset_entry, "sha256")
            expected_hash = expected_hash or _lookup_manifest_value(hashes, dataset_key)
            datasets.append(
                _validate_manifest_dataset(
                    dataset_key=dataset_key,
                    dataset_path=dataset_path,
                    manifest_path=manifest_path,
                    run_dir=run_dir,
                    expected_row_count=expected_count,
                    row_count_declared=raw_expected_count is not None,
                    expected_schema_version=expected_schema,
                    expected_sha256=expected_hash,
                    require_schema_and_row_count=True,
                )
            )
    elif payload.get("output_path") is not None:
        expected_hash = _lookup_manifest_value(hashes, "output")
        datasets.append(
            _validate_manifest_dataset(
                dataset_key="output",
                dataset_path=str(payload.get("output_path")),
                manifest_path=manifest_path,
                run_dir=run_dir,
                expected_row_count=_parse_int(payload.get("row_count")),
                row_count_declared=payload.get("row_count") is not None,
                expected_schema_version=_text_or_none(payload.get("schema_version"))
                if payload.get("schema_version") != RUN_MANIFEST_SCHEMA_VERSION
                else None,
                expected_sha256=expected_hash,
                require_schema_and_row_count=False,
            )
        )
    else:
        warnings.append("manifest does not declare dataset_paths or output_path")

    return ManifestValidationReport(
        manifest_path=str(manifest_path),
        datasets=tuple(datasets),
        errors=tuple(errors),
        warnings=tuple(warnings),
    )


_DATASET_ARTIFACT_FIELDS = {
    "path",
    "dataset_key",
    "schema_version",
    "row_count",
    "segment_manifest_path",
    "segment_manifest_hash",
    "completion_status",
}
_ARTIFACT_COMPLETION_STATUSES = {"open", "closed", "partial", "failed"}
_CAPTURE_SEGMENT_MANIFEST_FORMAT = "pmkt.capture_segments.v1"


def _validate_dataset_artifacts(
    artifacts: Mapping[str, Any],
    *,
    manifest_path: Path,
    run_dir: Path,
    materialize: bool = True,
) -> tuple[list[ManifestDatasetValidation], list[str]]:
    datasets: list[ManifestDatasetValidation] = []
    errors: list[str] = []
    resolved_paths: dict[str, str] = {}
    for role, raw_entry in artifacts.items():
        if not isinstance(raw_entry, Mapping):
            errors.append(f"dataset_artifacts.{role}: entry must be an object")
            continue
        missing_fields = sorted(_DATASET_ARTIFACT_FIELDS - set(raw_entry))
        extra_fields = sorted(set(raw_entry) - _DATASET_ARTIFACT_FIELDS)
        if missing_fields:
            errors.append(
                f"dataset_artifacts.{role}: missing fields {', '.join(missing_fields)}"
            )
        if extra_fields:
            errors.append(
                f"dataset_artifacts.{role}: unknown fields {', '.join(extra_fields)}"
            )
        dataset_key = _text_or_none(raw_entry.get("dataset_key"))
        schema_version = _text_or_none(raw_entry.get("schema_version"))
        raw_count = raw_entry.get("row_count")
        expected_count = raw_count if type(raw_count) is int else None
        completion_status = _text_or_none(raw_entry.get("completion_status"))
        role_errors: list[str] = []
        if not dataset_key:
            role_errors.append("dataset_key is required")
        if not schema_version:
            role_errors.append("schema_version is required")
        elif dataset_key:
            expected_dataset_key: str | None
            try:
                expected_dataset_key = get_table_spec(schema_version).name
            except KeyError:
                expected_dataset_key = (
                    str(role) if schema_version.startswith("legacy.") else None
                )
            if expected_dataset_key is not None and dataset_key != expected_dataset_key:
                role_errors.append(
                    "dataset_key does not match schema identity: "
                    f"expected {expected_dataset_key!r}, got {dataset_key!r}"
                )
        if raw_count is None or expected_count is None or expected_count < 0:
            role_errors.append("row_count must be a nonnegative integer")
        if completion_status not in _ARTIFACT_COMPLETION_STATUSES:
            role_errors.append(
                "completion_status must be one of "
                + ", ".join(sorted(_ARTIFACT_COMPLETION_STATUSES))
            )
        path_text, path_errors = _canonical_artifact_path(
            raw_entry.get("path"),
            run_dir=run_dir,
            key=f"dataset_artifacts.{role}.path",
        )
        role_errors.extend(path_errors)
        if path_text:
            resolved = (run_dir / path_text).resolve()
            normalized = str(resolved).casefold()
            previous = resolved_paths.get(normalized)
            if previous is not None:
                errors.append(
                    f"dataset_artifacts: roles {previous!r} and {role!r} use the same path"
                )
            else:
                resolved_paths[normalized] = str(role)
        if completion_status == "closed" and materialize:
            dataset = _validate_manifest_dataset(
                dataset_key=str(role),
                dataset_path=path_text,
                manifest_path=manifest_path,
                run_dir=run_dir,
                expected_row_count=expected_count,
                row_count_declared=raw_count is not None,
                expected_schema_version=schema_version,
                expected_sha256=None,
                require_schema_and_row_count=True,
            )
        elif completion_status == "closed":
            resolved_path = (run_dir / path_text).resolve() if path_text else None
            exists = resolved_path is not None and resolved_path.exists()
            structure_errors: list[str] = []
            if resolved_path is None:
                structure_errors.append("dataset path is empty")
            elif not exists:
                structure_errors.append(f"dataset does not exist: {resolved_path}")
            dataset = ManifestDatasetValidation(
                dataset_key=str(role),
                path=str(resolved_path) if resolved_path is not None else None,
                exists=exists,
                row_count=expected_count if exists else None,
                expected_row_count=expected_count,
                schema_version=schema_version,
                errors=tuple(structure_errors),
            )
        else:
            uncommitted_path = (run_dir / path_text).resolve() if path_text else None
            dataset = ManifestDatasetValidation(
                dataset_key=str(role),
                path=str(uncommitted_path) if uncommitted_path is not None else None,
                exists=False,
                expected_row_count=expected_count,
                schema_version=schema_version,
            )
        segment_errors = _segment_manifest_errors(
            raw_entry,
            run_dir=run_dir,
            dataset_path=path_text,
            expected_row_count=expected_count,
            completion_status=completion_status,
            verify_artifacts=materialize,
        )
        datasets.append(
            replace(
                dataset,
                errors=tuple([*dataset.errors, *role_errors, *segment_errors]),
            )
        )
    return datasets, errors


def _segment_manifest_errors(
    entry: Mapping[str, Any],
    *,
    run_dir: Path,
    dataset_path: str | None,
    expected_row_count: int | None,
    completion_status: str | None,
    verify_artifacts: bool = True,
) -> list[str]:
    raw_path = _text_or_none(entry.get("segment_manifest_path"))
    raw_hash = _text_or_none(entry.get("segment_manifest_hash"))
    if not raw_path and not raw_hash:
        return ["segment manifest path and hash are required for exact artifacts"]
    if not raw_path or not raw_hash:
        return ["segment manifest path and hash must be declared together"]
    if len(raw_hash) != 64 or any(
        character not in "0123456789abcdef" for character in raw_hash
    ):
        return ["segment_manifest_hash must be lowercase sha256"]
    canonical_path, path_errors = _canonical_artifact_path(
        raw_path,
        run_dir=run_dir,
        key="segment_manifest_path",
    )
    if path_errors:
        return path_errors
    assert canonical_path is not None
    resolved = (run_dir / canonical_path).resolve()
    if not resolved.exists() or not resolved.is_file():
        return [f"segment manifest does not exist: {resolved}"]
    actual = _sha256_file(resolved)
    errors: list[str] = []
    if actual != raw_hash:
        errors.append(
            f"segment manifest sha256 mismatch: expected {raw_hash}, got {actual}"
        )
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [*errors, f"segment manifest is not valid JSON: {exc}"]
    if not isinstance(payload, Mapping):
        return [*errors, "segment manifest root must be an object"]
    if payload.get("format") != _CAPTURE_SEGMENT_MANIFEST_FORMAT:
        errors.append(
            f"segment manifest format must be {_CAPTURE_SEGMENT_MANIFEST_FORMAT!r}"
        )
    if (
        completion_status in _ARTIFACT_COMPLETION_STATUSES
        and payload.get("status") != completion_status
    ):
        errors.append(f"segment manifest status must be {completion_status!r}")

    raw_manifest_count = payload.get("row_count")
    manifest_count = raw_manifest_count if type(raw_manifest_count) is int else None
    if manifest_count is None or manifest_count < 0:
        errors.append("segment manifest row_count must be a nonnegative integer")
    elif expected_row_count is not None and manifest_count != expected_row_count:
        errors.append(
            "segment manifest row_count mismatch: "
            f"expected {expected_row_count}, got {manifest_count}"
        )

    raw_segments = payload.get("completed_segments")
    if not isinstance(raw_segments, list):
        return [*errors, "segment manifest completed_segments must be a list"]
    if completion_status == "closed" and not raw_segments:
        errors.append(
            "closed segment manifest completed_segments must be a non-empty list"
        )
    artifact_path = (
        (run_dir / dataset_path).resolve() if dataset_path is not None else None
    )
    if artifact_path is None:
        return [*errors, "segment manifest cannot bind an empty dataset path"]
    segment_root = artifact_path if artifact_path.is_dir() else artifact_path.parent
    declared_paths: set[Path] = set()
    segment_row_count = 0
    segment_counts_valid = True
    expected_fields = {"index", "path", "row_count", "sha256"}
    for ordinal, raw_segment in enumerate(raw_segments):
        label = f"completed_segments[{ordinal}]"
        if not isinstance(raw_segment, Mapping):
            errors.append(f"{label} must be an object")
            segment_counts_valid = False
            continue
        missing = sorted(expected_fields - set(raw_segment))
        extra = sorted(set(raw_segment) - expected_fields)
        if missing:
            errors.append(f"{label} is missing fields {', '.join(missing)}")
        if extra:
            errors.append(f"{label} has unknown fields {', '.join(extra)}")
        raw_index = raw_segment.get("index")
        if type(raw_index) is not int or raw_index != ordinal:
            errors.append(f"{label}.index must equal {ordinal}")
        raw_segment_count = raw_segment.get("row_count")
        if type(raw_segment_count) is not int or raw_segment_count < 0:
            errors.append(f"{label}.row_count must be a nonnegative integer")
            segment_counts_valid = False
        else:
            segment_row_count += raw_segment_count
        segment_hash = _text_or_none(raw_segment.get("sha256"))
        if (
            segment_hash is None
            or len(segment_hash) != 64
            or any(character not in "0123456789abcdef" for character in segment_hash)
        ):
            errors.append(f"{label}.sha256 must be lowercase sha256")
            segment_hash = None
        canonical_segment, path_errors = _canonical_artifact_path(
            raw_segment.get("path"),
            run_dir=segment_root,
            key=f"{label}.path",
        )
        errors.extend(path_errors)
        if canonical_segment is None:
            continue
        if len(PurePosixPath(canonical_segment).parts) != 1:
            errors.append(f"{label}.path must name one artifact file")
            continue
        segment_path = (segment_root / canonical_segment).resolve()
        if artifact_path.is_file() and segment_path != artifact_path:
            errors.append(f"{label}.path does not match the file artifact")
            continue
        if segment_path in declared_paths:
            errors.append(f"{label}.path duplicates another completed segment")
            continue
        declared_paths.add(segment_path)
        if not segment_path.exists() or not segment_path.is_file():
            errors.append(f"{label} does not exist: {segment_path}")
            continue
        if verify_artifacts and segment_hash is not None:
            actual_segment_hash = _sha256_file(segment_path)
            if actual_segment_hash != segment_hash:
                errors.append(
                    f"{label} sha256 mismatch: expected {segment_hash}, "
                    f"got {actual_segment_hash}"
                )
    if (
        segment_counts_valid
        and manifest_count is not None
        and segment_row_count != manifest_count
    ):
        errors.append(
            "completed segment row counts do not match segment manifest row_count: "
            f"expected {manifest_count}, got {segment_row_count}"
        )

    if verify_artifacts and artifact_path.exists():
        if artifact_path.is_file():
            actual_paths = {artifact_path}
        elif artifact_path.is_dir():
            actual_paths = {
                child.resolve()
                for child in artifact_path.rglob("*")
                if child.is_file() and child.resolve() != resolved
            }
        else:
            actual_paths = set()
        missing_paths = sorted(declared_paths - actual_paths)
        extra_paths = sorted(actual_paths - declared_paths)
        if missing_paths:
            errors.append(
                "segment manifest declares files outside the artifact: "
                + ", ".join(str(path) for path in missing_paths)
            )
        if extra_paths:
            errors.append(
                "artifact contains undeclared files: "
                + ", ".join(str(path) for path in extra_paths)
            )
    return errors


def _storage_profile_errors(
    payload: Mapping[str, Any], artifacts: Mapping[str, Any]
) -> list[str]:
    from pmkt.streaming.profiles import (
        PROFILE_DEFINITIONS_BY_VERSION,
        DatasetRole,
        StorageProfileOverrides,
        get_storage_profile_definition,
        select_storage_profile,
    )

    profile = payload.get("storage_profile")
    if not isinstance(profile, Mapping):
        return ["storage_profile must be an object when dataset_artifacts is present"]
    errors: list[str] = []
    role_sets: dict[str, set[str]] = {}
    for key in (
        "required_roles",
        "enabled_roles",
        "disabled_roles",
        "successfully_committed_roles",
    ):
        raw = profile.get(key)
        if not isinstance(raw, list) or not all(
            isinstance(item, str) and item.strip() for item in raw
        ):
            errors.append(
                f"storage_profile.{key} must be an array of non-empty role names"
            )
            continue
        if len(raw) != len(set(raw)):
            errors.append(f"storage_profile.{key} contains duplicate roles")
        role_sets[key] = set(raw)

    name = _text_or_none(profile.get("name"))
    profile_version = _text_or_none(profile.get("profile_version"))
    raw_acknowledgement = profile.get("experimental_profile_acknowledged")
    experimental_profile_acknowledged = False
    if type(raw_acknowledgement) is not bool:
        errors.append(
            "storage_profile.experimental_profile_acknowledged must be a JSON boolean"
        )
    else:
        experimental_profile_acknowledged = raw_acknowledgement
    definition = None
    if name is None:
        errors.append("storage_profile.name must be a non-empty string")
    if profile_version is None:
        errors.append("storage_profile.profile_version must be a non-empty string")
    if name is not None and profile_version is not None:
        try:
            definition = get_storage_profile_definition(name, profile_version)
        except ValueError as exc:
            errors.append(f"storage_profile.profile_version: {exc}")
            registered = [
                candidate
                for (candidate_name, _), candidate in (
                    PROFILE_DEFINITIONS_BY_VERSION.items()
                )
                if candidate_name == name
            ]
            for field in (
                "change_trigger_version",
                "tape_encoding_version",
                "health_fingerprint_version",
                "replay_evidence_version",
            ):
                allowed = {getattr(candidate, field) for candidate in registered}
                if profile.get(field) not in allowed:
                    errors.append(
                        f"storage_profile.{field} is not registered for {name!r}"
                    )
            raw_excluded_flags = profile.get("excluded_topbook_quality_flags")
            allowed_excluded_flags = {
                tuple(sorted(candidate.excluded_topbook_quality_flags))
                for candidate in registered
            }
            if not isinstance(raw_excluded_flags, list) or (
                tuple(raw_excluded_flags) not in allowed_excluded_flags
            ):
                errors.append(
                    f"storage_profile.excluded_topbook_quality_flags is not registered for {name!r}"
                )

    default_overrides = StorageProfileOverrides()
    override_fields = set(default_overrides.to_mapping())
    raw_overrides = profile.get("effective_overrides")
    overrides = default_overrides
    if not isinstance(raw_overrides, Mapping):
        errors.append("storage_profile.effective_overrides must be an object")
    else:
        missing = sorted(override_fields - set(raw_overrides))
        extra = sorted(set(raw_overrides) - override_fields)
        if missing:
            errors.append(
                "storage_profile.effective_overrides missing fields "
                + ", ".join(missing)
            )
        if extra:
            errors.append(
                "storage_profile.effective_overrides has unknown fields "
                + ", ".join(extra)
            )
        if any(type(raw_overrides.get(field)) is not bool for field in override_fields):
            errors.append(
                "storage_profile.effective_overrides fields must be JSON booleans"
            )
        elif not missing and not extra:
            overrides = StorageProfileOverrides(
                **{field: raw_overrides[field] for field in override_fields}
            )

    selection = None
    if definition is not None:
        try:
            selection = select_storage_profile(
                definition.name,
                profile_version=definition.profile_version,
                overrides=overrides,
                experimental_profile_acknowledged=(experimental_profile_acknowledged),
                feed_health_interval_seconds=profile.get(
                    "feed_health_interval_seconds"
                ),
                topbook_checkpoint_interval_seconds=profile.get(
                    "topbook_checkpoint_interval_seconds"
                ),
                book_checkpoint_interval_seconds=profile.get(
                    "book_checkpoint_interval_seconds"
                ),
            )
        except (TypeError, ValueError) as exc:
            errors.append(f"storage_profile intervals or overrides are invalid: {exc}")

    required = role_sets.get("required_roles", set())
    enabled = role_sets.get("enabled_roles", set())
    disabled = role_sets.get("disabled_roles", set())
    committed = role_sets.get("successfully_committed_roles", set())
    if selection is not None:
        expected_mapping = selection.to_manifest_mapping()
        expected_sets = {
            "required_roles": {role.value for role in selection.required_roles},
            "enabled_roles": {role.value for role in selection.enabled_roles},
            "disabled_roles": {role.value for role in selection.disabled_roles},
        }
        for key, expected in expected_sets.items():
            if role_sets.get(key) != expected:
                errors.append(
                    f"storage_profile.{key} does not match named profile authority"
                )
        for key, scalar_expected in expected_mapping.items():
            if key in expected_sets:
                continue
            if profile.get(key) != scalar_expected:
                errors.append(
                    f"storage_profile.{key} does not match named profile authority"
                )

    if not required <= enabled:
        errors.append(
            "storage_profile required_roles must be a subset of enabled_roles"
        )
    if enabled & disabled:
        errors.append(
            "storage_profile enabled_roles and disabled_roles must be disjoint"
        )
    known_role_names = (
        {
            role.value
            for role in selection.enabled_roles | selection.disabled_roles
        }
        if selection is not None
        else {role.value for role in DatasetRole}
    )
    if enabled | disabled != known_role_names:
        errors.append(
            "storage_profile enabled_roles and disabled_roles must partition known roles"
        )
    artifact_roles = set(str(role) for role in artifacts)
    if artifact_roles != enabled:
        errors.append(
            "dataset_artifacts keys must exactly equal storage_profile enabled_roles"
        )
    if not committed <= enabled:
        errors.append(
            "storage_profile committed roles must be a subset of enabled_roles"
        )
    closed_roles = {
        str(role)
        for role, entry in artifacts.items()
        if isinstance(entry, Mapping) and entry.get("completion_status") == "closed"
    }
    if committed != closed_roles:
        errors.append(
            "storage_profile successfully_committed_roles must exactly equal closed artifacts"
        )
    verdict = _text_or_none(profile.get("terminal_completeness"))
    if verdict not in {"complete", "partial", "failed"}:
        errors.append(
            "storage_profile.terminal_completeness must be complete, partial, or failed"
        )
    if verdict == "complete" and (required - committed):
        errors.append("storage_profile claims complete with uncommitted required roles")
    if verdict == "partial" and not committed:
        errors.append("storage_profile claims partial without any committed roles")
    if any(
        isinstance(entry, Mapping) and entry.get("completion_status") == "open"
        for entry in artifacts.values()
    ):
        errors.append("terminal manifest cannot contain open artifacts")
    manifest_status = _text_or_none(payload.get("status"))
    expected_status_by_verdict: dict[str, str] = {
        "complete": "success",
        "partial": "partial",
        "failed": "failed",
    }
    expected_status = expected_status_by_verdict.get(verdict or "")
    if expected_status is not None and manifest_status != expected_status:
        errors.append(
            "manifest status does not match storage_profile terminal_completeness"
        )
    if selection is not None:
        for role_name in sorted(artifact_roles & enabled):
            try:
                role = DatasetRole(role_name)
            except ValueError:
                continue
            entry = artifacts.get(role_name)
            actual_version = (
                _text_or_none(entry.get("schema_version"))
                if isinstance(entry, Mapping)
                else None
            )
            allowed_versions = selection.definition.role_schema_versions.get(role)
            if allowed_versions is None or actual_version not in allowed_versions:
                errors.append(
                    f"dataset_artifacts.{role_name}.schema_version is not allowed "
                    "by the named profile authority"
                )
    return errors


def _capture_instrument_evidence_errors(
    payload: Mapping[str, Any],
    artifacts: Mapping[str, Any],
    datasets: Iterable[ManifestDatasetValidation],
) -> list[str]:
    profile = payload.get("storage_profile")
    if not isinstance(profile, Mapping) or profile.get("profile_version") != "2":
        return []
    from pmkt.streaming.instrument_evidence import (
        CAPTURE_INSTRUMENT_EVIDENCE_ROLE,
        evidence_manifest_reconciliation_errors,
    )

    errors: list[str] = []
    completeness = payload.get("capture_completeness")
    if not isinstance(completeness, Mapping):
        return ["capture_completeness must be an object for storage profile v2"]
    artifact = artifacts.get(CAPTURE_INSTRUMENT_EVIDENCE_ROLE)
    if not isinstance(artifact, Mapping):
        return ["storage profile v2 requires dataset_artifacts.instrument_evidence"]
    if completeness.get("evidence_artifact_role") != CAPTURE_INSTRUMENT_EVIDENCE_ROLE:
        errors.append(
            "capture_completeness.evidence_artifact_role must equal "
            f"{CAPTURE_INSTRUMENT_EVIDENCE_ROLE!r}"
        )
    if completeness.get("evidence_artifact_hash") != artifact.get(
        "segment_manifest_hash"
    ):
        errors.append(
            "capture_completeness.evidence_artifact_hash must equal the "
            "instrument-evidence segment manifest hash"
        )
    if completeness.get("evidence_artifact_reconciled") is not True:
        errors.append(
            "capture_completeness.evidence_artifact_reconciled must be true "
            "for a finalized profile-v2 manifest"
        )
    policy_status = completeness.get("policy_status")
    if policy_status not in {"provisional", "calibrated"}:
        errors.append(
            "capture_completeness.policy_status must be provisional or calibrated"
        )
    if (
        completeness.get("acceptance_eligible") is True
        and policy_status != "calibrated"
    ):
        errors.append("provisional completeness policy cannot be acceptance eligible")

    evidence_dataset = next(
        (
            dataset
            for dataset in datasets
            if dataset.dataset_key == CAPTURE_INSTRUMENT_EVIDENCE_ROLE
        ),
        None,
    )
    if (
        evidence_dataset is None
        or not evidence_dataset.exists
        or evidence_dataset.path is None
    ):
        errors.append("instrument-evidence dataset is not readable")
        return errors
    try:
        frame = _read_dataset_frame(Path(evidence_dataset.path))
    except (OSError, ValueError) as exc:
        errors.append(f"instrument-evidence dataset cannot be read: {exc}")
        return errors
    errors.extend(
        evidence_manifest_reconciliation_errors(frame.to_dict("records"), completeness)
    )
    return errors


def _validate_manifest_dataset(
    *,
    dataset_key: str,
    dataset_path: str | None,
    manifest_path: Path,
    run_dir: Path,
    expected_row_count: int | None,
    row_count_declared: bool,
    expected_schema_version: str | None,
    expected_sha256: str | None,
    require_schema_and_row_count: bool,
) -> ManifestDatasetValidation:
    errors: list[str] = []
    warnings: list[str] = []
    if require_schema_and_row_count:
        if not expected_schema_version:
            errors.append("schema version is required for dataset_paths entries")
        if not row_count_declared:
            errors.append("row count is required for dataset_paths entries")
    if row_count_declared and expected_row_count is None:
        errors.append("row count is not a valid nonnegative integer")
    if not dataset_path:
        return ManifestDatasetValidation(
            dataset_key=dataset_key,
            path=None,
            expected_row_count=expected_row_count,
            schema_version=expected_schema_version,
            expected_sha256=expected_sha256,
            errors=tuple([*errors, "dataset path is empty"]),
        )

    resolved_path = _resolve_dataset_path(dataset_path, manifest_path, run_dir)
    if not resolved_path.exists():
        return ManifestDatasetValidation(
            dataset_key=dataset_key,
            path=str(resolved_path),
            expected_row_count=expected_row_count,
            schema_version=expected_schema_version,
            expected_sha256=expected_sha256,
            errors=tuple([*errors, f"dataset does not exist: {resolved_path}"]),
        )

    actual_hash = None
    if expected_sha256:
        actual_hash = _sha256_file(resolved_path)
        if actual_hash.lower() != expected_sha256.lower():
            errors.append(
                f"sha256 mismatch: expected {expected_sha256}, got {actual_hash}"
            )

    df: pd.DataFrame | None = None
    row_count: int | None = None
    try:
        df = _read_dataset_frame(resolved_path)
        row_count = int(len(df))
    except ValueError as exc:
        errors.append(str(exc))

    if (
        expected_row_count is not None
        and row_count is not None
        and row_count != expected_row_count
    ):
        errors.append(
            f"row count mismatch: expected {expected_row_count}, got {row_count}"
        )

    if expected_schema_version and df is not None:
        from pmkt.data.validation import validate_frame

        try:
            report = validate_frame(df, expected_schema_version, strict=True)
        except KeyError as exc:
            if expected_schema_version.startswith("legacy."):
                warnings.append(
                    f"legacy physical schema {expected_schema_version!r} is not registry-validated"
                )
            else:
                errors.append(
                    f"unknown schema version {expected_schema_version!r}: {exc}"
                )
        else:
            if not report.ok:
                errors.extend(
                    f"schema {expected_schema_version}: {error}"
                    for error in report.errors
                )
    elif df is not None and "schema_version" not in df.columns:
        warnings.append("schema version not declared")

    return ManifestDatasetValidation(
        dataset_key=dataset_key,
        path=str(resolved_path),
        exists=True,
        row_count=row_count,
        expected_row_count=expected_row_count,
        schema_version=expected_schema_version,
        expected_sha256=expected_sha256,
        actual_sha256=actual_hash,
        errors=tuple(errors),
        warnings=tuple(warnings),
    )


def _manifest_mapping(
    payload: Mapping[str, Any],
    field: str,
    warnings: list[str],
) -> dict[str, Any]:
    value = payload.get(field)
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            warnings.append(f"{field} is a string but not valid JSON")
            return {}
        if isinstance(decoded, Mapping):
            return {str(key): item for key, item in decoded.items()}
    warnings.append(f"{field} must be an object")
    return {}


def _hash_mapping(payload: Mapping[str, Any], warnings: list[str]) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for field in ("dataset_hashes", "dataset_sha256", "sha256_hashes", "sha256"):
        value = payload.get(field)
        if value is None:
            continue
        if isinstance(value, str) and field == "sha256":
            hashes["output"] = value
            continue
        mapping = _manifest_mapping(payload, field, warnings)
        hashes.update(
            {key: str(item) for key, item in mapping.items() if item is not None}
        )
    return hashes


def _declared_run_dir_errors(
    payload: Mapping[str, Any], authoritative_run_dir: Path
) -> list[str]:
    raw = _text_or_none(payload.get("run_dir"))
    if raw is None:
        return ["run_dir is required when dataset_artifacts is present"]
    declared = Path(raw)
    if not declared.is_absolute():
        declared = authoritative_run_dir / declared
    if declared.resolve() != authoritative_run_dir:
        return [
            "run_dir must resolve to the directory containing the authoritative manifest"
        ]
    return []


def _canonical_artifact_path(
    value: Any,
    *,
    run_dir: Path,
    key: str,
) -> tuple[str | None, list[str]]:
    text = _text_or_none(value)
    if text is None:
        return None, []
    normalized = text.replace("\\", "/")
    posix = PurePosixPath(normalized)
    windows = PureWindowsPath(text)
    if (
        posix.is_absolute()
        or windows.is_absolute()
        or bool(windows.drive)
        or bool(windows.root)
        or any(part in {".", ".."} for part in posix.parts)
        or normalized != posix.as_posix()
    ):
        return None, [f"{key} must be a canonical path relative to the run directory"]
    resolved = (run_dir / normalized).resolve()
    try:
        resolved.relative_to(run_dir)
    except ValueError:
        return None, [f"{key} escapes the run directory"]
    return normalized, []


def _journal_artifact_run_binding(
    payload: Mapping[str, Any],
    artifacts: Mapping[str, Any],
    *,
    run_dir: Path,
    run_id: str,
    validated_state: Any | None = None,
    validated_records: Sequence[Any] | None = None,
) -> tuple[frozenset[str], list[str]]:
    raw_journal = payload.get("capture_commit_journal")
    if raw_journal is None:
        return frozenset(), []

    from pmkt.streaming.durability import (
        COMMIT_JOURNAL_V1_NAME,
        COMMIT_JOURNAL_V2_NAME,
        RUN_STATE_NAME,
    )
    from pmkt.streaming.recovery import validate_commit_journal
    from pmkt.streaming.recovery_contracts import (
        CaptureCommitCause,
        CaptureCommitRecordV2,
        RunStateV1,
    )

    errors: list[str] = []
    allowed_journals = (COMMIT_JOURNAL_V1_NAME, COMMIT_JOURNAL_V2_NAME)
    if type(raw_journal) is not str or raw_journal not in allowed_journals:
        return (
            frozenset(),
            [
                "capture_commit_journal must equal one of "
                + ", ".join(repr(name) for name in allowed_journals)
            ],
        )
    closed_roles = {
        str(role)
        for role, entry in artifacts.items()
        if isinstance(entry, Mapping) and entry.get("completion_status") == "closed"
    }
    if (validated_state is None) != (validated_records is None):
        return (
            frozenset(),
            ["validated journal state and records must be supplied together"],
        )
    try:
        if validated_state is None:
            state_payload = json.loads(
                (run_dir / RUN_STATE_NAME).read_text(encoding="utf-8")
            )
            if not isinstance(state_payload, Mapping):
                raise ValueError("run state root must be a JSON object")
            state = RunStateV1.from_mapping(state_payload)
            journal_path = run_dir / raw_journal
            records = (
                validate_commit_journal(run_dir)
                if closed_roles or journal_path.exists()
                else []
            )
        else:
            if not isinstance(validated_state, RunStateV1):
                raise ValueError("validated journal state has the wrong type")
            state = validated_state
            records = tuple(validated_records or ())
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return frozenset(), [f"capture journal run binding is invalid: {exc}"]

    if state.run_id != run_id:
        errors.append("capture journal run_id must exactly equal manifest run_id")
    profile = payload.get("storage_profile")
    if isinstance(profile, Mapping):
        state_profile = state.storage_profile
        if state_profile is None:
            from pmkt.streaming.profiles import select_storage_profile

            try:
                state_profile = select_storage_profile(
                    state.profile_name,
                    profile_version=state.profile_version,
                ).to_manifest_mapping()
            except ValueError as exc:
                errors.append(f"capture journal storage_profile is invalid: {exc}")
                state_profile = {}
        capture_profile = dict(state_profile)
        manifest_profile = dict(profile)
        for terminal_field in (
            "successfully_committed_roles",
            "terminal_completeness",
        ):
            capture_profile.pop(terminal_field, None)
            manifest_profile.pop(terminal_field, None)
        if capture_profile != manifest_profile:
            errors.append(
                "capture journal storage_profile must exactly match manifest "
                "capture-time profile settings"
            )

    raw_durability = payload.get("capture_durability")
    if state.capture_durability is not None:
        manifest_configuration = (
            raw_durability.get("configuration")
            if isinstance(raw_durability, Mapping)
            else None
        )
        if not isinstance(manifest_configuration, Mapping) or dict(
            manifest_configuration
        ) != dict(state.capture_durability):
            errors.append(
                "capture_durability configuration must exactly match run state"
            )

    raw_storage = payload.get("capture_storage")
    if state.capture_storage is not None:
        storage_configuration = (
            raw_storage.get("configuration")
            if isinstance(raw_storage, Mapping)
            else None
        )
        if not isinstance(storage_configuration, Mapping) or dict(
            storage_configuration
        ) != dict(state.capture_storage):
            errors.append(
                "capture_storage configuration must exactly match run state"
            )
        else:
            try:
                from pmkt.streaming.storage_backends import CaptureStorageSettings

                CaptureStorageSettings.from_mapping(storage_configuration)
            except (KeyError, TypeError, ValueError) as exc:
                errors.append(f"capture_storage configuration is invalid: {exc}")
        storage_metrics = (
            raw_storage.get("metrics") if isinstance(raw_storage, Mapping) else None
        )
        if not isinstance(storage_metrics, Mapping):
            errors.append("capture_storage.metrics must be an object")
        else:
            for field in (
                "logical_groups_committed",
                "committed_rows",
                "retained_database_bytes_after_promotion",
                "unpromoted_sealed_database_count",
                "unpromoted_sealed_bytes",
            ):
                value = storage_metrics.get(field)
                if type(value) is not int or value < 0:
                    errors.append(
                        f"capture_storage.metrics.{field} must be a nonnegative integer"
                    )

    if raw_journal == COMMIT_JOURNAL_V2_NAME:
        if not isinstance(raw_durability, Mapping):
            errors.append(
                "capture_commit_journal.v2 requires capture_durability metadata"
            )
        else:
            raw_metrics = raw_durability.get("metrics")
            if not isinstance(raw_metrics, Mapping):
                errors.append("capture_durability.metrics must be an object")
            else:
                expected_cause_counts = dict(
                    sorted(
                        Counter(
                            (
                                record.cause.value
                                if isinstance(record.cause, CaptureCommitCause)
                                else str(record.cause)
                            )
                            for record in records
                        ).items()
                    )
                )
                published = raw_metrics.get("groups_published")
                if type(published) is not int or published != len(records):
                    errors.append(
                        "capture_durability.metrics.groups_published must equal "
                        "the journaled group count"
                    )
                if raw_metrics.get("cause_counts") != expected_cause_counts:
                    errors.append(
                        "capture_durability.metrics.cause_counts must exactly "
                        "reconcile with the journal"
                    )
                errors.extend(
                    _durability_latency_metric_errors(
                        raw_metrics.get("acceptance_to_journal_latency_ms"),
                        records=tuple(
                            record
                            for record in records
                            if isinstance(record, CaptureCommitRecordV2)
                        ),
                        recovered=raw_metrics.get("recovered_from_journal") is True,
                    )
                )
                if raw_metrics.get("recovered_from_journal") is True:
                    if (
                        raw_metrics.get("groups_accepted") is not None
                        or raw_metrics.get("groups_discarded") is not None
                    ):
                        errors.append(
                            "recovered capture durability cannot claim unknown "
                            "accepted or discarded group counts"
                        )
                else:
                    accepted = raw_metrics.get("groups_accepted")
                    discarded = raw_metrics.get("groups_discarded")
                    if (
                        type(accepted) is not int
                        or accepted < 0
                        or type(discarded) is not int
                        or discarded < 0
                        or type(published) is not int
                        or accepted != published + discarded
                    ):
                        errors.append(
                            "capture_durability accepted groups must equal "
                            "published plus discarded groups"
                        )

    artifact_roles = {str(role) for role in artifacts}
    if set(state.expected_role_paths) != artifact_roles:
        errors.append(
            "capture journal expected roles must exactly equal dataset_artifacts roles"
        )
    for role, raw_entry in artifacts.items():
        if not isinstance(raw_entry, Mapping):
            continue
        if state.expected_role_paths.get(str(role)) != raw_entry.get("path"):
            errors.append(
                f"capture journal path for role {role!r} must exactly equal "
                "dataset_artifacts path"
            )

    journal_artifacts: dict[str, list[Any]] = {}
    for record in records:
        for artifact in record.artifacts:
            journal_artifacts.setdefault(artifact.role, []).append(artifact)
    journal_roles = set(journal_artifacts)
    if journal_roles != closed_roles:
        errors.append(
            "capture journal roles must exactly equal closed dataset_artifacts roles"
        )

    for role in sorted(artifact_roles):
        raw_entry = artifacts.get(role)
        if not isinstance(raw_entry, Mapping):
            continue
        committed = journal_artifacts.get(role, [])
        committed_count = sum(artifact.row_count for artifact in committed)
        if raw_entry.get("row_count") != committed_count:
            errors.append(
                f"dataset_artifacts.{role} row_count must equal journaled row count"
            )

        raw_segment_path = raw_entry.get("segment_manifest_path")
        segment_path, segment_path_errors = _canonical_artifact_path(
            raw_segment_path,
            run_dir=run_dir,
            key=f"dataset_artifacts.{role}.segment_manifest_path",
        )
        if segment_path_errors or segment_path is None:
            errors.append(
                f"dataset_artifacts.{role} requires a canonical segment manifest"
            )
        else:
            try:
                segment_payload = json.loads(
                    (run_dir / segment_path).read_text(encoding="utf-8")
                )
            except (OSError, json.JSONDecodeError) as exc:
                errors.append(
                    f"dataset_artifacts.{role} segment manifest is invalid: {exc}"
                )
            else:
                expected_segment_payload = {
                    "format": "pmkt.capture_segments.v1",
                    "status": "closed" if role in closed_roles else "failed",
                    "row_count": committed_count,
                    "completed_segments": [
                        {
                            "index": index,
                            "path": Path(artifact.path).name,
                            "row_count": artifact.row_count,
                            "sha256": artifact.sha256,
                        }
                        for index, artifact in enumerate(committed)
                    ],
                    "journal_path": raw_journal,
                }
                if segment_payload != expected_segment_payload:
                    errors.append(
                        f"dataset_artifacts.{role} segment manifest must exactly "
                        "match journal paths, hashes, and counts"
                    )

        dataset_path, dataset_path_errors = _canonical_artifact_path(
            raw_entry.get("path"),
            run_dir=run_dir,
            key=f"dataset_artifacts.{role}.path",
        )
        if dataset_path_errors or dataset_path is None:
            continue
        dataset_root = (run_dir / dataset_path).resolve()
        expected_files = {artifact.path for artifact in committed}
        if dataset_root.is_dir():
            observed_files = {
                path.relative_to(run_dir).as_posix()
                for path in dataset_root.rglob("*.parquet")
                if path.is_file()
            }
        elif dataset_root.is_file():
            observed_files = {dataset_path}
        else:
            observed_files = set()
        if observed_files != expected_files:
            errors.append(
                f"dataset_artifacts.{role} contains missing or unjournaled "
                "physical artifacts"
            )
    return (frozenset(journal_roles) if not errors else frozenset()), errors


def _durability_latency_metric_errors(
    raw_latency: Any,
    *,
    records: Sequence[Any],
    recovered: bool,
) -> list[str]:
    label = "capture_durability acceptance-to-journal latency"
    if not isinstance(raw_latency, Mapping):
        return [f"{label} must be an object"]
    required = {"sample_count", "p50", "p95", "p99", "maximum"}
    if set(raw_latency) != required:
        return [f"{label} must contain exactly {', '.join(sorted(required))}"]
    sample_count = raw_latency.get("sample_count")
    summaries = {
        field: raw_latency.get(field) for field in ("p50", "p95", "p99", "maximum")
    }
    if recovered:
        if sample_count != 0 or any(value is not None for value in summaries.values()):
            return [f"{label} must be unavailable after process-loss recovery"]
        return []
    if type(sample_count) is not int or sample_count != len(records):
        return [f"{label} sample count must equal the journaled group count"]
    if sample_count == 0:
        return (
            []
            if all(value is None for value in summaries.values())
            else [f"{label} summaries must be null when sample_count is zero"]
        )
    errors: list[str] = []
    normalized: dict[str, float] = {}
    for field, value in summaries.items():
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not isfinite(float(value))
            or float(value) < 0
        ):
            errors.append(f"{label}.{field} must be a nonnegative finite number")
        else:
            normalized[field] = float(value)
    if errors:
        return errors
    ordered = (
        normalized["p50"],
        normalized["p95"],
        normalized["p99"],
        normalized["maximum"],
    )
    if list(ordered) != sorted(ordered):
        errors.append(f"{label} summaries must be monotonic through maximum")
    return errors


def _artifact_bundle_errors(
    datasets: Iterable[ManifestDatasetValidation],
    *,
    run_id: str,
    journal_bound_roles: frozenset[str] = frozenset(),
) -> list[str]:
    errors: list[str] = []
    frames: dict[str, pd.DataFrame] = {}
    for dataset in datasets:
        if not dataset.exists or dataset.path is None:
            continue
        try:
            frame = _read_dataset_frame(Path(dataset.path))
        except (OSError, ValueError):
            continue
        frames[dataset.dataset_key] = frame
        if "collector_run_id" not in frame.columns:
            if not frame.empty and dataset.dataset_key not in journal_bound_roles:
                errors.append(
                    f"dataset_artifacts.{dataset.dataset_key}: non-empty exact "
                    "artifact has no collector_run_id authority"
                )
            continue
        invalid_run_id_count = sum(
            1
            for value in frame["collector_run_id"].tolist()
            if not isinstance(value, str) or not value or value != value.strip()
        )
        if invalid_run_id_count:
            errors.append(
                f"dataset_artifacts.{dataset.dataset_key}: collector_run_id contains "
                f"{invalid_run_id_count} null or blank values"
            )
        foreign_run_ids = sorted(
            {
                value
                for value in frame["collector_run_id"].tolist()
                if isinstance(value, str)
                and value
                and value == value.strip()
                and value != run_id
            }
        )
        if foreign_run_ids:
            errors.append(
                f"dataset_artifacts.{dataset.dataset_key}: collector_run_id values "
                f"do not match manifest run_id {run_id!r}: {foreign_run_ids}"
            )

    main = frames.get("topbook_main")
    checkpoint = frames.get("topbook_checkpoint")
    if main is not None and checkpoint is not None:
        primary_key = list(get_table_spec("topbook.v1").primary_key)
        if all(column in main.columns for column in primary_key) and all(
            column in checkpoint.columns for column in primary_key
        ):
            main_keys = set(main[primary_key].itertuples(index=False, name=None))
            checkpoint_keys = set(
                checkpoint[primary_key].itertuples(index=False, name=None)
            )
            overlap = main_keys & checkpoint_keys
            if overlap:
                errors.append(
                    "topbook_main and topbook_checkpoint primary keys must be disjoint; "
                    f"found {len(overlap)} overlapping rows"
                )
    controls = frames.get("tape_control")
    if controls is not None:
        from pmkt.data.validation import validate_book_control_evidence

        evidence_report = validate_book_control_evidence(
            controls,
            tape_events=frames.get("tape_event"),
            topbook_main=main,
            topbook_checkpoint=checkpoint,
        )
        errors.extend(evidence_report.errors)
    return errors


def _manifest_run_dir(payload: Mapping[str, Any], manifest_path: Path) -> Path:
    value = payload.get("run_dir")
    if value is None:
        return manifest_path.parent
    run_dir = Path(str(value))
    if run_dir.is_absolute():
        return run_dir
    return (manifest_path.parent / run_dir).resolve()


def _dataset_entry_path(value: Any) -> str | None:
    if isinstance(value, Mapping):
        for field in ("path", "output_path", "dataset_path"):
            text = _text_or_none(value.get(field))
            if text:
                return text
        return None
    return _text_or_none(value)


def _dataset_entry_value(entry: Mapping[str, Any], field: str) -> Any:
    return entry.get(field)


def _dataset_entry_text(entry: Mapping[str, Any], field: str) -> str | None:
    return _text_or_none(_dataset_entry_value(entry, field))


def _lookup_manifest_value(mapping: Mapping[str, Any], dataset_key: str) -> str | None:
    value = _lookup_manifest_raw_value(mapping, dataset_key)
    return str(value) if value is not None else None


def _lookup_manifest_raw_value(mapping: Mapping[str, Any], dataset_key: str) -> Any:
    for key in _candidate_keys(dataset_key):
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _candidate_keys(dataset_key: str) -> tuple[str, ...]:
    candidates: list[str] = []

    def add(value: str) -> None:
        if value and value not in candidates:
            candidates.append(value)

    key = str(dataset_key)
    add(key)
    lowered = key.lower()
    add(lowered)
    for suffix in (
        "_path",
        "_parquet",
        "_csv",
        "_jsonl",
        "_json",
        ".parquet",
        ".csv",
        ".jsonl",
        ".json",
    ):
        if lowered.endswith(suffix):
            lowered = lowered[: -len(suffix)]
            add(lowered)
    for suffix in ("_v1", "_v2", "_v3"):
        if lowered.endswith(suffix):
            add(lowered[: -len(suffix)])
    if lowered.startswith("topbook"):
        add("topbook")
    if lowered.startswith("depth"):
        add("depth")
    if lowered.startswith("feed_health"):
        add("feed_health")
    if lowered.endswith("events"):
        add("events")
    if lowered.endswith("snapshots"):
        add("snapshots")
    if lowered.endswith("levels"):
        add("levels")
    if lowered.startswith("order_book_levels"):
        add("levels")
    return tuple(candidates)


def _resolve_dataset_path(
    dataset_path: str, manifest_path: Path, run_dir: Path
) -> Path:
    path = Path(dataset_path)
    if path.is_absolute():
        return path
    candidates = [
        run_dir / path,
        manifest_path.parent / path,
        Path.cwd() / path,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def _read_dataset_frame(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if path.is_dir():
        parts = sorted(path.glob("part-*.parquet"))
        if not parts:
            return pd.DataFrame()
        frames = [pd.read_parquet(part) for part in parts]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    try:
        if path.stat().st_size == 0:
            return pd.DataFrame()
    except OSError:
        pass
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".csv":
        try:
            return pd.read_csv(path)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
    if suffix == ".jsonl":
        try:
            return pd.read_json(path, lines=True)
        except ValueError as exc:
            if "Expected object or value" in str(exc):
                return pd.DataFrame()
            raise
    if suffix == ".json":
        frame = pd.read_json(path)
        if isinstance(frame, pd.DataFrame):
            return frame
    raise ValueError(f"unsupported dataset format for validation: {path}")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    if path.is_dir():
        for child in sorted(item for item in path.rglob("*") if item.is_file()):
            relative = child.relative_to(path).as_posix().encode("utf-8")
            digest.update(relative)
            digest.update(b"\0")
            with child.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            digest.update(b"\0")
        return digest.hexdigest()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_int(value: Any) -> int | None:
    if isinstance(value, str) and not _MANIFEST_INTEGER_TEXT_RE.fullmatch(
        value.strip()
    ):
        return None
    parsed = parse_exact_int(value)
    if parsed is None or parsed < 0:
        return None
    return parsed


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


__all__ = [
    "ManifestDatasetValidation",
    "ManifestValidationReport",
    "RUN_MANIFEST_SCHEMA_VERSION",
    "build_run_manifest",
    "core_implementation_provenance",
    "count_quality_flags",
    "current_git_commit",
    "validate_run_manifest",
    "write_manifest",
]
