from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from shutil import copymode
from typing import Any, Callable, Iterable, Literal, Mapping
from uuid import uuid4

import pandas as pd

from pmkt.data.canonical import RUN_MANIFEST_SCHEMA_VERSION, run_manifest_row
from pmkt.data.types import parse_int as parse_exact_int
from pmkt.provenance import implementation_identity


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


def core_implementation_provenance() -> dict[str, Any]:
    """Return observed identity, raising on contradictory implementation evidence."""
    identity = implementation_identity(
        Path(__file__).resolve().parents[1] / "__init__.py", "pmkt"
    ).require_consistent()
    return {
        "pmkt_core_commit": identity.commit,
        "pmkt_core_version": identity.version,
        "pmkt_core_dirty": identity.dirty,
        "pmkt_core_provenance_source": identity.evidence_source,
    }


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
        manifest[key] = value
    if git_commit:
        manifest.setdefault("caller_git_commit", git_commit)
    return manifest


def write_manifest(path: str | Path, manifest: Mapping[str, Any]) -> Path:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    target = output.resolve()
    temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
    content = json.dumps(manifest, indent=2, sort_keys=True)
    handle = temporary.open("x", encoding="utf-8")
    try:
        with handle as writer:
            if target.exists():
                copymode(target, temporary)
            writer.write(content)
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
    return output


def validate_run_manifest(
    path: str | Path,
    *,
    exact_artifact_validation: Literal["full", "structure"] = "full",
    path_resolver: Callable[[Path], Path] | None = None,
) -> ManifestValidationReport:
    """Validate a run manifest with the same checks for either historical mode."""
    if exact_artifact_validation not in ("full", "structure"):
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
    if "storage_profile" in payload or "dataset_artifacts" in payload:
        return ManifestValidationReport(
            manifest_path=str(manifest_path),
            errors=("retired manifest declares storage_profile or dataset_artifacts",),
        )

    if "recording_format" in payload:
        from pmkt.streaming.recording_store import validate_recording_manifest
        return ManifestValidationReport(str(manifest_path), errors=validate_recording_manifest(manifest_path))

    if payload.get("schema_version") == RUN_MANIFEST_SCHEMA_VERSION:
        from pmkt.data.validation import validate_frame

        report = validate_frame(pd.DataFrame([payload]), RUN_MANIFEST_SCHEMA_VERSION)
        errors.extend(report.errors)

    dataset_paths = _manifest_mapping(payload, "dataset_paths", warnings)
    schema_versions = _manifest_mapping(payload, "schema_versions", warnings)
    row_counts = _manifest_mapping(payload, "row_counts", warnings)
    hashes = _hash_mapping(payload, warnings)
    run_dir = _manifest_run_dir(payload, manifest_path, path_resolver=path_resolver)

    datasets: list[ManifestDatasetValidation] = []
    if dataset_paths:
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
                    path_resolver=path_resolver,
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
                path_resolver=path_resolver,
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
    path_resolver: Callable[[Path], Path] | None = None,
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

    resolved_path = _resolve_dataset_path(dataset_path, manifest_path, run_dir, path_resolver=path_resolver)
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
    for field in ("dataset_hashes", "dataset_sha256", "sha256_hashes", "sha256", "output_sha256"):
        value = payload.get(field)
        if value is None:
            continue
        if isinstance(value, str) and field in {"sha256", "output_sha256"}:
            hashes["output"] = value
            continue
        mapping = _manifest_mapping(payload, field, warnings)
        hashes.update(
            {key: str(item) for key, item in mapping.items() if item is not None}
        )
    return hashes


def _manifest_run_dir(
    payload: Mapping[str, Any], manifest_path: Path,
    *, path_resolver: Callable[[Path], Path] | None = None,
) -> Path:
    value = payload.get("run_dir")
    if value is None:
        return manifest_path.parent
    run_dir = path_resolver(Path(str(value))) if path_resolver else Path(str(value))
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
    dataset_path: str, manifest_path: Path, run_dir: Path,
    *, path_resolver: Callable[[Path], Path] | None = None,
) -> Path:
    path = path_resolver(Path(dataset_path)) if path_resolver else Path(dataset_path)
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
        parts = sorted(path.rglob("part-*.parquet"))
        if not parts:
            return pd.DataFrame()
        frames = [pd.read_parquet(part) for part in parts]
        return pd.concat(frames, ignore_index=True)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".csv":
        try:
            return pd.read_csv(path)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
    if suffix == ".jsonl":
        return pd.read_json(path, lines=True)
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
