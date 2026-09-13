"""Pinned history-manifest loading and local artifact validation."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import ntpath
import os
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath
import posixpath
import re
import time
from typing import Any, Literal, Mapping

from pmkt.data.registry import (
    KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
    get_table_spec,
)

from .fs import sha256_file
from .types import CatalogError, HISTORY_MANIFEST_SCHEMA


ValidationLevel = Literal["full", "metadata"]

_ARTIFACTS = {
    "polymarket_all_markets": (
        "polymarket",
        POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
    ),
    "kalshi_all_markets": ("kalshi", KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION),
}


@dataclass(frozen=True)
class ResolvedCatalogArtifact:
    name: str
    venue: str
    schema_version: str
    descriptor: Mapping[str, Any]
    declared_path: str
    path: Path
    files: tuple[Path, ...]
    relative_files: tuple[str, ...]
    row_count: int
    size_bytes: int


@dataclass(frozen=True)
class CatalogOpenState:
    manifest_locator: str
    manifest_path: Path
    manifest_sha256: str
    manifest: Mapping[str, Any]
    artifacts: tuple[ResolvedCatalogArtifact, ...]
    validation_level: ValidationLevel
    identity_verification: str
    performed_checks: tuple[str, ...]
    skipped_checks: tuple[str, ...]
    elapsed_seconds: float

    @property
    def row_count(self) -> int:
        return sum(artifact.row_count for artifact in self.artifacts)

    @property
    def parquet_file_count(self) -> int:
        return sum(len(artifact.files) for artifact in self.artifacts)


def _read_json_object(path: Path) -> tuple[bytes, dict[str, Any]]:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise CatalogError(f"catalog evidence is not readable: {path}") from exc
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CatalogError(f"catalog evidence is not valid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise CatalogError(f"expected JSON object at {path}")
    return payload, value


RecordedPath = PurePosixPath | PureWindowsPath


def _is_network_path_text(value: str) -> bool:
    normalized = value.replace("\\", "/")
    return normalized.startswith("//") or bool(
        re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", value)
    )


def _recorded_absolute(value: str | Path | PurePath) -> RecordedPath:
    raw = str(value)
    windows = PureWindowsPath(raw)
    posix = PurePosixPath(raw)
    if windows.is_absolute():
        return PureWindowsPath(ntpath.normpath(raw))
    if posix.is_absolute():
        return PurePosixPath(posixpath.normpath(raw))
    absolute = os.path.abspath(os.path.normpath(os.path.expanduser(raw)))
    if os.name == "nt":
        return PureWindowsPath(ntpath.normpath(absolute))
    return PurePosixPath(posixpath.normpath(absolute))


def _recorded_relative(
    value: str, *, recorded_path_base: RecordedPath, label: str
) -> PurePath:
    if not value.strip():
        raise CatalogError(f"{label} has no path")
    if _is_network_path_text(value):
        raise CatalogError(f"{label} must be a local filesystem path")
    flavor = type(recorded_path_base)
    windows_value = PureWindowsPath(value)
    posix_value = PurePosixPath(value)
    if flavor is PurePosixPath and (bool(windows_value.drive) or "\\" in value):
        raise CatalogError(f"{label} uses an incompatible path flavor")
    if flavor is PureWindowsPath and (
        (posix_value.is_absolute() and not windows_value.is_absolute())
        or (bool(windows_value.drive) and not windows_value.is_absolute())
    ):
        raise CatalogError(f"{label} uses an incompatible path flavor")
    declared = flavor(value)
    if declared.is_absolute():
        lexical = flavor(
            ntpath.normpath(value)
            if flavor is PureWindowsPath
            else posixpath.normpath(value)
        )
    else:
        joined = str(recorded_path_base / declared)
        lexical = flavor(
            ntpath.normpath(joined)
            if flavor is PureWindowsPath
            else posixpath.normpath(joined)
        )
    try:
        return lexical.relative_to(recorded_path_base)
    except ValueError as exc:
        raise CatalogError(
            f"{label} escapes declared path base {recorded_path_base}: {lexical}"
        ) from exc


def _existing_base(value: str | Path | PurePath, *, label: str) -> Path:
    raw = str(value)
    if _is_network_path_text(raw):
        raise CatalogError(f"{label} must be a local filesystem path")
    path = Path(os.path.abspath(os.path.normpath(os.path.expanduser(raw)))).resolve(
        strict=False
    )
    if not path.is_dir():
        raise CatalogError(f"{label} is not an existing directory: {path}")
    return path


def _relative_to(path: Path, base: Path, *, label: str) -> Path:
    try:
        return path.relative_to(base)
    except ValueError as exc:
        raise CatalogError(f"{label} escapes declared path base {base}: {path}") from exc


def resolve_declared_path(
    value: str,
    *,
    recorded_path_base: RecordedPath,
    effective_path_base: Path,
    label: str,
    must_exist: bool = True,
) -> Path:
    relative = _recorded_relative(
        value, recorded_path_base=recorded_path_base, label=label
    )
    relocated = effective_path_base.joinpath(*relative.parts)
    try:
        resolved = relocated.resolve(strict=must_exist)
    except (OSError, RuntimeError) as exc:
        raise CatalogError(f"{label} is missing or unresolvable: {relocated}") from exc
    _relative_to(resolved, effective_path_base, label=label)
    if must_exist and not resolved.exists():
        raise CatalogError(f"{label} is missing: {resolved}")
    return resolved


def _resolved_parquet_files(
    root: Path, *, containment_base: Path, artifact_name: str
) -> tuple[tuple[Path, ...], tuple[str, ...]]:
    resolved_root = root.resolve(strict=True)
    _relative_to(
        resolved_root,
        containment_base,
        label=f"history artifact {artifact_name}",
    )
    if resolved_root.is_file():
        if resolved_root.suffix.casefold() != ".parquet":
            raise CatalogError(
                f"history artifact {artifact_name} is not Parquet: {resolved_root}"
            )
        return (resolved_root,), (resolved_root.name,)
    if not resolved_root.is_dir():
        raise CatalogError(f"history artifact {artifact_name} has unsupported type")

    pending: list[tuple[Path, str]] = [(root, "")]
    visited_directories: set[Path] = set()
    files: list[tuple[str, Path]] = []
    seen_files: set[Path] = set()
    while pending:
        lexical_directory, relative_directory = pending.pop()
        try:
            resolved_directory = lexical_directory.resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise CatalogError(
                f"history artifact {artifact_name} has an unresolvable directory: "
                f"{lexical_directory}"
            ) from exc
        _relative_to(
            resolved_directory,
            containment_base,
            label=f"history artifact {artifact_name}",
        )
        if resolved_directory in visited_directories:
            raise CatalogError(
                f"history artifact {artifact_name} has a link cycle or alias: "
                f"{lexical_directory}"
            )
        visited_directories.add(resolved_directory)
        try:
            entries = list(os.scandir(lexical_directory))
        except OSError as exc:
            raise CatalogError(
                f"history artifact {artifact_name} is not traversable: "
                f"{lexical_directory}"
            ) from exc
        for entry in entries:
            entry_path = Path(entry.path)
            relative = (
                f"{relative_directory}/{entry.name}"
                if relative_directory
                else entry.name
            )
            try:
                resolved_entry = entry_path.resolve(strict=True)
            except (OSError, RuntimeError) as exc:
                raise CatalogError(
                    f"history artifact {artifact_name} has an unresolvable entry: "
                    f"{entry_path}"
                ) from exc
            _relative_to(
                resolved_entry,
                containment_base,
                label=f"history artifact {artifact_name}",
            )
            if entry.is_dir(follow_symlinks=True):
                pending.append((entry_path, relative))
            elif entry.is_file(follow_symlinks=True) and entry.name.casefold().endswith(
                ".parquet"
            ):
                if resolved_entry in seen_files:
                    raise CatalogError(
                        f"history artifact {artifact_name} aliases a Parquet file: "
                        f"{entry_path}"
                    )
                seen_files.add(resolved_entry)
                files.append((relative.replace("\\", "/"), resolved_entry))
    # ``fs.tree_sha256`` hashes files in ``sorted(Path)`` order, which compares
    # path components rather than the joined string. Sorting the joined string
    # diverges when one directory name is a prefix of a sibling followed by a
    # character below "/" (for example ``date=2026-08`` and ``date=2026-08-22``),
    # so the declared tree hash would be rejected for a valid artifact.
    files.sort(key=lambda item: PurePath(item[0]))
    if not files:
        raise CatalogError(f"history artifact {artifact_name} contains no Parquet")
    return tuple(path for _relative, path in files), tuple(
        relative for relative, _path in files
    )


def _count_field(
    descriptor: Mapping[str, Any], field: str, *, artifact_name: str
) -> int:
    value = descriptor.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CatalogError(
            f"history artifact {artifact_name} has invalid {field}: {value!r}"
        )
    return value


def _hash_tree(files: tuple[Path, ...], relative_files: tuple[str, ...]) -> str:
    digest = hashlib.sha256()
    for relative, path in zip(relative_files, files):
        digest.update(relative.encode("utf-8"))
        digest.update(bytes.fromhex(sha256_file(path)))
    return digest.hexdigest()


def _validate_canonical_columns(
    names: list[str], expected_names: list[str], *, artifact_name: str, path: Path
) -> None:
    if len(names) != len(set(names)) or set(names) != set(expected_names):
        raise CatalogError(
            f"history artifact {artifact_name} has incompatible columns: {path}"
        )


def _validate_canonical_batches(
    files: tuple[Path, ...], schema_version: str, *, artifact_name: str
) -> None:
    import pyarrow.parquet as pq
    from pmkt.data.validation import validate_frame

    spec = get_table_spec(schema_version)
    expected_names = list(spec.columns)
    for path in files:
        try:
            parquet = pq.ParquetFile(path)
            schema = parquet.schema_arrow
        except (OSError, ValueError) as exc:
            raise CatalogError(
                f"history artifact {artifact_name} has unreadable Parquet: {path}"
            ) from exc
        _validate_canonical_columns(
            schema.names, expected_names, artifact_name=artifact_name, path=path
        )
        try:
            for batch in parquet.iter_batches(batch_size=65_536):
                report = validate_frame(batch.to_pandas(), spec, strict=False)
                if not report.ok:
                    raise CatalogError(
                        f"history artifact {artifact_name} violates canonical schema "
                        f"{schema_version}: {'; '.join(report.errors)}: {path}"
                    )
        except CatalogError:
            raise
        except (OSError, ValueError) as exc:
            raise CatalogError(
                f"history artifact {artifact_name} has unreadable Parquet: {path}"
            ) from exc


def _validate_lineage(manifest: Mapping[str, Any]) -> None:
    if manifest.get("dataset_family") != "market_history":
        raise CatalogError("history manifest has invalid dataset_family")
    release_id = manifest.get("release_id")
    if not isinstance(release_id, str) or not release_id.strip():
        raise CatalogError("history manifest has no release_id")
    parent = manifest.get("parent_release")
    if parent is not None:
        if not isinstance(parent, dict):
            raise CatalogError("history manifest parent_release is invalid")
        for field in ("release_id", "manifest_path", "manifest_sha256"):
            if not isinstance(parent.get(field), str) or not parent[field].strip():
                raise CatalogError(
                    f"history manifest parent_release has no {field}"
                )
        if not _is_sha256(parent["manifest_sha256"]):
            raise CatalogError("history manifest parent_release hash is invalid")
    parents = manifest.get("parent_manifests")
    if parents is not None:
        if not isinstance(parents, list):
            raise CatalogError("history manifest parent_manifests is invalid")
        for parent_reference in parents:
            if not isinstance(parent_reference, dict) or not all(
                isinstance(parent_reference.get(field), str)
                and bool(parent_reference[field].strip())
                for field in ("path", "sha256")
            ):
                raise CatalogError("history manifest has an invalid parent manifest")
            if not _is_sha256(parent_reference["sha256"]):
                raise CatalogError("history manifest has an invalid parent manifest hash")


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(
        character in "0123456789abcdefABCDEF" for character in value
    )


def _validate_parent_manifests(
    manifest: Mapping[str, Any],
    *,
    recorded_path_base: RecordedPath,
    effective_path_base: Path,
    validation: ValidationLevel,
) -> None:
    references: list[tuple[str, str, str | None]] = []
    parent_release = manifest.get("parent_release")
    if isinstance(parent_release, dict):
        references.append(
            (
                parent_release["manifest_path"],
                parent_release["manifest_sha256"],
                parent_release["release_id"],
            )
        )
    parent_manifests = manifest.get("parent_manifests")
    if isinstance(parent_manifests, list):
        references.extend(
            (reference["path"], reference["sha256"], None)
            for reference in parent_manifests
        )

    seen: dict[Path, str] = {}
    for locator, expected_hash, expected_release in references:
        path = resolve_declared_path(
            locator,
            recorded_path_base=recorded_path_base,
            effective_path_base=effective_path_base,
            label="parent history manifest",
        )
        prior_hash = seen.get(path)
        if prior_hash is not None:
            if prior_hash != expected_hash.casefold():
                raise CatalogError(
                    f"parent history manifest has conflicting hashes: {path}"
                )
            continue
        seen[path] = expected_hash.casefold()
        if validation != "full":
            continue
        payload, parent = _read_json_object(path)
        actual_hash = hashlib.sha256(payload).hexdigest()
        if actual_hash != expected_hash.casefold():
            raise CatalogError(f"parent history manifest hash is invalid: {path}")
        if parent.get("schema_version") != HISTORY_MANIFEST_SCHEMA:
            raise CatalogError(f"parent history manifest schema is invalid: {path}")
        if expected_release is not None and parent.get("release_id") != expected_release:
            raise CatalogError(f"parent history manifest release_id is invalid: {path}")


def load_history_manifest(
    *,
    manifest_locator: str,
    expected_sha256: str | None,
    recorded_path_base: str | Path | PurePath,
    effective_path_base: str | Path | PurePath | None = None,
    pointer_artifacts: Mapping[str, Any] | None = None,
    validation: ValidationLevel = "full",
    identity_verification: str,
) -> CatalogOpenState:
    if validation not in {"full", "metadata"}:
        raise ValueError("validation must be 'full' or 'metadata'")
    started = time.perf_counter()
    recorded_base = _recorded_absolute(recorded_path_base)
    effective_base = _existing_base(
        effective_path_base or recorded_base, label="effective path_base"
    )
    manifest_path = resolve_declared_path(
        manifest_locator,
        recorded_path_base=recorded_base,
        effective_path_base=effective_base,
        label="history manifest",
    )
    if not manifest_path.is_file():
        raise CatalogError(f"history manifest is not a file: {manifest_path}")
    manifest_bytes, manifest = _read_json_object(manifest_path)
    actual_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    if expected_sha256 is not None:
        if not _is_sha256(expected_sha256):
            raise CatalogError("history manifest expected_sha256 is invalid")
        if actual_sha256 != expected_sha256.casefold():
            raise CatalogError(
                f"history manifest hash is invalid: {manifest_path}; "
                f"actual={actual_sha256}, expected={expected_sha256}"
            )
    if manifest.get("schema_version") != HISTORY_MANIFEST_SCHEMA:
        raise CatalogError("unsupported market history manifest schema")
    _validate_lineage(manifest)
    _validate_parent_manifests(
        manifest,
        recorded_path_base=recorded_base,
        effective_path_base=effective_base,
        validation=validation,
    )

    manifest_artifacts = manifest.get("artifacts")
    if not isinstance(manifest_artifacts, dict):
        raise CatalogError("history manifest has no artifacts")
    resolved_artifacts: list[ResolvedCatalogArtifact] = []
    legacy_artifacts: list[str] = []
    legacy_file_counts: list[str] = []
    total_rows = 0
    for artifact_name, (venue, expected_schema) in _ARTIFACTS.items():
        descriptor = manifest_artifacts.get(artifact_name)
        if not isinstance(descriptor, dict):
            raise CatalogError(f"history manifest has no {artifact_name} artifact")
        if pointer_artifacts is not None:
            pointer_descriptor = pointer_artifacts.get(artifact_name)
            if not isinstance(pointer_descriptor, dict):
                raise CatalogError(
                    f"history pointer has no {artifact_name} artifact reference"
                )
            if dict(pointer_descriptor) != dict(descriptor):
                raise CatalogError(
                    f"history pointer and manifest disagree for {artifact_name}"
                )
        declared_path = str(descriptor.get("path") or "")
        path = resolve_declared_path(
            declared_path,
            recorded_path_base=recorded_base,
            effective_path_base=effective_base,
            label=f"history artifact {artifact_name}",
        )
        files, relative_files = _resolved_parquet_files(
            path, containment_base=effective_base, artifact_name=artifact_name
        )
        recorded_format = descriptor.get("format")
        actual_format = "parquet" if path.is_file() else "partitioned_parquet"
        legacy_single_file = (
            "schema" not in descriptor
            and recorded_format == "parquet_file"
            and path.is_file()
        )
        if recorded_format != actual_format and not legacy_single_file:
            raise CatalogError(
                f"history artifact {artifact_name} has invalid format: "
                f"{recorded_format!r}"
            )
        # Only a missing key is legacy; an explicit null is an invalid declaration.
        # Infer from the known artifact role without rewriting hashed evidence.
        declared_schema = descriptor.get("schema")
        if "schema" not in descriptor:
            legacy_artifacts.append(artifact_name)
        elif declared_schema != expected_schema:
            raise CatalogError(
                f"history artifact {artifact_name} has unsupported schema: "
                f"{descriptor.get('schema')!r}"
            )
        expected_rows = _count_field(descriptor, "rows", artifact_name=artifact_name)
        if legacy_single_file and "parquet_file_count" not in descriptor:
            # The old single-file descriptor has an unambiguous count. Never
            # infer the count of an undeclared partitioned dataset.
            expected_files = 1
            legacy_file_counts.append(artifact_name)
        else:
            expected_files = _count_field(
                descriptor, "parquet_file_count", artifact_name=artifact_name
            )
        expected_size = _count_field(
            descriptor, "size_bytes", artifact_name=artifact_name
        )
        if len(files) != expected_files:
            raise CatalogError(
                f"history artifact {artifact_name} file count is invalid: "
                f"actual={len(files)}, expected={expected_files}"
            )
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            from pmkt.errors import OptionalDependencyError

            raise OptionalDependencyError(
                "catalog reading requires the 'data' extra: pip install 'pmkt[data]'"
            ) from exc
        try:
            actual_rows = 0
            # Both modes require canonical columns; only full mode validates values.
            expected_names = list(get_table_spec(expected_schema).columns)
            for parquet_path in files:
                parquet = pq.ParquetFile(parquet_path)
                _validate_canonical_columns(
                    parquet.schema_arrow.names,
                    expected_names,
                    artifact_name=artifact_name,
                    path=parquet_path,
                )
                actual_rows += int(parquet.metadata.num_rows)
        except CatalogError:
            raise
        except (OSError, ValueError) as exc:
            raise CatalogError(
                f"history artifact {artifact_name} has unreadable Parquet metadata"
            ) from exc
        if actual_rows != expected_rows:
            raise CatalogError(
                f"history artifact {artifact_name} row count is invalid: "
                f"actual={actual_rows}, expected={expected_rows}"
            )
        actual_size = sum(path.stat().st_size for path in files)
        if actual_size != expected_size:
            raise CatalogError(
                f"history artifact {artifact_name} size is invalid: "
                f"actual={actual_size}, expected={expected_size}"
            )
        if validation == "full":
            if path.is_file():
                expected_hash = descriptor.get("sha256")
                if not isinstance(expected_hash, str) or sha256_file(path) != expected_hash:
                    raise CatalogError(
                        f"history artifact {artifact_name} content hash is invalid: {path}"
                    )
            else:
                expected_tree = descriptor.get("tree_sha256")
                if (
                    not isinstance(expected_tree, str)
                    or _hash_tree(files, relative_files) != expected_tree
                ):
                    raise CatalogError(
                        f"history artifact {artifact_name} tree hash is invalid: {path}"
                    )
            _validate_canonical_batches(
                files, expected_schema, artifact_name=artifact_name
            )
        total_rows += actual_rows
        resolved_artifacts.append(
            ResolvedCatalogArtifact(
                name=artifact_name,
                venue=venue,
                schema_version=expected_schema,
                descriptor=dict(descriptor),
                declared_path=declared_path,
                path=path,
                files=files,
                relative_files=relative_files,
                row_count=actual_rows,
                size_bytes=actual_size,
            )
        )

    accounting = ("base_row_count", "uncompacted_delta_rows")
    if any(field in manifest for field in accounting):
        if not all(field in manifest for field in accounting):
            raise CatalogError("history promotion row accounting is incomplete")
        expected_total = sum(
            _count_field(manifest, field, artifact_name="promotion_accounting")
            for field in accounting
        )
        if total_rows != expected_total:
            raise CatalogError(
                "history base-plus-delta row accounting is invalid: "
                f"actual={total_rows}, expected={expected_total}"
            )

    performed = [
        "manifest_identity",
        "manifest_structure_and_lineage",
        "parent_manifest_reference_containment",
        "artifact_path_containment",
        "artifact_file_count",
        "artifact_size",
        "artifact_row_count",
        "artifact_column_names",
    ]
    skipped: list[str] = []
    if legacy_artifacts:
        performed.append("artifact_schema_declarations_present")
        for artifact_name in legacy_artifacts:
            performed.append(f"legacy_schema_inferred_from_columns:{artifact_name}")
            skipped.append(f"artifact_schema_declaration:{artifact_name}")
    else:
        performed.append("artifact_schema_declarations")
    for artifact_name in legacy_file_counts:
        performed.append(f"legacy_single_file_count_inferred:{artifact_name}")
        skipped.append(f"artifact_file_count_declaration:{artifact_name}")
    if validation == "full":
        performed.extend(
            [
                "artifact_canonical_schemas",
                "artifact_content_hashes",
                "parent_manifest_content_hashes",
            ]
        )
    else:
        skipped.extend(
            [
                "artifact_canonical_schemas",
                "artifact_content_hashes",
                "parent_manifest_content_hashes",
            ]
        )
    return CatalogOpenState(
        manifest_locator=manifest_locator,
        manifest_path=manifest_path,
        manifest_sha256=actual_sha256,
        manifest=manifest,
        artifacts=tuple(resolved_artifacts),
        validation_level=validation,
        identity_verification=identity_verification,
        performed_checks=tuple(performed),
        skipped_checks=tuple(skipped),
        elapsed_seconds=time.perf_counter() - started,
    )


def load_latest_history(
    market_root: str | Path,
    *,
    path_base: str | Path,
    validation: ValidationLevel = "full",
) -> CatalogOpenState:
    recorded_base = _recorded_absolute(path_base)
    effective_base = _existing_base(path_base, label="path_base")
    root = resolve_declared_path(
        str(market_root),
        recorded_path_base=recorded_base,
        effective_path_base=effective_base,
        label="market_root",
    )
    if not root.is_dir():
        raise CatalogError(f"market_root is not a directory: {root}")
    pointer_path = root / "history" / "LATEST.json"
    if not pointer_path.is_file():
        raise CatalogError(
            "history/LATEST.json is required; select or restore an existing "
            "published history release first. `pmkt markets promote-history` "
            "updates an initialized catalog after discovery; it does not create "
            "the initial history release"
        )
    _relative_to(pointer_path.resolve(strict=True), effective_base, label="history pointer")
    _pointer_bytes, pointer = _read_json_object(pointer_path)
    manifest_reference = pointer.get("manifest")
    if not isinstance(manifest_reference, dict):
        raise CatalogError("history pointer has no manifest reference")
    locator = str(manifest_reference.get("path") or "")
    expected_sha256 = manifest_reference.get("sha256")
    if not isinstance(expected_sha256, str):
        raise CatalogError("history pointer has no manifest hash")
    state = load_history_manifest(
        manifest_locator=locator,
        expected_sha256=expected_sha256,
        recorded_path_base=recorded_base,
        effective_path_base=effective_base,
        pointer_artifacts=pointer,
        validation=validation,
        identity_verification="history_pointer",
    )
    pointer_release = pointer.get("release_id")
    if pointer_release is not None and pointer_release != state.manifest.get("release_id"):
        raise CatalogError("history pointer and manifest disagree on release_id")
    return state


__all__ = [
    "CatalogOpenState",
    "ResolvedCatalogArtifact",
    "ValidationLevel",
    "load_history_manifest",
    "load_latest_history",
    "resolve_declared_path",
]
