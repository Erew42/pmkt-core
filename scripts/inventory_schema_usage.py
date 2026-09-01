from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Iterable, Mapping, Sequence

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - optional only for local script use.
    pq = None


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pmkt.data.registry import list_table_specs  # noqa: E402


DEFAULT_ARTIFACT_ROOTS = ("data", "generated", "local_data")
DEFAULT_TEXT_ROOTS = ("src", "apps", "scripts", "tests", "docs")
DEFAULT_CATALOG = "docs/schema_lifecycle.json"
TEXT_SUFFIXES = {
    ".csv",
    ".ipynb",
    ".json",
    ".jsonl",
    ".md",
    ".py",
    ".rst",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}
_TOKEN_CHARACTERS = rb"A-Za-z0-9_.-"
_REPARSE_POINT_ATTRIBUTE = 0x400


def _repo_path(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except (OSError, ValueError):
        return path.resolve().as_posix()


def _is_link_or_reparse(path: Path) -> bool:
    try:
        stat = path.lstat()
    except OSError:
        return False
    attributes = int(getattr(stat, "st_file_attributes", 0))
    return path.is_symlink() or bool(attributes & _REPARSE_POINT_ATTRIBUTE)


def _walk_files(scan_root: Path) -> tuple[list[Path], list[dict[str, str]]]:
    files: list[Path] = []
    errors: list[dict[str, str]] = []
    if not scan_root.exists():
        return files, [
            {
                "path": scan_root.as_posix(),
                "kind": "missing_root",
                "message": "declared scan root does not exist",
            }
        ]
    if _is_link_or_reparse(scan_root):
        return files, [
            {
                "path": scan_root.as_posix(),
                "kind": "reparse_or_symlink",
                "message": "declared scan root is a link or reparse point",
            }
        ]
    for current, directory_names, file_names in os.walk(scan_root, followlinks=False):
        current_path = Path(current)
        retained_directories: list[str] = []
        for name in sorted(directory_names):
            candidate = current_path / name
            if _is_link_or_reparse(candidate):
                errors.append(
                    {
                        "path": candidate.as_posix(),
                        "kind": "reparse_or_symlink",
                        "message": "directory was not traversed",
                    }
                )
            else:
                retained_directories.append(name)
        directory_names[:] = retained_directories
        for name in sorted(file_names):
            candidate = current_path / name
            if _is_link_or_reparse(candidate):
                errors.append(
                    {
                        "path": candidate.as_posix(),
                        "kind": "reparse_or_symlink",
                        "message": "file was not read",
                    }
                )
            else:
                files.append(candidate)
    return sorted(files), errors


def _schema_pattern(schema_versions: Sequence[str]) -> re.Pattern[bytes]:
    alternatives = b"|".join(
        re.escape(version.encode("ascii"))
        for version in sorted(schema_versions, key=lambda value: (-len(value), value))
    )
    return re.compile(
        rb"(?<!["
        + _TOKEN_CHARACTERS
        + rb"])(?:"
        + alternatives
        + rb")(?!["
        + _TOKEN_CHARACTERS
        + rb"])"
    )


def _schema_tokens(path: Path, pattern: re.Pattern[bytes]) -> set[str]:
    matches: set[str] = set()
    carry = b""
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            payload = carry + chunk
            matches.update(match.group(0).decode("ascii") for match in pattern.finditer(payload))
            carry = payload[-128:]
    return matches


def _text_surface(root: Path, path: Path, artifact_roots: Sequence[Path]) -> str:
    relative = _repo_path(root, path)
    if relative == "src/pmkt/data/registry.py":
        return "registry"
    if relative == "src/pmkt/data/__init__.py":
        return "public_export"
    if relative == DEFAULT_CATALOG:
        return "lifecycle_catalog"
    if relative.startswith("tests/"):
        return "tests"
    if relative.startswith("apps/"):
        return "application"
    if relative.startswith("scripts/"):
        return "scripts"
    if relative.startswith("docs/"):
        return "docs"
    if relative.startswith("src/"):
        return "source"
    if path.suffix.lower() == ".ipynb":
        return "notebook"
    if "manifest" in path.name.casefold():
        return "manifest"
    if any(_is_within(path, artifact_root) for artifact_root in artifact_roots):
        return "artifact_text"
    return "other"


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
    except (OSError, ValueError):
        return False
    return True


def _physical_column_index(parquet_file: Any, name: str) -> int | None:
    """Resolve an exact top-level Parquet leaf path, not an Arrow field ordinal."""

    matches = [
        index
        for index in range(len(parquet_file.schema))
        if parquet_file.schema.column(index).path == name
    ]
    if len(matches) > 1:
        raise ValueError(f"Parquet contains multiple physical columns named {name!r}")
    return matches[0] if matches else None


def _parquet_schema_counts(path: Path) -> tuple[Counter[str], int, str]:
    if pq is None:
        raise RuntimeError("pyarrow is required to inspect Parquet schema versions")
    parquet_file = pq.ParquetFile(path)
    total_rows = int(parquet_file.metadata.num_rows)
    column_index = _physical_column_index(parquet_file, "schema_version")
    if column_index is None:
        return Counter(), total_rows, "absent"
    if total_rows == 0:
        return Counter(), total_rows, "present_empty"
    counts: Counter[str] = Counter()
    for row_group_index in range(parquet_file.num_row_groups):
        row_group = parquet_file.metadata.row_group(row_group_index)
        row_count = int(row_group.num_rows)
        column = row_group.column(column_index)
        statistics = column.statistics
        if (
            statistics is not None
            and statistics.has_min_max
            and statistics.min == statistics.max
            and int(statistics.null_count or 0) == 0
        ):
            value = statistics.min
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            counts[str(value)] += row_count
            continue
        table = parquet_file.read_row_group(row_group_index, columns=["schema_version"])
        for value in table.column(0).to_pylist():
            counts["<null>" if value is None else str(value)] += 1
    return counts, total_rows, "present_with_values"


def _inspect_parquet(
    path: Path,
) -> tuple[Path, Counter[str] | None, int | None, str | None, str | None]:
    try:
        counts, total_rows, column_state = _parquet_schema_counts(path)
    except Exception as exc:
        return path, None, None, None, f"{type(exc).__name__}: {exc}"
    return path, counts, total_rows, column_state, None


def load_lifecycle_catalog(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("schema lifecycle catalog must contain a JSON object")
    return payload


def lifecycle_entries(catalog: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    status_definitions = catalog.get("status_definitions")
    groups = catalog.get("groups")
    overrides = catalog.get("evidence_overrides", {})
    generic_consumers = catalog.get("generic_consumers", [])
    if not isinstance(status_definitions, dict) or not status_definitions:
        raise ValueError("catalog status_definitions must be a non-empty object")
    if not isinstance(groups, list):
        raise ValueError("catalog groups must be a list")
    if not isinstance(overrides, dict):
        raise ValueError("catalog evidence_overrides must be an object")
    if not isinstance(generic_consumers, list):
        raise ValueError("catalog generic_consumers must be a list")
    for consumer in generic_consumers:
        if not isinstance(consumer, dict):
            raise ValueError("each generic consumer must be an object")
        for field in ("path", "kind", "mechanism", "scope", "removal_effect"):
            value = consumer.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"each generic consumer requires non-empty {field!r}"
                )
    entries: dict[str, dict[str, Any]] = {}
    for group in groups:
        if not isinstance(group, dict):
            raise ValueError("each lifecycle group must be an object")
        owner = group.get("owner")
        status = group.get("status")
        summary = group.get("summary")
        schemas = group.get("schemas")
        if not all(isinstance(value, str) and value for value in (owner, status, summary)):
            raise ValueError("each lifecycle group requires owner, status, and summary")
        if status not in status_definitions:
            raise ValueError(f"unknown lifecycle status {status!r}")
        if not isinstance(schemas, list) or not schemas:
            raise ValueError("each lifecycle group requires a non-empty schemas list")
        for schema in schemas:
            if not isinstance(schema, str) or not schema:
                raise ValueError("schema versions must be non-empty strings")
            if schema in entries:
                raise ValueError(f"schema {schema!r} appears in more than one lifecycle group")
            entries[schema] = {
                "group": group.get("id"),
                "owner": owner,
                "status": status,
                "summary": summary,
            }
    unknown_overrides = sorted(set(overrides) - set(entries))
    if unknown_overrides:
        raise ValueError(
            "evidence overrides reference unknown schemas: " + ", ".join(unknown_overrides)
        )
    for schema, override in overrides.items():
        if not isinstance(override, dict):
            raise ValueError(f"evidence override for {schema!r} must be an object")
        entries[schema].update(override)
    for schema, entry in entries.items():
        if entry["status"] != "removal_candidate":
            continue
        for field in ("persistence", "decision", "rollback", "semantic_review"):
            value = entry.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"removal candidate {schema!r} requires non-empty {field!r} evidence"
                )
        for field in ("tests", "stop_conditions"):
            value = entry.get(field)
            if (
                not isinstance(value, list)
                or not value
                or not all(isinstance(item, str) and item.strip() for item in value)
            ):
                raise ValueError(
                    f"removal candidate {schema!r} requires a non-empty {field!r} list"
                )
        for field in ("producers", "readers"):
            value = entry.get(field)
            if not isinstance(value, list) or not all(
                isinstance(item, str) and item.strip() for item in value
            ):
                raise ValueError(
                    f"removal candidate {schema!r} requires a reviewed {field!r} list"
                )
    return entries


def validate_lifecycle_catalog(
    catalog: Mapping[str, Any],
    registry_versions: Iterable[str],
) -> dict[str, dict[str, Any]]:
    entries = lifecycle_entries(catalog)
    registered = set(registry_versions)
    cataloged = set(entries)
    missing = sorted(registered - cataloged)
    extra = sorted(cataloged - registered)
    if missing or extra:
        parts: list[str] = []
        if missing:
            parts.append("missing registry schemas: " + ", ".join(missing))
        if extra:
            parts.append("unknown catalog schemas: " + ", ".join(extra))
        raise ValueError("; ".join(parts))
    return entries


def inventory_schema_usage(
    root: Path,
    *,
    artifact_roots: Iterable[Path],
    text_roots: Iterable[Path],
    catalog_path: Path,
    exclude_paths: Iterable[Path] = (),
    parquet_workers: int = 1,
) -> dict[str, Any]:
    root = root.resolve()
    resolved_artifact_roots = tuple(path.resolve() for path in artifact_roots)
    resolved_text_roots = tuple(path.resolve() for path in text_roots)
    excluded = {path.resolve() for path in exclude_paths}
    registry_versions = tuple(spec.version for spec in list_table_specs())
    catalog = load_lifecycle_catalog(catalog_path)
    lifecycle = validate_lifecycle_catalog(catalog, registry_versions)
    pattern = _schema_pattern(registry_versions)

    paths: set[Path] = set()
    errors: list[dict[str, str]] = []
    for scan_root in (*resolved_text_roots, *resolved_artifact_roots):
        found, scan_errors = _walk_files(scan_root)
        paths.update(path.resolve() for path in found if path.resolve() not in excluded)
        errors.extend(scan_errors)

    references: dict[str, dict[str, set[str]]] = {
        version: defaultdict(set) for version in registry_versions
    }
    artifact_file_counts: Counter[str] = Counter()
    artifact_row_counts: Counter[str] = Counter()
    artifact_samples: dict[str, list[str]] = defaultdict(list)
    unknown_versions: dict[str, dict[str, Any]] = {}
    unversioned_parquet_files: list[str] = []
    empty_versioned_parquet_files: list[str] = []
    populated_versioned_parquet_files = 0
    text_files_scanned = 0
    parquet_files_scanned = 0

    parquet_paths: list[Path] = []
    for path in sorted(paths, key=lambda value: _repo_path(root, value)):
        display_path = _repo_path(root, path)
        suffix = path.suffix.casefold()
        if suffix == ".parquet" and any(
            _is_within(path, artifact_root) for artifact_root in resolved_artifact_roots
        ):
            parquet_paths.append(path)
            continue
        if suffix not in TEXT_SUFFIXES:
            continue
        try:
            tokens = _schema_tokens(path, pattern)
        except OSError as exc:
            errors.append(
                {
                    "path": display_path,
                    "kind": "text_read_error",
                    "message": f"{type(exc).__name__}: {exc}",
                }
            )
            continue
        text_files_scanned += 1
        surface = _text_surface(root, path, resolved_artifact_roots)
        for version in tokens:
            references[version][surface].add(display_path)

    worker_count = max(1, parquet_workers)
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        for offset in range(0, len(parquet_paths), 512):
            inspected = executor.map(_inspect_parquet, parquet_paths[offset : offset + 512])
            for path, counts, total_rows, column_state, error in inspected:
                display_path = _repo_path(root, path)
                if (
                    error is not None
                    or counts is None
                    or total_rows is None
                    or column_state is None
                ):
                    errors.append(
                        {
                            "path": display_path,
                            "kind": "parquet_read_error",
                            "message": error or "Parquet inspection returned no result",
                        }
                    )
                    continue
                parquet_files_scanned += 1
                if column_state == "absent":
                    unversioned_parquet_files.append(display_path)
                    continue
                if column_state == "present_empty":
                    empty_versioned_parquet_files.append(display_path)
                    continue
                if column_state != "present_with_values":
                    errors.append(
                        {
                            "path": display_path,
                            "kind": "parquet_schema_state_error",
                            "message": f"unexpected schema_version column state {column_state!r}",
                        }
                    )
                    continue
                populated_versioned_parquet_files += 1
                if not counts:
                    errors.append(
                        {
                            "path": display_path,
                            "kind": "parquet_schema_values_missing",
                            "message": "schema_version column has rows but yielded no values",
                        }
                    )
                    continue
                if sum(counts.values()) != total_rows:
                    errors.append(
                        {
                            "path": display_path,
                            "kind": "parquet_count_mismatch",
                            "message": f"counted {sum(counts.values())} of {total_rows} rows",
                        }
                    )
                for version, row_count in sorted(counts.items()):
                    if version not in lifecycle:
                        entry = unknown_versions.setdefault(
                            version,
                            {"file_count": 0, "row_count": 0, "samples": []},
                        )
                        entry["file_count"] += 1
                        entry["row_count"] += row_count
                        if len(entry["samples"]) < 5:
                            entry["samples"].append(display_path)
                        continue
                    artifact_file_counts[version] += 1
                    artifact_row_counts[version] += row_count
                    if len(artifact_samples[version]) < 5:
                        artifact_samples[version].append(display_path)

    schema_reports: dict[str, dict[str, Any]] = {}
    for version in sorted(registry_versions):
        schema_reports[version] = {
            "lifecycle": lifecycle[version],
            "text_references": {
                surface: sorted(reference_paths)
                for surface, reference_paths in sorted(references[version].items())
            },
            "artifact_evidence": {
                "file_count": artifact_file_counts[version],
                "row_count": artifact_row_counts[version],
                "samples": artifact_samples[version],
            },
        }

    normalized_errors = [
        {
            **error,
            "path": _repo_path(root, Path(error["path"])),
        }
        if Path(error["path"]).is_absolute()
        else error
        for error in errors
    ]
    normalized_errors.sort(
        key=lambda item: (item["path"], item["kind"], item["message"])
    )
    attribution_blockers: list[dict[str, Any]] = []
    if normalized_errors:
        attribution_blockers.append(
            {
                "kind": "scan_errors",
                "file_count": len(normalized_errors),
                "message": "One or more declared paths or files could not be attributed safely.",
            }
        )
    if unversioned_parquet_files:
        attribution_blockers.append(
            {
                "kind": "schema_version_column_absent",
                "file_count": len(unversioned_parquet_files),
                "message": (
                    "Parquet without schema_version requires separate semantic classification "
                    "before this scan can support removal evidence."
                ),
            }
        )
    if empty_versioned_parquet_files:
        attribution_blockers.append(
            {
                "kind": "schema_version_column_present_but_empty",
                "file_count": len(empty_versioned_parquet_files),
                "message": (
                    "Empty versioned Parquet proves the column exists but cannot attribute "
                    "the artifact to a concrete version."
                ),
            }
        )
    if unknown_versions:
        attribution_blockers.append(
            {
                "kind": "unknown_schema_versions",
                "version_count": len(unknown_versions),
                "message": "Unknown schema_version values require review before removal evidence.",
            }
        )
    generic_consumers = catalog.get("generic_consumers", [])
    return {
        "report_version": "pmkt.schema_usage_inventory.v2",
        "catalog_version": catalog.get("catalog_version"),
        "scan": {
            "artifact_roots": sorted(_repo_path(root, path) for path in resolved_artifact_roots),
            "text_roots": sorted(_repo_path(root, path) for path in resolved_text_roots),
            "text_files_scanned": text_files_scanned,
            "parquet_files_scanned": parquet_files_scanned,
            "parquet_workers": worker_count,
            "complete": not normalized_errors,
            "text_reference_method": "literal_registered_schema_version_tokens",
        },
        "schemas": schema_reports,
        "unknown_schema_versions": {
            version: unknown_versions[version] for version in sorted(unknown_versions)
        },
        "unversioned_parquet": {
            "file_count": len(unversioned_parquet_files),
            "samples": sorted(unversioned_parquet_files)[:20],
        },
        "empty_versioned_parquet": {
            "file_count": len(empty_versioned_parquet_files),
            "samples": sorted(empty_versioned_parquet_files)[:20],
        },
        "schema_version_column_states": {
            "absent_file_count": len(unversioned_parquet_files),
            "present_empty_file_count": len(empty_versioned_parquet_files),
            "present_with_values_file_count": populated_versioned_parquet_files,
        },
        "semantic_evidence": {
            "generic_consumers": generic_consumers,
            "review_required": True,
            "message": (
                "Literal-token absence does not establish zero semantic producers or readers; "
                "review generic consumers and catalog evidence for each candidate."
            ),
        },
        "removal_evidence": {
            "scanner_is_sufficient": False,
            "artifact_attribution_complete": not attribution_blockers,
            "blockers": attribution_blockers,
        },
        "errors": normalized_errors,
    }


def _paths(root: Path, values: Sequence[str] | None, defaults: Sequence[str]) -> tuple[Path, ...]:
    selected = values if values is not None else defaults
    return tuple(
        (Path(value) if Path(value).is_absolute() else root / value).resolve()
        for value in selected
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a read-only literal-reference and Parquet schema inventory. "
            "The report is evidence for human review and never authorizes artifact deletion."
        )
    )
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--catalog", type=Path, default=None)
    parser.add_argument("--artifact-root", action="append", default=None)
    parser.add_argument("--text-root", action="append", default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument(
        "--parquet-workers",
        type=int,
        default=min(8, os.cpu_count() or 1),
        help="Bounded worker count for read-only Parquet metadata inspection.",
    )
    parser.add_argument(
        "--allow-incomplete",
        action="store_true",
        help=(
            "Return success despite scan or artifact-attribution blockers; the report "
            "still records why it is incomplete for removal evidence."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = args.root.resolve()
    catalog_path = (
        args.catalog.resolve()
        if args.catalog is not None
        else (root / DEFAULT_CATALOG).resolve()
    )
    output = args.output.resolve() if args.output is not None else None
    report = inventory_schema_usage(
        root,
        artifact_roots=_paths(root, args.artifact_root, DEFAULT_ARTIFACT_ROOTS),
        text_roots=_paths(root, args.text_root, DEFAULT_TEXT_ROOTS),
        catalog_path=catalog_path,
        exclude_paths=(() if output is None else (output,)),
        parquet_workers=args.parquet_workers,
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if output is None:
        sys.stdout.write(rendered)
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8")
    if (
        report["errors"]
        or not report["removal_evidence"]["artifact_attribution_complete"]
    ) and not args.allow_incomplete:
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
