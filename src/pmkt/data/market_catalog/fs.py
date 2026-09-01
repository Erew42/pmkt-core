"""Catalog filesystem, hashing, timestamp, and Parquet helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any


from .types import CatalogError


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def parse_timestamp(value: Any) -> datetime | None:
    """Parse venue timestamp variants without converting malformed values."""
    if value is None or value == "" or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric > 10_000_000_000:
            numeric /= 1000.0
        try:
            return datetime.fromtimestamp(numeric, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def row_timestamp(row: Mapping[str, Any], keys: Sequence[str]) -> datetime | None:
    for key in keys:
        parsed = parse_timestamp(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _raw_key(row: Mapping[str, Any], venue: str) -> str:
    keys = ("id", "market_id") if venue == "polymarket" else ("ticker", "market_key")
    for key in keys:
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _payload_hash(row: Mapping[str, Any]) -> str:
    payload = json.dumps(dict(row), ensure_ascii=True, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parquet_files(path: Path) -> list[Path]:
    if path.is_file() and path.suffix.casefold() == ".parquet":
        return [path]
    return sorted(item for item in path.rglob("*.parquet") if item.is_file())


def parquet_row_count(path: Path) -> int:
    import pyarrow.parquet as pq

    return sum(
        int(pq.ParquetFile(item).metadata.num_rows) for item in parquet_files(path)
    )


def tree_sha256(path: Path) -> str:
    files = parquet_files(path)
    if not files:
        raise CatalogError(f"no Parquet files found under {path}")
    digest = hashlib.sha256()
    for item in files:
        relative = item.name if path.is_file() else item.relative_to(path).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(bytes.fromhex(sha256_file(item)))
    return digest.hexdigest()


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(dict(value), indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise CatalogError(f"expected JSON object at {path}")
    return value


def _repository_root(market_root: Path) -> Path:
    root = market_root.resolve()
    if root.name == "markets" and root.parent.name == "data":
        return root.parent.parent
    return root.parent


def _stored_path(path: Path, *, repository_root: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(repository_root.resolve()).as_posix()
    except ValueError:
        return resolved.as_posix()


def _resolve_stored_path(value: str, *, repository_root: Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else repository_root / path


def _artifact(
    path: Path, *, repository_root: Path, rows: int, schema: str
) -> dict[str, Any]:
    files = parquet_files(path)
    if not files:
        raise CatalogError(f"artifact contains no Parquet: {path}")
    return {
        "format": "parquet" if path.is_file() else "partitioned_parquet",
        "parquet_file_count": len(files),
        "path": _stored_path(path, repository_root=repository_root),
        "rows": int(rows),
        "schema": schema,
        "sha256": sha256_file(path) if path.is_file() else None,
        "tree_sha256": tree_sha256(path) if not path.is_file() else None,
        "size_bytes": sum(item.stat().st_size for item in files),
    }


def _artifact_from_staged(
    staged_path: Path,
    *,
    final_path: Path,
    repository_root: Path,
    rows: int,
    schema: str,
    as_of_utc: str | None = None,
) -> dict[str, Any]:
    artifact = _artifact(
        staged_path,
        repository_root=repository_root,
        rows=rows,
        schema=schema,
    )
    artifact["path"] = _stored_path(final_path, repository_root=repository_root)
    if as_of_utc is not None:
        artifact["as_of_utc"] = as_of_utc
    return artifact


def _quote_sql(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _parquet_sql(path: Path, *, filename: bool = False) -> str:
    files = parquet_files(path)
    if not files:
        raise CatalogError(f"no readable Parquet files at {path}")
    paths = ", ".join(_quote_sql(item.resolve().as_posix()) for item in files)
    filename_option = ", filename=true" if filename else ""
    return (
        f"read_parquet([{paths}], union_by_name=true, hive_partitioning=false"
        f"{filename_option})"
    )


def _parquet_paths_sql(paths: Sequence[Path]) -> str:
    files = [item for path in paths for item in parquet_files(path)]
    if not files:
        raise CatalogError("no readable Parquet files in artifact set")
    literals = ", ".join(_quote_sql(item.resolve().as_posix()) for item in files)
    return f"read_parquet([{literals}], union_by_name=true, hive_partitioning=false)"


def _run_id(prefix: str) -> str:
    stamp = utc_now().strftime("%Y%m%dT%H%M%S%fZ")
    return f"{prefix}_{stamp}"


def _timestamp_range(values: Iterable[datetime | None]) -> dict[str, str | None]:
    usable = [value for value in values if value is not None]
    return {
        "minimum_utc": iso_utc(min(usable)) if usable else None,
        "maximum_utc": iso_utc(max(usable)) if usable else None,
    }


def _hardlink_artifact(source: Path, target: Path) -> int:
    """Hard-link immutable Parquet content without linking mutable manifests."""
    count = 0
    if source.is_file():
        destination = target / "source=parent" / "part-000000.parquet"
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.link(source, destination)
        return 1
    for item in parquet_files(source):
        destination = target / item.relative_to(source)
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.link(item, destination)
        count += 1
    return count


def _profile_history_artifact(path: Path, *, key_column: str) -> dict[str, int]:
    import duckdb

    with duckdb.connect(database=":memory:") as connection:
        row = connection.execute(
            f"""
            SELECT count(*), count(DISTINCT {key_column}),
                   count(*) FILTER (WHERE {key_column} IS NULL),
                   count(*) FILTER (WHERE raw_json_sha256 IS NULL)
            FROM {_parquet_sql(path)}
            """
        ).fetchone()
    assert row is not None
    return {
        "row_count": int(row[0]),
        "distinct_key_count": int(row[1]),
        "missing_key_count": int(row[2]),
        "missing_payload_hash_count": int(row[3]),
        "duplicate_key_count": int(row[0]) - int(row[1]),
    }
