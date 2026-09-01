from __future__ import annotations

import glob
import re
from pathlib import Path
from typing import Mapping, Sequence

ParquetPath = str | Path | Sequence[str | Path]

_VIEW_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _import_duckdb():
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError(
            "DuckDB support requires the 'duckdb' package. Install pmkt with its "
            "runtime dependencies before using Parquet SQL queries."
        ) from exc
    return duckdb


def _quote_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _path_text(path: str | Path) -> str:
    text = str(path)
    if not text.strip():
        raise ValueError("Parquet path must not be empty")
    return text


def _validate_parquet_path(path: str | Path) -> str:
    text = _path_text(path)
    expanded = str(Path(text).expanduser())
    if glob.has_magic(expanded):
        if not glob.glob(expanded):
            raise FileNotFoundError(f"Parquet path pattern matched no files: {text}")
        return expanded

    candidate = Path(expanded)
    if not candidate.exists():
        raise FileNotFoundError(f"Parquet path does not exist: {text}")
    return str(candidate)


def _read_parquet_expr(path_or_paths: ParquetPath) -> str:
    if isinstance(path_or_paths, (str, Path)):
        return _quote_string(_validate_parquet_path(path_or_paths))
    paths = [_quote_string(_validate_parquet_path(path)) for path in path_or_paths]
    if not paths:
        raise ValueError("Parquet path list must not be empty")
    return "[" + ", ".join(paths) + "]"


def validate_view_name(name: str) -> str:
    if not _VIEW_NAME_PATTERN.fullmatch(name):
        raise ValueError(
            f"Invalid DuckDB view name {name!r}; use letters, numbers, and underscores."
        )
    return name


def connect(database: str | Path | None = None):
    duckdb = _import_duckdb()
    return duckdb.connect(":memory:" if database is None else str(database))


def register_parquet_view(
    connection,
    name: str,
    path_or_paths: ParquetPath,
    *,
    union_by_name: bool = True,
) -> None:
    view_name = validate_view_name(name)
    read_expr = _read_parquet_expr(path_or_paths)
    union_sql = "true" if union_by_name else "false"
    connection.execute(
        f"CREATE OR REPLACE VIEW {view_name} AS "
        f"SELECT * FROM read_parquet({read_expr}, union_by_name={union_sql})"
    )


def register_parquet_views(
    connection,
    datasets: Mapping[str, ParquetPath],
    *,
    union_by_name: bool = True,
) -> None:
    if not datasets:
        raise ValueError("At least one Parquet dataset is required")
    for name, path_or_paths in datasets.items():
        register_parquet_view(
            connection,
            name,
            path_or_paths,
            union_by_name=union_by_name,
        )


def query_parquet(
    sql: str,
    datasets: Mapping[str, ParquetPath],
    *,
    database: str | Path | None = None,
    union_by_name: bool = True,
):
    with connect(database) as connection:
        register_parquet_views(connection, datasets, union_by_name=union_by_name)
        return connection.execute(sql).df()


def describe_parquet(path_or_paths: ParquetPath):
    with connect() as connection:
        read_expr = _read_parquet_expr(path_or_paths)
        return connection.execute(f"DESCRIBE SELECT * FROM read_parquet({read_expr})").df()
