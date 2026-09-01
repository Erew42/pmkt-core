from __future__ import annotations

from typing import Annotated, Optional

import typer

from pmkt.cli.shared import error_exit, parse_dataset_options
from pmkt.data.storage.duckdb import query_parquet


def query_cmd(
    sql: Annotated[str, typer.Argument(help="SQL query to run against registered Parquet views.")],
    dataset: Annotated[
        Optional[list[str]],
        typer.Option(
            "--dataset",
            "-d",
            help="Register a Parquet view as name=path_or_glob. Repeat for multiple views.",
        ),
    ] = None,
    limit: Annotated[
        Optional[int],
        typer.Option(help="Limit rows printed to the terminal; use 0 for no limit."),
    ] = 100,
) -> None:
    """Run DuckDB SQL over Parquet datasets."""
    datasets = parse_dataset_options(dataset)
    try:
        df = query_parquet(sql, datasets)
    except Exception as exc:
        error_exit(f"query failed: {exc}")
    if limit and limit > 0:
        df = df.head(limit)
    print(df.to_string(index=False))
