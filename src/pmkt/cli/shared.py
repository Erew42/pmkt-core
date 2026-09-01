from __future__ import annotations

from pathlib import Path
from typing import NoReturn

import typer

from pmkt.data.storage.duckdb import validate_view_name


def error_exit(message: str) -> NoReturn:
    print(f"Error: {message}", flush=True)
    raise typer.Exit(code=1)


def parse_dataset_options(dataset_options: list[str] | None) -> dict[str, Path]:
    datasets: dict[str, Path] = {}
    for option in dataset_options or []:
        if "=" not in option:
            error_exit(f"dataset must use name=path format, got {option!r}")
        name, path = option.split("=", 1)
        name = name.strip()
        path = path.strip()
        if not name or not path:
            error_exit(f"dataset must use name=path format, got {option!r}")
        try:
            validate_view_name(name)
        except ValueError as exc:
            error_exit(str(exc))
        datasets[name] = Path(path)
    if not datasets:
        error_exit("query requires at least one --dataset name=path")
    return datasets


def format_columns(df) -> str:
    columns = [str(column) for column in getattr(df, "columns", [])]
    return ", ".join(columns) if columns else "none"


def required_column(df, candidates: tuple[str, ...], *, path: Path, label: str) -> str:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    expected = "/".join(candidates)
    error_exit(
        f"{label} parquet {path} is missing {expected} column "
        f"(available columns: {format_columns(df)})"
    )


def unique_nonempty_strings(values) -> list[str]:
    import pandas as pd

    unique: list[str] = []
    for value in values:
        try:
            if pd.isna(value):
                continue
        except (TypeError, ValueError):
            pass
        text = str(value).strip()
        if text and text not in unique:
            unique.append(text)
    return unique
