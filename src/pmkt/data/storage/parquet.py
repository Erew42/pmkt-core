from __future__ import annotations

from pathlib import Path
from typing import Any


def write_parquet(
    df: Any,
    path: Path,
    *,
    overwrite: bool = True,
    schema: str | None = None,
    validate: bool = False,
    coerce: bool = False,
    strict: bool = False,
) -> Path:
    """Write a DataFrame to parquet. Requires pyarrow."""
    if not overwrite and path.exists():
        raise FileExistsError(f"{path} already exists")
    if schema is not None:
        from pmkt.data.validation import coerce_frame, validate_frame

        if coerce:
            df = coerce_frame(df, schema)
        if validate or strict:
            report = validate_frame(df, schema, strict=strict)
            if not report.ok:
                raise ValueError("; ".join(report.errors))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def read_parquet(path: Path):
    """Read a parquet file into a DataFrame. Requires pyarrow."""
    import pandas as pd

    return pd.read_parquet(path)
