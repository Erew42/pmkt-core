from __future__ import annotations

import contextlib
import os
import tempfile
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
    """Atomically publish a DataFrame as parquet. Requires pyarrow."""
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
    descriptor, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    os.close(descriptor)
    temp_path = Path(temp_name)
    try:
        df.to_parquet(temp_path, index=False)
        with temp_path.open("r+b") as handle:
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        with contextlib.suppress(OSError):
            temp_path.unlink(missing_ok=True)
    return path


def read_parquet(path: Path):
    """Read a parquet file into a DataFrame. Requires pyarrow."""
    import pandas as pd

    return pd.read_parquet(path)
