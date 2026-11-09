from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any, Optional

import typer

from pmkt.data.features import (
    DEFAULT_FEATURE_BAR_SECONDS,
    DEFAULT_FEATURE_WINDOW_SECONDS,
    compute_features,
    join_market_metadata,
)
from pmkt.data.storage.parquet import read_parquet, write_parquet


def compute_features_cmd(
    books: Annotated[Path, typer.Option(help="Book summaries parquet path.")],
    out: Annotated[Path, typer.Option(help="Output parquet path.")],
    markets: Annotated[Optional[Path], typer.Option(help="Markets parquet path.")] = None,
    bar_seconds: Annotated[
        Optional[int],
        typer.Option(help="Bar size in seconds.", show_default=str(DEFAULT_FEATURE_BAR_SECONDS)),
    ] = None,
    window_seconds: Annotated[
        Optional[int],
        typer.Option(
            help="Rolling window size in seconds.",
            show_default=str(DEFAULT_FEATURE_WINDOW_SECONDS),
        ),
    ] = None,
):
    """Compute feature store parquet."""
    book_df = read_parquet(books)
    overrides: dict[str, Any] = {}
    if bar_seconds is not None:
        overrides["bar_seconds"] = bar_seconds
    if window_seconds is not None:
        overrides["window_seconds"] = window_seconds
    features_df = compute_features(book_df, **overrides)
    if markets:
        markets_df = read_parquet(markets)
        features_df = join_market_metadata(features_df, markets_df)

    path = write_parquet(features_df, out, overwrite=True)
    print(f"Wrote {len(features_df)} feature rows to {path}")
