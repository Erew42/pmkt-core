from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer

from pmkt.data.features import compute_features, join_market_metadata
from pmkt.data.storage.parquet import read_parquet, write_parquet


def compute_features_cmd(
    books: Annotated[Path, typer.Option(help="Book summaries parquet path.")],
    out: Annotated[Path, typer.Option(help="Output parquet path.")],
    markets: Annotated[Optional[Path], typer.Option(help="Markets parquet path.")] = None,
    bar_seconds: Annotated[int, typer.Option(help="Bar size in seconds.")] = 60,
    window_seconds: Annotated[
        int, typer.Option(help="Rolling window size in seconds.")
    ] = 600,
):
    """Compute feature store parquet."""
    book_df = read_parquet(books)
    features_df = compute_features(
        book_df,
        bar_seconds=bar_seconds,
        window_seconds=window_seconds,
    )
    if markets:
        markets_df = read_parquet(markets)
        features_df = join_market_metadata(features_df, markets_df)

    path = write_parquet(features_df, out, overwrite=True)
    print(f"Wrote {len(features_df)} feature rows to {path}")
