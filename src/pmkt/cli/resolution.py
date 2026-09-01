from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Annotated

import typer

from pmkt.resolution.cache import resolve_market_resolution_cache


def resolve_market_resolutions_cmd(
    polymarket_markets: Annotated[
        Path,
        typer.Option(
            "--polymarket-markets",
            exists=True,
            file_okay=True,
            dir_okay=True,
            readable=True,
            help="Path to Polymarket market snapshot parquet file or dataset directory.",
        ),
    ],
    kalshi_markets: Annotated[
        Path,
        typer.Option(
            "--kalshi-markets",
            exists=True,
            file_okay=True,
            dir_okay=True,
            readable=True,
            help="Path to Kalshi market snapshot parquet file or dataset directory.",
        ),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            file_okay=False,
            dir_okay=True,
            writable=True,
            help="Directory for market_resolutions.parquet and summary.json.",
        ),
    ],
    matches: Annotated[
        Path | None,
        typer.Option(
            "--matches",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            help="Optional match parquet to restrict resolution to matched markets.",
        ),
    ] = None,
    polygon_rpc_url: Annotated[
        str | None,
        typer.Option(
            "--polygon-rpc-url",
            help="Optional Polygon JSON-RPC URL for canonical CTF payout reads.",
        ),
    ] = None,
    concurrency: Annotated[
        int,
        typer.Option("--concurrency", min=1, help="Maximum concurrent resolver tasks."),
    ] = 8,
    refresh: Annotated[
        bool,
        typer.Option("--refresh", help="Re-resolve markets even if a final canonical cache row exists."),
    ] = False,
    max_markets: Annotated[
        int | None,
        typer.Option("--max-markets", min=1, help="Limit markets per platform for tests."),
    ] = None,
) -> None:
    """Resolve final market outcomes into canonical cache rows."""

    summary = asyncio.run(
        resolve_market_resolution_cache(
            polymarket_markets_path=polymarket_markets,
            kalshi_markets_path=kalshi_markets,
            output_dir=output_dir,
            matches_path=matches,
            polygon_rpc_url=polygon_rpc_url,
            concurrency=concurrency,
            refresh=refresh,
            max_markets=max_markets,
        )
    )
    typer.echo(json.dumps(summary, indent=2, sort_keys=True))


__all__ = ["resolve_market_resolutions_cmd"]
