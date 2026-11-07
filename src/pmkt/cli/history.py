from __future__ import annotations

from pmkt.config import PmktConfig

import asyncio
from collections.abc import Iterable
from pathlib import Path
from typing import Annotated, Any, Optional

import pandas as pd
import typer

from pmkt.data.historical_books import (
    DEFAULT_HISTORY_INTERVAL,
    DEFAULT_HISTORY_PERIOD_INTERVAL_MINUTES,
    DEFAULT_HISTORY_USE_HISTORICAL,
    DEFAULT_HISTORY_INCLUDE_TRADES,
    DEFAULT_HISTORY_MAX_TRADE_PAGES,
    DEFAULT_HISTORY_TRADE_LIMIT,
    backfill_venue_history,
    write_historical_backfill_result,
)
from pmkt.data.recorder import record_topbooks
from pmkt.data.storage.parquet import read_parquet
from pmkt.exchanges.kalshi.client import AsyncKalshiClient
from pmkt.exchanges.polymarket.clob import AsyncClobClient


def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    raise typer.BadParameter(f"unsupported table format for {path}")


def _unique_text(values: Iterable[object]) -> list[str]:
    result: list[str] = []
    for value in values:
        text = str(value).strip()
        if text and text not in result:
            result.append(text)
    return result


def _instrument_list(values: Iterable[str] | None, path: Path | None) -> list[str]:
    instruments = _unique_text(values or ())
    if path is None:
        return instruments
    if path.suffix.lower() not in {".parquet", ".csv"}:
        return _unique_text(
            [*instruments, *path.read_text(encoding="utf-8").splitlines()]
        )
    frame = _read_table(path)
    for column in (
        "instrument_id",
        "polymarket_token_id",
        "token_id",
        "ticker",
        "market_ticker",
        "kalshi_market_key",
    ):
        if column in frame.columns:
            return _unique_text([*instruments, *frame[column].dropna().tolist()])
    raise typer.BadParameter(
        "instrument file must contain an instrument, token, or ticker column"
    )


def backfill_venue_history_cmd(
    venue: Annotated[str, typer.Option("--venue", help="polymarket or kalshi")],
    instrument: Annotated[
        Optional[list[str]],
        typer.Option("--instrument", "-i", help="Repeatable venue instrument id."),
    ] = None,
    instruments_file: Annotated[
        Optional[Path],
        typer.Option("--instruments-file", help="Newline, CSV, or parquet list."),
    ] = None,
    out_dir: Annotated[Path, typer.Option("--out-dir")] = Path(
        "generated/historical_backfill"
    ),
    start_ts: Annotated[Optional[int], typer.Option("--start-ts")] = None,
    end_ts: Annotated[Optional[int], typer.Option("--end-ts")] = None,
    interval: Annotated[
        Optional[str],
        typer.Option(
            "--interval",
            help="Polymarket history time span (venue interval selector).",
            show_default=str(DEFAULT_HISTORY_INTERVAL),
        ),
    ] = None,
    # Polymarket CLOB OpenAPI /prices-history defines fidelity in minutes:
    # https://docs.polymarket.com/api-spec/clob-openapi.yaml (2026-11-07).
    fidelity: Annotated[
        Optional[int],
        typer.Option(
            "--fidelity",
            help="Polymarket sampling resolution in minutes.",
        ),
    ] = None,
    period_interval: Annotated[
        Optional[int],
        typer.Option(
            "--period-interval",
            help="Kalshi candlestick period in minutes.",
            show_default=str(DEFAULT_HISTORY_PERIOD_INTERVAL_MINUTES),
        ),
    ] = None,
    series_ticker: Annotated[Optional[str], typer.Option("--series-ticker")] = None,
    historical: Annotated[
        Optional[bool],
        typer.Option(
            "--historical/--live",
            show_default=str(DEFAULT_HISTORY_USE_HISTORICAL),
        ),
    ] = None,
    include_trades: Annotated[
        Optional[bool],
        typer.Option(
            "--include-trades/--skip-trades",
            show_default=str(DEFAULT_HISTORY_INCLUDE_TRADES),
        ),
    ] = None,
    max_trade_pages: Annotated[
        Optional[int],
        typer.Option(
            "--max-trade-pages",
            show_default=str(DEFAULT_HISTORY_MAX_TRADE_PAGES),
        ),
    ] = None,
    trade_limit: Annotated[
        Optional[int],
        typer.Option(
            "--trade-limit",
            show_default=str(DEFAULT_HISTORY_TRADE_LIMIT),
        ),
    ] = None,
    run_id: Annotated[Optional[str], typer.Option("--run-id")] = None,
) -> None:
    """Backfill documented historical prices, candles, trades, and gaps."""
    instruments = _instrument_list(instrument, instruments_file)
    if not instruments:
        raise typer.BadParameter("provide --instrument or --instruments-file")
    normalized_venue = venue.strip().lower()
    common_overrides: dict[str, Any] = {
        key: value for key, value in {
            "start_ts": start_ts, "end_ts": end_ts, "run_id": run_id,
        }.items() if value is not None
    }
    polymarket_overrides: dict[str, Any] = {
        key: value for key, value in {"interval": interval, "fidelity": fidelity}.items()
        if value is not None
    }
    kalshi_overrides: dict[str, Any] = {
        key: value for key, value in {
            "period_interval": period_interval, "series_ticker": series_ticker,
            "use_historical": historical, "include_trades": include_trades,
            "max_trade_pages": max_trade_pages, "trade_limit": trade_limit,
        }.items() if value is not None
    }

    async def run():
        if normalized_venue == "polymarket":
            async with AsyncClobClient( config=PmktConfig.from_env()) as client:
                return await backfill_venue_history(
                    normalized_venue,
                    instruments,
                    **common_overrides,
                    **polymarket_overrides,
                    polymarket_client=client,
                )
        if normalized_venue != "kalshi":
            raise typer.BadParameter("--venue must be polymarket or kalshi")
        async with AsyncKalshiClient( config=PmktConfig.from_env()) as client:
            return await backfill_venue_history(
                normalized_venue,
                instruments,
                **common_overrides,
                **kalshi_overrides,
                kalshi_client=client,
            )

    result = asyncio.run(run())
    paths = write_historical_backfill_result(result, out_dir)
    typer.echo(
        f"Historical backfill wrote {result.summary['historical_price_rows']} price "
        f"rows, {result.summary['trade_rows']} trade rows, and "
        f"{result.summary['gap_rows']} gap rows to {out_dir}"
    )
    for label, path in paths.items():
        typer.echo(f"{label}: {path}")


def record_topbooks_cmd(
    relations: Annotated[Path, typer.Option("--relations")],
    out_dir: Annotated[Path, typer.Option("--out-dir")] = Path(
        "generated/topbook_recordings"
    ),
    duration: Annotated[Optional[float], typer.Option("--duration")] = 60.0,
    interval: Annotated[float, typer.Option("--interval")] = 1.0,
    max_cycles: Annotated[Optional[int], typer.Option("--max-cycles")] = None,
    depth: Annotated[Optional[int], typer.Option("--depth")] = None,
    max_stale_seconds: Annotated[
        Optional[float], typer.Option("--max-stale-seconds")
    ] = 30.0,
    run_id: Annotated[Optional[str], typer.Option("--run-id")] = None,
) -> None:
    """Record public topbooks for instrument relations supplied as data."""

    async def run():
        async with AsyncClobClient( config=PmktConfig.from_env()) as polymarket, AsyncKalshiClient( config=PmktConfig.from_env()) as kalshi:
            return await record_topbooks(
                _read_table(relations),
                polymarket_client=polymarket,
                kalshi_client=kalshi,
                out_dir=out_dir,
                run_id=run_id,
                interval_s=interval,
                duration_s=duration,
                max_cycles=max_cycles,
                depth=depth,
                max_stale_seconds=(
                    None
                    if max_stale_seconds is not None and max_stale_seconds < 0
                    else max_stale_seconds
                ),
            )

    result = asyncio.run(run())
    typer.echo(
        f"Recorded {result.topbook_rows} topbook rows, {result.depth_rows} depth "
        f"rows, and {result.gap_rows} gap rows across {result.cycles} cycles to "
        f"{result.output_dir}"
    )


__all__ = ["backfill_venue_history_cmd", "record_topbooks_cmd"]
