from __future__ import annotations

from pmkt.config import PmktConfig

import asyncio
import importlib
import sys
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Optional

import pandas as pd
import typer

from pmkt.tokens import flatten_token_ids
from pmkt.exchanges.polymarket.clob import AsyncClobClient
from pmkt.exchanges.read_auth import (
    ReadAuthHeaderProvider,
)
from pmkt.exchanges.kalshi.client import AsyncKalshiClient
from pmkt.data.manifests import (
    build_run_manifest,
    count_quality_flags,
    current_git_commit,
    write_manifest,
)
from pmkt.data.market_data import (
    DEFAULT_BOOK_BATCH_SIZE,
    collect_order_book_summaries_parquet,
    collect_order_book_topbooks_parquet,
)
from pmkt.data.normalize_books import kalshi_orderbook_to_topbook
from pmkt.data.schemas import TOPBOOK_COLUMNS
from pmkt.data.storage.parquet import read_parquet, write_parquet
from pmkt.exchanges.ws_transport import (
    WS_MAX_QUEUE_FRAMES,
    WS_MAX_SIZE_BYTES,
)

from pmkt.cli.shared import error_exit, required_column, unique_nonempty_strings


class BookOutputFormat(str, Enum):
    LEGACY_SUMMARY = "legacy-summary"
    TOPBOOK = "topbook"


DEFAULT_ORDER_BOOK_STREAM_ROOT = Path("generated/order_book_streams")
DEFAULT_KALSHI_ORDER_BOOK_STREAM_ROOT = Path("generated/kalshi_order_book_streams")


def _load_stream_collector(module_name: str, attribute_name: str) -> Any:
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        missing_root = (exc.name or "").split(".", 1)[0]
        if missing_root == "websockets":
            error_exit(
                "WebSocket streaming requires the streaming extra; "
                "install pmkt[streaming] and retry"
            )
        raise
    return getattr(module, attribute_name)


async def _lazy_stream_order_book_data(*args: Any, **kwargs: Any) -> dict[str, Any]:
    collector = _load_stream_collector(
        "pmkt.exchanges.polymarket.order_book_stream", "stream_order_book_data"
    )
    kwargs.setdefault("ws_url", PmktConfig.from_env().clob_ws_url)
    return await collector(*args, **kwargs)


async def _lazy_stream_kalshi_order_book_data(
    *args: Any, **kwargs: Any
) -> dict[str, Any]:
    collector = _load_stream_collector(
        "pmkt.exchanges.kalshi.order_book_stream", "stream_kalshi_order_book_data"
    )
    kwargs.setdefault("ws_url", PmktConfig.from_env().resolved_kalshi_ws_url)
    return await collector(*args, **kwargs)


# Keep these module-level seams stable for callers and tests that replace collectors.
stream_order_book_data = _lazy_stream_order_book_data
stream_kalshi_order_book_data = _lazy_stream_kalshi_order_book_data














def _filter_markets(df, *, min_volume: float, min_liquidity: float):
    if "closed" in df.columns:
        df = df[df["closed"] == False]  # noqa: E712
    if "enable_orderbook" in df.columns:
        df = df[df["enable_orderbook"] == True]  # noqa: E712
    if min_volume > 0 and "volume" in df.columns:
        df = df[df["volume"].fillna(0) >= min_volume]
    if min_liquidity > 0 and "liquidity" in df.columns:
        df = df[df["liquidity"].fillna(0) >= min_liquidity]
    return df


def _token_ids_from_markets_df(markets_df, *, path: Path) -> list[str]:
    column = required_column(markets_df, ("token_ids",), path=path, label="markets")
    tokens: list[str] = []
    for value in markets_df[column].tolist():
        tokens.extend(flatten_token_ids(value))
    return unique_nonempty_strings(tokens)


def _tokens_from_markets_parquet(markets_path: Path) -> list[str]:
    markets_df = read_parquet(markets_path)
    return _token_ids_from_markets_df(markets_df, path=markets_path)




def _tickers_from_kalshi_markets_parquet(markets_path: Path) -> list[str]:
    markets_df = read_parquet(markets_path)
    ticker_col = required_column(
        markets_df,
        ("market_key", "ticker", "market_ticker"),
        path=markets_path,
        label="Kalshi markets",
    )
    return unique_nonempty_strings(markets_df[ticker_col])


def _current_command() -> str:
    return " ".join(str(arg) for arg in sys.argv if str(arg).strip())


def _load_read_auth_header_provider(spec: str | None) -> ReadAuthHeaderProvider:
    if spec is None:
        raise typer.BadParameter(
            "Kalshi websocket capture requires --header-provider MODULE:ATTRIBUTE",
            param_hint="--header-provider",
        )
    module_name, separator, attribute_name = spec.partition(":")
    if not separator or not module_name or not attribute_name:
        raise typer.BadParameter(
            "must use MODULE:ATTRIBUTE syntax",
            param_hint="--header-provider",
        )
    try:
        candidate = getattr(importlib.import_module(module_name), attribute_name)
    except (AttributeError, ImportError) as exc:
        raise typer.BadParameter(
            f"could not import read-auth header provider {spec!r}: {exc}",
            param_hint="--header-provider",
        ) from exc
    try:
        provider = (
            candidate()
            if callable(candidate) and not hasattr(candidate, "headers_for_get")
            else candidate
        )
    except Exception as exc:
        raise typer.BadParameter(
            f"could not initialize read-auth header provider {spec!r}: {exc}",
            param_hint="--header-provider",
        ) from exc
    if not callable(getattr(provider, "headers_for_get", None)):
        raise typer.BadParameter(
            f"{spec!r} does not provide a headers_for_get(path) method",
            param_hint="--header-provider",
        )
    return provider


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _default_one_shot_run_id(venue: str) -> str:
    stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{venue}-one-shot-{stamp}"


def _write_one_shot_topbook_manifest(
    manifest_out: Path,
    *,
    output_path: Path,
    run_id: str,
    started_at_utc: str,
    ended_at_utc: str,
    venue: str,
    topbooks: pd.DataFrame,
) -> Path:
    records = topbooks.to_dict("records")
    venue_counts = _value_counts(topbooks, "exchange")
    instrument_counts = _value_counts(topbooks, "instrument_id")
    manifest = build_run_manifest(
        run_id=run_id,
        run_dir=output_path.parent,
        started_at_utc=started_at_utc,
        ended_at_utc=ended_at_utc,
        status="success",
        command=_current_command(),
        dataset_paths={"topbook": str(output_path)},
        schema_versions={"topbook": "topbook.v1"},
        row_counts={"topbook": int(len(topbooks))},
        quality_flag_counts=count_quality_flags(records),
        venue_counts=venue_counts,
        instrument_counts=instrument_counts,
        git_commit=current_git_commit(Path.cwd()),
        extra={
            "collection_type": "one_shot_rest",
            "output_format": BookOutputFormat.TOPBOOK.value,
            "venue": venue,
        },
    )
    return write_manifest(manifest_out, manifest)


def _value_counts(df: pd.DataFrame, column: str) -> dict[str, int]:
    if column not in df.columns:
        return {}
    counts = df[column].dropna().astype(str).value_counts()
    return {key: int(value) for key, value in counts.items()}


async def _collect_books_async(
    markets_path: Path,
    out: Path,
    poll: float,
    duration: float,
    max_snapshots: int | None,
    min_volume: float,
    min_liquidity: float,
    max_tokens: int | None,
    also_jsonl_dir: Path | None,
    batch_size: int | None,
    allow_missing: bool,
    output_format: BookOutputFormat,
    manifest_out: Path | None,
    run_id: str | None,
) -> None:
    markets_df = read_parquet(markets_path)
    markets_df = _filter_markets(
        markets_df, min_volume=min_volume, min_liquidity=min_liquidity
    )

    token_ids = _token_ids_from_markets_df(markets_df, path=markets_path)
    if max_tokens is not None:
        token_ids = token_ids[:max_tokens]
    if not token_ids:
        error_exit("no token ids found after filtering")

    if output_format is BookOutputFormat.TOPBOOK:
        if also_jsonl_dir is not None:
            error_exit(
                "--also-jsonl-dir is only supported with --output-format legacy-summary"
            )
        resolved_run_id = run_id or _default_one_shot_run_id("polymarket")
        started_at_utc = _utc_now_iso()
        async with AsyncClobClient( config=PmktConfig.from_env()) as clob:
            path = await collect_order_book_topbooks_parquet(
                clob,
                token_ids,
                output_path=out,
                poll_interval_s=poll,
                duration_s=duration,
                max_snapshots=max_snapshots,
                batch_size=batch_size,
                collector_run_id=resolved_run_id,
                allow_missing_tokens=allow_missing,
            )
        ended_at_utc = _utc_now_iso()
        topbooks = read_parquet(path)
        if manifest_out is not None:
            _write_one_shot_topbook_manifest(
                manifest_out,
                output_path=path,
                run_id=resolved_run_id,
                started_at_utc=started_at_utc,
                ended_at_utc=ended_at_utc,
                venue="polymarket",
                topbooks=topbooks,
            )
        print(f"Wrote {len(topbooks)} Polymarket topbook.v1 rows to {path}")
        return

    if manifest_out is not None:
        error_exit("--manifest-out requires --output-format topbook")

    async with AsyncClobClient( config=PmktConfig.from_env()) as clob:
        path = await collect_order_book_summaries_parquet(
            clob,
            token_ids,
            output_path=out,
            poll_interval_s=poll,
            duration_s=duration,
            max_snapshots=max_snapshots,
            also_jsonl_dir=also_jsonl_dir,
            batch_size=batch_size,
        )
    print(f"Wrote {len(token_ids)} token summaries to {path}")


def collect_books(
    markets: Annotated[Path, typer.Option(help="Markets parquet path.")],
    out: Annotated[Path, typer.Option(help="Output parquet path.")],
    poll: Annotated[float, typer.Option(help="Polling interval in seconds.")] = 1.0,
    duration: Annotated[
        float, typer.Option(help="Collection duration in seconds.")
    ] = 300.0,
    max_snapshots: Annotated[
        Optional[int], typer.Option(help="Optional max snapshots to collect.")
    ] = None,
    min_volume: Annotated[float, typer.Option(help="Min volume filter.")] = 0.0,
    min_liquidity: Annotated[float, typer.Option(help="Min liquidity filter.")] = 0.0,
    max_tokens: Annotated[
        Optional[int], typer.Option(help="Limit number of tokens to collect.")
    ] = None,
    also_jsonl_dir: Annotated[
        Optional[Path],
        typer.Option(help="Optional directory to write raw JSONL snapshots."),
    ] = None,
    batch_size: Annotated[
        Optional[int],
        typer.Option(
            help=(
                "Polymarket /books batch size. Use 1 to force single-token /book polling."
            )
        ),
    ] = DEFAULT_BOOK_BATCH_SIZE,
    allow_missing: Annotated[
        bool,
        typer.Option(
            "--allow-missing",
            help=(
                "For canonical topbook output, write observed Polymarket token rows "
                "instead of failing when a requested token is absent from the snapshot."
            ),
        ),
    ] = False,
    output_format: Annotated[
        BookOutputFormat,
        typer.Option(
            "--output-format",
            help=(
                "Write legacy research summary rows or canonical topbook.v1 rows. "
                "Canonical topbook output is strict-schema validated."
            ),
        ),
    ] = BookOutputFormat.LEGACY_SUMMARY,
    manifest_out: Annotated[
        Optional[Path],
        typer.Option(
            "--manifest-out",
            help="Optional run_manifest.v1 JSON path for --output-format topbook.",
        ),
    ] = None,
    run_id: Annotated[
        Optional[str],
        typer.Option(
            "--run-id",
            help="Optional collector run id embedded in canonical topbook rows.",
        ),
    ] = None,
):
    """Collect Polymarket REST order books into legacy summaries or topbook.v1."""
    asyncio.run(
        _collect_books_async(
            markets,
            out,
            poll,
            duration,
            max_snapshots,
            min_volume,
            min_liquidity,
            max_tokens,
            also_jsonl_dir,
            batch_size,
            allow_missing,
            output_format,
            manifest_out,
            run_id,
        )
    )


async def _collect_kalshi_books_async(
    tickers: list[str],
    out: Path,
    *,
    depth: int | None,
    concurrency: int,
    allow_failures: bool,
    output_format: BookOutputFormat,
    manifest_out: Path | None,
    run_id: str | None,
) -> None:
    semaphore = asyncio.Semaphore(concurrency)
    received_at = pd.Timestamp.now(tz="UTC").isoformat()

    async def fetch_summary(
        kalshi: AsyncKalshiClient, ticker: str
    ) -> dict[str, object]:
        async with semaphore:
            try:
                row = await kalshi.normalized_orderbook(ticker, depth=depth)
            except Exception as exc:
                if not allow_failures:
                    raise
                return {
                    "exchange": "kalshi",
                    "market_ticker": ticker,
                    "received_at_utc": received_at,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            row["received_at_utc"] = received_at
            return row

    async def fetch_topbooks(
        kalshi: AsyncKalshiClient,
        ticker: str,
        local_sequence: int,
        collector_run_id: str,
    ) -> list[dict[str, Any]]:
        async with semaphore:
            try:
                payload = await kalshi.orderbook(ticker, depth=depth)
            except Exception:
                if not allow_failures:
                    raise
                return []
            return kalshi_orderbook_to_topbook(
                payload,
                market_ticker=ticker,
                collector_run_id=collector_run_id,
                source="rest_poll",
                received_at_utc=received_at,
                local_sequence=local_sequence,
                raw_event_ref=f"/trade-api/v2/markets/{ticker}/orderbook",
            )

    if output_format is BookOutputFormat.TOPBOOK:
        if allow_failures:
            error_exit(
                "--allow-failures is only supported with --output-format legacy-summary"
            )
        resolved_run_id = run_id or _default_one_shot_run_id("kalshi")
        started_at_utc = _utc_now_iso()
        async with AsyncKalshiClient( config=PmktConfig.from_env()) as kalshi:
            batches = await asyncio.gather(
                *(
                    fetch_topbooks(
                        kalshi,
                        ticker,
                        local_sequence=index,
                        collector_run_id=resolved_run_id,
                    )
                    for index, ticker in enumerate(tickers, start=1)
                )
            )
        ended_at_utc = _utc_now_iso()
        rows = [row for batch in batches for row in batch]
        topbooks = pd.DataFrame(rows, columns=TOPBOOK_COLUMNS)
        path = write_parquet(
            topbooks,
            out,
            overwrite=True,
            schema="topbook.v1",
            coerce=True,
            strict=True,
        )
        if manifest_out is not None:
            _write_one_shot_topbook_manifest(
                manifest_out,
                output_path=path,
                run_id=resolved_run_id,
                started_at_utc=started_at_utc,
                ended_at_utc=ended_at_utc,
                venue="kalshi",
                topbooks=topbooks,
            )
        print(f"Wrote {len(topbooks)} Kalshi topbook.v1 rows to {path}")
        return

    if manifest_out is not None:
        error_exit("--manifest-out requires --output-format topbook")

    async with AsyncKalshiClient( config=PmktConfig.from_env()) as kalshi:
        rows = await asyncio.gather(
            *(fetch_summary(kalshi, ticker) for ticker in tickers)
        )
    path = write_parquet(pd.DataFrame(rows), out, overwrite=True)
    print(f"Wrote {len(rows)} Kalshi legacy book summaries to {path}")


def collect_kalshi_books(
    ticker: Annotated[
        Optional[list[str]],
        typer.Option(
            "--ticker",
            "-t",
            help="Kalshi market ticker to snapshot. Repeat for multiple tickers.",
        ),
    ] = None,
    markets: Annotated[
        Optional[Path],
        typer.Option(
            help="Optional Kalshi markets parquet with market_key/ticker column."
        ),
    ] = None,
    out: Annotated[
        Path,
        typer.Option(help="Output parquet path."),
    ] = Path("generated/kalshi_books.parquet"),
    depth: Annotated[
        Optional[int],
        typer.Option(help="Optional Kalshi orderbook depth."),
    ] = None,
    max_markets: Annotated[
        Optional[int],
        typer.Option(help="Optional cap on tickers after filtering."),
    ] = None,
    concurrency: Annotated[
        int,
        typer.Option(help="Maximum concurrent REST orderbook requests."),
    ] = 10,
    allow_failures: Annotated[
        bool,
        typer.Option(
            "--allow-failures",
            help="Write error rows for failed tickers instead of stopping on first failure.",
        ),
    ] = False,
    output_format: Annotated[
        BookOutputFormat,
        typer.Option(
            "--output-format",
            help=(
                "Write legacy research summary rows or canonical topbook.v1 rows. "
                "Canonical topbook output is strict-schema validated."
            ),
        ),
    ] = BookOutputFormat.LEGACY_SUMMARY,
    manifest_out: Annotated[
        Optional[Path],
        typer.Option(
            "--manifest-out",
            help="Optional run_manifest.v1 JSON path for --output-format topbook.",
        ),
    ] = None,
    run_id: Annotated[
        Optional[str],
        typer.Option(
            "--run-id",
            help="Optional collector run id embedded in canonical topbook rows.",
        ),
    ] = None,
) -> None:
    """Collect Kalshi REST order books into legacy summaries or topbook.v1."""
    tickers: list[str] = []
    for item in ticker or []:
        ticker_text = str(item).strip()
        if ticker_text and ticker_text not in tickers:
            tickers.append(ticker_text)
    if markets:
        markets_df = read_parquet(markets)
        ticker_col = required_column(
            markets_df,
            ("market_key", "ticker", "market_ticker"),
            path=markets,
            label="Kalshi markets",
        )
        for item in unique_nonempty_strings(markets_df[ticker_col]):
            if item not in tickers:
                tickers.append(item)
    if max_markets is not None:
        tickers = tickers[:max_markets]
    if not tickers:
        error_exit("provide --ticker or --markets with at least one ticker")
    if concurrency < 1:
        raise typer.BadParameter("concurrency must be >= 1")
    asyncio.run(
        _collect_kalshi_books_async(
            tickers,
            out,
            depth=depth,
            concurrency=concurrency,
            allow_failures=allow_failures,
            output_format=output_format,
            manifest_out=manifest_out,
            run_id=run_id,
        )
    )






def stream_books(
    token_id: Annotated[Optional[list[str]], typer.Option("--token-id", "-t", help="Instrument to record; repeat for multiple instruments.")] = None,
    markets: Annotated[Optional[Path], typer.Option(help="Markets Parquet providing instrument IDs.")] = None,
    output_dir: Annotated[Path, typer.Option(help="Root directory for recording runs.")] = DEFAULT_ORDER_BOOK_STREAM_ROOT,
    run_name: Annotated[Optional[str], typer.Option(help="New run directory name.")] = None,
    duration: Annotated[float, typer.Option(help="Recording duration in seconds.")] = 300.0,
    max_messages: Annotated[Optional[int], typer.Option(help="Stop after this many source messages.")] = None,
    mode: Annotated[str, typer.Option(help="topbook or full; full adds selected depth snapshots.")] = "full",
    depth_check_interval_s: Annotated[str, typer.Option(help="Seconds between changed-book checks; 'off' disables periodic checks.")] = "10",
    depth_on_best_price_change: Annotated[bool, typer.Option(help="Save depth immediately on best bid/ask price changes.")] = False,
    raw_messages: Annotated[bool, typer.Option(help="Also save diagnostic raw_messages.jsonl.")] = False,
    max_reconnects: Annotated[int, typer.Option(help="Bound on replacement connection attempts.")] = 3,
    websocket_max_size_bytes: Annotated[int, typer.Option()] = WS_MAX_SIZE_BYTES,
    websocket_max_queue_frames: Annotated[int, typer.Option()] = WS_MAX_QUEUE_FRAMES,
) -> None:
    """Record live books and public trades into SQLite, then export Parquet."""
    instruments = list(dict.fromkeys(token_id or []))
    if markets:
        instruments = list(dict.fromkeys([*instruments, *_tokens_from_markets_parquet(markets)]))
    if not instruments:
        error_exit("provide --token-id or --markets")
    interval = _recording_interval(depth_check_interval_s)
    try:
        manifest = asyncio.run(stream_order_book_data(
            instruments, output_root=output_dir, run_name=run_name,
            duration_s=duration, max_messages=max_messages, mode=mode,
            depth_check_interval_s=interval,
            depth_on_best_price_change=depth_on_best_price_change,
            raw_messages=raw_messages, max_reconnects=max_reconnects,
            websocket_max_size_bytes=websocket_max_size_bytes,
            websocket_max_queue_frames=websocket_max_queue_frames,
        ))
    except (ValueError, RuntimeError) as exc:
        error_exit(str(exc))
    print(f"Recording {manifest['status']}: {manifest['run_dir']}")
    if manifest["status"] != "complete":
        raise typer.Exit(code=1)


def stream_kalshi_books(
    ticker: Annotated[Optional[list[str]], typer.Option("--ticker", "-t", help="Instrument to record; repeat for multiple instruments.")] = None,
    markets: Annotated[Optional[Path], typer.Option(help="Markets Parquet providing instrument IDs.")] = None,
    output_dir: Annotated[Path, typer.Option(help="Root directory for recording runs.")] = DEFAULT_KALSHI_ORDER_BOOK_STREAM_ROOT,
    run_name: Annotated[Optional[str], typer.Option(help="New run directory name.")] = None,
    duration: Annotated[float, typer.Option(help="Recording duration in seconds.")] = 300.0,
    max_messages: Annotated[Optional[int], typer.Option(help="Stop after this many source messages.")] = None,
    mode: Annotated[str, typer.Option(help="topbook or full; full adds selected depth snapshots.")] = "full",
    depth_check_interval_s: Annotated[str, typer.Option(help="Seconds between changed-book checks; 'off' disables periodic checks.")] = "10",
    depth_on_best_price_change: Annotated[bool, typer.Option(help="Save depth immediately on best bid/ask price changes.")] = False,
    raw_messages: Annotated[bool, typer.Option(help="Also save diagnostic raw_messages.jsonl.")] = False,
    max_reconnects: Annotated[int, typer.Option(help="Bound on replacement connection attempts.")] = 3,
    websocket_max_size_bytes: Annotated[int, typer.Option()] = WS_MAX_SIZE_BYTES,
    websocket_max_queue_frames: Annotated[int, typer.Option()] = WS_MAX_QUEUE_FRAMES,
    header_provider: Annotated[Optional[str], typer.Option(help="Read-auth provider MODULE:ATTRIBUTE.")] = None,
) -> None:
    """Record live books and public trades into SQLite, then export Parquet."""
    instruments = list(dict.fromkeys(ticker or []))
    if markets:
        instruments = list(dict.fromkeys([*instruments, *_tickers_from_kalshi_markets_parquet(markets)]))
    if not instruments:
        error_exit("provide --ticker or --markets")
    interval = _recording_interval(depth_check_interval_s)
    try:
        manifest = asyncio.run(stream_kalshi_order_book_data(
            instruments, output_root=output_dir, run_name=run_name,
            duration_s=duration, max_messages=max_messages, mode=mode,
            depth_check_interval_s=interval,
            depth_on_best_price_change=depth_on_best_price_change,
            raw_messages=raw_messages, max_reconnects=max_reconnects,
            websocket_max_size_bytes=websocket_max_size_bytes,
            websocket_max_queue_frames=websocket_max_queue_frames,
        auth=_load_read_auth_header_provider(header_provider),
        ))
    except (ValueError, RuntimeError) as exc:
        error_exit(str(exc))
    print(f"Recording {manifest['status']}: {manifest['run_dir']}")
    if manifest["status"] != "complete":
        raise typer.Exit(code=1)


def _recording_interval(value: str) -> float | None:
    if value.strip().lower() in {"off", "none"}:
        return None
    try:
        return float(value)
    except ValueError as exc:
        raise typer.BadParameter("use a positive number of seconds or 'off'", param_hint="--depth-check-interval-s") from exc
