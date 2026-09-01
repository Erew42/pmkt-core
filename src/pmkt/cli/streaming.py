from __future__ import annotations

import asyncio
import importlib
import sys
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Optional, Sequence

import pandas as pd
import typer

from pmkt.tokens import flatten_token_ids
from pmkt.exchanges.polymarket.clob import ClobClient
from pmkt.exchanges.read_auth import (
    ReadAuthHeaderProvider,
    ReadAuthenticationRequiredError,
)
from pmkt.exchanges.kalshi.client import AsyncKalshiClient
from pmkt.exchanges.kalshi.order_book_stream import (
    DEFAULT_KALSHI_ORDER_BOOK_STREAM_ROOT,
    stream_kalshi_order_book_data,
)
from pmkt.streaming.supervisor import FeedShardHealth, LiveFeedSupervisor
from pmkt.data.io import (
    RECOMMENDED_PARQUET_SEGMENT_ROWS,
)
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
from pmkt.exchanges.polymarket.order_book_stream import (
    DEFAULT_ORDER_BOOK_STREAM_ROOT,
    stream_order_book_data,
)
from pmkt.data.storage.parquet import read_parquet, write_parquet
from pmkt.exchanges.ws_transport import (
    WS_MAX_QUEUE_FRAMES,
    WS_MAX_SIZE_BYTES,
)
from pmkt.streaming import capture_group
from pmkt.streaming.capture_completeness import CaptureIntent
from pmkt.streaming.connection_partitions import (
    ConnectionPartition,
    build_connection_partitions,
    subscription_plan_relation_ids_by_instrument,
)
from pmkt.streaming.durability import file_sha256
from pmkt.streaming.durability_settings import MAX_SEGMENT_SECONDS
from pmkt.streaming.profiles import (
    ContractStatus,
    StorageProfileOverrides,
    StorageProfileSelection,
    select_storage_profile,
)
from pmkt.streaming.storage_backends import CaptureStorageBackend

from pmkt.cli.shared import error_exit, required_column, unique_nonempty_strings


class BookOutputFormat(str, Enum):
    LEGACY_SUMMARY = "legacy-summary"
    TOPBOOK = "topbook"


DEFAULT_CONNECTION_START_STAGGER_SECONDS = 0.1
DEFAULT_EXTENDED_SEGMENT_LIMIT_SECONDS = 30.0


def _default_feed_supervisor(
    *, venue: str, instruments: Sequence[str]
) -> LiveFeedSupervisor:
    return LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue=venue,
                shard_id=f"{venue}-0",
                subscribed_instruments=tuple(instruments),
            )
        ]
    )


def _capture_connection_partitions(
    *,
    venue: str,
    instruments: Sequence[str],
    supervisor: LiveFeedSupervisor | None,
    plan_payload: Mapping[str, Any] | None,
    connection_batch_size: int | None,
    affinity_key_by_instrument: Mapping[str, str] | None = None,
) -> tuple[ConnectionPartition, ...]:
    resolved_supervisor = supervisor or _default_feed_supervisor(
        venue=venue,
        instruments=instruments,
    )
    return build_connection_partitions(
        resolved_supervisor,
        venue=venue,
        instruments=instruments,
        max_instruments=connection_batch_size,
        relation_ids_by_instrument=(
            subscription_plan_relation_ids_by_instrument(plan_payload, venue=venue)
            if plan_payload is not None
            else None
        ),
        affinity_key_by_instrument=affinity_key_by_instrument,
    )


def _capture_summary_suffix(manifest: Mapping[str, Any]) -> str:
    summary = manifest.get("capture_summary")
    if not isinstance(summary, Mapping):
        summary = manifest.get("capture_completeness")
    if not isinstance(summary, Mapping):
        return ""
    requested = int(summary.get("requested_instrument_count") or 0)
    initial = int(summary.get("initial_snapshot_count") or 0)
    reconnects = int(
        summary.get("reconnect_count") or manifest.get("reconnect_count") or 0
    )
    if requested <= 0:
        return ""
    return f", {initial}/{requested} initial snapshots, {reconnects} reconnects"


def _storage_profile_selection(
    *,
    name: str,
    acknowledge_experimental: bool,
    keep_raw_jsonl: bool,
    topbook_emission_per_event: bool,
    emit_full_depth: bool,
    emit_legacy_book_artifacts: bool,
    feed_health_interval_seconds: float | None,
    topbook_checkpoint_interval_seconds: float | None,
    book_checkpoint_interval_seconds: float | None,
) -> StorageProfileSelection:
    try:
        selection = select_storage_profile(
            name,
            overrides=StorageProfileOverrides(
                keep_raw_jsonl=keep_raw_jsonl,
                topbook_emission_per_event=topbook_emission_per_event,
                emit_full_depth=emit_full_depth,
                emit_legacy_book_artifacts=emit_legacy_book_artifacts,
            ),
            experimental_profile_acknowledged=acknowledge_experimental,
            feed_health_interval_seconds=feed_health_interval_seconds,
            topbook_checkpoint_interval_seconds=topbook_checkpoint_interval_seconds,
            book_checkpoint_interval_seconds=book_checkpoint_interval_seconds,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--storage-profile") from exc
    if (
        selection.definition.contract_status is ContractStatus.EXPERIMENTAL
        and not acknowledge_experimental
    ):
        raise typer.BadParameter(
            f"storage profile {name!r} is experimental; pass "
            "--acknowledge-experimental-profile",
            param_hint="--storage-profile",
        )
    return selection


def _validate_stream_capture_cli_inputs(
    *,
    duration: float,
    max_messages: int | None,
    max_reconnects: int,
    parquet_segment_rows: int | None,
    parquet_segment_seconds: float | None,
    websocket_max_size_bytes: int,
    websocket_max_queue_frames: int,
    connection_batch_size: int | None = None,
    connection_processes: int = 1,
    connection_start_stagger_seconds: float = DEFAULT_CONNECTION_START_STAGGER_SECONDS,
    acknowledge_extended_durability_window: bool = False,
) -> None:
    if duration <= 0 and max_messages is None:
        raise typer.BadParameter(
            "must be > 0 unless --max-messages is set",
            param_hint="--duration",
        )
    if max_messages is not None and max_messages < 1:
        raise typer.BadParameter("must be >= 1", param_hint="--max-messages")
    if max_reconnects < 0:
        raise typer.BadParameter("must be >= 0", param_hint="--max-reconnects")
    if parquet_segment_rows is not None and parquet_segment_rows <= 0:
        raise typer.BadParameter(
            "must be > 0",
            param_hint="--parquet-segment-rows",
        )
    if parquet_segment_seconds is not None and parquet_segment_seconds <= 0:
        raise typer.BadParameter(
            "must be > 0",
            param_hint="--parquet-segment-seconds",
        )
    if (
        parquet_segment_seconds is not None
        and parquet_segment_seconds > MAX_SEGMENT_SECONDS
    ):
        raise typer.BadParameter(
            f"must be <= {MAX_SEGMENT_SECONDS:g}",
            param_hint="--parquet-segment-seconds",
        )
    if (
        parquet_segment_seconds is not None
        and parquet_segment_seconds > DEFAULT_EXTENDED_SEGMENT_LIMIT_SECONDS
        and not acknowledge_extended_durability_window
    ):
        raise typer.BadParameter(
            "values above 30 seconds increase crash-loss exposure; pass "
            "--acknowledge-extended-durability-window",
            param_hint="--parquet-segment-seconds",
        )
    if connection_batch_size is not None and connection_batch_size <= 0:
        raise typer.BadParameter(
            "must be > 0",
            param_hint="--connection-batch-size",
        )
    if connection_processes <= 0:
        raise typer.BadParameter(
            "must be > 0",
            param_hint="--connection-processes",
        )
    if connection_start_stagger_seconds < 0:
        raise typer.BadParameter(
            "must be >= 0",
            param_hint="--connection-start-stagger-seconds",
        )
    if websocket_max_size_bytes <= 0:
        raise typer.BadParameter(
            "must be > 0", param_hint="--websocket-max-size-bytes"
        )
    if websocket_max_queue_frames <= 0:
        raise typer.BadParameter(
            "must be > 0", param_hint="--websocket-max-queue-frames"
        )


def _warn_experimental_profile(selection: StorageProfileSelection) -> None:
    if selection.definition.contract_status is not ContractStatus.EXPERIMENTAL:
        return
    typer.echo(
        "Warning: using experimental storage profile "
        f"{selection.definition.name!r}; acknowledgement will be recorded "
        "in capture provenance.",
        err=True,
    )


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


def _polymarket_affinity_keys_from_markets_df(
    markets_df: pd.DataFrame, *, path: Path
) -> dict[str, str]:
    token_column = required_column(
        markets_df, ("token_ids",), path=path, label="markets"
    )
    market_column = next(
        (
            column
            for column in (
                "market_id",
                "market_key",
                "id",
                "condition_id",
                "venue_market_id",
            )
            if column in markets_df.columns
        ),
        None,
    )
    if market_column is None:
        return {}
    result: dict[str, str] = {}
    for token_value, market_value in zip(
        markets_df[token_column].tolist(),
        markets_df[market_column].tolist(),
        strict=True,
    ):
        if pd.isna(market_value):
            continue
        market_key = str(market_value).strip()
        if not market_key:
            continue
        for token in flatten_token_ids(token_value):
            previous = result.get(token)
            if previous is not None and previous != market_key:
                raise ValueError(
                    "markets parquet maps a Polymarket token to multiple markets: "
                    f"{token} -> {previous!r}, {market_key!r}"
                )
            result[token] = market_key
    return result


def _tickers_from_kalshi_markets_parquet(markets_path: Path) -> list[str]:
    markets_df = read_parquet(markets_path)
    ticker_col = required_column(
        markets_df,
        ("market_key", "ticker", "market_ticker"),
        path=markets_path,
        label="Kalshi markets",
    )
    return unique_nonempty_strings(markets_df[ticker_col])


def _tokens_from_subscription_plan_payload(plan: dict[str, Any]) -> list[str]:
    polymarket = plan.get("polymarket")
    if isinstance(polymarket, dict):
        values = polymarket.get("assets_ids", polymarket.get("asset_ids", []))
        return _unique_plan_texts(values)
    return _unique_plan_texts(
        item.get("asset_id")
        for item in plan.get("polymarket_assets", [])
        if isinstance(item, dict)
    )


def _tickers_from_subscription_plan_payload(plan: dict[str, Any]) -> list[str]:
    kalshi = plan.get("kalshi")
    if isinstance(kalshi, dict):
        values = kalshi.get("market_tickers", kalshi.get("tickers", []))
        return _unique_plan_texts(values)
    return _unique_plan_texts(
        item.get("market_ticker")
        for item in plan.get("kalshi_market_tickers", [])
        if isinstance(item, dict)
    )


def _subscription_plan_metadata(
    plan_path: Path, plan: Mapping[str, Any]
) -> dict[str, Any]:
    plan_hash = file_sha256(plan_path)
    source_reference = str(plan_path.resolve())

    def evidence(
        instrument_ids: Iterable[str],
        *,
        item_key: str,
        plan_items_key: str,
    ) -> dict[str, dict[str, Any]]:
        raw_items = plan.get(plan_items_key)
        items = (
            {
                str(item.get(item_key)): item
                for item in raw_items
                if isinstance(item, Mapping) and str(item.get(item_key) or "").strip()
            }
            if isinstance(raw_items, list)
            else {}
        )
        result: dict[str, dict[str, Any]] = {}
        for instrument_id in instrument_ids:
            item = items.get(instrument_id, {})
            active = item.get("active")
            eligible = active is not False
            result[instrument_id] = {
                "status": "eligible" if eligible else "ineligible",
                "reason": "source_active" if eligible else "source_inactive",
                "source_identity": "validated_subscription_plan.v1",
                "source_reference": source_reference,
                "source_sha256": plan_hash,
                "observed_at_utc": (
                    item.get("validated_at_utc") or plan.get("created_at_utc")
                ),
            }
        return result

    return {
        "schema_version": plan.get("schema_version"),
        "plan_id": plan.get("plan_id"),
        "path": str(plan_path),
        "sha256": plan_hash,
        "source_market_registry_path": plan.get("source_market_registry_path"),
        "instrument_eligibility": {
            "polymarket": evidence(
                _tokens_from_subscription_plan_payload(dict(plan)),
                item_key="asset_id",
                plan_items_key="polymarket_assets",
            ),
            "kalshi": evidence(
                _tickers_from_subscription_plan_payload(dict(plan)),
                item_key="market_ticker",
                plan_items_key="kalshi_market_tickers",
            ),
        },
    }


def _unique_plan_texts(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, (str, bytes)):
        iterable: Iterable[Any] = [values]
    elif isinstance(values, Iterable):
        iterable = values
    else:
        iterable = [values]
    return unique_nonempty_strings(str(value).strip() for value in iterable)


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
    provider = (
        candidate()
        if callable(candidate) and not hasattr(candidate, "headers_for_get")
        else candidate
    )
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
        async with ClobClient() as clob:
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

    async with ClobClient() as clob:
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
        async with AsyncKalshiClient() as kalshi:
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

    async with AsyncKalshiClient() as kalshi:
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
    token_id: Annotated[
        Optional[list[str]],
        typer.Option(
            "--token-id",
            "-t",
            help="CLOB asset/token id to stream. Repeat for multiple tokens.",
        ),
    ] = None,
    markets: Annotated[
        Optional[Path],
        typer.Option(help="Optional markets parquet with a token_ids column."),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="Root output directory for stream run folders."),
    ] = DEFAULT_ORDER_BOOK_STREAM_ROOT,
    duration: Annotated[
        float,
        typer.Option(help="Collection duration in seconds."),
    ] = 300.0,
    max_messages: Annotated[
        Optional[int],
        typer.Option(help="Optional cap on websocket messages before stopping."),
    ] = None,
    capture_intent: Annotated[
        CaptureIntent,
        typer.Option(
            "--capture-intent",
            help="Capture lifecycle intent: operational or explicit smoke.",
        ),
    ] = CaptureIntent.OPERATIONAL,
    websocket_max_size_bytes: Annotated[
        int,
        typer.Option(
            "--websocket-max-size-bytes",
            help="Maximum accepted WebSocket message size in bytes.",
        ),
    ] = WS_MAX_SIZE_BYTES,
    websocket_max_queue_frames: Annotated[
        int,
        typer.Option(
            "--websocket-max-queue-frames",
            help="Maximum buffered WebSocket frames per connection.",
        ),
    ] = WS_MAX_QUEUE_FRAMES,
    parquet_segment_rows: Annotated[
        Optional[int],
        typer.Option(
            "--parquet-segment-rows",
            help=(
                "Opt in to durable parquet directory datasets by rotating after this "
                f"many rows. Suggested value: {RECOMMENDED_PARQUET_SEGMENT_ROWS}."
            ),
        ),
    ] = None,
    parquet_segment_seconds: Annotated[
        Optional[float],
        typer.Option(
            "--parquet-segment-seconds",
            help=(
                "Opt in to durable parquet directory datasets by rotating after this "
                "many seconds. The safe default bound is 30; acknowledged "
                "read-only captures may use up to 300."
            ),
        ),
    ] = None,
    acknowledge_extended_durability_window: Annotated[
        bool,
        typer.Option(
            "--acknowledge-extended-durability-window",
            help=(
                "Acknowledge that a Parquet segment interval above 30 seconds "
                "increases the maximum data lost by a process or host crash."
            ),
        ),
    ] = False,
    max_reconnects: Annotated[
        int,
        typer.Option(
            "--max-reconnects",
            help="Maximum Polymarket websocket reconnect attempts during the run.",
        ),
    ] = 1000,
    max_tokens: Annotated[
        Optional[int],
        typer.Option(
            help="Optional cap on tokens from --markets after explicit --token-id values."
        ),
    ] = None,
    connection_batch_size: Annotated[
        Optional[int],
        typer.Option(
            "--connection-batch-size",
            help=(
                "Maximum instruments per real WebSocket connection. Existing "
                "market affinity boundaries are preserved and may be split further."
            ),
        ),
    ] = None,
    connection_processes: Annotated[
        int,
        typer.Option(
            "--connection-processes",
            help=(
                "Polymarket worker processes hosting the partitioned WebSocket "
                "connections. Use 2 to spread collector CPU across two cores."
            ),
        ),
    ] = 1,
    connection_start_stagger_seconds: Annotated[
        float,
        typer.Option(
            "--connection-start-stagger-seconds",
            help="Delay successive shard connections to avoid a subscription burst.",
        ),
    ] = DEFAULT_CONNECTION_START_STAGGER_SECONDS,
    run_name: Annotated[
        Optional[str],
        typer.Option(help="Optional run folder name. Defaults to a UTC timestamp."),
    ] = None,
    storage_profile: Annotated[
        str,
        typer.Option(
            "--storage-profile", help="Storage profile: full, book-tape, or mm-compact."
        ),
    ] = "full",
    capture_storage_backend: Annotated[
        CaptureStorageBackend,
        typer.Option(
            "--capture-storage-backend",
            help=(
                "Durable capture backend. parquet_segments remains the default; "
                "sqlite_wal_v1 is opt-in and promotes on finalization."
            ),
        ),
    ] = CaptureStorageBackend.PARQUET_SEGMENTS,
    acknowledge_experimental_profile: Annotated[
        bool,
        typer.Option(
            "--acknowledge-experimental-profile",
            help="Acknowledge use of an experimental reduced profile.",
        ),
    ] = False,
    feed_health_interval_seconds: Annotated[
        Optional[float], typer.Option("--feed-health-interval-seconds")
    ] = None,
    topbook_checkpoint_interval_seconds: Annotated[
        Optional[float], typer.Option("--topbook-checkpoint-interval-seconds")
    ] = None,
    book_checkpoint_interval_seconds: Annotated[
        Optional[float], typer.Option("--book-checkpoint-interval-seconds")
    ] = None,
    keep_raw_jsonl: Annotated[bool, typer.Option("--keep-raw-jsonl")] = False,
    topbook_emission_per_event: Annotated[
        bool, typer.Option("--topbook-emission-per-event")
    ] = False,
    emit_full_depth: Annotated[bool, typer.Option("--emit-full-depth")] = False,
    emit_legacy_book_artifacts: Annotated[
        bool, typer.Option("--emit-legacy-book-artifacts")
    ] = False,
) -> None:
    """Stream websocket order-book events into analysis-ready files."""
    _validate_stream_capture_cli_inputs(
        duration=duration,
        max_messages=max_messages,
        max_reconnects=max_reconnects,
        parquet_segment_rows=parquet_segment_rows,
        parquet_segment_seconds=parquet_segment_seconds,
        websocket_max_size_bytes=websocket_max_size_bytes,
        websocket_max_queue_frames=websocket_max_queue_frames,
        connection_batch_size=connection_batch_size,
        connection_processes=connection_processes,
        connection_start_stagger_seconds=connection_start_stagger_seconds,
        acknowledge_extended_durability_window=(
            acknowledge_extended_durability_window
        ),
    )
    selection = _storage_profile_selection(
        name=storage_profile,
        acknowledge_experimental=acknowledge_experimental_profile,
        keep_raw_jsonl=keep_raw_jsonl,
        topbook_emission_per_event=topbook_emission_per_event,
        emit_full_depth=emit_full_depth,
        emit_legacy_book_artifacts=emit_legacy_book_artifacts,
        feed_health_interval_seconds=feed_health_interval_seconds,
        topbook_checkpoint_interval_seconds=topbook_checkpoint_interval_seconds,
        book_checkpoint_interval_seconds=book_checkpoint_interval_seconds,
    )
    if (
        capture_storage_backend is CaptureStorageBackend.SQLITE_WAL
        and keep_raw_jsonl
    ):
        raise typer.BadParameter(
            "sqlite_wal_v1 does not yet support --keep-raw-jsonl",
            param_hint="--capture-storage-backend",
        )
    token_ids: list[str] = []
    affinity_key_by_instrument: dict[str, str] = {}
    for token in token_id or []:
        token_text = str(token).strip()
        if token_text and token_text not in token_ids:
            token_ids.append(token_text)
    if markets:
        markets_df = read_parquet(markets)
        affinity_key_by_instrument = _polymarket_affinity_keys_from_markets_df(
            markets_df, path=markets
        )
        for token in _token_ids_from_markets_df(markets_df, path=markets):
            if token not in token_ids:
                token_ids.append(token)
    if max_tokens is not None:
        token_ids = token_ids[:max_tokens]
    if not token_ids:
        if markets:
            error_exit(f"no token ids found in markets parquet {markets}")
        error_exit("provide --token-id or --markets")
    _warn_experimental_profile(selection)
    partitions = _capture_connection_partitions(
        venue="polymarket",
        instruments=token_ids,
        supervisor=None,
        plan_payload=None,
        connection_batch_size=connection_batch_size,
        affinity_key_by_instrument=affinity_key_by_instrument,
    )
    if len(partitions) > 1 and max_messages is not None:
        raise typer.BadParameter(
            "is ambiguous across multiple connections; use --duration",
            param_hint="--max-messages",
        )
    manifest = asyncio.run(
        capture_group.run_connection_partition_group(
            venue="polymarket",
            partitions=partitions,
            collector=stream_order_book_data,
            output_dir=output_dir,
            run_name=run_name,
            start_stagger_seconds=connection_start_stagger_seconds,
            collector_kwargs={
                "duration_s": duration,
                "max_messages": max_messages,
                "capture_intent": capture_intent,
                "websocket_max_size_bytes": websocket_max_size_bytes,
                "websocket_max_queue_frames": websocket_max_queue_frames,
                "parquet_segment_rows": parquet_segment_rows,
                "parquet_segment_seconds": parquet_segment_seconds,
                "max_reconnects": max_reconnects,
                "command": _current_command(),
                "git_cwd": Path.cwd(),
                "subscription_plan_metadata": None,
                "storage_profile": selection,
                "capture_storage_backend": capture_storage_backend,
            },
            process_count=connection_processes,
        )
    )
    counts = manifest["counts"]
    print(
        "Wrote order-book stream to "
        f"{manifest['run_dir']} "
        f"({counts.get('events', 0)} events, {counts.get('snapshots', 0)} snapshots, "
        f"{counts.get('levels', 0)} levels{_capture_summary_suffix(manifest)})"
    )


def stream_kalshi_books(
    ticker: Annotated[
        Optional[list[str]],
        typer.Option(
            "--ticker",
            "-t",
            help="Kalshi market ticker to stream. Repeat for multiple tickers.",
        ),
    ] = None,
    markets: Annotated[
        Optional[Path],
        typer.Option(
            help="Optional Kalshi markets parquet with market_key/ticker column."
        ),
    ] = None,
    header_provider: Annotated[
        Optional[str],
        typer.Option(
            "--header-provider",
            help="Import path MODULE:ATTRIBUTE for a read-auth header provider.",
        ),
    ] = None,
    max_markets: Annotated[
        Optional[int],
        typer.Option(
            help="Optional cap on tickers from --markets after explicit --ticker values."
        ),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="Root output directory for Kalshi stream run folders."),
    ] = DEFAULT_KALSHI_ORDER_BOOK_STREAM_ROOT,
    duration: Annotated[
        float,
        typer.Option(help="Collection duration in seconds."),
    ] = 300.0,
    max_messages: Annotated[
        Optional[int],
        typer.Option(help="Optional cap on websocket messages before stopping."),
    ] = None,
    capture_intent: Annotated[
        CaptureIntent,
        typer.Option(
            "--capture-intent",
            help="Capture lifecycle intent: operational or explicit smoke.",
        ),
    ] = CaptureIntent.OPERATIONAL,
    websocket_max_size_bytes: Annotated[
        int,
        typer.Option(
            "--websocket-max-size-bytes",
            help="Maximum accepted WebSocket message size in bytes.",
        ),
    ] = WS_MAX_SIZE_BYTES,
    websocket_max_queue_frames: Annotated[
        int,
        typer.Option(
            "--websocket-max-queue-frames",
            help="Maximum buffered WebSocket frames per connection.",
        ),
    ] = WS_MAX_QUEUE_FRAMES,
    parquet_segment_rows: Annotated[
        Optional[int],
        typer.Option(
            "--parquet-segment-rows",
            help=(
                "Opt in to durable parquet directory datasets by rotating after this "
                f"many rows. Suggested value: {RECOMMENDED_PARQUET_SEGMENT_ROWS}."
            ),
        ),
    ] = None,
    parquet_segment_seconds: Annotated[
        Optional[float],
        typer.Option(
            "--parquet-segment-seconds",
            help=(
                "Opt in to durable parquet directory datasets by rotating after this "
                "many seconds. The safe default bound is 30; acknowledged "
                "read-only captures may use up to 300."
            ),
        ),
    ] = None,
    acknowledge_extended_durability_window: Annotated[
        bool,
        typer.Option(
            "--acknowledge-extended-durability-window",
            help=(
                "Acknowledge that a Parquet segment interval above 30 seconds "
                "increases the maximum data lost by a process or host crash."
            ),
        ),
    ] = False,
    max_reconnects: Annotated[
        int,
        typer.Option(
            "--max-reconnects",
            help="Maximum Kalshi websocket reconnect attempts during the run.",
        ),
    ] = 1000,
    connection_batch_size: Annotated[
        Optional[int],
        typer.Option(
            "--connection-batch-size",
            help=(
                "Maximum markets per real WebSocket connection. Existing "
                "instrument partitions may be split further."
            ),
        ),
    ] = None,
    connection_start_stagger_seconds: Annotated[
        float,
        typer.Option(
            "--connection-start-stagger-seconds",
            help="Delay successive shard connections to avoid a subscription burst.",
        ),
    ] = DEFAULT_CONNECTION_START_STAGGER_SECONDS,
    run_name: Annotated[
        Optional[str],
        typer.Option(help="Optional run folder name. Defaults to a UTC timestamp."),
    ] = None,
    storage_profile: Annotated[
        str,
        typer.Option(
            "--storage-profile", help="Storage profile: full, book-tape, or mm-compact."
        ),
    ] = "full",
    capture_storage_backend: Annotated[
        CaptureStorageBackend,
        typer.Option(
            "--capture-storage-backend",
            help=(
                "Durable capture backend. parquet_segments remains the default; "
                "sqlite_wal_v1 is opt-in and promotes on finalization."
            ),
        ),
    ] = CaptureStorageBackend.PARQUET_SEGMENTS,
    acknowledge_experimental_profile: Annotated[
        bool,
        typer.Option(
            "--acknowledge-experimental-profile",
            help="Acknowledge use of an experimental reduced profile.",
        ),
    ] = False,
    feed_health_interval_seconds: Annotated[
        Optional[float], typer.Option("--feed-health-interval-seconds")
    ] = None,
    topbook_checkpoint_interval_seconds: Annotated[
        Optional[float], typer.Option("--topbook-checkpoint-interval-seconds")
    ] = None,
    book_checkpoint_interval_seconds: Annotated[
        Optional[float], typer.Option("--book-checkpoint-interval-seconds")
    ] = None,
    keep_raw_jsonl: Annotated[bool, typer.Option("--keep-raw-jsonl")] = False,
    topbook_emission_per_event: Annotated[
        bool, typer.Option("--topbook-emission-per-event")
    ] = False,
    emit_full_depth: Annotated[bool, typer.Option("--emit-full-depth")] = False,
    emit_legacy_book_artifacts: Annotated[
        bool, typer.Option("--emit-legacy-book-artifacts")
    ] = False,
) -> None:
    """Stream Kalshi order-book websocket events into analysis-ready files."""
    _validate_stream_capture_cli_inputs(
        duration=duration,
        max_messages=max_messages,
        max_reconnects=max_reconnects,
        parquet_segment_rows=parquet_segment_rows,
        parquet_segment_seconds=parquet_segment_seconds,
        websocket_max_size_bytes=websocket_max_size_bytes,
        websocket_max_queue_frames=websocket_max_queue_frames,
        connection_batch_size=connection_batch_size,
        connection_start_stagger_seconds=connection_start_stagger_seconds,
        acknowledge_extended_durability_window=(
            acknowledge_extended_durability_window
        ),
    )
    selection = _storage_profile_selection(
        name=storage_profile,
        acknowledge_experimental=acknowledge_experimental_profile,
        keep_raw_jsonl=keep_raw_jsonl,
        topbook_emission_per_event=topbook_emission_per_event,
        emit_full_depth=emit_full_depth,
        emit_legacy_book_artifacts=emit_legacy_book_artifacts,
        feed_health_interval_seconds=feed_health_interval_seconds,
        topbook_checkpoint_interval_seconds=topbook_checkpoint_interval_seconds,
        book_checkpoint_interval_seconds=book_checkpoint_interval_seconds,
    )
    if (
        capture_storage_backend is CaptureStorageBackend.SQLITE_WAL
        and keep_raw_jsonl
    ):
        raise typer.BadParameter(
            "sqlite_wal_v1 does not yet support --keep-raw-jsonl",
            param_hint="--capture-storage-backend",
        )
    tickers: list[str] = []
    for item in ticker or []:
        ticker_text = str(item).strip()
        if ticker_text and ticker_text not in tickers:
            tickers.append(ticker_text)
    if markets:
        for item in _tickers_from_kalshi_markets_parquet(markets):
            if item not in tickers:
                tickers.append(item)
    if max_markets is not None:
        tickers = tickers[:max_markets]
    if not tickers:
        if markets:
            error_exit(f"no tickers found in Kalshi markets parquet {markets}")
        error_exit("provide --ticker or --markets")
    resolved_header_provider = _load_read_auth_header_provider(header_provider)
    _warn_experimental_profile(selection)
    partitions = _capture_connection_partitions(
        venue="kalshi",
        instruments=tickers,
        supervisor=None,
        plan_payload=None,
        connection_batch_size=connection_batch_size,
    )
    if len(partitions) > 1 and max_messages is not None:
        raise typer.BadParameter(
            "is ambiguous across multiple connections; use --duration",
            param_hint="--max-messages",
        )

    try:
        manifest = asyncio.run(
            capture_group.run_connection_partition_group(
                venue="kalshi",
                partitions=partitions,
                collector=stream_kalshi_order_book_data,
                output_dir=output_dir,
                run_name=run_name,
                start_stagger_seconds=connection_start_stagger_seconds,
                collector_kwargs={
                    "duration_s": duration,
                    "max_messages": max_messages,
                    "capture_intent": capture_intent,
                    "websocket_max_size_bytes": websocket_max_size_bytes,
                    "websocket_max_queue_frames": websocket_max_queue_frames,
                    "parquet_segment_rows": parquet_segment_rows,
                    "parquet_segment_seconds": parquet_segment_seconds,
                    "max_reconnects": max_reconnects,
                    "command": _current_command(),
                    "git_cwd": Path.cwd(),
                    "use_yes_price": (
                        True
                    ),
                    "subscription_plan_metadata": None,
                    "auth": resolved_header_provider,
                    "storage_profile": selection,
                    "capture_storage_backend": capture_storage_backend,
                },
            )
        )
    except ReadAuthenticationRequiredError as exc:
        print(f"Error: {exc}", flush=True)
        raise typer.Exit(code=1) from exc

    counts = manifest["counts"]
    print(
        "Wrote Kalshi order-book stream to "
        f"{manifest['run_dir']} "
        f"({counts.get('events', 0)} events, {counts.get('snapshots', 0)} snapshots, "
        f"{counts.get('levels', 0)} levels{_capture_summary_suffix(manifest)})"
    )
