from __future__ import annotations

import json
import hashlib
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from pmkt.data.canonical import (
    HISTORICAL_BACKFILL_GAP_COLUMNS,
    HISTORICAL_PRICE_COLUMNS,
    TRADE_COLUMNS,
    VENUE_HISTORY_CAPABILITY_COLUMNS,
    historical_backfill_gap_row,
    historical_price_row,
    trade_row,
    venue_history_capability_row,
)
from pmkt.data.storage.parquet import write_parquet
from pmkt.data.time import isoformat_source_timestamp, utc_now_iso
from pmkt.data.types import parse_float, parse_int

POLYMARKET_PRICES_HISTORY_DOCS = (
    "https://docs.polymarket.com/api-reference/markets/get-prices-history"
)
POLYMARKET_BATCH_PRICES_HISTORY_DOCS = (
    "https://docs.polymarket.com/api-reference/markets/get-batch-prices-history"
)
KALSHI_MARKET_CANDLESTICKS_DOCS = (
    "https://docs.kalshi.com/api-reference/market/get-market-candlesticks"
)
KALSHI_BATCH_CANDLESTICKS_DOCS = (
    "https://docs.kalshi.com/api-reference/market/batch-get-market-candlesticks"
)
KALSHI_HISTORICAL_CANDLESTICKS_DOCS = (
    "https://docs.kalshi.com/api-reference/historical/get-historical-market-candlesticks"
)
KALSHI_HISTORICAL_TRADES_DOCS = (
    "https://docs.kalshi.com/api-reference/historical/get-historical-trades"
)


@dataclass(frozen=True)
class HistoricalBackfillResult:
    run_id: str
    historical_prices: pd.DataFrame
    trades: pd.DataFrame
    gaps: pd.DataFrame
    capabilities: pd.DataFrame
    summary: dict[str, Any]


def venue_history_capability_matrix(*, checked_at_utc: str | None = None) -> pd.DataFrame:
    checked_at_utc = checked_at_utc or utc_now_iso()
    rows = [
        venue_history_capability_row(
            checked_at_utc=checked_at_utc,
            venue="polymarket",
            capability="price_history",
            endpoint="/prices-history",
            supported=True,
            evidence_role="price_context",
            auth_required=False,
            retention_note="Public CLOB endpoint; retention depends on venue availability.",
            granularity_note="interval/fidelity controlled by Polymarket CLOB API.",
            limitation_note="Returns price points, not historical topbook or depth.",
            docs_url=POLYMARKET_PRICES_HISTORY_DOCS,
        ),
        venue_history_capability_row(
            checked_at_utc=checked_at_utc,
            venue="polymarket",
            capability="batch_price_history",
            endpoint="/batch-prices-history",
            supported=True,
            evidence_role="price_context",
            auth_required=False,
            retention_note="Public CLOB endpoint; maximum 20 markets per request.",
            granularity_note="interval/fidelity controlled by Polymarket CLOB API.",
            limitation_note="Returns price points, not historical topbook or depth.",
            docs_url=POLYMARKET_BATCH_PRICES_HISTORY_DOCS,
        ),
        venue_history_capability_row(
            checked_at_utc=checked_at_utc,
            venue="polymarket",
            capability="historical_topbook",
            endpoint="/book",
            supported=False,
            evidence_role="executable_book",
            auth_required=False,
            retention_note="No documented historical topbook backfill endpoint.",
            granularity_note="Current REST snapshot only.",
            limitation_note="Use CR-10.1 recorder for forward-looking topbook evidence.",
            docs_url="https://docs.polymarket.com/api-reference/orderbook/get-order-book-summary",
        ),
        venue_history_capability_row(
            checked_at_utc=checked_at_utc,
            venue="polymarket",
            capability="historical_depth",
            endpoint="/book",
            supported=False,
            evidence_role="executable_book",
            auth_required=False,
            retention_note="No documented historical depth backfill endpoint.",
            granularity_note="Current REST snapshot only.",
            limitation_note="Use CR-10.1 recorder for forward-looking depth/topbook evidence.",
            docs_url="https://docs.polymarket.com/api-reference/orderbook/get-order-book-summary",
        ),
        venue_history_capability_row(
            checked_at_utc=checked_at_utc,
            venue="kalshi",
            capability="market_candlesticks",
            endpoint="/series/{series_ticker}/markets/{ticker}/candlesticks",
            supported=True,
            evidence_role="price_context",
            auth_required=False,
            retention_note="Live dataset candlesticks until historical cutoff.",
            granularity_note="period_interval in minutes; documented values include 1, 60, 1440.",
            limitation_note="Candles include price/bid/ask aggregates, not executable depth.",
            docs_url=KALSHI_MARKET_CANDLESTICKS_DOCS,
        ),
        venue_history_capability_row(
            checked_at_utc=checked_at_utc,
            venue="kalshi",
            capability="batch_market_candlesticks",
            endpoint="/markets/candlesticks",
            supported=True,
            evidence_role="price_context",
            auth_required=False,
            retention_note="Live dataset batch candlesticks, up to documented endpoint limits.",
            granularity_note="period_interval in minutes; returns candlesticks grouped by market.",
            limitation_note="Candles include price/bid/ask aggregates, not executable depth.",
            docs_url=KALSHI_BATCH_CANDLESTICKS_DOCS,
        ),
        venue_history_capability_row(
            checked_at_utc=checked_at_utc,
            venue="kalshi",
            capability="historical_market_candlesticks",
            endpoint="/historical/markets/{ticker}/candlesticks",
            supported=True,
            evidence_role="price_context",
            auth_required=False,
            retention_note="Historical endpoint for markets archived from the live dataset.",
            granularity_note="period_interval in minutes; documented values include 1, 60, 1440.",
            limitation_note="Candles include price aggregates, not executable order-book depth.",
            docs_url=KALSHI_HISTORICAL_CANDLESTICKS_DOCS,
        ),
        venue_history_capability_row(
            checked_at_utc=checked_at_utc,
            venue="kalshi",
            capability="historical_trades",
            endpoint="/historical/trades",
            supported=True,
            evidence_role="trade_context",
            auth_required=False,
            retention_note="Historical trade endpoint with pagination and timestamp filters.",
            granularity_note="Trade-level context, not topbook/depth state.",
            limitation_note="Trades do not prove contemporaneous executable bid/ask.",
            docs_url=KALSHI_HISTORICAL_TRADES_DOCS,
        ),
        venue_history_capability_row(
            checked_at_utc=checked_at_utc,
            venue="kalshi",
            capability="historical_topbook",
            endpoint="/markets/{ticker}/orderbook",
            supported=False,
            evidence_role="executable_book",
            auth_required=False,
            retention_note="No documented historical topbook backfill endpoint.",
            granularity_note="Current REST snapshot only.",
            limitation_note="Use CR-10.1 recorder for forward-looking topbook evidence.",
            docs_url="https://docs.kalshi.com/api-reference/market/get-market-orderbook",
        ),
        venue_history_capability_row(
            checked_at_utc=checked_at_utc,
            venue="kalshi",
            capability="historical_depth",
            endpoint="/markets/{ticker}/orderbook",
            supported=False,
            evidence_role="executable_book",
            auth_required=False,
            retention_note="No documented historical depth backfill endpoint.",
            granularity_note="Current REST snapshot only.",
            limitation_note="Use CR-10.1 recorder for forward-looking depth/topbook evidence.",
            docs_url="https://docs.kalshi.com/api-reference/market/get-market-orderbook",
        ),
    ]
    return pd.DataFrame(rows, columns=VENUE_HISTORY_CAPABILITY_COLUMNS)


async def backfill_venue_history(
    venue: str,
    instruments: Sequence[str],
    *,
    start_ts: int | None = None,
    end_ts: int | None = None,
    interval: str | None = "1d",
    fidelity: int | None = None,
    period_interval: int = 1440,
    series_ticker: str | None = None,
    use_historical: bool = False,
    include_trades: bool = True,
    max_trade_pages: int = 10,
    trade_limit: int = 1000,
    polymarket_client: Any | None = None,
    kalshi_client: Any | None = None,
    run_id: str | None = None,
    checked_at_utc: str | None = None,
) -> HistoricalBackfillResult:
    venue_normalized = venue.strip().lower()
    if venue_normalized not in {"polymarket", "kalshi"}:
        raise ValueError("venue must be 'polymarket' or 'kalshi'")
    instrument_ids = _normalize_instruments(instruments)
    if venue_normalized == "kalshi" and (start_ts is None or end_ts is None):
        raise ValueError("Kalshi candlestick backfill requires start_ts and end_ts")

    run_id = run_id or f"historical-{uuid.uuid4().hex[:12]}"
    checked_at_utc = checked_at_utc or utc_now_iso()
    capabilities = venue_history_capability_matrix(checked_at_utc=checked_at_utc)
    price_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    gap_rows = _unsupported_book_gap_rows(
        run_id=run_id,
        exchange=venue_normalized,
        instruments=instrument_ids,
        start_ts=start_ts,
        end_ts=end_ts,
    )

    if venue_normalized == "polymarket":
        rows, fetch_gaps = await _backfill_polymarket_prices(
            polymarket_client,
            instrument_ids,
            run_id=run_id,
            start_ts=start_ts,
            end_ts=end_ts,
            interval=interval,
            fidelity=fidelity,
        )
        price_rows.extend(rows)
        gap_rows.extend(fetch_gaps)
    else:
        rows, fetch_gaps = await _backfill_kalshi_candles(
            kalshi_client,
            instrument_ids,
            run_id=run_id,
            start_ts=int(start_ts or 0),
            end_ts=int(end_ts or 0),
            period_interval=period_interval,
            series_ticker=series_ticker,
            use_historical=use_historical,
        )
        price_rows.extend(rows)
        gap_rows.extend(fetch_gaps)
        if include_trades:
            rows, trade_gaps = await _backfill_kalshi_trades(
                kalshi_client,
                instrument_ids,
                run_id=run_id,
                start_ts=start_ts,
                end_ts=end_ts,
                max_pages=max_trade_pages,
                limit=trade_limit,
            )
            trade_rows.extend(rows)
            gap_rows.extend(trade_gaps)

    prices = pd.DataFrame(price_rows, columns=HISTORICAL_PRICE_COLUMNS)
    trades = pd.DataFrame(trade_rows, columns=TRADE_COLUMNS)
    gaps = pd.DataFrame(gap_rows, columns=HISTORICAL_BACKFILL_GAP_COLUMNS)
    summary = {
        "run_id": run_id,
        "venue": venue_normalized,
        "instrument_count": len(instrument_ids),
        "historical_price_rows": len(prices),
        "trade_rows": len(trades),
        "gap_rows": len(gaps),
        "start_ts": start_ts,
        "end_ts": end_ts,
        "interval": interval,
        "fidelity": fidelity,
        "period_interval": period_interval,
        "series_ticker": series_ticker,
        "use_historical": use_historical,
        "include_trades": include_trades,
    }
    return HistoricalBackfillResult(
        run_id=run_id,
        historical_prices=prices,
        trades=trades,
        gaps=gaps,
        capabilities=capabilities,
        summary=summary,
    )


def write_historical_backfill_result(
    result: HistoricalBackfillResult,
    out_dir: str | Path,
) -> dict[str, Path]:
    output = Path(out_dir)
    output.mkdir(parents=True, exist_ok=True)
    paths = {
        "historical_prices": write_parquet(
            result.historical_prices,
            output / "historical_prices.parquet",
            schema="historical_price.v1",
            coerce=True,
            validate=True,
            strict=True,
        ),
        "trades": write_parquet(
            result.trades,
            output / "trades.parquet",
            schema="trade.v1",
            coerce=True,
            validate=True,
            strict=True,
        ),
        "historical_backfill_gaps": write_parquet(
            result.gaps,
            output / "historical_backfill_gaps.parquet",
            schema="historical_backfill_gap.v1",
            coerce=True,
            validate=True,
            strict=True,
        ),
        "venue_history_capabilities": write_parquet(
            result.capabilities,
            output / "venue_history_capabilities.parquet",
            schema="venue_history_capability.v1",
            coerce=True,
            validate=True,
            strict=True,
        ),
    }
    summary_path = output / "summary.json"
    summary_path.write_text(
        json.dumps(result.summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    paths["summary"] = summary_path
    return paths


async def _backfill_polymarket_prices(
    client: Any,
    instruments: Sequence[str],
    *,
    run_id: str,
    start_ts: int | None,
    end_ts: int | None,
    interval: str | None,
    fidelity: int | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    for instrument_id in instruments:
        try:
            payload = await client.prices_history(
                instrument_id,
                interval=interval,
                fidelity=fidelity,
                start_ts=start_ts,
                end_ts=end_ts,
            )
        except Exception as exc:
            gaps.append(
                _gap_row(
                    run_id=run_id,
                    exchange="polymarket",
                    instrument_id=instrument_id,
                    capability="price_history",
                    start_ts=start_ts,
                    end_ts=end_ts,
                    status="fetch_error",
                    reason=str(exc),
                    endpoint="/prices-history",
                )
            )
            continue
        rows.extend(
            _polymarket_history_rows(
                payload,
                collector_run_id=run_id,
                instrument_id=instrument_id,
                fidelity=fidelity,
            )
        )
    return rows, gaps


async def _backfill_kalshi_candles(
    client: Any,
    instruments: Sequence[str],
    *,
    run_id: str,
    start_ts: int,
    end_ts: int,
    period_interval: int,
    series_ticker: str | None,
    use_historical: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    for ticker in instruments:
        endpoint = (
            "/historical/markets/{ticker}/candlesticks"
            if use_historical
            else "/series/{series_ticker}/markets/{ticker}/candlesticks"
        )
        try:
            if use_historical:
                payload = await client.historical_market_candlesticks(
                    ticker,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    period_interval=period_interval,
                )
            else:
                if not series_ticker:
                    payload = await client.batch_market_candlesticks(
                        [ticker],
                        start_ts=start_ts,
                        end_ts=end_ts,
                        period_interval=period_interval,
                    )
                    endpoint = "/markets/candlesticks"
                else:
                    payload = await client.market_candlesticks(
                        series_ticker,
                        ticker,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        period_interval=period_interval,
                    )
        except Exception as exc:
            gaps.append(
                _gap_row(
                    run_id=run_id,
                    exchange="kalshi",
                    instrument_id=ticker,
                    capability="historical_market_candlesticks"
                    if use_historical
                    else "market_candlesticks",
                    start_ts=start_ts,
                    end_ts=end_ts,
                    status="fetch_error",
                    reason=str(exc),
                    endpoint=endpoint,
                )
            )
            continue
        price_rows, parse_gaps = _kalshi_candle_rows(
            payload,
            collector_run_id=run_id,
            run_id=run_id,
            market_ticker=ticker,
            endpoint=endpoint,
            period_interval=period_interval,
        )
        rows.extend(price_rows)
        gaps.extend(parse_gaps)
    return rows, gaps


async def _backfill_kalshi_trades(
    client: Any,
    instruments: Sequence[str],
    *,
    run_id: str,
    start_ts: int | None,
    end_ts: int | None,
    max_pages: int,
    limit: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    for ticker in instruments:
        cursor: str | None = None
        pages = 0
        while True:
            if pages >= max_pages:
                break
            try:
                payload = await client.historical_trades(
                    ticker=ticker.split(":", 1)[0],
                    min_ts=start_ts,
                    max_ts=end_ts,
                    limit=limit,
                    cursor=cursor,
                )
            except Exception as exc:
                gaps.append(
                    _gap_row(
                        run_id=run_id,
                        exchange="kalshi",
                        instrument_id=ticker,
                        capability="historical_trades",
                        start_ts=start_ts,
                        end_ts=end_ts,
                        status="fetch_error",
                        reason=str(exc),
                        endpoint="/historical/trades",
                    )
                )
                break
            pages += 1
            page_trades = _kalshi_trade_rows(
                payload,
                collector_run_id=run_id,
                default_ticker=ticker.split(":", 1)[0],
            )
            rows.extend(page_trades)
            cursor_value = _as_mapping(payload).get("cursor")
            cursor = str(cursor_value).strip() if cursor_value else None
            if not cursor:
                break
    return rows, gaps


def _polymarket_history_rows(
    payload: Any,
    *,
    collector_run_id: str,
    instrument_id: str,
    fidelity: int | None,
) -> list[dict[str, Any]]:
    points: list[tuple[int, float, Any]] = []
    for point in _extract_history(payload):
        item = _as_mapping(point)
        ts = parse_int(_first_present(item, "t", "timestamp", "time"))
        price = parse_float(_first_present(item, "p", "price", "value"))
        if ts is None or price is None:
            continue
        points.append((ts, price, item))
    interval_seconds = _polymarket_sample_interval_seconds(
        fidelity, [ts for ts, _, _ in points]
    )
    rows: list[dict[str, Any]] = []
    for ts, price, item in points:
        rows.append(
            historical_price_row(
                collector_run_id=collector_run_id,
                exchange="polymarket",
                venue_market_id=instrument_id,
                instrument_id=instrument_id,
                source="prices_history",
                endpoint="/prices-history",
                evidence_role="price_context",
                observed_at_utc=isoformat_source_timestamp(ts, epoch_unit="seconds"),
                interval_seconds=interval_seconds,
                price_type="last",
                close_dollars=price,
                raw_json=item,
            )
        )
    return rows


def _kalshi_trade_rows(
    payload: Any,
    *,
    collector_run_id: str,
    default_ticker: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in _as_mapping(payload).get("trades") or []:
        trade = _as_mapping(item)
        if not trade:
            continue
        ticker = str(_first_present(trade, "ticker", "market_ticker") or default_ticker)
        trade_id = str(
            _first_present(
                trade,
                "trade_id",
                "id",
                "external_id",
                "created_time",
                "created_ts",
                "ts",
            )
            or ""
        ).strip()
        price = parse_float(
            _first_present(
                trade,
                "yes_price_dollars",
                "price_dollars",
                "price",
                "yes_price",
            )
        )
        size = parse_float(
            _first_present(
                trade,
                "count",
                "count_fp",
                "size",
                "quantity",
                "contracts",
            )
        )
        ts = _first_present(trade, "created_time", "created_ts", "ts", "trade_ts")
        trade_ts = isoformat_source_timestamp(ts, epoch_unit="seconds") or (
            str(ts) if ts is not None else None
        )
        if not trade_id:
            trade_id = _stable_trade_id(ticker, trade_ts, price, size, trade)
        rows.append(
            trade_row(
                venue="kalshi",
                venue_trade_id=trade_id,
                venue_market_id=ticker,
                instrument_id=f"{ticker}:YES",
                outcome="YES",
                trade_ts_utc=trade_ts,
                received_at_utc=utc_now_iso(),
                price_dollars=price,
                size_contracts=size,
                notional_dollars=price * size if price is not None and size is not None else None,
                aggressor_side=_first_present(trade, "taker_side", "side", "action"),
                raw_json=trade,
            )
        )
    return rows


def _kalshi_candle_rows(
    payload: Any,
    *,
    collector_run_id: str,
    run_id: str,
    market_ticker: str,
    endpoint: str,
    period_interval: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    for ticker, candle in _iter_kalshi_candles(payload, default_ticker=market_ticker):
        end_ts = parse_int(candle.get("end_period_ts") or candle.get("period_end_ts"))
        observed_at_utc = (
            isoformat_source_timestamp(end_ts, epoch_unit="seconds")
            if end_ts is not None
            else None
        )
        row_count_before = len(rows)
        for price_type in ("yes_bid", "yes_ask", "price"):
            ohlc = candle.get(price_type)
            if not isinstance(ohlc, Mapping):
                continue
            open_dollars = _kalshi_price_value(ohlc, "open")
            high_dollars = _kalshi_price_value(ohlc, "high")
            low_dollars = _kalshi_price_value(ohlc, "low")
            close_dollars = _kalshi_price_value(ohlc, "close")
            mean_dollars = _kalshi_price_value(ohlc, "mean")
            min_dollars = _kalshi_price_value(ohlc, "min")
            max_dollars = _kalshi_price_value(ohlc, "max")
            previous_dollars = _kalshi_price_value(ohlc, "previous", "previous_price")
            if all(
                value is None
                for value in (
                    open_dollars,
                    high_dollars,
                    low_dollars,
                    close_dollars,
                    mean_dollars,
                    min_dollars,
                    max_dollars,
                    previous_dollars,
                )
            ):
                continue
            rows.append(
                historical_price_row(
                    collector_run_id=collector_run_id,
                    exchange="kalshi",
                    venue_market_id=ticker,
                    instrument_id=f"{ticker}:YES",
                    outcome="YES",
                    source="candlesticks",
                    endpoint=endpoint,
                    evidence_role="price_context",
                    observed_at_utc=observed_at_utc,
                    end_ts_utc=observed_at_utc,
                    interval_seconds=int(period_interval) * 60,
                    price_type=price_type,
                    open_dollars=open_dollars,
                    high_dollars=high_dollars,
                    low_dollars=low_dollars,
                    close_dollars=close_dollars,
                    mean_dollars=mean_dollars,
                    min_dollars=min_dollars,
                    max_dollars=max_dollars,
                    previous_dollars=previous_dollars,
                    volume_contracts=parse_float(candle.get("volume_fp") or candle.get("volume")),
                    open_interest_contracts=parse_float(
                        candle.get("open_interest_fp") or candle.get("open_interest")
                    ),
                    raw_json=candle,
                )
            )
        if end_ts is not None and len(rows) == row_count_before:
            gaps.append(
                _gap_row(
                    run_id=run_id,
                    exchange="kalshi",
                    instrument_id=f"{ticker}:YES",
                    capability=_kalshi_candle_capability(endpoint),
                    start_ts=end_ts,
                    end_ts=end_ts,
                    status="empty_response",
                    reason="candlestick_had_no_parseable_ohlc_price_fields",
                    endpoint=endpoint,
                    raw_json=candle,
                )
            )
    return rows, gaps


def _kalshi_candle_capability(endpoint: str) -> str:
    if endpoint.startswith("/historical/"):
        return "historical_market_candlesticks"
    return "market_candlesticks"


def _unsupported_book_gap_rows(
    *,
    run_id: str,
    exchange: str,
    instruments: Sequence[str],
    start_ts: int | None,
    end_ts: int | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for instrument_id in instruments:
        for capability in ("historical_topbook", "historical_depth"):
            endpoint = "/book" if exchange == "polymarket" else "/markets/{ticker}/orderbook"
            rows.append(
                _gap_row(
                    run_id=run_id,
                    exchange=exchange,
                    instrument_id=instrument_id,
                    capability=capability,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    status="unsupported",
                    reason="venue_does_not_offer_native_historical_orderbook_backfill",
                    endpoint=endpoint,
                )
            )
    return rows


def _gap_row(
    *,
    run_id: str,
    exchange: str,
    instrument_id: str,
    capability: str,
    start_ts: int | None,
    end_ts: int | None,
    status: str,
    reason: str,
    endpoint: str,
    row_count: int = 0,
    raw_json: Any | None = None,
) -> dict[str, Any]:
    venue_market_id = instrument_id.split(":", 1)[0] if exchange == "kalshi" else instrument_id
    return historical_backfill_gap_row(
        run_id=run_id,
        observed_at_utc=utc_now_iso(),
        exchange=exchange,
        venue_market_id=venue_market_id,
        instrument_id=instrument_id,
        capability=capability,
        start_ts_utc=isoformat_source_timestamp(start_ts, epoch_unit="seconds"),
        end_ts_utc=isoformat_source_timestamp(end_ts, epoch_unit="seconds"),
        status=status,
        reason=reason,
        endpoint=endpoint,
        row_count=row_count,
        raw_json=raw_json,
    )


def _iter_kalshi_candles(
    payload: Any,
    *,
    default_ticker: str,
) -> Iterable[tuple[str, Mapping[str, Any]]]:
    data = _as_mapping(payload)
    markets = data.get("markets")
    if isinstance(markets, list):
        for market in markets:
            market_payload = _as_mapping(market)
            ticker = str(market_payload.get("market_ticker") or default_ticker)
            for candle in market_payload.get("candlesticks") or []:
                candle_payload = _as_mapping(candle)
                if candle_payload:
                    yield ticker, candle_payload
        return
    for candle in data.get("candlesticks") or []:
        candle_payload = _as_mapping(candle)
        if candle_payload:
            yield default_ticker, candle_payload


def _extract_history(payload: Any) -> list[Any]:
    data = _as_mapping(payload)
    history = data.get("history")
    return history if isinstance(history, list) else []


def _as_mapping(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        value = value.model_dump()
    elif hasattr(value, "dict"):
        value = value.dict()
    return dict(value) if isinstance(value, Mapping) else {}


def _first_present(payload: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None


def _kalshi_price_value(payload: Mapping[str, Any], field: str, *aliases: str) -> float | None:
    keys = (f"{field}_dollars", field, *(f"{alias}_dollars" for alias in aliases), *aliases)
    value = parse_float(_first_present(payload, *keys))
    # Kalshi candle OHLC are probability prices in [0, 1] dollars. A bare-key
    # fallback can resurface a native integer-cents value (e.g. 41 for $0.41);
    # reject out-of-range values so they surface as a gap rather than a 100x row.
    if value is None or not 0.0 <= value <= 1.0:
        return None
    return value


def _stable_trade_id(
    ticker: str,
    trade_ts: str | None,
    price: float | None,
    size: float | None,
    payload: Mapping[str, Any],
) -> str:
    encoded = json.dumps(
        {
            "ticker": ticker,
            "trade_ts": trade_ts,
            "price": price,
            "size": size,
            "payload": payload,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _polymarket_sample_interval_seconds(
    fidelity: int | None, timestamps: Sequence[int] | None = None
) -> int | None:
    if fidelity is not None:
        return int(fidelity) * 60
    if timestamps:
        ordered = sorted(timestamps)
        deltas = sorted(b - a for a, b in zip(ordered, ordered[1:]) if b > a)
        if deltas:
            return deltas[len(deltas) // 2]
    return None


def _normalize_instruments(instruments: Sequence[str]) -> list[str]:
    unique: list[str] = []
    for instrument in instruments:
        text = str(instrument).strip()
        if text and text not in unique:
            unique.append(text)
    if not unique:
        raise ValueError("instruments must contain at least one id")
    return unique


__all__ = [
    "HistoricalBackfillResult",
    "backfill_venue_history",
    "venue_history_capability_matrix",
    "write_historical_backfill_result",
]
