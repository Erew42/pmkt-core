from __future__ import annotations

import hashlib
import json
from typing import Any

from pmkt.data.canonical import (
    POLYMARKET_MARKET_SNAPSHOT_COLUMNS,
    polymarket_market_snapshot_row,
)
from pmkt.data.types import parse_float as _shared_parse_float
from pmkt.tokens import extract_token_ids

VOLUME_KEYS = (
    "volume",
    "volume24h",
    "volume24hr",
    "volume_24h",
    "volume_24hr",
    "volumeUsd",
    "volumeUSD",
    "volume24hUsd",
    "volume24hrUsd",
)
LIQUIDITY_KEYS = (
    "liquidity",
    "liquidityNum",
    "liquidityUsd",
    "liquidityUSD",
)
BEST_BID_KEYS = ("best_bid", "bestBid", "yes_bid")
BEST_ASK_KEYS = ("best_ask", "bestAsk", "yes_ask")
LAST_TRADE_PRICE_KEYS = ("last_trade_price", "lastTradePrice", "last_price")
OUTCOME_PRICE_KEYS = ("outcome_prices", "outcomePrices")
CONDITION_ID_KEYS = ("condition_id", "conditionId", "conditionID")
QUESTION_ID_KEYS = ("question_id", "questionId", "questionID")
UMA_RESOLUTION_STATUS_KEYS = (
    "uma_resolution_status",
    "umaResolutionStatus",
    "resolutionStatus",
)
RESOLVED_BY_KEYS = ("resolved_by", "resolvedBy")
RESOLUTION_SOURCE_KEYS = ("resolution_source", "resolutionSource")
CLOSE_TIME_KEYS = (
    "closeTime",
    "close_time",
    "closeDate",
    "close_date",
    "endDate",
    "endDateIso",
    "end_date",
    "endTime",
    "end_time",
    "resolutionTime",
    "resolution_time",
    "resolveTime",
    "resolve_time",
    "expiresAt",
    "expires_at",
    "closedTime",
)
OPEN_TIME_KEYS = (
    "acceptingOrdersTimestamp",
    "accepting_orders_timestamp",
    "openTime",
    "open_time",
    "startDate",
    "start_date",
    "startDateIso",
    "start_date_iso",
    "createdAt",
    "created_at",
)
TOP_LEVEL_START_TIME_KEYS = (
    "eventStartTime",
    "event_start_time",
    "gameStartTime",
    "game_start_time",
    "startTime",
    "start_time",
    "eventDate",
    "event_date",
)
NESTED_EVENT_START_TIME_KEYS = (
    "startTime",
    "start_time",
    "eventStartTime",
    "event_start_time",
    "gameStartTime",
    "game_start_time",
    "eventDate",
    "event_date",
)
START_TIME_FALLBACK_KEYS = (
    "startDate",
    "start_date",
    "startDateIso",
    "start_date_iso",
)


def parse_float(value: Any) -> float | None:
    return _shared_parse_float(value)


def extract_event_id(market: dict[str, Any]) -> str | None:
    for key in ("event_id", "eventId", "eventID"):
        value = market.get(key)
        if value is not None:
            return str(value)
    event = market.get("event")
    if isinstance(event, dict):
        for key in ("id", "event_id", "eventId"):
            value = event.get(key)
            if value is not None:
                return str(value)
    events = market.get("events")
    if isinstance(events, list) and events:
        first = events[0]
        if isinstance(first, dict):
            for key in ("id", "event_id", "eventId"):
                value = first.get(key)
                if value is not None:
                    return str(value)
    return None


def extract_event_slug(market: dict[str, Any]) -> str | None:
    for key in ("event_slug", "eventSlug"):
        value = market.get(key)
        if isinstance(value, str) and value:
            return value
    event = market.get("event")
    if isinstance(event, dict):
        value = event.get("slug")
        if isinstance(value, str) and value:
            return value
    events = market.get("events")
    if isinstance(events, list) and events:
        first = events[0]
        if isinstance(first, dict):
            value = first.get("slug")
            if isinstance(value, str) and value:
                return value
    return None


def extract_close_time(market: dict[str, Any]) -> Any:
    return _first_nested_time(market, CLOSE_TIME_KEYS)


def _time_value(payload: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key not in payload:
            continue
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


def _event_payloads(market: dict[str, Any]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    event = market.get("event")
    if isinstance(event, dict):
        payloads.append(event)
    events = market.get("events")
    if isinstance(events, list):
        payloads.extend(item for item in events if isinstance(item, dict))
    return payloads


def _first_event_time(market: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for event in _event_payloads(market):
        value = _time_value(event, keys)
        if value is not None:
            return value
    return None


def _first_nested_time(market: dict[str, Any], keys: tuple[str, ...]) -> Any:
    value = _time_value(market, keys)
    if value is not None:
        return value
    value = _first_event_time(market, keys)
    if value is not None:
        return value
    return None


def extract_open_time(market: dict[str, Any]) -> Any:
    return _first_nested_time(market, OPEN_TIME_KEYS)


def extract_start_time(market: dict[str, Any]) -> Any:
    value = _time_value(market, TOP_LEVEL_START_TIME_KEYS)
    if value is not None:
        return value
    value = _first_event_time(market, NESTED_EVENT_START_TIME_KEYS)
    if value is not None:
        return value
    value = _time_value(market, START_TIME_FALLBACK_KEYS)
    if value is not None:
        return value
    return _first_event_time(market, START_TIME_FALLBACK_KEYS)


def extract_enable_orderbook(market: dict[str, Any]) -> bool | None:
    for key in ("enableOrderBook", "enable_orderbook", "enable_order_book"):
        if key in market:
            return _normalize_bool(market.get(key))
    tokens = market.get("clobTokenIds") or market.get("clob_token_ids")
    if isinstance(tokens, str):
        raw = tokens.strip()
        if raw.startswith("[") or raw.startswith("{"):
            try:
                tokens = json.loads(raw)
            except json.JSONDecodeError:
                tokens = None
    if isinstance(tokens, list):
        return bool(tokens)
    return None


def first_numeric(payload: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        if key not in payload:
            continue
        parsed = parse_float(payload.get(key))
        if parsed is not None:
            return parsed
    return None


def first_present(payload: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key not in payload:
            continue
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


def first_text(payload: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    value = first_present(payload, keys)
    return None if value is None else str(value)


def _json_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            return []
        return decoded if isinstance(decoded, list) else []
    return []


def extract_outcome_labels(market: dict[str, Any]) -> list[str]:
    labels: list[str] = []
    for outcome in _json_array(market.get("outcomes")):
        label = _outcome_label(outcome)
        if label is not None:
            labels.append(label)
    return labels


def extract_outcome_prices(market: dict[str, Any]) -> list[Any]:
    for key in OUTCOME_PRICE_KEYS:
        prices = _json_array(market.get(key))
        if prices:
            return prices
    return []


def _outcome_label(value: Any) -> str | None:
    if isinstance(value, str):
        return value.strip().lower()
    if isinstance(value, dict):
        for key in ("name", "title", "label", "outcome"):
            label = value.get(key)
            if isinstance(label, str) and label.strip():
                return label.strip().lower()
    return None


def extract_yes_outcome_price(market: dict[str, Any]) -> float | None:
    prices = extract_outcome_prices(market)
    if not prices:
        return None

    outcomes = extract_outcome_labels(market)
    for idx, outcome in enumerate(outcomes):
        if outcome == "yes" and idx < len(prices):
            return parse_float(prices[idx])

    if len(prices) == 2:
        return parse_float(prices[0])
    return None


def extract_mid_price(market: dict[str, Any]) -> float | None:
    bid = first_numeric(market, BEST_BID_KEYS)
    ask = first_numeric(market, BEST_ASK_KEYS)
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    outcome_price = extract_yes_outcome_price(market)
    if outcome_price is not None:
        return outcome_price
    return first_numeric(market, LAST_TRADE_PRICE_KEYS)


def _spread_from_best_quotes(market: dict[str, Any]) -> float | None:
    bid = first_numeric(market, BEST_BID_KEYS)
    ask = first_numeric(market, BEST_ASK_KEYS)
    if bid is None or ask is None:
        return None
    return ask - bid


def _normalize_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
    return None


def _stable_raw_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, default=str)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def extract_market_rows(markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for market in markets:
        if not isinstance(market, dict):
            continue
        market_id = market.get("id") or market.get("market_id")
        if market_id is None:
            continue
        market_id_str = str(market_id)
        token_ids = [token for token in extract_token_ids(market) if token != market_id_str]
        raw_json = _stable_raw_json(market)
        rows.append(
            polymarket_market_snapshot_row(
                market_id=market_id_str,
                event_id=extract_event_id(market),
                event_slug=extract_event_slug(market),
                slug=market.get("slug"),
                question=market.get("question") or market.get("title"),
                open_time=extract_open_time(market),
                start_time=extract_start_time(market),
                close_time=extract_close_time(market),
                closed=_normalize_bool(market.get("closed")),
                volume=first_numeric(market, VOLUME_KEYS),
                liquidity=first_numeric(market, LIQUIDITY_KEYS),
                enable_orderbook=extract_enable_orderbook(market),
                token_ids=token_ids,
                yes_bid=first_numeric(market, BEST_BID_KEYS),
                yes_ask=first_numeric(market, BEST_ASK_KEYS),
                mid=extract_mid_price(market),
                spread=_spread_from_best_quotes(market),
                last_trade_price=first_numeric(market, LAST_TRADE_PRICE_KEYS),
                condition_id=first_text(market, CONDITION_ID_KEYS),
                question_id=first_text(market, QUESTION_ID_KEYS),
                outcome_labels_json=extract_outcome_labels(market),
                outcome_prices_json=extract_outcome_prices(market),
                uma_resolution_status=first_text(market, UMA_RESOLUTION_STATUS_KEYS),
                resolved_by=first_text(market, RESOLVED_BY_KEYS),
                resolution_source=first_text(market, RESOLUTION_SOURCE_KEYS),
                raw_json=raw_json,
                raw_json_sha256=_sha256_text(raw_json),
            )
        )
    return rows


def markets_dataframe(markets: list[dict[str, Any]]):
    import pandas as pd

    rows = extract_market_rows(markets)
    df = pd.DataFrame(rows, columns=POLYMARKET_MARKET_SNAPSHOT_COLUMNS)
    if not df.empty and "market_id" in df.columns:
        df = df.sort_values("market_id")
    return df
