from __future__ import annotations

import hashlib
import json
from typing import Any

from pmkt.data.canonical import kalshi_market_snapshot_row
from pmkt.data.prices import complement_probability as _price_complement
from pmkt.data.types import parse_float as _parse_float


KALSHI_MARKET_STATUSES = frozenset(
    {
        "initialized",
        "active",
        "inactive",
        "closed",
        "determined",
        "disputed",
        "amended",
        "finalized",
    }
)
KALSHI_MARKET_QUERY_STATUS_RESPONSES = {
    "unopened": frozenset({"initialized"}),
    "open": frozenset({"active"}),
    "paused": frozenset({"inactive"}),
    "closed": frozenset({"closed", "determined", "disputed", "amended"}),
    "settled": frozenset({"finalized"}),
}
KALSHI_CLOSED_MARKET_STATUSES = frozenset(
    {"closed", "determined", "disputed", "amended", "finalized"}
)
KALSHI_PROVISIONAL_MARKET_STATUSES = frozenset(
    {"determined", "disputed", "amended"}
)
_KALSHI_LEGACY_STATUS_ALIASES = {
    "unopened": "initialized",
    "open": "active",
    "paused": "inactive",
    "settled": "finalized",
}
_KALSHI_LIFECYCLE_EVENT_STATUSES = {
    "created": "initialized",
    "activated": "active",
    "deactivated": "inactive",
    "determined": "determined",
    "settled": "finalized",
}


def normalize_kalshi_market_status(value: Any) -> str | None:
    """Return the canonical REST market status, accepting legacy filter aliases.

    Kalshi's ``GET /markets?status=`` query uses ``open``/``unopened`` while
    response objects use ``active``/``initialized``.  Keeping that distinction
    in one helper prevents query vocabulary from leaking into durable rows.
    Unknown non-empty values are preserved (case-folded) so callers can fail or
    report an upstream addition instead of silently treating it as active.
    """
    if value is None:
        return None
    normalized = str(value).strip().casefold()
    if not normalized:
        return None
    return _KALSHI_LEGACY_STATUS_ALIASES.get(normalized, normalized)


def kalshi_market_matches_query_status(value: Any, query_status: str) -> bool:
    """Whether a response status belongs to a documented REST query filter."""
    query = str(query_status).strip().casefold()
    try:
        expected = KALSHI_MARKET_QUERY_STATUS_RESPONSES[query]
    except KeyError as exc:
        known = ", ".join(sorted(KALSHI_MARKET_QUERY_STATUS_RESPONSES))
        raise ValueError(
            f"unsupported Kalshi market query status {query_status!r}; known: {known}"
        ) from exc
    return normalize_kalshi_market_status(value) in expected


def kalshi_status_from_lifecycle_event(
    event_type: Any,
    *,
    explicit_status: Any = None,
) -> str | None:
    """Project a lifecycle WebSocket event to a canonical REST status when known.

    Explicit source status wins.  ``close_date_updated`` and
    ``metadata_updated`` deliberately return ``None`` without an explicit status
    because those events do not uniquely identify a lifecycle state.
    """
    normalized = normalize_kalshi_market_status(explicit_status)
    if normalized is not None:
        return normalized
    event = str(event_type or "").strip().casefold()
    return _KALSHI_LIFECYCLE_EVENT_STATUSES.get(event)


def kalshi_market_is_closed(value: Any) -> bool:
    return normalize_kalshi_market_status(value) in KALSHI_CLOSED_MARKET_STATUSES


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or set(text) <= {":"}:
        return None
    return " ".join(text.split())


def _market_strike_text(market: dict[str, Any]) -> str | None:
    for key in ("yes_sub_title", "no_sub_title", "subtitle", "sub_title"):
        text = _clean_text(market.get(key))
        if text:
            return text
    custom_strike = market.get("custom_strike")
    if isinstance(custom_strike, dict):
        for value in custom_strike.values():
            text = _clean_text(value)
            if text:
                return text
    return None


def _market_question(market: dict[str, Any], ticker: Any) -> str | None:
    question = _clean_text(market.get("title")) or _clean_text(ticker)
    strike = _market_strike_text(market)
    if question and strike and strike.lower() not in question.lower():
        return f"{question} - {strike}"
    return question or strike


def _first_value(market: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key not in market:
            continue
        value = market.get(key)
        if value not in (None, ""):
            return value
    return None


def _first_text(market: dict[str, Any], *keys: str) -> str | None:
    value = _first_value(market, *keys)
    return None if value is None else str(value)


def _parse_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return None


def normalize_kalshi_market(market: dict[str, Any]) -> dict[str, Any]:
    """Normalize one Kalshi market payload without importing exchange clients."""
    ticker = market.get("ticker")
    yes_bid = _parse_float(market.get("yes_bid_dollars"))
    yes_ask = _parse_float(market.get("yes_ask_dollars"))
    no_bid = _parse_float(market.get("no_bid_dollars"))
    no_ask = _parse_float(market.get("no_ask_dollars"))
    if yes_ask is None:
        yes_ask = _price_complement(no_bid)
    if no_ask is None:
        no_ask = _price_complement(yes_bid)
    mid = (
        (yes_bid + yes_ask) / 2.0
        if yes_bid is not None and yes_ask is not None
        else None
    )
    spread = yes_ask - yes_bid if yes_bid is not None and yes_ask is not None else None
    status = normalize_kalshi_market_status(market.get("status"))
    is_provisional = _parse_bool(
        _first_value(market, "is_provisional", "isProvisional")
    )
    if is_provisional is None:
        is_provisional = status in KALSHI_PROVISIONAL_MARKET_STATUSES
    category = (
        market.get("category")
        or market.get("series_category")
        or market.get("event_category")
    )
    question = _market_question(market, ticker)
    raw_json = json.dumps(market, ensure_ascii=True, sort_keys=True, default=str)
    return kalshi_market_snapshot_row(
        exchange="kalshi",
        market_key=str(ticker) if ticker is not None else None,
        instrument_key=f"{ticker}:YES" if ticker is not None else None,
        ticker=ticker,
        event_ticker=market.get("event_ticker"),
        question=question,
        title=market.get("title"),
        subtitle=market.get("subtitle"),
        category=category,
        series_ticker=market.get("series_ticker"),
        close_time=market.get("close_time") or market.get("expiration_time"),
        status=status,
        closed=kalshi_market_is_closed(status),
        fee_type=market.get("fee_type"),
        fee_multiplier=_parse_float(market.get("fee_multiplier")),
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        mid=mid,
        spread=spread,
        volume=_parse_float(market.get("volume_fp")),
        volume_24h=_parse_float(market.get("volume_24h_fp")),
        liquidity=_parse_float(market.get("liquidity_dollars")),
        open_interest=_parse_float(market.get("open_interest_fp")),
        last_price=_parse_float(market.get("last_price_dollars")),
        open_time=market.get("open_time"),
        updated_time=market.get("updated_time"),
        result=_first_text(market, "result", "settlement_result", "settlementResult"),
        settlement_value_dollars=_first_text(
            market,
            "settlement_value_dollars",
            "settlementValueDollars",
            "settlement_value",
            "settlementValue",
        ),
        settlement_ts=_first_text(
            market,
            "settlement_ts",
            "settlementTime",
            "settlement_time",
            "settledTime",
            "settled_time",
        ),
        expiration_value=_first_text(market, "expiration_value", "expirationValue"),
        is_provisional=is_provisional,
        rules_primary=_first_text(
            market,
            "rules_primary",
            "rulesPrimary",
            "rules",
            "market_rules",
            "marketRules",
        ),
        rules_secondary=_first_text(market, "rules_secondary", "rulesSecondary"),
        raw_json=raw_json,
        raw_json_sha256=hashlib.sha256(raw_json.encode("utf-8")).hexdigest(),
    )


__all__ = [
    "KALSHI_CLOSED_MARKET_STATUSES",
    "KALSHI_MARKET_QUERY_STATUS_RESPONSES",
    "KALSHI_MARKET_STATUSES",
    "KALSHI_PROVISIONAL_MARKET_STATUSES",
    "kalshi_market_is_closed",
    "kalshi_market_matches_query_status",
    "kalshi_status_from_lifecycle_event",
    "normalize_kalshi_market",
    "normalize_kalshi_market_status",
]
