from __future__ import annotations

import time
from typing import Any

from pmkt.data.books import PriceLevel, best_ask, best_bid, compute_topbook, parse_levels
from pmkt.data.kalshi_quotes import (
    KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT,
    resolve_kalshi_quote_normalization_policy,
)
from pmkt.data.prices import complement_probability
from pmkt.data.schemas import topbook_row
from pmkt.data.time import isoformat_source_timestamp
from pmkt.data.time import utc_now_iso
from pmkt.data.types import parse_float as _parse_float


def _as_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, "as_dict"):
        value = value.as_dict()
    elif hasattr(value, "model_dump"):
        value = value.model_dump()
    elif hasattr(value, "dict"):
        value = value.dict()
    return value if isinstance(value, dict) else {}


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _current_kalshi_ask_size(
    payload: dict[str, Any],
    *,
    explicit_field: str,
    fallback_field: str,
) -> float | None:
    """Prefer a v2 explicit ask size without hiding malformed evidence.

    Absence permits the documented opposite-bid compatibility fallback. A
    present explicit value must be a finite positive size; otherwise falling
    back would make corrupted current-policy input look like valid legacy data.
    """

    if explicit_field not in payload or payload[explicit_field] is None:
        return _parse_float(payload.get(fallback_field))
    value = payload[explicit_field]
    parsed = _parse_float(value)
    if parsed is None or parsed <= 0:
        raise ValueError(
            f"Kalshi {explicit_field} must be a finite positive size when present; "
            f"got {value!r}"
        )
    return parsed


def _utc_now() -> str:
    return utc_now_iso()


def _direct_or_missing(value: Any) -> str:
    return "direct" if value is not None else "missing"


def _complement_or_missing(opposite_bid: Any) -> str:
    return "complement_derived" if opposite_bid is not None else "missing"


def _topbook_prices(
    *,
    best_bid: float | None,
    best_ask: float | None,
    bid_size: float | None,
    ask_size: float | None,
    best_bid_source: str | None = None,
    best_ask_source: str | None = None,
    base_flags: list[str] | None = None,
) -> dict[str, Any]:
    bids = [PriceLevel(best_bid, bid_size)] if best_bid is not None else []
    asks = [PriceLevel(best_ask, ask_size)] if best_ask is not None else []
    row = compute_topbook(bids, asks, base_flags=base_flags).as_dict()
    row["best_bid_source"] = best_bid_source or (
        "direct" if best_bid is not None else "missing"
    )
    row["best_ask_source"] = best_ask_source or (
        "direct" if best_ask is not None else "missing"
    )
    return row


def polymarket_book_to_topbook(
    book: Any,
    *,
    token_id: str | None = None,
    collector_run_id: str = "",
    source: str = "rest_poll",
    received_at_utc: str | None = None,
    received_at_monotonic_ns: int | None = None,
    local_sequence: int | None = None,
    raw_event_ref: str | None = None,
    quality_flags: list[str] | None = None,
) -> dict[str, Any]:
    payload = _as_dict(book)
    bids = parse_levels(payload.get("bids"))
    asks = parse_levels(payload.get("asks"))
    bid = best_bid(bids)
    ask = best_ask(asks)
    best_bid_value = bid.price if bid else None
    bid_size = bid.size if bid else None
    best_ask_value = ask.price if ask else None
    ask_size = ask.size if ask else None
    return topbook_row(
        collector_run_id=collector_run_id,
        exchange="polymarket",
        venue_market_id=payload.get("market") or token_id or payload.get("asset_id"),
        instrument_id=token_id or payload.get("asset_id"),
        source=source,
        received_at_utc=_utc_now() if received_at_utc is None else received_at_utc,
        received_at_monotonic_ns=(
            time.monotonic_ns()
            if received_at_monotonic_ns is None
            else received_at_monotonic_ns
        ),
        exchange_ts_utc=isoformat_source_timestamp(
            payload.get("timestamp"), epoch_unit="milliseconds"
        ),
        local_sequence=local_sequence,
        book_hash=payload.get("hash"),
        tick_size_dollars=_parse_float(payload.get("tick_size")),
        min_order_size_contracts=_parse_float(payload.get("min_order_size")),
        raw_event_ref=raw_event_ref,
        **_topbook_prices(
            best_bid=best_bid_value,
            best_ask=best_ask_value,
            bid_size=bid_size,
            ask_size=ask_size,
            base_flags=quality_flags,
        ),
    )


def polymarket_ws_snapshot_to_topbook(
    snapshot: Any,
    *,
    collector_run_id: str = "",
    source: str = "ws",
    received_at_utc: str | None = None,
    received_at_monotonic_ns: int | None = None,
    local_sequence: int | None = None,
    raw_event_ref: str | None = None,
) -> dict[str, Any]:
    payload = _as_dict(snapshot)
    return topbook_row(
        collector_run_id=collector_run_id,
        exchange="polymarket",
        venue_market_id=payload.get("market") or payload.get("asset_id"),
        instrument_id=payload.get("asset_id"),
        source=source,
        received_at_utc=_utc_now() if received_at_utc is None else received_at_utc,
        received_at_monotonic_ns=(
            time.monotonic_ns()
            if received_at_monotonic_ns is None
            else received_at_monotonic_ns
        ),
        exchange_ts_utc=(
            payload.get("datetime_utc")
            if payload.get("datetime_utc") is not None
            else isoformat_source_timestamp(
                payload.get("timestamp"), epoch_unit="milliseconds"
            )
        ),
        local_sequence=local_sequence,
        book_hash=payload.get("last_book_hash"),
        tick_size_dollars=_parse_float(payload.get("tick_size")),
        quote_age_ms=payload.get("quote_age_ms"),
        raw_event_ref=raw_event_ref,
        **_topbook_prices(
            best_bid=_parse_float(payload.get("best_bid")),
            best_ask=_parse_float(payload.get("best_ask")),
            bid_size=_parse_float(
                _first_present(payload.get("best_bid_size"), payload.get("bid_size"))
            ),
            ask_size=_parse_float(
                _first_present(payload.get("best_ask_size"), payload.get("ask_size"))
            ),
            base_flags=list(payload.get("quality_flags") or []),
        ),
    )


def _kalshi_outcome_rows(
    payload: dict[str, Any],
    *,
    market_ticker: str | None,
    market_id: str | None,
    collector_run_id: str,
    source: str,
    received_at_utc: str | None,
    received_at_monotonic_ns: int | None,
    exchange_ts_utc: str | None,
    local_sequence: int | None,
    venue_sequence: int | None,
    venue_sid: int | None,
    raw_event_ref: str | None,
    base_flags: list[str] | None,
) -> list[dict[str, Any]]:
    policy = resolve_kalshi_quote_normalization_policy(
        payload.get("quote_normalization_policy")
    )
    yes_bid = _parse_float(payload.get("yes_bid"))
    yes_ask = _parse_float(payload.get("yes_ask"))
    no_bid = _parse_float(payload.get("no_bid"))
    no_ask = _parse_float(payload.get("no_ask"))
    yes_bid_source = str(payload.get("yes_bid_source") or _direct_or_missing(yes_bid))
    no_bid_source = str(payload.get("no_bid_source") or _direct_or_missing(no_bid))
    yes_ask_source = str(
        payload.get("yes_ask_source")
        or (_direct_or_missing(yes_ask) if yes_ask is not None else _complement_or_missing(no_bid))
    )
    no_ask_source = str(
        payload.get("no_ask_source")
        or (_direct_or_missing(no_ask) if no_ask is not None else _complement_or_missing(yes_bid))
    )
    if yes_ask is None and no_bid is not None:
        yes_ask = complement_probability(no_bid)
    if no_ask is None and yes_bid is not None:
        no_ask = complement_probability(yes_bid)
    if policy == KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT:
        yes_ask_size = _current_kalshi_ask_size(
            payload,
            explicit_field="yes_ask_size",
            fallback_field="no_bid_size",
        )
        no_ask_size = _current_kalshi_ask_size(
            payload,
            explicit_field="no_ask_size",
            fallback_field="yes_bid_size",
        )
    else:
        # Captures written before the policy version existed always projected
        # ask sizes from the opposite bid field, even when explicit ask sizes
        # were present. Preserve that behavior for byte-reproducible replay.
        yes_ask_size = payload.get("no_bid_size")
        no_ask_size = payload.get("yes_bid_size")
    venue_market_id = market_id or market_ticker
    now_utc = _utc_now() if received_at_utc is None else received_at_utc
    now_mono = (
        time.monotonic_ns()
        if received_at_monotonic_ns is None
        else received_at_monotonic_ns
    )
    return [
        topbook_row(
            collector_run_id=collector_run_id,
            exchange="kalshi",
            venue_market_id=venue_market_id,
            instrument_id=f"{market_ticker}:YES" if market_ticker else None,
            outcome="YES",
            source=source,
            received_at_utc=now_utc,
            received_at_monotonic_ns=now_mono,
            exchange_ts_utc=exchange_ts_utc,
            local_sequence=local_sequence,
            venue_sequence=venue_sequence,
            venue_sid=venue_sid,
            raw_event_ref=raw_event_ref,
            **_topbook_prices(
                best_bid=yes_bid,
                best_ask=yes_ask,
                bid_size=_parse_float(payload.get("yes_bid_size")),
                ask_size=yes_ask_size,
                best_bid_source=yes_bid_source,
                best_ask_source=yes_ask_source,
                base_flags=base_flags,
            ),
        ),
        topbook_row(
            collector_run_id=collector_run_id,
            exchange="kalshi",
            venue_market_id=venue_market_id,
            instrument_id=f"{market_ticker}:NO" if market_ticker else None,
            outcome="NO",
            source=source,
            received_at_utc=now_utc,
            received_at_monotonic_ns=now_mono,
            exchange_ts_utc=exchange_ts_utc,
            local_sequence=local_sequence,
            venue_sequence=venue_sequence,
            venue_sid=venue_sid,
            raw_event_ref=raw_event_ref,
            **_topbook_prices(
                best_bid=no_bid,
                best_ask=no_ask,
                bid_size=_parse_float(payload.get("no_bid_size")),
                ask_size=no_ask_size,
                best_bid_source=no_bid_source,
                best_ask_source=no_ask_source,
                base_flags=base_flags,
            ),
        ),
    ]


def kalshi_orderbook_to_topbook(
    orderbook: Any,
    *,
    market_ticker: str | None = None,
    market_id: str | None = None,
    collector_run_id: str = "",
    source: str = "rest_poll",
    received_at_utc: str | None = None,
    received_at_monotonic_ns: int | None = None,
    local_sequence: int | None = None,
    raw_event_ref: str | None = None,
) -> list[dict[str, Any]]:
    payload = _as_dict(orderbook)
    book_raw = payload.get("orderbook_fp")
    book = book_raw if isinstance(book_raw, dict) else payload
    yes_levels = parse_levels(
        book.get("yes_dollars_fp") or book.get("yes_dollars") or book.get("yes") or []
    )
    no_levels = parse_levels(
        book.get("no_dollars_fp") or book.get("no_dollars") or book.get("no") or []
    )
    yes = best_bid(yes_levels)
    no = best_bid(no_levels)
    yes_bid = yes.price if yes else None
    yes_size = yes.size if yes else None
    no_bid = no.price if no else None
    no_size = no.size if no else None
    return _kalshi_outcome_rows(
        {
            "yes_bid": yes_bid,
            "yes_bid_size": yes_size,
            "no_bid": no_bid,
            "no_bid_size": no_size,
        },
        market_ticker=market_ticker,
        market_id=market_id,
        collector_run_id=collector_run_id,
        source=source,
        received_at_utc=received_at_utc,
        received_at_monotonic_ns=received_at_monotonic_ns,
        exchange_ts_utc=None,
        local_sequence=local_sequence,
        venue_sequence=None,
        venue_sid=None,
        raw_event_ref=raw_event_ref,
        base_flags=None,
    )


def kalshi_ws_snapshot_to_topbook(
    snapshot: Any,
    *,
    collector_run_id: str = "",
    source: str = "ws",
    received_at_utc: str | None = None,
    received_at_monotonic_ns: int | None = None,
    local_sequence: int | None = None,
    raw_event_ref: str | None = None,
) -> list[dict[str, Any]]:
    payload = _as_dict(snapshot)
    return _kalshi_outcome_rows(
        payload,
        market_ticker=payload.get("market_ticker"),
        market_id=payload.get("market_id"),
        collector_run_id=collector_run_id,
        source=source,
        received_at_utc=received_at_utc,
        received_at_monotonic_ns=received_at_monotonic_ns,
        exchange_ts_utc=(
            payload.get("datetime_utc")
            if payload.get("datetime_utc") is not None
            else isoformat_source_timestamp(
                # Kalshi websocket state retains either ``ts_ms`` or ``ts``
                # in one compatibility field, so this boundary owns the
                # historical unit heuristic until the state stores the key.
                payload.get("timestamp"),
                epoch_unit="auto",
            )
        ),
        local_sequence=local_sequence,
        venue_sequence=payload.get("seq"),
        venue_sid=payload.get("sid"),
        raw_event_ref=raw_event_ref,
        base_flags=list(payload.get("quality_flags") or []),
    )
