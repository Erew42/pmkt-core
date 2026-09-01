from __future__ import annotations

from typing import Any, Iterable, Mapping

from pmkt.exchanges.kalshi.ws import KalshiOrderBookState
from pmkt.exchanges.polymarket.ws import MarketBookState
from pmkt.streaming.tape import NativeBookLevel, canonical_decimal


def polymarket_book_levels(state: MarketBookState) -> tuple[NativeBookLevel, ...]:
    return _ordered_full_levels(("bid", state.bids), ("ask", state.asks))


def polymarket_delta_levels(
    state: MarketBookState,
    message: Mapping[str, Any],
) -> tuple[NativeBookLevel, ...]:
    changes = message.get("price_changes")
    if not isinstance(changes, list):
        return ()
    mutations: list[NativeBookLevel] = []
    for ordinal, change in enumerate(changes):
        if not isinstance(change, Mapping):
            continue
        change_book_id = str(
            change.get("asset_id") or message.get("asset_id") or ""
        )
        if change_book_id != state.asset_id:
            continue
        raw_side = str(change.get("side") or "").upper()
        side = "bid" if raw_side in {"BUY", "BID", "BIDS"} else "ask" if raw_side in {"SELL", "ASK", "ASKS"} else None
        price = _float_or_none(change.get("price"))
        if side is None or price is None:
            continue
        ladder = state.bids if side == "bid" else state.asks
        mutations.append(
            NativeBookLevel(
                source_side=side,
                price=price,
                size_after_contracts=ladder.get(price, 0.0),
                size_delta_contracts=None,
                level_ordinal=ordinal,
            )
        )
    return _collapse_mutations(mutations)


def kalshi_book_levels(state: KalshiOrderBookState) -> tuple[NativeBookLevel, ...]:
    return _ordered_full_levels(("yes", state.yes_bids), ("no", state.no_bids))


def kalshi_delta_levels(
    state: KalshiOrderBookState,
    message: Mapping[str, Any],
) -> tuple[NativeBookLevel, ...]:
    raw = message.get("msg")
    payload = raw if isinstance(raw, Mapping) else message
    side = str(payload.get("side") or "").lower()
    if side not in {"yes", "no"}:
        return ()
    price = _first_float(payload.get("price_dollars"), payload.get("price"))
    delta = _first_float(
        payload.get("delta_fp"), payload.get("delta"), payload.get("size_delta")
    )
    if price is None:
        return ()
    ladder = state.yes_bids if side == "yes" else state.no_bids
    return (
        NativeBookLevel(
            source_side=side,
            price=price,
            size_after_contracts=ladder.get(price, 0.0),
            size_delta_contracts=delta,
            level_ordinal=0,
        ),
    )


def _ordered_full_levels(
    *sides: tuple[str, Mapping[float, float]],
) -> tuple[NativeBookLevel, ...]:
    rows: list[NativeBookLevel] = []
    ordinal = 0
    for side, ladder in sides:
        for price, size in sorted(ladder.items(), key=lambda item: canonical_decimal(item[0])):
            rows.append(NativeBookLevel(side, price, size, level_ordinal=ordinal))
            ordinal += 1
    return tuple(rows)


def _collapse_mutations(levels: Iterable[NativeBookLevel]) -> tuple[NativeBookLevel, ...]:
    collapsed: dict[tuple[str, str], NativeBookLevel] = {}
    for level in levels:
        collapsed[(level.source_side, level.price_key)] = level
    return tuple(
        NativeBookLevel(
            source_side=level.source_side,
            price=level.price,
            size_after_contracts=level.size_after_contracts,
            size_delta_contracts=level.size_delta_contracts,
            level_ordinal=ordinal,
        )
        for ordinal, (_, level) in enumerate(sorted(collapsed.items()))
    )


def _float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _first_float(*values: Any) -> float | None:
    for value in values:
        parsed = _float_or_none(value)
        if parsed is not None:
            return parsed
    return None


__all__ = [
    "kalshi_book_levels",
    "kalshi_delta_levels",
    "polymarket_book_levels",
    "polymarket_delta_levels",
]
