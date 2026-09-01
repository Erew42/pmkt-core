from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from pmkt.data.types import parse_float


@dataclass(frozen=True)
class PriceLevel:
    price: float
    size: float | None


@dataclass(frozen=True)
class TopBookComputation:
    best_bid_dollars: float | None
    best_ask_dollars: float | None
    mid_dollars: float | None
    spread_dollars: float | None
    spread_bps: float | None
    bid_size_contracts: float | None
    ask_size_contracts: float | None
    valid_state: bool
    quality_flags: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "best_bid_dollars": self.best_bid_dollars,
            "best_ask_dollars": self.best_ask_dollars,
            "mid_dollars": self.mid_dollars,
            "spread_dollars": self.spread_dollars,
            "spread_bps": self.spread_bps,
            "bid_size_contracts": self.bid_size_contracts,
            "ask_size_contracts": self.ask_size_contracts,
            "valid_state": self.valid_state,
            "quality_flags": list(self.quality_flags),
        }


def parse_price_level(level: Any) -> PriceLevel | None:
    if isinstance(level, PriceLevel):
        return level if level.price >= 0 and level.size is not None and level.size > 0 else None
    if isinstance(level, (list, tuple)) and len(level) >= 2:
        price = parse_float(level[0])
        size = parse_float(level[1])
    elif isinstance(level, dict):
        price = parse_float(_first_present(level, ("price", "price_dollars")))
        size = parse_float(_first_present(level, ("size", "count", "count_fp")))
    elif hasattr(level, "model_dump"):
        return parse_price_level(level.model_dump())
    elif hasattr(level, "dict"):
        return parse_price_level(level.dict())
    else:
        return None
    if price is None or price < 0 or size is None or size <= 0:
        return None
    return PriceLevel(price=price, size=size)


def _first_present(payload: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None


def parse_levels(levels: Any) -> list[PriceLevel]:
    if levels is None:
        return []
    if not isinstance(levels, Iterable) or isinstance(levels, (str, bytes, dict)):
        return []
    parsed: list[PriceLevel] = []
    for level in levels:
        price_level = parse_price_level(level)
        if price_level is not None:
            parsed.append(price_level)
    return parsed


def best_bid(levels: Iterable[PriceLevel]) -> PriceLevel | None:
    values = list(levels)
    return max(values, key=lambda level: level.price) if values else None


def best_ask(levels: Iterable[PriceLevel]) -> PriceLevel | None:
    values = list(levels)
    return min(values, key=lambda level: level.price) if values else None


def compute_topbook(
    bids: Iterable[PriceLevel],
    asks: Iterable[PriceLevel],
    *,
    base_flags: Iterable[str] | None = None,
) -> TopBookComputation:
    bid = best_bid(bids)
    ask = best_ask(asks)
    flags = set(base_flags or [])
    best_bid_value = bid.price if bid else None
    best_ask_value = ask.price if ask else None
    mid = None
    spread = None
    spread_bps = None
    if bid is None:
        flags.add("empty_bid")
    if ask is None:
        flags.add("empty_ask")
    if bid is not None and ask is not None:
        mid = (bid.price + ask.price) / 2.0
        spread = ask.price - bid.price
        if spread < 0:
            flags.update({"crossed_book", "negative_spread"})
        elif spread == 0:
            flags.add("crossed_book")
        if mid > 0:
            spread_bps = spread / mid * 10_000
    return TopBookComputation(
        best_bid_dollars=best_bid_value,
        best_ask_dollars=best_ask_value,
        mid_dollars=mid,
        spread_dollars=spread,
        spread_bps=spread_bps,
        bid_size_contracts=bid.size if bid else None,
        ask_size_contracts=ask.size if ask else None,
        valid_state=not flags,
        quality_flags=tuple(sorted(flags)),
    )


__all__ = [
    "PriceLevel",
    "TopBookComputation",
    "best_ask",
    "best_bid",
    "compute_topbook",
    "parse_levels",
    "parse_price_level",
]
