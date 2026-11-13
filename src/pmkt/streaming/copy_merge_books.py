"""Check a merged stream: rebuild books and compare them with its full books.

Every full book in the merged stream (Polymarket ``book``, Kalshi
``orderbook_snapshot``) is compared, level by level, with the book rebuilt from
the earlier messages of the stream, then taken as the new state (a checkpoint).
A full book with no earlier full book of its instrument is only counted.

Book state reuses the venue adapters' ``MarketBookState`` and
``KalshiOrderBookState`` (decimal levels). A hole that no copy covered makes the
next comparison of the affected instruments fail; read mismatches together
with ``copy_coverage.uncovered_intervals``.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from pmkt.exchanges.book_integrity import decimal_levels, snapshot_ladder
from pmkt.exchanges.kalshi.ws import KalshiOrderBookState
from pmkt.exchanges.polymarket.ws import MarketBookState
from pmkt.streaming.copy_merge import MergedMessage

_EXAMPLES = 20


@dataclass
class BookCheck:
    full_books: int = 0
    # Full books with an earlier full book of the same instrument in the stream.
    compared: int = 0
    equal: int = 0
    late_compared: int = 0
    late_equal: int = 0
    reconnect_compared: int = 0
    reconnect_equal: int = 0
    mismatch_examples: list[dict[str, Any]] = field(default_factory=list)

    @property
    def unequal(self) -> int:
        return self.compared - self.equal


def _differing_levels(
    rebuilt: dict[Decimal, Decimal], stated: dict[Decimal, Decimal]
) -> int:
    return sum(
        1 for price in rebuilt.keys() | stated.keys() if rebuilt.get(price) != stated.get(price)
    )


def check_merged_books(venue: str, messages: Iterable[MergedMessage]) -> BookCheck:
    """Compare rebuilt books with the full books of a merged stream of one venue."""
    if venue not in {"polymarket", "kalshi"}:
        raise ValueError(f"unsupported venue: {venue!r}")
    result = BookCheck()
    poly: dict[str, MarketBookState] = {}
    kalshi: dict[str, KalshiOrderBookState] = {}
    for item in messages:
        message = item.message
        if not isinstance(message, dict):
            continue
        if venue == "polymarket":
            if item.kind == "price_change":
                changes = message.get("price_changes")
                for change in changes if isinstance(changes, list) else ():
                    if isinstance(change, dict) and isinstance(change.get("asset_id"), str):
                        asset = change["asset_id"]
                        state = poly.setdefault(asset, MarketBookState(asset_id=asset))
                        state.apply_price_change(change, message)
                continue
            if item.kind != "book" or not isinstance(message.get("asset_id"), str):
                continue
            instrument = message["asset_id"]
            state = poly.setdefault(instrument, MarketBookState(asset_id=instrument))
            sides = (
                (state.bids, decimal_levels(message.get("bids"))),
                (state.asks, decimal_levels(message.get("asks"))),
            )
            had_book = state.initial_snapshot_received
        else:
            msg = message.get("msg")
            if not isinstance(msg, dict) or not isinstance(msg.get("market_ticker"), str):
                continue
            ticker = msg["market_ticker"]
            if item.kind == "orderbook_delta":
                kalshi.setdefault(ticker, KalshiOrderBookState(ticker)).apply_delta(message)
                continue
            if item.kind != "orderbook_snapshot":
                continue
            instrument = ticker
            kstate = kalshi.setdefault(ticker, KalshiOrderBookState(ticker))
            sides = (
                (kstate.yes_bids, decimal_levels(snapshot_ladder(msg, "yes_dollars_fp", "yes_dollars", "yes"))),
                (kstate.no_bids, decimal_levels(snapshot_ladder(msg, "no_dollars_fp", "no_dollars", "no"))),
            )
            had_book = kstate.initial_snapshot_received
        result.full_books += 1
        if had_book:
            equal = all(rebuilt == stated for rebuilt, stated in sides)
            result.compared += 1
            result.equal += equal
            if item.late:
                result.late_compared += 1
                result.late_equal += equal
            if item.reconnect_snapshot:
                result.reconnect_compared += 1
                result.reconnect_equal += equal
            if not equal and len(result.mismatch_examples) < _EXAMPLES:
                result.mismatch_examples.append(
                    {
                        "instrument": instrument,
                        "venue_ts_ms": item.venue_ts_ms,
                        "order_ms": item.order_ms,
                        "late": item.late,
                        "reconnect_snapshot": item.reconnect_snapshot,
                        "copies": item.copies,
                        "differing_levels": sum(
                            _differing_levels(rebuilt, stated) for rebuilt, stated in sides
                        ),
                    }
                )
        if venue == "polymarket":
            poly[instrument].apply_book(message)
        else:
            kalshi[instrument].apply_snapshot(message)
    return result
