"""Check a merged stream: rebuild books and compare them with its full books.

Every full book in the merged stream (Polymarket ``book``, Kalshi
``orderbook_snapshot``) is compared, level by level, with the book rebuilt from
the earlier messages of the stream, then taken as the new state (a checkpoint).
A full book with no earlier full book of its instrument is only counted.

Read mismatches together with coverage evidence to locate missing changes.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from pmkt import _native
from pmkt.exchanges._ws_book import encode_message
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


def check_merged_books(venue: str, messages: Iterable[MergedMessage]) -> BookCheck:
    """Compare rebuilt books with the full books of a merged stream of one venue."""
    native = _native.BookCheck(venue)
    result = BookCheck()
    for item in messages:
        for instrument, difference in native.apply(encode_message(item.message)):
            result.full_books += 1
            if difference is None:
                continue
            equal = difference == 0
            result.compared += 1
            result.equal += equal
            if item.late:
                result.late_compared += 1
                result.late_equal += equal
            if item.reconnect_snapshot:
                result.reconnect_compared += 1
                result.reconnect_equal += equal
            if not equal and len(result.mismatch_examples) < _EXAMPLES:
                result.mismatch_examples.append({
                    "instrument": instrument,
                    "venue_ts_ms": item.venue_ts_ms,
                    "order_ms": item.order_ms,
                    "late": item.late,
                    "reconnect_snapshot": item.reconnect_snapshot,
                    "copies": item.copies,
                    "differing_levels": difference,
                })
    return result
