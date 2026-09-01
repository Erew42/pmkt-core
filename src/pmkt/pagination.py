from __future__ import annotations

from typing import Any, Literal

PolymarketCursorStopReason = Literal[
    "cursor_exhausted",
    "terminal_cursor_lte",
    "repeated_cursor",
]

POLYMARKET_TERMINAL_CURSOR = "LTE="


def normalize_polymarket_cursor(value: Any) -> str:
    return "" if value is None else str(value).strip()


def polymarket_cursor_stop_reason(
    next_cursor: Any,
    *,
    previous_cursor: Any = None,
) -> PolymarketCursorStopReason | None:
    cursor = normalize_polymarket_cursor(next_cursor)
    if not cursor:
        return "cursor_exhausted"
    if cursor == POLYMARKET_TERMINAL_CURSOR:
        return "terminal_cursor_lte"
    previous = normalize_polymarket_cursor(previous_cursor)
    if previous and cursor == previous:
        return "repeated_cursor"
    return None


__all__ = [
    "POLYMARKET_TERMINAL_CURSOR",
    "PolymarketCursorStopReason",
    "normalize_polymarket_cursor",
    "polymarket_cursor_stop_reason",
]
