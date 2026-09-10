"""Structural checks shared by the two public book adapters."""
from __future__ import annotations

import math
from typing import Any


def finite_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def well_formed_levels(levels: Any, *, allow_missing: bool = False) -> bool:
    if levels is None:
        return allow_missing
    if not isinstance(levels, list):
        return False
    for level in levels:
        if isinstance(level, dict):
            price = level.get("price", level.get("price_dollars"))
            size = level.get("size", level.get("count", level.get("count_fp")))
        elif isinstance(level, (list, tuple)) and len(level) == 2:
            price, size = level
        else:
            return False
        p, s = finite_number(price), finite_number(size)
        if p is None or s is None or not 0 <= p <= 1 or s < 0:
            return False
    return True


def snapshot_ladder(message: dict[str, Any], *names: str) -> Any:
    """Select the first present field; malformed/empty fields cannot fall through."""
    return next((message[name] for name in names if name in message), None)
