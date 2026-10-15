"""Structural checks shared by the two public book adapters."""
from __future__ import annotations

import math
from decimal import Decimal, InvalidOperation
from typing import Any


def finite_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def finite_decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    # Legacy snapshots and recordings still project to float; reject values that
    # would become infinite there instead of storing them as valid depth.
    if not number.is_finite() or not math.isfinite(float(number)):
        return None
    return number


def decimal_levels(levels: Any) -> dict[Decimal, Decimal]:
    parsed: dict[Decimal, Decimal] = {}
    if not isinstance(levels, list):
        return parsed
    for level in levels:
        if isinstance(level, dict):
            price = level.get("price", level.get("price_dollars"))
            size = level.get("size", level.get("count", level.get("count_fp")))
        elif isinstance(level, (list, tuple)) and len(level) == 2:
            price, size = level
        else:
            continue
        p, s = finite_decimal(price), finite_decimal(size)
        if p is not None and s is not None and 0 <= p <= 1 and s > 0:
            parsed[p] = s
    return parsed


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
        p, s = finite_decimal(price), finite_decimal(size)
        if p is None or s is None or not 0 <= p <= 1 or s < 0:
            return False
    return True


def snapshot_ladder(message: dict[str, Any], *names: str) -> Any:
    """Select the first present field; malformed/empty fields cannot fall through."""
    return next((message[name] for name in names if name in message), None)
