"""Bridge decoded WebSocket messages to the native book engine."""

from __future__ import annotations

import json
import math
from decimal import Decimal
from typing import Any

from pmkt import _native


def encode_message(value: Any) -> str:
    # Only used by callers supplying a decoded message. Preserve Decimal tokens
    # (including numeric source timestamps) without converting through float.
    if isinstance(value, dict):
        return "{" + ",".join(json.dumps(k) + ":" + encode_message(v) for k, v in value.items()) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(encode_message(item) for item in value) + "]"
    if isinstance(value, Decimal):
        return str(value) if value.is_finite() else json.dumps(str(value))
    if isinstance(value, float) and not math.isfinite(value):
        return json.dumps(str(value))
    return json.dumps(value)


def decimal_text(value: Decimal | float | None) -> str | None:
    return str(value) if value is not None else None


def local_state(
    first: dict[Decimal, Decimal], second: dict[Decimal, Decimal],
    initialized: bool, flags: set[str], *, sid: int | None = None,
    seq: int | None = None, last_hash: str | None = None,
    tick: float | None = None,
) -> _native._LocalState:
    return {
        "first": [(str(p), str(q)) for p, q in first.items()],
        "second": [(str(p), str(q)) for p, q in second.items()],
        "initialized": initialized, "flags": list(flags),
        "sid": sid, "seq": seq, "hash": last_hash, "tick": decimal_text(tick),
    }


def decimal_levels(levels: list[tuple[str, str]]) -> dict[Decimal, Decimal]:
    return {Decimal(price): Decimal(size) for price, size in levels}


def sync_ladder(
    ladder: dict[Decimal, Decimal], levels: list[tuple[str, str]],
) -> None:
    replacement = decimal_levels(levels)
    for price in ladder.keys() - replacement.keys():
        del ladder[price]
    for price, size in replacement.items():
        if ladder.get(price) != size:
            ladder[price] = size
