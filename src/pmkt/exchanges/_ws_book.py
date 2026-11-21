"""Bridge decoded WebSocket messages to the native book engine."""

from __future__ import annotations

import json
import math
from decimal import Decimal
from typing import Any

# json.loads builds a new decoder on every call with a custom parse_float.
decode_decimal_json = json.JSONDecoder(parse_float=Decimal).decode


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
