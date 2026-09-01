from __future__ import annotations

import math
import numbers
import re
from decimal import Decimal, InvalidOperation
from typing import Any


_MAX_SAFE_IEEE_754_INTEGER = (1 << 53) - 1
_MAX_TEXTUAL_INTEGER_DIGITS = 4_300
_MAX_INTEGER_TEXT_LENGTH = (_MAX_TEXTUAL_INTEGER_DIGITS * 2) + 32
_DIGIT_SEQUENCE = r"[0-9](?:_?[0-9])*"
_INTEGER_VALUE_TEXT_RE = re.compile(
    rf"^[+-]?(?:(?:{_DIGIT_SEQUENCE})(?:\.(?:{_DIGIT_SEQUENCE})?)?"
    rf"|\.(?:{_DIGIT_SEQUENCE}))(?:[eE][+-]?{_DIGIT_SEQUENCE})?$"
)
_NUMPY_FLOAT_PRECISION_BITS = {2: 11, 4: 24, 8: 53}


def parse_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, numbers.Real):
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = float(text)
        except ValueError:
            return None
        return parsed if math.isfinite(parsed) else None
    return None


def parse_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, Decimal):
        return _decimal_to_int(value)
    if isinstance(value, numbers.Rational):
        numerator = int(value.numerator)
        denominator = int(value.denominator)
        quotient, remainder = divmod(numerator, denominator)
        return quotient if remainder == 0 else None
    precision_bits = _floating_precision_bits(value)
    if precision_bits is not None:
        try:
            parsed = float(value)
        except (OverflowError, TypeError, ValueError):
            return None
        if (
            not math.isfinite(parsed)
            or not parsed.is_integer()
            or abs(parsed) > (1 << precision_bits) - 1
        ):
            return None
        return int(parsed)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if (
        not text
        or len(text) > _MAX_INTEGER_TEXT_LENGTH
        or _INTEGER_VALUE_TEXT_RE.fullmatch(text) is None
    ):
        return None
    try:
        parsed_decimal = Decimal(text)
    except InvalidOperation:
        return None
    return _decimal_to_int(parsed_decimal)


def _floating_precision_bits(value: Any) -> int | None:
    if isinstance(value, float):
        return _MAX_SAFE_IEEE_754_INTEGER.bit_length()
    value_type = type(value)
    if not value_type.__module__.startswith("numpy"):
        return None
    dtype = getattr(value, "dtype", None)
    if getattr(dtype, "kind", None) != "f":
        return None
    itemsize = getattr(dtype, "itemsize", None)
    if not isinstance(itemsize, int):
        return None
    return _NUMPY_FLOAT_PRECISION_BITS.get(itemsize)


def _decimal_to_int(value: Decimal) -> int | None:
    if not value.is_finite() or value != value.to_integral_value():
        return None
    if value.is_zero():
        return 0
    if value.adjusted() + 1 > _MAX_TEXTUAL_INTEGER_DIGITS:
        return None
    try:
        return int(value)
    except (OverflowError, ValueError):
        return None


def parse_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


__all__ = ["parse_bool", "parse_float", "parse_int"]
