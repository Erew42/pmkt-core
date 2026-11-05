"""Shared units and validation for public Kalshi trade economics."""
from __future__ import annotations

import math
from typing import Any, Mapping

class ObservationValidationError(ValueError):
    """A source observation cannot be projected into its strict durable row."""


def _kalshi_yes_price(payload: Mapping[str, Any]) -> float | None:
    dollars = _first_float(payload, "yes_price_dollars", "yes_price_dollars_fp", "price_dollars")
    if dollars is not None:
        price = _probability(dollars, "yes_price_dollars")
        legacy_cents = _first_float(payload, "yes_price")
        if legacy_cents is not None:
            if legacy_cents < 0 or legacy_cents > 100:
                raise ObservationValidationError(
                    "yes_price must be between 0 and 100 cents"
                )
            if not math.isclose(price, legacy_cents / 100.0, abs_tol=1e-12):
                raise ObservationValidationError(
                    "Kalshi dollar and legacy-cent trade prices disagree"
                )
        return price
    cents = _first_float(payload, "yes_price")
    if cents is None:
        if payload.get("price") is not None:
            raise ObservationValidationError(
                "Kalshi trade price is ambiguous; require yes_price_dollars "
                "or legacy yes_price cents"
            )
        return None
    if cents < 0 or cents > 100:
        raise ObservationValidationError("yes_price must be between 0 and 100 cents")
    return _probability(cents / 100.0, "yes_price")


def _first_float(payload: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in payload and payload[key] is not None:
            return _parsed_float(payload[key], key)
    return None


def _first_positive_float(payload: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in payload and payload[key] is not None:
            value = _parsed_float(payload[key], key)
            if value <= 0:
                raise ObservationValidationError(f"{key} must be positive")
            return value
    return None


def _required_probability(payload: Mapping[str, Any], key: str) -> float:
    if key not in payload or payload[key] is None:
        raise ObservationValidationError(f"{key} is required and must be numeric")
    return _probability(_parsed_float(payload[key], key), key)


def _probability(value: float, field: str) -> float:
    if value < 0 or value > 1:
        raise ObservationValidationError(f"{field} must be between 0 and 1")
    return value


def _optional_positive_float(payload: Mapping[str, Any], key: str) -> float | None:
    if key not in payload or payload[key] is None:
        return None
    value = _parsed_float(payload[key], key)
    if value <= 0:
        raise ObservationValidationError(f"{key} must be positive")
    return value


def _parsed_float(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise ObservationValidationError(f"{field} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ObservationValidationError(f"{field} must be numeric") from exc
    if not math.isfinite(parsed):
        raise ObservationValidationError(f"{field} must be finite")
    return parsed


