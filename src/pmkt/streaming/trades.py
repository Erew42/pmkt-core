"""Normalize public trade observations without inferring executions from books."""

from __future__ import annotations

from datetime import datetime, timezone
import math
from typing import Any, Mapping

from pmkt.data.time import EpochUnit, isoformat_source_timestamp


class ObservationValidationError(ValueError):
    """A source observation cannot be projected into its strict durable row."""


def recording_trade(venue: str, message: Mapping[str, Any]) -> dict[str, Any] | None:
    """Project a public trade into recording.v1 without embedded wire payloads.

    Polymarket observation identity comes from recorder coordinates, never a
    fabricated venue trade ID. Kalshi ID deduplication belongs to SQLite.
    """
    kind = str(message.get("event_type") or message.get("type") or "")
    if venue == "polymarket" and kind == "last_trade_price":
        return {
            "instrument_id": _required_text(message, "asset_id"),
            "venue_market_id": _optional_text(message.get("market")),
            "venue_trade_id": None,
            "price": _required_probability(message, "price"),
            "quantity": _optional_positive_float(message, "size"),
            "reported_side": _optional_text(message.get("side")),
            "venue_time_utc": _optional_timestamp_field(
                message, "timestamp", epoch_unit="milliseconds"
            ),
        }
    if venue == "kalshi" and kind == "trade":
        payload = _payload(message)
        price = _kalshi_yes_price(payload)
        quantity = _first_positive_float(payload, "count_fp", "count", "size")
        if price is None or quantity is None:
            raise ObservationValidationError("Kalshi trade requires price and count")
        return {
            "instrument_id": _required_text(payload, "market_ticker") + ":YES",
            "venue_market_id": _required_text(payload, "market_ticker"),
            "venue_trade_id": _required_text(payload, "trade_id"),
            "price": price,
            "quantity": quantity,
            "reported_side": _optional_text(payload.get("taker_side")),
            "venue_time_utc": _first_timestamp(
                payload,
                ("created_time", "seconds"),
                ("ts", "seconds"),
                ("ts_ms", "milliseconds"),
            ),
        }
    return None


def _payload(message: Mapping[str, Any]) -> Mapping[str, Any]:
    value = message.get("msg")
    return value if isinstance(value, Mapping) else message


def _timestamp(value: Any, field: str, *, epoch_unit: EpochUnit) -> str:
    parsed = isoformat_source_timestamp(value, epoch_unit=epoch_unit)
    if parsed is None:
        raise ObservationValidationError(f"{field} must be a valid UTC timestamp")
    return (
        datetime.fromisoformat(parsed.replace("Z", "+00:00"))
        .astimezone(timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    )


def _first_timestamp(
    payload: Mapping[str, Any],
    *fields: tuple[str, EpochUnit],
) -> str | None:
    for key, epoch_unit in fields:
        if key in payload and payload[key] is not None:
            return _timestamp(payload[key], key, epoch_unit=epoch_unit)
    return None


def _optional_timestamp_field(
    payload: Mapping[str, Any],
    key: str,
    *,
    epoch_unit: EpochUnit,
) -> str | None:
    if key not in payload or payload[key] is None:
        return None
    return _timestamp(payload[key], key, epoch_unit=epoch_unit)


def _kalshi_yes_price(payload: Mapping[str, Any]) -> float | None:
    dollars = _first_float(payload, "yes_price_dollars", "yes_price_dollars_fp")
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


def _required_text(payload: Mapping[str, Any], key: str) -> str:
    value = _optional_text(payload.get(key))
    if value is None:
        raise ObservationValidationError(f"{key} is required")
    return value


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
