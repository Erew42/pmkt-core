from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Mapping

from pmkt.data.canonical import stream_lifecycle_row, trade_row
from pmkt.data.normalize_kalshi import (
    kalshi_status_from_lifecycle_event,
    normalize_kalshi_market_status,
)
from pmkt.data.time import EpochUnit, isoformat_source_timestamp
from pmkt.streaming.tape import (
    CaptureCoordinate,
    canonical_json,
    canonical_utc,
    semantic_hash,
    versioned_id,
)

KALSHI_LIFECYCLE_EVENTS = frozenset(
    {
        "created",
        "activated",
        "deactivated",
        "close_date_updated",
        "determined",
        "settled",
        "metadata_updated",
    }
)
POLYMARKET_LIFECYCLE_EVENTS = frozenset(
    {"new_market", "market_resolved", "tick_size_change"}
)


class ObservationValidationError(ValueError):
    """A source observation cannot be projected into its strict durable row."""


@dataclass(frozen=True)
class StreamObservations:
    trades: tuple[Mapping[str, Any], ...] = ()
    lifecycle: tuple[Mapping[str, Any], ...] = ()


class StreamObservationProducer:
    """Normalize trade/lifecycle evidence without mutating venue book state."""

    def __init__(self, *, collector_run_id: str) -> None:
        run_id = _optional_text(collector_run_id)
        if run_id is None:
            raise ObservationValidationError("collector_run_id is required")
        self.collector_run_id = run_id
        self._seen_kalshi_trade_ids: set[str] = set()

    def _validate_coordinate(self, coordinate: CaptureCoordinate) -> None:
        if coordinate.collector_run_id != self.collector_run_id:
            raise ObservationValidationError(
                "capture coordinate collector_run_id does not match producer run"
            )

    def polymarket(
        self, message: Mapping[str, Any], coordinate: CaptureCoordinate
    ) -> StreamObservations:
        self._validate_coordinate(coordinate)
        event_type = str(message.get("event_type") or message.get("type") or "")
        if event_type == "last_trade_price":
            return StreamObservations(
                trades=(self._polymarket_trade(message, coordinate),)
            )
        if event_type in POLYMARKET_LIFECYCLE_EVENTS:
            return StreamObservations(
                lifecycle=(self._polymarket_lifecycle(message, coordinate, event_type),)
            )
        return StreamObservations()

    def kalshi(
        self, message: Mapping[str, Any], coordinate: CaptureCoordinate
    ) -> StreamObservations:
        self._validate_coordinate(coordinate)
        event_type = str(message.get("type") or "")
        if event_type == "trade":
            row = self._kalshi_trade(message, coordinate)
            return StreamObservations(trades=(row,) if row is not None else ())
        if "mve" in event_type.lower() and "lifecycle" in event_type.lower():
            raise ValueError("Kalshi MVE lifecycle channel is explicitly unsupported")
        payload = _payload(message)
        lifecycle_type = str(
            payload.get("event_type")
            or payload.get("market_event_type")
            or payload.get("type")
            or ""
        )
        if event_type == "market_lifecycle_v2":
            if lifecycle_type not in KALSHI_LIFECYCLE_EVENTS:
                raise ObservationValidationError(
                    "Kalshi lifecycle event_type is missing or unsupported"
                )
            return StreamObservations(
                lifecycle=(self._kalshi_lifecycle(message, coordinate, lifecycle_type),)
            )
        return StreamObservations()

    def _polymarket_trade(
        self, message: Mapping[str, Any], coordinate: CaptureCoordinate
    ) -> Mapping[str, Any]:
        book_id = _required_text(message, "asset_id")
        price = _required_probability(message, "price")
        size = _optional_positive_float(message, "size")
        trade_at = _optional_timestamp_field(
            message, "timestamp", epoch_unit="milliseconds"
        ) or canonical_utc(
            coordinate.received_at_utc
        )
        observation_id = versioned_id(
            "polymarket-trade-observation",
            [
                self.collector_run_id,
                book_id,
                trade_at,
                price,
                size,
                coordinate.local_sequence,
                coordinate.subsequence,
            ],
        )
        raw_json = canonical_json(message)
        return trade_row(
            collector_run_id=self.collector_run_id,
            venue="polymarket",
            venue_trade_id=observation_id,
            venue_market_id=str(message.get("market") or book_id),
            instrument_id=book_id,
            outcome=_optional_text(message.get("outcome")),
            trade_ts_utc=trade_at,
            received_at_utc=canonical_utc(coordinate.received_at_utc),
            received_at_monotonic_ns=coordinate.received_at_monotonic_ns,
            local_sequence=coordinate.local_sequence,
            subsequence=coordinate.subsequence,
            price_dollars=price,
            size_contracts=size,
            notional_dollars=None if size is None else price * size,
            aggressor_side=_optional_text(message.get("side")),
            raw_json=raw_json,
            raw_json_sha256=semantic_hash(message),
        )

    def _kalshi_trade(
        self, message: Mapping[str, Any], coordinate: CaptureCoordinate
    ) -> Mapping[str, Any] | None:
        payload = _payload(message)
        trade_id = _required_text(payload, "trade_id")
        if trade_id in self._seen_kalshi_trade_ids:
            return None
        ticker = _required_text(payload, "market_ticker")
        price = _kalshi_yes_price(payload)
        size = _first_positive_float(payload, "count_fp", "count", "size")
        if price is None or size is None:
            raise ObservationValidationError("Kalshi trade requires price and count")
        raw_json = canonical_json(message)
        row = trade_row(
            collector_run_id=self.collector_run_id,
            venue="kalshi",
            venue_trade_id=trade_id,
            venue_market_id=ticker,
            instrument_id=str(payload.get("instrument_id") or f"{ticker}:YES"),
            outcome="YES",
            trade_ts_utc=_first_timestamp(
                payload,
                ("created_time", "seconds"),
                ("ts", "seconds"),
                ("ts_ms", "milliseconds"),
            ),
            received_at_utc=canonical_utc(coordinate.received_at_utc),
            received_at_monotonic_ns=coordinate.received_at_monotonic_ns,
            local_sequence=coordinate.local_sequence,
            subsequence=coordinate.subsequence,
            price_dollars=price,
            size_contracts=size,
            notional_dollars=price * size,
            aggressor_side=_optional_text(payload.get("taker_side")),
            raw_json=raw_json,
            raw_json_sha256=semantic_hash(message),
        )
        self._seen_kalshi_trade_ids.add(trade_id)
        return row

    def _polymarket_lifecycle(
        self,
        message: Mapping[str, Any],
        coordinate: CaptureCoordinate,
        event_type: str,
    ) -> Mapping[str, Any]:
        market_id = str(message.get("market") or message.get("condition_id") or "")
        if not market_id:
            raise ObservationValidationError(
                "Polymarket lifecycle event requires market identity"
            )
        return _lifecycle_row(
            collector_run_id=self.collector_run_id,
            coordinate=coordinate,
            venue="polymarket",
            venue_market_id=market_id,
            instrument_id=_optional_text(message.get("asset_id")),
            event_type=event_type,
            payload=message,
            exchange_at=_first_timestamp(
                message,
                ("timestamp", "milliseconds"),
                ("ts", "milliseconds"),
                ("ts_ms", "milliseconds"),
            ),
            previous_tick=message.get("old_tick_size"),
            new_tick=message.get("new_tick_size"),
            resolution_status=message.get("resolution_status"),
            resolved_outcome=message.get("outcome"),
        )

    def _kalshi_lifecycle(
        self,
        message: Mapping[str, Any],
        coordinate: CaptureCoordinate,
        event_type: str,
    ) -> Mapping[str, Any]:
        payload = _payload(message)
        ticker = _required_text(payload, "market_ticker")
        previous_status = normalize_kalshi_market_status(
            payload.get("previous_status")
        )
        new_status = kalshi_status_from_lifecycle_event(
            event_type,
            explicit_status=payload.get("status") or payload.get("new_status"),
        )
        return _lifecycle_row(
            collector_run_id=self.collector_run_id,
            coordinate=coordinate,
            venue="kalshi",
            venue_market_id=ticker,
            instrument_id=_optional_text(payload.get("instrument_id")),
            event_type=event_type,
            payload=message,
            exchange_at=_first_timestamp(
                payload,
                ("timestamp", "auto"),
                ("ts", "seconds"),
                ("ts_ms", "milliseconds"),
            ),
            previous_status=previous_status,
            new_status=new_status,
            resolution_status=payload.get("result_status"),
            resolved_outcome=payload.get("result"),
            market_close=_first_timestamp(
                payload,
                ("close_time", "seconds"),
                ("close_ts", "seconds"),
            ),
            venue_sequence=message.get("seq"),
        )


def _lifecycle_row(
    *,
    collector_run_id: str,
    coordinate: CaptureCoordinate,
    venue: str,
    venue_market_id: str,
    instrument_id: str | None,
    event_type: str,
    payload: Mapping[str, Any],
    exchange_at: str | None = None,
    previous_status: Any = None,
    new_status: Any = None,
    previous_tick: Any = None,
    new_tick: Any = None,
    resolution_status: Any = None,
    resolved_outcome: Any = None,
    market_close: Any = None,
    venue_sequence: Any = None,
) -> Mapping[str, Any]:
    raw_hash = semantic_hash(payload)
    lifecycle_id = versioned_id(
        "stream-lifecycle-observation",
        [
            collector_run_id,
            coordinate.shard_id,
            venue,
            venue_market_id,
            instrument_id,
            coordinate.local_sequence,
            coordinate.subsequence,
            event_type,
            raw_hash,
        ],
    )
    return stream_lifecycle_row(
        collector_run_id=collector_run_id,
        lifecycle_event_id=lifecycle_id,
        venue=venue,
        venue_market_id=venue_market_id,
        instrument_id=instrument_id,
        event_type=event_type,
        received_at_utc=canonical_utc(coordinate.received_at_utc),
        received_at_monotonic_ns=coordinate.received_at_monotonic_ns,
        exchange_at_utc=exchange_at,
        local_sequence=coordinate.local_sequence,
        subsequence=coordinate.subsequence,
        venue_sequence=_optional_text(venue_sequence),
        previous_status=_optional_text(previous_status),
        new_status=_optional_text(new_status),
        previous_tick_size_dollars=_optional_nonnegative_float(
            previous_tick, "previous_tick_size"
        ),
        new_tick_size_dollars=_optional_nonnegative_float(new_tick, "new_tick_size"),
        resolution_status=_optional_text(resolution_status),
        resolved_outcome=_optional_text(resolved_outcome),
        market_close_at_utc=market_close,
        raw_event_ref=None,
        raw_event_hash=raw_hash,
        quality_flags_json="[]",
    )


def _payload(message: Mapping[str, Any]) -> Mapping[str, Any]:
    value = message.get("msg")
    return value if isinstance(value, Mapping) else message


def _timestamp(value: Any, field: str, *, epoch_unit: EpochUnit) -> str:
    parsed = isoformat_source_timestamp(value, epoch_unit=epoch_unit)
    if parsed is None:
        raise ObservationValidationError(f"{field} must be a valid UTC timestamp")
    return canonical_utc(parsed)


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


def _optional_nonnegative_float(value: Any, field: str) -> float | None:
    if value is None:
        return None
    parsed = _parsed_float(value, field)
    if parsed < 0:
        raise ObservationValidationError(f"{field} must be nonnegative")
    return parsed


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


__all__ = [
    "KALSHI_LIFECYCLE_EVENTS",
    "ObservationValidationError",
    "POLYMARKET_LIFECYCLE_EVENTS",
    "StreamObservationProducer",
    "StreamObservations",
]
