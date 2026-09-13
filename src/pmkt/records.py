"""Pure public records shared by read-only venue workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import ClassVar, Literal


DataScope = Literal["production", "demo", "synthetic", "unknown"]
TransportOrigin = Literal["library_default", "caller_supplied"]
RequestOutcome = Literal[
    "success",
    "http_error",
    "transport_error",
    "timeout",
    "cancelled",
    "invalid_response",
    "error",
]


def _require_identifier(value: object, name: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    if not value.strip():
        raise ValueError(f"{name} must not be empty")


def _require_optional_identifier(value: object, name: str) -> None:
    if value is not None:
        _require_identifier(value, name)


@dataclass(frozen=True)
class PolymarketMarketRef:
    venue: ClassVar[str] = "polymarket"

    market_id: str
    condition_id: str | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        _require_identifier(self.market_id, "market_id")
        _require_optional_identifier(self.condition_id, "condition_id")

    def identity_key(self) -> tuple[str, ...]:
        return (self.venue, self.market_id)


@dataclass(frozen=True)
class PolymarketInstrumentRef:
    venue: ClassVar[str] = "polymarket"

    token_id: str
    market: PolymarketMarketRef | None = field(default=None, compare=False)
    outcome_index: int | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        _require_identifier(self.token_id, "token_id")
        if self.market is not None and not isinstance(self.market, PolymarketMarketRef):
            raise TypeError("market must be a PolymarketMarketRef")
        if self.outcome_index is not None:
            if isinstance(self.outcome_index, bool) or not isinstance(
                self.outcome_index, int
            ):
                raise TypeError("outcome_index must be an int")
            if self.outcome_index < 0:
                raise ValueError("outcome_index must be nonnegative")

    def identity_key(self) -> tuple[str, ...]:
        return (self.venue, self.token_id)


@dataclass(frozen=True)
class KalshiMarketRef:
    venue: ClassVar[str] = "kalshi"

    ticker: str
    series_ticker: str | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        _require_identifier(self.ticker, "ticker")
        _require_optional_identifier(self.series_ticker, "series_ticker")

    def identity_key(self) -> tuple[str, ...]:
        return (self.venue, self.ticker)


@dataclass(frozen=True)
class KalshiInstrumentRef:
    venue: ClassVar[str] = "kalshi"

    market: KalshiMarketRef
    side: Literal["yes", "no"]

    def __post_init__(self) -> None:
        if not isinstance(self.market, KalshiMarketRef):
            raise TypeError("market must be a KalshiMarketRef")
        if self.side not in ("yes", "no"):
            raise ValueError("side must be 'yes' or 'no'")

    def identity_key(self) -> tuple[str, ...]:
        return (self.venue, self.market.ticker, self.side)


MarketRef = PolymarketMarketRef | KalshiMarketRef
InstrumentRef = PolymarketInstrumentRef | KalshiInstrumentRef


@dataclass(frozen=True)
class RequestObservation:
    """Sanitized provenance for one HTTP request within a workflow operation."""

    request_id: str
    venue: str
    data_scope: DataScope
    transport_origin: TransportOrigin
    origin: str
    endpoint_template: str
    effective_parameters: tuple[tuple[str, str], ...]
    started_at_utc: datetime
    received_at_utc: datetime | None
    attempt_count: int
    outcome: RequestOutcome
    status_code: int | None = None
    response_identities: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_identifier(self.request_id, "request_id")
        _require_identifier(self.venue, "venue")
        if self.data_scope not in ("production", "demo", "synthetic", "unknown"):
            raise ValueError("unsupported data_scope")
        if self.transport_origin not in ("library_default", "caller_supplied"):
            raise ValueError("unsupported transport_origin")
        if self.outcome not in (
            "success",
            "http_error",
            "transport_error",
            "timeout",
            "cancelled",
            "invalid_response",
            "error",
        ):
            raise ValueError("unsupported request outcome")
        _require_identifier(self.origin, "origin")
        if not self.endpoint_template.startswith("/"):
            raise ValueError("endpoint_template must be an absolute path template")
        _require_utc(self.started_at_utc, "started_at_utc")
        if self.received_at_utc is not None:
            _require_utc(self.received_at_utc, "received_at_utc")
        if isinstance(self.attempt_count, bool) or not isinstance(self.attempt_count, int):
            raise TypeError("attempt_count must be an int")
        if self.attempt_count < 0:
            raise ValueError("attempt_count must be nonnegative")
        if self.status_code is not None and (
            isinstance(self.status_code, bool) or not isinstance(self.status_code, int)
        ):
            raise TypeError("status_code must be an int")
        for name, value in self.effective_parameters:
            _require_identifier(name, "effective parameter name")
            if not isinstance(value, str):
                raise TypeError("effective parameter values must be strings")
        for identity in self.response_identities:
            _require_identifier(identity, "response identity")


def _require_utc(value: datetime, name: str) -> None:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{name} must be timezone-aware UTC")


__all__ = [
    "DataScope",
    "InstrumentRef",
    "KalshiInstrumentRef",
    "KalshiMarketRef",
    "MarketRef",
    "PolymarketInstrumentRef",
    "PolymarketMarketRef",
    "RequestObservation",
    "RequestOutcome",
    "TransportOrigin",
]
