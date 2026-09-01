from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from pmkt.data.canonical import market_resolution_row

RESOLVER_VERSION = "market_resolution_resolver.v2"

STATE_FINAL = "final"
STATE_OPEN = "open"
STATE_CLOSED_UNRESOLVED = "closed_unresolved"
STATE_PROVISIONAL = "provisional"
STATE_DISPUTED = "disputed"
STATE_ORACLE_RESOLVED_CHAIN_PENDING = "oracle_resolved_chain_pending"
STATE_PLATFORM_REPORTED_CHAIN_PENDING = "platform_reported_chain_pending"
STATE_METADATA_ONLY = "metadata_only"
STATE_INCONSISTENT = "inconsistent"
STATE_UNAVAILABLE = "unavailable"

CONFIDENCE_CANONICAL = "canonical"
CONFIDENCE_PLATFORM_CONFIRMED = "platform_confirmed"
CONFIDENCE_PROVISIONAL = "provisional"
CONFIDENCE_METADATA_ONLY = "metadata_only"
CONFIDENCE_INCONSISTENT = "inconsistent"
CONFIDENCE_UNAVAILABLE = "unavailable"

RESULT_TYPE_BINARY = "binary"
RESULT_TYPE_SCALAR = "scalar"
RESULT_TYPE_FRACTIONAL = "fractional"
RESULT_TYPE_UNKNOWN = "unknown"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if value != value:
            return None
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def compact_dict(values: dict[str, Any]) -> dict[str, Any]:
    return {
        key: _json_safe(value)
        for key, value in values.items()
        if value is not None and value != ""
    }


@dataclass(frozen=True)
class Payout:
    outcome_index: int | None = None
    outcome: str | None = None
    numerator: str | None = None
    denominator: str | None = None
    payout: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return compact_dict(
            {
                "outcome_index": self.outcome_index,
                "outcome": self.outcome,
                "numerator": self.numerator,
                "denominator": self.denominator,
                "payout": self.payout,
            }
        )


@dataclass(frozen=True)
class SourceObservation:
    source: str
    confidence: str
    observed_at_utc: str = field(default_factory=utc_now_iso)
    raw_status: str | None = None
    request_url: str | None = None
    evidence: dict[str, Any] | None = None
    error_type: str | None = None
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return compact_dict(
            {
                "source": self.source,
                "confidence": self.confidence,
                "observed_at_utc": self.observed_at_utc,
                "raw_status": self.raw_status,
                "request_url": self.request_url,
                "evidence": self.evidence,
                "error_type": self.error_type,
                "error_message": self.error_message,
            }
        )


@dataclass(frozen=True)
class ResolutionRecord:
    platform: str
    market_key: str
    input_identifier: str | None
    resolution_state: str
    result_type: str = RESULT_TYPE_UNKNOWN
    confidence: str = CONFIDENCE_UNAVAILABLE
    canonical_source: str | None = None
    result: str | None = None
    winner: str | None = None
    payouts: list[Payout] = field(default_factory=list)
    source_observations: list[SourceObservation] = field(default_factory=list)
    condition_id: str | None = None
    raw_status: str | None = None
    settlement_value_dollars: str | None = None
    settlement_ts: str | None = None
    observed_at_utc: str = field(default_factory=utc_now_iso)
    resolver_version: str = RESOLVER_VERSION
    error_type: str | None = None
    error_message: str | None = None

    def to_row(self) -> dict[str, Any]:
        return market_resolution_row(
            platform=self.platform,
            market_key=self.market_key,
            input_identifier=self.input_identifier,
            resolution_state=self.resolution_state,
            result_type=self.result_type,
            confidence=self.confidence,
            canonical_source=self.canonical_source,
            result=self.result,
            winner=self.winner,
            payouts_json=[payout.to_dict() for payout in self.payouts],
            source_observations_json=[
                observation.to_dict() for observation in self.source_observations
            ],
            condition_id=self.condition_id,
            raw_status=self.raw_status,
            settlement_value_dollars=self.settlement_value_dollars,
            settlement_ts=self.settlement_ts,
            observed_at_utc=self.observed_at_utc,
            resolver_version=self.resolver_version,
            error_type=self.error_type,
            error_message=self.error_message,
        )


def error_record(
    *,
    platform: str,
    market_key: str,
    input_identifier: str | None,
    error: BaseException,
    observed_at_utc: str | None = None,
) -> ResolutionRecord:
    observed = observed_at_utc or utc_now_iso()
    return ResolutionRecord(
        platform=platform,
        market_key=market_key,
        input_identifier=input_identifier,
        resolution_state=STATE_UNAVAILABLE,
        confidence=CONFIDENCE_UNAVAILABLE,
        observed_at_utc=observed,
        error_type=type(error).__name__,
        error_message=str(error),
        source_observations=[
            SourceObservation(
                source=platform,
                confidence=CONFIDENCE_UNAVAILABLE,
                observed_at_utc=observed,
                error_type=type(error).__name__,
                error_message=str(error),
            )
        ],
    )


__all__ = [
    "CONFIDENCE_CANONICAL",
    "CONFIDENCE_INCONSISTENT",
    "CONFIDENCE_METADATA_ONLY",
    "CONFIDENCE_PLATFORM_CONFIRMED",
    "CONFIDENCE_PROVISIONAL",
    "CONFIDENCE_UNAVAILABLE",
    "Payout",
    "RESOLVER_VERSION",
    "RESULT_TYPE_BINARY",
    "RESULT_TYPE_FRACTIONAL",
    "RESULT_TYPE_SCALAR",
    "RESULT_TYPE_UNKNOWN",
    "ResolutionRecord",
    "STATE_CLOSED_UNRESOLVED",
    "STATE_DISPUTED",
    "STATE_FINAL",
    "STATE_INCONSISTENT",
    "STATE_METADATA_ONLY",
    "STATE_OPEN",
    "STATE_ORACLE_RESOLVED_CHAIN_PENDING",
    "STATE_PLATFORM_REPORTED_CHAIN_PENDING",
    "STATE_PROVISIONAL",
    "STATE_UNAVAILABLE",
    "SourceObservation",
    "compact_dict",
    "error_record",
    "utc_now_iso",
]
