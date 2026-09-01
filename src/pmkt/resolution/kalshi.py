from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from pmkt.resolution.models import (
    CONFIDENCE_CANONICAL,
    CONFIDENCE_INCONSISTENT,
    CONFIDENCE_METADATA_ONLY,
    CONFIDENCE_PROVISIONAL,
    CONFIDENCE_UNAVAILABLE,
    Payout,
    RESULT_TYPE_BINARY,
    RESULT_TYPE_SCALAR,
    RESULT_TYPE_UNKNOWN,
    ResolutionRecord,
    STATE_CLOSED_UNRESOLVED,
    STATE_DISPUTED,
    STATE_FINAL,
    STATE_INCONSISTENT,
    STATE_METADATA_ONLY,
    STATE_OPEN,
    STATE_PROVISIONAL,
    STATE_UNAVAILABLE,
    SourceObservation,
    error_record,
    utc_now_iso,
)


def _mapping(payload: Mapping[str, Any] | Any | None) -> dict[str, Any]:
    if payload is None:
        return {}
    if isinstance(payload, Mapping):
        return dict(payload)
    if hasattr(payload, "to_dict"):
        result = payload.to_dict()
        return result if isinstance(result, dict) else {}
    return {}


def _first(payload: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key not in payload:
            continue
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, str) and value == "":
            continue
        try:
            if value != value:
                continue
        except (TypeError, ValueError):
            pass
        return value
    return None


def _text(payload: Mapping[str, Any], *keys: str) -> str | None:
    value = _first(payload, *keys)
    return None if value is None else str(value)


def _status(payload: Mapping[str, Any]) -> str | None:
    value = _text(payload, "status", "raw_status")
    return value.strip().lower() if value else None


def _market_key(payload: Mapping[str, Any], fallback: str | None = None) -> str:
    value = _text(payload, "market_key", "ticker", "market_ticker", "instrument_key")
    if value:
        return value.split(":")[0]
    if fallback:
        return fallback
    raise ValueError("Kalshi market row has no market key/ticker")


def _settlement_value(payload: Mapping[str, Any]) -> str | None:
    return _text(
        payload,
        "settlement_value_dollars",
        "settlementValueDollars",
        "settlement_value",
        "settlementValue",
    )


def _settlement_ts(payload: Mapping[str, Any]) -> str | None:
    return _text(
        payload,
        "settlement_ts",
        "settlementTime",
        "settlement_time",
        "settledTime",
        "settled_time",
    )


def _result(payload: Mapping[str, Any]) -> str | None:
    return _text(payload, "result", "settlement_result", "settlementResult")


def _settlement_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    try:
        settlement = Decimal(value)
    except InvalidOperation:
        return None
    return settlement if settlement.is_finite() else None


def _decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return format(normalized, "f")
    return format(normalized, "f").rstrip("0").rstrip(".")


def _binary_result(result: str | None, settlement_value: str | None) -> str | None:
    settlement = _settlement_decimal(settlement_value)
    if settlement == Decimal("1"):
        return "yes"
    if settlement == Decimal("0"):
        return "no"
    if settlement_value is not None:
        return None
    if result:
        normalized = result.strip().lower()
        if normalized in {"yes", "y", "true", "1", "yes_wins", "yes wins"}:
            return "yes"
        if normalized in {"no", "n", "false", "0", "no_wins", "no wins"}:
            return "no"
    return None


def _binary_payouts(winner: str) -> list[Payout]:
    return [
        Payout(
            outcome_index=0,
            outcome="yes",
            numerator="1" if winner == "yes" else "0",
            denominator="1",
            payout="1" if winner == "yes" else "0",
        ),
        Payout(
            outcome_index=1,
            outcome="no",
            numerator="1" if winner == "no" else "0",
            denominator="1",
            payout="1" if winner == "no" else "0",
        ),
    ]


def _apply_source_authority(
    record: ResolutionRecord, *, source: str
) -> ResolutionRecord:
    if source == "kalshi_snapshot":
        return _snapshot_fallback(record)
    return record


def kalshi_resolution_from_payload(
    payload: Mapping[str, Any] | Any,
    *,
    input_identifier: str | None = None,
    source: str = "kalshi_snapshot",
    observed_at_utc: str | None = None,
) -> ResolutionRecord:
    market = _mapping(payload)
    observed = observed_at_utc or utc_now_iso()
    market_key = _market_key(market, fallback=input_identifier)
    status = _status(market)
    result = _result(market)
    settlement_value = _settlement_value(market)
    settlement_decimal = _settlement_decimal(settlement_value)
    settlement_ts = _settlement_ts(market)
    observation = SourceObservation(
        source=source,
        confidence=CONFIDENCE_UNAVAILABLE,
        observed_at_utc=observed,
        raw_status=status,
    )

    has_authoritative_settlement = settlement_decimal is not None and (
        status in {"finalized", "settled"}
        or (source == "kalshi_historical_rest" and status is None)
    )
    if has_authoritative_settlement and settlement_decimal is not None:
        settlement_text = _decimal_text(settlement_decimal)
        winner = _binary_result(result, settlement_value)
        if winner:
            record = ResolutionRecord(
                platform="kalshi",
                market_key=market_key,
                input_identifier=input_identifier or market_key,
                resolution_state=STATE_FINAL,
                result_type=RESULT_TYPE_BINARY,
                confidence=CONFIDENCE_CANONICAL,
                canonical_source=source,
                result=winner,
                winner=winner,
                payouts=_binary_payouts(winner),
                source_observations=[
                    SourceObservation(
                        source=source,
                        confidence=CONFIDENCE_CANONICAL,
                        observed_at_utc=observed,
                        raw_status=status,
                    )
                ],
                raw_status=status,
                settlement_value_dollars=settlement_value,
                settlement_ts=settlement_ts,
                observed_at_utc=observed,
            )
            return _apply_source_authority(record, source=source)
        record = ResolutionRecord(
            platform="kalshi",
            market_key=market_key,
            input_identifier=input_identifier or market_key,
            resolution_state=STATE_FINAL,
            result_type=RESULT_TYPE_SCALAR,
            confidence=CONFIDENCE_CANONICAL,
            canonical_source=source,
            result=settlement_text,
            payouts=[
                Payout(
                    outcome="scalar",
                    numerator=settlement_text,
                    denominator="1",
                    payout=settlement_text,
                )
            ],
            source_observations=[
                SourceObservation(
                    source=source,
                    confidence=CONFIDENCE_CANONICAL,
                    observed_at_utc=observed,
                    raw_status=status,
                )
            ],
            raw_status=status,
            settlement_value_dollars=settlement_value,
            settlement_ts=settlement_ts,
            observed_at_utc=observed,
        )
        return _apply_source_authority(record, source=source)

    if status in {"determined", "amended"}:
        return ResolutionRecord(
            platform="kalshi",
            market_key=market_key,
            input_identifier=input_identifier or market_key,
            resolution_state=STATE_PROVISIONAL,
            confidence=CONFIDENCE_PROVISIONAL,
            result_type=RESULT_TYPE_BINARY
            if _binary_result(result, settlement_value)
            else RESULT_TYPE_UNKNOWN,
            result=_binary_result(result, settlement_value) or result,
            winner=_binary_result(result, settlement_value),
            source_observations=[
                SourceObservation(
                    source=source,
                    confidence=CONFIDENCE_PROVISIONAL,
                    observed_at_utc=observed,
                    raw_status=status,
                )
            ],
            raw_status=status,
            settlement_value_dollars=settlement_value,
            settlement_ts=settlement_ts,
            observed_at_utc=observed,
        )

    if status == "disputed":
        state = STATE_DISPUTED
    elif status in {"closed", "settled", "finalized"}:
        state = STATE_CLOSED_UNRESOLVED
    elif status in {"active", "open", "initialized", "inactive"}:
        state = STATE_OPEN
    else:
        state = STATE_UNAVAILABLE

    return ResolutionRecord(
        platform="kalshi",
        market_key=market_key,
        input_identifier=input_identifier or market_key,
        resolution_state=state,
        result_type=RESULT_TYPE_UNKNOWN,
        confidence=CONFIDENCE_UNAVAILABLE,
        result=result,
        source_observations=[observation],
        raw_status=status,
        settlement_value_dollars=settlement_value,
        settlement_ts=settlement_ts,
        observed_at_utc=observed,
    )


def _snapshot_fallback(record: ResolutionRecord) -> ResolutionRecord:
    if (
        record.resolution_state != STATE_FINAL
        or record.confidence != CONFIDENCE_CANONICAL
    ):
        return record
    return replace(
        record,
        resolution_state=STATE_METADATA_ONLY,
        confidence=CONFIDENCE_METADATA_ONLY,
        canonical_source=None,
        source_observations=[
            replace(observation, confidence=CONFIDENCE_METADATA_ONLY)
            for observation in record.source_observations
        ],
    )


def _with_observations(
    record: ResolutionRecord,
    observations: list[SourceObservation],
) -> ResolutionRecord:
    return replace(
        record,
        source_observations=observations or record.source_observations,
    )


def _final_result_key(record: ResolutionRecord) -> tuple[str, str | None]:
    if record.winner:
        return ("binary", record.winner.lower())
    settlement = _settlement_decimal(record.settlement_value_dollars)
    if settlement is not None:
        return ("settlement", _decimal_text(settlement))
    if record.result:
        return ("result", record.result.lower())
    return ("settlement", None)


def _final_records_conflict(left: ResolutionRecord, right: ResolutionRecord) -> bool:
    if left.resolution_state != STATE_FINAL or right.resolution_state != STATE_FINAL:
        return False
    return _final_result_key(left) != _final_result_key(right)


def _inconsistent_authority_record(
    *,
    market_key: str,
    input_identifier: str,
    observed_at_utc: str,
    left: ResolutionRecord,
    right: ResolutionRecord,
    observations: list[SourceObservation],
) -> ResolutionRecord:
    return ResolutionRecord(
        platform="kalshi",
        market_key=market_key,
        input_identifier=input_identifier,
        resolution_state=STATE_INCONSISTENT,
        result_type=RESULT_TYPE_UNKNOWN,
        confidence=CONFIDENCE_INCONSISTENT,
        source_observations=observations,
        observed_at_utc=observed_at_utc,
        error_type="KalshiAuthorityConflict",
        error_message=(
            f"{left.canonical_source or 'kalshi_rest'} result {left.result!r} "
            f"conflicts with {right.canonical_source or 'kalshi_historical_rest'} "
            f"result {right.result!r}"
        ),
    )


def _endpoint_error_record(
    *,
    source: str,
    market_key: str,
    input_identifier: str,
    error: BaseException,
    observed_at_utc: str,
) -> ResolutionRecord:
    record = error_record(
        platform="kalshi",
        market_key=market_key,
        input_identifier=input_identifier,
        error=error,
        observed_at_utc=observed_at_utc,
    )
    return replace(
        record,
        source_observations=[
            replace(observation, source=source)
            for observation in record.source_observations
        ],
    )


class KalshiResolutionResolver:
    def __init__(self, client: Any | None = None) -> None:
        self.client = client

    async def resolve(
        self,
        market_key: str,
        *,
        snapshot: Mapping[str, Any] | Any | None = None,
    ) -> ResolutionRecord:
        observed = utc_now_iso()
        observations: list[SourceObservation] = []
        snapshot_fallback: ResolutionRecord | None = None
        if snapshot is not None:
            snapshot_record = kalshi_resolution_from_payload(
                snapshot,
                input_identifier=market_key,
                source="kalshi_snapshot",
                observed_at_utc=observed,
            )
            snapshot_fallback = _snapshot_fallback(snapshot_record)
            observations.extend(snapshot_fallback.source_observations)

        if self.client is None:
            return snapshot_fallback or ResolutionRecord(
                platform="kalshi",
                market_key=market_key,
                input_identifier=market_key,
                resolution_state=STATE_UNAVAILABLE,
                confidence=CONFIDENCE_UNAVAILABLE,
                observed_at_utc=observed,
            )

        live_record: ResolutionRecord | None = None
        historical_record: ResolutionRecord | None = None
        best: ResolutionRecord | None = None
        try:
            live = await self.client.market(market_key)
            live_record = kalshi_resolution_from_payload(
                live,
                input_identifier=market_key,
                source="kalshi_rest",
                observed_at_utc=observed,
            )
            observations.extend(live_record.source_observations)
            best = live_record
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                best = _endpoint_error_record(
                    source="kalshi_rest",
                    market_key=market_key,
                    input_identifier=market_key,
                    error=exc,
                    observed_at_utc=observed,
                )
                observations.extend(best.source_observations)
        except Exception as exc:  # pragma: no cover - defensive live API guard
            best = _endpoint_error_record(
                source="kalshi_rest",
                market_key=market_key,
                input_identifier=market_key,
                error=exc,
                observed_at_utc=observed,
            )
            observations.extend(best.source_observations)

        try:
            historical = await self.client.historical_market(market_key)
            historical_record = kalshi_resolution_from_payload(
                historical,
                input_identifier=market_key,
                source="kalshi_historical_rest",
                observed_at_utc=observed,
            )
            observations.extend(historical_record.source_observations)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                best = _endpoint_error_record(
                    source="kalshi_historical_rest",
                    market_key=market_key,
                    input_identifier=market_key,
                    error=exc,
                    observed_at_utc=observed,
                )
                observations.extend(best.source_observations)
        except Exception as exc:  # pragma: no cover - defensive historical API guard
            best = _endpoint_error_record(
                source="kalshi_historical_rest",
                market_key=market_key,
                input_identifier=market_key,
                error=exc,
                observed_at_utc=observed,
            )
            observations.extend(best.source_observations)

        if live_record is not None and historical_record is not None:
            if _final_records_conflict(live_record, historical_record):
                return _inconsistent_authority_record(
                    market_key=live_record.market_key,
                    input_identifier=market_key,
                    observed_at_utc=observed,
                    left=live_record,
                    right=historical_record,
                    observations=observations,
                )
            if live_record.resolution_state == STATE_FINAL:
                return _with_observations(live_record, observations)
            if live_record.resolution_state != STATE_UNAVAILABLE:
                return _with_observations(live_record, observations)
            if historical_record.resolution_state == STATE_FINAL:
                return _with_observations(historical_record, observations)

        if live_record is not None and live_record.resolution_state == STATE_FINAL:
            return _with_observations(live_record, observations)
        if (
            live_record is not None
            and live_record.resolution_state != STATE_UNAVAILABLE
        ):
            return _with_observations(live_record, observations)
        if (
            historical_record is not None
            and historical_record.resolution_state == STATE_FINAL
        ):
            return _with_observations(historical_record, observations)
        if (
            historical_record is not None
            and historical_record.resolution_state != STATE_UNAVAILABLE
            and (best is None or best.resolution_state == STATE_UNAVAILABLE)
        ):
            return _with_observations(historical_record, observations)
        if best is not None and best.error_type:
            return _with_observations(best, observations)
        if snapshot_fallback is not None:
            return _with_observations(snapshot_fallback, observations)
        if historical_record is not None and (
            best is None or best.resolution_state == STATE_UNAVAILABLE
        ):
            return _with_observations(historical_record, observations)

        return best or ResolutionRecord(
            platform="kalshi",
            market_key=market_key,
            input_identifier=market_key,
            resolution_state=STATE_UNAVAILABLE,
            confidence=CONFIDENCE_UNAVAILABLE,
            observed_at_utc=observed,
        )


__all__ = [
    "KalshiResolutionResolver",
    "kalshi_resolution_from_payload",
]
