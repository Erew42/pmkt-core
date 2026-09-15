from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from json import JSONDecodeError
from typing import Any, NoReturn, Literal

import httpx

from pmkt.runtime import OperationExpiry
from pmkt.resolution.providers import KalshiResolutionProvider
from pmkt.exchanges.read_auth import ReadAuthenticationRequiredError
from pmkt.records import KalshiMarketRef
from pmkt.resolution._batch import resolve_ordered_batch
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
    _sanitized_error_message,
    error_record,
    utc_now_iso,
)


class InvalidResolutionEvidenceError(ValueError):
    """A venue response cannot safely be used as resolution evidence."""


_EXPECTED_SOURCE_ERRORS = (
    httpx.RequestError,
    JSONDecodeError,
    UnicodeDecodeError,
    InvalidResolutionEvidenceError,
)


@dataclass(frozen=True)
class _PreparedKalshiResolution:
    market_key: str
    typed_ref: KalshiMarketRef
    snapshot: dict[str, Any]
    has_snapshot: bool


def _mapping(payload: Mapping[str, Any] | Any | None) -> dict[str, Any]:
    if payload is None:
        return {}
    if isinstance(payload, Mapping):
        return dict(payload)
    if hasattr(payload, "to_dict"):
        result = payload.to_dict()
        return result if isinstance(result, dict) else {}
    return {}


def _snapshot_mapping(snapshot: Mapping[str, Any] | Any | None) -> dict[str, Any]:
    if snapshot is None:
        return {}
    if isinstance(snapshot, Mapping):
        return dict(snapshot)
    to_dict = getattr(snapshot, "to_dict", None)
    if not callable(to_dict):
        raise TypeError("snapshot must be a mapping or expose to_dict()")
    result = to_dict()
    if not isinstance(result, Mapping):
        raise TypeError("snapshot.to_dict() must return a mapping")
    return dict(result)


def _evidence_mapping(payload: object, *, source: str) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise InvalidResolutionEvidenceError(f"{source} returned a non-object payload")
    mapped = dict(payload)
    nested = mapped.get("market")
    return dict(nested) if isinstance(nested, Mapping) else mapped


def _require_market_input(value: KalshiMarketRef) -> tuple[str, KalshiMarketRef]:
    if not isinstance(value, KalshiMarketRef):
        raise TypeError("market must be a KalshiMarketRef")
    return value.ticker, value


def _raise_identity_mismatch(message: str, *, caller_input: bool) -> NoReturn:
    if caller_input:
        raise ValueError(message)
    raise InvalidResolutionEvidenceError(message)


def _identity_values(
    payload: Mapping[str, Any],
    fields: tuple[str, ...],
    *,
    source: str,
    caller_input: bool,
) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    for field in fields:
        if field not in payload or payload[field] is None:
            continue
        value = payload[field]
        if not isinstance(value, str) or not value.strip():
            _raise_identity_mismatch(
                f"{source} {field} identity must be a non-empty string",
                caller_input=caller_input,
            )
        values.append((field, value))
    return values


def _validate_typed_identity(
    payload: Mapping[str, Any],
    *,
    market: KalshiMarketRef,
    source: str,
    caller_input: bool,
) -> None:
    for field, observed_market in _identity_values(
        payload,
        ("market_key", "ticker", "market_ticker", "instrument_key"),
        source=source,
        caller_input=caller_input,
    ):
        if (
            observed_market is not None
            and observed_market.split(":")[0] != market.ticker
        ):
            _raise_identity_mismatch(
                f"{source} {field} identity {observed_market!r} does not match "
                f"KalshiMarketRef {market.ticker!r}",
                caller_input=caller_input,
            )
    series_values = _identity_values(
        payload,
        ("series_ticker", "seriesTicker"),
        source=source,
        caller_input=caller_input,
    )
    if len({value for _, value in series_values}) > 1:
        _raise_identity_mismatch(
            f"{source} contains contradictory series identities",
            caller_input=caller_input,
        )
    if market.series_ticker is not None:
        for field, observed_series in series_values:
            if observed_series is not None and observed_series != market.series_ticker:
                _raise_identity_mismatch(
                    f"{source} {field} identity {observed_series!r} does not match "
                    f"KalshiMarketRef enrichment {market.series_ticker!r}",
                    caller_input=caller_input,
                )


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
    message = _sanitized_error_message(error, source=source)
    return replace(
        record,
        error_message=message,
        source_observations=[
            replace(observation, source=source, error_message=message)
            for observation in record.source_observations
        ],
    )


class KalshiResolutionResolver:
    def __init__(self, client: KalshiResolutionProvider | None = None) -> None:
        self.client = client

    def _prepare_resolution_input(
        self,
        market_key: KalshiMarketRef,
        *,
        snapshot: Mapping[str, Any] | Any | None,
    ) -> _PreparedKalshiResolution:
        key, typed_ref = _require_market_input(market_key)
        snapshot_map = _snapshot_mapping(snapshot)
        _validate_typed_identity(
            snapshot_map,
            market=typed_ref,
            source="snapshot",
            caller_input=True,
        )
        return _PreparedKalshiResolution(
            market_key=key,
            typed_ref=typed_ref,
            snapshot=snapshot_map,
            has_snapshot=snapshot is not None,
        )

    def _prepare_batch_input(
        self, market: KalshiMarketRef
    ) -> _PreparedKalshiResolution:
        if not isinstance(market, KalshiMarketRef):
            raise TypeError("markets must contain only KalshiMarketRef values")
        return self._prepare_resolution_input(market, snapshot=None)

    async def _market_payload(
        self,
        ticker: str,
        *,
        source: Literal["live", "historical"],
        expiry: OperationExpiry,
    ) -> dict[str, Any]:
        client = self.client
        assert client is not None
        if source == "live":
            payload = await expiry.run(lambda: client.market(ticker, expiry=expiry))
        else:
            payload = await expiry.run(lambda: client.historical_market(ticker, expiry=expiry))
        expiry.checkpoint()
        result = _evidence_mapping(payload, source=f"kalshi_{source}_rest")
        expiry.checkpoint()
        return result



    async def resolve(
        self,
        market_key: KalshiMarketRef,
        *,
        snapshot: Mapping[str, Any] | Any | None = None,
        deadline_s: float | None = None,
    ) -> ResolutionRecord:
        return await self._resolve_with_expiry(
            market_key,
            snapshot=snapshot,
            expiry=OperationExpiry.after(deadline_s),
        )

    async def resolve_many(
        self,
        markets: Sequence[KalshiMarketRef],
        *,
        concurrency: int = 8,
        deadline_s: float = 120.0,
    ) -> list[ResolutionRecord]:
        return await resolve_ordered_batch(
            markets,
            concurrency=concurrency,
            deadline_s=deadline_s,
            prepare=self._prepare_batch_input,
            resolve_one=lambda prepared, expiry: self._resolve_prepared_with_expiry(
                prepared, expiry=expiry
            ),
        )

    async def _resolve_with_expiry(
        self,
        market_key: KalshiMarketRef,
        *,
        snapshot: Mapping[str, Any] | Any | None,
        expiry: OperationExpiry,
    ) -> ResolutionRecord:
        prepared = self._prepare_resolution_input(market_key, snapshot=snapshot)
        expiry.checkpoint()
        return await self._resolve_prepared_with_expiry(prepared, expiry=expiry)

    async def _resolve_prepared_with_expiry(
        self,
        prepared: _PreparedKalshiResolution,
        *,
        expiry: OperationExpiry,
    ) -> ResolutionRecord:
        market_key = prepared.market_key
        typed_ref = prepared.typed_ref
        snapshot_map = prepared.snapshot
        observed = utc_now_iso()
        observations: list[SourceObservation] = []
        snapshot_fallback: ResolutionRecord | None = None
        if prepared.has_snapshot:
            snapshot_record = kalshi_resolution_from_payload(
                snapshot_map,
                input_identifier=market_key,
                source="kalshi_snapshot",
                observed_at_utc=observed,
            )
            snapshot_fallback = _snapshot_fallback(snapshot_record)
            observations.extend(snapshot_fallback.source_observations)
            expiry.checkpoint()

        if self.client is None:
            record = snapshot_fallback or ResolutionRecord(
                platform="kalshi",
                market_key=market_key,
                input_identifier=market_key,
                resolution_state=STATE_UNAVAILABLE,
                confidence=CONFIDENCE_UNAVAILABLE,
                observed_at_utc=observed,
            )
            expiry.checkpoint()
            return record

        live_record: ResolutionRecord | None = None
        historical_record: ResolutionRecord | None = None
        best: ResolutionRecord | None = None
        try:
            live = await self._market_payload(
                market_key,
                source="live",
                expiry=expiry,
            )
            _validate_typed_identity(
                live,
                market=typed_ref,
                source="kalshi_rest",
                caller_input=False,
            )
            live_record = kalshi_resolution_from_payload(
                live,
                input_identifier=market_key,
                source="kalshi_rest",
                observed_at_utc=observed,
            )
            observations.extend(live_record.source_observations)
            best = live_record
            expiry.checkpoint()
        except ReadAuthenticationRequiredError:
            raise
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
        except _EXPECTED_SOURCE_ERRORS as exc:
            best = _endpoint_error_record(
                source="kalshi_rest",
                market_key=market_key,
                input_identifier=market_key,
                error=exc,
                observed_at_utc=observed,
            )
            observations.extend(best.source_observations)

        try:
            historical = await self._market_payload(
                market_key,
                source="historical",
                expiry=expiry,
            )
            _validate_typed_identity(
                historical,
                market=typed_ref,
                source="kalshi_historical_rest",
                caller_input=False,
            )
            historical_record = kalshi_resolution_from_payload(
                historical,
                input_identifier=market_key,
                source="kalshi_historical_rest",
                observed_at_utc=observed,
            )
            observations.extend(historical_record.source_observations)
            expiry.checkpoint()
        except ReadAuthenticationRequiredError:
            raise
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
        except _EXPECTED_SOURCE_ERRORS as exc:
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
                record = _inconsistent_authority_record(
                    market_key=live_record.market_key,
                    input_identifier=market_key,
                    observed_at_utc=observed,
                    left=live_record,
                    right=historical_record,
                    observations=observations,
                )
                expiry.checkpoint()
                return record
            if live_record.resolution_state == STATE_FINAL:
                record = _with_observations(live_record, observations)
                expiry.checkpoint()
                return record
            if live_record.resolution_state != STATE_UNAVAILABLE:
                record = _with_observations(live_record, observations)
                expiry.checkpoint()
                return record
            if historical_record.resolution_state == STATE_FINAL:
                record = _with_observations(historical_record, observations)
                expiry.checkpoint()
                return record

        if live_record is not None and live_record.resolution_state == STATE_FINAL:
            record = _with_observations(live_record, observations)
            expiry.checkpoint()
            return record
        if (
            live_record is not None
            and live_record.resolution_state != STATE_UNAVAILABLE
        ):
            record = _with_observations(live_record, observations)
            expiry.checkpoint()
            return record
        if (
            historical_record is not None
            and historical_record.resolution_state == STATE_FINAL
        ):
            record = _with_observations(historical_record, observations)
            expiry.checkpoint()
            return record
        if (
            historical_record is not None
            and historical_record.resolution_state != STATE_UNAVAILABLE
            and (best is None or best.resolution_state == STATE_UNAVAILABLE)
        ):
            record = _with_observations(historical_record, observations)
            expiry.checkpoint()
            return record
        if best is not None and best.error_type:
            record = _with_observations(best, observations)
            expiry.checkpoint()
            return record
        if snapshot_fallback is not None:
            record = _with_observations(snapshot_fallback, observations)
            expiry.checkpoint()
            return record
        if historical_record is not None and (
            best is None or best.resolution_state == STATE_UNAVAILABLE
        ):
            record = _with_observations(historical_record, observations)
            expiry.checkpoint()
            return record

        record = best or ResolutionRecord(
            platform="kalshi",
            market_key=market_key,
            input_identifier=market_key,
            resolution_state=STATE_UNAVAILABLE,
            confidence=CONFIDENCE_UNAVAILABLE,
            observed_at_utc=observed,
        )
        expiry.checkpoint()
        return record


__all__ = [
    "KalshiResolutionResolver",
    "kalshi_resolution_from_payload",
]
