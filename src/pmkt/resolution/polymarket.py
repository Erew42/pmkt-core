from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from fractions import Fraction
from json import JSONDecodeError
from typing import Any, NoReturn

import httpx

from pmkt.runtime import OperationExpiry
from pmkt.resolution.providers import ClobResolutionProvider, GammaResolutionProvider, CtfResolutionProvider
from pmkt.records import PolymarketMarketRef
from pmkt.resolution._batch import resolve_ordered_batch
from pmkt.resolution.evm import EvmRpcError, _normalize_hex32
from pmkt.resolution.models import (
    CONFIDENCE_CANONICAL,
    CONFIDENCE_INCONSISTENT,
    CONFIDENCE_METADATA_ONLY,
    CONFIDENCE_UNAVAILABLE,
    Payout,
    RESULT_TYPE_BINARY,
    RESULT_TYPE_FRACTIONAL,
    RESULT_TYPE_UNKNOWN,
    ResolutionRecord,
    STATE_FINAL,
    STATE_INCONSISTENT,
    STATE_METADATA_ONLY,
    STATE_OPEN,
    STATE_UNAVAILABLE,
    SourceObservation,
    _sanitized_error_message,
    utc_now_iso,
)

class InvalidResolutionEvidenceError(ValueError):
    """A venue response cannot safely be used as resolution evidence."""


_EXPECTED_SOURCE_ERRORS = (
    httpx.HTTPStatusError,
    httpx.RequestError,
    JSONDecodeError,
    UnicodeDecodeError,
    InvalidResolutionEvidenceError,
)


@dataclass(frozen=True)
class _PreparedPolymarketResolution:
    input_identifier: str
    typed_ref: PolymarketMarketRef
    snapshot: dict[str, Any]
    market_key: str
    condition_id: str | None


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
    return dict(payload)


def _require_market_input(value: PolymarketMarketRef) -> tuple[str, PolymarketMarketRef]:
    if not isinstance(value, PolymarketMarketRef):
        raise TypeError("market must be a PolymarketMarketRef")
    return value.market_id, value


def _same_condition_id(left: str, right: str) -> bool:
    return left.strip().lower() == right.strip().lower()


def _validate_condition_id(
    condition_id: str, *, source: str, caller_input: bool
) -> str:
    try:
        _normalize_hex32(condition_id)
    except ValueError as exc:
        if caller_input:
            raise
        raise InvalidResolutionEvidenceError(
            f"{source} returned a malformed condition identity"
        ) from exc
    return condition_id


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
    market: PolymarketMarketRef,
    source: str,
    expected_condition_id: str | None = None,
    caller_input: bool,
) -> None:
    for field, observed_market in _identity_values(
        payload,
        ("market_key", "market_id", "id"),
        source=source,
        caller_input=caller_input,
    ):
        if observed_market != market.market_id:
            _raise_identity_mismatch(
                f"{source} {field} identity {observed_market!r} does not match "
                f"PolymarketMarketRef {market.market_id!r}",
                caller_input=caller_input,
            )
    expected_condition = expected_condition_id or market.condition_id
    condition_values = _identity_values(
        payload,
        ("condition_id", "conditionId", "conditionID"),
        source=source,
        caller_input=caller_input,
    )
    if len({value.strip().lower() for _, value in condition_values}) > 1:
        _raise_identity_mismatch(
            f"{source} contains contradictory condition identities",
            caller_input=caller_input,
        )
    if expected_condition is not None:
        for field, observed_condition in condition_values:
            if observed_condition is not None and not _same_condition_id(
                observed_condition, expected_condition
            ):
                _raise_identity_mismatch(
                    f"{source} {field} identity {observed_condition!r} does not "
                    f"match PolymarketMarketRef enrichment {expected_condition!r}",
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


def _parse_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if hasattr(value, "tolist"):
        listed = value.tolist()
        return listed if isinstance(listed, list) else []
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _label(value: Any) -> str | None:
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned.lower() if cleaned else None
    if isinstance(value, Mapping):
        for key in ("outcome", "o", "label", "name", "title"):
            text = _text(value, key)
            if text:
                return text.strip().lower()
    return None


def _labels(*payloads: Mapping[str, Any]) -> list[str]:
    for payload in payloads:
        if not payload:
            continue
        raw_labels = _parse_array(
            _first(payload, "outcome_labels_json", "outcomes", "outcomeLabels")
        )
        labels = [label for item in raw_labels if (label := _label(item))]
        if labels:
            return labels
    return []


def _labels_with_source(
    snapshot_payload: Mapping[str, Any],
    gamma_payload: Mapping[str, Any],
) -> tuple[list[str], str, Mapping[str, Any]]:
    for source, payload in (
        ("polymarket_snapshot", snapshot_payload),
        ("polymarket_gamma", gamma_payload),
    ):
        labels = _labels(payload)
        if labels:
            return labels, source, payload
    return ["yes", "no"], "polymarket_default_binary_labels", {}


def _prices(*payloads: Mapping[str, Any]) -> list[float]:
    for payload in payloads:
        if not payload:
            continue
        raw_prices = _parse_array(
            _first(payload, "outcome_prices_json", "outcome_prices", "outcomePrices")
        )
        prices: list[float] = []
        for value in raw_prices:
            try:
                prices.append(float(value))
            except (TypeError, ValueError, OverflowError):
                prices = []
                break
        if prices:
            return prices
    return []


def _condition_id(*payloads: Mapping[str, Any]) -> str | None:
    for payload in payloads:
        value = _text(payload, "condition_id", "conditionId", "conditionID")
        if value:
            return value
    return None


def _market_key(payload: Mapping[str, Any], fallback: str | None = None) -> str:
    value = _text(payload, "market_key", "market_id", "id", "slug")
    if value:
        return value
    if fallback:
        return fallback
    raise ValueError("Polymarket market row has no market id/key")


def _payload_hash(payload: Mapping[str, Any]) -> str | None:
    if not payload:
        return None
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _label_mapping_observation(
    *,
    source: str,
    payload: Mapping[str, Any],
    market_key: str,
    condition_id: str,
    labels: list[str],
    observed_at_utc: str,
) -> SourceObservation:
    evidence: dict[str, Any] = {
        "market_key": market_key,
        "condition_id": condition_id,
        "outcome_labels": [
            {"outcome_index": index, "outcome": label}
            for index, label in enumerate(labels)
        ],
        "status": "success",
    }
    payload_digest = _payload_hash(payload)
    if payload_digest is not None:
        evidence["payload_sha256"] = payload_digest
    return SourceObservation(
        source=source,
        confidence=CONFIDENCE_METADATA_ONLY,
        observed_at_utc=observed_at_utc,
        evidence=evidence,
    )


def _endpoint_success_observation(
    *,
    source: str,
    payload: Mapping[str, Any],
    market_key: str,
    condition_id: str | None,
    observed_at_utc: str,
    evidence: Mapping[str, Any] | None = None,
) -> SourceObservation:
    observation_evidence: dict[str, Any] = {
        "market_key": market_key,
        "status": "success",
    }
    if condition_id:
        observation_evidence["condition_id"] = condition_id
    payload_digest = _payload_hash(payload)
    if payload_digest is not None:
        observation_evidence["payload_sha256"] = payload_digest
    if evidence:
        observation_evidence.update(dict(evidence))
    return SourceObservation(
        source=source,
        confidence=CONFIDENCE_METADATA_ONLY,
        observed_at_utc=observed_at_utc,
        evidence=observation_evidence,
    )


def _endpoint_error_observation(
    *,
    source: str,
    market_key: str,
    condition_id: str | None,
    observed_at_utc: str,
    error: BaseException,
) -> SourceObservation:
    evidence: dict[str, Any] = {
        "market_key": market_key,
        "status": "failure",
    }
    if condition_id:
        evidence["condition_id"] = condition_id
    return SourceObservation(
        source=source,
        confidence=CONFIDENCE_UNAVAILABLE,
        observed_at_utc=observed_at_utc,
        evidence=evidence,
        error_type=type(error).__name__,
        error_message=_sanitized_error_message(error, source=source),
    )


def _metadata_resolved(*payloads: Mapping[str, Any]) -> bool:
    for payload in payloads:
        if _metadata_resolved_status(payload):
            return True
    return False


def _metadata_resolved_status(payload: Mapping[str, Any]) -> str | None:
    raw_status = _text(
        payload, "uma_resolution_status", "umaResolutionStatus", "status"
    )
    status = (raw_status or "").strip().lower()
    return (
        raw_status if status in {"resolved", "final", "finalized", "settled"} else None
    )


def _metadata_observation(
    *,
    snapshot_payload: Mapping[str, Any],
    gamma_payload: Mapping[str, Any],
    observed_at_utc: str,
) -> SourceObservation | None:
    for source, payload in (
        ("polymarket_gamma", gamma_payload),
        ("polymarket_snapshot", snapshot_payload),
    ):
        raw_status = _metadata_resolved_status(payload)
        if raw_status:
            return SourceObservation(
                source=source,
                confidence=CONFIDENCE_METADATA_ONLY,
                observed_at_utc=observed_at_utc,
                raw_status=raw_status,
            )
    return None


def _clob_tokens(clob_payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    for key in ("t", "tokens", "clobTokens", "outcomes"):
        tokens = _parse_array(clob_payload.get(key))
        dict_tokens = [token for token in tokens if isinstance(token, Mapping)]
        if dict_tokens:
            return dict_tokens
    return []


def _clob_token_evidence(
    clob_payload: Mapping[str, Any],
    labels: list[str],
) -> dict[str, Any]:
    tokens = []
    for index, token in enumerate(_clob_tokens(clob_payload)):
        token_id = _text(token, "token_id", "tokenId", "id", "t")
        entry = {
            "outcome_index": index,
            "outcome": _label(token)
            or (labels[index] if index < len(labels) else None),
        }
        if token_id:
            entry["token_id"] = token_id
        tokens.append({key: value for key, value in entry.items() if value is not None})
    return {"tokens": tokens} if tokens else {}


def _platform_winner(
    *,
    labels: list[str],
    clob_payload: Mapping[str, Any],
    prices: list[float],
) -> str | None:
    direct = _text(clob_payload, "winner", "winningOutcome", "resolvedOutcome")
    if direct and direct.lower() not in {"true", "false"}:
        return direct.strip().lower()

    for index, token in enumerate(_clob_tokens(clob_payload)):
        winner_flag = _first(token, "winner", "isWinner", "winning")
        is_winner = winner_flag is True or (
            isinstance(winner_flag, str) and winner_flag.lower() == "true"
        )
        if not is_winner:
            continue
        return _label(token) or (labels[index] if index < len(labels) else None)

    if prices and len(prices) >= 2:
        max_price = max(prices)
        max_index = prices.index(max_price)
        other_prices = [
            price for index, price in enumerate(prices) if index != max_index
        ]
        if max_price >= 0.99 and all(price <= 0.01 for price in other_prices):
            return labels[max_index] if max_index < len(labels) else str(max_index)
    return None


def _payout_ratio(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "0"
    ratio = Fraction(numerator, denominator)
    if ratio.denominator == 1:
        return str(ratio.numerator)
    return f"{ratio.numerator}/{ratio.denominator}"


def _canonical_from_vector(
    *,
    market_key: str,
    input_identifier: str,
    condition_id: str,
    labels: list[str],
    label_observation: SourceObservation,
    auxiliary_observations: list[SourceObservation],
    denominator: int,
    numerators: list[int],
    observed_at_utc: str,
) -> ResolutionRecord:
    vector_error = _ctf_vector_error(denominator, numerators)
    if vector_error is None and labels and len(labels) != len(numerators):
        vector_error = "outcome label count does not match CTF numerator count"
        error_type = "OutcomeCountMismatch"
    else:
        error_type = "InvalidPayoutVector"
    if vector_error is not None:
        return ResolutionRecord(
            platform="polymarket",
            market_key=market_key,
            input_identifier=input_identifier,
            resolution_state=STATE_INCONSISTENT,
            result_type=RESULT_TYPE_UNKNOWN,
            confidence=CONFIDENCE_INCONSISTENT,
            condition_id=condition_id,
            source_observations=[
                *auxiliary_observations,
                label_observation,
                SourceObservation(
                    source="polygon_ctf",
                    confidence=CONFIDENCE_INCONSISTENT,
                    observed_at_utc=observed_at_utc,
                    evidence={"denominator": denominator, "numerators": numerators},
                ),
            ],
            observed_at_utc=observed_at_utc,
            error_type=error_type,
            error_message=vector_error,
        )

    effective_labels = labels or [str(index) for index in range(len(numerators))]
    payouts = [
        Payout(
            outcome_index=index,
            outcome=effective_labels[index]
            if index < len(effective_labels)
            else str(index),
            numerator=str(numerator),
            denominator=str(denominator),
            payout=_payout_ratio(numerator, denominator),
        )
        for index, numerator in enumerate(numerators)
    ]
    full_winners = [
        payout.outcome
        for payout, numerator in zip(payouts, numerators)
        if numerator == denominator and denominator > 0
    ]
    winner = full_winners[0] if len(full_winners) == 1 else None
    result_type = (
        RESULT_TYPE_BINARY
        if winner and len(numerators) == 2
        else RESULT_TYPE_FRACTIONAL
    )
    return ResolutionRecord(
        platform="polymarket",
        market_key=market_key,
        input_identifier=input_identifier,
        resolution_state=STATE_FINAL,
        result_type=result_type,
        confidence=CONFIDENCE_CANONICAL,
        canonical_source="polygon_ctf",
        result=winner or "fractional",
        winner=winner,
        payouts=payouts,
        source_observations=[
            *auxiliary_observations,
            label_observation,
            SourceObservation(
                source="polygon_ctf",
                confidence=CONFIDENCE_CANONICAL,
                observed_at_utc=observed_at_utc,
                evidence={"denominator": denominator, "numerators": numerators},
            ),
        ],
        condition_id=condition_id,
        observed_at_utc=observed_at_utc,
    )


def _ctf_vector_error(denominator: int, numerators: list[int]) -> str | None:
    if denominator <= 0:
        return "CTF payout denominator must be positive"
    if not numerators:
        return "CTF payout vector must include at least one numerator"
    if any(numerator < 0 for numerator in numerators):
        return "CTF payout vector contains a negative numerator"
    if any(numerator > denominator for numerator in numerators):
        return "CTF payout numerator exceeds denominator"
    if sum(numerators) != denominator:
        return "CTF payout numerators must sum to denominator"
    return None


class PolymarketResolutionResolver:
    def __init__(
        self,
        *,
        gamma_client: GammaResolutionProvider | None = None,
        clob_client: ClobResolutionProvider | None = None,
        ctf_client: CtfResolutionProvider | None = None,
    ) -> None:
        self.gamma_client = gamma_client
        self.clob_client = clob_client
        self.ctf_client = ctf_client
        self._ctf_chain_checked = False

    def _prepare_resolution_input(
        self,
        market_key: PolymarketMarketRef,
        *,
        snapshot: Mapping[str, Any] | Any | None,
    ) -> _PreparedPolymarketResolution:
        input_identifier, typed_ref = _require_market_input(market_key)
        snapshot_map = _snapshot_mapping(snapshot)
        _validate_typed_identity(
            snapshot_map,
            market=typed_ref,
            source="snapshot",
            caller_input=True,
        )
        key = typed_ref.market_id
        condition_id = _condition_id(snapshot_map) or (
            typed_ref.condition_id
        )
        if condition_id is not None and self.ctf_client is not None:
            condition_id = _validate_condition_id(
                condition_id,
                source="snapshot or PolymarketMarketRef",
                caller_input=True,
            )
        return _PreparedPolymarketResolution(
            input_identifier=input_identifier,
            typed_ref=typed_ref,
            snapshot=snapshot_map,
            market_key=key,
            condition_id=condition_id,
        )

    def _prepare_batch_input(
        self, market: PolymarketMarketRef
    ) -> _PreparedPolymarketResolution:
        if not isinstance(market, PolymarketMarketRef):
            raise TypeError("markets must contain only PolymarketMarketRef values")
        return self._prepare_resolution_input(market, snapshot=None)

    async def _gamma_market(
        self, market_key: str, *, expiry: OperationExpiry
    ) -> dict[str, Any]:
        client = self.gamma_client
        assert client is not None
        payload = await expiry.run(lambda: client.market(market_key, expiry=expiry))
        expiry.checkpoint()
        result = _evidence_mapping(payload, source="polymarket_gamma")
        expiry.checkpoint()
        return result

    async def _clob_market(
        self, condition_id: str, *, expiry: OperationExpiry
    ) -> dict[str, Any]:
        client = self.clob_client
        assert client is not None
        payload = await expiry.run(lambda: client.clob_market_info(condition_id, expiry=expiry))
        expiry.checkpoint()
        result = _evidence_mapping(payload, source="polymarket_clob")
        expiry.checkpoint()
        return result

    async def _ensure_ctf_chain(self, *, expiry: OperationExpiry) -> None:
        if self._ctf_chain_checked:
            expiry.checkpoint()
            return
        client = self.ctf_client
        assert client is not None
        await expiry.run(lambda: client.ensure_polygon(expiry=expiry))
        expiry.checkpoint()
        self._ctf_chain_checked = True

    async def _ctf_payout_vector(
        self,
        condition_id: str,
        outcome_count: int,
        *,
        expiry: OperationExpiry,
    ) -> tuple[int, list[int]]:
        client = self.ctf_client
        assert client is not None
        payload = await expiry.run(lambda: client.payout_vector(condition_id, outcome_count, expiry=expiry))
        expiry.checkpoint()
        if (
            not isinstance(payload, tuple)
            or len(payload) != 2
            or isinstance(payload[0], bool)
            or not isinstance(payload[0], int)
            or not isinstance(payload[1], (list, tuple))
            or any(isinstance(value, bool) or not isinstance(value, int) for value in payload[1])
        ):
            raise InvalidResolutionEvidenceError(
                "polygon_ctf returned an invalid payout vector"
            )
        result = payload[0], list(payload[1])
        expiry.checkpoint()
        return result



    async def resolve(
        self,
        market_key: PolymarketMarketRef,
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
        markets: Sequence[PolymarketMarketRef],
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
        market_key: PolymarketMarketRef,
        *,
        snapshot: Mapping[str, Any] | Any | None,
        expiry: OperationExpiry,
    ) -> ResolutionRecord:
        prepared = self._prepare_resolution_input(market_key, snapshot=snapshot)
        expiry.checkpoint()
        return await self._resolve_prepared_with_expiry(prepared, expiry=expiry)

    async def _resolve_prepared_with_expiry(
        self,
        prepared: _PreparedPolymarketResolution,
        *,
        expiry: OperationExpiry,
    ) -> ResolutionRecord:
        input_identifier = prepared.input_identifier
        typed_ref = prepared.typed_ref
        snapshot_map = prepared.snapshot
        key = prepared.market_key
        condition_id = prepared.condition_id
        observed = utc_now_iso()
        gamma_payload: dict[str, Any] = {}
        clob_payload: dict[str, Any] = {}
        endpoint_observations: list[SourceObservation] = []

        if self.gamma_client is not None and (
            not condition_id or not _labels(snapshot_map)
        ):
            try:
                gamma_payload = await self._gamma_market(key, expiry=expiry)
                _validate_typed_identity(
                    gamma_payload,
                    market=typed_ref,
                    source="polymarket_gamma",
                    expected_condition_id=condition_id,
                    caller_input=False,
                )
                candidate_condition_id = _condition_id(snapshot_map, gamma_payload) or (
                    typed_ref.condition_id
                )
                if candidate_condition_id is not None and self.ctf_client is not None:
                    candidate_condition_id = _validate_condition_id(
                        candidate_condition_id,
                        source="polymarket_gamma",
                        caller_input=False,
                    )
                condition_id = candidate_condition_id
                expiry.checkpoint()
                endpoint_observations.append(
                    _endpoint_success_observation(
                        source="polymarket_gamma",
                        payload=gamma_payload,
                        market_key=key,
                        condition_id=condition_id,
                        observed_at_utc=observed,
                    )
                )
            except _EXPECTED_SOURCE_ERRORS as exc:
                endpoint_observations.append(
                    _endpoint_error_observation(
                        source="polymarket_gamma",
                        market_key=key,
                        condition_id=condition_id,
                        observed_at_utc=observed,
                        error=exc,
                    )
                )
                gamma_payload = {}

        labels, label_source, label_payload = _labels_with_source(
            snapshot_map,
            gamma_payload,
        )
        expiry.checkpoint()
        label_observation = _label_mapping_observation(
            source=label_source,
            payload=label_payload,
            market_key=key,
            condition_id=condition_id or "",
            labels=labels,
            observed_at_utc=observed,
        )
        prices = _prices(snapshot_map, gamma_payload)
        expiry.checkpoint()

        if condition_id and self.clob_client is not None:
            try:
                clob_payload = await self._clob_market(condition_id, expiry=expiry)
                _validate_typed_identity(
                    clob_payload,
                    market=typed_ref,
                    source="polymarket_clob",
                    expected_condition_id=condition_id,
                    caller_input=False,
                )
                prices = prices or _prices(clob_payload)
                expiry.checkpoint()
                endpoint_observations.append(
                    _endpoint_success_observation(
                        source="polymarket_clob",
                        payload=clob_payload,
                        market_key=key,
                        condition_id=condition_id,
                        observed_at_utc=observed,
                        evidence=_clob_token_evidence(clob_payload, labels),
                    )
                )
            except _EXPECTED_SOURCE_ERRORS as exc:
                endpoint_observations.append(
                    _endpoint_error_observation(
                        source="polymarket_clob",
                        market_key=key,
                        condition_id=condition_id,
                        observed_at_utc=observed,
                        error=exc,
                    )
                )
                clob_payload = {}

        rpc_error: SourceObservation | None = None
        if condition_id and self.ctf_client is not None:
            try:
                await self._ensure_ctf_chain(expiry=expiry)
                denominator, numerators = await self._ctf_payout_vector(
                    condition_id,
                    len(labels),
                    expiry=expiry,
                )
                if denominator > 0:
                    record = _canonical_from_vector(
                        market_key=key,
                        input_identifier=input_identifier,
                        condition_id=condition_id,
                        labels=labels,
                        label_observation=label_observation,
                        auxiliary_observations=endpoint_observations,
                        denominator=denominator,
                        numerators=numerators,
                        observed_at_utc=observed,
                    )
                    expiry.checkpoint()
                    return record
                winner_hint = _platform_winner(
                    labels=labels,
                    clob_payload=clob_payload,
                    prices=prices,
                )
                metadata_observation = _metadata_observation(
                    snapshot_payload=snapshot_map,
                    gamma_payload=gamma_payload,
                    observed_at_utc=observed,
                )
                state = (
                    STATE_METADATA_ONLY
                    if metadata_observation or winner_hint
                    else STATE_OPEN
                )
                observations = [
                    *endpoint_observations,
                    SourceObservation(
                        source="polygon_ctf",
                        confidence=CONFIDENCE_METADATA_ONLY,
                        observed_at_utc=observed,
                        evidence={"denominator": denominator},
                    ),
                ]
                if metadata_observation:
                    observations.append(metadata_observation)
                if winner_hint:
                    observations.append(
                        SourceObservation(
                            source="polymarket_diagnostics",
                            confidence=CONFIDENCE_METADATA_ONLY,
                            observed_at_utc=observed,
                            evidence={"winner_hint": winner_hint, "prices": prices},
                        )
                    )
                record = ResolutionRecord(
                    platform="polymarket",
                    market_key=key,
                    input_identifier=input_identifier,
                    resolution_state=state,
                    result_type=RESULT_TYPE_UNKNOWN,
                    confidence=CONFIDENCE_METADATA_ONLY,
                    condition_id=condition_id,
                    source_observations=observations,
                    observed_at_utc=observed,
                )
                expiry.checkpoint()
                return record
            except (EvmRpcError, *_EXPECTED_SOURCE_ERRORS) as exc:
                rpc_error = SourceObservation(
                    source="polygon_ctf",
                    confidence=CONFIDENCE_UNAVAILABLE,
                    observed_at_utc=observed,
                    error_type=type(exc).__name__,
                    error_message=_sanitized_error_message(
                        exc, source="polygon_ctf"
                    ),
                )

        winner = _platform_winner(
            labels=labels, clob_payload=clob_payload, prices=prices
        )
        expiry.checkpoint()
        observations = list(endpoint_observations)
        if rpc_error is not None:
            observations.append(rpc_error)
        metadata_observation = _metadata_observation(
            snapshot_payload=snapshot_map,
            gamma_payload=gamma_payload,
            observed_at_utc=observed,
        )
        if winner:
            if metadata_observation:
                observations.append(metadata_observation)
            observations.append(
                SourceObservation(
                    source="polymarket_diagnostics",
                    confidence=CONFIDENCE_METADATA_ONLY,
                    observed_at_utc=observed,
                    evidence={"winner_hint": winner, "prices": prices},
                )
            )
            record = ResolutionRecord(
                platform="polymarket",
                market_key=key,
                input_identifier=input_identifier,
                resolution_state=STATE_METADATA_ONLY,
                result_type=RESULT_TYPE_UNKNOWN,
                confidence=CONFIDENCE_METADATA_ONLY,
                source_observations=observations,
                condition_id=condition_id,
                observed_at_utc=observed,
            )
            expiry.checkpoint()
            return record

        if _metadata_resolved(snapshot_map, gamma_payload):
            metadata_observation = _metadata_observation(
                snapshot_payload=snapshot_map,
                gamma_payload=gamma_payload,
                observed_at_utc=observed,
            )
            if metadata_observation:
                observations.append(metadata_observation)
            record = ResolutionRecord(
                platform="polymarket",
                market_key=key,
                input_identifier=input_identifier,
                resolution_state=STATE_METADATA_ONLY,
                result_type=RESULT_TYPE_UNKNOWN,
                confidence=CONFIDENCE_METADATA_ONLY,
                source_observations=observations,
                condition_id=condition_id,
                observed_at_utc=observed,
            )
            expiry.checkpoint()
            return record

        record = ResolutionRecord(
            platform="polymarket",
            market_key=key,
            input_identifier=input_identifier,
            resolution_state=STATE_UNAVAILABLE if not condition_id else STATE_OPEN,
            result_type=RESULT_TYPE_UNKNOWN,
            confidence=CONFIDENCE_UNAVAILABLE,
            source_observations=observations,
            condition_id=condition_id,
            observed_at_utc=observed,
        )
        expiry.checkpoint()
        return record


__all__ = [
    "PolymarketResolutionResolver",
]
