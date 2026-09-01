from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from fractions import Fraction
from typing import Any

from pmkt.resolution.evm import EvmRpcError, PolygonCtfClient
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
            except (TypeError, ValueError):
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
    error: Exception,
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
        error_message=str(error),
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
        gamma_client: Any | None = None,
        clob_client: Any | None = None,
        ctf_client: PolygonCtfClient | None = None,
    ) -> None:
        self.gamma_client = gamma_client
        self.clob_client = clob_client
        self.ctf_client = ctf_client
        self._ctf_chain_checked = False

    async def resolve(
        self,
        market_key: str,
        *,
        snapshot: Mapping[str, Any] | Any | None = None,
    ) -> ResolutionRecord:
        observed = utc_now_iso()
        snapshot_map = _mapping(snapshot)
        gamma_payload: dict[str, Any] = {}
        clob_payload: dict[str, Any] = {}
        endpoint_observations: list[SourceObservation] = []
        key = _market_key(snapshot_map, fallback=market_key)
        condition_id = _condition_id(snapshot_map)

        if self.gamma_client is not None and (
            not condition_id or not _labels(snapshot_map)
        ):
            try:
                gamma_payload = _mapping(await self.gamma_client.market(key))
                condition_id = _condition_id(snapshot_map, gamma_payload)
                endpoint_observations.append(
                    _endpoint_success_observation(
                        source="polymarket_gamma",
                        payload=gamma_payload,
                        market_key=key,
                        condition_id=condition_id,
                        observed_at_utc=observed,
                    )
                )
            except Exception as exc:
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
        label_observation = _label_mapping_observation(
            source=label_source,
            payload=label_payload,
            market_key=key,
            condition_id=condition_id or "",
            labels=labels,
            observed_at_utc=observed,
        )
        prices = _prices(snapshot_map, gamma_payload)

        if condition_id and self.clob_client is not None:
            try:
                clob_payload = _mapping(
                    await self.clob_client.clob_market_info(condition_id)
                )
                prices = prices or _prices(clob_payload)
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
            except Exception as exc:
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

        if condition_id and self.ctf_client is not None:
            try:
                if not self._ctf_chain_checked:
                    await self.ctf_client.ensure_polygon()
                    self._ctf_chain_checked = True
                denominator, numerators = await self.ctf_client.payout_vector(
                    condition_id,
                    len(labels),
                )
                if denominator > 0:
                    return _canonical_from_vector(
                        market_key=key,
                        input_identifier=market_key,
                        condition_id=condition_id,
                        labels=labels,
                        label_observation=label_observation,
                        auxiliary_observations=endpoint_observations,
                        denominator=denominator,
                        numerators=numerators,
                        observed_at_utc=observed,
                    )
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
                return ResolutionRecord(
                    platform="polymarket",
                    market_key=key,
                    input_identifier=market_key,
                    resolution_state=state,
                    result_type=RESULT_TYPE_UNKNOWN,
                    confidence=CONFIDENCE_METADATA_ONLY,
                    condition_id=condition_id,
                    source_observations=observations,
                    observed_at_utc=observed,
                )
            except EvmRpcError as exc:
                rpc_error = SourceObservation(
                    source="polygon_ctf",
                    confidence=CONFIDENCE_UNAVAILABLE,
                    observed_at_utc=observed,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            except Exception as exc:  # pragma: no cover - defensive RPC guard
                rpc_error = SourceObservation(
                    source="polygon_ctf",
                    confidence=CONFIDENCE_UNAVAILABLE,
                    observed_at_utc=observed,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
        else:
            rpc_error = None

        winner = _platform_winner(
            labels=labels, clob_payload=clob_payload, prices=prices
        )
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
            return ResolutionRecord(
                platform="polymarket",
                market_key=key,
                input_identifier=market_key,
                resolution_state=STATE_METADATA_ONLY,
                result_type=RESULT_TYPE_UNKNOWN,
                confidence=CONFIDENCE_METADATA_ONLY,
                source_observations=observations,
                condition_id=condition_id,
                observed_at_utc=observed,
            )

        if _metadata_resolved(snapshot_map, gamma_payload):
            metadata_observation = _metadata_observation(
                snapshot_payload=snapshot_map,
                gamma_payload=gamma_payload,
                observed_at_utc=observed,
            )
            if metadata_observation:
                observations.append(metadata_observation)
            return ResolutionRecord(
                platform="polymarket",
                market_key=key,
                input_identifier=market_key,
                resolution_state=STATE_METADATA_ONLY,
                result_type=RESULT_TYPE_UNKNOWN,
                confidence=CONFIDENCE_METADATA_ONLY,
                source_observations=observations,
                condition_id=condition_id,
                observed_at_utc=observed,
            )

        return ResolutionRecord(
            platform="polymarket",
            market_key=key,
            input_identifier=market_key,
            resolution_state=STATE_UNAVAILABLE if not condition_id else STATE_OPEN,
            result_type=RESULT_TYPE_UNKNOWN,
            confidence=CONFIDENCE_UNAVAILABLE,
            source_observations=observations,
            condition_id=condition_id,
            observed_at_utc=observed,
        )


__all__ = [
    "PolymarketResolutionResolver",
]
