from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from fractions import Fraction
from typing import Any

from pmkt.resolution.models import (
    CONFIDENCE_CANONICAL,
    CONFIDENCE_PLATFORM_CONFIRMED,
    RESOLVER_VERSION,
    RESULT_TYPE_SCALAR,
    STATE_FINAL,
    STATE_INCONSISTENT,
)

KNOWN_TERMINAL_LABELS = frozenset({"yes", "no", "refund"})
EXACT_PAYOUT_LABEL_PREFIX = "payout:"
EXACT_SCALAR_LABEL_PREFIX = "scalar:"


@dataclass(frozen=True)
class ResolutionLabelPolicy:
    policy_id: str = "canonical_final_binary.v1"
    allowed_sources: Mapping[str, tuple[str, ...]] | None = None
    allowed_resolver_versions: tuple[str, ...] = (RESOLVER_VERSION,)
    require_canonical: bool = True
    allow_platform_confirmed: bool = False
    allow_market_fallback_for_scoring: bool = True
    allow_market_fallback_for_fit: bool = False
    allow_proxy_labels: bool = False

    def sources_for_platform(self, platform: str | None) -> tuple[str, ...]:
        sources = self.allowed_sources or DEFAULT_ALLOWED_SOURCES
        return tuple(sources.get(str(platform or "").strip().lower(), ()))


@dataclass(frozen=True)
class ResolutionTrainingLabel:
    terminal_label: str | None
    binary_yes: int | None
    quality: str
    exclusion_reason: str | None
    canonical_source: str | None
    resolver_version: str | None
    result_type: str | None
    resolution_state: str | None
    confidence: str | None


DEFAULT_ALLOWED_SOURCES: Mapping[str, tuple[str, ...]] = {
    "polymarket": ("polygon_ctf",),
    "kalshi": ("kalshi_rest", "kalshi_historical_rest"),
}
DEFAULT_LABEL_POLICY = ResolutionLabelPolicy()


def is_authoritative_final_resolution(
    record: Mapping[str, Any],
    *,
    policy: ResolutionLabelPolicy | None = None,
) -> bool:
    return _eligibility_error(record, policy or DEFAULT_LABEL_POLICY) is None


def terminal_label_from_resolution(
    record: Mapping[str, Any],
    *,
    require_canonical: bool = True,
) -> str | None:
    if not _finality_allowed(record, require_canonical=require_canonical):
        return None
    payout_terminal = terminal_from_payouts(record.get("payouts_json"))
    if payout_terminal is not None:
        return payout_terminal
    scalar_terminal = terminal_from_scalar_record(record)
    if scalar_terminal is not None:
        return scalar_terminal
    settlement_terminal = terminal_from_kalshi_settlement(record)
    if settlement_terminal is not None:
        return settlement_terminal
    for key in ("winner", "result"):
        value = record.get(key)
        if _is_missing(value):
            continue
        normalized = str(value).strip().lower()
        if normalized in KNOWN_TERMINAL_LABELS:
            return normalized
    return None


def binary_yes_outcome_from_resolution(
    record: Mapping[str, Any],
    *,
    require_canonical: bool = True,
) -> int | None:
    terminal = terminal_label_from_resolution(record, require_canonical=require_canonical)
    if terminal == "yes":
        return 1
    if terminal == "no":
        return 0
    return None


def resolution_training_label(
    record: Mapping[str, Any],
    *,
    policy: ResolutionLabelPolicy | None = None,
) -> ResolutionTrainingLabel:
    active_policy = policy or DEFAULT_LABEL_POLICY
    terminal = _authoritative_terminal_label_from_resolution(record, active_policy)
    binary_yes = 1 if terminal == "yes" else 0 if terminal == "no" else None
    exclusion = _eligibility_error(record, active_policy)
    state = _clean_text(record.get("resolution_state"))
    confidence = _clean_text(record.get("confidence"))
    canonical_source = _clean_text(record.get("canonical_source"))
    resolver_version = _clean_text(record.get("resolver_version"))
    result_type = _clean_text(record.get("result_type"))
    if exclusion is not None:
        return ResolutionTrainingLabel(
            terminal_label=terminal,
            binary_yes=binary_yes,
            quality=_quality_for_exclusion(exclusion, state=state),
            exclusion_reason=exclusion,
            canonical_source=canonical_source,
            resolver_version=resolver_version,
            result_type=result_type,
            resolution_state=state,
            confidence=confidence,
        )
    if terminal is None:
        return ResolutionTrainingLabel(
            terminal_label=None,
            binary_yes=None,
            quality="unavailable",
            exclusion_reason="terminal_label_unavailable",
            canonical_source=canonical_source,
            resolver_version=resolver_version,
            result_type=result_type,
            resolution_state=state,
            confidence=confidence,
        )
    if binary_yes is None:
        return ResolutionTrainingLabel(
            terminal_label=terminal,
            binary_yes=None,
            quality="canonical_nonbinary",
            exclusion_reason="nonbinary_terminal_label",
            canonical_source=canonical_source,
            resolver_version=resolver_version,
            result_type=result_type,
            resolution_state=state,
            confidence=confidence,
        )
    return ResolutionTrainingLabel(
        terminal_label=terminal,
        binary_yes=binary_yes,
        quality="canonical_binary",
        exclusion_reason=None,
        canonical_source=canonical_source,
        resolver_version=resolver_version,
        result_type=result_type,
        resolution_state=state,
        confidence=confidence,
    )


def terminal_from_payouts(value: Any) -> str | None:
    payouts = json_array(value)
    if not payouts:
        return None
    by_outcome: dict[str, Fraction] = {}
    indexed: list[Fraction] = []
    for payout in payouts:
        if not isinstance(payout, Mapping):
            continue
        ratio = payout_fraction(payout)
        if ratio is None:
            continue
        outcome_value = payout.get("outcome")
        outcome = "" if is_missing_or_blank_scalar(outcome_value) else str(outcome_value).strip().lower()
        if outcome:
            by_outcome[outcome] = ratio
        indexed.append(ratio)

    yes = by_outcome.get("yes")
    no = by_outcome.get("no")
    if yes is None or no is None or set(by_outcome) != {"yes", "no"} or len(indexed) != 2:
        return None
    if not _valid_binary_payout_pair(yes=yes, no=no):
        return None
    if yes == Fraction(1) and no == Fraction(0):
        return "yes"
    if yes == Fraction(0) and no == Fraction(1):
        return "no"
    if yes == Fraction(1) and no == Fraction(1):
        return "refund"
    if yes or no:
        return exact_payout_label(yes=yes, no=no)
    return None


def terminal_from_scalar_record(record: Mapping[str, Any]) -> str | None:
    if str(record.get("result_type") or "").strip().lower() != RESULT_TYPE_SCALAR:
        return None
    for key in ("settlement_value_dollars", "result"):
        terminal = scalar_terminal_from_value(record.get(key))
        if terminal is not None:
            return terminal
    payouts = json_array(record.get("payouts_json"))
    if len(payouts) != 1 or not isinstance(payouts[0], Mapping):
        return None
    outcome_value = payouts[0].get("outcome")
    outcome = "" if is_missing_or_blank_scalar(outcome_value) else str(outcome_value).strip().lower()
    if outcome not in {"", "scalar"}:
        return None
    ratio = payout_fraction(payouts[0])
    if ratio is None:
        return None
    return exact_scalar_label(ratio)


def terminal_from_kalshi_settlement(record: Mapping[str, Any]) -> str | None:
    if _clean_text(record.get("platform")) != "kalshi":
        return None
    value = record.get("settlement_value_dollars")
    if is_missing_or_blank_scalar(value):
        return None
    try:
        fraction = Fraction(str(value).strip())
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    if fraction < 0 or fraction > 1:
        return None
    if fraction == 1:
        return "yes"
    if fraction == 0:
        return "no"
    return exact_scalar_label(fraction)


def scalar_terminal_from_value(value: Any) -> str | None:
    if is_missing_or_blank_scalar(value):
        return None
    try:
        fraction = Fraction(str(value).strip())
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    if fraction < 0 or fraction > 1:
        return None
    return exact_scalar_label(fraction)


def exact_payout_label(*, yes: Fraction, no: Fraction) -> str:
    return f"{EXACT_PAYOUT_LABEL_PREFIX}yes={format_fraction(yes)},no={format_fraction(no)}"


def exact_scalar_label(value: Fraction) -> str:
    return f"{EXACT_SCALAR_LABEL_PREFIX}{format_fraction(value)}"


def format_fraction(value: Fraction) -> str:
    if value.denominator == 1:
        return str(value.numerator)
    return f"{value.numerator}/{value.denominator}"


def is_known_terminal_label(label: Any) -> bool:
    if _is_missing(label):
        return False
    text = str(label).strip()
    return (
        text in KNOWN_TERMINAL_LABELS
        or is_exact_payout_label(text)
        or is_exact_scalar_label(text)
    )


def is_exact_payout_label(text: str) -> bool:
    if not text.startswith(EXACT_PAYOUT_LABEL_PREFIX):
        return False
    pieces = text.removeprefix(EXACT_PAYOUT_LABEL_PREFIX).split(",")
    if len(pieces) != 2:
        return False
    values: dict[str, Fraction] = {}
    for piece in pieces:
        key, separator, raw_value = piece.partition("=")
        if separator != "=" or key not in {"yes", "no"} or key in values:
            return False
        try:
            fraction = Fraction(raw_value)
        except (TypeError, ValueError, ZeroDivisionError):
            return False
        if fraction < 0 or fraction > 1:
            return False
        values[key] = fraction
    if set(values) != {"yes", "no"}:
        return False
    if not _valid_binary_payout_pair(yes=values["yes"], no=values["no"]):
        return False
    return text == exact_payout_label(
        yes=values["yes"],
        no=values["no"],
    )


def is_exact_scalar_label(text: str) -> bool:
    if not text.startswith(EXACT_SCALAR_LABEL_PREFIX):
        return False
    raw_value = text.removeprefix(EXACT_SCALAR_LABEL_PREFIX)
    try:
        fraction = Fraction(raw_value)
    except (TypeError, ValueError, ZeroDivisionError):
        return False
    if fraction < 0 or fraction > 1:
        return False
    return text == exact_scalar_label(fraction)


def payout_fraction(payout: Mapping[str, Any]) -> Fraction | None:
    value = payout.get("payout")
    if not is_missing_or_blank_scalar(value):
        try:
            fraction = Fraction(str(value).strip())
            return fraction if 0 <= fraction <= 1 else None
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    numerator = payout.get("numerator")
    denominator = payout.get("denominator")
    if is_missing_or_blank_scalar(numerator) or is_missing_or_blank_scalar(denominator):
        return None
    try:
        denominator_fraction = Fraction(str(denominator).strip())
        if denominator_fraction == 0:
            return None
        fraction = Fraction(str(numerator).strip()) / denominator_fraction
        return fraction if 0 <= fraction <= 1 else None
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def is_missing_or_blank_scalar(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return _is_missing(value)


def json_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    if not isinstance(value, (bytes, dict)) and hasattr(value, "tolist"):
        try:
            parsed = value.tolist()
        except (AttributeError, TypeError, ValueError):
            parsed = None
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, tuple):
            return list(parsed)
    if _is_missing(value):
        return []
    return []


def _finality_allowed(
    record: Mapping[str, Any],
    *,
    require_canonical: bool,
    allow_platform_confirmed: bool = False,
) -> bool:
    state = _clean_text(record.get("resolution_state"))
    confidence = _clean_text(record.get("confidence"))
    if state != STATE_FINAL:
        return False
    if confidence == CONFIDENCE_CANONICAL:
        return True
    if confidence == CONFIDENCE_PLATFORM_CONFIRMED:
        return allow_platform_confirmed or not require_canonical
    return False


def _eligibility_error(record: Mapping[str, Any], policy: ResolutionLabelPolicy) -> str | None:
    if _clean_text(record.get("resolution_state")) == STATE_INCONSISTENT:
        return "inconsistent_resolution"
    if not _finality_allowed(
        record,
        require_canonical=policy.require_canonical,
        allow_platform_confirmed=policy.allow_platform_confirmed,
    ):
        return "noncanonical_or_nonfinal_resolution"
    if _clean_text(record.get("error_type")):
        return "resolver_error"
    resolver_version = _clean_text(record.get("resolver_version"))
    if resolver_version not in set(policy.allowed_resolver_versions):
        return "resolver_version_mismatch"
    platform = _clean_text(record.get("platform"))
    if platform is None:
        return "platform_missing"
    allowed_sources = policy.sources_for_platform(platform)
    if not allowed_sources:
        return "platform_not_allowed"
    canonical_source = _clean_text(record.get("canonical_source"))
    if canonical_source not in set(allowed_sources):
        return "canonical_source_not_allowed"
    if _authoritative_terminal_label_from_resolution(record, policy) is None:
        return "terminal_label_unavailable"
    return None


def _authoritative_terminal_label_from_resolution(
    record: Mapping[str, Any],
    policy: ResolutionLabelPolicy,
) -> str | None:
    if not _finality_allowed(
        record,
        require_canonical=policy.require_canonical,
        allow_platform_confirmed=policy.allow_platform_confirmed,
    ):
        return None
    platform = _clean_text(record.get("platform"))
    if platform == "polymarket":
        return terminal_from_payouts(record.get("payouts_json"))
    if platform == "kalshi":
        return terminal_from_kalshi_settlement(record)
    return None


def _valid_binary_payout_pair(*, yes: Fraction, no: Fraction) -> bool:
    if yes < 0 or no < 0 or yes > 1 or no > 1:
        return False
    if yes == Fraction(1) and no == Fraction(1):
        return True
    return yes + no == 1


def _quality_for_exclusion(exclusion: str, *, state: str | None) -> str:
    if exclusion == "inconsistent_resolution" or state == STATE_INCONSISTENT:
        return "conflict"
    if exclusion == "resolver_error":
        return "error"
    if exclusion in {
        "noncanonical_or_nonfinal_resolution",
        "resolver_version_mismatch",
        "platform_missing",
        "platform_not_allowed",
        "canonical_source_not_allowed",
    }:
        return "noncanonical"
    return "unavailable"


def _clean_text(value: Any) -> str | None:
    if _is_missing(value):
        return None
    text = str(value).strip()
    return text or None


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    value_type = type(value)
    if value_type.__module__.startswith("pandas") and value_type.__name__ in {"NAType", "NaTType"}:
        return True
    try:
        missing = value != value
    except (TypeError, ValueError):
        return False
    try:
        return bool(missing)
    except (TypeError, ValueError):
        return False


__all__ = [
    "DEFAULT_ALLOWED_SOURCES",
    "DEFAULT_LABEL_POLICY",
    "EXACT_PAYOUT_LABEL_PREFIX",
    "EXACT_SCALAR_LABEL_PREFIX",
    "KNOWN_TERMINAL_LABELS",
    "ResolutionLabelPolicy",
    "ResolutionTrainingLabel",
    "binary_yes_outcome_from_resolution",
    "exact_payout_label",
    "exact_scalar_label",
    "format_fraction",
    "is_authoritative_final_resolution",
    "is_exact_payout_label",
    "is_exact_scalar_label",
    "is_known_terminal_label",
    "is_missing_or_blank_scalar",
    "json_array",
    "payout_fraction",
    "resolution_training_label",
    "scalar_terminal_from_value",
    "terminal_from_payouts",
    "terminal_from_kalshi_settlement",
    "terminal_from_scalar_record",
    "terminal_label_from_resolution",
]
