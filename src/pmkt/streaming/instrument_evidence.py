from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Iterable, Mapping, Sequence

from pmkt.data.registry import CAPTURE_INSTRUMENT_EVIDENCE_SCHEMA_VERSION
from pmkt.data.time import parse_utc_timestamp

CAPTURE_INSTRUMENT_EVIDENCE_POLICY_VERSION = "capture-instrument-evidence-policy.v1"
CAPTURE_INSTRUMENT_EVIDENCE_ROLE = "instrument_evidence"

DEFAULT_INITIAL_SNAPSHOT_SLA_SECONDS = 30.0
DEFAULT_ELIGIBILITY_MAX_AGE_SECONDS = 300.0
DEFAULT_TERMINAL_EVIDENCE_BATCH_ROWS = 4096


class EvidencePolicyStatus(str, Enum):
    PROVISIONAL = "provisional"
    CALIBRATED = "calibrated"


class EligibilityStatus(str, Enum):
    ELIGIBLE = "eligible"
    INELIGIBLE = "ineligible"
    UNKNOWN = "unknown"


class EligibilityReason(str, Enum):
    SOURCE_ACTIVE = "source_active"
    SOURCE_INACTIVE = "source_inactive"
    MISSING_EVIDENCE = "missing_evidence"
    STALE_EVIDENCE = "stale_evidence"
    MALFORMED_EVIDENCE = "malformed_evidence"
    UNAUTHORITATIVE_EVIDENCE = "unauthoritative_evidence"


class InitializationVerdict(str, Enum):
    ON_TIME = "on_time"
    LATE = "late"
    MISSING = "missing"
    NOT_REQUIRED = "not_required"


class InstrumentTerminalOutcome(str, Enum):
    OBSERVED_VALID = "observed_valid"
    LATE = "late"
    MISSING = "missing"
    INELIGIBLE = "ineligible"
    NOT_ESTABLISHED = "not_established"


@dataclass(frozen=True)
class CaptureEligibilityEvidence:
    status: EligibilityStatus
    reason: EligibilityReason
    source_identity: str | None = None
    source_reference: str | None = None
    source_sha256: str | None = None
    observed_at_utc: str | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> CaptureEligibilityEvidence:
        return cls(
            status=EligibilityStatus(str(value.get("status") or "")),
            reason=EligibilityReason(str(value.get("reason") or "")),
            source_identity=_optional_text(value.get("source_identity")),
            source_reference=_optional_text(value.get("source_reference")),
            source_sha256=_optional_text(value.get("source_sha256")),
            observed_at_utc=_optional_text(value.get("observed_at_utc")),
        )


@dataclass(frozen=True)
class CaptureInstrumentEvidencePolicy:
    policy_status: EvidencePolicyStatus = EvidencePolicyStatus.PROVISIONAL
    initialization_sla_seconds: float = DEFAULT_INITIAL_SNAPSHOT_SLA_SECONDS
    eligibility_max_age_seconds: float = DEFAULT_ELIGIBILITY_MAX_AGE_SECONDS
    policy_version: str = CAPTURE_INSTRUMENT_EVIDENCE_POLICY_VERSION

    def __post_init__(self) -> None:
        if self.initialization_sla_seconds <= 0:
            raise ValueError("initialization_sla_seconds must be positive")
        if self.eligibility_max_age_seconds <= 0:
            raise ValueError("eligibility_max_age_seconds must be positive")
        if not self.policy_version.strip():
            raise ValueError("policy_version must not be empty")

    def as_manifest_mapping(self, *, venue: str) -> dict[str, Any]:
        return {
            "policy_version": self.policy_version,
            "policy_status": self.policy_status.value,
            "initialization_sla_seconds": self.initialization_sla_seconds,
            "eligibility_max_age_seconds": self.eligibility_max_age_seconds,
            "subscription_establishment_rule": (
                "successful_socket_send_plus_no_rejection"
                if venue == "polymarket"
                else "successful_subscription_send_plus_no_rejection"
            ),
            "processing_drain_invariant": "journaled_terminal_evidence.v1",
        }


@dataclass(frozen=True)
class CaptureInstrumentEvidenceSummary:
    row_count: int
    requested_instrument_count: int
    eligible_instrument_count: int
    excluded_instrument_count: int
    unknown_instrument_count: int
    coverage_denominator_count: int
    initial_snapshot_count: int
    late_snapshot_count: int
    unexplained_missing_instrument_count: int
    subscription_attempt_count: int
    established_subscription_attempt_count: int
    eligible_established_instrument_count: int
    eligible_initial_snapshot_count: int

    @property
    def all_requested_ineligible(self) -> bool:
        return (
            self.requested_instrument_count > 0
            and self.excluded_instrument_count == self.requested_instrument_count
        )

    @property
    def has_usable_snapshot_evidence(self) -> bool:
        return self.initial_snapshot_count > 0

    def as_manifest_mapping(self) -> dict[str, int]:
        return {
            "evidence_row_count": self.row_count,
            "requested_instrument_count": self.requested_instrument_count,
            "eligible_instrument_count": self.eligible_instrument_count,
            "excluded_instrument_count": self.excluded_instrument_count,
            "unknown_instrument_count": self.unknown_instrument_count,
            "coverage_denominator_count": self.coverage_denominator_count,
            "initial_snapshot_count": self.initial_snapshot_count,
            "late_snapshot_count": self.late_snapshot_count,
            "unexplained_missing_instrument_count": (
                self.unexplained_missing_instrument_count
            ),
            "subscription_attempt_count": self.subscription_attempt_count,
            "established_subscription_attempt_count": (
                self.established_subscription_attempt_count
            ),
            "eligible_established_instrument_count": (
                self.eligible_established_instrument_count
            ),
            "eligible_initial_snapshot_count": (self.eligible_initial_snapshot_count),
        }


@dataclass
class _SubscriptionAttemptState:
    subscription_attempt_id: int
    eligibility_checked_at_utc: str
    subscription_sent_at_utc: str | None = None
    subscription_established_at_utc: str | None = None
    first_valid_snapshot_at_utc: list[datetime | None] = field(default_factory=list)


class CaptureInstrumentEvidenceTracker:
    """Collect exact subscription-attempt evidence for one capture shard."""

    def __init__(
        self,
        *,
        collector_run_id: str,
        venue: str,
        shard_id: str,
        instrument_ids: Sequence[str],
        eligibility_evidence: Mapping[
            str, CaptureEligibilityEvidence | Mapping[str, Any]
        ]
        | None = None,
        policy: CaptureInstrumentEvidencePolicy | None = None,
        now_utc: Callable[[], datetime] | None = None,
    ) -> None:
        instruments = tuple(
            dict.fromkeys(str(value).strip() for value in instrument_ids)
        )
        if not collector_run_id.strip():
            raise ValueError("collector_run_id must not be empty")
        if venue not in {"polymarket", "kalshi"}:
            raise ValueError("venue must be polymarket or kalshi")
        if not shard_id.strip():
            raise ValueError("shard_id must not be empty")
        if not instruments or any(not value for value in instruments):
            raise ValueError("instrument_ids must contain non-empty unique values")
        self.collector_run_id = collector_run_id
        self.venue = venue
        self.shard_id = shard_id
        self.instrument_ids = instruments
        self.policy = policy or CaptureInstrumentEvidencePolicy()
        # Freeze caller-owned mappings without eagerly changing validation
        # timing.  Historical attempt rows are derived later from each
        # attempt's fixed checked-at timestamp, so mutable input must not be
        # able to rewrite earlier evidence.
        self._eligibility_evidence = {
            instrument_id: (
                value if isinstance(value, CaptureEligibilityEvidence) else dict(value)
            )
            for instrument_id, value in (eligibility_evidence or {}).items()
        }
        self._now_utc = now_utc or (lambda: datetime.now(tz=timezone.utc))
        self._instrument_index = {
            instrument_id: index for index, instrument_id in enumerate(instruments)
        }
        # Attempt-level timestamps are shared by every instrument.  Keeping one
        # object per (attempt, instrument) duplicated the same collector,
        # eligibility, and subscription fields thousands of times on every
        # reconnect.  A compact timestamp vector preserves the exact terminal
        # evidence grain while keeping live state close to the information
        # content of the capture.
        self._attempts: list[_SubscriptionAttemptState] = []

    @property
    def attempt_count(self) -> int:
        return len(self._attempts)

    def begin_subscription_attempt(self) -> int:
        checked_at = _as_utc(self._now_utc())
        attempt_id = len(self._attempts) + 1
        self._attempts.append(
            _SubscriptionAttemptState(
                subscription_attempt_id=attempt_id,
                eligibility_checked_at_utc=checked_at.isoformat(),
                first_valid_snapshot_at_utc=[None] * len(self.instrument_ids),
            )
        )
        return attempt_id

    def mark_subscription_established(
        self,
        *,
        subscription_sent_at_utc: str | None = None,
        established_at_utc: str | None = None,
    ) -> None:
        attempt = self._current_attempt()
        established = (
            parse_utc_timestamp(established_at_utc)
            if established_at_utc is not None
            else _as_utc(self._now_utc())
        )
        if established is None:
            raise ValueError("established_at_utc must be a valid UTC timestamp")
        sent = (
            parse_utc_timestamp(subscription_sent_at_utc)
            if subscription_sent_at_utc is not None
            else established
        )
        if sent is None:
            raise ValueError("subscription_sent_at_utc must be a valid UTC timestamp")
        if established < sent:
            raise ValueError("subscription establishment cannot precede send")
        attempt.subscription_sent_at_utc = sent.isoformat()
        attempt.subscription_established_at_utc = established.isoformat()

    def record_valid_snapshot(
        self, instrument_id: str, *, observed_at_utc: str
    ) -> None:
        attempt = self._current_attempt()
        try:
            index = self._instrument_index[instrument_id]
        except KeyError:
            return
        observed = parse_utc_timestamp(observed_at_utc)
        if observed is None:
            raise ValueError("observed_at_utc must be a valid UTC timestamp")
        if attempt.first_valid_snapshot_at_utc[index] is None:
            attempt.first_valid_snapshot_at_utc[index] = observed

    def iter_terminal_rows(self, terminal_reason: Any) -> Iterable[dict[str, Any]]:
        """Yield exact attempt/instrument evidence without materializing it."""

        reason = _terminal_reason(terminal_reason)
        for attempt in self._attempts:
            yield from self._iter_attempt_rows(attempt, terminal_reason=reason)

    def terminal_rows(self, terminal_reason: Any) -> tuple[dict[str, Any], ...]:
        """Materialize terminal rows for compatibility with bounded callers."""

        return tuple(self.iter_terminal_rows(terminal_reason))

    def summary(self, terminal_reason: Any) -> CaptureInstrumentEvidenceSummary:
        reason = _terminal_reason(terminal_reason)
        if not self._attempts:
            return CaptureInstrumentEvidenceSummary(
                row_count=0,
                requested_instrument_count=0,
                eligible_instrument_count=0,
                excluded_instrument_count=0,
                unknown_instrument_count=0,
                coverage_denominator_count=0,
                initial_snapshot_count=0,
                late_snapshot_count=0,
                unexplained_missing_instrument_count=0,
                subscription_attempt_count=0,
                established_subscription_attempt_count=0,
                eligible_established_instrument_count=0,
                eligible_initial_snapshot_count=0,
            )

        latest_rows = self._iter_attempt_rows(
            self._attempts[-1], terminal_reason=reason
        )
        eligible = 0
        excluded = 0
        unknown = 0
        snapshots = 0
        late = 0
        missing = 0
        eligible_established = 0
        eligible_snapshots = 0
        for row in latest_rows:
            status = row["eligibility_status"]
            has_snapshot = bool(row["first_valid_snapshot_at_utc"])
            if status == EligibilityStatus.ELIGIBLE.value:
                eligible += 1
                eligible_established += bool(row["subscription_established_at_utc"])
                eligible_snapshots += has_snapshot
            elif status == EligibilityStatus.INELIGIBLE.value:
                excluded += 1
            else:
                unknown += 1
            snapshots += has_snapshot
            late += row["initialization_verdict"] == InitializationVerdict.LATE.value
            missing += status != EligibilityStatus.INELIGIBLE.value and row[
                "terminal_outcome"
            ] in {
                InstrumentTerminalOutcome.MISSING.value,
                InstrumentTerminalOutcome.NOT_ESTABLISHED.value,
            }

        attempt_count = len(self._attempts)
        requested_count = len(self.instrument_ids)
        return CaptureInstrumentEvidenceSummary(
            row_count=attempt_count * requested_count,
            requested_instrument_count=requested_count,
            eligible_instrument_count=eligible,
            excluded_instrument_count=excluded,
            unknown_instrument_count=unknown,
            coverage_denominator_count=eligible + unknown,
            initial_snapshot_count=snapshots,
            late_snapshot_count=late,
            unexplained_missing_instrument_count=missing,
            subscription_attempt_count=attempt_count,
            established_subscription_attempt_count=sum(
                attempt.subscription_established_at_utc is not None
                for attempt in self._attempts
            ),
            eligible_established_instrument_count=eligible_established,
            eligible_initial_snapshot_count=eligible_snapshots,
        )

    def _current_attempt(self) -> _SubscriptionAttemptState:
        if not self._attempts:
            self.begin_subscription_attempt()
        return self._attempts[-1]

    def _iter_attempt_rows(
        self,
        attempt: _SubscriptionAttemptState,
        *,
        terminal_reason: str,
    ) -> Iterable[dict[str, Any]]:
        checked_at = parse_utc_timestamp(attempt.eligibility_checked_at_utc)
        assert checked_at is not None
        for index, instrument_id in enumerate(self.instrument_ids):
            evidence = self._normalized_evidence(
                instrument_id,
                checked_at=checked_at,
            )
            yield self._terminal_row(
                attempt,
                instrument_id=instrument_id,
                evidence=evidence,
                first_valid_snapshot_at_utc=(
                    attempt.first_valid_snapshot_at_utc[index]
                ),
                terminal_reason=terminal_reason,
            )

    def _normalized_evidence(
        self, instrument_id: str, *, checked_at: datetime
    ) -> CaptureEligibilityEvidence:
        raw = self._eligibility_evidence.get(instrument_id)
        if raw is None:
            return CaptureEligibilityEvidence(
                EligibilityStatus.UNKNOWN,
                EligibilityReason.MISSING_EVIDENCE,
            )
        try:
            evidence = (
                raw
                if isinstance(raw, CaptureEligibilityEvidence)
                else CaptureEligibilityEvidence.from_mapping(raw)
            )
        except (TypeError, ValueError):
            return CaptureEligibilityEvidence(
                EligibilityStatus.UNKNOWN,
                EligibilityReason.MALFORMED_EVIDENCE,
            )
        if evidence.status is EligibilityStatus.UNKNOWN:
            return CaptureEligibilityEvidence(
                EligibilityStatus.UNKNOWN,
                evidence.reason
                if evidence.reason
                in {
                    EligibilityReason.MISSING_EVIDENCE,
                    EligibilityReason.STALE_EVIDENCE,
                    EligibilityReason.MALFORMED_EVIDENCE,
                    EligibilityReason.UNAUTHORITATIVE_EVIDENCE,
                }
                else EligibilityReason.UNAUTHORITATIVE_EVIDENCE,
                evidence.source_identity,
                evidence.source_reference,
                evidence.source_sha256,
                evidence.observed_at_utc,
            )
        expected_reason = (
            EligibilityReason.SOURCE_ACTIVE
            if evidence.status is EligibilityStatus.ELIGIBLE
            else EligibilityReason.SOURCE_INACTIVE
        )
        observed_at = parse_utc_timestamp(evidence.observed_at_utc)
        source_authoritative = (
            bool(evidence.source_identity)
            and bool(evidence.source_reference)
            and _is_sha256(evidence.source_sha256)
            and observed_at is not None
        )
        if evidence.reason is not expected_reason or not source_authoritative:
            return CaptureEligibilityEvidence(
                EligibilityStatus.UNKNOWN,
                EligibilityReason.UNAUTHORITATIVE_EVIDENCE,
                evidence.source_identity,
                evidence.source_reference,
                evidence.source_sha256,
                evidence.observed_at_utc,
            )
        assert observed_at is not None
        if (
            checked_at - observed_at
        ).total_seconds() > self.policy.eligibility_max_age_seconds:
            return CaptureEligibilityEvidence(
                EligibilityStatus.UNKNOWN,
                EligibilityReason.STALE_EVIDENCE,
                evidence.source_identity,
                evidence.source_reference,
                evidence.source_sha256,
                evidence.observed_at_utc,
            )
        return evidence

    def _terminal_row(
        self,
        attempt: _SubscriptionAttemptState,
        *,
        instrument_id: str,
        evidence: CaptureEligibilityEvidence,
        first_valid_snapshot_at_utc: datetime | None,
        terminal_reason: str,
    ) -> dict[str, Any]:
        latency_ms: float | None = None
        if (
            attempt.subscription_established_at_utc is not None
            and first_valid_snapshot_at_utc is not None
        ):
            established = parse_utc_timestamp(attempt.subscription_established_at_utc)
            assert established is not None
            latency_ms = max(
                0.0,
                (first_valid_snapshot_at_utc - established).total_seconds() * 1000,
            )

        if evidence.status is EligibilityStatus.INELIGIBLE:
            verdict = InitializationVerdict.NOT_REQUIRED
            outcome = InstrumentTerminalOutcome.INELIGIBLE
        elif attempt.subscription_established_at_utc is None:
            verdict = InitializationVerdict.MISSING
            outcome = InstrumentTerminalOutcome.NOT_ESTABLISHED
        elif first_valid_snapshot_at_utc is None:
            verdict = InitializationVerdict.MISSING
            outcome = InstrumentTerminalOutcome.MISSING
        elif (
            latency_ms is not None
            and latency_ms > self.policy.initialization_sla_seconds * 1000
        ):
            verdict = InitializationVerdict.LATE
            outcome = InstrumentTerminalOutcome.LATE
        else:
            verdict = InitializationVerdict.ON_TIME
            outcome = InstrumentTerminalOutcome.OBSERVED_VALID

        checked_at = parse_utc_timestamp(attempt.eligibility_checked_at_utc)
        observed_at = parse_utc_timestamp(evidence.observed_at_utc)
        assert checked_at is not None
        age_seconds = (
            max(0.0, (checked_at - observed_at).total_seconds())
            if observed_at is not None
            else None
        )

        return {
            "schema_version": CAPTURE_INSTRUMENT_EVIDENCE_SCHEMA_VERSION,
            "collector_run_id": self.collector_run_id,
            "venue": self.venue,
            "shard_id": self.shard_id,
            "subscription_attempt_id": attempt.subscription_attempt_id,
            "instrument_id": instrument_id,
            "requested": True,
            "eligibility_status": evidence.status.value,
            "eligibility_reason": evidence.reason.value,
            "eligibility_source_identity": evidence.source_identity,
            "eligibility_source_reference": evidence.source_reference,
            "eligibility_source_sha256": evidence.source_sha256,
            "eligibility_observed_at_utc": evidence.observed_at_utc,
            "eligibility_checked_at_utc": attempt.eligibility_checked_at_utc,
            "eligibility_evidence_age_seconds": age_seconds,
            "subscription_sent_at_utc": attempt.subscription_sent_at_utc,
            "subscription_established_at_utc": (
                attempt.subscription_established_at_utc
            ),
            "first_valid_snapshot_at_utc": (
                first_valid_snapshot_at_utc.isoformat()
                if first_valid_snapshot_at_utc is not None
                else None
            ),
            "initial_snapshot_latency_ms": latency_ms,
            "initialization_verdict": verdict.value,
            "terminal_outcome": outcome.value,
            "terminal_reason": terminal_reason,
        }


def eligibility_evidence_from_subscription_metadata(
    metadata: Mapping[str, Any] | None,
    *,
    venue: str,
) -> dict[str, Mapping[str, Any]]:
    if metadata is None:
        return {}
    raw = metadata.get("instrument_eligibility")
    if isinstance(raw, Mapping) and venue in raw:
        raw = raw.get(venue)
    if isinstance(raw, Mapping):
        return {
            str(instrument_id): dict(value)
            for instrument_id, value in raw.items()
            if str(instrument_id).strip() and isinstance(value, Mapping)
        }
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        result: dict[str, Mapping[str, Any]] = {}
        for item in raw:
            if not isinstance(item, Mapping):
                continue
            if str(item.get("venue") or venue) != venue:
                continue
            instrument_id = str(
                item.get("instrument_id")
                or item.get("asset_id")
                or item.get("market_ticker")
                or ""
            ).strip()
            if instrument_id:
                result[instrument_id] = dict(item)
        return result
    return {}


def summarize_capture_instrument_evidence(
    rows: Iterable[Mapping[str, Any]],
) -> CaptureInstrumentEvidenceSummary:
    materialized = [dict(row) for row in rows]
    seen_keys: set[tuple[str, str, str, int, str]] = set()
    latest: dict[tuple[str, str, str, str], Mapping[str, Any]] = {}
    attempts: set[tuple[str, str, str, int]] = set()
    established_attempts: set[tuple[str, str, str, int]] = set()
    for row in materialized:
        key = (
            str(row.get("collector_run_id") or ""),
            str(row.get("venue") or ""),
            str(row.get("shard_id") or ""),
            _strict_int(row.get("subscription_attempt_id")),
            str(row.get("instrument_id") or ""),
        )
        if not all((key[0], key[1], key[2], key[4])) or key[3] < 1:
            raise ValueError("capture instrument evidence has an invalid grain")
        if key in seen_keys:
            raise ValueError("capture instrument evidence contains duplicate grain")
        seen_keys.add(key)
        attempt_key = key[:4]
        attempts.add(attempt_key)
        if row.get("subscription_established_at_utc"):
            established_attempts.add(attempt_key)
        instrument_key = (key[0], key[1], key[2], key[4])
        prior = latest.get(instrument_key)
        if prior is None or _strict_int(prior.get("subscription_attempt_id")) < key[3]:
            latest[instrument_key] = row

    requested = [row for row in latest.values() if row.get("requested") is True]
    eligible = [
        row
        for row in requested
        if row.get("eligibility_status") == EligibilityStatus.ELIGIBLE.value
    ]
    excluded = [
        row
        for row in requested
        if row.get("eligibility_status") == EligibilityStatus.INELIGIBLE.value
    ]
    unknown = [
        row
        for row in requested
        if row.get("eligibility_status") == EligibilityStatus.UNKNOWN.value
    ]
    snapshots = [
        row for row in requested if bool(row.get("first_valid_snapshot_at_utc"))
    ]
    late = [
        row
        for row in requested
        if row.get("initialization_verdict") == InitializationVerdict.LATE.value
    ]
    missing = [
        row
        for row in requested
        if row.get("eligibility_status") != EligibilityStatus.INELIGIBLE.value
        and row.get("terminal_outcome")
        in {
            InstrumentTerminalOutcome.MISSING.value,
            InstrumentTerminalOutcome.NOT_ESTABLISHED.value,
        }
    ]
    return CaptureInstrumentEvidenceSummary(
        row_count=len(materialized),
        requested_instrument_count=len(requested),
        eligible_instrument_count=len(eligible),
        excluded_instrument_count=len(excluded),
        unknown_instrument_count=len(unknown),
        coverage_denominator_count=len(eligible) + len(unknown),
        initial_snapshot_count=len(snapshots),
        late_snapshot_count=len(late),
        unexplained_missing_instrument_count=len(missing),
        subscription_attempt_count=len(attempts),
        established_subscription_attempt_count=len(established_attempts),
        eligible_established_instrument_count=sum(
            bool(row.get("subscription_established_at_utc")) for row in eligible
        ),
        eligible_initial_snapshot_count=sum(
            bool(row.get("first_valid_snapshot_at_utc")) for row in eligible
        ),
    )


def evidence_manifest_reconciliation_errors(
    rows: Iterable[Mapping[str, Any]],
    manifest_completeness: Mapping[str, Any],
) -> list[str]:
    try:
        summary = summarize_capture_instrument_evidence(rows)
    except (TypeError, ValueError) as exc:
        return [str(exc)]
    errors: list[str] = []
    for key, expected in summary.as_manifest_mapping().items():
        actual = manifest_completeness.get(key)
        if type(actual) is not int or actual != expected:
            errors.append(
                f"capture_completeness.{key}={actual!r} does not reconcile "
                f"with instrument evidence value {expected}"
            )
    return errors


def _strict_int(value: Any) -> int:
    if isinstance(value, bool):
        return -1
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


_TERMINAL_REASONS = frozenset(
    {
        "deadline_reached",
        "max_messages_reached",
        "iterator_exhausted",
        "cancelled",
        "stream_error",
        "persistence_error",
        "finalization_error",
    }
)


def _terminal_reason(value: Any) -> str:
    candidate = getattr(value, "value", value)
    reason = str(candidate)
    if reason not in _TERMINAL_REASONS:
        allowed = ", ".join(sorted(_TERMINAL_REASONS))
        raise ValueError(f"terminal_reason must be one of: {allowed}")
    return reason


def _is_sha256(value: str | None) -> bool:
    if value is None or len(value) != 64:
        return False
    return all(character in "0123456789abcdef" for character in value.lower())


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("now_utc must return a timezone-aware datetime")
    return value.astimezone(timezone.utc)


__all__ = [
    "CAPTURE_INSTRUMENT_EVIDENCE_POLICY_VERSION",
    "CAPTURE_INSTRUMENT_EVIDENCE_ROLE",
    "CAPTURE_INSTRUMENT_EVIDENCE_SCHEMA_VERSION",
    "CaptureEligibilityEvidence",
    "CaptureInstrumentEvidencePolicy",
    "CaptureInstrumentEvidenceSummary",
    "CaptureInstrumentEvidenceTracker",
    "DEFAULT_ELIGIBILITY_MAX_AGE_SECONDS",
    "DEFAULT_INITIAL_SNAPSHOT_SLA_SECONDS",
    "DEFAULT_TERMINAL_EVIDENCE_BATCH_ROWS",
    "EligibilityReason",
    "EligibilityStatus",
    "EvidencePolicyStatus",
    "InitializationVerdict",
    "InstrumentTerminalOutcome",
    "evidence_manifest_reconciliation_errors",
    "eligibility_evidence_from_subscription_metadata",
    "summarize_capture_instrument_evidence",
]
