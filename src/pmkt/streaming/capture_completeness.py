"""Fail-closed capture-completeness evaluation.

A capture that observed no market data must not finalize as a success.

Observed on 2026-07-24: subscribing 600 Polymarket tokens exceeded the
websockets 1 MiB ``max_size`` default, so the server closed every connection
with 1009 "message too big".  The client reconnected 152 times in ten minutes
and recorded nothing, yet every layer reported success::

    stdout   "(0 events, 0 snapshots, 0 levels)"
    manifest status="success", error_type=None, error_message=None,
             counts={assets_with_snapshots: 0, events: 0, levels: 0, snapshots: 0}
    process  exit 0

Scope note
----------
This module deliberately implements only the rules that are unambiguous without
a specification decision:

* no instrument ever established a valid book -> the subscription never
  established;
* a sustained reconnect rate that invalidates the evidence.

It does **not** implement a coverage percentage. The correct denominator is not
"requested instruments": a request for 600 token ids does not mean 600 are
eligible for a book. Missing instruments may be delisted, resolved, legitimately
book-less, rejected by a transport limit, or lost locally, and those causes carry
different meanings. Until eligibility and cause attribution are specified, a
percentage cutoff would mix avoidable collector loss with legitimate
ineligibility.

The evaluation therefore returns the raw measures alongside the verdict so the
manifest can record what was observed and which policy version interpreted it,
rather than only a precomputed label.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

#: Bump when the interpretation rules below change, so a persisted verdict can
#: be recomputed against the policy that produced it.
COMPLETENESS_POLICY_VERSION = "capture_completeness.v2"

# A connection that is repeatedly rejected produces a high reconnect rate with no
# useful data. One reconnect every few seconds for a whole run is pathological
# regardless of venue.
MAX_RECONNECTS_PER_MINUTE = 6.0


class CaptureIntent(str, Enum):
    """Declared purpose of a capture."""

    OPERATIONAL = "operational"
    SMOKE = "smoke"


class CaptureTerminationReason(str, Enum):
    """Closed vocabulary describing why collection stopped."""

    DEADLINE_REACHED = "deadline_reached"
    MAX_MESSAGES_REACHED = "max_messages_reached"
    ITERATOR_EXHAUSTED = "iterator_exhausted"
    CANCELLED = "cancelled"
    STREAM_ERROR = "stream_error"
    PERSISTENCE_ERROR = "persistence_error"
    FINALIZATION_ERROR = "finalization_error"


class CaptureExecutionStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"


class CaptureStatus(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    FAILED = "failed"


class CaptureCompletenessError(RuntimeError):
    """Raised when a capture finished without usable market-data evidence."""


@dataclass(frozen=True)
class CaptureCompletenessReport:
    """Raw measures plus the verdict derived from them."""

    ok: bool
    policy_version: str
    venue: str
    instruments_requested: int | None
    instruments_with_snapshots: int
    instruments_with_invalid_snapshots: int
    event_count: int
    reconnect_count: int
    capture_intent: CaptureIntent
    terminal_reason: CaptureTerminationReason
    duration_seconds_requested: float | None
    duration_seconds_actual: float
    reconnects_per_minute: float | None
    evaluated: bool
    acceptance_eligible: bool
    reasons: tuple[str, ...] = field(default_factory=tuple)
    policy_status: str = "provisional"
    execution_status: CaptureExecutionStatus = CaptureExecutionStatus.SUCCESS
    capture_status: CaptureStatus = CaptureStatus.COMPLETE
    legacy_status: str = "success"
    requested_instrument_count: int | None = None
    eligible_instrument_count: int | None = None
    excluded_instrument_count: int | None = None
    unknown_instrument_count: int | None = None
    coverage_denominator_count: int | None = None
    initial_snapshot_count: int | None = None
    late_snapshot_count: int | None = None
    unexplained_missing_instrument_count: int | None = None
    subscription_attempt_count: int | None = None
    established_subscription_attempt_count: int | None = None
    eligible_established_instrument_count: int | None = None
    eligible_initial_snapshot_count: int | None = None
    evidence_row_count: int | None = None
    evidence_artifact_role: str | None = None
    evidence_artifact_hash: str | None = None
    evidence_artifact_reconciled: bool = False

    def as_manifest_mapping(self) -> dict[str, Any]:
        return {
            "policy_version": self.policy_version,
            "ok": self.ok,
            "evaluated": self.evaluated,
            "acceptance_eligible": self.acceptance_eligible,
            "policy_status": self.policy_status,
            "execution_status": self.execution_status.value,
            "capture_status": self.capture_status.value,
            "legacy_status": self.legacy_status,
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
            "evidence_row_count": self.evidence_row_count,
            "evidence_artifact_role": self.evidence_artifact_role,
            "evidence_artifact_hash": self.evidence_artifact_hash,
            "evidence_artifact_reconciled": self.evidence_artifact_reconciled,
            "instruments_requested": self.instruments_requested,
            "instruments_with_snapshots": self.instruments_with_snapshots,
            "instruments_with_invalid_snapshots": (
                self.instruments_with_invalid_snapshots
            ),
            "event_count": self.event_count,
            "reconnect_count": self.reconnect_count,
            "capture_intent": self.capture_intent.value,
            "terminal_reason": self.terminal_reason.value,
            "duration_seconds_requested": (
                round(self.duration_seconds_requested, 3)
                if self.duration_seconds_requested is not None
                else None
            ),
            "duration_seconds_actual": round(self.duration_seconds_actual, 3),
            "reconnects_per_minute": (
                round(self.reconnects_per_minute, 3)
                if self.reconnects_per_minute is not None
                else None
            ),
            "reasons": list(self.reasons),
        }


def evaluate_capture_completeness(
    *,
    venue: str,
    instruments_with_snapshots: int,
    event_count: int,
    reconnect_count: int,
    duration_seconds_actual: float,
    instruments_requested: int | None = None,
    instruments_with_invalid_snapshots: int = 0,
    capture_intent: CaptureIntent | str = CaptureIntent.OPERATIONAL,
    terminal_reason: CaptureTerminationReason | str = (
        CaptureTerminationReason.DEADLINE_REACHED
    ),
    duration_seconds_requested: float | None = None,
    acceptance_evidence_eligible: bool = False,
    instrument_evidence_summary: Any | None = None,
    evidence_policy_status: str = "provisional",
    evidence_artifact_role: str | None = None,
    evidence_artifact_hash: str | None = None,
    evidence_artifact_reconciled: bool = False,
) -> CaptureCompletenessReport:
    """Evaluate a finished capture. Never raises; returns the verdict.

    Actual time remains the reconnect-rate denominator. Requested time and the
    terminal reason decide whether an operational run completed its lifecycle;
    short runs are exempt only when their intent is explicitly ``smoke``.
    """
    intent = CaptureIntent(capture_intent)
    reason = CaptureTerminationReason(terminal_reason)
    if duration_seconds_actual < 0:
        raise ValueError("duration_seconds_actual must be nonnegative")
    if duration_seconds_requested is not None and duration_seconds_requested < 0:
        raise ValueError("duration_seconds_requested must be nonnegative")
    if instruments_with_snapshots < 0 or instruments_with_invalid_snapshots < 0:
        raise ValueError("snapshot counts must be nonnegative")

    execution_reasons: list[str] = []
    capture_reasons: list[str] = []
    per_minute: float | None = None
    if duration_seconds_actual > 0:
        per_minute = reconnect_count * 60.0 / duration_seconds_actual

    evaluated = intent is CaptureIntent.OPERATIONAL
    terminal_failure = reason in {
        CaptureTerminationReason.CANCELLED,
        CaptureTerminationReason.STREAM_ERROR,
        CaptureTerminationReason.PERSISTENCE_ERROR,
        CaptureTerminationReason.FINALIZATION_ERROR,
    }
    if terminal_failure:
        execution_reasons.append(f"capture terminated via {reason.value}")
    if evaluated:
        if reason is not CaptureTerminationReason.DEADLINE_REACHED:
            execution_reasons.append(
                f"operational capture terminated via {reason.value} before "
                "completing its requested lifecycle"
            )
        if (
            duration_seconds_requested is not None
            and reason is CaptureTerminationReason.DEADLINE_REACHED
            and duration_seconds_actual + 0.05 < duration_seconds_requested
        ):
            execution_reasons.append(
                f"operational capture ended after {duration_seconds_actual:.3f}s, "
                f"before requested duration {duration_seconds_requested:.3f}s"
            )
        if per_minute is not None and per_minute > MAX_RECONNECTS_PER_MINUTE:
            execution_reasons.append(
                f"reconnected {reconnect_count} times in "
                f"{duration_seconds_actual:.0f}s ({per_minute:.1f}/min, limit "
                f"{MAX_RECONNECTS_PER_MINUTE:.1f}/min); capture evidence is not "
                "trustworthy"
            )

    summary_mapping: dict[str, int] = {}
    if instrument_evidence_summary is not None:
        summary_mapping = dict(instrument_evidence_summary.as_manifest_mapping())
        eligible = summary_mapping["eligible_instrument_count"]
        unknown = summary_mapping["unknown_instrument_count"]
        late = summary_mapping["late_snapshot_count"]
        missing = summary_mapping["unexplained_missing_instrument_count"]
        eligible_established = summary_mapping["eligible_established_instrument_count"]
        eligible_snapshots = summary_mapping["eligible_initial_snapshot_count"]
        coverage_denominator = summary_mapping["coverage_denominator_count"]
        initial_snapshots = summary_mapping["initial_snapshot_count"]
        if reason in {
            CaptureTerminationReason.PERSISTENCE_ERROR,
            CaptureTerminationReason.FINALIZATION_ERROR,
        }:
            capture_status = CaptureStatus.FAILED
            capture_reasons.append(
                "capture persistence or finalization did not complete"
            )
        elif evaluated and coverage_denominator > 0 and initial_snapshots == 0:
            capture_status = CaptureStatus.FAILED
            capture_reasons.append(
                "the non-empty coverage denominator received no valid initial "
                "snapshot"
            )
        elif evaluated and eligible > 0 and eligible_established == 0:
            capture_status = CaptureStatus.FAILED
            capture_reasons.append("no eligible subscription attempt was established")
        elif evaluated and eligible > 0 and eligible_snapshots == 0:
            capture_status = CaptureStatus.FAILED
            capture_reasons.append(
                "eligible instruments existed but no valid initial snapshot arrived"
            )
        elif unknown > 0 or late > 0 or missing > 0:
            capture_status = CaptureStatus.PARTIAL
            if not evaluated:
                capture_reasons.append(
                    "smoke capture coverage is diagnostic and unevaluated"
                )
            if unknown:
                capture_reasons.append(
                    f"{unknown} instrument eligibility verdicts are unknown"
                )
            if late:
                capture_reasons.append(
                    f"{late} required initial snapshots arrived outside the SLA"
                )
            if missing:
                capture_reasons.append(
                    f"{missing} denominator instruments have no valid initial snapshot"
                )
        else:
            capture_status = CaptureStatus.COMPLETE
    elif evaluated and instruments_with_snapshots == 0:
        capture_status = CaptureStatus.FAILED
        capture_reasons.append(
            f"no instrument received a valid initial book snapshot in "
            f"{duration_seconds_actual:.0f}s (events={event_count}, "
            f"reconnects={reconnect_count}); the subscription never "
            "established. If the universe is large, check the websocket "
            "max_size limit against the initial snapshot size."
        )
    else:
        capture_status = CaptureStatus.COMPLETE

    execution_status = (
        CaptureExecutionStatus.FAILED
        if execution_reasons
        else CaptureExecutionStatus.SUCCESS
    )
    has_usable_evidence = (
        summary_mapping.get("initial_snapshot_count", instruments_with_snapshots) > 0
    )
    if (
        execution_status is CaptureExecutionStatus.SUCCESS
        and capture_status is CaptureStatus.COMPLETE
    ):
        legacy_status = "success"
    elif reason in {
        CaptureTerminationReason.PERSISTENCE_ERROR,
        CaptureTerminationReason.FINALIZATION_ERROR,
    } or (capture_status is CaptureStatus.FAILED and not has_usable_evidence):
        legacy_status = "failed"
    else:
        legacy_status = "partial"

    hash_valid = (
        isinstance(evidence_artifact_hash, str)
        and len(evidence_artifact_hash) == 64
        and all(
            character in "0123456789abcdef"
            for character in evidence_artifact_hash.lower()
        )
    )
    evidence_acceptance = (
        instrument_evidence_summary is not None
        and evidence_policy_status == "calibrated"
        and evidence_artifact_reconciled
        and hash_valid
        and summary_mapping["eligible_instrument_count"] > 0
    )
    acceptance_eligible = (
        evaluated
        and execution_status is CaptureExecutionStatus.SUCCESS
        and capture_status is CaptureStatus.COMPLETE
        and evidence_acceptance
        and acceptance_evidence_eligible
    )
    ok = (
        execution_status is CaptureExecutionStatus.SUCCESS
        and capture_status is not CaptureStatus.FAILED
    )
    reasons = [*execution_reasons, *capture_reasons]
    return CaptureCompletenessReport(
        ok=ok,
        policy_version=COMPLETENESS_POLICY_VERSION,
        venue=venue,
        instruments_requested=instruments_requested,
        instruments_with_snapshots=instruments_with_snapshots,
        instruments_with_invalid_snapshots=instruments_with_invalid_snapshots,
        event_count=event_count,
        reconnect_count=reconnect_count,
        capture_intent=intent,
        terminal_reason=reason,
        duration_seconds_requested=duration_seconds_requested,
        duration_seconds_actual=duration_seconds_actual,
        reconnects_per_minute=per_minute,
        evaluated=evaluated,
        acceptance_eligible=acceptance_eligible,
        reasons=tuple(reasons),
        policy_status=evidence_policy_status,
        execution_status=execution_status,
        capture_status=capture_status,
        legacy_status=legacy_status,
        requested_instrument_count=summary_mapping.get("requested_instrument_count"),
        eligible_instrument_count=summary_mapping.get("eligible_instrument_count"),
        excluded_instrument_count=summary_mapping.get("excluded_instrument_count"),
        unknown_instrument_count=summary_mapping.get("unknown_instrument_count"),
        coverage_denominator_count=summary_mapping.get("coverage_denominator_count"),
        initial_snapshot_count=summary_mapping.get("initial_snapshot_count"),
        late_snapshot_count=summary_mapping.get("late_snapshot_count"),
        unexplained_missing_instrument_count=summary_mapping.get(
            "unexplained_missing_instrument_count"
        ),
        subscription_attempt_count=summary_mapping.get("subscription_attempt_count"),
        established_subscription_attempt_count=summary_mapping.get(
            "established_subscription_attempt_count"
        ),
        eligible_established_instrument_count=summary_mapping.get(
            "eligible_established_instrument_count"
        ),
        eligible_initial_snapshot_count=summary_mapping.get(
            "eligible_initial_snapshot_count"
        ),
        evidence_row_count=summary_mapping.get("evidence_row_count"),
        evidence_artifact_role=evidence_artifact_role,
        evidence_artifact_hash=evidence_artifact_hash,
        evidence_artifact_reconciled=evidence_artifact_reconciled,
    )


def assert_capture_observed_market_data(
    **kwargs: Any,
) -> CaptureCompletenessReport:
    """Evaluate and raise on failure. Returns the report when the run passes."""
    report = evaluate_capture_completeness(**kwargs)
    if not report.ok:
        raise CaptureCompletenessError(
            f"{report.venue} capture completeness failed: " + "; ".join(report.reasons)
        )
    return report


__all__ = [
    "COMPLETENESS_POLICY_VERSION",
    "CaptureCompletenessError",
    "CaptureCompletenessReport",
    "CaptureIntent",
    "CaptureExecutionStatus",
    "CaptureStatus",
    "CaptureTerminationReason",
    "MAX_RECONNECTS_PER_MINUTE",
    "assert_capture_observed_market_data",
    "evaluate_capture_completeness",
]
