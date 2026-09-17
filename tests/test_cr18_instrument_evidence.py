from __future__ import annotations

from datetime import datetime, timedelta, timezone
from itertools import islice
import tracemalloc

import pandas as pd
import pytest

from pmkt.data.validation import validate_frame
from pmkt.streaming.legacy.capture_completeness import (
    CaptureIntent,
    CaptureStatus,
    evaluate_capture_completeness,
)
from pmkt.streaming.legacy.instrument_evidence import (
    CAPTURE_INSTRUMENT_EVIDENCE_ROLE,
    CaptureInstrumentEvidencePolicy,
    CaptureInstrumentEvidenceTracker,
    EvidencePolicyStatus,
    evidence_manifest_reconciliation_errors,
    summarize_capture_instrument_evidence,
)
from pmkt.streaming.legacy.profiles import DatasetRole, select_storage_profile

_NOW = datetime(2026, 7, 27, 10, 0, tzinfo=timezone.utc)
_SHA = "a" * 64


@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
def test_integrity_evidence_separates_receipt_quote_validity_and_latest_attempt(venue):
    tracker = CaptureInstrumentEvidenceTracker(collector_run_id="v3", venue=venue,
        shard_id="s", instrument_ids=["one"], integrity_evidence=True,
        now_utc=lambda: _NOW, eligibility_evidence={"one": _evidence("eligible")})
    assert tracker.summary("stream_error").integrity_evidence
    tracker.begin_subscription_attempt()
    tracker.mark_subscription_established(established_at_utc=_NOW.isoformat())
    tracker.record_book("one", observed_at_utc=(_NOW + timedelta(seconds=1)).isoformat(),
        snapshot_received=True, book_integrity_valid=False)
    tracker.record_book("one", observed_at_utc=(_NOW + timedelta(seconds=2)).isoformat(),
        snapshot_received=True, book_integrity_valid=True)
    row = tracker.terminal_rows("deadline_reached")[0]
    assert row["first_valid_snapshot_at_utc"] is None
    assert row["first_snapshot_received_at_utc"] == (_NOW + timedelta(seconds=1)).isoformat()
    assert row["first_integrity_valid_book_at_utc"] == (_NOW + timedelta(seconds=2)).isoformat()
    assert row["initial_snapshot_latency_ms"] is None
    assert row["first_integrity_valid_book_latency_ms"] == 2000
    assert row["terminal_outcome"] == "observed_intact"
    assert validate_frame(pd.DataFrame([row]), "capture_instrument_evidence.v2", strict=True).ok
    summary = tracker.summary("deadline_reached")
    assert summary.initial_snapshot_count == 1
    report = evaluate_capture_completeness(venue=venue, instruments_with_snapshots=0,
        event_count=2, reconnect_count=0, duration_seconds_actual=60, duration_seconds_requested=60,
        instrument_evidence_summary=summary, evidence_policy_status="provisional",
        evidence_artifact_role=CAPTURE_INSTRUMENT_EVIDENCE_ROLE, evidence_artifact_hash="b" * 64,
        evidence_artifact_reconciled=True, acceptance_evidence_eligible=True)
    assert report.capture_status is CaptureStatus.COMPLETE
    assert report.policy_version == "capture_completeness.v3"
    assert not report.acceptance_eligible
    tracker.begin_subscription_attempt()
    tracker.mark_subscription_established(established_at_utc=_NOW.isoformat())
    rows = tracker.terminal_rows("deadline_reached")
    assert rows[0] == row
    assert len(rows) == 2
    assert tracker.summary("deadline_reached").initial_snapshot_count == 0
    assert summarize_capture_instrument_evidence(rows).initial_snapshot_count == 0


def _evidence(status: str, *, observed_at: datetime = _NOW) -> dict[str, object]:
    return {
        "status": status,
        "reason": "source_active" if status == "eligible" else "source_inactive",
        "source_identity": "validated_subscription_plan.v1",
        "source_reference": "plan.json",
        "source_sha256": _SHA,
        "observed_at_utc": observed_at.isoformat(),
    }


@pytest.mark.parametrize("statuses,expected", [
    (["unknown", "unknown"], "unevaluated"),
    (["eligible", "unknown"], "partial"),
    (["ineligible", "unknown"], "partial"),
    (["eligible", "ineligible"], "evaluated"),
    (["ineligible"], "evaluated"),
])
@pytest.mark.parametrize("terminal", ["deadline_reached", "persistence_error"])
@pytest.mark.parametrize("snapshots", [False, True])
def test_eligibility_reporting_preserves_capture_verdicts(statuses, expected, terminal, snapshots):
    ids = [str(i) for i in range(len(statuses))]
    tracker = CaptureInstrumentEvidenceTracker(
        collector_run_id="eligibility", venue="polymarket", shard_id="s",
        instrument_ids=ids, now_utc=lambda: _NOW,
        eligibility_evidence={i: _evidence(status) for i, status in zip(ids, statuses)
                              if status != "unknown"})
    tracker.begin_subscription_attempt()
    tracker.mark_subscription_established(established_at_utc=_NOW.isoformat())
    if snapshots:
        for instrument in ids:
            tracker.record_valid_snapshot(instrument, observed_at_utc=_NOW.isoformat())
    summary = tracker.summary(terminal)
    report = evaluate_capture_completeness(
        venue="polymarket", instruments_with_snapshots=0, event_count=0,
        reconnect_count=0, duration_seconds_actual=60, duration_seconds_requested=60,
        instrument_evidence_summary=summary, terminal_reason=terminal)
    assert report.eligibility_evaluation_status == expected
    assert report.as_manifest_mapping()["eligibility_evaluation_status"] == expected
    assert not report.acceptance_eligible
    if terminal == "persistence_error" or (not snapshots and any(s != "ineligible" for s in statuses)):
        assert report.capture_status is CaptureStatus.FAILED
        assert report.reasons
    elif "unknown" in statuses:
        assert report.capture_status is CaptureStatus.PARTIAL
    else:
        assert report.capture_status is CaptureStatus.COMPLETE


def test_v3_recovered_evidence_stays_partial_and_provisional(tmp_path):
    from types import SimpleNamespace
    from pmkt.streaming.legacy.recovery import _recovered_capture_completeness

    tracker = CaptureInstrumentEvidenceTracker(collector_run_id="v3", venue="kalshi",
        shard_id="s", instrument_ids=["one"], integrity_evidence=True,
        now_utc=lambda: _NOW, eligibility_evidence={"one": _evidence("eligible")})
    tracker.begin_subscription_attempt()
    tracker.mark_subscription_established(established_at_utc=_NOW.isoformat())
    tracker.record_book("one", observed_at_utc=_NOW.isoformat(),
        snapshot_received=True, book_integrity_valid=True)
    pd.DataFrame(tracker.terminal_rows("stream_error")).to_parquet(tmp_path / "evidence.parquet")
    report = _recovered_capture_completeness(tmp_path, SimpleNamespace(profile_version="3"),
        {CAPTURE_INSTRUMENT_EVIDENCE_ROLE: {"path": "evidence.parquet", "segment_manifest_hash": _SHA}})
    assert report["policy_version"] == "capture_completeness.v3"
    assert report["initial_snapshot_count"] == 1
    assert report["capture_status"] == "partial"
    assert report["policy_status"] == "provisional"
    assert not report["acceptance_eligible"]


def _tracker(
    venue: str,
    *,
    instruments: tuple[str, ...] = ("one", "two"),
    eligibility: dict[str, dict[str, object]] | None = None,
    policy: CaptureInstrumentEvidencePolicy | None = None,
) -> CaptureInstrumentEvidenceTracker:
    return CaptureInstrumentEvidenceTracker(
        collector_run_id="run-1",
        venue=venue,
        shard_id=f"{venue}-0",
        instrument_ids=instruments,
        eligibility_evidence=eligibility,
        policy=policy,
        now_utc=lambda: _NOW,
    )


@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
def test_valid_evidence_tracks_exact_attempt_grain_and_acceptance(venue: str) -> None:
    policy = CaptureInstrumentEvidencePolicy(
        policy_status=EvidencePolicyStatus.CALIBRATED,
        initialization_sla_seconds=30,
    )
    tracker = _tracker(
        venue,
        instruments=("one",),
        eligibility={"one": _evidence("eligible")},
        policy=policy,
    )
    assert tracker.begin_subscription_attempt() == 1
    tracker.mark_subscription_established(
        subscription_sent_at_utc=_NOW.isoformat(),
        established_at_utc=(_NOW + timedelta(milliseconds=5)).isoformat(),
    )
    tracker.record_valid_snapshot(
        "one", observed_at_utc=(_NOW + timedelta(seconds=1)).isoformat()
    )

    rows = tracker.terminal_rows("deadline_reached")
    assert len(rows) == 1
    assert rows[0]["initialization_verdict"] == "on_time"
    assert rows[0]["terminal_outcome"] == "observed_valid"
    assert validate_frame(
        pd.DataFrame(rows), "capture_instrument_evidence.v1", strict=True
    ).ok

    summary = tracker.summary("deadline_reached")
    report = evaluate_capture_completeness(
        venue=venue,
        instruments_with_snapshots=1,
        event_count=1,
        reconnect_count=0,
        duration_seconds_actual=60,
        duration_seconds_requested=60,
        instrument_evidence_summary=summary,
        evidence_policy_status="calibrated",
        evidence_artifact_role=CAPTURE_INSTRUMENT_EVIDENCE_ROLE,
        evidence_artifact_hash="b" * 64,
        evidence_artifact_reconciled=True,
        acceptance_evidence_eligible=True,
    )
    assert report.capture_status is CaptureStatus.COMPLETE
    assert report.acceptance_eligible is True


@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
def test_unknown_eligibility_cannot_shrink_coverage_denominator(venue: str) -> None:
    tracker = _tracker(
        venue,
        eligibility={"one": _evidence("eligible")},
    )
    tracker.begin_subscription_attempt()
    tracker.mark_subscription_established(established_at_utc=_NOW.isoformat())
    tracker.record_valid_snapshot(
        "one", observed_at_utc=(_NOW + timedelta(seconds=1)).isoformat()
    )
    summary = tracker.summary("deadline_reached")

    assert summary.eligible_instrument_count == 1
    assert summary.unknown_instrument_count == 1
    assert summary.coverage_denominator_count == 2
    assert summary.unexplained_missing_instrument_count == 1


@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
def test_invalid_late_missing_and_all_ineligible_matrix(venue: str) -> None:
    late = _tracker(
        venue,
        instruments=("late", "missing", "inactive"),
        eligibility={
            "late": _evidence("eligible"),
            "missing": _evidence("eligible"),
            "inactive": _evidence("ineligible"),
        },
        policy=CaptureInstrumentEvidencePolicy(initialization_sla_seconds=1),
    )
    late.begin_subscription_attempt()
    late.mark_subscription_established(established_at_utc=_NOW.isoformat())
    late.record_valid_snapshot(
        "late", observed_at_utc=(_NOW + timedelta(seconds=2)).isoformat()
    )
    rows = {row["instrument_id"]: row for row in late.terminal_rows("deadline_reached")}
    assert rows["late"]["terminal_outcome"] == "late"
    assert rows["missing"]["terminal_outcome"] == "missing"
    assert rows["inactive"]["terminal_outcome"] == "ineligible"

    all_ineligible = _tracker(
        venue,
        eligibility={
            "one": _evidence("ineligible"),
            "two": _evidence("ineligible"),
        },
    )
    all_ineligible.begin_subscription_attempt()
    summary = all_ineligible.summary("deadline_reached")
    assert summary.all_requested_ineligible is True
    assert summary.coverage_denominator_count == 0


@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
def test_operational_unknown_denominator_with_zero_valid_snapshots_fails(
    venue: str,
) -> None:
    tracker = _tracker(venue, instruments=("unknown",))
    tracker.begin_subscription_attempt()
    tracker.mark_subscription_established(
        subscription_sent_at_utc=_NOW.isoformat(),
        established_at_utc=(_NOW + timedelta(milliseconds=5)).isoformat(),
    )
    report = evaluate_capture_completeness(
        venue=venue,
        instruments_with_snapshots=0,
        event_count=0,
        reconnect_count=0,
        duration_seconds_actual=60,
        duration_seconds_requested=60,
        instrument_evidence_summary=tracker.summary("deadline_reached"),
    )
    assert report.capture_status is CaptureStatus.FAILED
    assert report.ok is False
    assert "no valid initial snapshot" in " ".join(report.reasons)


@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
def test_profile_v2_smoke_missing_snapshot_is_unevaluated_partial(
    venue: str,
) -> None:
    tracker = _tracker(
        venue,
        instruments=("eligible",),
        eligibility={"eligible": _evidence("eligible")},
    )
    tracker.begin_subscription_attempt()
    tracker.mark_subscription_established(
        subscription_sent_at_utc=_NOW.isoformat(),
        established_at_utc=(_NOW + timedelta(milliseconds=5)).isoformat(),
    )
    report = evaluate_capture_completeness(
        venue=venue,
        instruments_with_snapshots=0,
        event_count=0,
        reconnect_count=0,
        duration_seconds_actual=1,
        duration_seconds_requested=60,
        capture_intent=CaptureIntent.SMOKE,
        terminal_reason="max_messages_reached",
        instrument_evidence_summary=tracker.summary("max_messages_reached"),
    )
    assert report.evaluated is False
    assert report.capture_status is CaptureStatus.PARTIAL
    assert report.ok is True
    assert report.acceptance_eligible is False


@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
def test_all_ineligible_operational_denominator_remains_complete(venue: str) -> None:
    tracker = _tracker(
        venue,
        instruments=("inactive",),
        eligibility={"inactive": _evidence("ineligible")},
    )
    tracker.begin_subscription_attempt()
    report = evaluate_capture_completeness(
        venue=venue,
        instruments_with_snapshots=0,
        event_count=0,
        reconnect_count=0,
        duration_seconds_actual=60,
        duration_seconds_requested=60,
        instrument_evidence_summary=tracker.summary("deadline_reached"),
    )
    assert report.capture_status is CaptureStatus.COMPLETE
    assert report.ok is True


def test_stale_or_malformed_evidence_becomes_unknown() -> None:
    tracker = _tracker(
        "polymarket",
        eligibility={
            "one": _evidence("eligible", observed_at=_NOW - timedelta(seconds=301)),
            "two": {"status": "eligible", "reason": "wrong"},
        },
    )
    tracker.begin_subscription_attempt()
    rows = {
        row["instrument_id"]: row for row in tracker.terminal_rows("deadline_reached")
    }
    assert rows["one"]["eligibility_reason"] == "stale_evidence"
    assert rows["two"]["eligibility_reason"] == "malformed_evidence"
    assert {row["eligibility_status"] for row in rows.values()} == {"unknown"}


def test_manifest_counts_reconcile_exactly_and_duplicate_grain_fails_closed() -> None:
    tracker = _tracker(
        "polymarket",
        instruments=("one",),
        eligibility={"one": _evidence("eligible")},
    )
    tracker.begin_subscription_attempt()
    rows = list(tracker.terminal_rows("deadline_reached"))
    summary = summarize_capture_instrument_evidence(rows)
    manifest = summary.as_manifest_mapping()
    assert evidence_manifest_reconciliation_errors(rows, manifest) == []

    manifest["requested_instrument_count"] = 2
    errors = evidence_manifest_reconciliation_errors(rows, manifest)
    assert any("requested_instrument_count" in error for error in errors)
    with pytest.raises(ValueError, match="duplicate grain"):
        summarize_capture_instrument_evidence([*rows, rows[0]])


def test_reconnect_attempt_state_stays_compact_and_streams_terminal_rows() -> None:
    instruments = tuple(f"instrument-{index}" for index in range(2_541))
    tracemalloc.start()
    try:
        tracker = _tracker("polymarket", instruments=instruments)
        for _ in range(32):
            tracker.begin_subscription_attempt()
            tracker.mark_subscription_established(established_at_utc=_NOW.isoformat())
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak_bytes < 8 * 1024 * 1024
    assert tracker.attempt_count == 32
    assert tracker.summary("iterator_exhausted").row_count == 81_312
    streamed = tracker.iter_terminal_rows("iterator_exhausted")
    assert not isinstance(streamed, tuple)
    assert [row["instrument_id"] for row in islice(streamed, 3)] == [
        "instrument-0",
        "instrument-1",
        "instrument-2",
    ]


def test_compact_summary_matches_materialized_multi_attempt_evidence() -> None:
    tracker = _tracker(
        "polymarket",
        eligibility={"one": _evidence("eligible")},
    )
    for attempt in range(3):
        tracker.begin_subscription_attempt()
        tracker.mark_subscription_established(established_at_utc=_NOW.isoformat())
        if attempt < 2:
            tracker.record_valid_snapshot(
                "one",
                observed_at_utc=(_NOW + timedelta(seconds=1)).isoformat(),
            )

    rows = tracker.terminal_rows("iterator_exhausted")
    assert tracker.summary("iterator_exhausted") == (
        summarize_capture_instrument_evidence(rows)
    )


def test_named_profiles_keep_v1_readers_and_require_evidence_in_v2() -> None:
    current = select_storage_profile("full")
    legacy = select_storage_profile("full", profile_version="1")

    assert current.definition.profile_version == "2"
    assert DatasetRole.INSTRUMENT_EVIDENCE in current.required_roles
    assert DatasetRole.INSTRUMENT_EVIDENCE not in legacy.enabled_roles
