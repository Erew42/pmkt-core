from __future__ import annotations

from copy import deepcopy
from decimal import Decimal

import pytest

from pmkt.data.schemas import topbook_row
from pmkt.streaming.profiles import (
    TOPBOOK_CHANGE_TRIGGER_VERSION,
    TOPBOOK_EXCLUDED_QUALITY_FLAGS,
    DatasetRole,
    select_storage_profile,
)
from pmkt.streaming.topbook_emission import (
    EXCLUDED_NON_STATE_QUALITY_FLAGS,
    TOPBOOK_BOUNDARY_REASONS,
    TopbookEmissionTracker,
    topbook_primary_key,
    topbook_state_fingerprint,
)


def _row(
    *,
    timestamp: str = "2026-07-19T10:00:00Z",
    exchange: str = "polymarket",
    instrument_id: str = "token-1",
) -> dict[str, object]:
    return topbook_row(
        collector_run_id="run-1",
        exchange=exchange,
        venue_market_id="market-1",
        instrument_id=instrument_id,
        received_at_utc=timestamp,
        received_at_monotonic_ns=1,
        local_sequence=1,
        best_bid_dollars=0.4,
        best_ask_dollars=0.6,
        bid_size_contracts=3.0,
        ask_size_contracts=4.0,
        best_bid_source="direct",
        best_ask_source="direct",
        tick_size_dollars=0.01,
        min_order_size_contracts=1.0,
        quote_age_ms=0,
        valid_state=True,
        quality_flags=[],
    )


def test_fingerprint_excludes_age_coordinates_and_nonstate_flags() -> None:
    left = _row()
    right = deepcopy(left)
    right.update(
        {
            "received_at_utc": "2026-07-19T10:00:01Z",
            "received_at_monotonic_ns": 2,
            "local_sequence": 2,
            "quote_age_ms": 999,
            "book_hash": "different",
            "raw_event_ref": "different",
            "quality_flags": sorted(EXCLUDED_NON_STATE_QUALITY_FLAGS),
        }
    )
    assert topbook_state_fingerprint(left) == topbook_state_fingerprint(right)


@pytest.mark.parametrize("missing", [float("nan"), Decimal("NaN")])
def test_fingerprint_normalizes_roundtrip_nan_as_missing(missing: object) -> None:
    expected = _row()
    expected["bid_size_contracts"] = None
    roundtripped = deepcopy(expected)
    roundtripped["bid_size_contracts"] = missing

    assert topbook_state_fingerprint(roundtripped) == topbook_state_fingerprint(
        expected
    )


def test_fingerprint_still_rejects_infinite_decimals() -> None:
    row = _row()
    row["bid_size_contracts"] = float("inf")

    with pytest.raises(ValueError, match="finite"):
        topbook_state_fingerprint(row)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("best_bid_dollars", 0.41),
        ("bid_size_contracts", 9.0),
        ("valid_state", False),
        ("tick_size_dollars", 0.02),
        ("min_order_size_contracts", 2.0),
        ("best_bid_source", "derived"),
        ("quality_flags", ["hash_mismatch"]),
    ],
)
def test_meaningful_state_and_source_changes_alter_fingerprint(
    field: str, value: object
) -> None:
    before = _row()
    after = deepcopy(before)
    after[field] = value
    assert topbook_state_fingerprint(before) != topbook_state_fingerprint(after)


def test_quiet_market_emits_only_due_checkpoint() -> None:
    tracker = TopbookEmissionTracker(checkpoint_interval_seconds=10)
    first = tracker.observe(_row(), now_monotonic_ns=0)
    assert first is not None and first.role is DatasetRole.TOPBOOK_MAIN
    quiet = _row(timestamp="2026-07-19T10:00:01Z")
    assert tracker.observe(quiet, now_monotonic_ns=9_000_000_000) is None
    checkpoint = tracker.observe(quiet, now_monotonic_ns=10_000_000_000)
    assert checkpoint is not None
    assert checkpoint.role is DatasetRole.TOPBOOK_CHECKPOINT
    assert checkpoint.reason == "periodic"


def test_due_restatements_use_required_fresh_coordinates() -> None:
    tracker = TopbookEmissionTracker(checkpoint_interval_seconds=10)
    main = tracker.observe(_row(), now_monotonic_ns=0)
    assert main is not None

    restatements = tracker.due_restatements(
        now_monotonic_ns=10_000_000_000,
        received_at_utc="2026-07-19T10:00:10Z",
        local_sequence=2,
    )

    assert len(restatements) == 1
    checkpoint = restatements[0]
    assert checkpoint.role is DatasetRole.TOPBOOK_CHECKPOINT
    assert checkpoint.row["received_at_utc"] == "2026-07-19T10:00:10Z"
    assert checkpoint.row["local_sequence"] == 2
    assert topbook_primary_key(checkpoint.row) != topbook_primary_key(main.row)


def test_tracker_identity_includes_exchange_and_instrument() -> None:
    tracker = TopbookEmissionTracker(checkpoint_interval_seconds=300)
    first = tracker.observe(_row(exchange="polymarket"), now_monotonic_ns=0)
    second = tracker.observe(_row(exchange="kalshi"), now_monotonic_ns=1)

    assert first is not None and first.role is DatasetRole.TOPBOOK_MAIN
    assert second is not None and second.role is DatasetRole.TOPBOOK_MAIN

    restatements = tracker.due_restatements(
        now_monotonic_ns=300_000_000_001,
        received_at_utc="2026-07-19T10:05:00Z",
        local_sequence=2,
    )
    assert {str(emission.row["exchange"]) for emission in restatements} == {
        "kalshi",
        "polymarket",
    }


def test_main_change_wins_over_requested_restatement() -> None:
    tracker = TopbookEmissionTracker()
    assert tracker.observe(_row(), now_monotonic_ns=0) is not None
    changed = _row(timestamp="2026-07-19T10:00:02Z")
    changed["valid_state"] = False
    emission = tracker.observe(
        changed, now_monotonic_ns=1, restatement_reason="reconnect"
    )
    assert emission is not None
    assert emission.role is DatasetRole.TOPBOOK_MAIN
    assert emission.reason == "state_change"


def test_same_primary_key_checkpoint_is_skipped_when_main_already_exists() -> None:
    tracker = TopbookEmissionTracker()
    row = _row()
    tracker.observe(row, now_monotonic_ns=0)
    assert (
        tracker.observe(row, now_monotonic_ns=1, restatement_reason="startup") is None
    )


def test_same_timestamp_state_changes_receive_unique_causal_primary_keys() -> None:
    tracker = TopbookEmissionTracker()
    first = tracker.observe(_row(), now_monotonic_ns=0)
    changed = _row()
    changed["best_bid_dollars"] = 0.41
    changed["local_sequence"] = 2
    second = tracker.observe(changed, now_monotonic_ns=1)

    assert first is not None and second is not None
    assert first.row["received_at_utc"] == "2026-07-19T10:00:00Z"
    assert second.row["received_at_utc"] == "2026-07-19T10:00:00.000001Z"


def test_dense_restatements_receive_unique_causal_primary_keys() -> None:
    tracker = TopbookEmissionTracker()
    first = tracker.observe(_row(), now_monotonic_ns=0, force_main=True)
    unchanged = _row()
    unchanged["local_sequence"] = 2
    second = tracker.observe(unchanged, now_monotonic_ns=1, force_main=True)

    assert first is not None and second is not None
    assert first.reason == "initial"
    assert second.reason == "dense_restatement"
    assert first.row["received_at_utc"] == "2026-07-19T10:00:00Z"
    assert second.row["received_at_utc"] == "2026-07-19T10:00:00.000001Z"


@pytest.mark.parametrize("reason", sorted(TOPBOOK_BOUNDARY_REASONS))
def test_boundary_restatements_emit_disjoint_checkpoint_coordinates(
    reason: str,
) -> None:
    tracker = TopbookEmissionTracker()
    main = tracker.observe(_row(), now_monotonic_ns=0)
    assert main is not None

    emissions = tracker.boundary_restatements(
        reason=reason,
        now_monotonic_ns=2,
        received_at_utc="2026-07-19T10:00:01Z",
        local_sequence=2,
    )

    assert len(emissions) == 1
    checkpoint = emissions[0]
    assert checkpoint.role is DatasetRole.TOPBOOK_CHECKPOINT
    assert checkpoint.reason == reason
    assert checkpoint.row["received_at_monotonic_ns"] == 2
    assert checkpoint.row["local_sequence"] == 2
    assert topbook_primary_key(checkpoint.row) != topbook_primary_key(main.row)


def test_suppressed_boundary_does_not_delay_periodic_checkpoint() -> None:
    tracker = TopbookEmissionTracker(checkpoint_interval_seconds=10)
    row = _row()
    assert tracker.observe(row, now_monotonic_ns=0) is not None
    assert (
        tracker.observe(
            row, now_monotonic_ns=9_000_000_000, restatement_reason="startup"
        )
        is None
    )

    due = tracker.observe(
        _row(timestamp="2026-07-19T10:00:10Z"),
        now_monotonic_ns=10_000_000_000,
    )

    assert due is not None
    assert due.role is DatasetRole.TOPBOOK_CHECKPOINT
    assert due.reason == "periodic"


def test_profile_manifest_records_exact_topbook_fingerprint_authority() -> None:
    manifest = select_storage_profile(
        "book-tape", profile_version="1"
    ).to_manifest_mapping()

    assert manifest["change_trigger_version"] == TOPBOOK_CHANGE_TRIGGER_VERSION
    assert manifest["excluded_topbook_quality_flags"] == sorted(
        TOPBOOK_EXCLUDED_QUALITY_FLAGS
    )
    assert EXCLUDED_NON_STATE_QUALITY_FLAGS == TOPBOOK_EXCLUDED_QUALITY_FLAGS


def test_dense_to_sparse_fixture_is_deterministic() -> None:
    rows = [
        _row(timestamp="2026-07-19T10:00:00Z"),
        _row(timestamp="2026-07-19T10:00:01Z"),
        _row(timestamp="2026-07-19T10:00:02Z"),
    ]
    rows[2]["best_bid_dollars"] = 0.41

    def materialize() -> list[tuple[str, str, str, dict[str, object]]]:
        tracker = TopbookEmissionTracker()
        result = []
        for index, row in enumerate(rows):
            emission = tracker.observe(row, now_monotonic_ns=index)
            if emission is not None:
                result.append(
                    (
                        emission.role.value,
                        emission.reason,
                        emission.fingerprint,
                        dict(emission.row),
                    )
                )
        return result

    assert materialize() == materialize()
