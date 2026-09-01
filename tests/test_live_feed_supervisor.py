from __future__ import annotations

import random
from collections import Counter

import pandas as pd
import pytest

from pmkt.data.validation import validate_frame
from pmkt.streaming.supervisor import (
    FeedRecoveryAction,
    FeedShardHealth,
    LiveFeedSupervisor,
)


def test_supervisor_preflight_blocks_invalid_subscription_plan() -> None:
    plan = {
        "schema_version": "subscription_plan.v1",
        "polymarket": {"assets_ids": ["1" * 152]},
        "kalshi": {"market_tickers": ["KXTEST"]},
    }

    supervisor = LiveFeedSupervisor.from_subscription_plan(plan)

    assert supervisor.preflight_report.ok is False
    assert supervisor.shards == {}
    assert any(
        "longer than single CLOB token IDs" in error
        for error in supervisor.preflight_report.errors
    )
    with pytest.raises(RuntimeError, match="feed preflight failed"):
        supervisor.require_preflight_ok()


def test_supervisor_builds_shards_from_valid_structured_plan() -> None:
    plan = {
        "schema_version": "subscription_plan.v1",
        "polymarket_assets": [
            {
                "asset_id": "token-1",
                "shard_id": "pm-a",
                "match_ids": ["match-1"],
            },
            {
                "asset_id": "token-2",
                "shard_id": "pm-a",
                "match_ids": ["match-2"],
            },
        ],
        "kalshi_market_tickers": [
            {
                "market_ticker": "KXTEST",
                "shard_id": "kx-a",
                "match_ids": ["match-1", "match-2"],
            }
        ],
    }
    polymarket = pd.DataFrame(
        [
            {
                "market_key": "pm-1",
                "token_ids": ["token-1", "token-2"],
                "closed": False,
            }
        ]
    )
    kalshi = pd.DataFrame([{"market_key": "KXTEST", "status": "open"}])

    supervisor = LiveFeedSupervisor.from_subscription_plan(
        plan,
        polymarket_markets=polymarket,
        kalshi_markets=kalshi,
    )
    rows = supervisor.health_rows(now_monotonic_ns=1_000_000_000)

    assert supervisor.preflight_report.ok is True
    assert rows == [
        {
            "venue": "kalshi",
            "shard_id": "kx-a",
            "connection_state": "initialized",
            "instrument_count": 1,
            "relation_count": 2,
            "reconnect_count": 0,
            "sequence_gap_count": 0,
            "resync_count": 0,
            "error_count": 0,
            "last_message_age_ms": None,
            "last_valid_book_age_ms": None,
            "valid_book_count": 0,
            "invalid_book_count": 0,
            "valid_instrument_count": 0,
            "invalid_instrument_count": 0,
            "stale_instrument_count": 0,
            "missing_instrument_count": 0,
            "instrument_state_json": "",
            "quality_flags": [],
        },
        {
            "venue": "polymarket",
            "shard_id": "pm-a",
            "connection_state": "initialized",
            "instrument_count": 2,
            "relation_count": 2,
            "reconnect_count": 0,
            "sequence_gap_count": 0,
            "resync_count": 0,
            "error_count": 0,
            "last_message_age_ms": None,
            "last_valid_book_age_ms": None,
            "valid_book_count": 0,
            "invalid_book_count": 0,
            "valid_instrument_count": 0,
            "invalid_instrument_count": 0,
            "stale_instrument_count": 0,
            "missing_instrument_count": 0,
            "instrument_state_json": "",
            "quality_flags": [],
        },
    ]


def test_supervisor_rejects_ambiguous_instrument_shard_mapping() -> None:
    with pytest.raises(ValueError, match="multiple feed shards"):
        LiveFeedSupervisor(
            [
                FeedShardHealth(
                    venue="polymarket",
                    shard_id="pm-a",
                    subscribed_instruments=("token-1",),
                ),
                FeedShardHealth(
                    venue="polymarket",
                    shard_id="pm-b",
                    subscribed_instruments=("token-1",),
                ),
            ]
        )


def test_shard_mutators_report_only_compact_semantic_transitions() -> None:
    shard = FeedShardHealth(
        venue="polymarket",
        shard_id="pm-0",
        subscribed_instruments=("token-1",),
    )

    assert shard.mark_connected(now_monotonic_ns=1_000_000_000) is True
    assert shard.mark_connected(now_monotonic_ns=1_010_000_000) is False
    assert (
        shard.record_book(
            valid_state=True,
            now_monotonic_ns=1_020_000_000,
            instrument="token-1",
        )
        is True
    )
    assert (
        shard.record_book(
            valid_state=True,
            now_monotonic_ns=1_030_000_000,
            instrument="token-1",
        )
        is False
    )
    assert (
        shard.record_message(
            now_monotonic_ns=1_040_000_000,
            instrument="token-1",
        )
        is False
    )
    assert shard.record_error("test") is True


def test_stale_invalidation_refreshes_cached_shard_book_flags() -> None:
    shard = FeedShardHealth(
        venue="polymarket",
        shard_id="pm-0",
        subscribed_instruments=("token-1", "token-2"),
    )
    shard.mark_connected(now_monotonic_ns=1_000_000_000)
    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_010_000_000,
        instrument="token-1",
    )
    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_010_000_000,
        instrument="token-2",
    )
    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_060_000_000,
        instrument="token-1",
    )

    assert shard.invalidate_if_stale(
        now_monotonic_ns=1_065_000_000,
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )
    row = shard.as_row(
        now_monotonic_ns=1_065_000_000,
        include_instrument_state=False,
    )

    assert row["connection_state"] == "connected"
    assert row["valid_instrument_count"] == 1
    assert row["invalid_instrument_count"] == 1
    assert row["stale_instrument_count"] == 1
    assert set(row["quality_flags"]) >= {
        "invalid_book",
        "invalid_instrument_books",
        "stale_instrument_books",
    }


def test_instrument_local_staleness_emits_scoped_recovery_action() -> None:
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-0",
                subscribed_instruments=("active", "stale"),
            )
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )
    shard = supervisor.shard("polymarket", "pm-0")
    shard.mark_connected(now_monotonic_ns=1_000_000_000)
    for instrument in shard.subscribed_instruments:
        shard.record_book(
            valid_state=True,
            now_monotonic_ns=1_010_000_000,
            instrument=instrument,
        )
    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_060_000_000,
        instrument="active",
    )

    assert supervisor.recovery_actions(now_monotonic_ns=1_065_000_000) == [
        FeedRecoveryAction(
            action="reconnect_socket",
            venue="polymarket",
            shard_id="pm-0",
            reasons=("stale_messages", "stale_books"),
            instruments=("stale",),
        )
    ]


def test_never_observed_instrument_emits_recovery_after_initial_grace() -> None:
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="kalshi",
                shard_id="kx-0",
                subscribed_instruments=("KXACTIVE", "KXMISSING"),
            )
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )
    shard = supervisor.shard("kalshi", "kx-0")
    shard.mark_connected(now_monotonic_ns=1_000_000_000)
    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_015_000_000,
        instrument="KXACTIVE",
    )

    assert supervisor.recovery_actions(now_monotonic_ns=1_020_000_000) == []
    assert supervisor.recovery_actions(now_monotonic_ns=1_025_000_000) == [
        FeedRecoveryAction(
            action="reconnect_socket",
            venue="kalshi",
            shard_id="kx-0",
            reasons=("missing_instrument_books",),
            instruments=("KXMISSING",),
        )
    ]

    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_026_000_000,
        instrument="KXMISSING",
    )
    assert supervisor.current_recovery_actions() == []


def test_unchanged_stale_sweep_skips_instrument_projections(monkeypatch) -> None:
    shard = FeedShardHealth(
        venue="polymarket",
        shard_id="pm-0",
        subscribed_instruments=("token-1", "token-2"),
    )
    shard.mark_connected(now_monotonic_ns=1_000_000_000)
    for instrument in shard.subscribed_instruments:
        shard.record_book(
            valid_state=True,
            now_monotonic_ns=1_010_000_000,
            instrument=instrument,
        )
    assert shard.invalidate_if_stale(
        now_monotonic_ns=2_000_000_000,
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )

    projection_calls = 0
    original_projection = FeedShardHealth._instrument_projection

    def counted_projection(state):
        nonlocal projection_calls
        projection_calls += 1
        return original_projection(state)

    monkeypatch.setattr(
        FeedShardHealth,
        "_instrument_projection",
        staticmethod(counted_projection),
    )

    assert not shard.invalidate_if_stale(
        now_monotonic_ns=2_010_000_000,
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )
    assert projection_calls == 0


def test_cached_instrument_summary_matches_reference_recomputation() -> None:
    shard = FeedShardHealth(
        venue="polymarket",
        shard_id="pm-0",
        subscribed_instruments=("token-1", "token-2", "token-3"),
    )

    def assert_reference(now_monotonic_ns: int) -> None:
        row = shard.as_row(
            now_monotonic_ns=now_monotonic_ns,
            include_instrument_state=False,
        )
        states = list(shard.instrument_health.values())
        projections = [shard._instrument_projection(state) for state in states]
        expected_current_flags: Counter[str] = Counter()
        expected_integrity_flags: Counter[str] = Counter()
        for projection in projections:
            expected_current_flags.update(projection[3])
            expected_integrity_flags.update(projection[4])
        expected_valid = sum(int(projection[0]) for projection in projections)
        expected_invalid = sum(int(projection[1]) for projection in projections)
        expected_stale = sum(int(projection[2]) for projection in projections)
        expected_reconnect_blocking = sum(
            int(projection[5]) for projection in projections
        )

        assert row["valid_instrument_count"] == expected_valid
        assert row["invalid_instrument_count"] == expected_invalid
        assert row["stale_instrument_count"] == expected_stale
        assert row["missing_instrument_count"] == (
            len(shard.subscribed_instruments) - len(states) if states else 0
        )
        assert shard._tracked_instrument_count == len(states)
        assert shard._valid_instrument_count == expected_valid
        assert shard._invalid_instrument_count == expected_invalid
        assert shard._stale_instrument_count == expected_stale
        assert shard._reconnect_blocking_instrument_count == expected_reconnect_blocking
        assert shard._current_book_flag_counts == expected_current_flags
        assert shard._book_integrity_flag_counts == expected_integrity_flags

    shard.mark_connected(now_monotonic_ns=1_000_000_000)
    assert_reference(1_000_000_000)
    shard.record_message(
        now_monotonic_ns=1_010_000_000,
        instrument="token-1",
    )
    assert_reference(1_010_000_000)
    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_020_000_000,
        instrument="token-1",
    )
    assert_reference(1_020_000_000)
    shard.record_book(
        valid_state=False,
        now_monotonic_ns=1_030_000_000,
        instrument="token-2",
        quality_flags=("crossed_book", "hash_mismatch"),
    )
    assert_reference(1_030_000_000)
    shard.record_sequence_gap(instrument="token-1")
    assert_reference(1_040_000_000)
    shard.record_resync(
        now_monotonic_ns=1_050_000_000,
        instrument="token-1",
    )
    assert_reference(1_050_000_000)
    shard.mark_reconnect(now_monotonic_ns=1_060_000_000)
    assert_reference(1_060_000_000)
    shard.invalidate_if_stale(
        now_monotonic_ns=2_000_000_000,
        max_message_age_ms=100,
        max_valid_book_age_ms=100,
    )
    assert_reference(2_000_000_000)

    rng = random.Random(20260824)
    now_monotonic_ns = 2_100_000_000
    for _ in range(250):
        now_monotonic_ns += rng.randint(1, 25) * 1_000_000
        instrument = rng.choice(shard.subscribed_instruments)
        operation = rng.randrange(8)
        if operation == 0:
            shard.record_message(
                now_monotonic_ns=now_monotonic_ns,
                instrument=instrument,
            )
        elif operation == 1:
            shard.record_book(
                valid_state=True,
                now_monotonic_ns=now_monotonic_ns,
                instrument=instrument,
            )
        elif operation == 2:
            shard.record_book(
                valid_state=False,
                now_monotonic_ns=now_monotonic_ns,
                instrument=instrument,
                quality_flags=("crossed_book", "hash_mismatch"),
            )
        elif operation == 3:
            shard.record_sequence_gap(instrument=instrument)
        elif operation == 4:
            shard.record_resync(
                now_monotonic_ns=now_monotonic_ns,
                instrument=instrument,
            )
        elif operation == 5:
            shard.mark_reconnect(now_monotonic_ns=now_monotonic_ns)
        elif operation == 6:
            shard.mark_connected(now_monotonic_ns=now_monotonic_ns)
        else:
            now_monotonic_ns += 200_000_000
            shard.invalidate_if_stale(
                now_monotonic_ns=now_monotonic_ns,
                max_message_age_ms=100,
                max_valid_book_age_ms=100,
            )
        assert_reference(now_monotonic_ns)


def test_cached_flag_aggregate_underflow_fails_closed() -> None:
    shard = FeedShardHealth(
        venue="polymarket",
        shard_id="pm-0",
        subscribed_instruments=("token-1",),
    )
    shard.record_book(
        valid_state=False,
        now_monotonic_ns=1_000_000_000,
        instrument="token-1",
        quality_flags=("hash_mismatch",),
    )
    shard._book_integrity_flag_counts.clear()

    with pytest.raises(RuntimeError, match="negative cached book-integrity"):
        shard.record_book(
            valid_state=True,
            now_monotonic_ns=1_100_000_000,
            instrument="token-1",
        )


def test_supervisor_can_select_health_and_current_recovery_by_shard() -> None:
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-a",
                subscribed_instruments=("token-1",),
            ),
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-b",
                subscribed_instruments=("token-2",),
            ),
        ]
    )
    first = supervisor.shard("polymarket", "pm-a")
    second = supervisor.shard("polymarket", "pm-b")
    first.mark_connected(now_monotonic_ns=1_000_000_000)
    second.mark_connected(now_monotonic_ns=1_000_000_000)
    first.record_book(
        valid_state=False,
        now_monotonic_ns=1_050_000_000,
        instrument="token-1",
        quality_flags=("hash_mismatch",),
    )

    rows = supervisor.health_rows(
        now_monotonic_ns=1_100_000_000,
        include_instrument_state=False,
        shard_keys=(("polymarket", "pm-b"),),
    )
    assert [row["shard_id"] for row in rows] == ["pm-b"]
    assert supervisor.current_recovery_actions(
        shard_keys=(("polymarket", "pm-a"),)
    ) == [
        FeedRecoveryAction(
            action="reconnect_socket",
            venue="polymarket",
            shard_id="pm-a",
            reasons=("hash_mismatch", "book_integrity"),
        )
    ]


def test_supervisor_feed_health_rows_validate_as_canonical_schema() -> None:
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-a",
                subscribed_instruments=("token-1",),
                relation_ids=("match-1",),
            )
        ]
    )
    shard = supervisor.shard("polymarket", "pm-a")
    shard.mark_connected(now_monotonic_ns=1_000_000_000)
    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_001_000_000,
        instrument="token-1",
    )

    rows = supervisor.feed_health_rows(
        now_monotonic_ns=1_002_000_000,
        observed_at_utc="2026-05-31T00:00:00+00:00",
        local_sequence=1,
    )

    report = validate_frame(pd.DataFrame(rows), "feed_health.v1")
    assert report.ok is True
    assert rows[0]["schema_version"] == "feed_health.v1"


def test_supervisor_can_build_compact_health_without_instrument_detail() -> None:
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-a",
                subscribed_instruments=("token-1", "token-2"),
            )
        ]
    )
    shard = supervisor.shard("polymarket", "pm-a")
    shard.mark_connected(now_monotonic_ns=1_000_000_000)
    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_001_000_000,
        instrument="token-1",
    )

    detailed = supervisor.feed_health_rows(
        now_monotonic_ns=1_002_000_000,
        observed_at_utc="2026-05-31T00:00:00+00:00",
        local_sequence=1,
    )[0]
    compact = supervisor.feed_health_rows(
        now_monotonic_ns=1_002_000_000,
        observed_at_utc="2026-05-31T00:00:00+00:00",
        local_sequence=1,
        include_instrument_state=False,
    )[0]

    assert detailed["instrument_state_json"]
    assert compact["instrument_state_json"] == ""
    assert {
        key: value for key, value in detailed.items() if key != "instrument_state_json"
    } == {
        key: value for key, value in compact.items() if key != "instrument_state_json"
    }


def test_supervisor_selection_preserves_plan_shard_and_relation_metadata() -> None:
    plan = {
        "schema_version": "subscription_plan.v1",
        "polymarket": {"assets_ids": ["token-1", "token-2"]},
        "kalshi": {"market_tickers": ["KXTEST"]},
        "polymarket_assets": [
            {
                "asset_id": "token-1",
                "shard_id": "pm-a",
                "match_ids": ["match-1"],
            },
            {
                "asset_id": "token-2",
                "shard_id": "pm-b",
                "match_ids": ["match-2"],
            },
        ],
        "kalshi_market_tickers": [
            {
                "market_ticker": "KXTEST",
                "shard_id": "kx-a",
                "match_ids": ["match-1", "match-2"],
            }
        ],
    }

    supervisor = LiveFeedSupervisor.from_subscription_plan_selection(
        plan,
        venue="polymarket",
        instruments=["token-2"],
    )

    assert supervisor.preflight_report.ok is True
    assert supervisor.shard_metadata() == [
        {
            "venue": "polymarket",
            "shard_id": "pm-b",
            "instrument_count": 1,
            "relation_count": 1,
            "subscribed_instruments": ["token-2"],
            "relation_ids": ["match-2"],
        }
    ]
    assert supervisor.shard_for_instrument("polymarket", "token-2").shard_id == "pm-b"


def test_shard_health_tracks_stale_feeds_and_reconnect_invalidation() -> None:
    shard = FeedShardHealth(
        venue="polymarket",
        shard_id="pm-0",
        subscribed_instruments=("token-1", "token-2"),
    )

    shard.mark_connected(now_monotonic_ns=1_000_000_000)
    shard.record_book(valid_state=True, now_monotonic_ns=1_100_000_000)
    shard.invalidate_if_stale(
        now_monotonic_ns=1_250_000_000,
        max_message_age_ms=500,
        max_valid_book_age_ms=500,
    )
    fresh_row = shard.as_row(now_monotonic_ns=1_250_000_000)

    assert fresh_row["connection_state"] == "connected"
    assert fresh_row["last_message_age_ms"] == 150
    assert fresh_row["last_valid_book_age_ms"] == 150
    assert fresh_row["valid_book_count"] == 1

    shard.invalidate_if_stale(
        now_monotonic_ns=2_000_000_000,
        max_message_age_ms=500,
        max_valid_book_age_ms=500,
    )
    stale_row = shard.as_row(now_monotonic_ns=2_000_000_000)

    assert stale_row["connection_state"] == "stale"
    assert stale_row["valid_book_count"] == 0
    assert stale_row["invalid_book_count"] == 2
    assert "stale_messages" in stale_row["quality_flags"]
    assert "stale_books" in stale_row["quality_flags"]

    shard.mark_reconnect(now_monotonic_ns=2_100_000_000)

    assert shard.connection_state == "reconnecting"
    assert shard.reconnect_count == 1
    assert shard.valid_book_count == 0
    assert shard.invalid_book_count == 2


def test_shard_health_allows_initial_book_freshness_window() -> None:
    shard = FeedShardHealth(
        venue="kalshi",
        shard_id="kalshi-0",
        subscribed_instruments=("KXTEST",),
    )

    shard.mark_connected(now_monotonic_ns=1_000_000_000)
    shard.invalidate_if_stale(
        now_monotonic_ns=1_010_000_000,
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )

    within_grace = shard.as_row(now_monotonic_ns=1_010_000_000)
    assert "stale_books" not in within_grace["quality_flags"]
    assert within_grace["valid_book_count"] == 0

    shard.invalidate_if_stale(
        now_monotonic_ns=1_030_000_000,
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )
    assert (
        "stale_books" in shard.as_row(now_monotonic_ns=1_030_000_000)["quality_flags"]
    )


def test_shard_health_clears_reconnect_after_valid_book() -> None:
    shard = FeedShardHealth(
        venue="kalshi",
        shard_id="kalshi-0",
        subscribed_instruments=("KXTEST",),
    )

    shard.mark_connected(now_monotonic_ns=1_000_000_000)
    shard.mark_reconnect(now_monotonic_ns=1_100_000_000)
    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_200_000_000,
        instrument="KXTEST",
    )

    row = shard.as_row(now_monotonic_ns=1_250_000_000)
    assert row["connection_state"] == "connected"
    assert row["valid_book_count"] == 1
    assert "reconnect" not in row["quality_flags"]


def test_shard_health_reconnect_requires_every_instrument_to_recover() -> None:
    shard = FeedShardHealth(
        venue="polymarket",
        shard_id="pm-0",
        subscribed_instruments=("token-1", "token-2"),
    )
    shard.mark_connected(now_monotonic_ns=1_000_000_000)
    for index, instrument in enumerate(shard.subscribed_instruments, start=1):
        shard.record_book(
            valid_state=True,
            now_monotonic_ns=1_000_000_000 + index * 100_000_000,
            instrument=instrument,
        )

    shard.mark_reconnect(now_monotonic_ns=1_300_000_000)
    shard.record_message(
        now_monotonic_ns=1_350_000_000,
        instrument="token-1",
    )
    reconnecting = shard.as_row(now_monotonic_ns=1_350_000_000)

    assert reconnecting["valid_instrument_count"] == 0
    assert reconnecting["invalid_instrument_count"] == 2
    assert reconnecting["invalid_book_count"] == 2
    assert "reconnect" in reconnecting["quality_flags"]

    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_400_000_000,
        instrument="token-1",
    )
    partial = shard.as_row(now_monotonic_ns=1_400_000_000)

    assert partial["valid_instrument_count"] == 1
    assert partial["invalid_instrument_count"] == 1
    assert partial["invalid_book_count"] == 1
    assert "reconnect" in partial["quality_flags"]

    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_500_000_000,
        instrument="token-2",
    )
    recovered = shard.as_row(now_monotonic_ns=1_500_000_000)

    assert recovered["valid_instrument_count"] == 2
    assert recovered["invalid_instrument_count"] == 0
    assert recovered["invalid_book_count"] == 0
    assert "reconnect" not in recovered["quality_flags"]


def test_shard_health_reconnect_does_not_wait_for_never_observed_instrument() -> None:
    shard = FeedShardHealth(
        venue="polymarket",
        shard_id="pm-0",
        subscribed_instruments=("token-active", "token-never-observed"),
    )
    shard.mark_connected(now_monotonic_ns=1_000_000_000)
    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_100_000_000,
        instrument="token-active",
    )

    shard.mark_reconnect(now_monotonic_ns=1_200_000_000)
    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_300_000_000,
        instrument="token-active",
    )
    recovered = shard.as_row(now_monotonic_ns=1_300_000_000)

    assert recovered["valid_instrument_count"] == 1
    assert recovered["missing_instrument_count"] == 1
    assert "reconnect" not in recovered["quality_flags"]


def test_supervisor_recovery_actions_emit_for_each_stale_shard() -> None:
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-active",
                subscribed_instruments=("token-1",),
            ),
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-idle",
                subscribed_instruments=("token-2",),
            ),
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )
    active = supervisor.shard("polymarket", "pm-active")
    idle = supervisor.shard("polymarket", "pm-idle")
    active.mark_connected(now_monotonic_ns=1_000_000_000)
    active.record_book(
        valid_state=True,
        now_monotonic_ns=1_045_000_000,
        instrument="token-1",
    )
    idle.mark_connected(now_monotonic_ns=1_000_000_000)

    actions = supervisor.recovery_actions(
        now_monotonic_ns=1_050_000_000,
        venue="polymarket",
    )

    assert actions == [
        FeedRecoveryAction(
            action="reconnect_socket",
            venue="polymarket",
            shard_id="pm-idle",
            reasons=("connection_stale", "stale_messages", "stale_books"),
        ),
    ]

    actions = supervisor.recovery_actions(
        now_monotonic_ns=1_100_000_000,
        venue="polymarket",
    )

    assert actions == [
        FeedRecoveryAction(
            action="reconnect_socket",
            venue="polymarket",
            shard_id="pm-active",
            reasons=("connection_stale", "stale_messages", "stale_books"),
        ),
        FeedRecoveryAction(
            action="reconnect_socket",
            venue="polymarket",
            shard_id="pm-idle",
            reasons=("connection_stale", "stale_messages", "stale_books"),
        ),
    ]


def test_supervisor_staleness_heap_examines_only_due_instruments() -> None:
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-0",
                subscribed_instruments=("active", "idle-1", "idle-2"),
            )
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )
    shard = supervisor.shard("polymarket", "pm-0")
    shard.mark_connected(now_monotonic_ns=1_000_000_000)
    for instrument in shard.subscribed_instruments:
        shard.record_book(
            valid_state=True,
            now_monotonic_ns=1_000_000_000,
            instrument=instrument,
        )
    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_015_000_000,
        instrument="active",
    )

    changed = supervisor.invalidate_stale(now_monotonic_ns=1_025_000_000)

    assert changed == (("polymarket", "pm-0"),)
    assert supervisor.last_staleness_instruments_examined == 2
    assert shard.instrument_health["active"].valid_state is True
    assert shard.instrument_health["idle-1"].valid_state is False
    assert shard.instrument_health["idle-2"].valid_state is False

    assert supervisor.invalidate_stale(now_monotonic_ns=1_026_000_000) == ()
    assert supervisor.last_staleness_instruments_examined == 0


def test_filtered_recovery_refreshes_staleness_for_every_venue() -> None:
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-0",
                subscribed_instruments=("token-1",),
            ),
            FeedShardHealth(
                venue="kalshi",
                shard_id="kx-0",
                subscribed_instruments=("KXTEST",),
            ),
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )
    for shard in supervisor.shards.values():
        shard.mark_connected(now_monotonic_ns=1_000_000_000)

    actions = supervisor.recovery_actions(
        now_monotonic_ns=1_050_000_000,
        venue="polymarket",
    )

    assert [action.venue for action in actions] == ["polymarket"]
    assert supervisor.shard("polymarket", "pm-0").connection_state == "stale"
    assert supervisor.shard("kalshi", "kx-0").connection_state == "stale"


def test_supervisor_recovery_actions_emit_for_book_hash_mismatch() -> None:
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-0",
                subscribed_instruments=("token-1",),
            ),
        ],
        max_message_age_ms=10_000,
        max_valid_book_age_ms=10_000,
    )
    shard = supervisor.shard("polymarket", "pm-0")
    shard.mark_connected(now_monotonic_ns=1_000_000_000)
    shard.record_book(
        valid_state=False,
        now_monotonic_ns=1_100_000_000,
        instrument="token-1",
        quality_flags=("hash_mismatch",),
    )

    actions = supervisor.recovery_actions(
        now_monotonic_ns=1_200_000_000,
        venue="polymarket",
    )

    assert actions == [
        FeedRecoveryAction(
            action="reconnect_socket",
            venue="polymarket",
            shard_id="pm-0",
            reasons=("hash_mismatch", "book_integrity"),
        ),
    ]

    shard.mark_reconnect(now_monotonic_ns=1_250_000_000)
    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_300_000_000,
        instrument="token-1",
    )

    assert (
        supervisor.recovery_actions(
            now_monotonic_ns=1_350_000_000,
            venue="polymarket",
        )
        == []
    )


def test_shard_health_reports_per_instrument_coverage() -> None:
    shard = FeedShardHealth(
        venue="polymarket",
        shard_id="pm-0",
        subscribed_instruments=("token-1", "token-2"),
    )

    shard.mark_connected(now_monotonic_ns=1_000_000_000)
    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_100_000_000,
        instrument="token-1",
    )
    one_sided = shard.as_row(now_monotonic_ns=1_200_000_000)

    assert one_sided["valid_instrument_count"] == 1
    assert one_sided["missing_instrument_count"] == 1
    assert "missing_instrument_books" in one_sided["quality_flags"]
    assert "token-1" in one_sided["instrument_state_json"]

    shard.record_book(
        valid_state=False,
        now_monotonic_ns=1_300_000_000,
        instrument="token-2",
    )
    invalid = shard.as_row(now_monotonic_ns=1_400_000_000)

    assert invalid["valid_instrument_count"] == 1
    assert invalid["missing_instrument_count"] == 0
    assert invalid["invalid_instrument_count"] == 1
    assert "invalid_instrument_books" in invalid["quality_flags"]


def test_kalshi_sequence_gap_requires_resync_before_valid_state() -> None:
    shard = FeedShardHealth(
        venue="kalshi",
        shard_id="kx-0",
        subscribed_instruments=("KXTEST",),
    )

    shard.record_book(valid_state=True, now_monotonic_ns=1_000_000_000)
    shard.record_sequence_gap()

    assert shard.sequence_gap_count == 1
    assert shard.valid_book_count == 0
    assert shard.invalid_book_count == 1
    assert "sequence_gap" in shard.quality_flags

    shard.record_resync(now_monotonic_ns=1_100_000_000)
    shard.record_book(valid_state=True, now_monotonic_ns=1_200_000_000)

    assert shard.resync_count == 1
    assert "sequence_gap" not in shard.quality_flags
    assert shard.valid_book_count == 1


def test_multi_instrument_sequence_gap_stays_blocked_until_all_resync() -> None:
    shard = FeedShardHealth(
        venue="kalshi",
        shard_id="kx-0",
        subscribed_instruments=("KXONE", "KXTWO"),
    )
    for instrument in shard.subscribed_instruments:
        shard.record_book(
            valid_state=True,
            now_monotonic_ns=1_000_000_000,
            instrument=instrument,
        )

    shard.record_sequence_gap(instruments=shard.subscribed_instruments)
    shard.record_resync(
        now_monotonic_ns=1_100_000_000,
        instrument="KXONE",
    )
    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_200_000_000,
        instrument="KXONE",
    )

    assert "sequence_gap" in shard.quality_flags
    assert shard.instrument_health["KXTWO"].valid_state is False

    shard.record_resync(
        now_monotonic_ns=1_300_000_000,
        instrument="KXTWO",
    )
    shard.record_book(
        valid_state=True,
        now_monotonic_ns=1_400_000_000,
        instrument="KXTWO",
    )

    assert shard.sequence_gap_count == 1
    assert "sequence_gap" not in shard.quality_flags
    assert all(state.valid_state for state in shard.instrument_health.values())
