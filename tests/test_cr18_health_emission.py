from __future__ import annotations

import json
from copy import deepcopy

import pytest

from pmkt.streaming.causal_health import (
    INCOMPLETE_DETAIL_FLAG,
    MALFORMED_DETAIL_FLAG,
    MISSING_DETAIL_FLAG,
    select_causal_health_by_shard,
)
from pmkt.streaming.health_emission import (
    SlimHealthEmitter,
    feed_health_fingerprint,
)


def _row(
    *,
    observed: str = "2026-07-19T10:00:00Z",
    sequence: int = 1,
    valid: bool = True,
) -> dict[str, object]:
    detail = [
        {
            "instrument": "token-1",
            "valid_state": valid,
            "last_message_age_ms": 2,
            "last_valid_book_age_ms": 3,
            "valid_book_count": 1 if valid else 0,
            "invalid_book_count": 0 if valid else 1,
            "quality_flags": [] if valid else ["invalid_book"],
        }
    ]
    return {
        "schema_version": "feed_health.v1",
        "observed_at_utc": observed,
        "local_sequence": sequence,
        "venue": "polymarket",
        "shard_id": "polymarket-0",
        "connection_state": "connected",
        "instrument_count": 1,
        "relation_count": 0,
        "reconnect_count": 0,
        "sequence_gap_count": 0,
        "resync_count": 0,
        "error_count": 0,
        "last_message_age_ms": 2,
        "last_valid_book_age_ms": 3,
        "valid_book_count": 1 if valid else 0,
        "invalid_book_count": 0 if valid else 1,
        "valid_instrument_count": 1 if valid else 0,
        "invalid_instrument_count": 0 if valid else 1,
        "stale_instrument_count": 0,
        "missing_instrument_count": 0,
        "instrument_state_json": json.dumps(detail),
        "quality_flags": [] if valid else ["invalid_book"],
    }


def test_health_fingerprint_excludes_monotonic_ages_in_aggregate_and_detail() -> None:
    left = _row()
    right = deepcopy(left)
    right["observed_at_utc"] = "2026-07-19T10:00:01Z"
    right["local_sequence"] = 2
    right["last_message_age_ms"] = 999
    right["last_valid_book_age_ms"] = 998
    detail = json.loads(str(right["instrument_state_json"]))
    detail[0]["last_message_age_ms"] = 997
    detail[0]["last_valid_book_age_ms"] = 996
    right["instrument_state_json"] = json.dumps(detail)
    assert feed_health_fingerprint(left) == feed_health_fingerprint(right)


def test_slim_health_emitter_reports_due_keys_and_evaluation_counts() -> None:
    emitter = SlimHealthEmitter(interval_seconds=10.0)
    keys = (("polymarket", "pm-b"), ("polymarket", "pm-a"))

    assert emitter.due_shard_keys(keys, now_monotonic_ns=0) == (
        ("polymarket", "pm-a"),
        ("polymarket", "pm-b"),
    )
    emitter.observe([_row()], now_monotonic_ns=0)

    assert emitter.due_shard_keys(
        (("polymarket", "polymarket-0"),),
        now_monotonic_ns=9_999_999_999,
    ) == ()
    assert emitter.due_shard_keys(
        (("polymarket", "polymarket-0"),),
        now_monotonic_ns=10_000_000_000,
    ) == (("polymarket", "polymarket-0"),)
    metrics = emitter.manifest_metrics()
    assert metrics["observe_calls"] == 1
    assert metrics["rows_evaluated"] == 1


def test_blocked_detail_deadline_can_precede_compact_deadline() -> None:
    emitter = SlimHealthEmitter(
        interval_seconds=600.0,
        detail_interval_seconds=300.0,
    )
    key = ("polymarket", "polymarket-0")
    blocked = _row(valid=False)

    startup = emitter.observe([blocked], now_monotonic_ns=0, cause="startup")

    assert startup[0].detail_included
    assert emitter.due_shard_keys(
        (key,),
        now_monotonic_ns=299_999_999_999,
    ) == ()
    assert emitter.due_shard_keys(
        (key,),
        now_monotonic_ns=300_000_000_000,
    ) == (key,)

    periodic = emitter.observe(
        [blocked],
        now_monotonic_ns=300_000_000_000,
    )
    assert len(periodic) == 1
    assert periodic[0].detail_included
    assert periodic[0].reason == "blocked_periodic"


def test_prepared_health_emission_advances_state_only_after_commit() -> None:
    emitter = SlimHealthEmitter(interval_seconds=10.0)
    key = ("polymarket", "polymarket-0")

    abandoned = emitter.prepare([_row()], now_monotonic_ns=0, cause="startup")

    assert len(abandoned.emissions) == 1
    assert emitter.due_shard_keys((key,), now_monotonic_ns=1) == (key,)
    assert emitter.manifest_metrics()["observe_calls"] == 0

    retried = emitter.prepare([_row()], now_monotonic_ns=1, cause="startup")
    assert len(retried.emissions) == 1
    emitter.commit(retried)

    assert emitter.due_shard_keys((key,), now_monotonic_ns=2) == ()
    metrics = emitter.manifest_metrics()
    assert metrics["observe_calls"] == 1
    assert metrics["rows_evaluated"] == 1
    with pytest.raises(ValueError, match="already committed"):
        emitter.commit(retried)
    with pytest.raises(RuntimeError, match="stale"):
        emitter.commit(abandoned)


def test_health_fingerprint_ignores_cumulative_book_counters() -> None:
    left = _row()
    right = deepcopy(left)
    right["observed_at_utc"] = "2026-07-19T10:00:01Z"
    right["local_sequence"] = 2
    right["valid_book_count"] = 99
    right["invalid_book_count"] = 88
    detail = json.loads(str(right["instrument_state_json"]))
    detail[0]["valid_book_count"] = 99
    detail[0]["invalid_book_count"] = 88
    right["instrument_state_json"] = json.dumps(detail)

    assert feed_health_fingerprint(left) == feed_health_fingerprint(right)

    emitter = SlimHealthEmitter(interval_seconds=10)
    initial = emitter.observe([left], now_monotonic_ns=0, cause="startup")
    assert initial[0].detail_included
    assert emitter.observe([right], now_monotonic_ns=1_000_000_000) == ()
    periodic = emitter.observe([right], now_monotonic_ns=10_000_000_000)
    assert len(periodic) == 1
    assert periodic[0].reason == "periodic"
    assert periodic[0].row["valid_book_count"] == 99
    assert periodic[0].row["invalid_book_count"] == 88
    assert periodic[0].row["instrument_state_json"] == ""


def test_slim_emitter_uses_transition_periodic_and_terminal_cadence() -> None:
    emitter = SlimHealthEmitter(interval_seconds=10)
    initial = emitter.observe([_row()], now_monotonic_ns=0, cause="startup")
    assert len(initial) == 1 and initial[0].detail_included
    aged = _row(observed="2026-07-19T10:00:05Z", sequence=2)
    aged["last_message_age_ms"] = 5_000
    assert emitter.observe([aged], now_monotonic_ns=5_000_000_000) == ()
    periodic = emitter.observe([aged], now_monotonic_ns=10_000_000_000)
    assert len(periodic) == 1
    assert periodic[0].reason == "periodic"
    assert periodic[0].row["instrument_state_json"] == ""
    terminal = emitter.observe(
        [aged], now_monotonic_ns=11_000_000_000, cause="terminal"
    )
    assert terminal[0].detail_included


def test_stale_transition_on_idle_emits_one_full_blob_per_shard() -> None:
    emitter = SlimHealthEmitter()
    emitter.observe([_row()], now_monotonic_ns=0)
    stale = _row(observed="2026-07-19T10:00:06Z", sequence=2, valid=False)
    stale["connection_state"] = "stale"
    stale["quality_flags"] = ["stale_books", "stale_messages"]
    emissions = emitter.observe(
        [stale, deepcopy(stale)], now_monotonic_ns=6_000_000_000
    )
    assert len(emissions) == 1
    assert emissions[0].detail_included
    assert emissions[0].reason == "error"


def test_blocked_transition_storm_refreshes_detail_only_on_bounded_cadence() -> None:
    emitter = SlimHealthEmitter(interval_seconds=10, detail_interval_seconds=300)
    emitter.observe([_row()], now_monotonic_ns=0, cause="startup")
    blocked = _row(observed="2026-07-19T10:00:01Z", sequence=2, valid=False)
    blocked["connection_state"] = "stale"
    blocked["quality_flags"] = ["stale_books", "stale_messages"]

    first = emitter.observe([blocked], now_monotonic_ns=1_000_000_000)
    assert first[0].detail_included
    assert first[0].reason == "error"

    for sequence in range(3, 53):
        changed = deepcopy(blocked)
        changed["local_sequence"] = sequence
        changed["relation_count"] = sequence
        emissions = emitter.observe(
            [changed],
            now_monotonic_ns=sequence * 1_000_000_000,
        )
        assert len(emissions) == 1
        assert emissions[0].detail_included is False
        assert emissions[0].row["instrument_state_json"] == ""

    refresh = emitter.observe(
        [blocked],
        now_monotonic_ns=301_000_000_000,
    )
    assert refresh[0].detail_included
    assert refresh[0].reason == "blocked_periodic"

    recovered = _row(observed="2026-07-19T10:05:02Z", sequence=54, valid=True)
    recovery = emitter.observe([recovered], now_monotonic_ns=302_000_000_000)
    assert recovery[0].detail_included
    assert recovery[0].reason == "recovery"


def test_detail_provider_hydrates_only_rows_that_require_detail() -> None:
    emitter = SlimHealthEmitter(interval_seconds=10, detail_interval_seconds=300)
    compact = _row()
    compact["instrument_state_json"] = ""
    detail = _row()
    calls: list[tuple[str, str]] = []

    def provide(key: tuple[str, str]) -> dict[str, object]:
        calls.append(key)
        return detail

    startup = emitter.observe(
        [compact],
        now_monotonic_ns=0,
        cause="startup",
        detail_provider=provide,
    )
    assert startup[0].row["instrument_state_json"] == detail["instrument_state_json"]
    assert calls == [("polymarket", "polymarket-0")]

    periodic = emitter.observe(
        [compact],
        now_monotonic_ns=10_000_000_000,
        detail_provider=provide,
    )
    assert periodic[0].row["instrument_state_json"] == ""
    assert calls == [("polymarket", "polymarket-0")]


def test_causal_selection_uses_latest_aggregate_and_prior_detail() -> None:
    detail = _row()
    aggregate = _row(observed="2026-07-19T10:00:10Z", sequence=2)
    aggregate["instrument_state_json"] = ""
    aggregate["last_message_age_ms"] = 9_000
    selected = select_causal_health_by_shard([aggregate, detail])["polymarket-0"]
    assert selected["last_message_age_ms"] == 9_000
    assert selected["instrument_state_json"] == detail["instrument_state_json"]


def test_missing_or_malformed_causal_detail_fails_closed() -> None:
    missing = _row()
    missing["instrument_state_json"] = ""
    missing["valid_instrument_count"] = 0
    missing["invalid_instrument_count"] = 1
    selected_missing = select_causal_health_by_shard([missing])["polymarket-0"]
    assert MISSING_DETAIL_FLAG in str(selected_missing["quality_flags"])
    assert selected_missing["valid_instrument_count"] == 0

    valid_old = _row()
    malformed_new = _row(observed="2026-07-19T10:00:01Z", sequence=2)
    malformed_new["instrument_state_json"] = "{bad-json"
    aggregate = _row(observed="2026-07-19T10:00:02Z", sequence=3)
    aggregate["instrument_state_json"] = ""
    selected_bad = select_causal_health_by_shard([valid_old, malformed_new, aggregate])[
        "polymarket-0"
    ]
    assert MALFORMED_DETAIL_FLAG in str(selected_bad["quality_flags"])
    assert selected_bad["instrument_state_json"] == ""


def test_recovery_detail_replaces_obsolete_invalid_detail() -> None:
    invalid = _row(valid=False)
    aggregate = _row(observed="2026-07-19T10:00:01Z", sequence=2, valid=False)
    aggregate["instrument_state_json"] = ""
    recovered = _row(observed="2026-07-19T10:00:02Z", sequence=3, valid=True)
    final = _row(observed="2026-07-19T10:00:03Z", sequence=4, valid=True)
    final["instrument_state_json"] = ""
    selected = select_causal_health_by_shard([invalid, aggregate, recovered, final])[
        "polymarket-0"
    ]
    state = json.loads(str(selected["instrument_state_json"]))[0]
    assert state["valid_state"] is True


def test_healthy_aggregate_without_causal_detail_fails_closed() -> None:
    row = _row()
    row["instrument_state_json"] = ""
    selected = select_causal_health_by_shard([row])["polymarket-0"]
    assert MISSING_DETAIL_FLAG in str(selected["quality_flags"])
    assert selected["valid_instrument_count"] == 0


def test_incomplete_detail_fails_closed_and_prior_valid_detail_remains_causal() -> None:
    incomplete = _row()
    incomplete["instrument_state_json"] = json.dumps([{"instrument": "token-1"}])
    selected_incomplete = select_causal_health_by_shard([incomplete])["polymarket-0"]
    assert INCOMPLETE_DETAIL_FLAG in str(selected_incomplete["quality_flags"])

    detailed = _row()
    aggregate = _row(observed="2026-07-19T10:00:01Z", sequence=2)
    aggregate["instrument_state_json"] = ""
    aggregate["reconnect_count"] = 1
    selected_stale = select_causal_health_by_shard([detailed, aggregate])[
        "polymarket-0"
    ]
    assert selected_stale["instrument_state_json"] == detailed["instrument_state_json"]
    assert selected_stale["reconnect_count"] == 1


def test_transition_storm_keeps_ordinary_rows_compact_and_reports_bytes() -> None:
    emitter = SlimHealthEmitter(interval_seconds=60)
    startup = emitter.observe([_row()], now_monotonic_ns=0, cause="startup")
    assert startup[0].detail_included

    for sequence in range(2, 52):
        row = _row(
            observed=f"2026-07-19T10:00:{sequence % 60:02d}Z",
            sequence=sequence,
        )
        row["relation_count"] = sequence
        emissions = emitter.observe([row], now_monotonic_ns=sequence * 1_000_000)
        assert len(emissions) == 1
        assert emissions[0].reason == "transition"
        assert emissions[0].detail_included is False
        assert emissions[0].row["instrument_state_json"] == ""

    metrics = emitter.manifest_metrics()
    assert metrics["policy_version"] == "feed-health-emission.v3"
    assert metrics["detail_interval_seconds"] == 300.0
    assert metrics["by_reason"]["startup"]["detail_rows"] == 1
    assert metrics["by_reason"]["transition"]["rows"] == 50
    assert metrics["by_reason"]["transition"]["detail_rows"] == 0
