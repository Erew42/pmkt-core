"""Acceptance tests for the CR-18.9 capture-completeness and transport fixes.

Unlike the temporary characterization probes, these assert the CORRECTED
behaviour and are expected to stay green.
"""

from __future__ import annotations

import inspect
import os

import pytest

from pmkt.exchanges.kalshi.order_book_stream import stream_kalshi_order_book_data
from pmkt.exchanges.polymarket.order_book_stream import stream_order_book_data
from pmkt.exchanges.ws_transport import (
    WS_MAX_QUEUE_FRAMES,
    WS_MAX_SIZE_BYTES,
    WebSocketTransportSettings,
    is_transport_teardown_race,
)
from pmkt.streaming.supervisor import (
    FeedShardHealth,
    LiveFeedSupervisor,
    _quality_flags,
)
from pmkt.streaming.capture_completeness import (
    COMPLETENESS_POLICY_VERSION,
    CaptureCompletenessError,
    CaptureIntent,
    CaptureTerminationReason,
    assert_capture_observed_market_data,
    evaluate_capture_completeness,
)


# --------------------------------------------------------------------------
# Completeness policy
# --------------------------------------------------------------------------


def _evaluate(**overrides):
    kwargs = dict(
        venue="polymarket",
        instruments_with_snapshots=74,
        event_count=1000,
        reconnect_count=1,
        duration_seconds_actual=600.0,
        duration_seconds_requested=600.0,
        instruments_requested=74,
    )
    kwargs.update(overrides)
    return evaluate_capture_completeness(**kwargs)


def test_established_capture_passes():
    assert _evaluate().ok is True


def test_zero_snapshots_fails_closed():
    report = _evaluate(instruments_with_snapshots=0, event_count=0, reconnect_count=152)
    assert report.ok is False
    assert any(
        "never" in reason or "no instrument" in reason for reason in report.reasons
    )


def test_quiet_but_established_market_is_not_a_failure():
    """Zero events alone must not fail: a quiet market legitimately has none."""
    assert _evaluate(event_count=0).ok is True


def test_reconnect_storm_fails_closed():
    report = _evaluate(reconnect_count=120, duration_seconds_actual=600.0)
    assert report.ok is False
    assert any("reconnected" in reason for reason in report.reasons)


def test_explicit_smoke_runs_are_exempt_but_not_acceptance_eligible():
    report = _evaluate(
        instruments_with_snapshots=0,
        event_count=0,
        duration_seconds_actual=10.0,
        duration_seconds_requested=600.0,
        capture_intent=CaptureIntent.SMOKE,
        terminal_reason=CaptureTerminationReason.MAX_MESSAGES_REACHED,
    )
    assert report.ok is True
    assert report.evaluated is False
    assert report.acceptance_eligible is False


def test_collectors_default_to_operational_intent() -> None:
    for collector in (
        stream_order_book_data,
        stream_kalshi_order_book_data,
    ):
        default = inspect.signature(collector).parameters["capture_intent"].default
        assert default is CaptureIntent.OPERATIONAL


def test_short_operational_run_fails_closed():
    report = _evaluate(
        instruments_with_snapshots=0,
        event_count=0,
        duration_seconds_actual=10.0,
        terminal_reason=CaptureTerminationReason.ITERATOR_EXHAUSTED,
    )
    assert report.ok is False
    assert report.evaluated is True
    assert any("iterator_exhausted" in reason for reason in report.reasons)


def test_invalid_snapshots_do_not_establish_coverage():
    report = _evaluate(
        instruments_with_snapshots=0,
        instruments_with_invalid_snapshots=74,
    )
    assert report.ok is False
    assert report.as_manifest_mapping()["instruments_with_invalid_snapshots"] == 74


def test_actual_duration_controls_the_reconnect_rate():
    """R2: the rate must use elapsed time, not the requested duration.

    12 reconnects is fine across 600 s (1.2/min) and a failure across 60 s
    (12/min). Only an actual-duration implementation distinguishes them.
    """
    assert _evaluate(reconnect_count=12, duration_seconds_actual=600.0).ok is True
    assert _evaluate(reconnect_count=12, duration_seconds_actual=60.0).ok is False


def test_report_carries_raw_measures_and_policy_version():
    """R3: persist what was measured and which rule read it, not just a label."""
    mapping = _evaluate(
        instruments_with_snapshots=446, instruments_requested=600
    ).as_manifest_mapping()
    assert mapping["policy_version"] == COMPLETENESS_POLICY_VERSION
    # requested != eligible: 600 requested, 446 established. The policy records
    # both rather than inferring a coverage percentage.
    assert mapping["instruments_requested"] == 600
    assert mapping["instruments_with_snapshots"] == 446
    assert mapping["capture_intent"] == "operational"
    assert mapping["terminal_reason"] == "deadline_reached"
    assert mapping["ok"] is True
    # Profile-v1/no-sidecar captures are never acceptance-eligible.
    assert mapping["acceptance_eligible"] is False
    assert "reconnects_per_minute" in mapping


def test_acceptance_boolean_cannot_replace_calibrated_profile_evidence():
    report = _evaluate(acceptance_evidence_eligible=True)
    assert report.ok is True
    assert report.evaluated is True
    assert report.acceptance_eligible is False


def test_assert_wrapper_raises_and_returns():
    with pytest.raises(CaptureCompletenessError):
        assert_capture_observed_market_data(
            venue="polymarket",
            instruments_with_snapshots=0,
            event_count=0,
            reconnect_count=10,
            duration_seconds_actual=600.0,
        )
    report = assert_capture_observed_market_data(
        venue="polymarket",
        instruments_with_snapshots=1,
        event_count=1,
        reconnect_count=0,
        duration_seconds_actual=600.0,
    )
    assert report.ok is True


def test_completeness_is_evaluated_before_terminal_emission():
    """R1: the check must precede the successful terminal control.

    Guarded structurally: the terminal emission is a forced barrier, so if the
    assertion moved back after it, a committed tape could claim the run
    completed while no success manifest exists.
    """
    import pmkt.exchanges.polymarket.order_book_stream as pm

    src = open(pm.__file__, encoding="utf-8").read()
    check = src.index("session.finalize_capture_completeness()")
    terminal = src.index('reason="completed"')
    assert check < terminal, "completeness must be decided before terminal emission"


# --------------------------------------------------------------------------
# Transport race classification (N1)
# --------------------------------------------------------------------------


def test_transport_race_requires_transport_origin():
    """An unrelated application error of the same shape must propagate."""
    try:
        None.pause_reading()  # type: ignore[attr-defined]
    except AttributeError as exc:
        assert is_transport_teardown_race(exc) is False


def test_transport_race_detected_from_transport_stack():
    ns = {"__name__": "asyncio.sslproto"}
    code = compile("None.resume_reading()", os.path.join("x", "sslproto.py"), "exec")
    try:
        exec(code, ns)
    except AttributeError as exc:
        assert is_transport_teardown_race(exc) is True


def test_unrelated_attribute_error_is_not_a_race():
    try:
        None.something_else()  # type: ignore[attr-defined]
    except AttributeError as exc:
        assert is_transport_teardown_race(exc) is False


def test_non_none_receiver_is_not_a_race():
    try:
        "x".resume_reading()  # type: ignore[attr-defined]
    except AttributeError as exc:
        assert is_transport_teardown_race(exc) is False


# --------------------------------------------------------------------------
# Flag summary (R4)
# --------------------------------------------------------------------------


def test_manifest_flag_summary_counts_individual_tokens():
    shard = FeedShardHealth(
        venue="polymarket", shard_id="pm-0", subscribed_instruments=("A",)
    )
    shard.record_book(
        valid_state=False,
        now_monotonic_ns=1,
        instrument="A",
        quality_flags=["empty_bid", "crossed_book"],
    )
    counts = LiveFeedSupervisor(shards=[shard]).feed_health_summary(now_monotonic_ns=2)[
        "quality_flag_counts"
    ]
    assert counts["empty_bid"] == 1
    assert counts["crossed_book"] == 1
    # the pre-fix defect produced a single key holding the list's repr
    assert not any(key.startswith("[") for key in counts)


@pytest.mark.parametrize(
    "value,expected",
    [
        (["a_flag", "b_flag"], {"a_flag", "b_flag"}),
        ("a_flag;b_flag", {"a_flag", "b_flag"}),  # legacy artifact
        (None, set()),
        ([], set()),
    ],
)
def test_flag_normalizer_handles_every_shape(value, expected):
    assert _quality_flags(value) == expected


def test_flag_normalizer_never_explodes_a_string_characterwise():
    assert _quality_flags("empty_ask") == {"empty_ask"}


# --------------------------------------------------------------------------
# WebSocket transport settings
# --------------------------------------------------------------------------


def test_websocket_transport_defaults_are_bounded():
    settings = WebSocketTransportSettings()
    assert settings.max_size_bytes == WS_MAX_SIZE_BYTES == 16 * 1024 * 1024
    assert settings.max_queue_frames == WS_MAX_QUEUE_FRAMES == 64
    assert settings.as_connect_kwargs() == {
        "max_size": 16 * 1024 * 1024,
        "max_queue": 64,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_size_bytes", 0),
        ("max_size_bytes", -1),
        ("max_size_bytes", True),
        ("max_queue_frames", 0),
        ("max_queue_frames", -1),
        ("max_queue_frames", False),
    ],
)
def test_websocket_transport_rejects_nonpositive_or_boolean_values(field, value):
    kwargs = {field: value}
    with pytest.raises(ValueError, match=field):
        WebSocketTransportSettings(**kwargs)


def test_websocket_transport_manifest_preserves_requested_and_effective_values():
    settings = WebSocketTransportSettings(max_size_bytes=32, max_queue_frames=8)
    assert settings.as_manifest_mapping(
        requested_max_size_bytes=32,
        requested_max_queue_frames=8,
    ) == {
        "requested": {"max_size_bytes": 32, "max_queue_frames": 8},
        "effective": {"max_size_bytes": 32, "max_queue_frames": 8},
    }
