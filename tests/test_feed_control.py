from __future__ import annotations

import pytest

from pmkt.streaming.feed_control import (
    FeedControlScheduler,
    control_interval_ns,
    feed_control_manifest,
)


@pytest.mark.parametrize(
    ("max_message_age_ms", "max_book_age_ms", "expected_ns"),
    [
        (5_000, 5_000, 250_000_000),
        (500, 750, 125_000_000),
        (20, 20, 50_000_000),
    ],
)
def test_control_interval_is_clamped_to_safe_bounds(
    max_message_age_ms: int,
    max_book_age_ms: int,
    expected_ns: int,
) -> None:
    assert (
        control_interval_ns(
            max_message_age_ms=max_message_age_ms,
            max_valid_book_age_ms=max_book_age_ms,
        )
        == expected_ns
    )


def test_control_scheduler_uses_deadline_and_never_catches_up() -> None:
    scheduler = FeedControlScheduler.from_thresholds(
        now_monotonic_ns=1_000_000_000,
        max_message_age_ms=5_000,
        max_valid_book_age_ms=5_000,
    )

    assert scheduler.due(now_monotonic_ns=1_249_999_999) is False
    assert scheduler.due(now_monotonic_ns=1_250_000_000) is True
    assert scheduler.wait_timeout(
        now_monotonic_ns=1_100_000_000,
        remaining_seconds=1.0,
    ) == pytest.approx(0.15)

    scheduler.record_tick(
        now_monotonic_ns=2_000_000_000,
        stale_instrument_checks=74,
    )

    assert scheduler.next_tick_ns == 2_250_000_000
    assert scheduler.tick_count == 1
    assert scheduler.late_tick_max_ns == 750_000_000
    assert scheduler.stale_instrument_checks == 74
    assert scheduler.manifest_metrics()["policy_version"] == "feed-control-plane.v1"


def test_disabled_control_manifest_records_suppression_reason() -> None:
    manifest = feed_control_manifest(
        scheduler=None,
        interval_ns=250_000_000,
        suppression_reason="runtime_projection_recorder_attached",
    )

    assert manifest["policy_version"] == "feed-control-plane.v1"
    assert manifest["enabled"] is False
    assert manifest["suppression_reason"] == (
        "runtime_projection_recorder_attached"
    )
    assert manifest["tick_interval_ms"] == 250.0
    assert manifest["ticks"] == 0
