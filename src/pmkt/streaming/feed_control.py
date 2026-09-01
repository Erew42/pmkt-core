from __future__ import annotations

from dataclasses import dataclass
from typing import Any


FEED_CONTROL_POLICY_VERSION = "feed-control-plane.v1"
MIN_CONTROL_INTERVAL_NS = 50_000_000
MAX_CONTROL_INTERVAL_NS = 250_000_000


def control_interval_ns(
    *,
    max_message_age_ms: int,
    max_valid_book_age_ms: int,
) -> int:
    minimum_age_ns = (
        min(int(max_message_age_ms), int(max_valid_book_age_ms)) * 1_000_000
    )
    candidate = minimum_age_ns // 4
    return max(MIN_CONTROL_INTERVAL_NS, min(MAX_CONTROL_INTERVAL_NS, candidate))


@dataclass
class FeedControlScheduler:
    interval_ns: int
    next_tick_ns: int
    tick_count: int = 0
    late_tick_total_ns: int = 0
    late_tick_max_ns: int = 0
    stale_instrument_checks: int = 0
    transition_row_builds: int = 0
    periodic_row_builds: int = 0
    detailed_rows_materialized: int = 0
    recovery_evaluations: int = 0
    recovery_actions: int = 0

    @classmethod
    def from_thresholds(
        cls,
        *,
        now_monotonic_ns: int,
        max_message_age_ms: int,
        max_valid_book_age_ms: int,
    ) -> "FeedControlScheduler":
        interval = control_interval_ns(
            max_message_age_ms=max_message_age_ms,
            max_valid_book_age_ms=max_valid_book_age_ms,
        )
        return cls(interval_ns=interval, next_tick_ns=now_monotonic_ns + interval)

    def due(self, *, now_monotonic_ns: int) -> bool:
        return now_monotonic_ns >= self.next_tick_ns

    def wait_timeout(
        self,
        *,
        now_monotonic_ns: int,
        remaining_seconds: float | None,
    ) -> float:
        until_tick_seconds = max(
            0.001,
            (self.next_tick_ns - now_monotonic_ns) / 1_000_000_000,
        )
        if remaining_seconds is None:
            return until_tick_seconds
        return min(remaining_seconds, until_tick_seconds)

    def record_tick(
        self,
        *,
        now_monotonic_ns: int,
        stale_instrument_checks: int,
    ) -> None:
        lateness = max(0, now_monotonic_ns - self.next_tick_ns)
        self.tick_count += 1
        self.late_tick_total_ns += lateness
        self.late_tick_max_ns = max(self.late_tick_max_ns, lateness)
        self.stale_instrument_checks += int(stale_instrument_checks)
        # Schedule from the observation time so a delayed loop never runs a
        # burst of catch-up ticks.
        self.next_tick_ns = now_monotonic_ns + self.interval_ns

    def record_health_rows(
        self,
        *,
        transition_rows: int = 0,
        periodic_rows: int = 0,
        detailed_rows: int = 0,
    ) -> None:
        self.transition_row_builds += int(transition_rows)
        self.periodic_row_builds += int(periodic_rows)
        self.detailed_rows_materialized += int(detailed_rows)

    def record_recovery(self, *, action_count: int) -> None:
        self.recovery_evaluations += 1
        self.recovery_actions += int(action_count)

    def manifest_metrics(self) -> dict[str, Any]:
        return {
            "policy_version": FEED_CONTROL_POLICY_VERSION,
            "enabled": True,
            "suppression_reason": None,
            "tick_interval_ms": self.interval_ns / 1_000_000,
            "ticks": self.tick_count,
            "late_tick_total_ms": self.late_tick_total_ns / 1_000_000,
            "late_tick_max_ms": self.late_tick_max_ns / 1_000_000,
            "stale_instrument_checks": self.stale_instrument_checks,
            "transition_row_builds": self.transition_row_builds,
            "periodic_row_builds": self.periodic_row_builds,
            "detailed_rows_materialized": self.detailed_rows_materialized,
            "recovery_evaluations": self.recovery_evaluations,
            "recovery_actions": self.recovery_actions,
        }


def feed_control_manifest(
    *,
    scheduler: FeedControlScheduler | None,
    interval_ns: int,
    suppression_reason: str | None,
) -> dict[str, Any]:
    if scheduler is not None:
        return scheduler.manifest_metrics()
    return {
        "policy_version": FEED_CONTROL_POLICY_VERSION,
        "enabled": False,
        "suppression_reason": suppression_reason or "collector_not_started",
        "tick_interval_ms": interval_ns / 1_000_000,
        "ticks": 0,
        "late_tick_total_ms": 0.0,
        "late_tick_max_ms": 0.0,
        "stale_instrument_checks": 0,
        "transition_row_builds": 0,
        "periodic_row_builds": 0,
        "detailed_rows_materialized": 0,
        "recovery_evaluations": 0,
        "recovery_actions": 0,
    }


__all__ = [
    "FEED_CONTROL_POLICY_VERSION",
    "FeedControlScheduler",
    "control_interval_ns",
    "feed_control_manifest",
]
