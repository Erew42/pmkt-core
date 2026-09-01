from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Mapping, Sequence

from pmkt.streaming.supervisor import FeedShardHealth, LiveFeedSupervisor
from pmkt.streaming.capture_clock import CaptureObservationClock
from pmkt.streaming.capture_completeness import (
    CaptureIntent,
    CaptureTerminationReason,
    evaluate_capture_completeness,
)
from pmkt.streaming.collector import RuntimeFeedProjectionRecorder
from pmkt.streaming.feed_control import FeedControlScheduler
from pmkt.streaming.health_emission import PreparedHealthEmissions, SlimHealthEmitter
from pmkt.streaming.instrument_evidence import (
    CAPTURE_INSTRUMENT_EVIDENCE_ROLE,
    DEFAULT_TERMINAL_EVIDENCE_BATCH_ROWS,
    CaptureInstrumentEvidenceTracker,
)
from pmkt.streaming.profile_runtime import ProfileCaptureRuntime
from pmkt.streaming.profiles import DatasetRole, StorageProfileSelection
from pmkt.streaming.tape import canonical_utc
from pmkt.streaming.topbook_emission import TopbookEmissionTracker


@dataclass(slots=True, kw_only=True)
class _CaptureSessionBookkeeping:
    venue: str
    instrument_ids: list[str]
    supervisor: LiveFeedSupervisor
    health_shards: list[FeedShardHealth]
    storage_profile: StorageProfileSelection | None
    profile_runtime: ProfileCaptureRuntime | None
    topbook_tracker: TopbookEmissionTracker | None
    health_emitter: SlimHealthEmitter | None
    runtime_projection_recorder: RuntimeFeedProjectionRecorder | None
    instrument_evidence_tracker: CaptureInstrumentEvidenceTracker | None
    started_monotonic: float
    resolved_capture_intent: CaptureIntent
    duration_s: float
    sequence: int = 0
    event_count: int = 0
    health_observation_count: int = 0
    reconnect_count: int = 0
    instruments_with_snapshots: set[str] = field(default_factory=set)
    instruments_with_invalid_snapshots: set[str] = field(default_factory=set)
    latest_topbooks: dict[str, Mapping[str, Any]] = field(default_factory=dict)
    last_topbook_boundary_at: datetime | None = None
    capture_clock: CaptureObservationClock = field(
        default_factory=CaptureObservationClock
    )
    feed_control_scheduler: FeedControlScheduler | None = None
    pending_health_shard_keys: set[tuple[str, str]] = field(default_factory=set)
    instrument_evidence_staged: bool = False
    terminal_reason: CaptureTerminationReason | None = None
    completeness_report_holder: dict[str, Any] = field(default_factory=dict)

    def begin_instrument_subscription_attempt(self) -> None:
        if self.instrument_evidence_tracker is not None:
            self.instrument_evidence_tracker.begin_subscription_attempt()

    def establish_instrument_subscription_attempt(
        self, sent_at_utc: str, established_at_utc: str
    ) -> None:
        if self.instrument_evidence_tracker is not None:
            self.instrument_evidence_tracker.mark_subscription_established(
                subscription_sent_at_utc=sent_at_utc,
                established_at_utc=established_at_utc,
            )

    def health_shard_for_instrument(self, instrument_id: str) -> FeedShardHealth:
        try:
            return self.supervisor.shard_for_instrument(self.venue, instrument_id)
        except KeyError:
            return self.health_shards[0]

    def record_runtime_feed_projection(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        observed_at_utc: str,
    ) -> None:
        if self.runtime_projection_recorder is None:
            return
        self.runtime_projection_recorder.record_feed_projection(
            feed_health_rows=rows,
            topbooks=self.latest_topbooks.values(),
            observed_at_utc=observed_at_utc,
        )

    async def write_health(
        self,
        sink: Any | None,
        *,
        observed_at_utc: str,
        local_sequence: int,
        now_monotonic_ns: int,
        cause: str = "message",
        shard_keys: Sequence[tuple[str, str]] | None = None,
        transition_row_count: int = 0,
        periodic_row_count: int = 0,
    ) -> bool:
        self.capture_clock.record(observed_at_utc)
        rows = self.supervisor.feed_health_rows(
            now_monotonic_ns=now_monotonic_ns,
            observed_at_utc=observed_at_utc,
            local_sequence=local_sequence,
            include_instrument_state=self.health_emitter is None,
            shard_keys=shard_keys,
        )
        emitted_rows = rows
        prepared_health: PreparedHealthEmissions | None = None
        emitter = self.health_emitter
        detail_by_shard: dict[tuple[str, str], dict[str, Any]] = {}
        if emitter is not None:

            def detail_provider(key: tuple[str, str]) -> Mapping[str, Any]:
                if key not in detail_by_shard:
                    detailed_rows = self.supervisor.feed_health_rows(
                        now_monotonic_ns=now_monotonic_ns,
                        observed_at_utc=observed_at_utc,
                        local_sequence=local_sequence,
                        include_instrument_state=True,
                        shard_keys=(key,),
                    )
                    if len(detailed_rows) != 1:
                        raise ValueError("feed-health detail shard is unavailable")
                    detail_by_shard[key] = detailed_rows[0]
                return detail_by_shard[key]

            prepared_health = emitter.prepare(
                rows,
                now_monotonic_ns=now_monotonic_ns,
                cause=cause,
                detail_provider=detail_provider,
            )
            emitted_rows = [
                dict(emission.row) for emission in prepared_health.emissions
            ]
        if sink is not None:
            for row in emitted_rows:
                await sink.write(row)
        if prepared_health is not None:
            assert emitter is not None
            emitter.commit(prepared_health)
        self.health_observation_count += len(emitted_rows)
        projection_rows = rows
        if (
            self.runtime_projection_recorder is not None
            and self.health_emitter is not None
        ):
            projection_rows = self.supervisor.feed_health_rows(
                now_monotonic_ns=now_monotonic_ns,
                observed_at_utc=observed_at_utc,
                local_sequence=local_sequence,
                include_instrument_state=True,
                shard_keys=shard_keys,
            )
        if self.feed_control_scheduler is not None:
            self.feed_control_scheduler.record_health_rows(
                transition_rows=transition_row_count,
                periodic_rows=periodic_row_count,
                detailed_rows=len(detail_by_shard),
            )
        self.record_runtime_feed_projection(
            projection_rows,
            observed_at_utc=observed_at_utc,
        )
        return bool(emitted_rows)

    def emit_topbook_boundary(
        self,
        *,
        reason: str,
        observed_at_utc: str,
        now_monotonic_ns: int,
    ) -> bool:
        if self.topbook_tracker is None or self.profile_runtime is None:
            return False
        checkpoint_spec = self.profile_runtime.specs_by_role.get(
            DatasetRole.TOPBOOK_CHECKPOINT.value
        )
        if checkpoint_spec is None:
            return False
        boundary_at = datetime.fromisoformat(
            observed_at_utc[:-1] + "+00:00"
            if observed_at_utc.endswith("Z")
            else observed_at_utc
        ).astimezone(timezone.utc)
        prior_coordinates = [
            datetime.fromisoformat(
                str(row["received_at_utc"])[:-1] + "+00:00"
                if str(row["received_at_utc"]).endswith("Z")
                else str(row["received_at_utc"])
            ).astimezone(timezone.utc)
            for row in self.latest_topbooks.values()
        ]
        if self.last_topbook_boundary_at is not None:
            prior_coordinates.append(self.last_topbook_boundary_at)
        if prior_coordinates:
            latest_prior = max(prior_coordinates)
            if boundary_at <= latest_prior:
                boundary_at = latest_prior + timedelta(microseconds=1)
        boundary_sequence = self.sequence + 1
        emissions = self.topbook_tracker.boundary_restatements(
            reason=reason,
            now_monotonic_ns=now_monotonic_ns,
            received_at_utc=canonical_utc(boundary_at),
            local_sequence=boundary_sequence,
        )
        for emission in emissions:
            self.profile_runtime.coordinator.add(
                DatasetRole.TOPBOOK_CHECKPOINT.value,
                dict(emission.row),
            )
        if emissions:
            self.last_topbook_boundary_at = boundary_at
            self.sequence = boundary_sequence
        return bool(emissions)

    def after_topbook_boundary(self, candidate: datetime) -> datetime:
        prior_coordinates = [
            coordinate
            for coordinate in (
                self.last_topbook_boundary_at,
                self.capture_clock.last_capture_observed_at,
            )
            if coordinate is not None
        ]
        if prior_coordinates and candidate <= max(prior_coordinates):
            return max(prior_coordinates) + timedelta(microseconds=1)
        return candidate

    def message_wait_timeout(
        self,
        remaining_seconds: float | None,
        *,
        now_monotonic_ns: int,
    ) -> float:
        if self.feed_control_scheduler is not None:
            return self.feed_control_scheduler.wait_timeout(
                now_monotonic_ns=now_monotonic_ns,
                remaining_seconds=remaining_seconds,
            )
        stale_check_seconds = max(
            0.05,
            min(
                self.supervisor.max_message_age_ms,
                self.supervisor.max_valid_book_age_ms,
            )
            / 1000,
        )
        if remaining_seconds is None:
            return stale_check_seconds
        return min(remaining_seconds, stale_check_seconds)

    def stage_instrument_evidence(self) -> None:
        if self.instrument_evidence_tracker is None or self.instrument_evidence_staged:
            return
        if self.profile_runtime is None:
            raise RuntimeError("instrument evidence requires a profile runtime")
        if self.instrument_evidence_tracker.attempt_count == 0:
            self.instrument_evidence_tracker.begin_subscription_attempt()
        evidence_reason = self.terminal_reason or CaptureTerminationReason.STREAM_ERROR
        # Mark before publication so a partial persistence failure remains
        # fail-closed instead of restaging duplicate evidence in the handler.
        self.instrument_evidence_staged = True
        self.profile_runtime.coordinator.add_rows_bounded(
            CAPTURE_INSTRUMENT_EVIDENCE_ROLE,
            self.instrument_evidence_tracker.iter_terminal_rows(evidence_reason),
            max_rows_per_commit=min(
                DEFAULT_TERMINAL_EVIDENCE_BATCH_ROWS,
                self.profile_runtime.coordinator.segment_row_limit,
            ),
            cause="termination",
        )

    def instrument_evidence_binding(self):
        if self.instrument_evidence_tracker is None:
            return None, None, False
        evidence_reason = self.terminal_reason or CaptureTerminationReason.STREAM_ERROR
        summary = self.instrument_evidence_tracker.summary(evidence_reason)
        artifact_hash = None
        reconciled = False
        if (
            self.profile_runtime is not None
            and self.profile_runtime.coordinator.segments_finalized
            and CAPTURE_INSTRUMENT_EVIDENCE_ROLE
            in self.profile_runtime.coordinator.committed_roles
        ):
            artifact = self.profile_runtime.coordinator.dataset_artifacts()[
                CAPTURE_INSTRUMENT_EVIDENCE_ROLE
            ]
            value = artifact.get("segment_manifest_hash")
            artifact_hash = str(value) if value is not None else None
            reconciled = (
                artifact.get("completion_status") == "closed"
                and artifact.get("row_count") == summary.row_count
            )
        return summary, artifact_hash, reconciled

    def finalize_capture_completeness(self):
        evidence_summary, evidence_hash, evidence_reconciled = (
            self.instrument_evidence_binding()
        )
        report = evaluate_capture_completeness(
            venue=self.venue,
            instruments_with_snapshots=len(self.instruments_with_snapshots),
            instruments_with_invalid_snapshots=len(
                self.instruments_with_invalid_snapshots
            ),
            event_count=self.event_count,
            reconnect_count=self.reconnect_count,
            capture_intent=self.resolved_capture_intent,
            terminal_reason=(
                self.terminal_reason or CaptureTerminationReason.STREAM_ERROR
            ),
            duration_seconds_requested=self.duration_s if self.duration_s > 0 else None,
            duration_seconds_actual=time.monotonic() - self.started_monotonic,
            instruments_requested=len(self.instrument_ids),
            instrument_evidence_summary=evidence_summary,
            evidence_policy_status=(
                self.instrument_evidence_tracker.policy.policy_status.value
                if self.instrument_evidence_tracker is not None
                else "provisional"
            ),
            evidence_artifact_role=(
                CAPTURE_INSTRUMENT_EVIDENCE_ROLE
                if self.instrument_evidence_tracker is not None
                else None
            ),
            evidence_artifact_hash=evidence_hash,
            evidence_artifact_reconciled=evidence_reconciled,
            acceptance_evidence_eligible=self.instrument_evidence_tracker is not None,
        )
        self.completeness_report_holder["object"] = report
        self.completeness_report_holder["report"] = report.as_manifest_mapping()
        return report

    async def run_incremental_feed_control(
        self,
        *,
        health_sink: Any | None,
        recover_socket: Callable[..., Awaitable[bool]],
        observed_at_utc: str,
        now_monotonic_ns: int,
        post_message: bool,
        allow_recovery: bool = True,
    ) -> bool:
        scheduler = self.feed_control_scheduler
        emitter = self.health_emitter
        if scheduler is None or emitter is None:
            return False

        tick_due = scheduler.due(now_monotonic_ns=now_monotonic_ns)
        periodic_keys: set[tuple[str, str]] = set()
        if tick_due:
            self.pending_health_shard_keys.update(
                self.supervisor.invalidate_stale(
                    now_monotonic_ns=now_monotonic_ns,
                    venue=self.venue,
                )
            )
            periodic_keys.update(
                emitter.due_shard_keys(
                    self.supervisor.shard_keys(venue=self.venue),
                    now_monotonic_ns=now_monotonic_ns,
                )
            )
            scheduler.record_tick(
                now_monotonic_ns=now_monotonic_ns,
                stale_instrument_checks=(
                    self.supervisor.last_staleness_instruments_examined
                ),
            )

        transition_keys = set(self.pending_health_shard_keys)
        selected_keys = transition_keys | periodic_keys
        emitted_health = False
        if selected_keys:
            health_sequence = (
                self.sequence
                if post_message or self.storage_profile is None
                else self.sequence + 1
            )
            emitted_health = await self.write_health(
                health_sink,
                observed_at_utc=observed_at_utc,
                local_sequence=health_sequence,
                now_monotonic_ns=now_monotonic_ns,
                shard_keys=tuple(sorted(selected_keys)),
                transition_row_count=len(transition_keys),
                periodic_row_count=len(periodic_keys - transition_keys),
            )
            self.pending_health_shard_keys.difference_update(transition_keys)
            if emitted_health and not post_message and self.storage_profile is not None:
                self.sequence = health_sequence

        recovery_keys = None if tick_due else transition_keys
        if allow_recovery and (tick_due or recovery_keys):
            recovery_actions = self.supervisor.current_recovery_actions(
                venue=self.venue,
                shard_keys=recovery_keys,
            )
            scheduler.record_recovery(action_count=len(recovery_actions))
            await recover_socket(
                now_monotonic_ns,
                recovery_actions=recovery_actions,
            )
        return tick_due
