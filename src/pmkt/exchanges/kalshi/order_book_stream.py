from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import pyarrow as pa
from websockets.exceptions import ConnectionClosed

from pmkt.data.canonical import FEED_HEALTH_SCHEMA_VERSION
from pmkt.data.kalshi_quotes import KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT
from pmkt.data.manifests import build_run_manifest, current_git_commit, write_manifest
from pmkt.data.normalize_books import kalshi_ws_snapshot_to_topbook
from pmkt.data.registry import arrow_schema, get_table_spec
from pmkt.data.schemas import DEPTH_SCHEMA_VERSION, TOPBOOK_SCHEMA_VERSION, depth_row
from pmkt.data.time import isoformat_source_timestamp
from pmkt.data.time import timestamp_seconds as _timestamp_seconds
from pmkt.data.types import parse_float as _parse_float
from pmkt.data.types import parse_int as _parse_int
from pmkt.streaming.supervisor import (
    FEED_HEALTH_SCHEMA,
    FeedRecoveryAction,
    FeedShardHealth,
    LiveFeedSupervisor,
    write_feed_health_parquet,
)
from pmkt.streaming.collector import (
    RuntimeFeedProjectionRecorder,
    StreamDatasetSpec,
    StreamRunOutputs,
)
from pmkt.streaming.capture_session import _CaptureSessionBookkeeping
from pmkt.streaming.capture_clock import (
    as_str as _as_str,
    raw_json as _raw_json,
    run_name as _run_name,
    utc_now as _utc_now,
)
from pmkt.streaming.datasets import merge_profile_dataset_specs
from pmkt.streaming.durability import COMMIT_JOURNAL_NAME, write_json_atomic_fsync
from pmkt.streaming.durability_settings import CaptureDurabilitySettings
from pmkt.streaming.feed_control import (
    FeedControlScheduler,
    control_interval_ns,
    feed_control_manifest,
)
from pmkt.streaming.health_emission import PreparedHealthEmissions, SlimHealthEmitter
from pmkt.streaming.instrument_evidence import (
    CaptureInstrumentEvidencePolicy,
    CaptureInstrumentEvidenceTracker,
    eligibility_evidence_from_subscription_metadata,
)
from pmkt.streaming.observations import (
    ObservationValidationError,
    StreamObservationProducer,
)
from pmkt.streaming.capture_completeness import (
    CaptureCompletenessError,
    CaptureIntent,
    CaptureTerminationReason,
)
from pmkt.exchanges.ws_transport import (
    WebSocketDeadlineExceeded,
    WebSocketRetryBudget,
    WebSocketTransportSettings,
)
from pmkt.streaming.profile_runtime import ProfileCaptureRuntime, create_profile_runtime
from pmkt.streaming.storage_backends import CaptureStorageBackend
from pmkt.streaming.profiles import (
    DatasetRole,
    StorageProfileSelection,
    TopbookEmissionMode,
    resolve_dataset_specs,
    integrity_dataset_specs,
    add_book_integrity,
)
from pmkt.streaming.tape import CaptureCoordinate, canonical_utc
from pmkt.streaming.tape_producers import CompactValidityProducer, KalshiTapeProducer
from pmkt.streaming.topbook_emission import TopbookEmissionTracker
from pmkt.exchanges.read_auth import ReadAuthHeaderProvider
from pmkt.exchanges.kalshi.ws import (
    AsyncKalshiWebSocketClient,
    KalshiConnectFactory,
    KalshiOrderBookState,
    apply_kalshi_orderbook_message,
    normalize_market_tickers,
)

DEFAULT_KALSHI_ORDER_BOOK_STREAM_ROOT = Path("generated/kalshi_order_book_streams")
_DEPTH_SNAPSHOT_EVENT_TYPES = {"orderbook_snapshot", "orderbook_delta"}

EVENT_COLUMNS = [
    "sequence",
    "received_at",
    "received_at_utc",
    "event_type",
    "market_ticker",
    "market_id",
    "sid",
    "seq",
    "exchange_timestamp",
    "exchange_timestamp_seconds",
    "raw_json",
]

SNAPSHOT_COLUMNS = [
    "sequence",
    "received_at",
    "received_at_utc",
    "event_type",
    "market_ticker",
    "market_id",
    "sid",
    "seq",
    "exchange_timestamp",
    "exchange_timestamp_seconds",
    "exchange_datetime_utc",
    "yes_bid",
    "yes_ask",
    "no_bid",
    "no_ask",
    "mid",
    "spread",
    "yes_bid_depth",
    "no_bid_depth",
    "valid_state",
    "quality_flags",
    "initial_snapshot_received",
]

LEVEL_COLUMNS = [
    "sequence",
    "received_at",
    "received_at_utc",
    "event_type",
    "market_ticker",
    "market_id",
    "sid",
    "seq",
    "side",
    "price",
    "size",
    "delta",
    "level_index",
    "is_delta",
]

EVENT_SCHEMA = pa.schema(
    [
        ("sequence", pa.int64()),
        ("received_at", pa.float64()),
        ("received_at_utc", pa.string()),
        ("event_type", pa.string()),
        ("market_ticker", pa.string()),
        ("market_id", pa.string()),
        ("sid", pa.int64()),
        ("seq", pa.int64()),
        ("exchange_timestamp", pa.string()),
        ("exchange_timestamp_seconds", pa.float64()),
        ("raw_json", pa.string()),
    ]
)

SNAPSHOT_SCHEMA = pa.schema(
    [
        ("sequence", pa.int64()),
        ("received_at", pa.float64()),
        ("received_at_utc", pa.string()),
        ("event_type", pa.string()),
        ("market_ticker", pa.string()),
        ("market_id", pa.string()),
        ("sid", pa.int64()),
        ("seq", pa.int64()),
        ("exchange_timestamp", pa.string()),
        ("exchange_timestamp_seconds", pa.float64()),
        ("exchange_datetime_utc", pa.string()),
        ("yes_bid", pa.float64()),
        ("yes_ask", pa.float64()),
        ("no_bid", pa.float64()),
        ("no_ask", pa.float64()),
        ("mid", pa.float64()),
        ("spread", pa.float64()),
        ("yes_bid_depth", pa.int64()),
        ("no_bid_depth", pa.int64()),
        ("valid_state", pa.bool_()),
        ("quality_flags", pa.string()),
        ("initial_snapshot_received", pa.bool_()),
    ]
)

LEVEL_SCHEMA = pa.schema(
    [
        ("sequence", pa.int64()),
        ("received_at", pa.float64()),
        ("received_at_utc", pa.string()),
        ("event_type", pa.string()),
        ("market_ticker", pa.string()),
        ("market_id", pa.string()),
        ("sid", pa.int64()),
        ("seq", pa.int64()),
        ("side", pa.string()),
        ("price", pa.float64()),
        ("size", pa.float64()),
        ("delta", pa.float64()),
        ("level_index", pa.int64()),
        ("is_delta", pa.bool_()),
    ]
)

CANONICAL_TOPBOOK_SCHEMA = arrow_schema(get_table_spec(TOPBOOK_SCHEMA_VERSION))
CANONICAL_DEPTH_SCHEMA = arrow_schema(get_table_spec(DEPTH_SCHEMA_VERSION))

STREAM_DATASETS = (
    StreamDatasetSpec(
        "events_parquet",
        "events.parquet",
        EVENT_SCHEMA,
        schema_version="legacy.kalshi.parsed_event.v1",
        role=DatasetRole.PARSED_EVENT.value,
    ),
    StreamDatasetSpec(
        "snapshots_parquet",
        "snapshots.parquet",
        SNAPSHOT_SCHEMA,
        schema_version="legacy.kalshi.snapshot.v1",
        role=DatasetRole.LEGACY_SNAPSHOT.value,
    ),
    StreamDatasetSpec(
        "order_book_levels_parquet",
        "order_book_levels.parquet",
        LEVEL_SCHEMA,
        schema_version="legacy.kalshi.level.v1",
        role=DatasetRole.LEGACY_LEVEL.value,
    ),
    StreamDatasetSpec(
        "topbook_v1_parquet",
        "topbook_v1.parquet",
        CANONICAL_TOPBOOK_SCHEMA,
        manifest_schema_key="topbook",
        schema_version=TOPBOOK_SCHEMA_VERSION,
        role=DatasetRole.TOPBOOK_MAIN.value,
    ),
    StreamDatasetSpec(
        "depth_v1_parquet",
        "depth_v1.parquet",
        CANONICAL_DEPTH_SCHEMA,
        manifest_schema_key="depth",
        schema_version=DEPTH_SCHEMA_VERSION,
        role=DatasetRole.DEPTH_MAIN.value,
    ),
    StreamDatasetSpec(
        "feed_health_parquet",
        "feed_health.parquet",
        FEED_HEALTH_SCHEMA,
        manifest_schema_key="feed_health",
        schema_version=FEED_HEALTH_SCHEMA_VERSION,
        role=DatasetRole.HEALTH.value,
    ),
)


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _first_parsed_float(*values: Any) -> float | None:
    for value in values:
        parsed = _parse_float(value)
        if parsed is not None:
            return parsed
    return None


def _msg(message: dict[str, Any]) -> dict[str, Any]:
    payload = message.get("msg")
    return payload if isinstance(payload, dict) else message


def _event_row(
    sequence: int,
    received_at: float,
    received_at_utc: str,
    message: dict[str, Any],
) -> dict[str, Any]:
    payload = _msg(message)
    exchange_ts = _first_present(payload.get("ts_ms"), payload.get("ts"))
    return {
        "sequence": sequence,
        "received_at": received_at,
        "received_at_utc": received_at_utc,
        "event_type": _as_str(message.get("type")),
        "market_ticker": _as_str(payload.get("market_ticker")),
        "market_id": _as_str(payload.get("market_id")),
        "sid": _parse_int(message.get("sid")),
        "seq": _parse_int(message.get("seq")),
        "exchange_timestamp": _as_str(exchange_ts),
        # Kalshi sends either ``ts_ms`` or ``ts`` through this combined field.
        "exchange_timestamp_seconds": _timestamp_seconds(exchange_ts, unit="auto"),
        "raw_json": _raw_json(message),
    }


def _level_rows(
    sequence: int,
    received_at: float,
    received_at_utc: str,
    message: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    event_type = message.get("type")
    payload = _msg(message)
    market_ticker = payload.get("market_ticker")
    market_id = payload.get("market_id")
    sid = message.get("sid")
    seq = message.get("seq")

    if event_type == "orderbook_snapshot":
        for side, key in (("yes", "yes_dollars_fp"), ("no", "no_dollars_fp")):
            levels = _first_present(
                payload.get(key), payload.get(key.replace("_fp", "")), []
            )
            if not isinstance(levels, list):
                continue
            for index, level in enumerate(levels):
                if isinstance(level, dict):
                    price = _first_parsed_float(
                        level.get("price"), level.get("price_dollars")
                    )
                    size = _first_parsed_float(level.get("size"), level.get("count_fp"))
                elif isinstance(level, (list, tuple)) and len(level) >= 2:
                    price = _parse_float(level[0])
                    size = _parse_float(level[1])
                else:
                    continue
                rows.append(
                    {
                        "sequence": sequence,
                        "received_at": received_at,
                        "received_at_utc": received_at_utc,
                        "event_type": event_type,
                        "market_ticker": _as_str(market_ticker),
                        "market_id": _as_str(market_id),
                        "sid": _parse_int(sid),
                        "seq": _parse_int(seq),
                        "side": side,
                        "price": price,
                        "size": size,
                        "delta": None,
                        "level_index": index,
                        "is_delta": False,
                    }
                )
        return rows

    if event_type == "orderbook_delta":
        rows.append(
            {
                "sequence": sequence,
                "received_at": received_at,
                "received_at_utc": received_at_utc,
                "event_type": event_type,
                "market_ticker": _as_str(market_ticker),
                "market_id": _as_str(market_id),
                "sid": _parse_int(sid),
                "seq": _parse_int(seq),
                "side": _as_str(payload.get("side")),
                "price": _parse_float(
                    _first_present(payload.get("price_dollars"), payload.get("price"))
                ),
                "size": None,
                "delta": _parse_float(
                    _first_present(
                        payload.get("delta_fp"),
                        payload.get("delta"),
                        payload.get("size_delta"),
                    )
                ),
                "level_index": 0,
                "is_delta": True,
            }
        )
    return rows


def _canonical_depth_rows_from_state(
    *,
    run_id: str,
    sequence: int,
    received_at_utc: str,
    snapshot: Any,
    state: KalshiOrderBookState,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    flags = list(snapshot.quality_flags)
    for outcome, side, levels in (
        ("YES", "yes", sorted(state.yes_bids.items(), reverse=True)),
        ("NO", "no", sorted(state.no_bids.items(), reverse=True)),
    ):
        cumulative = 0.0
        for index, (price, size) in enumerate(levels):
            cumulative += size
            rows.append(
                depth_row(
                    collector_run_id=run_id,
                    exchange="kalshi",
                    venue_market_id=state.market_id or state.market_ticker,
                    instrument_id=f"{state.market_ticker}:{outcome}",
                    outcome=outcome,
                    source="ws",
                    received_at_utc=canonical_utc(received_at_utc),
                    exchange_ts_utc=snapshot.datetime_utc,
                    local_sequence=sequence,
                    venue_sequence=snapshot.seq,
                    venue_sid=snapshot.sid,
                    side=side,
                    level_index=index,
                    price_dollars=price,
                    size_contracts=size,
                    cumulative_size_contracts=cumulative,
                    is_delta=False,
                    valid_state=snapshot.valid_state,
                    quality_flags=flags,
                )
            )
    return rows


def _should_emit_canonical_depth(snapshot: Any) -> bool:
    return getattr(snapshot, "event_type", None) in _DEPTH_SNAPSHOT_EVENT_TYPES


def _snapshot_row(
    sequence: int,
    received_at: float,
    received_at_utc: str,
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    return {
        "sequence": sequence,
        "received_at": received_at,
        "received_at_utc": received_at_utc,
        "event_type": _as_str(snapshot.get("event_type")),
        "market_ticker": _as_str(snapshot.get("market_ticker")),
        "market_id": _as_str(snapshot.get("market_id")),
        "sid": _parse_int(snapshot.get("sid")),
        "seq": _parse_int(snapshot.get("seq")),
        "exchange_timestamp": _as_str(snapshot.get("timestamp")),
        "exchange_timestamp_seconds": snapshot.get("timestamp_seconds"),
        "exchange_datetime_utc": _as_str(snapshot.get("datetime_utc")),
        "yes_bid": snapshot.get("yes_bid"),
        "yes_ask": snapshot.get("yes_ask"),
        "no_bid": snapshot.get("no_bid"),
        "no_ask": snapshot.get("no_ask"),
        "mid": snapshot.get("mid"),
        "spread": snapshot.get("spread"),
        "yes_bid_depth": _parse_int(snapshot.get("yes_bid_depth")),
        "no_bid_depth": _parse_int(snapshot.get("no_bid_depth")),
        "valid_state": bool(snapshot.get("valid_state")),
        "quality_flags": ";".join(snapshot.get("quality_flags") or []),
        "initial_snapshot_received": bool(snapshot.get("initial_snapshot_received")),
    }


@dataclass(slots=True, kw_only=True)
class _KalshiCaptureSession(_CaptureSessionBookkeeping):
    states: dict[str, KalshiOrderBookState]
    tape_producer: KalshiTapeProducer | None
    compact_control_producer: CompactValidityProducer | None
    monotonic_ns: Callable[[], int]
    outputs: StreamRunOutputs
    run_dir: Path
    started_at: datetime
    max_messages: int | None
    max_reconnects: int
    use_yes_price: bool
    command: str | None
    git_commit: str | None
    git_cwd: str | Path | None
    subscription_plan_metadata: Mapping[str, Any] | None
    transport_settings: WebSocketTransportSettings
    websocket_max_size_bytes: int | None
    websocket_max_queue_frames: int | None
    configured_feed_control_interval_ns: int
    next_book_checkpoint_ns: int | None
    snapshot_count: int = 0
    level_count: int = 0
    topbook_count: int = 0
    depth_count: int = 0
    socket_recovery_count: int = 0
    sequence_gap_count: int = 0
    snapshot_resync_request_count: int = 0
    transport_liveness_probe_count: int = 0
    transport_liveness_probe_success_count: int = 0
    transport_liveness_probe_failure_count: int = 0
    targeted_snapshot_refresh_count: int = 0
    targeted_snapshot_refresh_failure_count: int = 0
    targeted_snapshot_refresh_exhausted_count: int = 0
    targeted_snapshot_market_count: int = 0
    targeted_snapshot_response_count: int = 0
    targeted_snapshot_response_success_count: int = 0
    targeted_snapshot_response_timeout_count: int = 0
    orderbook_subscription_sid: int | None = None
    # instrument -> (connection generation, subscription ID, response deadline)
    pending_snapshot_requests: dict[str, tuple[int, int, int]] = field(default_factory=dict)
    quality_counter: Counter[str] = field(default_factory=Counter)
    instrument_counter: Counter[str] = field(default_factory=Counter)
    last_sequence_by_sid: dict[int, int] = field(default_factory=dict)

    def mark_reconnect(self) -> None:
        self.reconnect_count += 1
        self.pending_snapshot_requests.clear()
        self.orderbook_subscription_sid = None
        if self.storage_profile is not None:
            self.sequence += 1
        now = self.monotonic_ns()
        for shard in self.health_shards:
            if shard.mark_reconnect(now_monotonic_ns=now):
                self.pending_health_shard_keys.add((shard.venue, shard.shard_id))
        for state in self.states.values():
            state.mark_reconnect()
        if self.tape_producer is not None and self.profile_runtime is not None:
            self.tape_producer.reconnect(
                states=self.states,
                received_at_utc=_utc_now().isoformat(),
                received_at_monotonic_ns=now,
                local_sequence=self.sequence,
            ).write_to(self.profile_runtime.coordinator)
        elif (
            self.compact_control_producer is not None
            and self.profile_runtime is not None
        ):
            self.compact_control_producer.invalidate_books(
                books={
                    f"{book_id}:{outcome}": str(state.market_id or book_id)
                    for book_id, state in self.states.items()
                    for outcome in ("YES", "NO")
                },
                received_at_utc=_utc_now().isoformat(),
                received_at_monotonic_ns=now,
                local_sequence=self.sequence,
                reason="reconnect",
            ).write_to(self.profile_runtime.coordinator)
        self.emit_topbook_boundary(
            reason="reconnect",
            observed_at_utc=(_utc_now() + timedelta(milliseconds=1)).isoformat(),
            now_monotonic_ns=now + 1,
        )
        self.last_sequence_by_sid.clear()

    def observe_snapshot_response(self, snapshot: Any, *, now_monotonic_ns: int) -> None:
        pending = self.pending_snapshot_requests.get(snapshot.market_ticker)
        if (
            pending is not None
            and pending[0] == self.reconnect_count
            and pending[1] == snapshot.sid
            and snapshot.event_type == "orderbook_snapshot"
        ):
            self.targeted_snapshot_response_count += 1
            if snapshot.book_integrity_valid and now_monotonic_ns < pending[2]:
                del self.pending_snapshot_requests[snapshot.market_ticker]
                self.targeted_snapshot_response_success_count += 1

    def subscription_sequence_gap_sid(self, message: Mapping[str, Any]) -> int | None:
        if str(message.get("type") or "") not in {
            "orderbook_snapshot",
            "orderbook_delta",
        }:
            return None
        sid = _parse_int(message.get("sid"))
        message_sequence = _parse_int(message.get("seq"))
        if sid is None or message_sequence is None:
            return None
        previous = self.last_sequence_by_sid.get(sid)
        self.last_sequence_by_sid[sid] = message_sequence
        if previous is not None and message_sequence != previous + 1:
            return sid
        return None

    async def emit_scheduled_capture(
        self,
        *,
        checkpoint_sink: Any | None,
        observed_at_utc: str,
        now_monotonic_ns: int,
    ) -> None:
        scheduler_sequence = self.sequence + 1
        scheduled = False
        if self.topbook_tracker is not None and checkpoint_sink is not None:
            restatements = self.topbook_tracker.due_restatements(
                now_monotonic_ns=now_monotonic_ns,
                received_at_utc=observed_at_utc,
                local_sequence=scheduler_sequence,
            )
            for emission in restatements:
                await checkpoint_sink.write(dict(emission.row))
            scheduled = bool(restatements)
        if (
            self.tape_producer is not None
            and self.profile_runtime is not None
            and self.next_book_checkpoint_ns is not None
            and now_monotonic_ns >= self.next_book_checkpoint_ns
        ):
            self.tape_producer.checkpoint_states(
                states=self.states,
                received_at_utc=observed_at_utc,
                received_at_monotonic_ns=now_monotonic_ns,
                local_sequence=scheduler_sequence,
            ).write_to(self.profile_runtime.coordinator)
            scheduled = True
            assert self.storage_profile is not None
            interval = self.storage_profile.definition.book_checkpoint_interval_seconds
            assert interval is not None
            self.next_book_checkpoint_ns = now_monotonic_ns + int(
                interval * 1_000_000_000
            )
        if scheduled:
            self.sequence = scheduler_sequence
        if (
            self.profile_runtime is not None
            and self.profile_runtime.coordinator.barrier_due()
        ):
            self.profile_runtime.coordinator.commit()

    def make_manifest(
        self,
        *,
        status: str,
        ended_at: datetime,
        error: BaseException | None = None,
    ) -> dict[str, Any]:
        completeness_report = self.finalize_capture_completeness()
        effective_status = (
            completeness_report.legacy_status
            if self.instrument_evidence_tracker is not None
            else status
        )
        profile_rows = (
            self.profile_runtime.coordinator.row_counts
            if self.profile_runtime is not None
            else {}
        )
        legacy_counts = {
            "events": self.event_count,
            "snapshots": self.snapshot_count,
            "levels": self.level_count,
            "markets_with_snapshots": len(self.instruments_with_snapshots),
        }
        capture_role_counts = {
            "parsed_events": int(
                profile_rows.get(DatasetRole.PARSED_EVENT.value, self.event_count)
            ),
            "legacy_snapshots": int(
                profile_rows.get(DatasetRole.LEGACY_SNAPSHOT.value, self.snapshot_count)
            ),
            "legacy_levels": int(
                profile_rows.get(DatasetRole.LEGACY_LEVEL.value, self.level_count)
            ),
            "tape_events": int(profile_rows.get(DatasetRole.TAPE_EVENT.value, 0)),
            "tape_levels": int(profile_rows.get(DatasetRole.TAPE_LEVEL.value, 0)),
            "tape_controls": int(profile_rows.get(DatasetRole.TAPE_CONTROL.value, 0)),
            "topbook": int(
                profile_rows.get(DatasetRole.TOPBOOK_MAIN.value, self.topbook_count)
            ),
            "topbook_checkpoints": int(
                profile_rows.get(DatasetRole.TOPBOOK_CHECKPOINT.value, 0)
            ),
            "trades": int(profile_rows.get(DatasetRole.TRADE.value, 0)),
            "lifecycle": int(profile_rows.get(DatasetRole.LIFECYCLE.value, 0)),
            "instrument_evidence": int(
                profile_rows.get(DatasetRole.INSTRUMENT_EVIDENCE.value, 0)
            ),
            "feed_health": int(
                profile_rows.get(
                    DatasetRole.HEALTH.value, self.health_observation_count
                )
            ),
        }
        profile_extra: dict[str, Any] = {}
        if self.profile_runtime is not None:
            profile_extra = {
                "dataset_artifacts": self.profile_runtime.coordinator.dataset_artifacts(),
                "capture_durability": self.profile_runtime.coordinator.durability_manifest(),
                "capture_storage": self.profile_runtime.coordinator.storage_manifest(),
                "storage_profile": self.profile_runtime.manifest_profile(
                    terminal_completeness=completeness_report.capture_status.value
                    if self.instrument_evidence_tracker is not None
                    else ("complete" if status == "success" else status)
                ),
                "capture_commit_journal": COMMIT_JOURNAL_NAME,
            }
        compatibility_row_counts = {
            "events": self.event_count,
            "snapshots": self.snapshot_count,
            "levels": self.level_count,
            "topbook": self.topbook_count,
            "depth": self.depth_count,
            "feed_health": self.health_observation_count,
        }
        if self.profile_runtime is not None:
            compatibility_row_counts.update(self.profile_runtime.coordinator.row_counts)
        return build_run_manifest(
            run_id=self.run_dir.name,
            run_dir=self.run_dir,
            started_at_utc=self.started_at.isoformat(),
            ended_at_utc=ended_at.isoformat(),
            status=effective_status,
            command=self.command or "stream_kalshi_order_book_data",
            git_commit=self.git_commit
            if self.git_commit is not None
            else current_git_commit(
                Path(self.git_cwd) if self.git_cwd is not None else Path.cwd()
            ),
            dataset_paths=self.outputs.dataset_paths,
            schema_versions=self.outputs.schema_versions,
            row_counts=compatibility_row_counts,
            quality_flag_counts=dict(sorted(self.quality_counter.items())),
            venue_counts={"kalshi": self.topbook_count},
            instrument_counts=dict(sorted(self.instrument_counter.items())),
            reconnect_count=self.reconnect_count,
            sequence_gap_count=self.sequence_gap_count,
            resync_event_count=self.reconnect_count
            + self.snapshot_resync_request_count,
            error_type=type(error).__name__ if error is not None else None,
            error_message=str(error) if error is not None else None,
            extra={
                "capture_completeness": (self.completeness_report_holder.get("report")),
                "capture_lifecycle": {
                    "intent": self.resolved_capture_intent.value,
                    "terminal_reason": (
                        self.terminal_reason.value
                        if self.terminal_reason is not None
                        else None
                    ),
                    "duration_seconds_requested": (
                        self.duration_s if self.duration_s > 0 else None
                    ),
                    "duration_seconds_actual": (
                        ended_at - self.started_at
                    ).total_seconds(),
                },
                "websocket_transport": self.transport_settings.as_manifest_mapping(
                    requested_max_size_bytes=self.websocket_max_size_bytes,
                    requested_max_queue_frames=self.websocket_max_queue_frames,
                ),
                "capture_role_counts": capture_role_counts,
                "duration_seconds_actual": (ended_at - self.started_at).total_seconds(),
                "request": {
                    "market_tickers": self.instrument_ids,
                    "use_yes_price": self.use_yes_price,
                    "quote_normalization_policy": (
                        KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT
                    ),
                    "duration_s": self.duration_s,
                    "max_messages": self.max_messages,
                    "capture_intent": self.resolved_capture_intent.value,
                    "max_reconnects": self.max_reconnects,
                },
                "counts": legacy_counts,
                "files": self.outputs.files,
                "reconnect_count": self.reconnect_count,
                "socket_recovery_count": self.socket_recovery_count,
                "reconnect_diagnostics": self.reconnect_diagnostics,
                "snapshot_resync_request_count": self.snapshot_resync_request_count,
                "kalshi_feed_recovery": {
                    "transport_liveness_probe_count": (
                        self.transport_liveness_probe_count
                    ),
                    "transport_liveness_probe_success_count": (
                        self.transport_liveness_probe_success_count
                    ),
                    "transport_liveness_probe_failure_count": (
                        self.transport_liveness_probe_failure_count
                    ),
                    "targeted_snapshot_refresh_count": (
                        self.targeted_snapshot_refresh_count
                    ),
                    "targeted_snapshot_refresh_failure_count": (
                        self.targeted_snapshot_refresh_failure_count
                    ),
                    "targeted_snapshot_refresh_exhausted_count": (
                        self.targeted_snapshot_refresh_exhausted_count
                    ),
                    "targeted_snapshot_market_count": self.targeted_snapshot_market_count,
                    "targeted_snapshot_response_count": self.targeted_snapshot_response_count,
                    "targeted_snapshot_response_success_count": self.targeted_snapshot_response_success_count,
                    "targeted_snapshot_response_timeout_count": self.targeted_snapshot_response_timeout_count,
                    "pending_snapshot_request_count": len(
                        self.pending_snapshot_requests
                    ),
                },
                "subscription_plan": (
                    dict(self.subscription_plan_metadata)
                    if self.subscription_plan_metadata is not None
                    else None
                ),
                "feed_shards": self.supervisor.shard_metadata(),
                "feed_health_summary": self.supervisor.feed_health_summary(
                    now_monotonic_ns=self.monotonic_ns()
                ),
                "feed_health_emission": (
                    self.health_emitter.manifest_metrics()
                    if self.health_emitter is not None
                    else None
                ),
                "feed_control_plane": feed_control_manifest(
                    scheduler=self.feed_control_scheduler,
                    interval_ns=self.configured_feed_control_interval_ns,
                    suppression_reason=(
                        "slim_health_emitter_inactive"
                        if self.health_emitter is None
                        else None
                    ),
                ),
                "instrument_evidence_policy": (
                    self.instrument_evidence_tracker.policy.as_manifest_mapping(
                        venue="kalshi"
                    )
                    if self.instrument_evidence_tracker is not None
                    else None
                ),
                "parquet_segments": self.outputs.parquet_segment_manifests(),
                **profile_extra,
            },
        )


async def stream_kalshi_order_book_data(
    market_tickers: Sequence[str],
    *,
    ws_url: str | None = None,
    output_root: str | Path = DEFAULT_KALSHI_ORDER_BOOK_STREAM_ROOT,
    run_name: str | None = None,
    duration_s: float = 300.0,
    max_messages: int | None = None,
    capture_intent: CaptureIntent | str = CaptureIntent.OPERATIONAL,
    max_reconnects: int = 3,
    websocket_max_size_bytes: int | None = None,
    websocket_max_queue_frames: int | None = None,
    connect_factory: KalshiConnectFactory | None = None,
    auth: ReadAuthHeaderProvider | None = None,
    command: str | None = None,
    git_commit: str | None = None,
    git_cwd: str | Path | None = None,
    use_yes_price: bool = True,
    feed_supervisor: LiveFeedSupervisor | None = None,
    subscription_plan_metadata: Mapping[str, Any] | None = None,
    instrument_eligibility_evidence: Mapping[str, Mapping[str, Any]] | None = None,
    instrument_evidence_policy: CaptureInstrumentEvidencePolicy | None = None,
    runtime_projection_recorder: RuntimeFeedProjectionRecorder | None = None,
    parquet_segment_rows: int | None = None,
    parquet_segment_seconds: float | None = None,
    monotonic_ns: Callable[[], int] = time.monotonic_ns,
    storage_profile: StorageProfileSelection | None = None,
    capture_storage_backend: CaptureStorageBackend | str = (
        CaptureStorageBackend.PARQUET_SEGMENTS
    ),
) -> dict[str, Any]:
    """Stream Kalshi order-book messages to an analysis-ready run directory.

    ``monotonic_ns`` is the monotonic clock (nanoseconds) used for all feed
    staleness/health timing; it defaults to :func:`time.monotonic_ns` and is
    injectable so tests can drive staleness deterministically instead of
    depending on real elapsed wall-clock time.
    """
    tickers = normalize_market_tickers(market_tickers)
    if duration_s <= 0 and max_messages is None:
        raise ValueError("duration_s must be > 0 unless max_messages is set.")
    if max_messages is not None and max_messages < 1:
        raise ValueError("max_messages must be >= 1 when provided.")
    if max_reconnects < 0:
        raise ValueError("max_reconnects must be >= 0")
    resolved_capture_intent = CaptureIntent(capture_intent)
    transport_settings = WebSocketTransportSettings(
        max_size_bytes=(
            websocket_max_size_bytes
            if websocket_max_size_bytes is not None
            else WebSocketTransportSettings().max_size_bytes
        ),
        max_queue_frames=(
            websocket_max_queue_frames
            if websocket_max_queue_frames is not None
            else WebSocketTransportSettings().max_queue_frames
        ),
    )

    profile_specs = None
    if storage_profile is not None:
        profile_specs = resolve_dataset_specs(
            storage_profile,
            integrity_dataset_specs(storage_profile, merge_profile_dataset_specs(STREAM_DATASETS)),
        )

    root = Path(output_root).resolve()
    run_dir = root / (run_name or _run_name())
    run_dir.mkdir(parents=True, exist_ok=True)

    outputs = StreamRunOutputs(
        run_dir=run_dir,
        datasets=profile_specs or STREAM_DATASETS,
        include_raw_jsonl=(
            storage_profile is None
            or DatasetRole.RAW_JSONL in storage_profile.enabled_roles
        ),
        parquet_segment_rows=parquet_segment_rows,
        parquet_segment_seconds=parquet_segment_seconds,
    )
    raw_jsonl_path = outputs.raw_jsonl_path
    feed_health_path = outputs.path(
        DatasetRole.HEALTH.value
        if storage_profile is not None
        else "feed_health_parquet"
    )
    manifest_path = outputs.manifest_path

    started_at = _utc_now()
    started_monotonic = time.monotonic()
    requested_tickers = set(tickers)
    states = {
        ticker: KalshiOrderBookState(ticker, use_yes_price=use_yes_price)
        for ticker in tickers
    }
    supervisor = feed_supervisor or LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="kalshi",
                shard_id="kalshi-0",
                subscribed_instruments=tuple(tickers),
            )
        ]
    )
    integrity_evidence = storage_profile is not None and storage_profile.definition.profile_version == "3"
    supervisor.integrity_evidence = integrity_evidence
    supervisor.require_preflight_ok()
    health_shards = supervisor.venue_shards("kalshi")
    if not health_shards:
        raise ValueError("feed supervisor has no Kalshi shards")
    if len(health_shards) != 1:
        raise ValueError(
            "one Kalshi collector connection requires exactly one feed shard; "
            "partition the capture into one collector run per shard"
        )
    capture_shard = health_shards[0]
    if set(capture_shard.subscribed_instruments) != requested_tickers:
        raise ValueError(
            "Kalshi collector instruments must exactly match its feed shard"
        )
    capture_shard_id = capture_shard.shard_id
    active_sequence_gap_markets: set[str] = set()
    startup_boundary_pending = True
    terminal_control_staged = False
    profile_runtime: ProfileCaptureRuntime | None = None
    tape_producer: KalshiTapeProducer | None = None
    compact_control_producer: CompactValidityProducer | None = None
    observation_producer: StreamObservationProducer | None = None
    topbook_tracker: TopbookEmissionTracker | None = None
    dense_topbook_emission = False
    health_emitter: SlimHealthEmitter | None = None
    configured_feed_control_interval_ns = control_interval_ns(
        max_message_age_ms=supervisor.max_message_age_ms,
        max_valid_book_age_ms=supervisor.max_valid_book_age_ms,
    )
    instrument_evidence_tracker: CaptureInstrumentEvidenceTracker | None = None
    kalshi_adapter_settings = {
        "use_yes_price": use_yes_price,
        "quote_normalization_policy": KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT,
    }
    if storage_profile is not None:
        assert profile_specs is not None
        profile_runtime = create_profile_runtime(
            run_dir=run_dir,
            selection=storage_profile,
            specs=profile_specs,
            shard_plan={capture_shard_id: tickers},
            adapter_settings_by_venue={"kalshi": kalshi_adapter_settings},
            started_at_utc=started_at.isoformat(),
            durability_settings=CaptureDurabilitySettings.resolve(
                requested_segment_rows=parquet_segment_rows,
                requested_segment_seconds=parquet_segment_seconds,
            ),
            storage_backend=capture_storage_backend,
        )
        if DatasetRole.TAPE_EVENT in storage_profile.enabled_roles:
            tape_producer = KalshiTapeProducer(
            integrity_evidence=integrity_evidence,
                collector_run_id=run_dir.name,
                shard_id=capture_shard_id,
                use_yes_price=use_yes_price,
                quote_normalization_policy=(KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT),
            )
        elif DatasetRole.TAPE_CONTROL in storage_profile.enabled_roles:
            compact_control_producer = CompactValidityProducer(
                collector_run_id=run_dir.name,
                shard_id=capture_shard_id,
                venue="kalshi",
            )
        observation_producer = StreamObservationProducer(collector_run_id=run_dir.name)
        topbook_tracker = TopbookEmissionTracker(
            checkpoint_interval_seconds=(
                storage_profile.definition.topbook_checkpoint_interval_seconds
            )
        )
        dense_topbook_emission = (
            storage_profile.effective_topbook_mode is TopbookEmissionMode.DENSE
        )
        health_emitter = SlimHealthEmitter(
            interval_seconds=storage_profile.definition.feed_health_interval_seconds
        )
        if (
            storage_profile.definition.profile_version in {"2", "3"}
            and DatasetRole.INSTRUMENT_EVIDENCE in storage_profile.enabled_roles
        ):
            instrument_evidence_tracker = CaptureInstrumentEvidenceTracker(
            integrity_evidence=integrity_evidence,
                collector_run_id=run_dir.name,
                venue="kalshi",
                shard_id=capture_shard_id,
                instrument_ids=tickers,
                eligibility_evidence=(
                    instrument_eligibility_evidence
                    or eligibility_evidence_from_subscription_metadata(
                        subscription_plan_metadata, venue="kalshi"
                    )
                ),
                policy=instrument_evidence_policy,
            )

    deadline = time.monotonic() + float(duration_s) if duration_s > 0 else None
    capture_phase = "stream"
    message_count = 0
    next_book_checkpoint_ns = None
    if (
        storage_profile is not None
        and storage_profile.definition.book_checkpoint_interval_seconds is not None
    ):
        next_book_checkpoint_ns = monotonic_ns() + int(
            storage_profile.definition.book_checkpoint_interval_seconds * 1_000_000_000
        )

    session = _KalshiCaptureSession(
        venue="kalshi",
        instrument_ids=tickers,
        states=states,
        supervisor=supervisor,
        health_shards=health_shards,
        storage_profile=storage_profile,
        profile_runtime=profile_runtime,
        tape_producer=tape_producer,
        compact_control_producer=compact_control_producer,
        topbook_tracker=topbook_tracker,
        health_emitter=health_emitter,
        runtime_projection_recorder=runtime_projection_recorder,
        instrument_evidence_tracker=instrument_evidence_tracker,
        monotonic_ns=monotonic_ns,
        outputs=outputs,
        run_dir=run_dir,
        started_at=started_at,
        started_monotonic=started_monotonic,
        resolved_capture_intent=resolved_capture_intent,
        duration_s=duration_s,
        max_messages=max_messages,
        max_reconnects=max_reconnects,
        use_yes_price=use_yes_price,
        command=command,
        git_commit=git_commit,
        git_cwd=git_cwd,
        subscription_plan_metadata=subscription_plan_metadata,
        transport_settings=transport_settings,
        websocket_max_size_bytes=websocket_max_size_bytes,
        websocket_max_queue_frames=websocket_max_queue_frames,
        configured_feed_control_interval_ns=configured_feed_control_interval_ns,
        next_book_checkpoint_ns=next_book_checkpoint_ns,
    )
    retry_budget = WebSocketRetryBudget(
        session.max_reconnects,
        on_reconnect=session.mark_reconnect,
        on_retry=lambda event: session.record_reconnect_diagnostic(
            session.run_dir, event
        ),
        deadline=deadline,
    )

    try:
        capture_context = session.profile_runtime or session.outputs.open_sinks()
        async with capture_context as sinks:
            specs_by_role = session.outputs.dataset_specs_by_role

            def sink_for(role: DatasetRole) -> Any | None:
                spec = specs_by_role.get(role.value)
                return sinks[spec.file_key] if spec is not None else None

            event_sink = sink_for(DatasetRole.PARSED_EVENT)
            snapshot_sink = sink_for(DatasetRole.LEGACY_SNAPSHOT)
            level_sink = sink_for(DatasetRole.LEGACY_LEVEL)
            topbook_sink = sink_for(DatasetRole.TOPBOOK_MAIN)
            checkpoint_sink = sink_for(DatasetRole.TOPBOOK_CHECKPOINT)
            depth_sink = sink_for(DatasetRole.DEPTH_MAIN)
            trade_sink = sink_for(DatasetRole.TRADE)
            lifecycle_sink = sink_for(DatasetRole.LIFECYCLE)
            health_sink = sink_for(DatasetRole.HEALTH)
            async with AsyncKalshiWebSocketClient(
                session.instrument_ids,
                ws_url=ws_url,
                connect_factory=connect_factory,
                auth=auth,
                transport_settings=session.transport_settings,
                retry_budget=retry_budget,
                use_yes_price=session.use_yes_price,
                public_channels=(
                    ("orderbook_delta", "trade", "market_lifecycle_v2")
                    if session.storage_profile is not None
                    else ("orderbook_delta",)
                ),
                on_subscription_start=(
                    session.begin_instrument_subscription_attempt
                    if session.instrument_evidence_tracker is not None
                    else None
                ),
                on_subscription_established=(
                    session.establish_instrument_subscription_attempt
                    if session.instrument_evidence_tracker is not None
                    else None
                ),
            ) as ws:
                connected_at = session.monotonic_ns()
                if session.health_emitter is not None:
                    session.feed_control_scheduler = FeedControlScheduler.from_thresholds(
                        now_monotonic_ns=connected_at,
                        max_message_age_ms=session.supervisor.max_message_age_ms,
                        max_valid_book_age_ms=session.supervisor.max_valid_book_age_ms,
                    )
                for shard in session.health_shards:
                    shard.mark_connected(now_monotonic_ns=connected_at)
                if session.health_emitter is not None and health_sink is not None:
                    connection_observed_at = _utc_now().isoformat()
                    connection_sequence = session.sequence
                    await session.write_health(
                        health_sink,
                        observed_at_utc=connection_observed_at,
                        local_sequence=connection_sequence,
                        now_monotonic_ns=connected_at,
                        cause="connection",
                    )
                iterator = ws.iter_messages(
                    on_reconnect=session.mark_reconnect,
                    max_reconnects=max(
                        0, session.max_reconnects - session.reconnect_count
                    ),
                    fail_on_clean_close_exhausted=True,
                )
                raw_context = (
                    raw_jsonl_path.open("w", encoding="utf-8")
                    if session.outputs.include_raw_jsonl
                    else contextlib.nullcontext(None)
                )
                with raw_context as raw_file:
                    next_message_task: asyncio.Future[dict[str, Any]] | None = None

                    async def maybe_recover_socket(
                        now_monotonic_ns: int,
                        *,
                        recovery_actions: Sequence[FeedRecoveryAction] | None = None,
                    ) -> bool:
                        nonlocal iterator, next_message_task
                        if recovery_actions is None:
                            recovery_actions = session.supervisor.recovery_actions(
                                now_monotonic_ns=now_monotonic_ns,
                                venue="kalshi",
                            )
                        expired = [
                            ticker for ticker, pending in session.pending_snapshot_requests.items()
                            if now_monotonic_ns >= pending[2]
                        ]
                        for ticker in expired:
                            del session.pending_snapshot_requests[ticker]
                        session.targeted_snapshot_response_timeout_count += len(expired)
                        if expired:
                            session.targeted_snapshot_refresh_exhausted_count += 1
                        if not recovery_actions and not expired:
                            return False

                        targeted = not expired and all(
                            action.action == "reconnect_socket"
                            and set(action.reasons) == {"book_integrity"}
                            for action in recovery_actions
                        )
                        snapshot_request_error: BaseException | None = None
                        snapshot_targets: dict[int, list[str]] = {}
                        if targeted:
                            for action in recovery_actions:
                                for ticker in action.instruments:
                                    if ticker in session.pending_snapshot_requests:
                                        continue
                                    state = session.states.get(ticker)
                                    sid = state.sid if state is not None else None
                                    if sid is None:
                                        sid = session.orderbook_subscription_sid
                                    if sid is None:
                                        targeted = False
                                        break
                                    snapshot_targets.setdefault(sid, []).append(ticker)
                            if targeted:
                                try:
                                    for sid, tickers in sorted(snapshot_targets.items()):
                                        unique_tickers = sorted(set(tickers))
                                        await ws.request_snapshot(sid=sid, market_tickers=unique_tickers)
                                        session.snapshot_resync_request_count += 1
                                        session.targeted_snapshot_refresh_count += 1
                                        session.targeted_snapshot_market_count += len(unique_tickers)
                                        # Never renew a deadline when a scheduler observes the same condition.
                                        for ticker in unique_tickers:
                                            session.pending_snapshot_requests[ticker] = (
                                                session.reconnect_count, sid,
                                                session.monotonic_ns() + 30_000_000_000,
                                            )
                                        if session.runtime_projection_recorder is not None:
                                            session.runtime_projection_recorder.record_recovery_actions(
                                                actions=[
                                                    FeedRecoveryAction(
                                                        action="request_snapshot",
                                                        venue="kalshi",
                                                        shard_id=capture_shard_id,
                                                        reasons=("book_integrity",),
                                                        instruments=tuple(
                                                            unique_tickers
                                                        ),
                                                    )
                                                ],
                                                observed_at_utc=_utc_now().isoformat(),
                                            )
                                except (
                                    ConnectionClosed,
                                    OSError,
                                    RuntimeError,
                                    asyncio.TimeoutError,
                                ) as exc:
                                    snapshot_request_error = exc
                                    session.targeted_snapshot_refresh_failure_count += 1
                                else:
                                    return False

                        if session.reconnect_count >= session.max_reconnects:
                            raise RuntimeError("Kalshi recovery retry budget exhausted")
                        if session.runtime_projection_recorder is not None:
                            session.runtime_projection_recorder.record_recovery_actions(
                                actions=[FeedRecoveryAction(
                                    action="reconnect_socket", venue="kalshi",
                                    shard_id=capture_shard_id,
                                    reasons=("snapshot_response_timeout",) if expired else tuple(
                                        sorted({reason for action in recovery_actions for reason in action.reasons})
                                    ),
                                    instruments=tuple(sorted(expired)),
                                )],
                                observed_at_utc=_utc_now().isoformat(),
                            )
                        if (
                            next_message_task is not None
                            and not next_message_task.done()
                        ):
                            next_message_task.cancel()
                            with contextlib.suppress(asyncio.CancelledError):
                                await next_message_task
                        next_message_task = None
                        retry_budget.last_error = snapshot_request_error
                        retry_budget.retry_context = {
                            "origin": "supervisor",
                            "reason": "snapshot_request_failed"
                            if snapshot_request_error is not None
                            else "recovery_action",
                            "reasons": ["snapshot_response_timeout"]
                            if expired
                            else sorted(
                                {
                                    reason
                                    for action in recovery_actions
                                    for reason in action.reasons
                                }
                            ),
                            "instruments": sorted(
                                set(expired)
                                | {
                                    instrument
                                    for action in recovery_actions
                                    for instrument in action.instruments
                                }
                            ),
                        }
                        await retry_budget.run(
                            ws.reconnect, retry_first=True, immediate_first=True,
                        )
                        session.socket_recovery_count += 1
                        connected_at = session.monotonic_ns()
                        for shard in session.health_shards:
                            if shard.mark_connected(now_monotonic_ns=connected_at):
                                session.pending_health_shard_keys.add(
                                    (shard.venue, shard.shard_id)
                                )
                        iterator = ws.iter_messages(
                            on_reconnect=session.mark_reconnect,
                            max_reconnects=max(
                                0, session.max_reconnects - session.reconnect_count
                            ),
                            fail_on_clean_close_exhausted=True,
                        )
                        return True

                    try:
                        while True:
                            if (
                                session.max_messages is not None
                                and message_count >= session.max_messages
                            ):
                                session.terminal_reason = (
                                    CaptureTerminationReason.MAX_MESSAGES_REACHED
                                )
                                break
                            remaining = None
                            if deadline is not None:
                                remaining = deadline - time.monotonic()
                                if remaining <= 0:
                                    session.terminal_reason = (
                                        CaptureTerminationReason.DEADLINE_REACHED
                                    )
                                    break
                            if next_message_task is None:
                                next_message_task = asyncio.ensure_future(
                                    iterator.__anext__()
                                )
                            try:
                                wait_started_ns = session.monotonic_ns()
                                done, _ = await asyncio.wait(
                                    {next_message_task},
                                    timeout=session.message_wait_timeout(
                                        remaining,
                                        now_monotonic_ns=wait_started_ns,
                                    ),
                                )
                                if not done:
                                    raise asyncio.TimeoutError
                                message = next_message_task.result()
                                next_message_task = None
                            except asyncio.TimeoutError:
                                observed_at_utc = _utc_now().isoformat()
                                now_monotonic_ns = session.monotonic_ns()
                                if session.feed_control_scheduler is not None:
                                    await session.run_incremental_feed_control(
                                        health_sink=health_sink,
                                        recover_socket=maybe_recover_socket,
                                        observed_at_utc=observed_at_utc,
                                        now_monotonic_ns=now_monotonic_ns,
                                        post_message=False,
                                    )
                                else:
                                    session.supervisor.invalidate_stale(
                                        now_monotonic_ns=now_monotonic_ns
                                    )
                                    health_sequence = (
                                        session.sequence + 1
                                        if session.storage_profile is not None
                                        else session.sequence
                                    )
                                    emitted_health = await session.write_health(
                                        health_sink,
                                        observed_at_utc=observed_at_utc,
                                        local_sequence=health_sequence,
                                        now_monotonic_ns=now_monotonic_ns,
                                    )
                                    if (
                                        emitted_health
                                        and session.storage_profile is not None
                                    ):
                                        session.sequence = health_sequence
                                await session.emit_scheduled_capture(
                                    checkpoint_sink=checkpoint_sink,
                                    observed_at_utc=observed_at_utc,
                                    now_monotonic_ns=now_monotonic_ns,
                                )
                                if session.feed_control_scheduler is None:
                                    await maybe_recover_socket(now_monotonic_ns)
                                continue
                            except StopAsyncIteration:
                                session.terminal_reason = (
                                    CaptureTerminationReason.ITERATOR_EXHAUSTED
                                )
                                break

                            message_count += 1
                            session.sequence += 1
                            received_at = time.time()
                            received_at_utc = (
                                isoformat_source_timestamp(
                                    received_at, epoch_unit="seconds"
                                )
                                or ""
                            )
                            received_at_monotonic_ns = session.monotonic_ns()
                            if session.feed_control_scheduler is None:
                                session.supervisor.invalidate_stale(
                                    now_monotonic_ns=received_at_monotonic_ns
                                )
                            if raw_file is not None:
                                raw_file.write(
                                    json.dumps(
                                        {
                                            "sequence": session.sequence,
                                            "received_at": received_at,
                                            "received_at_utc": received_at_utc,
                                            "message": message,
                                        },
                                        ensure_ascii=True,
                                        sort_keys=True,
                                    )
                                    + "\n"
                                )
                            if event_sink is not None:
                                await event_sink.write(
                                    _event_row(
                                        session.sequence,
                                        received_at,
                                        received_at_utc,
                                        message,
                                    )
                                )
                                session.event_count += 1

                            level_rows = [
                                row
                                for row in _level_rows(
                                    session.sequence,
                                    received_at,
                                    received_at_utc,
                                    message,
                                )
                                if row.get("market_ticker") in requested_tickers
                            ]
                            if message.get("type") == "subscribed":
                                payload = message.get("msg")
                                if isinstance(payload, Mapping) and payload.get("channel") == "orderbook_delta":
                                    session.orderbook_subscription_sid = _parse_int(payload.get("sid"))
                            sequence_gap_sid = session.subscription_sequence_gap_sid(
                                message
                            )
                            snapshots = [
                                snapshot
                                for snapshot in apply_kalshi_orderbook_message(
                                    session.states,
                                    message,
                                    use_yes_price=session.use_yes_price,
                                )
                                if snapshot.market_ticker in requested_tickers
                            ]
                            if not snapshots:
                                raw_payload = message.get("msg")
                                payload = (
                                    raw_payload
                                    if isinstance(raw_payload, Mapping)
                                    else message
                                )
                                message_ticker = str(payload.get("market_ticker") or "")
                                message_shard = session.health_shard_for_instrument(
                                    message_ticker
                                )
                                if message_shard.record_message(
                                    now_monotonic_ns=received_at_monotonic_ns,
                                    instrument=(
                                        message_ticker
                                        if message_ticker in requested_tickers
                                        else None
                                    ),
                                ):
                                    session.pending_health_shard_keys.add(
                                        (message_shard.venue, message_shard.shard_id)
                                    )
                            if sequence_gap_sid is not None:
                                affected_markets = tuple(sorted(
                                    ticker for ticker in requested_tickers
                                    if session.states[ticker].sid == sequence_gap_sid
                                ))
                                snapshots_by_market = {
                                    snapshot.market_ticker: snapshot
                                    for snapshot in snapshots
                                }
                                for market_ticker in affected_markets:
                                    state = session.states.get(market_ticker)
                                    if state is None:
                                        continue
                                    current = snapshots_by_market.get(market_ticker)
                                    # This snapshot already replaces its own book; the
                                    # gap still invalidates siblings on the same SID.
                                    if current is None or current.event_type != "orderbook_snapshot":
                                        state.mark_sequence_gap()
                                    snapshots_by_market[market_ticker] = state.snapshot(
                                        event_type=(
                                            current.event_type
                                            if current is not None
                                            else "sequence_gap"
                                        )
                                    )
                                snapshots = [
                                    snapshots_by_market[market_ticker]
                                    for market_ticker in affected_markets
                                    if market_ticker in snapshots_by_market
                                ]
                                session.sequence_gap_count += 1
                                active_sequence_gap_markets.update(affected_markets)
                                affected_by_shard: dict[
                                    tuple[str, str], tuple[FeedShardHealth, list[str]]
                                ] = {}
                                for market_ticker in affected_markets:
                                    shard = session.health_shard_for_instrument(
                                        market_ticker
                                    )
                                    key = (shard.venue, shard.shard_id)
                                    if key not in affected_by_shard:
                                        affected_by_shard[key] = (shard, [])
                                    affected_by_shard[key][1].append(market_ticker)
                                for shard, shard_markets in affected_by_shard.values():
                                    if shard.record_sequence_gap(
                                        instruments=shard_markets
                                    ):
                                        session.pending_health_shard_keys.add(
                                            (shard.venue, shard.shard_id)
                                        )

                            if (
                                session.tape_producer is not None
                                and session.profile_runtime is not None
                            ):
                                session.tape_producer.observe(
                                    message=message,
                                    states=session.states,
                                    received_at_utc=canonical_utc(received_at_utc),
                                    received_at_monotonic_ns=received_at_monotonic_ns,
                                    local_sequence=session.sequence,
                                ).write_to(session.profile_runtime.coordinator)

                            if observation_producer is not None:
                                try:
                                    observations = observation_producer.kalshi(
                                        message,
                                        CaptureCoordinate(
                                            session.run_dir.name,
                                            capture_shard_id,
                                            received_at_utc,
                                            received_at_monotonic_ns,
                                            session.sequence,
                                            3,
                                        ),
                                    )
                                except ObservationValidationError:
                                    observation_payload = message.get("msg")
                                    message_ticker = str(
                                        observation_payload.get("market_ticker") or ""
                                        if isinstance(observation_payload, Mapping)
                                        else ""
                                    )
                                    error_shard = session.health_shard_for_instrument(
                                        message_ticker
                                    )
                                    if error_shard.record_error(
                                        "observation_validation"
                                    ):
                                        session.pending_health_shard_keys.add(
                                            (error_shard.venue, error_shard.shard_id)
                                        )
                                else:
                                    if trade_sink is not None:
                                        for row in observations.trades:
                                            await trade_sink.write(row)
                                    if lifecycle_sink is not None:
                                        for row in observations.lifecycle:
                                            await lifecycle_sink.write(row)

                            for row in level_rows:
                                if row.get("market_ticker") not in requested_tickers:
                                    continue
                                if level_sink is not None:
                                    await level_sink.write(row)
                                    session.level_count += 1

                            resync_boundary_due = False
                            for snapshot in snapshots:
                                session.observe_snapshot_response(snapshot, now_monotonic_ns=received_at_monotonic_ns)
                                flags = set(snapshot.quality_flags)
                                snapshot_shard = session.health_shard_for_instrument(
                                    snapshot.market_ticker
                                )
                                if "seq_gap" in flags:
                                    if (
                                        snapshot.market_ticker
                                        not in active_sequence_gap_markets
                                    ):
                                        session.sequence_gap_count += 1
                                        active_sequence_gap_markets.add(
                                            snapshot.market_ticker
                                        )
                                        if snapshot_shard.record_sequence_gap(
                                            instrument=snapshot.market_ticker
                                        ):
                                            session.pending_health_shard_keys.add(
                                                (
                                                    snapshot_shard.venue,
                                                    snapshot_shard.shard_id,
                                                )
                                            )
                                else:
                                    if (
                                        snapshot.market_ticker
                                        in active_sequence_gap_markets
                                    ):
                                        if snapshot_shard.record_resync(
                                            now_monotonic_ns=received_at_monotonic_ns,
                                            instrument=snapshot.market_ticker,
                                        ):
                                            session.pending_health_shard_keys.add(
                                                (
                                                    snapshot_shard.venue,
                                                    snapshot_shard.shard_id,
                                                )
                                            )
                                        resync_boundary_due = True
                                    active_sequence_gap_markets.discard(
                                        snapshot.market_ticker
                                    )
                                if snapshot_shard.record_book(
                                    valid_state=bool(snapshot.valid_state),
                                    book_integrity_valid=snapshot.book_integrity_valid,
                                    initial_snapshot_received=snapshot.initial_snapshot_received,
                                    now_monotonic_ns=received_at_monotonic_ns,
                                    instrument=snapshot.market_ticker,
                                    quality_flags=flags,
                                ):
                                    session.pending_health_shard_keys.add(
                                        (
                                            snapshot_shard.venue,
                                            snapshot_shard.shard_id,
                                        )
                                    )
                                snapshot_payload = snapshot.as_dict()
                                if snapshot_sink is not None:
                                    await snapshot_sink.write(
                                        _snapshot_row(
                                            session.sequence,
                                            received_at,
                                            received_at_utc,
                                            snapshot_payload,
                                        )
                                    )
                                    session.snapshot_count += 1
                                if session.instrument_evidence_tracker is not None:
                                    session.instrument_evidence_tracker.record_book(
                                        snapshot.market_ticker, observed_at_utc=received_at_utc,
                                        snapshot_received=snapshot.event_type == 'orderbook_snapshot',
                                        book_integrity_valid=snapshot.book_integrity_valid,
                                    )
                                if snapshot.valid_state:
                                    session.instruments_with_snapshots.add(
                                        snapshot.market_ticker
                                    )
                                    if session.instrument_evidence_tracker is not None:
                                        session.instrument_evidence_tracker.record_valid_snapshot(
                                            snapshot.market_ticker,
                                            observed_at_utc=received_at_utc,
                                        )
                                else:
                                    session.instruments_with_invalid_snapshots.add(
                                        snapshot.market_ticker
                                    )
                                for topbook in kalshi_ws_snapshot_to_topbook(
                                    snapshot_payload,
                                    collector_run_id=session.run_dir.name,
                                    received_at_utc=canonical_utc(received_at_utc),
                                    received_at_monotonic_ns=received_at_monotonic_ns,
                                    local_sequence=session.sequence,
                                ):
                                    topbook = add_book_integrity(topbook, integrity=snapshot.book_integrity_valid, selection=session.storage_profile)
                                    instrument_id = topbook.get("instrument_id")
                                    topbook_emission = None
                                    if session.topbook_tracker is not None:
                                        topbook_emission = session.topbook_tracker.observe(
                                            topbook,
                                            now_monotonic_ns=received_at_monotonic_ns,
                                            force_main=dense_topbook_emission,
                                        )
                                    if instrument_id is not None:
                                        session.latest_topbooks[str(instrument_id)] = (
                                            dict(topbook_emission.row)
                                            if topbook_emission is not None
                                            and topbook_emission.role
                                            is DatasetRole.TOPBOOK_MAIN
                                            else topbook
                                        )
                                    if session.topbook_tracker is None:
                                        if topbook_sink is not None:
                                            await topbook_sink.write(topbook)
                                    elif topbook_emission is not None:
                                        selected_sink = (
                                            topbook_sink
                                            if topbook_emission.role
                                            is DatasetRole.TOPBOOK_MAIN
                                            else checkpoint_sink
                                        )
                                        if selected_sink is not None:
                                            await selected_sink.write(
                                                dict(topbook_emission.row)
                                            )
                                    session.quality_counter.update(
                                        topbook.get("quality_flags") or []
                                    )
                                    if instrument_id is not None:
                                        session.instrument_counter.update(
                                            [str(instrument_id)]
                                        )
                                    if (
                                        session.topbook_tracker is None
                                        or topbook_emission is not None
                                    ):
                                        session.topbook_count += 1
                                    if (
                                        session.compact_control_producer is not None
                                        and session.profile_runtime is not None
                                        and topbook_emission is not None
                                        and topbook_emission.role
                                        is DatasetRole.TOPBOOK_MAIN
                                    ):
                                        # Controls must bind only to main rows that
                                        # are written into the same capture buffer.
                                        control_topbook = dict(topbook_emission.row)
                                        session.compact_control_producer.observe_topbook(
                                            row=control_topbook,
                                            coordinate=CaptureCoordinate(
                                                session.run_dir.name,
                                                capture_shard_id,
                                                str(control_topbook["received_at_utc"]),
                                                int(
                                                    control_topbook[
                                                        "received_at_monotonic_ns"
                                                    ]
                                                ),
                                                int(control_topbook["local_sequence"]),
                                                4,
                                            ),
                                            venue_book_id=str(
                                                control_topbook["instrument_id"]
                                            ),
                                            venue_market_id=str(
                                                snapshot.market_id
                                                or snapshot.market_ticker
                                            ),
                                            venue_sequence=control_topbook.get(
                                                "venue_sequence"
                                            ),
                                        ).write_to(session.profile_runtime.coordinator)
                                state = session.states.get(snapshot.market_ticker)
                                if (
                                    depth_sink is not None
                                    and state is not None
                                    and _should_emit_canonical_depth(snapshot)
                                ):
                                    depth_received_at = canonical_utc(received_at_utc)
                                    # Prefer a uniquified topbook clock for this market
                                    # when dense emission advanced receive timestamps.
                                    for outcome in ("YES", "NO"):
                                        instrument = (
                                            f"{snapshot.market_ticker}:{outcome}"
                                        )
                                        latest = session.latest_topbooks.get(instrument)
                                        if latest is None:
                                            continue
                                        candidate = str(
                                            latest.get("received_at_utc") or ""
                                        )
                                        if candidate > depth_received_at:
                                            depth_received_at = candidate

                                    for depth in _canonical_depth_rows_from_state(
                                        run_id=session.run_dir.name,
                                        sequence=session.sequence,
                                        received_at_utc=depth_received_at,
                                        snapshot=snapshot,
                                        state=state,
                                    ):
                                        await depth_sink.write(add_book_integrity(depth, integrity=snapshot.book_integrity_valid, selection=session.storage_profile))
                                        session.quality_counter.update(
                                            depth.get("quality_flags") or []
                                        )
                                        session.depth_count += 1
                            boundary_at_utc = (
                                _utc_now() + timedelta(milliseconds=1)
                            ).isoformat()
                            if startup_boundary_pending and session.latest_topbooks:
                                if session.emit_topbook_boundary(
                                    reason="startup",
                                    observed_at_utc=boundary_at_utc,
                                    now_monotonic_ns=received_at_monotonic_ns + 1,
                                ):
                                    startup_boundary_pending = False
                            if resync_boundary_due:
                                session.emit_topbook_boundary(
                                    reason="resync",
                                    observed_at_utc=(
                                        _utc_now() + timedelta(milliseconds=2)
                                    ).isoformat(),
                                    now_monotonic_ns=received_at_monotonic_ns + 2,
                                )
                            if session.feed_control_scheduler is not None:
                                await session.run_incremental_feed_control(
                                    health_sink=health_sink,
                                    recover_socket=maybe_recover_socket,
                                    observed_at_utc=received_at_utc,
                                    now_monotonic_ns=received_at_monotonic_ns,
                                    post_message=True,
                                    allow_recovery=(
                                        session.max_messages is None
                                        or message_count < session.max_messages
                                    ),
                                )
                            else:
                                await session.write_health(
                                    health_sink,
                                    observed_at_utc=received_at_utc,
                                    local_sequence=session.sequence,
                                    now_monotonic_ns=received_at_monotonic_ns,
                                )
                            await session.emit_scheduled_capture(
                                checkpoint_sink=checkpoint_sink,
                                observed_at_utc=received_at_utc,
                                now_monotonic_ns=received_at_monotonic_ns,
                            )
                            if session.feed_control_scheduler is None and (
                                session.max_messages is None
                                or message_count < session.max_messages
                            ):
                                await maybe_recover_socket(received_at_monotonic_ns)
                    finally:
                        if (
                            next_message_task is not None
                            and not next_message_task.done()
                        ):
                            next_message_task.cancel()
                            with contextlib.suppress(asyncio.CancelledError):
                                await next_message_task
            capture_phase = "finalization"
            if session.health_emitter is not None and health_sink is not None:
                terminal_health_at = session.capture_clock.reserve(
                    _utc_now()
                ).isoformat()
                terminal_health_now = session.monotonic_ns()
                terminal_rows = session.supervisor.feed_health_rows(
                    now_monotonic_ns=terminal_health_now,
                    observed_at_utc=terminal_health_at,
                    local_sequence=session.sequence + 1,
                )
                if session.feed_control_scheduler is not None:
                    session.feed_control_scheduler.record_health_rows(
                        detailed_rows=len(terminal_rows)
                    )
                prepared_terminal_health = session.health_emitter.prepare(
                    terminal_rows,
                    now_monotonic_ns=terminal_health_now,
                    cause="terminal",
                )
                for emission in prepared_terminal_health.emissions:
                    await health_sink.write(dict(emission.row))
                    session.health_observation_count += 1
                session.health_emitter.commit(prepared_terminal_health)
                # Decide completeness BEFORE the successful terminal control is
                # durable; raising here routes through the failure handler which
                # emits reason="failed" and writes a partial/failed manifest.
            completeness_report = session.finalize_capture_completeness()
            if not completeness_report.ok:
                raise CaptureCompletenessError(
                    "kalshi capture completeness failed: "
                    + "; ".join(completeness_report.reasons)
                )
            terminal_boundary_at = session.capture_clock.reserve(
                _utc_now() + timedelta(milliseconds=1)
            )
            session.emit_topbook_boundary(
                reason="terminal",
                observed_at_utc=terminal_boundary_at.isoformat(),
                now_monotonic_ns=session.monotonic_ns(),
            )
            if (
                session.tape_producer is not None
                and session.profile_runtime is not None
            ):
                terminal_at = session.after_topbook_boundary(
                    _utc_now() + timedelta(milliseconds=2)
                ).isoformat()
                terminal_emission = session.tape_producer.ended(
                    states=session.states,
                    received_at_utc=terminal_at,
                    received_at_monotonic_ns=session.monotonic_ns(),
                    local_sequence=session.sequence + 1,
                    reason="completed",
                )
                terminal_control_staged = bool(terminal_emission.controls)
                terminal_emission.write_to(session.profile_runtime.coordinator)
            elif (
                session.compact_control_producer is not None
                and session.profile_runtime is not None
            ):
                terminal_at = session.after_topbook_boundary(
                    _utc_now() + timedelta(milliseconds=1)
                ).isoformat()
                terminal_emission = session.compact_control_producer.ended(
                    books={
                        f"{book_id}:{outcome}": str(state.market_id or book_id)
                        for book_id, state in session.states.items()
                        for outcome in ("YES", "NO")
                    },
                    received_at_utc=terminal_at,
                    received_at_monotonic_ns=session.monotonic_ns(),
                    local_sequence=session.sequence + 1,
                    reason="completed",
                )
                terminal_control_staged = bool(terminal_emission.controls)
                terminal_emission.write_to(session.profile_runtime.coordinator)
            session.stage_instrument_evidence()
    except (Exception, asyncio.CancelledError) as exc:
        if not isinstance(exc, CaptureCompletenessError):
            if isinstance(exc, asyncio.CancelledError):
                session.terminal_reason = CaptureTerminationReason.CANCELLED
            elif isinstance(exc, WebSocketDeadlineExceeded):
                session.terminal_reason = CaptureTerminationReason.DEADLINE_REACHED
            elif capture_phase == "finalization":
                session.terminal_reason = CaptureTerminationReason.FINALIZATION_ERROR
                # ConnectionError is an OSError subclass, but here it represents
                # websocket transport exhaustion rather than a persistence failure.
            elif isinstance(exc, ConnectionError) or exc is retry_budget.last_error:
                session.terminal_reason = CaptureTerminationReason.STREAM_ERROR
            elif isinstance(exc, OSError):
                session.terminal_reason = CaptureTerminationReason.PERSISTENCE_ERROR
            else:
                session.terminal_reason = CaptureTerminationReason.STREAM_ERROR
            session.completeness_report_holder.clear()
        session.stage_instrument_evidence()
        session.finalize_capture_completeness()
        for shard in session.health_shards:
            shard.record_error(type(exc).__name__)
        if terminal_control_staged:
            ended_at = _utc_now()
            if session.profile_runtime is not None:
                await session.profile_runtime.force_finalize_async()
        else:
            error_health_at = session.capture_clock.reserve(_utc_now()).isoformat()
            error_health_rows = session.supervisor.feed_health_rows(
                now_monotonic_ns=session.monotonic_ns(),
                observed_at_utc=error_health_at,
                local_sequence=session.sequence + 1,
            )
            if session.feed_control_scheduler is not None:
                session.feed_control_scheduler.record_health_rows(
                    detailed_rows=len(error_health_rows)
                )
            emitted_error_health_rows = error_health_rows
            prepared_error_health: PreparedHealthEmissions | None = None
            if session.health_emitter is not None:
                prepared_error_health = session.health_emitter.prepare(
                    error_health_rows,
                    now_monotonic_ns=session.monotonic_ns(),
                    cause="error",
                )
                emitted_error_health_rows = [
                    dict(emission.row) for emission in prepared_error_health.emissions
                ]
            if session.profile_runtime is not None:
                health_spec = session.profile_runtime.specs_by_role.get(
                    DatasetRole.HEALTH.value
                )
                if health_spec is not None:
                    for row in emitted_error_health_rows:
                        session.profile_runtime.coordinator.add(
                            DatasetRole.HEALTH.value, row
                        )
            else:
                write_feed_health_parquet(
                    feed_health_path,
                    emitted_error_health_rows,
                )
            if prepared_error_health is not None:
                assert session.health_emitter is not None
                session.health_emitter.commit(prepared_error_health)
            session.health_observation_count += len(emitted_error_health_rows)
            session.record_runtime_feed_projection(
                error_health_rows,
                observed_at_utc=error_health_at,
            )
            terminal_boundary_at = session.capture_clock.reserve(
                _utc_now() + timedelta(milliseconds=1)
            )
            session.emit_topbook_boundary(
                reason="terminal",
                observed_at_utc=terminal_boundary_at.isoformat(),
                now_monotonic_ns=session.monotonic_ns(),
            )
            ended_at = session.after_topbook_boundary(
                _utc_now() + timedelta(milliseconds=1)
            )
            if session.profile_runtime is not None:
                if session.tape_producer is not None:
                    session.tape_producer.ended(
                        states=session.states,
                        received_at_utc=ended_at.isoformat(),
                        received_at_monotonic_ns=session.monotonic_ns(),
                        local_sequence=session.sequence + 1,
                        reason="failed",
                    ).write_to(session.profile_runtime.coordinator)
                elif session.compact_control_producer is not None:
                    session.compact_control_producer.ended(
                        books={
                            f"{book_id}:{outcome}": str(state.market_id or book_id)
                            for book_id, state in session.states.items()
                            for outcome in ("YES", "NO")
                        },
                        received_at_utc=ended_at.isoformat(),
                        received_at_monotonic_ns=session.monotonic_ns(),
                        local_sequence=session.sequence + 1,
                        reason="failed",
                    ).write_to(session.profile_runtime.coordinator)
                await session.profile_runtime.force_finalize_async()
        profile_evidence = bool(
            session.profile_runtime is not None
            and any(session.profile_runtime.coordinator.row_counts.values())
        )
        status = (
            "partial"
            if session.event_count
            or session.snapshot_count
            or session.level_count
            or profile_evidence
            else "failed"
        )
        failed_manifest = session.make_manifest(
            status=status, ended_at=ended_at, error=exc
        )
        if session.profile_runtime is not None:
            write_json_atomic_fsync(manifest_path, failed_manifest)
            session.profile_runtime.mark_finalized()
        else:
            write_manifest(manifest_path, failed_manifest)
        raise

    if (
        session.profile_runtime is None
        and session.event_count == 0
        and session.health_observation_count == 0
    ):
        observed_at_utc = _utc_now().isoformat()
        final_health_rows = session.supervisor.feed_health_rows(
            now_monotonic_ns=session.monotonic_ns(),
            observed_at_utc=observed_at_utc,
            local_sequence=session.sequence,
        )
        write_feed_health_parquet(
            feed_health_path,
            final_health_rows,
            append=False,
        )
        session.health_observation_count += len(final_health_rows)
        session.record_runtime_feed_projection(
            final_health_rows, observed_at_utc=observed_at_utc
        )
    manifest = session.make_manifest(status="success", ended_at=_utc_now())
    if session.profile_runtime is not None:
        write_json_atomic_fsync(manifest_path, manifest)
        session.profile_runtime.mark_finalized()
    else:
        write_manifest(manifest_path, manifest)
    return manifest


__all__ = [
    "DEFAULT_KALSHI_ORDER_BOOK_STREAM_ROOT",
    "stream_kalshi_order_book_data",
]
