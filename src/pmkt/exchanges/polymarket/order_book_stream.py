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

from pmkt.data.canonical import FEED_HEALTH_SCHEMA_VERSION
from pmkt.data.manifests import build_run_manifest, current_git_commit, write_manifest
from pmkt.data.normalize_books import polymarket_ws_snapshot_to_topbook
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
from pmkt.exchanges.ws_transport import WebSocketTransportSettings
from pmkt.streaming.profile_runtime import ProfileCaptureRuntime, create_profile_runtime
from pmkt.streaming.storage_backends import CaptureStorageBackend
from pmkt.streaming.profiles import (
    DatasetRole,
    StorageProfileSelection,
    TopbookEmissionMode,
    resolve_dataset_specs,
)
from pmkt.streaming.tape import CaptureCoordinate, canonical_utc
from pmkt.streaming.tape_producers import (
    CompactValidityProducer,
    PolymarketTapeProducer,
)
from pmkt.streaming.topbook_emission import TopbookEmissionTracker
from pmkt.exchanges.polymarket.ws import (
    AsyncMarketWebSocketClient,
    ConnectFactory,
    MarketBookState,
    POLYMARKET_BOOK_EVENT_TYPES,
    apply_market_message,
    market_subscription_payload,
)

DEFAULT_ORDER_BOOK_STREAM_ROOT = Path("generated/order_book_streams")
_DEPTH_SNAPSHOT_EVENT_TYPES = {"book", "price_change"}

EVENT_COLUMNS = [
    "sequence",
    "received_at",
    "received_at_utc",
    "event_type",
    "asset_id",
    "market",
    "exchange_timestamp",
    "exchange_timestamp_seconds",
    "raw_json",
]

SNAPSHOT_COLUMNS = [
    "sequence",
    "received_at",
    "received_at_utc",
    "event_type",
    "asset_id",
    "market",
    "exchange_timestamp",
    "exchange_timestamp_seconds",
    "exchange_datetime_utc",
    "best_bid",
    "best_ask",
    "spread",
    "midpoint",
    "bid_depth",
    "ask_depth",
    "last_trade_price",
    "last_trade_size",
    "last_trade_side",
    "tick_size",
    "valid_state",
    "quality_flags",
    "initial_snapshot_received",
    "last_book_hash",
    "quote_age_ms",
    "reconnect_count",
]

LEVEL_COLUMNS = [
    "sequence",
    "received_at",
    "received_at_utc",
    "event_type",
    "asset_id",
    "market",
    "exchange_timestamp",
    "exchange_timestamp_seconds",
    "side",
    "price",
    "size",
    "level_index",
    "is_delta",
    "best_bid",
    "best_ask",
    "hash",
]

EVENT_SCHEMA = pa.schema(
    [
        ("sequence", pa.int64()),
        ("received_at", pa.float64()),
        ("received_at_utc", pa.string()),
        ("event_type", pa.string()),
        ("asset_id", pa.string()),
        ("market", pa.string()),
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
        ("asset_id", pa.string()),
        ("market", pa.string()),
        ("exchange_timestamp", pa.string()),
        ("exchange_timestamp_seconds", pa.float64()),
        ("exchange_datetime_utc", pa.string()),
        ("best_bid", pa.float64()),
        ("best_ask", pa.float64()),
        ("spread", pa.float64()),
        ("midpoint", pa.float64()),
        ("bid_depth", pa.int64()),
        ("ask_depth", pa.int64()),
        ("last_trade_price", pa.float64()),
        ("last_trade_size", pa.float64()),
        ("last_trade_side", pa.string()),
        ("tick_size", pa.float64()),
        ("valid_state", pa.bool_()),
        ("quality_flags", pa.string()),
        ("initial_snapshot_received", pa.bool_()),
        ("last_book_hash", pa.string()),
        ("quote_age_ms", pa.int64()),
        ("reconnect_count", pa.int64()),
    ]
)

LEVEL_SCHEMA = pa.schema(
    [
        ("sequence", pa.int64()),
        ("received_at", pa.float64()),
        ("received_at_utc", pa.string()),
        ("event_type", pa.string()),
        ("asset_id", pa.string()),
        ("market", pa.string()),
        ("exchange_timestamp", pa.string()),
        ("exchange_timestamp_seconds", pa.float64()),
        ("side", pa.string()),
        ("price", pa.float64()),
        ("size", pa.float64()),
        ("level_index", pa.int64()),
        ("is_delta", pa.bool_()),
        ("best_bid", pa.float64()),
        ("best_ask", pa.float64()),
        ("hash", pa.string()),
    ]
)

CANONICAL_TOPBOOK_SCHEMA = arrow_schema(get_table_spec(TOPBOOK_SCHEMA_VERSION))
CANONICAL_DEPTH_SCHEMA = arrow_schema(get_table_spec(DEPTH_SCHEMA_VERSION))

STREAM_DATASETS = (
    StreamDatasetSpec(
        "events_parquet",
        "events.parquet",
        EVENT_SCHEMA,
        schema_version="legacy.polymarket.parsed_event.v1",
        role=DatasetRole.PARSED_EVENT.value,
    ),
    StreamDatasetSpec(
        "snapshots_parquet",
        "snapshots.parquet",
        SNAPSHOT_SCHEMA,
        schema_version="legacy.polymarket.snapshot.v1",
        role=DatasetRole.LEGACY_SNAPSHOT.value,
    ),
    StreamDatasetSpec(
        "order_book_levels_parquet",
        "order_book_levels.parquet",
        LEVEL_SCHEMA,
        schema_version="legacy.polymarket.level.v1",
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


def _normalize_asset_ids(asset_ids: Sequence[str]) -> list[str]:
    payload = market_subscription_payload(asset_ids)
    return [str(asset_id) for asset_id in payload["assets_ids"]]


def _event_asset_id(message: dict[str, Any]) -> str | None:
    asset_id = message.get("asset_id")
    if asset_id is not None:
        return str(asset_id)
    changes = message.get("price_changes")
    if isinstance(changes, list) and len(changes) == 1 and isinstance(changes[0], dict):
        change_asset = changes[0].get("asset_id")
        if change_asset is not None:
            return str(change_asset)
    return None


def _event_row(
    sequence: int,
    received_at: float,
    received_at_utc: str,
    message: dict[str, Any],
) -> dict[str, Any]:
    exchange_ts = message.get("timestamp")
    exchange_seconds = _timestamp_seconds(exchange_ts, unit="milliseconds")
    return {
        "sequence": sequence,
        "received_at": received_at,
        "received_at_utc": received_at_utc,
        "event_type": _as_str(message.get("event_type") or message.get("type")),
        "asset_id": _event_asset_id(message),
        "market": _as_str(message.get("market")),
        "exchange_timestamp": _as_str(exchange_ts),
        "exchange_timestamp_seconds": exchange_seconds,
        "raw_json": _raw_json(message),
    }


def _level_float(payload: dict[str, Any], key: str) -> float | None:
    return _parse_float(payload.get(key))


def _book_level_rows(
    sequence: int,
    received_at: float,
    received_at_utc: str,
    message: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    exchange_ts = message.get("timestamp")
    exchange_seconds = _timestamp_seconds(exchange_ts, unit="milliseconds")
    asset_id = str(message.get("asset_id") or "")
    market = message.get("market")
    for side, key in (("BUY", "bids"), ("SELL", "asks")):
        levels = message.get(key)
        if not isinstance(levels, list):
            continue
        for index, level in enumerate(levels):
            if isinstance(level, dict):
                price = _level_float(level, "price")
                size = _level_float(level, "size")
                level_hash = level.get("hash")
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                price = _parse_float(level[0])
                size = _parse_float(level[1])
                level_hash = None
            else:
                continue
            rows.append(
                {
                    "sequence": sequence,
                    "received_at": received_at,
                    "received_at_utc": received_at_utc,
                    "event_type": "book",
                    "asset_id": asset_id,
                    "market": _as_str(market),
                    "exchange_timestamp": _as_str(exchange_ts),
                    "exchange_timestamp_seconds": exchange_seconds,
                    "side": side,
                    "price": price,
                    "size": size,
                    "level_index": index,
                    "is_delta": False,
                    "best_bid": None,
                    "best_ask": None,
                    "hash": _as_str(level_hash),
                }
            )
    return rows


def _price_change_level_rows(
    sequence: int,
    received_at: float,
    received_at_utc: str,
    message: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    changes = message.get("price_changes")
    if not isinstance(changes, list):
        return rows
    exchange_ts = message.get("timestamp")
    exchange_seconds = _timestamp_seconds(exchange_ts, unit="milliseconds")
    market = message.get("market")
    for index, change in enumerate(changes):
        if not isinstance(change, dict):
            continue
        rows.append(
            {
                "sequence": sequence,
                "received_at": received_at,
                "received_at_utc": received_at_utc,
                "event_type": "price_change",
                "asset_id": str(change.get("asset_id") or ""),
                "market": _as_str(market),
                "exchange_timestamp": _as_str(exchange_ts),
                "exchange_timestamp_seconds": exchange_seconds,
                "side": _as_str(change.get("side")),
                "price": _level_float(change, "price"),
                "size": _level_float(change, "size"),
                "level_index": index,
                "is_delta": True,
                "best_bid": _level_float(change, "best_bid"),
                "best_ask": _level_float(change, "best_ask"),
                "hash": _as_str(change.get("hash")),
            }
        )
    return rows


def _level_rows(
    sequence: int,
    received_at: float,
    received_at_utc: str,
    message: dict[str, Any],
) -> list[dict[str, Any]]:
    event_type = message.get("event_type")
    if event_type == "book":
        return _book_level_rows(sequence, received_at, received_at_utc, message)
    if event_type == "price_change":
        return _price_change_level_rows(sequence, received_at, received_at_utc, message)
    return []


def _canonical_depth_rows_from_state(
    *,
    run_id: str,
    sequence: int,
    received_at_utc: str,
    snapshot: Any,
    state: MarketBookState,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    flags = list(snapshot.quality_flags)
    for side, levels in (
        ("bid", sorted(state.bids.items(), reverse=True)),
        ("ask", sorted(state.asks.items())),
    ):
        cumulative = 0.0
        for index, (price, size) in enumerate(levels):
            cumulative += size
            rows.append(
                depth_row(
                    collector_run_id=run_id,
                    exchange="polymarket",
                    venue_market_id=state.market,
                    instrument_id=state.asset_id,
                    source="ws",
                    received_at_utc=canonical_utc(received_at_utc),
                    exchange_ts_utc=snapshot.datetime_utc,
                    local_sequence=sequence,
                    book_hash=snapshot.last_book_hash,
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
        "asset_id": _as_str(snapshot.get("asset_id")),
        "market": _as_str(snapshot.get("market")),
        "exchange_timestamp": _as_str(snapshot.get("timestamp")),
        "exchange_timestamp_seconds": snapshot.get("timestamp_seconds"),
        "exchange_datetime_utc": _as_str(snapshot.get("datetime_utc")),
        "best_bid": snapshot.get("best_bid"),
        "best_ask": snapshot.get("best_ask"),
        "spread": snapshot.get("spread"),
        "midpoint": snapshot.get("midpoint"),
        "bid_depth": _parse_int(snapshot.get("bid_depth")),
        "ask_depth": _parse_int(snapshot.get("ask_depth")),
        "last_trade_price": snapshot.get("last_trade_price"),
        "last_trade_size": snapshot.get("last_trade_size"),
        "last_trade_side": _as_str(snapshot.get("last_trade_side")),
        "tick_size": snapshot.get("tick_size"),
        "valid_state": bool(snapshot.get("valid_state")),
        "quality_flags": ";".join(snapshot.get("quality_flags") or []),
        "initial_snapshot_received": bool(snapshot.get("initial_snapshot_received")),
        "last_book_hash": _as_str(snapshot.get("last_book_hash")),
        "quote_age_ms": _parse_int(snapshot.get("quote_age_ms")),
        "reconnect_count": _parse_int(snapshot.get("reconnect_count")),
    }


@dataclass(slots=True, kw_only=True)
class _PolymarketCaptureSession(_CaptureSessionBookkeeping):
    states: dict[str, MarketBookState]
    tape_producer: PolymarketTapeProducer | None
    compact_control_producer: CompactValidityProducer | None
    monotonic_ns: Callable[[], int]
    outputs: StreamRunOutputs
    run_dir: Path
    started_at: datetime
    max_messages: int | None
    max_reconnects: int
    custom_feature_enabled: bool
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
    quality_counter: Counter[str] = field(default_factory=Counter)
    instrument_counter: Counter[str] = field(default_factory=Counter)

    def owned_book_states(self) -> dict[str, MarketBookState]:
        """Return the exact connection-owned state surface for barrier work."""

        return {asset_id: self.states[asset_id] for asset_id in self.instrument_ids}

    def mark_reconnect(self) -> None:
        self.reconnect_count += 1
        if self.storage_profile is not None:
            self.sequence += 1
        now = self.monotonic_ns()
        for shard in self.health_shards:
            if shard.mark_reconnect(now_monotonic_ns=now):
                self.pending_health_shard_keys.add((shard.venue, shard.shard_id))
        owned_states = self.owned_book_states()
        for state in owned_states.values():
            state.mark_reconnect()
        if self.tape_producer is not None and self.profile_runtime is not None:
            self.tape_producer.reconnect(
                states=owned_states,
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
                    book_id: str(state.market or book_id)
                    for book_id, state in owned_states.items()
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
                states=self.owned_book_states(),
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
            "assets_with_snapshots": len(self.instruments_with_snapshots),
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
            command=self.command or "stream_order_book_data",
            git_commit=self.git_commit
            if self.git_commit is not None
            else current_git_commit(
                Path(self.git_cwd) if self.git_cwd is not None else Path.cwd()
            ),
            dataset_paths=self.outputs.dataset_paths,
            schema_versions=self.outputs.schema_versions,
            row_counts=compatibility_row_counts,
            quality_flag_counts=dict(sorted(self.quality_counter.items())),
            venue_counts={"polymarket": self.topbook_count},
            instrument_counts=dict(sorted(self.instrument_counter.items())),
            reconnect_count=self.reconnect_count,
            sequence_gap_count=self.sequence_gap_count,
            resync_event_count=self.reconnect_count + self.sequence_gap_count,
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
                    "asset_ids": self.instrument_ids,
                    "duration_s": self.duration_s,
                    "max_messages": self.max_messages,
                    "capture_intent": self.resolved_capture_intent.value,
                    "max_reconnects": self.max_reconnects,
                    "custom_feature_enabled": self.custom_feature_enabled,
                },
                "counts": legacy_counts,
                "files": self.outputs.files,
                "reconnect_count": self.reconnect_count,
                "socket_recovery_count": self.socket_recovery_count,
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
                        "runtime_projection_recorder_attached"
                        if self.runtime_projection_recorder is not None
                        else (
                            "slim_health_emitter_inactive"
                            if self.health_emitter is None
                            else None
                        )
                    ),
                ),
                "instrument_evidence_policy": (
                    self.instrument_evidence_tracker.policy.as_manifest_mapping(
                        venue="polymarket"
                    )
                    if self.instrument_evidence_tracker is not None
                    else None
                ),
                "parquet_segments": self.outputs.parquet_segment_manifests(),
                **profile_extra,
            },
        )


async def stream_order_book_data(
    asset_ids: Sequence[str],
    *,
    output_root: str | Path = DEFAULT_ORDER_BOOK_STREAM_ROOT,
    run_name: str | None = None,
    duration_s: float = 300.0,
    max_messages: int | None = None,
    capture_intent: CaptureIntent | str = CaptureIntent.OPERATIONAL,
    custom_feature_enabled: bool = True,
    heartbeat_interval: float | None = 10.0,
    max_reconnects: int = 3,
    websocket_max_size_bytes: int | None = None,
    websocket_max_queue_frames: int | None = None,
    connect_factory: ConnectFactory | None = None,
    command: str | None = None,
    git_commit: str | None = None,
    git_cwd: str | Path | None = None,
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
    """Stream CLOB market websocket events to an analysis-ready run directory.

    ``monotonic_ns`` is the monotonic clock (nanoseconds) used for all feed
    staleness/health timing; it defaults to :func:`time.monotonic_ns` and is
    injectable so tests can drive staleness deterministically instead of
    depending on real elapsed wall-clock time.
    """
    normalized_ids = _normalize_asset_ids(asset_ids)
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
            merge_profile_dataset_specs(STREAM_DATASETS),
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
    requested_assets = set(normalized_ids)
    states = {
        asset_id: MarketBookState(asset_id=asset_id) for asset_id in normalized_ids
    }

    supervisor = feed_supervisor or LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="polymarket-0",
                subscribed_instruments=tuple(normalized_ids),
            )
        ]
    )
    supervisor.require_preflight_ok()
    health_shards = supervisor.venue_shards("polymarket")
    if not health_shards:
        raise ValueError("feed supervisor has no Polymarket shards")
    if len(health_shards) != 1:
        raise ValueError(
            "one Polymarket collector connection requires exactly one feed shard; "
            "partition the capture into one collector run per shard"
        )
    capture_shard = health_shards[0]
    if set(capture_shard.subscribed_instruments) != requested_assets:
        raise ValueError(
            "Polymarket collector instruments must exactly match its feed shard"
        )
    capture_shard_id = capture_shard.shard_id
    active_sequence_gap_assets: set[str] = set()
    startup_boundary_pending = True
    terminal_control_staged = False
    profile_runtime: ProfileCaptureRuntime | None = None
    tape_producer: PolymarketTapeProducer | None = None
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
    if storage_profile is not None:
        assert profile_specs is not None
        profile_runtime = create_profile_runtime(
            run_dir=run_dir,
            selection=storage_profile,
            specs=profile_specs,
            shard_plan={capture_shard_id: normalized_ids},
            adapter_settings_by_venue={"polymarket": {}},
            started_at_utc=started_at.isoformat(),
            durability_settings=CaptureDurabilitySettings.resolve(
                requested_segment_rows=parquet_segment_rows,
                requested_segment_seconds=parquet_segment_seconds,
            ),
            storage_backend=capture_storage_backend,
        )
        if DatasetRole.TAPE_EVENT in storage_profile.enabled_roles:
            tape_producer = PolymarketTapeProducer(
                collector_run_id=run_dir.name, shard_id=capture_shard_id
            )
        elif DatasetRole.TAPE_CONTROL in storage_profile.enabled_roles:
            compact_control_producer = CompactValidityProducer(
                collector_run_id=run_dir.name,
                shard_id=capture_shard_id,
                venue="polymarket",
            )
        observation_producer = StreamObservationProducer(collector_run_id=run_dir.name)
        # Always track topbook emission under storage profiles so dense writes
        # uniquify primary keys and checkpoints remain available.
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
            storage_profile.definition.profile_version == "2"
            and DatasetRole.INSTRUMENT_EVIDENCE in storage_profile.enabled_roles
        ):
            instrument_evidence_tracker = CaptureInstrumentEvidenceTracker(
                collector_run_id=run_dir.name,
                venue="polymarket",
                shard_id=capture_shard_id,
                instrument_ids=normalized_ids,
                eligibility_evidence=(
                    instrument_eligibility_evidence
                    or eligibility_evidence_from_subscription_metadata(
                        subscription_plan_metadata, venue="polymarket"
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

    session = _PolymarketCaptureSession(
        venue="polymarket",
        instrument_ids=normalized_ids,
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
        custom_feature_enabled=custom_feature_enabled,
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
            async with AsyncMarketWebSocketClient(
                session.instrument_ids,
                custom_feature_enabled=session.custom_feature_enabled,
                heartbeat_interval=heartbeat_interval,
                connect_factory=connect_factory,
                transport_settings=session.transport_settings,
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
                if (
                    session.health_emitter is not None
                    and session.runtime_projection_recorder is None
                ):
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
                                venue="polymarket",
                            )
                        if (
                            recovery_actions
                            and session.runtime_projection_recorder is not None
                        ):
                            session.runtime_projection_recorder.record_recovery_actions(
                                actions=recovery_actions,
                                observed_at_utc=_utc_now().isoformat(),
                            )
                        if (
                            not recovery_actions
                            or session.reconnect_count >= session.max_reconnects
                        ):
                            return False
                        if (
                            next_message_task is not None
                            and not next_message_task.done()
                        ):
                            next_message_task.cancel()
                            with contextlib.suppress(asyncio.CancelledError):
                                await next_message_task
                        next_message_task = None
                        session.mark_reconnect()
                        await ws.reconnect()
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
                                message = await asyncio.wait_for(
                                    asyncio.shield(next_message_task),
                                    timeout=session.message_wait_timeout(
                                        remaining,
                                        now_monotonic_ns=wait_started_ns,
                                    ),
                                )
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

                            event_type = str(
                                message.get("event_type") or message.get("type") or ""
                            )
                            book_state_message = (
                                event_type in POLYMARKET_BOOK_EVENT_TYPES
                            )
                            level_rows = [
                                row
                                for row in _level_rows(
                                    session.sequence,
                                    received_at,
                                    received_at_utc,
                                    message,
                                )
                                if row.get("asset_id") in requested_assets
                            ]
                            snapshots = apply_market_message(
                                session.states,
                                message,
                                allowed_asset_ids=requested_assets,
                            )
                            if not snapshots:
                                message_asset = str(message.get("asset_id") or "")
                                message_shard = session.health_shard_for_instrument(
                                    message_asset
                                )
                                if message_shard.record_message(
                                    now_monotonic_ns=received_at_monotonic_ns,
                                    instrument=(
                                        message_asset
                                        if message_asset in requested_assets
                                        else None
                                    ),
                                ):
                                    session.pending_health_shard_keys.add(
                                        (message_shard.venue, message_shard.shard_id)
                                    )

                            if (
                                book_state_message
                                and session.tape_producer is not None
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
                                    observations = observation_producer.polymarket(
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
                                    message_asset = str(message.get("asset_id") or "")
                                    error_shard = session.health_shard_for_instrument(
                                        message_asset
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
                                if row.get("asset_id") not in requested_assets:
                                    continue
                                if level_sink is not None:
                                    await level_sink.write(row)
                                    session.level_count += 1

                            resync_boundary_due = False
                            for snapshot in snapshots:
                                flags = set(snapshot.quality_flags)
                                snapshot_shard = session.health_shard_for_instrument(
                                    snapshot.asset_id
                                )
                                if "seq_gap" in flags:
                                    if (
                                        snapshot.asset_id
                                        not in active_sequence_gap_assets
                                    ):
                                        session.sequence_gap_count += 1
                                        active_sequence_gap_assets.add(
                                            snapshot.asset_id
                                        )
                                        if snapshot_shard.record_sequence_gap(
                                            instrument=snapshot.asset_id
                                        ):
                                            session.pending_health_shard_keys.add(
                                                (
                                                    snapshot_shard.venue,
                                                    snapshot_shard.shard_id,
                                                )
                                            )
                                else:
                                    if snapshot.asset_id in active_sequence_gap_assets:
                                        if snapshot_shard.record_resync(
                                            now_monotonic_ns=received_at_monotonic_ns,
                                            instrument=snapshot.asset_id,
                                        ):
                                            session.pending_health_shard_keys.add(
                                                (
                                                    snapshot_shard.venue,
                                                    snapshot_shard.shard_id,
                                                )
                                            )
                                        resync_boundary_due = True
                                    active_sequence_gap_assets.discard(
                                        snapshot.asset_id
                                    )
                                if snapshot_shard.record_book(
                                    valid_state=bool(snapshot.valid_state),
                                    now_monotonic_ns=received_at_monotonic_ns,
                                    instrument=snapshot.asset_id,
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
                                if snapshot.valid_state:
                                    session.instruments_with_snapshots.add(
                                        snapshot.asset_id
                                    )
                                    if session.instrument_evidence_tracker is not None:
                                        session.instrument_evidence_tracker.record_valid_snapshot(
                                            snapshot.asset_id,
                                            observed_at_utc=received_at_utc,
                                        )
                                else:
                                    session.instruments_with_invalid_snapshots.add(
                                        snapshot.asset_id
                                    )
                                topbook = polymarket_ws_snapshot_to_topbook(
                                    snapshot_payload,
                                    collector_run_id=session.run_dir.name,
                                    received_at_utc=canonical_utc(received_at_utc),
                                    received_at_monotonic_ns=received_at_monotonic_ns,
                                    local_sequence=session.sequence,
                                )
                                instrument_id = topbook.get("instrument_id")
                                if instrument_id is not None:
                                    session.latest_topbooks[str(instrument_id)] = (
                                        topbook
                                    )
                                topbook_emission = None
                                if session.topbook_tracker is not None:
                                    topbook_emission = session.topbook_tracker.observe(
                                        topbook,
                                        now_monotonic_ns=received_at_monotonic_ns,
                                        force_main=dense_topbook_emission,
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
                                    # Controls must bind only to main rows written
                                    # into the same durable capture buffer.
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
                                        venue_book_id=snapshot.asset_id,
                                        venue_market_id=str(
                                            snapshot.market or snapshot.asset_id
                                        ),
                                    ).write_to(session.profile_runtime.coordinator)
                                state = session.states.get(snapshot.asset_id)
                                if (
                                    depth_sink is not None
                                    and state is not None
                                    and _should_emit_canonical_depth(snapshot)
                                ):
                                    depth_received_at = canonical_utc(received_at_utc)
                                    if (
                                        topbook_emission is not None
                                        and topbook_emission.role
                                        is DatasetRole.TOPBOOK_MAIN
                                    ):
                                        # Keep depth PK clocks aligned with uniquified topbook rows.
                                        depth_received_at = str(
                                            topbook_emission.row["received_at_utc"]
                                        )

                                    for depth in _canonical_depth_rows_from_state(
                                        run_id=session.run_dir.name,
                                        sequence=session.sequence,
                                        received_at_utc=depth_received_at,
                                        snapshot=snapshot,
                                        state=state,
                                    ):
                                        await depth_sink.write(depth)
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
                            control_tick_ran = False
                            if session.feed_control_scheduler is not None:
                                control_tick_ran = (
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
                                )
                            else:
                                await session.write_health(
                                    health_sink,
                                    observed_at_utc=received_at_utc,
                                    local_sequence=session.sequence,
                                    now_monotonic_ns=received_at_monotonic_ns,
                                )
                            if book_state_message or control_tick_ran:
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
                # Completeness must be decided BEFORE a successful terminal
                # control becomes durable. The terminal emission is a forced
                # barrier, so evaluating afterwards could leave a committed tape
                # asserting the run completed while no success manifest exists.
                # Raising here routes through the existing failure handler, which
                # emits reason="failed" and writes a partial/failed manifest.
            completeness_report = session.finalize_capture_completeness()
            if not completeness_report.ok:
                raise CaptureCompletenessError(
                    "polymarket capture completeness failed: "
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
                    states=session.owned_book_states(),
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
                        book_id: str(state.market or book_id)
                        for book_id, state in session.owned_book_states().items()
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
            elif capture_phase == "finalization":
                session.terminal_reason = CaptureTerminationReason.FINALIZATION_ERROR
                # ConnectionError is an OSError subclass, but here it represents
                # websocket transport exhaustion rather than a persistence failure.
            elif isinstance(exc, ConnectionError):
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
                        states=session.owned_book_states(),
                        received_at_utc=ended_at.isoformat(),
                        received_at_monotonic_ns=session.monotonic_ns(),
                        local_sequence=session.sequence + 1,
                        reason="failed",
                    ).write_to(session.profile_runtime.coordinator)
                elif session.compact_control_producer is not None:
                    session.compact_control_producer.ended(
                        books={
                            book_id: str(state.market or book_id)
                            for book_id, state in session.owned_book_states().items()
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
