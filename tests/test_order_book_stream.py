from __future__ import annotations

import asyncio
import json
import zipfile
from collections import deque
from itertools import count
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from pmkt.data.io import read_parquet_segment_manifest
from pmkt.data.manifests import validate_run_manifest
from pmkt.exchanges.polymarket.order_book_stream import stream_order_book_data
from pmkt.streaming.supervisor import FeedShardHealth, LiveFeedSupervisor
from pmkt.streaming.capture_archive import (
    archive_capture_connection_group,
    archive_finalized_capture,
)
from pmkt.streaming.capture_completeness import CaptureCompletenessError
from pmkt.streaming.durability import (
    DurableCaptureCoordinator,
    file_sha256,
    write_json_atomic_fsync,
)
from pmkt.streaming.health_emission import SlimHealthEmitter
from pmkt.streaming.profiles import StorageProfileOverrides, select_storage_profile
from pmkt.streaming.storage_backends import CaptureStorageBackend
from pmkt.streaming.topbook_emission import TopbookEmissionTracker


class FakeWebSocket:
    def __init__(self, messages: list[Any]) -> None:
        self.messages = deque(messages)
        self.sent: list[str] = []
        self.closed = False

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def close(self) -> None:
        self.closed = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.messages:
            raise StopAsyncIteration
        item = self.messages.popleft()
        if isinstance(item, BaseException):
            raise item
        return item


class SilentWebSocket(FakeWebSocket):
    def __init__(self) -> None:
        super().__init__([])

    async def __anext__(self):
        await asyncio.sleep(3600)
        raise StopAsyncIteration


class DelayedFakeWebSocket(FakeWebSocket):
    async def __anext__(self):
        while self.messages:
            item = self.messages.popleft()
            if isinstance(item, float):
                await asyncio.sleep(item)
                continue
            if isinstance(item, BaseException):
                raise item
            return item
        raise StopAsyncIteration


class ManualMonotonicClock:
    """Deterministic monotonic clock (nanoseconds) for staleness tests.

    The clock only advances when explicitly told to (typically by a websocket
    fixture between messages), so feed-staleness assertions do not depend on
    real elapsed wall-clock time or suite-wide load.
    """

    def __init__(self, start_ns: int = 0) -> None:
        self._now_ns = start_ns

    def __call__(self) -> int:
        return self._now_ns

    def advance_seconds(self, seconds: float) -> None:
        self._now_ns += int(seconds * 1_000_000_000)


class ClockDrivenFakeWebSocket(FakeWebSocket):
    """Like ``DelayedFakeWebSocket`` but advances a :class:`ManualMonotonicClock`
    instead of sleeping, so the inter-message gap that drives staleness is
    deterministic regardless of how fast the event loop runs."""

    def __init__(self, messages: list[Any], clock: ManualMonotonicClock) -> None:
        super().__init__(messages)
        self._clock = clock

    async def __anext__(self):
        while self.messages:
            item = self.messages.popleft()
            if isinstance(item, float):
                self._clock.advance_seconds(item)
                await asyncio.sleep(0)
                continue
            if isinstance(item, BaseException):
                raise item
            return item
        raise StopAsyncIteration


@pytest.mark.asyncio
async def test_relative_output_root_produces_authoritative_absolute_run_dir(
    tmp_path, monkeypatch
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            )
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    monkeypatch.chdir(tmp_path)
    manifest = await stream_order_book_data(
        ["token-1"],
        output_root=Path("relative-runs"),
        run_name="relative-run",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
        storage_profile=select_storage_profile("full"),
    )
    manifest_path = tmp_path / "relative-runs" / "relative-run" / "manifest.json"

    assert Path(manifest["run_dir"]).is_absolute()
    assert Path(manifest["run_dir"]) == manifest_path.parent
    assert validate_run_manifest(manifest_path).ok


@pytest.mark.asyncio
async def test_finalized_capture_can_be_verified_archived_and_removed(tmp_path) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                    "timestamp": "1766789469000",
                    "hash": "hash-book",
                }
            )
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="archive-source",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
        storage_profile=select_storage_profile(
            "mm-compact",
            experimental_profile_acknowledged=True,
        ),
    )
    source = tmp_path / "archive-source"
    result = archive_finalized_capture(
        source / "manifest.json",
        delete_source=True,
    )

    assert not source.exists()
    assert result.source_deleted is True
    assert result.archive_path.is_file()
    assert result.archive_manifest_path.is_file()
    archive_manifest = json.loads(
        result.archive_manifest_path.read_text(encoding="utf-8")
    )
    assert archive_manifest["source_deleted"] is True
    assert len(archive_manifest["members_digest"]) == 64
    with zipfile.ZipFile(result.archive_path) as archive:
        assert archive.testzip() is None
        assert "manifest.json" in archive.namelist()
        assert "run_state.v1.json" in archive.namelist()


@pytest.mark.asyncio
async def test_connection_group_archive_validates_children_before_removal(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            )
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    group_dir = tmp_path / "connection-group"
    manifest = await stream_order_book_data(
        ["token-1"],
        output_root=group_dir,
        run_name="child-000",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
        storage_profile=select_storage_profile("full"),
    )
    child_manifest = Path(manifest["run_dir"]) / "manifest.json"
    group_manifest = group_dir / "capture_connection_group.v1.json"
    write_json_atomic_fsync(
        group_manifest,
        {
            "schema_version": "capture_connection_group.v1",
            "run_id": "connection-group",
            "run_dir": str(group_dir.resolve()),
            "venue": "polymarket",
            "status": "success",
            "connection_count": 1,
            "connection_start_stagger_seconds": 0.0,
            "counts": manifest["counts"],
            "children": [
                {
                    "shard_id": "polymarket-0",
                    "instrument_count": 1,
                    "relation_count": 0,
                    "run_dir": manifest["run_dir"],
                    "manifest_path": str(child_manifest),
                    "manifest_sha256": file_sha256(child_manifest),
                    "status": manifest["status"],
                    "counts": manifest["counts"],
                }
            ],
        },
    )

    result = archive_capture_connection_group(group_manifest, delete_source=True)

    assert not group_dir.exists()
    assert result.source_deleted is True
    sidecar = json.loads(result.archive_manifest_path.read_text(encoding="utf-8"))
    assert sidecar["source_kind"] == "connection_group"
    assert sidecar["child_count"] == 1
    with zipfile.ZipFile(result.archive_path) as archive:
        assert archive.testzip() is None
        assert "capture_connection_group.v1.json" in archive.namelist()
        assert "child-000/manifest.json" in archive.namelist()


@pytest.mark.asyncio
async def test_full_storage_profile_writes_committed_role_artifacts(tmp_path) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                    "timestamp": "1766789469000",
                    "hash": "hash-book",
                }
            )
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    manifest = await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="profile-run",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
        storage_profile=select_storage_profile("full"),
    )

    run_dir = tmp_path / "profile-run"
    assert not (run_dir / "raw_events.jsonl").exists()
    assert (run_dir / "run_state.v1.json").exists()
    assert (run_dir / "capture_commit_journal.v2.jsonl").exists()
    assert manifest["storage_profile"]["name"] == "full"
    assert manifest["storage_profile"]["profile_version"] == "2"
    assert manifest["storage_profile"]["terminal_completeness"] == "partial"
    assert manifest["feed_control_plane"]["policy_version"] == ("feed-control-plane.v1")
    assert manifest["feed_control_plane"]["enabled"] is True
    assert manifest["feed_control_plane"]["suppression_reason"] is None
    assert manifest["feed_control_plane"]["tick_interval_ms"] == 250.0
    assert manifest["feed_control_plane"]["transition_row_builds"] >= 1
    assert manifest["feed_health_emission"]["rows_evaluated"] >= 1
    assert (run_dir / "capture_instrument_evidence.parquet").exists()
    assert manifest["capture_completeness"]["unknown_instrument_count"] == 1
    assert manifest["capture_completeness"]["evidence_artifact_reconciled"] is True
    assert set(manifest["dataset_artifacts"]) == set(
        manifest["storage_profile"]["enabled_roles"]
    )
    assert pd.read_parquet(run_dir / "book_tape_event.parquet").shape[0] == 1
    assert pd.read_parquet(run_dir / "book_tape_level.parquet").shape[0] == 2
    assert pd.read_parquet(run_dir / "book_tape_control.parquet").shape[0] == 2
    assert pd.read_parquet(run_dir / "trades.parquet").empty
    assert validate_run_manifest(run_dir / "manifest.json").ok


@pytest.mark.asyncio
async def test_sqlite_storage_profile_promotes_once_on_clean_shutdown(tmp_path) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                    "timestamp": "1766789469000",
                    "hash": "hash-book",
                }
            )
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    manifest = await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="sqlite-profile-run",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
        storage_profile=select_storage_profile("full"),
        capture_storage_backend=CaptureStorageBackend.SQLITE_WAL,
    )

    run_dir = tmp_path / "sqlite-profile-run"
    assert (run_dir / "capture.sqlite3").exists()
    assert manifest["capture_storage"]["configuration"]["backend"] == ("sqlite_wal_v1")
    storage_metrics = manifest["capture_storage"]["metrics"]
    assert storage_metrics["logical_groups_committed"] >= 1
    assert storage_metrics["promotion"]["attempt_count"] == 1
    assert storage_metrics["promotion"]["failure_count"] == 0
    assert storage_metrics["promotion"]["output_files"] == len(
        manifest["dataset_artifacts"]
    )
    for artifact in manifest["dataset_artifacts"].values():
        dataset = run_dir / artifact["path"]
        assert len(list(dataset.glob("*.parquet"))) == 1
    assert validate_run_manifest(run_dir / "manifest.json").ok


@pytest.mark.asyncio
async def test_profile_observations_do_not_advance_book_state_or_clocks(
    tmp_path,
) -> None:
    messages = [
        {
            "event_type": "book",
            "asset_id": "token-1",
            "market": "0xabc",
            "bids": [{"price": "0.40", "size": "10"}],
            "asks": [{"price": "0.60", "size": "5"}],
            "timestamp": "1766789469000",
            "hash": "hash-book",
        },
        {
            "event_type": "last_trade_price",
            "asset_id": "token-1",
            "market": "0xabc",
            "price": "0.55",
            "size": "2",
            "side": "BUY",
            "timestamp": "1766789470000",
        },
        {
            "event_type": "tick_size_change",
            "asset_id": "token-1",
            "market": "0xabc",
            "old_tick_size": "0.01",
            "new_tick_size": "0.001",
            "timestamp": "1766789471000",
        },
        {
            "event_type": "new_market",
            "asset_id": "token-1",
            "market": "0xabc",
            "timestamp": "1766789472000",
        },
        {
            "event_type": "market_resolved",
            "asset_id": "token-1",
            "market": "0xabc",
            "timestamp": "1766789473000",
            "outcome": "YES",
        },
    ]
    fake = FakeWebSocket([json.dumps(message) for message in messages])

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    shard = FeedShardHealth(
        venue="polymarket",
        shard_id="polymarket-0",
        subscribed_instruments=("token-1",),
    )
    supervisor = LiveFeedSupervisor([shard])
    clock = count(start=1_000_000_000, step=1_000_000)

    manifest = await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="observation-isolation-run",
        duration_s=10,
        max_messages=len(messages),
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
        feed_supervisor=supervisor,
        monotonic_ns=lambda: next(clock),
        storage_profile=select_storage_profile("full"),
    )

    run_dir = tmp_path / "observation-isolation-run"
    trades = pd.read_parquet(run_dir / "trades.parquet")
    lifecycle = pd.read_parquet(run_dir / "stream_lifecycle.parquet")

    assert pd.read_parquet(run_dir / "events.parquet").shape[0] == 5
    assert pd.read_parquet(run_dir / "snapshots.parquet").shape[0] == 1
    assert pd.read_parquet(run_dir / "topbook_v1.parquet").shape[0] == 1
    assert pd.read_parquet(run_dir / "depth_v1.parquet").shape[0] == 2
    assert pd.read_parquet(run_dir / "book_tape_event.parquet").shape[0] == 1
    assert pd.read_parquet(run_dir / "book_tape_level.parquet").shape[0] == 2
    assert trades["local_sequence"].tolist() == [2]
    assert lifecycle["local_sequence"].tolist() == [3, 4, 5]
    assert shard.valid_book_count == 1
    assert shard.invalid_book_count == 0
    assert shard.instrument_health["token-1"].valid_book_count == 1
    assert shard.last_valid_book_monotonic_ns is not None
    assert shard.last_message_monotonic_ns is not None
    assert shard.last_message_monotonic_ns > shard.last_valid_book_monotonic_ns
    assert manifest["row_counts"]["topbook"] == 1
    assert manifest["feed_control_plane"]["ticks"] == 0
    assert manifest["feed_control_plane"]["stale_instrument_checks"] == 0
    assert manifest["feed_control_plane"]["transition_row_builds"] == 1
    assert manifest["feed_control_plane"]["recovery_evaluations"] == 1
    assert manifest["feed_health_emission"]["rows_evaluated"] == 3
    assert validate_run_manifest(run_dir / "manifest.json").ok


@pytest.mark.asyncio
async def test_malformed_trade_observation_does_not_end_polymarket_capture(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "last_trade_price",
                    "asset_id": "token-1",
                    "market": "0xabc",
                }
            ),
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            ),
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    manifest = await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="malformed-trade-run",
        duration_s=10,
        max_messages=2,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
        storage_profile=select_storage_profile("full", profile_version="1"),
    )

    assert manifest["status"] == "success"
    assert pd.read_parquet(tmp_path / "malformed-trade-run" / "trades.parquet").empty
    assert (
        pd.read_parquet(tmp_path / "malformed-trade-run" / "topbook_v1.parquet").shape[
            0
        ]
        == 1
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("profile_name", ["full", "mm-compact"])
async def test_same_timestamp_changed_topbooks_are_both_persisted(
    monkeypatch, tmp_path, profile_name
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            ),
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.41", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            ),
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    monkeypatch.setattr(
        "pmkt.exchanges.polymarket.order_book_stream.time.time",
        lambda: 1_753_002_000.0,
    )
    await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name=f"same-timestamp-{profile_name}-run",
        duration_s=10,
        max_messages=2,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
        storage_profile=select_storage_profile(profile_name),
    )

    topbooks = pd.read_parquet(
        tmp_path / f"same-timestamp-{profile_name}-run" / "topbook_v1.parquet"
    )
    assert topbooks.shape[0] == 2
    assert topbooks["received_at_utc"].nunique() == 2


@pytest.mark.asyncio
async def test_mm_compact_opens_only_reduced_roles_and_emits_topbook_controls(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            )
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-connection-007",
                subscribed_instruments=("token-1",),
            )
        ]
    )

    manifest = await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="compact-run",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
        feed_supervisor=supervisor,
        storage_profile=select_storage_profile(
            "mm-compact",
            experimental_profile_acknowledged=True,
        ),
    )

    run_dir = tmp_path / "compact-run"
    assert not (run_dir / "events.parquet").exists()
    assert not (run_dir / "book_tape_event.parquet").exists()
    topbook_main = pd.read_parquet(run_dir / "topbook_v1.parquet")
    checkpoints = pd.read_parquet(run_dir / "topbook_checkpoints.parquet")
    assert topbook_main.shape[0] == 1
    assert checkpoints.shape[0] == 2
    assert set(topbook_main["received_at_utc"]).isdisjoint(
        set(checkpoints["received_at_utc"])
    )
    assert checkpoints["local_sequence"].tolist() == [2, 3]
    controls = pd.read_parquet(run_dir / "book_tape_control.parquet")
    assert controls["control_type"].tolist() == ["book_recovered", "stream_ended"]
    assert controls.iloc[0]["evidence_role"] == "topbook_main"
    terminal_at = pd.to_datetime(
        controls.loc[controls["control_type"] == "stream_ended", "received_at_utc"],
        utc=True,
    ).max()
    assert terminal_at > pd.to_datetime(checkpoints["received_at_utc"], utc=True).max()
    health = pd.read_parquet(run_dir / "feed_health.parquet")
    assert terminal_at > pd.to_datetime(health["observed_at_utc"], utc=True).max()
    assert set(manifest["dataset_artifacts"]) == {
        "topbook_main",
        "topbook_checkpoint",
        "tape_control",
        "trade",
        "lifecycle",
        "health",
        "instrument_evidence",
    }
    assert manifest["storage_profile"]["experimental_profile_acknowledged"] is True
    run_state = json.loads((run_dir / "run_state.v1.json").read_text(encoding="utf-8"))
    assert run_state["storage_profile"]["experimental_profile_acknowledged"] is True
    assert run_state["shard_plan"] == {"pm-connection-007": ["token-1"]}
    assert manifest["feed_shards"][0]["shard_id"] == "pm-connection-007"
    evidence = pd.read_parquet(run_dir / "capture_instrument_evidence.parquet")
    assert set(evidence["shard_id"]) == {"pm-connection-007"}
    assert validate_run_manifest(run_dir / "manifest.json").ok


@pytest.mark.asyncio
async def test_mm_compact_ignores_peer_asset_state_and_terminal_controls(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "owned-token",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            ),
            json.dumps(
                {
                    "event_type": "price_change",
                    "market": "0xabc",
                    "price_changes": [
                        {
                            "asset_id": "owned-token",
                            "price": "0.41",
                            "size": "11",
                            "side": "BUY",
                        },
                        {
                            "asset_id": "peer-token",
                            "price": "0.59",
                            "size": "7",
                            "side": "SELL",
                        },
                    ],
                }
            ),
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    await stream_order_book_data(
        ["owned-token"],
        output_root=tmp_path,
        run_name="owned-state-run",
        duration_s=10,
        max_messages=2,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
        storage_profile=select_storage_profile(
            "mm-compact",
            experimental_profile_acknowledged=True,
        ),
    )

    run_dir = tmp_path / "owned-state-run"
    controls = pd.read_parquet(run_dir / "book_tape_control.parquet")
    topbooks = pd.read_parquet(run_dir / "topbook_v1.parquet")
    assert set(controls["venue_book_id"]) == {"owned-token"}
    assert set(topbooks["instrument_id"]) == {"owned-token"}
    assert "peer-token" not in controls["venue_book_id"].tolist()


@pytest.mark.asyncio
async def test_terminal_health_failure_emits_one_final_failed_boundary(
    monkeypatch, tmp_path
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            )
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    original_prepare = SlimHealthEmitter.prepare

    def fail_terminal_health(self, *args, **kwargs):
        if kwargs.get("cause") == "terminal":
            raise RuntimeError("terminal health failed")
        return original_prepare(self, *args, **kwargs)

    monkeypatch.setattr(SlimHealthEmitter, "prepare", fail_terminal_health)
    with pytest.raises(RuntimeError, match="terminal health failed"):
        await stream_order_book_data(
            ["token-1"],
            output_root=tmp_path,
            run_name="terminal-health-failure",
            duration_s=10,
            max_messages=1,
            capture_intent="smoke",
            heartbeat_interval=None,
            connect_factory=connect_factory,
            storage_profile=select_storage_profile(
                "mm-compact",
                experimental_profile_acknowledged=True,
            ),
        )

    run_dir = tmp_path / "terminal-health-failure"
    controls = pd.read_parquet(run_dir / "book_tape_control.parquet")
    terminal = controls[controls["control_type"] == "stream_ended"]
    assert terminal.shape[0] == 1
    assert terminal.iloc[0]["reason"] == "failed"
    terminal_at = pd.to_datetime(terminal.iloc[0]["received_at_utc"], utc=True)
    checkpoints = pd.read_parquet(run_dir / "topbook_checkpoints.parquet")
    health = pd.read_parquet(run_dir / "feed_health.parquet")
    assert terminal_at > pd.to_datetime(checkpoints["received_at_utc"], utc=True).max()
    assert terminal_at > pd.to_datetime(health["observed_at_utc"], utc=True).max()
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert manifest["storage_profile"]["terminal_completeness"] == "failed"
    assert validate_run_manifest(run_dir / "manifest.json").ok


@pytest.mark.asyncio
async def test_termination_commit_failure_does_not_append_after_terminal(
    monkeypatch, tmp_path
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            )
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    original_commit = DurableCaptureCoordinator.commit
    failed = False

    def fail_first_termination(self, *, cause: str, force: bool = False):
        nonlocal failed
        if cause == "termination" and not failed:
            failed = True
            raise RuntimeError("termination commit failed")
        return original_commit(self, cause=cause, force=force)

    monkeypatch.setattr(DurableCaptureCoordinator, "commit", fail_first_termination)
    with pytest.raises(RuntimeError, match="termination commit failed"):
        await stream_order_book_data(
            ["token-1"],
            output_root=tmp_path,
            run_name="termination-commit-failure",
            duration_s=10,
            max_messages=1,
            capture_intent="smoke",
            heartbeat_interval=None,
            connect_factory=connect_factory,
            storage_profile=select_storage_profile(
                "mm-compact",
                experimental_profile_acknowledged=True,
            ),
        )

    run_dir = tmp_path / "termination-commit-failure"
    controls = pd.read_parquet(run_dir / "book_tape_control.parquet")
    terminal = controls[controls["control_type"] == "stream_ended"]
    assert terminal.shape[0] == 1
    assert terminal["reason"].tolist() == ["completed"]
    terminal_at = pd.to_datetime(terminal.iloc[0]["received_at_utc"], utc=True)
    checkpoints = pd.read_parquet(run_dir / "topbook_checkpoints.parquet")
    health = pd.read_parquet(run_dir / "feed_health.parquet")
    assert terminal_at > pd.to_datetime(checkpoints["received_at_utc"], utc=True).max()
    assert terminal_at > pd.to_datetime(health["observed_at_utc"], utc=True).max()
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert manifest["storage_profile"]["terminal_completeness"] == "failed"
    assert validate_run_manifest(run_dir / "manifest.json").ok


@pytest.mark.asyncio
async def test_profile_raw_jsonl_override_is_journaled_and_manifest_exact(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {"event_type": "book", "asset_id": "token-1", "bids": [], "asks": []}
            )
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    manifest = await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="raw-profile-run",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
        storage_profile=select_storage_profile(
            "full", overrides=StorageProfileOverrides(keep_raw_jsonl=True)
        ),
    )

    run_dir = tmp_path / "raw-profile-run"
    raw_artifact = manifest["dataset_artifacts"]["raw_jsonl"]
    assert raw_artifact["path"] == "raw_events.jsonl"
    assert raw_artifact["row_count"] == 1
    assert raw_artifact["completion_status"] == "closed"
    assert (run_dir / raw_artifact["segment_manifest_path"]).is_file()
    assert "raw_jsonl" in manifest["storage_profile"]["successfully_committed_roles"]
    assert validate_run_manifest(run_dir / "manifest.json").ok


@pytest.mark.asyncio
async def test_stream_order_book_data_writes_analysis_artifacts(tmp_path) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                    "timestamp": "1766789469000",
                    "hash": "hash-book",
                }
            ),
            json.dumps(
                {
                    "event_type": "price_change",
                    "market": "0xabc",
                    "price_changes": [
                        {
                            "asset_id": "token-1",
                            "price": "0.41",
                            "size": "12",
                            "side": "BUY",
                            "best_bid": "0.41",
                            "best_ask": "0.60",
                            "hash": "hash-delta",
                        }
                    ],
                    "timestamp": "1766789470000",
                }
            ),
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    manifest = await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="test-run",
        duration_s=10,
        max_messages=2,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
        command="pmkt stream order-book --fixture",
        git_commit="abc123",
        parquet_segment_rows=1,
    )

    run_dir = tmp_path / "test-run"
    assert manifest["run_dir"] == str(run_dir)
    assert manifest["counts"] == {
        "events": 2,
        "snapshots": 2,
        "levels": 3,
        "assets_with_snapshots": 1,
    }
    assert (run_dir / "manifest.json").exists()
    assert (run_dir / "raw_events.jsonl").exists()
    assert (run_dir / "topbook_v1.parquet").exists()
    assert (run_dir / "depth_v1.parquet").exists()
    assert (run_dir / "feed_health.parquet").exists()
    assert (
        len((run_dir / "raw_events.jsonl").read_text(encoding="utf-8").splitlines())
        == 2
    )

    events = pd.read_parquet(run_dir / "events.parquet")
    snapshots = pd.read_parquet(run_dir / "snapshots.parquet")
    levels = pd.read_parquet(run_dir / "order_book_levels.parquet")
    topbook = pd.read_parquet(run_dir / "topbook_v1.parquet")
    depth = pd.read_parquet(run_dir / "depth_v1.parquet")
    health = pd.read_parquet(run_dir / "feed_health.parquet")

    for name, expected_count in {
        "events.parquet": 2,
        "snapshots.parquet": 2,
        "order_book_levels.parquet": 3,
        "topbook_v1.parquet": 2,
        "depth_v1.parquet": 5,
        "feed_health.parquet": 2,
    }.items():
        path = run_dir / name
        segment_manifest = read_parquet_segment_manifest(path)
        assert path.is_dir()
        assert segment_manifest is not None
        assert len(segment_manifest["completed_segments"]) == expected_count
        assert segment_manifest["incomplete_segments"] == []

    assert events["event_type"].tolist() == ["book", "price_change"]
    assert snapshots["best_bid"].tolist() == pytest.approx([0.4, 0.41])
    assert snapshots["midpoint"].tolist() == pytest.approx([0.5, 0.505])
    assert snapshots["last_book_hash"].tolist() == ["hash-book", "hash-delta"]
    assert snapshots["valid_state"].tolist() == [True, True]
    assert snapshots["quality_flags"].tolist() == ["", ""]
    assert levels["event_type"].tolist() == ["book", "book", "price_change"]
    assert levels["is_delta"].tolist() == [False, False, True]
    assert topbook["schema_version"].tolist() == ["topbook.v1", "topbook.v1"]
    assert topbook["exchange"].tolist() == ["polymarket", "polymarket"]
    assert topbook["book_hash"].tolist() == ["hash-book", "hash-delta"]
    assert topbook["bid_size_contracts"].tolist() == pytest.approx([10, 12])
    assert topbook["ask_size_contracts"].tolist() == pytest.approx([5, 5])
    assert depth["schema_version"].tolist() == ["depth.v1"] * 5
    assert depth["valid_state"].tolist() == [True] * 5
    assert depth["is_delta"].tolist() == [False] * 5
    assert depth.groupby("local_sequence")["received_at_utc"].nunique().to_dict() == {
        1: 1,
        2: 1,
    }
    assert depth.groupby("local_sequence")["received_at_utc"].first().is_unique
    assert depth.groupby("local_sequence")["book_hash"].first().to_dict() == {
        1: "hash-book",
        2: "hash-delta",
    }
    assert depth["size_contracts"].notna().all()
    assert manifest["schema_versions"]["topbook"] == "topbook.v1"
    assert manifest["schema_versions"]["feed_health"] == "feed_health.v1"
    assert manifest["row_counts"]["topbook"] == 2
    assert manifest["row_counts"]["depth"] == 5
    assert manifest["row_counts"]["feed_health"] == 2
    assert manifest["sequence_gap_count"] == 0
    assert manifest["command"] == "pmkt stream order-book --fixture"
    assert manifest["git_commit"] == "abc123"
    assert manifest["venue_counts"] == {"polymarket": 2}
    assert manifest["instrument_counts"] == {"token-1": 2}
    assert manifest["dataset_paths"]["feed_health_parquet"] == str(
        run_dir / "feed_health.parquet"
    )
    assert health["local_sequence"].tolist() == [1, 2]
    assert health["venue"].tolist() == ["polymarket", "polymarket"]
    assert health["instrument_count"].tolist() == [1, 1]
    assert health["connection_state"].tolist() == ["connected", "connected"]
    assert health["valid_book_count"].tolist() == [1, 2]
    assert health["last_valid_book_age_ms"].notna().all()

    sent_subscription = json.loads(fake.sent[0])
    assert sent_subscription["assets_ids"] == ["token-1"]
    assert sent_subscription["type"] == "market"
    assert fake.closed is True
    assert validate_run_manifest(run_dir / "manifest.json").ok



@pytest.mark.asyncio
async def test_off_subscription_book_uses_connection_health_shard(tmp_path) -> None:
    clock = ManualMonotonicClock()
    fake = ClockDrivenFakeWebSocket(
        [
            1.0,
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "peer-token",
                    "market": "0xpeer",
                    "bids": [{"price": "0.30", "size": "8"}],
                    "asks": [{"price": "0.70", "size": "6"}],
                }
            ),
        ],
        clock,
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    shard = FeedShardHealth(
        venue="polymarket",
        shard_id="polymarket-0",
        subscribed_instruments=("owned-token",),
    )
    manifest = await stream_order_book_data(
        ["owned-token"],
        output_root=tmp_path,
        run_name="off-subscription-run",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
        feed_supervisor=LiveFeedSupervisor([shard]),
        monotonic_ns=clock,
    )

    assert manifest["counts"] == {
        "events": 1,
        "snapshots": 0,
        "levels": 0,
        "assets_with_snapshots": 0,
    }
    assert manifest["row_counts"]["topbook"] == 0
    assert shard.last_message_monotonic_ns == 1_000_000_000
    assert "peer-token" not in shard.instrument_health
    raw_rows = (
        (tmp_path / "off-subscription-run" / "raw_events.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    )
    assert len(raw_rows) == 1


@pytest.mark.asyncio
async def test_operational_max_messages_writes_failed_terminal_control(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            )
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    with pytest.raises(
        CaptureCompletenessError,
        match="operational capture terminated via max_messages_reached",
    ):
        await stream_order_book_data(
            ["token-1"],
            output_root=tmp_path,
            run_name="operational-max-messages",
            duration_s=10,
            max_messages=1,
            heartbeat_interval=None,
            connect_factory=connect_factory,
            storage_profile=select_storage_profile("full"),
        )

    run_dir = tmp_path / "operational-max-messages"
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    completeness = manifest["capture_completeness"]
    assert manifest["status"] == "partial"
    assert manifest["error_type"] == "CaptureCompletenessError"
    assert completeness["terminal_reason"] == "max_messages_reached"
    assert completeness["execution_status"] == "failed"
    assert manifest["storage_profile"]["terminal_completeness"] == "partial"
    controls = pd.read_parquet(run_dir / "book_tape_control.parquet")
    terminal = controls[controls["control_type"] == "stream_ended"]
    assert terminal["reason"].tolist() == ["failed"]
    assert manifest["dataset_artifacts"]["instrument_evidence"]["row_count"] == 1
    assert validate_run_manifest(run_dir / "manifest.json").ok


@pytest.mark.asyncio
async def test_stream_order_book_data_preserves_feed_supervisor_metadata(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            )
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-plan",
                subscribed_instruments=("token-1",),
                relation_ids=("match-1", "match-2"),
            )
        ]
    )

    manifest = await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="plan-health-run",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
        feed_supervisor=supervisor,
        subscription_plan_metadata={
            "plan_id": "plan-health",
            "path": "generated/plan.json",
            "sha256": "a" * 64,
        },
    )

    health = pd.read_parquet(tmp_path / "plan-health-run" / "feed_health.parquet")
    assert manifest["subscription_plan"] == {
        "plan_id": "plan-health",
        "path": "generated/plan.json",
        "sha256": "a" * 64,
    }
    assert health["shard_id"].tolist() == ["pm-plan"]
    assert health["relation_count"].tolist() == [2]
    assert manifest["feed_shards"] == [
        {
            "venue": "polymarket",
            "shard_id": "pm-plan",
            "instrument_count": 1,
            "relation_count": 2,
            "subscribed_instruments": ["token-1"],
            "relation_ids": ["match-1", "match-2"],
        }
    ]
    summary = manifest["feed_health_summary"]
    assert summary["shard_count"] == 1
    assert summary["instrument_count"] == 1
    assert summary["relation_count"] == 2
    assert summary["valid_book_count"] == 1
    assert summary["shards"][0]["shard_id"] == "pm-plan"


@pytest.mark.asyncio
async def test_stream_order_book_data_marks_silent_feed_stale(tmp_path) -> None:
    fake = SilentWebSocket()

    async def connect_factory(_: str) -> SilentWebSocket:
        return fake

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-plan",
                subscribed_instruments=("token-1",),
            )
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )

    manifest = await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="silent-run",
        duration_s=0.12,
        capture_intent="smoke",
        heartbeat_interval=None,
        max_reconnects=0,
        connect_factory=connect_factory,
        feed_supervisor=supervisor,
    )

    health = pd.read_parquet(tmp_path / "silent-run" / "feed_health.parquet")
    assert manifest["row_counts"]["events"] == 0
    assert manifest["feed_health_summary"]["stale_shard_count"] >= 1
    assert len(health) >= 1
    assert health["connection_state"].tolist()[-1] == "stale"
    assert health["valid_book_count"].tolist()[-1] == 0
    assert "stale_messages" in health["quality_flags"].tolist()[-1]
    assert "stale_books" in health["quality_flags"].tolist()[-1]
    assert manifest["socket_recovery_count"] == 0


@pytest.mark.asyncio
async def test_stream_order_book_data_recovers_silent_feed_with_reconnect(
    tmp_path,
) -> None:
    first = SilentWebSocket()
    second = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                    "hash": "hash-after-reconnect",
                }
            )
        ]
    )
    sockets = deque([first, second])

    async def connect_factory(_: str) -> FakeWebSocket:
        return sockets.popleft()

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-plan",
                subscribed_instruments=("token-1",),
            )
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )

    manifest = await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="silent-recovery-run",
        duration_s=1.0,
        max_messages=1,
        capture_intent="smoke",
        heartbeat_interval=None,
        max_reconnects=2,
        connect_factory=connect_factory,
        feed_supervisor=supervisor,
    )

    run_dir = tmp_path / "silent-recovery-run"
    topbook = pd.read_parquet(run_dir / "topbook_v1.parquet")
    health = pd.read_parquet(run_dir / "feed_health.parquet")

    assert first.closed is True
    assert json.loads(second.sent[0])["assets_ids"] == ["token-1"]
    assert manifest["row_counts"]["events"] == 1
    assert manifest["reconnect_count"] == 1
    assert manifest["socket_recovery_count"] == 1
    assert topbook["valid_state"].tolist() == [True]
    assert "stale" in health["connection_state"].tolist()
    assert health["connection_state"].tolist()[-1] == "connected"
    assert validate_run_manifest(run_dir / "manifest.json").ok


@pytest.mark.asyncio
async def test_stream_order_book_data_rejects_multiple_shards_on_one_connection(
    tmp_path,
) -> None:
    # The clock advances by exactly 60ms between the two token-1 messages, which
    # exceeds ``max_message_age_ms``/``max_valid_book_age_ms`` (20ms), so the idle
    # token-2 shard is deterministically stale by the final health write — no
    # dependence on real elapsed wall-clock time.
    clock = ManualMonotonicClock()
    fake = ClockDrivenFakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            ),
            0.06,
            json.dumps(
                {
                    "event_type": "price_change",
                    "market": "0xabc",
                    "price_changes": [
                        {
                            "asset_id": "token-1",
                            "price": "0.41",
                            "size": "12",
                            "side": "BUY",
                            "best_bid": "0.41",
                            "best_ask": "0.60",
                        }
                    ],
                }
            ),
        ],
        clock,
    )

    async def connect_factory(_: str) -> ClockDrivenFakeWebSocket:
        return fake

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-token-1",
                subscribed_instruments=("token-1",),
            ),
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-token-2",
                subscribed_instruments=("token-2",),
            ),
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )

    with pytest.raises(ValueError, match="exactly one feed shard"):
        await stream_order_book_data(
            ["token-1", "token-2"],
            output_root=tmp_path,
            run_name="multi-shard-stale-run",
            duration_s=10,
            max_messages=2,
            capture_intent="smoke",
            heartbeat_interval=None,
            max_reconnects=0,
            connect_factory=connect_factory,
            feed_supervisor=supervisor,
            monotonic_ns=clock,
            storage_profile=select_storage_profile("full"),
        )


@pytest.mark.asyncio
async def test_stream_order_book_data_emits_complete_same_shard_stale_transition(
    tmp_path,
) -> None:
    clock = ManualMonotonicClock()
    fake = ClockDrivenFakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            ),
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-2",
                    "market": "0xdef",
                    "bids": [{"price": "0.30", "size": "8"}],
                    "asks": [{"price": "0.70", "size": "6"}],
                }
            ),
            0.06,
            json.dumps(
                {
                    "event_type": "price_change",
                    "market": "0xabc",
                    "price_changes": [
                        {
                            "asset_id": "token-1",
                            "price": "0.41",
                            "size": "12",
                            "side": "BUY",
                            "best_bid": "0.41",
                            "best_ask": "0.60",
                        }
                    ],
                }
            ),
        ],
        clock,
    )

    async def connect_factory(_: str) -> ClockDrivenFakeWebSocket:
        return fake

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-shared",
                subscribed_instruments=("token-1", "token-2"),
            )
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )

    await stream_order_book_data(
        ["token-1", "token-2"],
        output_root=tmp_path,
        run_name="same-shard-stale-run",
        duration_s=10,
        max_messages=3,
        capture_intent="smoke",
        heartbeat_interval=None,
        max_reconnects=0,
        connect_factory=connect_factory,
        feed_supervisor=supervisor,
        monotonic_ns=clock,
        storage_profile=select_storage_profile("full"),
    )

    health = pd.read_parquet(tmp_path / "same-shard-stale-run" / "feed_health.parquet")
    stale_rows = health[
        (health["stale_instrument_count"] == 1)
        & (health["invalid_instrument_count"] == 1)
    ]

    assert not stale_rows.empty
    assert all("invalid_book" in flags for flags in stale_rows["quality_flags"])
    assert all(
        "invalid_instrument_books" in flags and "stale_instrument_books" in flags
        for flags in stale_rows["quality_flags"]
    )


@pytest.mark.asyncio
async def test_stream_order_book_data_recovers_idle_shard_while_peer_is_active(
    tmp_path,
) -> None:
    first = DelayedFakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                    "hash": "token-1-book",
                }
            ),
            0.03,
            json.dumps(
                {
                    "event_type": "price_change",
                    "market": "0xabc",
                    "price_changes": [
                        {
                            "asset_id": "token-1",
                            "price": "0.41",
                            "size": "12",
                            "side": "BUY",
                            "hash": "token-1-delta",
                        }
                    ],
                }
            ),
        ]
    )
    second = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-2",
                    "market": "0xdef",
                    "bids": [{"price": "0.30", "size": "8"}],
                    "asks": [{"price": "0.70", "size": "6"}],
                    "hash": "token-2-book",
                }
            ),
            json.dumps(
                {
                    "event_type": "price_change",
                    "market": "0xdef",
                    "price_changes": [
                        {
                            "asset_id": "token-2",
                            "price": "0.31",
                            "size": "9",
                            "side": "BUY",
                            "hash": "token-2-delta",
                        }
                    ],
                }
            ),
        ]
    )
    third = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.42", "size": "9"}],
                    "asks": [{"price": "0.58", "size": "4"}],
                    "hash": "token-1-reconnect-book",
                }
            )
        ]
    )
    sockets = deque([first, second, third])

    async def connect_factory(_: str) -> FakeWebSocket:
        return sockets.popleft()

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="pm-shared",
                subscribed_instruments=("token-1", "token-2"),
            ),
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )

    manifest = await stream_order_book_data(
        ["token-1", "token-2"],
        output_root=tmp_path,
        run_name="multi-shard-recovery-run",
        duration_s=2,
        max_messages=3,
        capture_intent="smoke",
        heartbeat_interval=None,
        max_reconnects=2,
        connect_factory=connect_factory,
        feed_supervisor=supervisor,
    )

    run_dir = tmp_path / "multi-shard-recovery-run"
    topbook = pd.read_parquet(run_dir / "topbook_v1.parquet")
    health = pd.read_parquet(run_dir / "feed_health.parquet")

    assert first.closed is True
    assert json.loads(second.sent[0])["assets_ids"] == ["token-1", "token-2"]
    assert manifest["row_counts"]["events"] >= 2
    assert 1 <= manifest["socket_recovery_count"] <= 2
    assert 1 <= manifest["reconnect_count"] <= 2
    assert "token-2" in set(topbook["instrument_id"])
    latest_token_2 = topbook[topbook["instrument_id"] == "token-2"].iloc[-1]
    assert bool(latest_token_2["valid_state"]) is True
    incomplete_rows = health[
        (health["shard_id"] == "pm-shared")
        & (
            health["quality_flags"].apply(
                lambda _f: "missing_instrument_books" in (_f if _f is not None else [])
            )
        )
    ]
    assert not incomplete_rows.empty


@pytest.mark.asyncio
async def test_stream_order_book_data_ignores_observation_only_best_hints(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            ),
            json.dumps(
                {
                    "event_type": "last_trade_price",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "price": "0.55",
                    "size": "2",
                    "side": "BUY",
                }
            ),
            json.dumps(
                {
                    "event_type": "best_bid_ask",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "best_bid": "0.41",
                    "best_ask": "0.59",
                }
            ),
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="depth-policy-run",
        duration_s=10,
        max_messages=3,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
    )

    run_dir = tmp_path / "depth-policy-run"
    topbook = pd.read_parquet(run_dir / "topbook_v1.parquet")
    depth = pd.read_parquet(run_dir / "depth_v1.parquet")
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))

    assert topbook["source"].tolist() == ["ws"]
    assert topbook["best_bid_dollars"].tolist() == [0.4]
    assert topbook["best_ask_dollars"].tolist() == [0.6]
    assert depth["side"].tolist() == ["bid", "ask"]
    assert manifest["row_counts"]["topbook"] == 1
    assert manifest["row_counts"]["depth"] == 2


@pytest.mark.asyncio
async def test_stream_order_book_data_price_change_does_not_invent_depth_from_best_hints(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            ),
            json.dumps(
                {
                    "event_type": "price_change",
                    "market": "0xabc",
                    "price_changes": [
                        {
                            "asset_id": "token-1",
                            "price": "0.60",
                            "size": "0",
                            "side": "SELL",
                            "best_bid": "0.40",
                            "best_ask": "0.61",
                        }
                    ],
                }
            ),
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="depth-hint-run",
        duration_s=10,
        max_messages=2,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
    )

    run_dir = tmp_path / "depth-hint-run"
    topbook = pd.read_parquet(run_dir / "topbook_v1.parquet")
    depth = pd.read_parquet(run_dir / "depth_v1.parquet")

    assert pd.isna(topbook.loc[1, "best_ask_dollars"])
    assert "empty_ask" in topbook.loc[1, "quality_flags"]
    assert depth[depth["local_sequence"] == 2]["side"].tolist() == ["bid"]


@pytest.mark.asyncio
async def test_stream_order_book_data_writes_partial_manifest_on_failure(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            ),
            RuntimeError("stream failed"),
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    with pytest.raises(RuntimeError, match="stream failed"):
        await stream_order_book_data(
            ["token-1"],
            output_root=tmp_path,
            run_name="failed-run",
            duration_s=10,
            max_messages=2,
            capture_intent="smoke",
            heartbeat_interval=None,
            connect_factory=connect_factory,
        )

    manifest = json.loads(
        (tmp_path / "failed-run" / "manifest.json").read_text(encoding="utf-8")
    )
    health = pd.read_parquet(tmp_path / "failed-run" / "feed_health.parquet")
    assert manifest["status"] == "partial"
    assert manifest["error_type"] == "RuntimeError"
    completeness = manifest["capture_completeness"]
    assert completeness["policy_version"] == "capture_completeness.v2"
    assert completeness["instruments_with_snapshots"] == 1
    assert completeness["terminal_reason"] == "stream_error"
    assert manifest["row_counts"]["topbook"] == 1
    assert health["error_count"].tolist()[-1] == 1
    assert "error:RuntimeError" in health["quality_flags"].tolist()[-1]


@pytest.mark.asyncio
async def test_clean_close_exhaustion_is_classified_as_stream_error(tmp_path) -> None:
    fake = FakeWebSocket([])

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    with pytest.raises(ConnectionError, match="reconnect budget"):
        await stream_order_book_data(
            ["token-1"],
            output_root=tmp_path,
            run_name="clean-close-failed-run",
            duration_s=10,
            max_messages=1,
            max_reconnects=0,
            capture_intent="smoke",
            heartbeat_interval=None,
            connect_factory=connect_factory,
        )

    manifest = json.loads(
        (tmp_path / "clean-close-failed-run" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["error_type"] == "ConnectionError"
    assert manifest["capture_completeness"]["terminal_reason"] == "stream_error"


@pytest.mark.asyncio
async def test_stream_order_book_data_persists_cancelled_manifest(tmp_path) -> None:
    fake = SilentWebSocket()

    async def connect_factory(_: str) -> SilentWebSocket:
        return fake

    task = asyncio.create_task(
        stream_order_book_data(
            ["token-1"],
            output_root=tmp_path,
            run_name="cancelled-run",
            duration_s=60,
            heartbeat_interval=None,
            connect_factory=connect_factory,
            storage_profile=select_storage_profile("full"),
        )
    )
    for _ in range(100):
        if fake.sent:
            break
        await asyncio.sleep(0.01)
    assert fake.sent

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    manifest_path = tmp_path / "cancelled-run" / "manifest.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] in {"failed", "partial"}
    assert manifest["capture_completeness"]["terminal_reason"] == "cancelled"
    assert validate_run_manifest(manifest_path).ok


@pytest.mark.asyncio
async def test_stream_order_book_data_finalizes_completeness_before_snapshot_failure(
    tmp_path,
) -> None:
    fake = FakeWebSocket([RuntimeError("stream failed before snapshot")])

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    with pytest.raises(RuntimeError, match="stream failed before snapshot"):
        await stream_order_book_data(
            ["token-1"],
            output_root=tmp_path,
            run_name="failed-before-snapshot",
            duration_s=10,
            max_messages=1,
            capture_intent="smoke",
            heartbeat_interval=None,
            connect_factory=connect_factory,
        )

    manifest = json.loads(
        (tmp_path / "failed-before-snapshot" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    completeness = manifest["capture_completeness"]
    assert manifest["error_type"] == "RuntimeError"
    assert completeness["policy_version"] == "capture_completeness.v2"
    assert completeness["instruments_with_snapshots"] == 0
    assert completeness["event_count"] == 0
    assert completeness["evaluated"] is False
    assert completeness["terminal_reason"] == "stream_error"


@pytest.mark.asyncio
async def test_stream_order_book_data_marks_reconnect_invalid_until_snapshot(
    tmp_path,
) -> None:
    first = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            ),
            OSError("connection dropped"),
        ]
    )
    second = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "price_change",
                    "market": "0xabc",
                    "price_changes": [
                        {
                            "asset_id": "token-1",
                            "price": "0.41",
                            "size": "12",
                            "side": "BUY",
                            "best_bid": "0.41",
                            "best_ask": "0.60",
                        }
                    ],
                }
            )
        ]
    )
    sockets = deque([first, second])

    async def connect_factory(_: str) -> FakeWebSocket:
        return sockets.popleft()

    await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="reconnect-run",
        duration_s=10,
        max_messages=2,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
    )

    topbook = pd.read_parquet(tmp_path / "reconnect-run" / "topbook_v1.parquet")
    depth = pd.read_parquet(tmp_path / "reconnect-run" / "depth_v1.parquet")
    health = pd.read_parquet(tmp_path / "reconnect-run" / "feed_health.parquet")
    manifest = json.loads(
        (tmp_path / "reconnect-run" / "manifest.json").read_text(encoding="utf-8")
    )
    assert topbook["valid_state"].tolist() == [True, False]
    assert "reconnect" in topbook.loc[1, "quality_flags"]
    assert manifest["reconnect_count"] == 1
    assert manifest["quality_flag_counts"]["reconnect"] >= 1
    assert depth["valid_state"].tolist()[-1] == False  # noqa: E712
    assert "reconnect" in depth["quality_flags"].tolist()[-1]
    assert health["reconnect_count"].tolist()[-1] == 1
    assert health["valid_book_count"].tolist()[-1] == 0
    assert "reconnect" in health["quality_flags"].tolist()[-1]


@pytest.mark.asyncio
async def test_mm_compact_restates_topbook_on_reconnect(monkeypatch, tmp_path) -> None:
    boundary_reasons: list[str] = []
    original_boundary_restatements = TopbookEmissionTracker.boundary_restatements

    def record_boundary(self, **kwargs):
        emissions = original_boundary_restatements(self, **kwargs)
        if emissions:
            boundary_reasons.append(kwargs["reason"])
        return emissions

    monkeypatch.setattr(
        TopbookEmissionTracker,
        "boundary_restatements",
        record_boundary,
    )
    first = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }
            ),
            OSError("connection dropped"),
        ]
    )
    second = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "price_change",
                    "market": "0xabc",
                    "price_changes": [
                        {
                            "asset_id": "token-1",
                            "price": "0.41",
                            "size": "12",
                            "side": "BUY",
                            "best_bid": "0.41",
                            "best_ask": "0.60",
                        }
                    ],
                }
            )
        ]
    )
    sockets = deque([first, second])

    async def connect_factory(_: str) -> FakeWebSocket:
        return sockets.popleft()

    await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="compact-reconnect-run",
        duration_s=10,
        max_messages=2,
        capture_intent="smoke",
        heartbeat_interval=None,
        connect_factory=connect_factory,
        storage_profile=select_storage_profile(
            "mm-compact",
            experimental_profile_acknowledged=True,
        ),
    )

    assert "reconnect" in boundary_reasons
    assert validate_run_manifest(
        tmp_path / "compact-reconnect-run" / "manifest.json"
    ).ok


@pytest.mark.asyncio
async def test_invalid_polymarket_snapshot_is_diagnostic_not_coverage(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-1",
                    "market": "0xabc",
                    "bids": [],
                    "asks": [],
                    "timestamp": "1766789469000",
                    "hash": "invalid-book",
                }
            )
        ]
    )

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    manifest = await stream_order_book_data(
        ["token-1"],
        output_root=tmp_path,
        run_name="invalid-snapshot",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        heartbeat_interval=None,
        websocket_max_size_bytes=32 * 1024 * 1024,
        websocket_max_queue_frames=128,
        connect_factory=connect_factory,
    )

    completeness = manifest["capture_completeness"]
    assert completeness["instruments_with_snapshots"] == 0
    assert completeness["instruments_with_invalid_snapshots"] == 1
    assert completeness["capture_intent"] == "smoke"
    assert completeness["terminal_reason"] == "max_messages_reached"
    assert completeness["acceptance_eligible"] is False
    assert manifest["websocket_transport"] == {
        "requested": {
            "max_size_bytes": 32 * 1024 * 1024,
            "max_queue_frames": 128,
        },
        "effective": {
            "max_size_bytes": 32 * 1024 * 1024,
            "max_queue_frames": 128,
        },
    }
