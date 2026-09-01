from __future__ import annotations

import asyncio
import json
from collections import deque
from itertools import count
from typing import Any

import pandas as pd
import pytest

from pmkt.data.io import read_parquet_segment_manifest
from pmkt.data.manifests import validate_run_manifest
from pmkt.exchanges.kalshi.order_book_stream import (
    _level_rows,
    stream_kalshi_order_book_data,
)
from pmkt.streaming.supervisor import FeedShardHealth, LiveFeedSupervisor
from pmkt.streaming.capture_completeness import CaptureCompletenessError
from pmkt.streaming.durability import DurableCaptureCoordinator
from pmkt.streaming.health_emission import SlimHealthEmitter
from pmkt.streaming.profiles import select_storage_profile
from pmkt.streaming.storage_backends import CaptureStorageBackend
from pmkt.streaming.topbook_emission import TopbookEmissionTracker


class FakeReadAuth:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.calls: list[str] = []

    def headers_for_get(self, path: str) -> dict[str, str]:
        self.calls.append(path)
        return {"KALSHI-ACCESS-KEY": "key-id"}

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


class SnapshotThenPongWebSocket(FakeWebSocket):
    def __init__(self, snapshot: str) -> None:
        super().__init__([snapshot])
        self.ping_count = 0

    async def __anext__(self):
        if self.messages:
            return await super().__anext__()
        await asyncio.sleep(3600)
        raise StopAsyncIteration

    async def ping(self) -> None:
        self.ping_count += 1


class SnapshotThenFailedRefreshWebSocket(SnapshotThenPongWebSocket):
    async def send(self, payload: str) -> None:
        self.sent.append(payload)
        message = json.loads(payload)
        if message.get("params", {}).get("action") == "get_snapshot":
            raise OSError("snapshot refresh transport failure")


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


def test_kalshi_stream_level_rows_preserve_explicit_zero_price() -> None:
    rows = _level_rows(
        1,
        1_700_000_000.0,
        "2023-11-14T22:13:20+00:00",
        {
            "type": "orderbook_snapshot",
            "msg": {
                "market_ticker": "KXTEST",
                "yes_dollars_fp": [
                    {"price": 0.0, "price_dollars": 0.4, "size": "2.00"}
                ],
                "no_dollars_fp": [],
            },
        },
    )

    assert rows[0]["price"] == 0.0


def test_kalshi_stream_delta_rows_preserve_explicit_zero_values() -> None:
    rows = _level_rows(
        1,
        1_700_000_000.0,
        "2023-11-14T22:13:20+00:00",
        {
            "type": "orderbook_delta",
            "msg": {
                "market_ticker": "KXTEST",
                "side": "yes",
                "price_dollars": 0.0,
                "price": 0.45,
                "delta_fp": 0.0,
                "delta": 2.0,
            },
        },
    )

    assert rows[0]["price"] == 0.0
    assert rows[0]["delta"] == 0.0


@pytest.mark.asyncio
async def test_full_storage_profile_writes_kalshi_tape_trade_and_lifecycle(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "market_id": "market-id",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            ),
            json.dumps(
                {
                    "type": "trade",
                    "msg": {
                        "trade_id": "native-trade-1",
                        "market_ticker": "KXTEST",
                        "yes_price_dollars": "0.41",
                        "count": 2,
                        "taker_side": "yes",
                        "created_time": "2026-07-19T10:00:00Z",
                    },
                }
            ),
            json.dumps(
                {
                    "type": "trade",
                    "msg": {
                        "trade_id": "native-trade-2",
                        "market_ticker": "KXTEST",
                        "yes_price": 42,
                        "count": 1,
                        "taker_side": "yes",
                        "created_time": "2026-07-19T10:00:00Z",
                    },
                }
            ),
            json.dumps(
                {
                    "type": "market_lifecycle_v2",
                    "seq": 4,
                    "msg": {
                        "event_type": "activated",
                        "market_ticker": "KXTEST",
                        "status": "active",
                    },
                }
            ),
        ]
    )

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="kalshi",
                shard_id="kx-connection-007",
                subscribed_instruments=("KXTEST",),
            )
        ]
    )

    test_clock = count(start=1_000_000_000, step=1_000)
    manifest = await stream_kalshi_order_book_data(
        ["KXTEST"],
        output_root=tmp_path,
        run_name="profile-run",
        duration_s=10,
        max_messages=4,
        capture_intent="smoke",
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
        feed_supervisor=supervisor,
        monotonic_ns=lambda: next(test_clock),
        storage_profile=select_storage_profile("full"),
    )

    run_dir = tmp_path / "profile-run"
    trades = pd.read_parquet(run_dir / "trades.parquet")
    health = pd.read_parquet(run_dir / "feed_health.parquet")
    assert pd.read_parquet(run_dir / "book_tape_event.parquet").shape[0] == 1
    assert pd.read_parquet(run_dir / "book_tape_level.parquet").shape[0] == 2
    assert trades["local_sequence"].tolist() == [2, 3]
    assert trades["subsequence"].tolist() == [3, 3]
    assert set(trades["local_sequence"]).isdisjoint(set(health["local_sequence"]))
    assert pd.read_parquet(run_dir / "stream_lifecycle.parquet").shape[0] == 1
    subscription = json.loads(fake.sent[0])
    assert subscription["params"]["channels"] == [
        "orderbook_delta",
        "trade",
        "market_lifecycle_v2",
    ]
    assert manifest["storage_profile"]["terminal_completeness"] == "partial"
    assert manifest["feed_control_plane"]["policy_version"] == ("feed-control-plane.v1")
    assert manifest["feed_control_plane"]["enabled"] is True
    assert manifest["feed_control_plane"]["suppression_reason"] is None
    assert manifest["feed_control_plane"]["tick_interval_ms"] == 250.0
    assert manifest["feed_control_plane"]["ticks"] == 0
    assert manifest["feed_control_plane"]["stale_instrument_checks"] == 0
    assert manifest["feed_control_plane"]["transition_row_builds"] == 1
    assert manifest["feed_control_plane"]["recovery_evaluations"] == 1
    assert manifest["feed_health_emission"]["rows_evaluated"] == 3
    assert validate_run_manifest(run_dir / "manifest.json").ok


@pytest.mark.asyncio
async def test_sqlite_storage_profile_promotes_once_for_kalshi(tmp_path) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "market_id": "market-id",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            )
        ]
    )

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    manifest = await stream_kalshi_order_book_data(
        ["KXTEST"],
        output_root=tmp_path,
        run_name="sqlite-profile-run",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
        storage_profile=select_storage_profile("full"),
        capture_storage_backend=CaptureStorageBackend.SQLITE_WAL,
    )

    run_dir = tmp_path / "sqlite-profile-run"
    assert (run_dir / "capture.sqlite3").is_file()
    assert manifest["capture_storage"]["configuration"]["backend"] == ("sqlite_wal_v1")
    assert manifest["capture_storage"]["metrics"]["promotion"]["attempt_count"] == 1
    assert manifest["capture_storage"]["metrics"]["promotion"]["failure_count"] == 0
    for artifact in manifest["dataset_artifacts"].values():
        assert len(list((run_dir / artifact["path"]).glob("*.parquet"))) == 1
    validation = validate_run_manifest(run_dir / "manifest.json")
    assert validation.ok, validation.all_errors



@pytest.mark.asyncio
async def test_malformed_trade_observation_does_not_end_kalshi_capture(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "market_id": "market-id",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            ),
            json.dumps(
                {
                    "type": "trade",
                    "msg": {"trade_id": "bad-trade", "market_ticker": "KXTEST"},
                }
            ),
        ]
    )

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    manifest = await stream_kalshi_order_book_data(
        ["KXTEST"],
        output_root=tmp_path,
        run_name="malformed-trade-run",
        duration_s=0,
        max_messages=2,
        capture_intent="smoke",
        max_reconnects=0,
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
        storage_profile=select_storage_profile("full", profile_version="1"),
    )

    assert manifest["status"] == "success"
    assert pd.read_parquet(tmp_path / "malformed-trade-run" / "trades.parquet").empty


@pytest.mark.asyncio
async def test_stream_kalshi_order_book_data_writes_analysis_artifacts(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "market_id": "market-id",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            ),
            json.dumps(
                {
                    "type": "orderbook_delta",
                    "sid": 1,
                    "seq": 2,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "market_id": "market-id",
                        "price_dollars": "0.45",
                        "delta_fp": "2.00",
                        "side": "yes",
                        "ts_ms": 1703123456789,
                    },
                }
            ),
        ]
    )

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    manifest = await stream_kalshi_order_book_data(
        ["KXTEST"],
        output_root=tmp_path,
        run_name="test-run",
        duration_s=10,
        max_messages=2,
        capture_intent="smoke",
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
        command="pmkt stream kalshi --fixture",
        git_commit="def456",
        parquet_segment_rows=1,
    )

    run_dir = tmp_path / "test-run"
    assert manifest["run_dir"] == str(run_dir)
    assert manifest["counts"] == {
        "events": 2,
        "snapshots": 2,
        "levels": 3,
        "markets_with_snapshots": 1,
    }
    assert (run_dir / "manifest.json").exists()
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
        "topbook_v1.parquet": 4,
        "depth_v1.parquet": 5,
        "feed_health.parquet": 2,
    }.items():
        path = run_dir / name
        segment_manifest = read_parquet_segment_manifest(path)
        assert path.is_dir()
        assert segment_manifest is not None
        assert len(segment_manifest["completed_segments"]) == expected_count
        assert segment_manifest["incomplete_segments"] == []

    assert events["event_type"].tolist() == ["orderbook_snapshot", "orderbook_delta"]
    assert snapshots["yes_bid"].tolist() == pytest.approx([0.4, 0.45])
    assert snapshots["yes_ask"].tolist() == pytest.approx([0.65, 0.65])
    assert snapshots["valid_state"].tolist() == [True, True]
    assert snapshots["quality_flags"].tolist() == ["", ""]
    assert levels["is_delta"].tolist() == [False, False, True]
    assert topbook["schema_version"].tolist() == ["topbook.v1"] * 4
    assert topbook["exchange"].tolist() == ["kalshi"] * 4
    assert topbook["bid_size_contracts"].tolist() == pytest.approx([10, 5, 2, 5])
    assert topbook["ask_size_contracts"].tolist() == pytest.approx([5, 10, 5, 2])
    assert depth["schema_version"].tolist() == ["depth.v1"] * 5
    assert depth["valid_state"].tolist() == [True] * 5
    assert depth["is_delta"].tolist() == [False] * 5
    assert depth.groupby("local_sequence")["received_at_utc"].nunique().to_dict() == {
        1: 1,
        2: 1,
    }
    assert depth.groupby("local_sequence")["received_at_utc"].first().is_unique
    assert depth["size_contracts"].notna().all()
    assert depth["level_index"].notna().all()
    assert manifest["schema_versions"]["topbook"] == "topbook.v1"
    assert manifest["schema_versions"]["feed_health"] == "feed_health.v1"
    assert manifest["row_counts"]["topbook"] == 4
    assert manifest["row_counts"]["depth"] == 5
    assert manifest["row_counts"]["feed_health"] == 2
    assert manifest["command"] == "pmkt stream kalshi --fixture"
    assert manifest["git_commit"] == "def456"
    assert manifest["venue_counts"] == {"kalshi": 4}
    assert manifest["instrument_counts"] == {"KXTEST:NO": 2, "KXTEST:YES": 2}
    assert manifest["dataset_paths"]["feed_health_parquet"] == str(
        run_dir / "feed_health.parquet"
    )
    assert health["local_sequence"].tolist() == [1, 2]
    assert health["venue"].tolist() == ["kalshi", "kalshi"]
    assert health["instrument_count"].tolist() == [1, 1]
    assert health["valid_book_count"].tolist() == [1, 2]
    assert health["last_valid_book_age_ms"].notna().all()
    assert json.loads(fake.sent[0])["params"] == {
        "channels": ["orderbook_delta"],
        "market_tickers": ["KXTEST"],
        "use_yes_price": True,
    }
    assert fake.closed is True
    assert validate_run_manifest(run_dir / "manifest.json").ok



@pytest.mark.asyncio
async def test_off_subscription_kalshi_book_uses_connection_health_shard(
    tmp_path,
) -> None:
    clock = ManualMonotonicClock()
    fake = ClockDrivenFakeWebSocket(
        [
            1.0,
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXPEER",
                        "market_id": "peer-market-id",
                        "yes_dollars_fp": [["0.30", "8.00"]],
                        "no_dollars_fp": [["0.70", "6.00"]],
                    },
                }
            ),
        ],
        clock,
    )

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    shard = FeedShardHealth(
        venue="kalshi",
        shard_id="kalshi-0",
        subscribed_instruments=("KXOWN",),
    )
    manifest = await stream_kalshi_order_book_data(
        ["KXOWN"],
        output_root=tmp_path,
        run_name="kalshi-off-subscription-run",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
        feed_supervisor=LiveFeedSupervisor([shard]),
        monotonic_ns=clock,
    )

    assert manifest["counts"] == {
        "events": 1,
        "snapshots": 0,
        "levels": 0,
        "markets_with_snapshots": 0,
    }
    assert manifest["row_counts"]["topbook"] == 0
    assert shard.last_message_monotonic_ns == 1_000_000_000
    assert "KXPEER" not in shard.instrument_health
    raw_rows = (
        (tmp_path / "kalshi-off-subscription-run" / "raw_events.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    )
    assert len(raw_rows) == 1


@pytest.mark.asyncio
async def test_operational_kalshi_max_messages_writes_failed_terminal_control(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "market_id": "market-id",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            )
        ]
    )

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    with pytest.raises(
        CaptureCompletenessError,
        match="operational capture terminated via max_messages_reached",
    ):
        await stream_kalshi_order_book_data(
            ["KXTEST"],
            output_root=tmp_path,
            run_name="kalshi-operational-max-messages",
            duration_s=10,
            max_messages=1,
            connect_factory=connect_factory,
            auth=FakeReadAuth(),
            storage_profile=select_storage_profile("full"),
        )

    run_dir = tmp_path / "kalshi-operational-max-messages"
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
async def test_stream_kalshi_order_book_data_preserves_feed_supervisor_metadata(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            )
        ]
    )

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="kalshi",
                shard_id="kx-plan",
                subscribed_instruments=("KXTEST",),
                relation_ids=("match-1", "match-2"),
            )
        ]
    )

    manifest = await stream_kalshi_order_book_data(
        ["KXTEST"],
        output_root=tmp_path,
        run_name="kalshi-plan-health-run",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
        feed_supervisor=supervisor,
        subscription_plan_metadata={
            "plan_id": "kalshi-plan-health",
            "path": "generated/plan.json",
            "sha256": "b" * 64,
        },
    )

    health = pd.read_parquet(
        tmp_path / "kalshi-plan-health-run" / "feed_health.parquet"
    )
    assert manifest["subscription_plan"] == {
        "plan_id": "kalshi-plan-health",
        "path": "generated/plan.json",
        "sha256": "b" * 64,
    }
    assert health["shard_id"].tolist() == ["kx-plan"]
    assert health["relation_count"].tolist() == [2]
    assert manifest["feed_shards"] == [
        {
            "venue": "kalshi",
            "shard_id": "kx-plan",
            "instrument_count": 1,
            "relation_count": 2,
            "subscribed_instruments": ["KXTEST"],
            "relation_ids": ["match-1", "match-2"],
        }
    ]
    summary = manifest["feed_health_summary"]
    assert summary["shard_count"] == 1
    assert summary["instrument_count"] == 1
    assert summary["relation_count"] == 2
    assert summary["valid_book_count"] == 1
    assert summary["shards"][0]["shard_id"] == "kx-plan"


@pytest.mark.asyncio
async def test_stream_kalshi_order_book_data_marks_silent_feed_stale(tmp_path) -> None:
    fake = SilentWebSocket()

    async def connect_factory(_: str, __: dict[str, str]) -> SilentWebSocket:
        return fake

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="kalshi",
                shard_id="kx-plan",
                subscribed_instruments=("KXTEST",),
            )
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )

    manifest = await stream_kalshi_order_book_data(
        ["KXTEST"],
        output_root=tmp_path,
        run_name="kalshi-silent-run",
        duration_s=0.12,
        capture_intent="smoke",
        max_reconnects=0,
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
        feed_supervisor=supervisor,
    )

    health = pd.read_parquet(tmp_path / "kalshi-silent-run" / "feed_health.parquet")
    assert manifest["row_counts"]["events"] == 0
    assert manifest["feed_health_summary"]["stale_shard_count"] >= 1
    assert len(health) >= 1
    assert health["connection_state"].tolist()[-1] == "stale"
    assert health["valid_book_count"].tolist()[-1] == 0
    assert "stale_messages" in health["quality_flags"].tolist()[-1]
    assert "stale_books" in health["quality_flags"].tolist()[-1]
    assert manifest["socket_recovery_count"] == 0


@pytest.mark.asyncio
async def test_stream_kalshi_order_book_data_recovers_silent_feed_with_reconnect(
    tmp_path,
) -> None:
    first = SilentWebSocket()
    second = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            )
        ]
    )
    sockets = deque([first, second])

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return sockets.popleft()

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="kalshi",
                shard_id="kx-plan",
                subscribed_instruments=("KXTEST",),
            )
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )

    manifest = await stream_kalshi_order_book_data(
        ["KXTEST"],
        output_root=tmp_path,
        run_name="kalshi-silent-recovery-run",
        duration_s=1.0,
        max_messages=1,
        capture_intent="smoke",
        max_reconnects=2,
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
        feed_supervisor=supervisor,
    )

    run_dir = tmp_path / "kalshi-silent-recovery-run"
    topbook = pd.read_parquet(run_dir / "topbook_v1.parquet")
    health = pd.read_parquet(run_dir / "feed_health.parquet")

    assert first.closed is True
    assert json.loads(second.sent[0])["params"]["market_tickers"] == ["KXTEST"]
    assert manifest["row_counts"]["events"] == 1
    assert manifest["reconnect_count"] == 1
    assert manifest["socket_recovery_count"] == 1
    assert topbook["valid_state"].tolist() == [True, True]
    assert "stale" in health["connection_state"].tolist()
    assert health["connection_state"].tolist()[-1] == "connected"
    assert validate_run_manifest(run_dir / "manifest.json").ok


@pytest.mark.asyncio
async def test_stream_kalshi_refreshes_stale_market_on_live_transport(
    tmp_path,
) -> None:
    fake = SnapshotThenPongWebSocket(
        json.dumps(
            {
                "type": "orderbook_snapshot",
                "sid": 7,
                "seq": 1,
                "msg": {
                    "market_ticker": "KXTEST",
                    "yes_dollars_fp": [["0.40", "10.00"]],
                    "no_dollars_fp": [["0.65", "5.00"]],
                },
            }
        )
    )

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="kalshi",
                shard_id="kx-plan",
                subscribed_instruments=("KXTEST",),
            )
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )

    manifest = await stream_kalshi_order_book_data(
        ["KXTEST"],
        output_root=tmp_path,
        run_name="kalshi-targeted-refresh-run",
        duration_s=0.5,
        capture_intent="smoke",
        max_reconnects=1,
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
        feed_supervisor=supervisor,
    )

    sent = [json.loads(payload) for payload in fake.sent]
    recovery = manifest["kalshi_feed_recovery"]
    assert fake.ping_count >= 1
    assert any(
        payload.get("cmd") == "update_subscription"
        and payload["params"].get("action") == "get_snapshot"
        for payload in sent
    )
    assert manifest["socket_recovery_count"] == 0
    assert recovery["transport_liveness_probe_success_count"] >= 1
    assert recovery["transport_liveness_probe_failure_count"] == 0
    assert recovery["targeted_snapshot_refresh_exhausted_count"] == 0
    assert recovery["targeted_snapshot_market_count"] >= 1
    shard_row = manifest["feed_health_summary"]["shards"][0]
    assert shard_row["connection_state"] == "connected"
    assert shard_row["stale_instrument_count"] == 1


@pytest.mark.asyncio
async def test_stream_kalshi_reconnects_when_targeted_refresh_send_fails(
    tmp_path,
) -> None:
    snapshot = json.dumps(
        {
            "type": "orderbook_snapshot",
            "sid": 7,
            "seq": 1,
            "msg": {
                "market_ticker": "KXTEST",
                "yes_dollars_fp": [["0.40", "10.00"]],
                "no_dollars_fp": [["0.65", "5.00"]],
            },
        }
    )
    first = SnapshotThenFailedRefreshWebSocket(snapshot)
    second = FakeWebSocket([snapshot])
    sockets = deque([first, second])

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return sockets.popleft()

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="kalshi",
                shard_id="kx-plan",
                subscribed_instruments=("KXTEST",),
            )
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )

    manifest = await stream_kalshi_order_book_data(
        ["KXTEST"],
        output_root=tmp_path,
        run_name="kalshi-targeted-refresh-fallback-run",
        duration_s=1.0,
        max_messages=2,
        capture_intent="smoke",
        max_reconnects=1,
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
        feed_supervisor=supervisor,
    )

    recovery = manifest["kalshi_feed_recovery"]
    assert first.ping_count >= 1
    assert recovery["transport_liveness_probe_success_count"] == 1
    assert recovery["transport_liveness_probe_failure_count"] == 0
    assert recovery["targeted_snapshot_refresh_failure_count"] == 1
    assert manifest["socket_recovery_count"] == 1
    assert manifest["reconnect_count"] == 1


@pytest.mark.asyncio
async def test_stream_kalshi_reconnects_when_targeted_refresh_is_ignored(
    tmp_path,
) -> None:
    snapshot = json.dumps(
        {
            "type": "orderbook_snapshot",
            "sid": 7,
            "seq": 1,
            "msg": {
                "market_ticker": "KXTEST",
                "yes_dollars_fp": [["0.40", "10.00"]],
                "no_dollars_fp": [["0.65", "5.00"]],
            },
        }
    )
    first = SnapshotThenPongWebSocket(snapshot)
    second = FakeWebSocket([snapshot])
    sockets = deque([first, second])

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return sockets.popleft()

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="kalshi",
                shard_id="kx-plan",
                subscribed_instruments=("KXTEST",),
            )
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )

    manifest = await stream_kalshi_order_book_data(
        ["KXTEST"],
        output_root=tmp_path,
        run_name="kalshi-targeted-refresh-exhausted-run",
        duration_s=1.5,
        max_messages=2,
        capture_intent="smoke",
        max_reconnects=1,
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
        feed_supervisor=supervisor,
    )

    recovery = manifest["kalshi_feed_recovery"]
    assert first.ping_count == 1
    assert recovery["targeted_snapshot_refresh_count"] == 1
    assert recovery["targeted_snapshot_refresh_exhausted_count"] == 1
    assert manifest["socket_recovery_count"] == 1
    assert manifest["reconnect_count"] == 1


@pytest.mark.asyncio
async def test_stream_kalshi_order_book_data_rejects_multiple_shards_on_one_connection(
    tmp_path,
) -> None:
    # The clock advances by exactly 60ms between the two KXONE messages, which
    # exceeds ``max_message_age_ms``/``max_valid_book_age_ms`` (20ms), so the idle
    # KXTWO shard is deterministically stale by the final health write — no
    # dependence on real elapsed wall-clock time.
    clock = ManualMonotonicClock()
    fake = ClockDrivenFakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXONE",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            ),
            0.06,
            json.dumps(
                {
                    "type": "orderbook_delta",
                    "sid": 1,
                    "seq": 2,
                    "msg": {
                        "market_ticker": "KXONE",
                        "price_dollars": "0.41",
                        "delta_fp": "2.00",
                        "side": "yes",
                    },
                }
            ),
        ],
        clock,
    )

    async def connect_factory(_: str, __: dict[str, str]) -> ClockDrivenFakeWebSocket:
        return fake

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="kalshi",
                shard_id="kx-one",
                subscribed_instruments=("KXONE",),
            ),
            FeedShardHealth(
                venue="kalshi",
                shard_id="kx-two",
                subscribed_instruments=("KXTWO",),
            ),
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )

    with pytest.raises(ValueError, match="exactly one feed shard"):
        await stream_kalshi_order_book_data(
            ["KXONE", "KXTWO"],
            output_root=tmp_path,
            run_name="kalshi-multi-shard-stale-run",
            duration_s=10,
            max_messages=2,
            capture_intent="smoke",
            max_reconnects=0,
            connect_factory=connect_factory,
            auth=FakeReadAuth(),
            feed_supervisor=supervisor,
            monotonic_ns=clock,
            storage_profile=select_storage_profile("full"),
        )


@pytest.mark.asyncio
async def test_stream_kalshi_order_book_data_emits_complete_same_shard_stale_transition(
    tmp_path,
) -> None:
    clock = ManualMonotonicClock()
    fake = ClockDrivenFakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXONE",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            ),
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 2,
                    "msg": {
                        "market_ticker": "KXTWO",
                        "yes_dollars_fp": [["0.30", "8.00"]],
                        "no_dollars_fp": [["0.75", "6.00"]],
                    },
                }
            ),
            0.06,
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 3,
                    "msg": {
                        "market_ticker": "KXONE",
                        "yes_dollars_fp": [["0.41", "12.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            ),
        ],
        clock,
    )

    async def connect_factory(_: str, __: dict[str, str]) -> ClockDrivenFakeWebSocket:
        return fake

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="kalshi",
                shard_id="kx-shared",
                subscribed_instruments=("KXONE", "KXTWO"),
            )
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )

    await stream_kalshi_order_book_data(
        ["KXONE", "KXTWO"],
        output_root=tmp_path,
        run_name="kalshi-same-shard-stale-run",
        duration_s=10,
        max_messages=3,
        capture_intent="smoke",
        max_reconnects=0,
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
        feed_supervisor=supervisor,
        monotonic_ns=clock,
        storage_profile=select_storage_profile("full"),
    )

    health = pd.read_parquet(
        tmp_path / "kalshi-same-shard-stale-run" / "feed_health.parquet"
    )
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
async def test_stream_kalshi_order_book_data_recovers_idle_shard_while_peer_is_active(
    tmp_path,
) -> None:
    first = DelayedFakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXONE",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            ),
            0.03,
            json.dumps(
                {
                    "type": "orderbook_delta",
                    "sid": 1,
                    "seq": 2,
                    "msg": {
                        "market_ticker": "KXONE",
                        "price_dollars": "0.41",
                        "delta_fp": "2.00",
                        "side": "yes",
                    },
                }
            ),
        ]
    )
    second = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 2,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTWO",
                        "yes_dollars_fp": [["0.30", "8.00"]],
                        "no_dollars_fp": [["0.75", "6.00"]],
                    },
                }
            ),
            json.dumps(
                {
                    "type": "orderbook_delta",
                    "sid": 2,
                    "seq": 2,
                    "msg": {
                        "market_ticker": "KXTWO",
                        "price_dollars": "0.31",
                        "delta_fp": "1.00",
                        "side": "yes",
                    },
                }
            ),
        ]
    )
    third = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 3,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXONE",
                        "yes_dollars_fp": [["0.42", "9.00"]],
                        "no_dollars_fp": [["0.63", "4.00"]],
                    },
                }
            )
        ]
    )
    sockets = deque([first, second, third])

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return sockets.popleft()

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="kalshi",
                shard_id="kx-shared",
                subscribed_instruments=("KXONE", "KXTWO"),
            ),
        ],
        max_message_age_ms=20,
        max_valid_book_age_ms=20,
    )

    manifest = await stream_kalshi_order_book_data(
        ["KXONE", "KXTWO"],
        output_root=tmp_path,
        run_name="kalshi-multi-shard-recovery-run",
        duration_s=2,
        max_messages=3,
        capture_intent="smoke",
        max_reconnects=2,
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
        feed_supervisor=supervisor,
    )

    run_dir = tmp_path / "kalshi-multi-shard-recovery-run"
    topbook = pd.read_parquet(run_dir / "topbook_v1.parquet")
    health = pd.read_parquet(run_dir / "feed_health.parquet")

    assert first.closed is True
    assert json.loads(second.sent[0])["params"]["market_tickers"] == [
        "KXONE",
        "KXTWO",
    ]
    assert manifest["row_counts"]["events"] >= 2
    assert 1 <= manifest["socket_recovery_count"] <= 2
    assert 1 <= manifest["reconnect_count"] <= 2
    assert "KXTWO:YES" in set(topbook["instrument_id"])
    latest_kxtwo = topbook[topbook["instrument_id"] == "KXTWO:YES"].iloc[-1]
    assert bool(latest_kxtwo["valid_state"]) is True
    incomplete_rows = health[
        (health["shard_id"] == "kx-shared")
        & (
            health["quality_flags"].apply(
                lambda _f: "missing_instrument_books" in (_f if _f is not None else [])
            )
        )
    ]
    assert not incomplete_rows.empty


@pytest.mark.asyncio
async def test_stream_kalshi_order_book_manifest_counts_sequence_gap_events_once(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            ),
            json.dumps(
                {
                    "type": "orderbook_delta",
                    "sid": 1,
                    "seq": 3,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "price_dollars": "0.45",
                        "delta_fp": "2.00",
                        "side": "yes",
                    },
                }
            ),
            json.dumps(
                {
                    "type": "orderbook_delta",
                    "sid": 1,
                    "seq": 4,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "price_dollars": "0.30",
                        "delta_fp": "1.00",
                        "side": "no",
                    },
                }
            ),
        ]
    )

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    manifest = await stream_kalshi_order_book_data(
        ["KXTEST"],
        output_root=tmp_path,
        run_name="gap-run",
        duration_s=10,
        max_messages=3,
        capture_intent="smoke",
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
    )

    assert manifest["sequence_gap_count"] == 1
    assert manifest["resync_event_count"] == 1
    assert manifest["snapshot_resync_request_count"] == 1
    assert manifest["quality_flag_counts"]["seq_gap"] > 1
    assert json.loads(fake.sent[1])["cmd"] == "update_subscription"
    assert json.loads(fake.sent[1])["params"] == {
        "sids": [1],
        "market_tickers": ["KXTEST"],
        "action": "get_snapshot",
        "use_yes_price": True,
    }
    health = pd.read_parquet(tmp_path / "gap-run" / "feed_health.parquet")
    assert health["sequence_gap_count"].tolist()[-1] == 1
    assert health["valid_book_count"].tolist()[-1] == 0
    assert "sequence_gap" in health["quality_flags"].tolist()[-1]


@pytest.mark.asyncio
async def test_stream_kalshi_order_book_resyncs_every_market_on_subscription_gap(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXONE",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            ),
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 2,
                    "msg": {
                        "market_ticker": "KXTWO",
                        "yes_dollars_fp": [["0.30", "10.00"]],
                        "no_dollars_fp": [["0.75", "5.00"]],
                    },
                }
            ),
            json.dumps(
                {
                    "type": "orderbook_delta",
                    "sid": 1,
                    "seq": 4,
                    "msg": {
                        "market_ticker": "KXONE",
                        "price_dollars": "0.45",
                        "delta_fp": "2.00",
                        "side": "yes",
                    },
                }
            ),
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 5,
                    "msg": {
                        "market_ticker": "KXONE",
                        "yes_dollars_fp": [["0.45", "12.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            ),
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 6,
                    "msg": {
                        "market_ticker": "KXTWO",
                        "yes_dollars_fp": [["0.35", "10.00"]],
                        "no_dollars_fp": [["0.75", "5.00"]],
                    },
                }
            ),
        ]
    )

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    manifest = await stream_kalshi_order_book_data(
        ["KXONE", "KXTWO"],
        output_root=tmp_path,
        run_name="multi-market-gap-run",
        duration_s=10,
        max_messages=5,
        capture_intent="smoke",
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
    )

    assert manifest["sequence_gap_count"] == 1
    assert manifest["snapshot_resync_request_count"] == 1
    assert json.loads(fake.sent[1])["params"] == {
        "sids": [1],
        "market_tickers": ["KXONE", "KXTWO"],
        "action": "get_snapshot",
        "use_yes_price": True,
    }
    snapshots = pd.read_parquet(tmp_path / "multi-market-gap-run" / "snapshots.parquet")
    gap_rows = snapshots[snapshots["sequence"] == 3]
    assert set(gap_rows["market_ticker"]) == {"KXONE", "KXTWO"}
    assert not gap_rows["valid_state"].any()
    assert (
        gap_rows["quality_flags"]
        .apply(lambda _f: "seq_gap" in (_f if _f is not None else []))
        .all()
    )
    health = pd.read_parquet(tmp_path / "multi-market-gap-run" / "feed_health.parquet")
    assert "sequence_gap" in health.iloc[2]["quality_flags"]
    assert len(health.iloc[-1]["quality_flags"]) == 0


@pytest.mark.asyncio
async def test_mm_compact_restates_topbook_after_resync(monkeypatch, tmp_path) -> None:
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
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            ),
            json.dumps(
                {
                    "type": "orderbook_delta",
                    "sid": 1,
                    "seq": 3,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "price_dollars": "0.45",
                        "delta_fp": "2.00",
                        "side": "yes",
                    },
                }
            ),
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 4,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "yes_dollars_fp": [["0.45", "12.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            ),
        ]
    )

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    manifest = await stream_kalshi_order_book_data(
        ["KXTEST"],
        output_root=tmp_path,
        run_name="compact-resync-run",
        duration_s=10,
        max_messages=3,
        capture_intent="smoke",
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
        storage_profile=select_storage_profile(
            "mm-compact",
            experimental_profile_acknowledged=True,
        ),
    )

    assert manifest["sequence_gap_count"] == 1
    assert "resync" in boundary_reasons
    assert validate_run_manifest(tmp_path / "compact-resync-run" / "manifest.json").ok


@pytest.mark.asyncio
async def test_concurrent_kalshi_streams_isolate_same_subscription_id(tmp_path) -> None:
    def websocket(ticker: str, start_sequence: int) -> FakeWebSocket:
        return FakeWebSocket(
            [
                json.dumps(
                    {
                        "type": "orderbook_snapshot",
                        "sid": 1,
                        "seq": start_sequence,
                        "msg": {
                            "market_ticker": ticker,
                            "yes_dollars_fp": [["0.40", "10.00"]],
                            "no_dollars_fp": [["0.65", "5.00"]],
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "orderbook_delta",
                        "sid": 1,
                        "seq": start_sequence + 1,
                        "msg": {
                            "market_ticker": ticker,
                            "price_dollars": "0.45",
                            "delta_fp": "2.00",
                            "side": "yes",
                        },
                    }
                ),
            ]
        )

    first = websocket("KXONE", 1)
    second = websocket("KXTWO", 100)

    async def first_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return first

    async def second_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return second

    manifests = await asyncio.gather(
        stream_kalshi_order_book_data(
            ["KXONE"],
            output_root=tmp_path,
            run_name="concurrent-one",
            duration_s=10,
            max_messages=2,
            capture_intent="smoke",
            connect_factory=first_factory,
            auth=FakeReadAuth(),
        ),
        stream_kalshi_order_book_data(
            ["KXTWO"],
            output_root=tmp_path,
            run_name="concurrent-two",
            duration_s=10,
            max_messages=2,
            capture_intent="smoke",
            connect_factory=second_factory,
            auth=FakeReadAuth(),
        ),
    )

    assert [manifest["sequence_gap_count"] for manifest in manifests] == [0, 0]
    assert len(first.sent) == 1
    assert len(second.sent) == 1


@pytest.mark.asyncio
async def test_stream_kalshi_order_book_data_marks_reconnect_invalid_until_snapshot(
    tmp_path,
) -> None:
    first = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            ),
            OSError("connection dropped"),
        ]
    )
    second = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_delta",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "price_dollars": "0.45",
                        "delta_fp": "2.00",
                        "side": "yes",
                    },
                }
            )
        ]
    )
    sockets = deque([first, second])

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return sockets.popleft()

    await stream_kalshi_order_book_data(
        ["KXTEST"],
        output_root=tmp_path,
        run_name="kalshi-reconnect-run",
        duration_s=10,
        max_messages=2,
        capture_intent="smoke",
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
    )

    run_dir = tmp_path / "kalshi-reconnect-run"
    topbook = pd.read_parquet(run_dir / "topbook_v1.parquet")
    depth = pd.read_parquet(run_dir / "depth_v1.parquet")
    health = pd.read_parquet(run_dir / "feed_health.parquet")
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert topbook["valid_state"].tolist() == [True, True, False, False]
    assert "reconnect" in topbook["quality_flags"].tolist()[-1]
    assert depth["valid_state"].tolist()[-1] == False  # noqa: E712
    assert "reconnect" in depth["quality_flags"].tolist()[-1]
    assert health["reconnect_count"].tolist()[-1] == 1
    assert health["valid_book_count"].tolist()[-1] == 0
    assert "reconnect" in health["quality_flags"].tolist()[-1]
    assert manifest["reconnect_count"] == 1
    assert manifest["quality_flag_counts"]["reconnect"] >= 1


@pytest.mark.asyncio
async def test_stream_kalshi_order_book_data_writes_partial_manifest_on_failure(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            ),
            RuntimeError("kalshi stream failed"),
        ]
    )

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    with pytest.raises(RuntimeError, match="kalshi stream failed"):
        await stream_kalshi_order_book_data(
            ["KXTEST"],
            output_root=tmp_path,
            run_name="failed-run",
            duration_s=10,
            max_messages=2,
            capture_intent="smoke",
            connect_factory=connect_factory,
            auth=FakeReadAuth(),
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
    assert manifest["row_counts"]["topbook"] == 2
    assert health["error_count"].tolist()[-1] == 1
    assert "error:RuntimeError" in health["quality_flags"].tolist()[-1]


@pytest.mark.asyncio
async def test_kalshi_clean_close_exhaustion_is_classified_as_stream_error(
    tmp_path,
) -> None:
    fake = FakeWebSocket([])

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    with pytest.raises(ConnectionError, match="reconnect budget"):
        await stream_kalshi_order_book_data(
            ["KXTEST"],
            output_root=tmp_path,
            run_name="clean-close-failed-run",
            duration_s=10,
            max_messages=1,
            max_reconnects=0,
            capture_intent="smoke",
            connect_factory=connect_factory,
            auth=FakeReadAuth(),
        )

    manifest = json.loads(
        (tmp_path / "clean-close-failed-run" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["error_type"] == "ConnectionError"
    assert manifest["capture_completeness"]["terminal_reason"] == "stream_error"


@pytest.mark.asyncio
async def test_stream_kalshi_persists_cancelled_manifest(tmp_path) -> None:
    fake = SilentWebSocket()

    async def connect_factory(_: str, __: dict[str, str]) -> SilentWebSocket:
        return fake

    task = asyncio.create_task(
        stream_kalshi_order_book_data(
            ["KXTEST"],
            output_root=tmp_path,
            run_name="cancelled-run",
            duration_s=60,
            connect_factory=connect_factory,
            auth=FakeReadAuth(),
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
async def test_stream_kalshi_finalizes_completeness_before_snapshot_failure(
    tmp_path,
) -> None:
    fake = FakeWebSocket([RuntimeError("kalshi failed before snapshot")])

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    with pytest.raises(RuntimeError, match="kalshi failed before snapshot"):
        await stream_kalshi_order_book_data(
            ["KXTEST"],
            output_root=tmp_path,
            run_name="failed-before-snapshot",
            duration_s=10,
            max_messages=1,
            capture_intent="smoke",
            connect_factory=connect_factory,
            auth=FakeReadAuth(),
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
async def test_mm_compact_kalshi_emits_startup_and_terminal_restatements(
    tmp_path,
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "market_id": "market-id",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            )
        ]
    )

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="kalshi",
                shard_id="kx-connection-007",
                subscribed_instruments=("KXTEST",),
            )
        ]
    )

    manifest = await stream_kalshi_order_book_data(
        ["KXTEST"],
        output_root=tmp_path,
        run_name="compact-kalshi-run",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
        feed_supervisor=supervisor,
        storage_profile=select_storage_profile(
            "mm-compact",
            profile_version="1",
            experimental_profile_acknowledged=True,
        ),
    )

    run_dir = tmp_path / "compact-kalshi-run"
    topbook_main = pd.read_parquet(run_dir / "topbook_v1.parquet")
    checkpoints = pd.read_parquet(run_dir / "topbook_checkpoints.parquet")
    assert topbook_main.shape[0] == 2
    assert checkpoints.shape[0] == 4
    assert checkpoints.groupby("local_sequence").size().to_dict() == {2: 2, 3: 2}
    assert set(topbook_main["received_at_utc"]).isdisjoint(
        set(checkpoints["received_at_utc"])
    )
    controls = pd.read_parquet(run_dir / "book_tape_control.parquet")
    terminal_at = pd.to_datetime(
        controls.loc[controls["control_type"] == "stream_ended", "received_at_utc"],
        utc=True,
    ).min()
    assert terminal_at > pd.to_datetime(checkpoints["received_at_utc"], utc=True).max()
    health = pd.read_parquet(run_dir / "feed_health.parquet")
    assert terminal_at > pd.to_datetime(health["observed_at_utc"], utc=True).max()
    assert manifest["storage_profile"]["experimental_profile_acknowledged"] is True
    run_state = json.loads((run_dir / "run_state.v1.json").read_text(encoding="utf-8"))
    assert run_state["storage_profile"]["experimental_profile_acknowledged"] is True
    assert run_state["shard_plan"] == {"kx-connection-007": ["KXTEST"]}
    assert manifest["feed_shards"][0]["shard_id"] == "kx-connection-007"
    assert validate_run_manifest(run_dir / "manifest.json").ok


@pytest.mark.asyncio
async def test_kalshi_terminal_health_failure_emits_one_final_failed_boundary(
    monkeypatch, tmp_path
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "market_id": "market-id",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            )
        ]
    )

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    original_prepare = SlimHealthEmitter.prepare

    def fail_terminal_health(self, *args, **kwargs):
        if kwargs.get("cause") == "terminal":
            raise RuntimeError("terminal health failed")
        return original_prepare(self, *args, **kwargs)

    monkeypatch.setattr(SlimHealthEmitter, "prepare", fail_terminal_health)
    with pytest.raises(RuntimeError, match="terminal health failed"):
        await stream_kalshi_order_book_data(
            ["KXTEST"],
            output_root=tmp_path,
            run_name="terminal-health-failure",
            duration_s=10,
            max_messages=1,
            capture_intent="smoke",
            connect_factory=connect_factory,
            auth=FakeReadAuth(),
            storage_profile=select_storage_profile(
                "mm-compact",
                experimental_profile_acknowledged=True,
            ),
        )

    run_dir = tmp_path / "terminal-health-failure"
    controls = pd.read_parquet(run_dir / "book_tape_control.parquet")
    terminal = controls[controls["control_type"] == "stream_ended"]
    assert terminal.shape[0] == 2
    assert set(terminal["reason"]) == {"failed"}
    terminal_at = pd.to_datetime(terminal["received_at_utc"], utc=True).min()
    checkpoints = pd.read_parquet(run_dir / "topbook_checkpoints.parquet")
    health = pd.read_parquet(run_dir / "feed_health.parquet")
    assert terminal_at > pd.to_datetime(checkpoints["received_at_utc"], utc=True).max()
    assert terminal_at > pd.to_datetime(health["observed_at_utc"], utc=True).max()
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert manifest["storage_profile"]["terminal_completeness"] == "failed"
    assert validate_run_manifest(run_dir / "manifest.json").ok


@pytest.mark.asyncio
async def test_kalshi_termination_commit_failure_does_not_append_after_terminal(
    monkeypatch, tmp_path
) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "market_id": "market-id",
                        "yes_dollars_fp": [["0.40", "10.00"]],
                        "no_dollars_fp": [["0.65", "5.00"]],
                    },
                }
            )
        ]
    )

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
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
        await stream_kalshi_order_book_data(
            ["KXTEST"],
            output_root=tmp_path,
            run_name="termination-commit-failure",
            duration_s=10,
            max_messages=1,
            capture_intent="smoke",
            connect_factory=connect_factory,
            auth=FakeReadAuth(),
            storage_profile=select_storage_profile(
                "mm-compact",
                experimental_profile_acknowledged=True,
            ),
        )

    run_dir = tmp_path / "termination-commit-failure"
    controls = pd.read_parquet(run_dir / "book_tape_control.parquet")
    terminal = controls[controls["control_type"] == "stream_ended"]
    assert terminal.shape[0] == 2
    assert set(terminal["reason"]) == {"completed"}
    terminal_at = pd.to_datetime(terminal["received_at_utc"], utc=True).min()
    checkpoints = pd.read_parquet(run_dir / "topbook_checkpoints.parquet")
    health = pd.read_parquet(run_dir / "feed_health.parquet")
    assert terminal_at > pd.to_datetime(checkpoints["received_at_utc"], utc=True).max()
    assert terminal_at > pd.to_datetime(health["observed_at_utc"], utc=True).max()
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert manifest["storage_profile"]["terminal_completeness"] == "failed"
    assert validate_run_manifest(run_dir / "manifest.json").ok


@pytest.mark.asyncio
async def test_invalid_kalshi_snapshot_is_diagnostic_not_coverage(tmp_path) -> None:
    fake = FakeWebSocket(
        [
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "market_ticker": "KXTEST",
                        "market_id": "market-id",
                        "yes_dollars_fp": [],
                        "no_dollars_fp": [],
                    },
                }
            )
        ]
    )

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    manifest = await stream_kalshi_order_book_data(
        ["KXTEST"],
        output_root=tmp_path,
        run_name="invalid-snapshot",
        duration_s=10,
        max_messages=1,
        capture_intent="smoke",
        websocket_max_size_bytes=24 * 1024 * 1024,
        websocket_max_queue_frames=96,
        connect_factory=connect_factory,
        auth=FakeReadAuth(),
    )

    completeness = manifest["capture_completeness"]
    assert completeness["instruments_with_snapshots"] == 0
    assert completeness["instruments_with_invalid_snapshots"] == 1
    assert completeness["capture_intent"] == "smoke"
    assert completeness["terminal_reason"] == "max_messages_reached"
    assert completeness["acceptance_eligible"] is False
