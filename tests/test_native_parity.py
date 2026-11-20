"""Differential tests at the public book, raw frame, and Arrow boundaries."""

import asyncio
import contextlib
import json
from dataclasses import asdict
from decimal import Decimal
from types import SimpleNamespace

import pyarrow as pa
import pytest

from pmkt import _native
from pmkt.data.registry import arrow_schema, get_table_spec
from pmkt.streaming import managed_feed
from pmkt.streaming.copy_merge import CopyMerge, session_occurrences
from pmkt.streaming.raw_capture import ChunkWriter
from pmkt.streaming.raw_reader import (
    inspect_session,
    read_session,
    reconstruct_raw_capture,
)
from test_managed_feed import Auth, delta, poly_change, snapshot, until


class Clock:
    ns = 1_000_000_000

    def monotonic_ns(self):
        return self.ns

    def monotonic(self):
        return self.ns / 1_000_000_000


class Replay:
    """Two independent feeds observe the same generation, wire bytes and clock."""

    def __init__(self, venue, ids, monkeypatch, *, sid=7):
        self.clock = Clock()
        monkeypatch.setattr(managed_feed, "time", self.clock)
        self.feeds = []
        for engine in ("python", "native"):
            monkeypatch.setenv("PMKT_ORACLE_ENGINE", engine)
            feed = managed_feed.ManagedBookFeed(
                ids,
                venue=venue,
                auth=Auth(),
                shard_size=len(ids),
            )
            self.feeds.append(feed)
            shard = feed._shards[0]
            shard.generation = 1
            shard.command_id = 1
            shard.client = SimpleNamespace(
                is_connected=True,
                heartbeat_diagnostics=lambda: {
                    "inbound_age_seconds": 0,
                    "heartbeat_error_type": None,
                },
            )
            shard.restart = False
            shard.connected = shard.alive = self.clock.monotonic()
            shard.health.mark_connected(now_monotonic_ns=self.clock.ns)
            if shard.native is not None:
                shard.native.connected(1, self.clock.ns, command_id=1)
        assert self.feeds[0]._shards[0].native is None
        assert self.feeds[1]._shards[0].native is not None
        self.count = 0
        if venue == "kalshi":
            self.send(
                {
                    "type": "subscribed",
                    "id": 1,
                    "msg": {"sid": sid, "channel": "orderbook_delta"},
                }
            )

    def send(self, message):
        self.raw(json.dumps(message).encode())

    def raw(self, frame):
        oracle, native = self.feeds
        decoded = json.loads(frame, parse_float=Decimal)
        for message in decoded if isinstance(decoded, list) else [decoded]:
            self.count += 1
            oracle._ingest(oracle._shards[0], message, self.count)
        native._ingest_raw(native._shards[0], frame)

    def compare(self, instrument):
        oracle, native = self.feeds
        a, b = oracle.health(instrument), native.health(instrument)
        lhs, rhs = asdict(a), asdict(b)
        for key in ("liveness_age_seconds", "book_update_age_seconds"):
            for h in (lhs, rhs):
                if h[key] is not None:
                    h[key] = round(h[key] * 1_000_000_000)
        assert lhs == rhs
        old, new = oracle.book(instrument), native.book(instrument)
        assert old == new
        return new, b


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
async def test_wire_text_reaches_native_unchanged(venue):
    from websockets.asyncio.client import connect
    from websockets.asyncio.server import serve

    wire = json.dumps(snapshot(venue), indent=2).replace(
        '"0.1234567890123456789"', "0.1234567890123456789"
    )

    async def publish(ws):
        command = json.loads(await ws.recv())
        if venue == "kalshi":
            await ws.send(
                json.dumps(
                    {
                        "type": "subscribed",
                        "id": command["id"],
                        "msg": {"channel": "orderbook_delta", "sid": 7},
                    }
                )
            )
        await ws.send(wire)
        await ws.wait_closed()

    async with serve(publish, "127.0.0.1", 0) as server:
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        feed = managed_feed.ManagedBookFeed(
            ["a"],
            venue=venue,
            auth=Auth(),
            connect_factory=lambda *args: connect(url, proxy=None),
        )
        frames = []
        ingest = feed._ingest_raw

        def receive(shard, frame):
            frames.append(frame)
            ingest(shard, frame)

        feed._ingest_raw = receive
        async with feed:
            await until(lambda: feed.book("a") is not None)
            assert frames[-1] == wire.encode()
            assert feed.book("a").bids[0][0] == Decimal("0.1234567890123456789")


def test_polymarket_frames_and_detached_cache(monkeypatch):
    replay = Replay("polymarket", ["a", "b"], monkeypatch)
    replay.send([snapshot("polymarket"), snapshot("polymarket", "b")])
    original, _ = replay.compare("a")
    sibling, _ = replay.compare("b")
    native = replay.feeds[1]
    assert native.book("a") is original
    with_timestamp = dict(snapshot("polymarket", "a"), timestamp=Decimal("123.456"))
    for feed in replay.feeds:
        feed._ingest(feed._shards[0], with_timestamp, 3)
    assert replay.compare("a")[0].source_timestamp == Decimal("123.456")
    replay.send(delta("polymarket"))
    changed, _ = replay.compare("a")
    assert changed.bids[0][1] == Decimal("0.30")
    assert native.book("b") is sibling
    # Multiple updates can share a monotonic coordinate.
    replay.send(poly_change("0.1234567890123456789", "0"))
    assert replay.compare("a")[0].bids == ()
    replay.send(
        {"event_type": "tick_size_change", "asset_id": "a", "new_tick_size": "0.01"}
    )
    assert replay.compare("a")[0].metadata_revision == 1
    replay.send(poly_change("0.8", "1"))
    assert replay.compare("a")[1].reason == "crossed_book"
    replay.clock.ns += 249_000_000
    replay.send(poly_change("0.8", "0", "SELL"))
    assert replay.compare("a")[0].asks == ()
    assert original.bids[0][1] == Decimal("0.10")


def test_identity_resolution_and_invalid_grace_evidence(monkeypatch):
    pair = Replay("polymarket", ["a"], monkeypatch)
    pair.send(snapshot("polymarket"))
    pair.send(poly_change("0.8", "1"))
    pair.send(poly_change("0.8", "bad"))
    assert pair.compare("a")[1].book_update_age_seconds == 0
    pair.send(dict(snapshot("polymarket"), market="foreign"))
    pair.send(dict(snapshot("polymarket"), bids=[{"price": "bad", "size": "2"}]))
    assert pair.compare("a")[1].reason == "market_identity_changed"
    pair.send(snapshot("polymarket"))
    assert pair.compare("a")[0] is not None
    pair.send({"event_type": "market_resolved", "market": "condition"})
    pair.send(snapshot("polymarket"))
    assert pair.compare("a")[1].reason == "market_resolved"


@pytest.mark.asyncio
async def test_same_snapshot_send_barrier(monkeypatch):
    replay = Replay("polymarket", ["a"], monkeypatch)
    replay.send(snapshot("polymarket"))
    replay.send(poly_change("0.8", "1"))
    replay.clock.ns += 250_000_000
    assert replay.compare("a")[1].reason == "book_integrity_failed"
    release = asyncio.Event()
    tasks = []
    try:
        for feed in replay.feeds:
            entered = asyncio.Event()

            async def request(shard, instruments, signal=entered):
                assert instruments == ["a"]
                signal.set()
                await release.wait()

            feed._request_snapshots = request
            tasks.append(asyncio.create_task(feed._recover(feed._shards[0])))
            await asyncio.wait_for(entered.wait(), timeout=2)
        replay.send(snapshot("polymarket"))
        assert replay.compare("a")[0] is None
        release.set()
        for _ in range(100):
            if all(f.book("a") is not None for f in replay.feeds):
                break
            await asyncio.sleep(0.001)
        assert replay.compare("a")[0] is not None
    finally:
        for task in tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


def test_kalshi_price_basis_and_sequence(monkeypatch):
    replay = Replay("kalshi", ["a"], monkeypatch)
    s = snapshot("kalshi")
    s["msg"].update(yes_dollars_fp=[["0.40", "2"]], no_dollars_fp=[["0.40", "3"]])
    replay.send(s)
    locked, health = replay.compare("a")
    assert health.usable and locked.bids[0][0] == locked.asks[0][0] == Decimal("0.4")
    d = delta("kalshi")
    d["msg"].update(side="no", price_dollars="0.40", delta_fp="-3")
    replay.send(d)
    assert replay.compare("a")[0].asks == ()
    s.update(seq=3)
    s["msg"]["no_dollars_fp"] = [["0.39", "3"]]
    replay.send(s)
    assert "crossed_book" in replay.compare("a")[1].quality_flags
    d["seq"] = 5
    for feed in replay.feeds:
        with pytest.raises(ConnectionError, match="subscription_sequence_gap"):
            feed._ingest(feed._shards[0], d, 5)
        # As in session teardown, all books are invalidated on this transport fault.
        feed._invalidate(feed._shards[0], "subscription_sequence_gap")
    assert replay.compare("a")[0] is None


@pytest.mark.parametrize("version", [1, 2])
def test_verified_reconstruction_schema_and_sequence_gap(
    tmp_path, monkeypatch, version
):
    writer = ChunkWriter(tmp_path, "run", "kalshi", "connection")
    s = snapshot("kalshi")
    for msg in (s, delta("kalshi"), delta("kalshi", seq=4), delta("kalshi", seq=5)):
        writer.write("text", json.dumps(msg), 1)
    writer.seal()
    monkeypatch.setenv("PMKT_ORACLE_ENGINE", "python")
    expected = inspect_session(tmp_path)
    monkeypatch.delenv("PMKT_ORACLE_ENGINE")
    assert inspect_session(tmp_path) == expected
    top, depth = reconstruct_raw_capture(tmp_path, version=version, target_rows=1)
    assert top.schema == arrow_schema(get_table_spec(f"topbook.v{version}"))
    assert depth.schema == arrow_schema(get_table_spec(f"depth.v{version}"))
    assert top.num_rows == 8 and depth.num_rows == 8
    assert top["raw_event_ref"].to_pylist() == [
        f"run:{i}" for i in range(1, 5) for _ in range(2)
    ]
    assert top["quote_age_ms"].null_count == 8
    assert top["valid_state"].to_pylist() == [True] * 4 + [False] * 4
    assert all("seq_gap" in flags for flags in top["quality_flags"].to_pylist()[4:])
    if version == 2:
        assert top["book_integrity_valid"].to_pylist() == top["valid_state"].to_pylist()


def test_two_copy_kalshi_clock_reset(tmp_path, monkeypatch):
    from pmkt.streaming import raw_capture
    from datetime import datetime, timezone

    epoch = 1_700_000_000_000
    now_ms = epoch
    monkeypatch.setattr(
        raw_capture,
        "utc",
        lambda: datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat(
            timespec="microseconds"
        ),
    )
    sources, streams = [], []
    for copy in ("a", "b"):
        session = tmp_path / copy
        session.mkdir()
        writer = ChunkWriter(session, copy, "kalshi", "conn")
        events = [
            (1, snapshot("kalshi"), 100, 90),
            (1, delta("kalshi"), 101, 100),
            (2, snapshot("kalshi"), 1, 200),
            (2, snapshot("kalshi", seq=2), 3, 230),
            (2, delta("kalshi", seq=3), 4, 220),
            (2, delta("kalshi", seq=4), 5, 220),
            (2, snapshot("kalshi", seq=5), 5, 240),
            (2, delta("kalshi", seq=6), 7, 220),
            (2, delta("kalshi", seq=7), 7, 220),
        ]
        receive_times = [91, 111, 201, 231, 232, 233, 241, 243, 244]
        for received, (gen, message, clock, ms) in zip(
            receive_times, events[: 9 if copy == "a" else 8]
        ):
            now_ms = epoch + received + int(copy == "b")
            message["sending_ts_ms"] = epoch + ms
            message["msg"]["clock"] = clock
            if message["type"] == "orderbook_delta":
                message["msg"]["ts_ms"] = epoch + ms
            writer.write("text", json.dumps(message), gen)
        writer.seal()
        streams.append(
            list(session_occurrences(copy, "conn", session.name, read_session(session)))
        )
        sources.append((copy, "conn", session))
    oracle = list(CopyMerge("kalshi").merge(streams))
    native = _native.merge_raw_captures("kalshi", sources, include_messages=True)
    messages = [json.loads(m) for m in native["messages"]]
    assert [m["message"]["msg"]["clock"] for m in messages] == [
        100,
        101,
        1,
        3,
        4,
        5,
        5,
        7,
        7,
    ]
    assert [(m["order_ms"], m["content_digest"], m["copies"]) for m in messages] == [
        (o.order_ms, o.content_digest, list(o.copies)) for o in oracle
    ]
    assert pa.table(native["topbook"]).num_rows == 18


def test_merge_continues_one_connection_across_sessions_and_honors_price_basis(
    tmp_path,
):
    sessions = []
    for i, name in enumerate(("first", "second")):
        path = tmp_path / name
        path.mkdir()
        writer = ChunkWriter(path, name, "kalshi", "conn")
        writer.write(
            "subscription",
            json.dumps(
                {
                    "params": {
                        "channels": ["orderbook_delta"],
                        "use_yes_price": False,
                        "market_tickers": ["a"],
                    }
                }
            ),
            1,
            "sent",
        )
        msg = snapshot("kalshi")
        msg["msg"].update(yes_dollars_fp=[["0.3", "4"]], no_dollars_fp=[["0.4", "5"]])
        msg["sending_ts_ms"] = 1000 + i
        writer.write("text", json.dumps(msg), 1)
        writer.seal()
        sessions.append(("copy", "conn", path))
    merged = _native.merge_raw_captures("kalshi", list(reversed(sessions)))
    top = pa.table(merged["topbook"])
    assert top["raw_event_ref"].to_pylist() == ["first:2"] * 2 + ["second:2"] * 2
    assert top["best_ask_dollars"].to_pylist() == [0.6, 0.7] * 2
