"""Native regressions at the public book, raw frame, and Arrow boundaries."""

import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace

import pyarrow as pa
import pytest

from pmkt import _native
from pmkt.data.registry import arrow_schema, get_table_spec
from pmkt.data.validation import validate_frame
from pmkt.streaming import managed_feed
from pmkt.streaming.copy_merge import CopyMerge, Occurrence, session_occurrences
from pmkt.streaming.raw_capture import ChunkWriter
from pmkt.streaming.raw_reader import (
    inspect_session,
    read_session,
    reconstruct_raw_capture,
)
from test_managed_feed import Auth, delta, ingest, poly_change, snapshot, until


class Clock:
    ns = 1_000_000_000

    def monotonic_ns(self):
        return self.ns

    def monotonic(self):
        return self.ns / 1_000_000_000


class Replay:
    """Drive a managed feed on a deterministic clock without a socket."""

    def __init__(self, venue, ids, monkeypatch, *, sid=7):
        self.clock = Clock()
        monkeypatch.setattr(managed_feed, "time", self.clock)
        self.feed = managed_feed.ManagedBookFeed(ids, venue=venue, auth=Auth())
        self.shard = shard = self.feed._shards[0]
        shard.generation = 1
        shard.client = SimpleNamespace(
            is_connected=True,
            _last_inbound=None,
            _heartbeat_error=None,
        )
        shard.restart = False
        shard.health.mark_connected(now_monotonic_ns=self.clock.ns)
        shard.native.connected(1, self.clock.ns, command_id=1)
        if venue == "kalshi":
            self.send({
                "type": "subscribed", "id": 1,
                "msg": {"sid": sid, "channel": "orderbook_delta"},
            })

    def send(self, message):
        ingest(self.feed, self.shard, message)


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
    native = replay.feed
    original = native.book("a")
    sibling = native.book("b")
    assert native.book("a") is original
    with_timestamp = dict(snapshot("polymarket", "a"), timestamp=Decimal("123.456"))
    replay.send(with_timestamp)
    assert replay.feed.book("a").source_timestamp == Decimal("123.456")
    replay.send(delta("polymarket"))
    changed = native.book("a")
    assert changed.bids[0][1] == Decimal("0.30")
    assert native.book("b") is sibling
    # Multiple updates can share a monotonic coordinate.
    replay.send(poly_change("0.1234567890123456789", "0"))
    assert replay.feed.book("a").bids == ()
    replay.send(
        {"event_type": "tick_size_change", "asset_id": "a", "new_tick_size": "0.01"}
    )
    assert replay.feed.book("a").metadata_revision == 1
    replay.send(poly_change("0.8", "1"))
    assert replay.feed.health("a").reason == "crossed_book"
    replay.clock.ns += 249_000_000
    replay.send(poly_change("0.8", "0", "SELL"))
    assert replay.feed.book("a").asks == ()
    assert original.bids[0][1] == Decimal("0.10")


def test_identity_resolution_and_invalid_grace_evidence(monkeypatch):
    pair = Replay("polymarket", ["a"], monkeypatch)
    pair.send(snapshot("polymarket"))
    pair.send(poly_change("0.8", "1"))
    pair.send(poly_change("0.8", "bad"))
    assert pair.feed.health("a").book_update_age_seconds == 0
    pair.send(dict(snapshot("polymarket"), market="foreign"))
    pair.send(dict(snapshot("polymarket"), bids=[{"price": "bad", "size": "2"}]))
    assert pair.feed.health("a").reason == "market_identity_changed"
    pair.send(snapshot("polymarket"))
    assert pair.feed.book("a") is not None
    pair.send({"event_type": "market_resolved", "market": "condition"})
    pair.send(snapshot("polymarket"))
    assert pair.feed.health("a").reason == "market_resolved"


@pytest.mark.asyncio
async def test_same_snapshot_send_barrier(monkeypatch):
    replay = Replay("polymarket", ["a"], monkeypatch)
    replay.send(snapshot("polymarket"))
    replay.send(poly_change("0.8", "1"))
    replay.clock.ns += 250_000_000
    assert replay.feed.health("a").reason == "book_integrity_failed"
    release, entered = asyncio.Event(), asyncio.Event()

    async def request(shard, instruments):
        assert instruments == ["a"]
        entered.set()
        await release.wait()

    replay.feed._request_snapshots = request
    task = asyncio.create_task(replay.feed._recover(replay.shard))
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        replay.send(snapshot("polymarket"))
        assert replay.feed.book("a") is None
        release.set()
        await until(lambda: replay.feed.book("a") is not None)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def test_kalshi_price_basis_and_sequence(monkeypatch):
    replay = Replay("kalshi", ["a"], monkeypatch)
    s = snapshot("kalshi")
    s["msg"].update(yes_dollars_fp=[["0.40", "2"]], no_dollars_fp=[["0.40", "3"]])
    replay.send(s)
    locked, health = replay.feed.book("a"), replay.feed.health("a")
    assert health.usable and locked.bids[0][0] == locked.asks[0][0] == Decimal("0.4")
    d = delta("kalshi")
    d["msg"].update(side="no", price_dollars="0.40", delta_fp="-3")
    replay.send(d)
    assert replay.feed.book("a").asks == ()
    s.update(seq=3)
    s["msg"]["no_dollars_fp"] = [["0.39", "3"]]
    replay.send(s)
    assert "crossed_book" in replay.feed.health("a").quality_flags
    d["seq"] = 5
    with pytest.raises(ConnectionError, match="subscription_sequence_gap"):
        replay.send(d)
    assert replay.feed.health("a").reason == "subscription_sequence_gap"
    assert replay.feed.book("a") is None


@pytest.mark.parametrize("version", [1, 2])
def test_verified_reconstruction_schema_and_sequence_gap(
    tmp_path, monkeypatch, version
):
    from pmkt.streaming import raw_capture

    stamps = iter(f"2025-01-01T00:00:0{i}+00:00" for i in range(4))
    monkeypatch.setattr(
        raw_capture, "utc", lambda: next(stamps, "2025-01-01T00:00:04+00:00")
    )
    writer = ChunkWriter(tmp_path, "run", "kalshi", "connection")
    s = snapshot("kalshi")
    for msg in (s, delta("kalshi"), delta("kalshi", seq=4), delta("kalshi", seq=5)):
        writer.write("text", json.dumps(msg), 1)
    writer.seal()
    inspection = inspect_session(tmp_path)
    assert inspection["frame_count"] == 4
    assert inspection["connection_generations"] == [1]
    assert inspection["frame_type_counts"] == {"text": 4}
    assert inspection["direction_counts"] == {"received": 4}
    assert inspection["chunk_gaps"] == []
    top, depth = reconstruct_raw_capture(tmp_path, version=version, target_rows=1)
    assert top.schema == arrow_schema(get_table_spec(f"topbook.v{version}"))
    assert depth.schema == arrow_schema(get_table_spec(f"depth.v{version}"))
    assert top.num_rows == 8 and depth.num_rows == 8
    assert top["raw_event_ref"].to_pylist() == [
        f"run:{i}" for i in range(1, 5) for _ in range(2)
    ]
    rows = top.to_pylist()
    for index, frame in enumerate(read_session(tmp_path)):
        for quote, outcome in zip(rows[index * 2 : index * 2 + 2], ("YES", "NO")):
            assert quote["outcome"] == outcome
            assert quote["received_at_utc"] == frame.received_utc
            assert quote["received_at_monotonic_ns"] == frame.received_monotonic_ns
            assert quote["exchange_ts_utc"] == (
                "2023-11-14T22:13:20+00:00" if index == 0 else None
            )
    assert [
        (r["side"], r["level_index"], r["price_dollars"], r["size_contracts"])
        for r in depth.to_pylist()
    ] == [
        (side, 0, price, size)
        for bid_size in (0.1, 0.3, 0.5, 0.7)
        for side, price, size in (
            ("yes", 0.12345678901234568, bid_size),
            ("no", 0.8, 2.345678901234568),
        )
    ]
    assert depth["quality_flags"].to_pylist() == top["quality_flags"].to_pylist()
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
    in_memory = list(CopyMerge("kalshi").merge(streams))
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
        (o.order_ms, o.content_digest, list(o.copies)) for o in in_memory
    ]
    assert pa.table(native["topbook"]).num_rows == 18


def test_copy_merge_pulls_only_enough_input_to_emit_the_next_group():
    received = []

    def source():
        for index, stamp in enumerate((1000, 1003, 1006), 1):
            received.append(stamp)
            message = dict(snapshot("polymarket"), timestamp=stamp)
            yield Occurrence("copy", "conn", "run", 1, index, None, stamp * 1000, message)
        raise LookupError("source failed")

    merger = CopyMerge("polymarket", lateness_ms=1)
    merged = merger.merge([source()])
    assert next(merged).venue_ts_ms == 1000
    assert received == [1000, 1003, 1006]
    with pytest.raises(LookupError, match="source failed"):
        next(merged)


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


def write_poly_session(path, monkeypatch, frames):
    from pmkt.streaming import raw_capture

    path.mkdir()
    writer = ChunkWriter(path, path.name, "polymarket", "conn")
    for micros, kind, direction, message in frames:
        utc = f"2023-11-14T22:13:20.{micros:06}+00:00"
        monkeypatch.setattr(raw_capture, "utc", lambda: utc)
        writer.write(kind, json.dumps(message), 1, direction)
    writer.seal()
    return path.name, "conn", path


@pytest.mark.parametrize("backup", ["absent", "interrupted", "continuous"])
@pytest.mark.parametrize("marker,direction", [
    ("disk_pause_gap", "local"),
    ("transport_error", "local"),
    ("ws_close", "received_control_base64"),
])
def test_merged_coverage_resets_only_across_uncovered_holes(
    tmp_path, monkeypatch, marker, direction, backup
):
    book = snapshot("polymarket")
    change = dict(delta("polymarket"), timestamp="1700000000001")
    sources = [write_poly_session(tmp_path / "a", monkeypatch, [
        (100, "text", "received", book),
        (200, marker, direction, ""),
        (300, "text", "received", change),
    ])]
    if backup != "absent":
        # Control frames alone can establish continuous redundant coverage.
        backup_frames = [(90, "ws_pong", "received_control_base64", "")]
        if backup == "interrupted":
            backup_frames.append((190, "transport_error", "local", ""))
        backup_frames.append((310, "ws_pong", "received_control_base64", ""))
        sources.append(write_poly_session(tmp_path / "b", monkeypatch, backup_frames))
    merged = _native.merge_raw_captures("polymarket", sources, target_rows=1)
    top = pa.table(merged["topbook"]).to_pylist()
    depth = pa.table(merged["depth"]).to_pylist()
    covered = backup == "continuous"
    assert [row["valid_state"] for row in top] == [True, covered]
    assert sum(row["local_sequence"] == 3 for row in depth) == (2 if covered else 0)
    if not covered:
        assert {"no_initial_snapshot", "delta_before_snapshot"} <= set(top[1]["quality_flags"])
        expected = reconstruct_raw_capture(sources[0][2])
        assert (top, depth) == tuple(table.to_pylist() for table in expected)


def test_merged_emission_keys_survive_batch_and_clock_step(tmp_path, monkeypatch):
    book = dict(snapshot("polymarket", "12345"), market="0xabc", hash="abcd")
    change = dict(delta("polymarket", "12345"), market="0xabc", timestamp="1700000000001")
    change["price_changes"][0].update(hash="def0", previous_hash="abcd")
    source = write_poly_session(tmp_path / "a", monkeypatch, [
        (500, "text", "received", [book, change]),
        (200, "text", "received", dict(book, timestamp="1700000000002")),
    ])
    merged = _native.merge_raw_captures("polymarket", [source], target_rows=1)
    expected = reconstruct_raw_capture(source[2])
    for name, expected_table in zip(("topbook", "depth"), expected):
        table = pa.table(merged[name])
        assert table.equals(expected_table)
        report = validate_frame(table.to_pandas(), f"{name}.v2", strict=True)
        assert report.ok, report.errors
        times = [f"2023-11-14T22:13:20.{us:06}+00:00" for us in (500, 501, 502)]
        if name == "topbook":
            assert table["raw_event_ref"].to_pylist() == ["a:1:0", "a:1:1", "a:2"]
            assert table["received_at_utc"].to_pylist() == times
        else:
            assert table["received_at_utc"].to_pylist() == [utc for utc in times for _ in range(2)]


def test_merged_resolution_before_initial_book(tmp_path, monkeypatch):
    source = write_poly_session(tmp_path / "a", monkeypatch, [
        (100, "subscription", "sent", {"assets_ids": ["a"]}),
        (200, "text", "received", {
            "event_type": "market_resolved", "assets_ids": ["a", "b"], "market": "condition",
            "timestamp": "1699999999999",
        }),
        (300, "text", "received", snapshot("polymarket")),
    ])
    merged = _native.merge_raw_captures("polymarket", [source], target_rows=1)
    top, depth = (pa.table(merged[name]) for name in ("topbook", "depth"))
    expected = reconstruct_raw_capture(source[2])
    assert top.equals(expected[0]) and depth.equals(expected[1])
    assert top["raw_event_ref"].to_pylist() == ["a:2", "a:3"]
    assert top["valid_state"].to_pylist() == [False, False]
    assert all("market_resolved" in flags for flags in top["quality_flags"].to_pylist())


def test_broadcast_lifecycle_in_a_chunk_without_subscription(tmp_path, monkeypatch):
    source = write_poly_session(tmp_path / "a", monkeypatch, [
        (100, "text", "received", {
            "event_type": "new_market", "assets_ids": ["other"], "timestamp": "1699999999998",
        }),
        (200, "text", "received", {
            "event_type": "market_resolved", "assets_ids": ["a", "b"], "timestamp": "1699999999999",
        }),
        (300, "text", "received", snapshot("polymarket")),
    ])
    merged = _native.merge_raw_captures("polymarket", [source])
    for top in (pa.table(merged["topbook"]), reconstruct_raw_capture(source[2])[0]):
        assert top["raw_event_ref"].to_pylist() == ["a:3"]
        assert top["instrument_id"].to_pylist() == ["a"]
        assert top["valid_state"].to_pylist() == [False]
        assert top["quality_flags"].to_pylist() == [["market_resolved"]]
