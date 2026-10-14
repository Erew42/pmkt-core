import asyncio
import json
from decimal import Decimal

import pytest
from websockets.protocol import State

from pmkt.streaming.managed_feed import ManagedBookFeed


class Auth:
    def headers_for_get(self, path):
        return {}


class Socket:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.sent = []
        self.closed = False
        self.respond = True
        self.probes = 0

    async def send(self, value):
        self.sent.append(value)
        if value == "PING" and self.respond:
            self.queue.put_nowait("PONG")

    async def ping(self):
        self.probes += 1
        if not self.respond:
            raise TimeoutError()
        done = asyncio.get_running_loop().create_future()
        done.set_result(None)
        return done

    async def close(self):
        self.closed = True
        self.queue.put_nowait(None)

    def __aiter__(self):
        return self

    async def __anext__(self):
        value = await self.queue.get()
        if value is None:
            raise StopAsyncIteration
        return value

    def put(self, value):
        self.queue.put_nowait(json.dumps(value))


class Connections:
    def __init__(self):
        self.sockets = []
        self.fail = False

    async def __call__(self, *args):
        if self.fail:
            raise OSError("synthetic failure")
        socket = Socket()
        self.sockets.append(socket)
        return socket


async def until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(0.002)

    await asyncio.wait_for(wait(), 3)


def snapshot(venue, instrument="a", seq=1):
    if venue == "polymarket":
        return {
            "event_type": "book",
            "asset_id": instrument,
            "market": "condition",
            "timestamp": "1700000000000",
            "hash": "h1",
            "bids": [{"price": "0.1234567890123456789", "size": "0.10"}],
            "asks": [{"price": "0.8", "size": "2.34567890123456789"}],
        }
    return {
        "type": "orderbook_snapshot",
        "sid": 7,
        "seq": seq,
        "msg": {
            "market_ticker": instrument,
            "market_id": "market",
            "ts": 1700000000,
            "yes_dollars_fp": [["0.1234567890123456789", "0.10"]],
            "no_dollars_fp": [["0.8", "2.34567890123456789"]],
        },
    }


def delta(venue, instrument="a", seq=2):
    if venue == "polymarket":
        return {
            "event_type": "price_change",
            "market": "condition",
            "price_changes": [
                {
                    "asset_id": instrument,
                    "side": "BUY",
                    "price": "0.1234567890123456789",
                    "size": "0.30",
                    "previous_hash": "h1",
                    "hash": "h2",
                }
            ],
        }
    return {
        "type": "orderbook_delta",
        "sid": 7,
        "seq": seq,
        "msg": {
            "market_ticker": instrument,
            "side": "yes",
            "price_dollars": "0.1234567890123456789",
            "delta_fp": "0.20",
        },
    }


def ack(socket):
    command = next(json.loads(s) for s in socket.sent if s.startswith("{"))
    socket.put(
        {
            "type": "subscribed",
            "id": command["id"],
            "msg": {"channel": "orderbook_delta", "sid": 7},
        }
    )


def feed(venue, connections, instruments=("a",), **kwargs):
    return ManagedBookFeed(
        instruments,
        venue=venue,
        connect_factory=connections,
        auth=Auth(),
        probe_interval=0.02,
        liveness_timeout=0.2,
        snapshot_timeout=0.3,
        retry_backoff=0.01,
        **kwargs,
    )


async def ready(f, connections, venue):
    await until(lambda: connections.sockets and connections.sockets[-1].sent)
    socket = connections.sockets[-1]
    if venue == "kalshi":
        ack(socket)
    socket.put(snapshot(venue))
    await until(lambda: f.book("a") is not None)
    return socket


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
async def test_exact_quiet_books_and_shutdown(venue):
    connections = Connections()
    f = feed(venue, connections)
    assert f.book("a") is None
    async with f:
        socket = await ready(f, connections, venue)
        original = f.book("a")
        socket.put(delta(venue))
        await until(lambda: f.book("a").bids[0][1] == Decimal("0.30"))
        changed = f.book("a")
        assert original.bids == ((Decimal("0.1234567890123456789"), Decimal("0.10")),)
        assert changed.asks == ((Decimal("0.8"), Decimal("2.34567890123456789")),)
        await asyncio.sleep(0.35)
        assert f.book("a") == changed
        h = f.health("a")
        assert h.usable and h.book_update_age_seconds > f.liveness_timeout
        assert h.liveness_age_seconds < f.liveness_timeout
        assert len(connections.sockets) == 1
        with pytest.raises(KeyError):
            f.book("unknown")
    assert socket.closed and f.book("a") is None
    assert f.health("a").connection_state == "closed"
    assert all(s.task.done() for s in f._shards)
    await f.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
async def test_disconnect_failed_recovery_and_generation_snapshot_barrier(venue):
    connections = Connections()
    async with feed(venue, connections) as f:
        old = await ready(f, connections, venue)
        generation = f.book("a").generation
        connections.fail = True
        await old.close()
        assert f.book("a") is None
        await until(lambda: f.health("a").reconnect_count >= 2)
        assert f.book("a") is None
        connections.fail = False
        await until(
            lambda: len(connections.sockets) == 2 and connections.sockets[-1].sent
        )
        new = connections.sockets[-1]
        old.put(snapshot(venue))
        await asyncio.sleep(0.02)
        assert f.book("a") is None
        if venue == "kalshi":
            ack(new)
        new.put(snapshot(venue))
        await until(lambda: f.book("a") is not None)
        assert f.book("a").generation > generation


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
async def test_stalled_transport_and_silent_snapshot_recovery(venue):
    connections = Connections()
    async with feed(venue, connections) as f:
        socket = await ready(f, connections, venue)
        socket.respond = False
        await until(lambda: f.health("a").reconnect_count > 0)
        assert f.book("a") is None
        # New sockets respond to heartbeats, but never deliver replacement books.
        if venue == "kalshi":
            await until(lambda: len(connections.sockets) == 2 and connections.sockets[-1].sent)
            ack(connections.sockets[-1])
        await until(lambda: f.health("a").reason == "snapshot_timeout")
        assert f.book("a") is None
        assert f.health("a").reconnect_count == 1
        assert f.health("a").connection_state == "connected"


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
@pytest.mark.parametrize(
    "failure", ["delta_before_snapshot", "malformed", "integrity", "decode"]
)
async def test_integrity_failures_require_new_snapshot(venue, failure):
    connections = Connections()
    async with feed(venue, connections) as f:
        if failure == "delta_before_snapshot":
            await until(lambda: connections.sockets and connections.sockets[-1].sent)
            socket = connections.sockets[-1]
            if venue == "kalshi":
                ack(socket)
        else:
            socket = await ready(f, connections, venue)
        message = delta(venue)
        if failure == "decode":
            socket.queue.put_nowait("{broken")
        else:
            if failure == "malformed":
                if venue == "polymarket":
                    message["price_changes"][0]["size"] = "NaN"
                else:
                    message["msg"]["delta_fp"] = "NaN"
            if failure == "integrity":
                if venue == "polymarket":
                    message["price_changes"][0]["previous_hash"] = "wrong"
                else:
                    message["seq"] = 5
            socket.put(message)
        shard_fault = failure == "decode" or (venue == "kalshi" and failure == "integrity")
        if shard_fault:
            await until(lambda: f.health("a").reconnect_count > 0)
            assert f.book("a") is None
            assert socket.closed
        else:
            await until(lambda: f.health("a").reason == "book_integrity_failed")
            assert f.book("a") is None
            assert not socket.closed
            assert f.health("a").reconnect_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
async def test_shards_have_independent_connections_and_books(venue):
    connections = Connections()
    async with feed(venue, connections, ("a", "b", "a"), shard_size=1) as f:
        await until(
            lambda: (
                len(connections.sockets) == 2
                and all(s.sent for s in connections.sockets)
            )
        )
        for socket in connections.sockets:
            command = json.loads(socket.sent[0])
            instrument = (
                command["assets_ids"][0]
                if venue == "polymarket"
                else command["params"]["market_tickers"][0]
            )
            if venue == "kalshi":
                ack(socket)
            socket.put(snapshot(venue, instrument))
        await until(lambda: f.book("a") and f.book("b"))
        await connections.sockets[0].close()
        assert f.book("a") is None
        assert f.book("b") is not None


@pytest.mark.asyncio
async def test_kalshi_interleaved_sequence_gap_invalidates_siblings():
    connections = Connections()
    async with feed("kalshi", connections, ("a", "b")) as f:
        socket = await ready(f, connections, "kalshi")
        socket.put(snapshot("kalshi", "b", seq=2))
        socket.put(delta("kalshi", "a", seq=3))
        await until(lambda: f.book("b") and f.book("a").bids[0][1] == Decimal("0.30"))
        socket.put(delta("kalshi", "b", seq=5))
        await until(lambda: f.health("a").reconnect_count > 0)
        assert f.book("a") is f.book("b") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["ack", "boolean_id", "sid", "ticker"])
async def test_kalshi_rejects_wrong_subscription_identity(bad):
    connections = Connections()
    async with feed("kalshi", connections) as f:
        await until(lambda: connections.sockets and connections.sockets[-1].sent)
        socket = connections.sockets[-1]
        if bad in {"ack", "boolean_id"}:
            socket.put(
                {
                    "type": "subscribed",
                    "id": True if bad == "boolean_id" else 99,
                    "msg": {"sid": 7, "channel": "orderbook_delta"},
                }
            )
        else:
            ack(socket)
            message = snapshot("kalshi", "wrong" if bad == "ticker" else "a")
            if bad == "sid":
                message["sid"] = 99
            socket.put(message)
        await until(lambda: f.health("a").reconnect_count > 0)
        assert f.book("a") is None


@pytest.mark.asyncio
async def test_polymarket_top_of_book_does_not_initialize_depth():
    connections = Connections()
    async with feed("polymarket", connections) as f:
        await until(lambda: connections.sockets and connections.sockets[-1].sent)
        socket = connections.sockets[-1]
        socket.put(snapshot("polymarket", "foreign"))
        socket.put(
            {
                "event_type": "best_bid_ask",
                "asset_id": "a",
                "best_bid": "0.4",
                "best_ask": "0.6",
            }
        )
        await asyncio.sleep(0.04)
        assert f.book("a") is None


@pytest.mark.asyncio
async def test_close_cancels_silent_connection_setup():
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def connect(*args):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    f = feed("polymarket", connect)
    await f.start()
    await entered.wait()
    await f.close()
    assert cancelled.is_set()
    assert f.book("a") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
async def test_reader_latches_expired_liveness_before_monitor_wakes(venue):
    connections = Connections()
    async with feed(venue, connections) as f:
        socket = await ready(f, connections, venue)
        shard = f._shards[0]
        if venue == "polymarket":
            shard.client._last_inbound -= 1
        else:
            shard.alive -= 1
        assert f.book("a") is None
        assert f.health("a").reason == "liveness_expired"
        # Late liveness evidence from this connection cannot restore cleared depth.
        if venue == "polymarket":
            shard.client._last_inbound += 1
        else:
            shard.alive = asyncio.get_running_loop().time()
        socket.put(snapshot(venue))
        assert f.book("a") is None
        await until(lambda: socket.closed)


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
async def test_native_json_numbers_keep_precision(venue):
    connections = Connections()
    async with feed(venue, connections) as f:
        await until(lambda: connections.sockets and connections.sockets[-1].sent)
        socket = connections.sockets[-1]
        if venue == "kalshi":
            ack(socket)
        wire = json.dumps(snapshot(venue)).replace(
            '"0.1234567890123456789"', "0.1234567890123456789"
        )
        wire = wire.replace('"2.34567890123456789"', "2.34567890123456789")
        socket.queue.put_nowait(wire)
        await until(lambda: f.book("a"))
        assert f.book("a").bids[0][0] == Decimal("0.1234567890123456789")
        assert f.book("a").asks[0][1] == Decimal("2.34567890123456789")


@pytest.mark.asyncio
async def test_polymarket_malformed_delta_envelope_invalidates_book():
    connections = Connections()
    async with feed("polymarket", connections) as f:
        socket = await ready(f, connections, "polymarket")
        socket.put({"event_type": "price_change", "price_changes": ["broken"]})
        await until(lambda: f.health("a").reconnect_count > 0)
        assert f.book("a") is None


def poly_change(price, size, side="BUY", instrument="a"):
    return {
        "event_type": "price_change", "timestamp": "1700000001000",
        "price_changes": [{"asset_id": instrument, "price": price, "size": size,
                           "side": side, "best_bid": "0.8", "best_ask": "0.9"}],
    }


def recovery_commands(socket, venue):
    commands = [json.loads(s) for s in socket.sent if s.startswith("{")]
    return [c for c in commands if c.get("operation") == "subscribe" or
            c.get("params", {}).get("action") == "get_snapshot"]


@pytest.mark.asyncio
@pytest.mark.parametrize("price", ["0.8", "0.85"])
async def test_polymarket_transient_cross_withholds_only_affected_instrument(price):
    connections = Connections()
    async with feed("polymarket", connections, ("a", "b")) as f:
        socket = await ready(f, connections, "polymarket")
        socket.put(snapshot("polymarket", "b"))
        await until(lambda: f.book("b"))
        sibling = f.book("b")
        socket.put(poly_change(price, "1"))
        await until(lambda: f.book("a") is None)
        assert f.health("a").reason == "crossed_book"
        assert f.book("b") == sibling
        socket.put({"event_type": "best_bid_ask", "asset_id": "a",
                    "best_bid": "0.8", "best_ask": "0.9"})
        await asyncio.sleep(0.03)
        assert f.book("a") is None
        # A different timestamp/hash is normal; hints alone never repair depth.
        socket.put(poly_change("0.8", "0", "SELL"))
        await until(lambda: f.book("a"))
        assert f.book("a").bids[0] == (Decimal(price), Decimal(1))
        assert f.book("b") == sibling
        assert f.health("a").generation == sibling.generation
        assert not recovery_commands(socket, "polymarket")
        assert len(connections.sockets) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
async def test_missing_snapshot_is_local_and_late_snapshot_recovers(venue):
    connections = Connections()
    async with feed(venue, connections, ("a", "b")) as f:
        socket = await ready(f, connections, venue)
        healthy = f.book("a")
        assert f.health("b").reason == "no_initial_snapshot"
        await until(lambda: f.health("b").reason == "snapshot_timeout")
        await until(lambda: recovery_commands(socket, venue))
        await asyncio.sleep(0.35)  # Also exceed the targeted recovery deadline.
        assert f.book("a") == healthy
        assert f.book("b") is None
        assert f.health("b").reason == "snapshot_timeout"
        assert len(recovery_commands(socket, venue)) == 1
        assert len(connections.sockets) == 1
        socket.put(snapshot(venue, "b", seq=2))
        await until(lambda: f.book("b"))
        assert f.book("a") == healthy
        assert f.health("b").reason is None


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
async def test_local_malformed_depth_requests_only_affected_snapshot(venue):
    connections = Connections()
    async with feed(venue, connections, ("a", "b")) as f:
        socket = await ready(f, connections, venue)
        socket.put(snapshot(venue, "b", seq=2))
        await until(lambda: f.book("b"))
        sibling = f.book("b")
        bad = delta(venue, seq=3)
        if venue == "polymarket":
            bad["price_changes"][0]["size"] = "NaN"
        else:
            bad["msg"]["delta_fp"] = "NaN"
        socket.put(bad)
        await until(lambda: recovery_commands(socket, venue))
        command = recovery_commands(socket, venue)[0]
        assert (command["assets_ids"] if venue == "polymarket" else
                command["params"]["market_tickers"]) == ["a"]
        assert f.book("a") is None
        assert f.book("b") == sibling
        socket.put(delta(venue, seq=4))
        await asyncio.sleep(0.03)
        assert f.book("a") is None  # Deltas cannot bypass the snapshot barrier.
        socket.put(snapshot(venue, seq=5))
        await until(lambda: f.book("a"))
        assert f.book("b") == sibling
        assert len(connections.sockets) == 1


@pytest.mark.asyncio
async def test_cross_grace_expires_without_traffic_and_late_delta_cannot_repair():
    connections = Connections()
    async with feed("polymarket", connections, ("a", "b")) as f:
        socket = await ready(f, connections, "polymarket")
        socket.put(snapshot("polymarket", "b"))
        await until(lambda: f.book("b"))
        sibling = f.book("b")
        socket.put(poly_change("0.8", "1"))
        await until(lambda: f.health("a").reason == "crossed_book")
        await until(lambda: recovery_commands(socket, "polymarket"))
        socket.put(poly_change("0.8", "0", "SELL"))
        await until(lambda: f.health("a").reason == "recovery_timeout")
        assert f.book("a") is None
        assert f.book("b") == sibling
        assert len(recovery_commands(socket, "polymarket")) == 1
        socket.put(snapshot("polymarket"))
        await until(lambda: f.book("a"))
        assert f.book("b") == sibling


@pytest.mark.asyncio
async def test_cross_grace_deadline_is_not_renewed_and_late_correction_needs_snapshot():
    connections = Connections()
    async with feed("polymarket", connections) as f:
        socket = await ready(f, connections, "polymarket")
        socket.put(poly_change("0.8", "1"))
        await until(lambda: f.health("a").reason == "crossed_book")
        shard = f._shards[0]
        deadline = shard.recovery["a"].deadline
        socket.put(poly_change("0.8", "2"))
        await until(lambda: shard.books.polymarket["a"].bids.get(Decimal("0.8")) == 2)
        assert shard.recovery["a"].deadline == deadline
        # Model an event-loop pause; ingress must latch expiry before application.
        shard.recovery["a"].deadline -= 1
        f._ingest(shard, poly_change("0.8", "0", "SELL"), 10)
        assert f.book("a") is None
        assert shard.recovery["a"].phase == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
async def test_silent_recovery_retries_after_cooldown_without_reconnecting(venue):
    connections = Connections()
    async with feed(venue, connections, ("a", "b")) as f:
        socket = await ready(f, connections, venue)
        healthy = f.book("a")
        await until(lambda: recovery_commands(socket, venue))
        shard = f._shards[0]
        await until(lambda: shard.recovery["b"].phase == "cooldown")
        assert f.book("b") is None
        assert shard.recovery["b"].deadline > asyncio.get_running_loop().time() + 25
        shard.recovery["b"].deadline -= 31
        await until(lambda: len(recovery_commands(socket, venue)) == 2)
        socket.put(snapshot(venue, "b", seq=2))
        await until(lambda: f.book("b"))
        assert f.book("a") == healthy
        assert len(connections.sockets) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
async def test_pending_recovery_does_not_block_siblings_and_close_cancels_it(venue):
    connections = Connections()
    entered, cancelled = asyncio.Event(), asyncio.Event()
    f = feed(venue, connections, ("a", "b"))

    async def blocked(shard, instruments):
        assert instruments == ["b"]
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    f._request_snapshots = blocked
    async with f:
        socket = await ready(f, connections, venue)
        await entered.wait()
        socket.put(delta(venue, seq=2))
        await until(lambda: f.book("a").bids[0][1] == Decimal("0.30"))
        socket.put(snapshot(venue, "b", seq=3))
        await asyncio.sleep(0.03)
        assert f.book("b") is None  # Cannot publish during a subscription operation.
    assert cancelled.is_set()
    assert socket.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
async def test_recovery_send_failure_still_invalidates_shard(venue):
    connections = Connections()
    async with feed(venue, connections, ("a", "b")) as f:
        socket = await ready(f, connections, venue)

        async def fail(shard, instruments):
            raise OSError("synthetic transport failure")

        f._request_snapshots = fail
        await until(lambda: f.health("a").reconnect_count > 0)
        assert f.book("a") is None
        assert socket.closed


@pytest.mark.asyncio
async def test_kalshi_sequence_gap_during_local_recovery_invalidates_siblings():
    connections = Connections()
    async with feed("kalshi", connections, ("a", "b")) as f:
        socket = await ready(f, connections, "kalshi")
        socket.put(snapshot("kalshi", "b", seq=2))
        await until(lambda: f.book("b"))
        message = delta("kalshi", seq=3)
        message["msg"]["delta_fp"] = "NaN"
        socket.put(message)
        await until(lambda: recovery_commands(socket, "kalshi"))
        assert f.book("b") is not None
        socket.put(delta("kalshi", "b", seq=5))
        await until(lambda: f.health("b").reconnect_count > 0)
        assert f.book("a") is f.book("b") is None


@pytest.mark.asyncio
async def test_kalshi_missing_subscription_ack_is_a_shard_fault():
    connections = Connections()
    async with feed("kalshi", connections, ("a", "b")) as f:
        await until(lambda: connections.sockets and connections.sockets[0].sent)
        await until(lambda: f.health("a").reconnect_count > 0)
        assert f.book("a") is f.book("b") is None
        assert f.health("a").last_error == "subscription_ack_timeout"
        assert connections.sockets[0].closed


@pytest.mark.asyncio
async def test_resolution_during_recovery_send_is_not_erased():
    connections = Connections()
    entered, release = asyncio.Event(), asyncio.Event()
    async with feed("polymarket", connections) as f:
        socket = await ready(f, connections, "polymarket")

        async def blocked(shard, instruments):
            entered.set()
            await release.wait()

        f._request_snapshots = blocked
        message = delta("polymarket")
        message["price_changes"][0]["size"] = "NaN"
        socket.put(message)
        await entered.wait()
        socket.put({"event_type": "market_resolved", "asset_id": "a"})
        await until(lambda: f.health("a").reason == "market_resolved")
        release.set()
        await asyncio.sleep(0.4)
        assert f.book("a") is None
        assert f.health("a").reason == "market_resolved"


@pytest.mark.asyncio
async def test_polymarket_batched_books_and_deltas_with_unrelated_announcements():
    # Public market traffic includes multi-token snapshots, absolute-size deltas
    # without previous hashes, and announcements outside the subscription.
    connections = Connections()
    async with feed("polymarket", connections, ("a", "b")) as f:
        await until(lambda: connections.sockets and connections.sockets[-1].sent)
        socket = connections.sockets[-1]
        socket.put([snapshot("polymarket", "a"), snapshot("polymarket", "b")])
        await until(lambda: f.book("a") and f.book("b"))
        original = {i: f.book(i) for i in f.instruments}
        socket.put({
            "event_type": "price_change", "market": "condition",
            "timestamp": "1700000001000",
            "price_changes": [
                {"asset_id": "a", "side": "BUY", "price": "0.1234567890123456789",
                 "size": "0", "hash": "a2"},
                {"asset_id": "a", "side": "BUY", "price": "0.12",
                 "size": "1.23456789", "hash": "a2"},
                {"asset_id": "b", "side": "SELL", "price": "0.8",
                 "size": "9.87654321", "hash": "b2"},
            ],
        })
        await until(lambda: f.book("b").source_timestamp == "1700000001000")
        changed = {i: f.book(i) for i in f.instruments}
        assert changed["a"].bids == ((Decimal("0.12"), Decimal("1.23456789")),)
        assert changed["a"].asks == original["a"].asks
        assert changed["b"].asks == ((Decimal("0.8"), Decimal("9.87654321")),)
        assert changed["b"].bids == original["b"].bids
        assert all(changed[i].generation == original[i].generation for i in f.instruments)
        socket.put([
            {"event_type": "new_market", "market": "unrelated", "assets_ids": ["foreign"]},
            {"event_type": "best_bid_ask", "asset_id": "a",
             "best_bid": "0.13", "best_ask": "0.7", "timestamp": "1700000002000"},
            {"event_type": "last_trade_price", "asset_id": "b", "price": "0.6"},
        ])
        # A subsequent delta acts as a processing barrier for the whole frame.
        socket.put({
            "event_type": "price_change", "market": "unrelated",
            "price_changes": [{"asset_id": "foreign", "side": "BUY", "price": "0.2", "size": "1"}],
        })
        await until(lambda: f._shards[0].client.last_frame_sequence == 4)
        assert {i: f.book(i) for i in f.instruments} == changed
        assert set(f._shards[0].books.polymarket) == {"a", "b"}
        assert f.health("a").reconnect_count == 0


@pytest.mark.asyncio
async def test_kalshi_pending_probe_does_not_block_deltas():
    connections = Connections()
    async with feed("kalshi", connections) as f:
        socket = await ready(f, connections, "kalshi")
        probing = asyncio.Event()
        pong = asyncio.get_running_loop().create_future()

        async def ping():
            probing.set()
            return pong

        socket.ping = ping
        await probing.wait()
        socket.put(delta("kalshi"))
        # The pong stays pending: book processing must be independent of probes.
        for _ in range(10):
            await asyncio.sleep(0)
        assert f.book("a").bids[0][1] == Decimal("0.30")
        pong.set_result(None)


@pytest.mark.asyncio
async def test_kalshi_late_probe_cannot_refresh_expired_liveness():
    connections = Connections()
    async with feed("kalshi", connections) as f:
        await ready(f, connections, "kalshi")
        shard = f._shards[0]
        probed = asyncio.Event()

        async def late_probe(**kwargs):
            # Model a clock advance while awaiting the probe, without a reader.
            shard.alive -= f.liveness_timeout + 1
            probed.set()
            return True

        shard.client.probe_liveness = late_probe
        await probed.wait()
        assert f.book("a") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
async def test_replacement_snapshot_does_not_inherit_old_market_identity(venue):
    connections = Connections()
    async with feed(venue, connections) as f:
        old = await ready(f, connections, venue)
        await old.close()
        await until(lambda: len(connections.sockets) == 2 and connections.sockets[-1].sent)
        socket = connections.sockets[-1]
        message = snapshot(venue)
        if venue == "kalshi":
            ack(socket)
            del message["msg"]["market_id"]
        else:
            del message["market"]
        socket.put(message)
        await until(lambda: f.book("a") is not None)
        assert not f.book("a").market_id


@pytest.mark.asyncio
async def test_polymarket_metadata_event_preserves_depth_timestamp():
    connections = Connections()
    async with feed("polymarket", connections) as f:
        socket = await ready(f, connections, "polymarket")
        original = f.book("a")
        socket.put({
            "event_type": "tick_size_change", "asset_id": "a",
            "new_tick_size": "0.01", "timestamp": "1700000001000",
        })
        await until(lambda: f._shards[0].books.polymarket["a"].tick_size == 0.01)
        assert f.book("a").source_timestamp == original.source_timestamp
        assert f.book("a").received_monotonic == original.received_monotonic


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
async def test_modern_socket_closing_state_invalidates_immediately(venue):
    connections = Connections()
    async with feed(venue, connections) as f:
        socket = await ready(f, connections, venue)
        # Modern websockets exposes state instead of the legacy closed property.
        del socket.closed
        socket.state = State.CLOSING
        assert f.book("a") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
async def test_concurrent_close_waits_for_socket_cleanup(venue):
    connections = Connections()
    f = feed(venue, connections)
    await f.start()
    socket = await ready(f, connections, venue)
    closing, release = asyncio.Event(), asyncio.Event()
    original_close = socket.close

    async def slow_close():
        closing.set()
        await release.wait()
        await original_close()

    socket.close = slow_close
    first = asyncio.create_task(f.close())
    await closing.wait()
    second = asyncio.create_task(f.close())
    try:
        for _ in range(10):
            await asyncio.sleep(0)
        assert not second.done()
    finally:
        release.set()
        await asyncio.gather(first, second)
    assert socket.closed
