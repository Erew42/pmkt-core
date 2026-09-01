from __future__ import annotations

import asyncio
import json
from collections import deque
from copy import deepcopy
from typing import Any

import pytest

from pmkt.exchanges.polymarket.ws import (
    AsyncMarketWebSocketClient,
    MarketBookState,
    MarketStreamSnapshot,
    apply_market_message,
    collect_market_snapshots,
    decode_market_messages,
    market_operation_payload,
    market_subscription_payload,
)


class FakeWebSocket:
    def __init__(self, messages: list[Any] | None = None) -> None:
        self.messages = deque(messages or [])
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


class SlowFailingPingWebSocket(FakeWebSocket):
    async def send(self, payload: str) -> None:
        if payload == "PING":
            raise OSError("ping failed")
        await super().send(payload)

    async def __anext__(self):
        await asyncio.sleep(0.05)
        raise StopAsyncIteration


class UnexpectedFailingPingWebSocket(SlowFailingPingWebSocket):
    async def send(self, payload: str) -> None:
        if payload == "PING":
            raise ValueError("unexpected ping failure")
        await super().send(payload)


def decoded_payload(raw: str) -> dict[str, Any]:
    payload = json.loads(raw)
    assert isinstance(payload, dict)
    return payload


async def no_sleep(_: float) -> None:
    return None


def test_market_payloads_use_asset_ids_and_custom_features() -> None:
    assert market_subscription_payload(["a1", "a1", "a2"]) == {
        "assets_ids": ["a1", "a2"],
        "type": "market",
        "custom_feature_enabled": True,
    }
    assert market_operation_payload("subscribe", "a3", custom_feature_enabled=True) == {
        "assets_ids": ["a3"],
        "operation": "subscribe",
        "custom_feature_enabled": True,
    }
    assert market_operation_payload("unsubscribe", ["a3"]) == {
        "assets_ids": ["a3"],
        "operation": "unsubscribe",
    }


def test_market_payload_rejects_malformed_polymarket_asset_ids() -> None:
    with pytest.raises(ValueError, match="too long"):
        market_subscription_payload(["1" * 152])
    with pytest.raises(ValueError, match="serialized list"):
        market_subscription_payload(['["token-yes", "token-no"]'])


def test_polymarket_reconnect_clears_stale_book_levels() -> None:
    state = MarketBookState("asset-a")
    state.apply_book(
        {
            "asset_id": "asset-a",
            "market": "market-a",
            "bids": [{"price": "0.40", "size": "10"}],
            "asks": [{"price": "0.60", "size": "12"}],
        }
    )

    state.mark_reconnect()

    assert state.bids == {}
    assert state.asks == {}
    assert state.best_bid is None
    assert state.best_ask is None
    assert state.valid_state is False

    snapshot = state.apply_last_trade({"asset_id": "asset-a", "price": "0.50"})
    assert snapshot.best_bid is None
    assert snapshot.best_ask is None
    assert snapshot.valid_state is False


def test_polymarket_price_change_hash_mismatch_invalid_until_book() -> None:
    state = MarketBookState("asset-a")
    state.apply_book(
        {
            "asset_id": "asset-a",
            "market": "market-a",
            "bids": [{"price": "0.40", "size": "10"}],
            "asks": [{"price": "0.60", "size": "12"}],
            "hash": "book-hash",
        }
    )

    mismatch = state.apply_price_change(
        {
            "asset_id": "asset-a",
            "side": "BUY",
            "price": "0.41",
            "size": "4",
            "previous_hash": "unexpected-hash",
            "hash": "delta-hash",
        },
        {"event_type": "price_change", "market": "market-a"},
    )

    assert mismatch.valid_state is False
    assert "hash_mismatch" in mismatch.quality_flags

    recovered = state.apply_book(
        {
            "asset_id": "asset-a",
            "market": "market-a",
            "bids": [{"price": "0.42", "size": "10"}],
            "asks": [{"price": "0.62", "size": "12"}],
            "hash": "fresh-book",
        }
    )

    assert recovered.valid_state is True
    assert "hash_mismatch" not in recovered.quality_flags


@pytest.mark.asyncio
async def test_client_sends_initial_dynamic_and_ping_payloads() -> None:
    fake = FakeWebSocket()

    async def connect_factory(url: str) -> FakeWebSocket:
        assert url == "wss://example/ws/market"
        return fake

    async with AsyncMarketWebSocketClient(
        ["asset-a"],
        ws_url="wss://example/ws/market",
        heartbeat_interval=None,
        connect_factory=connect_factory,
    ) as client:
        await client.subscribe(["asset-b"])
        await client.unsubscribe(["asset-a"])
        await client.ping()

    sent = [decoded_payload(raw) if raw != "PING" else raw for raw in fake.sent]
    assert sent == [
        {
            "assets_ids": ["asset-a"],
            "type": "market",
            "custom_feature_enabled": True,
        },
        {
            "assets_ids": ["asset-b"],
            "operation": "subscribe",
            "custom_feature_enabled": True,
        },
        {
            "assets_ids": ["asset-a"],
            "operation": "unsubscribe",
        },
        "PING",
    ]
    assert fake.closed is True


@pytest.mark.asyncio
async def test_default_connector_disables_protocol_keepalive(monkeypatch) -> None:
    import pmkt.exchanges.polymarket.ws as ws_module

    fake = FakeWebSocket()
    seen: dict[str, Any] = {}

    def connect(url: str, **kwargs: Any) -> FakeWebSocket:
        seen.update(url=url, **kwargs)
        return fake

    monkeypatch.setattr(ws_module.websockets, "connect", connect)
    async with ws_module.AsyncMarketWebSocketClient(
        ["asset-a"],
        ws_url="wss://example/ws/market",
        heartbeat_interval=None,
    ):
        pass

    assert seen == {
        "url": "wss://example/ws/market",
        "ping_interval": None,
        **ws_module.WS_TRANSPORT_LIMITS,
    }


@pytest.mark.asyncio
async def test_heartbeat_sends_text_ping() -> None:
    fake = FakeWebSocket()
    ticks = 0

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    async def fast_sleep(_: float) -> None:
        nonlocal ticks
        ticks += 1
        await asyncio.sleep(0)
        if ticks > 1:
            fake.closed = True

    async with AsyncMarketWebSocketClient(
        ["asset-a"],
        heartbeat_interval=0.01,
        connect_factory=connect_factory,
        sleep=fast_sleep,
    ):
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    assert "PING" in fake.sent


@pytest.mark.asyncio
async def test_iter_messages_surfaces_heartbeat_send_failure() -> None:
    fake = SlowFailingPingWebSocket()

    async def connect_factory(_: str) -> SlowFailingPingWebSocket:
        return fake

    async def fast_sleep(_: float) -> None:
        await asyncio.sleep(0)

    async with AsyncMarketWebSocketClient(
        ["asset-a"],
        heartbeat_interval=0.01,
        connect_factory=connect_factory,
        sleep=fast_sleep,
    ) as client:
        with pytest.raises(OSError, match="ping failed"):
            await client.iter_messages(reconnect=False).__anext__()

    assert fake.closed is True


@pytest.mark.asyncio
async def test_iter_messages_surfaces_unexpected_heartbeat_send_failure() -> None:
    fake = UnexpectedFailingPingWebSocket()

    async def connect_factory(_: str) -> UnexpectedFailingPingWebSocket:
        return fake

    async def fast_sleep(_: float) -> None:
        await asyncio.sleep(0)

    async with AsyncMarketWebSocketClient(
        ["asset-a"],
        heartbeat_interval=0.01,
        connect_factory=connect_factory,
        sleep=fast_sleep,
    ) as client:
        with pytest.raises(ValueError, match="unexpected ping failure"):
            await client.iter_messages(reconnect=False).__anext__()

    assert fake.closed is True


@pytest.mark.asyncio
async def test_iter_messages_decodes_json_and_skips_heartbeats() -> None:
    messages = [
        "PONG",
        json.dumps({"event_type": "book", "asset_id": "a1", "bids": [], "asks": []}),
        "not-json",
        [{"event_type": "best_bid_ask", "asset_id": "a1", "best_bid": "0.4"}],
    ]
    fake = FakeWebSocket(messages)

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    async with AsyncMarketWebSocketClient(
        ["a1"],
        heartbeat_interval=None,
        connect_factory=connect_factory,
    ) as client:
        seen = [message async for message in client.iter_messages(reconnect=False)]

    assert [message["event_type"] for message in seen] == ["book", "best_bid_ask"]


@pytest.mark.asyncio
async def test_iter_messages_reconnects_and_resubscribes_after_disconnect() -> None:
    first = FakeWebSocket([OSError("connection dropped")])
    second = FakeWebSocket(
        [json.dumps({"event_type": "book", "asset_id": "a1", "bids": [], "asks": []})]
    )
    sockets = deque([first, second])
    sleeps: list[float] = []

    async def connect_factory(_: str) -> FakeWebSocket:
        return sockets.popleft()

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    async with AsyncMarketWebSocketClient(
        ["a1"],
        heartbeat_interval=None,
        connect_factory=connect_factory,
        sleep=record_sleep,
    ) as client:
        message = await client.iter_messages(
            max_reconnects=1,
            reconnect_backoff=0.25,
        ).__anext__()

    assert message["event_type"] == "book"
    assert sleeps == [0.25]
    assert first.closed is True
    assert [decoded_payload(raw) for raw in first.sent] == [
        {
            "assets_ids": ["a1"],
            "type": "market",
            "custom_feature_enabled": True,
        }
    ]
    assert [decoded_payload(raw) for raw in second.sent] == [
        {
            "assets_ids": ["a1"],
            "type": "market",
            "custom_feature_enabled": True,
        }
    ]


@pytest.mark.asyncio
async def test_iter_messages_reconnects_after_clean_remote_close() -> None:
    first = FakeWebSocket()
    second = FakeWebSocket(
        [json.dumps({"event_type": "book", "asset_id": "a1", "bids": [], "asks": []})]
    )
    sockets = deque([first, second])
    reconnects = 0

    async def connect_factory(_: str) -> FakeWebSocket:
        return sockets.popleft()

    def mark_reconnect() -> None:
        nonlocal reconnects
        reconnects += 1

    async with AsyncMarketWebSocketClient(
        ["a1"],
        heartbeat_interval=None,
        connect_factory=connect_factory,
        sleep=no_sleep,
    ) as client:
        message = await client.iter_messages(
            max_reconnects=1,
            on_reconnect=mark_reconnect,
        ).__anext__()

    assert message["event_type"] == "book"
    assert reconnects == 1
    assert first.closed is True


@pytest.mark.asyncio
async def test_clean_remote_close_fails_when_reconnect_budget_is_exhausted() -> None:
    fake = FakeWebSocket()

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    async with AsyncMarketWebSocketClient(
        ["a1"],
        heartbeat_interval=None,
        connect_factory=connect_factory,
    ) as client:
        with pytest.raises(ConnectionError, match="reconnect budget"):
            await client.iter_messages(
                max_reconnects=0,
                fail_on_clean_close_exhausted=True,
            ).__anext__()


@pytest.mark.asyncio
async def test_iter_messages_reconnects_after_windows_resume_reading_race(
    monkeypatch,
) -> None:
    import pmkt.exchanges.polymarket.ws as ws_module

    monkeypatch.setattr(ws_module, "is_transport_teardown_race", lambda _: True)
    first = FakeWebSocket(
        [AttributeError("'NoneType' object has no attribute 'resume_reading'")]
    )
    second = FakeWebSocket(
        [json.dumps({"event_type": "book", "asset_id": "a1", "bids": [], "asks": []})]
    )
    sockets = deque([first, second])

    async def connect_factory(_: str) -> FakeWebSocket:
        return sockets.popleft()

    async with AsyncMarketWebSocketClient(
        ["a1"],
        heartbeat_interval=None,
        connect_factory=connect_factory,
        sleep=no_sleep,
    ) as client:
        message = await client.iter_messages(max_reconnects=1).__anext__()

    assert message["event_type"] == "book"
    assert first.closed is True


@pytest.mark.asyncio
async def test_iter_messages_surfaces_unrelated_attribute_error() -> None:
    fake = FakeWebSocket([AttributeError("application bug")])

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    async with AsyncMarketWebSocketClient(
        ["a1"],
        heartbeat_interval=None,
        connect_factory=connect_factory,
        sleep=no_sleep,
    ) as client:
        with pytest.raises(AttributeError, match="application bug"):
            await client.iter_messages(max_reconnects=1).__anext__()


@pytest.mark.asyncio
async def test_collect_market_snapshots_applies_book_and_price_changes() -> None:
    messages = [
        json.dumps(
            {
                "event_type": "book",
                "asset_id": "a1",
                "market": "0xabc",
                "bids": [{"price": "0.48", "size": "30"}],
                "asks": [{"price": "0.52", "size": "20"}],
                "timestamp": "1766789469000",
            }
        ),
        json.dumps(
            {
                "event_type": "price_change",
                "market": "0xabc",
                "timestamp": "1766789470000",
                "price_changes": [
                    {
                        "asset_id": "a1",
                        "price": "0.49",
                        "size": "10",
                        "side": "BUY",
                        "best_bid": "0.49",
                        "best_ask": "0.52",
                    },
                    {
                        "asset_id": "a1",
                        "price": "0.52",
                        "size": "0",
                        "side": "SELL",
                        "best_bid": "0.49",
                        "best_ask": "0.53",
                    },
                ],
            }
        ),
        json.dumps(
            {
                "event_type": "last_trade_price",
                "asset_id": "a1",
                "market": "0xabc",
                "price": "0.50",
                "size": "5",
                "side": "BUY",
                "timestamp": "1766789471000",
            }
        ),
    ]
    fake = FakeWebSocket(messages)

    async def connect_factory(_: str) -> FakeWebSocket:
        return fake

    snapshots = await collect_market_snapshots(
        ["a1"],
        max_updates=4,
        timeout_seconds=1,
        heartbeat_interval=None,
        reconnect_attempts=0,
        connect_factory=connect_factory,
    )

    assert len(snapshots) == 2
    assert snapshots[0]["event_type"] == "book"
    assert snapshots[0]["best_bid"] == 0.48
    assert snapshots[0]["best_ask"] == 0.52
    assert snapshots[0]["valid_state"] is True
    assert snapshots[0]["quality_flags"] == []
    assert snapshots[1]["best_bid"] == 0.49
    assert snapshots[1]["best_ask"] is None
    assert "empty_ask" in snapshots[1]["quality_flags"]


def test_apply_market_message_keeps_independent_asset_state() -> None:
    states = {"a1": MarketBookState("a1"), "a2": MarketBookState("a2")}
    snapshots = apply_market_message(
        states,
        {
            "event_type": "price_change",
            "market": "0xabc",
            "timestamp": "1000",
            "price_changes": [
                {"asset_id": "a1", "price": "0.2", "size": "4", "side": "BUY"},
                {"asset_id": "a2", "price": "0.8", "size": "5", "side": "SELL"},
            ],
        },
    )

    assert [snapshot.asset_id for snapshot in snapshots] == ["a1", "a2"]
    assert states["a1"].best_bid == 0.2
    assert states["a1"].best_ask is None
    assert states["a2"].best_bid is None
    assert states["a2"].best_ask == 0.8


def test_apply_market_message_does_not_allocate_unowned_asset_state() -> None:
    states = {"owned": MarketBookState("owned")}

    snapshots = apply_market_message(
        states,
        {
            "event_type": "price_change",
            "market": "0xabc",
            "price_changes": [
                {
                    "asset_id": "owned",
                    "price": "0.2",
                    "size": "4",
                    "side": "BUY",
                },
                {
                    "asset_id": "peer",
                    "price": "0.8",
                    "size": "5",
                    "side": "SELL",
                },
            ],
        },
        allowed_asset_ids={"owned"},
    )

    assert [snapshot.asset_id for snapshot in snapshots] == ["owned"]
    assert set(states) == {"owned"}


def test_apply_market_message_recomputes_levels_and_ignores_tick_observation() -> None:
    states = {"a1": MarketBookState("a1")}

    apply_market_message(
        states,
        {
            "event_type": "book",
            "asset_id": "a1",
            "market": "0xabc",
            "bids": [["0.40", "10"], ["0.50", "5"]],
            "asks": [["0.60", "4"]],
        },
    )
    zero_size = apply_market_message(
        states,
        {
            "event_type": "price_change",
            "market": "0xabc",
            "price_changes": [
                {"asset_id": "a1", "price": "0.50", "size": "0", "side": "BUY"}
            ],
        },
    )[0]
    before_tick = deepcopy(states["a1"])
    tick_snapshots = apply_market_message(
        states,
        {
            "event_type": "tick_size_change",
            "asset_id": "a1",
            "market": "0xabc",
            "new_tick_size": "0.01",
        },
    )

    assert zero_size.best_bid == pytest.approx(0.4)
    assert zero_size.bid_depth == 1
    assert tick_snapshots == []
    assert states["a1"] == before_tick
    assert states["a1"].best_ask == pytest.approx(0.6)


def test_market_price_change_before_book_is_invalid() -> None:
    states = {"a1": MarketBookState("a1")}

    snapshot = apply_market_message(
        states,
        {
            "event_type": "price_change",
            "market": "0xabc",
            "price_changes": [
                {"asset_id": "a1", "price": "0.2", "size": "4", "side": "BUY"}
            ],
        },
    )[0]

    assert snapshot.valid_state is False
    assert "delta_before_snapshot" in snapshot.quality_flags
    assert "no_initial_snapshot" in snapshot.quality_flags


def test_market_crossed_book_sets_quality_flag() -> None:
    states = {"a1": MarketBookState("a1")}

    snapshot = apply_market_message(
        states,
        {
            "event_type": "book",
            "asset_id": "a1",
            "market": "0xabc",
            "bids": [["0.70", "10"]],
            "asks": [["0.60", "5"]],
        },
    )[0]

    assert snapshot.valid_state is False
    assert "crossed_book" in snapshot.quality_flags
    assert "negative_spread" in snapshot.quality_flags


@pytest.mark.parametrize(
    "message",
    [
        {
            "event_type": "last_trade_price",
            "asset_id": "a1",
            "market": "0xabc",
            "price": "0.51",
            "size": "2",
        },
        {
            "event_type": "tick_size_change",
            "asset_id": "a1",
            "market": "0xabc",
            "new_tick_size": "0.01",
        },
        {"event_type": "new_market", "asset_id": "a1", "market": "0xabc"},
        {"event_type": "market_resolved", "market": "0xabc"},
    ],
)
def test_observation_only_messages_do_not_mutate_or_create_book_state(
    message: dict[str, object],
) -> None:
    states = {"a1": MarketBookState("a1")}
    apply_market_message(
        states,
        {
            "event_type": "book",
            "asset_id": "a1",
            "market": "0xabc",
            "bids": [["0.40", "10"]],
            "asks": [["0.60", "5"]],
        },
    )
    before = deepcopy(states["a1"])

    assert apply_market_message(states, message) == []
    assert states["a1"] == before

    empty_states: dict[str, MarketBookState] = {}
    assert apply_market_message(empty_states, message) == []
    assert empty_states == {}


def test_market_snapshot_malformed_timestamp_returns_none() -> None:
    snapshot = MarketStreamSnapshot(asset_id="a1", timestamp=[])

    assert snapshot.timestamp_seconds is None
    assert snapshot.datetime_utc is None
    assert snapshot.as_dict()["datetime_utc"] is None


def test_decode_market_messages_handles_dict_list_bytes_and_control_frames() -> None:
    assert decode_market_messages("PING") == []
    assert decode_market_messages(b"PONG") == []
    assert decode_market_messages({"event_type": "book"}) == [{"event_type": "book"}]
    assert decode_market_messages('[{"event_type":"book"}]') == [{"event_type": "book"}]
    assert decode_market_messages(b'[{"event_type":"price_change"}, "bad"]') == [
        {"event_type": "price_change"}
    ]
    assert decode_market_messages("bad-json") == []
