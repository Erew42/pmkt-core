import json
from collections import deque
from decimal import Decimal
from typing import Any

import pytest

from pmkt.exchanges.kalshi.ws import (
    AsyncKalshiWebSocketClient,
    KalshiBookSnapshot,
    KalshiOrderBookState,
    apply_kalshi_orderbook_message,
    decode_kalshi_messages,
    kalshi_orderbook_subscription_payload,
    kalshi_public_subscription_payload,
    kalshi_update_subscription_payload,
    parse_kalshi_subscription_ack,
    parse_kalshi_ticker_update,
    parse_kalshi_websocket_error,
)
from pmkt.exchanges.read_auth import ReadAuthenticationRequiredError


class FakeReadAuth:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.calls: list[str] = []

    def headers_for_get(self, path: str) -> dict[str, str]:
        self.calls.append(path)
        return {"KALSHI-ACCESS-KEY": "key-id"}

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


async def no_sleep(_: float) -> None:
    return None


def test_kalshi_orderbook_subscription_uses_market_tickers() -> None:
    assert kalshi_orderbook_subscription_payload(
        ["KXONE", "KXONE", "KXTWO"], message_id=7
    ) == {
        "id": 7,
        "cmd": "subscribe",
        "params": {
            "channels": ["orderbook_delta"],
            "market_tickers": ["KXONE", "KXTWO"],
            "use_yes_price": True,
        },
    }


def test_kalshi_public_observation_channels_are_explicit_opt_in() -> None:
    payload = kalshi_orderbook_subscription_payload(
        ["KXONE"],
        channels=("orderbook_delta", "trade", "market_lifecycle_v2"),
    )
    assert payload["params"]["channels"] == [
        "orderbook_delta",
        "trade",
        "market_lifecycle_v2",
    ]
    with pytest.raises(ValueError, match="unsupported"):
        kalshi_orderbook_subscription_payload(["KXONE"], channels=("mve_lifecycle",))
    with pytest.raises(ValueError, match="unsupported"):
        kalshi_public_subscription_payload(["KXONE"], channels=("fill",))


def test_kalshi_ticker_subscription_omits_orderbook_only_fields() -> None:
    assert kalshi_public_subscription_payload(
        ["KXONE", "KXONE", "KXTWO"], channels=("ticker",), message_id=9
    ) == {
        "id": 9,
        "cmd": "subscribe",
        "params": {
            "channels": ["ticker"],
            "market_tickers": ["KXONE", "KXTWO"],
        },
    }


def test_kalshi_update_subscription_payload_supports_snapshot_requests() -> None:
    assert kalshi_update_subscription_payload(
        sid=12,
        market_tickers=["KXONE"],
        action="get_snapshot",
        message_id=8,
    ) == {
        "id": 8,
        "cmd": "update_subscription",
        "params": {
            "sids": [12],
            "market_tickers": ["KXONE"],
            "action": "get_snapshot",
            "use_yes_price": True,
        },
    }


def test_kalshi_orderbook_state_applies_snapshot_then_delta() -> None:
    states = {"KXTEST": KalshiOrderBookState("KXTEST")}

    snapshot = apply_kalshi_orderbook_message(
        states,
        {
            "type": "orderbook_snapshot",
            "sid": 2,
            "seq": 1,
            "msg": {
                "market_ticker": "KXTEST",
                "yes_dollars_fp": [["0.40", "10.00"]],
                "no_dollars_fp": [["0.65", "5.00"]],
            },
        },
    )[0]
    delta = apply_kalshi_orderbook_message(
        states,
        {
            "type": "orderbook_delta",
            "sid": 2,
            "seq": 2,
            "msg": {
                "market_ticker": "KXTEST",
                "price_dollars": "0.45",
                "delta_fp": "2.00",
                "side": "yes",
                "ts_ms": 1703123456789,
            },
        },
    )[0]

    assert snapshot.yes_bid == pytest.approx(0.4)
    assert snapshot.yes_ask == pytest.approx(0.65)
    assert snapshot.mid == pytest.approx(0.525)
    assert snapshot.valid_state is True
    assert snapshot.quality_flags == ()
    assert delta.yes_bid == pytest.approx(0.45)
    assert delta.no_ask == pytest.approx(0.55)
    assert delta.valid_state is True
    assert delta.datetime_utc == "2023-12-21T01:50:56.789000+00:00"


def test_kalshi_orderbook_state_uses_yes_price_scale_for_no_side_levels_by_default() -> (
    None
):
    state = KalshiOrderBookState("KXTEST")

    snapshot = state.apply_snapshot(
        {
            "type": "orderbook_snapshot",
            "sid": 2,
            "seq": 1,
            "msg": {
                "market_ticker": "KXTEST",
                "yes_dollars_fp": [["0.40", "10.00"]],
                "no_dollars_fp": [["0.44", "5.00"], ["0.46", "8.00"]],
            },
        }
    )

    assert snapshot.yes_bid == pytest.approx(0.4)
    assert snapshot.yes_ask == pytest.approx(0.44)
    assert snapshot.spread == pytest.approx(0.04)
    assert snapshot.no_bid == pytest.approx(0.56)
    assert snapshot.valid_state is True


def test_kalshi_orderbook_state_can_use_no_price_scale_for_no_side_levels() -> None:
    state = KalshiOrderBookState("KXTEST", use_yes_price=False)

    snapshot = state.apply_snapshot(
        {
            "type": "orderbook_snapshot",
            "sid": 2,
            "seq": 1,
            "msg": {
                "market_ticker": "KXTEST",
                "yes_dollars_fp": [["0.40", "10.00"]],
                "no_dollars_fp": [["0.35", "5.00"]],
            },
        }
    )

    assert snapshot.yes_bid == pytest.approx(0.4)
    assert snapshot.yes_ask == pytest.approx(0.65)
    assert snapshot.no_bid == pytest.approx(0.35)
    assert snapshot.valid_state is True


def test_kalshi_snapshot_malformed_timestamp_returns_none() -> None:
    snapshot = KalshiBookSnapshot(market_ticker="KXTEST", timestamp=[])

    assert snapshot.timestamp_seconds is None
    assert snapshot.datetime_utc is None
    assert snapshot.as_dict()["datetime_utc"] is None


def test_kalshi_reconnect_clears_stale_book_levels() -> None:
    state = KalshiOrderBookState("KXTEST")
    state.apply_snapshot(
        {
            "msg": {
                "market_ticker": "KXTEST",
                "yes_dollars_fp": [["0.40", "10.00"]],
                "no_dollars_fp": [["0.65", "5.00"]],
            }
        }
    )

    state.mark_reconnect()

    assert state.yes_bids == {}
    assert state.no_bids == {}
    assert state.valid_state is False


def test_kalshi_orderbook_delta_removes_depleted_level() -> None:
    states = {"KXTEST": KalshiOrderBookState("KXTEST")}

    apply_kalshi_orderbook_message(
        states,
        {
            "type": "orderbook_snapshot",
            "msg": {
                "market_ticker": "KXTEST",
                "yes_dollars_fp": [["0.40", "3.00"], ["0.30", "1.00"]],
                "no_dollars_fp": [["0.65", "5.00"]],
            },
        },
    )
    snapshot = apply_kalshi_orderbook_message(
        states,
        {
            "type": "orderbook_delta",
            "msg": {
                "market_ticker": "KXTEST",
                "price_dollars": "0.40",
                "delta_fp": "-3.00",
                "side": "yes",
            },
        },
    )[0]

    assert snapshot.yes_bid == pytest.approx(0.3)
    assert snapshot.yes_bid_depth == 1
    assert states["KXTEST"].yes_bids == {0.3: 1.0}


def test_kalshi_orderbook_preserves_explicit_zero_price_fields() -> None:
    states = {"KXTEST": KalshiOrderBookState("KXTEST")}

    apply_kalshi_orderbook_message(
        states,
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
    apply_kalshi_orderbook_message(
        states,
        {
            "type": "orderbook_delta",
            "msg": {
                "market_ticker": "KXTEST",
                "price_dollars": 0.0,
                "price": 0.45,
                "delta_fp": "1.00",
                "side": "yes",
            },
        },
    )

    assert states["KXTEST"].yes_bids == {0.0: 3.0}


def test_kalshi_delta_before_snapshot_is_invalid() -> None:
    states = {"KXTEST": KalshiOrderBookState("KXTEST")}

    snapshot = apply_kalshi_orderbook_message(
        states,
        {
            "type": "orderbook_delta",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "KXTEST",
                "price_dollars": "0.40",
                "delta_fp": "2.00",
                "side": "yes",
            },
        },
    )[0]

    assert snapshot.valid_state is False
    assert "delta_before_snapshot" in snapshot.quality_flags
    assert "no_initial_snapshot" in snapshot.quality_flags


def test_kalshi_market_state_accepts_interleaved_subscription_sequence() -> None:
    state = KalshiOrderBookState("KXTEST")

    ok_snapshot = state.apply_snapshot(
        {
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "KXTEST",
                "yes_dollars_fp": [["0.40", "10.00"]],
                "no_dollars_fp": [["0.65", "5.00"]],
            },
        }
    )
    interleaved_snapshot = state.apply_delta(
        {
            "sid": 1,
            "seq": 3,
            "msg": {
                "market_ticker": "KXTEST",
                "price_dollars": "0.45",
                "delta_fp": "2.00",
                "side": "yes",
            },
        }
    )
    restored = state.apply_snapshot(
        {
            "sid": 1,
            "seq": 4,
            "msg": {
                "market_ticker": "KXTEST",
                "yes_dollars_fp": [["0.42", "10.00"]],
                "no_dollars_fp": [["0.65", "5.00"]],
            },
        }
    )

    assert ok_snapshot.valid_state is True
    assert interleaved_snapshot.valid_state is True
    assert "seq_gap" not in interleaved_snapshot.quality_flags
    assert restored.valid_state is True
    assert restored.quality_flags == ()


def test_kalshi_snapshot_with_new_sid_resynchronizes_state() -> None:
    state = KalshiOrderBookState("KXTEST")
    state.apply_snapshot(
        {
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "KXTEST",
                "yes_dollars_fp": [["0.40", "10.00"]],
                "no_dollars_fp": [["0.65", "5.00"]],
            },
        }
    )
    state.apply_delta(
        {
            "sid": 2,
            "seq": 1,
            "msg": {
                "market_ticker": "KXTEST",
                "price_dollars": "0.45",
                "delta_fp": "2.00",
                "side": "yes",
            },
        }
    )

    restored = state.apply_snapshot(
        {
            "sid": 2,
            "seq": 2,
            "msg": {
                "market_ticker": "KXTEST",
                "yes_dollars_fp": [["0.42", "10.00"]],
                "no_dollars_fp": [["0.65", "5.00"]],
            },
        }
    )

    assert restored.valid_state is True
    assert restored.quality_flags == ()
    assert state.sid == 2
    assert state.last_seq == 2


def test_kalshi_reconnect_invalidates_until_snapshot_restores() -> None:
    state = KalshiOrderBookState("KXTEST")
    state.apply_snapshot(
        {
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "KXTEST",
                "yes_dollars_fp": [["0.40", "10.00"]],
                "no_dollars_fp": [["0.65", "5.00"]],
            },
        }
    )

    state.mark_reconnect()
    after_delta = state.apply_delta(
        {
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
    restored = state.apply_snapshot(
        {
            "sid": 1,
            "seq": 2,
            "msg": {
                "market_ticker": "KXTEST",
                "yes_dollars_fp": [["0.42", "10.00"]],
                "no_dollars_fp": [["0.65", "5.00"]],
            },
        }
    )

    assert after_delta.valid_state is False
    assert "reconnect" in after_delta.quality_flags
    assert "no_initial_snapshot" in after_delta.quality_flags
    assert restored.valid_state is True


def test_decode_kalshi_messages_handles_bytes_lists_and_control_frames() -> None:
    assert decode_kalshi_messages("PING") == []
    assert decode_kalshi_messages(b"PONG") == []
    assert decode_kalshi_messages(b'{"type":"subscribed","sid":1}') == [
        {"type": "subscribed", "sid": 1}
    ]
    assert decode_kalshi_messages('[{"type":"orderbook_delta"}, "bad"]') == [
        {"type": "orderbook_delta"}
    ]
    assert decode_kalshi_messages("not-json") == []


def test_decode_kalshi_messages_reports_malformed_input() -> None:
    failures: list[tuple[str, Any]] = []
    assert decode_kalshi_messages(
        '[{"type":"ticker"}, "bad"]',
        on_decode_error=lambda reason, raw: failures.append((reason, raw)),
    ) == [{"type": "ticker"}]
    assert failures == [("non_object_list_item", "bad")]


def test_parse_kalshi_ticker_and_control_frames() -> None:
    ack = parse_kalshi_subscription_ack(
        {"id": 7, "type": "subscribed", "msg": {"channel": "ticker", "sid": 11}}
    )
    assert ack is not None
    assert (ack.request_id, ack.channel, ack.sid) == (7, "ticker", 11)
    error = parse_kalshi_websocket_error(
        {"id": 7, "type": "error", "msg": {"code": 8, "msg": "bad channel"}}
    )
    assert error is not None
    assert (error.request_id, error.code, error.message) == (7, 8, "bad channel")
    update = parse_kalshi_ticker_update(
        {
            "type": "ticker",
            "sid": 11,
            "msg": {
                "market_ticker": "KXONE",
                "market_id": "market-1",
                "yes_bid_dollars": "0.450",
                "yes_ask_dollars": "0.530",
                "volume_fp": "33896.00",
                "open_interest_fp": "20422.00",
                "yes_bid_size_fp": "300.00",
                "last_trade_size_fp": "25.00",
                "ts": 1669149841,
                "ts_ms": 1669149841000,
                "time": "2022-11-22T20:44:01Z",
            },
        }
    )
    assert update is not None
    assert update.market_ticker == "KXONE"
    assert update.volume_fp == Decimal("33896.00")
    assert update.open_interest_fp == Decimal("20422.00")


def test_parse_kalshi_ticker_rejects_invalid_present_values() -> None:
    with pytest.raises(ValueError, match="volume_fp"):
        parse_kalshi_ticker_update(
            {
                "type": "ticker",
                "sid": 1,
                "msg": {"market_ticker": "KXONE", "volume_fp": "nan"},
            }
        )


@pytest.mark.asyncio
async def test_kalshi_websocket_client_sends_auth_headers_and_subscription() -> None:
    auth = FakeReadAuth()
    fake = FakeWebSocket()
    seen_headers: dict[str, str] = {}

    async def connect_factory(url: str, headers: dict[str, str]) -> FakeWebSocket:
        assert url == "wss://example.com/trade-api/ws/v2"
        seen_headers.update(headers)
        return fake

    async with AsyncKalshiWebSocketClient(
        ["KXTEST"],
        ws_url="wss://example.com/trade-api/ws/v2",
        auth=auth,
        connect_factory=connect_factory,
    ):
        pass

    assert seen_headers["KALSHI-ACCESS-KEY"] == "key-id"
    assert auth.calls == ["/trade-api/ws/v2"]
    assert json.loads(fake.sent[0])["params"] == {
        "channels": ["orderbook_delta"],
        "market_tickers": ["KXTEST"],
        "use_yes_price": True,
    }
    assert fake.closed is True


@pytest.mark.asyncio
async def test_kalshi_websocket_requires_auth_before_connecting() -> None:
    connect_calls: list[str] = []

    async def connect_factory(url: str, headers: dict[str, str]) -> FakeWebSocket:
        connect_calls.append(url)
        return FakeWebSocket()

    client = AsyncKalshiWebSocketClient(
        ["KXTEST"],
        ws_url="wss://example.com/trade-api/ws/v2",
        connect_factory=connect_factory,
    )

    with pytest.raises(ReadAuthenticationRequiredError):
        await client.connect()

    assert connect_calls == []


@pytest.mark.asyncio
async def test_kalshi_subscribe_returns_correlatable_message_id() -> None:
    auth = FakeReadAuth()
    fake = FakeWebSocket()

    async def connect_factory(url: str, headers: dict[str, str]) -> FakeWebSocket:
        return fake

    async with AsyncKalshiWebSocketClient(
        ws_url="wss://example.com/trade-api/ws/v2",
        auth=auth,
        connect_factory=connect_factory,
        public_channels=("ticker",),
    ) as client:
        message_id = await client.subscribe(["KXONE"])

    assert message_id == 1
    assert json.loads(fake.sent[0]) == {
        "id": 1,
        "cmd": "subscribe",
        "params": {"channels": ["ticker"], "market_tickers": ["KXONE"]},
    }


@pytest.mark.asyncio
async def test_kalshi_iter_messages_preserves_exact_raw_frames() -> None:
    auth = FakeReadAuth()
    raw = '{ "type": "ticker", "msg": {"market_ticker": "KXONE", "sid": 4} }'
    fake = FakeWebSocket([raw])
    captured: list[str | bytes] = []

    async def connect_factory(url: str, headers: dict[str, str]) -> FakeWebSocket:
        return fake

    async with AsyncKalshiWebSocketClient(
        ws_url="wss://example.com/trade-api/ws/v2",
        auth=auth,
        connect_factory=connect_factory,
        public_channels=("ticker",),
    ) as client:
        message = await client.iter_messages(
            reconnect=False,
            on_raw_frame=captured.append,
        ).__anext__()

    assert message["type"] == "ticker"
    assert captured == [raw]


@pytest.mark.asyncio
async def test_kalshi_websocket_client_sends_update_subscription() -> None:
    auth = FakeReadAuth()
    fake = FakeWebSocket()

    async def connect_factory(url: str, headers: dict[str, str]) -> FakeWebSocket:
        return fake

    async with AsyncKalshiWebSocketClient(
        ["KXTEST"],
        ws_url="wss://example.com/trade-api/ws/v2",
        auth=auth,
        connect_factory=connect_factory,
    ) as client:
        await client.request_snapshot(sid=22, market_tickers=["KXTEST"])

    assert json.loads(fake.sent[1]) == {
        "id": 2,
        "cmd": "update_subscription",
        "params": {
            "sids": [22],
            "market_tickers": ["KXTEST"],
            "action": "get_snapshot",
            "use_yes_price": True,
        },
    }


@pytest.mark.asyncio
async def test_kalshi_websocket_client_updates_market_membership() -> None:
    auth = FakeReadAuth()
    fake = FakeWebSocket()

    async def connect_factory(url: str, headers: dict[str, str]) -> FakeWebSocket:
        return fake

    async with AsyncKalshiWebSocketClient(
        ["KXTEST"],
        ws_url="wss://example.com/trade-api/ws/v2",
        auth=auth,
        connect_factory=connect_factory,
    ) as client:
        await client.update_subscription(
            sid=22,
            market_tickers=["KXADD"],
            action="add_markets",
        )
        await client.update_subscription(
            sid=22,
            market_tickers=["KXTEST"],
            action="delete_markets",
        )

    assert client.market_tickers == ["KXADD"]
    assert json.loads(fake.sent[1])["params"] == {
        "sids": [22],
        "market_tickers": ["KXADD"],
        "action": "add_markets",
        "use_yes_price": True,
    }
    assert json.loads(fake.sent[2])["params"] == {
        "sids": [22],
        "market_tickers": ["KXTEST"],
        "action": "delete_markets",
        "use_yes_price": True,
    }


@pytest.mark.asyncio
async def test_kalshi_websocket_client_rejects_invalid_update_action_before_mutation() -> (
    None
):
    auth = FakeReadAuth()
    fake = FakeWebSocket()

    async def connect_factory(url: str, headers: dict[str, str]) -> FakeWebSocket:
        return fake

    async with AsyncKalshiWebSocketClient(
        ["KXTEST"],
        ws_url="wss://example.com/trade-api/ws/v2",
        auth=auth,
        connect_factory=connect_factory,
    ) as client:
        with pytest.raises(ValueError, match="action must be"):
            await client.update_subscription(
                sid=22,
                market_tickers=["KXBAD"],
                action="replace_markets",
            )
        assert client.market_tickers == ["KXTEST"]

    assert len(fake.sent) == 1


@pytest.mark.asyncio
async def test_kalshi_iter_messages_reconnects_after_windows_resume_race(
    monkeypatch,
) -> None:
    import pmkt.exchanges.kalshi.ws as ws_module

    monkeypatch.setattr(ws_module, "is_transport_teardown_race", lambda _: True)
    auth = FakeReadAuth()
    first = FakeWebSocket(
        [AttributeError("'NoneType' object has no attribute 'resume_reading'")]
    )
    second = FakeWebSocket([json.dumps({"type": "subscribed", "sid": 1})])
    sockets = deque([first, second])

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return sockets.popleft()

    async with AsyncKalshiWebSocketClient(
        ["KXTEST"],
        auth=auth,
        connect_factory=connect_factory,
        sleep=no_sleep,
    ) as client:
        message = await client.iter_messages(max_reconnects=1).__anext__()

    assert message == {"type": "subscribed", "sid": 1}
    assert first.closed is True


@pytest.mark.asyncio
async def test_kalshi_iter_messages_reconnects_after_clean_remote_close() -> None:
    auth = FakeReadAuth()
    first = FakeWebSocket()
    second = FakeWebSocket([json.dumps({"type": "subscribed", "sid": 1})])
    sockets = deque([first, second])
    reconnects = 0

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return sockets.popleft()

    def mark_reconnect() -> None:
        nonlocal reconnects
        reconnects += 1

    async with AsyncKalshiWebSocketClient(
        ["KXTEST"],
        auth=auth,
        connect_factory=connect_factory,
        sleep=no_sleep,
    ) as client:
        message = await client.iter_messages(
            max_reconnects=1,
            on_reconnect=mark_reconnect,
        ).__anext__()

    assert message == {"type": "subscribed", "sid": 1}
    assert reconnects == 1
    assert first.closed is True


@pytest.mark.asyncio
async def test_kalshi_clean_close_fails_when_reconnect_budget_is_exhausted() -> None:
    auth = FakeReadAuth()
    fake = FakeWebSocket()

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    async with AsyncKalshiWebSocketClient(
        ["KXTEST"],
        auth=auth,
        connect_factory=connect_factory,
    ) as client:
        with pytest.raises(ConnectionError, match="reconnect budget"):
            await client.iter_messages(
                max_reconnects=0,
                fail_on_clean_close_exhausted=True,
            ).__anext__()


@pytest.mark.asyncio
async def test_kalshi_iter_messages_surfaces_unrelated_attribute_error() -> None:
    auth = FakeReadAuth()
    fake = FakeWebSocket([AttributeError("application bug")])

    async def connect_factory(_: str, __: dict[str, str]) -> FakeWebSocket:
        return fake

    async with AsyncKalshiWebSocketClient(
        ["KXTEST"],
        auth=auth,
        connect_factory=connect_factory,
        sleep=no_sleep,
    ) as client:
        with pytest.raises(AttributeError, match="application bug"):
            await client.iter_messages(max_reconnects=1).__anext__()
