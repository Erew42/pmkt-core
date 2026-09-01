from __future__ import annotations

import asyncio
from decimal import Decimal, InvalidOperation
import inspect
import json
import logging
import time
from dataclasses import dataclass, field
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Mapping,
    Sequence,
    cast,
)
from urllib.parse import urlparse

import websockets
from websockets.exceptions import ConnectionClosed

from pmkt.config import get_config
from pmkt.exchanges.ws_transport import (
    WS_TRANSPORT_LIMITS,  # noqa: F401 - legacy module re-export
    WebSocketTransportSettings,
    is_transport_teardown_race,
)
from pmkt.data.kalshi_quotes import (
    KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT,
    project_kalshi_quotes,
)
from pmkt.data.prices import complement_probability as _price_complement
from pmkt.data.time import isoformat_source_timestamp
from pmkt.data.time import timestamp_seconds as _timestamp_seconds
from pmkt.data.types import parse_float as _parse_float
from pmkt.data.types import parse_int as _parse_int
from pmkt.exchanges.read_auth import (
    ReadAuthHeaderProvider,
    ReadAuthenticationRequiredError,
    headers_for_read,
)

logger = logging.getLogger(__name__)

KalshiConnectFactory = Callable[[str, dict[str, str]], Any | Awaitable[Any]]
SleepFunc = Callable[[float], Awaitable[None]]
DecodeErrorCallback = Callable[[str, Any], None]
RawFrameCallback = Callable[[str | bytes], None | Awaitable[None]]

DEFAULT_RECONNECT_ATTEMPTS = 3
DEFAULT_RECONNECT_BACKOFF_SECONDS = 0.5


def _first_parsed_float(*values: Any) -> float | None:
    for value in values:
        parsed = _parse_float(value)
        if parsed is not None:
            return parsed
    return None


def normalize_market_tickers(tickers: str | Iterable[str]) -> list[str]:
    if isinstance(tickers, str):
        values = [tickers]
    else:
        values = [str(ticker) for ticker in tickers if ticker is not None]
    cleaned: list[str] = []
    seen: set[str] = set()
    for ticker in values:
        token = str(ticker).strip()
        if not token or token in seen:
            continue
        seen.add(token)
        cleaned.append(token)
    if not cleaned:
        raise ValueError("At least one Kalshi market ticker is required.")
    return cleaned


def kalshi_public_subscription_payload(
    market_tickers: str | Iterable[str] | None = None,
    *,
    message_id: int = 1,
    use_yes_price: bool = True,
    channels: Sequence[str] = ("orderbook_delta",),
) -> dict[str, Any]:
    allowed_channels = {"orderbook_delta", "ticker", "trade", "market_lifecycle_v2"}
    normalized_channels = list(dict.fromkeys(str(channel) for channel in channels))
    if not normalized_channels or any(
        channel not in allowed_channels for channel in normalized_channels
    ):
        raise ValueError("unsupported or empty Kalshi public channel selection")
    params: dict[str, Any] = {"channels": normalized_channels}
    if market_tickers is not None:
        params["market_tickers"] = normalize_market_tickers(market_tickers)
    if "orderbook_delta" in normalized_channels:
        params["use_yes_price"] = bool(use_yes_price)
    return {
        "id": int(message_id),
        "cmd": "subscribe",
        "params": params,
    }


def kalshi_orderbook_subscription_payload(
    market_tickers: str | Iterable[str],
    *,
    message_id: int = 1,
    use_yes_price: bool = True,
    channels: Sequence[str] = ("orderbook_delta",),
) -> dict[str, Any]:
    return kalshi_public_subscription_payload(
        market_tickers,
        message_id=message_id,
        use_yes_price=use_yes_price,
        channels=channels,
    )


def kalshi_update_subscription_payload(
    *,
    sid: int,
    market_tickers: str | Iterable[str],
    action: str,
    message_id: int = 1,
    use_yes_price: bool = True,
) -> dict[str, Any]:
    if action not in {"add_markets", "delete_markets", "get_snapshot"}:
        raise ValueError("action must be add_markets, delete_markets, or get_snapshot")
    return {
        "id": int(message_id),
        "cmd": "update_subscription",
        "params": {
            "sids": [int(sid)],
            "market_tickers": normalize_market_tickers(market_tickers),
            "action": action,
            "use_yes_price": bool(use_yes_price),
        },
    }


def decode_kalshi_messages(
    raw: Any,
    *,
    on_decode_error: DecodeErrorCallback | None = None,
) -> list[dict[str, Any]]:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        frame = raw.strip()
        if not frame or frame.upper() in {"PING", "PONG"}:
            return []
        try:
            decoded = json.loads(frame)
        except json.JSONDecodeError:
            logger.warning("Failed to parse Kalshi websocket message: %s", raw)
            if on_decode_error is not None:
                on_decode_error("invalid_json", raw)
            return []
    else:
        decoded = raw

    if isinstance(decoded, dict):
        return [decoded]
    if isinstance(decoded, list):
        messages: list[dict[str, Any]] = []
        for item in decoded:
            if isinstance(item, dict):
                messages.append(item)
            elif on_decode_error is not None:
                on_decode_error("non_object_list_item", item)
        return messages
    if on_decode_error is not None:
        on_decode_error("unsupported_top_level", decoded)
    return []


def _required_int(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"Kalshi websocket {field_name} must be an integer.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Kalshi websocket {field_name} must be an integer."
        ) from exc
    return parsed


def _optional_decimal(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    maximum: Decimal | None = None,
) -> Decimal | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"Kalshi ticker {field_name} is invalid: {value}")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Kalshi ticker {field_name} is invalid: {value}") from exc
    if not parsed.is_finite() or parsed < 0 or (
        maximum is not None and parsed > maximum
    ):
        raise ValueError(f"Kalshi ticker {field_name} is invalid: {value}")
    return parsed


@dataclass(frozen=True)
class KalshiSubscriptionAck:
    request_id: int
    channel: str
    sid: int


@dataclass(frozen=True)
class KalshiWebSocketError:
    request_id: int | None
    code: int | None
    message: str


@dataclass(frozen=True)
class KalshiTickerUpdate:
    sid: int
    market_ticker: str
    market_id: str | None
    price_dollars: Decimal | None
    yes_bid_dollars: Decimal | None
    yes_ask_dollars: Decimal | None
    volume_fp: Decimal | None
    open_interest_fp: Decimal | None
    dollar_volume: Decimal | None
    dollar_open_interest: Decimal | None
    yes_bid_size_fp: Decimal | None
    yes_ask_size_fp: Decimal | None
    last_trade_size_fp: Decimal | None
    ts: int | None
    ts_ms: int | None
    time: str | None
    raw_message: dict[str, Any]


def parse_kalshi_subscription_ack(
    message: Mapping[str, Any],
) -> KalshiSubscriptionAck | None:
    if message.get("type") != "subscribed":
        return None
    payload = message.get("msg")
    if not isinstance(payload, Mapping):
        raise ValueError("Kalshi subscribed frame lacks an object msg.")
    channel = str(payload.get("channel") or "").strip()
    if not channel:
        raise ValueError("Kalshi subscribed frame lacks channel.")
    return KalshiSubscriptionAck(
        request_id=_required_int(message.get("id"), field_name="id"),
        channel=channel,
        sid=_required_int(payload.get("sid"), field_name="sid"),
    )


def parse_kalshi_websocket_error(
    message: Mapping[str, Any],
) -> KalshiWebSocketError | None:
    if message.get("type") != "error":
        return None
    payload = message.get("msg")
    if not isinstance(payload, Mapping):
        raise ValueError("Kalshi error frame lacks an object msg.")
    request_id = message.get("id")
    code = payload.get("code")
    return KalshiWebSocketError(
        request_id=(
            None if request_id is None else _required_int(request_id, field_name="id")
        ),
        code=None if code is None else _required_int(code, field_name="error code"),
        message=str(payload.get("msg") or "").strip(),
    )


def parse_kalshi_ticker_update(
    message: Mapping[str, Any],
) -> KalshiTickerUpdate | None:
    if message.get("type") != "ticker":
        return None
    payload = message.get("msg")
    if not isinstance(payload, Mapping):
        raise ValueError("Kalshi ticker frame lacks an object msg.")
    ticker = str(payload.get("market_ticker") or "").strip()
    if not ticker:
        raise ValueError("Kalshi ticker frame lacks market_ticker.")
    time_value = payload.get("time")
    parsed_time = None if time_value is None else str(time_value).strip()
    if parsed_time == "":
        raise ValueError("Kalshi ticker time cannot be empty when present.")
    ts = payload.get("ts")
    ts_ms = payload.get("ts_ms")
    return KalshiTickerUpdate(
        sid=_required_int(message.get("sid"), field_name="sid"),
        market_ticker=ticker,
        market_id=(
            str(payload["market_id"]).strip()
            if payload.get("market_id") is not None
            else None
        ),
        price_dollars=_optional_decimal(
            payload, "price_dollars", maximum=Decimal("1")
        ),
        yes_bid_dollars=_optional_decimal(
            payload, "yes_bid_dollars", maximum=Decimal("1")
        ),
        yes_ask_dollars=_optional_decimal(
            payload, "yes_ask_dollars", maximum=Decimal("1")
        ),
        volume_fp=_optional_decimal(payload, "volume_fp"),
        open_interest_fp=_optional_decimal(payload, "open_interest_fp"),
        dollar_volume=_optional_decimal(payload, "dollar_volume"),
        dollar_open_interest=_optional_decimal(payload, "dollar_open_interest"),
        yes_bid_size_fp=_optional_decimal(payload, "yes_bid_size_fp"),
        yes_ask_size_fp=_optional_decimal(payload, "yes_ask_size_fp"),
        last_trade_size_fp=_optional_decimal(payload, "last_trade_size_fp"),
        ts=None if ts is None else _required_int(ts, field_name="ts"),
        ts_ms=None if ts_ms is None else _required_int(ts_ms, field_name="ts_ms"),
        time=parsed_time,
        raw_message=dict(message),
    )


def _message_payload(message: dict[str, Any]) -> dict[str, Any]:
    msg = message.get("msg")
    return msg if isinstance(msg, dict) else message


def _levels_to_map(levels: Any) -> dict[float, float]:
    parsed: dict[float, float] = {}
    if not isinstance(levels, list):
        return parsed
    for level in levels:
        if isinstance(level, dict):
            price = _first_parsed_float(level.get("price"), level.get("price_dollars"))
            size = _first_parsed_float(
                level.get("size"), level.get("count"), level.get("count_fp")
            )
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price = _parse_float(level[0])
            size = _parse_float(level[1])
        else:
            continue
        if price is None or size is None or size <= 0:
            continue
        parsed[price] = size
    return parsed


@dataclass
class KalshiBookSnapshot:
    market_ticker: str
    market_id: str | None = None
    event_type: str | None = None
    sid: Any = None
    seq: Any = None
    timestamp: Any = None
    yes_bid: float | None = None
    yes_ask: float | None = None
    no_bid: float | None = None
    no_ask: float | None = None
    yes_bid_size: float | None = None
    yes_ask_size: float | None = None
    no_bid_size: float | None = None
    no_ask_size: float | None = None
    yes_bid_source: str | None = None
    yes_ask_source: str | None = None
    no_bid_source: str | None = None
    no_ask_source: str | None = None
    use_yes_price: bool = True
    quote_normalization_policy: str = KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT
    yes_bid_depth: int = 0
    no_bid_depth: int = 0
    valid_state: bool = False
    quality_flags: tuple[str, ...] = ()
    initial_snapshot_received: bool = False

    @property
    def spread(self) -> float | None:
        if self.yes_bid is None or self.yes_ask is None:
            return None
        return self.yes_ask - self.yes_bid

    @property
    def mid(self) -> float | None:
        if self.yes_bid is None or self.yes_ask is None:
            return None
        return (self.yes_bid + self.yes_ask) / 2.0

    @property
    def timestamp_seconds(self) -> float | None:
        # State currently combines Kalshi ``ts_ms`` and ``ts`` source fields.
        return _timestamp_seconds(self.timestamp, unit="auto")

    @property
    def datetime_utc(self) -> str | None:
        return isoformat_source_timestamp(self.timestamp, epoch_unit="auto")

    def as_dict(self) -> dict[str, Any]:
        return {
            "exchange": "kalshi",
            "market_ticker": self.market_ticker,
            "market_id": self.market_id,
            "event_type": self.event_type,
            "sid": self.sid,
            "seq": self.seq,
            "timestamp": self.timestamp,
            "timestamp_seconds": self.timestamp_seconds,
            "datetime_utc": self.datetime_utc,
            "yes_bid": self.yes_bid,
            "yes_ask": self.yes_ask,
            "no_bid": self.no_bid,
            "no_ask": self.no_ask,
            "yes_bid_size": self.yes_bid_size,
            "yes_ask_size": self.yes_ask_size,
            "no_bid_size": self.no_bid_size,
            "no_ask_size": self.no_ask_size,
            "yes_bid_source": self.yes_bid_source,
            "yes_ask_source": self.yes_ask_source,
            "no_bid_source": self.no_bid_source,
            "no_ask_source": self.no_ask_source,
            "use_yes_price": self.use_yes_price,
            "quote_normalization_policy": self.quote_normalization_policy,
            "mid": self.mid,
            "spread": self.spread,
            "yes_bid_depth": self.yes_bid_depth,
            "no_bid_depth": self.no_bid_depth,
            "valid_state": self.valid_state,
            "quality_flags": list(self.quality_flags),
            "initial_snapshot_received": self.initial_snapshot_received,
        }


@dataclass
class KalshiOrderBookState:
    market_ticker: str
    use_yes_price: bool = True
    quote_normalization_policy: str = KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT
    market_id: str | None = None
    yes_bids: dict[float, float] = field(default_factory=dict)
    no_bids: dict[float, float] = field(default_factory=dict)
    last_event_type: str | None = None
    sid: int | None = None
    last_seq: int | None = None
    timestamp: Any = None
    initial_snapshot_received: bool = False
    valid_state: bool = False
    quality_flags: set[str] = field(default_factory=lambda: {"no_initial_snapshot"})

    def mark_reconnect(self) -> None:
        self.sid = None
        self.last_seq = None
        self.yes_bids.clear()
        self.no_bids.clear()
        self.initial_snapshot_received = False
        self.valid_state = False
        self.quality_flags.update({"reconnect", "no_initial_snapshot"})

    def snapshot(self, *, event_type: str | None = None) -> KalshiBookSnapshot:
        yes_bid = max(self.yes_bids) if self.yes_bids else None
        no_price = (
            min(self.no_bids)
            if self.use_yes_price and self.no_bids
            else max(self.no_bids)
            if self.no_bids
            else None
        )
        yes_bid_size = self.yes_bids.get(yes_bid) if yes_bid is not None else None
        no_price_size = self.no_bids.get(no_price) if no_price is not None else None
        quotes = project_kalshi_quotes(
            yes_bid=yes_bid,
            no_ladder_price=no_price,
            yes_bid_size=yes_bid_size,
            no_ladder_size=no_price_size,
            use_yes_price=self.use_yes_price,
            policy_version=self.quote_normalization_policy,
        )
        return KalshiBookSnapshot(
            market_ticker=self.market_ticker,
            market_id=self.market_id,
            event_type=event_type or self.last_event_type,
            sid=self.sid,
            seq=self.last_seq,
            timestamp=self.timestamp,
            **quotes.as_dict(),
            yes_bid_depth=len(self.yes_bids),
            no_bid_depth=len(self.no_bids),
            valid_state=self.valid_state,
            quality_flags=tuple(sorted(self.quality_flags)),
            initial_snapshot_received=self.initial_snapshot_received,
        )

    def apply_snapshot(self, message: dict[str, Any]) -> KalshiBookSnapshot:
        msg = _message_payload(message)
        ticker = msg.get("market_ticker") or self.market_ticker
        self.market_ticker = str(ticker)
        market_id = msg.get("market_id")
        if market_id is not None:
            self.market_id = str(market_id)
        self.timestamp = (
            msg.get("ts_ms") if msg.get("ts_ms") is not None else msg.get("ts")
        )
        self.yes_bids = _levels_to_map(
            msg.get("yes_dollars_fp") or msg.get("yes_dollars") or msg.get("yes") or []
        )
        self.no_bids = _levels_to_map(
            msg.get("no_dollars_fp") or msg.get("no_dollars") or msg.get("no") or []
        )
        self.initial_snapshot_received = True
        self.valid_state = True
        self.quality_flags.clear()
        self.sid = None
        self.last_seq = None
        self._apply_sequence(message.get("sid"), message.get("seq"))
        self._refresh_validity(preserve=self._sequence_flags())
        self.last_event_type = "orderbook_snapshot"
        return self.snapshot(event_type="orderbook_snapshot")

    def apply_delta(self, message: dict[str, Any]) -> KalshiBookSnapshot:
        msg = _message_payload(message)
        ticker = msg.get("market_ticker") or self.market_ticker
        self.market_ticker = str(ticker)
        market_id = msg.get("market_id")
        if market_id is not None:
            self.market_id = str(market_id)
        self.timestamp = (
            msg.get("ts_ms") if msg.get("ts_ms") is not None else msg.get("ts")
        )
        if not self.initial_snapshot_received:
            self.valid_state = False
            self.quality_flags.update({"delta_before_snapshot", "no_initial_snapshot"})
        self._apply_sequence(message.get("sid"), message.get("seq"))
        side = str(msg.get("side") or "").lower()
        price = _first_parsed_float(msg.get("price_dollars"), msg.get("price"))
        delta = _first_parsed_float(
            msg.get("delta_fp"), msg.get("delta"), msg.get("size_delta")
        )
        if side in {"yes", "no"} and price is not None and delta is not None:
            levels = self.yes_bids if side == "yes" else self.no_bids
            next_size = levels.get(price, 0.0) + delta
            if next_size <= 0:
                levels.pop(price, None)
            else:
                levels[price] = next_size
        self._refresh_validity(preserve=self._persistent_flags())
        self.last_event_type = "orderbook_delta"
        return self.snapshot(event_type="orderbook_delta")

    def _apply_sequence(self, sid: Any, seq: Any) -> None:
        sid_i = _parse_int(sid)
        seq_i = _parse_int(seq)
        if sid_i is None or seq_i is None:
            self.valid_state = False
            self.quality_flags.add("missing_sequence")
            return

        if self.sid is None:
            self.sid = sid_i
        elif sid_i != self.sid:
            self.valid_state = False
            self.quality_flags.add("sid_changed")

        # ``seq`` advances for the whole subscription, not independently for
        # each market.  Forward jumps are therefore expected when messages for
        # multiple markets are interleaved; the collector checks contiguous
        # subscription-level sequence before dispatching to market states.
        if self.last_seq is not None and seq_i <= self.last_seq:
            self.valid_state = False
            self.quality_flags.add("seq_gap")

        self.last_seq = seq_i

    def mark_sequence_gap(self) -> None:
        self.valid_state = False
        self.quality_flags.add("seq_gap")

    def _sequence_flags(self) -> set[str]:
        return self.quality_flags.intersection(
            {"missing_sequence", "seq_gap", "sid_changed"}
        )

    def _persistent_flags(self) -> set[str]:
        return self.quality_flags.intersection(
            {
                "delta_before_snapshot",
                "missing_sequence",
                "no_initial_snapshot",
                "reconnect",
                "seq_gap",
                "sid_changed",
            }
        )

    def _refresh_validity(self, *, preserve: set[str] | None = None) -> None:
        flags = set(preserve or set())
        if not self.initial_snapshot_received:
            flags.add("no_initial_snapshot")
        if not self.yes_bids:
            flags.add("empty_bid")
        if not self.no_bids:
            flags.add("empty_ask")
        yes_bid = max(self.yes_bids) if self.yes_bids else None
        yes_ask = (
            min(self.no_bids)
            if self.use_yes_price and self.no_bids
            else _price_complement(max(self.no_bids))
            if self.no_bids
            else None
        )
        if yes_bid is not None and yes_ask is not None and yes_bid > yes_ask:
            flags.update({"crossed_book", "negative_spread"})
        self.quality_flags = flags
        self.valid_state = self.initial_snapshot_received and not flags


def _state_for(
    states: dict[str, KalshiOrderBookState],
    market_ticker: Any,
    *,
    use_yes_price: bool,
) -> KalshiOrderBookState | None:
    if market_ticker is None:
        return None
    ticker = str(market_ticker)
    if not ticker:
        return None
    state = states.setdefault(
        ticker, KalshiOrderBookState(ticker, use_yes_price=use_yes_price)
    )
    state.use_yes_price = use_yes_price
    return state


def apply_kalshi_orderbook_message(
    states: dict[str, KalshiOrderBookState],
    message: dict[str, Any],
    *,
    use_yes_price: bool = True,
) -> list[KalshiBookSnapshot]:
    event_type = str(message.get("type") or "")
    msg = _message_payload(message)
    ticker = msg.get("market_ticker")

    if event_type == "orderbook_snapshot":
        state = _state_for(states, ticker, use_yes_price=use_yes_price)
        return [state.apply_snapshot(message)] if state is not None else []

    if event_type == "orderbook_delta":
        state = _state_for(states, ticker, use_yes_price=use_yes_price)
        return [state.apply_delta(message)] if state is not None else []

    return []


def _connect_with_headers(
    url: str,
    headers: dict[str, str],
    *,
    transport_settings: WebSocketTransportSettings | None = None,
) -> Any:
    try:
        parameters = inspect.signature(websockets.connect).parameters
        header_kw = (
            "additional_headers"
            if "additional_headers" in parameters
            else "extra_headers"
        )
    except (TypeError, ValueError):
        header_kw = "additional_headers"
    # See WS_TRANSPORT_LIMITS: the 1 MiB max_size default is a hard ceiling on
    # universe size, and the 16-frame max_queue default puts ordinary commit
    # latency onto the transport flow-control path.
    return cast(Any, websockets.connect)(
        url,
        **{header_kw: headers},
        **(transport_settings or WebSocketTransportSettings()).as_connect_kwargs(),
    )


class AsyncKalshiWebSocketClient:
    """Authenticated Kalshi websocket client for order-book market data."""

    def __init__(
        self,
        market_tickers: Sequence[str] | None = None,
        *,
        ws_url: str | None = None,
        auth: ReadAuthHeaderProvider | None = None,
        connect_factory: KalshiConnectFactory | None = None,
        transport_settings: WebSocketTransportSettings | None = None,
        sleep: SleepFunc = asyncio.sleep,
        use_yes_price: bool = True,
        public_channels: Sequence[str] = ("orderbook_delta",),
        on_subscription_start: Callable[[], None] | None = None,
        on_subscription_established: Callable[[str, str], None] | None = None,
    ) -> None:
        self.ws_url = ws_url or get_config().resolved_kalshi_ws_url
        self.market_tickers = (
            normalize_market_tickers(market_tickers) if market_tickers else []
        )
        self.header_provider = auth
        self.use_yes_price = bool(use_yes_price)
        self.public_channels = tuple(public_channels)
        self.transport_settings = transport_settings or WebSocketTransportSettings()
        self._connect_factory = connect_factory or (
            lambda url, headers: _connect_with_headers(
                url, headers, transport_settings=self.transport_settings
            )
        )
        self._sleep = sleep
        self._on_subscription_start = on_subscription_start
        self._on_subscription_established = on_subscription_established
        self._ws: Any | None = None
        self._message_id = 1
        self._last_subscription_message_id: int | None = None

    @property
    def is_connected(self) -> bool:
        if self._ws is None:
            return False
        return not bool(getattr(self._ws, "closed", False))

    def auth_headers(self) -> dict[str, str]:
        if self.header_provider is None:
            raise ReadAuthenticationRequiredError(
                "Kalshi websocket reads require an injected header provider"
            )
        path = urlparse(self.ws_url).path or "/trade-api/ws/v2"
        return headers_for_read(self.header_provider, "GET", path)

    async def connect(self) -> None:
        if self.is_connected:
            return
        headers = self.auth_headers()
        maybe_ws = self._connect_factory(self.ws_url, headers)
        self._ws = await maybe_ws if inspect.isawaitable(maybe_ws) else maybe_ws
        if self.market_tickers:
            self._last_subscription_message_id = await self._send_subscription(
                self.market_tickers
            )
        logger.info("Connected to Kalshi websocket at %s", self.ws_url)

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
            self._ws = None

    async def reconnect(self) -> None:
        await self.close()
        await self.connect()

    async def __aenter__(self) -> "AsyncKalshiWebSocketClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def subscribe(self, market_tickers: str | Iterable[str]) -> int:
        tickers = normalize_market_tickers(market_tickers)
        current = set(self.market_tickers)
        self.market_tickers.extend(
            ticker for ticker in tickers if ticker not in current
        )
        if not self.is_connected:
            await self.connect()
            message_id = self._last_subscription_message_id
            if message_id is None:
                raise RuntimeError("Kalshi subscription was not sent after connect.")
            return message_id
        return await self._send_subscription(tickers)

    async def _send_subscription(
        self, market_tickers: str | Iterable[str]
    ) -> int:
        tickers = normalize_market_tickers(market_tickers)
        message_id = self._message_id
        payload = kalshi_orderbook_subscription_payload(
            tickers,
            message_id=message_id,
            use_yes_price=self.use_yes_price,
            channels=self.public_channels,
        )
        self._message_id += 1
        if self._on_subscription_start is not None:
            self._on_subscription_start()
        sent_at_utc = (
            isoformat_source_timestamp(time.time(), epoch_unit="seconds") or ""
        )
        await self._send_json(payload)
        if self._on_subscription_established is not None:
            self._on_subscription_established(
                sent_at_utc,
                isoformat_source_timestamp(time.time(), epoch_unit="seconds")
                or sent_at_utc,
            )
        self._last_subscription_message_id = message_id
        return message_id

    async def update_subscription(
        self,
        *,
        sid: int,
        market_tickers: str | Iterable[str],
        action: str,
    ) -> None:
        if action not in {"add_markets", "delete_markets", "get_snapshot"}:
            raise ValueError(
                "action must be add_markets, delete_markets, or get_snapshot"
            )
        tickers = normalize_market_tickers(market_tickers)
        if action == "add_markets":
            current = set(self.market_tickers)
            self.market_tickers.extend(
                ticker for ticker in tickers if ticker not in current
            )
        elif action == "delete_markets":
            remove = set(tickers)
            self.market_tickers = [
                ticker for ticker in self.market_tickers if ticker not in remove
            ]
        if not self.is_connected:
            await self.connect()
            return
        payload = kalshi_update_subscription_payload(
            sid=sid,
            market_tickers=tickers,
            action=action,
            message_id=self._message_id,
            use_yes_price=self.use_yes_price,
        )
        self._message_id += 1
        await self._send_json(payload)

    async def request_snapshot(
        self, *, sid: int, market_tickers: str | Iterable[str]
    ) -> None:
        await self.update_subscription(
            sid=sid,
            market_tickers=market_tickers,
            action="get_snapshot",
        )

    async def probe_liveness(self, *, timeout_seconds: float = 1.0) -> bool:
        """Confirm ping/pong transport liveness without treating it as book data."""
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        ws = self._ws
        ping = getattr(ws, "ping", None) if ws is not None else None
        if not self.is_connected or not callable(ping):
            return False
        try:
            result = ping()
            if inspect.isawaitable(result):
                result = await asyncio.wait_for(result, timeout=timeout_seconds)
            if inspect.isawaitable(result):
                await asyncio.wait_for(result, timeout=timeout_seconds)
        except (ConnectionClosed, OSError, RuntimeError, asyncio.TimeoutError):
            return False
        return self.is_connected

    async def iter_messages(
        self,
        *,
        reconnect: bool = True,
        max_reconnects: int = DEFAULT_RECONNECT_ATTEMPTS,
        reconnect_backoff: float = DEFAULT_RECONNECT_BACKOFF_SECONDS,
        on_reconnect: Callable[[], None] | None = None,
        on_decode_error: DecodeErrorCallback | None = None,
        on_raw_frame: RawFrameCallback | None = None,
        fail_on_clean_close_exhausted: bool = False,
    ) -> AsyncIterator[dict[str, Any]]:
        if not self.is_connected:
            await self.connect()
        reconnects = 0
        while self.is_connected:
            ws = self._ws
            if ws is None:
                return
            try:
                async for raw in ws:
                    if on_raw_frame is not None:
                        callback_result = on_raw_frame(raw)
                        if inspect.isawaitable(callback_result):
                            await callback_result
                    for message in decode_kalshi_messages(
                        raw, on_decode_error=on_decode_error
                    ):
                        yield message
                # A clean remote close is still a lost operational feed.  Keep
                # reconnecting within the caller's explicit budget rather than
                # reporting iterator exhaustion as a completed capture.
                if not reconnect:
                    return
                if reconnects >= max_reconnects:
                    if fail_on_clean_close_exhausted:
                        raise ConnectionError(
                            "Kalshi websocket closed cleanly and exhausted the "
                            "reconnect budget"
                        )
                    return
                reconnects += 1
                if on_reconnect is not None:
                    on_reconnect()
                await self._sleep(reconnect_backoff * reconnects)
                await self.reconnect()
            except (ConnectionClosed, OSError, asyncio.TimeoutError):
                if not reconnect or reconnects >= max_reconnects:
                    raise
                reconnects += 1
                if on_reconnect is not None:
                    on_reconnect()
                await self._sleep(reconnect_backoff * reconnects)
                await self.reconnect()
            except AttributeError as exc:
                # asyncio's SSL transport can race teardown against receive
                # flow control (pause_reading/resume_reading). Treat only that
                # structural failure as a disconnect; unrelated AttributeErrors
                # remain programming errors and must fail closed.
                if not is_transport_teardown_race(exc):
                    raise
                if not reconnect or reconnects >= max_reconnects:
                    raise
                reconnects += 1
                if on_reconnect is not None:
                    on_reconnect()
                await self._sleep(reconnect_backoff * reconnects)
                await self.reconnect()

    async def _send_json(self, payload: dict[str, Any]) -> None:
        if not self.is_connected:
            raise RuntimeError("WebSocket is not connected.")
        ws = self._ws
        if ws is None:
            raise RuntimeError("WebSocket is not connected.")
        await ws.send(json.dumps(payload, separators=(",", ":")))


__all__ = [
    "AsyncKalshiWebSocketClient",
    "DecodeErrorCallback",
    "KalshiBookSnapshot",
    "KalshiConnectFactory",
    "KalshiOrderBookState",
    "KalshiSubscriptionAck",
    "KalshiTickerUpdate",
    "KalshiWebSocketError",
    "RawFrameCallback",
    "apply_kalshi_orderbook_message",
    "kalshi_public_subscription_payload",
    "kalshi_update_subscription_payload",
    "decode_kalshi_messages",
    "kalshi_orderbook_subscription_payload",
    "normalize_market_tickers",
    "parse_kalshi_subscription_ack",
    "parse_kalshi_ticker_update",
    "parse_kalshi_websocket_error",
]
