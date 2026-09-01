from __future__ import annotations

import asyncio
import contextlib
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
    Collection,
    Iterable,
    Literal,
    Sequence,
)

import websockets
from websockets.exceptions import ConnectionClosed

from pmkt.config import get_config
from pmkt.exchanges.ws_transport import (
    WS_TRANSPORT_LIMITS,  # noqa: F401 - legacy module re-export
    WebSocketTransportSettings,
    is_transport_teardown_race,
)
from pmkt.data.time import isoformat_source_timestamp
from pmkt.data.time import timestamp_seconds as _timestamp_seconds
from pmkt.data.types import parse_float as _parse_float

logger = logging.getLogger(__name__)

MARKET_CHANNEL_TYPE = "market"
POLYMARKET_MAX_DECIMAL_ASSET_ID_DIGITS = 90
DEFAULT_HEARTBEAT_SECONDS = 10.0
DEFAULT_RECONNECT_ATTEMPTS = 3
DEFAULT_RECONNECT_BACKOFF_SECONDS = 0.5
POLYMARKET_BOOK_EVENT_TYPES = frozenset({"book", "price_change"})

ConnectFactory = Callable[[str], Any | Awaitable[Any]]
SleepFunc = Callable[[float], Awaitable[None]]


def _connect_market_websocket(
    url: str,
    *,
    transport_settings: WebSocketTransportSettings | None = None,
) -> Any:
    # Polymarket documents an application-level text PING/PONG heartbeat.  The
    # endpoint doesn't reliably answer websockets' separate RFC keepalive, so
    # running both can close an otherwise healthy stream with a ping timeout.
    settings = transport_settings or WebSocketTransportSettings()
    return websockets.connect(
        url,
        ping_interval=None,
        max_size=settings.max_size_bytes,
        max_queue=settings.max_queue_frames,
    )


class WebSocketProtocolError(RuntimeError):
    """Raised when the websocket client cannot form a valid protocol request."""


def _normalize_asset_ids(asset_ids: str | Iterable[str]) -> list[str]:
    if isinstance(asset_ids, str):
        ids = [asset_ids]
    else:
        ids = [str(asset_id) for asset_id in asset_ids if asset_id is not None]
    cleaned: list[str] = []
    seen: set[str] = set()
    for asset_id in ids:
        token = str(asset_id).strip()
        if not token or token in seen:
            continue
        if token.isdigit() and len(token) > POLYMARKET_MAX_DECIMAL_ASSET_ID_DIGITS:
            raise ValueError(
                "Polymarket CLOB asset/token id looks too long to be a single token id "
                f"({len(token)} decimal digits)."
            )
        if token.startswith("[") or token.startswith("(") or "," in token:
            raise ValueError(
                "Polymarket CLOB asset/token id must be one token, not a serialized list."
            )
        seen.add(token)
        cleaned.append(token)
    if not cleaned:
        raise ValueError("At least one CLOB asset/token id is required.")
    return cleaned


def market_subscription_payload(
    asset_ids: str | Iterable[str],
    *,
    custom_feature_enabled: bool = True,
) -> dict[str, Any]:
    """Build the initial market-channel subscription payload."""
    return {
        "assets_ids": _normalize_asset_ids(asset_ids),
        "type": MARKET_CHANNEL_TYPE,
        "custom_feature_enabled": bool(custom_feature_enabled),
    }


def market_operation_payload(
    operation: Literal["subscribe", "unsubscribe"],
    asset_ids: str | Iterable[str],
    *,
    custom_feature_enabled: bool | None = None,
) -> dict[str, Any]:
    """Build a dynamic market-channel subscription update payload."""
    payload: dict[str, Any] = {
        "assets_ids": _normalize_asset_ids(asset_ids),
        "operation": operation,
    }
    if custom_feature_enabled is not None and operation == "subscribe":
        payload["custom_feature_enabled"] = bool(custom_feature_enabled)
    return payload


def decode_market_messages(raw: Any) -> list[dict[str, Any]]:
    """Decode a raw websocket frame into market message dictionaries."""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        frame = raw.strip()
        if not frame or frame.upper() in {"PING", "PONG"}:
            return []
        try:
            decoded = json.loads(frame)
        except json.JSONDecodeError:
            logger.warning("Failed to parse websocket message: %s", raw)
            return []
    else:
        decoded = raw

    if isinstance(decoded, dict):
        return [decoded]
    if isinstance(decoded, list):
        return [item for item in decoded if isinstance(item, dict)]
    return []


def _levels_to_map(levels: Any) -> dict[float, float]:
    parsed: dict[float, float] = {}
    if not isinstance(levels, list):
        return parsed
    for level in levels:
        if isinstance(level, dict):
            price = _parse_float(level.get("price"))
            size = _parse_float(level.get("size"))
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price = _parse_float(level[0])
            size = _parse_float(level[1])
        else:
            continue
        if price is None or size is None or size <= 0:
            continue
        parsed[price] = size
    return parsed


def _previous_hash(change: dict[str, Any], parent: dict[str, Any]) -> str | None:
    for payload in (change, parent):
        for key in ("previous_hash", "prev_hash", "old_hash", "previous_book_hash"):
            value = payload.get(key)
            if value is not None:
                return str(value)
    return None


@dataclass
class MarketStreamSnapshot:
    asset_id: str
    market: str | None = None
    event_type: str | None = None
    timestamp: Any = None
    best_bid: float | None = None
    best_ask: float | None = None
    best_bid_size: float | None = None
    best_ask_size: float | None = None
    bid_depth: int = 0
    ask_depth: int = 0
    last_trade_price: float | None = None
    last_trade_size: float | None = None
    last_trade_side: str | None = None
    tick_size: float | None = None
    valid_state: bool = False
    quality_flags: tuple[str, ...] = ()
    initial_snapshot_received: bool = False
    last_book_hash: str | None = None
    quote_age_ms: int | None = None
    reconnect_count: int = 0

    @property
    def spread(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def midpoint(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0

    @property
    def timestamp_seconds(self) -> float | None:
        return _timestamp_seconds(self.timestamp, unit="milliseconds")

    @property
    def datetime_utc(self) -> str | None:
        return isoformat_source_timestamp(
            self.timestamp, epoch_unit="milliseconds"
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "market": self.market,
            "event_type": self.event_type,
            "timestamp": self.timestamp,
            "timestamp_seconds": self.timestamp_seconds,
            "datetime_utc": self.datetime_utc,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "best_bid_size": self.best_bid_size,
            "best_ask_size": self.best_ask_size,
            "spread": self.spread,
            "midpoint": self.midpoint,
            "bid_depth": self.bid_depth,
            "ask_depth": self.ask_depth,
            "last_trade_price": self.last_trade_price,
            "last_trade_size": self.last_trade_size,
            "last_trade_side": self.last_trade_side,
            "tick_size": self.tick_size,
            "valid_state": self.valid_state,
            "quality_flags": list(self.quality_flags),
            "initial_snapshot_received": self.initial_snapshot_received,
            "last_book_hash": self.last_book_hash,
            "quote_age_ms": self.quote_age_ms,
            "reconnect_count": self.reconnect_count,
        }


@dataclass
class MarketBookState:
    """Mutable in-memory state for one CLOB asset/token stream."""

    asset_id: str
    market: str | None = None
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    best_bid: float | None = None
    best_ask: float | None = None
    last_trade_price: float | None = None
    last_trade_size: float | None = None
    last_trade_side: str | None = None
    tick_size: float | None = None
    timestamp: Any = None
    last_event_type: str | None = None
    initial_snapshot_received: bool = False
    valid_state: bool = False
    quality_flags: set[str] = field(default_factory=lambda: {"no_initial_snapshot"})
    last_hash: str | None = None
    last_received_at_monotonic_ns: int | None = None
    reconnect_count: int = 0

    def snapshot(self, *, event_type: str | None = None) -> MarketStreamSnapshot:
        quote_age_ms = None
        if self.last_received_at_monotonic_ns is not None:
            quote_age_ms = (
                time.monotonic_ns() - self.last_received_at_monotonic_ns
            ) // 1_000_000
        return MarketStreamSnapshot(
            asset_id=self.asset_id,
            market=self.market,
            event_type=event_type or self.last_event_type,
            timestamp=self.timestamp,
            best_bid=self.best_bid,
            best_ask=self.best_ask,
            best_bid_size=(
                self.bids.get(self.best_bid) if self.best_bid is not None else None
            ),
            best_ask_size=(
                self.asks.get(self.best_ask) if self.best_ask is not None else None
            ),
            bid_depth=len(self.bids),
            ask_depth=len(self.asks),
            last_trade_price=self.last_trade_price,
            last_trade_size=self.last_trade_size,
            last_trade_side=self.last_trade_side,
            tick_size=self.tick_size,
            valid_state=self.valid_state,
            quality_flags=tuple(sorted(self.quality_flags)),
            initial_snapshot_received=self.initial_snapshot_received,
            last_book_hash=self.last_hash,
            quote_age_ms=quote_age_ms,
            reconnect_count=self.reconnect_count,
        )

    def apply_book(self, message: dict[str, Any]) -> MarketStreamSnapshot:
        self.market = str(message.get("market") or self.market or "")
        self.bids = _levels_to_map(message.get("bids"))
        self.asks = _levels_to_map(message.get("asks"))
        self.timestamp = message.get("timestamp")
        self.last_hash = (
            str(message["hash"]) if message.get("hash") is not None else self.last_hash
        )
        self.last_received_at_monotonic_ns = time.monotonic_ns()
        self.initial_snapshot_received = True
        self.last_event_type = "book"
        self._recompute_best_prices()
        self._refresh_validity()
        return self.snapshot(event_type="book")

    def apply_price_change(
        self,
        change: dict[str, Any],
        parent: dict[str, Any],
    ) -> MarketStreamSnapshot:
        self.market = str(parent.get("market") or self.market or "")
        self.timestamp = (
            parent.get("timestamp")
            if parent.get("timestamp") is not None
            else change.get("timestamp")
        )
        self.last_received_at_monotonic_ns = time.monotonic_ns()
        self.last_event_type = "price_change"
        if not self.initial_snapshot_received:
            self.valid_state = False
            self.quality_flags.update({"delta_before_snapshot", "no_initial_snapshot"})
        side = str(change.get("side") or "").upper()
        price = _parse_float(change.get("price"))
        size = _parse_float(change.get("size"))
        if price is not None and size is not None:
            self._set_level(side, price, size)
        previous_hash = _previous_hash(change, parent)
        if (
            previous_hash is not None
            and self.last_hash is not None
            and previous_hash != self.last_hash
        ):
            self.valid_state = False
            self.quality_flags.add("hash_mismatch")
        if change.get("hash") is not None:
            self.last_hash = str(change["hash"])
        self._refresh_validity(preserve=self._persistent_flags())
        return self.snapshot(event_type="price_change")

    def apply_best_bid_ask(self, message: dict[str, Any]) -> MarketStreamSnapshot:
        self.market = str(message.get("market") or self.market or "")
        self.timestamp = message.get("timestamp")
        self.last_received_at_monotonic_ns = time.monotonic_ns()
        self.last_event_type = "best_bid_ask"
        self._apply_best_fields(message)
        self._refresh_validity(preserve=self._persistent_flags())
        return self.snapshot(event_type="best_bid_ask")

    def apply_last_trade(self, message: dict[str, Any]) -> MarketStreamSnapshot:
        self.market = str(message.get("market") or self.market or "")
        self.timestamp = message.get("timestamp")
        self.last_received_at_monotonic_ns = time.monotonic_ns()
        self.last_event_type = "last_trade_price"
        self.last_trade_price = _parse_float(message.get("price"))
        self.last_trade_size = _parse_float(message.get("size"))
        side = message.get("side")
        self.last_trade_side = str(side) if side is not None else None
        self._refresh_validity(preserve=self._persistent_flags())
        return self.snapshot(event_type="last_trade_price")

    def apply_tick_size_change(self, message: dict[str, Any]) -> MarketStreamSnapshot:
        self.market = str(message.get("market") or self.market or "")
        self.timestamp = message.get("timestamp")
        self.last_received_at_monotonic_ns = time.monotonic_ns()
        self.last_event_type = "tick_size_change"
        self.tick_size = _parse_float(message.get("new_tick_size"))
        self._refresh_validity(preserve=self._persistent_flags())
        return self.snapshot(event_type="tick_size_change")

    def apply_lifecycle(
        self, message: dict[str, Any], event_type: str
    ) -> MarketStreamSnapshot:
        self.market = str(message.get("market") or self.market or "")
        self.timestamp = message.get("timestamp")
        self.last_received_at_monotonic_ns = time.monotonic_ns()
        self.last_event_type = event_type
        if event_type == "market_resolved":
            self.valid_state = False
            self.quality_flags.add("market_resolved")
        elif event_type == "new_market":
            self.quality_flags.add("new_market")
            self._refresh_validity(preserve=self._persistent_flags() | {"new_market"})
        return self.snapshot(event_type=event_type)

    def mark_reconnect(self) -> None:
        self.reconnect_count += 1
        self.bids.clear()
        self.asks.clear()
        self.best_bid = None
        self.best_ask = None
        self.valid_state = False
        self.initial_snapshot_received = False
        self.quality_flags.update({"reconnect", "no_initial_snapshot"})

    def _set_level(self, side: str, price: float, size: float) -> None:
        if side in {"BUY", "BID", "BIDS"}:
            book_side = self.bids
        elif side in {"SELL", "ASK", "ASKS"}:
            book_side = self.asks
        else:
            return
        if size <= 0:
            book_side.pop(price, None)
        else:
            book_side[price] = size
        self._recompute_best_prices()

    def _apply_best_fields(self, payload: dict[str, Any]) -> None:
        best_bid = _parse_float(payload.get("best_bid"))
        best_ask = _parse_float(payload.get("best_ask"))
        if best_bid is not None:
            self.best_bid = best_bid
        if best_ask is not None:
            self.best_ask = best_ask

    def _recompute_best_prices(self) -> None:
        self.best_bid = max(self.bids) if self.bids else None
        self.best_ask = min(self.asks) if self.asks else None

    def _persistent_flags(self) -> set[str]:
        persistent = {
            "delta_before_snapshot",
            "hash_mismatch",
            "market_resolved",
            "no_initial_snapshot",
            "new_market",
            "reconnect",
        }
        return self.quality_flags.intersection(persistent)

    def _refresh_validity(self, *, preserve: set[str] | None = None) -> None:
        flags = set(preserve or set())
        if not self.initial_snapshot_received:
            flags.add("no_initial_snapshot")
        if self.best_bid is None:
            flags.add("empty_bid")
        if self.best_ask is None:
            flags.add("empty_ask")
        if self.best_bid is not None and self.best_ask is not None:
            if self.best_ask < self.best_bid:
                flags.update({"crossed_book", "negative_spread"})
            elif self.best_ask == self.best_bid:
                flags.add("crossed_book")
        self.quality_flags = flags
        self.valid_state = self.initial_snapshot_received and not flags


def _state_for(
    states: dict[str, MarketBookState],
    asset_id: Any,
) -> MarketBookState | None:
    if asset_id is None:
        return None
    token = str(asset_id)
    if not token:
        return None
    return states.setdefault(token, MarketBookState(asset_id=token))


def _states_for_lifecycle(
    states: dict[str, MarketBookState],
    message: dict[str, Any],
) -> list[MarketBookState]:
    asset_id = message.get("asset_id")
    state = _state_for(states, asset_id)
    if state is not None:
        return [state]
    market = message.get("market")
    if market is None:
        return []
    market_text = str(market)
    return [state for state in states.values() if state.market == market_text]


def apply_market_message(
    states: dict[str, MarketBookState],
    message: dict[str, Any],
    *,
    allowed_asset_ids: Collection[str] | None = None,
) -> list[MarketStreamSnapshot]:
    """Apply only state-bearing market messages and return updated snapshots.

    Trade and lifecycle messages are observation evidence. They deliberately return
    no snapshots and cannot mutate local book state through this dispatcher.
    """
    event_type = str(message.get("event_type") or message.get("type") or "")
    if event_type not in POLYMARKET_BOOK_EVENT_TYPES:
        return []

    if event_type == "book":
        asset_id = message.get("asset_id")
        if (
            allowed_asset_ids is not None
            and str(asset_id or "") not in allowed_asset_ids
        ):
            return []
        state = _state_for(states, asset_id)
        return [state.apply_book(message)] if state is not None else []

    if event_type == "price_change":
        snapshots_by_asset: dict[str, MarketStreamSnapshot] = {}
        changes = message.get("price_changes")
        if not isinstance(changes, list):
            return []
        for change in changes:
            if not isinstance(change, dict):
                continue
            asset_id = change.get("asset_id")
            if (
                allowed_asset_ids is not None
                and str(asset_id or "") not in allowed_asset_ids
            ):
                continue
            state = _state_for(states, asset_id)
            if state is not None:
                snapshots_by_asset[state.asset_id] = state.apply_price_change(
                    change, message
                )
        return list(snapshots_by_asset.values())

    return []


class AsyncMarketWebSocketClient:
    """Polymarket CLOB public market websocket client."""

    def __init__(
        self,
        asset_ids: Sequence[str] | None = None,
        *,
        ws_url: str | None = None,
        custom_feature_enabled: bool = True,
        heartbeat_interval: float | None = DEFAULT_HEARTBEAT_SECONDS,
        connect_factory: ConnectFactory | None = None,
        transport_settings: WebSocketTransportSettings | None = None,
        sleep: SleepFunc = asyncio.sleep,
        on_subscription_start: Callable[[], None] | None = None,
        on_subscription_established: Callable[[str, str], None] | None = None,
    ) -> None:
        self.ws_url = ws_url or get_config().clob_ws_url
        self.asset_ids = _normalize_asset_ids(asset_ids) if asset_ids else []
        self.custom_feature_enabled = bool(custom_feature_enabled)
        self.heartbeat_interval = heartbeat_interval
        self.transport_settings = transport_settings or WebSocketTransportSettings()
        self._connect_factory = connect_factory or (
            lambda url: _connect_market_websocket(
                url, transport_settings=self.transport_settings
            )
        )
        self._sleep = sleep
        self._on_subscription_start = on_subscription_start
        self._on_subscription_established = on_subscription_established
        # Application-dequeue telemetry. These describe when the websocket
        # iterator yielded a raw frame, before decoding its individual messages.
        # They are not transport/socket-receipt timestamps. Kept separate from
        # the tape's received_at, whose CR-18 authority semantics are unchanged.
        self.last_frame_received_at_utc: str | None = None
        self.last_frame_received_monotonic_ns: int | None = None
        self.last_frame_sequence: int = 0
        self.last_message_index_in_frame: int = 0
        self._ws: Any | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._heartbeat_error: BaseException | None = None

    @property
    def is_connected(self) -> bool:
        if self._ws is None:
            return False
        return not bool(getattr(self._ws, "closed", False))

    async def connect(self) -> None:
        if self.is_connected:
            return
        maybe_ws = self._connect_factory(self.ws_url)
        self._ws = await maybe_ws if inspect.isawaitable(maybe_ws) else maybe_ws
        self._heartbeat_error = None
        if self.asset_ids:
            if self._on_subscription_start is not None:
                self._on_subscription_start()
            sent_at_utc = (
                isoformat_source_timestamp(time.time(), epoch_unit="seconds") or ""
            )
            await self._send_json(
                market_subscription_payload(
                    self.asset_ids,
                    custom_feature_enabled=self.custom_feature_enabled,
                )
            )
            if self._on_subscription_established is not None:
                self._on_subscription_established(
                    sent_at_utc,
                    isoformat_source_timestamp(time.time(), epoch_unit="seconds")
                    or sent_at_utc,
                )
        self._start_heartbeat()
        logger.info("Connected to Polymarket market websocket at %s", self.ws_url)

    async def close(self) -> None:
        await self._stop_heartbeat()
        if self._ws is not None:
            await self._ws.close()
            self._ws = None

    async def reconnect(self) -> None:
        await self.close()
        await self.connect()

    async def __aenter__(self) -> "AsyncMarketWebSocketClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def subscribe(
        self,
        asset_ids: str | Iterable[str],
        *,
        custom_feature_enabled: bool | None = None,
    ) -> None:
        ids = _normalize_asset_ids(asset_ids)
        current = set(self.asset_ids)
        self.asset_ids.extend(asset_id for asset_id in ids if asset_id not in current)
        if not self.is_connected:
            await self.connect()
            return
        await self._send_json(
            market_operation_payload(
                "subscribe",
                ids,
                custom_feature_enabled=(
                    self.custom_feature_enabled
                    if custom_feature_enabled is None
                    else custom_feature_enabled
                ),
            )
        )

    async def unsubscribe(self, asset_ids: str | Iterable[str]) -> None:
        ids = _normalize_asset_ids(asset_ids)
        remove = set(ids)
        self.asset_ids = [
            asset_id for asset_id in self.asset_ids if asset_id not in remove
        ]
        if self.is_connected:
            await self._send_json(market_operation_payload("unsubscribe", ids))

    async def ping(self) -> None:
        if not self.is_connected:
            raise RuntimeError("WebSocket is not connected.")
        ws = self._ws
        if ws is None:
            raise RuntimeError("WebSocket is not connected.")
        await ws.send("PING")

    async def iter_messages(
        self,
        *,
        reconnect: bool = True,
        max_reconnects: int = DEFAULT_RECONNECT_ATTEMPTS,
        reconnect_backoff: float = DEFAULT_RECONNECT_BACKOFF_SECONDS,
        on_reconnect: Callable[[], None] | None = None,
        fail_on_clean_close_exhausted: bool = False,
    ) -> AsyncIterator[dict[str, Any]]:
        if not self.is_connected:
            await self.connect()
        reconnects = 0
        while True:
            if not self.is_connected:
                if self._heartbeat_error is None:
                    return
                if not reconnect or reconnects >= max_reconnects:
                    error = self._heartbeat_error
                    self._heartbeat_error = None
                    raise error
                reconnects += 1
                if on_reconnect is not None:
                    on_reconnect()
                await self._sleep(reconnect_backoff * reconnects)
                await self.reconnect()
                continue
            ws = self._ws
            if ws is None:
                return
            try:
                async for raw in ws:
                    # Stamp application dequeue BEFORE decoding. Messages 2..N
                    # of a frame would otherwise inherit processing time spent
                    # on their predecessors, including synchronous commits.
                    # This does not claim transport receipt timing.
                    self.last_frame_received_at_utc = isoformat_source_timestamp(
                        time.time(), epoch_unit="seconds"
                    )
                    self.last_frame_received_monotonic_ns = time.monotonic_ns()
                    self.last_frame_sequence += 1
                    for index, message in enumerate(decode_market_messages(raw)):
                        self.last_message_index_in_frame = index
                        yield message
                if self._heartbeat_error is not None:
                    if not reconnect or reconnects >= max_reconnects:
                        error = self._heartbeat_error
                        self._heartbeat_error = None
                        raise error
                    reconnects += 1
                    if on_reconnect is not None:
                        on_reconnect()
                    await self._sleep(reconnect_backoff * reconnects)
                    await self.reconnect()
                    continue
                # A remote peer may finish the iterator with a normal close
                # frame.  For an operational stream that is still a transport
                # disconnect, not a successful capture boundary.  Retry it
                # under the same explicit reconnect budget used for errors.
                if not reconnect:
                    return
                if reconnects >= max_reconnects:
                    if fail_on_clean_close_exhausted:
                        raise ConnectionError(
                            "Polymarket websocket closed cleanly and exhausted the "
                            "reconnect budget"
                        )
                    return
                reconnects += 1
                if on_reconnect is not None:
                    on_reconnect()
                await self._sleep(reconnect_backoff * reconnects)
                await self.reconnect()
                continue
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

    def _start_heartbeat(self) -> None:
        if self.heartbeat_interval is None or self.heartbeat_interval <= 0:
            return
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            return
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _stop_heartbeat(self) -> None:
        task = self._heartbeat_task
        self._heartbeat_task = None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _heartbeat_loop(self) -> None:
        interval = self.heartbeat_interval
        if interval is None or interval <= 0:
            return
        try:
            while self.is_connected:
                await self._sleep(float(interval))
                if self.is_connected:
                    await self.ping()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Polymarket websocket heartbeat failed: %s", exc)
            self._heartbeat_error = exc
            ws = self._ws
            self._ws = None
            if ws is not None:
                with contextlib.suppress(Exception):
                    await ws.close()


async def collect_market_snapshots(
    asset_ids: Sequence[str],
    *,
    max_updates: int = 30,
    timeout_seconds: float = 30.0,
    ws_url: str | None = None,
    custom_feature_enabled: bool = True,
    heartbeat_interval: float | None = DEFAULT_HEARTBEAT_SECONDS,
    reconnect_attempts: int = DEFAULT_RECONNECT_ATTEMPTS,
    connect_factory: ConnectFactory | None = None,
) -> list[dict[str, Any]]:
    """Collect a bounded set of market websocket snapshots."""
    normalized_ids = _normalize_asset_ids(asset_ids)
    states = {
        asset_id: MarketBookState(asset_id=asset_id) for asset_id in normalized_ids
    }
    snapshots: list[dict[str, Any]] = []
    deadline = time.monotonic() + float(timeout_seconds)

    async with AsyncMarketWebSocketClient(
        normalized_ids,
        ws_url=ws_url,
        custom_feature_enabled=custom_feature_enabled,
        heartbeat_interval=heartbeat_interval,
        connect_factory=connect_factory,
    ) as client:

        def mark_reconnects() -> None:
            for state in states.values():
                state.mark_reconnect()

        iterator = client.iter_messages(
            max_reconnects=reconnect_attempts,
            on_reconnect=mark_reconnects,
        )
        while len(snapshots) < max_updates:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                message = await asyncio.wait_for(
                    iterator.__anext__(), timeout=remaining
                )
            except (StopAsyncIteration, asyncio.TimeoutError):
                break
            for snapshot in apply_market_message(states, message):
                if snapshot.asset_id not in normalized_ids:
                    continue
                snapshots.append(snapshot.as_dict())
                if len(snapshots) >= max_updates:
                    break

    return snapshots


AsyncWebSocketClient = AsyncMarketWebSocketClient

__all__ = [
    "AsyncMarketWebSocketClient",
    "AsyncWebSocketClient",
    "MarketBookState",
    "MarketStreamSnapshot",
    "POLYMARKET_BOOK_EVENT_TYPES",
    "WebSocketProtocolError",
    "apply_market_message",
    "collect_market_snapshots",
    "decode_market_messages",
    "market_operation_payload",
    "market_subscription_payload",
]
