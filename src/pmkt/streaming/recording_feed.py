"""Venue adapters and one bounded recording loop; no storage profiles or tape."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import asdict
import json
import math
from pathlib import Path
import time
from typing import Any, Callable, Sequence, cast

import pmkt
from pmkt import _native
from pmkt.provenance import implementation_identity
from pmkt.data.time import isoformat_source_timestamp
from pmkt.exchanges._ws_book import encode_message
from pmkt.exchanges.kalshi.ws import (
    AsyncKalshiWebSocketClient,
    normalize_market_tickers,
)
from pmkt.exchanges.polymarket.ws import (
    AsyncMarketWebSocketClient,
    _UNSET_HEARTBEAT_INTERVAL,
    _normalize_asset_ids,
)
from pmkt.exchanges.ws_transport import (
    WebSocketDeadlineExceeded,
    WebSocketMessagePump,
    WebSocketRetryBudget,
    WebSocketTransportSettings,
)
from pmkt.streaming.trades import recording_trade
from pmkt.streaming.recording import (
    DEFAULT_RECORDING_DURATION_S,
    BookRecorder,
    BookView,
    RecordingOptions,
)


class RecordingError(RuntimeError):
    """A failed recording retains committed SQLite evidence in run_dir."""

    def __init__(self, message: str, run_dir: Path) -> None:
        super().__init__(message)
        self.run_dir = run_dir


class VenueBooks:
    def __init__(self, venue: str, instruments: list[str]) -> None:
        self.venue = venue
        self.native = _native.RecordingBooks(venue, instruments)

    def reset(self) -> None:
        self.native.reset()

    def apply(self, message: dict[str, Any], count: int) -> list[BookView]:
        try:
            updates = self.native.apply(encode_message(message), count)
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc
        views = []
        for book in updates:
            ticker = book["instrument"]
            kalshi = self.venue == "kalshi"
            ts = book["timestamp_json"]
            views.append(BookView(
                ticker + ":YES" if kalshi else ticker,
                {float(p): float(q) for p, q in book["first"]},
                {float(p): float(q) for p, q in book["second"]},
                book["initialized"], book["intact"], book["valid"], tuple(book["flags"]),
                tick_size=float(book["tick"]) if book["tick"] is not None else None,
                minimum_order_size=float(book["minimum"]) if book["minimum"] is not None else None,
                venue_time_utc=isoformat_source_timestamp(
                    json.loads(ts) if ts is not None else None,
                    epoch_unit="milliseconds" if book["milliseconds"] else "seconds",
                ),
                venue_market_id=ticker if kalshi else book["market"],
                source_book_update=book["source_update"],
            ))
        return views

    def recovery_due(self, count: int) -> bool:
        return self.native.recovery_due(count, time.monotonic_ns())


async def record_feed(
    instruments: Sequence[str],
    *,
    venue: str,
    output_root: str | Path,
    options: RecordingOptions,
    run_name: str | None = None,
    duration_s: float = DEFAULT_RECORDING_DURATION_S,
    max_messages: int | None = None,
    max_reconnects: int | None = None,
    ws_url: str | None = None,
    connect_factory: Callable[..., Any] | None = None,
    auth: Any = None,
    heartbeat_interval: float | None = cast('float | None', _UNSET_HEARTBEAT_INTERVAL),
    custom_feature_enabled: bool = True,
    websocket_max_size_bytes: int | None = None,
    websocket_max_queue_frames: int | None = None,
) -> dict[str, Any]:
    """Record with a finite retry budget inherited when max_reconnects is None.

    Omitted heartbeat_interval inherits the venue default; explicit None disables it.
    """
    ids = (
        _normalize_asset_ids(instruments)
        if venue == "polymarket"
        else normalize_market_tickers(instruments)
    )
    if not math.isfinite(duration_s) or (duration_s <= 0 and max_messages is None):
        raise ValueError(
            "duration_s must be finite and positive unless max_messages is set"
        )
    if max_messages is not None and (
        isinstance(max_messages, bool)
        or not isinstance(max_messages, int)
        or max_messages < 1
    ):
        raise ValueError("max_messages must be a positive integer")
    if max_reconnects is not None and (
        isinstance(max_reconnects, bool)
        or not isinstance(max_reconnects, int)
        or max_reconnects < 0
    ):
        raise ValueError("max_reconnects must be a nonnegative integer")
    retry_overrides: dict[str, Any] = {}
    if max_reconnects is not None:
        retry_overrides["max_reconnects"] = max_reconnects
    budget = WebSocketRetryBudget(**retry_overrides)
    settings = WebSocketTransportSettings(
        max_size_bytes=websocket_max_size_bytes
        if websocket_max_size_bytes is not None
        else WebSocketTransportSettings().max_size_bytes,
        max_queue_frames=websocket_max_queue_frames
        if websocket_max_queue_frames is not None
        else WebSocketTransportSettings().max_queue_frames,
    )
    books = VenueBooks(venue, ids)
    recorder = BookRecorder(
        venue=venue,
        instruments=ids if venue == "polymarket" else [i + ":YES" for i in ids],
        output_root=output_root,
        options=options,
        run_name=run_name,
        provenance={
            "implementation": implementation_identity(pmkt.__file__, "pmkt").as_dict(),
            "transport": asdict(settings),
            "max_reconnects": budget.max_reconnects,
            "duration_s": duration_s,
            "max_messages": max_messages,
            "adapter": {
                "venue": venue,
                "kalshi_price_basis": "YES" if venue == "kalshi" else None,
                "custom_feature_enabled": custom_feature_enabled
                if venue == "polymarket"
                else None,
            },
        },
    )
    deadline = time.monotonic() + duration_s if duration_s > 0 else None

    def invalidate(reason: str) -> None:
        # Connection-level invalidation must retain terminal resolution evidence.
        for instrument, book in list(recorder.books.items()):
            flags = (reason, "market_resolved") if books.native.resolved(instrument) else (reason,)
            recorder.observe_books([BookView(
                instrument, {}, {}, False, False, False, flags,
                venue_market_id=book.venue_market_id,
            )], source=None)
        recorder.event(reason, {})

    def disconnected() -> None:
        invalidate("disconnect")
        books.reset()

    budget.deadline = deadline
    budget.on_reconnect = disconnected
    budget.on_retry = lambda details: recorder.event("reconnect_attempt", details)
    shared: dict[str, Any] = {
        "ws_url": ws_url,
        "connect_factory": connect_factory,
        "transport_settings": settings,
        "retry_budget": budget,
        "on_subscription_start": recorder.subscription_start,
        "on_subscription_established": lambda sent, completed: recorder.event(
            "subscription_sent", {"sent_at_utc": sent, "completed_at_utc": completed}
        ),
    }
    client: Any
    if venue == "polymarket":
        client_overrides: dict[str, Any] = {}
        if heartbeat_interval is not _UNSET_HEARTBEAT_INTERVAL:
            client_overrides["heartbeat_interval"] = heartbeat_interval
        client = AsyncMarketWebSocketClient(
            ids,
            **client_overrides,
            custom_feature_enabled=custom_feature_enabled,
            **shared,
        )
    else:
        client = AsyncKalshiWebSocketClient(
            ids,
            auth=auth,
            use_yes_price=True,
            public_channels=("orderbook_delta", "trade", "market_lifecycle_v2"),
            **shared,
        )
    def decode_error(reason: str, payload: Any) -> None:
        # Do not persist raw frames when raw_messages is disabled.
        raise RuntimeError(f"invalid Kalshi frame: {reason}")

    def messages() -> Any:
        if venue == "polymarket":
            return client.iter_messages(
                fail_on_clean_close_exhausted=True, strict_decode=True,
                # Preserve the recorder's JSON-number representation while
                # validating frame structure strictly. Managed feeds keep Decimal.
                decimal_numbers=False,
            )
        return client.iter_messages(
            fail_on_clean_close_exhausted=True, on_decode_error=decode_error
        )

    iterator = messages()
    pump = WebSocketMessagePump(iterator)
    error: BaseException | None = None
    reason, normal = "interrupted", False
    try:
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                reason, normal = "duration", True
                break
            # Keep one receive task alive across timer checks. Cancelling an async
            # generator on every timer would lose its connection and queued data.
            wake = min(
                recorder.next_depth,
                recorder.next_liveness,
                time.monotonic() + 1.0,
                deadline or math.inf,
            )
            complement = books.native.next_deadline_ns()
            if complement is not None:
                wake = min(wake, complement / 1_000_000_000)
            received = await pump.wait(wake - time.monotonic())
            # Enforce the grace bound before a late corrective message can
            # erase the evidence that this connection needs a fresh snapshot.
            if books.native.complementary_due(
                count=recorder.source_sequence + int(received),
                now_ns=time.monotonic_ns(),
            ):
                await pump.close()
                await budget.run(
                    client.reconnect, retry_first=True, immediate_first=True
                )
                iterator = messages()
                pump = WebSocketMessagePump(iterator)
                continue
            if received:
                try:
                    message = pump.pop()
                except StopAsyncIteration:
                    reason = "feed_ended"
                    break
                source = recorder.message(message)
                recorder.observe_books(
                    books.apply(message, recorder.source_sequence), source=source
                )
                trade = recording_trade(venue, message)
                if trade is not None and trade["instrument_id"] in recorder.instruments:
                    native_id = trade["venue_trade_id"]
                    duplicate = (
                        native_id is not None
                        and recorder.store.connection.execute(
                            "SELECT 1 FROM trades WHERE venue=? AND venue_trade_id=?",
                            (venue, native_id),
                        ).fetchone()
                    )
                    if not duplicate:
                        recorder.append(
                            "trades",
                            {
                                **recorder.common(
                                    trade["instrument_id"], source=source
                                ),
                                **trade,
                            },
                        )
                kind = str(message.get("event_type") or message.get("type") or "")
                if kind in {
                    "market_resolved",
                    "new_market",
                    "tick_size_change",
                    "market_lifecycle_v2",
                    "subscribed",
                    "error",
                }:
                    recorder.event(kind, message, source=source)
                if kind == "error":
                    raise RuntimeError(
                        "venue reported a subscription error; see events"
                    )
                if (
                    max_messages is not None
                    and recorder.source_sequence >= max_messages
                ):
                    reason, normal = "max_messages", True
                    break
            transport = (
                client.heartbeat_diagnostics()
                if venue == "polymarket"
                else {
                    "connected": client.is_connected,
                    "last_frame_received_at_utc": client.last_frame_received_at_utc,
                }
            )
            recorder.tick(transport)
            if books.recovery_due(recorder.source_sequence):
                # A single bounded policy: obtain fresh subscriptions for the
                # connection. The existing Polymarket corrective-delta grace
                # avoids reconnecting between complementary updates.
                await pump.close()
                await budget.run(
                    client.reconnect, retry_first=True, immediate_first=True
                )
                iterator = messages()
                pump = WebSocketMessagePump(iterator)
    except WebSocketDeadlineExceeded:
        reason, normal = "duration", True
    except BaseException as exc:
        error = exc
        reason = (
            "cancelled" if isinstance(exc, asyncio.CancelledError) else "feed_error"
        )
        if not recorder.store.failed:
            invalidate("feed_interrupted")
    finally:
        with contextlib.suppress(Exception):
            await pump.close()
        # Take any terminal sample before our own socket teardown. A cancellation
        # during teardown is still recorded as cancellation and propagated.
        try:
            if normal and not recorder.store.failed:
                for book in recorder.books.values():
                    recorder.snapshot(book, ["stop"])
        except BaseException as exc:
            error, normal, reason = exc, False, "persistence_error"
        try:
            await client.close()
        except BaseException as exc:
            error, normal = exc, False
            reason = (
                "cancelled"
                if isinstance(exc, asyncio.CancelledError)
                else "close_error"
            )
        manifest = recorder.finish(reason=reason, error=error, normal=normal)
    if error is not None and not isinstance(error, Exception):
        raise error
    if manifest["status"] == "failed":
        raise RecordingError(
            recorder.error or "no intact books were recorded", recorder.run_dir
        ) from error
    return manifest
