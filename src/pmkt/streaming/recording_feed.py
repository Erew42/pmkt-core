"""Venue adapters and one bounded recording loop; no storage profiles or tape."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import asdict
import math
from pathlib import Path
import time
from typing import Any, Callable, Sequence

import pmkt
from pmkt.provenance import implementation_identity
from pmkt.data.time import isoformat_source_timestamp
from pmkt.exchanges.book_integrity import finite_number
from pmkt.exchanges.kalshi.ws import (
    AsyncKalshiWebSocketClient,
    KalshiOrderBookState,
    normalize_market_tickers,
    apply_kalshi_orderbook_message,
)
from pmkt.exchanges.polymarket.ws import (
    AsyncMarketWebSocketClient,
    MarketBookState,
    _normalize_asset_ids,
    apply_market_message,
)
from pmkt.exchanges.polymarket.recovery import ComplementaryDeltaRecovery
from pmkt.exchanges.ws_transport import (
    WebSocketDeadlineExceeded,
    WebSocketRetryBudget,
    WebSocketTransportSettings,
)
from pmkt.streaming.trades import recording_trade
from pmkt.streaming.recording import BookRecorder, BookView, RecordingOptions


class RecordingError(RuntimeError):
    """A failed recording retains committed SQLite evidence in run_dir."""

    def __init__(self, message: str, run_dir: Path) -> None:
        super().__init__(message)
        self.run_dir = run_dir


class VenueBooks:
    def __init__(self, venue: str, instruments: list[str]) -> None:
        self.venue = venue
        self.polymarket = (
            {i: MarketBookState(i) for i in instruments}
            if venue == "polymarket"
            else {}
        )
        self.kalshi = (
            {i: KalshiOrderBookState(i, use_yes_price=True) for i in instruments}
            if venue == "kalshi"
            else {}
        )
        self.sequences: dict[int, int] = {}
        self.venue_times: dict[str, str | None] = {}
        self.minimum_sizes: dict[str, float | None] = {}
        self.complementary = ComplementaryDeltaRecovery()

    def reset(self) -> None:
        for poly in self.polymarket.values():
            poly.mark_reconnect()
        for kalshi in self.kalshi.values():
            kalshi.mark_reconnect()
        self.sequences.clear()
        self.venue_times.clear()
        self.complementary.clear()

    def apply(self, message: dict[str, Any], count: int) -> list[BookView]:
        if self.venue == "polymarket":
            previous = self.complementary.intact_changed_assets(
                message, self.polymarket
            )
            snapshots = apply_market_message(
                self.polymarket, message, allowed_asset_ids=self.polymarket
            )
            kind = message.get("event_type") or message.get("type")
            if (
                kind == "tick_size_change"
                and str(message.get("asset_id")) in self.polymarket
            ):
                snapshots = [
                    self.polymarket[str(message["asset_id"])].apply_tick_size_change(
                        message
                    )
                ]
            elif kind in {"new_market", "market_resolved"}:
                targets = [
                    state
                    for key, state in self.polymarket.items()
                    if key == str(message.get("asset_id"))
                    or (
                        message.get("asset_id") is None
                        and message.get("market") is not None
                        and state.market == str(message["market"])
                    )
                ]
                snapshots = [
                    state.apply_lifecycle(message, str(kind)) for state in targets
                ]
            self.complementary.observe(
                message, self.polymarket, previous, message_count=count
            )
            views = []
            for snapshot in snapshots:
                state = self.polymarket[snapshot.asset_id]
                if kind == "book":
                    for field in ("tick_size", "min_order_size"):
                        if message.get(field) is not None:
                            value = finite_number(message[field])
                            if value is None or value <= 0:
                                state.quality_flags.add("malformed_book")
                                state.valid_state = False
                            elif field == "tick_size":
                                state.tick_size = value
                            else:
                                self.minimum_sizes[state.asset_id] = value
                views.append(
                    BookView(
                        state.asset_id,
                        state.bids,
                        state.asks,
                        state.initial_snapshot_received,
                        state.book_integrity_valid,
                        state.valid_state,
                        tuple(sorted(state.quality_flags)),
                        tick_size=state.tick_size,
                        minimum_order_size=self.minimum_sizes.get(state.asset_id),
                        venue_time_utc=isoformat_source_timestamp(
                            state.timestamp, epoch_unit="milliseconds"
                        ),
                        venue_market_id=state.market,
                    )
                )
            return views
        payload = message.get("msg", message)
        ticker = str(payload.get("market_ticker") or "")
        if (
            message.get("type") not in {"orderbook_snapshot", "orderbook_delta"}
            or ticker not in self.kalshi
        ):
            return []
        sid, seq = message.get("sid"), message.get("seq")
        gap = not isinstance(sid, int) or not isinstance(seq, int)
        if isinstance(sid, int) and isinstance(seq, int):
            gap = sid in self.sequences and seq != self.sequences[sid] + 1
            self.sequences[sid] = seq
        apply_kalshi_orderbook_message(self.kalshi, message, use_yes_price=True)
        self.venue_times[ticker] = isoformat_source_timestamp(
            self.kalshi[ticker].timestamp,
            epoch_unit="milliseconds" if "ts_ms" in payload else "seconds",
        )
        touched = {ticker}
        if gap:
            for other, sibling in self.kalshi.items():
                if sid is None or sibling.sid == sid:
                    sibling.mark_sequence_gap()
                    touched.add(other)
        views = []
        for key in sorted(touched):
            state_k = self.kalshi[key]
            views.append(
                BookView(
                    key + ":YES",
                    state_k.yes_bids,
                    state_k.no_bids,
                    state_k.initial_snapshot_received,
                    state_k.book_integrity_valid,
                    state_k.valid_state,
                    tuple(sorted(state_k.quality_flags)),
                    venue_time_utc=self.venue_times.get(key),
                    venue_market_id=key,
                    source_book_update=key == ticker,
                )
            )
        return views

    def recovery_due(self, count: int) -> bool:
        now = time.monotonic_ns()
        if self.venue == "polymarket":
            return any(
                state.initial_snapshot_received
                and not state.book_integrity_valid
                and not self.complementary.defer(asset, message_count=count, now_ns=now)
                for asset, state in self.polymarket.items()
            ) or self.complementary.recovery_due(message_count=count, now_ns=now)
        return any(
            state.initial_snapshot_received and not state.book_integrity_valid
            for state in self.kalshi.values()
        )


async def record_feed(
    instruments: Sequence[str],
    *,
    venue: str,
    output_root: str | Path,
    options: RecordingOptions,
    run_name: str | None = None,
    duration_s: float = 300.0,
    max_messages: int | None = None,
    max_reconnects: int = 3,
    ws_url: str | None = None,
    connect_factory: Callable[..., Any] | None = None,
    auth: Any = None,
    heartbeat_interval: float | None = 10.0,
    custom_feature_enabled: bool = True,
    websocket_max_size_bytes: int | None = None,
    websocket_max_queue_frames: int | None = None,
) -> dict[str, Any]:
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
    if (
        isinstance(max_reconnects, bool)
        or not isinstance(max_reconnects, int)
        or max_reconnects < 0
    ):
        raise ValueError("max_reconnects must be a nonnegative integer")
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
            "max_reconnects": max_reconnects,
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

    def disconnected() -> None:
        recorder.invalidate("disconnect")
        books.reset()

    budget = WebSocketRetryBudget(
        max_reconnects,
        deadline=deadline,
        on_reconnect=disconnected,
        on_retry=lambda details: recorder.event("reconnect_attempt", details),
    )
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
        client = AsyncMarketWebSocketClient(
            ids,
            heartbeat_interval=heartbeat_interval,
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
    iterator = client.iter_messages(fail_on_clean_close_exhausted=True)
    pending: asyncio.Task[Any] | None = None
    error: BaseException | None = None
    reason, normal = "interrupted", False
    try:
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                reason, normal = "duration", True
                break
            if pending is None:
                pending = asyncio.create_task(anext(iterator))
            # Keep one receive task alive across timer checks. Cancelling an async
            # generator on every timer would lose its connection and queued data.
            wake = min(
                recorder.next_depth,
                recorder.next_liveness,
                time.monotonic() + 1.0,
                deadline or math.inf,
            )
            complement = books.complementary.next_deadline_ns
            if complement is not None:
                wake = min(wake, complement / 1_000_000_000)
            done, _ = await asyncio.wait(
                {pending}, timeout=max(0.0, wake - time.monotonic())
            )
            # Enforce the grace bound before a late corrective message can
            # erase the evidence that this connection needs a fresh snapshot.
            if books.complementary.recovery_due(
                message_count=recorder.source_sequence + int(pending in done),
                now_ns=time.monotonic_ns(),
            ):
                pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
                pending = None
                await iterator.aclose()
                await budget.run(
                    client.reconnect, retry_first=True, immediate_first=True
                )
                iterator = client.iter_messages(fail_on_clean_close_exhausted=True)
                continue
            if pending in done:
                try:
                    message = pending.result()
                except StopAsyncIteration:
                    reason = "feed_ended"
                    break
                pending = None
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
                if pending is not None:
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
                    pending = None
                await iterator.aclose()
                await budget.run(
                    client.reconnect, retry_first=True, immediate_first=True
                )
                iterator = client.iter_messages(fail_on_clean_close_exhausted=True)
    except WebSocketDeadlineExceeded:
        reason, normal = "duration", True
    except BaseException as exc:
        error = exc
        reason = (
            "cancelled" if isinstance(exc, asyncio.CancelledError) else "feed_error"
        )
        if not recorder.store.failed:
            recorder.invalidate("feed_interrupted")
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        with contextlib.suppress(Exception):
            await iterator.aclose()
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
