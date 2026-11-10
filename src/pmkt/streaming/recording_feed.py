"""Venue adapters and one bounded recording loop; no storage profiles or tape."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import asdict
import math
from pathlib import Path
import time
from typing import Any, Callable, Sequence, cast

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
    _UNSET_HEARTBEAT_INTERVAL,
    MarketBookState,
    _normalize_asset_ids,
    apply_market_message,
)
from pmkt.exchanges.polymarket.recovery import ComplementaryDeltaRecovery
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
    def __init__(
        self, venue: str, instruments: list[str], *, enforce_market_identity: bool = False
    ) -> None:
        self.venue = venue
        # ManagedBookFeed owns its identity/recovery policy; recording opts in.
        self.enforce_market_identity = enforce_market_identity
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
        self.market_ids: dict[str, str] = {}
        self.identity_conflicts: set[str] = set()
        self.resolved: set[str] = set()
        self.complementary = ComplementaryDeltaRecovery()

    def reset(self) -> None:
        for poly in self.polymarket.values():
            poly.mark_reconnect()
            if poly.asset_id in self.identity_conflicts:
                poly.quality_flags.add("market_identity_changed")
            if poly.asset_id in self.resolved:
                poly.quality_flags.add("market_resolved")
        for kalshi in self.kalshi.values():
            kalshi.mark_reconnect()
        self.sequences.clear()
        self.venue_times.clear()
        self.complementary.clear()

    def apply(self, message: dict[str, Any], count: int) -> list[BookView]:
        if self.venue == "polymarket":
            kind = message.get("event_type") or message.get("type")
            if kind == "book" and not message.get("asset_id"):
                raise RuntimeError("missing_book_identity")
            if kind == "price_change":
                changes = message.get("price_changes")
                if not isinstance(changes, list) or any(
                    not isinstance(change, dict) or not change.get("asset_id")
                    for change in changes
                ):
                    raise RuntimeError("malformed_price_changes")
            # Identity and resolution outlive replaceable ladder state. Validate
            # before mutation so a conflicting delta cannot merge two markets.
            affected = []
            if kind == "price_change":
                affected = [str(change["asset_id"]) for change in message["price_changes"]]
            elif kind in {"book", "tick_size_change"}:
                affected = [str(message.get("asset_id") or "")]
            rejected = set()
            market = str(message.get("market") or "")
            for asset in sorted(set(affected)):
                if asset not in self.polymarket:
                    continue
                known = self.market_ids.get(asset)
                if self.enforce_market_identity and known and (
                    (market and known != market)
                    or (kind == "book" and asset in self.identity_conflicts and market != known)
                ):
                    self.identity_conflicts.add(asset)
                    state = self.polymarket[asset]
                    state.mark_reconnect()
                    state.market = known
                    state.quality_flags.add("market_identity_changed")
                    rejected.add(asset)
                elif market:
                    self.market_ids[asset] = market
            if rejected and kind == "price_change":
                message = {**message, "price_changes": [
                    change for change in message["price_changes"]
                    if str(change["asset_id"]) not in rejected
                ]}
            previous = self.complementary.intact_changed_assets(
                message, self.polymarket
            )
            snapshots = [] if kind == "book" and rejected else apply_market_message(
                self.polymarket, message, allowed_asset_ids=self.polymarket
            )
            if (
                kind == "tick_size_change"
                and str(message.get("asset_id")) in self.polymarket
                and not rejected
            ):
                snapshots = [
                    self.polymarket[str(message["asset_id"])].apply_tick_size_change(
                        message
                    )
                ]
            elif kind in {"new_market", "market_resolved"}:
                assets = message.get("assets_ids") or message.get("asset_ids") or ()
                targets = [
                    state
                    for key, state in self.polymarket.items()
                    if key == str(message.get("asset_id"))
                    or (isinstance(assets, list) and key in assets)
                    or (
                        market and self.market_ids.get(key) == market
                    )
                ]
                if market:
                    for state in targets:
                        self.market_ids.setdefault(state.asset_id, market)
                # Only the recorder persists resolution; ManagedBookFeed keeps
                # its own terminal state keyed by its first-established market.
                if kind == "market_resolved" and self.enforce_market_identity:
                    self.resolved.update(state.asset_id for state in targets)
                snapshots = [
                    state.apply_lifecycle(
                        {**message, "market": self.market_ids.get(state.asset_id)}
                        if self.enforce_market_identity else message,
                        str(kind),
                    ) for state in targets
                ]
            snapshots.extend(self.polymarket[asset].snapshot() for asset in sorted(rejected))
            self.complementary.observe(
                message, self.polymarket, previous, message_count=count
            )
            views = []
            for snapshot in snapshots:
                state = self.polymarket[snapshot.asset_id]
                if state.asset_id in self.resolved:
                    state.valid_state = False
                    state.quality_flags.add("market_resolved")
                if kind == "book" and state.asset_id not in rejected:
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
                if state.asset_id in self.identity_conflicts:
                    if (
                        kind == "book" and state.asset_id not in rejected
                        and market == self.market_ids[state.asset_id]
                        and state.book_integrity_valid
                    ):
                        self.identity_conflicts.discard(state.asset_id)
                    else:
                        state.valid_state = False
                        state.quality_flags.add("market_identity_changed")
                views.append(
                    BookView(
                        state.asset_id,
                        {float(p): float(q) for p, q in state.bids.items()},
                        {float(p): float(q) for p, q in state.asks.items()},
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
                    {float(p): float(q) for p, q in state_k.yes_bids.items()},
                    {float(p): float(q) for p, q in state_k.no_bids.items()},
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
                "market_identity_changed" in state.quality_flags
                or (
                    state.initial_snapshot_received
                    and not state.book_integrity_valid
                    and not self.complementary.defer(asset, message_count=count, now_ns=now)
                )
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
    duration_s: float = DEFAULT_RECORDING_DURATION_S,
    max_messages: int | None = None,
    max_reconnects: int | None = None,
    ws_url: str | None = None,
    connect_factory: Callable[..., Any] | None = None,
    auth: Any = None,
    heartbeat_interval: float | None = cast(Any, _UNSET_HEARTBEAT_INTERVAL),
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
    books = VenueBooks(venue, ids, enforce_market_identity=True)
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
            flags = (reason, "market_resolved") if instrument in books.resolved else (reason,)
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
            complement = books.complementary.next_deadline_ns
            if complement is not None:
                wake = min(wake, complement / 1_000_000_000)
            received = await pump.wait(wake - time.monotonic())
            # Enforce the grace bound before a late corrective message can
            # erase the evidence that this connection needs a fresh snapshot.
            if books.complementary.recovery_due(
                message_count=recorder.source_sequence + int(received),
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
