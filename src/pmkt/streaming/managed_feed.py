"""Managed, read-only public depth feeds. See docs/managed-book-feeds.md."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from decimal import Decimal
import json
import math
import time
from typing import Any, Awaitable, Callable, Literal, Sequence, TypeVar

from pmkt import _native
from pmkt.streaming import _python_oracle
from pmkt.exchanges.kalshi.ws import (
    AsyncKalshiWebSocketClient,
    KalshiOrderBookState,
    normalize_market_tickers,
)
from pmkt.exchanges.polymarket.ws import (
    AsyncMarketWebSocketClient,
    MarketBookState,
    _normalize_asset_ids,
)
from pmkt.exchanges.polymarket.recovery import CORRECTIVE_DELTA_GRACE_SECONDS
from pmkt.exchanges.read_auth import ReadAuthHeaderProvider
from pmkt.exchanges.ws_transport import (
    DEFAULT_RECONNECT_BACKOFF_SECONDS, WebSocketMessagePump, WebSocketRetryBudget,
)
from pmkt.streaming.recording_feed import VenueBooks
from pmkt.streaming.supervisor import FeedShardHealth, LiveFeedSupervisor


# Ongoing feeds keep retrying; cap delay so a long outage still gets regular probes.
MANAGED_FEED_MAX_RECONNECT_BACKOFF_SECONDS = 30.0

_T = TypeVar("_T")


def _timeout_ns(seconds: float) -> int:
    return min(2**64 - 1, max(1, round(min(seconds, 2**64) * 1_000_000_000)))


def _encode_message(value: Any) -> str:
    # Only used by callers supplying a decoded message. Preserve Decimal tokens
    # (including numeric source timestamps) without converting through float.
    if isinstance(value, dict):
        return "{" + ",".join(json.dumps(k) + ":" + _encode_message(v) for k, v in value.items()) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_encode_message(item) for item in value) + "]"
    if isinstance(value, Decimal):
        return str(value) if value.is_finite() else json.dumps(str(value))
    return json.dumps(value)


async def _bounded(operation: Awaitable[_T], timeout: float) -> _T:
    # wait_for can swallow outer cancellation at child completion on 3.10/3.11.
    task = asyncio.ensure_future(operation)
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
        if not done:
            raise asyncio.TimeoutError()
        return task.result()
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@dataclass(frozen=True)
class PublicBook:
    """A detached depth observation; Kalshi ladders are on the YES price basis."""

    venue: str
    instrument: str
    market_id: str | None
    shard_id: str
    generation: int
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    received_monotonic: float
    source_timestamp: Any
    quality_flags: tuple[str, ...]
    metadata_revision: int = 0


@dataclass(frozen=True)
class PublicBookHealth:
    venue: str
    instrument: str
    shard_id: str
    generation: int
    connection_state: str
    usable: bool
    reason: str | None
    liveness_age_seconds: float | None
    book_update_age_seconds: float | None
    reconnect_count: int
    error_count: int
    last_error: str | None
    quality_flags: tuple[str, ...]
    metadata_revision: int = 0


@dataclass
class _Recovery:
    reason: str
    deadline: float
    phase: Literal["grace", "pending", "sending", "waiting", "cooldown"]
    # A valid snapshot arrived during the send and has not since been invalidated.
    answered: bool = False


@dataclass
class _Shard:
    health: FeedShardHealth
    oracle_books: VenueBooks | None
    native: _native.ShardFeed | None
    cached: dict[str, PublicBook] = field(default_factory=dict)

    client: Any = None
    task: asyncio.Task[None] | None = None
    generation: int = 0
    alive: float | None = None
    connected: float | None = None
    updates: dict[str, float] = field(default_factory=dict)
    source_timestamps: dict[str, Any] = field(default_factory=dict)
    # Explicit metadata-event evidence, retained through same-generation recovery.
    metadata_revisions: dict[str, int] = field(default_factory=dict)
    recovery: dict[str, _Recovery] = field(default_factory=dict)
    restart: bool = True
    reason: str = "not_started"
    last_error: str | None = None
    sid: int | None = None
    command_id: int | None = None
    # Resolution is terminal: kept across snapshots, recoveries and generations.
    resolved: set[str] = field(default_factory=set)
    # Native market identity first seen per instrument, kept across book resets so
    # market-scoped lifecycle events still find instruments under recovery.
    market_ids: dict[str, str] = field(default_factory=dict)
    # Only an established-market snapshot can clear an identity conflict, even
    # after recovery timeouts or connection replacement.
    identity_conflicts: set[str] = field(default_factory=set)

    @property
    def books(self) -> VenueBooks:
        assert self.oracle_books is not None
        return self.oracle_books


class ManagedBookFeed:
    """Hold public books on one asyncio loop, with one connection per shard.

    ``start`` schedules connections without waiting for snapshots. ``book`` is
    nonblocking and returns None until a current, intact, live book is available.
    Empty sides are intact depth, and are reported in quality_flags. Use ``health``
    for unavailability reasons. Callers must read again for each decision: a
    previously returned PublicBook is an observation, not a live freshness lease.
    """

    def __init__(
        self,
        instruments: Sequence[str],
        *,
        venue: Literal["polymarket", "kalshi"],
        shard_size: int = 200,
        liveness_timeout: float = 30.0,
        snapshot_timeout: float = 30.0,
        connect_timeout: float = 10.0,
        probe_interval: float = 10.0,
        retry_backoff: float = DEFAULT_RECONNECT_BACKOFF_SECONDS,
        ws_url: str | None = None,
        auth: ReadAuthHeaderProvider | None = None,
        connect_factory: Callable[..., Any] | None = None,
    ) -> None:
        if venue not in {"polymarket", "kalshi"}:
            raise ValueError("unsupported venue")
        if (
            isinstance(shard_size, bool)
            or not isinstance(shard_size, int)
            or shard_size < 1
        ):
            raise ValueError("shard_size must be a positive integer")
        for name, value in (
            ("liveness_timeout", liveness_timeout),
            ("snapshot_timeout", snapshot_timeout),
            ("connect_timeout", connect_timeout),
            ("probe_interval", probe_interval),
            ("retry_backoff", retry_backoff),
        ):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if probe_interval >= liveness_timeout:
            raise ValueError("probe_interval must be less than liveness_timeout")
        if venue == "kalshi" and auth is None:
            # Not a transport fault: retrying would only hide the misconfiguration.
            raise ValueError("a Kalshi feed needs a read-auth provider (auth=...)")
        self.venue = venue
        ids = (
            _normalize_asset_ids(instruments)
            if venue == "polymarket"
            else normalize_market_tickers(instruments)
        )
        self.instruments = tuple(ids)
        self.liveness_timeout = liveness_timeout
        self.snapshot_timeout = snapshot_timeout
        self.connect_timeout = connect_timeout
        self.probe_interval = probe_interval
        self.retry_backoff = retry_backoff
        self._ws_url, self._auth, self._connect_factory = ws_url, auth, connect_factory
        section, id_key = (
            ("polymarket_assets", "asset_id")
            if venue == "polymarket"
            else ("kalshi_market_tickers", "market_ticker")
        )
        self._supervisor = LiveFeedSupervisor.from_subscription_plan(
            {
                section: [
                    {id_key: instrument, "shard_id": str(n // shard_size)}
                    for n, instrument in enumerate(ids)
                ]
            }
        )
        self._supervisor.require_preflight_ok()
        self._shards = []
        oracle = _python_oracle()
        for health in self._supervisor.venue_shards(venue):
            ids_in_shard = list(health.subscribed_instruments)
            native = None if oracle else _native.ShardFeed(
                venue, health.shard_id, ids_in_shard,
                liveness_ns=_timeout_ns(liveness_timeout),
                snapshot_ns=_timeout_ns(snapshot_timeout),
            )
            books = VenueBooks(venue, ids_in_shard) if oracle else None
            self._shards.append(_Shard(health, books, native))
        self._by_instrument = {
            i: s for s in self._shards for i in s.health.subscribed_instruments
        }
        self._started = False
        self._closed = False
        self._close_waiter: asyncio.Future[Any] | None = None

    async def __aenter__(self) -> ManagedBookFeed:
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("feed is closed")
        if self._started:
            return
        self._started = True
        for shard in self._shards:
            shard.task = asyncio.create_task(
                self._run(shard), name=f"{self.venue}-books-{shard.health.shard_id}"
            )

    async def close(self) -> None:
        if self._close_waiter is None:
            self._closed = True
            tasks = []
            for shard in self._shards:
                self._invalidate(shard, "closed")
                if shard.task is not None:
                    shard.task.cancel()
                    tasks.append(shard.task)
            self._close_waiter = asyncio.gather(*tasks, return_exceptions=True)
        # Multiple callers (or a cancelled waiter) must not cancel teardown again.
        await asyncio.shield(self._close_waiter)

    def _invalidate(self, shard: _Shard, reason: str) -> None:
        # Reset metadata as well as ladders: no evidence crosses generations.
        if shard.native is None:
            shard.oracle_books = VenueBooks(self.venue, list(shard.health.subscribed_instruments))
        elif self._closed:
            shard.native.close()
        elif shard.native.transport() is not None:
            shard.native.disconnected(shard.generation, reason)
        shard.cached.clear()
        shard.updates.clear()
        shard.source_timestamps.clear()
        shard.recovery.clear()
        shard.alive = None
        shard.reason = reason
        shard.restart = True
        shard.health.mark_disconnected()
        for instrument in shard.health.subscribed_instruments:
            shard.health.record_book(
                instrument=instrument,
                valid_state=False,
                book_integrity_valid=False,
                initial_snapshot_received=False,
                quality_flags=("no_initial_snapshot",),
                now_monotonic_ns=time.monotonic_ns(),
            )

    def _check_liveness(self, shard: _Shard) -> None:
        if shard.health.connection_state != "connected":
            return
        if not shard.client.is_connected:
            self._invalidate(shard, "disconnected")
            return
        now = time.monotonic()
        # A monitor/event-loop stall cannot be healed by a late heartbeat.
        if shard.alive is None or now - shard.alive > self.liveness_timeout:
            self._invalidate(shard, "liveness_expired")
            return
        if (
            self.venue == "kalshi"
            and shard.sid is None
            and shard.connected is not None
            and now - shard.connected >= self.snapshot_timeout
        ):
            self._invalidate(shard, "subscription_ack_timeout")
            return
        if self.venue == "polymarket":
            diagnostics = shard.client.heartbeat_diagnostics()
            age = diagnostics["inbound_age_seconds"]
            if diagnostics["heartbeat_error_type"]:
                self._invalidate(shard, "heartbeat_failed")
                return
            if age is not None:
                shard.alive = now - age
        if shard.alive is None or now - shard.alive > self.liveness_timeout:
            self._invalidate(shard, "liveness_expired")
        elif shard.native is not None and self.venue == "polymarket":
            transport = shard.native.transport()
            evidence = round(shard.alive * 1_000_000_000)
            if transport is not None and evidence > transport[0]:
                shard.native.heartbeat(shard.generation, evidence)

    def _reset_book(self, shard: _Shard, instrument: str) -> None:
        # Subscription sequence continuity belongs to the shard, not this book.
        if self.venue == "polymarket":
            shard.books.polymarket[instrument] = MarketBookState(instrument)
            shard.books.complementary.pending.pop(instrument, None)
        else:
            shard.books.kalshi[instrument] = KalshiOrderBookState(
                instrument, use_yes_price=True
            )
        shard.books.venue_times.pop(instrument, None)
        shard.books.minimum_sizes.pop(instrument, None)
        shard.updates.pop(instrument, None)
        shard.source_timestamps.pop(instrument, None)

    def _check_books(self, shard: _Shard, *, recovery_only: bool = False) -> None:
        if shard.restart or shard.health.connection_state != "connected":
            return
        now = time.monotonic()
        states = (
            shard.books.polymarket if self.venue == "polymarket" else shard.books.kalshi
        )
        for instrument in shard.recovery if recovery_only else states:
            state = states[instrument]
            if instrument in shard.resolved or "market_resolved" in state.quality_flags:
                continue
            recovery = shard.recovery.get(instrument)
            if recovery is None:
                if (
                    not state.initial_snapshot_received
                    and shard.connected is not None
                    and now - shard.connected >= self.snapshot_timeout
                ):
                    shard.recovery[instrument] = _Recovery(
                        "snapshot_timeout", now, "pending"
                    )
                continue
            if now < recovery.deadline:
                continue
            if recovery.phase == "grace":
                # Latch expiry before a late corrective delta can restore depth.
                self._reset_book(shard, instrument)
                recovery.phase = "pending"
                recovery.reason = "book_integrity_failed"
            elif recovery.phase == "waiting":
                self._reset_book(shard, instrument)
                recovery.phase = "cooldown"
                if recovery.reason != "snapshot_timeout":
                    recovery.reason = "recovery_timeout"
                recovery.deadline = now + max(30.0, self.snapshot_timeout)
            elif recovery.phase == "cooldown":
                recovery.phase = "pending"

    async def _recover(self, shard: _Shard) -> None:
        # Sends must not block receive/application or heartbeat checks. One owned
        # task batches only the affected instruments and is cancelled on teardown.
        try:
            while not shard.restart:
                if shard.native is not None:
                    self._check_liveness(shard)
                    if shard.restart:
                        return
                    ids = shard.native.begin_requests(shard.generation, time.monotonic_ns())
                    if ids:
                        await _bounded(self._request_snapshots(shard, ids), self.connect_timeout)
                        if shard.restart:
                            return
                        self._check_liveness(shard)
                        if shard.restart:
                            return
                        shard.native.finish_requests(shard.generation, ids, time.monotonic_ns())
                    await asyncio.sleep(0.05)
                    continue
                self._check_books(shard)
                pending = {
                    i: r for i, r in shard.recovery.items() if r.phase == "pending"
                }
                if pending and (self.venue == "polymarket" or shard.sid is not None):
                    for instrument, recovery in pending.items():
                        recovery.phase = "sending"
                        self._reset_book(shard, instrument)
                    await _bounded(
                        self._request_snapshots(shard, list(pending)),
                        self.connect_timeout,
                    )
                    for instrument, recovery in pending.items():
                        if shard.recovery.get(instrument) is not recovery:
                            continue
                        if recovery.answered:
                            # A complete, valid snapshot already arrived during the send.
                            shard.recovery.pop(instrument)
                            continue
                        # Ignore partial depth received during unsubscribe/subscribe.
                        # A subsequent snapshot must establish a complete ladder.
                        self._reset_book(shard, instrument)
                        recovery.phase = "waiting"
                        recovery.deadline = time.monotonic() + self.snapshot_timeout
                await asyncio.sleep(0.05)
        except Exception:
            # Failure to send a subscription operation is a transport fault.
            self._invalidate(shard, "snapshot_request_failed")

    async def _request_snapshots(self, shard: _Shard, instruments: list[str]) -> None:
        if not shard.client.is_connected:
            raise ConnectionError("disconnected")
        if self.venue == "polymarket":
            await shard.client.unsubscribe(instruments)
            if not shard.client.is_connected:
                raise ConnectionError("disconnected")
            await shard.client.subscribe(instruments)
        else:
            await shard.client.request_snapshot(sid=shard.sid, market_tickers=instruments)

    def health(self, instrument: str) -> PublicBookHealth:
        """Unknown instruments raise KeyError; liveness expiry latches invalidation."""
        shard = self._by_instrument[instrument]
        self._check_liveness(shard)
        if shard.native is not None:
            now = time.monotonic()
            h = shard.native.health(instrument, round(now * 1_000_000_000))
            age = h["book_update_age_ns"]
            connected = shard.health.connection_state == "connected" and not shard.restart
            reason = h["reason"]
            if shard.restart:
                reason = shard.reason
            elif not connected:
                reason = "market_resolved" if shard.native.resolved(instrument) else "no_initial_snapshot"
            return PublicBookHealth(
                self.venue, instrument, shard.health.shard_id, shard.generation,
                "closed" if self._closed else shard.health.connection_state,
                connected and h["usable"], reason,
                None if shard.alive is None else max(0.0, now - shard.alive),
                None if age is None else age / 1_000_000_000,
                shard.health.reconnect_count, shard.health.error_count, shard.last_error,
                tuple(h["quality_flags"]), h["metadata_revision"] if h["generation"] == shard.generation else 0,
            )
        self._check_books(shard)
        state = (
            shard.books.polymarket[instrument]
            if self.venue == "polymarket"
            else shard.books.kalshi[instrument]
        )
        resolved = instrument in shard.resolved or "market_resolved" in state.quality_flags
        intact = state.book_integrity_valid and not resolved
        usable = (
            shard.health.connection_state == "connected"
            and not shard.restart
            and intact
            and instrument not in shard.recovery
        )
        now = time.monotonic()
        updated = shard.updates.get(instrument)
        return PublicBookHealth(
            self.venue,
            instrument,
            shard.health.shard_id,
            shard.generation,
            "closed" if self._closed else shard.health.connection_state,
            usable,
            None
            if usable
            else (
                shard.reason if shard.restart else
                "market_resolved" if resolved else
                shard.recovery[instrument].reason if instrument in shard.recovery else
                "no_initial_snapshot" if not state.initial_snapshot_received else
                "book_integrity_failed"
            ),
            None if shard.alive is None else max(0.0, now - shard.alive),
            None if updated is None else max(0.0, now - updated),
            shard.health.reconnect_count,
            shard.health.error_count,
            shard.last_error,
            tuple(sorted(state.quality_flags)),
            shard.metadata_revisions.get(instrument, 0),
        )

    def book(self, instrument: str) -> PublicBook | None:
        shard = self._by_instrument[instrument]
        if shard.native is not None:
            self._check_liveness(shard)
            if shard.restart or shard.health.connection_state != "connected":
                return None
            cached = shard.cached.get(instrument)
            if cached is not None:
                shard.native.poll_books(time.monotonic_ns())
                return cached
            observed = shard.native.observation(instrument, time.monotonic_ns())["book"]
            if observed is None:
                return None
            source = observed["source_timestamp_json"]
            result = PublicBook(
                self.venue, instrument,
                (observed["market_id"] or "") if self.venue == "polymarket" else observed["market_id"],
                shard.health.shard_id,
                observed["generation"],
                tuple((Decimal(p), Decimal(q)) for p, q in observed["bids"]),
                tuple((Decimal(p), Decimal(q)) for p, q in observed["asks"]),
                observed["received_monotonic_ns"] / 1_000_000_000,
                json.loads(source, parse_float=Decimal) if source is not None else None,
                tuple(observed["quality_flags"]), observed["metadata_revision"],
            )
            shard.cached[instrument] = result
            return result
        health = self.health(instrument)
        if not health.usable:
            return None
        shard = self._by_instrument[instrument]
        if self.venue == "polymarket":
            p = shard.books.polymarket[instrument]
            bids, asks, market_id = p.bids, p.asks, p.market
        else:
            k = shard.books.kalshi[instrument]
            bids, asks, market_id = (
                k.yes_bids,
                k.no_bids,
                k.market_id,
            )
        return PublicBook(
            self.venue,
            instrument,
            market_id,
            health.shard_id,
            health.generation,
            tuple(sorted(bids.items(), reverse=True)),
            tuple(sorted(asks.items())),
            shard.updates[instrument],
            shard.source_timestamps[instrument],
            health.quality_flags,
            health.metadata_revision,
        )

    async def _run(self, shard: _Shard) -> None:
        def reconnecting() -> None:
            shard.health.mark_reconnect()

        budget = WebSocketRetryBudget(
            None,
            backoff=self.retry_backoff,
            max_backoff=MANAGED_FEED_MAX_RECONNECT_BACKOFF_SECONDS,
            on_reconnect=reconnecting,
        )
        try:
            await budget.run(lambda: self._session(shard))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            shard.last_error = type(exc).__name__
            shard.health.record_error()
            self._invalidate(shard, "feed_failed")

    async def _session(self, shard: _Shard) -> None:
        if self._closed:
            raise asyncio.CancelledError()
        shard.generation += 1
        shard.metadata_revisions.clear()
        shard.restart = False
        shard.reason = "connecting"
        shard.sid = None
        shard.command_id = None
        shard.health.connection_state = "connecting"
        kwargs: dict[str, Any] = {
            "ws_url": self._ws_url,
            "connect_factory": self._connect_factory,
        }
        ids = shard.health.subscribed_instruments
        if self.venue == "polymarket":
            client: Any = AsyncMarketWebSocketClient(
                ids,
                heartbeat_interval=self.probe_interval,
                pong_timeout_seconds=self.liveness_timeout,
                **kwargs,
            )
        else:
            client = AsyncKalshiWebSocketClient(
                auth=self._auth, use_yes_price=True, **kwargs
            )
        shard.client = client
        pump: WebSocketMessagePump | None = None
        probe: asyncio.Task[None] | None = None
        recovery_task: asyncio.Task[None] | None = None
        try:
            await _bounded(client.connect(), self.connect_timeout)
            if self._closed:
                raise asyncio.CancelledError()
            if self.venue == "kalshi":
                shard.command_id = await _bounded(
                    client.subscribe(ids), self.connect_timeout
                )
            if self._closed:
                raise asyncio.CancelledError()
            shard.connected = shard.alive = time.monotonic()
            shard.health.mark_connected(now_monotonic_ns=time.monotonic_ns())
            if shard.native is not None:
                shard.native.connected(shard.generation, round(shard.connected * 1_000_000_000),
                                       command_id=shard.command_id)
            iterator = (
                client._iter_raw_frames() if shard.native is not None else
                client.iter_messages(reconnect=False, strict_decode=True)
                if self.venue == "polymarket"
                else client.iter_messages(
                    reconnect=False,
                    on_decode_error=self._decode_error,
                    decimal_numbers=True,
                )
            )
            pump = WebSocketMessagePump(iterator)
            if self.venue == "kalshi":
                probe = asyncio.create_task(self._probe(shard))
            recovery_task = asyncio.create_task(self._recover(shard))
            count = 0
            while True:
                self._check_liveness(shard)
                if shard.restart:
                    raise ConnectionError(shard.reason)
                received = await pump.wait(min(0.1, self.probe_interval))
                self._check_liveness(shard)
                if shard.restart:
                    raise ConnectionError(shard.reason)
                if received:
                    try:
                        message = pump.pop()
                    except StopAsyncIteration:
                        raise ConnectionError("disconnected") from None
                    count += 1
                    if isinstance(message, bytes):
                        self._ingest_raw(shard, message)
                    else:
                        self._ingest(shard, message, count)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            shard.last_error = (
                str(exc) if isinstance(exc, ConnectionError) else type(exc).__name__
            )
            shard.health.record_error()
            raise ConnectionError(shard.last_error) from exc
        finally:
            # Invalidate before awaiting teardown or retry backoff.
            self._invalidate(
                shard,
                "closed" if self._closed else (shard.last_error or "disconnected"),
            )
            teardown = asyncio.ensure_future(
                self._teardown(client, pump, probe, recovery_task)
            )
            try:
                await asyncio.shield(teardown)
            except asyncio.CancelledError:
                # close() may cancel while the socket is closing: finish closing it.
                await teardown
                raise

    async def _teardown(
        self,
        client: Any,
        pump: WebSocketMessagePump | None,
        probe: asyncio.Task[None] | None,
        recovery_task: asyncio.Task[None] | None,
    ) -> None:
        for task in (probe, recovery_task):
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        if pump is not None:
            with contextlib.suppress(Exception):
                await pump.close()
        with contextlib.suppress(Exception):
            await _bounded(client.close(), self.connect_timeout)

    async def _probe(self, shard: _Shard) -> None:
        # Waiting for a pong must not suspend receipt/application of book deltas.
        try:
            while not shard.restart:
                await asyncio.sleep(self.probe_interval)
                healthy = await shard.client.probe_liveness(
                    timeout_seconds=min(self.connect_timeout, self.liveness_timeout / 3)
                )
                # Check the old deadline before accepting late liveness evidence,
                # even when no reader ran while the probe was outstanding.
                self._check_liveness(shard)
                if shard.restart:
                    return
                if not healthy:
                    self._invalidate(shard, "heartbeat_failed")
                    return
                shard.alive = time.monotonic()
                if shard.native is not None:
                    shard.native.heartbeat(shard.generation, round(shard.alive * 1_000_000_000))
        except Exception:
            self._invalidate(shard, "heartbeat_failed")

    @staticmethod
    def _decode_error(reason: str, raw: Any) -> None:
        raise ConnectionError("malformed_frame")

    def _ingest_raw(self, shard: _Shard, frame: bytes) -> None:
        native = shard.native
        assert native is not None
        try:
            touched = native.ingest(shard.health.shard_id, shard.generation, frame,
                                    time.monotonic_ns(), detach=False)
        except ValueError as exc:
            reason = str(exc)
            if self.venue == "polymarket" and reason == "malformed_frame":
                reason = "WebSocketProtocolError"
            self._invalidate(shard, reason)
            raise ConnectionError(reason) from exc
        for instrument in touched:
            shard.cached.pop(instrument, None)
        if self.venue == "kalshi":
            transport = native.transport()
            assert transport is not None
            alive, shard.sid = transport
            shard.alive = alive / 1_000_000_000

    def _ingest(self, shard: _Shard, message: dict[str, Any], count: int) -> None:
        if shard.native is not None:
            self._ingest_raw(shard, _encode_message(message).encode())
            return
        # The recovery task checks missing initial snapshots every 50 ms. Ingress
        # only needs to latch existing deadlines before applying a late repair.
        if shard.recovery:
            self._check_books(shard, recovery_only=True)
        kind = message.get("event_type") or message.get("type")
        if kind == "error":
            raise ConnectionError("subscription_error")
        if self.venue == "kalshi":
            if kind == "subscribed":
                msg = message.get("msg")
                if (
                    not isinstance(msg, dict)
                    or type(message.get("id")) is not int
                    or message.get("id") != shard.command_id
                    or msg.get("channel") != "orderbook_delta"
                    or shard.sid is not None
                    or type(msg.get("sid")) is not int
                    or msg["sid"] < 0
                ):
                    raise ConnectionError("invalid_subscription_ack")
                shard.sid = msg["sid"]
                return
            if kind not in {"orderbook_snapshot", "orderbook_delta"}:
                return
            payload = message.get("msg")
            if (
                shard.sid is None
                or type(message.get("sid")) is not int
                or message["sid"] != shard.sid
                or type(message.get("seq")) is not int
                or message["seq"] < 0
                or not isinstance(payload, dict)
                or payload.get("market_ticker") not in shard.books.kalshi
            ):
                raise ConnectionError("invalid_book_identity_or_sequence")
            previous = shard.books.sequences.get(shard.sid)
            if previous is not None and message["seq"] != previous + 1:
                raise ConnectionError("subscription_sequence_gap")
        else:
            if kind == "book" and not message.get("asset_id"):
                raise ConnectionError("missing_book_identity")
            if kind == "price_change":
                changes = message.get("price_changes")
                if not isinstance(changes, list) or any(
                    not isinstance(change, dict) or not change.get("asset_id")
                    for change in changes
                ):
                    raise ConnectionError("malformed_price_changes")
        if self.venue == "polymarket" and kind == "market_resolved":
            # Market-scoped: reach instruments whose state was reset for recovery.
            market = str(message.get("market") or "")
            assets = message.get("assets_ids") or message.get("asset_ids") or ()
            for instrument in shard.health.subscribed_instruments:
                if (market and shard.market_ids.get(instrument) == market) or instrument in assets \
                        or instrument == message.get("asset_id"):
                    shard.resolved.add(instrument)
                    shard.recovery.pop(instrument, None)
        if self.venue == "polymarket" and kind == "tick_size_change":
            instrument = str(message.get("asset_id"))
            if instrument in shard.books.polymarket:
                # Event evidence only: even repeated or malformed tick values
                # require consumers to reconsider independently cached metadata.
                shard.metadata_revisions[instrument] = shard.metadata_revisions.get(instrument, 0) + 1
        views = shard.books.apply(message, count)
        now = time.monotonic()
        # Public health is derived from book state and the clocks below. The
        # supervisor supplies connection counters, not per-instrument age gates.
        if self.venue == "kalshi":
            shard.alive = now
        for view in views:
            instrument = (
                view.instrument_id
                if self.venue == "polymarket"
                else view.instrument_id.removesuffix(":YES")
            )
            if self.venue == "polymarket":
                market = str(shard.books.polymarket[instrument].market or "")
            else:
                market = str(shard.books.kalshi[instrument].market_id or "")
            known = shard.market_ids.get(instrument)
            if market and known and market != known:
                # A message for another market merged into this book: drop the
                # mixed ladder and require a fresh snapshot.
                self._reset_book(shard, instrument)
                shard.identity_conflicts.add(instrument)
                if instrument not in shard.resolved:
                    recovery = shard.recovery.get(instrument)
                    if recovery is not None and recovery.phase == "sending":
                        # Keep the outstanding send barrier when identity changes.
                        recovery.reason = "market_identity_changed"
                        recovery.answered = False
                    else:
                        shard.recovery[instrument] = _Recovery("market_identity_changed", now, "pending")
                continue
            if market and not known:
                shard.market_ids[instrument] = market
            recovery = shard.recovery.get(instrument)
            if instrument in shard.identity_conflicts:
                incoming_market = (
                    message.get("market") if self.venue == "polymarket"
                    else message["msg"].get("market_id")
                )
                if kind not in {"book", "orderbook_snapshot"} or str(incoming_market or "") != known:
                    # State can inherit identity from a malformed prior snapshot;
                    # only the incoming snapshot can explicitly verify it.
                    self._reset_book(shard, instrument)
                    continue
            if view.integrity_valid and kind in {"book", "orderbook_snapshot"}:
                shard.identity_conflicts.discard(instrument)
            if "market_resolved" in view.quality_flags or instrument in shard.resolved:
                shard.resolved.add(instrument)
                shard.recovery.pop(instrument, None)
            elif (
                recovery is not None
                and recovery.phase == "sending"
            ):
                if not view.integrity_valid:
                    recovery.answered = False
                elif kind in {"book", "orderbook_snapshot"}:
                    recovery.answered = True
            elif not view.integrity_valid and recovery is None:
                grace = (
                    self.venue == "polymarket"
                    and kind == "price_change"
                    and view.initialized
                    and set(view.quality_flags) <= {"crossed_book", "negative_spread"}
                )
                shard.recovery[instrument] = _Recovery(
                    "crossed_book" if grace else "book_integrity_failed",
                    now + CORRECTIVE_DELTA_GRACE_SECONDS if grace else now,
                    "grace" if grace else "pending",
                )
            elif recovery is not None and recovery.phase != "sending":
                if view.integrity_valid and (
                    kind in {"book", "orderbook_snapshot"}
                    or (recovery.phase == "grace" and kind == "price_change")
                ):
                    shard.recovery.pop(instrument)
                elif recovery.phase == "grace" and set(view.quality_flags) - {
                    "crossed_book", "negative_spread"
                }:
                    self._reset_book(shard, instrument)
                    recovery.phase = "pending"
                    recovery.reason = "book_integrity_failed"
            if kind in {
                "book",
                "price_change",
                "orderbook_snapshot",
                "orderbook_delta",
            }:
                shard.updates[instrument] = now
                state = (
                    shard.books.polymarket[instrument]
                    if self.venue == "polymarket"
                    else shard.books.kalshi[instrument]
                )
                shard.source_timestamps[instrument] = state.timestamp


__all__ = ["ManagedBookFeed", "PublicBook", "PublicBookHealth"]
