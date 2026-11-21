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
from pmkt.exchanges.kalshi.ws import (
    AsyncKalshiWebSocketClient,
    normalize_market_tickers,
)
from pmkt.exchanges.polymarket.ws import (
    AsyncMarketWebSocketClient,
    _normalize_asset_ids,
)
from pmkt.exchanges.read_auth import ReadAuthHeaderProvider
from pmkt.exchanges.ws_transport import (
    DEFAULT_RECONNECT_BACKOFF_SECONDS, WebSocketMessagePump, WebSocketRetryBudget,
)
from pmkt.streaming.supervisor import FeedShardHealth, LiveFeedSupervisor


# Ongoing feeds keep retrying; cap delay so a long outage still gets regular probes.
MANAGED_FEED_MAX_RECONNECT_BACKOFF_SECONDS = 30.0

_T = TypeVar("_T")


def _timeout_ns(seconds: float) -> int:
    return min(2**64 - 1, max(1, round(min(seconds, 2**64) * 1_000_000_000)))


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
class _Shard:
    health: FeedShardHealth
    native: _native.ShardFeed
    cached: dict[str, PublicBook] = field(default_factory=dict)

    client: Any = None
    task: asyncio.Task[None] | None = None
    generation: int = 0
    restart: bool = True
    reason: str = "not_started"
    last_error: str | None = None
    sid: int | None = None
    command_id: int | None = None


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
        for health in self._supervisor.venue_shards(venue):
            ids_in_shard = list(health.subscribed_instruments)
            native = _native.ShardFeed(
                venue, health.shard_id, ids_in_shard,
                liveness_ns=_timeout_ns(liveness_timeout),
                snapshot_ns=_timeout_ns(snapshot_timeout),
                polymarket_socket_liveness=venue == "polymarket",
            )
            self._shards.append(_Shard(health, native))
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
        if self._closed:
            shard.native.close()
        elif shard.native.transport() is not None:
            shard.native.disconnected(shard.generation, reason)
        shard.cached.clear()
        shard.reason = reason
        shard.restart = True
        shard.health.mark_disconnected()

    def _check_liveness(self, shard: _Shard) -> None:
        if shard.health.connection_state != "connected":
            return
        if not shard.client.is_connected:
            self._invalidate(shard, "disconnected")
            return
        now_ns = time.monotonic_ns()
        reason = shard.native.poll_transport(now_ns)
        if reason is not None:
            self._invalidate(shard, reason)
            return
        if self.venue == "polymarket":
            if shard.client._heartbeat_error is not None:
                self._invalidate(shard, "heartbeat_failed")
                return
            # The socket reader timestamps receipt before placing the frame in
            # the queue. Use that timestamp directly on every poll.
            if (received := shard.client._last_inbound) is not None:
                evidence = round(received * 1_000_000_000)
                transport = shard.native.transport()
                if transport is not None and evidence > transport[0]:
                    shard.native.heartbeat(shard.generation, evidence)

    async def _recover(self, shard: _Shard) -> None:
        # Sends must not block receive/application or heartbeat checks. One owned
        # task batches only the affected instruments and is cancelled on teardown.
        try:
            while not shard.restart:
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
        h = shard.native.health(instrument, time.monotonic_ns())
        liveness_age = h["liveness_age_ns"]
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
            None if liveness_age is None else liveness_age / 1_000_000_000,
            None if age is None else age / 1_000_000_000,
            shard.health.reconnect_count, shard.health.error_count, shard.last_error,
            tuple(h["quality_flags"]), h["metadata_revision"] if h["generation"] == shard.generation else 0,
        )

    def book(self, instrument: str) -> PublicBook | None:
        shard = self._by_instrument[instrument]
        self._check_liveness(shard)
        if shard.restart or shard.health.connection_state != "connected":
            return None
        cached = shard.cached.get(instrument)
        if cached is not None:
            shard.native.poll_books(time.monotonic_ns())
            return cached
        observed = shard.native.read_book(instrument, time.monotonic_ns())
        if observed is None:
            return None
        source = observed["source_timestamp_json"]
        result = PublicBook(
            self.venue, instrument,
            (observed["market_id"] or "") if self.venue == "polymarket" else observed["market_id"],
            shard.health.shard_id,
            observed["generation"],
            observed["bids"], observed["asks"],
            observed["received_monotonic_ns"] / 1_000_000_000,
            json.loads(source, parse_float=Decimal) if source is not None else None,
            tuple(observed["quality_flags"]), observed["metadata_revision"],
        )
        shard.cached[instrument] = result
        return result

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
            now_ns = time.monotonic_ns()
            shard.health.mark_connected(now_monotonic_ns=now_ns)
            shard.native.connected(shard.generation, now_ns, command_id=shard.command_id)
            pump = WebSocketMessagePump(client._iter_raw_frames())
            if self.venue == "kalshi":
                probe = asyncio.create_task(self._probe(shard))
            recovery_task = asyncio.create_task(self._recover(shard))
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
                    self._ingest_raw(shard, message)
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
                shard.native.heartbeat(shard.generation, time.monotonic_ns())
        except Exception:
            self._invalidate(shard, "heartbeat_failed")

    def _ingest_raw(self, shard: _Shard, frame: bytes) -> None:
        native = shard.native
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
        if self.venue == "kalshi" and shard.sid is None:
            transport = native.transport()
            assert transport is not None
            _, shard.sid = transport

__all__ = ["ManagedBookFeed", "PublicBook", "PublicBookHealth"]
