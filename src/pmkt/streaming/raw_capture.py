"""Verbatim public WebSocket capture, separate from normalized ``recording.v1``.

One process records one connection and its reconnects into a fresh session.
Capture requires POSIX signal and directory-fsync support; importing is portable.
Kalshi read authentication is injected; core never loads credentials.
See ``docs/raw_capture_contract.md`` for the persisted byte contract.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import gzip
import hashlib
import json
import logging
import math
import os
import random
import re
import shutil
import signal
import sys
import time
import uuid
from collections import deque
from collections.abc import Callable, Coroutine
from typing import Any, BinaryIO, TypeVar
from urllib.parse import urlparse
from datetime import datetime, timezone
from json.encoder import encode_basestring_ascii
from pathlib import Path
from statistics import median

from websockets.asyncio.client import ClientConnection, connect
from websockets.frames import Frame, Opcode

from pmkt.exchanges.kalshi.ws import kalshi_public_subscription_payload
from pmkt.exchanges.read_auth import (
    ReadAuthHeaderProvider,
    ReadAuthenticationRequiredError,
    headers_for_read,
)

try:
    import uvloop as _uvloop
except ImportError:
    uvloop: Any = None
else:
    uvloop = _uvloop

WS_URLS = {
    "polymarket": {"wss://ws-subscriptions-clob.polymarket.com/ws/market"},
    "kalshi": {
        "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
        "wss://api.elections.kalshi.com/trade-api/ws/v2",
    },
}
KALSHI_CHANNELS = ("orderbook_delta", "trade", "market_lifecycle_v2")
GiB, MiB = 1024**3, 1024**2
# Warn below 2 GiB free; warning only, leaving room to archive while recording continues.
LOW_FREE = 2 * GiB
# Below 512 MiB free, pause writing and count dropped frames to preserve disk headroom.
CRITICAL_FREE = 512 * MiB
# Rotate once per minute to bound the active, unsealed portion of a recording.
CHUNK_SECONDS = 60
# Refresh replaceable state every five seconds without frequent heartbeat fsyncs.
STATE_SECONDS = 5
# Start retries at one second for prompt recovery from brief disconnects.
BACKOFF_START = 1
# Cap retry delays at half a minute so outages are retried without a tight loop.
BACKOFF_MAX = 30
# Reset retry delays after a stable minute, even if readiness is incomplete.
BACKOFF_RESET_SECONDS = 60
# Double retry delays to back off exponentially during sustained outages.
BACKOFF_MULTIPLIER = 2
# Add +/-20% jitter to spread simultaneous reconnects across collectors.
BACKOFF_JITTER = (0.8, 1.2)
# Flush/check disk once per second to limit buffered data and react to disk pressure.
TICK_SECONDS = 1
# Supervise once per second for liveness and lag without per-frame timer work.
SUPERVISE_SECONDS = 1.0
# Tolerate brief heartbeat stalls (25 s Polymarket, 60 s Kalshi) before reconnecting.
RECEIVE_TIMEOUTS = {"polymarket": 25, "kalshi": 60}
# After a Kalshi sequence gap, reconnect for fresh snapshots at most once per
# interval; intervening gaps are recorded and logged only, avoiding reconnect churn.
GAP_RECONNECT_MIN_INTERVAL = 300
# Level 6 roughly halves CPU (owner laptop, Polymarket/Kalshi: 13/6 vs 29/14 us
# per frame at level 9). Recompressing tests/fixtures/raw_capture/{venue}_00.jsonl.gz
# (all JSONL bytes, gzip.GzipFile/BytesIO, mtime=0, zlib 1.3.1) on 2026-11-07:
# Polymarket 37,610 vs 36,551 B (level 9 saves 1,059 B, 2.82%); Kalshi 14,067 vs
# 13,261 B (806 B, 5.73%). The modest size savings favor the cheaper default.
GZIP_LEVEL = 6
# Warn above a five-second sampled exchange-lag median to flag sustained delays.
LAG_WARN_MS = 5000
# Keep a minute of lag samples to smooth short bursts while exposing recent stalls.
LAG_WINDOW_SECONDS = 60
# Require ten samples before warning so isolated delayed frames do not trigger it.
LAG_MIN_SAMPLES = 10
# Repeat persistent lag warnings once per minute to limit log volume.
LAG_WARN_SECONDS = 60
# Repeat persistent disk warnings once per minute to limit log volume.
DISK_LOG_SECONDS = 60
# Summarize once per minute for a compact operational history.
SUMMARY_SECONDS = 60
# Offload fsync every ten seconds to balance durability and event-loop stalls.
SYNC_SECONDS = 10
# Allow 15 seconds for a handshake, tolerating brief connection stalls.
OPEN_TIMEOUT = 15
# Bound closing to two seconds so an unresponsive peer cannot stall recovery.
CLOSE_TIMEOUT = 2
# Accept snapshots up to 16 MiB to accommodate large public order books.
MAX_SIZE = 16 * MiB
# Buffer at most 128 frames to absorb short bursts with bounded queue growth.
MAX_QUEUE = 128
# Ping Kalshi every 20 seconds to keep otherwise quiet connections alive.
KALSHI_PING_INTERVAL = 20
# Allow 20 seconds for the pong to tolerate brief transport stalls.
KALSHI_PING_TIMEOUT = 20
# Polymarket expects an application heartbeat every ten seconds.
POLYMARKET_PING_SECONDS = 10
# Yield during buffered bursts so timers and shutdown signals can run promptly.
RECEIVE_YIELD_FRAMES = 128
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,95}")
KALSHI_DELTA = re.compile(
    r'\{"type":"orderbook_delta","sid":(-?(?:0|[1-9][0-9]{0,18})),'
    r'"seq":(-?(?:0|[1-9][0-9]{0,18})),"msg":\{'
    r'(?:"[a-z_]+":(?:"[^"\\\x00-\x1f]*"|0|[1-9][0-9]{0,18})'
    r'(?:,"[a-z_]+":(?:"[^"\\\x00-\x1f]*"|0|[1-9][0-9]{0,18}))*)?'
    r'\}(?:,"sending_ts_ms":(?:0|[1-9][0-9]{0,18}))?\}\n?'
)
EXCHANGE_TIMESTAMP = {
    "kalshi": re.compile(r'"sending_ts_ms"\s*:\s*(0|[1-9][0-9]{0,18})(?=\s*[,}])'),
    "polymarket": re.compile(r'"timestamp"\s*:\s*"([0-9]{1,19})"'),
}
logger = logging.getLogger(__name__)
# The websockets client logs handshake request headers, including Kalshi signatures, at DEBUG.
WS_LOGGER = logging.getLogger(__name__ + ".websockets")
WS_LOGGER.setLevel(logging.INFO)


class NoRedirectConnect(connect):
    """Refuse handshake redirects: they would carry signed headers past ``WS_URLS``."""

    def process_redirect(self, exc: Exception) -> Exception | str:
        return exc


DISCLAIMER = dict(
    raw_only=True,
    recording_v1=False,
    reconstruction_pending=True,
    network_losslessness_proven=False,
    timestamp_semantics="collector_receive_not_network_arrival",
)


def utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def event_loop_name(loop: asyncio.AbstractEventLoop) -> str:
    return (
        f"uvloop {uvloop.__version__}"
        if uvloop is not None and isinstance(loop, uvloop.Loop)
        else "asyncio"
    )


_Result = TypeVar("_Result")


def run(coro: Coroutine[Any, Any, _Result]) -> _Result:
    """Run capture with optional uvloop, including on Python 3.10."""
    if sys.version_info >= (3, 11):
        with asyncio.Runner(
            loop_factory=uvloop.new_event_loop if uvloop is not None else None
        ) as runner:
            logger.info(
                "event_loop",
                extra=dict(implementation=event_loop_name(runner.get_loop())),
            )
            return runner.run(coro)
    loop = uvloop.new_event_loop() if uvloop is not None else asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        logger.info("event_loop", extra=dict(implementation=event_loop_name(loop)))
        return loop.run_until_complete(coro)
    finally:
        # Match asyncio.run's cleanup without changing the process-wide loop policy.
        try:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
        finally:
            asyncio.set_event_loop(None)
            loop.close()


def atomic(path: Path, value: Any, sync: bool = True) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as out:
        json.dump(value, out, indent=2)
        out.flush()
        if sync:
            os.fsync(out.fileno())
    os.replace(tmp, path)
    if not sync or not hasattr(os, "O_DIRECTORY"):
        return
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def digest(path: Path) -> dict[str, Any]:
    sha, md5 = hashlib.sha256(), hashlib.md5()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            sha.update(block)
            md5.update(block)
    return dict(
        sha256=sha.hexdigest(), md5=md5.hexdigest(), size_bytes=path.stat().st_size
    )


def validate_root(path: Path, runs_root: Path) -> Path:
    """root = runs_root/RUN/INSTANCE; each process start creates a fresh session directory below it."""
    path, runs_root = Path(path), Path(runs_root)
    if (
        not runs_root.is_absolute()
        or ".." in runs_root.parts
        or not path.is_absolute()
        or ".." in path.parts
        or path.parent.parent != runs_root
        or not all(NAME.fullmatch(p) for p in (path.name, path.parent.name))
        or any(p.is_symlink() for p in (path, *path.parents))
    ):
        raise ValueError(
            "root must be a non-symlink runs_root/RUN/INSTANCE path with an absolute runs_root"
        )
    return path


def parse_end(value: str) -> datetime:
    end = datetime.fromisoformat(value.replace("Z", "+00:00"))
    offset = end.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise ValueError("end-utc must be an explicit UTC timestamp")
    return end


def load_markets(path: Path, venue: str) -> dict[str, Any]:
    spec = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(spec, dict):
        raise ValueError("market list must be a JSON object")
    ids = spec.get("instruments")
    if spec.get("venue") != venue or spec.get("ws_url") not in WS_URLS[venue]:
        raise ValueError("market list venue/ws_url mismatch")
    if not NAME.fullmatch(str(spec.get("name", ""))):
        raise ValueError("invalid market list name")
    if (
        not isinstance(ids, list)
        or not ids
        or any(not isinstance(x, str) or not x or x.strip() != x for x in ids)
        or len(set(ids)) != len(ids)
    ):
        raise ValueError(
            "instruments must be a nonempty list of unique nonempty strings"
        )
    if venue == "kalshi" and any(x.endswith(":YES") for x in ids):
        raise ValueError("Kalshi lists use native tickers without :YES")
    return spec


def subscriptions(venue, instruments):
    if venue == "polymarket":
        return [
            json.dumps(
                {
                    "assets_ids": list(instruments),
                    "type": "market",
                    "custom_feature_enabled": True,
                }
            )
        ]
    # Separate commands so a rejected channel cannot take the book subscription with it.
    return [
        json.dumps(
            kalshi_public_subscription_payload(
                instruments, message_id=i, channels=(channel,)
            ),
            separators=(",", ":"),
        )
        for i, channel in enumerate(KALSHI_CHANNELS, 1)
    ]


class Tracker:
    """Decodes only what readiness and Kalshi sequence checks need; payloads are stored verbatim."""

    def __init__(self, venue, instruments):
        self.venue, self.expected = venue, frozenset(instruments)
        self.reset(0)

    def reset(self, generation):
        self.generation, self.books, self.seqs, self.acks = generation, set(), {}, set()
        self.gap = None

    @property
    def ready(self):
        return self.books == self.expected

    def observe(self, raw):
        if self.venue == "polymarket":
            # Escapes can spell book without the literal substring (e.g. bo\u006fk).
            if "book" not in raw and "\\" not in raw:
                return
        else:
            # Validate the tail too: malformed JSON or duplicate outer keys must fall back.
            match = KALSHI_DELTA.fullmatch(raw)
            if match is not None:
                self._sequence(int(match[1]), int(match[2]))
                return
        try:
            events = json.loads(raw)
        except (ValueError, RecursionError):
            return
        for event in events if isinstance(events, list) else [events]:
            if not isinstance(event, dict):
                continue
            if self.venue == "polymarket":
                if (
                    event.get("event_type") == "book"
                    and event.get("asset_id") in self.expected
                ):
                    self.books.add(event["asset_id"])
                continue
            kind, msg = event.get("type"), event.get("msg")
            msg = msg if isinstance(msg, dict) else {}
            if kind == "subscribed" and msg.get("channel"):
                self.acks.add(str(msg["channel"]))
            if kind in ("orderbook_snapshot", "orderbook_delta"):
                if (
                    kind == "orderbook_snapshot"
                    and msg.get("market_ticker") in self.expected
                ):
                    self.books.add(msg["market_ticker"])
                sid, seq = event.get("sid"), event.get("seq")
                if isinstance(sid, int) and isinstance(seq, int):
                    self._sequence(sid, seq)

    def _sequence(self, sid, seq):
        last = self.seqs.get(sid)
        if last is not None and seq != last + 1 and self.gap is None:
            self.gap = dict(sid=sid, expected=last + 1, got=seq)
        self.seqs[sid] = seq


class ChunkWriter:
    def __init__(
        self,
        root: Path,
        recording_id: str,
        venue: str,
        instance: str,
        *,
        gzip_level: int = GZIP_LEVEL,
    ) -> None:
        self.root, self.recording_id, self.venue, self.instance = (
            root,
            recording_id,
            venue,
            instance,
        )
        self.prefix = (
            json.dumps(
                dict(
                    schema=f"{venue}.public_raw_frame.v1",
                    venue=venue,
                    recording_id=recording_id,
                ),
                ensure_ascii=True,
                separators=(",", ":"),
            )[:-1]
            + ","
        )
        self.gzip_level = gzip_level
        self.seq = self.index = self.sealed_count = self.dropped = (
            self.dropped_total
        ) = 0
        self.previous: str | None = None
        self.file: BinaryIO | None = None
        self.gz: gzip.GzipFile | None = None
        self.part: Path | None = None
        self.paused = False
        self.opened = time.monotonic()

    def write(self, frame_type, payload_raw, generation, direction="received"):
        if self.paused:
            self.dropped += 1
            self.dropped_total += 1
            return None
        received_utc, received_monotonic_ns = utc(), time.monotonic_ns()
        data = (
            f'{self.prefix}"connection_generation":{generation},"local_frame_seq":{self.seq + 1},'
            f'"received_utc":{encode_basestring_ascii(received_utc)},"received_monotonic_ns":{received_monotonic_ns},'
            f'"frame_type":{encode_basestring_ascii(frame_type)},"direction":{encode_basestring_ascii(direction)},'
            f'"payload_raw":{encode_basestring_ascii(payload_raw)}}}\n'
        ).encode()
        if self.file is None:
            self.index += 1
            self.part = self.root / f"chunk_{self.index:06d}.jsonl.gz.part"
            self.file = self.part.open("xb", buffering=0)
            self.gz = gzip.GzipFile(
                fileobj=self.file, mode="wb", compresslevel=self.gzip_level, mtime=0
            )
            self.opened, self.start, self.first = (
                time.monotonic(),
                received_utc,
                self.seq + 1,
            )
            self.generations = set()
        self.gz.write(data)
        self.seq += 1
        self.end = received_utc
        self.generations.add(generation)

    def pause(self):
        self.paused = True

    def resume(self, generation):
        """Leave pause mode and record the hole as a local frame; caller forces a fresh baseline."""
        if not self.paused:
            return
        self.paused, dropped, self.dropped = False, self.dropped, 0
        self.write(
            "disk_pause_gap",
            json.dumps({"dropped_frames": dropped}),
            generation,
            "local",
        )

    def flush(self, sync=True):
        if self.gz:
            self.gz.flush()
            self.file.flush()
            if sync:
                os.fsync(self.file.fileno())

    def seal(self, reason="rotation"):
        if self.file is None:
            return None
        self.gz.close()
        self.file.flush()
        os.fsync(self.file.fileno())
        self.file.close()
        # Hash before rename: once the final name exists only the manifest is still to come,
        # and the uploader never touches a chunk whose manifest is missing.
        hashes = digest(self.part)
        dest = self.part.with_suffix("")
        os.rename(self.part, dest)
        manifest = dict(
            schema=f"{self.venue}.public_raw_chunk.v1",
            venue=self.venue,
            instance=self.instance,
            recording_id=self.recording_id,
            filename=dest.name,
            **hashes,
            frame_count=self.seq - self.first + 1,
            first_frame_seq=self.first,
            last_frame_seq=self.seq,
            connection_generations=sorted(self.generations),
            start_utc=self.start,
            end_utc=self.end,
            previous_chunk_sha256=self.previous,
            requires_prior_chunks=self.previous is not None,
            standalone=False,
            continuity="local_sequence_only; reconnects_and_network_gaps_possible",
            dependencies="prior_chunk_chain_contains_earlier_generation_baselines",
            seal_reason=reason,
        )
        atomic(self.root / (dest.name + ".manifest.json"), manifest)
        # Chain state lives in memory only, so uploaded-and-deleted predecessors are never re-read.
        self.previous = manifest["sha256"]
        self.sealed_count += 1
        self.file = self.gz = None
        return manifest


class LagMonitor:
    def __init__(self, venue: str) -> None:
        self.timestamp = EXCHANGE_TIMESTAMP[venue]
        self.raw: str | None = None
        self.received_ns: int | None = None
        self.exchange: deque[tuple[float, float]] = deque()
        self.loop: deque[tuple[float, float]] = deque()

    def sample(self, now, intended_wake):
        self.loop.append((now, max(0, now - intended_wake) * 1000))
        raw, self.raw = (
            self.raw,
            None,
        )  # Consume each sampled frame once, including timestamp-free frames.
        if raw is not None and (match := self.timestamp.search(raw)) is not None:
            self.exchange.append(
                (now, (self.received_ns - int(match[1]) * 1_000_000) / 1_000_000)
            )

    def stats(self, now):
        for samples in (self.exchange, self.loop):
            while samples and samples[0][0] <= now - LAG_WINDOW_SECONDS:
                samples.popleft()
        exchange = [value for _, value in self.exchange]
        return dict(
            exchange_lag_ms_last=exchange[-1] if exchange else None,
            exchange_lag_ms_p50_60s=median(exchange) if exchange else None,
            exchange_lag_ms_max_60s=max(exchange) if exchange else None,
            loop_lag_ms_max_60s=max((value for _, value in self.loop), default=None),
            lag_samples_60s=len(exchange),
        )


class ForcedReconnect(Exception):
    pass


async def capture_raw_ws(
    root: Path,
    end: datetime,
    venue: str,
    markets_path: Path,
    *,
    runs_root: Path,
    auth: ReadAuthHeaderProvider | None = None,
    connector: Callable[..., Any] = NoRedirectConnect,
    chunk_seconds: float = CHUNK_SECONDS,
    free_bytes: Callable[[], int] | None = None,
    low_free: int = LOW_FREE,
    critical_free: int = CRITICAL_FREE,
    receive_timeout: float | None = None,
    backoff_start: float = BACKOFF_START,
    backoff_max: float = BACKOFF_MAX,
    tick_seconds: float = TICK_SECONDS,
    supervise_seconds: float = SUPERVISE_SECONDS,
    gzip_level: int = GZIP_LEVEL,
    lag_warn_ms: float = LAG_WARN_MS,
    gap_reconnect_min_interval: float = GAP_RECONNECT_MIN_INTERVAL,
) -> str:
    """Record until explicit UTC ``end`` or SIGTERM/SIGINT; return the FINAL reason.

    ``root`` must be an absolute, non-symlink ``runs_root/RUN/INSTANCE`` path.
    The market-list JSON selects a public endpoint and native instruments. Kalshi
    requires an injected read-auth provider. Timers and connector are injectable
    for offline replay; use one capture per process (it owns signal handlers).
    """
    if os.name != "posix" or not hasattr(os, "O_DIRECTORY"):
        raise RuntimeError(
            "raw WebSocket capture requires POSIX signals and directory fsync"
        )
    if venue not in WS_URLS:
        raise ValueError("venue must be polymarket or kalshi")
    if not 1 <= gzip_level <= 9:
        raise ValueError("gzip-level must be between 1 and 9")
    if not math.isfinite(chunk_seconds) or chunk_seconds <= 0:
        raise ValueError("chunk-seconds must be finite and positive")
    if not math.isfinite(lag_warn_ms) or lag_warn_ms < 0:
        raise ValueError("lag-warn-ms must be finite and nonnegative")
    if not math.isfinite(gap_reconnect_min_interval) or gap_reconnect_min_interval < 0:
        raise ValueError("gap-reconnect-min-interval must be finite and nonnegative")
    if not 0 <= critical_free <= low_free:
        raise ValueError("disk thresholds must satisfy 0 <= critical-free <= low-free")
    root = validate_root(root, runs_root)
    end = parse_end(end.isoformat())
    spec = load_markets(markets_path, venue)
    instruments, url, instance = spec["instruments"], spec["ws_url"], root.name
    free_bytes = free_bytes or (lambda: shutil.disk_usage(root).free)
    receive_timeout = receive_timeout or RECEIVE_TIMEOUTS[venue]
    if end <= datetime.now(timezone.utc):
        logger.info(
            "end_already_passed", extra=dict(instance=instance, end_utc=end.isoformat())
        )
        return "end_already_passed"
    if venue == "kalshi" and auth is None:
        raise ReadAuthenticationRequiredError(
            "Kalshi raw capture requires a read-auth header provider"
        )
    root.mkdir(parents=True, exist_ok=True)
    session = root / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "_"
        + uuid.uuid4().hex[:8]
    )
    session.mkdir()
    recording_id = str(uuid.uuid4())
    writer = ChunkWriter(session, recording_id, venue, instance, gzip_level=gzip_level)
    tracker, stop = Tracker(venue, instruments), asyncio.Event()
    lag = LagMonitor(venue)
    lag_warned, last_lag_warning = False, float("-inf")
    force = asyncio.Event()
    loop = asyncio.get_running_loop()
    with contextlib.ExitStack() as signal_handlers:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
            signal_handlers.callback(loop.remove_signal_handler, sig)
        deadline = time.monotonic() + (end - datetime.now(timezone.utc)).total_seconds()
        counters = dict(
            errors=0,
            reconnects=0,
            seq_gaps=0,
            gap_reconnects=0,
            ready_generations=0,
            lag_warnings=0,
        )
        status: dict[str, Any] = dict(
            connected=False,
            last_error=None,
            disk="ok",
            free_bytes=None,
            last_activity=time.monotonic(),
            disk_logged=0.0,
        )
        reason = "deadline"
        source_hashes = {
            str(p): digest(p) for p in (Path(markets_path), Path(__file__))
        }
        atomic(
            session / "PROVENANCE.json",
            dict(
                venue=venue,
                instance=instance,
                url=url,
                instrument_count=len(instruments),
                instruments=instruments,
                market_list=spec.get("source"),
                kalshi_channels=KALSHI_CHANNELS if venue == "kalshi" else None,
                end_utc=end.isoformat(),
                started_utc=utc(),
                recording_id=recording_id,
                pid=os.getpid(),
                source_hashes=source_hashes,
                event_loop=event_loop_name(loop),
                gzip_level=gzip_level,
                lag_warn_ms=lag_warn_ms,
                **DISCLAIMER,
            ),
        )
        logger.info(
            "session_start",
            extra=dict(
                instance=instance,
                venue=venue,
                session=session.name,
                instruments=len(instruments),
            ),
        )

        def state():
            value = dict(
                recording_id=recording_id,
                venue=venue,
                instance=instance,
                session=session.name,
                updated_utc=utc(),
                connected=status["connected"],
                ready=status["connected"] and tracker.ready,
                connection_generation=tracker.generation,
                book_count_current_generation=len(tracker.books),
                expected_book_count=len(tracker.expected),
                subscribed_channels=sorted(tracker.acks),
                frame_count=writer.seq,
                sealed_chunks=writer.sealed_count,
                disk=status["disk"],
                free_bytes=status["free_bytes"],
                paused=writer.paused,
                dropped_frames_total=writer.dropped_total,
                last_error=status["last_error"],
                **counters,
                **lag.stats(time.monotonic()),
            )
            # Heartbeats skip fsync: they are rewritten every few seconds and only matter while running.
            atomic(session / "live_state.json", value, sync=False)
            atomic(session / "READY.json", value, sync=False)
            return value

        def check_lag(now, intended_wake):
            nonlocal lag_warned, last_lag_warning
            lag.sample(now, intended_wake)
            values = lag.stats(now)
            p50 = values["exchange_lag_ms_p50_60s"]
            if values["lag_samples_60s"] >= LAG_MIN_SAMPLES and p50 > lag_warn_ms:
                if not lag_warned or now - last_lag_warning >= LAG_WARN_SECONDS:
                    counters["lag_warnings"] += 1
                    logger.warning(
                        "lag_warning",
                        extra=dict(
                            instance=instance, lag_warn_ms=lag_warn_ms, **values
                        ),
                    )
                    last_lag_warning = now
                lag_warned = True
            elif lag_warned and p50 is not None and p50 <= lag_warn_ms:
                logger.info(
                    "lag_recovered",
                    extra=dict(instance=instance, lag_warn_ms=lag_warn_ms, **values),
                )
                lag_warned = False

        class ObservedConnection(ClientConnection):
            def process_event(self, event):
                if isinstance(event, Frame) and event.opcode in (
                    Opcode.PING,
                    Opcode.PONG,
                    Opcode.CLOSE,
                ):
                    status["last_activity"] = time.monotonic()
                    try:
                        writer.write(
                            "ws_" + event.opcode.name.lower(),
                            base64.b64encode(event.data).decode(),
                            tracker.generation,
                            direction="received_control_base64",
                        )
                    except OSError as exc:
                        logger.error(
                            "control_frame_write_failed",
                            extra=dict(error=type(exc).__name__),
                        )
                        stop.set()
                super().process_event(event)

        def check_disk():
            free = free_bytes()
            status["free_bytes"] = free
            level = (
                "critical"
                if free < critical_free
                else "low"
                if free < low_free
                else "ok"
            )
            if level != status["disk"] or (
                level != "ok"
                and time.monotonic() - status["disk_logged"] >= DISK_LOG_SECONDS
            ):
                logger.log(
                    logging.ERROR
                    if level == "critical"
                    else logging.WARNING
                    if level == "low"
                    else logging.INFO,
                    "disk_state",
                    extra=dict(
                        instance=instance,
                        disk=level,
                        free_bytes=free,
                        action="frames_dropped_until_space_returns"
                        if level == "critical"
                        else "recording",
                    ),
                )
                status["disk_logged"] = time.monotonic()
            status["disk"] = level
            if level == "critical" and not writer.paused:
                writer.flush()
                writer.pause()
            elif level != "critical" and writer.paused:
                writer.resume(tracker.generation)
                force.set()  # Frames were dropped: a new connection generation restores native baselines.

        async def tick():
            last_state = last_summary = last_sync = 0
            while not stop.is_set() and time.monotonic() < deadline:
                await asyncio.sleep(
                    min(tick_seconds, max(0, deadline - time.monotonic()))
                )
                # Data reaches the page cache every tick (survives a process crash); fsync, which can stall
                # for long on a busy SSD, runs off the event loop every 10 s and at every seal.
                writer.flush(sync=False)
                if writer.file and time.monotonic() - last_sync >= SYNC_SECONDS:
                    last_sync = time.monotonic()
                    with contextlib.suppress(OSError, ValueError):
                        await asyncio.to_thread(os.fsync, writer.file.fileno())
                check_disk()
                if writer.file and time.monotonic() - writer.opened >= chunk_seconds:
                    writer.seal()
                now = time.monotonic()
                if now - last_state >= STATE_SECONDS:
                    state()
                    last_state = now
                if now - last_summary >= SUMMARY_SECONDS:
                    values = lag.stats(now)
                    logger.info(
                        "summary",
                        extra=dict(
                            instance=instance,
                            generation=tracker.generation,
                            connected=status["connected"],
                            books=len(tracker.books),
                            expected=len(tracker.expected),
                            frames=writer.seq,
                            sealed=writer.sealed_count,
                            disk=status["disk"],
                            **counters,
                            exchange_lag_ms_p50_60s=values["exchange_lag_ms_p50_60s"],
                            exchange_lag_ms_max_60s=values["exchange_lag_ms_max_60s"],
                            loop_lag_ms_max_60s=values["loop_lag_ms_max_60s"],
                        ),
                    )
                    last_summary = now

        sent_header_values: list[str] = []

        def connect_kwargs():
            common = dict(
                open_timeout=OPEN_TIMEOUT,
                close_timeout=CLOSE_TIMEOUT,
                max_size=MAX_SIZE,
                max_queue=MAX_QUEUE,
                proxy=None,
                create_connection=ObservedConnection,
                logger=WS_LOGGER,
            )
            if venue == "polymarket":
                return dict(common, ping_interval=None)
            # Fresh GET signature per connection; header values are never written or logged.
            assert auth is not None
            try:
                headers = headers_for_read(
                    auth, "GET", urlparse(url).path or "/trade-api/ws/v2"
                )
            except Exception:
                # Provider exceptions may contain credential material; record only this safe error.
                raise ReadAuthenticationRequiredError(
                    "Kalshi read-auth provider failed"
                ) from None
            sent_header_values[:] = [value for value in headers.values() if value]
            return dict(
                common,
                additional_headers=headers,
                ping_interval=KALSHI_PING_INTERVAL,
                ping_timeout=KALSHI_PING_TIMEOUT,
            )

        async def transport():
            backoff, last_gap_reconnect = backoff_start, float("-inf")
            while not stop.is_set() and time.monotonic() < deadline:
                tracker.reset(tracker.generation + 1)
                force.clear()
                opened, was_ready = time.monotonic(), False
                try:
                    async with connector(url, **connect_kwargs()) as ws:
                        status["connected"] = True
                        logger.info(
                            "connected",
                            extra=dict(
                                instance=instance, generation=tracker.generation
                            ),
                        )
                        for payload in subscriptions(venue, instruments):
                            await ws.send(payload)
                            writer.write(
                                "subscription", payload, tracker.generation, "sent"
                            )
                        state()
                        status["last_activity"] = time.monotonic()
                        next_ping = status["last_activity"] + POLYMARKET_PING_SECONDS

                        async def receive():
                            nonlocal backoff, was_ready, last_gap_reconnect
                            received = 0
                            while True:
                                raw = await ws.recv()
                                status["last_activity"] = time.monotonic()
                                if isinstance(raw, str):
                                    lag.raw = raw
                                    lag.received_ns = time.time_ns()
                                    kind = (
                                        "app_" + raw.lower()
                                        if raw in ("PING", "PONG")
                                        else "text"
                                    )
                                    writer.write(kind, raw, tracker.generation)
                                    tracker.observe(raw)
                                    if venue == "polymarket" and raw == "PING":
                                        await ws.send("PONG")
                                        writer.write(
                                            "app_pong",
                                            "PONG",
                                            tracker.generation,
                                            "sent",
                                        )
                                else:
                                    writer.write(
                                        "binary_base64",
                                        base64.b64encode(raw).decode(),
                                        tracker.generation,
                                    )
                                if not was_ready and tracker.ready:
                                    was_ready = True
                                    backoff = backoff_start
                                    counters["ready_generations"] += 1
                                    logger.info(
                                        "ready",
                                        extra=dict(
                                            instance=instance,
                                            generation=tracker.generation,
                                            books=len(tracker.books),
                                        ),
                                    )
                                if tracker.gap is not None:
                                    counters["seq_gaps"] += 1
                                    writer.write(
                                        "seq_gap",
                                        json.dumps(tracker.gap),
                                        tracker.generation,
                                        "local",
                                    )
                                    logger.warning(
                                        "seq_gap",
                                        extra=dict(instance=instance, **tracker.gap),
                                    )
                                    tracker.gap = None
                                    if (
                                        time.monotonic() - last_gap_reconnect
                                        >= gap_reconnect_min_interval
                                    ):
                                        last_gap_reconnect = time.monotonic()
                                        counters["gap_reconnects"] += 1
                                        raise ForcedReconnect(
                                            "kalshi sequence gap; reconnecting for fresh snapshots"
                                        )
                                # Buffered recv() calls may not suspend; let timers and signals run during bursts.
                                received += 1
                                if received == RECEIVE_YIELD_FRAMES:
                                    received = 0
                                    await asyncio.sleep(0)

                        receiver = asyncio.create_task(receive())
                        forced = asyncio.create_task(force.wait())
                        try:
                            while not stop.is_set() and time.monotonic() < deadline:
                                now = time.monotonic()
                                intended_wake = min(now + supervise_seconds, deadline)
                                done, _ = await asyncio.wait(
                                    {receiver, forced},
                                    timeout=max(0, intended_wake - now),
                                    return_when=asyncio.FIRST_COMPLETED,
                                )
                                check_lag(time.monotonic(), intended_wake)
                                if receiver in done:
                                    receiver.result()
                                if time.monotonic() - opened >= BACKOFF_RESET_SECONDS:
                                    backoff = backoff_start
                                if force.is_set():
                                    raise ForcedReconnect(
                                        "disk pause ended; reconnecting for fresh baselines"
                                    )
                                now = time.monotonic()
                                if venue == "polymarket" and now >= next_ping:
                                    await ws.send("PING")
                                    writer.write(
                                        "app_ping", "PING", tracker.generation, "sent"
                                    )
                                    next_ping = now + POLYMARKET_PING_SECONDS
                                if now - status["last_activity"] >= receive_timeout:
                                    raise TimeoutError(
                                        f"{receive_timeout} second receive timeout"
                                    )
                        finally:
                            # Finish cancellation before closing this connection or starting another receiver.
                            receiver.cancel()
                            forced.cancel()
                            await asyncio.gather(
                                receiver, forced, return_exceptions=True
                            )
                except Exception as exc:  # noqa: BLE001 - nothing transient may end the run.
                    # A broken local disk re-raises from the transport_error write below; the process
                    # then exits non-zero; a subsequent capture starts a fresh session.
                    on_error(exc)
                finally:
                    status["connected"] = False
                    tracker.books.clear()
                if not stop.is_set() and time.monotonic() < deadline:
                    counters["reconnects"] += 1
                    delay = min(
                        backoff, max(0, deadline - time.monotonic())
                    ) * random.uniform(*BACKOFF_JITTER)
                    state()
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=delay)
                    except asyncio.TimeoutError:
                        pass
                    backoff = min(backoff_max, backoff * BACKOFF_MULTIPLIER)

        def on_error(exc):
            counters["errors"] += 1
            text = str(exc)
            # A misbehaving endpoint can echo request headers into handshake errors.
            for value in sent_header_values:
                text = text.replace(value, "<redacted>")
            status["last_error"] = f"{type(exc).__name__}: {text[:250]}"
            writer.write(
                "transport_error", status["last_error"], tracker.generation, "local"
            )
            logger.warning(
                "transport_error",
                extra=dict(
                    instance=instance,
                    generation=tracker.generation,
                    error=status["last_error"],
                ),
            )

        check_disk()
        state()
        tasks = [
            asyncio.create_task(transport()),
            asyncio.create_task(tick()),
            asyncio.create_task(stop.wait()),
        ]
        try:
            done, _ = await asyncio.wait(
                tasks,
                timeout=max(0, deadline - time.monotonic()),
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                task.result()
            if stop.is_set():
                reason = "signal_or_control_storage_error"
        except BaseException as exc:
            reason = f"partial_{type(exc).__name__}"
            raise
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            status["connected"] = False
            tracker.books.clear()
            try:
                if reason.startswith("partial_"):
                    if writer.gz:
                        assert writer.file is not None
                        writer.gz.close()
                        os.fsync(writer.file.fileno())
                        writer.file.close()  # Keep .part on fatal error as incomplete evidence.
                else:
                    writer.paused = False
                    writer.seal(reason)
            except (OSError, ValueError) as exc:
                reason = "partial_seal_" + type(exc).__name__
            finally:
                atomic(
                    session / "FINAL.json",
                    dict(
                        **state(),
                        reason=reason,
                        partial=reason != "deadline",
                        final_chunk_sha256=writer.previous,
                        closed_chunk_count=writer.sealed_count,
                        active_partial_filename=writer.part.name
                        if writer.part is not None and writer.part.exists()
                        else None,
                        end_utc=utc(),
                        source_hashes=source_hashes,
                    ),
                )
                logger.info(
                    "session_end",
                    extra=dict(
                        instance=instance,
                        reason=reason,
                        frames=writer.seq,
                        sealed=writer.sealed_count,
                        **counters,
                    ),
                )
        return reason
