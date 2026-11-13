"""Merge several recorded copies of one venue's public feed into one stream.

Several machines ("copies") recorded the same public WebSocket feeds, so that a
disconnect on one can be bridged from another. :class:`CopyMerge` turns their
message streams into one stream in which every venue message appears once.
The rules and their limits are documented in ``docs/copy_merge.md``; in short:

1. Union by content: two received messages are the same venue message when
   their content is equal (Kalshi: ignoring the per-connection ``sid``, ``seq``
   and ``sending_ts_ms``). A message received k times by one stream is kept
   k times (the maximum over streams), so genuine venue repeats survive.
2. Order by venue time: the stream is ordered by the venue's millisecond
   timestamp. Each message keeps its earliest receive time and that copy.
3. Within one venue timestamp and one market, the received order is kept.
4. Full books (Polymarket ``book``, Kalshi ``orderbook_snapshot``) are flagged
   as checkpoints and placed at their own timestamp like any other message.

Input is a set of streams of :class:`Occurrence`, one stream per
(copy, connection), each in receive order. They can come from the raw recorder
(:func:`connection_occurrences`) or from any other store that keeps the same
fields. Memory is bounded by the lateness window, not by the recording length.
"""

from __future__ import annotations

import hashlib
import heapq
import itertools
import json
from collections import Counter, deque
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pmkt.streaming.copy_coverage import (
    CoverageTracker,
    SessionCoverage,
    utc_microseconds,
)
from pmkt.streaming.raw_reader import (
    ChunkGap,
    RawFrame,
    VenueMessage,
    iter_venue_messages,
    read_session,
)

VENUES = ("polymarket", "kalshi")
# Kalshi numbers and stamps these per connection; copies differ only here.
KALSHI_CONNECTION_FIELDS = frozenset({"sid", "seq", "sending_ts_ms"})
CHECKPOINT_KINDS = {"polymarket": "book", "kalshi": "orderbook_snapshot"}
# A message received more than this after its venue timestamp is "late": it is
# no longer placed at its own timestamp. Two minutes covers ordinary latency and
# all but the oldest reconnect snapshots (median 2-4 s, 5% at 43 s or more),
# while keeping about two minutes of messages in memory.
DEFAULT_LATENESS_MS = 120_000
# Messages without a venue timestamp (e.g. Kalshi lifecycle) received by
# different streams within this distance of each other are the same message.
DEFAULT_UNTIMED_MATCH_MS = 5_000
# How long emitted content is remembered to recognise late repeats of it.
DEFAULT_RETENTION_MS = 900_000
_CONFLICT_EXAMPLES = 20
# One input stream: (copy, connection).
Stream = tuple[str, str]


class CopyMergeError(ValueError):
    """Inputs violate the merge contract, or copies disagree in strict mode."""


@dataclass(frozen=True, slots=True)
class Origin:
    """Where one received occurrence of a message lies in its recording."""

    copy: str
    connection: str
    session: str
    generation: int
    frame_seq: int
    # Index in a batch frame (a JSON list), None for a single-message frame.
    position: int | None
    received_us: int


@dataclass(frozen=True, slots=True)
class Occurrence:
    """One venue message as one stream received it (the merge's input unit).

    ``message`` is the decoded JSON value of one venue message. Every copy must
    be decoded the same way: identity is computed from this value.
    ``reconnect_snapshot`` marks a full book sent in reply to a (re)subscribe.
    """

    copy: str
    connection: str
    session: str
    generation: int
    frame_seq: int
    position: int | None
    received_us: int
    message: Any
    reconnect_snapshot: bool = False

    def origin(self) -> Origin:
        return Origin(
            self.copy,
            self.connection,
            self.session,
            self.generation,
            self.frame_seq,
            self.position,
            self.received_us,
        )


@dataclass(frozen=True, slots=True)
class MessageKey:
    kind: str
    # Ordering scope: Polymarket condition id, Kalshi market/event ticker.
    scope: str
    venue_ts_ms: int | None
    # Kalshi sending_ts_ms of this occurrence; None for Polymarket.
    sent_ms: int | None
    digest: bytes


@dataclass(frozen=True)
class MergedMessage:
    kind: str
    scope: str
    # The venue timestamp in the content; None if the message carries none.
    venue_ts_ms: int | None
    # Position of the message in the merged order (see docs/copy_merge.md).
    order_ms: int
    late: bool
    checkpoint: bool
    reconnect_snapshot: bool
    # The decoded message of the earliest-received occurrence (``origin``).
    message: Any
    content_digest: str
    copies: tuple[str, ...]
    origin: Origin
    occurrences: tuple[Origin, ...]

    @property
    def first_received_us(self) -> int:
        return self.origin.received_us

    @property
    def first_copy(self) -> str:
        return self.origin.copy


@dataclass
class MergeStats:
    occurrences: int = 0
    messages: int = 0
    # Occurrences attached to a message another occurrence already created.
    repeats_merged: int = 0
    untimed_messages: int = 0
    late_messages: int = 0
    # Late or untimed occurrences dropped because their content was already emitted.
    repeats_after_emit: int = 0
    # Occurrences dropped because their stream ordered them differently.
    order_conflicts: int = 0
    # Gaps where two streams each held messages the other lacked; their
    # relative order is a convention (base stream first).
    ambiguous_insertions: int = 0
    clock_steps_back: int = 0
    max_pending_occurrences: int = 0
    late_by_copy: Counter[str] = field(default_factory=Counter)
    conflict_examples: list[dict[str, Any]] = field(default_factory=list)


def _ms(value: Any) -> int | None:
    if type(value) is int:
        return value if value >= 0 else None
    if isinstance(value, str) and value.isascii() and value.isdigit():
        return int(value)
    return None


def _digest(value: Any) -> bytes:
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.blake2b(text.encode("ascii"), digest_size=16).digest()


def message_key(venue: str, message: Any) -> MessageKey:
    """Identity, ordering scope and venue time of one decoded venue message.

    Identity is a 128-bit BLAKE2b digest of the message serialised as JSON with
    sorted keys, so key order and whitespace do not matter but every value does.
    """
    if not isinstance(message, dict):
        return MessageKey("", "", None, None, _digest(message))
    if venue == "polymarket":
        kind = message.get("event_type")
        scope = message.get("market") or message.get("asset_id") or ""
        return MessageKey(
            kind if isinstance(kind, str) else "",
            scope if isinstance(scope, str) else "",
            _ms(message.get("timestamp")),
            None,
            _digest(message),
        )
    if venue == "kalshi":
        kind = message.get("type")
        msg = message.get("msg")
        msg = msg if isinstance(msg, dict) else {}
        scope = msg.get("market_ticker") or msg.get("event_ticker") or ""
        return MessageKey(
            kind if isinstance(kind, str) else "",
            scope if isinstance(scope, str) else "",
            _ms(msg.get("ts_ms")),
            _ms(message.get("sending_ts_ms")),
            _digest(
                {k: v for k, v in message.items() if k not in KALSHI_CONNECTION_FIELDS}
            ),
        )
    raise ValueError(f"unsupported venue: {venue!r}")


def _order(origin: Origin) -> tuple[Any, ...]:
    return (
        origin.received_us,
        origin.copy,
        origin.connection,
        origin.session,
        origin.frame_seq,
        -1 if origin.position is None else origin.position,
    )


class _Entry:
    """One merged message being built: its content and matched occurrences."""

    __slots__ = ("key", "message", "origins", "reconnect")

    def __init__(self, key: MessageKey, message: Any, origin: Origin, reconnect: bool):
        self.key, self.message, self.reconnect = key, message, reconnect
        self.origins = [origin]

    def attach(self, message: Any, origin: Origin, reconnect: bool) -> None:
        # Keep the message of the earliest-received occurrence (for Kalshi the
        # per-connection envelope fields then match ``origin``).
        if _order(origin) < _order(self.origins[0]):
            self.message = message
            self.origins.insert(0, origin)
        else:
            self.origins.append(origin)
        self.reconnect = self.reconnect or reconnect


class _Untimed(_Entry):
    """A pending message without venue time, matched across streams by time."""

    __slots__ = ("order_ms", "streams", "heap_id")

    def __init__(
        self,
        key: MessageKey,
        message: Any,
        origin: Origin,
        reconnect: bool,
        *,
        order_ms: int,
        stream: Stream,
        heap_id: int,
    ) -> None:
        super().__init__(key, message, origin, reconnect)
        self.order_ms = order_ms
        self.streams = {stream}
        self.heap_id = heap_id



def _align(a: list[bytes], b: list[bytes]) -> list[tuple[int | None, int | None]]:
    """Longest-common-subsequence alignment with a fixed tie rule.

    Returns pairs in order: (i, j) matched, (i, None) only in ``a``, (None, j)
    only in ``b``. Equal elements are always matched (that is optimal); on a
    tie the ``a`` element is skipped first.
    """
    n, m = len(a), len(b)
    lcs = [[0] * (m + 1) for _ in range(n + 1)]
    for i in range(n - 1, -1, -1):
        row, below, ai = lcs[i], lcs[i + 1], a[i]
        for j in range(m - 1, -1, -1):
            row[j] = below[j + 1] + 1 if ai == b[j] else max(below[j], row[j + 1])
    ops: list[tuple[int | None, int | None]] = []
    i = j = 0
    while i < n and j < m:
        if a[i] == b[j]:
            ops.append((i, j))
            i, j = i + 1, j + 1
        elif lcs[i + 1][j] >= lcs[i][j + 1]:
            ops.append((i, None))
            i += 1
        else:
            ops.append((None, j))
            j += 1
    ops.extend((k, None) for k in range(i, n))
    ops.extend((None, k) for k in range(j, m))
    return ops


def _is_subsequence(short: list[bytes], long: list[bytes]) -> list[int] | None:
    """Leftmost positions of ``short`` inside ``long``, or None."""
    positions, start = [], 0
    for item in short:
        try:
            start = long.index(item, start)
        except ValueError:
            return None
        positions.append(start)
        start += 1
    return positions


class CopyMerge:
    """Streaming merge of one venue's copies; run :meth:`merge` once.

    ``lateness_ms``: an occurrence received (by its own copy's clock) more than
    this after its venue timestamp is late, see docs/copy_merge.md.
    ``untimed_match_ms``: matching distance for messages without venue time.
    ``retention_ms``: how long emitted content is remembered for late repeats.
    ``strict_order``: raise :class:`CopyMergeError` when two streams order the
    same messages differently inside one timestamp, instead of counting it.
    """

    def __init__(
        self,
        venue: str,
        *,
        lateness_ms: int = DEFAULT_LATENESS_MS,
        untimed_match_ms: int = DEFAULT_UNTIMED_MATCH_MS,
        retention_ms: int = DEFAULT_RETENTION_MS,
        strict_order: bool = False,
    ) -> None:
        if venue not in VENUES:
            raise ValueError(f"unsupported venue: {venue!r}")
        if lateness_ms <= 0 or untimed_match_ms < 0 or retention_ms < 0:
            raise ValueError("lateness_ms must be positive; other windows non-negative")
        self.venue = venue
        self.lateness_ms = lateness_ms
        self.untimed_match_ms = untimed_match_ms
        self.retention_ms = retention_ms
        self.strict_order = strict_order
        self.stats = MergeStats()
        self._checkpoint_kind = CHECKPOINT_KINDS[venue]
        self._started = False
        # Pending timed groups keyed (venue_ts_ms, scope): per stream, the
        # digests in received order plus one _Entry list per stream.
        self._groups: dict[tuple[int, str], dict[Stream, list[tuple[MessageKey, Any, Origin, bool]]]] = {}
        self._untimed: dict[bytes, list[_Untimed]] = {}
        # (order_ms, scope, rank, id, ref): rank 0 = timed group, 1 = untimed.
        self._heap: list[tuple[int, str, int, int, Any]] = []
        self._ids = itertools.count(1)
        self._pending = 0
        # Emitted content remembered for retention_ms of order time.
        self._emitted_timed: dict[bytes, int] = {}
        self._emitted_untimed: dict[bytes, list[tuple[int, frozenset[Stream]]]] = {}
        self._expiry: deque[tuple[int, bytes, tuple[int, frozenset[Stream]] | None]] = deque()

    # -- driver ---------------------------------------------------------------

    def merge(self, streams: Iterable[Iterable[Occurrence]]) -> Iterator[MergedMessage]:
        """Yield merged messages in order. ``self.stats`` is final when exhausted."""
        if self._started:
            raise RuntimeError("a CopyMerge instance runs only once")
        self._started = True
        for occurrence, received in self._interleave(streams):
            watermark = received // 1000 - self.lateness_ms
            yield from self._flush(watermark)
            self._expire(watermark)
            yield from self._add(occurrence, watermark)
        yield from self._flush(None)

    def _interleave(
        self, streams: Iterable[Iterable[Occurrence]]
    ) -> Iterator[tuple[Occurrence, int]]:
        """Interleave streams by receive time; ties by stream name, never input order."""
        heap: list[tuple[int, Stream, int, Occurrence, Iterator[Occurrence]]] = []
        clocks: dict[Stream, int] = {}
        counter = itertools.count()

        def push(stream: Stream, occurrence: Occurrence, rest: Iterator[Occurrence]) -> None:
            if (occurrence.copy, occurrence.connection) != stream:
                raise CopyMergeError(
                    f"stream {stream} yielded an occurrence of "
                    f"{(occurrence.copy, occurrence.connection)}"
                )
            # A wall clock that steps back would break the receive-order merge;
            # keep each stream's clock monotonic and count the steps.
            clock = clocks.get(stream, occurrence.received_us)
            if occurrence.received_us < clock:
                self.stats.clock_steps_back += 1
            clocks[stream] = clock = max(clock, occurrence.received_us)
            heapq.heappush(heap, (clock, stream, next(counter), occurrence, rest))

        for source in streams:
            rest = iter(source)
            first = next(rest, None)
            if first is None:
                continue
            stream = (first.copy, first.connection)
            if stream in clocks:
                raise CopyMergeError(f"two input streams for copy/connection {stream}")
            push(stream, first, rest)
        while heap:
            received, stream, _, occurrence, rest = heapq.heappop(heap)
            following = next(rest, None)
            if following is not None:
                push(stream, following, rest)
            yield occurrence, received

    # -- adding occurrences ----------------------------------------------------

    def _add(self, occurrence: Occurrence, watermark: int) -> Iterator[MergedMessage]:
        key = message_key(self.venue, occurrence.message)
        stream = (occurrence.copy, occurrence.connection)
        origin = occurrence.origin()
        self.stats.occurrences += 1
        if key.venue_ts_ms is None:
            yield from self._add_untimed(key, stream, occurrence, origin, watermark)
            return
        if key.venue_ts_ms < watermark:
            if key.digest in self._emitted_timed:
                self.stats.repeats_after_emit += 1
                return
            self._remember(key.digest, watermark, None)
            yield self._late(key, occurrence, origin, watermark)
            return
        group_key = (key.venue_ts_ms, key.scope)
        group = self._groups.get(group_key)
        if group is None:
            group = self._groups[group_key] = {}
            heapq.heappush(self._heap, (key.venue_ts_ms, key.scope, 0, 0, group_key))
        group.setdefault(stream, []).append(
            (key, occurrence.message, origin, occurrence.reconnect_snapshot)
        )
        self._grow()

    def _add_untimed(
        self,
        key: MessageKey,
        stream: Stream,
        occurrence: Occurrence,
        origin: Origin,
        watermark: int,
    ) -> Iterator[MergedMessage]:
        at = key.sent_ms if key.sent_ms is not None else occurrence.received_us // 1000
        tolerance = self.untimed_match_ms
        for pending in self._untimed.get(key.digest, ()):
            if stream not in pending.streams and abs(at - pending.order_ms) <= tolerance:
                pending.attach(occurrence.message, origin, occurrence.reconnect_snapshot)
                pending.streams.add(stream)
                self._grow()
                if watermark <= at < pending.order_ms:
                    pending.order_ms, pending.heap_id = at, next(self._ids)
                    heapq.heappush(
                        self._heap, (at, key.scope, 1, pending.heap_id, pending)
                    )
                return
        for emitted_at, streams in self._emitted_untimed.get(key.digest, ()):
            if stream not in streams and abs(at - emitted_at) <= tolerance:
                self.stats.repeats_after_emit += 1
                return
        if at < watermark:
            self._remember(key.digest, watermark, frozenset({stream}))
            self.stats.untimed_messages += 1
            yield self._late(key, occurrence, origin, watermark)
            return
        entry = _Untimed(
            key,
            occurrence.message,
            origin,
            occurrence.reconnect_snapshot,
            order_ms=at,
            stream=stream,
            heap_id=next(self._ids),
        )
        self._untimed.setdefault(key.digest, []).append(entry)
        heapq.heappush(self._heap, (at, key.scope, 1, entry.heap_id, entry))
        self._grow()

    def _grow(self) -> None:
        self._pending += 1
        if self._pending > self.stats.max_pending_occurrences:
            self.stats.max_pending_occurrences = self._pending

    def _late(
        self, key: MessageKey, occurrence: Occurrence, origin: Origin, order_ms: int
    ) -> MergedMessage:
        self.stats.late_messages += 1
        self.stats.late_by_copy[occurrence.copy] += 1
        entry = _Entry(key, occurrence.message, origin, occurrence.reconnect_snapshot)
        return self._message(entry, order_ms, late=True)

    # -- emitting --------------------------------------------------------------

    def _flush(self, limit: int | None) -> Iterator[MergedMessage]:
        heap = self._heap
        while heap and (limit is None or heap[0][0] < limit):
            order_ms, scope, rank, heap_id, ref = heapq.heappop(heap)
            if rank == 0:
                for entry in self._resolve(ref, self._groups.pop(ref)):
                    self._remember(entry.key.digest, order_ms, None)
                    yield self._message(entry, order_ms, late=False)
                continue
            if ref.heap_id != heap_id:
                continue  # superseded by an earlier placement of the same entry
            siblings = self._untimed[ref.key.digest]
            siblings.remove(ref)
            if not siblings:
                del self._untimed[ref.key.digest]
            self._pending -= len(ref.origins)
            self.stats.untimed_messages += 1
            self._remember(ref.key.digest, order_ms, frozenset(ref.streams))
            yield self._message(ref, order_ms, late=False)

    def _remember(
        self, digest: bytes, order_ms: int, streams: frozenset[Stream] | None
    ) -> None:
        if streams is None:
            self._emitted_timed[digest] = order_ms
            self._expiry.append((order_ms, digest, None))
        else:
            record = (order_ms, streams)
            self._emitted_untimed.setdefault(digest, []).append(record)
            self._expiry.append((order_ms, digest, record))

    def _expire(self, watermark: int) -> None:
        limit = watermark - self.retention_ms
        while self._expiry and self._expiry[0][0] < limit:
            order_ms, digest, record = self._expiry.popleft()
            if record is None:
                if self._emitted_timed.get(digest) == order_ms:
                    del self._emitted_timed[digest]
                continue
            records = self._emitted_untimed[digest]
            records.remove(record)
            if not records:
                del self._emitted_untimed[digest]

    def _message(self, entry: _Entry, order_ms: int, *, late: bool) -> MergedMessage:
        origins = sorted(entry.origins, key=_order)
        self.stats.messages += 1
        self.stats.repeats_merged += len(origins) - 1
        key = entry.key
        return MergedMessage(
            kind=key.kind,
            scope=key.scope,
            venue_ts_ms=key.venue_ts_ms,
            order_ms=order_ms,
            late=late,
            checkpoint=key.kind == self._checkpoint_kind,
            reconnect_snapshot=entry.reconnect,
            message=entry.message,
            content_digest=key.digest.hex(),
            copies=tuple(sorted({origin.copy for origin in origins})),
            origin=origins[0],
            occurrences=tuple(origins),
        )

    def _resolve(
        self,
        group_key: tuple[int, str],
        group: dict[Stream, list[tuple[MessageKey, Any, Origin, bool]]],
    ) -> list[_Entry]:
        """Union of one (timestamp, scope) group, keeping every stream's order.

        The stream with the most messages (ties: smallest copy/connection) is
        the base. Each other stream is aligned to the merged list by longest
        common subsequence; its unmatched messages are inserted at their
        aligned place, after the base's own unmatched messages in the same gap.
        A content may then appear at most as often as in the stream holding it
        most often; surplus insertions are order disagreements and are dropped.
        """
        for items in group.values():
            self._pending -= len(items)
        streams = sorted(group, key=lambda stream: (-len(group[stream]), stream))
        merged = [
            _Entry(key, message, origin, reconnect)
            for key, message, origin, reconnect in group[streams[0]]
        ]
        for stream in streams[1:]:
            items = group[stream]
            a = [entry.key.digest for entry in merged]
            b = [key.digest for key, _, _, _ in items]
            positions = _is_subsequence(b, a)
            if positions is not None:
                for index, (_, message, origin, reconnect) in zip(positions, items):
                    merged[index].attach(message, origin, reconnect)
                continue
            merged = self._fold(group_key, stream, merged, items, a, b)
        return merged

    def _fold(
        self,
        group_key: tuple[int, str],
        stream: Stream,
        merged: list[_Entry],
        items: list[tuple[MessageKey, Any, Origin, bool]],
        a: list[bytes],
        b: list[bytes],
    ) -> list[_Entry]:
        have, want = Counter(a), Counter(b)
        allowed = {d: want[d] - have[d] for d in want if want[d] > have[d]}
        result: list[_Entry] = []
        only_a: list[int] = []
        only_b: list[int] = []

        def close_gap() -> None:
            kept = []
            for j in only_b:
                digest = b[j]
                if allowed.get(digest, 0) > 0:
                    allowed[digest] -= 1
                    kept.append(j)
                else:
                    self._conflict(group_key, stream, items[j][2])
            if only_a and kept:
                self.stats.ambiguous_insertions += 1
            result.extend(merged[i] for i in only_a)
            for j in kept:
                key, message, origin, reconnect = items[j]
                result.append(_Entry(key, message, origin, reconnect))
            only_a.clear()
            only_b.clear()

        for i, j in _align(a, b):
            if i is not None and j is not None:
                close_gap()
                _, message, origin, reconnect = items[j]
                merged[i].attach(message, origin, reconnect)
                result.append(merged[i])
            elif i is not None:
                only_a.append(i)
            else:
                assert j is not None
                only_b.append(j)
        close_gap()
        return result

    def _conflict(self, group_key: tuple[int, str], stream: Stream, origin: Origin) -> None:
        self.stats.order_conflicts += 1
        example = {
            "venue_ts_ms": group_key[0],
            "scope": group_key[1],
            "copy": stream[0],
            "connection": stream[1],
            "session": origin.session,
            "frame_seq": origin.frame_seq,
            "position": origin.position,
        }
        if self.strict_order:
            raise CopyMergeError(f"streams disagree on order inside a timestamp: {example}")
        if len(self.stats.conflict_examples) < _CONFLICT_EXAMPLES:
            self.stats.conflict_examples.append(example)


# -- reading the raw recorder ----------------------------------------------------


def session_occurrences(
    copy: str,
    connection: str,
    session: str,
    items: Iterable[VenueMessage | RawFrame | ChunkGap],
    *,
    coverage: CoverageTracker | None = None,
) -> Iterator[Occurrence]:
    """Occurrences of one session from ``raw_reader.iter_venue_messages`` output.

    Every decoded venue message (any JSON value) becomes an occurrence; text
    that is not JSON is only counted on ``coverage``. A Polymarket ``book`` is a
    reconnect snapshot when it is the first book of its ``asset_id`` since the
    generation's last sent subscription; every Kalshi ``orderbook_snapshot`` is.
    """
    frame_id: tuple[int, int] | None = None
    received_us = 0
    books_seen: set[Any] = set()
    generation: int | None = None
    for item in items:
        if isinstance(item, ChunkGap):
            if coverage is not None:
                coverage.chunk_gap()
            continue
        frame = item.frame if isinstance(item, VenueMessage) else item
        if (frame.connection_generation, frame.local_frame_seq) != frame_id:
            frame_id = (frame.connection_generation, frame.local_frame_seq)
            received_us = utc_microseconds(frame.received_utc)
            if frame.connection_generation != generation:
                generation = frame.connection_generation
                books_seen.clear()
            if frame.frame_type == "subscription":
                books_seen.clear()
            if coverage is not None:
                coverage.frame(
                    received_us, frame.connection_generation, frame.frame_type, frame.direction
                )
        if not isinstance(item, VenueMessage):
            continue
        if not item.parsed:
            if coverage is not None:
                coverage.unparsed_text()
            continue
        value = item.value
        reconnect = False
        if isinstance(value, dict):
            if frame.venue == "kalshi":
                reconnect = value.get("type") == "orderbook_snapshot"
                sid, seq = value.get("sid"), value.get("seq")
                if coverage is not None and type(sid) is int and type(seq) is int:
                    coverage.sequence(sid, seq)
            elif value.get("event_type") == "book":
                asset = value.get("asset_id")
                reconnect = asset not in books_seen
                books_seen.add(asset)
        yield Occurrence(
            copy,
            connection,
            session,
            frame.connection_generation,
            frame.local_frame_seq,
            item.batch_index,
            received_us,
            value,
            reconnect,
        )


def connection_occurrences(
    copy: str,
    connection: str,
    sessions: Iterable[str | Path],
    *,
    verify: bool = True,
    coverage: list[SessionCoverage] | None = None,
) -> Iterator[Occurrence]:
    """One merge input stream: a connection's sessions in name (start-time) order.

    When ``coverage`` is given, each session's :class:`SessionCoverage` is
    appended to it once that session has been read completely.
    """
    for path in sorted((Path(s) for s in sessions), key=lambda p: p.name):
        tracker = CoverageTracker(copy, connection, path.name)
        frames = iter_venue_messages(read_session(path, verify=verify))
        yield from session_occurrences(
            copy, connection, path.name, frames, coverage=tracker
        )
        if coverage is not None:
            coverage.append(tracker.finish())


def discover_sessions(copies: dict[str, str | Path]) -> dict[Stream, list[Path]]:
    """Map (copy, connection) to session directories under ``COPY/CONNECTION/SESSION``."""
    found: dict[Stream, list[Path]] = {}
    for copy, root in sorted(copies.items()):
        for connection in sorted(p for p in Path(root).iterdir() if p.is_dir()):
            sessions = sorted(p for p in connection.iterdir() if p.is_dir())
            if sessions:
                found[(copy, connection.name)] = sessions
    return found
