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
3. Within one venue timestamp and one market, the received order is kept;
   the order between book changes is settled before trades and other messages.
4. Full books (Polymarket ``book``, Kalshi ``orderbook_snapshot``) are flagged
   as checkpoints and placed at their own timestamp like any other message.
   Kalshi snapshots carry no timestamp; they are placed by their ``clock``,
   just before the first delta of their market with a higher clock.

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
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Sequence
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
# Messages that change a book. Inside one group, their received order is
# applied before the order of other messages (trades, quotes, lifecycle).
BOOK_CHANGE_KINDS = {
    "polymarket": frozenset({"price_change", "book"}),
    "kalshi": frozenset({"orderbook_delta", "orderbook_snapshot"}),
}
# Price-level fields of a full book (Kalshi: inside ``msg``, any spelling).
_LADDER_FIELDS = {
    "book": ("bids", "asks"),
    "orderbook_snapshot": (
        "yes_dollars_fp", "yes_dollars", "yes", "no_dollars_fp", "no_dollars", "no",
    ),
}
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

    @property
    def snapshot_class(self) -> str | None:
        """None for a message that is not a full book; otherwise one of
        "reconnect" (first of its instrument after a sent subscription in some
        occurrence), "empty" (no price levels, e.g. Kalshi's snapshot when a
        market is determined) or "later" (any other full book)."""
        if not self.checkpoint:
            return None
        if self.reconnect_snapshot:
            return "reconnect"
        body = self.message.get("msg") if self.kind == "orderbook_snapshot" else self.message
        fields = _LADDER_FIELDS.get(self.kind, ())
        if isinstance(body, dict) and not any(body.get(name) for name in fields):
            return "empty"
        return "later"


@dataclass
class MergeStats:
    occurrences: int = 0
    messages: int = 0
    # Occurrences attached to a message another occurrence already created.
    repeats_merged: int = 0
    untimed_messages: int = 0
    late_messages: int = 0
    # Of late_messages: venue time more than lateness_ms ahead of receipt.
    future_messages: int = 0
    # Occurrences dropped because their content was already emitted.
    repeats_after_emit: int = 0
    # Order constraints of a stream that contradicted stronger evidence and
    # were ignored (the occurrence itself is still attached to its message):
    # the total, and split into pairs of two book changes and all other pairs.
    order_conflicts: int = 0
    book_order_conflicts: int = 0
    other_order_conflicts: int = 0
    # Times when more than one message could come next inside a group, so the
    # order between them is a convention (see docs/copy_merge.md).
    ambiguous_choices: int = 0
    clock_steps_back: int = 0
    max_pending_occurrences: int = 0
    # Kalshi snapshots placed by their clock, and those placed at their
    # sending time instead, by reason: "zero" (clock 0, an empty book),
    # "missing" (no integer clock) or "decreased" (clock below the last book
    # message of the market already emitted). Late snapshots are in neither.
    snapshots_by_clock: int = 0
    snapshot_clock_fallbacks: Counter[str] = field(default_factory=Counter)
    # Emitted Kalshi deltas whose clock is below the previous emitted delta of
    # their market, or not above a clock-placed snapshot emitted before them
    # (exchange maintenance, or venue times that disagree with the clock).
    clock_regressions: int = 0
    late_by_copy: Counter[str] = field(default_factory=Counter)
    conflict_examples: list[dict[str, Any]] = field(default_factory=list)


def _ms(value: Any) -> int | None:
    if type(value) is int:
        return value if value >= 0 else None
    if isinstance(value, str) and value.isascii() and value.isdigit():
        return int(value)
    return None


def _clock(message: Any) -> int | None:
    """Kalshi ``msg.clock`` of a book message, or None if it has no integer clock."""
    msg = message.get("msg") if isinstance(message, dict) else None
    clock = msg.get("clock") if isinstance(msg, dict) else None
    return clock if type(clock) is int and clock >= 0 else None


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


# One pending timed occurrence: key, message, origin, reconnect flag, input index.
_Item = tuple[MessageKey, Any, Origin, bool, int]


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

    __slots__ = ("order_ms", "streams", "heap_id", "positions", "clock")

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
        index: int,
    ) -> None:
        super().__init__(key, message, origin, reconnect)
        self.order_ms = order_ms
        self.streams = {stream}
        self.heap_id = heap_id
        # (stream, input index, origin) of every occurrence, for ordering.
        self.positions = [(stream, index, origin)]
        # Kalshi snapshot placed by its clock (docs/copy_merge.md), else None.
        self.clock: int | None = None


class _Emitted:
    """Remembered untimed message: its matching time and the streams it absorbed."""

    __slots__ = ("at", "order_ms", "streams")

    def __init__(self, at: int, order_ms: int, streams: set[Stream]) -> None:
        self.at, self.order_ms, self.streams = at, order_ms, streams


def _reaches(successors: dict[Any, set[Any]], start: Any, target: Any) -> bool:
    stack, seen = [start], {start}
    while stack:
        node = stack.pop()
        if node == target:
            return True
        for following in successors[node]:
            if following not in seen:
                seen.add(following)
                stack.append(following)
    return False


def _order_nodes(
    sequences: list[tuple[Stream, list[tuple[Any, Origin]]]],
    is_checkpoint: Callable[[Any], bool],
    is_book_change: Callable[[Any], bool],
    fixed: list[tuple[Any, Any]],
    on_conflict: Callable[[Stream, Origin, bool], None],
) -> tuple[list[Any], int]:
    """One order of all nodes that keeps every stream's order where possible.

    ``sequences`` are the streams in rank order, each a list of (node, origin)
    in received order. Constraints ("a before b") are added by strength:

    1. ``fixed``: pairs that must hold (Kalshi clock order); never cycle.
    2. For each stream in rank order, its consecutive book changes (other
       messages between them skipped), so that book-change order is settled by
       the evidence of all streams before any trade is placed.
    3. For each stream in rank order, its remaining consecutive pairs.

    A constraint of steps 2 or 3 that would form a cycle with those already
    added is skipped and reported through ``on_conflict(stream, origin,
    book_pair)``; the stream's next constraint still starts from that node.
    The nodes are then sorted topologically. When several nodes could come
    next, a non-checkpoint comes before a checkpoint (a full book includes the
    changes stamped with its timestamp), then the node first seen in the
    highest-ranked stream. Returns the order and the number of such choices.
    """
    rank: dict[Any, tuple[int, int]] = {}
    for stream_rank, (_, sequence) in enumerate(sequences):
        for position, (node, _) in enumerate(sequence):
            rank.setdefault(node, (stream_rank, position))
    successors: dict[Any, set[Any]] = {node: set() for node in rank}
    indegree = dict.fromkeys(rank, 0)

    def add(before: Any, after: Any) -> bool:
        if after in successors[before]:
            return True
        if _reaches(successors, after, before):
            return False
        successors[before].add(after)
        indegree[after] += 1
        return True

    for before, after in fixed:
        if not add(before, after):  # pragma: no cover - clock pairs are acyclic
            raise AssertionError("cycle in fixed merge order constraints")
    for stream, sequence in sequences:
        previous = None
        for node, origin in sequence:
            if not is_book_change(node):
                continue
            if previous is not None and not add(previous, node):
                on_conflict(stream, origin, True)
            previous = node
    for stream, sequence in sequences:
        previous, previous_book = None, False
        for node, origin in sequence:
            book = is_book_change(node)
            # Pairs of two book changes were added (or rejected) above.
            if previous is not None and not (book and previous_book):
                if not add(previous, node):
                    on_conflict(stream, origin, False)
            previous, previous_book = node, book
    by_key = {(bool(is_checkpoint(node)), rank[node]): node for node in rank}
    ready = [key for key, node in by_key.items() if indegree[node] == 0]
    heapq.heapify(ready)
    order, choices = [], 0
    while ready:
        if len(ready) > 1:
            choices += 1
        node = by_key[heapq.heappop(ready)]
        order.append(node)
        for following in successors[node]:
            indegree[following] -= 1
            if indegree[following] == 0:
                heapq.heappush(ready, (bool(is_checkpoint(following)), rank[following]))
    if len(order) != len(rank):  # pragma: no cover - edges never form a cycle
        raise AssertionError("cycle in merge order constraints")
    return order, choices


class CopyMerge:
    """Streaming merge of one venue's copies; run :meth:`merge` once.

    ``lateness_ms``: an occurrence received (by its own copy's clock) more than
    this after its venue timestamp is late, see docs/copy_merge.md.
    ``untimed_match_ms``: matching distance for messages without venue time.
    ``retention_ms``: how long emitted content is remembered for late repeats.
    ``strict_order``: raise :class:`CopyMergeError` when two streams order the
    same messages differently inside one group, instead of counting it.
    ``preferred_copies``: the caller's preference among copies, most preferred
    first. Inside a group it ranks streams after the number of the group's
    messages they hold and before the copy name; it decides only what the
    clock, book-change order and the other streams leave open.
    """

    def __init__(
        self,
        venue: str,
        *,
        lateness_ms: int = DEFAULT_LATENESS_MS,
        untimed_match_ms: int = DEFAULT_UNTIMED_MATCH_MS,
        retention_ms: int = DEFAULT_RETENTION_MS,
        strict_order: bool = False,
        preferred_copies: Sequence[str] = (),
    ) -> None:
        if venue not in VENUES:
            raise ValueError(f"unsupported venue: {venue!r}")
        if lateness_ms <= 0 or untimed_match_ms < 0 or retention_ms < 0:
            raise ValueError("lateness_ms must be positive; other windows non-negative")
        if isinstance(preferred_copies, str) or len(set(preferred_copies)) != len(
            preferred_copies
        ):
            raise ValueError("preferred_copies must be a sequence of distinct copy names")
        self.venue = venue
        self.lateness_ms = lateness_ms
        self.untimed_match_ms = untimed_match_ms
        self.retention_ms = retention_ms
        self.strict_order = strict_order
        self.preferred_copies = tuple(preferred_copies)
        self._preference = {copy: i for i, copy in enumerate(self.preferred_copies)}
        self.stats = MergeStats()
        self._checkpoint_kind = CHECKPOINT_KINDS[venue]
        self._book_change_kinds = BOOK_CHANGE_KINDS[venue]
        self._started = False
        # Pending timed groups keyed (venue_ts_ms, scope): per stream, the
        # occurrences in received order with their input index.
        self._groups: dict[tuple[int, str], dict[Stream, list[_Item]]] = {}
        self._untimed: dict[bytes, list[_Untimed]] = {}
        # Kalshi: pending snapshots placed by clock, per market; and per market
        # the clock of the last emitted book message and whether it was a
        # snapshot (one small entry per market seen).
        self._clocked: dict[str, list[_Untimed]] = {}
        self._last_clock: dict[str, tuple[int, bool]] = {}
        # (order_ms, scope, rank, id, ref): rank 0 = timed group, 1 = untimed.
        self._heap: list[tuple[int, str, int, int, Any]] = []
        self._ids = itertools.count(1)
        self._pending = 0
        # Emitted content, remembered until the watermark is retention_ms past
        # the later of its order time and its own (venue or sending) time.
        self._emitted_timed: dict[bytes, int] = {}
        self._emitted_untimed: dict[bytes, list[_Emitted]] = {}
        self._expiry: list[tuple[int, int, bytes, _Emitted | None]] = []

    # -- driver ---------------------------------------------------------------

    def merge(self, streams: Iterable[Iterable[Occurrence]]) -> Iterator[MergedMessage]:
        """Yield merged messages in order. ``self.stats`` is final when exhausted."""
        if self._started:
            raise RuntimeError("a CopyMerge instance runs only once")
        self._started = True
        for index, (occurrence, received) in enumerate(self._interleave(streams)):
            watermark = received // 1000 - self.lateness_ms
            yield from self._flush(watermark)
            self._expire(watermark)
            yield from self._add(occurrence, received // 1000, watermark, index)
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

    def _add(
        self, occurrence: Occurrence, received_ms: int, watermark: int, index: int
    ) -> Iterator[MergedMessage]:
        key = message_key(self.venue, occurrence.message)
        stream = (occurrence.copy, occurrence.connection)
        origin = occurrence.origin()
        self.stats.occurrences += 1
        if key.venue_ts_ms is None:
            yield from self._add_untimed(
                key, stream, occurrence, origin, received_ms, watermark, index
            )
            return
        # A venue time far ahead of receipt (a wrong clock) would stay pending
        # until the receive clock caught up; handle it like a late message so
        # that memory stays bounded by the lateness window.
        future = key.venue_ts_ms > received_ms + self.lateness_ms
        if key.venue_ts_ms < watermark or future or key.digest in self._emitted_timed:
            # The last case: content placed early as a future message.
            if key.digest in self._emitted_timed:
                self.stats.repeats_after_emit += 1
                return
            self._remember(key.digest, max(watermark, key.venue_ts_ms), None)
            self.stats.future_messages += future
            yield self._late(key, occurrence, origin, watermark)
            return
        group_key = (key.venue_ts_ms, key.scope)
        group = self._groups.get(group_key)
        if group is None:
            group = self._groups[group_key] = {}
            heapq.heappush(self._heap, (key.venue_ts_ms, key.scope, 0, 0, group_key))
        group.setdefault(stream, []).append(
            (key, occurrence.message, origin, occurrence.reconnect_snapshot, index)
        )
        self._grow()

    def _add_untimed(
        self,
        key: MessageKey,
        stream: Stream,
        occurrence: Occurrence,
        origin: Origin,
        received_ms: int,
        watermark: int,
        index: int,
    ) -> Iterator[MergedMessage]:
        at = key.sent_ms if key.sent_ms is not None else received_ms
        tolerance = self.untimed_match_ms
        for pending in self._untimed.get(key.digest, ()):
            if stream not in pending.streams and abs(at - pending.order_ms) <= tolerance:
                pending.attach(occurrence.message, origin, occurrence.reconnect_snapshot)
                pending.streams.add(stream)
                pending.positions.append((stream, index, origin))
                self._grow()
                if watermark <= at < pending.order_ms:
                    pending.order_ms, pending.heap_id = at, next(self._ids)
                    heapq.heappush(
                        self._heap, (at, key.scope, 1, pending.heap_id, pending)
                    )
                return
        for emitted in self._emitted_untimed.get(key.digest, ()):
            if stream not in emitted.streams and abs(at - emitted.at) <= tolerance:
                # Already emitted: this stream's occurrence is absorbed, so a
                # second one from the same stream is a genuine repeat.
                emitted.streams.add(stream)
                self.stats.repeats_after_emit += 1
                return
        future = at > received_ms + self.lateness_ms
        if at < watermark or future:
            self._remember(key.digest, max(watermark, at), _Emitted(at, watermark, {stream}))
            self.stats.untimed_messages += 1
            self.stats.future_messages += future
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
            index=index,
        )
        if self.venue == "kalshi" and key.kind == "orderbook_snapshot":
            entry.clock = self._snapshot_clock(key.scope, occurrence.message)
            if entry.clock is not None:
                self._clocked.setdefault(key.scope, []).append(entry)
        self._untimed.setdefault(key.digest, []).append(entry)
        heapq.heappush(self._heap, (at, key.scope, 1, entry.heap_id, entry))
        self._grow()

    def _snapshot_clock(self, scope: str, message: Any) -> int | None:
        """The clock that places this Kalshi snapshot, or None to place it at sending time.

        Decided when the snapshot arrives, from its content and the market's
        already emitted messages, which do not depend on other markets.
        """
        clock = _clock(message)
        last = self._last_clock.get(scope)
        if clock is None:
            reason = "missing"
        elif clock == 0:
            reason = "zero"
        elif last is not None and clock < last[0]:
            reason = "decreased"
        else:
            self.stats.snapshots_by_clock += 1
            return clock
        self.stats.snapshot_clock_fallbacks[reason] += 1
        return None

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
            # Everything placed at this (millisecond, scope) is ordered
            # together: the timed group (rank 0) and untimed messages (rank 1).
            group = None
            batch: list[_Untimed] = []
            while True:
                if rank == 0:
                    group = self._groups.pop(ref)
                elif ref.heap_id == heap_id:  # else superseded by an earlier placement
                    batch.append(ref)
                if not heap or heap[0][:2] != (order_ms, scope):
                    break
                order_ms, scope, rank, heap_id, ref = heapq.heappop(heap)
            if group is not None and scope in self._clocked:
                batch.extend(self._placed_by_clock(scope, group, batch))
            if group is None and not batch:
                # Only superseded placements of messages emitted earlier.
                continue
            for entry in self._resolve((order_ms, scope), group or {}, batch):
                if isinstance(entry, _Untimed):
                    siblings = self._untimed[entry.key.digest]
                    siblings.remove(entry)
                    if not siblings:
                        del self._untimed[entry.key.digest]
                    if entry.clock is not None:
                        clocked = self._clocked[scope]
                        clocked.remove(entry)
                        if not clocked:
                            del self._clocked[scope]
                    self._pending -= len(entry.origins)
                    self.stats.untimed_messages += 1
                    # Matching later occurrences uses the sending time, even
                    # when the clock placed the snapshot earlier.
                    record = _Emitted(entry.order_ms, order_ms, set(entry.streams))
                    self._remember(entry.key.digest, order_ms, record)
                else:
                    self._remember(entry.key.digest, order_ms, None)
                yield self._message(entry, order_ms, late=False)

    def _placed_by_clock(
        self, scope: str, group: dict[Stream, list[_Item]], batch: list[_Untimed]
    ) -> list[_Untimed]:
        """Pending snapshots of ``scope`` that belong in this timed group.

        A snapshot goes into the first group of its market (in merged order)
        that holds a delta with a higher clock, before that delta. If no such
        group comes before its sending time, it stays at its sending time.
        """
        clocks = [
            clock
            for items in group.values()
            for key, message, *_ in items
            if key.kind == "orderbook_delta" and (clock := _clock(message)) is not None
        ]
        if not clocks:
            return []
        newest = max(clocks)
        placed = []
        for entry in self._clocked[scope]:
            if entry.clock is not None and entry.clock < newest and entry not in batch:
                entry.heap_id = 0  # its entry at the sending time is now superseded
                placed.append(entry)
        return placed

    def _track_clock(self, entry: _Entry) -> None:
        """Count Kalshi deltas emitted out of clock order (``clock_regressions``)."""
        scope = entry.key.scope
        if entry.key.kind == "orderbook_delta":
            clock = _clock(entry.message)
            if clock is None:
                return
            last = self._last_clock.get(scope)
            if last is not None and (clock < last[0] or (last[1] and clock == last[0])):
                self.stats.clock_regressions += 1
            self._last_clock[scope] = (clock, False)
        elif isinstance(entry, _Untimed) and entry.clock is not None:
            self._last_clock[scope] = (entry.clock, True)

    def _remember(self, digest: bytes, until_ms: int, record: _Emitted | None) -> None:
        if record is None:
            self._emitted_timed[digest] = until_ms
        else:
            self._emitted_untimed.setdefault(digest, []).append(record)
        heapq.heappush(self._expiry, (until_ms, next(self._ids), digest, record))

    def _expire(self, watermark: int) -> None:
        limit = watermark - self.retention_ms
        while self._expiry and self._expiry[0][0] < limit:
            until_ms, _, digest, record = heapq.heappop(self._expiry)
            if record is None:
                if self._emitted_timed.get(digest) == until_ms:
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
        if self.venue == "kalshi" and key.kind in self._book_change_kinds:
            self._track_clock(entry)
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
        group: dict[Stream, list[_Item]],
        untimed: list[_Untimed],
    ) -> list[_Entry]:
        """Union of everything placed at one (millisecond, scope), in order.

        A timed message is identified by (digest, k): the k-th occurrence of a
        content in one stream is the same message as the k-th in another, so a
        content is kept as often as the stream holding it most often. Untimed
        messages were already matched across streams by time. Streams are
        ranked by how many of these messages they hold, then by the caller's
        ``preferred_copies``, then by (copy, connection). The order is a
        topological sort of the Kalshi clock pairs and all streams' received
        orders (see :func:`_order_nodes`).
        """
        for items in group.values():
            self._pending -= len(items)
        entries: dict[Any, _Entry] = {}
        per_stream: dict[Stream, list[tuple[int, Any, Origin]]] = {}
        for stream, items in group.items():
            seen: Counter[bytes] = Counter()
            sequence = per_stream.setdefault(stream, [])
            for key, message, origin, reconnect, index in items:
                node = (key.digest, seen[key.digest])
                seen[key.digest] += 1
                if node in entries:
                    entries[node].attach(message, origin, reconnect)
                else:
                    entries[node] = _Entry(key, message, origin, reconnect)
                sequence.append((index, node, origin))
        for entry in untimed:
            entries[entry] = entry
            for stream, index, origin in entry.positions:
                per_stream.setdefault(stream, []).append((index, entry, origin))
        unlisted = len(self._preference)
        streams = sorted(
            per_stream,
            key=lambda stream: (
                -len(per_stream[stream]),
                self._preference.get(stream[0], unlisted),
                stream,
            ),
        )
        sequences = []
        for stream in streams:
            sequence = per_stream[stream]
            if untimed:
                sequence.sort(key=lambda item: item[0])
            sequences.append((stream, [(node, origin) for _, node, origin in sequence]))
        fixed = self._clock_pairs(entries, untimed)
        # Fast path: every stream follows the top-ranked stream's order.
        top = {node: position for position, (node, _) in enumerate(sequences[0][1])}
        if not fixed and all(
            all(node in top for node, _ in sequence)
            and all(top[a] < top[b] for (a, _), (b, _) in zip(sequence, sequence[1:]))
            for _, sequence in sequences[1:]
        ):
            return [entries[node] for node, _ in sequences[0][1]]
        order, choices = _order_nodes(
            sequences,
            lambda node: entries[node].key.kind == self._checkpoint_kind,
            lambda node: entries[node].key.kind in self._book_change_kinds,
            fixed,
            lambda stream, origin, book: self._conflict(group_key, stream, origin, book),
        )
        self.stats.ambiguous_choices += choices
        return [entries[node] for node in order]

    @staticmethod
    def _clock_pairs(
        entries: dict[Any, _Entry], untimed: list[_Untimed]
    ) -> list[tuple[Any, Any]]:
        """Kalshi clock order: each delta with clock <= a snapshot's clock before it, others after."""
        snapshots = [entry for entry in untimed if entry.clock is not None]
        if not snapshots:
            return []
        deltas = [
            (node, clock)
            for node, entry in entries.items()
            if entry.key.kind == "orderbook_delta" and (clock := _clock(entry.message)) is not None
        ]
        return [
            (node, snapshot) if clock <= snapshot.clock else (snapshot, node)
            for snapshot in snapshots
            if snapshot.clock is not None
            for node, clock in deltas
        ]

    def _conflict(
        self, group_key: tuple[int, str], stream: Stream, origin: Origin, book_pair: bool
    ) -> None:
        self.stats.order_conflicts += 1
        if book_pair:
            self.stats.book_order_conflicts += 1
        else:
            self.stats.other_order_conflicts += 1
        example = {
            "venue_ts_ms": group_key[0],
            "scope": group_key[1],
            "copy": stream[0],
            "connection": stream[1],
            "session": origin.session,
            "frame_seq": origin.frame_seq,
            "position": origin.position,
            "book_changes": book_pair,
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
    frames: Iterable[RawFrame | ChunkGap],
    *,
    coverage: CoverageTracker | None = None,
) -> Iterator[Occurrence]:
    """Occurrences of one session from ``raw_reader.read_session`` output.

    Every frame is shown to ``coverage`` before it is decoded (an empty JSON
    list still proves the connection was alive). Every decoded venue message
    (any JSON value) becomes an occurrence; text that is not JSON is only
    counted on ``coverage``. A full book is a reconnect snapshot when it is the
    first of its instrument (Polymarket ``asset_id``, Kalshi ``market_ticker``)
    since the generation's last sent subscription.

    Kalshi snapshots are placed by ``clock``, which holds only for real-time
    subscriptions; a sent subscription with ``update_interval_ms`` (conflated
    mode) raises :class:`CopyMergeError`.
    """
    books_seen: set[Any] = set()
    generation: int | None = None
    for frame in frames:
        if isinstance(frame, ChunkGap):
            if coverage is not None:
                coverage.chunk_gap()
            continue
        received_us = utc_microseconds(frame.received_utc)
        if frame.connection_generation != generation:
            generation = frame.connection_generation
            books_seen.clear()
        if frame.frame_type == "subscription":
            books_seen.clear()
            if frame.venue == "kalshi" and "update_interval_ms" in frame.payload_raw:
                raise CopyMergeError(
                    f"{copy}/{connection}/{session}: conflated Kalshi subscription "
                    "(update_interval_ms) is not supported by the clock placement"
                )
        if coverage is not None:
            coverage.frame(
                received_us, frame.connection_generation, frame.frame_type, frame.direction
            )
        for item in iter_venue_messages((frame,)):
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
                    msg = value.get("msg")
                    if value.get("type") == "orderbook_snapshot" and isinstance(msg, dict):
                        ticker = msg.get("market_ticker")
                        reconnect = ticker not in books_seen
                        books_seen.add(ticker)
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
        tracker = CoverageTracker(copy, connection, path.name) if coverage is not None else None
        yield from session_occurrences(
            copy, connection, path.name, read_session(path, verify=verify), coverage=tracker
        )
        if coverage is not None and tracker is not None:
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
