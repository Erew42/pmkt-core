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

import json
from collections import Counter
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pmkt import _native
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
        return _native.snapshot_class(self.kind, self.reconnect_snapshot, json.dumps(self.message))


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
    # "missing" (no integer clock) or "decreased" (a reset observed in the
    # input stream, or a clock below a book message already emitted in the
    # same epoch). Late snapshots are in neither.
    snapshots_by_clock: int = 0
    snapshot_clock_fallbacks: Counter[str] = field(default_factory=Counter)
    # Emitted Kalshi deltas whose clock is below the previous emitted delta of
    # their market, or not above a clock-placed snapshot emitted before them
    # (exchange maintenance, or venue times that disagree with the clock).
    clock_regressions: int = 0
    late_by_copy: Counter[str] = field(default_factory=Counter)
    conflict_examples: list[dict[str, Any]] = field(default_factory=list)


def message_key(venue: str, message: Any) -> MessageKey:
    """Identity, ordering scope and venue time of one decoded venue message."""
    kind, scope, venue_ms, sent_ms, digest = _native.message_key(venue, json.dumps(message))
    return MessageKey(kind, scope, venue_ms, sent_ms, bytes(digest))


def _native_occurrences(source: Iterable[Occurrence]) -> Iterator[_native._MergeInput]:
    for item in source:
        yield (
            (item.copy, item.connection), item.session, item.generation, item.frame_seq,
            item.position, item.received_us, json.dumps(item.message), item.reconnect_snapshot,
        )


def _origin(data: dict[str, Any]) -> Origin:
    return Origin(
        data["stream"]["copy"], data["stream"]["connection"], data["session"],
        data["generation"], data["frame_seq"], data["position"], data["received_us"],
    )


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
        self.stats = MergeStats()
        self._started = False

    def merge(self, streams: Iterable[Iterable[Occurrence]]) -> Iterator[MergedMessage]:
        """Yield merged messages in order. ``self.stats`` is final when exhausted."""
        if self._started:
            raise RuntimeError("a CopyMerge instance runs only once")
        self._started = True
        native = None
        try:
            native = _native.CopyMerge(
                self.venue, [_native_occurrences(source) for source in streams],
                self.lateness_ms, self.untimed_match_ms, self.retention_ms,
                self.strict_order, self.preferred_copies,
            )
            for encoded in native:
                row = json.loads(encoded)
                origins = tuple(_origin(item) for item in row["occurrences"])
                yield MergedMessage(**{
                    **row, "origin": origins[0], "occurrences": origins,
                    "copies": tuple(row["copies"]),
                })
        except _native.MergeError as exc:
            raise CopyMergeError(str(exc)) from exc
        finally:
            if native is not None:
                stats = json.loads(native.stats())
                stats["order_conflicts"] = stats["book_order_conflicts"] + stats["other_order_conflicts"]
                stats["late_by_copy"] = Counter(stats["late_by_copy"])
                stats["snapshot_clock_fallbacks"] = Counter(stats["snapshot_clock_fallbacks"])
                stats["conflict_examples"] = [
                    {
                        "venue_ts_ms": item["order_ms"], "scope": item["scope"],
                        "copy": origin.copy, "connection": origin.connection,
                        "session": origin.session, "frame_seq": origin.frame_seq,
                        "position": origin.position, "book_changes": item["book_changes"],
                    }
                    for item in stats["conflict_examples"]
                    for origin in (_origin(item["origin"]),)
                ]
                self.stats.__dict__.update(stats)


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
    subscriptions; a sent subscription with a nonzero ``update_interval_ms``
    (conflated mode) raises :class:`CopyMergeError`.
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
            if frame.venue == "kalshi" and _native.conflated_subscription(frame.payload_raw):
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
