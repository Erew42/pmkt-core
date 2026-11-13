"""When was each recorded copy of a public WebSocket feed connected?

A *copy* is one machine's recording; a *connection* is one recorder market list
(``COPY/CONNECTION/SESSION`` on disk). For each session this module reports the
receive-time intervals in which the copy held an uninterrupted stream, and
:func:`uncovered_intervals` turns the intervals of several copies into the
periods that no copy covered. It reports; it never drops or invents messages.

An interval opens at the first received frame (text, pong, control) of a
connection generation and is extended by every later received frame. It ends at
the last received frame before one of:

- ``new_generation``: the recorder reconnected (the next generation re-subscribes);
- ``transport_error``: the recorder's local marker for a failed connection;
- ``disk_pause_gap``: frames were dropped while the disk was low;
- ``chunk_gap``: chunks are missing from the session (``raw_reader.ChunkGap``);
- ``seq_gap``: a Kalshi ``seq`` did not follow its predecessor on the same
  ``sid`` in the same generation (detected here; the recorder's ``seq_gap``
  marker frames are only counted);
- ``session_end``: the session's last available frame.

Times are the recorder's ``received_utc`` in integer microseconds since the
epoch: one machine's clock, not exchange time. A received frame proves the
connection was alive, not that the venue sent nothing else: Polymarket has no
sequence numbers, so a Polymarket interval is a statement about connectivity
only. Silence inside an interval (for example the up to 25 s before a receive
timeout) is counted as covered.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def utc_microseconds(text: str) -> int:
    """Parse a recorder ``received_utc`` (ISO 8601 with offset) to epoch microseconds."""
    value = datetime.fromisoformat(text)
    if value.tzinfo is None:
        raise ValueError(f"timestamp without UTC offset: {text!r}")
    delta = value - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


@dataclass(frozen=True)
class CoverageInterval:
    """Receive-time span ``[start_us, end_us]`` of one uninterrupted stream."""

    copy: str
    connection: str
    session: str
    generation: int
    start_us: int
    end_us: int
    # session_start, new_generation or after_gap (same generation, after a gap).
    start_reason: str
    end_reason: str


@dataclass(frozen=True)
class SessionCoverage:
    copy: str
    connection: str
    session: str
    intervals: tuple[CoverageInterval, ...]
    chunk_gaps: int
    seq_gaps: int
    seq_gap_markers: int
    disk_pause_markers: int
    transport_errors: int
    # Received text that is not JSON; the merge adapter does not forward it.
    unparsed_text_frames: int


class CoverageTracker:
    """Incremental coverage of one session, fed frame by frame.

    Call :meth:`frame` once per recorded frame in file order, then
    :meth:`sequence` for every Kalshi message of that frame that carries an
    integer ``sid`` and ``seq``. Call :meth:`chunk_gap` where chunks are
    missing. The calls only need the recorded fields, so a caller reading
    another storage format (for example Parquet tables of the same frames) can
    drive it directly.
    """

    def __init__(self, copy: str, connection: str, session: str) -> None:
        self.copy, self.connection, self.session = copy, connection, session
        self._intervals: list[CoverageInterval] = []
        self._generation: int | None = None
        self._start: int | None = None
        self._last: int | None = None
        # End of the open interval before the current frame, and that frame's time.
        self._before: int | None = None
        self._frame_us: int | None = None
        self._start_reason = "session_start"
        self._next_reason = "session_start"
        self._seqs: dict[int, int] = {}
        self.chunk_gaps = self.seq_gaps = self.seq_gap_markers = 0
        self.disk_pause_markers = self.transport_errors = 0
        self.unparsed_text_frames = 0

    def frame(
        self, received_us: int, generation: int, frame_type: str, direction: str
    ) -> None:
        if generation != self._generation:
            if self._generation is not None:
                self._close("new_generation")
                self._next_reason = "new_generation"
            self._generation = generation
            self._seqs.clear()
        if direction == "local":
            if frame_type == "transport_error":
                self.transport_errors += 1
                self._close("transport_error")
            elif frame_type == "disk_pause_gap":
                self.disk_pause_markers += 1
                self._close("disk_pause_gap")
                self._next_reason = "after_gap"
                self._seqs.clear()
            elif frame_type == "seq_gap":
                self.seq_gap_markers += 1
            return
        if not direction.startswith("received"):
            return  # sent subscriptions and heartbeats prove nothing about receipt
        self._frame_us = received_us
        if self._start is None:
            self._start, self._last, self._before = received_us, received_us, None
            self._start_reason = self._next_reason
            return
        assert self._last is not None
        self._before = self._last
        self._last = max(self._last, received_us)

    def sequence(self, sid: int, seq: int) -> None:
        last = self._seqs.get(sid)
        self._seqs[sid] = seq
        if last is None or seq == last + 1:
            return
        self.seq_gaps += 1
        if self._start is None or self._frame_us is None:
            return
        # The loss lies between the previous received frame and this one. For a
        # jump inside one batch frame this is conservative by one frame.
        end = self._before if self._before is not None else self._start
        self._close("seq_gap", end=end)
        self._start = self._last = self._frame_us
        self._before = None
        self._start_reason = "after_gap"

    def unparsed_text(self) -> None:
        self.unparsed_text_frames += 1

    def chunk_gap(self) -> None:
        self.chunk_gaps += 1
        self._close("chunk_gap")
        self._next_reason = "after_gap"
        self._seqs.clear()

    def finish(self) -> SessionCoverage:
        self._close("session_end")
        return SessionCoverage(
            self.copy,
            self.connection,
            self.session,
            tuple(self._intervals),
            self.chunk_gaps,
            self.seq_gaps,
            self.seq_gap_markers,
            self.disk_pause_markers,
            self.transport_errors,
            self.unparsed_text_frames,
        )

    def _close(self, reason: str, *, end: int | None = None) -> None:
        if self._start is None or self._generation is None:
            return
        stop = self._last if end is None else end
        assert stop is not None
        self._intervals.append(
            CoverageInterval(
                self.copy,
                self.connection,
                self.session,
                self._generation,
                self._start,
                stop,
                self._start_reason,
                reason,
            )
        )
        self._start = self._last = self._before = None
        self._next_reason = "after_gap"


def uncovered_intervals(
    intervals: Iterable[CoverageInterval], start_us: int, end_us: int
) -> list[tuple[int, int]]:
    """Open spans ``(a, b)`` inside ``[start_us, end_us]`` that no interval covers.

    Pass the intervals of every copy and connection that carry the same
    instruments; this function does not know which connections those are.
    """
    if end_us < start_us:
        raise ValueError("end_us is before start_us")
    holes: list[tuple[int, int]] = []
    cursor = start_us
    for begin, finish in sorted((i.start_us, i.end_us) for i in intervals):
        if finish < cursor:
            continue
        if begin > cursor:
            holes.append((cursor, min(begin, end_us)))
        cursor = max(cursor, finish)
        if cursor >= end_us:
            break
    if cursor < end_us:
        holes.append((cursor, end_us))
    return [(a, b) for a, b in holes if a < b]
