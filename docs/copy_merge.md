# Merging recorded copies

Several machines may record the same public feeds with the raw recorder
([raw_capture_contract.md](raw_capture_contract.md)), so that a disconnect on
one machine can be bridged from another. Each machine's recording is a *copy*.
`pmkt.streaming.copy_merge` turns the copies of one venue into one stream in
which every venue message appears once. `pmkt.streaming.copy_coverage` reports
when each copy was connected and which periods no copy covered.
`pmkt.streaming.copy_merge_books` rebuilds books from a merged stream and
compares them with the full books in it. All three are standard library plus
core only, read local data, and make no network calls.

## Usage

```python
from pmkt.streaming.copy_coverage import uncovered_intervals
from pmkt.streaming.copy_merge import CopyMerge, connection_occurrences, discover_sessions

found = discover_sessions({"pc": "/data/pc", "vm1": "/data/vm1"})  # COPY/CONNECTION/SESSION
coverage = []
streams = [
    connection_occurrences(copy, connection, sessions, coverage=coverage)
    for (copy, connection), sessions in found.items()
    if connection.startswith("polymarket_")
]
merger = CopyMerge("polymarket")
for message in merger.merge(streams):
    message.order_ms, message.kind, message.scope, message.message
    message.copies, message.origin, message.late, message.checkpoint
print(merger.stats)  # complete once the iterator is exhausted
holes = uncovered_intervals(
    [i for s in coverage if s.connection in {"polymarket_00", "polymarket_02"} for i in s.intervals],
    start_us, end_us,
)
```

One input stream is one (copy, connection): its sessions in start order, each
in recorded order. A stream is any iterable of `Occurrence` (decoded venue
message plus copy, connection, session, generation, frame number, position in
a batch frame and receive time in microseconds), so a caller can feed it from
another store holding the same fields. Every copy must be decoded the same way,
since identity is computed from the decoded value. `CoverageTracker` takes the
same frame fields call by call. Streams are pulled lazily and interleaved by
receive time; memory holds the lateness window, not the recording.

## The four rules and how they are implemented

1. **Union by content.** Identity is a 128-bit BLAKE2b digest of the message
   serialised as JSON with sorted keys. For Kalshi the envelope fields `sid`,
   `seq` and `sending_ts_ms` are left out, since every connection numbers and
   stamps them itself. Within one group (rule 3) a content is kept as many times
   as the stream holding it most often received it, so a venue repeat survives
   and a copy of it in a second stream does not add one. A market received on
   two connections of one copy (Polymarket Yes/No split) is two streams and is
   merged like two copies.
2. **Order by venue time.** Polymarket `timestamp`; Kalshi `msg.ts_ms`. Each
   merged message carries the earliest receive time over its occurrences
   (`origin`, `first_received_us`, `first_copy`) and all occurrences
   (`occurrences`, `copies`). Receive times of different machines are not
   combined otherwise.
3. **Inside one venue timestamp, the received order is kept.** Messages are
   grouped by (venue timestamp, scope); scope is the Polymarket `market`
   (condition id, which covers both tokens of a `price_change`) and the Kalshi
   `market_ticker` (or `event_ticker`). The stream holding most messages of a
   group (ties: smallest copy, then connection name) is the base. Every other
   stream is aligned to the merged list by longest common subsequence, and its
   messages missing there are inserted at their aligned place. No group is
   sorted or combined.
4. **Full books are checkpoints.** Polymarket `book` and Kalshi
   `orderbook_snapshot` have `checkpoint=True` and are placed at their own
   timestamp like any message. `reconnect_snapshot=True` marks a book sent in
   reply to a (re)subscribe: every Kalshi snapshot, and a Polymarket book that is
   the first of its `asset_id` after the generation's last sent subscription.

## Guarantees

- **Deterministic.** The output depends only on the set of input streams: not
  on their pass order (streams are ranked by name and receive time), and not on
  how markets are split across runs: grouping, lateness and late placement are
  decided per message from its own stream, so the messages of each scope come
  out the same, in the same order, whether markets are merged together or
  separately. This assumes each stream's receive times do not step back
  (`stats.clock_steps_back`). Across scopes the stream is ordered by
  `(order_ms, scope)`, except that late messages come before the other
  messages of their `order_ms`, in arrival order.
- **No message is dropped for coverage reasons and none is invented.** Only
  identical content is merged. The only occurrences dropped are counted:
  `order_conflicts` and `repeats_after_emit`.
- **Ordered output.** `order_ms` never decreases. For a message that is not
  late and has a venue timestamp, `order_ms` is that timestamp.
- **Bounded memory.** Pending messages cover `lateness_ms` of receive time;
  remembered digests cover `retention_ms`. `stats.max_pending_occurrences`
  reports the peak.

## Conventions chosen here (owner to confirm)

- **Order across markets** inside one millisecond is by scope (condition id or
  ticker), not by receipt: copies interleave markets differently, so no copy's
  interleaving is authoritative. Kalshi book, trade and lifecycle messages of
  one market share a scope and keep their received order.
- **Disagreement inside a timestamp** (two streams order the same messages
  differently): the base order wins, the other stream's conflicting occurrence
  is not attached and is counted in `stats.order_conflicts` with up to 20
  examples in `stats.conflict_examples`. `CopyMerge(strict_order=True)` raises
  `CopyMergeError` instead.
- **Messages each stream lacks in the same gap** (stream A has `a b d`, stream B
  has `a c d`): the true order of `b` and `c` is unknown; the base stream's go
  first, counted in `stats.ambiguous_insertions`. A reconnect snapshot that
  another copy lacks therefore follows the changes stamped with its own
  timestamp, which matches the venue's rule that a snapshot includes them.
- **Late messages.** An occurrence received more than `lateness_ms` (default
  120 s) after its venue timestamp, by its own copy's clock, is late. The merge
  cannot place it at its timestamp any more. It is emitted at
  `order_ms = its receive ms - lateness_ms` with `late=True` and the true
  `venue_ts_ms`; a late repeat of content already emitted within
  `retention_ms` (default 15 min) is dropped and counted, and late messages are
  kept once even if a stream received them twice. A Polymarket reconnect
  snapshot carries the time of its token's last change, so placing it later but
  before its arrival does not contradict that token's book. A copy whose clock
  runs ahead by more than `lateness_ms` shows up in `stats.late_by_copy`.
- **Messages without venue time** (Kalshi `market_lifecycle_v2`,
  `event_lifecycle`, subscription acknowledgements, and Kalshi snapshots if
  they lack `ts_ms`) are placed at Kalshi's `sending_ts_ms` (Polymarket: receive
  time). Occurrences with equal content from different streams within
  `untimed_match_ms` (default 5 s) are one message; a second occurrence in the
  same stream is a repeat.
- **What the adapter forwards.** `session_occurrences` forwards every decoded
  JSON value of received text frames, including acknowledgements and
  `new_market`. Text that is not JSON is only counted
  (`SessionCoverage.unparsed_text_frames`).

## What is not guaranteed

- The order of messages that no single stream holds together (see ambiguous
  insertions) and the order across markets are conventions, not facts.
- Periods that no copy covered are reported, not represented in the stream: the
  merged stream simply has no messages there, and books rebuilt across such a
  period are wrong until the next full book.
- A Polymarket coverage interval proves connectivity, not completeness (no
  sequence numbers). A Kalshi interval is split at every `seq` discontinuity.
- Late handling depends on each copy's clock. Late repeats older than
  `retention_ms` are emitted again (still flagged late).
- Identity is exact content. Two different venue sends with identical content in
  one group are told apart only by count.

## Coverage

`CoverageTracker` (or `connection_occurrences(..., coverage=list)`) reports per
session a `SessionCoverage` with `CoverageInterval`s in receive microseconds and
counts of chunk gaps, Kalshi `seq` gaps, recorder markers and transport errors.
An interval runs from the first to the last received frame of a connection
generation and is cut at `transport_error`, `disk_pause_gap`, a missing chunk
or a Kalshi `seq` jump (`end_reason`). `uncovered_intervals(intervals, start_us,
end_us)` returns the spans no given interval covers; pass the intervals of all
copies and connections that carry the same instruments.

## Book check

`check_merged_books(venue, merged)` replays the stream with the adapters' book
state (`MarketBookState`, `KalshiOrderBookState`) and compares every full book
with the rebuilt book of its instrument, then takes it as the new state. It
counts comparisons and equal books, separately for late and reconnect
snapshots, with up to 20 mismatch examples.
