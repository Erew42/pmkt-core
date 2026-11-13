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
    message.copies, message.origin, message.late, message.snapshot_class
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
   stamps them itself. Within one group (rule 3) the k-th occurrence of a
   content in one stream is the same message as the k-th in another, so a
   content is kept as many times as the stream holding it most often received
   it: a venue repeat survives and a copy of it in a second stream does not add
   one. When streams miss different repeats (one stream has the first of three
   equal messages, another the second and third), that count is a lower bound
   on what the venue sent. A market received on two connections of one copy
   (Polymarket Yes/No split) is two streams and is merged like two copies.
2. **Order by venue time.** Polymarket `timestamp`; Kalshi `msg.ts_ms`. Each
   merged message carries the earliest receive time over its occurrences
   (`origin`, `first_received_us`, `first_copy`) and all occurrences
   (`occurrences`, `copies`). Receive times of different machines are not
   combined otherwise.
3. **Inside one venue timestamp, the received order is kept.** Messages are
   grouped by (venue timestamp, scope); scope is the Polymarket `market`
   (condition id, which covers both tokens of a `price_change`) and the Kalshi
   `market_ticker` (or `event_ticker`). Each stream's received order inside the
   group becomes "follows" constraints, and the group is output in an order
   that satisfies all of them (a topological sort). So messages are never
   sorted by content or combined, and the order evidence of all copies is used
   together: if one copy has `a b d e`, another `a c d` and a third `a c b`,
   the result is `a c b d e`. Constraints are added in order of strength (see
   *Constraint priority* under the conventions): Kalshi clock order, then the
   order between book changes from every stream, then all other pairs.
   Messages without venue time placed at the same millisecond and scope are
   ordered in the same sort, together with the timed messages there, by each
   stream's input order.
4. **Full books are checkpoints.** Polymarket `book` and Kalshi
   `orderbook_snapshot` have `checkpoint=True`. A Polymarket book is placed at
   its own timestamp like any message; where no stream's order decides between
   it and another message of its group, the other message goes first, since a
   full book includes the changes stamped with its own timestamp. Kalshi
   snapshots carry no venue time and are placed by their `clock` (next
   section). `snapshot_class` tells the kinds of full book apart.

## Kalshi snapshots and `clock`

Kalshi `orderbook_snapshot` messages carry no `ts_ms` (none of 24,670 in the
2026 recording), only the connection's `sending_ts_ms` and the market's
exchange `clock`. Kalshi documents the `clock` as the same on every
connection: a snapshot includes every change with a `clock` at or below its
own, and the deltas after it on that connection have higher clocks. Placing a
snapshot at its sending time would put it after deltas stamped earlier but not
included in it, and a replay would then overwrite those deltas.

The rule: a snapshot with a positive `clock` C comes after every delta of its
market with clock at or below C and before every delta with a higher clock.
It is implemented in streaming form:

- When the snapshot arrives, it is held per market. Every group of the market
  before the flush point is already emitted.
- While the merge emits that market's groups in time order, the snapshot goes
  into the first group that holds a delta with a clock above C, before that
  delta and after the group's deltas with clock at or below C. Its `order_ms`
  is that group's millisecond.
- If no such group comes before its sending time, it is placed at its sending
  time, ordered against the deltas of that millisecond by clock.

Which group a snapshot joins depends only on its own arrival and its market's
messages, so the result is the same however markets are split across runs.
Each pending snapshot waits at most until its sending time passes the flush
point, so memory stays bounded by the lateness window, plus one clock per
market seen.

Fallbacks, counted in `stats.snapshot_clock_fallbacks` by reason; such a
snapshot is placed at its sending time as an untimed message (see the
conventions):

- `zero`: clock 0, which Kalshi sends for a market without a recorded change.
  Every clock-0 snapshot checked in the recording was an empty book. The rule
  would move such a snapshot before any pending delta of its market; that is
  wrong if it is the empty book of a determined market, so it stays at its
  sending time.
- `missing`: no integer `clock`.
- `decreased`: a clock below the clock of the last book message of its market
  already emitted, such as after exchange maintenance (Kalshi documents that
  the clock may decrease then) or a snapshot that arrived after its place.

`stats.snapshots_by_clock` counts the snapshots placed by clock. Late snapshots
(older than `lateness_ms` on arrival) follow the late rule and are in neither
count. `stats.clock_regressions` counts emitted deltas whose clock is below the
previous delta of their market, or not above a clock-placed snapshot before
them: deltas the clock rule could not keep in order, for example after
maintenance or a delta stamped after the sending time of a snapshot that
includes it. Deltas among themselves keep their venue-time and received order;
the clock only places snapshots. A Kalshi snapshot that does carry `ts_ms` is
treated like other timed messages.

**Limit: real-time subscriptions only.** The rule relies on the documented
`clock` semantics of real-time subscriptions. Conflated subscriptions
(`update_interval_ms`) have different rules and are not supported: neither
deduplication nor ordering is defined for them, and `session_occurrences`
raises `CopyMergeError` on a sent Kalshi subscription with
`update_interval_ms`. The core raw recorder subscribes in real time
(`channels`, `market_tickers`, `use_yes_price`). Callers that build
`Occurrence`s from another store must check this themselves.

## Snapshot classes

`MergedMessage.snapshot_class` is `None` for messages that are not full books,
otherwise:

- `reconnect`: in some occurrence, the first full book of its instrument
  (Polymarket `asset_id`, Kalshi `market_ticker`) after the connection
  generation's last sent subscription (`reconnect_snapshot=True`). Later Kalshi
  snapshots are no longer marked reconnect.
- `empty`: not a reconnect snapshot, and no price levels; Kalshi sends one when
  a market is determined, before removing it from the subscription.
- `later`: any other full book, such as a Polymarket `book` after a trade.

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
  identical content is merged. Occurrences are dropped only as counted repeats
  of already emitted content (`repeats_after_emit`).
- **Ordered output.** `order_ms` never decreases. For a message that is not
  late and has a venue timestamp, `order_ms` is that timestamp. For late
  messages, untimed messages and Kalshi snapshots, `order_ms` is a processing
  order (receive time minus `lateness_ms`, sending time, or the millisecond of
  the group a snapshot joined), not the time of the event; read `venue_ts_ms`
  and the message's own fields for that.
- **Bounded memory.** Pending messages cover at most `lateness_ms` of venue
  time on either side of the receive clock (messages stamped further ahead are
  handled as late); `stats.max_pending_occurrences` reports the peak. Digests of
  emitted messages are kept for `retention_ms` past the later of their place and
  their own time (venue time, or sending time for untimed messages, including a
  Kalshi snapshot placed earlier by its clock), so a copy with a clock far
  behind costs digest memory in proportion to its error (visible in `stats.future_messages`). For Kalshi, one
  clock per market seen is kept.

## Conventions chosen here (owner to confirm)

- **Order across markets** inside one millisecond is by scope (condition id or
  ticker), not by receipt: copies interleave markets differently, so no copy's
  interleaving is authoritative. Kalshi book, trade and lifecycle messages of
  one market share a scope and keep their received order.
- **Constraint priority.** Inside one group, "a before b" constraints are
  added in this order, and one that would contradict those already added (a
  cycle) is skipped:
  1. Kalshi clock order between snapshots and deltas (never skipped).
  2. For each stream in rank order, the order of its consecutive book changes
     (Polymarket `price_change` and `book`; Kalshi `orderbook_delta` and
     `orderbook_snapshot`), skipping the other messages between them.
  3. For each stream in rank order, its remaining consecutive pairs (trades,
     quotes, lifecycle and acknowledgements against anything).

  So trade placements seen by two streams cannot overturn the order of two book
  changes that a third stream received directly. In the 2026 recording, copies
  never disagreed on the order of book changes; every disagreement involved a
  trade.
- **Stream rank.** Streams are ranked by how many messages of the group they
  hold, then by the caller's `preferred_copies` (listed copies in list order,
  unlisted ones after them), then by copy and connection name. The rank decides
  the disagreements that remain after the evidence above, and free choices. It
  is a caller policy, not evidence, and core names no machines. For example, a
  caller who trusts different recorders for each venue runs
  `CopyMerge("polymarket", preferred_copies=("copy_b", "copy_a", "copy_c"))`
  and `CopyMerge("kalshi", preferred_copies=("copy_a", "copy_b", "copy_c"))`.
- **Disagreement inside a timestamp** (two streams order the same messages
  differently): only the skipped constraint is lost; the stream's next
  constraint still starts from the skipped message, and the occurrence is still
  attached to its message. Each skipped constraint is counted in
  `stats.order_conflicts`, split into `stats.book_order_conflicts` (between two
  book changes) and `stats.other_order_conflicts`, with up to 20 examples in
  `stats.conflict_examples` (`book_changes` tells which).
  `CopyMerge(strict_order=True)` raises `CopyMergeError` instead.
- **Free choices** (stream A has `a b d`, stream B has `a c d`: nothing says
  whether `b` or `c` came first): a non-checkpoint comes before a checkpoint,
  then the message first seen in the higher-ranked stream. Each such choice is
  counted in `stats.ambiguous_choices`.
- **Late messages.** An occurrence received more than `lateness_ms` (default
  120 s) after its venue timestamp, by its own copy's clock, is late; so is one
  stamped more than `lateness_ms` after its receipt (`stats.future_messages`). The merge
  cannot place it at its timestamp any more. It is emitted at
  `order_ms = its receive ms - lateness_ms` with `late=True` and the true
  `venue_ts_ms`; a late repeat of content already emitted within
  `retention_ms` (default 15 min) is dropped and counted. A Polymarket reconnect
  snapshot carries the time of its token's last change, so placing it later but
  before its arrival does not contradict that token's book. A copy whose clock
  runs ahead by more than `lateness_ms` shows up in `stats.late_by_copy`.
- **Messages without venue time** (Kalshi `market_lifecycle_v2`,
  `event_lifecycle`, subscription acknowledgements, and Kalshi snapshots that
  fall back from the clock rule) are placed at Kalshi's `sending_ts_ms`
  (Polymarket: receive time). Occurrences with equal content from different streams within
  `untimed_match_ms` (default 5 s) are one message; a second occurrence in the
  same stream is a repeat. Placement uses the earliest sending time, so untimed
  messages of one scope that are closer together than the skew between
  connections may follow the earliest sender's order, not any single stream's.
- **What the adapter forwards.** `session_occurrences` forwards every decoded
  JSON value of received text frames, including acknowledgements and
  `new_market`. Text that is not JSON is only counted
  (`SessionCoverage.unparsed_text_frames`).

## What is not guaranteed

- The order of messages that no stream orders against each other (free
  choices) and the order across markets are conventions, not facts.
- Repeats are matched by count: if a stream lacks the first of two equal
  messages, its occurrence is taken as the first. This affects provenance and,
  rarely, order constraints. The number kept is the largest count in any one
  stream, a lower bound on what the venue sent when streams miss different
  repeats.
- Kalshi clock placement covers snapshots against deltas of real-time
  subscriptions only; see its fallbacks and `clock_regressions`.
- Periods that no copy covered are reported, not represented in the stream: the
  merged stream simply has no messages there, and books rebuilt across such a
  period are wrong until the next full book.
- A Polymarket coverage interval proves connectivity, not completeness (no
  sequence numbers). A Kalshi interval is split at every `seq` discontinuity.
- Late handling depends on each copy's clock. Late repeats older than
  `retention_ms` are emitted again (still flagged late), and a late message is
  kept once even if one stream received it twice.
- Identity is exact content. Two different venue sends with identical content in
  one group are told apart only by count.

## Coverage

`CoverageTracker` (or `connection_occurrences(..., coverage=list)`) reports per
session a `SessionCoverage` with `CoverageInterval`s in receive microseconds and
counts of chunk gaps, Kalshi `seq` gaps, recorder markers and transport errors.
Every frame counts, including an empty JSON list. An interval runs from the
first to the last received frame of a connection generation and is cut at `transport_error`, `disk_pause_gap`, a missing chunk
or a Kalshi `seq` jump (`end_reason`). `uncovered_intervals(intervals, start_us,
end_us)` returns the spans no given interval covers; pass the intervals of all
copies and connections that carry the same instruments.

## Book check

`check_merged_books(venue, merged)` replays the stream with the adapters' book
state (`MarketBookState`, `KalshiOrderBookState`) and compares every full book
with the rebuilt book of its instrument, then takes it as the new state. It
counts comparisons and equal books, separately for late and reconnect
snapshots, with up to 20 mismatch examples.
