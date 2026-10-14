# Managed public order-book feeds

`pmkt.streaming.ManagedBookFeed` is the supported in-memory public depth workflow
for Polymarket and Kalshi. Install the existing `streaming` extra. Use one feed
per venue on the consumer's asyncio event loop:

```python
from pmkt.streaming import ManagedBookFeed

async def consume(tokens):
    async with ManagedBookFeed(tokens, venue="polymarket") as feed:
        # start() returns without waiting for connections or snapshots.
        # In the consumer's existing loop:
        book = feed.book(tokens[0])
        health = feed.health(tokens[0])
        if book is not None:
            bids, asks = book.bids, book.asks
            # Both are immutable tuples of (Decimal price, Decimal size).
            # Apply the consumer's identity and trading policy here.
```

For Kalshi, use `venue="kalshi"` and inject `auth`, an existing
`ReadAuthHeaderProvider`. Core only requests headers for the public WebSocket
GET handshake; it loads no keys or credentials. These are public order-book
subscriptions, not authenticated account/user channels. `connect_factory` and
`ws_url` retain the socket clients' injection conventions, including for offline
synthetic tests. There is no REST depth call in this workflow.

## Interface and ownership

`start()` is idempotent and schedules a task per shard; `close()` invalidates
books immediately, cancels and awaits those tasks, and closes the sockets.
Concurrent close calls share the same cleanup; cancelling a close waiter does
not cancel socket teardown.
The async context manager owns the same lifecycle. A closed feed cannot restart.
Use a new instance to change the subscription universe. Calls must stay on the
owning asyncio loop; this is not a thread-safe service.

`instruments` is the normalized, deduplicated subscription tuple. `shard_size`
defaults to 200. `LiveFeedSupervisor` validates the subscription plan, groups the
shards, and records connection and per-instrument book health. This workflow does
not apply the supervisor's legacy price/message-age staleness policy to quiet
books. `VenueBooks`, the recorder's existing snapshot/delta adapter, is reused;
`WebSocketMessagePump` now shares persistent receive and cancellation handling
with the recorder. `WebSocketRetryBudget` supplies retry handling with unlimited
attempts and a 30-second delay cap. Retry delays grow by `retry_backoff` (default
0.5 seconds) per replacement attempt and do not reset on connection success.

`book(instrument)` returns `PublicBook | None`; `health(instrument)` returns
`PublicBookHealth`. Unknown instruments raise `KeyError`. Every result identifies
its venue, native instrument, shard, and connection `generation`. Generations
increase on connection attempts and are scoped to a shard and feed instance.
Results are detached observations. **Read again for each decision**: a previously
returned book is not a freshness lease and cannot be revoked after it is returned.

`PublicBook` contains full bids descending, full asks ascending, raw source
timestamp of the last depth update, native market ID when supplied, local
monotonic application receipt time (after dequeue), and quality flags.
Metadata events do not advance either depth timestamp. Market IDs and other
state metadata are cleared on invalidation and never inherited from an older
connection generation. Kalshi always subscribes with `use_yes_price=True`: bids are
YES bids and asks are YES asks (the wire NO ladder on the YES price basis).
To construct NO bids, complement YES asks with `Decimal(1) - price`; NO asks
come from complemented YES bids. Quantities retain their source ladder's size.
An intact empty side is usable depth with `empty_bid`/`empty_ask` flags; usability
does not imply that an executable two-sided quote exists.

`PublicBookHealth` reports `usable`, unavailability `reason`, connection state,
last observed transport-liveness age, book-update age, generation, reconnect and
error counts, last error, and current quality flags. Times and ages are seconds
on the local monotonic clock, not UTC and not venue event time. A source timestamp
is preserved, not rewritten with read time. This API does not manufacture a fresh
book timestamp on each poll. Counters are per shard; book flags and ages are per
instrument. Typical reasons include `not_started`, `no_initial_snapshot`,
`disconnected`, `heartbeat_failed`, `liveness_expired`, `snapshot_timeout`,
`crossed_book`, `book_integrity_failed`, `recovery_timeout`, `market_resolved`,
and `closed`. Treat the reason as diagnostic text, not
an exhaustive policy enum.

## Integrity, liveness, and recovery

Polymarket application heartbeats and inbound-frame diagnostics run independently
of price changes. Kalshi uses received book traffic plus bounded transport
ping/pong probes. Default `probe_interval` is 10 seconds and `liveness_timeout`
is 30 seconds. Kalshi probes run independently of delta processing and check
the existing liveness deadline before accepting a pong.
A valid quiet book remains usable indefinitely while these checks
succeed. Reads themselves check liveness and latch invalidation if expired, even
before the monitor wakes; a late heartbeat cannot revive that book.

Startup and every replacement connection require an initial snapshot for each
instrument. `snapshot_timeout` defaults to 30 seconds; `connect_timeout` defaults
to 10 seconds. An instrument with no snapshot reports `no_initial_snapshot`, then
`snapshot_timeout` after the deadline. Healthy siblings stay available on the same
connection, including when an ended market never supplies a snapshot.

Fault scope is explicit:

| Fault | Polymarket | Kalshi |
| --- | --- | --- |
| Missing initial/recovery snapshot | Instrument | Instrument |
| Crossed ladder (Polymarket also treats locked depth as invalid) | Instrument; bounded corrective-delta grace | Instrument; replacement snapshot required |
| Malformed ladder/quantity, delta before snapshot | Instrument when identity is known | Instrument when ticker and subscription identity are valid |
| Supplied previous-hash mismatch | Instrument; replacement snapshot required | Not applicable |
| Resolution event | Affected instrument(s); `market_resolved` | Not part of this book-channel contract |
| Decode failure, unscoped malformed envelope/missing identity, subscription error | Shard | Shard |
| Missing/invalid acknowledgement, wrong SID/ticker, missing/noncontiguous subscription sequence | Not applicable; foreign token IDs are ignored | Shard; acknowledgement deadline is `snapshot_timeout` |
| Disconnect, expired liveness, failed heartbeat/probe, failed/timed-out recovery send | Shard | Shard |

Polymarket locked or crossed depth from a price change is withheld immediately
with reason `crossed_book`. Only these nonpersistent ladder flags qualify for a
250 ms grace. Later complete depth updates may correct the retained ladder;
neither best-price hints nor activity events repair it. Repeated invalid updates
do not extend the deadline. Reads and ingress latch expiry before a late update
can revive the book. This managed policy accepts corrective updates across
timestamps/hashes; it does not change the recorder's narrower complementary-delta
policy.

After grace expires, or immediately for other local integrity failures, depth is
withheld with `book_integrity_failed` and only that instrument is reset. Recovery
uses public Polymarket unsubscribe/subscribe operations for the affected tokens,
or Kalshi `get_snapshot` for the affected tickers on the existing SID. Missing
initial snapshots use the same targeted recovery, retaining `snapshot_timeout`.
One separate owned task batches requests without blocking depth processing or
heartbeats. Updates during the subscription send cannot make a book usable.
A valid subsequent snapshot is required; deltas cannot bypass this barrier.

Each recovery attempt has a `snapshot_timeout` response deadline. Silent or
invalid recovery falls back to an unavailable instrument (`recovery_timeout`, or
`snapshot_timeout` if it never initialized), followed by a cooldown of
`max(30, snapshot_timeout)` seconds before another targeted attempt. This bounds
each attempt and its retry rate without reconnecting healthy siblings. A valid
late snapshot can restore the instrument during cooldown. A venue that never
sends a valid snapshot remains explicitly unavailable; recovery cannot invent
depth. Local recovery does not change the connection generation or the shard's
reconnect/error counters.

Shard faults invalidate all depth before awaited cleanup or backoff. A new valid
snapshot on a replacement connection is required for reuse.
Cleanup cancels the outstanding receive before closing its iterator, so queued
frames from an older socket cannot enter the next generation.

Kalshi validates the subscription acknowledgement's request ID, channel and SID,
then strictly checks book SID and contiguous subscription-level sequence across
interleaved tickers. A gap invalidates sibling books too. Unexpected tickers fail
the shard. Polymarket uses the existing ladder, malformed-data, crossed-book and
supplied previous-hash checks; that channel has no equivalent guaranteed
contiguous sequence. Hash chaining can only be checked when the message supplies
the previous hash. This is not a computed venue checksum or proof against
undetectable venue-side message loss. Foreign token IDs are ignored, never
allocated as subscribed state. Top-of-book/activity events do not initialize or
repair a complete ladder. Local Kalshi recovery preserves the subscription's
sequence cursor, including while the affected instrument is unavailable.

`usable` describes the received WebSocket evidence and local integrity/liveness
checks. It does not certify agreement with a concurrent REST response. Live
comparisons can differ even when their source timestamps match; timestamps are
not cross-transport sequence numbers. This workflow does not reconcile the two
sources. Consumers retain their REST fallback and reconciliation policy.

Market IDs and source times are native evidence, not reviewed identity bindings.
Depth health does not establish current signing constraints. Polymarket tick
exposure remains core issue #18; this API deliberately offers no authoritative
constraint contract. Resolved Polymarket books are unavailable for managed reads.

## Precision and compatibility

Both state classes now store ladder keys and sizes as `Decimal`:
`MarketBookState.bids/asks` and `KalshiOrderBookState.yes_bids/no_bids`.
Polymarket state's `best_bid/best_ask` are also decimal. Direct state consumers
must use decimal keys and decimal arithmetic, not float dictionary lookups or
mixed float/decimal arithmetic. Kalshi deltas add exact decimal quantities;
negative resulting size is an integrity failure without a float epsilon.
There is no grid quantization in the managed reader. Native decimal strings
and JSON decimal numbers are preserved by managed decoding. Direct callers
supplying Python floats have already lost any precision beyond those floats.

Legacy `MarketStreamSnapshot`/`KalshiBookSnapshot` projections and their `as_dict()`
outputs retain float prices/sizes. The recording adapter explicitly converts to
its existing float storage representation. Tape delta lookup uses decimal keys
and tape serialization retains its existing float schema. Canonical schema
versions, row grain, price basis, timestamp units, identifiers and provenance
fields are unchanged. Correcting prior floating accumulation can change the
numeric result for newly captured evidence; existing recordings are not rewritten.
Consumers requiring exact ladders must use `PublicBook` or the state maps rather
than legacy snapshot projections.

## Adopting from pmkt-trading

Create one shared instance for the union of paired and standalone instruments per
venue and own it in the existing run lifecycle. Replace duplicated socket tasks,
sharding, retry/backoff, public book accumulation, and heartbeat checks with
`book()` and `health()`. An unavailable result lets the consumer apply its REST
fallback policy; expose health alongside the actual source chosen by the reader.
Pin the exact landed core revision before integration validation.

Keep reviewed token/condition/ticker bindings, signing metadata, fee checks,
account reconciliation, own-order subtraction, local-mutation barriers, and
REST fallback policy in trading. This feed has no mutation notification or
post-mutation snapshot request/reply API: a current public book is not proof
that an order mutation is reflected. Trading must preserve that barrier through
its own integration; it must not infer it from a generation number or a heartbeat.
No MT-P default change or live deployment is implied by this contract.
