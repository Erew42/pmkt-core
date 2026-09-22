# Stream recording contract

Contract for the simplified recorder. The two venue recording entrypoints use
this format; historical profile captures are handled separately by legacy readers.
The new format is a deliberate migration, not a reinterpretation of historical
profile or schema versions. The current REST, catalog and resolution APIs are
outside this change.

## Mental model

Maintain live venue books. Record changed topbooks and, in full mode, selected
complete depth snapshots. Commit to SQLite and export ordinary Parquet tables.
Optionally keep decoded source messages as JSONL for diagnosis.

Full means every level in each saved snapshot. It does not mean every depth
change between snapshots is retained. Optional raw messages do not turn the
recording into a promised reconstruction tape.

## Recording options

| Option | Default | Meaning |
|---|---|---|
| `mode` | `full` | `topbook` records topbook changes; `full` also records depth snapshots. |
| `depth_check_interval_s` | `10.0` | In full mode, check for a net change at this interval. A number must be finite and positive; `None` disables periodic depth checks. |
| `depth_on_best_price_change` | `false` | In full mode, snapshot when best bid or ask price changes, including a side becoming empty or available. Can operate with or without periodic checks. |
| `raw_messages` | `false` | Also write decoded incoming messages to `raw_messages.jsonl`. |

The depth options have no effect in topbook mode. Instrument selection, requested
duration/message limit, and existing venue read-auth requirements remain explicit
run inputs. Existing heartbeat, cancellation and bounded retry behavior remain
the responsibility of the live feed. The manifest records the resolved options,
transport bounds, adapter settings and observed implementation identity.
SQLite is the single recording backend; Parquet is the export format.

Full mode requires at least one ongoing depth trigger: a positive interval or
`depth_on_best_price_change=True`. Thus `None` with `True` is valid and records
depth on best-price changes only during normal operation; `None` with `False`
is rejected before opening a connection or creating output. Use `None` for off,
not zero. Initial, recovery and graceful-stop snapshots still apply when the
periodic depth check is disabled. Connection-liveness reporting remains enabled.

## Topbook recording

Record the first initialized topbook and subsequent changes in best bid/ask
price or size, tick size, minimum order size, side availability, or book validity.
These observations are processed on arrival, independently of the depth timer.
Deeper-level changes alone do not produce topbook rows.

Record loss and restoration of book integrity immediately, including during
quiet periods. A disconnected or invalid book must not continue to appear as a
current valid quote. A missing side stays null. Initialization, structural
integrity and usable two-sided quotes are distinct facts; one-sided or empty
initialized books may be structurally intact.

Changes only to receipt time, venue timestamp/hash, calculated age, or diagnostic
counters do not produce duplicate topbooks. A fresh initial book after reconnect
or resynchronization is always recorded, even if its prices match the old book.

## Full-depth sampling

Maintain the full local book on every accepted venue update in both modes. In
full mode, compare each intact book with its last saved depth snapshot:

1. Save the first initialized, structurally intact book immediately.
2. If periodic checks are enabled, inspect the current book every 10 seconds by
   default. Save it only if its semantic state differs from the last saved
   snapshot. With `depth_check_interval_s=None`, no periodic depth check runs.
3. When the optional best-price trigger is enabled, save immediately after an
   applied message changes the best bid or ask price. A size-only change does
   not activate this trigger, though it can create a topbook row.
4. After reconnect or integrity recovery, save the first fresh intact book
   immediately and start comparison from that snapshot.
5. On a graceful stop, save any final changed intact book. Do not duplicate an
   unchanged snapshot. A crash or failed write cannot promise this final sample.

Semantic equality means equal normalized bid and ask price-to-quantity maps,
tick size, minimum order size and state-defining quality. Zero-size levels are
absent. Compare normalized numeric values under the existing venue rules;
string formatting and level iteration order are irrelevant. Timestamps, message
counts, age-only flags and venue hashes are not changes in book content.

The comparison baseline is the last snapshot accepted by the recorder, including
an initial, recovery, price-triggered or terminal snapshot. A failed database
batch stops recording; its uncommitted samples are not durable evidence.

A dirty flag may skip comparisons when no relevant updates arrived. It cannot
replace the equality check: A -> B -> A between checks produces no new snapshot
when A is still the last saved state. There is no transient-change archive in
the depth tables.

Only initialized, structurally intact books produce depth snapshots. On a gap
or invalidation, record an immediate event/topbook validity change and suspend
depth sampling until integrity is restored. Never silently join book state
across connections. An intact empty book produces a snapshot header with zero
levels; it is different from an instrument that never initialized.

When enabled, the depth timer follows a monotonic schedule anchored to run start;
price-triggered snapshots do not reset it. Messages and checks are serialized so
a snapshot cannot mix levels from before and after one applied
message. A message affecting several instruments is applied completely before
evaluating its price-change triggers. If triggers coincide, write one snapshot
per instrument and record the applicable causes.

If processing delays a check, inspect the actual current state once when serviced;
do not invent missed historical samples. Continue at the next future scheduled
check. Preserve the actual sampling time and source time. An unchanged result is
not emitted merely because a timer ran, and a timer does not prove feed liveness.

### Example: 10-second checks

| Time | Current book | Action with the price trigger disabled |
|---|---|---|
| 0 s | Initial intact A | Save A immediately. |
| 3 s | B | Update memory and any changed topbook. |
| 7 s | A again | Update memory and any changed topbook. |
| 10 s | A | Save nothing: equal to the last saved snapshot. |
| 14 s | C | Update memory and any changed topbook. |
| 20 s | C | Save C; its source observation may still be from 14 s. |
| 30 s | C | Save nothing. |
| 34 s | D; graceful stop | Save D as the final snapshot. |

## Stored records and ownership

Each run has one `manifest.json`. The logical tables are:

| Output | Grain and minimum content |
|---|---|
| `topbook.parquet` | One emitted topbook observation: instrument identity, coordinates, bid/ask prices and quantities, tick/minimum-order size and state flags. |
| `book_snapshots.parquet` | One saved full-book snapshot: snapshot ID, instrument identity, coordinates, sample time, causes and bid/ask level counts. Full mode only. |
| `book_levels.parquet` | One price level per snapshot ID and side: price and quantity. Full mode only. |
| `trades.parquet` | One normalized observed trade, retaining the existing venue identity and duplicate-handling semantics. Absence of trade messages does not prove absence of trades. |
| `events.parquet` | Connection, subscription, gap, invalidation, recovery and market lifecycle facts: kind, affected connection/instrument, time and relevant details. This is not a second copy of every wire message. |

A compact connection-liveness observation is also recorded every 10 seconds in
both modes, independently of depth sampling. It reports actual last transport
activity and last source-message time, plus each instrument's latest observed
book-message coordinate and current initialization/integrity state. Traffic for
one instrument does not establish another instrument's freshness. The timer
itself supplies no new evidence of venue activity. This keeps quiet, connected
periods distinguishable from disconnects without rewriting unchanged books.

Snapshot headers and their levels form one complete unit, including zero-level
snapshots. Use a unique run-local record sequence for emitted records; snapshot
IDs bind their child levels. Sequence ordering describes recorder observations,
not a global exchange clock. Every source-derived record identifies its venue,
instrument where applicable, connection generation and source-message sequence.

Preserve UTC source receipt time, venue time when actually supplied, and depth
sampling time as different fields. Preserve the latest contributing source
coordinate for sampled books; never replace it with the timer time. Do not
fabricate timestamps to make rows unique. Liveness and invalidation events bound
where a consumer may carry a previous observation forward.

These are the new format's logical fields. Implementation must register and
document the exact physical types, keys and schema IDs before writing them.
Reuse canonical venue identities, price/quantity units and normalization rules.
An altered grain or field set must receive a new schema ID; do not label the new
tables `topbook.v2` or `depth.v2` unless they exactly satisfy those contracts.
The run manifest declares one recording-format version and the exact table
schemas; there are no selectable historical writer/profile combinations.

Spread, midpoint and research features are computed downstream. Depth totals,
level counts and imbalance require depth input, not topbook alone. Snapshot
level counts remain stored structural checks. Core owns normalized observations
and book correctness; research owns research feature choices.

### What trades contain

`trades.parquet` records received public trade reports in both modes, independently
of depth checks and topbook changes. Each row identifies the instrument and source
observation, reported price, quantity when supplied, venue time when supplied,
receipt time and reported side when supplied. A trade can arrive without changing
the best quote. A change in displayed book quantity is not itself a trade report;
do not infer trades from book differences. These are public market observations,
not the user's orders or account fills.

The existing Polymarket producer consumes `last_trade_price` messages. Quantity
can be missing, and its generated ID identifies an observation rather than a
venue-unique execution. It does not deduplicate separate messages as the same
execution. The existing Kalshi producer consumes `trade` messages and deduplicates
their venue trade IDs within a run. Preserve these distinctions; do not claim a
complete, globally deduplicated transaction history. Reported side retains its
venue meaning. An absent venue timestamp must remain distinguishable from receipt
time in the new format, even where the old producer used receipt time as fallback.

This table makes observed execution prices and reported quantities available for
analysis without reparsing diagnostic JSONL. Volume and trade-count analysis must
respect source coverage, missing quantities and duplicate-handling limits.

## Persistence and optional raw messages

SQLite transactions are the durability boundary. Commit bounded batches; record
the actual batch limits and final committed progress in run metadata. Snapshot
headers and levels must commit atomically. Buffered, uncommitted work may be lost
on process failure. Apply bounded backpressure or stop with an error rather than
silently dropping observations. Host/power-loss guarantees require separate
qualification; this contract does not infer them from the use of SQLite WAL.

Export committed SQLite records into Parquet on normal finalization or explicit
recovery. Validate schema, counts and snapshot relationships, then publish the
manifest only after the exported files are closed successfully. A failed export
leaves SQLite recoverable and is reported as a failure. Re-export must be safe
without duplicating committed rows. Exported files are described by role, schema,
row count and content hash. No separate live segment-commit journal is required.

Retain SQLite until export is verified. The verified Parquet export can be used
without the database. Removal of the retained database is a separate storage
cleanup decision, not a prerequisite for a valid recording.

When requested, raw JSONL stores each decoded venue message delivered to the
recorder, with connection ID, source-message sequence and receipt time. It is
independent of normalized table roles and contains no authentication headers,
credentials or outbound authentication/subscription traffic. Transport control
frames are not promised. Raw messages are diagnostic input, not an independently
validated reconstruction product or an atomic replica of SQLite commits.

Raw output may have a different recoverable tail after a crash. Record that
limitation. If a requested raw writer fails, stop and report the failure rather
than silently dropping the requested diagnostic output. The default is no raw
file; compression can be an offline operation.

## Recording report

Report requested instruments, initialized instruments, missing initial books,
per-instrument connection/initialization and final integrity state, counts of
topbooks/snapshots/trades, gap/reconnect events, actual start/end times, stop
reason, storage/export errors and committed progress. Keep facts sufficient to
identify missing or invalid intervals, not only total counters.

Use one small terminal verdict:

- `complete`: reached the requested stopping condition, persistence/export
  succeeded, and every requested instrument is initialized and intact in its
  latest subscription attempt.
- `partial`: usable book evidence exists, but recording was interrupted or some
  requested instruments remain missing or invalid at the end.
- `failed`: no instrument initialized an intact book, or persistence/export or
  requested raw output failed. Previously committed evidence may still be usable.

Evaluate final book state before the recorder's own orderly socket teardown.
Failure takes precedence over partial. One-sided or empty intact books do not
fail solely because quotes are unavailable. Recovered gaps remain explicitly
recorded even when the final verdict is complete. Complete describes terminal
recording completion, not uninterrupted historical coverage or research fitness.

External market-status metadata can explain missing instruments. Its absence
does not downgrade otherwise observed intact books, and missing instruments
must not be silently excluded from the requested set. There is no calibrated
acceptance policy or `acceptance_eligible` flag in this recorder.

## Required behavior checks for implementation

- First book, size-only change, deep-only change, and A -> B -> A before a check.
- Unchanged timer checks; net change at 10 seconds; optional best-price trigger;
  coincident triggers and delayed checks without invented samples.
- Full mode with `None` and the price trigger enabled: no periodic depth samples;
  size/deep-only changes wait for a price trigger or graceful stop. Initial and
  recovery snapshots and independent liveness reporting still occur. Reject
  full mode with both ongoing depth triggers disabled before I/O.
- Empty/one-sided initialized books; no initial book; gap, disconnect and recovery
  with a fresh snapshot even when content matches the old connection.
- Source time distinct from sample time; atomic snapshot levels; final changed
  snapshot on graceful stop; no current-valid carry-forward across invalidation.
- Interrupted database transactions, failed raw writes, failed/retried export and
  equality of committed content before and after Parquet export.

The new writer omits legacy parsed events, summary/level copies and tape
production. There are no writer profiles or acceptance/eligibility policies.
`TapeBatchIntent` lives with historical tape types in `streaming/legacy/tape.py`;
the unused router, profile runtime, capture session, feed-control scheduler,
health-emission wrapper and connection-group orchestration have been removed.
Historical readers/recovery/conversion code lives in `streaming/legacy` and is
not imported by the new live recording path. Shared book/transport validation
and independently used supervisor utilities remain.

## Where to look after a month away

- `streaming/recording_feed.py`: receive messages, maintain venue books, handle
  connection changes and run the recorder's timer. Kalshi requests YES-price
  books and records one `TICKER:YES` book; NO quotes can be derived by complement.
- `streaming/recording.py`: decide when to emit topbook/depth rows and recording
  facts. This is the place to change sampling behavior.
- `streaming/trades.py`: normalize public trade reports, retaining missing fields.
- `streaming/recording_store.py`: bounded SQLite transactions, locked export,
  inspection and manifest validation. The database is retained after export.
- `data/recording_schema.py`: physical field types and keys registered by the
  canonical registry. The data dictionary lists every column.
- `streaming/legacy/`: only needed to read/recover/convert historical captures.

The venue `order_book_stream.py` modules are small public entrypoints. The
underlying `ws.py` modules still own venue book semantics and WebSocket transport.
Live recording does not import the historical profile, tape or durability stack.

```python
from pmkt.exchanges.polymarket import stream_order_book_data

report = await stream_order_book_data(
    token_ids,
    mode="full",
    depth_check_interval_s=10.0,
    raw_messages=False,
)
# Or set depth_check_interval_s=None and depth_on_best_price_change=True.
```

`recover-stream-run RUN --finalize` detects new SQLite recordings and exports
committed rows. Without `--finalize` it only inspects committed metadata/counts.
`dataset validate-manifest RUN/manifest.json` validates the exported files without
requiring retained SQLite. An OS-held lock prevents export during live recording.
Old direct imports of profile/tape reader helpers now use `pmkt.streaming.legacy`.
The historical producer helpers retained there support conversion and reader
fixtures; they are not selectable live writers.

This is a recording API migration: old profile, eligibility, durability-backend,
connection-group and `runtime_projection_recorder` arguments are removed.
Sampled recording tables are not a private execution-feed projection. The private
consumer's existing exact core dependency pin must be upgraded separately with
its execution-feed integration checked against the chosen public feed API.
