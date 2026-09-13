# PR #5: capture initialization, recovery, and coverage

Status: implemented; PR is ready for review. Grid sampling,
raw/tape reduction, and further storage architecture work remain separate.

## The problem and the resulting model

The original capture path mixed several different questions: whether an
instrument had sent its first snapshot, whether its current book was intact,
whether a projected output row was valid, whether the socket was alive, and
whether the instrument was eligible for the capture's acceptance policy.
Treating missing initialization as connection failure reset healthy peers and
spent reconnect budgets without establishing any additional evidence.

PR #5 keeps those decisions separate while reusing existing subscription,
book, supervisor, evidence, and durability structures.

| Question | Evidence and resulting action |
|---|---|
| Did this instrument initialize? | An authoritative snapshot, including an explicitly empty one. Keep missing/overdue evidence; silence and deltas alone do not trigger recovery. |
| Is an initialized native book intact? | State-level integrity and its actual failure cause. Preserve corruption recovery and venue-specific escalation. |
| Is this emitted row intact? | Upstream integrity AND absence of unresolved failures on that row. Projection must never promote an invalid upstream book. |
| Is the connection alive? | Transport activity, bounded heartbeat checks, and socket errors. Reconnect with a recorded cause. |
| Was eligibility evaluated? | Existing eligible/excluded/unknown verdict counts. Report the evaluation separately from capture success. |
| Which stored data survived a crash? | Published journal groups. Pending checkpoints do not become durable merely because they were staged. |

## 1. Instrument initialization and recovery

Both venues retain every subscribed instrument, including silent instruments
and those sending only deltas. Subscription attempts, the initialization SLA,
overdue flags, and completeness denominators stay in place. A connected shard
with no initialized instruments also remains connected solely with respect to
that missing initialization. Existing zero-snapshot completion failures remain.

The shared supervisor no longer emits recovery actions for missing initial
snapshots. Such absence consumes no reconnect budget, invalidates no healthy
peer, and emits no reconnect tape controls. Kalshi consequently no longer sends
automatic snapshot requests just because initialization is overdue, removing
that request-timeout route to reconnecting the socket.

Real disconnections and initialized-book corruption still recover. Kalshi
retains targeted snapshot refresh for integrity failures and its existing
request-failure/response-timeout escalation. Polymarket adds no refresh request.
There is no quarantine, automatic dropping, or inference of inactivity.

An empty authoritative snapshot establishes initialization and can have intact
native-book state even though its quotes are unusable. Deltas before the first
snapshot establish neither initialization nor integrity. A late snapshot updates
the already tracked instrument normally.

Both tape profile versions omit deltas before the first baseline when no prior
epoch exists. This also affects v2 tape row population: restoring those rows in
review reproduced a strict commit failure on both venues, because a delta had
no committed checkpoint. The validator is unchanged. Raw/parsed observations
retain the messages when those roles are enabled, and v2/v3 instrument evidence
retains missing initialization. Post-reconnect delta audit remains available
when an earlier epoch exists. No synthetic checkpoint is fabricated.

## 2. Kalshi native integrity and projected-row integrity

The reproduced `full@3` commit error was a locked native book with YES and NO
bids both at 0.50. Native state considers strict crossing invalid; the canonical
topbook also flags equality as `crossed_book`. Copying native integrity directly
onto that projected row produced true integrity alongside an unresolved failure,
which strict durability validation correctly rejected.

The shared version-3 stamping helper now computes:

```text
row.book_integrity_valid = upstream_book_integrity
                           AND no_unresolved_failure_on_this_row
```

The helper reuses `UNRESOLVED_BOOK_FAILURE_FLAGS`, the validator's own definition.
It covers affected topbook, checkpoint, and depth paths without a competing flag
list. A locked topbook remains flagged and has false row integrity. A native
depth row without the projection-only failure can retain true integrity.
Empty-side and stale-quote flags alone do not invalidate native integrity.
Sequence and initialization failures remain false throughout projection.

This corrects emitted rows before writing. It does not suppress validation
errors, relax canonical lock semantics, or feed stricter projected-row validity
back into native socket-recovery decisions. Existing legacy-profile row
semantics and canonical table schema versions are preserved. The original live
payload causing the first reported Kalshi crash was not verified; the locked
fixture is the confirmed reproduction, not proof of every historical live cause.

## 3. Eligibility reporting

Completeness, recovered manifests, and connection-group summaries add
`eligibility_evaluation_status` derived from existing evidence counts:

| Value | Meaning |
|---|---|
| `unevaluated` | No classified instruments, including no evidence summary. |
| `partial` | Classified instruments and unknown verdicts coexist. |
| `evaluated` | A nonempty classified set has no unknown verdicts. |

Eligible and excluded verdicts are classified. Traffic, snapshot arrival, and
external probe selection do not manufacture canonical eligibility evidence.
CLI summaries display total initialization, eligibility evaluation, unknown
count, and eligible snapshot coverage separately. Older manifests without the
field retain their previous display.

The existing complete/partial/failed verdict rules, CLI exit behavior,
denominators, and acceptance gates are unchanged. For example, every instrument
can initialize while eligibility remains unevaluated and the conservative
capture verdict stays partial. A persistence failure remains a failure even
when eligibility is unevaluated. No catalog calls or CLI options are added.

## 4. Polymarket transport liveness and diagnostics

The market-channel client sends application `PING`; incoming `PONG` or data
provides transport-activity evidence. An inbound application `PING` is answered
with `PONG` as well. The earlier explanation that the market venue need not
answer client PING confused market and sports protocols and is withdrawn.
See the venue's [market WebSocket documentation](https://docs.polymarket.com/api-reference/wss/market).

The receiver now runs as a separate task with a bounded application queue. It
handles heartbeat frames before handing market data to the collector while
queue capacity is available. Synchronous collector work still blocks the same
event loop; a full queue also prevents reading subsequent heartbeat frames.
The reader therefore improves separation but does not make capture independent
of downstream processing speed.

A pending client ping establishes a bounded silence check; repeated sends do
not renew it. Incoming data refreshes activity. Detectable event-loop stalls
and queue backpressure grant the receiver time to drain instead of declaring
remote silence from local blocking. Heartbeat sends are bounded, and task
cleanup preserves cancellation on Python 3.10 through 3.12.

The configured transport receive bound and the application queue are separate
buffers with the same configured capacity. `websocket_transport.effective`
describes transport settings, not total process memory. Decoding, in-flight
messages, book state, and persistence buffers add further memory.

Both venues persist a retry record before peer invalidation and before a new
connection clears the triggering error. `reconnect_diagnostics.jsonl` is flushed
and fsynced; the manifest projects the same records. Fields distinguish
transport, connection setup, and supervisor recovery, including exception type,
errno, received close details, affected instruments, and control-plane metrics.
Polymarket adds heartbeat and receive-queue observations. A missing received
close code does not establish which endpoint or network component caused failure.

A sidecar write failure deliberately stops capture as a persistence failure;
it does not silently continue without the promised recovery evidence. Tests
cover this policy on both venues. Records describe attempted replacements;
a final error after the retry budget is exhausted is reported by the failed
capture manifest and need not have a replacement record.

## 5. Narrow complementary-delta recovery delay

A 180-second raw-only reproduction received 12,710 messages. Ten temporary
instrument locks resolved in the next message/frame within 0.251 ms, with the
same timestamp and instrument hash. Actual opposite-side deletions exposed
existing depth; best-price hints alone did not establish the missing levels.

A delay is permitted only for an already initialized, previously intact book
when a positive price delta creates equality at the changed price, the sole
failure is `crossed_book`, and the message contains a timestamp, hash, and an
unlocked best-price hint in the valid price range. Other failures retain their
existing recovery behavior. Another broken peer on the shard prevents deferral.

The wait is bounded by 250 ms from the first recovery decision after synchronous
writes, or 16 following messages. Neither bound renews. The first commit cannot
consume the entire receive opportunity. Once armed, bounds are checked before
applying a later message, and the next pending deadline also caps an idle wait.
A late correction cannot erase an expired failure. A changed hash/timestamp or
new authoritative snapshot that leaves the book invalid withdraws the delay
and forces a control decision even if the health flags themselves are unchanged.
Synchronous work can still delay when the collector runs that decision.

The invalid row stays invalid and closes its tape epoch. An actual update or
snapshot restoring the native book produces a validated resync checkpoint.
No depth is inferred from hints. Manifest counters record candidates, restored
books (`resolved`, including authoritative snapshots), expired bounds, and
pending candidates. Withdrawal of a delay or a socket reset can remove a
candidate without counting a bound expiration; these counters are not a full
partition of candidate outcomes. On socket replacement, already queued
old-connection data is discarded and the replacement book must initialize again.

The final review reproduced and repaired timer, message-count, and changed-hash
bypasses in the collector. Tests cover both exhausted budgets and successful
replacement, legacy idle capture, and preservation of the 12-second commit-stall
grace. The pre-update integrity lookup now visits only assets named in the
delta, avoiding a full subscription-universe scan for every received message.

## 6. Bounded checkpoint publication

A fixed 1,000-message replay previously forced 118 native checkpoint groups plus
termination/shutdown. It took 78.193 seconds, dominated by synchronous validation
and writing. Version-3 Parquet profiles now stage routine startup, resync, and
periodic checkpoint barriers within the existing one-second coalescing window.
Every checkpoint and companion row remains present; this is not sampling.

The first pending checkpoint starts the window and supplies the staged cause;
later requests do not renew it. The journal records the barrier that actually
publishes the group, so a first startup request followed by resync requests can
publish with `checkpoint_startup`. Row/time thresholds can publish earlier.
Invalidations, termination, shutdown, and explicit forced commits still drain
synchronously. Legacy profiles and SQLite retain immediate checkpoints.

Staging occurs before durable acceptance. Strict prewrite validation, artifact
write/readback validation, and journal publication are unchanged. A crash can
lose pending rows; only journaled groups are authoritative. Tests exercise actual
child-process crashes at staging, pre-journal, and post-journal boundaries.
The coordinator protocol now explicitly declares checkpoint requests rather than
using an optional runtime-method lookup. See the
[capture runbook](../storage_profile_capture_runbook.md) for durability boundaries.

The identical replay fell to 20.274 seconds and six commit groups, retaining all
118 checkpoints and matching non-health data semantics. This is useful bounded
improvement, not proof that synchronous `full@3` sustains every live workload.

## Verification and remaining limits

Earlier artifacts remain on `erik-pc1`:

| Evidence | Artifact directory |
|---|---|
| Original probes and the 25+25 heartbeat iterations | `/home/erike/pmkt-core-pr5-98fea14/tmp/` |
| Recorded recovery causes and raw complementary-lock reproduction | `/home/erike/pmkt-core-pr5-liveness/tmp/` |
| Checkpoint replay parity and pre-commit dual-venue probe | `/home/erike/pmkt-core-pr5-complementary/tmp/pr5-candidate/` |
| Fresh active 25-market/25-ticker stress run at `de30b34` | `/home/erike/pr5-stress-fresh-de30b34/` |

The fresh 600-second stress run initialized all 50 Polymarket tokens and all
25 Kalshi tickers. Polymarket captured 40,125 events with three reconnects;
Kalshi captured 28,957 with no reconnect and one successful targeted refresh.
Both finalized artifacts passed strict validation. Polymarket nevertheless
accumulated about 370 seconds of additional backlog relative to the raw-screen
clock baseline; control-plane lateness reached 11.51 seconds (Kalshi 9.94).
One PM recovery involved a true cross outside the lock-only rule; two were
transport errors without a received close cause. Aggregate memory headroom did
not establish sufficient single-event-loop processing capacity.

This remains a correctness and bounded-recovery PR, not scaling acceptance for
hundreds or thousands of active instruments. Configurable state grids, raw/tape
reduction, asynchronous publication, broader corruption isolation, and
eligibility acquisition remain separate work. PR #5 should not expand to absorb
those investigations.

### Final review verification

Local full suite: **1,421 passed, 2 skipped**. Repository hygiene, pytest-lane
coverage, Ruff, mypy, and public book/price/midpoint/history contracts passed.
Independent read-only reviews covered the full `fd8a375` through `de30b34` diff;
Codex reproduced material findings before deciding which changes to implement.

The final pre-commit probe used base `de30b34` plus source patch SHA-256
`f92838de45c7b45532ee641bec841703d15e52a7f5747e2d39ae51713a9aa0a6`.
All 11 changed source files were hash-matched in the isolated checkout; both
venue imports resolved there. The fixed 50-token/25-ticker selection SHA-256 was
`b1a3cd5bc98e8bfb61150d520f276c77dd5b9c34e9a29eb24fc622d2d8d8e496`.
The requested duration was 600 seconds with `full@3`, starting at recorded host
time `2026-09-13T01:38:03Z`. Both collectors exited 0 at their deadlines.

| Venue | Events | Initial snapshots | Reconnects | Maximum control lag |
|---|---:|---:|---:|---:|
| Polymarket | 31,229 | 40/50 | 0 | 5.676 s |
| Kalshi | 6,537 | 25/25 | 0 | 3.058 s |

Polymarket resolved all six qualifying temporary locks, with no expired or
pending candidates. Ten missing initial snapshots caused no reconnect. Kalshi
completed one targeted snapshot refresh successfully without escalation. Both
captures remain partial with eligibility unevaluated and acceptance false;
Polymarket additionally retains its ten missing-initialization reasons.

This run verifies the candidate under live traffic but is not a controlled
comparison with the earlier stress run: 38 PM tokens and 18 Kalshi tickers sent
deltas, and total workload differed. Multi-second control stalls remain. Bound
expiry and revoked-delay faults are established by deterministic regression
tests, not claimed to have occurred in this live run.

Probe metadata, selection, exact patch/source hashes, manifests, raw events,
resource observations, summaries, and strict validation outputs are retained in
`/home/erike/pr5-final-review-2/` on `erik-pc1`. An earlier review candidate probe
was stopped and superseded; it is not the final verification run.

Both complete manifest/artifact validations passed with zero errors before
commit or push: Polymarket in 318.62 seconds and Kalshi in 42.04 seconds.

### Cancellation handoff follow-up

Review of the bounded Polymarket receiver exposed a cancellation race: a helper
could dequeue frame A, then the application consumer could be cancelled before
receiving it. A later iterator on the same connection would begin with frame B.
The helper now waits on a readiness event without consuming a frame; dequeue
and delivery remain together in the consumer. Socket replacement still discards
the old connection's queue, and the buffer remains bounded.

Four deterministic cases cancel before readiness or at the ready handoff, then
either resume the same connection or reconnect. The ready/resume case reproduced
frame loss on Python 3.10 before the fix. The prior live probe records the source
that became `7b36a21`; it does not exercise this subsequent cancellation fix.

Follow-up validation on Python 3.10: **1,425 passed, 2 skipped**. Repository
hygiene, pytest-lane coverage, Ruff, mypy, and public book/price/midpoint/history
contract checks passed.

Full-tree integration checks used main `fd8a375`, draft #3 at `6b6d121`, and
draft #4 at `afd0a91`, with the cancellation fix applied to each combined tree:

| Combined tree | Integration | Full Python 3.10 suite |
|---|---|---:|
| #5 + #3 | One conflict in the quiet-PONG test; resolved as below | 1,823 passed, 2 skipped |
| #5 + #4 | Clean merge | 1,433 passed, 2 skipped |

Both combined trees also passed hygiene, pytest-lane coverage, Ruff, and mypy.
The #3 combination passed the public API contracts. Its full suite includes
feeds, durability, reconstruction, public workflows, installed-wheel behavior,
API inventory, and optional-dependency boundaries.

After #5 merges, synchronize #3 with main and retain its deterministic
clock/event orchestration in the shared heartbeat test. Assert that receiving
only PONG leaves `last_frame_sequence` at zero, and that the receiver survives
application-waiter cancellation but is cancelled on client closure. Preserve the
other #5 tests. #4 remains independent; this integration check does not replace
its own review. Neither draft branch was changed by these isolated checks.
