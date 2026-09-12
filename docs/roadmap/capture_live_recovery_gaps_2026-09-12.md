# Live capture recovery gaps (2026-09-12)

Status: implemented and validated in PR #5. The PR remains draft for review;
no merge or scaling has been performed.

## Original evidence

Two 10-minute probes on `erik-pc1` followed PR #1 (`3690f5b` / `a6165f5`),
which separated conservative quote validity from initialized-book integrity.
The original probe artifacts have not been re-audited as part of the diagnosis.

| Selection | Profile | Reported result |
|---|---|---|
| 5 PM + 5 Kalshi | PM `full@3`; Kalshi `full@3`, then `full@2` | PM 0 reconnects, 381 events; Kalshi v3 commit failure, v2 completed 33,599 events |
| 10 PM + 10 Kalshi, 24h volume and unique events | both `full@2` | PM 7,834 events, 11 reconnects / 3 socket recoveries, 18/20 snapshots; Kalshi 682 events, 0 reconnects, 8/10 snapshots |

The previous 10x10 selection is stored on the host at
`/home/erike/pmkt-trading/runs/ws-probe/selection-active-10.json`.

## Initialization is instrument coverage

Subscription tracking, book initialization, and connection health are separate
concerns. Missing initial snapshots remain visible in subscription/evidence
tracking, overdue-SLA accounting, and completeness. They no longer generate
socket recovery actions, even when every instrument on a connected shard is
uninitialized. Missing snapshots consume no reconnect budget and do not reset
healthy peers or emit reconnect tape controls.

This applies to both venues. Kalshi no longer sends automatic targeted refresh
requests solely for overdue initialization, removing their timeout-to-reconnect
path. Targeted refresh and escalation for initialized books with proven integrity
failures are preserved, as are transport-disconnection recovery and retry bounds.
No quarantine, dropping, or new Polymarket refresh mechanism is introduced.

An explicit empty snapshot initializes a book; deltas alone do not. A later
snapshot initializes the existing tracked instrument normally. Before that
snapshot, deltas remain capture observations but cannot enter the reconstruction
tape: there is no checkpoint against which to apply them. Raw/parsed observations
remain available when enabled by the profile, and instrument evidence remains
available in profiles v2/v3. No synthetic initial checkpoint is created.

## Kalshi projected-row integrity

The reported v3 failure was `book_integrity_valid cannot accompany unresolved
book failures`. The confirmed local reproduction is a locked book with YES and
NO bids both at 0.50: native state integrity is true, while canonical topbook
construction flags both rows as `crossed_book`. The state checks strict crossing;
topbook construction also flags equality. The original live payload has not been
verified, so a NO-only crossing is not asserted as its established cause.

Stored row integrity is now upstream integrity AND absence of unresolved failures
on the emitted row, using the same flag definition as validation. Locked/crossed
rows remain flagged and have false row integrity. Empty-side and stale-only flags
do not invalidate an otherwise intact book. Upstream initialization, sequence,
and other integrity failures cannot be promoted to valid by projection.

Durability validation remains strict; rows are corrected before writing. Native
book integrity and recovery decisions retain their existing semantics. No
canonical schema version or price-normalization policy changes.

## Eligibility reporting

Completeness reports, recovered reports, and connection-group summaries add
`eligibility_evaluation_status`:

- `unevaluated`: no classified instruments or no evidence summary;
- `partial`: classified instruments and unknown verdicts coexist;
- `evaluated`: a nonempty classified set has no unknown verdicts.

Eligible and ineligible (excluded) verdicts count as classified. Existing
`capture_status`, execution status, legacy status, coverage denominators,
acceptance gates, and CLI exit behavior are unchanged. Unknown eligibility can
still make the conservative capture verdict partial; the independent label
explains why. It never hides persistence failures or missing eligible snapshots.

CLI summaries show eligibility evaluation, unknown count, and eligible snapshot
coverage separately from total snapshot coverage. Older manifests without the
new field retain their previous display. No catalog calls or CLI flags are added.

## Acceptance and deferred work

Deterministic coverage includes silent/delta-only peers beyond the 30-second SLA,
high delta traffic before initialization, late/empty snapshots, and unchanged
transport/corrupt-book recovery. Storage tests cover locked, crossed, one-sided,
and normal books in full and checkpoint profiles. Reporting tests cover unknown,
mixed, classified, absent evidence, and failure cases.

Run repository hygiene, test-lane coverage, Ruff, mypy, full pytest, and contract
checks. Then repeat the 600-second dual-venue probe on `erik-pc1` using `full@3`
and the recorded 10x10 selection; retain commit, selection, manifests, counters,
and recovery causes. Do not claim that a live run exercised a fault absent from
its evidence; use deterministic tests for that condition.

Broader corruption isolation, catalog eligibility acquisition, REST preflight
selection, quarantine, and scaling are deferred. Volume or liquidity alone does
not establish present activity or eligibility.

## PR #5 validation record

Implementation revision: `98fea1427ef1979c9a668301ac9d3b1a3777b653`.
Local verification: 1,369 tests passed, 2 skipped; hygiene, pytest-lane coverage,
Ruff, mypy, and the public API contract check passed. CI also passed on Python
3.10, 3.11, and 3.12. The PONG regression test was made independent of Windows
sub-20ms timer scheduling using actual reply synchronization and an injected
clock; production transport behavior was not changed.

The 600-second probe on `erik-pc1` used the previous 20-token / 10-ticker
selection, `full@3`, and an isolated checkout. Both collectors imported the
implementation revision's source and exited 0 after reaching their deadlines.

| Venue | Events | Final-attempt initial snapshots | Socket recoveries | Transport reconnects | Eligibility |
|---|---:|---:|---:|---:|---|
| Polymarket | 9,983 | 14/20 | 0 | 7 | unevaluated (20 unknown) |
| Kalshi | 1,111 | 10/10 | 0 | 0 | unevaluated (10 unknown) |

Polymarket recorded zero supervisor recovery actions across 1,158 evaluations.
Six final-attempt instruments lacked valid initial snapshot evidence. Its seven
reconnects came through the transport retry path on a ~43s cadence after 4.6
minutes. The initial diagnosis confused the market and sports heartbeat
protocols. Market clients send `PING` and receive `PONG`; the former
server-PING explanation is withdrawn. These runs do not establish transport
stability or prove why any particular reply was delayed.
Kalshi issued no targeted refreshes and finalized v3 without a commit failure.
Both conservative capture verdicts remain `partial`, with eligibility reporting
and missing-snapshot reasons preserved independently.

Follow-up revision: `c1f62e67073547a8a29576ba932714faa27c2fac` answers inbound
Polymarket application `PING` with `PONG` before later commits delay the
iterator. A second 600-second `full@3` probe on `erik-pc1` with the same
selection imported that revision and exited 0:

| Venue | Events | Final-attempt initial snapshots | Socket recoveries | Transport reconnects | Eligibility |
|---|---:|---:|---:|---:|---|
| Polymarket | 4,273 | 12/20 | 0 | 0 | unevaluated (20 unknown) |
| Kalshi | 3,127 | 10/10 | 0 | 0 | unevaluated (10 unknown) |

Polymarket tape had zero `reconnect` controls. Supervisor recovery actions
remained 0. Missing snapshots remain coverage gaps; absence alone does not establish
that these instruments have no book or are inactive.

Probe metadata, selection, logs, manifests, and artifact-validation results are
retained under `/home/erike/pmkt-core-pr5-98fea14/tmp/pr5-live/`.
Both manifests passed full artifact validation with no errors. Polymarket tape
contains 140 reconnect invalidations (20 instruments x 7 transport retries);
Kalshi has none. These do not originate from missing-initialization recovery.
PR remains draft; no merge or scaling was performed.

25-market follow-up (`full@3`, 50 PM tokens + 25 Kalshi tickers, same host):

| Revision | PM reconnects | PM cadence | KX reconnects | KX snapshots |
|---|---:|---|---:|---|
| `24122ba` (PING reply only) | 13 | ~43s | 1 | 24/25 |
| `13a3250` (PONG expiry on reader) | 13 | ~43s | 0 | 24/25 |
| `6fb5cf7` (no outbound-PING deadline) | **6** | 67–139s, irregular | **0** | **25/25** |

Removing the outbound-PING deadline removed the regular cadence, but also
removed bounded silence detection. It did not establish that the market venue
does not answer PING. Of the remaining six reconnects, two followed supervisor
`crossed_book` controls; four took the transport retry path without a persisted
cause. Kalshi targeted refresh stayed healthy (4/4 successful). Aggregate
CPU/RSS did not show exhaustion, but 12.47 seconds of control-plane lag prevents
ruling out local blocking or backpressure.

The follow-up restores bounded transport liveness with a separate bounded
receive task, stall-aware silence detection, and persisted retry diagnostics.
Missing initialization still causes no recovery. Corruption recovery policy
is unchanged pending replay evidence. The market-channel heartbeat reference
is https://docs.polymarket.com/api-reference/wss/market; the sports-channel
reference is https://docs.polymarket.com/api-reference/wss/sports.


Replay of the `6fb5cf7` raw log reproduces the two supervisor recoveries at
sequences 2880 and 4280. The first pair becomes `0.50/0.50`; the second becomes
`0.88/0.88` and `0.12/0.12`. These are local locked books, while the triggering
deltas advertise unlocked best prices (`0.50/0.51`, `0.49/0.50`, `0.88/0.889`,
and `0.111/0.12`). This confirms disagreement between reconstructed depth and
venue best-price hints. It does not establish whether a deletion was missing,
delayed, or incorrectly applied. Replay evidence is retained beside that
probe as `crossed_replay.json`. Investigate this separately before changing
corruption policy; best-price hints must not invent depth.


Instrumented follow-up at `927e6f565ec5ec3bde2d5966a12ef2bd96879cb2`:

- Same 50-token / 25-ticker selection, SHA-256
  `5a883ea2c9dac100b68da232358d6c2b54399fb39da3ecceda5741c7e3b952bb`.
- Isolated checkout `/home/erike/pmkt-core-pr5-liveness`; both imports verified.
  Started 2026-09-12 21:31:58 UTC, requested 600 seconds, `full@3`.
- Both exited 0 at their deadlines. Polymarket recorded 8,453 events and
  28/50 final-attempt initial snapshots; Kalshi recorded 3,562 events and 25/25
  initial snapshots. Both remain `partial` with eligibility `unevaluated`.
- Polymarket had six reconnects, all six from supervisor `book_integrity`
  recovery, with no transport exception or heartbeat failure. The triggering
  rows are locked books at sequences 1326, 2483, 4213, 5483, 7115 and 8343.
  Kalshi had zero reconnects and zero targeted refresh requests in this run.
- All six persisted retry records reconcile exactly with the manifest. Each
  records receive backpressure and a full 64-frame application queue. Maximum
  control-plane lateness was 12.83 seconds. This is evidence of local pressure;
  aggregate resource headroom cannot rule it out.
- Both finalized manifests passed full artifact validation with zero errors
  (Polymarket 218 seconds; Kalshi 21 seconds). Results are in `validation.json`.
- Artifact directory:
  `/home/erike/pmkt-core-pr5-liveness/tmp/pr5-live-25x25/`.
  The four transport retries in the older run were not reproduced; their
  historical causes remain unknown. This is not a scaling acceptance claim.
- Local full suite: 1,385 passed, 2 skipped; final heartbeat/retry coverage:
  105 passed. Hygiene, lane coverage, Ruff, mypy and public API contracts passed.
  All 17 CI checks passed on `927e6f5`, including Python 3.10-3.12 test lanes.

Next work should isolate the first depth/best-price disagreement from raw
messages and measure commit work that blocks the event loop. Preserve strict
locked-book integrity, subscription evidence and current corruption recovery
until a separate change has reproduction-backed semantics. PR remains draft.
