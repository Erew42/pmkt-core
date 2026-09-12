# Live capture recovery gaps (2026-09-12)

Status: implementation in PR #5; validation pending. Keep the PR draft until
required checks and the 10-minute dual-venue probe have completed.

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
