# Storage Profile Capture Runbook

This runbook covers CR-18 profile captures, crash recovery, and offline tape
reconstruction. Reconstructed books are research/audit evidence, not runtime or
execution authority.

## Choose and validate a profile

`full` is the stable initial default. It preserves legacy datasets while also
recording all committed CR-18 evidence. `book-tape` and `mm-compact` are
experimental until the CR-18.9 operational gates and maintainer acceptance are
durably recorded, so either requires `--acknowledge-experimental-profile`.

```powershell
pmkt stream-books --token-id TOKEN --storage-profile full --duration 3600
pmkt stream-kalshi-books --ticker TICKER --storage-profile book-tape --acknowledge-experimental-profile --duration 3600
```

Profile, interval, acknowledgement, override, plan, preflight, and credential
validation finishes before a run directory or websocket is opened. Only after
those side-effect-free checks succeed does an experimental-profile warning go
to stderr. The acknowledgement is persisted as
`experimental_profile_acknowledged=true` in both manifest and run-state
provenance. Overrides only add evidence. Prefer the defaults unless a
measurement or audit requirement calls for `--keep-raw-jsonl`,
`--topbook-emission-per-event`, `--emit-full-depth`, or
`--emit-legacy-book-artifacts`.

## Verify a clean run

For a clean run:

1. `manifest.json` has `status=success`.
2. `storage_profile.terminal_completeness=complete`.
3. `dataset_artifacts` keys exactly equal `storage_profile.enabled_roles`.
4. Every artifact is `closed`, has a readable segment manifest, and its
   segment-manifest SHA-256 matches.
5. `successfully_committed_roles` exactly equals the closed role set.
6. `run_state.v1.json` is `finalized` and was written after the manifest.
7. `capture_commit_journal.v2.jsonl` validates without gaps, mixed versions,
   invalid causes/indexes, or corrupt checksums. Legacy v1 journals remain
   recovery-readable and report `cause=legacy_unknown`; they are never rewritten.
8. `capture_durability.configuration` exactly matches run state, and its terminal
   group/cause/latency metrics reconcile with the journal.

Do not infer completeness from the presence of a `.parquet` path or a tape
header. Empty mandatory datasets are valid only when their zero-row segment is
journaled and closed.

## Recover a crashed run

Start report-only:

```powershell
pmkt recover-stream-run generated\order_book_streams\RUN_DIR
```

Review journal errors and orphan paths. If the report is consistent, finalize:

```powershell
pmkt recover-stream-run generated\order_book_streams\RUN_DIR --finalize
```

Finalization moves unjournaled parquet or raw files under `_orphans`, promotes
only journaled groups, records `capture_termination=crashed`, and writes
`partial` when journaled evidence exists or `failed` otherwise. Never copy an
orphan segment back into a committed role manually.

The worst-case uncommitted window is the first **effective** row/time threshold,
capped at 30 seconds. Where a requested limit differs from the enforced one, the
manifest records both and the reason. Row threshold wins if both thresholds are
due together. Startup, resync, and periodic checkpoints, compact recovery
controls, invalidations, termination, and shutdown force barriers earlier.

Journal-v2 runs persist publication mode, coalescing window, the fixed
15-second publication deadline, queue capacity, segment thresholds, journal
version, and requested/effective adjustments. The initial publication mode is
`inline`; the recorded coalescing and queue fields do not relax synchronous
barriers until the separate async canary is accepted. Inline mode reports zero
queue depth and queue-full waits.

Recovery guarantees **child-process crash consistency**: if the capture process
dies while the operating system keeps running, only complete journaled groups
are promoted. This is not a host or power-loss durability claim. On Windows,
directory entries created by rename are not fsynced, so survival of a sudden
power loss or hard reset is not claimed on any platform without the named,
tested configuration required by CR-18.9.

## Reconstruct book tape

```powershell
pmkt reconstruct-book-tape --manifest generated\order_book_streams\RUN_DIR\manifest.json --out-dir generated\reconstructed\RUN_DIR
```

The reconstructor fails closed on manifest/journal gaps, count or payload/hash
mismatches, duplicate keys, unsupported sides, invalid numeric values, causal
ordering failures, or deltas outside an open epoch. Explicitly
non-reconstructible audit deltas are reported and skipped. Use
`--venue-book-id` for a focused audit.

Inspect `book_tape_reconstruction_report.v1.json` before using the output. A
successful report records source hashes, journal coverage, ignored events,
epoch coverage, and recorded/reconstructed topbook comparisons.

## Replay scope

Core provides capture evidence and book-tape reconstruction. Strategy replay
and simulated-fill replay belong to consuming applications and are not exposed
by the `pmkt` CLI.

## Operational acceptance

Do not change the default or call a reduced profile stable based on a local
smoke test. CR-18.9 requires the tracked two-venue suite, hashed one-hour
concurrent per-profile venue captures with ten-hour projections, storage and
feed-health reduction measurements, throughput impact, documented
uncommitted-window evidence, a child-process crash matrix, committed
subscription-time capture-completeness evidence, independent review, and
explicit maintainer acceptance. That leaf is intentionally not auto-merged.

Storage figures are reference targets, not cutoffs. Report measured sizes
against them with the exact universe and window stated. A missed target needs a
target-variance record — enumerating every known remaining reduction with its
measured benefit — reviewed independently and decided by the maintainer.
Until that exists, profile stabilization and the default flip stay blocked; the
rest of the leaf does not.

## Explicit v3 capture and recovery qualification

Both venue CLIs accept `--profile-version 3`; their default remains v2. For
example, `pmkt stream-books --token-id <TOKEN> --storage-profile full
--profile-version 3 --duration 60` selects the integrity-aware full contract.
For Kalshi use `stream-kalshi-books --ticker <TICKER>` and the existing narrow
read-auth `--header-provider`. Reduced profiles still require explicit
experimental acknowledgement. No `mm-compact@3` contract exists.

Before merging recovery changes, run the cross-venue cancellation and transport
checks, ignored/failed Kalshi snapshot-refresh tests, Polymarket heartbeat and
reconnect tests, and tape durability/reconstruction tests. Qualification must
cover concurrent operation completion and cancellation, receive, reconnect,
backoff, journal boundaries, and manifest finalization. Cancellation must not
consume another retry or become a timeout; cancelled captures retain a valid
manifest with terminal reason `cancelled`.

These controlled offline failures establish software behavior. A healthy live
window with no recovery events does not establish live recovery behavior,
continuous coverage, or thesis acceptance. Keep completeness policy provisional
and experimental profiles unpromoted until separately recorded operational
acceptance evidence exists. Retain original capture artifacts and their hashes.

A process-loss recovery may have journaled book data but no terminal instrument
evidence. Recovery publishes a partial/failed manifest, reconciles zero committed
evidence rows, and records `terminal_instrument_evidence_not_committed`.
It does not reconstruct missing coverage evidence. Both reconstruction readers
continue to require successful clean captures and reject these partial runs.
