# `pmkt` command reference

The public CLI exposes only data acquisition, validation, storage, streaming,
reconstruction, market-structure, and resolution workflows. Run
`pmkt COMMAND --help` for the complete option reference generated from the
installed version.

## Command groups

- `schema list|show`: inspect canonical dataset schemas.
- `dataset validate|validate-manifest|stats|archive-run`: validate and manage
  local read-only dataset artifacts.
- `markets discover-new|refresh-current|promote-history|compact-history|status`:
  maintain a local market catalog.

## Data and query commands

- `ingest-markets`: fetch normalized Polymarket market snapshots.
- `ingest-markets-keyset`: fetch Polymarket markets by keyset pagination.
- `ingest-kalshi-markets`: fetch normalized Kalshi market snapshots.
- `query`: run DuckDB SQL, optionally registering local Parquet datasets as views.
- `compute-features`: compute book-derived data features.
- `record-topbooks`: record normalized top-of-book observations.
- `backfill-venue-history`: fetch public venue history.

## Capture and reconstruction commands

- `collect-books` / `stream-books`: collect or stream public Polymarket books.
- `collect-kalshi-books` / `stream-kalshi-books`: collect or stream Kalshi
  books. Authenticated read access requires a separately installed read-auth
  provider; core never loads a private key.
- `recover-stream-run`: validate and recover a durable capture run.
- `reconstruct-book-tape`: reconstruct books from committed capture evidence.

## Structure and resolution commands

- `discover-structures`: discover threshold, range, and outcome structures.
- `build-groups`: materialize discovered structures as canonical group tables.
- `resolve-market-resolutions`: build canonical market-resolution evidence.

## Excluded interfaces

Matching, tracking, opportunity scans, replay/strategy workflows, credentials,
deployment, execution, ledger, alerts, soak, runtime backup, and operator
commands belong to `pmkt-trading`. They are not registered by this package.

Storage capture commands `stream-books` and `stream-kalshi-books` accept
`--profile-version 3` with `--storage-profile full` or `book-tape` to select the
integrity-aware contract explicitly. Omitting `--profile-version` retains v2.
`book-tape` still requires `--acknowledge-experimental-profile`; unsupported
name/version pairs fail before capture or output creation.

### Capture eligibility reporting

`stream-books` and `stream-kalshi-books` summaries show total initial snapshots,
eligibility evaluation (`unevaluated`, `partial`, or `evaluated`), unknown
eligibility count, and eligible initial snapshots. The additive manifest field
`eligibility_evaluation_status` also appears in connection-group and recovered
completeness summaries. Eligible and excluded verdicts count as classified.
With no classified instruments the label is `unevaluated`; with both classified
and unknown instruments it is `partial`; otherwise a nonempty classified set is
`evaluated`. Older manifests without this field retain their previous display.

These labels describe eligibility evidence, independently of capture success.
Ad-hoc ids without evidence retain unknown verdicts and the existing conservative
capture status. Missing snapshots, persistence failures, acceptance gates, and
exit codes are unchanged. Missing initialization alone no longer reconnects a
connected socket on either venue; instruments remain tracked for coverage.


Capture reconnect diagnostics are retained in `reconnect_diagnostics.jsonl`
inside each run directory and in the optional manifest `reconnect_diagnostics`
list. Each replacement attempt records its origin and cause before book-state
invalidation. Polymarket includes heartbeat activity and bounded receive-queue
metrics; both venues include control-plane lag. No new CLI option is needed.
Failure to persist this sidecar stops the capture as a persistence failure.
These are replacement-attempt records; an exhausted-budget terminal error need
not have a corresponding replacement record.

Polymarket has separate transport and application receive buffers with the same
configured capacity. The manifest's `websocket_transport.effective` describes
transport limits, not the total connection memory budget. When the application
queue is full, later heartbeat frames also wait for downstream processing.

Polymarket manifests also record `complementary_delta_recovery`: bounded
deferrals for an initialized book that becomes locked during a price update
whose advertised top remains unlocked. The invalid row remains invalid; a
follow-up has at most 250 ms from the first recovery decision or 16 messages.
Bounds are checked before applying a later message and also cap idle waits.
A changed hash/timestamp that leaves the book invalid withdraws the delay even
without a change in health flags. Synchronous work can delay when these checks
run. `resolved` counts restoration by an actual update or authoritative snapshot.
Missing initialization never enters this path.

Both v2 and v3 tape profiles omit pre-snapshot deltas until a first baseline
exists, as strict commit validation requires a checkpoint. Raw/parsed roles
retain those observations when enabled; no initialization is inferred from them.

Version-3 Parquet captures batch routine checkpoint publication using the
existing durability coalescing window (one second by default). Pending rows
are not crash-durable until journal publication. Invalidations, termination,
and explicit forced commits still publish synchronously. See the
[capture runbook](docs/storage_profile_capture_runbook.md) for the exact boundary
and the additive `capture_durability.metrics.checkpoint_publication` diagnostics.

## Offline book-grid storage experiment

`python scripts/benchmark_book_grid.py --help` documents an offline storage
comparison for recorded single-segment public book traffic. Configure
`--interval 0.25` (or `0` for dense) and
`--retention all|no-sidecar|tape|states|raw-archive`. This is a repository experiment, not a
`pmkt stream-books` option or a replacement capture contract. See
[`docs/book_grid_storage_experiment.md`](docs/book_grid_storage_experiment.md)
for timestamp semantics, input requirements, measurements and reconstruction
limits. Generated outputs belong under ignored `tmp/` paths.
