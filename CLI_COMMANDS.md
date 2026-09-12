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
