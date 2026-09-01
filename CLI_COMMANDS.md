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
- `query`: query local Parquet data with DuckDB.
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
