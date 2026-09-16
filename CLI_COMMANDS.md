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

## Stream recording

`stream-books` and `stream-kalshi-books` use `recording.v1`: SQLite WAL during
recording, then verified Parquet exports. `--mode full` is the default;
`--mode topbook` omits full-depth snapshots. Both retain public trade reports.

- `--depth-check-interval-s 10`: check every ten seconds and save depth only
  when the book differs from the last saved snapshot.
- `--depth-on-best-price-change`: also save depth after best bid/ask changes.
- `--depth-check-interval-s off --depth-on-best-price-change`: use price changes
  as the only ongoing depth trigger. Initial/recovery/final snapshots remain.
- `--raw-messages`: additionally save diagnostic decoded messages to JSONL.

Use repeated `--token-id` / `--ticker` or a `--markets` Parquet file to select
instruments. Each invocation uses one connection. Duration, message limit,
reconnect budget and transport bounds remain explicit options. Kalshi requires
`--header-provider MODULE:ATTRIBUTE` and subscribes to public books, trades and
market lifecycle messages.

The old profile/version/override matrix, eligibility and acceptance flags,
Parquet live-backend selection, segment rotation and connection-group/process
flags are retired. Reports use `complete`, `partial` or `failed`; partial/failed
CLI runs exit nonzero. Missing eligibility metadata does not downgrade an intact
book. See [the recording contract](docs/stream_recording_contract.md).

`recover-stream-run` automatically recognizes SQLite recordings: without
`--finalize` it inspects committed state; with `--finalize` it exports/re-exports
committed rows. The Python equivalent is
`pmkt.streaming.export_recording(run_directory)`. An active recorder is locked
against concurrent export. `dataset validate-manifest` also recognizes the new
format. Historical profile recovery and `reconstruct-book-tape` remain available.

## Runtime configuration in 0.2

Application entrypoints explicitly load `PmktConfig.from_env()` and pass the
result to core clients. Python client construction alone uses deterministic
defaults. Repository API-check/example scripts now use `--max-attempts` for the
total request budget; `--max-retries` is removed. Existing `pmkt` command names
remain; live recording has the deliberate format migration described above.
