# pmkt

`pmkt` is the public, read-only data plane for prediction-market research. It
provides venue clients, canonical schemas, local storage, streaming capture,
historical book reconstruction, and resolution utilities for Polymarket and
Kalshi.

This repository deliberately contains no order signing or submission,
credential derivation, matching policy, opportunity selection, OMS/risk logic,
strategy runtime, or operator dashboard. Those consumers live in the private
`pmkt-trading` project and depend on this package.

## Install

Python 3.10 through 3.12 are supported.

```bash
python -m pip install -e ".[data,streaming]"
```

The base install supplies HTTP clients, schemas, models, and the `pmkt` CLI.
The `data`, `storage`, and `streaming` extras add local dataframe, Parquet,
DuckDB, and WebSocket support.

## Examples

```bash
pmkt --help
pmkt ingest-markets --out data/polymarket_markets.parquet
pmkt ingest-kalshi-markets --out data/kalshi_markets.parquet
pmkt resolve-market-resolutions --help
```

See [CLI_COMMANDS.md](CLI_COMMANDS.md) for the supported command surface and
[docs/data_dictionary.md](docs/data_dictionary.md) for canonical datasets.

## Capture recovery

Each venue collector uses one retry budget across initial connection failures,
receive failures, clean closes, and supervisor-requested replacement connections.
The initial attempt is free; each retry is charged once. Successful connections
and replacement message iterators do not reset the budget. Existing capture
limits remain unchanged. Transport retries retain linear backoff; the first
supervisor-requested attempt remains immediate.

Connection and backoff waits respect the capture deadline. Expiry during recovery
raises `WebSocketDeadlineExceeded` and persists the existing completeness result
with a `deadline_reached` reason; it does not turn incomplete books into accepted
data. Failed subscription setup closes the new socket. DNS exhaustion retains
the original exception and is classified as a stream error, not a storage error.

Direct client users can supply a `WebSocketRetryBudget` to share accounting across
context entry and iterator generations. When supplied, it owns the limit, backoff,
and retry callback instead of the iterator's per-call settings. Without one,
context entry remains a single attempt and `iter_messages` uses its existing
retry arguments. `reconnect=False` disables retries in that iterator.

These changes do not alter quiet-market recovery policy, heartbeat settings,
book-validation rules, or causal reconstruction ordering.

## Safety boundary

Core transports are public/read-only. In particular, the package does not ship
private-key loaders, generic signed HTTP transports, authenticated user
streams, or venue order endpoints. Kalshi feeds that require authenticated
read access accept a narrow read-auth provider supplied by a separate consumer;
the core package itself does not load private keys.

Generated data and local credentials belong in ignored directories such as
`data/`, `generated/`, or `local_data/`. Do not commit them.

## Development

Create a repository-local virtual environment without system-site packages and
use its interpreter for installation and checks (`.venv/Scripts/python.exe` on
Windows; `.venv/bin/python` on POSIX). Existing global installations need not
be changed.

```bash
python -m venv .venv
.venv/bin/python -m pip install -e ".[test]"
python scripts/check_repo_hygiene.py
python scripts/check_pytest_lane_coverage.py .github/workflows/tests.yml tests
python -m ruff check .
python -m mypy src
python -m pytest -q
```

Public Git history is retained. The current package contains only the public
read-side implementation.

## Implementation and artifact identity

`pmkt.provenance.implementation_identity` observes the loaded package, independently
of the caller's directory. Source installations report their own Git commit,
version, and dirty state. Wheels and source distributions embed the same fields;
rebuilding a wheel from an sdist preserves them without consulting enclosing Git
repositories. An unidentified source archive stays unidentified. Conflicting
embedded, source, or applicable distribution metadata is an error.

Run manifests preserve their existing identity fields and add `pmkt_core_dirty`
and `pmkt_core_provenance_source`. Caller metadata cannot override observed core
identity. Dataset schema versions are unchanged.

`validate_run_manifest(path, *, path_resolver=None)` optionally accepts a
`Callable[[Path], Path]` for legacy dataset references and declared run directories.
The caller owns any relocation policy. Exact artifact paths must still be
canonical and contained within the authoritative manifest directory; hashes,
schemas, counts, and journal bindings are always validated after resolution.
With no callback, existing path behavior is unchanged.

## Python API 0.2

See [the 0.2 migration guide](docs/migration_0_2.md) for deterministic configuration,
typed workflow inputs, shared request deadlines, and consolidated result provenance.
