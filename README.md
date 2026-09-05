# pmkt

`pmkt` is the public, read-only data plane for prediction-market research. It
provides venue clients, canonical schemas, local storage, streaming capture,
historical book reconstruction, market-structure discovery, and resolution
utilities for Polymarket and Kalshi.

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
pmkt discover-structures --markets data/polymarket_markets.parquet
pmkt resolve-market-resolutions --help
```

See [CLI_COMMANDS.md](CLI_COMMANDS.md) for the supported command surface and
[docs/data_dictionary.md](docs/data_dictionary.md) for canonical datasets.

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
