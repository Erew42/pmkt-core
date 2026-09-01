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

```bash
python -m pip install -e ".[test]"
python scripts/check_repo_hygiene.py
python scripts/check_pytest_lane_coverage.py .github/workflows/tests.yml tests
python -m ruff check .
python -m mypy src
python -m pytest -q
```

The repository has a clean initial history. Historical trading implementation
and operational artifacts are intentionally not part of it.
