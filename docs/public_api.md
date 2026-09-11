# Public Python API

This inventory records the import surface shipped by `pmkt` 0.1.x. It is a
compatibility baseline, not a preview: names appear here only when they are
importable from the current package. The machine-readable facade inventory is
[`tests/fixtures/public_api_inventory.json`](../tests/fixtures/public_api_inventory.json),
and CI checks every listed lazy and eager export.

## Compatibility tiers

- **Supported workflow** means a delivered high-level workflow with explicit
  result types and error semantics. The pinned history catalog described below
  is the first workflow in this tier; the proposed REST workflows have not
  shipped yet.
- **Retained native** means an existing venue method, model, or canonical
  schema/storage contract kept under its current lifecycle. These APIs remain
  compatible, but do not acquire the guarantees of a future high-level
  workflow merely by being importable.
- **Inherited** means an existing convenience or advanced surface preserved
  pending deliberate migration. Its presence in `__all__` is not a promise of
  the supported-workflow tier.

The package root deliberately exports only `pmkt.__version__`. Import clients,
records, and utilities from their owning modules.

## Pinned history catalog

`pmkt.catalog` exports `CatalogSnapshot`, `CatalogReference`,
`CatalogValidationReport`, and `CatalogQueryResult`. The opener selects only
the published history pointer, pins the selected manifest and view contract,
and requires an explicit publisher path base:

```python
from pathlib import Path

from pmkt.catalog import CatalogSnapshot

path_base = Path("/srv/pmkt-publisher")
with CatalogSnapshot.open_latest_history(
    Path("data/markets"), path_base=path_base
) as catalog:
    result = catalog.query(
        "SELECT venue, market_key, question FROM market_catalog ORDER BY venue, market_key",
        max_result_rows=10_000,
        max_result_bytes=16 * 1024 * 1024,
    )
table = result.to_arrow()  # The bounded result remains valid after close.
catalog.reference.write_json("catalog-reference.json")
```

`open_latest_history` reads `market_root/history/LATEST.json`; it never falls
back to the current or discovery pointers and never follows the pointer again.
Use `CatalogSnapshot.open("catalog-reference.json")` to reopen the same
manifest. If a publisher tree moved without changing its contents, supply an
explicit mapping such as
`relocation={"/old/publisher": "/restored/publisher"}`. Relative manifest and
artifact identities remain anchored to the recorded path base. References are
written with exclusive creation unless `overwrite=True` is explicit.

Full validation verifies the selected manifest identity, declared direct
parent manifests, containment, file and row counts, canonical schema values,
and content hashes. `validation="metadata"` retains structural and containment
checks while the validation report names the content checks it skipped. Both
modes are local-only and reject path escapes, network locators, missing files,
and changed declared metadata. Full validation additionally rejects changed
content; metadata validation can accept same-size, same-count content changes
because it explicitly skips content hashes and canonical values.

Managed queries use a private DuckDB connection with access limited to the
exact validated Parquet files. One DuckDB-parsed SELECT-class statement and
typed scalar parameters are accepted. Row and byte caps raise
`ResultLimitExceededError` without returning a truncated result, and the
connection remains reusable after a cap failure. `PRAGMA version` is accepted
because DuckDB classifies it as a SELECT; configuration PRAGMAs, writes,
attachments, extension operations, replacement scans, undeclared files, and
network scans are blocked.

Catalog pinning depends on publishers retaining immutable files; it cannot stop
an external process from changing or deleting evidence. The DuckDB restrictions
provide reproducible dependency boundaries, not a sandbox for hostile SQL, and
result caps bound retained Arrow output rather than DuckDB intermediate memory
or process RSS. For async callers, run the entire synchronous open, query, and
close lifetime on one owned worker thread. Unsupported saved view-contract
versions raise instead of being reinterpreted. Fixed catalog and parameter
inputs also do not make unordered, random, or time-dependent SQL deterministic.

The locally qualified dependency floor is DuckDB 1.5.5 with PyArrow 14.0.0.
The catalog compatibility workflow is configured to exercise DuckDB 1.5.5 on
Windows and Linux with Python 3.10 through 3.12, using both PyArrow 14.0.0 with
NumPy 1.26.4 and PyArrow 25.0.1 with NumPy 2.2.6; those matrix results remain a
CI acceptance check rather than local evidence.

The pinned view contract provides these grains:

| View | Grain and identity | Interpretation provenance |
| --- | --- | --- |
| `market_catalog_polymarket` | One selected-manifest Polymarket history row; `market_id` is exposed | `family_provenance='venue_identity'`; slug remains an operational field |
| `market_catalog_kalshi` | One selected-manifest Kalshi history row; the native market key is exposed | Family labels come only from validated artifact-root-relative partition labels, with the retained ticker fallback recorded explicitly |
| `market_catalog` | One row per native view row; `venue` and `market_key` are exposed | Cross-venue common columns only; venue-specific fields remain on native views |

History views do not promise uniqueness for `market_key` alone or for
`(venue, market_key)`. Add an `ORDER BY` when result order matters.

For a fully offline example, repository maintainers can create canonical
synthetic evidence with:

```console
python scripts/create_synthetic_catalog_fixture.py --output generated/catalog-example
```

The script is repository tooling. Consumer examples then use only the public
API: open `generated/catalog-example/CATALOG_REFERENCE.json` with
`CatalogSnapshot.open(...)` and query the three views above. Production
maintenance requires an already initialized catalog with a retained published
history release. From that initialized state, the production acquisition and
reader sequence is:

```console
cd /srv/pmkt-publisher
pmkt markets discover-new --all --market-root data/markets
pmkt markets refresh-current --scope all --market-root data/markets
pmkt markets promote-history --market-root data/markets
```

```python
from pathlib import Path

from pmkt.catalog import CatalogSnapshot

with CatalogSnapshot.open_latest_history(
    Path("data/markets"), path_base=Path("/srv/pmkt-publisher")
) as catalog:
    verified = catalog.query("SELECT count(*) AS rows FROM market_catalog")
```

`discover-new --all` advances all discovery streams. The first current census
must use `refresh-current --scope all` before promotion. Promotion advances an
initialized history catalog; it does not bootstrap the first history release.
The offline synthetic fixture and query above are bounded executable
verification. The sequence runs from the same declared publisher `path_base`:
discovery and refresh access live public APIs, `refresh-current --scope all`
performs the full live census, and promotion is local. The CLI options were
checked, but the live acquisition sequence was not run as part of the offline
catalog qualification.

## Retained native venue clients

The public async clients are `pmkt.exchanges.polymarket.AsyncGammaClient`,
`pmkt.exchanges.polymarket.AsyncClobClient`, and
`pmkt.exchanges.kalshi.AsyncKalshiClient`. `GammaClient`, `ClobClient`, and
`KalshiClient` are compatibility aliases for those async classes. Use
`async with`; calling a synchronous context manager raises `RuntimeError`.

`AsyncGammaClient` retains these native methods:

- `market(market_id)` and `market_with_events(market_id)` return raw Gamma
  objects as dictionaries.
- `markets_page(...)` and `events_page(...)` return validated `Market` and
  `Event` models. Their `iter_` variants yield the same model grain.
- `markets_raw_page(...)` returns raw market dictionaries.
- `markets_keyset_page(...)`, `markets_keyset_raw_page(...)`, and
  `iter_markets_keyset(...)` expose the existing keyset endpoint behavior.

One page method call has page grain; one iterator item has market or event
grain. Offset methods accept `limit` from 1 through 1000. Keyset methods accept
`limit` from 1 through 100. Current filters are passed to Gamma as implemented;
they are not the proposed `PolymarketFilter` contract.

`AsyncClobClient` retains `book(token_id)`, `books(token_ids)`,
`clob_market_info(condition_id)`, `price(token_id, side)`,
`midpoint(token_id)`, `fee_rate(token_id)`, `prices_history(...)`,
`batch_prices_history(...)`, and `event_prices_history(...)`. A `book` result is
one token snapshot represented by `pmkt.models.OrderBook`; `books` preserves the
server response as a list of token snapshots. `prices_history` returns one
`PriceHistory` series whose points contain Unix seconds and a sampled price.
These are native endpoint results, not the proposed typed `get_book` or
historical-observation workflows.

`AsyncKalshiClient` retains:

- market and event reads: `markets_page`, `market`, `historical_market`,
  `iter_markets`, `events_page`, and `iter_events`;
- books: `orderbook` and `normalized_orderbook`;
- time series: `market_candlesticks`, `batch_market_candlesticks`,
  `historical_market_candlesticks`, `historical_cutoff`, `trades`, and
  `historical_trades`;
- series metadata: `series` and `series_list`.

Kalshi page methods return the native response envelope as a dictionary; the
iterators yield one market or event dictionary at a time. `orderbook` returns
the native YES/NO bid envelope, while `normalized_orderbook` returns the
existing policy-neutral normalized dictionary. Candlestick and trade methods
retain upstream units and layouts; no cross-venue candle schema is promised.
The default `markets_page` and `iter_markets` status is `"open"`.

The narrow read-auth surface is importable from `pmkt.exchanges.read_auth`:
`ReadAuthHeaderProvider`, `ReadAuthenticationRequiredError`,
`ReadOnlyRequestError`, and `headers_for_read`. Authenticated access accepts an
injected header provider. Non-GET methods are rejected before authentication or
network transport.

## Models, resolution, and errors

The retained wire models are `Event`, `Market`, `Order`, `OrderBook`,
`PriceHistory`, and `PriceHistoryPoint` from `pmkt.models`. They validate the
fields currently declared by the models and allow additional upstream fields;
they are not canonical persisted records.

`pmkt.resolution` exports `ResolutionRecord`, `Payout`, `SourceObservation`, and
their current state, result-type, confidence, and resolver-version constants.
The existing resolvers remain directly importable as
`pmkt.resolution.polymarket.PolymarketResolutionResolver` and
`pmkt.resolution.kalshi.KalshiResolutionResolver`. Read-only Polygon evidence is
available as `PolygonCtfClient` and `EvmRpcError` from `pmkt.resolution.evm`.
Resolver package reexports beyond these records have not shipped.

Native REST methods use `ValueError` for invalid caller parameters, `TypeError`
or `ValueError` for malformed upstream payloads, and propagate `httpx` request
and HTTP status exceptions. WebSocket helpers additionally use
`WebSocketProtocolError` or their documented frame/state errors. `pmkt.errors`
exports `CatalogError`, `OptionalDependencyError`, and
`ResultLimitExceededError` for the supported catalog workflow.

## Canonical data and storage contracts

`pmkt.data` reexports the registered `*_COLUMNS`, `*_SCHEMA_VERSION`, row
builders, registry lookup functions, physical validators, provenance hashes,
and sink classes. These are retained native contracts even when a schema name
describes paper, order, matching, or tracking data: core owns policy-neutral
physical schemas, while consumer policy remains outside this package.

The authoritative list of schema versions, lifecycle states, compatibility
decisions, and removal gates is
[`schema_lifecycle.json`](schema_lifecycle.json). Grain, keys, timestamps,
units, nullable fields, and provenance columns are documented in
[`data_dictionary.md`](data_dictionary.md). Schema versions are immutable
identifiers; changing grain, meaning, units, or required provenance requires a
new version and an explicit lifecycle decision. `pmkt.data.storage.read_parquet`
and `write_parquet` retain their current file-level storage contract.

The following `pmkt.data` conveniences are inherited rather than promoted:
`find_event_by_slug`, `fetch_trade_history`, `trade_history_dataframe`,
`iter_order_book_metrics`, `order_book_summary_dataframe`,
`collect_order_book_summaries_parquet`, `batched`, `compute_features`,
`join_market_metadata`, and `logit`, plus their defaults. In particular,
`fetch_trade_history` returns sampled prices, not exchange trades; absent size
remains absent. Keep the import for compatibility, but do not use that name in
new quickstarts.

## Inherited advanced surfaces

The complete inherited facade membership is recorded in the machine-readable
inventory. It currently includes `pmkt.data.market_catalog`,
`pmkt.market_structure`, `pmkt.streaming`, `pmkt.text`, `VenueAdapter`, the
Polymarket Data API and Subgraph clients, venue WebSocket clients and decoding
helpers, capture stream entry points, and Kalshi normalization/dataframe
helpers. These imports remain available. They have not been qualified as the
new supported REST or capture workflows. The supported pinned reader is the
separate `pmkt.catalog` facade.

`MarketCatalogService` remains the mutable maintenance service and the CLI
`query` command exposes an unmanaged DuckDB connection. Use `pmkt.catalog` for
the pinned reader and bounded managed query result. WebSocket and capture
helpers retain their existing recovery and evidence behavior until the
separate capture qualification milestone.

## Optional dependencies

The base install provides REST clients, configuration, models, resolution
records, the catalog reference/result types, and the CLI. Importing the catalog
facade does not import pandas, PyArrow, or DuckDB. Install `pmkt[data]` to open,
validate, and query catalogs with pandas, PyArrow, and DuckDB.
`pmkt[storage]` currently installs the same storage stack.
`pmkt[streaming]` adds pandas, PyArrow, and `websockets`. Conversions and modules
that require an extra may fail at call or import time when that extra is absent;
the base package does not implicitly install those dependencies.
