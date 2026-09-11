# Public Python API

This inventory records the import surface shipped by `pmkt` 0.1.x. It is a
compatibility baseline, not a preview: names appear here only when they are
importable from the current package. The machine-readable facade inventory is
[`tests/fixtures/public_api_inventory.json`](../tests/fixtures/public_api_inventory.json),
and CI checks every listed lazy and eager export.

## Compatibility tiers

- **Supported workflow** means a delivered high-level workflow with explicit
  result types and error semantics. No proposed REST-to-workflow or managed
  catalog API has shipped at this inventory point.
- **Retained native** means an existing venue method, model, or canonical
  schema/storage contract kept under its current lifecycle. These APIs remain
  compatible, but do not acquire the guarantees of a future high-level
  workflow merely by being importable.
- **Inherited** means an existing convenience or advanced surface preserved
  pending deliberate migration. Its presence in `__all__` is not a promise of
  the supported-workflow tier.

The package root deliberately exports only `pmkt.__version__`. Import clients,
records, and utilities from their owning modules.

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
`WebSocketProtocolError` or their documented frame/state errors. There is no
shipped `pmkt.errors` facade yet.

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
new supported REST, catalog, or capture workflows.

`MarketCatalogService` is the existing mutable maintenance service and the CLI
`query` command exposes an unmanaged DuckDB connection. Neither is the proposed
pinned catalog reader or managed query result. WebSocket and capture helpers
retain their existing recovery and evidence behavior until the separate capture
qualification milestone.

## Optional dependencies

The base install provides REST clients, configuration, models, resolution
records, and the CLI. Install `pmkt[data]` for pandas, PyArrow, and DuckDB data
operations. `pmkt[storage]` currently installs the same storage stack.
`pmkt[streaming]` adds pandas, PyArrow, and `websockets`. Conversions and modules
that require an extra may fail at call or import time when that extra is absent;
the base package does not implicitly install those dependencies.
