# Public Python API

This inventory records the import surface shipped by `pmkt` 0.1.x. It is a
compatibility baseline, not a preview: names appear here only when they are
importable from the current package. The machine-readable facade inventory is
[`tests/fixtures/public_api_inventory.json`](../tests/fixtures/public_api_inventory.json),
and CI checks every listed lazy and eager export.

## Compatibility tiers

- **Supported workflow** means a delivered high-level workflow with explicit
  result types and error semantics. The pinned history catalog and the
  Polymarket and Kalshi discovery-to-book paths described below are in this tier.
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

## Shared references and request runtime

`pmkt.records` exports frozen Polymarket and Kalshi market/instrument
references plus `MarketRef` and `InstrumentRef` union aliases. Native lookup
identity determines equality and hashing; optional condition, series, parent,
and outcome-index enrichment does not. Constructors reject empty identifiers,
wrong parent-reference classes, non-YES/NO Kalshi sides, booleans and negative
outcome indexes before any venue request. The venue facades reexport their own
reference classes.

`PmktConfig.from_values(...)` creates independent endpoint settings from only
its arguments and declared defaults. It does not read OS variables, dotenv
files, or the process-wide legacy cache. `PmktConfig.from_env(...)` and direct
`PmktConfig(...)` construction retain environment-aware behavior. REST client
endpoint precedence is an explicit `base_url`, then a supplied `config`, then
the legacy cached configuration. REST clients accept compatible keyword-only
`config`, `timeout_s`, and `request_policy` arguments while retaining existing
positional meanings.

`RequestPolicy(max_attempts=N)` is the preferred spelling for the existing
total-attempt count. `max_retries=N` remains compatible and still means `N`
total attempts. Supplying both names is an error. CLOB retries read-only POSTs
only for `/books` and `/batch-prices-history`; arbitrary POST behavior and the
Kalshi pre-authentication GET-only boundary remain unchanged.

The private HTTP runtime accepts one monotonic expiry passed explicitly through
each nested call. It covers limiter acquisition, retry waits, transport, and
JSON parsing checkpoints, and caps each dispatched request timeout to the
remaining operation budget. The state is per call, so future workflow methods
can safely borrow one client with different concurrent expiries. Timeout and
caller cancellation drain owned work before returning, and the borrowed client
remains reusable. `OperationTimeoutError` reports expiry without converting
unrelated worker errors. Retained native methods remain unbounded when they do
not supply an expiry.

`RequestObservation` stores sanitized operation-local request provenance.
Only explicit endpoint templates and adapter-allowlisted effective parameters
are accepted; headers, raw bodies, URL credentials, queries, and raw dynamic
paths are not retained. Known secure default Gamma, CLOB, and Kalshi endpoints
receive conservative production/demo scope. Injected transports, custom paths,
ports, insecure endpoints, and unrecognized redirect targets remain unknown.

## Polymarket discovery and current books

The supported Polymarket facade stays venue-specific:

```python
from pmkt.exchanges.polymarket import (
    AsyncClobClient,
    AsyncGammaClient,
    PolymarketFilter,
    PolymarketInstrumentRef,
    PolymarketMarket,
    PolymarketMarketRef,
)
from pmkt.records import BookSnapshot, DiscoveryResult
```

`AsyncGammaClient.discover_markets(...)` returns
`DiscoveryResult[PolymarketMarket]`. Its `filters` argument accepts
`condition_ids`, `closed`, `tag_id`, `related_tags`, `question_contains`,
`outcome_count`, and `has_instruments`. Targeted condition IDs are deduplicated
in caller order and split into adapter chunks of 20. That chunk size is a
conservative client bound, not an upstream maximum. With `closed=None`, each
chunk visits `closed=false` and `closed=true` in round-robin order. With no
condition selector, the same lifecycle partitions form a bounded keyset scan.
Gamma defines row order within each page; cross-partition order is the
documented round-robin traversal order.

Gamma applies the condition, lifecycle, tag, and related-tag filters. The
adapter rechecks requested condition IDs and lifecycle values, then applies
the question, outcome-count, and instrument filters locally. Question matching
uses Unicode case folding. `outcome_count` can use a validated outcome-label
array even when the token array is unavailable. `has_instruments=False`
matches only a validated empty mapping; unknown or inconsistent mappings do
not match either boolean choice.

`max_markets`, `max_pages`, and `deadline_s` have positive defaults; caller
overrides must also be valid positive bounds. A literal empty
`condition_ids=()` selection performs no request. The report
states whether traversal ended because the result cap, page cap, source
exhaustion, or empty selection was reached. It also preserves page and row
counts, duplicate counts, applied server and local filters, unknown-filter
counts, bounded diagnostics, request observations, source/data scope, adapter
interpretation ID, package version, and traversal strategy. A result or page
cap is a bounded sample rather than a complete venue census.

Gamma normalization requires unambiguous native market and condition identity.
Outcome labels and CLOB token IDs may arrive as arrays or JSON-encoded arrays;
aliases must decode to equal arrays. Only equal-length, nonempty arrays with
unique nonempty token IDs produce `mapping_status="mapped"`. Validated empty
arrays produce `"empty"`; absent evidence produces `"unknown"`; malformed,
duplicate, contradictory, or length-mismatched evidence produces
`"inconsistent"`. Outcome prices are optional supplemental evidence. Invalid
prices are reported without inventing or discarding an otherwise valid
label-to-token mapping. `instrument_for_label(...)` uses exact, case-sensitive
label equality and fails when the mapping is unavailable, the label is absent,
or the label is ambiguous.

Fetch a known market by Gamma market ID with
`await gamma.get_market(market_id=..., deadline_s=...)`. The ID is encoded as
one URL path segment. A Gamma 404 raises `MarketNotFoundError` scoped to current
Gamma detail. Discovery and detail records retain a defensive copy of the
native payload and the actual `RequestObservation` used to produce them.

After selecting an instrument, fetch its current CLOB REST snapshot directly:

```python
async with AsyncGammaClient() as gamma, AsyncClobClient() as clob:
    result = await gamma.discover_markets(
        filters=PolymarketFilter(condition_ids=("condition-id",)),
        max_markets=1,
        max_pages=2,
        deadline_s=10.0,
    )
    instrument = result.items[0].instrument_for_label("Yes")
    book = await clob.get_book(instrument, depth=10, deadline_s=10.0)
```

`get_book` accepts only a `PolymarketInstrumentRef`. It validates prices in
`[0, 1]` and nonnegative quantities, removes zero-quantity levels, sorts bids
descending and asks ascending, then applies the optional positive depth per
side. Quantities are shares. Counts distinguish the native ladder, the
validated pre-trim ladder, and returned levels. Empty sides remain a valid
response payload but set quality flags and make `valid_state=False`; a crossed
book is also flagged. The exchange timestamp is UTC when the response supplies
a valid millisecond timestamp, otherwise it is absent.

The CLOB response `market` field is a condition ID. It is compared only with a
supplied condition-ID enrichment and is never interpreted as the Gamma market
ID. A supplied token or condition identity contradiction raises
`InvalidDataError`; absent response identity remains unverified in the request
observation. A CLOB 404 raises `MarketNotFoundError` scoped to the current token
book. A book is a current REST snapshot from one request, not a historical or
atomic cross-instrument view.

All three workflow methods require a finite positive deadline and fail before
I/O for `None`, booleans, nonpositive values, or nonfinite values. The deadline
covers transport, decoding, and normalization. Expiry raises
`OperationTimeoutError` and returns no partial result; the borrowed client
remains reusable. Malformed or contradictory upstream workflow data raises
`InvalidDataError`.

## Kalshi discovery, detail, and current books

The supported Kalshi facade exports `AsyncKalshiClient`, `KalshiFilter`,
`KalshiMarket`, `KalshiMarketRef`, and `KalshiInstrumentRef`. Discovery always
requires an explicit filter object. `KalshiFilter()` means the unrestricted
standard metadata dataset: the adapter sends no status restriction and includes
MVE markets. It does not include the historical archive. A common conventional
market selection is explicit:

```python
result = await kalshi.discover_markets(
    filters=KalshiFilter(status="open", mve_filter="exclude"),
    max_markets=100,
    max_pages=20,
    deadline_s=60.0,
)
```

The frozen filter supports `tickers`, `event_ticker`, `series_ticker`, `status`,
`mve_filter`, `question_contains`, and `has_instruments`. Status values are
`unopened`, `open`, `paused`, `closed`, and `settled`; MVE values are `only` and
`exclude`. A nonempty ticker selection is deduplicated in caller order and sent
as comma-separated native requests in adapter chunks of 20. This is a bounded
client choice, not a claimed venue maximum. Page and result budgets are global
across chunks, and chunks advance in round-robin order. The first observation
of a ticker wins before local filtering. Missing or inconsistent instrument
mapping does not satisfy either value of `has_instruments`.

Kalshi response statuses use a different vocabulary from query values, so the
adapter rechecks them through the retained status mapping: for example, query
`open` matches response `active`, and query `settled` matches `finalized`.
`question_contains` is a literal Unicode-casefolded match against the returned
title. A `series_ticker` filter remains request evidence when rows omit their
series; it does not fabricate `market.ref.series_ticker`. An actual returned
series contradiction is excluded and diagnosed.

`await kalshi.get_market(ticker=..., source="historical")` reads only the
selected dataset and never falls back; `source="live"` is the default. IDs are encoded as single URL path
segments. A 404 reports its exact lookup scope; live absence explicitly says the
historical archive was not checked. Returned binary markets map to YES then NO
instrument references. Missing or recognized unsupported market types retain
metadata with unknown mapping and no book capability. Contradictory type
evidence produces inconsistent mapping, while malformed or contradictory native
ticker identity raises `InvalidDataError`.

`await kalshi.get_book(instrument, depth=..., deadline_s=...)` first reads live
market metadata under the same operation deadline to qualify binary-book
support, even when the caller supplies a series hint. It then fetches the full
current `orderbook_fp` once without a native depth cap. Only the qualified
`yes_dollars` and `no_dollars` probability/contract ladders are accepted by this
workflow; legacy integer-cent or ambiguous aliases raise `InvalidDataError`.
Native `orderbook` and `normalized_orderbook` behavior remains available.

YES bids come directly from the YES ladder and YES asks complement the NO ladder
with NO quantities. NO bids come directly from the NO ladder and NO asks
complement the YES ladder with YES quantities. Projection and sorting happen
before optional depth is applied independently to each output side. Prices are
finite probabilities, quantities are finite nonnegative contracts, and the
existing `kalshi_quote_normalization.v2` complement and provenance policy is
retained. Duplicate prices retain the last positive quantity under the existing
native normalizer behavior. Empty or unusable sides remain an inspectable snapshot with missing
provenance and quality flags. `BookSnapshot.observations` retains both the
capability and book requests; `observation` remains the final book request.

[`scripts/kalshi_book_example.py`](../scripts/kalshi_book_example.py) is a
runnable offline discovery-to-book example using injected synthetic HTTP
responses and the actual public methods.

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
`limit` from 1 through 100. These native parameters are passed to Gamma as
implemented. They remain separate from the supported `PolymarketFilter`
workflow contract.

`AsyncClobClient` retains `book(token_id)`, `books(token_ids)`,
`clob_market_info(condition_id)`, `price(token_id, side)`,
`midpoint(token_id)`, `fee_rate(token_id)`, `prices_history(...)`,
`batch_prices_history(...)`, and `event_prices_history(...)`. A `book` result is
one token snapshot represented by `pmkt.models.OrderBook`; `books` preserves the
server response as a list of token snapshots. `prices_history` returns one
`PriceHistory` series whose points contain Unix seconds and a sampled price.
These are native endpoint results. The supported typed current snapshot is the
separate `get_book` method described above; historical observations remain a
later workflow.

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
exports `CatalogError`, `OptionalDependencyError`, `UnsupportedCapabilityError`,
`ResultLimitExceededError`, `OperationTimeoutError`, `InvalidDataError`,
`MarketNotFoundError`, and the compatible `ReadAuthenticationRequiredError`
reexport.

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
