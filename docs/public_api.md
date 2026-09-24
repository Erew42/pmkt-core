# Public Python API

This inventory records the import surface shipped by `pmkt` 0.2.x. It is a
supported surface, not a preview: names appear here only when they are
importable from the current package. The machine-readable facade inventory is
[`tests/fixtures/public_api_inventory.json`](../tests/fixtures/public_api_inventory.json),
and CI checks every listed lazy and eager export.

## Interface tiers

- **Supported workflow** provides normalized results with explicit identity,
  coverage, provenance, and error semantics.
- **Native** provides venue endpoint access and canonical data contracts under
  their documented semantics.
- **Advanced** provides supported low-level utilities with domain-specific
  contracts; importability does not imply normalized-workflow guarantees.

See [Migrating to 0.2](migration_0_2.md) for the deliberate breaking changes.

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

Qualified catalog environments use DuckDB 1.5.5 on Windows and Linux with
Python 3.10 through 3.12, and either PyArrow 14.0.0 with NumPy 1.26.4 or
PyArrow 25.0.1 with NumPy 2.2.6.

New publishers declare each artifact's canonical `schema`. Older descriptors
with no `schema` key are accepted using the expected schema for that artifact
role, provided every Parquet file has exactly the canonical column names, with
no duplicates. Both validation modes check columns. A present but wrong schema,
including explicit null, is rejected. Pointer and manifest descriptors must
still agree; inference never rewrites their hashed bytes. The validation report
records each `legacy_schema_inferred_from_columns:<artifact>` check and skipped
`artifact_schema_declaration:<artifact>`. Full validation still checks canonical
values and content hashes; metadata mode does not establish value validity.
The legacy `parquet_file` descriptor is accepted for an actual single file
without a schema declaration. If that descriptor omits `parquet_file_count`,
the reader infers one and records the missing declaration in its report.
Partitioned datasets and current descriptors still require explicit file counts.

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
Run the sequence from the declared publisher `path_base`. Discovery and refresh
access live public APIs; promotion operates on local evidence. The synthetic
fixture is an offline example and does not exercise live acquisition.

## Shared references and request runtime

`pmkt.records` exports frozen Polymarket and Kalshi market/instrument
references plus `MarketRef` and `InstrumentRef` union aliases. Native lookup
identity determines equality and hashing; optional condition, series, parent,
and outcome-index enrichment does not. Constructors reject empty identifiers,
wrong parent-reference classes, non-YES/NO Kalshi sides, booleans and negative
outcome indexes before any venue request. The venue facades reexport their own
reference classes.

`PmktConfig(...)` constructs validated endpoint values using explicit arguments
and defaults only. `PmktConfig.from_env(...)` explicitly loads the process
environment and dotenv sources. REST endpoint precedence is explicit `base_url`,
then supplied `config`, then deterministic defaults. Clients never load settings
implicitly. Applications and CLI entrypoints load configuration and pass it down.

Import `RequestPolicy` and `OperationExpiry` from `pmkt.runtime`.
`RequestPolicy(max_attempts=N)` means N total attempts, including the first;
positive integers are required. The old retry-name alias is removed. CLOB retries
read-only POSTs only for `/books` and `/batch-prices-history`; the Kalshi
pre-authentication GET-only boundary remains unchanged. REST `timeout_s` must
be finite and positive.

The private HTTP runtime accepts one monotonic expiry passed explicitly through
each nested call. It covers limiter acquisition, retry waits, transport, and
JSON parsing checkpoints, and caps each dispatched request timeout to the
remaining operation budget. The state is per call, so future workflow methods
can safely borrow one client with different concurrent expiries. Timeout and
caller cancellation drain owned work before returning, and the borrowed client
remains reusable. `OperationTimeoutError` reports expiry without converting
unrelated worker errors. Native request methods accept optional keyword-only `expiry`; omission retains
per-request timeouts without an operation-wide deadline.

`RequestObservation` stores sanitized operation-local request provenance.
Only explicit endpoint templates and adapter-allowlisted effective parameters
are accepted; headers, raw bodies, URL credentials, queries, and raw dynamic
paths are not retained. Known secure default Gamma, CLOB, and Kalshi endpoints
receive conservative production/demo scope. Injected transports, custom paths,
ports, insecure endpoints, and unrecognized redirect targets remain unknown.

## Polymarket discovery, current books, and sampled history

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
from pmkt.records import BookSnapshot, DiscoveryResult, PriceHistoryResult
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

Condition-ID matching is case-insensitive for discovery filters and CLOB book
and history identity checks. Requests, references and response evidence retain
their original spelling; market IDs and token IDs remain exact identifiers.

Fetch a known market by Gamma market ID with
`await gamma.get_market(market=PolymarketMarketRef(...), deadline_s=...)`. The ID is encoded as
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
a millisecond timestamp; an absent timestamp leaves the field empty, and a
malformed one raises `InvalidDataError`.

The CLOB response `market` field is a condition ID. It is compared only with a
supplied condition-ID enrichment and is never interpreted as the Gamma market
ID. A supplied token or condition identity contradiction raises
`InvalidDataError`; absent response identity remains unverified in the request
observation. A CLOB 404 raises `MarketNotFoundError` scoped to the current token
book. A book is a current REST snapshot from one request, not a historical or
atomic cross-instrument view.

Fetch sampled CLOB prices for one selected outcome token with an explicit UTC
window and sampling request:

```python
from datetime import datetime, timezone

history: PriceHistoryResult = await clob.get_price_history(
    instrument,
    start=datetime(2026, 1, 1, tzinfo=timezone.utc),
    end=datetime(2026, 1, 2, tzinfo=timezone.utc),
    sampling_minutes=60,
    max_points=10_000,
    deadline_s=10.0,
)
```

Both bounds must be timezone-aware and are converted to UTC before ordering,
including ambiguous daylight-saving folds. `sampling_minutes` maps directly to
the native `fidelity` parameter. The adapter sends one explicit-window request;
the upstream documentation states no per-request maximum span, so this method
does not claim or impose a vendor chunk size. Integer request markers are
widened only enough for the endpoint's strict `startTs` and `endTs`
comparisons, then points are selected locally with
`start <= timestamp < end`.

Returned `SampledPricePoint` values are sorted and immutable. Equal points at
one token/timestamp key collapse. The default `invalid_rows="raise"` fails on
any malformed row or conflicting price. `invalid_rows="report"` reports
malformed rows and removes every occurrence of a conflicting key, including
equal observations seen before or after the conflict. The output cap is checked
after reconciliation and containment; overflow raises
`ResultLimitExceededError` and never returns a shortened success.

`HistoryCoverage` partitions every source row into accepted, rejected,
nonconflicting duplicate, or outside-window counts. `conflicting_rows` is a
subset of rejected rows. Reconciliation precedes containment, so strict mode
also detects a conflicting key in the widened query margin. In report mode all
occurrences of that key are rejected; every occurrence of a nonconflicting key
outside the requested window is counted as outside-window. This distinguishes
a source-empty response from an all-rejected response. Coverage retains the
requested bounds, transmitted query markers, dataset, and observed returned
extent; the owning result retains request observations under `provenance`. A
successful request sets `requests_complete=True`, while
`source_completeness` remains `"unknown"`: one HTTP response does not prove the
venue supplied every possible sample.

`price_basis="venue_defined"` reflects the qualified evidence. Fidelity does
not promise a regular grid, and these rows are not represented as trades,
quotes, sizes, volume, depth, or executable prices. `to_arrow()` and
`to_pandas()` load optional dependencies only when called. Both preserve typed
UTC timestamp columns for empty results and attach result metadata to the
materialized table or frame; the full observations, coverage, issues, and
defensive native payload remain on the owning result.

[`scripts/polymarket_history_example.py`](../scripts/polymarket_history_example.py)
is a runnable offline example using an injected synthetic response and the
installed public client method.

All four workflow methods require a finite positive deadline and fail before
I/O for `None`, booleans, nonpositive values, or nonfinite values. The deadline
covers transport, decoding, and normalization. Expiry raises
`OperationTimeoutError` and returns no partial result; the borrowed client
remains reusable. Malformed or contradictory upstream workflow data raises
`InvalidDataError`.

## Polymarket participants and wallet history

`AsyncPolymarketDataClient.market_participants(condition_id)` reads Data API v2
`/positions` for `OPEN` and `CLOSED` positions anchored on one condition ID. It
groups the returned proxy wallets and keeps current and past position rows
separate. `OPEN` is the venue's held-position superset, including unredeemed
resolved positions; `CLOSED` means exited positions. Both requests use a zero
token threshold rather than the API's default 0.1-share floor. The
`OPEN` request includes archived markets. The Data API rejects
`include_archived` for `CLOSED`, so that request omits it. The result exposes
the page count and next cursor for each status. `complete` means both requested
walks reached a terminal cursor, not that the two live reads formed an atomic
point-in-time snapshot. The result's
UTC start and completion times bound the local observation interval.

`AsyncPolymarketDataClient.wallet_history(wallet)` reads that wallet's
`start=1` trade feed plus its `OPEN` and `CLOSED` positions. The v2 trade
endpoint defaults to a three-year window when `start` is omitted; it ignores
the `full_history` query parameter. The request sends `taker_only=false`:
the API default returns only the wallet's
taker fills, which omits every maker fill. Rows carry no maker or taker role,
and `side` is the wallet's own side. It retains
trade sizes and prices as `Decimal`, with source timestamps in epoch seconds.
These are public proxy-wallet observations, not verified human identities or
authenticated fills. The result's three next cursors and `complete` property
make page-cap truncation visible. `positions_page` and `trades_page` accept a
cursor for callers that need to resume individual feeds. Each workflow shares
one finite deadline across its pages and uses at most 20 pages per feed by
default; callers can raise `max_pages_per_status` or `max_pages_per_feed`.

```python
from pmkt.exchanges.polymarket import AsyncPolymarketDataClient

async with AsyncPolymarketDataClient() as data:
    market = await data.market_participants(condition_id)
    history = await data.wallet_history(market.participants[0].wallet)
```

The position API reports the venue's current lifecycle classification. It does
not provide a dated ledger of every past ownership interval. A wallet that
changed exposure within an outcome can have one aggregate position row, so
position rows must not be interpreted as individual fills. The trade feed is
the separate execution history. This feature adds no participant fields to
the existing public trade recording or canonical `trade.v1` schema.

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

`await kalshi.get_market(market=KalshiMarketRef(...), source="historical")` reads only the
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
provenance and quality flags. `BookSnapshot.provenance.observations` retains both the
capability and book requests. Request-linked raw responses live alongside them
in `provenance.raw_responses`; no duplicate singular observation is stored.

[`scripts/kalshi_book_example.py`](../scripts/kalshi_book_example.py) is a
runnable offline discovery-to-book example using injected synthetic HTTP
responses and the actual public methods.

Fetch normalized candles for one market with an explicit aware window and
fixed native period:

```python
from datetime import datetime, timezone
from pmkt.records import CandleHistoryResult

history: CandleHistoryResult = await kalshi.get_candles(
    KalshiMarketRef("KXMARKET"),
    start=datetime(2026, 1, 1, tzinfo=timezone.utc),
    end=datetime(2026, 1, 2, tzinfo=timezone.utc),
    period_minutes=60,
    source="auto",
    max_candles=10_000,
    deadline_s=60.0,
)
```

`period_minutes` is exactly `1`, `60`, or `1440`. Both bounds must be aware;
they are normalized to UTC before ordering. Returned candles are fully
contained in the original window and complete against one UTC clock value
frozen at operation start. Thus 10:30â€“12:00 includes the 11:00â€“12:00 hourly
bar only. Inclusive native end-label requests may overlap at internal adapter
boundaries; all valid keys are reconciled before original-window containment,
completion filtering, and the output cap. Chunk size is 5,000 elapsed periods
(`end_ts - start_ts`), not candle count: a max-sized chunk may include 5,001
inclusive end-labels. Kalshi publishes a 10,000-candlestick / 100-ticker cap
only on the batch endpoint; the single-market live and historical endpoints do
not document a numeric cap. The adapter bound is a conservative client choice,
not a claimed upstream maximum.

Live and historical payloads have separate contracts. Live fields use
`*_dollars`, `volume_fp`, and `open_interest_fp`, and explicitly send
`include_latest_before_start=false`. Historical fields use bare probability
strings plus `volume` and `open_interest`, and receive no invented flag. The
result keeps traded-price OHLC separate from YES bid and ask OHLC, with native
mean, previous, volume, open interest, dataset, end label, and defensive native
payload. Live traded-price OHLC keys may be absent, including a price object
containing only `previous_dollars`. Missing fields become null while supplied
mean and previous values remain intact. All-null traded OHLC carries
`no_traded_price_ohlc`; partly absent OHLC carries `partial_traded_price_ohlc`.
Bid and ask OHLC layouts remain strict. Historical null trade OHLC with valid
quotes is also retained. Values are never scaled by magnitude, filled from
previous prices, zeroed, or complemented into NO trades.

The versioned `kalshi_market_candles.v1` interpretation treats a 1440-minute
bar as a fixed 86,400-second interval whose inferred start must be midnight in
`America/New_York`; the native end label is preserved. Around spring DST the
end can be 01:00 local, and around fall DST it can be 23:00 local, so adjacent
nominal intervals may overlap or have a gap. This rule is supported by bounded
primary API observations, including both DST transitions, and is an analytical
nominal-period convention. It does not assert a vendor timezone, calendar-day
grid, source completeness, finality, or clock-skew bound. The base install
includes `tzdata>=2026.3` so this rule is available on Windows without pandas.

`source="live"` and `source="historical"` never switch. Explicit historical
reads need no live metadata, event, or series lookup. Auto routing retains the
historical `market_settled_ts` cutoff and a normalized routing market: a market
settled strictly before the cutoff uses the archive, while equality stays live.
Routing timestamps require explicit UTC offsets and at most six fractional
digits. Finer precision is rejected rather than silently truncated at a
live/archive routing boundary; this differs from catalog timestamp ingestion.
Live series identity comes only from verified market or event evidence; ticker
splitting and unverified caller hints are rejected. A live metadata 404 may
trigger one archive existence lookup. A selected candle endpoint 404 may
trigger one bounded alternate dataset attempt under the same deadline; an
empty 200, authentication error, timeout, or server error never does.

`HistoryCoverage` partitions every raw candle occurrence into accepted,
rejected, nonconflicting duplicate, outside-window, running, or identified
synthetic counts. Conflicts reject every occurrence of the key in report mode,
including occurrences outside the requested window; strict mode raises.
Malformed individual scalar rows follow `invalid_rows`, while malformed
envelopes, identity contradictions, and unsupported live/archive component
layouts always raise. `requests_complete=True` still leaves
`source_completeness="unknown"`. `to_arrow()` and `to_pandas()` load optional
dependencies lazily and preserve typed UTC columns for empty results.

[`scripts/kalshi_candles_example.py`](../scripts/kalshi_candles_example.py) is
a runnable offline example using the public method with an injected historical
candle response.

## Retained native venue clients

The public async clients are `pmkt.exchanges.polymarket.AsyncGammaClient`,
`pmkt.exchanges.polymarket.AsyncClobClient`, and
`pmkt.exchanges.kalshi.AsyncKalshiClient`. The former aliases `GammaClient`,
`ClobClient`, and `KalshiClient` have been removed. Use
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
separate `get_book` method described above. The supported normalized sampled
history is the separate `get_price_history` method; native methods keep their
existing signatures, interval suppression, models, and validation.

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
existing policy-neutral normalized dictionary. Native candlestick and trade
methods retain upstream units, layouts, signatures, defaults, and explicit
source behavior. The separate `get_candles` workflow supplies the normalized
candle contract described above; no cross-venue candle facade is promised.
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

`pmkt.resolution` exports `PolymarketResolutionResolver`,
`KalshiResolutionResolver`, `PolygonCtfClient`, `EvmRpcError`,
`ResolutionRecord`, `Payout`, `SourceObservation`, and their state, result-type,
confidence, and resolver-version constants. The resolvers accept their matching
`PolymarketMarketRef` or `KalshiMarketRef`, including through the
`market_key=` keyword, with an optional snapshot. Strings are rejected. A keyword-only
`deadline_s` bounds the complete single-market operation. Its default `None`
leaves the operation unbounded while each underlying client keeps its
own request timeout.

Resolvers borrow every supplied client and never create or close one. The
caller constructs `PolygonCtfClient(rpc_url=...)` explicitly and owns its
lifetime. No RPC endpoint is read from package configuration or the environment.
Gamma and CLOB evidence can describe lifecycle, labels, prices, and tokens, but
only validated retained Polygon CTF payout evidence can make a Polymarket record
canonical. Snapshot-only resolution is fully offline: it retains useful state
and payout hints at its existing metadata-only ceiling. The offline ownership
and typed-reference flow is executable in
[`scripts/resolution_example.py`](../scripts/resolution_example.py).

Both resolvers also expose an ordered typed batch:

```python
records = await resolver.resolve_many(
    markets,
    concurrency=8,
    deadline_s=120.0,
)
```

`markets` must be a sequence of the resolver's matching market references.
The full sequence and options are validated before requests. Empty input returns
an empty list. Results preserve input order, cardinality, and duplicates; each
duplicate is resolved independently and retains its own observations. A fixed
number of workers bounds active work, and one deadline covers worker queueing,
all nested source requests, and normalization. Expected per-market evidence
failures still occupy their result slots. Expiry, caller cancellation,
`ReadAuthenticationRequiredError`, and unexpected implementation errors raise
only after the resolver has cancelled and drained its owned workers. Borrowed
clients remain open and reusable. A whole-batch failure returns no partial list,
even if some markets completed before the final deadline check. For durable
incremental progress, callers can resolve and save smaller batches; their
orchestration must enforce any deadline spanning those batches. The offline
ordered and duplicate-preserving flow is executable in
[`scripts/resolution_batch_example.py`](../scripts/resolution_batch_example.py).

To materialize results, including an empty batch, with stable canonical
columns:

```python
import pandas as pd
from pmkt.data import MARKET_RESOLUTION_COLUMNS

frame = pd.DataFrame(
    [record.to_row() for record in records],
    columns=MARKET_RESOLUTION_COLUMNS,
)
```

Matching typed references protect identity before interpretation. Conflicting
snapshot identity or enrichment fails before I/O; conflicting returned venue
identity becomes a source error observation, so another valid source can still
support the result. Expected HTTP, transport, malformed JSON, and source-evidence
failures become sanitized observations. `ReadAuthenticationRequiredError`,
operation expiry, caller cancellation, and unexpected implementation errors
raise. An HTTP 401 or 403 alone remains per-market evidence and is not treated as
a global credential diagnosis. Stored diagnostics include safe source, error
class, and status summaries rather than RPC URLs, request secrets, or raw remote
error bodies.

New records use `market_resolution_resolver.v3`. The default terminal-label
policy accepts eligible v2 and v3 canonical finals. Separately, the resolution
cache preserves eligible retained v2 canonical finals and v2 conflicts without
relabeling them; conflicts remain ineligible for terminal labels. A refresh with
weaker evidence carries forward the original payout and resolver version, while
contradictory canonical finals retain conflict handling. The existing unsafe v1
cache migration behavior is unchanged.

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
inventory. It currently includes `pmkt.data.market_catalog`, `pmkt.streaming`,
`pmkt.text`, `VenueAdapter`, the
Polymarket Data API and Subgraph clients, venue WebSocket clients and decoding
helpers, capture stream entry points, and Kalshi normalization/dataframe
helpers. These imports remain available. They have not been qualified as the
new supported REST or capture workflows. The supported pinned reader is the
separate `pmkt.catalog` facade.

`MarketCatalogService` remains the mutable maintenance service and the CLI
`query` command exposes an unmanaged DuckDB connection. Use `pmkt.catalog` for
the pinned reader and bounded managed query result. WebSocket clients retain their transport and book-state behavior. The two
`stream_*_order_book_data` recording entrypoints now implement
[the simplified recording contract](stream_recording_contract.md). Historical
storage-profile arguments are removed; use `mode`, `depth_check_interval_s`,
`depth_on_best_price_change` and `raw_messages`. Both modes retain public trade
observations. `pmkt.streaming.export_recording` exports committed SQLite rows
from stopped recordings, including after an interrupted process.

## Optional dependencies

The base install provides REST clients, configuration, models, resolution
records, the catalog reference/result types, and the CLI. Importing the catalog
facade does not import pandas, PyArrow, or DuckDB. Install `pmkt[data]` to open,
validate, and query catalogs with pandas, PyArrow, and DuckDB.
`pmkt[storage]` currently installs the same storage stack.
`pmkt[streaming]` adds pandas, PyArrow, and `websockets`. Conversions and modules
that require an extra may fail at call or import time when that extra is absent;
the base package does not implicitly install those dependencies.
