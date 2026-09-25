# Polymarket participant and holder history: architecture contract

Status: proposed contract for the read-side milestones after participant discovery
PR #9. The pilot below decides whether a chain indexer is needed. This document
proposes meanings and decision gates; it does not add an endpoint, persisted
schema, or indexer.

The bounded API-only pilot and chain capability observations are recorded in
[`polymarket_participant_api_pilot_2026-09-24.md`](polymarket_participant_api_pilot_2026-09-24.md).
They support current holder observations and a source-limited wallet timeline,
but not exact market-wide dated balances from the API alone. A production chain
indexer remains subject to the archival-source and full-range extraction gate.

## Scope and existing behavior

`pmkt-core` owns public venue reads, evidence capture, storage, and balance
reconstruction. Wallet selection, monitoring cadence, alerts, and trading
interpretation belong to downstream consumers. A Polymarket wallet in this
work is an **address**, not an inferred person or a transaction sender.

The concrete consumer questions are: for a market, which addresses currently
or previously held each outcome and what were their balances on a requested
date; and for a chosen address, what trades and other events explain its
current positions? The pilot first tests whether the Data API can answer those
questions at a declared level of coverage. Build a chain indexer only if the
required answer cannot be supported by the API and the chain source is viable.

PR #9 reads `OPEN` and `CLOSED` positions for a market and reads a wallet's
trades and positions. Its `complete` property means that the requested page
walks exhausted their cursors within the caller's limits. Preserve that
behavior and the cost of these methods. New activity or chain reads must be
opt-in or deliberately versioned. The existing `PolymarketInstrumentRef`
continues to identify a venue instrument by token ID; the ledger identity
below is narrower and does not change that public reference.

The user-facing evidence classes are distinct:

| Evidence | Permitted claim |
| --- | --- |
| Wallet-attributed trade or activity | The source reported that address in an event. A trade alone does not establish its balance. |
| API position or holder row | The source reported a balance during a particular request or scan interval. |
| Replayed chain transfers | The address held a calculated token balance at a covered canonical block. |

Participant discovery may union these addresses, but each result retains its
evidence type and source scope. If a chain ledger is built, API observations
stay alongside it; reconciliation reports differences and never silently
changes its balances.

## Identity and grain

- The chain asset key is `(chain_id, token_contract, token_id)`. A holding adds
  `holder_address`. Normalize addresses for comparison while retaining the
  source address and payload. Contract custody and proxy-wallet addresses
  remain addresses; do not replace either with `tx.from` or a guessed owner.
  An API `proxy_wallet` and a chain holder are compared only when their
  relationship is established, not assumed from a matching market.
- Map the asset to venue, position class, market/condition, outcome, and, for
  combinatorial positions, its own condition and underlying legs. Retain the
  source and effective observation or block range of each mapping. An
  ambiguous or missing mapping is reported rather than guessed.
- Standard and negative-risk outcome tokens share the Conditional Tokens
  Framework (CTF); a negative-risk conversion changes several token balances
  and requires event-level explanation. Combinatorial tokens live on the
  separate Positions Framework. A combo is not counted as a direct holding
  of each underlying leg; compression may change its token identity.
- Keep chain quantities as lossless integers in base units. A documented,
  verified scale produces shares for display. Do not use floating-point
  amounts for transfer replay.

## Time and coverage

An API scan needs request start, response receipt, exact query parameters,
incoming and outgoing cursor, and first/final response times. PR #9's
provenance update retains request metadata in memory but deliberately
leaves `raw_responses` empty; raw response retention belongs to a separate
durable capture path. Rows on different pages may reflect different source
states:
exhausting a holder cursor does **not** create an atomic snapshot. The same
wallet/token may appear more than once and each observation remains evidence.
`last_event_at` on a position is last reported activity, not the start of
ownership.

Keep these coverage statements separate:

1. **Pagination:** exhausted, capped, interrupted, or failed for this query.
2. **Source scope:** endpoint, filters, history window, excluded markets or
   event types, and known amount floors. Cursor exhaustion does not certify
   lifetime source completeness.
3. **Observation consistency:** whether the source provides an independently
   established point-in-time snapshot. Otherwise consistency is unknown.
4. **Chain range:** first block with a known opening balance, last covered
   canonical block, and any gaps by contract/token.
5. **Reconciliation:** which balances were compared, at which block/time and
   grain, and which passed or differed. Passing samples is not proof that
   every historical transfer was retrieved.

API absence means **not observed**, even after an exhausted scan, unless that
source has independently established exhaustive snapshot semantics. Chain
absence can mean zero only for an asset and address with a known opening state
and uninterrupted transfer coverage through the queried block. Never convert
unknown coverage to zero.

For an observed-at query at UTC time `T`, choose the scan with the latest
completion time at or before `T`, and return its full interval plus the receipt
time of each contributing page. The answer is "reported during this interval,"
not "held at exactly T." Differences between scans bound changes in reported
state only; they do not date the underlying transfer.

If a chain-backed historical query is needed, resolve time `T` to the highest
applicable canonical block whose timestamp is at or before `T`. Balances mean
**end-of-block** state. A block beyond coverage or inside a gap is `uncovered`,
not the last known balance; any stale convenience answer must label its actual
block. Positive-balance intervals start at the first positive end-of-block
state and end before the first subsequent nonpositive one. Exit and re-entry
yield separate intervals. Intra-transaction changes remain event evidence,
not sustained holdings.

## Durable API observations

If durable capture is built, its unit is an immutable page attempt, not a
final Parquet file. As capture progresses, persist the raw response, request
identity, cursor chain, receipt times, and a content hash. Repeating an
identical attempt is idempotent; a later response to the same cursor with
changed content remains distinct evidence. The scan manifest identifies the
accepted page chain and whether it ended exhausted, capped, interrupted, or
failed. A delayed resume extends the recorded observation interval rather than
making it appear
instantaneous.

Publish a manifest only after all files it names are durable, using an atomic
publication pattern like `contract_evidence_manifest.py`. An interrupted scan
retains recoverable page evidence but is not presented as an exhausted scan.
For holdings capture, compare market-scoped `OPEN` and `CLOSED` positions with
`/v2/holders?include_pnl=true`. The bounded pilot found positive residual
balances in `CLOSED` rows that were absent from `OPEN` but present in gross
holders and latest CTF balances. Use gross holders for observed current
per-token balances, and treat position status as a lifecycle classification.
The default holder net balance remains a separate view. These API reads do
not form a simultaneous snapshot.

## Chain evidence if the pilot requires it

Retain `TransferSingle` and each item of `TransferBatch`, including mint and
burn, with chain ID, contract, block number/hash/timestamp, transaction hash,
transaction index, log index, batch item index, from/to/operator, token ID,
and raw quantity. The block hash makes orphaned evidence identifiable. Replay
uses canonical block, transaction, log, and batch-item order. Opening balance
is zero only when indexing begins before that token's creation and all
intervening transfers are covered; a later start requires an independently
established opening state for every address claimed.

If a production indexer is justified, publish source events, derived balances,
and coverage consistently, and handle chain reorganizations before claiming
canonical history. Direct historical `balanceOf` checks require an RPC
provider and an `eth_call` block parameter; the current
`PolygonCtfClient.eth_call` uses `latest` only.

## Trading-history promise

Expose wallet trades, activity, and current positions as separate feeds with
their query scope. Activity can duplicate a trade; transaction hash alone is
not a unique fill key. The current Data API specification describes a fixed
three-year window for condition-scoped trades, wallet-anchored `start=1`
history for both trades and activity, a trade-size floor, and position
visibility that excludes inactive markets even when archived positions are
requested. Trade reads also default to `taker_only=true`, which omits maker
fills; market discovery and wallet reconstruction must request
`taker_only=false`. Record those limits, the exact query sent, and any future
source changes before a completeness claim. The core activity read explicitly
uses `exclude_deposits_withdrawals=true`: `start=1` extends the time range but
does not include those cash movements. The default activity type set also
omits opt-in `TIP` rows.

If the available wallet feed omits historical executions or prices, either
backfill them before promising full trading history or publish an explicitly
source-limited wallet trade history. Transfer replay alone does not
reconstruct every trade price.

## API-only reconstruction pilot and decision

Before implementing chain ingestion, try the smallest answer to the consumer
questions above. For selected markets, discover candidate addresses from
market positions, holders, and maker-inclusive trades. For each candidate,
replay wallet trades and lifecycle activity (including splits, merges,
redeems, and conversions) from a documented opening state. Match overlapping
trade and activity records without transaction-hash-only deduplication. Keep
feed-specific timestamps and unknown activity types rather than inventing
balance changes. This can be a bounded research script using public reads; it
does not require a persisted schema or a production indexer.

Test active, resolved, negative-risk, older, and inactive markets; maker-only
wallets; repeated entry and exit; small fills near the source floor; and
addresses that received tokens without a reported trade. Compare balances at
several intermediate dates, not just final `total_size`, against independent
historical `balanceOf` checks or bounded chain-transfer evidence. Check
whether market-wide discovery found transfer-only recipients. A final match
for three wallets is encouraging but cannot establish complete history.

The pilot records three answers separately: (1) how much of the requested
market/wallet history the API can retrieve, (2) which intermediate balances
reconcile, and (3) whether the declared consumer question is answered at its
required precision. If API evidence suffices for that question, stop before a
chain indexer and label the result as API-derived with its measured limits. If
important holders or intervals remain missing, decide whether a narrower
answer is acceptable. Only an unmet concrete requirement proceeds to a bounded
chain-extraction and cost pilot; only a feasible pilot justifies production
ingestion, replay, and reorganization recovery. If neither path covers a
date, return `uncovered`. An API-derived result cannot be labeled exact
all-holder history without independent evidence that its sources cover every
relevant balance-changing event and address.

## Acceptance cases for the relevant path

| Case | Required result |
| --- | --- |
| Holder ranking changes between pages; a wallet repeats or disappears | Preserve both page observations and times. Exhausted pagination does not become a consistent snapshot; absence is not zero. |
| API-only replay matches a current position but misses an intermediate balance | Record the gap and withhold an exact historical-holding claim. |
| Maker-only wallet or transfer-only recipient | Find the maker with maker-inclusive trades; test independently whether the API can discover and account for the recipient. |
| Capture stops after durable pages and resumes much later | Retain prior evidence, resume the exact query/cursor, avoid duplicate identical attempts, and expose the long scan interval. |
| Manifest publication is interrupted | Readers see the prior published state, not a manifest pointing to missing files. |
| Requested date falls after the covered head or inside a gap | Return uncovered; do not substitute the last covered balance unless stale mode was requested. |
| Wallet receives, exits, and re-enters in one covered history | Reconstruct intermediate balances and two positive-balance intervals. A final `balanceOf` match alone is insufficient. |
| Transfer-only recipient or mint-to-burn lifecycle | Discover the address and reconstruct its full interval without relying on API trades. |
| Canonical block is replaced | Orphaned logs no longer drive the published generation; replay and coverage agree on the replacement chain. |
| Several fills share one transaction hash | Preserve distinct fill evidence; no hash-only deduplication. |
| Combo is compressed after partial resolution | Preserve its position class, token mappings, and history without reclassifying it as direct holdings of underlying legs. |

If the API-only pilot fails its declared question and chain extraction is
considered, the chain pilot must establish an opening boundary, compare event
identities over bounded ranges, check intermediate and final balances, and
measure both full-contract scan work and relevant-token output. These are
separate claims: event-range coverage, reconstructed balances, and sampled
reconciliation.

## Delivery order and release gates

1. Add typed `/v2/holders` reads, then condition-scoped trades and wallet
   activity as separate, opt-in changes; preserve PR #9 defaults. Sync the
   upstream OpenAPI contract before each new endpoint.
2. Run the API-only reconstruction pilot against the stated consumer questions.
   Capture durable gross-balance observations if dated *observations* are
   useful; they need not wait for wallet activity or a chain decision.
3. Stop at the API path when its measured coverage answers the declared
   question. Otherwise, test a bounded chain extraction for the specific gap.
   Choose an indexed provider or direct RPC using measured completeness,
   throughput, and cost. ERC-1155 token IDs are not indexed event topics, so a
   direct market-wide token scan may require broad contract-log retrieval.
4. Only if that second pilot establishes need and feasibility, build transfer
   ingestion, replay/publication, and reorganization recovery as separate
   reviewable changes. Add strict dated queries after coverage is demonstrated.
5. Extend to negative-risk and combinatorial assets only as required by the
   declared consumer scope. Publish coverage by position class, source feed,
   asset, and block range; CTF-only coverage cannot include combos.

This contract remains proposed until the pilot resolves API coverage,
capture-source choice, chain need and feasibility, token scale, and actual
historical trade coverage.

## Source references

- [Polymarket market analytics: trades and holders](https://docs.polymarket.com/market-data/public-analytics)
- [Polymarket wallet activity and positions](https://docs.polymarket.com/trading/wallet-activity)
- [Polymarket combinatorial positions](https://docs.polymarket.com/trading/positions/combinatorial)
- [Polymarket Data API v2 specification](https://data-api.polymarket.com/v2/openapi.json)
- [ERC-1155 transfer event specification](https://eips.ethereum.org/EIPS/eip-1155)
