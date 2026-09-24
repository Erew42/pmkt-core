# Polymarket participant and holder history: architecture contract

Status: proposed contract for the read-side milestones after participant discovery
PR #9. This document fixes meanings and acceptance gates; it does not add an
endpoint, persisted schema, or indexer.

## Scope and existing behavior

`pmkt-core` owns public venue reads, evidence capture, storage, and balance
reconstruction. Wallet selection, monitoring cadence, alerts, and trading
interpretation belong to downstream consumers. A Polymarket wallet in this
work is an **address**, not an inferred person or a transaction sender.

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
evidence type and source scope. API observations stay alongside the chain
ledger; reconciliation reports differences and never silently changes the
ledger.

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

An API page records request start, response receipt, exact query parameters,
incoming and outgoing cursor, and raw response. A scan records its first
request and final response times. Rows on different pages may reflect
different source states: exhausting a holder cursor does **not** create an
atomic snapshot. The same wallet/token may appear more than once and each
observation remains evidence. `last_event_at` on a position is last reported
activity, not the start of ownership.

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

For an observed-at query at UTC time `T`, choose only a scan whose completion
time is at or before `T`, and return the full scan interval plus the receipt
time of each contributing page. The answer is "reported during this interval,"
not "held at exactly T." Differences between scans bound changes in reported
state only; they do not date the underlying transfer.

For a historical chain query at `T`, first resolve the highest applicable
canonical block whose timestamp is at or before `T`. Balances mean
**end-of-block** state. If that block is beyond coverage or inside a gap, return
`uncovered` with the reason. A prior covered block may be returned only by a
separately requested stale mode that labels its actual block and timestamp.
Positive-balance intervals use an inclusive first end-of-block state and an
exclusive first subsequent end-of-block state without that positive balance.
An exit followed by re-entry yields two intervals. Event history retains
intra-transaction and intra-block changes; intervals do not present those as
sustained holdings.

## Durable API observations

The capture unit is an immutable page attempt, not a final Parquet file. As
capture progresses, persist the raw response, request identity, cursor chain,
receipt times, and a content hash. Repeating an identical attempt is
idempotent; a later response to the same cursor with changed content remains
distinct evidence. The scan manifest identifies the accepted page chain and
whether it ended exhausted, capped, interrupted, or failed. A delayed resume
extends the recorded observation interval rather than making it appear
instantaneous.

Publish a manifest only after all files it names are durable, using an atomic
publication pattern like `contract_evidence_manifest.py`. An interrupted scan
retains recoverable page evidence but is not presented as an exhausted scan.
For holdings capture, compare gross per-token semantics from market-scoped
`OPEN` positions with `/v2/holders?include_pnl=true`; choose the source after
checking coverage and request cost. The default holder net balance remains a
separate view. Neither API mode implies a simultaneous snapshot.

## Chain evidence and ledger publication

Retain `TransferSingle` and each item of `TransferBatch`, including mint and
burn, with chain ID, contract, block number/hash/timestamp, transaction hash,
transaction index, log index, batch item index, from/to/operator, token ID,
and raw quantity. The block hash makes orphaned evidence identifiable. Replay
uses canonical block, transaction, log, and batch-item order. Opening balance
is zero only when indexing begins before that token's creation and all
intervening transfers are covered; a later start requires an independently
established opening state for every address claimed.

Keep source events, derived balances/intervals, and coverage metadata in one
published ledger generation. Checkpoint only durable event ranges; on restart,
recheck an overlap and replace derived state after a reorganization. A new
generation becomes visible to readers atomically. Direct historical
`balanceOf` checks require an RPC provider and an `eth_call` block parameter;
the current `PolygonCtfClient.eth_call` uses `latest` only.

## Trading-history promise

Expose wallet trades, activity, and current positions as separate feeds with
their query scope. Activity can duplicate a trade; transaction hash alone is
not a unique fill key. The current Data API specification describes a fixed
three-year window for condition-scoped trades, wallet-anchored `start=1`
history, a trade-size floor, and position visibility that excludes inactive
markets even when archived positions are requested. Record those limits, the
exact query sent, and any future source changes before a completeness claim.

The pilot tests whether the available wallet feed actually covers historical
executions and prices for representative wallets. If it does not, either
backfill the missing execution events before promising full trading history or
publish the narrower contract: complete reconstructed holdings for declared
chain assets and blocks, alongside source-limited wallet trade history.
Transfer replay alone does not reconstruct every trade price.

## Acceptance cases before implementation is called complete

| Case | Required result |
| --- | --- |
| Holder ranking changes between pages; a wallet repeats or disappears | Preserve both page observations and times. Exhausted pagination does not become a consistent snapshot; absence is not zero. |
| Capture stops after durable pages and resumes much later | Retain prior evidence, resume the exact query/cursor, avoid duplicate identical attempts, and expose the long scan interval. |
| Manifest publication is interrupted | Readers see the prior published state, not a manifest pointing to missing files. |
| Requested date falls after the covered head or inside a gap | Return uncovered; do not substitute the last covered balance unless stale mode was requested. |
| Wallet receives, exits, and re-enters in one covered history | Reconstruct intermediate balances and two positive-balance intervals. A final `balanceOf` match alone is insufficient. |
| Transfer-only recipient or mint-to-burn lifecycle | Discover the address and reconstruct its full interval without relying on API trades. |
| Canonical block is replaced | Orphaned logs no longer drive the published generation; replay and coverage agree on the replacement chain. |
| Several fills share one transaction hash | Preserve distinct fill evidence; no hash-only deduplication. |
| Combo is compressed after partial resolution | Preserve its position class, token mappings, and history without reclassifying it as direct holdings of underlying legs. |

The extraction pilot must establish an opening boundary, compare event
identities over bounded ranges, check intermediate and final balances, and
measure both full-contract scan work and relevant-token output. These are
separate claims: event-range coverage, reconstructed balances, and sampled
reconciliation.

## Delivery order and release gates

1. Add typed `/v2/holders` reads. Then add condition-scoped trades and wallet
   activity as separate, opt-in changes; preserve PR #9 defaults.
2. Capture durable gross-balance observations once the holder source and query
   contract are settled. It need not wait for wallet activity.
3. Run the one-market chain extraction and historical-trade pilot early. Choose
   an indexed provider or direct RPC using measured completeness, throughput,
   and cost. ERC-1155 token IDs are not indexed event topics, so a direct
   market-wide token scan may require broad contract-log retrieval.
4. Build transfer ingestion, replay/publication, and reorganization recovery
   as separate reviewable changes. Add strict dated queries after the
   coverage contract is demonstrated.
5. Extend to negative-risk semantics and combinatorial assets. Publish coverage
   by position class, source feed, asset, and block range. Do not label CTF-only
   coverage as complete for combinatorial positions.

The architecture contract is stable; source selection, capture-source choice,
token scale, and actual historical trade coverage are empirical pilot results,
not assumptions to encode in advance.

## Source references

- [Polymarket market analytics: trades and holders](https://docs.polymarket.com/market-data/public-analytics)
- [Polymarket wallet activity and positions](https://docs.polymarket.com/trading/wallet-activity)
- [Polymarket combinatorial positions](https://docs.polymarket.com/trading/positions/combinatorial)
- [Polymarket Data API v2 specification](https://data-api.polymarket.com/v2/openapi.json)
- [ERC-1155 transfer event specification](https://eips.ethereum.org/EIPS/eip-1155)
