# Polymarket participant API pilot and chain capability decision

Date: 2026-09-24 UTC. Status: bounded research observation, not a production
historical-balance contract. This applies the go/no-go gate in
`polymarket_participant_history_contract.md` to the consumer question: **who
held each outcome on a chosen date, including wallets no longer holding it?**

## Sample and method

The pilot read the official Data API v2 OpenAPI document, then used public
`/v2/positions` (`OPEN`, `CLOSED`), `/v2/holders?include_pnl=true`,
`/v2/trades?condition=...&taker_only=false`, and wallet
`/v2/activity?start=1&condition=...`. Wallet `/v2/trades?start=1` was also
walked up to five 1,000-row pages per sampled wallet. The market scans used
one 100-row page per feed. The wallets below were selected from the returned
market positions, holders, and trades. All source reads were live and
non-atomic; page caps and cursor exhaustion are reported separately.

| Case | Gamma market ID and condition | OPEN / CLOSED position page | Gross holder page | Condition trade page |
| --- | --- | --- | --- | --- |
| Active | `665374`, `0x5db999fad322cea2914535aae5517060c3f80ad6d8c0231cde2124a434d16846` | 100+ / 100+ | 200+ rows in two token groups | 100+ |
| Recently closed | `4897525`, `0x7590bb39104ef7ca80f6d42944f0f3c23464a36e0f26fd3254b042085d67fe49` | 30 / 100+ | 31, cursor exhausted | 100+ |
| Negative risk | `2063134`, `0x7d0aaf81bbd3fd73b6a1651cce08a452c0cbf9c0cbb4520ce0f981065b639d88` | 100+ / 100+ | 153, cursor exhausted | 100+ |
| Older closed, created 2020 | `12`, `0xe3b423dfad8c22ff75c9899c4e8176f628cf4ad4caa00481764d320e7415f7a9` | 25 / 7, cursors exhausted | 31, cursor exhausted | 0, cursor exhausted |

`+` means the page returned a continuation cursor, not an estimated total.
Gamma reports the older market as `active=true`, `closed=true`,
`archived=false`; it is **not** an inactive-market test. A bounded Gamma
search did not find a genuinely inactive candidate, so that contract case
remains unverified here. The official v2 specification still states that
inactive positions are excluded even with `include_archived`.

The official trade contract fixes condition queries to three years and floors
size at 0.01 shares. These source limits make the old market's empty condition
trade response expected; it does not prove no trading occurred. The activity
default omits opt-in `TIP` rows. Neither cursor exhaustion nor `start=1`
certifies all token transfers.

## API-only balance replay

For nine selected wallet/market pairs, the pilot walked condition-filtered
activity and used **activity TRADE rows as the only trade source** to avoid
double-counting overlapping `/v2/trades` rows. It added BUY and subtracted
SELL shares; SPLIT added and MERGE subtracted the reported size for each known
outcome token; REDEEM subtracted only when an outcome token was identified.
Unknown or blank-token actions were retained as unresolved. This diagnostic
assumes zero opening balance and is not an exact ledger. Intra-transaction
ordering and direct transfers are absent from activity rows.

Observed examples:

- Recently closed market wallet `0x04c2...943c4`: two TRADE activity rows
  sum to 219.739752 shares; the `OPEN` position reports 219.7397. The
  0.000052 difference is consistent with the position's four-decimal
  presentation. Both wallet trade and activity cursors exhausted, and each
  showed two market trades.
- Older market wallet `0x0fec...c9f7`: one SPLIT activity row reproduces
  five shares on each of two outcome tokens, matching its positions. Its
  wallet trade walk did **not** exhaust within five pages; a match on this
  pair does not establish full wallet history.
- Older market wallet `0x37ed...3e4`: wallet trade and condition activity
  cursors both exhausted with zero rows, while its current position is
  0.6881 shares and latest CTF `balanceOf` is 688189 raw units (0.688189
  shares). The API event replay cannot explain this holding. The cause may be
  a direct transfer or missing older API events; this sample does not decide
  which.
- Recently closed wallet `0x0539...c1f` has a REDEEM activity row whose
  `token_id` is empty. Without a per-outcome amount from that row, the API
  event alone cannot assign the redemption to a token. `MERGE` rows likewise
  carry an empty token ID. The parser now retains these valid rows.
- On the active market, wallet `0x0224...8292a`'s activity trade sum differs
  from its displayed position by 0.000081 shares. Its global wallet trade
  feed still had a cursor after five pages, so there is no full independent
  trade comparison for that wallet. On the negative-risk market, one wallet's
  activity replay differed by 91.62 shares from its reported position; its
  global wallet trade feed also remained capped. These are gaps to diagnose,
  not evidence that a particular API row is wrong.

The negative-risk position sample contained a source `avg_price` of 1.0089.
The prior client rejected it as if cost basis were a bounded trade price;
the read now preserves nonnegative finite source values.

No intermediate date was independently checked against historical on-chain
balances: the tested public RPC endpoint returned `historical state ... is not
available` for the sampled blocks. The API-only final matches therefore do
not prove accurate dated balances or zero opening states.

## Bounded chain capability check

The read-only probe used the CTF contract
`0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` on Polygon. At the
tested public RPC endpoint, `eth_chainId` returned `0x89`. Receipts and
blocks for two trades on the recently closed market returned block numbers,
hashes, and timestamps. `eth_getLogs` on those **two individual blocks**
returned 509 CTF logs; 374 were `TransferSingle` or `TransferBatch`, expanding
to 546 token transfer items, including 43 mint and 77 burn events. These are
sample counts, not a chain-wide rate estimate.

The two sampled wallet trades corresponded to transfers of 194151516 and
25588236 raw units of one outcome token into the wallet. Together they equal
219739752 raw units, matching latest on-chain `balanceOf`; the API position
displays 219.7397 shares. This validates the observed 1,000,000 raw
units/share scale for this token. Token IDs were decoded from event data,
not indexed topics. The same transaction can contain intermediary transfers,
minting, and a final transfer into the wallet; a transaction hash alone is
not a transfer-item key. Replay needs block, transaction, log, and batch-item
ordering plus the from/to addresses, including the zero address.

The public RPC supported current `eth_call` and bounded historical logs, but
rejected `eth_call` at the sampled historical blocks because it lacked the
required state. A second public endpoint gave inconsistent historical-state
availability, and another returned HTTP 401. No suitable archival RPC is
configured in this workspace. `PolygonCtfClient.eth_call` currently fixes
the block argument to `latest`; a historical check would require a small
read-side extension even with an archival provider. We did **not** establish
token-creation blocks, a complete opening state, reorg handling, or the cost
of a full condition-wide transfer scan. Because token ID is not a topic,
market-wide extraction must read and decode CTF transfers across a bounded
block range before filtering token IDs. Address-indexed topics may help a
known-wallet query, but cannot discover all holders of a token alone.

## Decision

| Consumer answer | Decision | Evidence boundary |
| --- | --- | --- |
| Current observed holders | Supported with gaps | Gross holder and OPEN-position pages are available, but scans are not atomic and capped pages must be resumed. |
| Known-wallet API-derived dated timeline | Supported with gaps | Trades and lifecycle activity explain some sampled balances; blank-token lifecycle rows, direct transfers, older coverage, source floors, and unavailable historical checks prevent exactness. Dates without covered opening/events must be `uncovered`, never zero. |
| Exact market-wide dated holder history | Unsupported by API-only evidence | The old market has an on-chain holder whose API trade/activity feeds contain no event, and market-wide transfer recipients cannot be proven from these feeds. |

**Go/no-go:** build only a clearly labelled, source-limited API timeline if a
consumer accepts the gaps. The original exact market-wide question remains
unanswered. The two-block chain extraction demonstrates a plausible path but
does not justify a production indexer. Before that commitment, obtain an
archival Polygon RPC and run a one-market, token-creation-to-head transfer
extraction with a documented zero opening state, direct-recipient discovery,
dated `balanceOf` checks, request/storage cost, and reorg/reconciliation
criteria. Keep the result `uncovered` where those checks cannot be made.
