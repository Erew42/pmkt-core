# Polymarket participant API pilot and chain capability decision

Date: 2026-09-24 UTC. Status: bounded research observation, not a production
historical-balance contract. This applies the go/no-go gate in
`polymarket_participant_history_contract.md` to the consumer question: **who
held each outcome on a chosen date, including wallets no longer holding it?**

## Sample and method

The pilot read the [official Data API v2 OpenAPI document](https://data-api.polymarket.com/v2/openapi.json), then used public
`/v2/positions` (`OPEN`, `CLOSED`), `/v2/holders?include_pnl=true`,
`/v2/trades?condition=...&taker_only=false`, and wallet
`/v2/activity?start=1&condition=...`. Wallet `/v2/trades?start=1` was also
walked up to five 1,000-row pages per sampled wallet. The market scans used
one 100-row page per feed. The market-page sample finished near 21:10 UTC and
the wallet replay near 21:30 UTC. The exact replay wallets are listed below.
All source reads were live and non-atomic; page caps and cursor exhaustion
are reported separately.

| Case | Gamma market ID and condition | OPEN / CLOSED position page | Gross holder page | Condition trade page |
| --- | --- | --- | --- | --- |
| Active | `665374`, `0x5db999fad322cea2914535aae5517060c3f80ad6d8c0231cde2124a434d16846` | 100+ / 100+ | 200+ rows in two token groups | 100+ |
| Recently closed | `4897525`, `0x7590bb39104ef7ca80f6d42944f0f3c23464a36e0f26fd3254b042085d67fe49` | 30 / 100+ | 31, cursor exhausted | 100+ |
| Negative risk | `2063134`, `0x7d0aaf81bbd3fd73b6a1651cce08a452c0cbf9c0cbb4520ce0f981065b639d88` | 100+ / 100+ | 153, cursor exhausted | 100+ |
| Older closed, created 2020 | `12`, `0xe3b423dfad8c22ff75c9899c4e8176f628cf4ad4caa00481764d320e7415f7a9` | 25 / 7, cursors exhausted | 31, cursor exhausted | 0, cursor exhausted |

`+` means the page returned a continuation cursor, not an estimated total.
The selected wallets were:

| Case | Replay wallets |
| --- | --- |
| Active | `0x0224bb9eb0a5c9fd261ac9123a72cbdd5748292a`, `0x000d257d2dc7616feaef4ae0f14600fdf50a758e` |
| Recently closed | `0x04c2f84c8a94637144f694be9ed45921dfd943c4`, `0x0539490293b8d671188fdf900de22de501746c1f`, `0x071f9c6bfa9cb3c609fe8e9ffbf43521cb1892bd` |
| Negative risk | `0x04b26f4f28716d305e7eb746e8953de5833de801`, `0x03805a13a0b3e058f55f6c6af95389d4f431073d` |
| Older closed | `0x0fecb2d97acad2d6dee3f1d04d3c868a2829c9f7`, `0x37ed804f6e56ab691ad6c77d78dca41a505e13e4` |

Gamma reports the older market as `active=true`, `closed=true`,
`archived=false`; it is **not** an inactive-market test. A bounded Gamma
search did not find a genuinely inactive candidate, so that contract case
remains unverified here. The official v2 specification still states that
inactive positions are excluded even with `include_archived`.

The official trade contract fixes condition queries to three years and floors
size at 0.01 shares. These source limits make the old market's empty condition
trade response expected; it does not prove no trading occurred. The activity
default omits opt-in `TIP` pUSD transfers. Outcome-token transfers need
separate ERC-1155 evidence; neither cursor exhaustion nor `start=1`
certifies them.

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
- One negative-risk wallet's global trade page contained a 62-hex-digit
  `condition_id` rather than a canonical 64-digit ID. The prior strict parser
  rejected the entire wallet page. Wallet-scoped reads now preserve that
  source hex ID; condition-scoped reads still require a match to the requested
  canonical ID. After the fix, this wallet's single market activity trade
  reproduced its five-share current position, while its global wallet trade
  walk still had a cursor after five pages.

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

## Coverage recheck

A follow-up at approximately 21:54 UTC compared completed gross-holder and
`OPEN` position scans for two markets. In the recently closed market, gross
holders contained 25 wallet/token rows and `OPEN` positions 24; the extra
holder had a positive `CLOSED` position. In the older market, gross holders
contained 31 rows and `OPEN` positions 25; all six extra holder rows were
positive residual balances in `CLOSED`. One such wallet had two gross amounts
of 0.062071 and 0.061887 shares; latest CTF `balanceOf` returned 62071 and
61887 raw units. Position `current_size` displayed 0.062 and 0.0618. Across
matched rows, the largest gross-versus-position display difference was below
0.0001 shares. The recently closed market's holder count changed from the
earlier sample, illustrating that these live scans are not a shared snapshot.
`OPEN` alone therefore misses some observed current holders; gross holders
are the better current per-token observation, with their own scan limits.

The same recently closed market had exhausted trade cursors with 197
`taker_only=true` rows and 522 `taker_only=false` rows. Twenty-five wallets
appeared only on the maker-inclusive side of this complete market feed. For
one such wallet (`0x33ab58e55895f39619815d31dfc92d90d65f9523`), a
maker-side fill appeared in both the wallet trade and condition activity
feeds. This verifies the need for `taker_only=false` in market discovery and
shows that overlapping activity TRADE rows must not be added again.

## Decision

| Consumer answer | Decision | Evidence boundary |
| --- | --- | --- |
| Current observed holders | Supported with gaps | Gross holder pages include small positive balances classified `CLOSED` by positions. `OPEN` alone misses them; scans are not atomic and capped pages must be resumed. |
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
