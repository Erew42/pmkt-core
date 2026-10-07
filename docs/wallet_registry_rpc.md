# Historical Polymarket wallet registry

Run from the repository root on Linux, using Python 3.10–3.12 with the core
dependencies installed:

```bash
python scripts/collect_polymarket_wallets_rpc.py --output data/wallet-registry
```

For a standalone deployment, keep `wallet_registry_export.py` beside the
collector script. An existing running interpreter needs a controlled restart
to adopt these changes; editing the source does not update that process.

The default is the public `https://polygon.gateway.tenderly.co` endpoint, with
one request at most every second (`--request-delay 1`), including startup checks
and each retry. This is a client pacing limit; it does not establish the
provider's effective quota. No account credentials, signing key, authenticated
venue endpoint, or paid service is used. The RPC transport accepts only
`eth_chainId`, `eth_blockNumber`, `eth_getBlockByNumber`, and `eth_getLogs`.
It verifies Polygon mainnet (137), freezes an inclusive end at latest minus 256
blocks, and checks the end-block hash on resume and completion.

The first research dataset is a registry, not each wallet's full trade history.
The script scans these public settlement events:

| Source | Scope | First scanned block |
| --- | --- | --- |
| AMM factory | `0x8b9805a2f595b6705e74f7310829f2d299d21522`, pools using CTF `0x4d97dcd97ec945f40cf65f87097ace5ea0476045` | 4,023,693 |
| AMM trades | `FPMMBuy` and `FPMMSell` from those discovered pools, any collateral | 4,023,693 |
| CLOB v1 | `0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e`, `0xc5d563a36ae78145c45a50134d48a1215220f80a` | 33,605,403 |
| CTF v2 | `0xe111180000d2663c0091e4f400237545b87b996b`, `0xe2222d279d744050d28e00520010520000310f59` | 33,605,403 (includes time before deployment) |
| Combo exchange proxy | `0xe3333700ca9d93003f00f0f71f8515005f6c00aa` | 33,605,403 (includes time before deployment) |

Exchange reads select `OrderFilled` in both ABI versions. The signed-order maker
and the fill counterparty are distinct evidence roles. Counterparties can be
operators or contracts; known exchange/router/adapter/CTF addresses are excluded
as structural counterparts. Other smart-contract wallets are retained. AMM
events identify the buyer/seller. Positive outcome-token quantity is required;
zero collateral payment alone does not invalidate a trade. Transfers, liquidity
funding, position splits, holders, market creation, and transaction senders alone
do not qualify a wallet. Addresses are normalized lowercase; no owner/signer
identity or cross-wallet linkage is inferred.

Factory and AMM scans advance together, with factory discovery always committed
before an AMM range. Unrelated contracts emitting the same AMM topics are ignored.
Their RPC log envelopes still undergo validation; their payloads are not decoded
using the discovered pools' ABI. Malformed events from a discovered pool fail
without advancing its range.
The factory is permissionless: this attributes trades to a contract family, and
does not independently establish that each pool was listed on the website.

## Persistence and evidence

Keep output under an ignored directory such as `data/`. Outputs include:

- `registry.sqlite3`: durable wallets, factory witnesses, every request's exact
  method/params, endpoint, receipt times, response hash/byte count/error, accepted
  ranges, and independent stream checkpoints. SQLite WAL permits a snapshot
  reader alongside collection. An OS writer lock prevents a second collector
  or independent `--export-only` invocation from using the same output.
- `status.json`: atomically updated committed counts and next blocks after each
  productive collection iteration, together with export health and freshness.
  Stream checkpoints commit after every accepted range and are authoritative
  after a crash. `rpc_wait` records the cooldown reason, remaining duration at
  publication and not-before deadline; `phase_timings_s` separates RPC, status
  and validation/commit time. `wal_maintenance` reports reclamation or deferral.
- `wallets.csv`: one `(chain_id, wallet_address)` row, observed first/last blocks,
  observed roles, and a witness transaction hash and log index.
- `wallet-evidence.jsonl`: one earliest observed raw trade log per wallet,
  linked to request provenance. AMM witnesses also retain the factory creation
  event, collateral and condition IDs. Raw logs include block hash, transaction
  hash, log index, topics and data.
- `manifest.json`: source inventory, event signatures, collector hashes, product
  hashes, coverage and limitations. `lifetime_coverage_proven` is always false.
- `latest-export.json`: an atomic pointer to a completed immutable export
  generation containing the matching CSV, evidence and manifest. Resolve this
  pointer once and read that generation when consuming the bundle. The root
  `wallets.csv`, `wallet-evidence.jsonl` and `manifest.json` symlinks are
  compatibility views; separate reads of them can span generations during
  publication. The export manifest describes its database snapshot and does
  not overwrite the collector's live `status.json`. The publisher retains the
  newest and immediately preceding completed generations.

Collection requires SQLite to confirm WAL mode rather than silently fall back to
a rollback journal. Status totals are initialized when opening a registry and
updated after committed changes, avoiding full table scans after every range.
These invocation caches do not replace the database as the resume authority.
Evidence exports preload the earliest request-provenance records up to a 32 MiB
estimate, including Python object overhead, and use a separate 4,096-entry
lookup cache. The manifest records preload size, lookup misses, cache statistics
and serialization time to
help measure the benefit; witness contents and product hashes retain their
existing meaning.

The writer sets `journal_size_limit` to 64 MiB to reclaim oversized retained WAL
allocation when the WAL resets. This is not a hard cap while a snapshot reader
still needs old pages. Writer maintenance at startup and after worker release
attempts to reclaim a larger WAL with no lock wait; a busy reader defers it.
Maintenance also runs before launching snapshots, including replacing an older
worker for the final export. Collection shutdown leaves reclamation for the next
startup instead of adding checkpoint I/O to the stop path. Checkpoint transfer
still performs disk I/O. This SQLite WAL maintenance is separate from committing
an application's stream checkpoint after an accepted range. See
[SQLite's WAL description](https://www.sqlite.org/wal.html) and
[journal-size setting](https://www.sqlite.org/pragma.html#pragma_journal_size_limit).

Full RPC responses and every fill are not retained; their hashes cannot by
themselves reconstruct omitted bytes. Witnesses can be independently checked
against public transaction or block receipts. Some RPCs prune historical
transaction-hash lookup while retaining block receipts. Accepted-range counts describe returned
logs, not economic executions. A transaction may contain several different fills.
Within each range, identical logs are deduplicated by block hash and log index;
conflicting duplicates, malformed ABI data from in-scope contracts, removed
logs, unexpected addresses, and logs outside the requested range fail without
advancing the checkpoint.

Throttling and recognized transient transport/server failures retry with bounded
exponential backoff. HTTP 429, compute throughput errors and any `Retry-After`
header take priority over range reduction, including on a timeout response.
The same retry handling applies to startup identity and anchor checks. Retry
exhaustion leaves checkpoints in place. The remaining provider cooldown is
retained across restarts, including request-budget exits and interrupted retry
waits. The request error record includes HTTP status, RPC code and parsed
`Retry-After`.

A provider cooldown longer than 900 seconds stops the invocation with an error
without shortening the delay. The full not-before deadline remains saved.
A restart with more than 900 seconds remaining also fails before an RPC call;
once the remainder is within that bound, collection
waits it out before requesting again. This limit bounds a single wait, not the
provider's cooldown.

Recognized provider range/result limits, or responses reaching `--log-cap`
(default 5,000), shrink the range immediately. Two consecutive capacity failures
on the same `eth_getLogs` range also trigger reduction: read timeouts, recognized
RPC timeout errors, or HTTP 502/504 without throttling or a `Retry-After` header.
A single such failure retries the original range. This is a collector recovery
heuristic; it does not prove that the provider failure was caused by range size.
The reduced window is saved without advancing the stream's next block. Explicit
invalid-request, unknown-method and invalid-params errors fail immediately unless
recognized as a provider range limit or throttling response. Any supplied
cooldown is still saved before exit. Malformed responses also fail without range
reduction.
A single block reaching that cap fails explicitly. Successful small responses
allow growth up to `--max-blocks` (default 100,000). Empty responses advance only
as provider observations. Public RPC can silently omit data, and the cap does
not prove absence of truncation below the threshold. All configured ranges
being scanned is distinct from independently verified lifetime coverage.

Provider limits checked on 2026-10-07: [Tenderly's public endpoint
documentation](https://docs.tenderly.co/node-rpc/overview#public-endpoint-limits)
lists a 3,000-result limit per `eth_getLogs` call and the oversized-query error
`-32602`; its network table lists no daily response-byte limit for Polygon.
Those public-endpoint docs give no numeric requests-per-second guarantee or
timeout policy. The collector's configured log cap is a separate truncation
safeguard and is pinned for an existing dataset; it does not override a
provider's lower limit.
[Alchemy's throughput documentation](https://www.alchemy.com/docs/reference/throughput)
distinguishes compute-unit capacity throttling from query size. Its
[error reference](https://www.alchemy.com/docs/reference/error-reference)
also documents gateway timeouts and internal errors. These sources inform error
classification; they do not establish a Tenderly rate guarantee.

## Bounded runs and resume

```bash
# Stop after at most 100 network calls, retaining committed checkpoints.
python scripts/collect_polymarket_wallets_rpc.py \
  --output data/wallet-registry --max-requests 100

# Resume the same frozen history with the original collection options.
python scripts/collect_polymarket_wallets_rpc.py --output data/wallet-registry

# Export committed rows after collection stops, without network access.
python scripts/collect_polymarket_wallets_rpc.py \
  --output data/wallet-registry --export-only

# Example rollout for a dataset whose factory and AMM scans are already complete.
# Replace the scratch placeholder with an SSD directory outside the checkout.
python scripts/collect_polymarket_wallets_rpc.py \
  --output data/wallet-registry-example --streams exchange \
  --request-delay 1 --export-interval-s 43200 \
  --export-mode snapshot \
  --export-scratch-dir /path/to/ssd/wallet-registry-export-scratch

# Isolated early AMM pilot; this is explicitly a restricted historical window.
python scripts/collect_polymarket_wallets_rpc.py \
  --output data/wallet-registry-early-pilot --end-block 4123692 \
  --streams factory,amm
```

The end block, contract inventory, start blocks, RPC URL and collection limits
are pinned. Resume rejects changes to these options or the anchor hash; use a
different directory for a different collection. `--exchange-start-block` is for
partial pilots and cannot later be silently turned into a lifetime scan.
`--streams` may select subsets without changing existing checkpoints; AMM
requires factory. Select only `exchange` when the other streams are already
complete. No holes are jumped over after errors. A request-budget exit, Ctrl+C,
SIGTERM or startup failure retains committed checkpoints without initiating a
full export. Normal collection completion retains a final export. Use
`--export-only` after collection stops to export committed rows without RPC
access. Export-only requires an initialized registry; a missing or empty one
fails without creating or initializing a database. Other failures exit nonzero.

## Export scheduling and concurrent snapshots

The full-export scheduling interval defaults to 43,200 seconds (12 hours).
`--export-interval-s` accepts a positive interval in seconds. The default
`--export-mode sync` performs exports in the collection process and pauses
collection while generating the files. Its interval starts after the preceding
export finishes. Snapshot mode
schedules approximately between export starts instead. Resume uses the last
published generation's time, so restarting does not reset a due export's
schedule. Publication freshness includes copy/serialization time and deferrals.

Opt into `--export-mode snapshot` to keep collection running during periodic
exports. One worker establishes a fixed database snapshot, copies it to
`--export-scratch-dir`, releases the live reader, then generates and publishes
the export from the copy. This mode requires an SSD scratch directory outside
the checkout. The snapshot copy retains old WAL pages while its reader is
active; generating the CSV and evidence afterward no longer holds that reader.
The worker makes no RPC calls and inherits the process's low priority when the
collector is run at low priority. Concurrent copying still competes for source
disk bandwidth, so full-size measurements are needed to establish throughput.

The copy controls are:

| Option | Default | Purpose |
| --- | ---: | --- |
| `--snapshot-timeout-s` | `900` | Bound snapshot-copy time. |
| `--snapshot-max-wal-bytes` | `2147483648` (2 GiB) | Bound live WAL size while copying. |
| `--snapshot-min-free-bytes` | `10737418240` (10 GiB) | Reserve free space beyond the remaining snapshot copy. |
| `--snapshot-pause-s` | `0.01` | Pause between incremental copy steps. |

Only one periodic worker runs at a time; schedules do not queue a backlog.
Resource limits can defer an export or abort its copy. Worker failures before
publication leave collection running and retain the previous completed
generation. Updating `latest-export.json` commits publication; a later
compatibility-symlink error is reported without undoing that completed bundle.
Live status
reports export health and the age of the last completed export, which can
exceed the selected interval when copying is deferred or fails. Graceful stops
cancel outstanding periodic work without starting a new full export; final
exports on normal completion and explicit offline exports remain available.

Export interval, mode and worker limits are recorded as invocation metadata
alongside collector identity and may change on resume. They do not change the
pinned dataset configuration, frozen anchor, wallet/evidence grain, schema or
request provenance. The public endpoint may become unavailable; checkpoints
remain available, but choosing a new provider requires a separate dataset.

Source inventory checked on 2026-10-01: [current contract addresses](https://docs.polymarket.com/resources/contracts),
[historical deployments](https://github.com/Polymarket/polymarket-subgraph/blob/main/networks.yaml),
[AMM ABIs](https://github.com/Polymarket/polymarket-subgraph/tree/main/abis),
[CLOB v1 interface](https://github.com/Polymarket/ctf-exchange/blob/main/src/exchange/interfaces/ITrading.sol),
[CTF v2 interface](https://github.com/Polymarket/ctf-exchange-v2/blob/ccc0596074f4dfd62c944fbca4de252893b82b4b/src/exchange/interfaces/ITrading.sol),
and [verified combo implementation ABI](https://polygonscan.com/address/0x7345C6842b244926125ed4054905cAc49620B5dc#code).
New contracts or upgraded event ABIs require a reviewed inventory change and
their own coverage assessment.
