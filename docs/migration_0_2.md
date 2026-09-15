# Migrating to pmkt 0.2

Version 0.2 deliberately breaks the experimental Python API. Endpoint coverage,
canonical dataset schemas, resolution serialization, and existing catalog
formats remain available. No runtime compatibility aliases are supplied.

## Configuration and runtime

`PmktConfig(...)` validates explicit values and defaults without reading the
environment or searching for dotenv files. Applications that need those sources
must call `PmktConfig.from_env(...)` and pass the result to clients. The explicit
loader retains argument > process environment > dotenv precedence and the
existing `PMKT_ENV_FILE`, `PMKT_ENV_DIR`, `.env`, and `.env.local` conventions.
`from_values()` and `get_config()` have been removed.

```python
from pmkt.config import PmktConfig
from pmkt.runtime import OperationExpiry, RequestPolicy
from pmkt.exchanges.polymarket import AsyncGammaClient, PolymarketMarketRef

config = PmktConfig.from_env()
policy = RequestPolicy(max_attempts=3)  # Initial attempt plus at most two retries.

async def read_market():
    async with AsyncGammaClient(config=config, request_policy=policy) as gamma:
        return await gamma.get_market(market=PolymarketMarketRef("123"))
```

`RequestPolicy` and `OperationExpiry` are public in `pmkt.runtime`.
`max_retries` is removed; its former meaning was total attempts, so migrate
`max_retries=3` to `max_attempts=3`, not four. Nonpositive and noninteger budgets
are rejected. The repository API scripts use `--max-attempts`.

Normalized workflows accept `deadline_s`. They create one monotonic expiry for
all nested work, including limiting, retries, pagination and source fallback.
Native request methods accept an optional keyword-only `expiry`; omission keeps
the per-request timeout without introducing an operation-wide deadline.
Cancellation drains owned work. Synchronous parsing checkpoints and cleanup do
not imply a hard process wall-clock cutoff.

## Clients and resolution

Use `AsyncGammaClient`, `AsyncClobClient`, `AsyncKalshiClient`, and
`AsyncSubgraphClient`. The shorter names that aliased these asynchronous clients
are removed. Native endpoint methods retain native identifiers and capabilities.

| Old normalized call | New call |
| --- | --- |
| `gamma.get_market(market_id=id)` | `gamma.get_market(market=PolymarketMarketRef(id))` |
| `kalshi.get_market(ticker=ticker)` | `kalshi.get_market(market=KalshiMarketRef(ticker))` |
| `resolver.resolve(id)` | `resolver.resolve(PolymarketMarketRef(id))` or the matching Kalshi reference |

Resolution accepts providers implementing the documented narrow protocols in
`pmkt.resolution.providers`. Their native methods receive the same expiry as the
operation. Built-in clients and custom providers use this one convention; old
method signatures and private resolution methods are unsupported. Resolvers
borrow supplied providers and do not close them.

## Results and evidence

Book and history results have `result.provenance`, containing `observations`,
`interpretation_id`, `package_version`, and `raw_responses`. Each raw response
has a `request_id` linking it to an observation and a decoded `payload` copied
once when retained. The nested payload is caller-owned mutable evidence; it is
not a recursively immutable object. Candle rows no longer contain raw payloads.

All history bounds and row accounting live in `result.coverage`. Use
`coverage.requested_start_utc`, `coverage.requested_end_utc`,
`coverage.queried_windows`, and the observed bounds there. Query windows preserve
each actual request rather than implying that the extent was one continuous
query. Removed fields have no alias properties.

Single-observation market records and resolution authority evidence keep their
own domain contracts. Candle routing and completion cutoffs, source completeness,
quality flags, and price basis retain their distinct meanings. Arrow/pandas
conversion derives existing metadata and schemas from the new layout.

## Trading consumers

The coordinated `pmkt-trading` release constructs core configuration through
`get_trading_config().core_config()`. Private credentials, deployment authority,
signed transports, and execution retry policy remain in trading. Trading does
not subclass or import core's private HTTP implementation.

Use the exact committed core dependency recorded by trading. Package verification
must use installed distributions outside the source checkout; importing a sibling
core worktree does not validate the dependency pin.

## Raw-evidence memory comparison

A local Python 3.10 `tracemalloc` comparison normalized the same 5,000 synthetic
live Kalshi minute candles with retained evidence, using identical dependencies.
The reviewed `53e9d1b` implementation peaked at 21,081,517 bytes; the 0.2 layout
peaked at 15,161,387 bytes, approximately 28% lower. Both returned 5,000 candles.
This measures Python allocations during normalization, not process RSS or a
guaranteed reduction for every payload. The rejected-row regression also checks
that retained evidence survives fixture mutation without per-candle raw copies.
