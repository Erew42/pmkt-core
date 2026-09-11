# Venue Historical Data Capabilities

Checked for CR-10.0 on 2026-06-17 and the public sampled-history workflow on
2026-09-11.

## Polymarket

- `/prices-history` is available for CLOB market price history and supports time-bounded price context. Source: <https://docs.polymarket.com/api-reference/markets/get-prices-history>
- `/batch-prices-history` is available for batched CLOB market price history, with a documented maximum of 20 markets per request. Source: <https://docs.polymarket.com/api-reference/markets/get-batch-prices-history>
- There is no documented historical topbook or historical depth endpoint. `/book` is a current order-book snapshot endpoint, so old `/prices-history` rows must not be treated as executable order-book evidence.
- `AsyncClobClient.get_price_history(...)` is the supported single-token,
  explicit-window adapter over `/prices-history`. It maps `sampling_minutes` to
  fidelity and retains `price_basis="venue_defined"`; it does not imply a
  regular grid, trades, quotes, sizes, volume, depth, or source completeness.
- The upstream reference documents strict second-based bounds but no maximum
  request span. The adapter therefore widens integer boundary markers only for
  half-open local containment and performs one request; it makes no vendor
  maximum-span claim.

## Kalshi

- `/series/{series_ticker}/markets/{ticker}/candlesticks` provides live-dataset market candlesticks with price, yes-bid, yes-ask, volume, and open-interest context. Source: <https://docs.kalshi.com/api-reference/market/get-market-candlesticks>
- `/markets/candlesticks` provides batched live-dataset candlesticks for up to 100 market tickers and up to 10,000 candles total. Source: <https://docs.kalshi.com/api-reference/market/batch-get-market-candlesticks>
- `/historical/markets/{ticker}/candlesticks` provides archived market candlesticks after Kalshi's historical cutoff. Source: <https://docs.kalshi.com/api-reference/historical/get-historical-market-candlesticks>
- `/historical/trades` provides historical trade context. Source: <https://docs.kalshi.com/api-reference/historical/get-historical-trades>
- There is no documented historical topbook or historical depth endpoint. `/markets/{ticker}/orderbook` is a current order-book snapshot endpoint, so CR-10.1 recording is required for forward-looking executable topbook evidence.

## Implementation Notes

- CR-10.0 writes `historical_price.v1`, Kalshi trade context as `trade.v1`,
  `venue_history_capability.v1`, and `historical_backfill_gap.v1`.
- Unsupported topbook/depth backfill is recorded as explicit gap rows instead of silently omitted.
- CR-10.2 must consume recorded `topbook.v1` rows for decision-time arbitrage checks; historical price/candle rows are only contextual diagnostics.
