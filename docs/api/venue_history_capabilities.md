# Venue Historical Data Capabilities

Checked for CR-10.0 on 2026-06-17, the public sampled-price and Kalshi
candle-history workflows on 2026-09-11, and archived market listing on
2026-09-24.

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

- `/historical/markets` lists archived market metadata and settlement fields with
  cursor pagination. Market, event, series, and `mve_filter=exclude` filters are
  mutually exclusive. Kalshi does not accept `mve_filter=only` here. The ingest
  command pins a snapshot to the starting `market_settled_ts` cutoff and keeps
  only rows settled before it, even if the cutoff advances during pagination.
  Resumed segments carry that same cutoff. The live and archive listings can
  overlap, so combined listings require ticker deduplication. Kalshi market
  objects omit `series_ticker`; for a filtered snapshot, read that scope from
  the collection manifest rather than the normalized row. This is a market
  listing, not historical order-book evidence. Source:
  <https://docs.kalshi.com/api-reference/historical/get-historical-markets>
- `/series/{series_ticker}/markets/{ticker}/candlesticks` provides live-dataset market candlesticks with price, yes-bid, yes-ask, volume, and open-interest context. `start_ts`/`end_ts` are inclusive end-labels. Source: <https://docs.kalshi.com/api-reference/market/get-market-candlesticks>
- `/markets/candlesticks` provides batched live-dataset candlesticks for up to 100 market tickers and up to 10,000 candles total. Source: <https://docs.kalshi.com/api-reference/market/batch-get-market-candlesticks>
- `/historical/markets/{ticker}/candlesticks` provides archived market candlesticks after Kalshi's historical cutoff, with the same inclusive `start_ts`/`end_ts` labels. The single-market live and historical endpoints do not publish a numeric cap. Source: <https://docs.kalshi.com/api-reference/historical/get-historical-market-candlesticks>
- `/historical/trades` provides historical trade context. Source: <https://docs.kalshi.com/api-reference/historical/get-historical-trades>
- There is no documented historical topbook or historical depth endpoint. `/markets/{ticker}/orderbook` is a current order-book snapshot endpoint, so CR-10.1 recording is required for forward-looking executable topbook evidence.
- `AsyncKalshiClient.get_candles(...)` is the supported single-market,
  explicit-window adapter. Auto routing compares native market settlement with
  the retained `market_settled_ts` cutoff; explicit source modes do not switch.
  A qualified 404 permits at most one alternate dataset attempt, while empty
  success, authentication, timeout, and server errors do not.
- Live and archive field names and units are decoded independently. Returned
  records separate traded price from YES bid and ask OHLC and retain native
  evidence. Fully contained, completed periods are selected after global
  reconciliation. HTTP traversal does not establish source completeness.
  The adapter chunks single-market requests at 5,000 elapsed periods
  (`end_ts - start_ts`), not candle count, so a max-sized chunk may include
  5,001 inclusive end-labels. That bound is a conservative client choice
  relative to the published batch 10,000-candle cap.
- For the versioned initial 1440-minute interpretation, the native end minus
  86,400 seconds must be `America/New_York` midnight. Saved primary API checks
  across both 2025 fall and 2026 spring DST transitions support this bounded
  analytical rule; it is not a vendor timezone or calendar-day guarantee.

## Implementation Notes

- CR-10.0 writes `historical_price.v1`, Kalshi trade context as `trade.v1`,
  `venue_history_capability.v1`, and `historical_backfill_gap.v1`.
- Unsupported topbook/depth backfill is recorded as explicit gap rows instead of silently omitted.
- CR-10.2 must consume recorded `topbook.v1` rows for decision-time arbitrage checks; historical price/candle rows are only contextual diagnostics.
