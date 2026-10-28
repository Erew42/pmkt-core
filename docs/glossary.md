# Glossary

Terms as this repository uses them: the code name for each venue concept, and
the traps where the venue's word and ours differ. It assumes you know
prediction markets. Prices are `*_dollars` (0-1) and sizes `*_contracts`. For
columns and dtypes run `pmkt schema show <version>`; this page is a map, not a spec.

## Venue identifiers

- **Event, market, series.** Polymarket: an event (`models.Event`) groups markets;
  a market (`models.Market`) is one binary question. Kalshi: `series_ticker` >
  `event_ticker` > `ticker` (the market); `/historical/markets` rows omit
  `series_ticker`. Columns: `venue_event_id`, `venue_market_id`.
- **Market id vs condition id (Polymarket).** `market_id` is Gamma's numeric id;
  `condition_id` is the 0x + 64-hex on-chain id, sent as `market` on CLOB payloads.
  Data API wallet rows can shorten it (`_SOURCE_CONDITION_RE`): validate before keying.
- **Token id / asset id (Polymarket).** One CLOB token per outcome: `token_ids` or
  `clob_token_ids` in market payloads, `asset_id` on CLOB and WebSocket messages.
  It is the Polymarket `instrument_id` (`pmkt.tokens.extract_token_ids`).
- **CTF.** The Conditional Tokens contract on Polygon. Resolution reads payouts
  from it (`PolygonCtfClient`, `CTF_CONTRACT_ADDRESS`, source `polygon_ctf`).
- **Kalshi ticker and `instrument_id`.** One ticker holds both outcomes, so an
  instrument is `<ticker>:YES` or `<ticker>:NO`; trades are recorded against
  `<ticker>:YES` at the YES price.
- **MVE.** Kalshi parlay-style markets, kept apart from conventional ones:
  `mve_filter` on listings, `KXMVE` prefix (`KALSHI_LEGACY_MVE_PREFIX`), catalog
  `native_family` `kalshi_mve`.

## Sides, prices and trades

- **YES/NO and bid/ask.** Kalshi's wire has only bid ladders (`yes`, `no`); a NO
  bid at p is a YES ask at 1-p. Asks are derived with `complement_probability`
  and labelled `direct`, `complement_derived` or `missing` (`BookSideProvenance`,
  `best_ask_source`). Polymarket sends `bids` and `asks` per token. `depth.v1.side`
  is `bid`/`ask` for Polymarket, `yes`/`no` for Kalshi.
- **`use_yes_price` and `depth.v1`.** Kalshi WebSocket flag, default true
  (`kalshi_public_subscription_payload`). True: the NO ladder arrives on the YES
  price scale (its lowest value is a direct YES ask); false: it is a direct NO bid
  (`project_kalshi_quotes`). Capture settings store it; absent means true
  (`resolve_kalshi_use_yes_price`). **Trap:** `depth.v1` `outcome=NO` rows copy the
  native NO ladder uncomplemented, so `price_dollars` is YES-scale for WebSocket
  captures with the flag true (`_kalshi_depth`) and NO-scale for REST polls
  (`source=rest_poll`, `_kalshi_depth_rows`). The row does not record which.
- **Trap: Kalshi `taker_side`.** It names the outcome the taker is positioned for,
  not the book side consumed: `yes` means a YES ask was lifted (taker bought YES
  or sold NO). The venue deprecates it for `taker_outcome_side` (same values);
  `taker_book_side` `bid` also means `yes`. Stored unnormalized as
  `trade.v1.aggressor_side` and `recording_trades.v1.reported_side`.
- **Trap: Data API v2 `taker_only`.** `/v2/trades` defaults to `taker_only=true`,
  which drops maker fills, and rows carry no maker/taker role. Core always sends
  `taker_only=false` (`trades_page`, `market_trades_page`) and, for full wallet
  history, `start=1`. Condition-scoped reads have a fixed three-year window.

## Books and streams

- **Topbook, depth.** `topbook.v1`: best bid/ask per instrument. `depth.v1`: one
  row per level of a full-book snapshot (`is_delta` is always false).
  `recording.v1` equivalents: `recording_topbook.v1`, `recording_book_snapshots.v1`
  with `recording_book_levels.v1`.
- **Snapshot and delta.** Polymarket `book` is a snapshot, `price_change` a delta
  (`MarketBookState`); Kalshi `orderbook_snapshot` / `orderbook_delta`
  (`KalshiOrderBookState`). Persisted depth is always full state.
- **Sequence.** Kalshi `seq` and `sid` (`venue_sequence`, `venue_sid`) run per
  subscription, not per market, so forward jumps within one market are normal; a
  real gap sets `seq_gap`. `local_sequence` is capture-local order; Polymarket has
  `book_hash` and no sequence. `recording.v1` uses `record_sequence`,
  `source_sequence`, `connection_generation`.
- **Book validity.** Legacy rows carry `valid_state` and `quality_flags`
  (`crossed_book`, `empty_bid`, `no_initial_snapshot`). `book_integrity_valid`
  exists only in `.v2` schemas (`INTEGRITY_SCHEMA_VERSIONS`); absent means
  unknown, never true. `recording.v1` has `initialized`, `integrity_valid`, `quote_valid`.
- **Managed feed.** `pmkt.streaming.ManagedBookFeed`: in-memory live public depth,
  no REST. It returns detached `PublicBook` and `PublicBookHealth`; re-read both
  for every decision. Kalshi needs an injected `ReadAuthHeaderProvider`.

## Capture, reconstruction and catalog

- **Capture.** Recording a live feed, since neither venue documents a historical
  book endpoint (`/book`, `/markets/{ticker}/orderbook` are current only).
  `stream-books` and `stream-kalshi-books` write `recording.v1` (`BookRecorder`,
  `RecordingOptions.mode` `topbook` or `full`); older profile runs use
  `collector_run_id` and `run_manifest.v1`. `recover-stream-run` reads both.
- **Tape and reconstruction.** The tape (`book_tape_event.v1`, `book_tape_level.v1`,
  `book_tape_control.v1`) is the legacy replayable log; `recording.v1` is not one.
  `reconstruct-book-tape` replays it into `topbook`/`depth` rows with
  `source=reconstructed_book_tape`, checking post-book hashes
  (`BookTapeReconstructionError`).
- **Catalog, census, discovery.** `data/markets` (`pmkt markets`). A census is the
  full list of current nonterminal markets per stream (`polymarket`,
  `kalshi-conventional`, `kalshi-mve`); discovery finds markets created since a
  watermark. Rows reuse `polymarket_market_snapshot.v1`, `kalshi_market_snapshot.v1`.

## Resolution and wallets

- **Resolution.** `pmkt.resolution` returns a `ResolutionRecord`, stored as
  `market_resolution.v1`. `resolution_state` (`final`, `provisional`, `disputed`,
  `oracle_resolved_chain_pending`) and `confidence` are separate: `canonical` is
  the venue's authoritative source (`canonical_source`: Polygon CTF payout for
  Polymarket, settlement fields for Kalshi). `payouts` are per-outcome numerators.
- **Wallets.** Core has no wallet census. It has Data API participant reads
  (`market_participants`, `wallet_history`, `holders_page`) and the registry
  collector `scripts/collect_polymarket_wallets_rpc.py`. A wallet is a public
  proxy address, not an identity, and trades do not reconcile to positions.

## Schemas and time

- **Schema id and version.** `schema_version` is `<name>.v<N>`, registered in
  `SCHEMA_REGISTRY` (`pmkt schema list|show`). A changed grain or field set gets a
  new id (`topbook.v2`, `depth.v2` only add integrity). Registered is not produced:
  see `docs/schema_lifecycle.json`.
- **Timestamps.** `received_at_utc` is local receipt; `exchange_ts_utc` and
  `venue_time_utc` are venue-supplied and may be null; `observed_at_utc` is the
  recorder's clock. Epoch units differ: Polymarket WebSocket trade `timestamp` is
  milliseconds, Kalshi `created_time` and Data API `timestamp_s` are seconds
  (`isoformat_source_timestamp`); canonical text needs a UTC offset
  (`parse_utc_timestamp`).
