# Public raw capture samples

Small samples from `recorder-opt-20261106` revision `d38d566`, from the public
midterms recording on 2026-11-03 around 02:36 UTC:

- Polymarket: `polymarket_00/20261102T230001Z_cb498a42/chunk_000216.jsonl.gz`.
- Kalshi: `kalshi_00/20261102T230002Z_fa7dd56e/chunk_000215.jsonl.gz`.

Selection keeps the first occurrence of every frame/direction and event kind,
plus 200 evenly spaced frames per venue, deduplicated and kept in source order.
`coverage.json` records the kinds found by scanning each complete source chunk,
and source/sample counts. Payloads and observation times remain unchanged;
`local_frame_seq` is resequenced from 1 so each sample is a contiguous test tape.
These are not sealed source chunks or evidence of uninterrupted venue coverage.
The two committed gzip files total 49,812 bytes; the full chunks are not included.

Kalshi includes orderbook deltas, trades, lifecycle messages and WebSocket pongs.
Polymarket includes books, price changes, best bid/ask, last trade prices, new
markets and application heartbeats. Synthetic tests cover snapshots,
subscriptions, binary/control frames, gaps and malformed/unusual JSON not present
in these mid-session samples.
