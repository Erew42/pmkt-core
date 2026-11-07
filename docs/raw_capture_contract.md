# Public raw WebSocket capture

`pmkt record-raw-ws` and `pmkt.streaming.capture_raw_ws` preserve public
Polymarket or Kalshi WebSocket messages for later offline reconstruction and
merging of independent collectors. One process owns one connection and its
reconnects, for one fixed market list. This format is separate from the sampled,
normalized [recording.v1](stream_recording_contract.md) format. It produces no
canonical quotes and makes no wire-level losslessness claim.

The frame and chunk format preserves the standalone recorder at revision
`d38d566`: schema names, envelope field order and serialization, compressed bytes,
checksums and hash chains remain interchangeable. Disclaimer fields now live only
in `PROVENANCE.json`; sessions written by earlier versions also carry them in
chunk manifests and state files (including `live_state.json`, `READY.json` and
`FINAL.json`). Readers must ignore unknown fields. Retained recordings require
no migration; this metadata cleanup does not change frame lines or chunk bytes.

## Inputs and runtime

Install `pmkt[streaming]`. The recorder requires `websockets>=15` for its asyncio
connection/control-frame hooks and explicit proxy setting. On POSIX, optionally
install `pmkt[streaming,uvloop]` to use uvloop automatically. Without uvloop it
uses asyncio. Both paths support Python 3.10–3.12; the 3.10 runner cancels pending
tasks and shuts down asynchronous generators and the default executor explicitly.
The optional uvloop extra excludes Windows.

Capture requires POSIX signal handling in the main thread and directory fsync.
Importing the module and showing CLI help remain portable; capture rejects
unsupported platforms before creating output. SIGTERM/SIGINT request a partial
stop and seal the active chunk when possible. The CLI exits nonzero for a partial
result. A deadline or an already-passed deadline exits successfully; neither
proves uninterrupted coverage or readiness.

All recorder tuning defaults are named constants in
[`raw_capture.py`](../src/pmkt/streaming/raw_capture.py). Optional CLI tuning
options default to omission: only supplied values reach `capture_raw_ws`, so the
library selects the defaults. These include `--chunk-seconds`, `--gzip-level`,
`--lag-warn-ms`, `--low-free`, `--critical-free` and
`--gap-reconnect-min-interval`.

Supply an absolute `--runs-root` and an absolute `--root` exactly two levels
below it: `RUNS/RUN/INSTANCE`. Neither the path nor any ancestor may be a symlink;
`..` is rejected. RUN and INSTANCE must match
`[A-Za-z0-9][A-Za-z0-9_-]{0,95}`. The runs root is supplied explicitly, rather
than inferred from the installed module directory. The path checks run once at
startup and do not guard against concurrent changes, so only the recording user
may be able to write under the runs root. Every process start creates
a new session directory, including after a crash:

```text
RUNS/RUN/INSTANCE/YYYYmmddTHHMMSSZ_<8 random hex digits>/
    PROVENANCE.json
    READY.json
    live_state.json
    chunk_000001.jsonl.gz
    chunk_000001.jsonl.gz.manifest.json
    FINAL.json
```

A market-list JSON contains `venue`, `name` (the same name pattern), `ws_url`,
`instruments` and optionally `source` (market-list provenance). Instruments must
be a nonempty list of unique, nonempty, unpadded strings: Polymarket token IDs or
Kalshi native tickers, without `:YES`. Accepted URLs are:

- Polymarket: `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
- Kalshi: `wss://external-api-ws.kalshi.com/trade-api/ws/v2` or
  `wss://api.elections.kalshi.com/trade-api/ws/v2`.

Core imposes no instrument-count cap. Checked on 2026-11-07: Polymarket's
[market-channel documentation](https://docs.polymarket.com/developers/CLOB/websocket/market-channel)
now redirects to its real-time data guide; its
[May 28, 2025 changelog](https://docs.polymarket.com/changelog/predictions)
states that the former market-channel token subscription limit was removed.
Kalshi's [WebSocket connection reference](https://docs.kalshi.com/websockets/websocket-connection)
and [WebSocket quick start](https://docs.kalshi.com/getting_started/quick_start_websockets)
do not document a numeric per-connection instrument limit. Kalshi's
[AsyncAPI specification](https://docs.kalshi.com/asyncapi.yaml) defines error 26
for exceeding a per-subscription market limit, without specifying the bound;
its `market_tickers` arrays have no `maxItems`. No numeric warning threshold is
justified by these sources. Venue rejections, if any, remain recorded as raw
messages.

Polymarket uses no auth and subscribes with `assets_ids`, `type=market` and
`custom_feature_enabled=true`. Kalshi requires an injected
`ReadAuthHeaderProvider`; the CLI reuses `--header-provider MODULE:ATTRIBUTE`
(a provider object or a zero-argument factory). Every connection calls
`headers_for_read(provider, 'GET', '/trade-api/ws/v2')` for fresh headers. Core
does not load keys, credential files or scope-check endpoints. Headers are not
persisted. Kalshi uses core's `kalshi_public_subscription_payload` for three
separate subscriptions, IDs 1/2/3: `orderbook_delta` with `use_yes_price=true`,
`trade`, `market_lifecycle_v2`. Server acknowledgements/errors remain raw text.

## Frame envelopes

Each gzip member contains UTF-8 JSONL: one envelope per received message, control
frame, sent subscription/heartbeat or local diagnostic. TEXT payloads are the
exact strings delivered by websockets, including whitespace, escapes and JSON
batches. Binary messages and received WebSocket control payloads are base64.
Message reassembly belongs to websockets; these are not TCP packets or an archive
of fragmented wire bytes.

Fields, in serialization order:

| Field | Meaning |
| --- | --- |
| `schema` | `polymarket.public_raw_frame.v1` or `kalshi.public_raw_frame.v1`. |
| `venue` | `polymarket` or `kalshi`. |
| `recording_id` | A UUID for this process session; never shared across restarts. |
| `connection_generation` | Increments for each connection attempt in the session. |
| `local_frame_seq` | Session-wide sequence starting at 1 for stored envelopes, including sent/local ones. |
| `received_utc` | Collector receive/observation time, ISO 8601 UTC with `+00:00`; not network arrival or exchange time. |
| `received_monotonic_ns` | Collector monotonic clock at the same envelope creation; local to the host/boot, not network arrival or exchange time. |
| `frame_type` | The kind listed below. |
| `direction` | `received`, `sent`, `local` or `received_control_base64`. |
| `payload_raw` | Exact TEXT string, base64 payload, or a local diagnostic string. |

JSON uses ASCII escaping, compact `,`/`:` separators and a trailing newline.
The optimized serializer produces the same bytes as `json.dumps(...,
ensure_ascii=True, separators=(',', ':')) + '\n'`. Gzip uses `mtime=0` and
`--gzip-level` 1–9 (library default `GZIP_LEVEL`). Compression level changes
compressed hashes, not the decompressed envelope format.

`frame_type` values are `text`, `app_ping`, `app_pong`, `binary_base64`,
`ws_ping`, `ws_pong`, `ws_close`, `subscription`, `transport_error`, `seq_gap`
and `disk_pause_gap`. Application PING/PONG strings are Polymarket heartbeats;
received WebSocket control frames use `direction=received_control_base64`.
Binary data messages retain `direction=received`.

Timestamps are **collector receive/observation times, not network arrival or
exchange times**. TEXT/binary stamps are taken when the writer processes the
delivered message, after `recv()`; sent/local stamps describe the local action.
Scheduling, buffering, serialization and system clock error affect them. Sequences and hash
chains prove local ordering, not global exchange ordering or network completeness.

## Chunks and manifests

Chunks rotate at the library's `CHUNK_SECONDS` interval (`--chunk-seconds`),
without a reconnect. A session may have no chunks if nothing was stored. The
active chunk has suffix `.jsonl.gz.part`. The writer closes and fsyncs it, hashes its exact
compressed bytes, renames it to `.jsonl.gz`, then atomically publishes and fsyncs
the `.manifest.json` sidecar. Only a sealed chunk **with its manifest** is a
published pair. A missing manifest, `.part` or `.tmp` is incomplete evidence;
do not treat it as a sealed chunk. A crashed `.part` may lack a valid gzip tail.

Each manifest contains:

- `schema`: `<venue>.public_raw_chunk.v1`; `venue`, `instance`, `recording_id`,
  `filename`, `sha256`, `md5`, `size_bytes` (hashes/sizes of compressed bytes).
- `frame_count`, `first_frame_seq`, `last_frame_seq`, sorted
  `connection_generations`, `start_utc`, `end_utc` (first/last envelope collector
  receive/observation times, not network arrival or exchange times).
- `previous_chunk_sha256` (null for the first), `requires_prior_chunks`
  (whether a predecessor exists), `standalone=false`,
  `continuity="local_sequence_only; reconnects_and_network_gaps_possible"`,
  `dependencies="prior_chunk_chain_contains_earlier_generation_baselines"`,
  and `seal_reason` (`rotation` or the terminal reason).

Preserve the whole session chain: even a first chunk is not a claim of complete
baselines, and later chunks can depend on earlier snapshots. The chain lives in
memory, so removing an already archived predecessor does not break subsequent
seals. A new process starts a new recording ID, sequence and hash chain; order
session directory names chronologically. There is no cross-session chain.

## Session files and failure semantics

`PROVENANCE.json` is written once and contains `venue`, `instance`, `url`,
`instrument_count`, `instruments`, `market_list` (the input's optional `source`),
`kalshi_channels` (null for Polymarket), `end_utc` (requested deadline),
`started_utc`, `recording_id`, `pid`, `source_hashes` (input list and recorder
module path mapped to SHA-256, MD5 and byte size), `event_loop` (`asyncio` or
`uvloop <version>`), `gzip_level`, `lag_warn_ms`, and the disclaimer fields.
Only this object contains `raw_only=true`, `recording_v1=false`,
`reconstruction_pending=true`, `network_losslessness_proven=false`, and
`timestamp_semantics="collector_receive_not_network_arrival"` in new sessions.
Its `started_utc` and requested `end_utc` describe the collector session, not
exchange events.

`live_state.json` and `READY.json` contain the same state, atomically replaced
about every five seconds and at connection/final transitions, without heartbeat
fsync. They are observations, not durable commit markers. Keys are
`recording_id`, `venue`, `instance`, `session`, `updated_utc`, `connected`,
`ready`, `connection_generation`, `book_count_current_generation`,
`expected_book_count`, `subscribed_channels`, `frame_count`, `sealed_chunks`,
`disk`, `free_bytes`, `paused`, `dropped_frames_total`, `last_error`, the counters
and lag fields below. `updated_utc` is the collector's state-write time.
Readiness requires an initial book for **every requested instrument in the
current generation**, plus a connection.
At finalization `connected` and `ready` become false; prior readiness is reported
by `ready_generations`. Readiness does not validate a reconstructed book.

`FINAL.json` adds `reason`, `partial`, `final_chunk_sha256`, `closed_chunk_count`,
`active_partial_filename`, `end_utc` (collector's actual finalization time) and
`source_hashes`.
Reasons are `deadline`, `signal_or_control_storage_error`, `partial_<exception>`
or `partial_seal_<exception>`. Only `deadline` sets `partial=false`. A signal
remains partial even when its active chunk seals successfully. Fatal errors
retain `.part` where possible; a storage failure may prevent even FINAL from
being written. Existing sealed pairs remain evidence. An already-passed deadline
returns `end_already_passed` without creating a session.

Counters: `errors`, `reconnects`, `seq_gaps`, `gap_reconnects`,
`ready_generations`, `lag_warnings`, plus `dropped_frames_total`. Reconnects use
exponential backoff (1–30 seconds, ±20% jitter), reset on readiness or a stable
60-second connection. Receive silence triggers reconnection after 25 seconds
for Polymarket or 60 for Kalshi; control traffic extends liveness. Polymarket
sends an application ping every ten seconds. The receive coroutine yields every
128 buffered messages; a separate supervisor wakes every second. The supervisor
period is injectable for deterministic offline tests.

A Kalshi orderbook sequence jump on one subscription SID writes a local
`seq_gap` with `{"sid": ..., "expected": ..., "got": ...}`. It reconnects for
fresh snapshots at most once per `gap_reconnect_min_interval` seconds (library
default `GAP_RECONNECT_MIN_INTERVAL`, CLI `--gap-reconnect-min-interval`). Gaps
within that interval are recorded and logged only. The disk thresholds are
`low_free` (`LOW_FREE`, CLI `--low-free`) and `critical_free` (`CRITICAL_FREE`, CLI
`--critical-free`), both in bytes, with `0 <= critical_free <= low_free`.
Below the low threshold, disk state is `low`: warning only, recording continues.
Below the critical threshold it is `critical`: writes pause and count dropped
frames.
On recovery a local `disk_pause_gap` records `{"dropped_frames": N}`, then the
recorder reconnects for fresh baselines. This is an explicit loss interval.
Normal ticks flush to the page cache, fsync is offloaded about every ten seconds,
and sealing fsyncs synchronously. These are not power-loss guarantees.

## Sampled lag

At each supervisor wake, sample only the latest received TEXT frame since the
previous wake, using its wall-clock `time.time_ns()` observation. Extract the
first Polymarket `"timestamp":"<milliseconds>"` or Kalshi numeric
`"sending_ts_ms":<milliseconds>` with the existing lightweight regex. No JSON
decode is added to the hot path. Timestamp-free frames consume the sample slot;
silence produces no exchange-lag sample. Negative lag remains visible as clock
skew. These diagnostic regex samples do not validate venue timestamp semantics.

The state files and FINAL contain `exchange_lag_ms_last`,
`exchange_lag_ms_p50_60s`, `exchange_lag_ms_max_60s`, `loop_lag_ms_max_60s`, and
`lag_samples_60s`. Exchange lag is observed wall time minus the extracted venue
time. Loop lag is nonnegative actual minus intended supervisor wake, in ms.
Samples expire after 60 seconds; absent metrics are null and sample count is zero.
After at least ten exchange samples, a median strictly above `--lag-warn-ms`
(library default `LAG_WARN_MS`) emits a warning, at most once per minute. A
non-null median at or below the threshold emits one recovery; silence alone is
not recovery.
`lag_warnings` counts warnings. Minute summaries include the median/max exchange
lag and max loop lag.

## Logging

The module uses the standard `logging` logger named
`pmkt.streaming.raw_capture`. Event names are log messages; structured context
(instance, generation, counters, lag or error fields) is available as attributes
on each `LogRecord` through `extra`. Standard record timestamps describe log
emission. The CLI configures INFO logging to stderr when no handlers are already
configured; library callers control their own logging. Journal JSON formatting
belongs to downstream deployment tooling. The separate WebSocket client logger
remains capped at INFO to prevent DEBUG handshake logs from emitting signed
headers, and echoed header values remain redacted from transport errors.

## Contract ownership

These versioned JSON artifact schemas are not tabular registry schemas.
`data.registry` and `schema_lifecycle.json` cover registered table versions, so
no table registration, column migration or lifecycle catalog entry is added.
This document owns the exact artifact contract; the data dictionary and glossary
link here to distinguish it from normalized recording. Changes to retained
field names, ordering or semantics require a deliberate compatibility decision.

Deployment, upload/archive verification, market-list construction and credential
loading belong to downstream operator workflows. This command neither uploads
nor deletes recordings and does not make `recover-stream-run` or
`dataset validate-manifest` readers for these raw chunks.
