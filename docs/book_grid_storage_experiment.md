# Causal book-grid storage experiment

`scripts/benchmark_book_grid.py` compares storage alternatives against the same
recorded input using the current venue book adapters, canonical row builders,
tape producers and strict durable Parquet coordinator. It is an offline storage
component benchmark, not a new capture profile or a live capture acceptance test.
The production collectors and `full@3` behavior are unchanged.

## Draft scope and remaining implementation

This follow-up preserves the implemented offline harness, its regression tests,
and the recorded experiment results separately from merged PR #5. It is a
starting point for production storage work, not a completed capture feature.
The results below were measured on the recorded `de30b34` revision; moving this
harness onto current main does not remeasure those results.

Implemented in this draft:

- [x] Causal global-interval selection for topbook and full-depth projections.
- [x] Dense, full-evidence, tape, states-only and compressed-source comparisons.
- [x] Independent selection checks, strict storage validation, retained-role
  hashes, empty-frame metadata and tests for unsampled integrity failures.
- [x] Recorded measurements, reproduction instructions and CI test-lane coverage.

Remaining before a production candidate is ready for review:

- [ ] Define an opt-in sampled capture profile and manifest contract. Record the
  segment origin, interval policy, selected observation coordinates, empty
  frames, retained evidence and reconstruction limits. Existing profiles keep
  their current semantics.
- [ ] Add a capture-wide interval and per-instrument overrides for both venues.
  Support intervals of several seconds or longer as well as subsecond settings;
  250 ms is an experimental example. Consumers supply instrument choices and
  frequency policy; strategy and edge evaluation remain outside core. Start
  with fixed configuration per segment, rather than dynamic policy machinery.
- [ ] Integrate grid scheduling with timers, equal-time observations, reconnect
  segment boundaries and shutdown. Preserve source clocks and avoid future
  observations or invented final grid points. Every incoming update still
  participates in book state, initialization, integrity and recovery evidence.
- [ ] Design a crash-safe, lossless source journal with explicit publication and
  recovery boundaries. The experimental gzip file is not that journal. Preserve
  required control and non-book events; the current harness only archives
  selected instrument messages and omits collector-generated controls.
- [ ] Build and verify deferred canonical tape generation from that journal.
  Record the decoder/adapter version and settings needed for reproducible
  replay. Never thin dependent deltas while claiming full reconstructibility.
- [ ] Bound buffering and move expensive storage work out of the receive path
  where measurements justify it. Define overload, write-failure, cancellation
  and shutdown behavior explicitly, without suppressing validation errors.
- [ ] Test crash/restart behavior, disk failures, slow writers, reconnects,
  mixed instrument intervals, empty books and intermediate integrity failures.
  Verify exact replay for both venues and unchanged existing capture contracts.
- [ ] Repeat controlled benchmarks on the production candidate and a fresh
  10-minute dual-venue active-market probe. Record commit and selection, manifests,
  input rate, queue growth, lag, commit/journal latency, reconnect causes, CPU,
  memory and storage. Scale instrument count gradually; the 75-instrument offline
  experiment does not establish capacity for thousands of instruments.
- [ ] Update supported CLI documentation and run repository checks and contract
  validation before requesting review. Keep the PR draft until the production
  contract, implementation and acceptance evidence are complete.

Implementation may be split into smaller follow-ups if the source-journal and
sampled-profile contracts are easier to review independently. The checklist
records the intended outcome, not a requirement to merge a large redesign at once.

## Reproduce

Run from the repository root with the streaming dependencies installed:

```bash
python scripts/benchmark_book_grid.py \
  --venue pm \
  --input tmp/probe/pm-screen.jsonl \
  --selection tmp/probe/selection.json \
  --output-dir tmp/grid-pm-025 \
  --seconds 5 --interval 0.25 --retention all
```

Use `--venue kx` for Kalshi. Selection JSON contains `polymarket_token_ids` and
`kalshi_tickers`. Input JSONL has `message` and either `observed_at_utc` or
`received_at_utc`. Supply one uninterrupted segment with ordered observation
timestamps. Split input at reconnects; this harness does not simulate recovery
or carry state across connections. It rejects backward clock steps.

The input must extend through the requested `--seconds` window; otherwise omit
that option to end at the final recorded observation. The output directory must
not exist. Keep all generated artifacts in an ignored directory.

## Grid and timestamp semantics

* `--interval 0` emits every observed state. A positive interval specifies seconds
  between grid points. The harness defaults to `0.25` for comparison only;
  this is not a chosen production default or an upper bound on the interval.
* The origin defaults to the first input observation. `--origin-utc` supplies an
  explicit recorded segment start at or before that observation. Parquet commits
  never reset the grid. Benchmark execution speed does not affect its selection.
* Each book contributes its last observed state at or before a completed grid
  point. All updates with the same timestamp are consumed before that point is
  sealed. Later input is never applied to an earlier grid state.
* An interval with no book update produces no additional state. An update that
  returns the book to its earlier values still counts as an observation. Silent
  or uninitialized instruments are not fabricated into initialized books.
* A book is a Polymarket token or a Kalshi ticker; one Kalshi state yields both
  YES and NO topbook rows. "One state" can therefore occupy several physical
  rows, especially for full depth.
* Rows keep the selected observation's canonical clocks and source sequence.
  `sampling-index.jsonl` separately maps each book to its relative grid point,
  observation time and source sequence. Existing primary-key clock disambiguation
  still applies to dense rows with equal timestamps.
* Only completed grid points are emitted. A pending partial interval at shutdown
  is counted in the summary, rather than projected onto a future grid point.
* Every selected coordinate is checked against an independent bucket reference.
  Tests additionally check backward-as-of selection and persisted depth values.

## Retention comparisons

| `--retention` | What is persisted | Information available afterward |
| --- | --- | --- |
| `all` | Grid/dense topbook and full depth, canonical tape, parsed events, legacy snapshots/levels and raw JSONL | Full normalized tape and source messages for this input |
| `no-sidecar` | Same as `all`, without raw JSONL | Tape retained; parsed events still include raw payloads |
| `tape` | Grid/dense topbook and depth plus the complete canonical tape | Normalized book reconstruction retained; non-book source messages are omitted by this component experiment |
| `states` | Grid/dense topbook and depth only | Observed state at retained grid points; no event reconstruction between points |
| `raw-archive` | Grid/dense topbook and depth plus gzip level-1 source-message archive | Source messages can be normalized offline; no directly queryable canonical tape |

The `raw-archive` experiment does not filter book deltas from the archive. It
tests deferring normalization and using lossless compression, rather than
discarding information. Its archive is finalized and fsynced separately; it is
**not** a crash-safe replacement for the current cross-role commit protocol.
The decompressed content hash is comparable to `all`'s raw sidecar.

Never thin individual tape deltas and still claim the remaining tape is
reconstructible. In particular, Kalshi size changes are incremental. Keeping
only the last delta in a bucket does not produce the last state. Sampled depth
states also need an explicit frame index to represent empty books and replace
all prior levels; absent level rows alone do not distinguish deletion from an
absent observation. The experimental sampling index records such empty frames.

## Measurements and limits

`summary.json` records the input and selection hashes, harness hash, source
commit, interval/origin/window, retained row counts, bytes, CPU/wall time,
validation/write/publication stages, commit causes and logical row hashes.
Compare tape hashes across intervals to verify that projection sampling does
not alter the event history. Use repeated dense and 0.25-second runs to assess
ordinary timing variation.

Input loading and the reference selection check are outside the timed region.
Book application, projection generation, tape generation, raw serialization,
commit validation, Parquet writing/readback and finalization are inside it.
Coordinator row/time thresholds and checkpoint coalescing remain enabled;
validation exceptions propagate and fail the experiment.

This component harness omits transport, supervisors, health/eligibility evidence,
trade/lifecycle canonical projections and scheduled collector controls. Its
outputs have an experimental run-state name and do not claim `full@3`, complete
capture coverage, or live timeliness acceptance. Native invalidations produced
by the retained tape are still written and validated. A transient invalid state
may fall entirely between grid points; only retained event evidence can reveal
that transition afterward.

A production candidate would require a declared sampled profile/manifest
contract, a recorded segment origin, timer and shutdown integration, an empty
book frame index and explicit replay limitations. Sampling must not change the
state, subscription or integrity evidence used by recovery. A live test must
then check queue growth, receive/source lag and reconnect causes, not just row
counts or successful finalization.

## PR #5 experiment on 2026-09-13

The unchanged `de30b34298d632c2c0e02d8b16f0866a6a66bd6d` core was benchmarked on
`erik-pc1` using the fresh stress selection: 50 Polymarket tokens and 25 Kalshi
tickers. All initialized in the first five recorded seconds. Twenty-two serial
matrix runs covered the retention choices and 0.1/0.25/0.5/1-second intervals;
dense and 0.25-second all-evidence runs were repeated. Artifacts are under
`/home/erike/pr5-grid-storage-final-de30b34/`.

For the identical five-second input windows:

| Storage variant | Polymarket elapsed | Kalshi elapsed |
| --- | ---: | ---: |
| Dense, all evidence | 39.87 s | 7.40 s |
| 0.25-second grid, all evidence | 15.61 s | 2.60 s |
| 0.25-second grid, tape without legacy/raw copies | 13.51 s | 2.18 s |
| 0.25-second grid, states only | 2.50 s | 0.69 s |
| 0.25-second grid, compressed raw archive | 2.53 s | 0.71 s |

The first two rows are means of two runs; other rows are single runs. PM depth
rows fell from 191,222 to 18,532 (90.3%); Kalshi from 17,489 to 1,989 (88.6%).
Removing raw JSONL alone barely affected runtime; PM still needed 15.42 seconds.
At a one-second grid with all evidence it still needed 14.18 seconds.

Four longer runs compared tape and compressed raw retention over the entire
recorded activity windows (39.93 seconds PM, 39.88 seconds Kalshi):

| 0.25-second grid | Polymarket elapsed | Kalshi elapsed |
| --- | ---: | ---: |
| Complete canonical tape retained | 102.63 s | 11.66 s |
| Lossless raw archive, canonical tape deferred | 16.95 s | 3.02 s |

All runs passed strict commit validation and the causal selection reference.
The archive regenerated the exact logical canonical tape: 40,542 events,
55,902 levels and 350 controls for PM; 4,681 events, 5,288 levels and 25 controls
for Kalshi. Projected rows were identical across those retention choices.

PM had 28 invalid intermediate state observations in this longer window; none
landed on a sampled grid point. They remain recoverable from the retained
event history. This demonstrates why sampled-state validity cannot replace
continuous evidence. Also, fewer commits do not guarantee shorter stalls:
the PM archive variant still had a maximum acceptance-to-journal latency of
5.74 seconds for a sampled-state commit, excluding pre-acceptance validation.

The measured direction is an opt-in sampled-state contract plus a durable,
lossless source journal, with canonical tape production and validation outside
the reader's critical path. Neither dropping arbitrary deltas nor suppressing
validation is justified by these results. Crash recovery and a fresh 10-minute
live test are required before changing production capture behavior.
