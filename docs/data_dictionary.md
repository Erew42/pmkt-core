# Canonical Data Dictionary

## Account reservation v2

`runtime_account_reservations` binds each reservation to an authoritative
`account_id`, venue, and caller idempotency key. `reservation_id` includes the
account scope. Caller metadata is nested and may not reuse authoritative field
names. `runtime_account_reservation_aggregates` is updated in the same database
transaction at account, account/venue, account/event, and account/match grains.

## Run and restart authority v2

`runtime_live_run_account_locks_v2` is the database-unique account-scoped
live-run authority; `runtime_live_run_claims_v2` retains claim history.
`runtime_unmatched_exposure_legs_v2` retains per-run legs so risk totals span
all persisted runs. `runtime_restart_evidence_v2` binds a complete,
unblocked `run_manifest.v2` to exact artifact IDs, SHA-256 hashes, and explicit
UTC timestamps. Maker-rebate credit is zero until separate authority exists.

This document defines the first stable normalized schemas for prediction-market
research in `pmkt`. The raw exchange clients may expose venue-specific payloads,
but durable research datasets should move toward these canonical rows.

Schema versions, column constants, and typed schema objects live in
`src/pmkt/data/registry.py`, and dataframe validation/coercion helpers live in
`src/pmkt/data/validation.py`. `src/pmkt/data/canonical.py` and
`src/pmkt/data/schemas.py` provide row builders and registry-backed public
import paths.

Freshness pass: 2026-06-02.

## Design principles

- Preserve raw payloads or raw payload hashes for auditability.
- Use venue-neutral names for normalized columns.
- Keep venue identifiers in their original string form.
- Store prices in dollars/probability units from 0.0 to 1.0 when possible.
- Store UTC timestamps as ISO-8601 strings with timezone information.
- Separate markets/contracts from tradable instruments/outcomes.
- Treat cross-venue matches and arbitrage candidates as review artifacts, not
  facts or executable trade instructions.

## Canonical timestamp-ingestion policy

Decision recorded 2026-08-21 and implemented for the Part 3 replay,
convergence, signal, matching, scan-planning, and tracking consumers on
2026-08-22. Other source-specific date handling is not implicitly canonical.

- Canonical textual timestamps must include an explicit UTC offset (`Z` or a
  numeric offset). `+HH`, `+HHMM`, and `+HH:MM` spellings are accepted for
  retained-data compatibility, and accepted values are normalized to UTC.
- Timezone-naive text is rejected in both scalar and vector paths. Vector
  parsing must not silently localize naive values as UTC.
- Numeric epochs are accepted only by a caller whose input contract explicitly
  declares the epoch unit or the existing seconds/milliseconds heuristic. They
  are not a universal canonical timestamp format.
- Empty, malformed, out-of-range, and unsupported values fail according to the
  owning ingestion contract rather than being replaced with the current time.
- Raw source values or raw evidence hashes remain available when normalization
  is used for a persisted research result.

`pmkt.data.time.parse_utc_timestamp()` and
`parse_utc_timestamp_series()` now share the same explicit-offset policy.
Authoritative Part 3 consumers use the vector parser's fail-closed `raise`
mode, whose diagnostic identifies the field, row index, and value. The
`coerce` mode remains available for an explicitly best-effort caller. Raw venue
adapters use `isoformat_source_timestamp()` with seconds, milliseconds, or a
documented compatibility heuristic instead of routing epochs through the
canonical parser.

## Permanent market catalog

The operational catalog at `data/markets` reuses the physical
`polymarket_market_snapshot.v1` and `kalshi_market_snapshot.v1` row contracts.
It deliberately adds no venue-row schema. Creation discovery, current censuses,
weekly immutable-history promotion, and periodic latest-row compaction are
implemented in `pmkt.data.market_catalog` and operated manually through
`pmkt markets ...`.

`pmkt.market_discovery_manifest.v1` and
`pmkt.market_discovery_pointer.v1` are JSON-only internal lineage contracts;
they are not table schemas and do not belong in the schema registry. Discovery
manifests record request parameters, cursor stop evidence, watermarks, counts,
artifact hashes, and an exact predecessor hash. `DISCOVERY_LATEST.json` has one
independent head per discovery stream. A DuckDB known-key index is replaceable
cache only and rebuilds whenever its bound pointer hashes change.

New history partitions carry immutable acquisition provenance in their path:
`native_family=polymarket`, `native_family=kalshi_conventional`, or
`native_family=kalshi_mve`. Discovery deltas add a unique immutable
`promotion_id=<history_release_id>` level between `source=discovery_delta` and
`native_family`; Kalshi retains its stable bucket below that level. Repeated
promotions therefore never reuse a parent delta path. `register_catalog_views()` injects that value,
`family_provenance`, `operational_family`, and
`family_classifier_version=operational_family.v1` at read time. Legacy Kalshi
rows use the narrow `KXMVE` ticker compatibility rule; the catalog does not call
the matching-oriented `infer_contract_family()` heuristic. Polymarket derived
families recognize only the high-confidence 5-minute, 15-minute, and 4-hour
`updown`/`up-or-down` slug forms.

Catalog classifiers share one set of Python constants and DuckDB expression
builders. For Kalshi, an explicit `native_family=` partition wins; otherwise a
null or blank key is `family_unknown`, a trimmed case-insensitive `KXMVE*` key
is `kalshi_mve`, and every other legacy key is `kalshi_conventional`. The Python
helpers are the behavioral oracle for SQL parity tests. This catalog-only rule
does not change matching-family inference.

History freshness has two clocks. `coverage_complete_through_utc` describes
when newly created market keys have been promoted. `metadata_compacted_through_utc`
describes when known-row status and metadata upserts were folded into the latest
base. API absence removes a key from the current view only; it never deletes or
terminally rewrites historical evidence.

Every history read performs metadata integrity validation: the pointer's
manifest hash, pointer/manifest artifact agreement, artifact existence, Parquet
row and file counts, total byte size, and base-plus-delta row accounting when
promotion fields are present. Mutation gates add full content-hash validation;
`pmkt markets status --deep` exposes the same on-demand scan. Promotion verifies
the parent before staging and again before publication, while compaction verifies
it after the due decision and before writing a new base. Validation failures do
not change the history pointer.

## Frame conversion boundaries

`pmkt.data.validation.coerce_frame()` remains the explicitly best-effort
compatibility cleaner. It may discard extra columns and convert incompatible
values to nullable missing data, so it is not a lossless ingestion or migration
boundary.

New authoritative writers can opt into
`pmkt.data.validation.convert_frame_strict()`. The strict converter adds only a
missing target `schema_version` and absent nullable columns. It rejects extra
columns, explicit version mismatches, duplicate primary keys, incompatible
values, malformed JSON in fields registered as `json`, and schema invariant
failures before conversion. Valid JSON strings are preserved exactly rather
than reserialized.

`market_taxonomy_evidence_frame()` is the Part 3 pilot. In addition to the
registry contract, it validates explicit UTC request/observation timestamps,
lowercase SHA-256 provenance, and the JSON array/object shapes of its
large-string metadata fields. The retained spelling `"null"` remains accepted
for the optional `structured_sport_json` object; it unambiguously represents
missing metadata and was present throughout existing releases.

Generic `validate_frame()` deliberately retains its historical behavior of
accepting arbitrary strings for registry `json` fields. Tightening that shared
compatibility surface is deferred: a proposed exhaustive 105,781-file
retained-data scan did not complete within its bounded pass because large
`raw_json` payload families dominate the read. The new strict converter does
not inherit that permissiveness.

## Registered schema coverage

`src/pmkt/data/registry.py` is the source truth for registered schema versions,
column order, required fields, primary keys, and validation metadata. This table
is a human-readable coverage index; detailed column descriptions live in the
sections below or in the registry descriptions.

| Schema version | Registry name | Summary |
| --- | --- | --- |
| `arbitrage_candidate.v1` | `arbitrage_candidate` | Manual-review apparent-edge candidate. |
| `backtest_report.v1` | `backtest_report` | Unified return-centric backtest report for taker and maker replay paths. |
| `basket_order_intent.v1` | `basket_order_intent` | Paper-only basket leg order intent. |
| `basket_paper_fill.v1` | `basket_paper_fill` | Paper-only basket leg fill event. |
| `basket_paper_position.v1` | `basket_paper_position` | Paper-only basket position and reconciliation summary. |
| `canary_candidate.v1` | `canary_candidate` | Basket-aware pre-trade canary candidate with formula proof and gates. |
| `canary_rejection.v1` | `canary_rejection` | Rejected or observe-only canary formula with explicit reason. |
| `co_resolution_observation.v1` | `co_resolution_observation` | Offline co-resolution observations built from match candidates and authoritative resolution-cache labels. |
| `co_resolution_score.v1` | `co_resolution_score` | Research-only Bayesian co-resolution score sidecar for match candidates. |
| `convergence_observation.v1` | `convergence_observation` | Decision-time opportunity markout and convergence/divergence observation. |
| `convergence_summary.v1` | `convergence_summary` | Per-match convergence/divergence aggregates. |
| `contract_evidence.v1` | `contract_evidence` | Venue contract-semantics evidence with endpoint provenance, distinct hashes, instrument mapping, and explicit completeness. |
| `depth.v1` | `depth` | Canonical order-book depth levels. |
| `event.v1` | `event` | Venue event metadata. |
| `execution_sizing_plan.v1` | `execution_sizing_plan` | Versioned execution sizing decision with plan-derived exposure evidence. |
| `feed_health.v1` | `feed_health` | Shard-level websocket feed supervision and instrument coverage state. |
| `capture_instrument_evidence.v1` | `capture_instrument_evidence` | Per-instrument, per-subscription-attempt eligibility and initial valid-book evidence for profile-v2 captures. |
| `book_tape_event.v1` | `book_tape_event` | Logical commit header for one venue-native book checkpoint or delta. |
| `book_tape_level.v1` | `book_tape_level` | Absolute post-event venue-native price-level mutation. |
| `book_tape_control.v1` | `book_tape_control` | Book validity, epoch, and stream-terminal transition. |
| `stream_lifecycle.v1` | `stream_lifecycle` | Source market-lifecycle observation from a stream run. |
| `historical_backfill_gap.v1` | `historical_backfill_gap` | Historical backfill unsupported capability, fetch error, or data-quality gap. |
| `historical_price.v1` | `historical_price` | Historical price/candle context, not executable topbook/depth evidence. |
| `instrument.v1` | `instrument` | Tradable outcome instrument metadata. |
| `kalshi_market_snapshot.v1` | `kalshi_market_snapshot` | Legacy Kalshi market snapshot export used by matchers and candidate discovery. |
| `kalshi_market_snapshot.v2` | `kalshi_market_snapshot_v2` | Trimmed Kalshi market snapshot export without legacy settlement metadata. |
| `maker_quote_plan.v1` | `maker_quote_plan` | Offline maker quote plan with post-only guardrails and fill-conditional hedge EV. |
| `market.v1` | `market` | Venue market or contract metadata. |
| `market_match.v1` | `market_match_v1` | Legacy cross-platform market/instrument equivalence record. |
| `market_match.v2` | `market_match` | Cross-platform market/instrument equivalence record. |
| `market_resolution.v1` | `market_resolution` | Resolved market outcome cache produced by reusable Kalshi and Polymarket resolvers. |
| `market_taxonomy_evidence.v1` | `market_taxonomy_evidence` | Additive source-native category, tag, series, and structured-sports evidence keyed by venue market. |
| `event_taxonomy_prediction.v1` | `event_taxonomy_prediction` | Event-grain hybrid taxonomy prediction sidecar with abstention and model/evidence provenance. |
| `match_relation.v1` | `match_relation` | Source-level relation between cross-platform markets for trading and tracking gates. |
| `order_intent.v1` | `order_intent` | Pre-trade order proposal before venue submission. |
| `order_state.v1` | `order_state` | Reconciled order and fill state. |
| `paper_fill.v1` | `paper_fill` | Paper simulator fill event. |
| `paper_position.v1` | `paper_position` | Paper execution position and PnL summary by signal. |
| `passive_quote_evaluation.v1` | `passive_quote_evaluation` | Local passive quote replay evaluation row; no real order placement. |
| `passive_fill.v1` | `passive_fill` | Hypothetical passive replay fill row; no exchange fill evidence. |
| `passive_markout.v1` | `passive_markout` | Post-fill passive replay markout row for local analysis. |
| `polymarket_market_snapshot.v1` | `polymarket_market_snapshot` | Legacy Polymarket Gamma market snapshot export used by matchers and stream selectors. |
| `polymarket_market_snapshot.v2` | `polymarket_market_snapshot_v2` | Trimmed Polymarket Gamma market snapshot export without legacy resolution metadata. |
| `run_manifest.v1` | `run_manifest` | Auditable run metadata. |
| `scan_cycle.v1` | `scan_cycle` | One live/paper canary scan cycle summary. |
| `signal.v1` | `signal` | Price-aware cross-venue signal state before order intent. |
| `soak_run_plan.v1` | `soak_run_plan` | Read-only long-duration soak evidence plan; does not start runtime processes. |
| `soak_run_report.v1` | `soak_run_report` | Summary of recorded no-order soak evidence from runtime store and artifacts. |
| `topbook.v1` | `topbook` | Canonical top-of-book snapshot. |
| `topbook_capture_gap.v1` | `topbook_capture_gap` | Continuous topbook recorder fetch and coverage gaps. |
| `tracking_health.v1` | `tracking_health` | Match-level websocket tracking coverage, freshness, and book validity. |
| `tracking_match.v1` | `tracking_match` | Candidate cross-venue match rows intended for websocket tracking. |
| `trade.v1` | `trade` | Normalized trade observation. |
| `venue_history_capability.v1` | `venue_history_capability` | Documented venue historical-data capability and limitation matrix. |

## Contract evidence schema: `contract_evidence.v1`

`contract_evidence.v1` is an additive sidecar at venue-market/source-observation
grain. It preserves the endpoint projection used to interpret a contract while
keeping raw-payload, normalized source-row, and semantic-projection hashes
separate. It is evidence for matching and review policy, not proof that a pair is
tradable or executable.

| Column | Meaning |
| --- | --- |
| `evidence_id` | Deterministic observation identity over venue, market, endpoint/scope, explicit observation time, and canonical raw hash (or normalized source-row hash when raw data is unavailable). |
| `venue` / `market_key` / `venue_event_key` | Venue identity carried by the observed source payload. |
| `source_endpoint` / `payload_scope` | Endpoint and projection type, such as list, keyset, detail, snapshot, or fixture. |
| `observed_at_utc` / `derived_at_utc` | Required explicit UTC venue-observation and projection-generation times. Observation time comes from the function argument first, then the source row; it is never inferred from derivation time. |
| `source_row_hash` | Stable normalized-row hash excluding raw payload fields and transient/internal columns. |
| `raw_payload_hash` | SHA-256 of the exact decoded venue payload, canonically serialized when raw source data is available. |
| `evidence_projection_hash` | Stable hash of versioned semantic evidence, excluding endpoint, timestamps, and raw-payload noise. |
| `rules_text` | Canonical resolution text: Polymarket `description`, or Kalshi primary then secondary rules. |
| `instrument_mapping_json` | Exact venue instrument/outcome mapping. |
| `contract_fields_json` / `field_provenance_json` | Canonical contract fields and the venue field paths that supplied them. |
| `identity_complete` / `rules_complete` / `instrument_mapping_complete` | Independent fail-closed completeness capabilities. |
| `completeness_reasons_json` | Machine-readable reasons for incomplete evidence. |

List payloads are never assumed to be detail-complete, and projection performs
no hidden venue requests. Missing source information remains missing. Arbitrary
decoded mappings are review-only unless the caller explicitly identifies an
approved `source_payload_kind`; stored `raw_json` envelopes validate their own
hash and are authoritative raw inputs. Multiple plausible Polymarket events are
ambiguous unless a stable event identifier selects exactly one.

New CLI captures publish one atomic `PATH.bundle/` containing the evidence
parquet, `source_collection_manifest.json`, and
`contract_evidence_manifest.v2`. The v2 manifest records collection
completeness, stop reason, continuation cursor, collection errors, source
manifest hash, aggregate payload hash, artifact hash, row count, observation
range, and evidence-ID-set hash. It is authoritative-complete only when the
collection is complete and error-free, the source manifest verifies, and the
payload kind is an approved raw envelope. `contract_evidence_manifest.v1`
remains readable for compatibility but is always ineligible for new
authoritative-completeness decisions.

## Market taxonomy evidence schema: `market_taxonomy_evidence.v1`

`market_taxonomy_evidence.v1` is an additive sidecar keyed by venue and market.
It preserves source-native event/category/tag/series and structured sports
metadata without expanding or silently reinterpreting either venue's market
snapshot schema. It records evidence only: it does not assign a repository
domain, establish contract equivalence, or authorize execution.

| Column | Meaning |
| --- | --- |
| `venue` / `market_key` / `event_key` | Stable venue market and owning-event identity. |
| `native_category` / `native_tags_json` | Native venue category and tag labels exactly as projected from source metadata. |
| `native_series_json` / `series_ticker` | Native series identities; the ticker is populated when the venue exposes one. |
| `structured_sport_json` / `game_id` / `sports_market_type` | Source-native sports object and market/event identifiers, when exposed. |
| `requested_at_utc` / `observed_at_utc` | Request start and successful response receipt time for the source metadata. |
| `source_endpoint` / `source_payload_sha256` | Exact endpoint and hash of the decoded source payload supplying the metadata. |
| `snapshot_raw_json_sha256` | Optional hash binding to the market snapshot row used in the join. |
| `issues_json` | Explicit missing, changed, or join-quality issues; missing evidence is never replaced silently. |

## Event taxonomy prediction schema: `event_taxonomy_prediction.v1`

`event_taxonomy_prediction.v1` is an event-grain, model-derived sidecar keyed by
venue and event. It is optional input to scan planning in `hybrid_v1` mode;
legacy taxonomy remains the default. Candidate generation never loads or trains
the model. Accepted predictions can reject a pair only when both venue events
have accepted, different primary domains. Abstentions remain
`taxonomy_uncertain`. An evidence-complete Kalshi `abstain_low_confidence` row
with model margin at least `0.20` is searched against its top four model domains;
lower-margin, incomplete, conflicting, missing, and Polymarket abstentions retain
all eight domains. This routing changes search effort, not prediction acceptance
or contract-equivalence authority.

| Column | Meaning |
| --- | --- |
| `venue` / `event_key` | Stable venue event identity. |
| `primary_domain` | Operational domain when accepted; null for every abstention. |
| `model_primary_domain` | Highest-scoring model class, retained even when the prediction abstains. |
| `domain_confidence` / `domain_margin` / `domain_scores_json` | Calibrated ranking evidence used by the frozen acceptance rule. |
| `prediction_status` | `accepted_model`, `accepted_structural`, or an explicit abstention reason. `accepted_structural` sports requires source-structured sports evidence or Kalshi-only agreement among the sole native domain, deterministic domain, and model top class. |
| `family_shadow` / `family_confidence` | Diagnostic event-family output; never a candidate-generation gate. |
| `model_sha256` / `evidence_sha256` | Hashes binding the prediction to the local model bundle and event evidence. |
| `classified_at_utc` | UTC inference timestamp. |
| `issues_json` | Explicit novelty, evidence, or conflict issues. |

Hybrid candidate artifacts retain the routing decision in
`kalshi_taxonomy_routing_policy`,
`kalshi_taxonomy_routing_domains_json`, and
`kalshi_taxonomy_routing_margin`. Run manifests also pin the routing policy,
threshold, top-k, and policy counts. Older immutable checkpoints without those
columns resume under the explicit `legacy_all_domains_checkpoint` policy.

### Authoritative relation binding

`relation_rules.v2` binds relation evidence in this order: resolved top-level
identity, the newest qualifying complete-v2 contract-evidence sidecar, a direct
source market row for diagnostics, then candidate data for fields still
missing. Direct source rows are labeled `source_market_row_diagnostic`; only
fields labeled `contract_evidence_sidecar` can authorize completeness or
settlement classification. Sidecars require an `EvidenceAuthorityPolicy` with
an explicit as-of time, caller-supplied maximum observation age and maximum
derivation lag, and pinned companion-manifest hashes. Both `observed_at_utc`
and `derived_at_utc` must be at or before the as-of time. The age and lag
boundaries are inclusive, and there is no repository-wide implicit freshness
age.

Candidate fallbacks are labeled `candidate_non_authoritative` and cannot
satisfy required rules or instrument completeness. Candidate-only and direct
source hashes remain visible for compatibility, but `hash_authority` labels
them non-authoritative or diagnostic. Observation history is kept per market
and alias; selection orders by observation time, derivation time, and evidence
ID. New authoritative decisions reject v1 or incomplete manifests. Stable
authority diagnostics include `future_evidence`, `stale_evidence`,
`incomplete_collection`, `ambiguous_evidence`, `duplicate_source_identity`,
and `source_clock_invalid`. Unpinned or mismatched manifests, incomplete
outcome identity, mapping disagreement, and conflicting authoritative fields
also fail closed.

The binding records the selected evidence ID, observation and derivation times,
age, derivation lag, pinned manifest hash, source-manifest hash, aggregate
payload-hash-set hash, artifact hash, trust state, and freshness status.
Historic `matching_policy` output is discarded. Mandatory settlement scope is
derived only from trusted sidecar contract/market-family fields and recomputed
from trusted rules; caller scope hints remain diagnostic and cannot create,
disable, or satisfy the requirement. Exact instrument equivalence still
requires resolved market, instrument, and outcome.

### Sports audit artifacts (generated, unregistered)

`sports_review_label.v2` is a human-reviewed CSV at exact cross-venue
instrument-pair grain. Its unique key is Polymarket market/instrument plus
Kalshi market/instrument after canonical Unicode/whitespace normalization and
Kalshi casing; non-canonical and normalized-duplicate keys are invalid. Outcome fields are attributes that must agree with
the authoritative venue mappings. It records a blinded assignment ID, one
freeze ID, dev/holdout designation, expected relation,
an explicit `hard_negative` boolean and canonical `hard_negative_reason`
taxonomy token, both reviewed selections, one supported sports shape,
structured event-identity evidence, match date, two distinct reviewers with UTC
timestamps, disagreement state, independent adjudication when needed, and
notes. A
trade-equivalent positive cannot be a hard negative; a hard negative requires a
reason, while an ordinary negative does not satisfy the gate's hard-negative
composition requirement. Legacy `sports_review_label.v1` files remain readable
for comparison, but are explicitly review-only and cannot establish
operational acceptance.

`sports_pair_audit.v1` is a generated local review artifact, not a registered
execution schema. One row reconciles an input pair and reviewed label. Unmatched
inputs and unmatched reviewed labels remain visible as machine-explained
abstentions rather than disappearing from gate counts.

| Field group | Meaning |
| --- | --- |
| Pair/source identity | Exact venue market, instrument, outcome, source match id, and preserved source relation label. |
| Review truth | Freeze/split, expected relation, reviewed selections/shape, structured event identity, two reviews, disagreement, and adjudication. |
| Authoritative binding | Binding status/issues, evidence ids, projection hashes, and whether the row is evaluable. |
| Parser proposal | Parsed shapes, selections, match keys, settlement scopes, proposed relation/type, and reasons. |
| Evaluation | Relation, shape, and orientation correctness; reviewed-negative and false-trade-equivalent flags. |
| Safety | `artifact_scope=local_exploratory`, `review_only=true`, `execution_ready=false`, and `order_routing_capable=false`. |

Each immutable atomic run directory writes four audit artifacts:

- `sports_pair_audit.parquet`
- `summary.json`
- `false_trade_equivalents.csv`
- `abstentions.csv`

The companion `sports_audit_manifest.json` inventories those four files with
SHA-256 hashes and byte sizes. Run identity covers input hashes, audit-quality config,
audit/parser/relation versions, and generation time. Existing run IDs are never
overwritten.

`summary.json` records exact input-file hashes, sorted evidence-projection
hashes, `sports_contract_parser.v1`, `relation_rules.v2`, audit version,
generation time, complete Balanced-50 thresholds, class composition, confusion
counts, per-shape metrics, Wilson intervals, and machine-readable audit-quality
reasons. The immutable, version-bound quality profile measures 50 evaluable
labels, 20 positives,
25 explicitly reviewed hard negatives,
two dates, five required shapes, overall and per-shape parser coverage,
relation/shape/orientation accuracy and lower bounds, trade precision/recall and
recall lower bound, zero false trade equivalents, a bounded false-positive-rate
upper bound, and explained abstentions. Conservative parser abstention remains
in relation and shape denominators; orientation is separately reported only for
parsed rows. These metrics do not evaluate or claim operational acceptance.

These artifacts cannot promote or modify `match_relation.v1`, cannot produce
signals or order intents, and are not execution-ready. Canonical sports event
identity and typed upstream candidates remain deferred.

## Venue and exchange values

### Artifact bundle and pagination health v2

`artifact_bundle_manifest.v2` is complete only when an atomic publication has
finished and every normalized relative path, byte size, and SHA-256 is present.
Absolute paths, raw empty/dot/traversal segments, backslashes, duplicate
normalized paths, unmanifested files, interrupted directories, incomplete or
blocked health, incomplete manifests, and hash mismatches fail closed.

`pagination_health.v2` persists page/item counts, stop reason, continuation
cursor, collection errors, and blocking flags. Cursor cycles and page-limit
exhaustion are blocking incomplete states and never disappear with partial rows.

These v2 contracts are foundation-only in CR-17.1. No pre-existing collector,
CLI workflow, or dashboard becomes authoritative merely because the shared
helpers exist; each consumer must explicitly adopt and expose the persisted
health and verified bundle in a separately reviewed change.

### Market-data health v2

`market_data_health.v2` records connection state, snapshot requirement, current
sequence, bounded recent history, and blocking flags. Books normalize bids
descending and asks ascending. Sequence gaps, crossed books, future source
clocks, stale source clocks, and deltas received before a reconnect snapshot
fail closed.

CR-17.2 defines this as a foundation-only in-memory contract. Existing feed
adapters do not yet persist this block, and no current CLI or dashboard may
treat it as execution authority until an owning consumer integration is
implemented and independently reviewed.

### Submission state v2

`submission_state.v2` binds one idempotency key to one payload hash and records
typed accepted, rejected, retryable-failure, or unknown outcomes. Unknown state
blocks retry until a complete authoritative lookup proves zero matching orders
or binds exactly one existing venue order. Incomplete or multiple-order recovery
evidence remains blocked. Recovery completeness must be the literal boolean true,
its observation cannot predate the current attempt, and any venue order ID
attached to a non-accepted outcome fails closed as unknown.

The snapshot includes normalized recovery evidence and resulting state, but
CR-17.3 remains a foundation-only in-memory contract. No runtime hydration,
venue adapter, CLI, or dashboard may use it as durable submission authority;
reported venue-order evidence must reconcile exactly before unknown state can
clear.

Supported canonical `venue` values are:

- `polymarket`
- `kalshi`

Quote schemas use `exchange` instead of `venue`; supported canonical
`exchange` values are the same two strings.

## Event schema: `event.v1`

A venue event groups one or more markets/contracts.

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `event.v1`. |
| `venue` | Venue name. |
| `venue_event_id` | Native event id/ticker/slug. |
| `event_ticker` | Venue event ticker when available, especially Kalshi. |
| `series_ticker` | Venue series ticker when available. |
| `title` | Event title. |
| `subtitle` | Event subtitle. |
| `category` | Venue or inferred broad category. In scan planning this is a recall-routing category, not by itself a safe Cartesian matching bucket. |
| `status` | Native status string. |
| `open_time_utc` | Event open time in UTC. |
| `close_time_utc` | Event close time in UTC. |
| `settlement_time_utc` | Settlement/resolution time in UTC. |
| `raw_json` | Optional raw payload JSON. |
| `raw_json_sha256` | SHA-256 of raw payload when raw JSON is stored elsewhere. |

## Market schema: `market.v1`

A market is a venue contract with resolution semantics. It may have one or more
tradable instruments/outcomes.

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `market.v1`. |
| `venue` | Venue name. |
| `venue_market_id` | Native market id, ticker, condition id, or slug. |
| `venue_event_id` | Parent event id when known. |
| `market_ticker` | Native market ticker when available. |
| `condition_id` | Polymarket condition id when available. |
| `slug` | Human-readable market slug. |
| `question` | Market question or title. |
| `description` | Venue description. |
| `rules` | Resolution rules text. |
| `resolution_source` | Official resolution source if known. |
| `category` | Venue or inferred broad category. In scan planning this is a recall-routing category, not by itself a safe Cartesian matching bucket. |
| `status` | Native status string. |
| `closed` | Boolean closed flag. |
| `open_time_utc` | Market open time in UTC. |
| `close_time_utc` | Market close time in UTC. |
| `settlement_time_utc` | Settlement/resolution time in UTC. |
| `contract_type` | Parsed contract type, e.g. `general_election`, `nomination`, `endorsement`, `pardon`, `run_or_participate`, `threshold_ladder`, `range_ladder`, `sports_prop`, `parlay`, or `other_binary`. |
| `market_family` | Direct, parlay/MVE, ladder, range, multi-outcome, or other family. |
| `outcome_type` | Binary, categorical, scalar/range, parlay, etc. |
| `time_cutoff` | Parsed cutoff phrase/date when known. |
| `inclusivity` | Whether boundaries are inclusive/exclusive when known. |
| `volume_dollars` | Venue volume converted to dollars. |
| `liquidity_dollars` | Venue liquidity converted to dollars. |
| `open_interest_dollars` | Venue open interest converted to dollars when applicable. |
| `raw_json` | Optional raw payload JSON. |
| `raw_json_sha256` | SHA-256 of raw payload when raw JSON is stored elsewhere. |

## Instrument schema: `instrument.v1`

An instrument is a tradable outcome side within a market.

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `instrument.v1`. |
| `venue` | Venue name. |
| `venue_market_id` | Parent market id. |
| `instrument_id` | Stable tradable id, e.g. Polymarket token id or `KX...:YES`. |
| `outcome` | Outcome label such as YES or NO. |
| `side` | Venue side if distinct from outcome. |
| `token_id` | Polymarket CLOB token id when available. |
| `ticker` | Kalshi ticker or side ticker when available. |
| `min_order_size_contracts` | Minimum order size. |
| `tick_size_dollars` | Price tick size. |
| `active` | Whether instrument is active/tradable. |
| `raw_json` | Optional raw payload JSON. |
| `raw_json_sha256` | SHA-256 of raw payload when raw JSON is stored elsewhere. |

## Quote schemas

Top-of-book and depth quote schemas are owned by the registry and exposed in
code as:

- `topbook.v1`: `pmkt.data.registry.TOPBOOK_COLUMNS`
- `depth.v1`: `pmkt.data.registry.DEPTH_COLUMNS`

`pmkt.data.schemas` re-exports those constants alongside `topbook_row()` and
`depth_row()`.

These should be used for REST polling and websocket-derived quote datasets.
Current websocket streamers keep raw JSONL and legacy parquet outputs, and also
write canonical `topbook_v1.parquet`, `depth_v1.parquet`, and a
`run_manifest.v1` manifest.

`topbook.v1` rows represent the best executable bid/ask view for one instrument
at one receive time. `depth.v1` rows represent full-state book depth snapshots,
not incremental deltas; the streamers emit them only for book-mutating messages
such as Polymarket `book`/`price_change` and Kalshi `orderbook_snapshot`/
`orderbook_delta`. For these canonical depth snapshots, `is_delta` is currently
`False`.

Within a capture run, the persisted tracker emission is the sole authority for
`received_at_utc`. The `topbook.v1` primary key remains `(exchange,
instrument_id, received_at_utc)`: when a changed main-row timestamp collides or
does not increase, the tracker advances the persisted value by whole
microseconds until it is unique. A checkpoint restatement at a duplicate or
non-increasing timestamp is suppressed instead. Consumers must use the emitted
row and its causal coordinates; they must not reconstruct the pre-adjustment
wall clock. This is a v1 compatibility rule, not evidence that the source
message arrived at the adjusted instant.

`topbook.v1` columns:

```text
schema_version, collector_run_id, exchange, venue_market_id, instrument_id,
outcome, source, received_at_utc, received_at_monotonic_ns, exchange_ts_utc,
local_sequence, venue_sequence, venue_sid, book_hash, best_bid_dollars,
best_ask_dollars, mid_dollars, spread_dollars, spread_bps,
bid_size_contracts, ask_size_contracts, best_bid_source, best_ask_source, tick_size_dollars,
min_order_size_contracts, quote_age_ms, valid_state, quality_flags,
raw_event_ref
```

`best_bid_source` and `best_ask_source` are `direct` for venue-observed
top-of-book fields, `complement_derived` when a binary-outcome quote is inferred
from the opposite outcome, and `missing` when that side is unavailable.

For Kalshi websocket books, `use_yes_price` changes the meaning of the native
NO ladder. Under `kalshi_quote_normalization.v2`, a YES-price-mode NO level is a
direct YES ask and a complement-derived NO bid. In NO-price mode it is a direct
NO bid and a complement-derived YES ask. The NO ask is always derived from the
YES bid. Each ask size follows the ladder that produced that ask; explicit ask
sizes are preferred by the canonical normalizer. New capture run state persists
both `use_yes_price` and `quote_normalization_policy`, and tape post-book hashes
bind to that exact adapter-settings mapping. A missing policy identifies a
pre-v2 capture and intentionally selects legacy source and opposite-bid-size
behavior during replay; an empty or unknown policy is invalid evidence rather
than another legacy spelling.

The provenance path is intentionally explicit at each boundary:

| Stage | Preserved authority |
| --- | --- |
| Raw websocket message | `yes_dollars_fp` and `no_dollars_fp` retain venue-native ladder side, price convention, and size. The transcript fixtures cover both wire modes. |
| `KalshiOrderBookState` | Native YES/NO bid ladders remain separate; `use_yes_price` determines whether the NO ladder is expressed on the YES or NO scale. |
| Serialized snapshot | All four projected prices and sizes carry explicit `direct`, `complement_derived`, or `missing` source labels plus the policy version. |
| Canonical `topbook.v1` | One YES row and one NO row retain executable price, originating size, and bid/ask source. Source changes participate in evidence IDs and on-change fingerprints. |
| Tape reconstruction | Native ladders are replayed first, then projected using the adapter settings pinned by run state. Missing policy replays legacy semantics; v2 replays corrected semantics. |

`depth.v1` columns:

```text
schema_version, collector_run_id, exchange, venue_market_id, instrument_id,
outcome, source, received_at_utc, exchange_ts_utc, local_sequence,
venue_sequence, venue_sid, book_hash, side, level_index, price_dollars,
size_contracts, cumulative_size_contracts, is_delta, valid_state,
quality_flags
```

## Trade schema: `trade.v1`

A normalized executed trade observation.

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `trade.v1`. |
| `collector_run_id` | Capture-run authority for streaming trades; null only for non-capture historical trade rows. |
| `venue` | Venue name. |
| `venue_trade_id` | Native trade id if available. |
| `venue_market_id` | Parent market id. |
| `instrument_id` | Tradable instrument id. |
| `outcome` | Outcome label. |
| `trade_ts_utc` | Exchange trade timestamp. |
| `received_at_utc` | Local receive timestamp. |
| `received_at_monotonic_ns` | Capture-local monotonic receive clock for streaming trades. |
| `local_sequence` / `subsequence` | Exact within-shard causal coordinate for streaming trades. Capture coordinates are either all present or all absent. |
| `price_dollars` | Trade price in dollars/probability units. |
| `size_contracts` | Contract quantity. |
| `notional_dollars` | Price times size. |
| `aggressor_side` | Buy/sell or taker side if inferable. |
| `raw_json` | Optional raw payload JSON. |
| `raw_json_sha256` | SHA-256 of raw payload when raw JSON is stored elsewhere. |

## Market match schema: `market_match.v2`

A cross-venue equivalence candidate. `market_match.v1` remains registered as
`market_match_v1` for legacy persisted files, but new match outputs should use
`market_match.v2`.

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `market_match.v2`. |
| `polymarket_exchange` / `kalshi_exchange` | Venue names. |
| `polymarket_market_key` / `kalshi_market_key` | Venue market identifiers. |
| `polymarket_instrument_key` / `kalshi_instrument_key` | Side/instrument identifiers. These are part of the v2 primary key. |
| `polymarket_token_ids` | Polymarket CLOB token ids when available. |
| `polymarket_question` / `kalshi_question` | Market question or title. |
| `polymarket_category` / `kalshi_category` | Venue category labels. |
| `polymarket_close_time` / `kalshi_close_time` | Venue close/expiration timestamps. |
| `polymarket_status` / `kalshi_status` | Venue market status. |
| `polymarket_mid` / `kalshi_mid` | Mid prices when available. |
| `polymarket_price_source` / `kalshi_price_source` | Source used for the venue price. |
| `polymarket_bid` / `polymarket_ask` | Polymarket top-of-book prices. |
| `kalshi_bid` / `kalshi_ask` | Kalshi top-of-book prices. |
| `polymarket_spread` / `kalshi_spread` | Bid/ask spread by venue. |
| `polymarket_bid_size` / `polymarket_ask_size` | Polymarket top-of-book sizes. |
| `kalshi_bid_size` / `kalshi_ask_size` | Kalshi top-of-book sizes. |
| `polymarket_depth` / `kalshi_depth` | Numeric depth level counts when available. |
| `polymarket_book_ts` / `kalshi_book_ts` | Book observation timestamps. |
| `polymarket_volume` / `kalshi_volume` | Venue volume metadata. |
| `polymarket_liquidity` / `kalshi_liquidity` | Venue liquidity metadata. |
| `kalshi_open_interest` | Kalshi open-interest metadata, normalized as a float because live payloads may include fractional dollar values. |
| `polymarket_resolution_source` / `kalshi_resolution_source` | Contract resolution sources. |
| `polymarket_rules` / `kalshi_rules` | Contract rule text. |
| `polymarket_time_cutoff` / `kalshi_time_cutoff` | Parsed or venue-provided cutoff terms. |
| `polymarket_inclusivity` / `kalshi_inclusivity` | Boundary inclusivity terms. |
| `title_similarity` | Text similarity feature. |
| `token_jaccard` | Token-set Jaccard similarity. |
| `specific_token_overlap` | Shared non-generic token count. |
| `entity_overlap` | Shared entity/phrase count. |
| `role_entity_status` | Semantic role/entity match status. |
| `close_time_distance_hours` | Absolute close-time distance. |
| `category_match` | Category compatibility score. |
| `contract_type_pair` | Polymarket and Kalshi contract-type pair. |
| `resolution_source_similarity` | Resolution-source text similarity. |
| `rules_similarity` | Rules text similarity. |
| `price_coverage_status` | Whether both sides have comparable prices. |
| `price_gap` | Absolute venue price gap when available. |
| `spread_pair` / `depth_pair` | JSON per-venue spread/depth values. |
| `tradeability_score` | Price/depth tradeability feature. |
| `equivalence_confidence` | Price-agnostic confidence that the contracts are equivalent enough to track. |
| `equivalence_status` | Equivalence review bucket such as auto_high, review, or low_confidence. |
| `confidence_score` | Legacy alias for `equivalence_confidence`. |
| `review_status` | Review bucket for the candidate. |
| `semantic_match_status` | Detailed semantic compatibility status. |
| `semantic_mismatch_reasons` | Semicolon-delimited semantic mismatch reasons. |
| `shared_phrase_entities` | Comma-delimited shared phrase/entity strings. |
| `match_explanation` | Human-readable explanation of the score. |

## Co-resolution observation schema: `co_resolution_observation.v1`

An offline research sidecar built from `market_match.v2` candidate pairs and
authoritative `market_resolution.v1` cache rows. It is the training/evaluation
input for CR-14 Bayesian co-resolution models and is not accepted by live,
paper-trading, signal, or OMS paths.

Primary key:

```text
observation_run_id, polymarket_market_key, polymarket_instrument_key,
kalshi_market_key, kalshi_instrument_key
```

Core columns:

| Column | Meaning |
| --- | --- |
| `observation_run_id` / `created_at_utc` | Observation build run identity and creation time. |
| Pair keys | Matched Polymarket/Kalshi market and instrument identifiers copied from `market_match.v2`. Missing instrument keys are emitted as explicit empty values with `data_quality_flags`. |
| Matcher feature columns | `equivalence_confidence`, text/semantic similarity features, close-time/category/rules/source features, and review/status fields copied from the candidate row for later bucketed models. |
| `polymarket_terminal_label` / `kalshi_terminal_label` | Strict terminal labels from authoritative resolution evidence under the row's `label_policy_id`. |
| `polymarket_binary_yes` / `kalshi_binary_yes` | Binary YES outcomes only when safely attributable to the matched instrument. |
| `binary_label_grain` | `instrument` when Polymarket YES attribution is proven; `market_fallback` otherwise. |
| `both_terminal_known` / `same_terminal_outcome` | Diagnostic terminal-label comparison, including refund, fractional payout, and scalar labels. |
| `known_binary_pair` / `same_binary_outcome` / `inverse_binary_outcome` | Primary binary co-resolution targets. These are populated only for instrument-grain binary pairs. |
| Marginal baseline columns | Per-venue marginal probabilities, source/quality/timestamps, and independent same/inverse probabilities. `polymarket_baseline_source_ts_utc` and `kalshi_baseline_source_ts_utc` preserve venue provenance; `baseline_source_ts_utc` is the latest venue source timestamp summary. |
| Provenance columns | Source schema versions, label policy id, resolver-version pair, feature-bucket version, data-quality flags, and inclusion/exclusion state. |

Validation invariants require fit rows to be known binary instrument-grain pairs
with no exclusion reason, excluded rows to carry a stable `exclusion_reason`,
binary targets to be null outside `known_binary_pair`, terminal targets to be
null outside `both_terminal_known`, probabilities to stay in `[0, 1]`, and the
composite observation key to be unique. CR-14 leakage checks add
`marginal_probability_leakage` quality flags, and in strict mode exclude rows,
when either venue marginal baseline timestamp or close-time cutoff is missing or
unparsable, when the latest baseline timestamp is after the earliest venue
decision cutoff, or when a venue baseline timestamp is after known resolution
evidence.

## Co-resolution score schema: `co_resolution_score.v1`

An offline, research-only score sidecar produced by CR-14 co-resolution models.
It is designed for research reports and manual review, not live execution.

Primary key:

```text
experiment_id, scorer_run_id, polymarket_market_key,
polymarket_instrument_key, kalshi_market_key, kalshi_instrument_key
```

Core columns:

| Column | Meaning |
| --- | --- |
| `experiment_id` / `manifest_hash` | Semantic experiment identity and manifest content hash. |
| Model identifiers | `model_version`, `model_family`, `model_spec_id`, `bucket_strategy_id`, `feature_set_id`, `label_policy_id`, and `score_semantics`. |
| Pair keys | The Polymarket/Kalshi market and instrument keys being scored. Market and instrument keys are required because scores are instrument-level sidecars. |
| `co_resolution_probability` / `inverse_resolution_probability` | Posterior probabilities for binary same-outcome and inverse-outcome semantics. |
| `*_lower` / `*_upper` | Credible or documented interval estimates for each probability. |
| Independent baseline columns | Independent same/inverse probabilities and lift fields when marginal probability evidence is trusted. |
| Bucket diagnostics | Selected bucket level, selected bucket row count, feature bucket version, score source, and model diagnostics JSON. |
| Safety fields | `complement_residual`, `data_quality_flags`, `risk_flags`, `research_only`, and `allowed_consumers_json`. |

Validation invariants require posterior probabilities and intervals to be
present, stay in `[0, 1]`, intervals to be ordered, the composite score key to
be unique and non-empty, CR-14 rows to be `research_only=true`, allowed
consumers to exclude paper/live/signal/OMS paths, and incoherent same+inverse
complements to carry
`binary_complement_incoherent` in both `data_quality_flags` and `risk_flags`.

## Websocket tracking match schema: `tracking_match.v1`

`websocket_tracking_matches.parquet` is a validated match-oriented table
derived from generic matcher output. It contains only cross-venue relationships
that are useful for live websocket monitoring, not price-dislocation or
arbitrage candidates.

Tracking rows can be built from accepted matcher output, direct market-snapshot
rules, or scan candidate pairs. The candidate-pair path is review/tracking
oriented: it joins scan pairs back to source market rows and applies tracking
relation rules without promoting the rows to strict equivalence.

`pmkt build-tracking-matches --diagnostics-out` can write a JSON sidecar with
the build mode, output counts, tier/relation counts, zero-output reasons, and
candidate-pair resolution counters such as `source_rows_joined`,
`source_rows_missing`, `relation_detected`, and `no_tracking_relation`.

`pmkt build-websocket-tracking-set` runs the tracking-only pipeline end to end:
it writes `tracking_match.v1`, converts it to `match_relation.v1`, builds a
`subscription_plan.v1`, and writes validation/diagnostic JSON sidecars.
Both `build-match-relations` and `build-websocket-tracking-set` accept
`--review-labels` with `market_match_label.v2` CSV files. Label overlays join by
instrument keys first and market keys second; matched labels override the
inferred relation label and are copied into `evidence_json.review_label`.
Market-level `match`, `exact_equivalent`, and `inverse_equivalent` labels are
downgraded to tracking-only context rows during instrument expansion; strict
trade-equivalent labels must be instrument-scoped.

Important columns:

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `tracking_match.v1`. |
| `tracking_pair_id` | Stable `pm:<market>|kalshi:<ticker>` pair id. |
| `match_tier` | `track_event_related` for tight event tracking or `review_related` for broader context tracking. |
| `relation_type` | Specific relation rule such as World Cup same-team/group, retirement cutoff, sports final-series or same-matchup context, `generic_high_confidence_market_match`, or tightly gated `generic_review_market_match` rows. |
| `quality_note` | Human-readable reason the relation is trackable but not necessarily equivalent. |
| `scan_domain` | Domain that produced the relation. |
| `polymarket_*` / `kalshi_*` | Venue identifiers, questions, close times, and instrument hints. |
| `polymarket_source_row_hash` / `kalshi_source_row_hash` | Stable hashes of the source market rows used to create the tracking pair. |
| `polymarket_raw_payload_hash` / `kalshi_raw_payload_hash` | Optional hashes of the venue raw payloads when available from ingestion. |
| `polymarket_contract_fields_json` / `kalshi_contract_fields_json` | JSON contract evidence carried forward for relation evidence and review. |
| `confidence_score` | Generic matcher confidence retained for audit and sorting. |

## Match relation schema: `match_relation.v1`

A source-level relation used to separate strict trading equivalence from
websocket/event tracking relationships. Relation rows are instrument-level:
market-level tracking rows with multiple Polymarket tokens are expanded into one
relation per concrete token, so websocket plans subscribe only identifiers that
are explicitly relation-linked.

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `match_relation.v1`. |
| `match_id` | Stable relation id derived from venue market and instrument identifiers. |
| `relation_version` | Rule version used to assign the relation. |
| `polymarket_market_key` / `kalshi_market_key` | Venue market identifiers. |
| `polymarket_instrument_key` / `kalshi_instrument_key` | Venue instrument identifiers when known. |
| `polymarket_token_id` | One Polymarket CLOB token id, not a serialized token list. |
| `polymarket_outcome` / `kalshi_outcome` | Outcome labels when known. |
| `relation_label` | One of `exact_equivalent`, `inverse_equivalent`, `same_event_different_outcome`, `same_event_different_cutoff`, `same_event_different_settlement_scope`, `same_event_different_resolution_source`, `same_context_only`, `parlay_or_composite`, `ambiguous_requires_review`, or `unrelated`. |
| `relation_type` | More specific rule or domain relation tag. |
| `match_tier` | Source tier such as `track_event_related`, `review_related`, or `strict_equivalent`. |
| `quality_note` | Human-readable relation note. |
| `is_trade_equivalent` | True only for strict allowed trading labels. |
| `is_inverse` | True for inverse-equivalent relations. |
| `is_tracking_useful` | True for relations suitable for websocket context tracking. |
| `confidence_score` | Relation confidence score when available. |
| `review_status` | Review state such as `tracking_only`, `requires_review`, or `rejected`. |
| `reviewer` / `reviewed_at_utc` | Optional human or process review metadata. |
| `evidence_json` | JSON evidence bundle from source match fields, including source row hashes and contract fields for tracking-useful or trade-equivalent rows. Scope-sensitive relations are classified from source-backed contract fields; caller scope flags are retained only under `candidate_hints`. Parsed per-venue scopes remain alongside the original rules in `contract_fields`. |
| `risk_flags` | Semicolon-delimited warnings and blocking flags. |
| `created_at_utc` | Row creation timestamp. |

Invariant: `same_event_*`, `same_context_only`, `parlay_or_composite`,
`ambiguous_requires_review`, and `unrelated` rows must never have
`is_trade_equivalent=true`.

`same_event_different_settlement_scope` identifies contracts for the same event
whose rules use incompatible timing boundaries, such as regulation-only versus
rules that include extra time. It is tracking-useful but never trade-equivalent.
When source-backed contract fields classify a relation as scope-sensitive,
`evidence_json.contract_semantics.settlement_scope_required=true` and a
trade-equivalent row requires non-empty rules for both venues, known parsed
scopes that agree with those rules, equal venue scopes, and no scope/rules risk
flag. Caller hints cannot disable this policy.

The in-memory `SportsContractSpec` structural identity includes normalized
push, tie, and void settlement conventions. Contracts with different
dispositions for those outcomes are not structurally equivalent.

Generic matcher relation types fail closed during relation promotion. Generic
rows without explicit semantic compatibility, or low-confidence generic review
rows without near close times and shared phrase/entity evidence, are written as
`ambiguous_requires_review` with `is_tracking_useful=false`.

Reviewed labels can promote or demote relation rows. Instrument-level
`exact_equivalent` labels are trade-equivalent. Instrument-level
`inverse_equivalent` labels become trade-equivalent only when the label carries
reviewer and reviewed-at metadata or the operator passes an explicit
inverse-approval flag in code. Market-level trade labels remain tracking-only
after expansion. Event/context labels remain tracking-only, and `non_match`,
`same_topic_not_equivalent`, and `unrelated` labels are kept out of websocket
plans.

## Websocket subscription plan: `subscription_plan.v1`

A source-backed manifest for opening market-data websocket subscriptions from a
match set. The plan is match-oriented: when `is_tracking_useful` exists on the
source relation table, only rows with `is_tracking_useful=true` are included.
Rejected or unrelated rows can remain in the relation file for audit without
entering the websocket plan.

Important fields:

| Field | Meaning |
| --- | --- |
| `schema_version` | Always `subscription_plan.v1`. |
| `plan_id` | Stable run or operator-provided plan identifier. |
| `mode` | Intended use, usually `tracking`, `paper`, or `canary`. |
| `source_match_relation_path` | Relation file used to build and preflight the plan. Stream commands require this path, or an explicit `--relations` override, when `--plan` is used. |
| `source_market_registry_path` | Source market registry paths. The normal generated form is a JSON object with `polymarket` and `kalshi` paths. Stream commands require these paths, or venue-specific CLI overrides, when `--plan` is used. |
| `polymarket.assets_ids` | Flattened Polymarket CLOB token IDs for the market websocket. |
| `kalshi.market_tickers` | Kalshi market tickers for `orderbook_delta` websocket subscription. |
| `kalshi.use_yes_price` | Whether Kalshi orderbook subscriptions request YES-price convention. |
| `polymarket_assets` / `kalshi_market_tickers` | Structured entries with source market keys, relation-linked identifiers, outcome metadata, shard ids, active flags, and `match_ids`. |
| `relation_counts` | Counts by relation label/type/tier after tracking-useful filtering. |
| `source_relation_count` | Input relation rows before filtering. |
| `tracking_relation_count` | Relation rows used in the websocket plan. |
| `blocked_reasons` | Non-empty values mean the plan must not be used, including empty-plan reasons such as `no_tracking_relations`, `no_polymarket_assets`, or `no_kalshi_market_tickers`, plus relation-coverage reasons such as `incomplete_tracking_relation_links` or `no_complete_tracking_relations`. |

When relation rows provide `polymarket_token_id` or
`polymarket_instrument_key`, those identifiers are the primary subscription
keys. Market-level `token_ids` are only a source-registry join and fallback for
legacy/incomplete relation rows, so the plan does not subscribe unrelated
sibling outcome tokens merely because they share a Polymarket market.

Structured Polymarket asset entries include `outcome`, `outcome_sides`,
`outcome_mapping_status`, and `outcome_mapping_source`. Structured Kalshi
entries include the analogous `expected_outcome`, `outcome_sides`,
`outcome_mapping_status`, and `outcome_mapping_source`. These fields are
evidence-only metadata for tracking. They are populated only from source
token/outcome pairs, explicit relation outcomes, or Kalshi instrument suffixes
such as `KXTEST:YES`. Missing evidence remains `null`, and conflicting or
multi-sided evidence is surfaced as `conflict` or `ambiguous`; the builder does
not infer outcomes from token order or from `kalshi.use_yes_price`.

Before opening sockets from `pmkt stream-books --plan` or
`pmkt stream-kalshi-books --plan`, the CLI reloads those source files and
revalidates the plan against source token IDs, Kalshi tickers, active/tradable
status, duplicate identifiers, blocked reasons, relation match links, and any
explicit outcome metadata. In plan mode, explicit token IDs or tickers are
subset selectors; they cannot add ad hoc instruments outside the preflighted
match plan.

Stream run manifests created from a plan include a `subscription_plan` block
with the plan id, plan path, SHA-256 hash, source relation path, source market
paths, tracking relation count, and relation counts. This lets later
`tracking_health.v1` reports be traced back to the exact match set used to open
the websockets.

## Feed health schema: `feed_health.v1`

`feed_health.v1` is the shard-level control-plane output written by the
Polymarket and Kalshi stream collectors to `feed_health.parquet`. It tracks
whether each websocket shard is connected, stale, recovering, or blocked by bad
book state. It is separate from topbook/depth price data and is consumed by
`tracking_health.v1` so a relation can remain blocked even when an old quote row
exists.

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `feed_health.v1`. |
| `observed_at_utc` | Local observation timestamp for the health row. |
| `local_sequence` | Collector-local stream sequence at the time health was sampled. |
| `venue` / `shard_id` | Venue and feed shard identifier. |
| `connection_state` | State such as `initialized`, `connected`, `reconnecting`, `stale`, or `disconnected`. |
| `instrument_count` / `relation_count` | Number of subscribed instruments and linked match relations on the shard. |
| `reconnect_count` / `sequence_gap_count` / `resync_count` / `error_count` | Control-plane counters for connection and book recovery behavior. |
| `last_message_age_ms` / `last_valid_book_age_ms` | Freshness ages recomputed from monotonic receive times. |
| `valid_book_count` / `invalid_book_count` | Shard-level book validity counters. |
| `valid_instrument_count` / `invalid_instrument_count` / `stale_instrument_count` / `missing_instrument_count` | Per-instrument coverage summary for the subscribed instruments on the shard. |
| `instrument_state_json` | JSON string with per-instrument validity, age, count, and quality flag state. |
| `quality_flags` | Canonical `list<string>` control-plane tokens such as `stale_messages`, `stale_books`, `sequence_gap`, `reconnect`, or `invalid_instrument_books`; legacy delimited values are normalized only at explicit compatibility-loading boundaries. |


## Capture instrument evidence: `capture_instrument_evidence.v1`

`capture_instrument_evidence.v1` is mandatory for every storage-profile-v2
capture. It records whether each requested instrument was authoritatively
eligible when its subscription attempt began, whether that subscription was
established, and when the first valid (not merely observed-invalid) snapshot
arrived. Missing, stale, malformed, or unauthoritative eligibility evidence is
`unknown`; it never reduces the completeness denominator.

The exact grain is `(collector_run_id, venue, shard_id, subscription_attempt_id, instrument_id)`.

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `capture_instrument_evidence.v1`. |
| `collector_run_id` / `venue` / `shard_id` | Exact capture and shard authority. |
| `subscription_attempt_id` / `instrument_id` | Monotonic attempt and requested instrument identity. |
| `requested` | Whether the instrument was included in this attempt. |
| `eligibility_status` / `eligibility_reason` | Closed eligible, ineligible, or unknown verdict and its cause. |
| `eligibility_source_*` | Source identity, reference, SHA-256, and observation time used for the verdict. |
| `eligibility_checked_at_utc` / `eligibility_evidence_age_seconds` | Capture-time check and evidence age. |
| `subscription_sent_at_utc` / `subscription_established_at_utc` | Transport establishment evidence. |
| `first_valid_snapshot_at_utc` / `initial_snapshot_latency_ms` | First valid-book observation and latency from establishment. |
| `initialization_verdict` | `on_time`, `late`, `missing`, or `not_required`. |
| `terminal_outcome` / `terminal_reason` | Per-instrument outcome and the run's closed terminal reason. |

Manifest `capture_completeness.v2` aggregate counts are derived from and must
reconcile exactly with this sidecar, including its segment-manifest hash.
Only profile v2 with a calibrated evidence policy can be acceptance-eligible;
profile-v1 captures remain readable but cannot supply this acceptance evidence.

`feed_health.v1` retains its schema while using `feed-health-emission.v2`.
Full `instrument_state_json` detail is emitted for connection/startup,
reconnect, gap, error, recovery, and terminal causes. Ordinary transitions and
periodic observations are compact aggregates. Manifests record rows and bytes
by reason, including detail rows and bytes. Consumers combine the latest
aggregate with the latest causally prior valid detail and fail closed if detail
is absent, incomplete, or malformed.
## Storage profiles and committed capture authority

Profile stream runs use `extra.dataset_artifacts` as the physical source of
truth. Each role entry names its exact relative path, dataset identity, schema
version, row count, segment-manifest path and SHA-256, and completion status.
The manifest's `storage_profile.enabled_roles` must equal the artifact keys and
`successfully_committed_roles` must equal the closed roles. `dataset_paths`,
`schema_versions`, and `row_counts` remain readable compatibility projections;
they do not override an exact role entry.

| Profile | Stable status | Mandatory evidence |
| --- | --- | --- |
| `full` | stable and initial default | Legacy parsed/snapshot/level roles, dense topbook/depth, committed book tape, trade, lifecycle, and health. |
| `book-tape` | experimental pending CR-18.9 acceptance | Committed tape/control, changed topbook plus checkpoints, trade, lifecycle, and health. |
| `mm-compact` | experimental pending CR-18.9 acceptance | Changed topbook plus checkpoints, topbook-backed validity controls, trade, lifecycle, and health. No depth-reconstruction promise. |

Overrides are additive only. Raw JSONL, dense topbooks, full depth, and legacy
book artifacts may be added; a caller cannot subtract a mandatory role.
Only roles actually opened appear in stream output discovery.

The capture commit journal is the crash boundary.
`capture_commit_journal.v2.jsonl` is the write format. Every record contains a
monotonic group index, one closed canonical cause, acceptance and commit UTC
timestamps, the exact artifact list, and a checksum over the complete record.
`committed_at_utc` is sampled after artifact durability and immediately before
the checksummed record is appended. The exact acceptance-to-journal-fsync
latency is measured with the process monotonic clock after the append and
directory fsync; it is therefore a terminal process metric, not a value that can
be reconstructed exactly from the journal after process loss.
The reader retains `v1` compatibility and reports its cause as
`legacy_unknown`; v1 is never rewritten. Multiple journal versions or mixed
record formats in one run fail closed.

`run_state.v1.capture_durability` is the capture-time durability configuration
authority. The final manifest projects it exactly under
`capture_durability.configuration` and adds terminal metrics for accepted,
published, and discarded groups, published cause counts, queue depth/waits,
acceptance-to-journal latency percentiles, and maximum uncommitted age. New
captures begin in inline publication mode; async publication remains gated.

A crash-recovered manifest can prove published groups and journal-derived cause
counts, but represents the process-local accepted/discarded, queue,
acceptance-to-journal latency, and maximum-uncommitted-age metrics as unknown
rather than zero.
A tape event header is only a logical candidate until its event/level counts,
foreign keys, side counts, payload and post-book hashes, epoch, segment hashes,
and journal group all validate. Clean shutdown closes and journals every
enabled role, atomically writes the final manifest, and only then marks
`run_state.v1` finalized. Crash recovery promotes journaled groups only.

Two crash guarantees are distinguished, with separate evidence.

**Child-process crash consistency** covers abrupt termination of the capture
process while the operating system and mounted filesystem keep running. Recovery
promotes only complete journaled groups and rejects or quarantines everything
else. Because a killed process leaves the operating system holding the renamed
directory entry, this guarantee does not depend on a directory fsync, and
Windows evidence can satisfy it. Existing real child-process probes cover the
pre-journal and post-journal-fsync boundaries only; full write, flush, fsync,
rename, journal, manifest, and run-state crash-point qualification remains a
CR-18.9 acceptance requirement.

**Host and power-loss durability** is a separate claim. On POSIX platforms,
atomic metadata publication fsyncs both files and parent directories. On
Windows, file contents are flushed and fsynced but directory handles are not, so
survival of rename metadata across a sudden power loss or hard reset is not
claimed. Neither is it claimed on POSIX by the platform label alone: a
host-crash guarantee requires a named, tested combination of operating system,
filesystem and mount settings, persistence implementation and journal version,
and a fault method demonstrating post-restart recovery. Until such evidence
exists, describe the implemented guarantee as child-process crash consistent.

## Book tape schemas

`book_tape_event.v1` is one immutable checkpoint or delta header at the grain
`(collector_run_id, event_id)`. It carries the causal coordinate, epoch,
expected level count, checkpoint side counts, logical payload hash, post-book
hash, validity, and reconstructibility. A checkpoint opens an epoch. A
reconstructible delta must name that open epoch.

`book_tape_level.v1` is an absolute post-event mutation at
`(collector_run_id, event_id, source_side, price_key)`. `price_key` is canonical
fixed-decimal identity; `size_after_contracts=0` removes the price. Polymarket
persists native `bid`/`ask`; Kalshi persists native `yes`/`no`. These native
sides are normalized only after offline reconstruction.

`book_tape_control.v1` records `book_recovered`, `book_invalidated`, and
`stream_ended` boundaries. Invalidations close an epoch immediately. Recovery
controls point to the checkpoint or topbook evidence that reopened valid state.
`stream_lifecycle.v1` is separate market-lifecycle evidence and never changes
book validity.

Offline reconstruction validates the manifest and journal before applying
absolute deltas. Its `topbook.v1`, `depth.v1`, and versioned JSON report are
research/audit artifacts, never live-book or execution authority.

## Sparse replay evidence

`sparse-v2` loads main topbooks, checkpoint topbooks, trades, tape controls,
slim health, exact manifests, and shard subscription mappings as one typed
bundle. State is selected causally at or before the requested time and is never
forward-filled over invalidation, terminal, liveness, or coverage boundaries.
Only later main-topbook changes and deduplicated public trades are fill
triggers. Checkpoints establish coverage but are not market activity. Existing
file-based replay remains explicitly `dense-v1`.

Each streaming trade is bound to its persisted collector run, capture-local
monotonic time, local sequence, and subsequence, plus the exact source run and
shard supplied by manifest/journal artifact ownership. `sparse-v2` uses those
coordinates directly and never infers a trade sequence from a health timestamp.
The loader snapshots manifest, run-state, and journal authority and verifies
artifact hashes before and after materialization and again before returning, so
concurrent evidence mutation fails closed.

Slim `feed_health.v1` consumers use the latest causal aggregate row plus the
most recent non-empty detail blob at or before it. A future heartbeat cannot
prove past liveness; malformed or unavailable required detail fails closed.

## Tracking health schema: `tracking_health.v1`

`tracking_health.v1` joins `match_relation.v1` rows to latest per-venue
`topbook.v1` observations. It is a monitoring artifact for deciding whether a
matched relation is ready to track over websockets; it is not an arbitrage or
execution signal.

The `pmkt build-tracking-health` CLI writes this artifact from a relation file
and Polymarket/Kalshi topbook files. Optional per-venue `feed_health` inputs and
stream manifests add control-plane readiness checks, so reconnects, sequence
gaps, stale feed messages, and missing shard health can block a relation even
when an old topbook row is present. Its JSON summary includes
separate coverage and readiness metrics: `observed_two_sided_count` counts
relations with topbook rows on both venues, `valid_two_sided_count` adds
per-book validity, `fresh_two_sided_count` adds quote-age freshness,
`fresh_valid_two_sided_count` requires both freshness and validity, and
`ready_relation_count` additionally requires clean feed-health and manifest
provenance.

`pmkt filter-ready-relations` can use this artifact to materialize the rows
whose `tracking_ready=true` as a new `match_relation.v1` file. This is useful
when a broad tracking set is observable over websockets but only a subset has
fresh, valid books at the current observation time.

Feed health rows include shard-level counters plus per-instrument coverage
fields: `valid_instrument_count`, `invalid_instrument_count`,
`stale_instrument_count`, `missing_instrument_count`, and
`instrument_state_json`. A shard with only one fresh book in a multi-instrument
subscription can still contain ready relations: when `instrument_state_json` is
present, tracking health gates each relation against its own instrument state
instead of applying sibling-instrument invalid/stale counters to the whole
shard. Shard-level disconnect, reconnect, error, and stale-message flags still
block all relations on that shard.

When stream manifests are supplied, tracking health also verifies manifest
coverage. The current `match_id` and the venue instrument must appear on the
same manifest shard; a relation link on one shard and an instrument on another
does not count. Kalshi manifest instruments may use the market ticker while the
relation/topbook key uses `TICKER:YES` or `TICKER:NO`. Mismatches produce
`missing_polymarket_manifest_relation_link` or
`missing_kalshi_manifest_relation_link` and block readiness.

If both venue manifests include `subscription_plan` provenance, their plan id,
plan SHA-256, and source relation path must agree. Mismatches produce
`cross_venue_manifest_subscription_plan_mismatch`. When
`build-tracking-health` is run through the CLI, the relation file path is also
compared against each manifest's `subscription_plan.source_match_relation_path`;
mismatches produce `polymarket_manifest_relation_source_mismatch` or
`kalshi_manifest_relation_source_mismatch`.

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `tracking_health.v1`. |
| `match_id` | Source `match_relation.v1` identifier. |
| `observed_at_utc` | Health evaluation timestamp. |
| `relation_label` | Source relation label. |
| `is_tracking_useful` / `is_trade_equivalent` | Source relation booleans retained for filtering and audit. |
| `polymarket_instrument_key` / `kalshi_instrument_key` | Instruments used to look up venue topbooks. |
| `polymarket_topbook_ref` / `kalshi_topbook_ref` | Stable JSON references to the selected latest topbooks. |
| `polymarket_quote_age_ms` / `kalshi_quote_age_ms` | Per-venue quote age at `observed_at_utc`. |
| `max_quote_age_ms` | Freshness threshold used for readiness. |
| `polymarket_valid_book` / `kalshi_valid_book` | Whether each selected topbook is marked valid. |
| `tracking_ready` | True only when both venues have fresh valid topbooks and no health flags. |
| `health_status` | One of `ready`, `missing`, `invalid`, `stale`, `skipped`, or `blocked`. |
| `health_flags` | Semicolon-delimited blockers such as `missing_kalshi_topbook`, `invalid_polymarket_book`, `stale_kalshi_quote`, `kalshi_feed_sequence_gap`, `polymarket_feed_reconnect`, or `missing_kalshi_feed_health`. |

## Scan candidate pairs

Scan planning writes `candidate_pairs.parquet` as a prefilter artifact for
matching and websocket collection. New files retain legacy
`polymarket_row_id` and `kalshi_row_id` columns for backward compatibility, but
downstream code must prefer stable identifiers:

```text
polymarket_market_key, polymarket_instrument_key, polymarket_token_ids,
polymarket_source_row_hash, kalshi_market_key, kalshi_instrument_key,
kalshi_event_ticker, kalshi_source_row_hash
```

The row-id columns are selected-DataFrame positions, not durable source IDs.
They should only be used as a fallback for older artifacts that lack stable
market and instrument keys.

`scan_summary.json` includes candidate-pair diagnostics beside the parquet
artifact. `candidate_pair_diagnostics` counts the first rejection gate for each
indexed pair, including cross-category, contract-family, subtype, semantic,
shared-token, specific-token, pair-score, and top-k truncation rejections.
`candidate_pair_zero_output_reasons` highlights the dominant reason when a scan
selects markets but emits no pairs.

When `pmkt match-markets --nlp-diagnostics-out` is used, the NLP output is a
non-canonical sidecar. It contains character TF-IDF nearest-neighbor rows,
RapidFuzz score components, blank-spaCy rule-entity overlaps, and whether a
diagnostic pair appeared in scan candidates or emitted matches. These columns
are for review and tuning only and do not extend the canonical market-match
schema.

Large-universe matching uses two distinct category concepts:

- `category` / scan category is the broad recall route used to avoid obvious
  cross-domain comparisons.
- `proposed_broad_category` is an analysis/research label for rows initially
  classified as `other_unclear` but safely routable to a broad category such as
  sports, crypto/economics, culture, or geopolitics.
- `scan_subcategory` or `blocking_domain` is a deterministic fine
  domain/template stratum, for example soccer match props, esports, token
  launch/depeg markets, or music-streaming ladders.

The 2026-06-02 open-universe analysis found that broad reassignment improves
recall, but broad Cartesian scoring is too large. For recall-grade scans, build
candidates with same broad category plus rare/specific token overlap, then use
same fine stratum, close-date match, subtype compatibility, and top-k caps as
precision/ranking features unless the run is intentionally high precision.

## Market match label schemas

Evaluation labels are not canonical persisted market data, but the matcher
evaluation tools expect stable CSV schemas.

`market_match_label.v1` columns:

```text
label_schema_version, polymarket_market_key, kalshi_market_key, label,
label_confidence, reviewer, reviewed_at_utc, review_notes
```

`market_match_label.v2` columns:

```text
label_schema_version, polymarket_market_key, polymarket_instrument_key,
kalshi_market_key, kalshi_instrument_key, label, label_confidence, reason,
reviewer, reviewed_at_utc, review_notes
```

Supported labels include `match`, `non_match`, `exact_equivalent`,
`inverse_equivalent`, `same_event_different_cutoff`,
`same_event_different_outcome`, `same_event_different_resolution_source`,
`same_context_only`, `same_topic_not_equivalent`, `unrelated`,
`parlay_or_composite`, and `ambiguous_requires_review`.

## Signal schema: `signal.v1`

`signal.v1` is the deterministic top-of-book opportunity state produced after
relation validation and live book freshness checks. It is not an order and does
not contain credentials or private order payloads.

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `signal.v1`. |
| `signal_id` | Deterministic hash over match id, strategy version, book refs, observation time, and side plan. |
| `match_id` | Source `match_relation.v1` identifier. |
| `strategy_version` | Signal engine version, e.g. `cross_venue_topbook.v1`. |
| `observed_at_utc` | Signal observation timestamp. |
| `relation_label` | Relation label used for execution gating. |
| `side_plan` | Proposed side comparison such as `buy_polymarket_sell_kalshi` or `buy_both_inverse`. |
| `polymarket_topbook_ref` / `kalshi_topbook_ref` | Stable JSON reference to the venue topbook observation. |
| `gross_edge` | Top-of-book gross edge before costs. |
| `fee_estimate` / `slippage_estimate` | Conservative cost buffers. |
| `net_edge` | `gross_edge - fee_estimate - slippage_estimate`. |
| `executable_size` | Maximum top-of-book size for the selected side plan. |
| `quote_age_ms` | Maximum per-venue quote age recomputed at signal time when monotonic receive timestamps are present. |
| `book_quality_flags` | Combined non-blocking or diagnostic book quality flags. |
| `risk_flags` | Blocking signal risk flags. Empty means no blocker was detected. |
| `decision` | `allow`, `observe`, or `reject`. |
| `decision_reason` | `ok` or semicolon-delimited blocking reasons. |
| `execution_allowed` | True only when strict relation, valid fresh books, positive net edge, positive size, and no risk flags all hold. |

Invariant: `execution_allowed=true` is valid only for `exact_equivalent` or
`inverse_equivalent` rows with positive `net_edge`, positive `executable_size`,
empty `risk_flags`, and `decision=allow`. Tracking-only relations must remain
`decision=observe` or `decision=reject`.

Batch signal generation joins `match_relation.v1` rows to latest per-venue
topbooks by instrument id. By default, missing topbooks still emit
`signal.v1` rows with blocking risk flags so replay/reporting can account for
why a relation did not produce an executable signal.

Full `signal.v1` frames are reporting artifacts: tracking-only matches can
retain side plans, edge metrics, and blocking reasons so they remain visible in
websocket monitoring reports. Simulated order flow must use
`select_simulation_eligible_signals()`, which redundantly requires
`execution_allowed=true`, `decision=allow`, empty `risk_flags`, a strict
relation label, positive net edge, and positive size.

`SignalEngineConfig` controls strategy version, strict allowed relation labels,
maximum quote age, fee/slippage buffers, minimum net edge, maximum simulated
size, and venue enablement. Configured allowed labels are still intersected
with the strict label set, so event-related tracking matches cannot be made
executable by configuration. `summarize_signal_blocks()` produces a compact
reason-count report over allowed, observed, and rejected rows for dashboards and
paper-run review.

Execution readiness is a private-consumer concern and is not evaluated by this
package. A trading consumer may combine these reporting rows with approved
relation evidence, current book state, runtime capability controls, and sizing
plans, but the canonical schema itself grants no execution authority.

## Paper execution artifacts

The paper execution artifacts are deterministic simulator outputs for matched
relations selected by `select_simulation_eligible_signals()`. They are not
venue order payloads and must not be interpreted as live submissions.
Tracking-only matches remain observable websocket pairs and are skipped by the
simulator even when they have positive-looking price metrics.

`replay_topbook_paper_execution()` and the `pmkt replay-paper-signals` CLI
connect the Phase 3 replay path: `match_relation.v1` plus per-venue
`topbook.v1` inputs produce full `signals.parquet`, `signal_block_report`,
`eligible_signals.parquet`, `order_intents.parquet`, `paper_fills.parquet`,
`order_states.parquet`, and `paper_positions.parquet` artifacts.

### Order intent schema: `order_intent.v1`

A pre-submission order proposal derived from one signal leg.

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `order_intent.v1`. |
| `order_intent_id` | Deterministic simulator intent id. |
| `signal_id` | Source `signal.v1` id. |
| `venue` | `polymarket` or `kalshi`. |
| `instrument_id` | Venue instrument id. |
| `outcome_side` | Outcome side when known. |
| `action` | `buy` or `sell`. |
| `book_side` | Book side crossed by the simulator, `ask` for buys and `bid` for sells. |
| `limit_price` | Simulated limit/fill price from the referenced topbook. |
| `size_contracts` | Requested contracts for this leg. |
| `order_type` | Simulator order type, currently `limit`. |
| `post_only` / `reduce_only` | Boolean intent flags. |
| `client_order_id` | Deterministic client id for idempotency checks. |
| `expires_at_utc` | Optional expiry timestamp. |
| `risk_check_status` | `passed` is required for live-mode rows. |
| `risk_check_json` | JSON risk context. |
| `created_at_utc` | Intent creation timestamp. |
| `mode` | `paper`, `canary`, or `live`; current simulator emits `paper`. |

Invariant: duplicate `client_order_id` values must not have conflicting order
payloads. Live-mode rows require `risk_check_status=passed`.

### Order state schema: `order_state.v1`

A reconciled simulated or venue order state. Paper execution uses this to
summarize fills and cancelled remainders without submitting to either venue.

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `order_state.v1`. |
| `client_order_id` | Intent client id. |
| `venue_order_id` | Venue order id or simulator order id. |
| `venue` | Venue name. |
| `instrument_id` | Venue instrument id. |
| `status` | `filled`, `partially_filled`, `cancelled`, `rejected`, or `open`. |
| `submitted_at_utc` / `last_update_at_utc` | State timestamps. |
| `filled_size_contracts` | Filled contracts. |
| `remaining_size_contracts` | Remaining contracts after any simulated cancel. |
| `average_fill_price` | Average fill price for filled rows. |
| `fees_dollars` | Simulated or reconciled fees. |
| `venue_sequence` | Venue sequence when known. |
| `source` | `paper` for simulator output or venue/source identifier. |
| `raw_event_ref` | Source signal or venue event reference. |
| `reconcile_status` | Reconciliation status, e.g. `simulated`. |
| `reconcile_flags` | Semicolon-delimited diagnostics such as `partial_fill`. |

### Paper fill schema: `paper_fill.v1`

One simulated fill event for one signal leg.

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `paper_fill.v1`. |
| `paper_fill_id` | Deterministic fill id. |
| `order_intent_id` | Source intent id. |
| `signal_id` | Source signal id. |
| `client_order_id` | Intent client id. |
| `venue` | Venue name. |
| `instrument_id` | Venue instrument id. |
| `outcome_side` | Outcome side when known. |
| `action` | Simulated buy/sell action. |
| `book_side` | Book side crossed. |
| `fill_price_dollars` | Topbook price used by the simulator. |
| `size_contracts` | Filled contracts. |
| `notional_dollars` | `fill_price_dollars * size_contracts`. |
| `fees_dollars` | Simulated fee amount. |
| `filled_at_utc` | Fill timestamp after configured latency. |
| `simulator_version` | Paper execution simulator version. |
| `fill_type` | `full` or `partial`. |
| `latency_ms` | Configured simulated latency. |
| `topbook_ref` | Stable JSON topbook reference used for this fill. |
| `risk_flags` | Fill-level warnings. |

Invariant: `notional_dollars` must equal fill price times size.

### Paper position schema: `paper_position.v1`

A per-signal matched-position and PnL summary.

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `paper_position.v1`. |
| `paper_position_id` | Deterministic position id. |
| `signal_id` / `match_id` | Source signal and relation ids. |
| `strategy_version` | Source signal strategy version. |
| `opened_at_utc` / `as_of_utc` | Position timestamps. |
| `status` | Current simulator status, normally `open`. |
| `polymarket_instrument_id` / `kalshi_instrument_id` | Filled venue instruments. |
| `source_fill_ids` | Paper fill ids backing the position. |
| `polymarket_position_contracts` / `kalshi_position_contracts` | Signed venue positions. |
| `filled_size_contracts` | Matched filled size. |
| `unmatched_leg_size_contracts` | Absolute leg imbalance. |
| `buy_notional_dollars` / `sell_notional_dollars` | Simulated cash outflow and inflow. |
| `fees_dollars` | Total simulated fees. |
| `gross_pnl_dollars` | `sell_notional_dollars - buy_notional_dollars`. |
| `realized_pnl_dollars` / `unrealized_pnl_dollars` | PnL decomposition. |
| `net_pnl_dollars` | Gross PnL less fees. |
| `risk_flags` | Semicolon-delimited diagnostics such as cancelled remainders. |

Invariant: `gross_pnl_dollars` equals sell minus buy notional,
`net_pnl_dollars` equals gross less fees, and realized plus unrealized PnL must
equal net PnL.

## Arbitrage candidate schema: `arbitrage_candidate.v1`

A price-dislocation review artifact. This is not an order instruction.

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `arbitrage_candidate.v1`. |
| `match_id` | Stable id/link to a market match record. |
| `polymarket_instrument_id` | Polymarket side instrument. |
| `kalshi_instrument_id` | Kalshi side instrument. |
| `observed_at_utc` | Observation timestamp. |
| `polymarket_bid_dollars` | Polymarket bid. |
| `polymarket_ask_dollars` | Polymarket ask. |
| `kalshi_bid_dollars` | Kalshi bid. |
| `kalshi_ask_dollars` | Kalshi ask. |
| `gross_edge_dollars` | Best gross bid/ask edge. |
| `apparent_gross_edge` | Compatibility/research alias emphasizing that the edge is apparent. |
| `fee_buffer_dollars` | Legacy v1 column name containing the modeled venue fees per contract; no flat fallback is applied. |
| `net_edge_dollars` | Gross edge less modeled venue fees. |
| `apparent_net_edge` | Compatibility/research alias emphasizing that the edge is apparent. |
| `polymarket_fee_source` / `kalshi_fee_source` | Per-venue fee source used in the review edge estimate, such as custom model, configured bps, venue metadata, or fallback default. |
| `fee_source_status` | `explicit` when both venue fee sources are explicit; `fallback` when either side relies on a default or missing-fee assumption. |
| `fee_sensitivity_dollars` | Net edge after an additional one-cent per-contract fee stress; reviewer-facing only. |
| `slippage_sensitivity_dollars` | Net edge after spread/depth slippage stress; reviewer-facing only. |
| `arbitrage_direction` | Human-readable edge direction. |
| `max_executable_size_contracts` | Top-of-book or depth-aware executable size. |
| `top_of_book_max_size` | Top-of-book size alias for candidate review workflows. |
| `received_at_utc` | Local quote receive timestamp. |
| `exchange_ts_utc` | Exchange quote timestamp when available. |
| `quote_age_ms` | Age of the underlying quotes. |
| `polymarket_quote_age_ms` / `kalshi_quote_age_ms` | Per-venue quote ages. |
| `quote_age_threshold_ms` | Review quote-age threshold applied when deriving quote quality and sensitivity status. |
| `quote_age_sensitivity_status` | Reviewer-facing threshold result such as `within_threshold`, `stale_at_threshold`, or missing/invalid timestamp status. |
| `local_sequence` | Local collector sequence. |
| `venue_sequence` | Venue websocket sequence when available. |
| `polymarket_local_sequence` / `kalshi_local_sequence` | Per-venue local collector sequences. |
| `polymarket_venue_sequence` / `kalshi_venue_sequence` | Per-venue exchange sequence values. |
| `book_hash` | Venue book hash when available. |
| `valid_state` | Whether the quote state was valid. |
| `polymarket_valid_state` / `kalshi_valid_state` | Per-venue quote validity states. |
| `quality_flags` | Quote quality flags. |
| `risk_flags` | Semicolon-delimited warnings. |
| `review_required_reason` | Human-readable reason the row requires manual review. |
| `review_status` | review, review_high, rejected, etc. |
| `is_research_candidate` | Always true for research candidate outputs. |
| `execution_ready` | Always false; outputs are not order instructions. |

Generated arbitrage review frames also include venue and review-enrichment
columns from `ARBITRAGE_CANDIDATE_COLUMNS`, including:

- Polymarket identifiers, text, status, quote, depth, and book metadata:
  `polymarket_market_key`, `polymarket_instrument_key`,
  `polymarket_token_ids`, `polymarket_question`, `polymarket_category`,
  `polymarket_close_time`, `polymarket_status`, `polymarket_bid`,
  `polymarket_ask`, `polymarket_spread`, `polymarket_bid_size`,
  `polymarket_ask_size`, `polymarket_depth`, `polymarket_book_token_id`,
  `polymarket_book_ts`, `polymarket_book_observations`,
  `polymarket_quote_source`, and `polymarket_quote_provenance`.
- Polymarket contract evidence: `polymarket_resolution_source`,
  `polymarket_rules`, `polymarket_time_cutoff`, and
  `polymarket_inclusivity`.
- Kalshi event/market identifiers, text, status, quote, and depth metadata:
  `kalshi_event_ticker`, `kalshi_event_title`, `kalshi_market_key`,
  `kalshi_instrument_key`, `kalshi_question`, `kalshi_category`,
  `kalshi_close_time`, `kalshi_status`, `kalshi_bid`, `kalshi_ask`,
  `kalshi_spread`, `kalshi_bid_size`, `kalshi_ask_size`, and
  `kalshi_depth`, `kalshi_quote_source`, and `kalshi_quote_provenance`.
- Kalshi contract evidence: `kalshi_resolution_source`, `kalshi_rules`,
  `kalshi_time_cutoff`, and `kalshi_inclusivity`.
- Scoring and persistence fields: `polymarket_contract_type`,
  `kalshi_contract_type`, `event_score`, `title_similarity`, `entity_score`,
  `contract_equivalence_score`, `close_time_distance_hours`, `gross_edge`,
  `net_edge`, `max_executable_size`, `fee_buffer` (legacy v1 name for the
  modeled fee estimate),
  `apparent_edge_observations`, `apparent_edge_positive_observations`,
  `apparent_edge_seconds`, `apparent_edge_positive_ratio`,
  `max_observed_net_edge`, and `median_observed_net_edge`.

## Passive quote replay schemas

The passive quote replay CLI writes local analysis artifacts, not exchange fill
evidence and not order instructions. Its `manifest.json` is a `run_manifest.v1`
with row counts and schema versions for:

- `passive_quote_evaluation.v1`: one row per quote proposal evaluation,
  including activation-book evidence, fill status, and post-only rejection flag.
- `passive_fill.v1`: one hypothetical fill row per replayed quote fill,
  including opposing top-of-book evidence, fee dollars, and fill confidence.
- `passive_markout.v1`: markout rows keyed by quote and window, including
  mark lag, mark mid, gross PnL, fees, net PnL, and markout reason.

Replay summaries and manifests must label these artifacts as
`artifact_scope=local_exploratory`, `review_only=true`, `execution_ready=false`,
and `real_order_routing_enabled=false`.

## Run manifest schema: `run_manifest.v1`

A reproducibility record for collection and analysis runs.

| Column | Meaning |
| --- | --- |
| `schema_version` | Always `run_manifest.v1`. |
| `run_id` | Stable run id. |
| `started_at_utc` | Run start timestamp. |
| `ended_at_utc` | Run end timestamp. |
| `status` | success, partial, or failed. |
| `command` | CLI command or script invocation. |
| `git_commit` | Backward-compatible caller-checkout commit when available. |
| `dataset_paths` | Input/output dataset paths. |
| `schema_versions` | Schema versions written or read. |
| `row_counts` | Row counts by dataset. |
| `quality_flag_counts` | Quality flag counts by dataset. |
| `venue_counts` | Row/event counts grouped by venue when available. |
| `instrument_counts` | Row/event counts grouped by instrument when available. |
| `reconnect_count` | Number of websocket reconnect events observed by the run. |
| `sequence_gap_count` | Number of sequence-gap events observed by the run. |
| `resync_event_count` | Combined reconnect and sequence-gap resynchronization events. |
| `error_type` | Failure type when status is failed or partial. |
| `error_message` | Failure message when status is failed or partial. |
| `notes` | Free-form notes. |

JSON manifests produced by `build_run_manifest()` also include non-registry
extension fields: `run_dir`, `pmkt_core_version`, `pmkt_core_commit` when the
installed/source provenance can be resolved, and `caller_git_commit`. A consumer
may add its own implementation provenance; the private trading package adds its
commit and recovers the exact core commit from its pinned dependency when a plain
wheel install does not carry VCS metadata.

## Migration guidance

Near-term code can keep accepting legacy/ad hoc columns for compatibility. New
persisted research datasets should prefer these schemas. Matching and arbitrage
outputs should gradually add stable ids that link back to `market.v1`,
`instrument.v1`, `topbook.v1`, and `market_match.v2` records.
