# Schema lifecycle and deprecation evidence

This document is the maintainer-facing guide to the schema lifecycle catalog in
[`docs/schema_lifecycle.json`](schema_lifecycle.json). The catalog covers every
version returned by `pmkt.data.registry.list_table_specs()` exactly once. Tests
fail if a registry version is added or removed without a lifecycle decision.

This is a literal-reference and artifact inventory plus reviewed semantic
evidence, not deletion authority. A schema's status does not authorize
rewriting, moving, or deleting an artifact.

## Status model

- `active_core`: current capture, ingestion, matching, tracking, or persistence
  behavior depends on the contract.
- `active_experiment`: research, paper, canary, or deferred execution code owns
  the contract. It is retained conservatively even when not routinely run.
- `compatibility_legacy`: the contract may be needed to read older data.
- `provisional_unintegrated`: the registry contains the contract, but no
  end-to-end producer and reader has been established.
- `removal_candidate`: current evidence favors retirement, subject to every
  stop condition in the catalog.

The execution, paper, canary, and soak schemas remain `active_experiment` for
this phase. Their internal simplification is deliberately out of scope.

## Current decisions

| Contract | Current status | Decision |
| --- | --- | --- |
| `polymarket_market_snapshot.v2` / `kalshi_market_snapshot.v2` | `removal_candidate` | Retire later if exhaustive artifact and external-consumer evidence remains zero. Do not rename a persisted schema version. Preserve useful trimming only as an explicitly non-authoritative projection. |
| `market_match.v1` | `compatibility_legacy` | Keep registered while the legacy-persistence claim is checked. The current match-registry projection is not a safe v1 reader because it does not map v1 keys before selecting v2 columns. |
| `event.v1` / `market.v1` | `provisional_unintegrated` | Propose retirement after exhaustive zero-use evidence. Active venue-specific market frames are separate contracts. |
| `instrument.v1` | `provisional_unintegrated` | Retire unless a future proposal supplies a writer, stable identity and update semantics, a persistence location, and consumer joins. |
| `market_taxonomy_evidence.v1` | `active_experiment` | Retain. It has hash-pinned retained research data, but still needs a tracked producer/reader workflow and artifact-level schema, grain, key, and provenance metadata. |

The evidence baseline recorded on 2026-08-20 found no snapshot-v2,
`market_match.v1`, `event.v1`, `market.v1`, or `instrument.v1` artifacts in the
main retained roots or a representative `tmp` sample. That sample contained
unreadable paths and was not exhaustive, so it is not sufficient for removal.

## Reproducing the evidence report

The scanner is read-only. Without `--output` it writes JSON to standard output:

```powershell
python scripts/inventory_schema_usage.py
```

Write an ignored report and include all local research roots explicitly:

```powershell
python scripts/inventory_schema_usage.py `
  --artifact-root data `
  --artifact-root generated `
  --artifact-root local_data `
  --artifact-root tmp `
  --output tmp/schema_usage_inventory.json
```

The default exit code is nonzero when a declared root or file cannot be read.
`--allow-incomplete` is available for exploratory reporting, but it does not
turn an incomplete report into removal evidence.

Routine checks should scope `--artifact-root` to the dataset under review. The
all-root command is an intentionally exhaustive manual gate and can take several
minutes on the current 100,000-plus-file tree. Parquet inspection uses at most
eight workers by default; override that bound with `--parquet-workers` when local
I/O characteristics require a smaller value.

The default text roots are `src`, `apps`, `scripts`, `tests`, and `docs`. The
report separates registry, public-export, package source, application source,
scripts, tests, documentation, notebooks, manifest text, and persisted Parquet
evidence. Text matches are exact literal registered-version tokens only. They
cannot discover readers that resolve a version dynamically.

For example, the dashboard resolves manifest-supplied versions through
`get_table_spec(schema_version)`. The catalog therefore records reviewed generic
consumers separately, and every removal packet must supplement literal scanning
with reviewed semantic producer/reader evidence.

Parquet row counts are calculated per `schema_version`, including mixed-version
files. The scanner distinguishes:

- no `schema_version` physical column;
- a present column in an empty file; and
- a present column with observed values.

Unreadable files, unknown versions, unversioned Parquet, and empty versioned
Parquet make artifact attribution incomplete. The CLI returns nonzero unless
`--allow-incomplete` is used. That option permits exploratory output only; it
does not make the report removal evidence.

Every catalog entry assigned `removal_candidate` must carry persistence
evidence, reviewed producer and reader lists, tests, a decision, schema-specific
stop conditions, semantic-review notes, and rollback information. Catalog
validation fails when any current or future candidate omits them.

## Removal packet

Actual removal belongs in a later phase and a separate commit per contract. A
removal packet must contain:

1. Reviewed zero-use producer/reader evidence across package source,
   applications, scripts, generic consumers, and external/manual callers;
   literal-reference absence alone is insufficient.
2. A complete scan of every retained and external root.
3. Zero manifest, journal, bundle, or downstream hash-pin references.
4. The public import and external notebook decision.
5. Any required read-only migration, tested against real retained bytes.
6. The exact tests and documentation affected by removal.
7. A rollback procedure that leaves original data untouched.

Unknown keys are dropped by `canonical_row()`, while `coerce_frame()` overwrites
the target schema version and returns only registered columns. Neither function
is a lossless migration mechanism. Raw retained data must be inspected and
validated before any projection.

## Scope boundaries

This lifecycle phase does not perform structural registry rewrites, broad public
API cleanup, dataset moves, per-file reorganization, execution cleanup, or
artifact conversion. Exact run directories and evidence bundles remain
byte-identical. Catalog and report updates are reversible metadata changes.
