# Schema lifecycle and deprecation evidence

This document is the maintainer-facing guide to the schema lifecycle catalog in
[`docs/schema_lifecycle.json`](schema_lifecycle.json). The catalog covers every
version returned by `pmkt.data.registry.list_table_specs()` exactly once. Tests
fail if a registry version is added or removed without a lifecycle decision.

This is a literal-reference and artifact inventory plus reviewed semantic
evidence, not deletion authority. A schema's status does not authorize
rewriting, moving, or deleting an artifact.

## Status model

- `active_core`: current capture, ingestion, or consumer persistence behavior
  depends on the contract; this status is distinct from repository ownership.
- `active_experiment`: research, paper, canary, or deferred execution code owns
  the contract. It is retained conservatively even when not routinely run.
- `compatibility_legacy`: the contract may be needed to read older data.
- `provisional_unintegrated`: the registry contains the contract, but no
  end-to-end producer and reader has been established.
- `removal_candidate`: current evidence favors retirement, subject to every
  stop condition in the catalog.

The execution, paper, canary, and soak schemas remain `active_experiment` and
frozen for this phase except for the six-schema paper-canary family explicitly
approved in the
[`0.2 retirement packet`](schema_retirements/paper_canary_contract_family_0_2.md).
That exception ends the freeze only for the named family; simplification or
removal of the remaining contracts is deliberately out of scope.

## Current decisions

| Contract | Current status | Decision |
| --- | --- | --- |
| `polymarket_market_snapshot.v2` / `kalshi_market_snapshot.v2` | `removal_candidate` | Retire later if exhaustive artifact and external-consumer evidence remains zero. Do not rename a persisted schema version. Preserve useful trimming only as an explicitly non-authoritative projection. |
| `market_match.v1` | `compatibility_legacy` | Keep registered while the legacy-persistence claim is checked. The current match-registry projection is not a safe v1 reader because it does not map v1 keys before selecting v2 columns. |
| `event.v1` / `market.v1` | `provisional_unintegrated` | Propose retirement after exhaustive zero-use evidence. Active venue-specific market frames are separate contracts. |
| `instrument.v1` | `provisional_unintegrated` | Retire unless a future proposal supplies a writer, stable identity and update semantics, a persistence location, and consumer joins. |
| `market_taxonomy_evidence.v1` | `active_experiment` | Retain. It has hash-pinned retained research data, but still needs a tracked producer/reader workflow and artifact-level schema, grain, key, and provenance metadata. |
| Paper-canary contract family (six schemas) | `active_experiment` pending approved removal | Retire atomically during the 0.2 breaking-release window under the tracked family retirement packet. The remaining `execution_deferred` contracts stay frozen. |

The evidence baseline recorded on 2026-08-20 found no snapshot-v2,
`market_match.v1`, `event.v1`, `market.v1`, or `instrument.v1` artifacts in the
main retained roots or a representative `tmp` sample. That sample contained
unreadable paths and was not exhaustive, so it is not sufficient for removal.

## Post-split ownership and compatibility

The 2026-09-11 source baseline names each repository and revision separately in
`evidence_as_of.repositories`. Research has no committed revision yet; its
source inventory cannot be reproduced from a Git SHA. The older August scan is
retained as `historical_evidence_as_of`, not represented as a current core commit.
Historical paths in `evidence_overrides` describe the combined layout.

`repository_ownership` covers every registered version. Core owns physical
contracts and policy-neutral validation. `cross_repository_contract` entries
have private semantic policy in `pmkt-trading`; registration or public export
is not an ownership violation. Existing exports remain compatible. No schemas
are removed by this refresh, and the five proposed dimension/snapshot removals
still require complete retained-artifact and consumer evidence.

The dashboard's generic reader is now
`pmkt-trading:src/pmkt_trading/dashboard/data/artifacts.py`. Literal token scans
cannot enumerate its runtime manifest-supplied versions. Review it and actual
producer functions before concluding that a schema is unused.

## Reproducing the evidence report

Run the scanner from the repository being inventoried. Public core's default
text roots are `src`, `scripts`, `tests`, and `docs`. It declares no artifact
roots because core intentionally holds no workspace datasets:

```powershell
python scripts/inventory_schema_usage.py --allow-incomplete
```

This produces a source-only report, with `artifact_roots_not_declared` as an
explicit removal-evidence blocker. Without `--allow-incomplete`, its exit code
is 2. A successful source scan is not zero-use evidence for retained artifacts.
Every report records the repository name, Git HEAD, and dirty/untracked status.
Use an explicit `--catalog` to keep the public catalog authoritative when
scanning a consumer checkout. For example, from the trading Git root:

```powershell
python ../pmkt-core/scripts/inventory_schema_usage.py --root . `
  --catalog ../pmkt-core/docs/schema_lifecycle.json `
  --artifact-root data --artifact-root generated --artifact-root local_data `
  --artifact-root tmp --output tmp/schema_usage_inventory.json
```

Declare the real retained roots, including any external archival locations;
missing roots remain errors. For a research source scan, provide its actual
`--text-root` directories explicitly. Keep generated reports in ignored local
locations; never copy consumer datasets into core.

Reports distinguish registry, public-export, source, tests, scripts, docs,
notebooks, manifests, and persisted Parquet. References are exact literal
registered-version tokens, not proof of semantic producer/reader behavior.
Parquet inspection uses at most eight workers by default; `--parquet-workers`
can lower this bound. `--allow-incomplete` permits exploration but never turns
incomplete attribution into removal evidence.

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

Actual removal belongs in a later phase and normally uses a separate commit per
contract. One atomic commit may instead retire a producer-owned contract family
when one reviewed removal packet covers every member and records why splitting
the change would leave partial producer contracts behind. A removal packet must
contain:

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

Except for the explicitly approved paper-canary retirement, this lifecycle
phase does not perform structural registry rewrites, broad public API cleanup,
dataset moves, per-file reorganization, execution cleanup, or artifact
conversion. Exact run directories and evidence bundles remain byte-identical.
Catalog and report updates are reversible metadata changes.
