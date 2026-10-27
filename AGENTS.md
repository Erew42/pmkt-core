# Public Core Agent Instructions

## Repository boundary

This is the `pmkt-core` Git root. Run Git, tests, and repository scripts here.
It builds the public `pmkt` distribution from `src/pmkt`. It is a
read-only prediction-market data plane. Keep venue REST/WebSocket reads,
canonical schemas, storage, capture, reconstruction, and
resolution here.

Do not add matching policy, tracking, opportunity selection, strategies,
execution/OMS/risk, operator dashboards, private-key loading, credential
derivation, authenticated user streams, or venue order submission/cancellation.
Those belong to the private `pmkt-trading` consumer. Core must never import
`pmkt_trading` or any removed legacy private package.

The workspace parent and sibling repositories are not part of this Git root.
Never copy their ignored `data`, `generated`, `tmp`, environments, credentials,
or workspace artifacts into this repository.

## Design and complexity

Prefer straightforward implementations with explicit data flow and interfaces.
Add abstractions, configuration, or dependencies when they solve a current
problem or simplify actual reuse; avoid frameworks for hypothetical consumers.
Treat complexity as a maintenance cost, not a line-count target. Preserve
required validation, provenance, compatibility, and the read-only boundary when
simplifying. Reusable library infrastructure can justify more structure than a
single research script.

When asked to grill an idea or review a design, focus on public API and schema
contracts, data grain and time semantics, provenance, and consumer compatibility.
Check whether the proposed behavior belongs in core before discussing its
implementation. Apply these priorities to the affected surface; a local helper
change does not require reviewing every package contract.

## Compatibility and contracts

- Supported Python versions are 3.10 through 3.12.
- Keep the import name and console script `pmkt`.
- Keep generated OpenAPI examples untracked under `generated/`.
- Preserve canonical schema versions, grain, identifiers, timestamps, and
  provenance unless a deliberate schema migration is part of the change.
  Removing or renaming a persisted column, changing its meaning, or no longer
  keeping a raw field counts as such a migration.
- Normalize venue payloads at one boundary. Keep the raw payload or its hash
  where the schema or workflow requires it; `recording.v1` deliberately keeps
  raw messages off by default (`RecordingOptions.raw_messages=False`).
  Artifact, normalized-row and source-payload hashes are not interchangeable.
- Keep Kalshi authentication behind the narrow read-auth protocol. Core must
  reject write methods before invoking an authenticator or network transport.

### Where facts live

Read or change these at their source; do not restate them in this file.

- Columns, dtypes, units and primary keys: `src/pmkt/data/registry.py`
  (`pmkt schema show <version>`).
- Column meaning and the timestamp, price and raw-retention conventions:
  `docs/data_dictionary.md`.
- Schema status and retirement: `docs/schema_lifecycle.md` and
  `docs/schema_lifecycle.json`.
- Venue quirks: comments next to the code that parses the field.

### Changes that travel together

| If you change | Also update in the same PR | Checked by |
| --- | --- | --- |
| A registered schema or its columns | `docs/data_dictionary.md`, `docs/schema_lifecycle.json` | `tests/test_schema_registry_validation.py`, `tests/test_schema_lifecycle.py` |
| The supported CLI | `CLI_COMMANDS.md` | review |
| A Polymarket endpoint | `openapi/polymarket.min.json`, before the code | review |

The checks are partial. The dictionary test only finds each column name
somewhere in `docs/data_dictionary.md`, and the lifecycle test only requires a
record per registered version. Neither checks that a description is correct.

## Pull request descriptions

Write for the owner deciding whether to merge the current head, who has not
seen the agent session.

Title: imperative and behavior-level, under 72 characters, no type prefix.

Start the body with **<Type>:** and one or two sentences: the problem and what
this PR does about it. State the problem so it would stay true under a
different fix. Types: feature, bug fix, refactor, pin bump, docs, research,
chore. Put `Closes #N` or stacking notes on the next line. Then:

- **Scope**: the one problem this PR solves, and related work deliberately not
  done. If the changes cover more than one independent problem, split the PR.
- **Changes**: bullets grouped by behavior, not by file.
- **Contracts**: `none`, or each change with a reason: schema versions and
  columns added, removed or retyped; raw fields no longer kept; public API or
  CLI changes.
- **Review focus**: one to three places where a mistake would be costly. Omit
  for trivial PRs.
- **Validation**: commands run, their results, the commit, Python version and
  OS. Name failures that also occur on `main`. List skipped checks as
  `Not run: <check> (<reason>)`. Do not list checks CI already runs.
- **Follow-ups** (optional): known gaps, with issue links.
- **Suggested AGENTS.md changes** (optional): rules this work showed are
  missing. The owner decides.

End with `Created with <model name> (<reasoning effort> reasoning)`. Use the
exact model identity provided by the runtime when available; otherwise use the
model family, such as GPT, Gemini, or Claude. Omit reasoning effort if
unavailable. Never guess either value.

Keep most PRs under 40 lines. After each push, rewrite the body to match the
head; put review rounds and history in PR comments. Leave out conversation
history, abandoned approaches and local paths. The headings are in
`.github/pull_request_template.md`.

## Editing this file

Do not add rules here to record a one-off fix; add a test instead. Propose new
rules under "Suggested AGENTS.md changes" in the PR description.

## Verification

Run focused tests while working. Before a commit, PR, or readiness claim run:

```bash
python scripts/check_repo_hygiene.py
python scripts/check_pytest_lane_coverage.py .github/workflows/tests.yml tests
python -m ruff check .
python -m mypy src
python -m pytest -q
```

For contract changes also run `python scripts/contract_check.py`. After OpenAPI
changes run `python scripts/update_openapi_examples.py`; after upstream snapshot
changes run `python scripts/sync_upstream_docs.py`.

Generated datasets, reports, databases, logs, keys, and secrets must remain in
ignored local directories. No license should be invented or added implicitly.
