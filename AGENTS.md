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
- Update `CLI_COMMANDS.md` with supported CLI changes.
- Update `openapi/polymarket.min.json` before adding a Polymarket endpoint.
- Keep generated OpenAPI examples untracked under `generated/`.
- Preserve canonical schema versions, grain, identifiers, timestamps, and
  provenance unless a deliberate schema migration is part of the change.
- Keep Kalshi authentication behind the narrow read-auth protocol. Core must
  reject write methods before invoking an authenticator or network transport.

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
