# Public Core Agent Instructions

## Repository boundary

This repository builds the public `pmkt` distribution from `src/pmkt`. It is a
read-only prediction-market data plane. Keep venue REST/WebSocket reads,
canonical schemas, storage, capture, reconstruction, market structure, and
resolution here.

Do not add matching policy, tracking, opportunity selection, strategies,
execution/OMS/risk, operator dashboards, private-key loading, credential
derivation, authenticated user streams, or venue order submission/cancellation.
Those belong to the private `pmkt-trading` consumer. Core must never import
`pmkt_trading` or any removed legacy private package.

The workspace parent and sibling repositories are not part of this Git root.
Never copy their ignored `data`, `generated`, `tmp`, environments, credentials,
or workspace artifacts into this repository.

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
