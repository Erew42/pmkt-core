# Drift Check Runbook

Use this workflow before adding new endpoints or merging changes that touch API contracts.
Snapshot and example-refresh commands write into `generated/` by default
(ignored by git). Live contract checks and parameter validation do not write
generated files.

Freshness pass: 2026-05-28. The same live drift steps are also scheduled in
`.github/workflows/drift-check.yml` on Mondays at 12:00 UTC and can be run
manually with `workflow_dispatch`.

## 1) Snapshot upstream docs
```bash
python scripts/sync_upstream_docs.py
```
Sources are configured in `docs/api/upstream_sources.json`.

Outputs: `generated/upstream_snapshots/<source>/...` and
`generated/upstream_snapshots/index.jsonl`. Keep raw upstream snapshots ignored;
if a snapshot finding should become public, write a cleaned summary into
`docs/api/`.

## 2) Run contract checks
```bash
python scripts/contract_check.py
```
Outputs: console summary (use `--json` for machine-readable output).
See `docs/api/contract_checks.md` for options and defaults.

## 3) Refresh OpenAPI examples + manifest
```bash
python scripts/update_openapi_examples.py
```
Outputs: example payloads under `generated/openapi/examples/<endpoint>/` and
`generated/openapi/examples/manifest.json`.

If the auto-selected token has no `/prices-history` response, rerun with
`--token-id <known_token_id>`; unlike `contract_check.py`, this script treats
non-2xx example fetches as failures.

## 4) Validate the checked-in OpenAPI contract
```bash
python -m pytest tests/test_openapi_params.py -q
```

This compares `openapi/polymarket.min.json` to
`openapi/param_contract.json`, checks the repo-supported CLOB read endpoints
are present, and confirms trading methods are not exposed. It does not validate
against current upstream docs.

## 5) Run repository hygiene and normal gates
```bash
python scripts/check_repo_hygiene.py
python -m ruff check .
python -m mypy src
python -m pytest -q
```

For package/API surface changes, also rely on the packaging smoke job in
`.github/workflows/tests.yml`: it builds a wheel, force-installs it, imports key
nested modules, and verifies `pmkt --help`.

## Notes
- Use `--output-dir` only for local generated outputs. Do not commit raw
  snapshots or live example payloads unless they have been deliberately cleaned
  and reviewed.
- The OpenAPI spec remains static unless endpoint coverage changes; example
  payloads live in the generated manifest for agent and drift review.
