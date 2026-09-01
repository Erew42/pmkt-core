# Repository split ownership manifest

This manifest records the extraction boundary from the original combined
repository. Rules are evaluated from most specific to least specific; anything
not explicitly assigned defaults to private so a new surface cannot leak into
the public repository by accident.

## Public core

The following paths are owned by `pmkt-core`:

- `src/pmkt/{__init__,_http,config,models,pagination,tokens}.py` and
  `src/pmkt/py.typed`.
- `src/pmkt/data/**`, except semantic sports-corpus and matching-policy code.
- `src/pmkt/exchanges/**`, except credential/private-key loaders, generic signed
  transports, authenticated user streams, and SDK execution clients.
- `src/pmkt/streaming/**`, including the neutral feed supervisor and injected
  feed-state sink protocols.
- `src/pmkt/market_structure/**`, `src/pmkt/resolution/**`, and generic
  `src/pmkt/text/**` utilities and taxonomy data.
- The core CLI modules and commands documented in `CLI_COMMANDS.md`.
- `openapi/**`, `docs/api/**`, `docs/data_dictionary.md`,
  `docs/schema_lifecycle.{md,json}`, and
  `docs/storage_profile_capture_runbook.md`.
- Read-only capture/contract/example scripts retained under `scripts/` and the
  tests for the public modules.
- The two normalized Kalshi order-book fixtures under `tests/fixtures/`.

## Private trading

The following original paths are owned by `pmkt-trading` and must not be
present in a core wheel:

- `src/pmkt/{matching,tracking,opportunities,strategies,execution,cross_platform}/**`.
- `src/pmkt/auth.py`, `src/pmkt/polymarket_paper_canary.py`,
  `src/pmkt/data/sports_corpus.py`, `src/pmkt/exchanges/kalshi/auth.py`, and
  `src/pmkt/exchanges/polymarket/sdk.py`.
- Matching, tracking, opportunity, paper/live, credential, deployment,
  execution, ledger, alert, soak, runtime-backup, and canary CLI modules.
- `apps/**`, `.streamlit/**`, `test_support/deployment.py`, matching/review
  fixtures, and their corresponding tests.
- Original matching, trading, execution, dashboard, architecture, refactor,
  roadmap, review, and operational documents and scripts.

Private code moves to the `pmkt_trading` namespace. It depends on core; core
never imports it. No compatibility copy remains under the legacy `pmkt`
private-module paths.

## Tailored independently

Each repository owns its own `README.md`, `AGENTS.md`, `CLI_COMMANDS.md`,
`pyproject.toml`, `.gitignore`, pull-request template, workflows, test
configuration, hygiene/archive scripts, and repository-boundary tests. Mixed
CLI, configuration, schema-validation, and streaming tests are split so each
repository verifies only its own side of the interface.

## Default and review rule

A new tracked path not covered above is private until this manifest and the
automated boundary tests are deliberately updated. Canonical data schemas may
remain in core even when private trading consumes them; semantic policy and
decision logic do not.
