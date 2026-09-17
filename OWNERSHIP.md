# Repository split ownership manifest

This manifest records the extraction boundary from the original combined
repository. Rules are evaluated from most specific to least specific; anything
not explicitly assigned defaults to private so a new surface cannot leak into
the public repository by accident.

## Public core

The following paths are owned by `pmkt-core`:

- `src/pmkt/{__init__,_http,_observations,_operation,catalog,config,errors,models,pagination,records,tokens}.py` and
  `src/pmkt/py.typed`.
- `src/pmkt/data/**`, except semantic sports-corpus and matching-policy code.
- `src/pmkt/exchanges/**`, except credential/private-key loaders, generic signed
  transports, authenticated user streams, and SDK execution clients.
- `src/pmkt/streaming/**`, including the neutral feed supervisor and injected
  feed-state sink protocols.
- `src/pmkt/resolution/**` and generic
  `src/pmkt/text/**` utilities and taxonomy data.
- The core CLI modules and commands documented in `CLI_COMMANDS.md`.
- `openapi/**`, `docs/api/**`, `docs/data_dictionary.md`,
  `docs/public_api.md`, `docs/schema_lifecycle.{md,json}`, and
  `docs/storage_profile_capture_runbook.md`.
- Read-only capture/contract/example scripts retained under `scripts/` and the
  tests for the public modules.
- The two normalized Kalshi order-book fixtures under `tests/fixtures/`.
- `tests/test_public_api_inventory.py` and
  `tests/fixtures/public_api_inventory.json`, which classify the current public
  facade without reserving future workflow modules.
- `tests/test_catalog.py` and `scripts/create_synthetic_catalog_fixture.py`,
  which verify the pinned read-only catalog contract and provide its offline
  synthetic fixture recipe.
- The supported Polymarket discovery, typed-book, and sampled CLOB history
  workflows, their private decoders, OpenAPI contracts, offline examples, and
  focused records/Gamma/CLOB/package tests.
- The supported Kalshi standard-market discovery, source-scoped detail,
  projected current-book, and explicit-window candle workflows, their private
  decoders and routing planner, offline examples, and focused
  records/Kalshi/package tests.
- The supported typed single-market and ordered bounded-batch resolution
  workflows, explicit borrowed Polygon CTF evidence client, v2/v3 cache and
  label compatibility, sanitized evidence errors, offline examples, and
  focused resolution/package tests.

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
decision logic do not. Core validation is limited to physical types, required
fields, identifiers, enumerated representations, arithmetic consistency, and
other policy-neutral artifact integrity. Approval, tracking eligibility,
execution permission, risk gates, strategy compatibility, and allowed-consumer
decisions are applied by `pmkt_trading.data.validation`.
