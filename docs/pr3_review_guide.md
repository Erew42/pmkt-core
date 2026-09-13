# PR #3 owner review guide

This guide follows the review of `f90c242` and the targeted fixes made on
2026-09-13. PR #3 remains a draft. The confirmed defects below are addressed;
that is not completion of the original public-API milestone or approval to merge.

The branch is rebased onto `53455c3`, which includes merged PRs #4 and #5.
Earlier slice commit IDs changed during that rebase. The review order below
uses the current IDs. Start with the follow-up fix diff, then review the API
contracts that matter to your use cases.

## Assessment of Claude's findings

| Finding | Assessment and action |
| --- | --- |
| Sparse Kalshi live candles abort the call | Confirmed with regression tests and retained public responses. Missing live traded OHLC fields now become null; available previous/mean prices remain present. Existing missing/partial flags describe the result. Bid/ask layouts, historical layouts, and supplied numeric values retain validation. |
| `from_values` settings cannot be pickled | Confirmed. Restored the fix from local-only `76068f2`, which was absent at the reviewed PR head. Rebuild the runtime class from its public class and restore saved Pydantic state without rereading environment or dotenv settings. |
| Uppercase condition IDs fail discovery/book/history checks | Confirmed in both case directions. Compare condition IDs case-insensitively while preserving supplied identifiers and observed payload spelling. Token and market IDs still use exact matching. |
| Old catalogs have no artifact `schema` | Owner selected validated legacy support. Infer the expected schema only when the key is absent, check exact unique column names in every file in both modes, and retain full value/hash validation. Present invalid/null declarations still fail. Inference is recorded and does not rewrite evidence. |
| Older single-file descriptor | Additional issue found while checking the actual first release: its `parquet_file` format omits `parquet_file_count`. Accept that legacy descriptor only for an actual single file without a schema declaration; infer one file and report the absent declaration. Other missing counts and format mismatches still fail. |
| `MarketNotFoundError` cannot be pickled/copied | Confirmed. Restore its keyword-only lookup identity and attached exception context; pickle, shallow-copy and deep-copy tests cover it. |
| Catalog matrix described as a required merge gate | Corrected wording: the workflow runs in CI. Repository merge-rule enforcement is separate from the API contract. No repository protection settings were changed. |
| `HttpClient.timeout_s` rejects values httpx previously accepted | Confirmed compatibility restriction, retained and explicitly documented: finite positive numbers only. `None`, zero and `httpx.Timeout` objects are unsupported. Review this at the consumer pin bump. |
| More than six routing timestamp fractional digits rejected | Confirmed intentional restriction, retained and documented. Unlike catalog ingestion, this timestamp decides which dataset is queried. Truncating both sides could turn a strictly-before relationship into equality; widening precision needs a deliberate routing contract. |
| Catalog pandas conversion lacks the optional-dependency error | Confirmed older low finding. Added the same explicit `OptionalDependencyError` convention used by history conversions and a DataFrame return annotation; Arrow access still works without calling pandas conversion. |
| Whole-batch deadline loses completed return values | Confirmed specified behavior, retained and made explicit in the API guide. A failure returns no partial list. This does not promise rollback of any independently written cache entries. |

The partial-OHLC case is a defensive extension of missing-value semantics;
previous-only/no-OHLC periods are the form reproduced in the saved live evidence.
Neither form supplies an invented trade price. Native payloads remain available.

## What to read first

1. Read [the public API guide](public_api.md) as the compatibility promise.
   Concentrate on configuration, failure behavior, time windows, provenance,
   catalog validation levels, and the distinction between native and normalized
   methods. Decide whether those promises suit your research workflows.
2. Review the follow-up changes with `git show` on the review-fix commit(s).
   Focus on `_live_price`, `_preflight_candle_layout`, `load_history_manifest`,
   `_init_only_variant`, `MarketNotFoundError.__reduce__`, and the condition-ID
   comparisons. The tests listed below are concrete examples of each contract.
3. Review runtime and resolution closely. These affect existing callers and
   determine whether cached final outcomes are trusted. A green test run cannot
   choose the batch failure policy for your application.
4. Review the two history workflows and catalog boundaries. For discovery and
   book workflows, concentrate on pagination/termination, identity checks, and
   the Kalshi YES/NO price-and-quantity projection.
5. Skim repetitive exports and generated inventories after checking that the
   public names match the guide. Use schema/OpenAPI changes to verify the
   relevant contracts rather than treating all generated lines as hand-written
   business logic. Optional deduplication is not a reason to redesign the PR.

## Follow-up commits to review first

| Commit | Targeted change |
| --- | --- |
| `df388fd` | Restore the existing configuration pickle fix |
| `99d9884` | Make lookup errors picklable and copyable |
| `e2cccdc` | Preserve missing live Kalshi traded prices |
| `c8488f3` | Match Polymarket condition IDs case-insensitively |
| `e83bd27` | Validate and open legacy catalog descriptors; guard pandas conversion |

For the complete implementation follow-up, use `git diff fa8f92c..e83bd27`.
The final documentation commit updates this guide and the public API contract.

## Existing slice commits after the rebase

Use `git show <commit> -- <path>` to narrow a slice. New review-fix commits are
listed after these in `git log --reverse origin/main..HEAD`.

| Commit | Slice | Suggested depth |
| --- | --- | --- |
| `699c7ba` | Configuration, request runtime, typed references | Close: `config.py`, `_http.py`, `_operation.py`, references |
| `1b698af` | Kalshi candles | Close: `_history.py` and `AsyncKalshiClient.get_candles`; read together with the sparse-price fix |
| `ef29323` | Single resolution and compatible v2 cache reuse | Close: resolver models, cache, terminal labels and typed identity |
| `5a3b5f4` | Ordered bounded batch resolution | Close on `_batch.py` and failure/cancellation semantics |
| `eae26cc`, `4581fe3` | Pinned catalog and path relocation | Medium: reference identity, containment, validation and managed SQL boundaries |
| `1f76536` | Kalshi discovery and current books | Medium, especially the YES/NO projection and capability qualification |
| `52535af`, `29ab167` | Polymarket discovery/books and CLOB history | Medium: condition IDs, limits, sampling windows and invalid-row policies |
| `7e0f110`, `28d8faf`, `b2372d8`, `cce631d`, `fa8f92c` | Inventory and earlier compatibility fixes | Skim alongside the affected feature; do not rely on historical doc wording where this follow-up replaces it |

## Tests to use as executable examples

- `tests/test_runtime.py`: `test_from_values_instances_pickle_without_rereading_settings`
  and `test_market_not_found_error_survives_pickle_and_copy`; also the existing
  runtime deadline, retry and old-signature subclass tests.
- `tests/test_kalshi_history.py`: `test_live_sparse_trade_prices_preserve_quotes_and_missing_evidence`,
  `test_sparse_price_does_not_relax_quote_layout`,
  `test_sparse_price_invalid_value_still_obeys_row_policy`, source-layout
  separation tests, cutoff boundary tests and daily/DST cases.
- `tests/test_gamma.py`, `tests/test_clob.py`, `tests/test_clob_history.py`:
  the new case-insensitive condition tests alongside existing mismatch rejection.
- `tests/test_catalog.py`: `test_legacy_schemaless_catalog_is_validated_and_reopens`,
  `test_catalog_present_invalid_schema_is_not_legacy`,
  `test_legacy_schema_requires_exact_columns`,
  `test_legacy_schema_keeps_full_value_validation_and_metadata_limits`,
  `test_legacy_single_file_descriptor_without_count`, and the existing tampering,
  relocation, parameterization and SQL-access boundary tests.
- `tests/test_resolution_batch.py`: ordering/duplicates, shared expiry and worker
  cancellation. `tests/test_resolution_cache.py` and
  `tests/test_resolution_terminal_labels.py`: retained final outcomes and conflicts.

## Validation and evidence

Final local validation on the complete implementation: **1,870 passed, 2 skipped**
in 546.10 seconds on Python 3.10. Repository hygiene, pytest-lane coverage, Ruff,
mypy (130 source files), diff whitespace and public book/price/midpoint/history
contract checks passed. Installed-wheel examples and typing checks are included
in the full suite. Current-head GitHub CI status is recorded separately in the PR;
the older green run at `f90c242` does not qualify this updated head.

Offline replay used ten retained public candle responses from 2026-09-12, across
five KXFED markets and hourly/daily intervals. All ten public `get_candles` replays
succeeded with zero rejected rows. The first hourly response decoded all 219
rows, including 199 with no traded OHLC; 218 remained after original-window
filtering. These are replays, not fresh live qualification or complete-market
coverage claims. The retained input SHA-256 is
`a63bb8a2f9e495630ee6a9ec0dd73f1173a7cc09d92b1a0efcc417ece37a2d8f`.
Synthetic regression fixtures reproduce its sparse shape; operational datasets
and generated review evidence are not committed.

Both actual schema-less releases now open without modification in metadata
mode: `market_history_20260822T204300Z` has 53,991,846 rows and
`market_history_20260822T225500Z` has 53,991,956. These checks validate the real
file metadata and columns, not a new full scan of all values and hashes.
Synthetic tests cover full-mode value rejection, malformed columns, declaration
conflicts, and saved-reference reopening. The final focused catalog suite
passed 79 tests, including the single-file and optional-pandas regressions.

## Decisions and work still open

- **Batch progress:** accept all-or-nothing return semantics, or plan a separate
  partial-result API. For now, callers needing saved progress can submit smaller
  batches and persist each result, while enforcing their own overall deadline.
- **Catalog trust/cost:** full validation remains the default and scans content.
  Metadata validation checks structure and columns but skips hashes and value
  checks; it is faster and provides weaker evidence. Legacy support does not
  make these modes equivalent.
- **Original milestone:** production catalog source guards, including targeted
  conflict reads during compaction (ID-02), and final documentation/migration
  and clean-install qualification (M1) remain outstanding.
- **RPC evidence:** no fresh Polygon RPC qualification was performed; this needs
  an explicit endpoint and bounded test plan.
- **Consumer integration:** a later trading pin-bump must update exact revision
  expectations, both `_request` overrides and the authority snapshot, then run
  consumer qualification against the installed candidate. No private consumer
  code or dependency pins are changed here.
- **Low-priority hardening:** observation construction in `finally`, record
  construction checks, and helper duplication remain review notes, not newly
  confirmed production failures. Broad helper/RPC refactors are deferred.

Keep the PR draft until the remaining scope is completed or explicitly narrowed,
you have reviewed the chosen contracts, and final candidate checks are green.
