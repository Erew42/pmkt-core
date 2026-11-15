# Diagnostics investigation handover

Updated: 2026-11-15. This record separates established changes, intentional
behavior and unresolved investigation candidates.

## Revisions and scope

- Investigation branch: `feat/capture-diagnostics-20261114`.
- Original base: `b1510f6`.
- Reviewed and validated source: `d1e34518505790917a6bfcb5138aeecb18997987`.
- Public read-side work only; no venue writes, credentials or live probes.

The private `pmkt-trading` branch `feat/research-diagnostics-20261114` has a
separate handover with this filename under `docs/roadmap/`. Its diagnostics and
private helper optimizations retain their existing exact core dependency pin;
they do not consume this branch's new raw-inspection command.

## Established findings and proposed PR units

| Finding | Contract, change and evidence | PR boundary |
| --- | --- | --- |
| Feature: inspect sealed raw capture evidence offline. | `inspect-raw-capture SESSION` reuses the verified reader, exhausts chunks, and reports frame/direction/local-event counts, generations, missing chunks/recorded frames and incomplete files. It does not decode venue payloads or establish capture completeness. `d37b991`; `test_raw_inspection.py`. | Independent raw-inspection PR with CLI docs, command inventory and test lane. |
| Packaging defect: retired modules leaked from reused staging into wheels. | Clear only the package staging tree before copying source; reject overlap with `src/pmkt`. Wheel regression seeds a retired module. `b63dad7`. | Focused packaging PR, independent of the private package's equivalent hook. |
| Platform/test clarity. | POSIX signal, directory-sync and wallet-locking requirements are documented. Wallet tests skip when `fcntl` is unavailable; the symlink test skips only observed Windows privilege error 1314. `d32746b`, `d1e3451`. | Separate platform documentation/test-capability PR; no claim of Windows capture support. |

The raw-reader refactor inventories chunks once and delegates the existing
algorithm to `_read_session_chunks`; verification and the public read path
share the same implementation. Inspecting a valid capture with local
`seq_gap`/`disk_pause_gap`, missing chunks, incomplete files, no sealed chunks
or corrupted evidence gives a nonzero CLI result. The report is diagnostic
evidence, not an assertion that every venue event was captured.

## Decisions to preserve

- Compressed hashes are checked before any frames from that chunk are yielded.
  A single combined hash/decompression pass would change the shared reader
  contract in `docs/raw_capture_contract.md`; it was not adopted as a speedup.
- Sealed chunks do not prove a missing suffix or network-level losslessness.
  Collector `FINAL`/`READY` state is not promoted into commit evidence.
- Arbitrary venue payload text is valid raw evidence. Payload interpretation
  belongs to the decoder/reconstruction layer.
- Manifest-only summaries would omit envelope and EOF validation. They are
  not an equivalent shortcut for the verified inspector.
- Temporary dry-run handling (`a2bd2a1`) was removed (`dadd023`). No net dry-run
  change should be extracted. The exercised setuptools 84 toolchain does not
  expose that legacy mode; no assertion is made about every older release.
- `inspect_session` has not been separately promoted into the supported Python
  API inventory. Decide that only if a public Python consumer is intended;
  the documented CLI already supplies the requested interface.

## Validation and limits

At exact source `d1e3451`, the focused raw-reader/inspection, CLI, import and
wheel checks passed 84 tests. Guarded Linux tests for wallet export, lifecycle,
storage, review and RPC, plus symlink-root/import checks, passed 179 tests.
Ruff, mypy (131 source files), repository hygiene and pytest-lane coverage
passed. Packaging and outside-checkout installed-command checks were exercised
for the feature/build changes.

These are historical focused results, not a full native Windows suite claim.
Raw capture runtime and wallet lock/export operations require POSIX. Native
full-suite runs reached those unsupported runtime paths; the matching focused
POSIX tests passed on Linux. No product behavior was changed to imitate those
capabilities on Windows. A stale bytecode-only source directory also caused a
retired-namespace import check to fail locally; that was environment state,
not evidence of a tracked-source defect.

Before a PR, extract one net change against its intended base and rerun the
affected checks at that head. Avoid publishing the add/revert history for
dry-run handling. Reused staging cleanup intentionally trades incremental
copy speed for correct wheel contents.

## Additional investigation: catalog portability

**Confirmed conditional compatibility defect; left for a focused PR.**
Partitioned tree hashes depend on the publisher host's path ordering.
`market_catalog/fs.py::parquet_files` sorts native `Path` objects, and
`market_catalog/reader.py::_resolved_parquet_files` sorts native `PurePath`
objects to match. Mixed-case paths such as `a/part-000000.parquet` and
`B/part-000000.parquet` sort in different orders on POSIX and Windows.
Python documents flavor-specific comparison/case-folding in
[pathlib's general properties](https://docs.python.org/3.12/library/pathlib.html#general-properties).

Reproducer: extend `tests/test_catalog.py::_catalog_fixture` with a partitioned
canonical artifact under those two relative paths; compute its declared tree
hash in POSIX path order, update manifest/pointer identity as the publisher
does, and reopen its POSIX saved reference with explicit Windows relocation.
A valid three-row synthetic catalog passed metadata validation but full
validation reported an invalid tree hash. Execution was on Windows: POSIX
publisher hashing was emulated and the reference's recorded base was replaced
with a POSIX base. This is not a completed Linux-to-Windows integration run.
Existing cross-flavor reference
coverage uses single-file artifacts; same-host tree-order coverage does not
exercise this case. `docs/public_api.md` supports reopening an unchanged
publisher tree after relocation, so this is a contract issue for that input.

The inspected new publisher path templates do not generate the demonstrated
mixed-case sibling pattern. Some run IDs contain uppercase `T`/`Z`, and artifact
hard-linking preserves inherited names, so a universal lowercase-name claim
would be incorrect. No affected retained production catalog was established.
Preserve that distinction when prioritizing the PR.

Candidate fix: derive verification order from the saved reference's recorded
path flavor while preserving existing publisher hashes. Add mixed-case
partitioned-reference relocation coverage in both directions, plus the existing
prefix/punctuation order cases. This design is not implemented or fully
validated. Do not change publisher ordering silently: persisted tree identities
would change. Direct manifest opening after an undocumented move has no saved
original-host evidence and needs separate consideration.

## Additional investigation: catalog validation cost

Full catalog validation opens each Parquet footer twice: once for declared
metadata and again for canonical batch validation. A synthetic canonical
129-file/129-row catalog produced 258 footer opens in full mode and 129 in
metadata mode. Warm median complete `CatalogSnapshot` opens took 1.503 s full
versus 0.137 s metadata;
full footer-constructor time totaled 0.159-0.190 s. Removing one footer pass
might save only about 0.05-0.10 s in that workload. No caching or resource
lifetime machinery was added for this small demonstrated benefit.

Intended contracts remain: metadata validation skips value/hash checks; full
uses `validate_frame(strict=False)`; duplicate venue/market keys are explicitly
allowed. Reader/views preserve raw rows and do not impose lifecycle/time
filters. Native timestamp strings and raw payload/hash columns pass through;
local `validated_at_utc` is separate evidence. Stricter rejection would need
its own justification.

An additional targeted catalog reader/path/lineage run passed 28 tests, with
53 deselected, on Python 3.10.11/PyArrow 22.0.0. Managed queries were not
exercised; that environment's DuckDB 1.5.2 is below the qualified query floor.
Preserve lineage, projection and file mutation/error behavior if footer reuse
is investigated later.
