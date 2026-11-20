# Committed book-tape contract-family retirement packet

Decision date: 2026-11-20\
Decision owner: pmkt-core repository owner\
Retirement commit: `242b1bad494836ae6955cb166229c5fc88dcd232`

## Decision

The owner approved closing out the committed book-tape producer and retiring
its five schemas together.

- `book_tape_event.v1`
- `book_tape_event.v2`
- `book_tape_level.v1`
- `book_tape_control.v1`
- `book_tape_control.v2`

An event header commits its level rows, and control rows mark the epochs in
which those events can be replayed. The v2 event and control schemas share the
v1 level schema. Retiring the versions separately would leave only part of the
producer's contract registered. PR 1 (`907ce11`) removed the legacy producer,
reconstruction readers, and CLI commands before the schema retirement.

## Consumer and persistence evidence

The source review traced the core producer and readers through capture,
reconstruction, manifest handling, CLI commands, and tests. After PR 1 the
remaining core uses were schema definitions, row builders, validators, tests,
and documentation. The review found zero runtime or import callers in
`pmkt-trading-impl-b`, the `pmkt-research-*` checkouts, or `research-kb`,
including scripts and notebooks. Generic trading artifact discovery reports
unregistered versions as validation warnings. The public schema constants, row
builders, and tape validators are retired. Historical notebook or manual use
goes through the pinned archive reader below.

When `pmkt-trading` bumps its core pin past this retirement, update
`required_schemas` in `pmkt-trading-impl-b`'s
`docs/architecture/execution_authority_map.md` to remove `book_tape_event.v1`,
`book_tape_level.v1`, and `book_tape_control.v1`.

The local disk check found only August 2026 profile-v1 probe and scratch runs
using committed tapes. Midterm recordings use raw WebSocket `.jsonl.gz` chunks.
The review identified no active manifest, journal, bundle, or downstream hash
pin depending on the family. Historical references in the probe and scratch
runs remain with those runs. The artifact finding is scoped to the local disk
check underlying the owner's close-out decision.

## Validation and documentation

PR 2 updated `tests/test_schema_lifecycle.py`,
`tests/test_schema_registry_validation.py`, and
`tests/fixtures/data_dictionary_column_gaps.json`, along with the data
dictionary, glossary, and lifecycle catalog and guide. Review at `242b1bad`
checked all 54 surviving schemas, compared 544 paired validation reports, and
passed 152 focused core tests.

## Data handling and rollback

Keep archived runs and their manifests, journals, and bundles byte-identical.
Use a separate checkout of pre-v1 core at `2688001` to read an archived run with
`pmkt reconstruct-book-tape`, placing reconstruction outputs outside the archive.
This path reads the original format and requires no data migration.

To restore the schemas on the current branch, revert the packet and its link,
then revert `242b1bad`. If the producer, reconstruction workflow, or legacy
manifest handling is needed again, also revert the PR 1 changes in reverse order
(`fe2baec`, then `907ce11`). These Git reversions leave the original data intact.
