# Paper-canary contract-family retirement packet

Decision date: 2026-09-17  
Decision owner: pmkt-core repository owner  
Release: 0.2 breaking-release window

## Decision

The owner approves retiring the producer-owned paper-canary contract family:

- `basket_order_intent.v1`
- `basket_paper_fill.v1`
- `basket_paper_position.v1`
- `canary_candidate.v1`
- `canary_rejection.v1`
- `scan_cycle.v1`

This decision ends the lifecycle freeze for these six schemas only. Every other
contract in the `execution_deferred` group remains frozen as
`active_experiment`. Because one retiring producer owns all six contracts, one
atomic schema-removal commit is approved for the family. This packet is the
single evidence and rollback record for that commit.

No deprecation aliases or schema tombstones will be added. The public
market-structure package and CLI workflows are retired in their own commit so
their removal remains independently reviewable.

## Consumer and persistence evidence

The repositories are still under development and the owner reports no external
or manual consumers. Reviewed source searches found the paper-canary producer
and its tests as the only semantic users of this family. No manifest names any
of the six schema identifiers. The generic trading artifact reader treats an
unregistered schema identifier as an unknown-version warning instead of
crashing, and the underlying files remain readable directly as ordinary
Parquet.

The complete source and artifact inventories required by the lifecycle policy
must be regenerated immediately before the schema-removal commit. Any
unexplained unreadable retained root stops removal.

## June campaign evidence

The ignored local paper-canary evidence contains 17 runs and 102 Parquet files.
Across those files there are 507,999 rejection rows and 419 scan-cycle rows.
The adjudicated 311-cycle monitor summary found zero positive candidates, zero
execution-allowed candidates, and no intents, fills, or positions. A relaxed
exploratory run produced one false-positive paper fill; the campaign review
explicitly rejected it as opportunity evidence.

These results support retiring the current N-leg paper-canary implementation
until a real consumer justifies reintroducing the behavior. They do not weaken
the independent production matching, signal, two-leg sizing, risk, order-intent,
coordinator, `canary-submit`, or live-ladder contracts.

## Data handling and rollback

Existing campaign files are not deleted, moved, or rewritten. They remain
directly readable as Parquet even after registry removal. A generic catalog
view may label their schema versions unknown, which is the intended read-only
behavior.

Rollback is Git restoration: revert the family-removal commit to restore the
registry specifications, builders, exports, tests, and documentation, then
restore the paper-canary producer from its separate trading commit if needed.
No data rollback is required because retained artifacts remain untouched.

