# Live capture recovery gaps (2026-09-12)

Status: open. Evidence from two 10-minute live probes on a single always-on host.
Do not treat this note as a completeness or thesis acceptance record.

Related: PR #1 (`3690f5b` / `a6165f5`) already split conservative quote
validity (`valid_state`) from initialized-book integrity
(`book_integrity_valid`) and bounded WebSocket retry. One-sided CLOB books
must not, by themselves, reconnect a shard. That path is still considered
fixed. This note covers what the live probes showed **after** that split.

## Probe summary

| Run | Selection | Profile | Result |
|---|---|---|---|
| 5 PM + 5 Kalshi | PM by `liquidityNum`; Kalshi by 24h volume | PM `full@3`; Kalshi `full@3` then `full@v2` | PM 0 reconnects, 381 events. Kalshi `@3` crashed on commit; `@v2` completed 33,599 events |
| 10 PM + 10 Kalshi | 24h volume, unique events | both `full@v2` | PM 7,834 events, **11 reconnects / 3 socket recoveries**, 18/20 initial snapshots. Kalshi 682 events, 0 reconnects, 8/10 snapshots |

Host was not CPU- or RAM-bound (peak ~67% / ~400 MB RSS, ~13 GB free). Scaling
the universe is the wrong next step.

## Issue 1 — Polymarket: missing initial book still reconnects the whole socket

**Symptom.** `socket_recovery_count=3`, `reconnect_count=11`, 220 tape
`reconnect` controls (20 instruments × 11). First recovery at ~28 s (SLA is
30 s). Both tokens of one dead LoL market never received `event_type=book`.
Eighteen other live tokens were reset with them.

**What is already fixed.** `empty_bid` / `empty_ask` are excluded from
`book_integrity_valid`. Stale quotes flip `valid_state` only.
`test_stream_order_book_data_does_not_recover_for_quietness_while_peer_is_active`
asserts `socket_recovery_count == 0` for a quiet *initialized* peer.

**What is not fixed.** `LiveFeedSupervisor.current_recovery_actions` still
emits `action="reconnect_socket"` when any subscribed id is in
`_overdue_initial_instruments` (`missing_instrument_books`). Polymarket
`maybe_recover_socket` honors **any** recovery action with `ws.reconnect` and
`mark_reconnect()` on every `MarketBookState`.

Kalshi already intercepts the same reasons and calls `request_snapshot` for
those tickers only (`test_targeted_refresh_matches_one_sided_and_new_sibling_responses`).
Polymarket has no equivalent.

`apply_price_change` does not set `initial_snapshot_received`. A token that
only sees deltas (or a resolved/empty book) stays overdue forever. Health
`missing_instrument_count` is `subscribed - tracked`, so it can read 0 while
the SLA set is still non-empty.

**Intended fix.**

1. Do not reconnect the Polymarket shard solely for `missing_instrument_books`
   / per-instrument `book_integrity` while other instruments are initialized
   and the socket is up.
2. Prefer a targeted book request or isolate/drop the overdue ids. If the
   venue cannot refresh one asset, record coverage loss; do not wipe peers.
3. Keep `reconnect_socket` for transport death, `hash_mismatch` at shard
   scope, and exhausted targeted refresh.

**Regression test.** Two tokens on one shard: token-1 gets `book` + deltas;
token-2 gets only `price_change` for >30 s; `max_reconnects=0`. Assert
`socket_recovery_count == 0` and token-1 state is not `mark_reconnect`'d.

## Issue 2 — Kalshi `full@3` commit suicide on per-outcome flags

**Symptom.** `ValueError: invalid topbook_main capture segment:
book_integrity_valid cannot accompany unresolved book failures`. Process
exit; journal recoverable as `CaptureCrash`. Same five sports tickers
completed on `full@v2`.

**Cause.** Market-level `KalshiOrderBookState.book_integrity_valid` ignores
only `empty_bid` / `empty_ask`. `kalshi_ws_snapshot_to_topbook` then splits
YES/NO and `compute_topbook` can add `crossed_book` / `negative_spread` on
the NO row (NO bid vs complement of YES bid) even when the YES view is not
crossed. `add_book_integrity(..., integrity=snapshot.book_integrity_valid)`
stamps **True** onto both rows. `validation.py` rejects that combination.

**Intended fix.** Stamp integrity from the **emitted row's** flags, or
recompute `book_integrity_valid` after per-outcome topbook construction.
Fail closed in the collector (flag the row) rather than aborting the process.
Keep the invariant for truly inconsistent rows.

**Regression test.** Fixture where market-level integrity is true and the NO
complement is crossed; `full@3` must finalize (or mark that row invalid)
without raising in durability commit.

## Issue 3 — Ad-hoc CLI captures cannot become complete

**Symptom.** Every probe finalized `capture_status=partial` with
`N instrument eligibility verdicts are unknown`.

**Cause.** `stream-books --token-id` / `stream-kalshi-books --ticker` do not
pass `instrument_eligibility`. `_normalized_evidence` then yields `UNKNOWN` /
`MISSING_EVIDENCE`. Completeness treats unknown as part of the denominator.

This is documented as provisional policy, but the CLI does not say so.
Live-probe operators cannot tell collector loss from “we never asserted
these ids were live.”

**Intended fix (small).** For ad-hoc captures, label coverage `unevaluated`
or inject eligibility from a live catalog snapshot when one is provided.
Do not call unknown-eligibility `partial` in the same bucket as missing
snapshots on eligible ids. Plan-mode remains the path to `eligible`.

## Issue 4 — Selection vs activity (operational, not a code defect)

24h volume and `liquidityNum` are poor proxies for “ticking now.” Dead LoL
maps and next-day gas strikes produced missing snapshots; TVL-heavy Fed
books produced almost no WS events. A REST book preflight (2–3 s) before
subscribe would have dropped the LoL Game 2 pair. Optional follow-up, not
required to close issues 1–3.

## Validation on the always-on host

After code lands on this branch, re-run the 10-minute dual-venue probe on
the same host with the same 10×10 activity selection (or a refreshed
in-play set):

- Polymarket: one known book-less token must not reset peers;
  `socket_recovery_count` should stay 0 unless the transport actually dies.
- Kalshi `full@3` must not crash the process on crossed NO complements.
- Completeness reasons should distinguish unknown eligibility from missing
  eligible snapshots.

Do not scale instrument count until those hold.
