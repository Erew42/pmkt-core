from __future__ import annotations

import json
import heapq
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Protocol

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pmkt.data.canonical import FEED_HEALTH_COLUMNS, FEED_HEALTH_SCHEMA_VERSION


FEED_HEALTH_SCHEMA = pa.schema(
    [
        ("schema_version", pa.string()),
        ("observed_at_utc", pa.string()),
        ("local_sequence", pa.int64()),
        ("venue", pa.string()),
        ("shard_id", pa.string()),
        ("connection_state", pa.string()),
        ("instrument_count", pa.int64()),
        ("relation_count", pa.int64()),
        ("reconnect_count", pa.int64()),
        ("sequence_gap_count", pa.int64()),
        ("resync_count", pa.int64()),
        ("error_count", pa.int64()),
        ("last_message_age_ms", pa.int64()),
        ("last_valid_book_age_ms", pa.int64()),
        ("valid_book_count", pa.int64()),
        ("invalid_book_count", pa.int64()),
        ("valid_instrument_count", pa.int64()),
        ("invalid_instrument_count", pa.int64()),
        ("stale_instrument_count", pa.int64()),
        ("missing_instrument_count", pa.int64()),
        ("instrument_state_json", pa.string()),
        # Must match the canonical registry type (list[string]); a competing
        # pa.string() here was a second, conflicting physical definition.
        ("quality_flags", pa.list_(pa.string())),
    ]
)
BOOK_INTEGRITY_QUALITY_FLAGS = {
    "hash_mismatch",
    "seq_gap",
    "sequence_gap",
    "resync_required",
    "delta_before_snapshot",
    "no_initial_snapshot",
}
# Flags that describe the CURRENT book only.  They are replaced on every
# authoritative book update and must never survive a later book.
CURRENT_BOOK_QUALITY_FLAGS = {
    "crossed_book",
    "negative_spread",
    "empty_bid",
    "empty_ask",
}


def write_feed_health_parquet(
    path: str | Path,
    rows: Iterable[dict[str, Any]],
    *,
    append: bool = True,
) -> None:
    row_list = list(rows)
    output_path = Path(path)
    if output_path.exists() and output_path.is_dir():
        from pmkt.data.io import append_parquet_segment

        if not append:
            for child in output_path.iterdir():
                if child.is_dir():
                    raise IsADirectoryError(
                        f"feed health parquet dataset contains a nested directory: {child}"
                    )
                child.unlink()
        append_parquet_segment(output_path, FEED_HEALTH_SCHEMA, row_list)
        return

    new_table = pa.Table.from_pylist(row_list, schema=FEED_HEALTH_SCHEMA)
    if append and output_path.exists():
        existing = pq.read_table(output_path, schema=FEED_HEALTH_SCHEMA)
        new_table = pa.concat_tables([existing, new_table], promote_options="default")
    pq.write_table(new_table, output_path)


@dataclass(frozen=True)
class FeedPreflightReport:
    ok: bool
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    polymarket_asset_count: int = 0
    kalshi_market_ticker_count: int = 0


@dataclass(frozen=True)
class FeedRecoveryAction:
    action: str
    venue: str
    shard_id: str
    reasons: tuple[str, ...] = ()
    instruments: tuple[str, ...] = ()


class SubscriptionPlanValidation(Protocol):
    """Structural result accepted from an optional higher-level validator."""

    ok: bool
    errors: tuple[str, ...]
    warnings: tuple[str, ...]
    polymarket_asset_count: int
    kalshi_market_ticker_count: int


class SubscriptionPlanValidator(Protocol):
    """Optional semantic validator injected by a consumer of the data package."""

    def __call__(
        self,
        plan: dict[str, Any],
        *,
        polymarket_markets: pd.DataFrame | None = None,
        kalshi_markets: pd.DataFrame | None = None,
        relations: pd.DataFrame | None = None,
    ) -> SubscriptionPlanValidation: ...


@dataclass
class InstrumentFeedHealth:
    instrument: str
    last_message_monotonic_ns: int | None = None
    last_valid_book_monotonic_ns: int | None = None
    valid_state: bool = False
    valid_book_count: int = 0
    invalid_book_count: int = 0
    quality_flags: set[str] = field(default_factory=set)

    def record_message(self, *, now_monotonic_ns: int) -> None:
        self.last_message_monotonic_ns = now_monotonic_ns
        self.quality_flags.discard("stale_messages")

    def record_book(
        self,
        *,
        valid_state: bool,
        now_monotonic_ns: int,
        quality_flags: Iterable[str] = (),
    ) -> None:
        self.record_message(now_monotonic_ns=now_monotonic_ns)
        incoming_flags = _quality_flags(quality_flags)
        self.valid_state = bool(valid_state)
        # Current-book flags describe the latest authoritative book, so they are
        # REPLACED wholesale on every update.  Previously they were only
        # partially discarded on a valid book, so crossed_book/negative_spread/
        # empty_bid/empty_ask from an older snapshot survived indefinitely and
        # contaminated every later health row.
        self.quality_flags.difference_update(CURRENT_BOOK_QUALITY_FLAGS)
        self.quality_flags.update(incoming_flags & CURRENT_BOOK_QUALITY_FLAGS)
        # Integrity flags persist until their explicit resync/snapshot event.
        self.quality_flags.update(incoming_flags & BOOK_INTEGRITY_QUALITY_FLAGS)
        if valid_state:
            self.valid_book_count += 1
            self.last_valid_book_monotonic_ns = now_monotonic_ns
            self.quality_flags.discard("invalid_book")
            self.quality_flags.discard("stale_books")
            self.quality_flags.discard("reconnect")
            self.quality_flags.difference_update(BOOK_INTEGRITY_QUALITY_FLAGS)
        else:
            self.invalid_book_count += 1
            self.quality_flags.add("invalid_book")
            self.quality_flags.update(incoming_flags)

    def mark_reconnect(self) -> None:
        self.valid_state = False
        self.quality_flags.add("reconnect")

    def record_sequence_gap(self) -> None:
        self.valid_state = False
        self.quality_flags.add("sequence_gap")

    def record_resync(self, *, now_monotonic_ns: int | None = None) -> None:
        self.quality_flags.discard("sequence_gap")
        if now_monotonic_ns is not None:
            self.last_message_monotonic_ns = now_monotonic_ns

    def invalidate_if_stale(
        self,
        *,
        now_monotonic_ns: int,
        max_message_age_ms: int,
        max_valid_book_age_ms: int,
    ) -> bool:
        before = (
            self.valid_state,
            "stale_messages" in self.quality_flags,
            "stale_books" in self.quality_flags,
        )
        message_age = self.last_message_age_ms(now_monotonic_ns=now_monotonic_ns)
        if message_age is None or message_age > max_message_age_ms:
            self.valid_state = False
            self.quality_flags.add("stale_messages")
        book_age = self.last_valid_book_age_ms(now_monotonic_ns=now_monotonic_ns)
        if book_age is None or book_age > max_valid_book_age_ms:
            self.valid_state = False
            self.quality_flags.add("stale_books")
        return before != (
            self.valid_state,
            "stale_messages" in self.quality_flags,
            "stale_books" in self.quality_flags,
        )

    def _would_invalidate_if_stale(
        self,
        *,
        now_monotonic_ns: int,
        max_message_age_ms: int,
        max_valid_book_age_ms: int,
    ) -> bool:
        """Return whether a staleness check would change compact state."""
        message_age = self.last_message_age_ms(now_monotonic_ns=now_monotonic_ns)
        message_stale = message_age is None or message_age > max_message_age_ms
        book_age = self.last_valid_book_age_ms(now_monotonic_ns=now_monotonic_ns)
        book_stale = book_age is None or book_age > max_valid_book_age_ms
        return (
            (message_stale and "stale_messages" not in self.quality_flags)
            or (book_stale and "stale_books" not in self.quality_flags)
            or (self.valid_state and (message_stale or book_stale))
        )

    def last_message_age_ms(self, *, now_monotonic_ns: int) -> int | None:
        if self.last_message_monotonic_ns is None:
            return None
        return max(0, (now_monotonic_ns - self.last_message_monotonic_ns) // 1_000_000)

    def last_valid_book_age_ms(self, *, now_monotonic_ns: int) -> int | None:
        if self.last_valid_book_monotonic_ns is None:
            return None
        return max(
            0, (now_monotonic_ns - self.last_valid_book_monotonic_ns) // 1_000_000
        )

    def as_dict(self, *, now_monotonic_ns: int) -> dict[str, Any]:
        return {
            "instrument": self.instrument,
            "valid_state": self.valid_state,
            "last_message_age_ms": self.last_message_age_ms(
                now_monotonic_ns=now_monotonic_ns
            ),
            "last_valid_book_age_ms": self.last_valid_book_age_ms(
                now_monotonic_ns=now_monotonic_ns
            ),
            "valid_book_count": self.valid_book_count,
            "invalid_book_count": self.invalid_book_count,
            "quality_flags": sorted(self.quality_flags),
        }


@dataclass
class FeedShardHealth:
    venue: str
    shard_id: str
    subscribed_instruments: tuple[str, ...]
    relation_ids: tuple[str, ...] = ()
    connection_state: str = "initialized"
    connected_monotonic_ns: int | None = None
    last_message_monotonic_ns: int | None = None
    last_valid_book_monotonic_ns: int | None = None
    reconnect_count: int = 0
    sequence_gap_count: int = 0
    resync_count: int = 0
    error_count: int = 0
    valid_book_count: int = 0
    invalid_book_count: int = 0
    quality_flags: set[str] = field(default_factory=set)
    instrument_health: dict[str, InstrumentFeedHealth] = field(default_factory=dict)
    _subscribed_instrument_set: frozenset[str] = field(init=False, repr=False)
    _tracked_instrument_count: int = field(init=False, default=0, repr=False)
    _valid_instrument_count: int = field(init=False, default=0, repr=False)
    _invalid_instrument_count: int = field(init=False, default=0, repr=False)
    _stale_instrument_count: int = field(init=False, default=0, repr=False)
    _reconnect_blocking_instrument_count: int = field(init=False, default=0, repr=False)
    _current_book_flag_counts: Counter[str] = field(
        init=False, default_factory=Counter, repr=False
    )
    _book_integrity_flag_counts: Counter[str] = field(
        init=False, default_factory=Counter, repr=False
    )
    _deadline_change_callback: (
        Callable[["FeedShardHealth", str | None, bool], None] | None
    ) = field(init=False, default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        self._subscribed_instrument_set = frozenset(self.subscribed_instruments)
        if len(self._subscribed_instrument_set) != len(self.subscribed_instruments):
            raise ValueError(
                f"duplicate subscribed instrument in {self.venue}/{self.shard_id}"
            )
        unknown = sorted(set(self.instrument_health) - self._subscribed_instrument_set)
        if unknown:
            raise ValueError(
                f"instrument health outside subscription for {self.venue}/{self.shard_id}: "
                + ", ".join(unknown)
            )
        self._rebuild_instrument_aggregates()

    @property
    def instrument_count(self) -> int:
        return len(self.subscribed_instruments)

    @property
    def relation_count(self) -> int:
        return len(self.relation_ids)

    def mark_connected(self, *, now_monotonic_ns: int) -> bool:
        before = self._compact_semantic_signature()
        self.connection_state = "connected"
        self.connected_monotonic_ns = now_monotonic_ns
        self.last_message_monotonic_ns = now_monotonic_ns
        self.quality_flags.discard("disconnected")
        self.quality_flags.discard("stale_messages")
        self.quality_flags.discard("stale_books")
        # Every subscribed instrument receives a fresh initialization grace
        # period on connection, including instruments never observed before.
        self._notify_deadline_change(rebuild_instruments=True)
        return before != self._compact_semantic_signature()

    def mark_disconnected(self) -> bool:
        before = self._compact_semantic_signature()
        self.connection_state = "disconnected"
        self.quality_flags.add("disconnected")
        return before != self._compact_semantic_signature()

    def mark_transport_alive(self) -> bool:
        """Record a successful transport probe without refreshing book clocks."""
        before = self._compact_semantic_signature()
        if self.connection_state == "stale":
            self.connection_state = "connected"
        self.quality_flags.discard("disconnected")
        return before != self._compact_semantic_signature()

    def mark_reconnect(self, *, now_monotonic_ns: int | None = None) -> bool:
        before = self._compact_semantic_signature()
        self.reconnect_count += 1
        self.connection_state = "reconnecting"
        self.quality_flags.add("reconnect")
        self.valid_book_count = 0
        self.invalid_book_count = self.instrument_count
        self.connected_monotonic_ns = None
        for state in self.instrument_health.values():
            state.mark_reconnect()
        self._rebuild_instrument_aggregates()
        self._refresh_derived_book_flags()
        if now_monotonic_ns is not None:
            self.last_message_monotonic_ns = now_monotonic_ns
        self._notify_deadline_change(rebuild_instruments=True)
        return before != self._compact_semantic_signature()

    def record_message(
        self,
        *,
        now_monotonic_ns: int,
        instrument: str | None = None,
    ) -> bool:
        before = self._compact_semantic_signature()
        self._record_message(
            now_monotonic_ns=now_monotonic_ns,
            instrument=instrument,
        )
        return before != self._compact_semantic_signature()

    def _record_message(
        self,
        *,
        now_monotonic_ns: int,
        instrument: str | None = None,
    ) -> None:
        self.last_message_monotonic_ns = now_monotonic_ns
        if self.connection_state in {"initialized", "stale", "reconnecting"}:
            self.connection_state = "connected"
        self.quality_flags.discard("stale_messages")
        state = self._instrument_state(instrument)
        if state is not None:
            state_before = self._instrument_projection(state)
            state.record_message(now_monotonic_ns=now_monotonic_ns)
            self._apply_instrument_projection_change(
                before=state_before,
                after=self._instrument_projection(state),
            )
        self._notify_deadline_change(instrument=state.instrument if state else None)

    def record_book(
        self,
        *,
        valid_state: bool,
        now_monotonic_ns: int,
        instrument: str | None = None,
        quality_flags: Iterable[str] = (),
    ) -> bool:
        before = self._compact_semantic_signature()
        self._record_message(now_monotonic_ns=now_monotonic_ns, instrument=instrument)
        incoming_flags = _quality_flags(quality_flags)
        state = self._instrument_state(instrument)
        if state is not None:
            state_before = self._instrument_projection(state)
            state.record_book(
                valid_state=valid_state,
                now_monotonic_ns=now_monotonic_ns,
                quality_flags=incoming_flags,
            )
            self._apply_instrument_projection_change(
                before=state_before,
                after=self._instrument_projection(state),
            )
        if valid_state:
            self.valid_book_count += 1
            self.last_valid_book_monotonic_ns = now_monotonic_ns
            self.quality_flags.discard("stale_books")
            if not self._instrument_book_integrity_active():
                self.quality_flags.difference_update(BOOK_INTEGRITY_QUALITY_FLAGS)
        else:
            self.invalid_book_count += 1
            self.quality_flags.update(incoming_flags - CURRENT_BOOK_QUALITY_FLAGS)
        if self.instrument_health:
            # Shard book state is DERIVED from current instrument state, never
            # accumulated from whichever instrument was processed most recently.
            self._refresh_derived_book_flags()
        elif valid_state:
            # No per-instrument tracking for this shard: retain the original
            # last-update semantics rather than leaving the flag unmanaged.
            self.quality_flags.discard("invalid_book")
            self.quality_flags.difference_update(CURRENT_BOOK_QUALITY_FLAGS)
        else:
            self.quality_flags.add("invalid_book")
            self.quality_flags.update(incoming_flags & CURRENT_BOOK_QUALITY_FLAGS)
        self._refresh_reconnect_flag()
        self._notify_deadline_change(instrument=state.instrument if state else None)
        return before != self._compact_semantic_signature()

    def record_sequence_gap(
        self,
        *,
        instrument: str | None = None,
        instruments: Iterable[str] | None = None,
    ) -> bool:
        before = self._compact_semantic_signature()
        self.sequence_gap_count += 1
        self.valid_book_count = 0
        self.invalid_book_count = self.instrument_count
        self.quality_flags.add("sequence_gap")
        if instruments is None:
            state = self._instrument_state(instrument)
            if state is not None:
                state_before = self._instrument_projection(state)
                state.record_sequence_gap()
                self._apply_instrument_projection_change(
                    before=state_before,
                    after=self._instrument_projection(state),
                )
        else:
            for affected_instrument in dict.fromkeys(instruments):
                state = self._instrument_state(affected_instrument)
                if state is not None:
                    state_before = self._instrument_projection(state)
                    state.record_sequence_gap()
                    self._apply_instrument_projection_change(
                        before=state_before,
                        after=self._instrument_projection(state),
                    )
        self._notify_deadline_change(rebuild_instruments=True)
        return before != self._compact_semantic_signature()

    def record_resync(
        self,
        *,
        now_monotonic_ns: int | None = None,
        instrument: str | None = None,
    ) -> bool:
        before = self._compact_semantic_signature()
        self.resync_count += 1
        if now_monotonic_ns is not None:
            self.last_message_monotonic_ns = now_monotonic_ns
        state = self._instrument_state(instrument)
        if state is not None:
            state_before = self._instrument_projection(state)
            state.record_resync(now_monotonic_ns=now_monotonic_ns)
            self._apply_instrument_projection_change(
                before=state_before,
                after=self._instrument_projection(state),
            )
        if not self._instrument_book_integrity_active():
            self.quality_flags.discard("sequence_gap")
        self._notify_deadline_change(instrument=state.instrument if state else None)
        return before != self._compact_semantic_signature()

    def record_error(self, message: str | None = None) -> bool:
        before = self._compact_semantic_signature()
        self.error_count += 1
        self.quality_flags.add("error" if not message else f"error:{message}")
        return before != self._compact_semantic_signature()

    def invalidate_if_stale(
        self,
        *,
        now_monotonic_ns: int,
        max_message_age_ms: int,
        max_valid_book_age_ms: int,
    ) -> bool:
        before = self._compact_semantic_signature()
        self._invalidate_shard_clocks_if_stale(
            now_monotonic_ns=now_monotonic_ns,
            max_message_age_ms=max_message_age_ms,
            max_valid_book_age_ms=max_valid_book_age_ms,
        )
        instrument_transitioned = False
        for instrument in tuple(self.instrument_health):
            instrument_transitioned = (
                self._invalidate_instrument_if_stale(
                    instrument=instrument,
                    now_monotonic_ns=now_monotonic_ns,
                    max_message_age_ms=max_message_age_ms,
                    max_valid_book_age_ms=max_valid_book_age_ms,
                    refresh_derived=False,
                )
                or instrument_transitioned
            )
        if instrument_transitioned:
            self._refresh_derived_book_flags()
        return before != self._compact_semantic_signature()

    def _invalidate_shard_clocks_if_stale(
        self,
        *,
        now_monotonic_ns: int,
        max_message_age_ms: int,
        max_valid_book_age_ms: int,
    ) -> bool:
        before = self._compact_semantic_signature()
        message_age = self.last_message_age_ms(now_monotonic_ns=now_monotonic_ns)
        if message_age is None or message_age > max_message_age_ms:
            self.connection_state = "stale"
            self.quality_flags.add("stale_messages")
            self.valid_book_count = 0
            self.invalid_book_count = self.instrument_count
        book_age = self.last_valid_book_age_ms(now_monotonic_ns=now_monotonic_ns)
        if book_age is None and self.connected_monotonic_ns is not None:
            book_age = max(
                0,
                (now_monotonic_ns - self.connected_monotonic_ns) // 1_000_000,
            )
        if book_age is None or book_age > max_valid_book_age_ms:
            self.quality_flags.add("stale_books")
            self.valid_book_count = 0
            self.invalid_book_count = self.instrument_count
        return before != self._compact_semantic_signature()

    def _invalidate_instrument_if_stale(
        self,
        *,
        instrument: str,
        now_monotonic_ns: int,
        max_message_age_ms: int,
        max_valid_book_age_ms: int,
        refresh_derived: bool = True,
    ) -> bool:
        state = self.instrument_health.get(instrument)
        if state is None or not state._would_invalidate_if_stale(
            now_monotonic_ns=now_monotonic_ns,
            max_message_age_ms=max_message_age_ms,
            max_valid_book_age_ms=max_valid_book_age_ms,
        ):
            return False
        before = self._compact_semantic_signature()
        state_before = self._instrument_projection(state)
        transitioned = state.invalidate_if_stale(
            now_monotonic_ns=now_monotonic_ns,
            max_message_age_ms=max_message_age_ms,
            max_valid_book_age_ms=max_valid_book_age_ms,
        )
        if not transitioned:
            return False
        self._apply_instrument_projection_change(
            before=state_before,
            after=self._instrument_projection(state),
        )
        if refresh_derived:
            self._refresh_derived_book_flags()
        return before != self._compact_semantic_signature()

    def _notify_deadline_change(
        self,
        *,
        instrument: str | None = None,
        rebuild_instruments: bool = False,
    ) -> None:
        callback = self._deadline_change_callback
        if callback is not None:
            callback(self, instrument, rebuild_instruments)

    def last_message_age_ms(self, *, now_monotonic_ns: int) -> int | None:
        if self.last_message_monotonic_ns is None:
            return None
        return max(0, (now_monotonic_ns - self.last_message_monotonic_ns) // 1_000_000)

    def last_valid_book_age_ms(self, *, now_monotonic_ns: int) -> int | None:
        if self.last_valid_book_monotonic_ns is None:
            return None
        return max(
            0, (now_monotonic_ns - self.last_valid_book_monotonic_ns) // 1_000_000
        )

    def as_row(
        self,
        *,
        now_monotonic_ns: int,
        include_instrument_state: bool = True,
    ) -> dict[str, Any]:
        instrument_state = self._instrument_state_summary(
            now_monotonic_ns=now_monotonic_ns,
            include_instrument_state=include_instrument_state,
        )
        row_quality_flags = set(self.quality_flags)
        if instrument_state["missing_instrument_count"]:
            row_quality_flags.add("missing_instrument_books")
        if instrument_state["invalid_instrument_count"]:
            row_quality_flags.add("invalid_instrument_books")
        if instrument_state["stale_instrument_count"]:
            row_quality_flags.add("stale_instrument_books")
        invalid_book_count = (
            self.instrument_count - instrument_state["valid_instrument_count"]
            if self.instrument_health
            else self.invalid_book_count
        )
        return {
            "venue": self.venue,
            "shard_id": self.shard_id,
            "connection_state": self.connection_state,
            "instrument_count": self.instrument_count,
            "relation_count": self.relation_count,
            "reconnect_count": self.reconnect_count,
            "sequence_gap_count": self.sequence_gap_count,
            "resync_count": self.resync_count,
            "error_count": self.error_count,
            "last_message_age_ms": self.last_message_age_ms(
                now_monotonic_ns=now_monotonic_ns
            ),
            "last_valid_book_age_ms": self.last_valid_book_age_ms(
                now_monotonic_ns=now_monotonic_ns
            ),
            "valid_book_count": self.valid_book_count,
            "invalid_book_count": invalid_book_count,
            "valid_instrument_count": instrument_state["valid_instrument_count"],
            "invalid_instrument_count": instrument_state["invalid_instrument_count"],
            "stale_instrument_count": instrument_state["stale_instrument_count"],
            "missing_instrument_count": instrument_state["missing_instrument_count"],
            "instrument_state_json": instrument_state["instrument_state_json"],
            # Canonical registry type for feed_health.v1.quality_flags is
            # list[string].  Emitting a ";"-joined string here caused PyArrow to
            # persist a per-character array that strict validation accepted.
            "quality_flags": sorted(row_quality_flags),
        }

    def _instrument_state(self, instrument: str | None) -> InstrumentFeedHealth | None:
        if instrument is None:
            if len(self.subscribed_instruments) != 1:
                return None
            instrument = self.subscribed_instruments[0]
        text = str(instrument).strip()
        if not text:
            return None
        if text not in self._subscribed_instrument_set:
            return None
        state = self.instrument_health.get(text)
        if state is not None:
            return state
        state = InstrumentFeedHealth(text)
        self.instrument_health[text] = state
        self._tracked_instrument_count += 1
        self._apply_instrument_projection_change(
            before=None,
            after=self._instrument_projection(state),
        )
        return state

    def _refresh_derived_book_flags(self) -> None:
        """Recompute shard book flags from the current instrument states.

        ``invalid_book`` means "at least one currently-tracked instrument is
        invalid", not "the most recent update was invalid".  Current-book flags
        are the union over currently-invalid instruments only, so a healthy book
        never advertises a peer's stale condition.
        """
        if not self.instrument_health:
            return
        self.quality_flags.difference_update(CURRENT_BOOK_QUALITY_FLAGS)
        self.quality_flags.discard("invalid_book")
        if self._invalid_instrument_count:
            self.quality_flags.add("invalid_book")
        self.quality_flags.update(
            flag for flag, count in self._current_book_flag_counts.items() if count > 0
        )

    def _refresh_reconnect_flag(self) -> None:
        if "reconnect" not in self.quality_flags:
            return
        if not self.instrument_health:
            return
        if self._reconnect_blocking_instrument_count == 0:
            self.quality_flags.discard("reconnect")

    def _instrument_book_integrity_active(self) -> bool:
        return any(count > 0 for count in self._book_integrity_flag_counts.values())

    def _instrument_state_summary(
        self,
        *,
        now_monotonic_ns: int,
        include_instrument_state: bool,
    ) -> dict[str, Any]:
        if not self.instrument_health:
            return {
                "valid_instrument_count": 0,
                "invalid_instrument_count": 0,
                "stale_instrument_count": 0,
                "missing_instrument_count": 0,
                "instrument_state_json": "",
            }
        instrument_state_json = ""
        if include_instrument_state:
            states = [
                self.instrument_health[instrument]
                for instrument in self.subscribed_instruments
                if instrument in self.instrument_health
            ]
            instrument_state_json = json.dumps(
                [state.as_dict(now_monotonic_ns=now_monotonic_ns) for state in states],
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
        return {
            "valid_instrument_count": self._valid_instrument_count,
            "invalid_instrument_count": self._invalid_instrument_count,
            "stale_instrument_count": self._stale_instrument_count,
            "missing_instrument_count": (
                self.instrument_count - self._tracked_instrument_count
            ),
            "instrument_state_json": instrument_state_json,
        }

    @staticmethod
    def _instrument_projection(
        state: InstrumentFeedHealth,
    ) -> tuple[bool, bool, bool, frozenset[str], frozenset[str], bool]:
        stale = any(flag.startswith("stale_") for flag in state.quality_flags)
        return (
            state.valid_state and not stale,
            not state.valid_state,
            stale,
            (
                frozenset(state.quality_flags & CURRENT_BOOK_QUALITY_FLAGS)
                if not state.valid_state
                else frozenset()
            ),
            frozenset(state.quality_flags & BOOK_INTEGRITY_QUALITY_FLAGS),
            not (state.valid_state and "reconnect" not in state.quality_flags),
        )

    def _apply_instrument_projection_change(
        self,
        *,
        before: tuple[bool, bool, bool, frozenset[str], frozenset[str], bool] | None,
        after: tuple[bool, bool, bool, frozenset[str], frozenset[str], bool] | None,
    ) -> None:
        if before is not None:
            self._adjust_projection(before, -1)
        if after is not None:
            self._adjust_projection(after, 1)

    def _adjust_projection(
        self,
        projection: tuple[bool, bool, bool, frozenset[str], frozenset[str], bool],
        delta: int,
    ) -> None:
        valid, invalid, stale, current_flags, integrity_flags, reconnect_blocking = (
            projection
        )
        self._valid_instrument_count += delta * int(valid)
        self._invalid_instrument_count += delta * int(invalid)
        self._stale_instrument_count += delta * int(stale)
        self._reconnect_blocking_instrument_count += delta * int(reconnect_blocking)
        if (
            min(
                self._valid_instrument_count,
                self._invalid_instrument_count,
                self._stale_instrument_count,
                self._reconnect_blocking_instrument_count,
            )
            < 0
        ):
            raise RuntimeError(
                "negative cached feed-health aggregate for "
                f"{self.venue}/{self.shard_id}"
            )
        for flag in current_flags:
            updated = self._current_book_flag_counts[flag] + delta
            if updated < 0:
                raise RuntimeError(
                    "negative cached current-book flag aggregate for "
                    f"{self.venue}/{self.shard_id}: {flag}"
                )
            if updated == 0:
                del self._current_book_flag_counts[flag]
            else:
                self._current_book_flag_counts[flag] = updated
        for flag in integrity_flags:
            updated = self._book_integrity_flag_counts[flag] + delta
            if updated < 0:
                raise RuntimeError(
                    "negative cached book-integrity flag aggregate for "
                    f"{self.venue}/{self.shard_id}: {flag}"
                )
            if updated == 0:
                del self._book_integrity_flag_counts[flag]
            else:
                self._book_integrity_flag_counts[flag] = updated

    def _rebuild_instrument_aggregates(self) -> None:
        self._tracked_instrument_count = len(self.instrument_health)
        self._valid_instrument_count = 0
        self._invalid_instrument_count = 0
        self._stale_instrument_count = 0
        self._reconnect_blocking_instrument_count = 0
        self._current_book_flag_counts.clear()
        self._book_integrity_flag_counts.clear()
        for state in self.instrument_health.values():
            self._adjust_projection(self._instrument_projection(state), 1)

    def _compact_semantic_signature(self) -> tuple[Any, ...]:
        flags = set(self.quality_flags)
        if self.instrument_health:
            missing_count = self.instrument_count - self._tracked_instrument_count
            if missing_count:
                flags.add("missing_instrument_books")
            if self._invalid_instrument_count:
                flags.add("invalid_instrument_books")
            if self._stale_instrument_count:
                flags.add("stale_instrument_books")
        else:
            missing_count = 0
        return (
            self.connection_state,
            self.reconnect_count,
            self.sequence_gap_count,
            self.resync_count,
            self.error_count,
            self._valid_instrument_count,
            self._invalid_instrument_count,
            self._stale_instrument_count,
            missing_count,
            frozenset(flags),
        )


class LiveFeedSupervisor:
    """Control-plane state for supervised read-only market-data feeds."""

    def __init__(
        self,
        shards: Iterable[FeedShardHealth],
        *,
        preflight_report: FeedPreflightReport | None = None,
        max_message_age_ms: int = 5_000,
        max_valid_book_age_ms: int = 5_000,
    ) -> None:
        self.shards: dict[tuple[str, str], FeedShardHealth] = {}
        self._instrument_shards: dict[tuple[str, str], FeedShardHealth] = {}
        for shard in shards:
            shard_key = (shard.venue, shard.shard_id)
            if shard_key in self.shards:
                raise ValueError(
                    f"duplicate feed shard: {shard.venue}/{shard.shard_id}"
                )
            self.shards[shard_key] = shard
            for instrument in shard.subscribed_instruments:
                instrument_key = (shard.venue, instrument)
                existing = self._instrument_shards.get(instrument_key)
                if existing is not None:
                    raise ValueError(
                        "instrument subscribed by multiple feed shards: "
                        f"{shard.venue}/{instrument} in "
                        f"{existing.shard_id} and {shard.shard_id}"
                    )
                self._instrument_shards[instrument_key] = shard
        self.preflight_report = preflight_report or FeedPreflightReport(ok=True)
        self.max_message_age_ms = max_message_age_ms
        self.max_valid_book_age_ms = max_valid_book_age_ms
        self._stale_deadline_heaps: dict[str, list[tuple[int, int, str, str, str]]] = {}
        self._stale_deadline_generations: dict[tuple[str, str, str, str], int] = {}
        self._stale_deadline_values: dict[tuple[str, str, str, str], int] = {}
        self._active_stale_deadline_counts: Counter[str] = Counter()
        self._overdue_initial_instruments: dict[tuple[str, str], set[str]] = {
            key: set() for key in self.shards
        }
        self.last_staleness_instruments_examined = 0
        for shard in self.shards.values():
            shard._deadline_change_callback = self._on_shard_deadline_change
            self._schedule_shard_deadlines(shard, rebuild_instruments=True)

    @classmethod
    def from_subscription_plan(
        cls,
        plan: dict[str, Any],
        *,
        polymarket_markets: pd.DataFrame | None = None,
        kalshi_markets: pd.DataFrame | None = None,
        relations: pd.DataFrame | None = None,
        validator: SubscriptionPlanValidator | None = None,
        max_message_age_ms: int = 5_000,
        max_valid_book_age_ms: int = 5_000,
    ) -> "LiveFeedSupervisor":
        preflight = (
            _preflight_from_validation(
                validator(
                    plan,
                    polymarket_markets=polymarket_markets,
                    kalshi_markets=kalshi_markets,
                    relations=relations,
                )
            )
            if validator is not None
            else _core_subscription_plan_preflight(plan)
        )
        if not preflight.ok:
            return cls(
                [],
                preflight_report=preflight,
                max_message_age_ms=max_message_age_ms,
                max_valid_book_age_ms=max_valid_book_age_ms,
            )
        return cls(
            _shards_from_plan(plan),
            preflight_report=preflight,
            max_message_age_ms=max_message_age_ms,
            max_valid_book_age_ms=max_valid_book_age_ms,
        )

    @classmethod
    def from_subscription_plan_selection(
        cls,
        plan: dict[str, Any],
        *,
        venue: str,
        instruments: Iterable[str],
        validator: SubscriptionPlanValidator | None = None,
        max_message_age_ms: int = 5_000,
        max_valid_book_age_ms: int = 5_000,
    ) -> "LiveFeedSupervisor":
        preflight = (
            _preflight_from_validation(validator(plan))
            if validator is not None
            else _core_subscription_plan_preflight(plan)
        )
        if not preflight.ok:
            return cls(
                [],
                preflight_report=preflight,
                max_message_age_ms=max_message_age_ms,
                max_valid_book_age_ms=max_valid_book_age_ms,
            )
        return cls(
            _shards_from_plan_selection(plan, venue=venue, instruments=instruments),
            preflight_report=preflight,
            max_message_age_ms=max_message_age_ms,
            max_valid_book_age_ms=max_valid_book_age_ms,
        )

    def require_preflight_ok(self) -> None:
        if not self.preflight_report.ok:
            raise RuntimeError(
                "feed preflight failed: " + "; ".join(self.preflight_report.errors)
            )

    def shard(self, venue: str, shard_id: str) -> FeedShardHealth:
        return self.shards[(venue, shard_id)]

    def venue_shards(self, venue: str) -> list[FeedShardHealth]:
        return [
            shard
            for shard in sorted(self.shards.values(), key=lambda item: item.shard_id)
            if shard.venue == venue
        ]

    def shard_for_instrument(self, venue: str, instrument: str) -> FeedShardHealth:
        return self._instrument_shards[(venue, instrument)]

    def shard_keys(self, *, venue: str | None = None) -> tuple[tuple[str, str], ...]:
        return tuple(
            key for key in sorted(self.shards) if venue is None or key[0] == venue
        )

    def shard_metadata(self) -> list[dict[str, Any]]:
        return [
            {
                "venue": shard.venue,
                "shard_id": shard.shard_id,
                "instrument_count": shard.instrument_count,
                "relation_count": shard.relation_count,
                "subscribed_instruments": list(shard.subscribed_instruments),
                "relation_ids": list(shard.relation_ids),
            }
            for shard in sorted(
                self.shards.values(), key=lambda item: (item.venue, item.shard_id)
            )
        ]

    def invalidate_stale(
        self,
        *,
        now_monotonic_ns: int,
        venue: str | None = None,
    ) -> tuple[tuple[str, str], ...]:
        changed: set[tuple[str, str]] = set()
        examined: set[tuple[str, str, str]] = set()
        venues = (
            (venue,) if venue is not None else tuple(sorted(self._stale_deadline_heaps))
        )
        for selected_venue in venues:
            heap = self._stale_deadline_heaps.get(selected_venue)
            if heap is None:
                continue
            while heap and heap[0][0] <= now_monotonic_ns:
                deadline_ns, generation, shard_id, instrument, kind = heapq.heappop(
                    heap
                )
                deadline_key = (selected_venue, shard_id, instrument, kind)
                if self._stale_deadline_generations.get(deadline_key) != generation:
                    continue
                if self._stale_deadline_values.get(deadline_key) != deadline_ns:
                    continue
                del self._stale_deadline_values[deadline_key]
                self._active_stale_deadline_counts[selected_venue] -= 1
                shard = self.shards[(selected_venue, shard_id)]
                if instrument and kind == "initial_book":
                    examined.add((selected_venue, shard_id, instrument))
                    overdue = self._overdue_initial_instruments[
                        (selected_venue, shard_id)
                    ]
                    if (
                        instrument not in shard.instrument_health
                        and shard.connected_monotonic_ns is not None
                        and instrument not in overdue
                    ):
                        overdue.add(instrument)
                        transitioned = True
                    else:
                        transitioned = False
                elif instrument:
                    examined.add((selected_venue, shard_id, instrument))
                    transitioned = shard._invalidate_instrument_if_stale(
                        instrument=instrument,
                        now_monotonic_ns=now_monotonic_ns,
                        max_message_age_ms=self.max_message_age_ms,
                        max_valid_book_age_ms=self.max_valid_book_age_ms,
                    )
                else:
                    transitioned = shard._invalidate_shard_clocks_if_stale(
                        now_monotonic_ns=now_monotonic_ns,
                        max_message_age_ms=self.max_message_age_ms,
                        max_valid_book_age_ms=self.max_valid_book_age_ms,
                    )
                if transitioned:
                    changed.add((selected_venue, shard_id))
        self.last_staleness_instruments_examined = len(examined)
        return tuple(sorted(changed))

    def _on_shard_deadline_change(
        self,
        shard: FeedShardHealth,
        instrument: str | None,
        rebuild_instruments: bool,
    ) -> None:
        self._schedule_shard_deadlines(shard, rebuild_instruments=False)
        if rebuild_instruments:
            for subscribed in shard.subscribed_instruments:
                self._schedule_instrument_deadlines(shard, subscribed)
        elif instrument is not None:
            self._schedule_instrument_deadlines(shard, instrument)

    def _schedule_shard_deadlines(
        self,
        shard: FeedShardHealth,
        *,
        rebuild_instruments: bool,
    ) -> None:
        self._schedule_stale_deadline(
            shard,
            instrument="",
            kind="message",
            source_monotonic_ns=shard.last_message_monotonic_ns,
            max_age_ms=self.max_message_age_ms,
        )
        book_source = shard.last_valid_book_monotonic_ns
        if book_source is None:
            book_source = shard.connected_monotonic_ns
        self._schedule_stale_deadline(
            shard,
            instrument="",
            kind="book",
            source_monotonic_ns=book_source,
            max_age_ms=self.max_valid_book_age_ms,
        )
        if rebuild_instruments:
            for instrument in shard.subscribed_instruments:
                self._schedule_instrument_deadlines(shard, instrument)

    def _schedule_instrument_deadlines(
        self, shard: FeedShardHealth, instrument: str
    ) -> None:
        state = shard.instrument_health.get(instrument)
        if state is None:
            if shard.connected_monotonic_ns is None:
                self._cancel_stale_deadline(
                    shard, instrument=instrument, kind="initial_book"
                )
                self._overdue_initial_instruments[
                    (shard.venue, shard.shard_id)
                ].discard(instrument)
                return
            self._schedule_stale_deadline(
                shard,
                instrument=instrument,
                kind="initial_book",
                source_monotonic_ns=shard.connected_monotonic_ns,
                max_age_ms=self.max_valid_book_age_ms,
            )
            return
        self._cancel_stale_deadline(shard, instrument=instrument, kind="initial_book")
        self._overdue_initial_instruments[(shard.venue, shard.shard_id)].discard(
            instrument
        )
        message_source = state.last_message_monotonic_ns
        book_source = state.last_valid_book_monotonic_ns
        connected = shard.connected_monotonic_ns
        if connected is not None:
            if message_source is None or "reconnect" in state.quality_flags:
                message_source = max(message_source or connected, connected)
            if book_source is None or "reconnect" in state.quality_flags:
                book_source = max(book_source or connected, connected)
        self._schedule_stale_deadline(
            shard,
            instrument=instrument,
            kind="message",
            source_monotonic_ns=message_source,
            max_age_ms=self.max_message_age_ms,
        )
        self._schedule_stale_deadline(
            shard,
            instrument=instrument,
            kind="book",
            source_monotonic_ns=book_source,
            max_age_ms=self.max_valid_book_age_ms,
        )

    def _schedule_stale_deadline(
        self,
        shard: FeedShardHealth,
        *,
        instrument: str,
        kind: str,
        source_monotonic_ns: int | None,
        max_age_ms: int,
    ) -> None:
        # Ages use floor milliseconds and become stale only when age > max.
        deadline_ns = (
            0
            if source_monotonic_ns is None
            else source_monotonic_ns + (max_age_ms + 1) * 1_000_000
        )
        key = (shard.venue, shard.shard_id, instrument, kind)
        generation = self._stale_deadline_generations.get(key, 0) + 1
        self._stale_deadline_generations[key] = generation
        if key not in self._stale_deadline_values:
            self._active_stale_deadline_counts[shard.venue] += 1
        self._stale_deadline_values[key] = deadline_ns
        heap = self._stale_deadline_heaps.setdefault(shard.venue, [])
        heapq.heappush(
            heap,
            (deadline_ns, generation, shard.shard_id, instrument, kind),
        )
        active = self._active_stale_deadline_counts[shard.venue]
        if len(heap) > max(1_024, active * 4):
            self._compact_stale_deadline_heap(shard.venue)

    def _cancel_stale_deadline(
        self,
        shard: FeedShardHealth,
        *,
        instrument: str,
        kind: str,
    ) -> None:
        key = (shard.venue, shard.shard_id, instrument, kind)
        self._stale_deadline_generations[key] = (
            self._stale_deadline_generations.get(key, 0) + 1
        )
        if key in self._stale_deadline_values:
            del self._stale_deadline_values[key]
            self._active_stale_deadline_counts[shard.venue] -= 1

    def _compact_stale_deadline_heap(self, venue: str) -> None:
        heap = [
            (deadline, self._stale_deadline_generations[key], key[1], key[2], key[3])
            for key, deadline in self._stale_deadline_values.items()
            if key[0] == venue
        ]
        heapq.heapify(heap)
        self._stale_deadline_heaps[venue] = heap

    def current_recovery_actions(
        self,
        *,
        venue: str | None = None,
        shard_keys: Iterable[tuple[str, str]] | None = None,
    ) -> list[FeedRecoveryAction]:
        """Return socket recovery actions without refreshing staleness."""
        actions: list[FeedRecoveryAction] = []
        for shard in self._selected_shards(shard_keys=shard_keys, venue=venue):
            reasons = _socket_recovery_reasons(shard)
            instruments: tuple[str, ...] = ()
            if not reasons:
                stale_states = {
                    instrument: state
                    for instrument, state in shard.instrument_health.items()
                    if any(flag.startswith("stale_") for flag in state.quality_flags)
                }
                missing = self._overdue_initial_instruments[
                    (shard.venue, shard.shard_id)
                ]
                instrument_reasons: list[str] = []
                if any(
                    "stale_messages" in state.quality_flags
                    for state in stale_states.values()
                ):
                    instrument_reasons.append("stale_messages")
                if any(
                    "stale_books" in state.quality_flags
                    for state in stale_states.values()
                ):
                    instrument_reasons.append("stale_books")
                if missing:
                    instrument_reasons.append("missing_instrument_books")
                reasons = tuple(instrument_reasons)
                instruments = tuple(sorted(set(stale_states) | missing))
            if not reasons:
                continue
            actions.append(
                FeedRecoveryAction(
                    action="reconnect_socket",
                    venue=shard.venue,
                    shard_id=shard.shard_id,
                    reasons=reasons,
                    instruments=instruments,
                )
            )
        return actions

    def recovery_actions(
        self,
        *,
        now_monotonic_ns: int,
        venue: str | None = None,
    ) -> list[FeedRecoveryAction]:
        """Return socket-level recovery actions for stale shards."""
        # Preserve the historical refresh side effect: a venue filter limits the
        # returned actions, not which shards have their staleness refreshed.
        self.invalidate_stale(now_monotonic_ns=now_monotonic_ns)
        return self.current_recovery_actions(venue=venue)

    def health_rows(
        self,
        *,
        now_monotonic_ns: int,
        include_instrument_state: bool = True,
        shard_keys: Iterable[tuple[str, str]] | None = None,
    ) -> list[dict[str, Any]]:
        return [
            shard.as_row(
                now_monotonic_ns=now_monotonic_ns,
                include_instrument_state=include_instrument_state,
            )
            for shard in self._selected_shards(shard_keys=shard_keys)
        ]

    def feed_health_rows(
        self,
        *,
        now_monotonic_ns: int,
        observed_at_utc: str,
        local_sequence: int,
        include_instrument_state: bool = True,
        shard_keys: Iterable[tuple[str, str]] | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for row in self.health_rows(
            now_monotonic_ns=now_monotonic_ns,
            include_instrument_state=include_instrument_state,
            shard_keys=shard_keys,
        ):
            rows.append(
                {
                    "schema_version": FEED_HEALTH_SCHEMA_VERSION,
                    "observed_at_utc": observed_at_utc,
                    "local_sequence": int(local_sequence),
                    **row,
                }
            )
        return rows

    def _selected_shards(
        self,
        *,
        shard_keys: Iterable[tuple[str, str]] | None,
        venue: str | None = None,
    ) -> list[FeedShardHealth]:
        keys = self.shard_keys(venue=venue) if shard_keys is None else tuple(shard_keys)
        selected: list[FeedShardHealth] = []
        seen: set[tuple[str, str]] = set()
        for key in sorted(keys):
            normalized = (str(key[0]), str(key[1]))
            if normalized in seen:
                continue
            seen.add(normalized)
            shard = self.shards[normalized]
            if venue is not None and shard.venue != venue:
                continue
            selected.append(shard)
        return selected

    def feed_health_summary(self, *, now_monotonic_ns: int) -> dict[str, Any]:
        rows = self.health_rows(now_monotonic_ns=now_monotonic_ns)
        quality_counts: Counter[str] = Counter()
        for row in rows:
            # as_row() emits the canonical list[str]; str(list) would produce a
            # single token containing the Python repr of the whole list.
            quality_counts.update(_quality_flags(row.get("quality_flags")))
        return {
            "shard_count": len(rows),
            "instrument_count": sum(int(row["instrument_count"]) for row in rows),
            "relation_count": sum(int(row["relation_count"]) for row in rows),
            "connected_shard_count": sum(
                1 for row in rows if row["connection_state"] == "connected"
            ),
            "stale_shard_count": sum(
                1 for row in rows if row["connection_state"] == "stale"
            ),
            "reconnect_count": sum(int(row["reconnect_count"]) for row in rows),
            "sequence_gap_count": sum(int(row["sequence_gap_count"]) for row in rows),
            "resync_count": sum(int(row["resync_count"]) for row in rows),
            "error_count": sum(int(row["error_count"]) for row in rows),
            "valid_book_count": sum(int(row["valid_book_count"]) for row in rows),
            "invalid_book_count": sum(int(row["invalid_book_count"]) for row in rows),
            "quality_flag_counts": dict(sorted(quality_counts.items())),
            "shards": rows,
        }


def _preflight_from_validation(
    report: SubscriptionPlanValidation,
) -> FeedPreflightReport:
    return FeedPreflightReport(
        ok=report.ok,
        errors=report.errors,
        warnings=report.warnings,
        polymarket_asset_count=report.polymarket_asset_count,
        kalshi_market_ticker_count=report.kalshi_market_ticker_count,
    )


def _core_subscription_plan_preflight(plan: dict[str, Any]) -> FeedPreflightReport:
    """Validate only the feed-shard structure owned by the public package."""
    try:
        shards = _shards_from_plan(plan)
    except (KeyError, TypeError, ValueError) as exc:
        return FeedPreflightReport(ok=False, errors=(str(exc),))
    polymarket_count = sum(
        shard.instrument_count for shard in shards if shard.venue == "polymarket"
    )
    kalshi_count = sum(
        shard.instrument_count for shard in shards if shard.venue == "kalshi"
    )
    errors: list[str] = []
    polymarket_assets = [
        instrument
        for shard in shards
        if shard.venue == "polymarket"
        for instrument in shard.subscribed_instruments
    ]
    long_decimal_assets = [
        asset for asset in polymarket_assets if asset.isdigit() and len(asset) > 90
    ]
    if long_decimal_assets:
        errors.append(
            "polymarket assets_ids look longer than single CLOB token IDs "
            "(>90 decimal digits): "
            + ", ".join(long_decimal_assets[:5])
        )
    serialized_assets = [
        asset
        for asset in polymarket_assets
        if asset.startswith(("[", "(")) or "," in asset
    ]
    if serialized_assets:
        errors.append(
            "polymarket assets_ids contain serialized/list-like values: "
            + ", ".join(serialized_assets[:5])
        )
    if polymarket_count + kalshi_count == 0:
        errors.append("subscription plan contains no feed instruments")
    if errors:
        return FeedPreflightReport(
            ok=False,
            errors=tuple(errors),
            polymarket_asset_count=polymarket_count,
            kalshi_market_ticker_count=kalshi_count,
        )
    return FeedPreflightReport(
        ok=True,
        polymarket_asset_count=polymarket_count,
        kalshi_market_ticker_count=kalshi_count,
    )


def _shards_from_plan(plan: dict[str, Any]) -> list[FeedShardHealth]:
    return [
        *_polymarket_shards(plan),
        *_kalshi_shards(plan),
    ]


def _shards_from_plan_selection(
    plan: dict[str, Any],
    *,
    venue: str,
    instruments: Iterable[str],
) -> list[FeedShardHealth]:
    selected = _ordered_unique(instruments)
    if venue == "polymarket":
        return _selected_shards(
            plan,
            section="polymarket_assets",
            id_key="asset_id",
            selected=selected,
            venue="polymarket",
            default_shard_id="polymarket-0",
            legacy_values=_legacy_polymarket_assets(plan),
        )
    if venue == "kalshi":
        return _selected_shards(
            plan,
            section="kalshi_market_tickers",
            id_key="market_ticker",
            selected=selected,
            venue="kalshi",
            default_shard_id="kalshi-0",
            legacy_values=_legacy_kalshi_tickers(plan),
        )
    raise ValueError(f"unsupported venue {venue!r}")


def _selected_shards(
    plan: dict[str, Any],
    *,
    section: str,
    id_key: str,
    selected: list[str],
    venue: str,
    default_shard_id: str,
    legacy_values: Iterable[str],
) -> list[FeedShardHealth]:
    entries = plan.get(section)
    if not selected:
        selected = _ordered_unique(legacy_values)
    if not isinstance(entries, list) or not any(
        isinstance(item, dict) for item in entries
    ):
        return [
            FeedShardHealth(
                venue=venue,
                shard_id=default_shard_id,
                subscribed_instruments=tuple(selected),
            )
        ]
    selected_set = set(selected)
    grouped: dict[str, list[dict[str, Any]]] = {}
    planned: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        instrument = str(entry.get(id_key) or "").strip()
        if not instrument or instrument not in selected_set:
            continue
        planned.add(instrument)
        grouped.setdefault(str(entry.get("shard_id") or default_shard_id), []).append(
            entry
        )
    leftovers = [instrument for instrument in selected if instrument not in planned]
    for instrument in leftovers:
        grouped.setdefault(default_shard_id, []).append(
            {id_key: instrument, "match_ids": []}
        )
    return [
        FeedShardHealth(
            venue=venue,
            shard_id=shard_id,
            subscribed_instruments=tuple(
                str(entry.get(id_key))
                for entry in sorted(
                    items,
                    key=lambda item: (
                        selected.index(str(item.get(id_key)))
                        if str(item.get(id_key)) in selected
                        else len(selected)
                    ),
                )
                if entry.get(id_key) is not None
            ),
            relation_ids=_relation_ids(items),
        )
        for shard_id, items in sorted(grouped.items())
    ]


def _polymarket_shards(plan: dict[str, Any]) -> list[FeedShardHealth]:
    entries = plan.get("polymarket_assets")
    if isinstance(entries, list) and entries:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for entry in entries:
            if isinstance(entry, dict):
                grouped.setdefault(
                    str(entry.get("shard_id") or "polymarket-0"), []
                ).append(entry)
        return [
            FeedShardHealth(
                venue="polymarket",
                shard_id=shard_id,
                subscribed_instruments=tuple(
                    str(entry.get("asset_id"))
                    for entry in items
                    if entry.get("asset_id") is not None
                ),
                relation_ids=_relation_ids(items),
            )
            for shard_id, items in sorted(grouped.items())
        ]
    legacy = plan.get("polymarket")
    assets = legacy.get("assets_ids") if isinstance(legacy, dict) else []
    return [
        FeedShardHealth(
            venue="polymarket",
            shard_id="polymarket-0",
            subscribed_instruments=tuple(str(asset) for asset in assets or []),
        )
    ]


def _kalshi_shards(plan: dict[str, Any]) -> list[FeedShardHealth]:
    entries = plan.get("kalshi_market_tickers")
    if isinstance(entries, list) and entries:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for entry in entries:
            if isinstance(entry, dict):
                grouped.setdefault(str(entry.get("shard_id") or "kalshi-0"), []).append(
                    entry
                )
        return [
            FeedShardHealth(
                venue="kalshi",
                shard_id=shard_id,
                subscribed_instruments=tuple(
                    str(entry.get("market_ticker"))
                    for entry in items
                    if entry.get("market_ticker") is not None
                ),
                relation_ids=_relation_ids(items),
            )
            for shard_id, items in sorted(grouped.items())
        ]
    legacy = plan.get("kalshi")
    tickers = legacy.get("market_tickers") if isinstance(legacy, dict) else []
    return [
        FeedShardHealth(
            venue="kalshi",
            shard_id="kalshi-0",
            subscribed_instruments=tuple(str(ticker) for ticker in tickers or []),
        )
    ]


def _legacy_polymarket_assets(plan: dict[str, Any]) -> list[str]:
    legacy = plan.get("polymarket")
    assets = legacy.get("assets_ids") if isinstance(legacy, dict) else []
    return _ordered_unique(assets or [])


def _legacy_kalshi_tickers(plan: dict[str, Any]) -> list[str]:
    legacy = plan.get("kalshi")
    tickers = legacy.get("market_tickers") if isinstance(legacy, dict) else []
    return _ordered_unique(tickers or [])


def _ordered_unique(values: Iterable[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _relation_ids(items: Iterable[dict[str, Any]]) -> tuple[str, ...]:
    relation_ids: set[str] = set()
    for item in items:
        values = item.get("match_ids")
        if isinstance(values, list):
            relation_ids.update(str(value) for value in values if value is not None)
    return tuple(sorted(relation_ids))


def _socket_recovery_reasons(shard: FeedShardHealth) -> tuple[str, ...]:
    reasons: list[str] = []
    if shard.connection_state == "stale":
        reasons.append("connection_stale")
    if shard.connection_state == "disconnected":
        reasons.append("connection_disconnected")
    if "stale_messages" in shard.quality_flags:
        reasons.append("stale_messages")
    if "stale_books" in shard.quality_flags and "reconnect" not in shard.quality_flags:
        reasons.append("stale_books")
    if "hash_mismatch" in shard.quality_flags:
        reasons.append("hash_mismatch")
        reasons.append("book_integrity")
    return tuple(dict.fromkeys(reasons))


def _quality_flags(flags: Iterable[str] | str | None) -> set[str]:
    """Normalize a flag value to a set of whole tokens.

    Canonical form is ``list[str]``.  A ``str`` is only produced by legacy
    ``feed_health.v1`` artifacts written before the list conversion; it is split
    on the historical ";" delimiter rather than iterated, because iterating a
    string yields one flag per character -- the corruption this column already
    suffered once.
    """
    if flags is None:
        return set()
    if isinstance(flags, str):
        return {token.strip() for token in flags.split(";") if token.strip()}
    return {str(flag).strip() for flag in flags if str(flag).strip()}


__all__ = [
    "FEED_HEALTH_COLUMNS",
    "FEED_HEALTH_SCHEMA",
    "FeedRecoveryAction",
    "FeedPreflightReport",
    "FeedShardHealth",
    "LiveFeedSupervisor",
    "write_feed_health_parquet",
]
