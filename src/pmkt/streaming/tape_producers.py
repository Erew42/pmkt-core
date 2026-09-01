from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from pmkt.data.schemas import topbook_evidence_id
from pmkt.data.kalshi_quotes import (
    KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT,
    resolve_kalshi_quote_normalization_policy,
)
from pmkt.exchanges.kalshi.ws import KalshiOrderBookState
from pmkt.exchanges.polymarket.ws import MarketBookState
from pmkt.streaming.capture import TapeBatchIntent
from pmkt.streaming.storage_backends import CaptureCoordinator
from pmkt.streaming.recovery_contracts import CaptureCommitCause
from pmkt.streaming.tape import (
    CaptureCoordinate,
    build_control_row,
    build_tape_batch,
    epoch_id,
    semantic_hash,
)
from pmkt.streaming.venue_tape import (
    kalshi_book_levels,
    kalshi_delta_levels,
    polymarket_book_levels,
    polymarket_delta_levels,
)


@dataclass(frozen=True)
class TapeCaptureEmission:
    batches: tuple[TapeBatchIntent, ...] = ()
    controls: tuple[Mapping[str, Any], ...] = ()
    barrier_cause: CaptureCommitCause | None = None

    def write_to(self, coordinator: CaptureCoordinator) -> None:
        for batch in self.batches:
            for level in batch.levels:
                coordinator.add("tape_level", level)
            coordinator.add("tape_event", batch.event)
        for control in self.controls:
            coordinator.add("tape_control", control)
        if self.barrier_cause is not None:
            coordinator.commit(cause=self.barrier_cause, force=True)
        elif coordinator.barrier_due():
            coordinator.commit()


class _EpochTracker:
    def __init__(self) -> None:
        self.open_epochs: dict[str, str] = {}
        self.generations: dict[str, int] = {}

    def issue(self, coordinate: CaptureCoordinate, venue_book_id: str) -> str:
        generation = self.generations.get(venue_book_id, -1) + 1
        self.generations[venue_book_id] = generation
        return epoch_id(
            coordinate,
            venue_book_id=venue_book_id,
            epoch_generation=generation,
        )

    def open(self, coordinate: CaptureCoordinate, venue_book_id: str) -> str:
        opened = self.issue(coordinate, venue_book_id)
        self.open_epochs[venue_book_id] = opened
        return opened

    def close(self, venue_book_id: str) -> str | None:
        return self.open_epochs.pop(venue_book_id, None)


class CompactValidityProducer:
    """Emit mm-compact validity boundaries backed by canonical topbook evidence."""

    def __init__(self, *, collector_run_id: str, shard_id: str, venue: str) -> None:
        self.collector_run_id = collector_run_id
        self.shard_id = shard_id
        self.venue = venue
        self._valid: dict[str, bool] = {}

    def observe_topbook(
        self,
        *,
        row: Mapping[str, Any],
        coordinate: CaptureCoordinate,
        venue_book_id: str,
        venue_market_id: str,
        venue_sequence: Any | None = None,
    ) -> TapeCaptureEmission:
        current = bool(row.get("valid_state"))
        previous = self._valid.get(venue_book_id)
        self._valid[venue_book_id] = current
        if previous is current:
            return TapeCaptureEmission()
        if current:
            control = build_control_row(
                coordinate=coordinate,
                venue=self.venue,
                venue_market_id=venue_market_id,
                venue_book_id=venue_book_id,
                control_type="book_recovered",
                reason="topbook_validated",
                valid_after=True,
                venue_sequence=row.get("venue_sequence"),
                exchange_at_utc=row.get("exchange_ts_utc"),
                evidence_role="topbook_main",
                evidence_id=topbook_evidence_id(row),
                quality_flags=_row_quality_flags(row),
            )
            # Recovery is safety-conservative while pending: replay cannot use
            # the book until this control and its topbook are journaled.  Let
            # the normal row/time barrier coalesce a reconnect wave instead of
            # creating one Parquet part per recovered instrument.  Invalidations
            # remain forced barriers below.
            return TapeCaptureEmission(controls=(control,))
        control = build_control_row(
            coordinate=coordinate,
            venue=self.venue,
            venue_market_id=venue_market_id,
            venue_book_id=venue_book_id,
            control_type="book_invalidated",
            reason="invalid_state",
            valid_after=False,
            venue_sequence=venue_sequence,
            quality_flags=_row_quality_flags(row),
        )
        return TapeCaptureEmission(
            controls=(control,), barrier_cause=CaptureCommitCause.INVALIDATION
        )

    def invalidate_books(
        self,
        *,
        books: Mapping[str, str],
        received_at_utc: str,
        received_at_monotonic_ns: int,
        local_sequence: int,
        reason: str,
    ) -> TapeCaptureEmission:
        controls = []
        for subsequence, (book_id, market_id) in enumerate(sorted(books.items())):
            self._valid[book_id] = False
            controls.append(
                build_control_row(
                    coordinate=CaptureCoordinate(
                        self.collector_run_id,
                        self.shard_id,
                        received_at_utc,
                        received_at_monotonic_ns,
                        local_sequence,
                        subsequence,
                    ),
                    venue=self.venue,
                    venue_market_id=market_id,
                    venue_book_id=book_id,
                    control_type="book_invalidated",
                    reason=reason,
                    valid_after=False,
                    quality_flags=(reason,),
                )
            )
        return TapeCaptureEmission(
            controls=tuple(controls), barrier_cause=CaptureCommitCause.INVALIDATION
        )

    def ended(
        self,
        *,
        books: Mapping[str, str],
        received_at_utc: str,
        received_at_monotonic_ns: int,
        local_sequence: int,
        reason: str,
    ) -> TapeCaptureEmission:
        controls = []
        for subsequence, (book_id, market_id) in enumerate(sorted(books.items())):
            self._valid[book_id] = False
            controls.append(
                build_control_row(
                    coordinate=CaptureCoordinate(
                        self.collector_run_id,
                        self.shard_id,
                        received_at_utc,
                        received_at_monotonic_ns,
                        local_sequence,
                        subsequence,
                    ),
                    venue=self.venue,
                    venue_market_id=market_id,
                    venue_book_id=book_id,
                    control_type="stream_ended",
                    reason=reason,
                    valid_after=False,
                    quality_flags=(),
                )
            )
        return TapeCaptureEmission(
            controls=tuple(controls), barrier_cause=CaptureCommitCause.TERMINATION
        )


class PolymarketTapeProducer:
    def __init__(self, *, collector_run_id: str, shard_id: str) -> None:
        self.collector_run_id = collector_run_id
        self.shard_id = shard_id
        self._epochs = _EpochTracker()

    def observe(
        self,
        *,
        message: Mapping[str, Any],
        states: Mapping[str, MarketBookState],
        received_at_utc: str,
        received_at_monotonic_ns: int,
        local_sequence: int,
    ) -> TapeCaptureEmission:
        event_type = str(message.get("event_type") or message.get("type") or "")
        books = _polymarket_message_books(message)
        batches: list[TapeBatchIntent] = []
        controls: list[Mapping[str, Any]] = []
        cursor = 0
        for book_id in books:
            state = states.get(book_id)
            if state is None:
                continue
            market_id = str(state.market or message.get("market") or book_id)
            if event_type == "book":
                coordinate = self._coordinate(
                    received_at_utc,
                    received_at_monotonic_ns,
                    local_sequence,
                    cursor + 1,
                )
                closed_epoch = None
                if state.valid_state:
                    opened = self._epochs.open(coordinate, book_id)
                else:
                    closed_epoch = self._epochs.close(book_id)
                    opened = self._epochs.issue(coordinate, book_id)
                levels = polymarket_book_levels(state)
                batch = build_tape_batch(
                    coordinate=coordinate,
                    venue="polymarket",
                    venue_market_id=market_id,
                    venue_book_id=book_id,
                    event_kind="checkpoint",
                    checkpoint_reason="startup"
                    if self._epochs.generations[book_id] == 0
                    else "resync",
                    epoch=opened,
                    levels=levels,
                    full_book_levels=levels,
                    allowed_source_sides=("bid", "ask"),
                    valid_state=state.valid_state,
                    reconstructible=state.valid_state,
                    quality_flags=state.quality_flags,
                    raw_event_hash=semantic_hash(message),
                )
                batches.append(batch)
                if closed_epoch is not None:
                    controls.append(
                        build_control_row(
                            coordinate=self._coordinate(
                                received_at_utc,
                                received_at_monotonic_ns,
                                local_sequence,
                                cursor,
                            ),
                            venue="polymarket",
                            venue_market_id=market_id,
                            venue_book_id=book_id,
                            control_type="book_invalidated",
                            reason="invalid_snapshot",
                            valid_after=False,
                            epoch=closed_epoch,
                            quality_flags=state.quality_flags,
                        )
                    )
                if state.valid_state:
                    controls.append(
                        build_control_row(
                            coordinate=self._coordinate(
                                received_at_utc,
                                received_at_monotonic_ns,
                                local_sequence,
                                cursor + 2,
                            ),
                            venue="polymarket",
                            venue_market_id=market_id,
                            venue_book_id=book_id,
                            control_type="book_recovered",
                            reason="snapshot_validated",
                            valid_after=True,
                            epoch=opened,
                            evidence_role="tape_event",
                            evidence_id=str(batch.event["event_id"]),
                            exchange_at_utc=batch.event.get("exchange_at_utc"),
                            venue_sequence=batch.event.get("venue_sequence"),
                            quality_flags=state.quality_flags,
                        )
                    )
                cursor += 3
                continue
            if event_type != "price_change":
                continue
            open_epoch = self._epochs.open_epochs.get(book_id)
            invalid_reason = _polymarket_invalidation_reason(state)
            if invalid_reason is not None and open_epoch is not None:
                closed_epoch = self._epochs.close(book_id)
                controls.append(
                    build_control_row(
                        coordinate=self._coordinate(
                            received_at_utc,
                            received_at_monotonic_ns,
                            local_sequence,
                            cursor,
                        ),
                        venue="polymarket",
                        venue_market_id=market_id,
                        venue_book_id=book_id,
                        control_type="book_invalidated",
                        reason=invalid_reason,
                        valid_after=False,
                        epoch=closed_epoch,
                        quality_flags=state.quality_flags,
                    )
                )
                open_epoch = None
                cursor += 1
            mutations = polymarket_delta_levels(state, message)
            if mutations and state.valid_state and open_epoch is None:
                coordinate = self._coordinate(
                    received_at_utc,
                    received_at_monotonic_ns,
                    local_sequence,
                    cursor + 1,
                )
                opened = self._epochs.open(coordinate, book_id)
                levels = polymarket_book_levels(state)
                batch = build_tape_batch(
                    coordinate=coordinate,
                    venue="polymarket",
                    venue_market_id=market_id,
                    venue_book_id=book_id,
                    event_kind="checkpoint",
                    checkpoint_reason="resync",
                    epoch=opened,
                    levels=levels,
                    full_book_levels=levels,
                    allowed_source_sides=("bid", "ask"),
                    valid_state=True,
                    reconstructible=True,
                    quality_flags=state.quality_flags,
                    raw_event_hash=semantic_hash(message),
                )
                batches.append(batch)
                controls.append(
                    build_control_row(
                        coordinate=self._coordinate(
                            received_at_utc,
                            received_at_monotonic_ns,
                            local_sequence,
                            cursor + 2,
                        ),
                        venue="polymarket",
                        venue_market_id=market_id,
                        venue_book_id=book_id,
                        control_type="book_recovered",
                        reason="checkpoint_validated",
                        valid_after=True,
                        epoch=opened,
                        evidence_role="tape_event",
                        evidence_id=str(batch.event["event_id"]),
                        exchange_at_utc=batch.event.get("exchange_at_utc"),
                        quality_flags=state.quality_flags,
                    )
                )
                cursor += 3
                continue
            if mutations:
                batches.append(
                    build_tape_batch(
                        coordinate=self._coordinate(
                            received_at_utc,
                            received_at_monotonic_ns,
                            local_sequence,
                            cursor,
                        ),
                        venue="polymarket",
                        venue_market_id=market_id,
                        venue_book_id=book_id,
                        event_kind="delta",
                        epoch=open_epoch,
                        levels=mutations,
                        full_book_levels=polymarket_book_levels(state),
                        allowed_source_sides=("bid", "ask"),
                        valid_state=state.valid_state,
                        reconstructible=open_epoch is not None and state.valid_state,
                        quality_flags=state.quality_flags,
                        raw_event_hash=semantic_hash(message),
                    )
                )
                cursor += 1
        barrier = (
            _emission_barrier(batches, controls)
            if any(batch.event["event_kind"] == "checkpoint" for batch in batches)
            else None
        )
        if event_type == "price_change" and any(
            control["control_type"] == "book_invalidated" for control in controls
        ):
            barrier = CaptureCommitCause.INVALIDATION
        return TapeCaptureEmission(tuple(batches), tuple(controls), barrier)

    def reconnect(
        self,
        *,
        states: Mapping[str, MarketBookState],
        received_at_utc: str,
        received_at_monotonic_ns: int,
        local_sequence: int,
    ) -> TapeCaptureEmission:
        controls = []
        for subsequence, book_id in enumerate(sorted(states)):
            state = states[book_id]
            closed_epoch = self._epochs.close(book_id)
            controls.append(
                build_control_row(
                    coordinate=self._coordinate(
                        received_at_utc,
                        received_at_monotonic_ns,
                        local_sequence,
                        subsequence,
                    ),
                    venue="polymarket",
                    venue_market_id=str(state.market or book_id),
                    venue_book_id=book_id,
                    control_type="book_invalidated",
                    reason="reconnect",
                    valid_after=False,
                    epoch=closed_epoch,
                    quality_flags={*state.quality_flags, "reconnect"},
                )
            )
        return TapeCaptureEmission(
            controls=tuple(controls), barrier_cause=CaptureCommitCause.INVALIDATION
        )

    def checkpoint_states(
        self,
        *,
        states: Mapping[str, MarketBookState],
        received_at_utc: str,
        received_at_monotonic_ns: int,
        local_sequence: int,
        reason: str = "periodic",
    ) -> TapeCaptureEmission:
        batches: list[TapeBatchIntent] = []
        controls: list[Mapping[str, Any]] = []
        cursor = 0
        for book_id in sorted(states):
            state = states[book_id]
            if not state.initial_snapshot_received:
                continue
            coordinate = self._coordinate(
                received_at_utc, received_at_monotonic_ns, local_sequence, cursor + 1
            )
            closed_epoch = None
            if state.valid_state:
                opened = self._epochs.open(coordinate, book_id)
            else:
                closed_epoch = self._epochs.close(book_id)
                opened = self._epochs.issue(coordinate, book_id)
            levels = polymarket_book_levels(state)
            batch = build_tape_batch(
                coordinate=coordinate,
                venue="polymarket",
                venue_market_id=str(state.market or book_id),
                venue_book_id=book_id,
                event_kind="checkpoint",
                checkpoint_reason=reason,
                epoch=opened,
                levels=levels,
                full_book_levels=levels,
                allowed_source_sides=("bid", "ask"),
                valid_state=state.valid_state,
                reconstructible=state.valid_state,
                quality_flags=state.quality_flags,
            )
            batches.append(batch)
            if closed_epoch is not None:
                controls.append(
                    build_control_row(
                        coordinate=self._coordinate(
                            received_at_utc,
                            received_at_monotonic_ns,
                            local_sequence,
                            cursor,
                        ),
                        venue="polymarket",
                        venue_market_id=str(state.market or book_id),
                        venue_book_id=book_id,
                        control_type="book_invalidated",
                        reason="invalid_checkpoint",
                        valid_after=False,
                        epoch=closed_epoch,
                        quality_flags=state.quality_flags,
                    )
                )
            if state.valid_state:
                controls.append(
                    build_control_row(
                        coordinate=self._coordinate(
                            received_at_utc,
                            received_at_monotonic_ns,
                            local_sequence,
                            cursor + 2,
                        ),
                        venue="polymarket",
                        venue_market_id=str(state.market or book_id),
                        venue_book_id=book_id,
                        control_type="book_recovered",
                        reason="checkpoint_validated",
                        valid_after=True,
                        epoch=opened,
                        evidence_role="tape_event",
                        evidence_id=str(batch.event["event_id"]),
                        exchange_at_utc=batch.event.get("exchange_at_utc"),
                        venue_sequence=batch.event.get("venue_sequence"),
                        quality_flags=state.quality_flags,
                    )
                )
            cursor += 3
        return TapeCaptureEmission(
            tuple(batches),
            tuple(controls),
            _emission_barrier(batches, controls) if batches else None,
        )

    def ended(
        self,
        *,
        states: Mapping[str, MarketBookState],
        received_at_utc: str,
        received_at_monotonic_ns: int,
        local_sequence: int,
        reason: str,
    ) -> TapeCaptureEmission:
        controls = []
        for subsequence, book_id in enumerate(sorted(states)):
            state = states[book_id]
            controls.append(
                build_control_row(
                    coordinate=self._coordinate(
                        received_at_utc,
                        received_at_monotonic_ns,
                        local_sequence,
                        subsequence,
                    ),
                    venue="polymarket",
                    venue_market_id=str(state.market or book_id),
                    venue_book_id=book_id,
                    control_type="stream_ended",
                    reason=reason,
                    valid_after=False,
                    epoch=self._epochs.close(book_id),
                    quality_flags=state.quality_flags,
                )
            )
        return TapeCaptureEmission(
            controls=tuple(controls), barrier_cause=CaptureCommitCause.TERMINATION
        )

    def _coordinate(
        self, received_at_utc: str, monotonic_ns: int, sequence: int, subsequence: int
    ) -> CaptureCoordinate:
        return CaptureCoordinate(
            self.collector_run_id,
            self.shard_id,
            received_at_utc,
            monotonic_ns,
            sequence,
            subsequence,
        )


class KalshiTapeProducer:
    def __init__(
        self,
        *,
        collector_run_id: str,
        shard_id: str,
        use_yes_price: bool,
        quote_normalization_policy: str = KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT,
    ) -> None:
        self.collector_run_id = collector_run_id
        self.shard_id = shard_id
        self.use_yes_price = use_yes_price
        self.quote_normalization_policy = resolve_kalshi_quote_normalization_policy(
            quote_normalization_policy
        )
        self._epochs = _EpochTracker()

    def _adapter_settings(self) -> dict[str, Any]:
        return {
            "use_yes_price": self.use_yes_price,
            "quote_normalization_policy": self.quote_normalization_policy,
        }

    def _require_matching_state_settings(self, state: KalshiOrderBookState) -> None:
        if bool(state.use_yes_price) != bool(self.use_yes_price) or (
            state.quote_normalization_policy != self.quote_normalization_policy
        ):
            raise ValueError("Kalshi state and tape-producer adapter settings differ")

    def observe(
        self,
        *,
        message: Mapping[str, Any],
        states: Mapping[str, KalshiOrderBookState],
        received_at_utc: str,
        received_at_monotonic_ns: int,
        local_sequence: int,
    ) -> TapeCaptureEmission:
        event_type = str(message.get("type") or "")
        raw_payload = message.get("msg")
        payload = raw_payload if isinstance(raw_payload, Mapping) else message
        book_id = str(payload.get("market_ticker") or "")
        state = states.get(book_id)
        if state is None or not book_id:
            return TapeCaptureEmission()
        self._require_matching_state_settings(state)
        market_id = str(state.market_id or book_id)
        base = CaptureCoordinate(
            self.collector_run_id,
            self.shard_id,
            received_at_utc,
            received_at_monotonic_ns,
            local_sequence,
            1,
        )
        settings = self._adapter_settings()
        if event_type == "orderbook_snapshot":
            closed_epoch = None
            if state.valid_state:
                opened = self._epochs.open(base, book_id)
            else:
                closed_epoch = self._epochs.close(book_id)
                opened = self._epochs.issue(base, book_id)
            levels = kalshi_book_levels(state)
            batch = build_tape_batch(
                coordinate=base,
                venue="kalshi",
                venue_market_id=market_id,
                venue_book_id=book_id,
                event_kind="checkpoint",
                checkpoint_reason="startup"
                if self._epochs.generations[book_id] == 0
                else "resync",
                epoch=opened,
                levels=levels,
                full_book_levels=levels,
                allowed_source_sides=("yes", "no"),
                valid_state=state.valid_state,
                reconstructible=state.valid_state,
                quality_flags=state.quality_flags,
                venue_sequence=state.last_seq,
                venue_sid=state.sid,
                raw_event_hash=semantic_hash(message),
                adapter_settings=settings,
            )
            snapshot_controls: tuple[Mapping[str, Any], ...] = ()
            if closed_epoch is not None:
                invalidation = build_control_row(
                    coordinate=CaptureCoordinate(
                        self.collector_run_id,
                        self.shard_id,
                        received_at_utc,
                        received_at_monotonic_ns,
                        local_sequence,
                        0,
                    ),
                    venue="kalshi",
                    venue_market_id=market_id,
                    venue_book_id=book_id,
                    control_type="book_invalidated",
                    reason="invalid_snapshot",
                    valid_after=False,
                    epoch=closed_epoch,
                    venue_sequence=state.last_seq,
                    quality_flags=state.quality_flags,
                )
                snapshot_controls = (invalidation,)
            if state.valid_state:
                recovery = build_control_row(
                    coordinate=CaptureCoordinate(
                        self.collector_run_id,
                        self.shard_id,
                        received_at_utc,
                        received_at_monotonic_ns,
                        local_sequence,
                        2,
                    ),
                    venue="kalshi",
                    venue_market_id=market_id,
                    venue_book_id=book_id,
                    control_type="book_recovered",
                    reason="snapshot_validated",
                    valid_after=True,
                    epoch=opened,
                    venue_sequence=batch.event.get("venue_sequence"),
                    evidence_role="tape_event",
                    evidence_id=str(batch.event["event_id"]),
                    exchange_at_utc=batch.event.get("exchange_at_utc"),
                    quality_flags=state.quality_flags,
                )
                snapshot_controls = (recovery,)
            return TapeCaptureEmission(
                (batch,),
                snapshot_controls,
                _emission_barrier((batch,), snapshot_controls),
            )
        if event_type != "orderbook_delta":
            return TapeCaptureEmission()
        controls: list[Mapping[str, Any]] = []
        open_epoch = self._epochs.open_epochs.get(book_id)
        invalid_reason = _kalshi_invalidation_reason(state)
        if invalid_reason is not None and open_epoch is not None:
            closed_epoch = self._epochs.close(book_id)
            controls.append(
                build_control_row(
                    coordinate=CaptureCoordinate(
                        self.collector_run_id,
                        self.shard_id,
                        received_at_utc,
                        received_at_monotonic_ns,
                        local_sequence,
                        0,
                    ),
                    venue="kalshi",
                    venue_market_id=market_id,
                    venue_book_id=book_id,
                    control_type="book_invalidated",
                    reason=invalid_reason,
                    valid_after=False,
                    epoch=closed_epoch,
                    venue_sequence=state.last_seq,
                    quality_flags=state.quality_flags,
                )
            )
            open_epoch = None
        mutations = kalshi_delta_levels(state, message)
        if not mutations:
            return TapeCaptureEmission(
                controls=tuple(controls),
                barrier_cause=CaptureCommitCause.INVALIDATION if controls else None,
            )
        if state.valid_state and open_epoch is None:
            opened = self._epochs.open(base, book_id)
            levels = kalshi_book_levels(state)
            batch = build_tape_batch(
                coordinate=base,
                venue="kalshi",
                venue_market_id=market_id,
                venue_book_id=book_id,
                event_kind="checkpoint",
                checkpoint_reason="resync",
                epoch=opened,
                levels=levels,
                full_book_levels=levels,
                allowed_source_sides=("yes", "no"),
                valid_state=True,
                reconstructible=True,
                quality_flags=state.quality_flags,
                venue_sequence=state.last_seq,
                venue_sid=state.sid,
                raw_event_hash=semantic_hash(message),
                adapter_settings=settings,
            )
            recovery = build_control_row(
                coordinate=CaptureCoordinate(
                    self.collector_run_id,
                    self.shard_id,
                    received_at_utc,
                    received_at_monotonic_ns,
                    local_sequence,
                    2,
                ),
                venue="kalshi",
                venue_market_id=market_id,
                venue_book_id=book_id,
                control_type="book_recovered",
                reason="checkpoint_validated",
                valid_after=True,
                epoch=opened,
                venue_sequence=batch.event.get("venue_sequence"),
                evidence_role="tape_event",
                evidence_id=str(batch.event["event_id"]),
                exchange_at_utc=batch.event.get("exchange_at_utc"),
                quality_flags=state.quality_flags,
            )
            return TapeCaptureEmission(
                (batch,), (recovery,), _emission_barrier((batch,), (recovery,))
            )
        batch = build_tape_batch(
            coordinate=base,
            venue="kalshi",
            venue_market_id=market_id,
            venue_book_id=book_id,
            event_kind="delta",
            epoch=open_epoch,
            levels=mutations,
            full_book_levels=kalshi_book_levels(state),
            allowed_source_sides=("yes", "no"),
            valid_state=state.valid_state,
            reconstructible=open_epoch is not None and state.valid_state,
            quality_flags=state.quality_flags,
            venue_sequence=state.last_seq,
            venue_sid=state.sid,
            raw_event_hash=semantic_hash(message),
            adapter_settings=settings,
        )
        return TapeCaptureEmission(
            (batch,),
            tuple(controls),
            CaptureCommitCause.INVALIDATION if controls else None,
        )

    def checkpoint_states(
        self,
        *,
        states: Mapping[str, KalshiOrderBookState],
        received_at_utc: str,
        received_at_monotonic_ns: int,
        local_sequence: int,
        reason: str = "periodic",
    ) -> TapeCaptureEmission:
        batches: list[TapeBatchIntent] = []
        controls: list[Mapping[str, Any]] = []
        cursor = 0
        for book_id in sorted(states):
            state = states[book_id]
            self._require_matching_state_settings(state)
            if not state.initial_snapshot_received:
                continue
            coordinate = CaptureCoordinate(
                self.collector_run_id,
                self.shard_id,
                received_at_utc,
                received_at_monotonic_ns,
                local_sequence,
                cursor + 1,
            )
            closed_epoch = None
            if state.valid_state:
                opened = self._epochs.open(coordinate, book_id)
            else:
                closed_epoch = self._epochs.close(book_id)
                opened = self._epochs.issue(coordinate, book_id)
            levels = kalshi_book_levels(state)
            batch = build_tape_batch(
                coordinate=coordinate,
                venue="kalshi",
                venue_market_id=str(state.market_id or book_id),
                venue_book_id=book_id,
                event_kind="checkpoint",
                checkpoint_reason=reason,
                epoch=opened,
                levels=levels,
                full_book_levels=levels,
                allowed_source_sides=("yes", "no"),
                valid_state=state.valid_state,
                reconstructible=state.valid_state,
                quality_flags=state.quality_flags,
                venue_sequence=state.last_seq,
                venue_sid=state.sid,
                adapter_settings=self._adapter_settings(),
            )
            batches.append(batch)
            if closed_epoch is not None:
                controls.append(
                    build_control_row(
                        coordinate=CaptureCoordinate(
                            self.collector_run_id,
                            self.shard_id,
                            received_at_utc,
                            received_at_monotonic_ns,
                            local_sequence,
                            cursor,
                        ),
                        venue="kalshi",
                        venue_market_id=str(state.market_id or book_id),
                        venue_book_id=book_id,
                        control_type="book_invalidated",
                        reason="invalid_checkpoint",
                        valid_after=False,
                        epoch=closed_epoch,
                        venue_sequence=state.last_seq,
                        quality_flags=state.quality_flags,
                    )
                )
            if state.valid_state:
                controls.append(
                    build_control_row(
                        coordinate=CaptureCoordinate(
                            self.collector_run_id,
                            self.shard_id,
                            received_at_utc,
                            received_at_monotonic_ns,
                            local_sequence,
                            cursor + 2,
                        ),
                        venue="kalshi",
                        venue_market_id=str(state.market_id or book_id),
                        venue_book_id=book_id,
                        control_type="book_recovered",
                        reason="checkpoint_validated",
                        valid_after=True,
                        epoch=opened,
                        venue_sequence=batch.event.get("venue_sequence"),
                        evidence_role="tape_event",
                        evidence_id=str(batch.event["event_id"]),
                        exchange_at_utc=batch.event.get("exchange_at_utc"),
                        quality_flags=state.quality_flags,
                    )
                )
            cursor += 3
        return TapeCaptureEmission(
            tuple(batches),
            tuple(controls),
            _emission_barrier(batches, controls) if batches else None,
        )

    def reconnect(
        self,
        *,
        states: Mapping[str, KalshiOrderBookState],
        received_at_utc: str,
        received_at_monotonic_ns: int,
        local_sequence: int,
    ) -> TapeCaptureEmission:
        controls = []
        for subsequence, book_id in enumerate(sorted(states)):
            state = states[book_id]
            controls.append(
                build_control_row(
                    coordinate=CaptureCoordinate(
                        self.collector_run_id,
                        self.shard_id,
                        received_at_utc,
                        received_at_monotonic_ns,
                        local_sequence,
                        subsequence,
                    ),
                    venue="kalshi",
                    venue_market_id=str(state.market_id or book_id),
                    venue_book_id=book_id,
                    control_type="book_invalidated",
                    reason="reconnect",
                    valid_after=False,
                    epoch=self._epochs.close(book_id),
                    venue_sequence=state.last_seq,
                    quality_flags={*state.quality_flags, "reconnect"},
                )
            )
        return TapeCaptureEmission(
            controls=tuple(controls), barrier_cause=CaptureCommitCause.INVALIDATION
        )

    def ended(
        self,
        *,
        states: Mapping[str, KalshiOrderBookState],
        received_at_utc: str,
        received_at_monotonic_ns: int,
        local_sequence: int,
        reason: str,
    ) -> TapeCaptureEmission:
        controls = []
        for subsequence, book_id in enumerate(sorted(states)):
            state = states[book_id]
            controls.append(
                build_control_row(
                    coordinate=CaptureCoordinate(
                        self.collector_run_id,
                        self.shard_id,
                        received_at_utc,
                        received_at_monotonic_ns,
                        local_sequence,
                        subsequence,
                    ),
                    venue="kalshi",
                    venue_market_id=str(state.market_id or book_id),
                    venue_book_id=book_id,
                    control_type="stream_ended",
                    reason=reason,
                    valid_after=False,
                    epoch=self._epochs.close(book_id),
                    venue_sequence=state.last_seq,
                    quality_flags=state.quality_flags,
                )
            )
        return TapeCaptureEmission(
            controls=tuple(controls), barrier_cause=CaptureCommitCause.TERMINATION
        )


def _emission_barrier(
    batches: Sequence[TapeBatchIntent],
    controls: Sequence[Mapping[str, Any]],
) -> CaptureCommitCause:
    if any(
        str(control.get("control_type") or "") == "book_invalidated"
        for control in controls
    ):
        return CaptureCommitCause.INVALIDATION
    return _checkpoint_cause(batches)


def _checkpoint_cause(
    batches: Sequence[TapeBatchIntent],
) -> CaptureCommitCause:
    reasons = {str(batch.event.get("checkpoint_reason") or "") for batch in batches}
    if "resync" in reasons:
        return CaptureCommitCause.CHECKPOINT_RESYNC
    if "startup" in reasons:
        return CaptureCommitCause.CHECKPOINT_STARTUP
    return CaptureCommitCause.CHECKPOINT_PERIODIC


def _polymarket_message_books(message: Mapping[str, Any]) -> tuple[str, ...]:
    event_type = str(message.get("event_type") or message.get("type") or "")
    if event_type == "price_change":
        changes = message.get("price_changes")
        if not isinstance(changes, Sequence) or isinstance(changes, (str, bytes)):
            return ()
        books = {
            str(change.get("asset_id") or message.get("asset_id") or "")
            for change in changes
            if isinstance(change, Mapping)
        }
        return tuple(sorted(book for book in books if book))
    book = str(message.get("asset_id") or "")
    return (book,) if book else ()


def _polymarket_invalidation_reason(state: MarketBookState) -> str | None:
    if "hash_mismatch" in state.quality_flags:
        return "hash_mismatch"
    if "delta_before_snapshot" in state.quality_flags:
        return "delta_before_snapshot"
    if not state.valid_state:
        return "invalid_state"
    return None


def _kalshi_invalidation_reason(state: KalshiOrderBookState) -> str | None:
    flags = state.quality_flags
    if "sid_changed" in flags:
        return "sid_changed"
    if "seq_gap" in flags:
        return "sequence_gap"
    if "missing_sequence" in flags:
        return "missing_sequence"
    if "delta_before_snapshot" in flags:
        return "delta_before_snapshot"
    if not state.valid_state:
        return "invalid_state"
    return None


def _row_quality_flags(row: Mapping[str, Any]) -> tuple[str, ...]:
    value = row.get("quality_flags")
    if isinstance(value, str):
        return tuple(flag for flag in value.replace(",", ";").split(";") if flag)
    if isinstance(value, Sequence):
        return tuple(str(flag) for flag in value if str(flag))
    return ()


__all__ = [
    "CompactValidityProducer",
    "KalshiTapeProducer",
    "PolymarketTapeProducer",
    "TapeCaptureEmission",
]
