"""Changed topbooks, sampled full books, trade observations and recording facts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import math
from pathlib import Path
import time
from typing import Any, Callable, Literal, Mapping, TextIO
from uuid import uuid4

from pmkt.streaming.recording_store import (
    BATCH_ROWS,
    BATCH_SECONDS,
    FORMAT,
    RecordingStore,
    export_recording,
    json_text,
    write_json,
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class RecordingOptions:
    mode: Literal["topbook", "full"] = "full"
    depth_check_interval_s: float | None = 10.0
    depth_on_best_price_change: bool = False
    raw_messages: bool = False

    def __post_init__(self) -> None:
        if self.mode not in {"topbook", "full"}:
            raise ValueError("mode must be 'topbook' or 'full'")
        if not isinstance(self.depth_on_best_price_change, bool) or not isinstance(
            self.raw_messages, bool
        ):
            raise ValueError("recording flags must be booleans")
        if self.mode == "full":
            interval = self.depth_check_interval_s
            if interval is None:
                if not self.depth_on_best_price_change:
                    raise ValueError(
                        "full mode requires a depth interval or the best-price trigger"
                    )
            elif (
                isinstance(interval, bool)
                or not math.isfinite(interval)
                or interval <= 0
            ):
                raise ValueError(
                    "depth_check_interval_s must be finite and positive, or None"
                )


@dataclass
class BookView:
    instrument_id: str
    bids: Mapping[float, float]
    asks: Mapping[float, float]
    initialized: bool
    integrity_valid: bool
    quote_valid: bool
    quality_flags: tuple[str, ...] = ()
    tick_size: float | None = None
    minimum_order_size: float | None = None
    venue_time_utc: str | None = None
    venue_market_id: str | None = None
    source_book_update: bool = True

    @property
    def prices(self) -> tuple[float | None, float | None]:
        return max(self.bids, default=None), min(self.asks, default=None)

    def state_fields(self) -> dict[str, Any]:
        return {
            "initialized": self.initialized,
            "integrity_valid": self.integrity_valid,
            "quote_valid": self.quote_valid,
            "tick_size": self.tick_size,
            "minimum_order_size": self.minimum_order_size,
            "quality_flags_json": json_text(sorted(self.quality_flags)),
        }

    def top_fields(self) -> dict[str, Any]:
        bid, ask = self.prices
        return {
            **self.state_fields(),
            "bid_price": bid,
            "ask_price": ask,
            "bid_quantity": self.bids.get(bid) if bid is not None else None,
            "ask_quantity": self.asks.get(ask) if ask is not None else None,
        }

    def depth_state(self) -> tuple[Any, ...]:
        return dict(self.bids), dict(self.asks), self.state_fields()


class BookRecorder:
    def __init__(
        self,
        *,
        venue: str,
        instruments: list[str],
        output_root: str | Path,
        options: RecordingOptions,
        run_name: str | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], str] = utc_now,
        provenance: Mapping[str, Any] | None = None,
    ) -> None:
        if not instruments or len(set(instruments)) != len(instruments):
            raise ValueError("requested instruments must be nonempty and unique")
        name = (
            run_name
            or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            + "-"
            + uuid4().hex[:10]
        )
        if (
            name in {".", ".."}
            or Path(name).name != name
            or "/" in name
            or "\\" in name
        ):
            raise ValueError("run_name must be a single directory name")
        self.run_dir = Path(output_root).resolve() / name
        self.run_dir.mkdir(parents=True, exist_ok=False)
        self.options, self.venue, self.instruments = options, venue, instruments
        self.provenance = dict(provenance or {})
        self.monotonic, self.wall_clock = monotonic, wall_clock
        self.started = monotonic()
        self.started_utc = wall_clock()
        interval = options.depth_check_interval_s if options.mode == "full" else None
        self.next_depth = self.started + interval if interval is not None else math.inf
        self.next_liveness = self.started + 10.0
        self.generation = 0
        self.sequence = 0
        self.source_sequence = 0
        self.last_message_time: str | None = None
        self.books: dict[str, BookView] = {}
        self.coordinates: dict[str, dict[str, Any]] = {}
        self.latest_book_source: dict[str, dict[str, Any]] = {}
        self.last_top: dict[str, dict[str, Any]] = {}
        self.last_depth: dict[str, tuple[Any, ...]] = {}
        self.depth_dirty: set[str] = set()
        self.ever_intact: set[str] = set()
        self.initialized: set[str] = set()
        self.raw: TextIO | None = None
        self.counts = {
            "topbook": 0,
            "book_snapshots": 0,
            "book_levels": 0,
            "trades": 0,
            "events": 0,
        }
        self.status = "recording"
        self.stop_reason: str | None = None
        self.raw_failed = False
        self.error: str | None = None
        self.store = RecordingStore(self.run_dir, self.report())
        try:
            if options.raw_messages:
                self.raw = (self.run_dir / "raw_messages.jsonl").open(
                    "x", encoding="utf-8"
                )
        except BaseException:
            self.store.close()
            raise

    def report(self) -> dict[str, Any]:
        return {
            "recording_format": FORMAT,
            "run_id": self.run_dir.name,
            "run_dir": str(self.run_dir),
            "venue": self.venue,
            "options": asdict(self.options),
            "requested_instruments": self.instruments,
            "initialized_instruments": sorted(self.initialized),
            "ever_intact_instruments": sorted(self.ever_intact),
            "missing_initial_books": sorted(set(self.instruments) - self.initialized),
            "instrument_states": {
                instrument: {
                    "initialized": bool(
                        (book := self.books.get(instrument)) and book.initialized
                    ),
                    "integrity_valid": bool(book and book.integrity_valid),
                    "latest_book_observation": self.latest_book_source.get(instrument),
                }
                for instrument in self.instruments
            },
            "connection_generation": self.generation,
            "reconnect_count": max(0, self.generation - 1),
            "message_count": self.source_sequence,
            "record_sequence": self.sequence,
            "counts": dict(self.counts),
            "started_at_utc": self.started_utc,
            "as_of_utc": self.wall_clock(),
            "status": self.status,
            "stop_reason": self.stop_reason,
            "error": self.error,
            "batch_rows": BATCH_ROWS,
            "batch_seconds": BATCH_SECONDS,
            "batch_boundary": "after a complete source message or timer observation; an atomic snapshot may exceed batch_rows",
            "raw_atomic_with_sqlite": False,
            "provenance": self.provenance,
        }

    def common(
        self, instrument: str | None = None, *, source: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        self.sequence += 1
        return {
            "run_id": self.run_dir.name,
            "record_sequence": self.sequence,
            "venue": self.venue,
            "instrument_id": instrument,
            "venue_market_id": None,
            "connection_generation": self.generation,
            "source_sequence": None,
            "received_at_utc": None,
            "received_monotonic_ns": None,
            "venue_time_utc": None,
            **(source or {}),
            "observed_at_utc": self.wall_clock(),
        }

    def append(self, table: str, row: dict[str, Any]) -> None:
        try:
            self.store.append(table, row)
        except BaseException:
            self.store.failed = True
            self.store.connection.rollback()
            raise
        self.counts[table] += 1

    def event(
        self,
        kind: str,
        details: Mapping[str, Any],
        *,
        instrument: str | None = None,
        source: Mapping[str, Any] | None = None,
    ) -> None:
        self.append(
            "events",
            {
                **self.common(instrument, source=source),
                "kind": kind,
                "details_json": json_text(details),
            },
        )

    def invalidate(self, reason: str) -> None:
        for instrument, book in list(self.books.items()):
            invalid = BookView(instrument, {}, {}, False, False, False, (reason,))
            self.observe_books([invalid], source=None)
        self.event(reason, {})

    def subscription_start(self) -> None:
        self.generation += 1
        self.event("subscription_start", {"instruments": self.instruments})

    def message(self, message: Mapping[str, Any]) -> dict[str, Any]:
        self.source_sequence += 1
        self.last_message_time = self.wall_clock()
        coordinate = {
            "source_sequence": self.source_sequence,
            "connection_generation": self.generation,
            "received_at_utc": self.last_message_time,
            "received_monotonic_ns": time.monotonic_ns(),
        }
        if self.raw is not None:
            try:
                self.raw.write(json_text({**coordinate, "message": message}) + "\n")
            except (OSError, ValueError):
                self.raw_failed = True
                raise
        return coordinate

    def depth_due(self) -> bool:
        return self.monotonic() >= self.next_depth

    def observe_books(
        self, books: list[BookView], *, source: Mapping[str, Any] | None
    ) -> None:
        # Adapters apply a whole source message before handing over any views.
        for book in books:
            instrument = book.instrument_id
            previous = self.last_top.get(instrument)
            top = book.top_fields()
            recovered = book.integrity_valid and not (
                previous and previous["integrity_valid"]
            )
            first = instrument not in self.ever_intact
            self.books[instrument] = book
            self.depth_dirty.add(instrument)
            self.coordinates[instrument] = {
                **(source or {}),
                "venue_time_utc": book.venue_time_utc,
                "venue_market_id": book.venue_market_id,
            }
            if source is not None and book.source_book_update:
                self.latest_book_source[instrument] = dict(self.coordinates[instrument])
            if book.initialized:
                self.initialized.add(instrument)
            if previous != top or recovered:
                self.append(
                    "topbook",
                    {
                        **self.common(instrument, source=self.coordinates[instrument]),
                        **top,
                    },
                )
                self.last_top[instrument] = top
            if previous and previous["integrity_valid"] and not book.integrity_valid:
                self.event(
                    "book_invalidated",
                    {"quality_flags": book.quality_flags},
                    instrument=instrument,
                    source=source,
                )
            if recovered:
                self.event(
                    "book_initialized" if first else "book_recovered",
                    {},
                    instrument=instrument,
                    source=source,
                )
            causes = []
            if recovered:
                causes.append("initial" if first else "recovery")
            if (
                self.options.depth_on_best_price_change
                and previous
                and book.prices != (previous["bid_price"], previous["ask_price"])
            ):
                causes.append("best_price_change")
            if self.depth_due():
                causes.append("interval")
            if causes:
                self.snapshot(book, causes, force=recovered)
            if book.integrity_valid:
                self.ever_intact.add(instrument)

    def snapshot(
        self, book: BookView, causes: list[str], *, force: bool = False
    ) -> None:
        if self.options.mode != "full" or not book.integrity_valid:
            return
        if not force and book.instrument_id not in self.depth_dirty:
            return
        state = book.depth_state()
        if not force and self.last_depth.get(book.instrument_id) == state:
            self.depth_dirty.discard(book.instrument_id)
            return
        header = {
            **self.common(
                book.instrument_id, source=self.coordinates[book.instrument_id]
            ),
            **book.state_fields(),
            "causes_json": json_text(causes),
            "bid_level_count": len(book.bids),
            "ask_level_count": len(book.asks),
        }
        snapshot_id = header["record_sequence"]
        header["snapshot_id"] = snapshot_id
        self.append("book_snapshots", header)
        for side, levels in (("bid", book.bids), ("ask", book.asks)):
            for price, quantity in sorted(levels.items()):
                self.append(
                    "book_levels",
                    {
                        "run_id": self.run_dir.name,
                        "snapshot_id": snapshot_id,
                        "side": side,
                        "price": price,
                        "quantity": quantity,
                    },
                )
        self.last_depth[book.instrument_id] = state
        self.depth_dirty.discard(book.instrument_id)

    def tick(self, transport: Mapping[str, Any] | None = None) -> None:
        now = self.monotonic()
        if self.depth_due():
            for book in self.books.values():
                self.snapshot(book, ["interval"])
            interval = self.options.depth_check_interval_s
            assert interval is not None
            self.next_depth = (
                self.started
                + (math.floor((now - self.started) / interval) + 1) * interval
            )
        if now >= self.next_liveness:
            self.event(
                "liveness",
                {
                    "last_message_received_at_utc": self.last_message_time,
                    "transport": dict(transport or {}),
                    "instruments": self.report()["instrument_states"],
                },
            )
            self.next_liveness = (
                self.started + (math.floor((now - self.started) / 10) + 1) * 10
            )
        if self.store.flush_due():
            self.store.flush(self.report())
        if self.raw is not None:
            try:
                self.raw.flush()
            except (OSError, ValueError):
                self.raw_failed = True
                raise

    def finish(
        self, *, reason: str, error: BaseException | None = None, normal: bool = False
    ) -> dict[str, Any]:
        self.stop_reason = reason
        self.error = str(error) if error else None
        intact = all(
            self.books.get(i) is not None and self.books[i].integrity_valid
            for i in self.instruments
        )
        self.status = "complete" if normal and intact else "partial"
        if not self.ever_intact or self.store.failed or self.raw_failed:
            self.status = "failed"
        try:
            if normal:
                for book in self.books.values():
                    self.snapshot(book, ["stop"])
            if self.raw is not None:
                self.raw.close()
                self.raw = None
            self.event("stop", {"reason": reason, "error": self.error})
            self.store.flush(self.report(), force=True)
        except BaseException as exc:
            self.status = "failed"
            self.error = str(exc)
            write_json(
                self.run_dir / "failure.json", {**self.report(), "phase": "persistence"}
            )
            raise
        finally:
            self.store.close()
            if self.raw is not None:
                self.raw.close()
                self.raw = None
        return export_recording(self.run_dir)
