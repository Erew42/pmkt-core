from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sqlite3

import pyarrow.parquet as pq
import pytest

from pmkt.streaming.recording import BookRecorder, BookView, RecordingOptions
from pmkt.streaming.recording_feed import RecordingError, record_feed
from pmkt.streaming.recording_store import export_recording


class Clock:
    now = 0.0

    def __call__(self):
        return self.now


def view(bid=0.4, size=10.0, *, deep=1.0, intact=True):
    return BookView("a", {bid: size, 0.1: deep}, {0.6: 8.0}, True, intact, intact)


def start(tmp_path, **options):
    clock = Clock()
    recorder = BookRecorder(
        venue="polymarket",
        instruments=["a"],
        output_root=tmp_path,
        options=RecordingOptions(**options),
        monotonic=clock,
    )
    recorder.subscription_start()
    return recorder, clock


def observe(recorder, book):
    recorder.observe_books([book], source=recorder.message({"test": True}))


def rows(recorder, table):
    return pq.read_table(recorder.run_dir / f"{table}.parquet").to_pylist()


def test_interval_compares_net_state_and_preserves_source_time(tmp_path):
    r, c = start(tmp_path)
    observe(r, view())
    c.now = 3
    observe(r, view(deep=2))
    c.now = 7
    observe(r, view())
    c.now = 10
    r.tick()
    assert r.counts["book_snapshots"] == 1
    c.now = 14
    observe(r, view(deep=3))
    c.now = 20
    r.tick()
    c.now = 30
    r.tick()
    r.finish(reason="duration", normal=True)
    books = rows(r, "book_snapshots")
    assert len(books) == 2
    assert books[1]["source_sequence"] == 4
    assert len(rows(r, "topbook")) == 1
    assert len([e for e in rows(r, "events") if e["kind"] == "liveness"]) == 3


def test_price_only_retains_initial_recovery_and_terminal_snapshots(tmp_path):
    r, c = start(tmp_path, depth_check_interval_s=None, depth_on_best_price_change=True)
    observe(r, view())
    observe(r, view(size=11, deep=2))
    c.now = 50
    r.tick()
    assert r.counts["book_snapshots"] == 1
    observe(r, view(bid=0.5))
    r.invalidate("disconnect")
    r.subscription_start()
    observe(r, view(bid=0.5))
    observe(r, view(bid=0.5, deep=4))
    r.finish(reason="duration", normal=True)
    books = rows(r, "book_snapshots")
    assert len(books) == 4
    assert [json.loads(row["causes_json"]) for row in books] == [
        ["initial"],
        ["best_price_change"],
        ["recovery", "best_price_change"],
        ["stop"],
    ]
    assert books[2]["connection_generation"] == 2


def test_combined_triggers_do_not_duplicate_or_reset_schedule(tmp_path):
    r, c = start(tmp_path, depth_on_best_price_change=True)
    observe(r, view())
    c.now = 10
    observe(r, view(bid=0.5))
    r.tick()
    assert r.counts["book_snapshots"] == 2
    assert r.next_depth == 20
    c.now = 19
    observe(r, view(bid=0.45))
    assert r.next_depth == 20
    c.now = 37
    observe(r, view(deep=8))
    r.tick()
    assert r.next_depth == 40
    r.finish(reason="duration", normal=True)
    assert json.loads(rows(r, "book_snapshots")[1]["causes_json"]) == [
        "best_price_change",
        "interval",
    ]


def test_empty_intact_book_has_header_and_no_levels(tmp_path):
    r, _ = start(tmp_path)
    observe(r, BookView("a", {}, {}, True, True, False, ("empty_bid", "empty_ask")))
    report = r.finish(reason="duration", normal=True)
    assert report["status"] == "complete"
    assert len(rows(r, "book_snapshots")) == 1
    assert rows(r, "book_levels") == []


@pytest.mark.parametrize("interval", [0, -1, float("nan"), float("inf"), False])
def test_invalid_interval(interval):
    with pytest.raises(ValueError):
        RecordingOptions(depth_check_interval_s=interval)


def test_none_requires_price_trigger_in_full_mode():
    with pytest.raises(ValueError, match="requires"):
        RecordingOptions(depth_check_interval_s=None)
    RecordingOptions(mode="topbook", depth_check_interval_s=None)


def test_topbook_only_raw_optional_and_reexport_idempotent(tmp_path):
    r, _ = start(tmp_path, mode="topbook", raw_messages=True)
    observe(r, view())
    observe(r, view(size=11))
    report = r.finish(reason="duration", normal=True)
    assert set(report["artifacts"]) == {"topbook", "trades", "events"}
    assert not (r.run_dir / "book_levels.parquet").exists()
    assert len((r.run_dir / "raw_messages.jsonl").read_text().splitlines()) == 2
    assert export_recording(r.run_dir) == report


def test_snapshot_transaction_rolls_back_as_a_unit(tmp_path):
    r, _ = start(tmp_path)
    observe(r, view())
    r.store.flush(r.report(), force=True)
    original = r.store.append

    def fail_level(table, row):
        if table == "book_levels":
            row = dict(row, quantity=None)
        original(table, row)

    r.store.append = fail_level
    observe(r, view(deep=2))
    with pytest.raises(sqlite3.IntegrityError):
        r.snapshot(view(deep=2), ["stop"])
    r.store.close()
    report = export_recording(r.run_dir)
    assert report["status"] == "partial"
    assert report["counts"]["book_snapshots"] == 1
    assert report["counts"]["book_levels"] == 3


def test_export_refuses_broken_snapshot_relationships(tmp_path):
    r, _ = start(tmp_path)
    observe(r, view())
    r.finish(reason="duration", normal=True)
    with sqlite3.connect(r.run_dir / "recording.sqlite") as db:
        db.execute("DELETE FROM book_levels WHERE side='bid'")
    with pytest.raises(ValueError, match="disagree"):
        export_recording(r.run_dir)
    assert not (r.run_dir / "manifest.json").exists()
    assert (r.run_dir / "failure.json").exists()


class Socket:
    def __init__(self, messages):
        self.messages = iter(messages)
        self.closed = False
        self.sent = []

    async def send(self, message):
        self.sent.append(json.loads(message))

    async def close(self):
        self.closed = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = next(self.messages, None)
        if item is None:
            await asyncio.sleep(3600)
        if isinstance(item, BaseException):
            raise item
        return json.dumps(item)


def poly_book(asset="a", *, bids=None, asks=None):
    return {
        "event_type": "book",
        "asset_id": asset,
        "market": "m",
        "bids": bids if bids is not None else [{"price": "0.4", "size": "10"}],
        "asks": asks if asks is not None else [{"price": "0.6", "size": "10"}],
    }


@pytest.mark.asyncio
async def test_polymarket_recording_retains_trade_observations(tmp_path):
    trade = {"event_type": "last_trade_price", "asset_id": "a", "price": "0.42"}
    socket = Socket([poly_book(), trade, trade])
    report = await record_feed(
        ["a"],
        venue="polymarket",
        output_root=tmp_path,
        options=RecordingOptions(),
        max_messages=3,
        heartbeat_interval=None,
        connect_factory=lambda _: socket,
    )
    assert report["status"] == "complete"
    root = Path(report["run_dir"])
    trades = pq.read_table(root / "trades.parquet").to_pylist()
    assert len(trades) == 2
    assert trades[0]["quantity"] is None
    assert trades[0]["venue_trade_id"] is None
    assert trades[0]["venue_time_utc"] is None
    assert report["counts"]["topbook"] == 1
    assert not (root / "raw_messages.jsonl").exists()
    assert socket.closed


class ReadAuth:
    def headers_for_get(self, path):
        return {}


@pytest.mark.asyncio
async def test_kalshi_trade_dedup_and_yes_book(tmp_path):
    book = {
        "type": "orderbook_snapshot",
        "sid": 1,
        "seq": 1,
        "msg": {
            "market_ticker": "A",
            "yes_dollars_fp": [["0.4", "10"]],
            "no_dollars_fp": [["0.6", "20"]],
        },
    }
    trade = {
        "type": "trade",
        "msg": {
            "market_ticker": "A",
            "trade_id": "t",
            "yes_price_dollars": "0.42",
            "count_fp": "2",
        },
    }
    socket = Socket([book, trade, trade])
    report = await record_feed(
        ["A"],
        venue="kalshi",
        output_root=tmp_path,
        options=RecordingOptions(),
        max_messages=3,
        auth=ReadAuth(),
        connect_factory=lambda *args: socket,
    )
    assert report["status"] == "complete"
    assert report["counts"]["trades"] == 1
    top = pq.read_table(Path(report["run_dir"]) / "topbook.parquet").to_pylist()[0]
    assert (top["instrument_id"], top["bid_price"], top["ask_price"]) == (
        "A:YES",
        0.4,
        0.6,
    )
    assert "trade" in socket.sent[0]["params"]["channels"]


@pytest.mark.asyncio
async def test_no_initial_books_cannot_report_success(tmp_path):
    socket = Socket([])
    with pytest.raises(RecordingError) as exc:
        await record_feed(
            ["a"],
            venue="polymarket",
            output_root=tmp_path,
            options=RecordingOptions(),
            duration_s=0.02,
            heartbeat_interval=None,
            connect_factory=lambda _: socket,
        )
    report = json.loads((exc.value.run_dir / "manifest.json").read_text())
    assert report["status"] == "failed"
    assert report["missing_initial_books"] == ["a"]


@pytest.mark.asyncio
async def test_missing_instrument_is_partial_without_eligibility_rules(tmp_path):
    socket = Socket([poly_book()])
    report = await record_feed(
        ["a", "b"],
        venue="polymarket",
        output_root=tmp_path,
        options=RecordingOptions(),
        max_messages=1,
        heartbeat_interval=None,
        connect_factory=lambda _: socket,
    )
    assert report["status"] == "partial"
    assert report["missing_initial_books"] == ["b"]


@pytest.mark.asyncio
async def test_reconnect_invalidates_and_fresh_identical_book_is_saved(tmp_path):
    sockets = iter([Socket([poly_book(), OSError("lost")]), Socket([poly_book()])])
    report = await record_feed(
        ["a"],
        venue="polymarket",
        output_root=tmp_path,
        options=RecordingOptions(),
        max_messages=2,
        heartbeat_interval=None,
        connect_factory=lambda _: next(sockets),
    )
    assert report["status"] == "complete"
    assert report["counts"]["book_snapshots"] == 2
    assert report["reconnect_count"] == 1
    top = pq.read_table(Path(report["run_dir"]) / "topbook.parquet").to_pylist()
    assert [row["integrity_valid"] for row in top] == [True, False, True]


@pytest.mark.asyncio
async def test_book_metadata_and_resolution_are_recorded(tmp_path):
    first = {**poly_book(), "tick_size": "0.01", "min_order_size": "5"}
    socket = Socket([first, {"event_type": "market_resolved", "market": "m"}])
    report = await record_feed(
        ["a"],
        venue="polymarket",
        output_root=tmp_path,
        options=RecordingOptions(),
        max_messages=2,
        heartbeat_interval=None,
        connect_factory=lambda _: socket,
    )
    top = pq.read_table(Path(report["run_dir"]) / "topbook.parquet").to_pylist()
    assert len(top) == 2
    assert top[0]["tick_size"] == 0.01
    assert top[0]["minimum_order_size"] == 5
    assert top[1]["integrity_valid"] and not top[1]["quote_valid"]
    assert "market_resolved" in top[1]["quality_flags_json"]


@pytest.mark.asyncio
async def test_terminal_snapshot_failure_closes_socket_and_releases_store(
    tmp_path, monkeypatch
):
    original = BookRecorder.snapshot

    def fail_at_stop(self, book, causes, **kwargs):
        if causes == ["stop"]:
            self.store.failed = True
            raise OSError("terminal write failed")
        return original(self, book, causes, **kwargs)

    monkeypatch.setattr(BookRecorder, "snapshot", fail_at_stop)
    socket = Socket([poly_book()])
    with pytest.raises((OSError, RuntimeError)):
        await record_feed(
            ["a"],
            venue="polymarket",
            output_root=tmp_path,
            options=RecordingOptions(),
            max_messages=1,
            heartbeat_interval=None,
            connect_factory=lambda _: socket,
        )
    assert socket.closed
    root = next(tmp_path.iterdir())
    assert export_recording(root)["status"] == "failed"


def test_active_run_cannot_be_exported(tmp_path):
    r, _ = start(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="active"):
            export_recording(r.run_dir)
    finally:
        r.finish(reason="stopped")


def test_manifest_validation_checks_export_hash_and_does_not_need_sqlite(tmp_path):
    from pmkt.data.manifests import validate_run_manifest

    r, _ = start(tmp_path)
    observe(r, view())
    r.finish(reason="duration", normal=True)
    (r.run_dir / "recording.sqlite").unlink()
    assert validate_run_manifest(r.run_dir / "manifest.json").ok
    with (r.run_dir / "topbook.parquet").open("ab") as handle:
        handle.write(b"corruption")
    assert not validate_run_manifest(r.run_dir / "manifest.json").ok


@pytest.mark.parametrize(
    "field,value",
    [("options", None), ("options", {"mode": []}), ("counts", None), ("status", [])],
)
def test_malformed_manifest_fields_return_validation_errors(tmp_path, field, value):
    from pmkt.data.manifests import validate_run_manifest

    r, _ = start(tmp_path)
    observe(r, view())
    report = r.finish(reason="duration", normal=True)
    report[field] = value
    path = r.run_dir / "manifest.json"
    path.write_text(json.dumps(report), encoding="utf-8")
    assert not validate_run_manifest(path).ok


def test_failed_export_can_be_retried_without_duplicates(tmp_path, monkeypatch):
    from pmkt.streaming import recording_store

    r, _ = start(tmp_path)
    observe(r, view())
    original = recording_store.pq.ParquetWriter

    def fail(*args, **kwargs):
        raise OSError("export disk failure")

    monkeypatch.setattr(recording_store.pq, "ParquetWriter", fail)
    with pytest.raises(OSError, match="export disk failure"):
        r.finish(reason="duration", normal=True)
    assert not (r.run_dir / "manifest.json").exists()
    monkeypatch.setattr(recording_store.pq, "ParquetWriter", original)
    report = export_recording(r.run_dir)
    assert report["status"] == "complete"
    assert report["counts"]["book_snapshots"] == 1


def test_requested_raw_failure_marks_recording_failed(tmp_path):
    r, _ = start(tmp_path, raw_messages=True)
    observe(r, view())
    r.store.flush(r.report(), force=True)
    r.raw.close()

    class BrokenRaw:
        def write(self, value):
            raise OSError("raw disk full")

        def close(self):
            pass

    r.raw = BrokenRaw()
    with pytest.raises(OSError):
        r.message({"event_type": "book"})
    report = r.finish(reason="raw_error", error=OSError("raw disk full"))
    assert report["status"] == "failed"
    assert report["counts"]["book_snapshots"] == 1


@pytest.mark.asyncio
async def test_kalshi_subscription_gap_invalidates_sibling_books(tmp_path):
    def book(ticker, sequence):
        return {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": sequence,
            "msg": {
                "market_ticker": ticker,
                "yes_dollars_fp": [["0.4", "10"]],
                "no_dollars_fp": [["0.6", "20"]],
            },
        }

    socket = Socket([book("A", 1), book("B", 2), book("A", 4)])
    report = await record_feed(
        ["A", "B"],
        venue="kalshi",
        output_root=tmp_path,
        options=RecordingOptions(),
        max_messages=3,
        auth=ReadAuth(),
        connect_factory=lambda *args: socket,
    )
    assert report["status"] == "partial"
    top = pq.read_table(Path(report["run_dir"]) / "topbook.parquet").to_pylist()
    assert [(r["instrument_id"], r["integrity_valid"]) for r in top[-2:]] == [
        ("A:YES", False),
        ("B:YES", False),
    ]
    assert report["counts"]["book_snapshots"] == 2


@pytest.mark.asyncio
async def test_periodic_check_runs_during_quiet_feed_without_duplicate_books(tmp_path):
    socket = Socket([poly_book()])
    report = await record_feed(
        ["a"],
        venue="polymarket",
        output_root=tmp_path,
        options=RecordingOptions(depth_check_interval_s=0.01),
        duration_s=0.06,
        heartbeat_interval=None,
        connect_factory=lambda _: socket,
    )
    assert report["status"] == "complete"
    assert report["counts"]["book_snapshots"] == 1
    assert report["message_count"] == 1


@pytest.mark.asyncio
async def test_late_complementary_correction_cannot_erase_recovery_bound(
    tmp_path, monkeypatch
):
    from pmkt.exchanges.polymarket.recovery import ComplementaryDeltaRecovery

    monkeypatch.setattr(ComplementaryDeltaRecovery, "max_followup_messages", 1)
    locked = {
        "event_type": "price_change",
        "timestamp": "1000",
        "price_changes": [
            {
                "asset_id": "a",
                "side": "BUY",
                "price": "0.6",
                "size": "10",
                "best_bid": "0.6",
                "best_ask": "0.7",
                "hash": "h",
            }
        ],
    }
    correction = {
        "event_type": "price_change",
        "timestamp": "1000",
        "price_changes": [
            {
                "asset_id": "a",
                "side": "SELL",
                "price": "0.6",
                "size": "0",
                "best_bid": "0.6",
                "best_ask": "0.7",
                "hash": "h",
            }
        ],
    }
    sockets = iter([Socket([poly_book(), locked, correction]), Socket([poly_book()])])
    report = await record_feed(
        ["a"],
        venue="polymarket",
        output_root=tmp_path,
        options=RecordingOptions(),
        max_messages=3,
        heartbeat_interval=None,
        connect_factory=lambda _: next(sockets),
    )
    assert report["reconnect_count"] == 1
    assert report["status"] == "complete"
    assert report["counts"]["book_snapshots"] == 2
