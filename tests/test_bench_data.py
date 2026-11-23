"""Check benchmark inputs, results, and boundaries without a network transport."""

import hashlib
import json
from pathlib import Path
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pmkt.bench.datasets import open_dataset
from pmkt.bench.workloads import workloads

FIXTURES = Path(__file__).parent / "fixtures/raw_capture"


def test_sample_capture_provenance_and_isolated_staging(tmp_path):
    source = tmp_path / "sample.jsonl.gz"
    shutil.copyfile(FIXTURES / "polymarket_00.jsonl.gz", source)
    with open_dataset(tmp_path) as dataset:
        (capture,) = dataset.captures
        assert capture.frames == sum(1 for _ in capture.envelopes()) == 205
        assert not capture.session.is_relative_to(tmp_path)
        (details,) = dataset.provenance["files"]
        assert details == {
            "path": source.name,
            "size_bytes": source.stat().st_size,
            "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        }
    assert not capture.session.exists()
    assert list(tmp_path.iterdir()) == [source]
    with open_dataset(tmp_path) as again:
        assert dataset.provenance == again.provenance


def test_manifest_paired_sources_parquet_and_selected_measurements(tmp_path):
    files = ["a.jsonl.gz", "b.jsonl.gz"]
    for name in files:
        shutil.copyfile(FIXTURES / "polymarket_00.jsonl.gz", tmp_path / name)
    pq.write_table(pa.table({'q"uote': [3, 5, 8]}), tmp_path / "book.parquet")
    manifest = {"captures": files, "merge_groups": [files], "parquet": ["book.parquet"]}
    (tmp_path / "bench-manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    (tmp_path / "ignored.jsonl.gz").write_bytes(b"unselected")
    with workloads(
        tmp_path,
        42,
        lambda n: n.endswith("parquet.1"),
    ) as (meta, cases):
        assert meta["merge_groups"] == [files]
        arrow, pandas, scan = (c.run() for c in cases)
        assert (
            arrow.to_pylist()
            == pandas.to_dict("records")
            == [{'q"uote': i} for i in (3, 5, 8)]
        )
        assert isinstance(scan[0], int)
        # The zero-copy C-stream import is one op; pandas and DuckDB touch each row.
        assert [(c.units_per_call, c.unit) for c in cases] == [
            (1, "ops"),
            (3, "rows"),
            (3, "rows"),
        ]
    with workloads(tmp_path, 42, lambda n: n.startswith("native.merge_copies")) as (
        _,
        cases,
    ):
        (case,) = cases
        result = case.run()
        assert result["topbook_rows"] > 0 and case.units_per_call == 410
        assert json.loads(result["summary"])["stats"]["repeats_merged"] > 0


def test_fixture_shard_and_catalog_produce_results():
    with workloads(
        None, 42, lambda n: n.startswith(("native.shard_feed", "research.catalog"))
    ) as (_, cases):
        *shards, catalog = cases
        assert [s.name.rsplit(".", 1)[-1] for s in shards] == ["1", "256", "512"]
        assert all(len(s.run()) == 1 for s in shards)
        assert catalog.run().to_arrow().to_pylist() == [
            {"venue": "kalshi", "markets": 128},
            {"venue": "polymarket", "markets": 128},
        ]


def test_fixture_kalshi_deltas_update_seeded_nonempty_books(monkeypatch):
    from pmkt import _native
    from pmkt.bench import workloads as module
    from pmkt.exchanges.kalshi.ws import KalshiOrderBookState

    seeded = []
    original = module._seed_kalshi_books

    def record(events):
        seeded.extend(original(events))
        return seeded

    monkeypatch.setattr(module, "_seed_kalshi_books", record)
    with workloads(None, 42, lambda n: n == "native.update_kalshi_book.kalshi") as (
        _,
        (case,),
    ):
        case.run()
    # The fixture holds deltas only: each ticker gets one synthetic snapshot first.
    kinds = [kind for _, kind, *_ in seeded]
    snapshots = kinds.count("orderbook_snapshot")
    assert kinds.count("orderbook_delta") == 189 and snapshots == len(
        {ticker for ticker, *_ in seeded}
    )
    assert kinds[:snapshots] == ["orderbook_snapshot"] * snapshots
    states: dict[str, KalshiOrderBookState] = {}
    for ticker, kind, body, sid, seq in seeded:
        state = states.setdefault(ticker, KalshiOrderBookState(ticker))
        top = _native.update_kalshi_book(state, kind, body, sid, seq)
        assert top != (None, None)
    real = [("K", "orderbook_snapshot", {"market_ticker": "K"}, 1, 1)]
    assert original(real) == real


def test_manifest_errors_do_not_fall_back_to_fixtures(tmp_path):
    with pytest.raises(ValueError, match="no raw captures"):
        with open_dataset(tmp_path):
            pass
    manifest = tmp_path / "bench-manifest.json"
    manifest.write_text(json.dumps({"captures": [".."]}), encoding="utf-8")
    with pytest.raises(ValueError, match="invalid benchmark input path"):
        with open_dataset(tmp_path):
            pass


def test_sealed_provenance_hash_covers_added_fields():
    from pmkt.bench.datasets import seal_provenance

    with open_dataset() as dataset:
        base = dataset.provenance
    assert seal_provenance(base) == base
    extended = seal_provenance({**base, "consumer_notes": [1]})
    assert extended["consumer_notes"] == [1] and extended["sha256"] != base["sha256"]
    assert seal_provenance(extended) == extended


def test_wire_replay_reads_a_recorded_polymarket_book():
    from pmkt.bench import WireReplay
    from pmkt.bench.datasets import open_dataset as open_inputs

    with open_inputs() as dataset:
        capture = next(c for c in dataset.captures if c.venue == "polymarket")
        frames, assets = [], {}
        for row in capture.envelopes():
            if row["direction"] == "received" and row["frame_type"] == "text":
                frames.append(
                    (row["received_monotonic_ns"], row["payload_raw"].encode())
                )
                try:
                    data = json.loads(row["payload_raw"])
                except json.JSONDecodeError:
                    continue  # a recorded PONG
                for event in data if isinstance(data, list) else [data]:
                    if event.get("event_type") == "book" and event.get("bids"):
                        assets.setdefault(str(event["asset_id"]), None)
    replay = WireReplay("polymarket", list(assets), frames)
    bids, asks = replay.book(next(iter(assets)))
    assert bids and all(price > 0 and size > 0 for price, size in bids)
    with pytest.raises(ValueError, match="unknown instrument"):
        replay.book("not-subscribed")
    with pytest.raises(ValueError):
        WireReplay("polymarket", [], [])
