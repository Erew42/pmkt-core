from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path
import random
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from benchmark_book_grid import CausalGrid, benchmark  # noqa: E402


def test_grid_matches_independent_backward_asof_oracle():
    rng = random.Random(51)
    observations = sorted((rng.randrange(10000), rng.choice("ABC"), i) for i in range(8000))
    grid = CausalGrid(0, 250)
    actual = []
    for ns, key, value in observations:
        actual.extend(grid.advance(ns))
        grid.observe(key, ns, value)
    actual.extend(grid.advance(10000, inclusive=True))
    expected = []
    previous = -1
    for point in range(0, 10001, 250):
        for key in "ABC":
            candidates = [(ns, value) for ns, instrument, value in observations
                          if instrument == key and previous < ns <= point]
            if candidates:
                ns, value = candidates[-1]
                expected.append((key, point, ns, value))
        previous = point
    assert actual == expected


def test_grid_ties_silent_intervals_tail_and_sealed_boundaries():
    grid = CausalGrid(1000, 250)
    assert grid.advance(1100) == []
    grid.observe("A", 1100, "old")
    assert grid.advance(1250) == []
    grid.observe("A", 1250, "first")
    grid.observe("A", 1250, "last")
    assert grid.advance(1999) == [("A", 1250, 1250, "last")]
    assert grid.advance(5000) == []  # no periodic restatements for silent books
    grid.observe("A", 5000, "exact")
    assert grid.advance(5000, inclusive=True) == [("A", 5000, 5000, "exact")]
    with pytest.raises(ValueError):
        grid.observe("A", 5000, "too late")
    grid.advance(5100)
    grid.observe("A", 5100, "unfinished")
    assert grid.advance(5200, inclusive=True) == []
    assert len(grid.pending) == 1  # do not invent a future point on shutdown
    with pytest.raises(ValueError):
        grid.advance(1000)
    with pytest.raises(ValueError):
        CausalGrid(0, 0)


@pytest.mark.parametrize("venue", ["pm", "kx"])
def test_grid_storage_causal_depth_and_tape_parity(tmp_path, venue):
    selection = tmp_path / "selection.json"
    selection.write_text(json.dumps({"polymarket_token_ids": ["A"], "kalshi_tickers": ["A"]}))
    if venue == "pm":
        def message(size, seq):
            return {"event_type": "book", "asset_id": "A", "market": "m",
                    "timestamp": str(1704067200000 + seq),
                    "bids": [{"price": "0.4", "size": str(size)}],
                    "asks": [{"price": "0.6", "size": "10"}]}
    else:
        def message(size, seq):
            return {"type": "orderbook_snapshot", "sid": 1, "seq": seq,
                    "msg": {"market_ticker": "A", "yes_dollars_fp": [["0.4", str(size)]],
                            "no_dollars_fp": [["0.4", "10"]]}}
    source = tmp_path / "source.jsonl"
    source.write_text("\n".join(json.dumps({
        "observed_at_utc": f"2024-01-01T00:00:00.{micros:06d}Z", "message": message(size, seq),
    }) for seq, (micros, size) in enumerate([(100000, 1), (200000, 2), (300000, 3), (900000, 9)], 1)))
    summaries = {}
    for name, interval, retention in [("dense", 0, "all"), ("grid", .25, "all"), ("states", .25, "states"), ("archive", .25, "raw-archive")]:
        summaries[name] = benchmark(argparse.Namespace(
            input=source, selection=selection, venue=venue, output_dir=tmp_path / name,
            interval=interval, retention=retention, seconds=.75,
            origin_utc="2024-01-01T00:00:00Z", segment_rows=100000,
        ))
    for role in ("tape_event", "tape_level", "tape_control", "parsed_event", "legacy_snapshot", "legacy_level"):
        assert summaries["dense"]["logical_sha256"][role] == summaries["grid"]["logical_sha256"][role]
    assert summaries["grid"]["causal_selection_oracle"] == "passed"
    assert summaries["grid"]["counts"]["persisted_states"] == 2
    for name in ("grid", "states"):
        frame = pd.read_parquet(tmp_path / name / "replay" / "depth_main")
        side = "bid" if venue == "pm" else "yes"
        # A post-boundary mutation must never be copied into the prior grid book.
        rows = frame[frame.side == side].sort_values("local_sequence")
        assert rows.size_contracts.astype(float).tolist() == [2., 3.]
        assert rows.local_sequence.tolist() == [2, 3]
        assert summaries[name]["strict_commit_validation"] == "passed"
    assert summaries["states"]["raw_bytes"] == 0
    assert set(summaries["states"]["row_counts"]) == {"topbook_main", "depth_main"}
    with gzip.open(tmp_path / "archive" / "raw_events.jsonl.gz", "rt") as handle:
        assert handle.read() == (tmp_path / "dense" / "replay" / "raw_events.jsonl").read_text()
    assert summaries["archive"]["raw_logical_sha256"] == summaries["dense"]["raw_logical_sha256"]


@pytest.mark.parametrize("venue", ["pm", "kx"])
@pytest.mark.parametrize("case", ["transient_failure", "empty_frame"])
def test_grid_keeps_evidence_of_unsampled_failures_and_empty_frames(tmp_path, venue, case):
    selection = tmp_path / "selection.json"
    selection.write_text(json.dumps({"polymarket_token_ids": ["A"], "kalshi_tickers": ["A"]}))
    records = []
    states = [(10000, "normal"), (100000, "bad"), (200000, "normal"), (300000, "empty")]
    for seq, (micros, kind) in enumerate(states, 1):
        if case == "empty_frame" and kind == "bad":
            kind = "normal"
        if venue == "pm":
            msg = {"event_type": "book", "asset_id": "A", "market": "m", "timestamp": str(1704067200000 + seq),
                   "bids": [] if kind == "empty" else [{"price": ".8" if kind == "bad" else ".4", "size": "10"}],
                   "asks": [] if kind == "empty" else [{"price": ".6", "size": "10"}]}
        else:
            msg = {"type": "orderbook_snapshot", "sid": 1, "seq": seq,
                   "msg": {"market_ticker": "A", "yes_dollars_fp": [] if kind == "empty" else [[".8" if kind == "bad" else ".4", "10"]],
                           "no_dollars_fp": [] if kind == "empty" else [[".4", "10"]]}}
        records.append({"observed_at_utc": f"2024-01-01T00:00:00.{micros:06d}Z", "message": msg})
    # A final non-book message closes the time window without dirtying any book.
    records.append({"observed_at_utc": "2024-01-01T00:00:01Z", "message": {"type": "heartbeat"}})
    source = tmp_path / "source.jsonl"
    source.write_text("\n".join(json.dumps(record) for record in records))
    out = tmp_path / "result"
    summary = benchmark(argparse.Namespace(
        input=source, selection=selection, venue=venue, output_dir=out, interval=.25,
        retention="tape", seconds=.75, origin_utc="2024-01-01T00:00:00Z", segment_rows=100000,
    ))
    index = [json.loads(line) for line in (out / "sampling-index.jsonl").read_text().splitlines()]
    assert [row["grid_relative_ns"] for row in index] == [250000000, 500000000]
    assert [row["local_sequence"] for row in index] == [3, 4]
    assert summary["initialized_instruments"] == 1
    assert summary["counts"]["observed_invalid_states"] == int(case == "transient_failure")
    assert summary["counts"]["persisted_invalid_states"] == 0
    depth = pd.read_parquet(out / "replay" / "depth_main")
    assert set(depth.local_sequence) == {3}  # empty state has an index entry and zero levels
    tape = pd.read_parquet(out / "replay" / "tape_event")
    assert len(tape) == 4  # sampling cannot discard the intervening failure evidence
