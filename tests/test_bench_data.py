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
        assert all(c.units_per_call == 3 for c in cases)
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


def test_manifest_errors_do_not_fall_back_to_fixtures(tmp_path):
    with pytest.raises(ValueError, match="no raw captures"):
        with open_dataset(tmp_path):
            pass
    manifest = tmp_path / "bench-manifest.json"
    manifest.write_text(json.dumps({"captures": [".."]}), encoding="utf-8")
    with pytest.raises(ValueError, match="invalid benchmark input path"):
        with open_dataset(tmp_path):
            pass
