from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from pmkt import _native


SNAPSHOT = (
    b'{"event_type":"book","asset_id":"X","market":"M","timestamp":"100",'
    b'"bids":[["0.40000000000000000001","1"]],"asks":[["0.5","2"]]}'
)


def test_raw_frames_detached_snapshots_and_recovery_barrier() -> None:
    shard = _native.ShardFeed("polymarket", "s", ["X"], snapshot_ns=5, liveness_ns=20)
    shard.connected(1, 0)
    assert shard.begin_requests(1, 5) == ["X"]
    assert shard.ingest("s", 1, SNAPSHOT, 6)[0]["book"] is None
    shard.finish_requests(1, ["X"], 7)
    original = shard.observation("X", 8)
    assert original["book"]["bids"] == [("0.40000000000000000001", "1")]
    assert original["book"]["received_monotonic_ns"] == 6
    assert original["health"]["book_update_age_ns"] == 2

    tick = b'{"event_type":"tick_size_change","asset_id":"X","new_tick_size":"0.001"}'
    revised = shard.ingest("s", 1, tick, 9)[0]
    assert revised["book"]["tick_size"] == "0.001"
    assert revised["health"]["metadata_revision"] == 1
    assert original["book"]["metadata_revision"] == 0
    revised["book"]["bids"].clear()
    assert shard.observation("X", 10)["book"]["bids"] == original["book"]["bids"]
    with pytest.raises(TypeError):
        shard.ingest("s", 1, SNAPSHOT.decode(), 10)

    shard.disconnected(1, "transport_closed")
    assert shard.health("X", 11)["reason"] == "transport_closed"
    shard.connected(2, 12)
    with pytest.raises(ValueError, match="generation"):
        shard.ingest("s", 1, SNAPSHOT, 13)
    shard.heartbeat(2, 14)
    assert shard.observation("X", 35)["health"]["reason"] == "liveness_expired"
    shard.close()
    assert shard.health("X", 36)["connection_state"] == "closed"


@pytest.mark.parametrize("version", [1, 2])
def test_verified_capture_to_one_shot_arrow_streams(tmp_path: Path, version: int) -> None:
    import pyarrow as pa

    source = Path(__file__).parent / "fixtures/raw_capture/polymarket_00.jsonl.gz"
    data = source.read_bytes()
    chunk = tmp_path / "chunk_000001.jsonl.gz"
    chunk.write_bytes(data)
    manifest = {
        "schema": "polymarket.public_raw_chunk.v1", "venue": "polymarket",
        "recording_id": "a51260f6-b1e2-4c79-bb41-82926544d227", "filename": chunk.name,
        "size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
        "frame_count": 205, "first_frame_seq": 1, "last_frame_seq": 205,
        "previous_chunk_sha256": None,
    }
    sidecar = chunk.with_suffix(".gz.manifest.json")
    sidecar.write_text(json.dumps(manifest), encoding="utf-8")
    info = _native.inspect_raw_capture(tmp_path)
    assert (info["sealed_chunk_count"], info["frame_count"], info["compressed_bytes"]) == (1, 205, len(data))
    assert info["recording_id"] == manifest["recording_id"]
    assert info["chunk_gaps"] == []

    result = _native.reconstruct_raw_capture(tmp_path, version=version, target_rows=10)
    stream = result["topbook"]
    with pytest.raises(NotImplementedError, match="requested_schema"):
        stream.__arrow_c_stream__(requested_schema=object())
    if hasattr(pa.RecordBatchReader, "from_stream"):
        with pytest.raises(NotImplementedError, match="requested_schema"):
            pa.RecordBatchReader.from_stream(stream, schema=pa.schema([("other", pa.int64())]))
    top = pa.table(stream)
    assert top.num_rows == result["topbook_rows"] > 0
    assert set(top["schema_version"].to_pylist()) == {f"topbook.v{version}"}
    assert ("book_integrity_valid" in top.column_names) == (version == 2)
    with pytest.raises(ValueError, match="consumed"):
        pa.table(result["topbook"])

    capsule = result["depth"].__arrow_c_stream__()
    reader = pa.RecordBatchReader._import_from_c_capsule(capsule)
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        pa.RecordBatchReader._import_from_c_capsule(capsule)
    del capsule, result["depth"]
    depth = reader.read_all()
    assert depth.num_rows == result["depth_rows"] > 0
    assert set(depth["schema_version"].to_pylist()) == {f"depth.v{version}"}

    manifest["frame_count"] += 1
    sidecar.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError):
        _native.reconstruct_raw_capture(tmp_path, version=version, target_rows=10)


def test_empty_capture_keeps_schemas(tmp_path: Path) -> None:
    import pyarrow as pa

    result = _native.reconstruct_raw_capture(tmp_path)
    for name in ("topbook", "depth"):
        table = pa.table(result[name])
        assert table.num_rows == 0
        assert "schema_version" in table.column_names
