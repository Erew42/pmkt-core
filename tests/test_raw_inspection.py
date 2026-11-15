from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from pmkt.cli.app import app
from pmkt.streaming.raw_capture import ChunkWriter
from pmkt.streaming.raw_reader import RawCaptureError, inspect_session


def test_inspection_verifies_counts_without_decoding_payloads(tmp_path):
    writer = ChunkWriter(tmp_path, "capture", "polymarket", "polymarket_00")
    writer.write("subscription", "{}", 1, "sent")
    writer.write("text", "not JSON", 1)
    writer.seal()
    writer.write("text", "[]", 2)
    writer.seal()
    # Collector state is an observation and must not substitute for evidence.
    (tmp_path / "FINAL.json").write_text('{"frame_count": 9999, "partial": false}')

    result = CliRunner().invoke(app, ["inspect-raw-capture", str(tmp_path)])

    assert result.exit_code == 0, result.output
    report = json.loads(result.output)
    assert report["verified_present_chunks"] is True
    assert report["frame_count"] == 3
    assert report["sealed_chunk_count"] == 2
    assert report["connection_generations"] == [1, 2]
    assert report["frame_type_counts"] == {"subscription": 1, "text": 2}
    assert report["direction_counts"] == {"received": 2, "sent": 1}
    assert report["compressed_bytes"] == sum(p.stat().st_size for p in tmp_path.glob("*.gz"))


def test_inspection_reports_missing_prefix_and_middle_and_unsealed_files(tmp_path):
    writer = ChunkWriter(tmp_path, "capture", "kalshi", "kalshi_00")
    for generation in range(1, 5):
        writer.write("text", "{}", generation)
        writer.seal()
    for index in (1, 3):
        chunk = tmp_path / f"chunk_{index:06d}.jsonl.gz"
        chunk.unlink()
        chunk.with_name(chunk.name + ".manifest.json").unlink()
    (tmp_path / "chunk_000005.jsonl.gz.part").write_bytes(b"unfinished")

    result = CliRunner().invoke(app, ["inspect-raw-capture", str(tmp_path)])

    assert result.exit_code == 2
    report = json.loads(result.output)
    assert report["frame_count"] == 2
    assert report["missing_chunk_count"] == 2
    assert report["missing_recorded_frame_count"] == 2
    assert [g["first_missing"] for g in report["chunk_gaps"]] == [1, 3]
    assert report["incomplete_files"] == ["chunk_000005.jsonl.gz.part"]


@pytest.mark.parametrize("event", ["seq_gap", "disk_pause_gap"])
def test_recorded_loss_events_return_verified_with_gaps(tmp_path, event):
    writer = ChunkWriter(tmp_path, "capture", "kalshi", "kalshi_00")
    writer.write(event, "{}", 1, "local")
    writer.seal()
    result = CliRunner().invoke(app, ["inspect-raw-capture", str(tmp_path)])
    assert result.exit_code == 2
    assert json.loads(result.output)["local_event_counts"] == {event: 1}


def test_inspection_exhausts_reader_to_check_manifest_count(tmp_path):
    writer = ChunkWriter(tmp_path, "capture", "polymarket", "polymarket_00")
    writer.write("text", "{}", 1)
    writer.seal()
    manifest = next(tmp_path.glob("*.manifest.json"))
    payload = json.loads(manifest.read_text())
    payload["frame_count"] = 2
    manifest.write_text(json.dumps(payload))

    with pytest.raises(RawCaptureError, match="frame_count mismatch"):
        inspect_session(tmp_path)
    result = CliRunner().invoke(app, ["inspect-raw-capture", str(tmp_path)])
    assert result.exit_code == 1
    assert "frame_count mismatch" in result.output
    assert "verified_present_chunks" not in result.output


def test_empty_and_missing_sessions_are_not_successful(tmp_path):
    empty = CliRunner().invoke(app, ["inspect-raw-capture", str(tmp_path)])
    assert empty.exit_code == 1
    assert json.loads(empty.output)["sealed_chunk_count"] == 0
    missing = CliRunner().invoke(app, ["inspect-raw-capture", str(tmp_path / "absent")])
    assert missing.exit_code == 1
    assert "cannot list session" in missing.output


def test_archived_prefix_alone_is_verified_with_gaps(tmp_path):
    writer = ChunkWriter(tmp_path, "capture", "kalshi", "kalshi_00")
    for generation in (1, 2):
        writer.write("text", "{}", generation)
        writer.seal()
    chunk = tmp_path / "chunk_000001.jsonl.gz"
    chunk.unlink()
    chunk.with_name(chunk.name + ".manifest.json").unlink()

    result = CliRunner().invoke(app, ["inspect-raw-capture", str(tmp_path)])

    assert result.exit_code == 2
    report = json.loads(result.output)
    assert report["verified_present_chunks"] is True
    assert [g["first_missing"] for g in report["chunk_gaps"]] == [1]
