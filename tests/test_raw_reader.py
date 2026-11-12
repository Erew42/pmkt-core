"""Synthetic writer/reader agreement and offline reconstruction examples."""

from __future__ import annotations

import gzip
import hashlib
import json
from dataclasses import FrozenInstanceError, asdict
from decimal import Decimal

import pytest

from pmkt.exchanges.polymarket.ws import apply_market_message
from pmkt.streaming.raw_capture import ChunkWriter
from pmkt.streaming.raw_reader import (
    ChunkGap,
    RawCaptureError,
    RawFrame,
    VenueMessage,
    iter_venue_messages,
    list_session_chunks,
    load_session_metadata,
    read_chunk,
    read_session,
)


@pytest.fixture
def writer(tmp_path):
    return ChunkWriter(tmp_path, "recording", "polymarket", "polymarket_00")


def manifest_path(path):
    return path.with_name(path.name + ".manifest.json")


def edit_manifest(path, **updates):
    sidecar = manifest_path(path)
    manifest = json.loads(sidecar.read_text())
    manifest.update(updates)
    sidecar.write_text(json.dumps(manifest))


def rewrite_frames(path, frames):
    compressed = gzip.compress(
        b"".join((json.dumps(frame) + "\n").encode() for frame in frames), mtime=0
    )
    path.write_bytes(compressed)
    edit_manifest(
        path, sha256=hashlib.sha256(compressed).hexdigest(), size_bytes=len(compressed)
    )


@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
def test_writer_round_trip_preserves_every_envelope_field(tmp_path, monkeypatch, venue):
    from pmkt.streaming import raw_capture

    monkeypatch.setattr(raw_capture, "utc", lambda: "2026-11-04T15:50:19+00:00")
    monkeypatch.setattr(raw_capture.time, "monotonic_ns", lambda: 123456789)
    writer = ChunkWriter(tmp_path, "recording", venue, venue + "_00")
    payload = ' {"escaped": "é\\n", "value": 0} \n'
    writer.write("subscription", "{}", 1, "sent")
    writer.write("text", payload, 1)
    writer.write("transport_error", "disconnect", 1, "local")
    writer.write("text", "null", 2)
    writer.seal()
    discovery = list_session_chunks(tmp_path)
    assert not discovery.incomplete and len(discovery.chunks) == 1
    chunk = discovery.chunks[0]
    frames = list(read_chunk(chunk.path))
    assert [asdict(frame) for frame in frames] == [
        json.loads(line)
        for line in gzip.decompress(chunk.path.read_bytes()).splitlines()
    ]
    assert frames[1] == RawFrame(
        f"{venue}.public_raw_frame.v1",
        venue,
        "recording",
        1,
        2,
        "2026-11-04T15:50:19+00:00",
        123456789,
        "text",
        "received",
        payload,
    )
    with pytest.raises(FrozenInstanceError):
        frames[0].local_frame_seq = 99
    assert list(read_session(tmp_path)) == frames


def test_multi_chunk_session_and_connection_generations(writer, tmp_path):
    writer.write("text", '{"n":1}', 1)
    writer.seal()
    writer.write("ws_close", "", 1, "received_control_base64")
    writer.write("subscription", "{}", 2, "sent")
    writer.seal()
    writer.write("text", '{"n":2}', 2)
    writer.seal()
    frames = list(read_session(tmp_path))
    assert [frame.local_frame_seq for frame in frames] == [1, 2, 3, 4]
    assert [frame.connection_generation for frame in frames] == [1, 1, 2, 2]
    assert [c.path.name for c in list_session_chunks(tmp_path).chunks] == [
        f"chunk_{i:06d}.jsonl.gz" for i in (1, 2, 3)
    ]


def test_incomplete_artifacts_are_reported_and_never_read(writer, tmp_path):
    writer.write("text", "{}", 1)
    writer.seal()
    incomplete = [
        tmp_path / "chunk_000002.jsonl.gz.part",
        tmp_path / "chunk_000003.jsonl.gz",
        tmp_path / "chunk_000004.jsonl.gz.manifest.json",
        tmp_path / "READY.json.tmp",
    ]
    for path in incomplete:
        path.write_bytes(b"not JSON or gzip")
    found = list_session_chunks(tmp_path)
    assert found.incomplete == tuple(sorted(incomplete))
    assert len(found.chunks) == 1
    assert len(list(read_session(tmp_path))) == 1
    for path in incomplete[:2]:
        with pytest.raises(RawCaptureError):
            list(read_chunk(path))


def test_legacy_unknown_fields_and_session_metadata(writer, tmp_path):
    writer.write("text", "{}", 1)
    writer.seal()
    path = list_session_chunks(tmp_path).chunks[0].path
    rows = [asdict(frame) for frame in read_chunk(path)]
    rows[0]["future_envelope_field"] = {"ignored": True}
    rewrite_frames(path, rows)
    edit_manifest(
        path, raw_only=True, reconstruction_pending=True, future_manifest_field=0
    )
    assert len(list(read_session(tmp_path))) == 1
    legacy = {"recording_id": "recording", "raw_only": True, "future_field": []}
    (tmp_path / "PROVENANCE.json").write_text(json.dumps(legacy))
    assert load_session_metadata(tmp_path) == {"PROVENANCE.json": legacy}
    for name in ("READY.json", "FINAL.json"):
        (tmp_path / name).write_text(json.dumps(legacy))
    assert load_session_metadata(tmp_path) == {
        name: legacy for name in ("PROVENANCE.json", "READY.json", "FINAL.json")
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("frame_count", 3),
        ("first_frame_seq", 0),
        ("last_frame_seq", 12),
        ("size_bytes", 0),
        ("sha256", "bad"),
    ],
)
def test_manifest_mismatch_raises_specific_error(writer, tmp_path, field, value):
    writer.write("text", "{}", 1)
    writer.seal()
    path = list_session_chunks(tmp_path).chunks[0].path
    edit_manifest(path, **{field: value})
    with pytest.raises(RawCaptureError, match=field + " mismatch"):
        list(read_chunk(path))
    assert len(list(read_chunk(path, verify=False))) == 1


def test_corrupt_compressed_bytes_fail_before_yield(writer, tmp_path):
    writer.write("text", "{}", 1)
    writer.seal()
    path = list_session_chunks(tmp_path).chunks[0].path
    compressed = bytearray(path.read_bytes())
    compressed[len(compressed) // 2] ^= 0xFF
    path.write_bytes(compressed)
    with pytest.raises(RawCaptureError, match="sha256 mismatch"):
        next(read_chunk(path))


def test_invalid_gzip_has_specific_error_even_with_matching_hash(writer, tmp_path):
    writer.write("text", "{}", 1)
    writer.seal()
    path = list_session_chunks(tmp_path).chunks[0].path
    data = b"not gzip"
    path.write_bytes(data)
    edit_manifest(path, size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
    with pytest.raises(RawCaptureError, match="cannot read compressed envelopes"):
        list(read_chunk(path))


def test_sequence_gap_inside_chunk(writer, tmp_path):
    for _ in range(3):
        writer.write("text", "{}", 1)
    writer.seal()
    path = list_session_chunks(tmp_path).chunks[0].path
    rows = [asdict(frame) for frame in read_chunk(path)]
    rows[1]["local_frame_seq"] = 3
    rewrite_frames(path, rows)
    with pytest.raises(RawCaptureError, match="local_frame_seq is not consecutive"):
        list(read_chunk(path))


@pytest.mark.parametrize("break_kind", ["hash", "sequence", "identity"])
def test_adjacent_chunks_must_link(writer, tmp_path, break_kind):
    writer.write("text", "{}", 1)
    writer.seal()
    if break_kind == "sequence":
        writer.seq += 1
    writer.write("text", "{}", 1)
    writer.seal()
    path = list_session_chunks(tmp_path).chunks[1].path
    if break_kind == "hash":
        edit_manifest(path, previous_chunk_sha256="bad")
        match = "previous_chunk_sha256 mismatch"
    elif break_kind == "identity":
        edit_manifest(path, recording_id="different")
        match = "session recording_id mismatch"
    else:
        match = "local_frame_seq discontinuity"
    with pytest.raises(RawCaptureError, match=match):
        list(read_session(tmp_path))


@pytest.mark.parametrize("missing", [1, 2])
@pytest.mark.parametrize("verify", [True, False])
def test_archived_prefix_or_middle_yields_gap_then_resumes(
    writer, tmp_path, missing, verify
):
    for _ in range(3):
        writer.write("text", "{}", 1)
        writer.seal()
    path = tmp_path / f"chunk_{missing:06d}.jsonl.gz"
    path.unlink()
    manifest_path(path).unlink()
    items = list(read_session(tmp_path, verify=verify))
    # The gap sits exactly where the chunk was, before the next chunk's frames.
    assert [
        "gap" if isinstance(item, ChunkGap) else item.local_frame_seq for item in items
    ] == (["gap", 2, 3] if missing == 1 else [1, "gap", 3])
    [gap] = [item for item in items if isinstance(item, ChunkGap)]
    assert (gap.first_missing, gap.last_missing) == (missing, missing)
    assert gap.next_chunk == tmp_path / f"chunk_{missing + 1:06d}.jsonl.gz"
    assert gap.previous_last_frame_seq == (None if missing == 1 else 1)
    assert gap.next_first_frame_seq == missing + 1
    assert gap.missing_frame_count == 1
    # Passed through unchanged by the message decoder.
    assert gap in list(iter_venue_messages(items))


def test_message_decoding_preserves_exact_text_and_diagnostics(writer, tmp_path):
    single = ' \n {"event_type": "book", "x":"\\u00e9"} \t'
    elements = ['{ "x": "\\u00e9" }', "[1,  2]", "null", '"hi"', "false", "12"]
    batch = "[ \n" + ", \t".join(elements) + " ]"
    payloads = [single, batch, "not JSON", "null", "[]"]
    for payload in payloads:
        writer.write("text", payload, 1)
    diagnostics = [
        ("app_ping", "received"),
        ("app_pong", "received"),
        ("subscription", "sent"),
        ("text", "sent"),
        ("transport_error", "local"),
        ("seq_gap", "local"),
        ("disk_pause_gap", "local"),
        ("ws_ping", "received_control_base64"),
        ("ws_close", "received_control_base64"),
        ("binary_base64", "received"),
    ]
    for kind, direction in diagnostics:
        writer.write(kind, "{}", 2, direction)
    writer.seal()
    frames = list(read_session(tmp_path))
    decoded = list(iter_venue_messages(frames))
    messages = [item for item in decoded if isinstance(item, VenueMessage)]
    assert messages[0].text == single and messages[0].frame is frames[0]
    assert messages[0].value["x"] == "é"
    assert [m.text for m in messages[1:7]] == elements
    assert [m.batch_index for m in messages[1:7]] == list(range(6))
    assert all(m.frame is frames[1] for m in messages[1:7])
    assert messages[7].text == "not JSON" and not messages[7].parsed
    assert messages[8].value is None and messages[8].parsed
    assert messages[8].batch_index is None
    assert decoded[9:] == frames[5:]
    assert all(a is b for a, b in zip(decoded[9:], frames[5:]))


def test_rebuild_books_and_reset_on_new_connection_generation(writer, tmp_path):
    book = {
        "event_type": "book",
        "asset_id": "A",
        "market": "m",
        "bids": [{"price": "0.4", "size": "2"}],
        "asks": [{"price": "0.6", "size": "3"}],
    }
    change = {
        "event_type": "price_change",
        "price_changes": [
            {"asset_id": "A", "side": "BUY", "price": "0.4", "size": "7"}
        ],
    }
    writer.write("text", json.dumps([book, change]), 1)
    writer.seal()
    writer.write("subscription", "{}", 2, "sent")
    # A different asset's baseline must not leave generation 1's A book alive.
    writer.write("text", json.dumps(dict(book, asset_id="B")), 2)
    writer.seal()
    states, generation, observed = {}, None, []
    for item in iter_venue_messages(read_session(tmp_path)):
        frame = item.frame if isinstance(item, VenueMessage) else item
        if frame.connection_generation != generation:
            states.clear()
            generation = frame.connection_generation
        if isinstance(item, VenueMessage) and isinstance(item.value, dict):
            apply_market_message(states, item.value)
            observed.append(
                {asset: dict(state.bids) for asset, state in states.items()}
            )
    assert observed == [
        {"A": {Decimal("0.4"): Decimal("2")}},
        {"A": {Decimal("0.4"): Decimal("7")}},
        {"B": {Decimal("0.4"): Decimal("2")}},
    ]


@pytest.mark.parametrize("content", ['{"schema":', "[]", '{"schema": "unknown"}'])
def test_malformed_manifest_has_specific_error(writer, tmp_path, content):
    writer.write("text", "{}", 1)
    writer.seal()
    path = list_session_chunks(tmp_path).chunks[0].path
    manifest_path(path).write_text(content)
    with pytest.raises(RawCaptureError):
        list_session_chunks(tmp_path)


def test_empty_session_and_missing_provenance(tmp_path):
    assert list_session_chunks(tmp_path).chunks == ()
    assert list(read_session(tmp_path)) == []
    with pytest.raises(RawCaptureError, match="PROVENANCE.json"):
        load_session_metadata(tmp_path)


@pytest.mark.parametrize(
    ("key", "value", "match"),
    [
        ("schema", "polymarket.public_raw_chunk.v1", "schema mismatch"),
        ("venue", "kalshi", "venue mismatch"),
        ("recording_id", "different", "recording_id mismatch"),
        ("local_frame_seq", True, "local_frame_seq must be int"),
        ("payload_raw", None, "payload_raw must be str"),
    ],
)
def test_bad_envelope_has_specific_error(writer, tmp_path, key, value, match):
    writer.write("text", "{}", 1)
    writer.seal()
    path = list_session_chunks(tmp_path).chunks[0].path
    rows = [asdict(frame) for frame in read_chunk(path)]
    rows[0][key] = value
    rewrite_frames(path, rows)
    with pytest.raises(RawCaptureError, match=match):
        list(read_chunk(path))


@pytest.mark.parametrize("payload", ["NaN", "Infinity", "[NaN]", '{"x":-Infinity}'])
def test_non_json_constants_are_unparsed(writer, tmp_path, payload):
    writer.write("text", payload, 1)
    writer.seal()
    [message] = iter_venue_messages(read_session(tmp_path))
    assert isinstance(message, VenueMessage)
    assert not message.parsed and message.text == payload


def test_gap_counts_every_missing_chunk_and_frame(writer, tmp_path):
    writer.index = 3
    writer.seq = 5
    writer.previous = "0" * 64  # As if chunks 1-3 were sealed and archived.
    writer.write("text", "{}", 1)
    writer.seal()
    gap = next(read_session(tmp_path))
    assert isinstance(gap, ChunkGap)
    assert (gap.first_missing, gap.last_missing) == (1, 3)
    assert gap.previous_last_frame_seq is None
    assert gap.missing_frame_count == 5


def test_reconstruction_resets_books_at_a_gap(writer, tmp_path):
    book = {
        "event_type": "book",
        "asset_id": "A",
        "market": "m",
        "bids": [{"price": "0.4", "size": "2"}],
        "asks": [],
    }
    writer.write("text", json.dumps(book), 1)
    writer.seal()
    writer.write("text", json.dumps(dict(book, asset_id="B")), 1)
    writer.seal()
    writer.write("text", json.dumps(dict(book, asset_id="C")), 1)
    writer.seal()
    (tmp_path / "chunk_000002.jsonl.gz").unlink()
    manifest_path(tmp_path / "chunk_000002.jsonl.gz").unlink()
    states = {}
    for item in iter_venue_messages(read_session(tmp_path)):
        if isinstance(item, ChunkGap):
            states.clear()  # Same generation, but the books are stale.
        elif isinstance(item, VenueMessage):
            apply_market_message(states, item.value)
    assert set(states) == {"C"}


# Deep enough to exhaust the recursive JSON decoder on every supported Python.
DEEP = "[" * 100_000 + "]" * 100_000


def test_deeply_nested_text_is_unparsed_and_replay_continues(writer, tmp_path):
    writer.write("text", DEEP[:-1] + "x", 1)
    writer.write("text", '{"next": 1}', 1)
    writer.seal()
    first, second = iter_venue_messages(read_session(tmp_path))
    assert not first.parsed and first.value is None
    assert second.parsed and second.value == {"next": 1}


def test_deeply_nested_artifact_json_has_specific_error(writer, tmp_path):
    (tmp_path / "PROVENANCE.json").write_text(DEEP)
    with pytest.raises(RawCaptureError, match="PROVENANCE.json"):
        load_session_metadata(tmp_path)
    writer.write("text", "{}", 1)
    writer.seal()
    path = list_session_chunks(tmp_path).chunks[0].path
    [row] = [asdict(frame) for frame in read_chunk(path)]
    line = json.dumps(row)[:-1] + ',"extra":' + DEEP + "}\n"
    compressed = gzip.compress(line.encode(), mtime=0)
    path.write_bytes(compressed)
    edit_manifest(
        path, sha256=hashlib.sha256(compressed).hexdigest(), size_bytes=len(compressed)
    )
    with pytest.raises(RawCaptureError, match="line 1"):
        list(read_chunk(path))


def test_chunk_after_gap_must_name_a_predecessor(writer, tmp_path):
    writer.index = 1  # Chunk 1 missing, yet chunk 2 claims to be first.
    writer.write("text", "{}", 1)
    writer.seal()
    with pytest.raises(RawCaptureError, match="null after chunk 1"):
        list(read_session(tmp_path))


def test_sequence_must_cover_missing_chunks(writer, tmp_path):
    writer.write("text", "{}", 1)
    writer.seal()
    writer.index += 1  # Skip a chunk number without any frames in between.
    writer.write("text", "{}", 1)
    writer.seal()
    with pytest.raises(RawCaptureError, match="too low for missing chunks"):
        list(read_session(tmp_path))
