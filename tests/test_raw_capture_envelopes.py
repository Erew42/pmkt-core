"""Direct envelope formatting must produce the original json.dumps bytes."""

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pmkt.streaming import raw_capture as rr


def test_compact_fixtures_keep_all_source_frame_and_event_kinds():
    directory = Path(__file__).parent / "fixtures" / "raw_capture"
    coverage = json.loads((directory / "coverage.json").read_text())
    assert sum(path.stat().st_size for path in directory.iterdir()) <= 200_000
    for venue, expected in coverage.items():
        with gzip.open(directory / f"{venue}_00.jsonl.gz", "rt") as source:
            rows = [json.loads(line) for line in source]
        kinds = set()
        for row in rows:
            kinds.add(f"frame:{row['frame_type']}:{row['direction']}")
            if row["frame_type"] == "text":
                payload = json.loads(row["payload_raw"])
                for event in payload if isinstance(payload, list) else [payload]:
                    kinds.add(
                        "event:" + str(event.get("event_type", event.get("type")))
                    )
        assert sorted(kinds) == expected["kinds"]
        assert len(rows) == expected["frames"] < expected["source_frames"]
        assert (
            sum(row["frame_type"] == "text" for row in rows) == expected["text_frames"]
        )


def original_line(
    writer,
    frame_type,
    payload_raw,
    generation,
    received_utc,
    received_monotonic_ns,
    direction="received",
):
    envelope = dict(
        schema=f"{writer.venue}.public_raw_frame.v1",
        venue=writer.venue,
        recording_id=writer.recording_id,
        connection_generation=generation,
        local_frame_seq=writer.seq + 1,
        received_utc=received_utc,
        received_monotonic_ns=received_monotonic_ns,
        frame_type=frame_type,
        direction=direction,
        payload_raw=payload_raw,
    )
    return (
        json.dumps(envelope, ensure_ascii=True, separators=(",", ":")) + "\n"
    ).encode()


@pytest.mark.parametrize(("venue", "count"), [("polymarket", 205), ("kalshi", 203)])
def test_every_recorded_envelope_has_identical_bytes(
    tmp_path, monkeypatch, venue, count
):
    with gzip.open(
        Path(__file__).parent / "fixtures" / "raw_capture" / f"{venue}_00.jsonl.gz",
        "rt",
    ) as source:
        rows = [json.loads(line) for line in source]
    assert len(rows) == count
    current = rows[0]
    monkeypatch.setattr(rr, "utc", lambda: current["received_utc"])
    monkeypatch.setattr(
        rr.time, "monotonic_ns", lambda: current["received_monotonic_ns"]
    )
    writer = rr.ChunkWriter(tmp_path, current["recording_id"], venue, f"{venue}_00")
    writer.seq = current["local_frame_seq"] - 1
    expected = []
    for current in rows:
        expected.append(
            original_line(
                writer,
                current["frame_type"],
                current["payload_raw"],
                current["connection_generation"],
                current["received_utc"],
                current["received_monotonic_ns"],
                current["direction"],
            )
        )
        assert (
            expected[-1]
            == (
                json.dumps(current, ensure_ascii=True, separators=(",", ":")) + "\n"
            ).encode()
        )
        writer.write(
            current["frame_type"],
            current["payload_raw"],
            current["connection_generation"],
            current["direction"],
        )
    manifest = writer.seal()
    actual = gzip.decompress((tmp_path / manifest["filename"]).read_bytes()).splitlines(
        keepends=True
    )
    assert actual == expected
    assert manifest["frame_count"] == count
    assert manifest["first_frame_seq"] == rows[0]["local_frame_seq"]
    assert manifest["last_frame_seq"] == rows[-1]["local_frame_seq"]
    assert (
        manifest["start_utc"] == rows[0]["received_utc"]
        and manifest["end_utc"] == rows[-1]["received_utc"]
    )
    assert manifest["connection_generations"] == sorted(
        {row["connection_generation"] for row in rows}
    )


@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
def test_all_frame_types_and_escaped_strings_have_identical_bytes(
    tmp_path, monkeypatch, venue
):
    received_utc, received_monotonic_ns = (
        "2026-11-06T12:00:00+00:00",
        9876543210123456789,
    )
    monkeypatch.setattr(rr, "utc", lambda: received_utc)
    monkeypatch.setattr(rr.time, "monotonic_ns", lambda: received_monotonic_ns)
    writer = rr.ChunkWriter(tmp_path, 'rec"\\é😀\n', venue, f"{venue}_00")
    writer.seq = 2**63 - 2
    first = writer.seq + 1
    kinds = [
        ("text", "received"),
        ("app_ping", "received"),
        ("app_ping", "sent"),
        ("app_pong", "received"),
        ("app_pong", "sent"),
        ("ws_ping", "received_control_base64"),
        ("ws_pong", "received_control_base64"),
        ("ws_close", "received_control_base64"),
        ("subscription", "sent"),
        ("binary_base64", "received"),
        ("seq_gap", "local"),
        ("disk_pause_gap", "local"),
        ("transport_error", "local"),
        ('type"\\é', "direction\u2028"),
    ]
    payloads = [
        "",
        "é😀漢字",
        "".join(map(chr, range(32))) + "\x7f",
        '"\\',
        "\u2028",
        "\ud800\udfff",
        'é😀漢字"\\\n\u2028' * 65536,
    ]
    expected, generations = [], set()
    for frame_type, direction in kinds:
        for index, payload_raw in enumerate(payloads):
            generation = (0, 1, 2, 2**80)[index % 4]
            expected.append(
                original_line(
                    writer,
                    frame_type,
                    payload_raw,
                    generation,
                    received_utc,
                    received_monotonic_ns,
                    direction,
                )
            )
            writer.write(frame_type, payload_raw, generation, direction)
            generations.add(generation)
    manifest = writer.seal()
    actual = gzip.decompress((tmp_path / manifest["filename"]).read_bytes()).splitlines(
        keepends=True
    )
    assert actual == expected
    assert (
        manifest["first_frame_seq"] == first
        and manifest["last_frame_seq"] == first + len(expected) - 1
    )
    assert manifest["frame_count"] == len(expected) and manifest[
        "connection_generations"
    ] == sorted(generations)
    assert manifest["start_utc"] == manifest["end_utc"] == received_utc


def test_utc_zero_microseconds_and_calendar_boundaries_keep_isoformat_bytes(
    tmp_path, monkeypatch
):
    moments = [
        datetime(2026, 12, 31, 23, 59, 59, 999999, timezone.utc),
        datetime(2027, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 4, 30, 23, 59, 59, 1, timezone.utc),
        datetime(2026, 5, 1, tzinfo=timezone.utc),
        datetime(2024, 2, 28, 23, 59, 59, 123000, timezone.utc),
        datetime(2024, 2, 29, tzinfo=timezone.utc),
        datetime(2024, 3, 1, tzinfo=timezone.utc),
    ]

    class Clock(datetime):
        @classmethod
        def now(cls, tz):
            assert tz is timezone.utc
            return current

    monkeypatch.setattr(rr, "datetime", Clock)
    monkeypatch.setattr(rr.time, "monotonic_ns", lambda: 123)
    writer = rr.ChunkWriter(tmp_path, "rec", "polymarket", "polymarket_00")
    expected = []
    for current in moments:
        assert rr.utc() == current.isoformat()
        expected.append(original_line(writer, "text", "", 1, current.isoformat(), 123))
        writer.write("text", "", 1)
    manifest = writer.seal()
    assert gzip.decompress((tmp_path / manifest["filename"]).read_bytes()) == b"".join(
        expected
    )
    assert b'"received_utc":"2027-01-01T00:00:00+00:00"' in expected[1]


def test_write_does_not_call_json_dumps_after_prefix_is_built(tmp_path, monkeypatch):
    writer = rr.ChunkWriter(tmp_path, "rec", "polymarket", "polymarket_00")

    def unexpected_dump(*args, **kwargs):
        pytest.fail("per-frame envelope used json.dumps")

    with monkeypatch.context() as patched:
        patched.setattr(rr.json, "dumps", unexpected_dump)
        writer.write("text", 'é😀漢字\n"\\', 1)
        writer.write("ws_ping", "", 2, "received_control_base64")
    assert writer.seal()["frame_count"] == 2


def test_paused_write_does_not_read_clocks_or_advance_bookkeeping(
    tmp_path, monkeypatch
):
    writer = rr.ChunkWriter(tmp_path, "rec", "polymarket", "polymarket_00")
    writer.write("text", "first", 1)
    before = (
        writer.seq,
        writer.index,
        writer.start,
        writer.end,
        writer.generations.copy(),
    )
    writer.pause()

    def unexpected_clock():
        pytest.fail("paused frame read a clock")

    with monkeypatch.context() as patched:
        patched.setattr(rr, "utc", unexpected_clock)
        patched.setattr(rr.time, "monotonic_ns", unexpected_clock)
        assert writer.write("text", "dropped", 2) is None
    assert (
        writer.seq,
        writer.index,
        writer.start,
        writer.end,
        writer.generations,
    ) == before
    assert writer.dropped == writer.dropped_total == 1
    assert writer.seal()["frame_count"] == 1
