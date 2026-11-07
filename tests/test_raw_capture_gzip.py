"""Gzip level changes preserve the raw envelope and compressed-byte checksum contracts."""

import gzip
import json


from pmkt.streaming import raw_capture as rr
from raw_capture_helpers import validate_chunk


def test_gzip_levels_reach_compressor_and_preserve_exact_envelopes(
    tmp_path, monkeypatch
):
    gzip_file, calls = gzip.GzipFile, []

    def compressor(*args, **kwargs):
        if kwargs.get("mode") == "wb":
            calls.append((kwargs["compresslevel"], kwargs["mtime"]))
        return gzip_file(*args, **kwargs)

    monkeypatch.setattr(rr.gzip, "GzipFile", compressor)
    monkeypatch.setattr(rr, "utc", lambda: "2026-11-06T12:00:00+00:00")
    monkeypatch.setattr(rr.time, "monotonic_ns", lambda: 123456789)
    payloads = [' [{"n":1.000, "escape":"\\u00e3"}] \n', "PING", "not JSON"]
    decompressed = []
    for level in (None, 1, 6, 9):
        session = tmp_path / str(level) / "session"
        session.mkdir(parents=True)
        options = {} if level is None else dict(gzip_level=level)
        writer = rr.ChunkWriter(
            session, "rec", "polymarket", "polymarket_00", **options
        )
        envelopes = [
            dict(
                schema="polymarket.public_raw_frame.v1",
                venue="polymarket",
                recording_id="rec",
                connection_generation=1,
                local_frame_seq=i,
                received_utc="2026-11-06T12:00:00+00:00",
                received_monotonic_ns=123456789,
                frame_type="text",
                direction="received",
                payload_raw=payload,
            )
            for i, payload in enumerate(payloads, 1)
        ]
        for payload in payloads:
            writer.write("text", payload, 1)
        manifest = writer.seal()
        chunk = session / manifest["filename"]
        data = chunk.read_bytes()
        assert data[:3] == b"\x1f\x8b\x08" and data[4:8] == b"\0" * 4
        lines = gzip.decompress(data)
        assert (
            lines
            == "".join(
                json.dumps(e, ensure_ascii=True, separators=(",", ":")) + "\n"
                for e in envelopes
            ).encode()
        )
        assert [e["payload_raw"] for e in envelopes] == payloads
        assert validate_chunk(chunk, str(chunk) + ".manifest.json")["ok"]
        decompressed.append(lines)
    assert calls == [(6, 0), (1, 0), (6, 0), (9, 0)]
    assert all(lines == decompressed[0] for lines in decompressed)
