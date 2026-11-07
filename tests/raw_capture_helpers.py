"""Local sealed-chunk checks, independent of deployment and upload tools."""

import gzip
import hashlib
import json
from pathlib import Path


def validate_chunk(chunk, manifest_path):
    data = Path(chunk).read_bytes()
    manifest = json.loads(Path(manifest_path).read_text())
    frames = [json.loads(line) for line in gzip.decompress(data).splitlines()]
    assert manifest["filename"] == Path(chunk).name
    assert manifest["sha256"] == hashlib.sha256(data).hexdigest()
    assert manifest["md5"] == hashlib.md5(data).hexdigest()
    assert manifest["size_bytes"] == len(data)
    assert manifest["frame_count"] == len(frames)
    assert [frame["local_frame_seq"] for frame in frames] == list(
        range(manifest["first_frame_seq"], manifest["last_frame_seq"] + 1)
    )
    assert manifest["start_utc"] == frames[0]["received_utc"]
    assert manifest["end_utc"] == frames[-1]["received_utc"]
    assert manifest["connection_generations"] == sorted(
        {frame["connection_generation"] for frame in frames}
    )
    assert all(
        frame["recording_id"] == manifest["recording_id"]
        and frame["schema"] == f"{manifest['venue']}.public_raw_frame.v1"
        for frame in frames
    )
    return {"ok": True}
