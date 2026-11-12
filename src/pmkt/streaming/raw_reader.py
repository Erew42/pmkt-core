"""Streaming reads of sealed public raw WebSocket captures (no venue I/O)."""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import warnings
import zlib
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class RawCaptureError(ValueError):
    """A capture cannot be decoded or fails a declared integrity check."""


class RawCaptureGapWarning(UserWarning):
    """Predecessors are absent; frames after this break may lack baselines."""


@dataclass(frozen=True)
class RawFrame:
    schema: str
    venue: str
    recording_id: str
    connection_generation: int
    local_frame_seq: int
    received_utc: str
    received_monotonic_ns: int
    frame_type: str
    direction: str
    payload_raw: str


@dataclass(frozen=True)
class SealedChunk:
    path: Path
    manifest: dict[str, Any]


@dataclass(frozen=True)
class SessionChunks:
    chunks: tuple[SealedChunk, ...]
    incomplete: tuple[Path, ...]


@dataclass(frozen=True)
class VenueMessage:
    frame: RawFrame
    value: Any
    text: str
    batch_index: int | None = None
    parsed: bool = True


_CHUNK_NAME = re.compile(r"chunk_([0-9]{6,})\.jsonl\.gz\Z")


def _object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise RawCaptureError(f"{path}: cannot read JSON object: {exc}") from exc
    if not isinstance(value, dict):
        raise RawCaptureError(f"{path}: expected a JSON object")
    return value


def _field(obj: dict[str, Any], key: str, kind: type, source: Path) -> Any:
    value = obj.get(key)
    if type(value) is not kind:
        raise RawCaptureError(f"{source}: {key} must be {kind.__name__}")
    return value


def _sealed_chunk(path: Path) -> SealedChunk:
    match = _CHUNK_NAME.fullmatch(path.name)
    if (
        match is None
        or int(match[1]) < 1
        or path.name != f"chunk_{int(match[1]):06d}.jsonl.gz"
    ):
        raise RawCaptureError(f"{path}: expected a published chunk filename")
    manifest_path = path.with_name(path.name + ".manifest.json")
    manifest = _object(manifest_path)
    for key in ("schema", "venue", "recording_id", "filename", "sha256"):
        _field(manifest, key, str, manifest_path)
    for key in ("size_bytes", "frame_count", "first_frame_seq", "last_frame_seq"):
        _field(manifest, key, int, manifest_path)
    if "previous_chunk_sha256" not in manifest or (
        manifest["previous_chunk_sha256"] is not None
        and not isinstance(manifest["previous_chunk_sha256"], str)
    ):
        raise RawCaptureError(f"{manifest_path}: invalid previous_chunk_sha256")
    if manifest["venue"] not in {"kalshi", "polymarket"} or manifest["schema"] != (
        f"{manifest['venue']}.public_raw_chunk.v1"
    ):
        raise RawCaptureError(f"{manifest_path}: unsupported schema or venue")
    if manifest["filename"] != path.name:
        raise RawCaptureError(f"{manifest_path}: filename does not match {path.name}")
    return SealedChunk(path, manifest)


def list_session_chunks(session: str | Path) -> SessionChunks:
    """List published pairs in numeric order, separately reporting incomplete files.

    Includes orphan manifests and any .part/.tmp file as incomplete evidence.
    Metadata is kept in memory, never the chunk contents. Unknown manifest keys
    are retained in the plain dict but have no effect on verification.
    """
    root = Path(session)
    try:
        files = {path.name: path for path in root.iterdir() if path.is_file()}
    except OSError as exc:
        raise RawCaptureError(f"{root}: cannot list session: {exc}") from exc
    chunks, incomplete = [], []
    for name, path in files.items():
        if _CHUNK_NAME.fullmatch(name):
            if name + ".manifest.json" in files:
                chunks.append(_sealed_chunk(path))
            else:
                incomplete.append(path)
        elif name.endswith((".part", ".tmp")):
            incomplete.append(path)
        elif name.endswith(".manifest.json"):
            chunk_name = name.removesuffix(".manifest.json")
            if _CHUNK_NAME.fullmatch(chunk_name) and chunk_name not in files:
                incomplete.append(path)
    chunks.sort(key=lambda chunk: int(chunk.path.name.split("_")[1].split(".")[0]))
    return SessionChunks(tuple(chunks), tuple(sorted(incomplete)))


def load_session_metadata(session: str | Path) -> dict[str, dict[str, Any]]:
    """Load required PROVENANCE.json and optional FINAL.json/READY.json by filename.

    Return whole objects, including legacy disclaimer and other unknown fields.
    State files are observations, never evidence of sealing or completeness.
    """
    root = Path(session)
    result = {"PROVENANCE.json": _object(root / "PROVENANCE.json")}
    for name in ("FINAL.json", "READY.json"):
        if (root / name).exists():
            result[name] = _object(root / name)
    return result


def _frame(value: Any, path: Path) -> RawFrame:
    if not isinstance(value, dict):
        raise RawCaptureError(f"{path}: envelope must be a JSON object")
    return RawFrame(
        schema=_field(value, "schema", str, path),
        venue=_field(value, "venue", str, path),
        recording_id=_field(value, "recording_id", str, path),
        connection_generation=_field(value, "connection_generation", int, path),
        local_frame_seq=_field(value, "local_frame_seq", int, path),
        received_utc=_field(value, "received_utc", str, path),
        received_monotonic_ns=_field(value, "received_monotonic_ns", int, path),
        frame_type=_field(value, "frame_type", str, path),
        direction=_field(value, "direction", str, path),
        payload_raw=_field(value, "payload_raw", str, path),
    )


def read_chunk(path: str | Path, *, verify: bool = True) -> Iterator[RawFrame]:
    """Read a published pair; exhaust the iterator to complete verification.

    Compressed size/SHA-256 are checked before yielding; counts and endpoints
    are checked at EOF. verify=False skips integrity/identity checks, but still
    requires a manifest and decodable, typed envelopes. Files must be immutable.
    """
    yield from _read_chunk(_sealed_chunk(Path(path)), verify=verify)


def _read_chunk(chunk: SealedChunk, *, verify: bool) -> Iterator[RawFrame]:
    path, manifest = chunk.path, chunk.manifest
    count = 0
    first = last = None
    try:
        with path.open("rb") as compressed:
            if verify:
                digest, size = hashlib.sha256(), 0
                while block := compressed.read(1024 * 1024):
                    digest.update(block)
                    size += len(block)
                for key, actual in (
                    ("size_bytes", size),
                    ("sha256", digest.hexdigest()),
                ):
                    if actual != manifest[key]:
                        raise RawCaptureError(f"{path}: {key} mismatch")
                compressed.seek(0)
            with gzip.open(compressed, "rt", encoding="utf-8") as source:
                for line_number, line in enumerate(source, 1):
                    try:
                        frame = _frame(json.loads(line), path)
                    except (ValueError, TypeError) as exc:
                        raise RawCaptureError(
                            f"{path}: line {line_number}: {exc}"
                        ) from exc
                    if verify:
                        for key, actual in (
                            ("venue", frame.venue),
                            ("recording_id", frame.recording_id),
                        ):
                            if actual != manifest[key]:
                                raise RawCaptureError(
                                    f"{path}: line {line_number}: {key} mismatch"
                                )
                        if frame.schema != f"{manifest['venue']}.public_raw_frame.v1":
                            raise RawCaptureError(
                                f"{path}: line {line_number}: schema mismatch"
                            )
                        if last is not None and frame.local_frame_seq != last + 1:
                            raise RawCaptureError(
                                f"{path}: line {line_number}: local_frame_seq is not consecutive"
                            )
                    if first is None:
                        first = frame.local_frame_seq
                    last = frame.local_frame_seq
                    count += 1
                    yield frame
    except (OSError, EOFError, UnicodeError, zlib.error) as exc:
        raise RawCaptureError(
            f"{path}: cannot read compressed envelopes: {exc}"
        ) from exc
    if verify:
        for key, observed in (
            ("frame_count", count),
            ("first_frame_seq", first),
            ("last_frame_seq", last),
        ):
            if observed != manifest[key]:
                raise RawCaptureError(
                    f"{path}: {key} mismatch (expected {manifest[key]}, got {observed})"
                )


def read_session(session: str | Path, *, verify: bool = True) -> Iterator[RawFrame]:
    """Stream existing chunks; warn before a missing prefix/middle, then resume.

    Adjacent chunks must link hashes and sequences. Across absent chunk numbers
    the link cannot be verified, but sequences must still move forward. A
    session cannot change venue or recording_id. Incomplete artefacts are only
    reported by list_session_chunks. Warnings can be promoted to errors by callers.
    """
    previous: SealedChunk | None = None
    previous_index = 0
    for chunk in list_session_chunks(session).chunks:
        index = int(chunk.path.name.split("_")[1].split(".")[0])
        manifest = chunk.manifest
        if verify:
            missing = index != previous_index + 1
            if previous is not None:
                for key in ("venue", "recording_id"):
                    if manifest[key] != previous.manifest[key]:
                        raise RawCaptureError(f"{chunk.path}: session {key} mismatch")
                expected_seq = previous.manifest["last_frame_seq"] + 1
                if manifest["first_frame_seq"] < expected_seq:
                    raise RawCaptureError(
                        f"{chunk.path}: overlapping or reversed local_frame_seq"
                    )
            else:
                expected_seq = 1
            if missing:
                warnings.warn(
                    f"{chunk.path}: missing chunks {previous_index + 1}..{index - 1}; "
                    "predecessor chain and baselines unavailable",
                    RawCaptureGapWarning,
                    stacklevel=2,
                )
            else:
                expected_hash = previous.manifest["sha256"] if previous else None
                if manifest["previous_chunk_sha256"] != expected_hash:
                    raise RawCaptureError(
                        f"{chunk.path}: previous_chunk_sha256 mismatch"
                    )
                if manifest["first_frame_seq"] != expected_seq:
                    raise RawCaptureError(
                        f"{chunk.path}: local_frame_seq discontinuity between chunks"
                    )
        yield from _read_chunk(chunk, verify=verify)
        previous, previous_index = chunk, index


def _reject_json_constant(value: str) -> Any:
    raise ValueError(f"invalid JSON constant: {value}")


def iter_venue_messages(
    frames: Iterable[RawFrame],
) -> Iterator[VenueMessage | RawFrame]:
    """Decode received text; pass every other frame through unchanged.

    Non-JSON text becomes VenueMessage(parsed=False, value=None). Valid JSON
    scalars (including null) are parsed=True. Lists yield one message per element
    with its zero-based position and exact source substring, without reserializing.
    Empty lists yield no messages; the original frame remains in the input stream.
    """
    decoder = json.JSONDecoder(parse_constant=_reject_json_constant)
    for frame in frames:
        if frame.frame_type != "text" or frame.direction != "received":
            yield frame
            continue
        text = frame.payload_raw
        try:
            value = decoder.decode(text)
        except ValueError:
            yield VenueMessage(frame, None, text, parsed=False)
            continue
        if not isinstance(value, list):
            yield VenueMessage(frame, value, text)
            continue
        offset = text.index("[") + 1
        for index, element in enumerate(value):
            while text[offset] in " \t\r\n":
                offset += 1
            _, end = decoder.raw_decode(text, offset)
            yield VenueMessage(frame, element, text[offset:end], batch_index=index)
            offset = end
            while text[offset] in " \t\r\n":
                offset += 1
            offset += 1  # comma or closing bracket
