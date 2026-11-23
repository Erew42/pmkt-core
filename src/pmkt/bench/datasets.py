"""Resolve immutable benchmark inputs. Temporary sidecars exist only for samples."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import gzip
import hashlib
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from typing import Any, Literal

from pmkt import _native
from pmkt.data.market_catalog.fs import sha256_file
from pmkt.data.market_catalog.types import HISTORY_MANIFEST_SCHEMA

Venue = Literal["kalshi", "polymarket"]


@dataclass(frozen=True)
class Capture:
    venue: Venue
    session: Path
    frames: int
    label: str

    def envelopes(self) -> Iterator[dict[str, Any]]:
        """Iterate source frames in recorder order, retaining the original wire text."""
        chunks = sorted(
            self.session.glob("chunk_*.jsonl.gz"), key=lambda p: int(p.name[6:-9])
        )
        for chunk in chunks:
            with gzip.open(chunk, "rt", encoding="utf-8") as source:
                for line in source:
                    yield json.loads(line)


@dataclass(frozen=True)
class Dataset:
    captures: tuple[Capture, ...]
    parquet: tuple[Path, ...]
    catalogs: tuple[Path, ...]
    merge_groups: tuple[tuple[Capture, ...], ...]
    provenance: dict[str, Any]


def _stage_sample(source: Path, session: Path) -> None:
    """Wrap an unsealed, contiguous sample in a temporary benchmark session."""
    session.mkdir()
    target = session / "chunk_000001.jsonl.gz"
    shutil.copyfile(source, target)
    with gzip.open(target, "rt", encoding="utf-8") as handle:
        first_line = handle.readline()
        if not first_line:
            raise ValueError(f"empty capture sample: {source}")
        first = json.loads(first_line)
        count = 1 + sum(1 for _ in handle)
    if first.get("local_frame_seq") != 1:
        raise ValueError(
            f"standalone sample must start at frame 1; use the sealed session: {source}"
        )
    manifest = {
        "schema": f"{first['venue']}.public_raw_chunk.v1",
        "venue": first["venue"],
        "recording_id": first["recording_id"],
        "filename": target.name,
        "size_bytes": target.stat().st_size,
        "sha256": sha256_file(target),
        "frame_count": count,
        "first_frame_seq": 1,
        "last_frame_seq": count,
        "previous_chunk_sha256": None,
    }
    target.with_suffix(".gz.manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )


def _paths(root: Path, entries: Any) -> list[Path]:
    if not isinstance(entries, list) or not all(isinstance(s, str) for s in entries):
        raise ValueError("manifest entries must be arrays of relative paths")
    result = []
    for entry in entries:
        path = (root / entry).resolve()
        if (
            Path(entry).is_absolute()
            or not path.is_relative_to(root)
            or not path.exists()
        ):
            raise ValueError(f"invalid benchmark input path: {entry}")
        result.append(path)
    if len(result) != len(set(result)):
        raise ValueError("duplicate path in benchmark manifest")
    return result


def _inputs(root: Path) -> tuple[list[Path], list[Path], list[Path], list[list[Path]]]:
    manifest = root / "bench-manifest.json"
    if manifest.is_file():
        obj = json.loads(manifest.read_text(encoding="utf-8"))
        if not isinstance(obj, dict) or set(obj) - {
            "captures",
            "parquet",
            "catalogs",
            "merge_groups",
        }:
            raise ValueError("invalid bench-manifest.json fields")
        groups = obj.get("merge_groups", [])
        if not isinstance(groups, list):
            raise ValueError("merge_groups must be an array of capture groups")
        return (
            _paths(root, obj.get("captures", [])),
            _paths(root, obj.get("parquet", [])),
            _paths(root, obj.get("catalogs", [])),
            [_paths(root, group) for group in groups],
        )
    # A sealed session's chunks form one input, including its hash chain.
    captures = set()
    for path in root.rglob("*.jsonl.gz"):
        captures.add(path.parent if path.name.startswith("chunk_") else path)
    return (
        sorted(captures),
        sorted(p for p in root.rglob("*.parquet") if p.is_file()),
        sorted(
            p
            for p in root.rglob("PUBLISHED_MANIFEST.json")
            if json.loads(p.read_text(encoding="utf-8")).get("schema_version")
            == HISTORY_MANIFEST_SCHEMA
        ),
        [],
    )


def seal_provenance(fields: dict[str, Any]) -> dict[str, Any]:
    """Return a copy whose sha256 covers every other field as canonical JSON.

    Consumers that add provenance to a dataset reseal it instead of editing a
    sealed dictionary in place.
    """
    body = {key: value for key, value in fields.items() if key != "sha256"}
    payload = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return {**body, "sha256": hashlib.sha256(payload).hexdigest()}


@contextmanager
def open_dataset(data_dir: Path | None = None) -> Iterator[Dataset]:
    """Discover captures and Parquet, or use relative paths in bench-manifest.json.

    A session directory contains the recorder's sealed chunk/sidecar pairs. An
    individual sealed chunk selects just that pair. Other gzip inputs must be
    contiguous public_raw_frame.v1 samples starting at sequence 1.
    """
    kind = "fixture" if data_dir is None else "external"
    if data_dir is None:
        root = Path(__file__).resolve().parent / "fixtures"
        if not root.is_dir():
            root = Path(__file__).resolve().parents[3] / "tests/fixtures/raw_capture"
        capture_paths = [root / f"{v}_00.jsonl.gz" for v in ("polymarket", "kalshi")]
        parquet: list[Path] = []
        catalogs: list[Path] = []
        groups: list[list[Path]] = []
    else:
        root = Path(data_dir).resolve()
        if not root.is_dir():
            raise ValueError(f"benchmark data directory does not exist: {root}")
        capture_paths, parquet, catalogs, groups = _inputs(root)
    if not (capture_paths or parquet or catalogs):
        raise ValueError(f"no raw captures or Parquet files under {root}")

    files: dict[str, dict[str, Any]] = {}

    def record(path: Path) -> None:
        label = path.relative_to(root).as_posix()
        files[label] = {
            "path": label,
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }

    with TemporaryDirectory(prefix="pmkt-bench-capture-") as directory:
        captures: dict[Path, Capture] = {}
        summaries = []
        for index, path in enumerate(capture_paths):
            if path.is_dir():
                session = path
                for chunk in sorted(path.glob("chunk_*.jsonl.gz")):
                    record(chunk)
                    record(chunk.with_suffix(".gz.manifest.json"))
            elif path.name.endswith(".jsonl.gz"):
                record(path)
                session = Path(directory) / str(index)
                if path.name.startswith("chunk_"):
                    sidecar = path.with_suffix(".gz.manifest.json")
                    record(sidecar)
                    session.mkdir()
                    shutil.copyfile(path, session / path.name)
                    shutil.copyfile(sidecar, session / sidecar.name)
                else:
                    _stage_sample(path, session)
            else:
                raise ValueError(f"unsupported capture: {path}")
            info = _native.inspect_raw_capture(session)
            venue = info["venue"]
            if venue is None or not info["frame_count"] or info["incomplete_files"]:
                raise ValueError(f"empty or unsealed capture: {path}")
            label = path.relative_to(root).as_posix()
            captures[path] = Capture(venue, session, info["frame_count"], label)
            summaries.append(
                {
                    "path": label,
                    "venue": venue,
                    "frames": info["frame_count"],
                    "recording_id": info["recording_id"],
                    "first_utc": info["first_received_utc"],
                    "last_utc": info["last_received_utc"],
                    "frame_types": info["frame_type_counts"],
                    "missing_frames": sum(
                        g["missing_frame_count"] for g in info["chunk_gaps"]
                    ),
                }
            )
        for path in parquet:
            if not path.is_file() or path.suffix.lower() != ".parquet":
                raise ValueError(f"expected a Parquet file: {path}")
            record(path)
        for path in catalogs:
            record(path)
        merge_groups = []
        for group in groups:
            if len(group) < 2 or any(path not in captures for path in group):
                raise ValueError("merge_groups require at least two selected captures")
            copies = tuple(captures[path] for path in group)
            if len({copy.venue for copy in copies}) != 1:
                raise ValueError("merge_groups must share a venue")
            merge_groups.append(copies)
        provenance = seal_provenance(
            {
                "kind": kind,
                "files": list(files.values()),
                "captures": summaries,
                "parquet": [p.relative_to(root).as_posix() for p in parquet],
                "catalogs": [p.relative_to(root).as_posix() for p in catalogs],
                "merge_groups": [[c.label for c in group] for group in merge_groups],
            }
        )
        yield Dataset(
            tuple(captures.values()),
            tuple(parquet),
            tuple(catalogs),
            tuple(merge_groups),
            provenance,
        )
