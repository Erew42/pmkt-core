from __future__ import annotations

import hashlib
import json
import os
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pmkt.data.manifests import validate_run_manifest
from pmkt.streaming.durability import (
    RUN_STATE_NAME,
    file_sha256,
    write_json_atomic_fsync,
)
from pmkt.streaming.recovery_contracts import RunStateV1


@dataclass(frozen=True)
class CaptureArchiveResult:
    archive_path: Path
    archive_manifest_path: Path
    archive_sha256: str
    member_count: int
    unpacked_bytes: int
    source_deleted: bool


def archive_finalized_capture(
    manifest_path: str | Path,
    *,
    archive_path: str | Path | None = None,
    delete_source: bool = False,
    compress: bool = False,
) -> CaptureArchiveResult:
    """Package a validated finalized run into one verified ZIP container.

    Parquet is already compressed, so the default uses ZIP_STORED: it removes
    filesystem inode/file-count amplification without spending CPU recompressing
    columnar data.  Source deletion is performed only after exact manifest
    validation, archive CRC/member verification, and sidecar publication.
    """

    manifest = Path(manifest_path).resolve()
    validation = validate_run_manifest(manifest)
    if not validation.ok:
        raise ValueError(
            "capture manifest is invalid: " + "; ".join(validation.all_errors)
        )
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("capture manifest must be a JSON object")
    run_dir = Path(str(payload.get("run_dir") or manifest.parent)).resolve()
    if manifest.parent != run_dir or manifest != run_dir / "manifest.json":
        raise ValueError("capture manifest must be located in its exact run_dir")
    state = RunStateV1.from_mapping(
        json.loads((run_dir / RUN_STATE_NAME).read_text(encoding="utf-8"))
    )
    if state.status != "finalized":
        raise ValueError("capture archive requires a finalized run state")

    return _archive_validated_directory(
        source_dir=run_dir,
        authority_manifest=manifest,
        source_id=state.run_id,
        source_kind="run",
        authority_filename="manifest.json",
        archive_path=archive_path,
        delete_source=delete_source,
        compress=compress,
        sidecar_extra={"source_run_id": state.run_id},
    )


def archive_capture_connection_group(
    group_manifest_path: str | Path,
    *,
    archive_path: str | Path | None = None,
    delete_source: bool = False,
    compress: bool = False,
) -> CaptureArchiveResult:
    """Validate and package a complete multi-connection capture group."""

    group_manifest = Path(group_manifest_path).resolve()
    payload = json.loads(group_manifest.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("capture group manifest must be a JSON object")
    if payload.get("schema_version") != "capture_connection_group.v1":
        raise ValueError("expected capture_connection_group.v1")
    group_dir = group_manifest.parent.resolve()
    declared_group_dir = Path(str(payload.get("run_dir") or ""))
    if not declared_group_dir.is_absolute():
        declared_group_dir = group_dir / declared_group_dir
    if declared_group_dir.resolve() != group_dir:
        raise ValueError("capture group run_dir must contain its authoritative manifest")
    children = payload.get("children")
    if not isinstance(children, list) or not children:
        raise ValueError("capture group requires a non-empty children array")
    if int(payload.get("connection_count") or 0) != len(children):
        raise ValueError("capture group connection_count does not match children")

    child_manifest_hashes: list[str] = []
    child_manifest_paths: set[Path] = set()
    for index, child in enumerate(children):
        if not isinstance(child, dict):
            raise ValueError(f"capture group child {index} must be an object")
        raw_manifest_path = str(child.get("manifest_path") or "")
        manifest_path = Path(raw_manifest_path)
        if not manifest_path.is_absolute():
            manifest_path = group_dir / manifest_path
        manifest_path = manifest_path.resolve()
        if not _is_within(manifest_path, group_dir):
            raise ValueError(f"capture group child {index} escapes the group directory")
        if manifest_path.name != "manifest.json" or not manifest_path.is_file():
            raise ValueError(f"capture group child {index} manifest is unavailable")
        raw_child_dir = Path(str(child.get("run_dir") or ""))
        if not raw_child_dir.is_absolute():
            raw_child_dir = group_dir / raw_child_dir
        if raw_child_dir.resolve() != manifest_path.parent:
            raise ValueError(f"capture group child {index} run_dir mismatch")
        if manifest_path in child_manifest_paths:
            raise ValueError("capture group repeats a child manifest")
        child_manifest_paths.add(manifest_path)
        expected_hash = str(child.get("manifest_sha256") or "")
        actual_hash = file_sha256(manifest_path)
        if actual_hash != expected_hash:
            raise ValueError(f"capture group child {index} manifest hash mismatch")
        child_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(child_payload, dict):
            raise ValueError(f"capture group child {index} manifest must be an object")
        if str(child_payload.get("status") or "unknown") != str(
            child.get("status") or "unknown"
        ):
            raise ValueError(f"capture group child {index} status mismatch")
        feed_shards = child_payload.get("feed_shards")
        child_shard_id = str(child.get("shard_id") or "")
        if (
            not isinstance(feed_shards, list)
            or len(feed_shards) != 1
            or not isinstance(feed_shards[0], dict)
            or str(feed_shards[0].get("shard_id") or "") != child_shard_id
        ):
            raise ValueError(f"capture group child {index} shard ownership mismatch")
        validation = validate_run_manifest(manifest_path)
        if not validation.ok:
            raise ValueError(
                f"capture group child {index} is invalid: "
                + "; ".join(validation.all_errors)
            )
        state = RunStateV1.from_mapping(
            json.loads(
                (manifest_path.parent / RUN_STATE_NAME).read_text(encoding="utf-8")
            )
        )
        if state.status != "finalized":
            raise ValueError(f"capture group child {index} is not finalized")
        child_manifest_hashes.append(actual_hash)

    source_id = str(payload.get("run_id") or group_dir.name)
    child_digest = hashlib.sha256(
        "\n".join(sorted(child_manifest_hashes)).encode("ascii")
    ).hexdigest()
    return _archive_validated_directory(
        source_dir=group_dir,
        authority_manifest=group_manifest,
        source_id=source_id,
        source_kind="connection_group",
        authority_filename=group_manifest.name,
        archive_path=archive_path,
        delete_source=delete_source,
        compress=compress,
        sidecar_extra={
            "child_count": len(children),
            "child_manifest_hashes_digest": child_digest,
        },
    )


def archive_capture(
    authority_path: str | Path,
    *,
    archive_path: str | Path | None = None,
    delete_source: bool = False,
    compress: bool = False,
) -> CaptureArchiveResult:
    """Archive either one finalized run or a connection-group authority."""

    authority = Path(authority_path)
    payload = json.loads(authority.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and payload.get("schema_version") == (
        "capture_connection_group.v1"
    ):
        return archive_capture_connection_group(
            authority,
            archive_path=archive_path,
            delete_source=delete_source,
            compress=compress,
        )
    return archive_finalized_capture(
        authority,
        archive_path=archive_path,
        delete_source=delete_source,
        compress=compress,
    )


def _archive_validated_directory(
    *,
    source_dir: Path,
    authority_manifest: Path,
    source_id: str,
    source_kind: str,
    authority_filename: str,
    archive_path: str | Path | None,
    delete_source: bool,
    compress: bool,
    sidecar_extra: dict[str, Any],
) -> CaptureArchiveResult:
    run_dir = source_dir
    manifest = authority_manifest

    destination = (
        Path(archive_path).resolve()
        if archive_path is not None
        else run_dir.with_name(run_dir.name + ".pmkt.zip")
    )
    if destination == run_dir or _is_within(destination, run_dir):
        raise ValueError("capture archive must be outside the source run directory")
    if destination.exists():
        raise FileExistsError(f"capture archive already exists: {destination}")
    sidecar = destination.with_name(destination.name + ".json")
    if sidecar.exists():
        raise FileExistsError(f"capture archive sidecar already exists: {sidecar}")

    members = _capture_members(run_dir)
    if not members:
        raise ValueError("capture run contains no files")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_name(destination.name + ".partial")
    if temp.exists():
        raise FileExistsError(f"capture archive staging path already exists: {temp}")
    compression = zipfile.ZIP_DEFLATED if compress else zipfile.ZIP_STORED
    try:
        with zipfile.ZipFile(
            temp,
            mode="x",
            compression=compression,
            compresslevel=1 if compress else None,
            allowZip64=True,
        ) as archive:
            for path in members:
                archive.write(path, path.relative_to(run_dir).as_posix())
        with temp.open("rb+") as handle:
            os.fsync(handle.fileno())
        _verify_archive(temp, run_dir=run_dir, members=members)
        os.replace(temp, destination)
    except BaseException:
        if temp.exists():
            temp.unlink()
        raise

    archive_sha256 = file_sha256(destination)
    sidecar_payload: dict[str, Any] = {
        "schema_version": "capture_archive.v1",
        "archive_path": str(destination),
        "archive_sha256": archive_sha256,
        "compression": "deflate-1" if compress else "stored",
        "source_kind": source_kind,
        "source_id": source_id,
        "source_run_dir": str(run_dir),
        "source_manifest_sha256": file_sha256(manifest),
        "member_count": len(members),
        "unpacked_bytes": sum(path.stat().st_size for path in members),
        "members_digest": _members_digest(run_dir, members),
        "source_deleted": False,
        "restore_required_for_replay": True,
        **sidecar_extra,
    }
    write_json_atomic_fsync(sidecar, sidecar_payload)
    _verify_published_archive(destination, sidecar_payload)

    if delete_source:
        _require_safe_source_deletion(
            run_dir,
            destination=destination,
            authority_filename=authority_filename,
        )
        if _members_digest(run_dir, members) != sidecar_payload["members_digest"]:
            raise ValueError("capture source changed after archive verification")
        shutil.rmtree(run_dir)
        sidecar_payload["source_deleted"] = True
        write_json_atomic_fsync(sidecar, sidecar_payload)
    return CaptureArchiveResult(
        archive_path=destination,
        archive_manifest_path=sidecar,
        archive_sha256=archive_sha256,
        member_count=len(members),
        unpacked_bytes=int(sidecar_payload["unpacked_bytes"]),
        source_deleted=bool(delete_source),
    )


def _capture_members(run_dir: Path) -> tuple[Path, ...]:
    members: list[Path] = []
    for path in sorted(run_dir.rglob("*"), key=lambda item: item.as_posix()):
        if path.is_symlink():
            raise ValueError(f"capture archive rejects symlinks: {path}")
        if path.is_file():
            members.append(path)
    return tuple(members)


def _verify_archive(archive_path: Path, *, run_dir: Path, members: tuple[Path, ...]) -> None:
    expected = {
        path.relative_to(run_dir).as_posix(): (path.stat().st_size, file_sha256(path))
        for path in members
    }
    with zipfile.ZipFile(archive_path, mode="r") as archive:
        bad_member = archive.testzip()
        if bad_member is not None:
            raise ValueError(f"capture archive CRC validation failed: {bad_member}")
        actual = {item.filename: item.file_size for item in archive.infolist()}
        if actual != {name: size for name, (size, _) in expected.items()}:
            raise ValueError("capture archive member list or sizes do not match source")
        for name, (_, expected_sha256) in expected.items():
            with archive.open(name, mode="r") as member:
                if _stream_sha256(member) != expected_sha256:
                    raise ValueError(
                        f"capture archive member hash does not match source: {name}"
                    )


def _verify_published_archive(
    archive_path: Path, sidecar_payload: dict[str, Any]
) -> None:
    if file_sha256(archive_path) != sidecar_payload["archive_sha256"]:
        raise ValueError("published capture archive hash does not match its sidecar")
    with zipfile.ZipFile(archive_path, mode="r") as archive:
        if archive.testzip() is not None:
            raise ValueError("published capture archive failed CRC validation")
        if len(archive.infolist()) != int(sidecar_payload["member_count"]):
            raise ValueError("published capture archive member count mismatch")


def _members_digest(run_dir: Path, members: tuple[Path, ...]) -> str:
    digest = hashlib.sha256()
    for path in members:
        relative = path.relative_to(run_dir).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(path.stat().st_size).encode("ascii"))
        digest.update(b"\0")
        digest.update(file_sha256(path).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def _stream_sha256(handle: Any) -> str:
    digest = hashlib.sha256()
    while chunk := handle.read(1024 * 1024):
        digest.update(chunk)
    return digest.hexdigest()


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _require_safe_source_deletion(
    run_dir: Path,
    *,
    destination: Path,
    authority_filename: str,
) -> None:
    resolved = run_dir.resolve()
    if resolved == Path(resolved.anchor) or len(resolved.parts) < 3:
        raise ValueError("refusing to delete an unsafe capture source path")
    if not resolved.is_dir() or not (resolved / authority_filename).is_file():
        raise ValueError("capture source directory changed before deletion")
    if _is_within(destination, resolved):
        raise ValueError("capture archive must remain outside deleted source")


__all__ = [
    "CaptureArchiveResult",
    "archive_capture",
    "archive_capture_connection_group",
    "archive_finalized_capture",
]
