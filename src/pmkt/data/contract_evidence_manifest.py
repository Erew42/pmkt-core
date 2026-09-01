from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from pmkt.data.registry import CONTRACT_EVIDENCE_SCHEMA_VERSION
from pmkt.data.storage.parquet import write_parquet

CONTRACT_EVIDENCE_MANIFEST_VERSION_V1 = "contract_evidence_manifest.v1"
CONTRACT_EVIDENCE_MANIFEST_VERSION = "contract_evidence_manifest.v2"
_AUTHORITATIVE_TERMINAL_STOP_REASONS = frozenset(
    {"cursor_exhausted", "empty_page", "terminal_cursor_lte"}
)


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def contract_evidence_manifest_path(artifact_path: str | Path) -> Path:
    artifact = Path(artifact_path)
    return artifact.with_name(f"{artifact.name}.manifest.json")


def contract_evidence_bundle_path(artifact_path: str | Path) -> Path:
    artifact = Path(artifact_path)
    return artifact.with_name(f"{artifact.name}.bundle")


def write_contract_evidence_bundle(
    frame: pd.DataFrame,
    *,
    artifact_path: str | Path,
    venue: str,
    source_endpoint: str,
    payload_scope: str,
    observation_time_source: str,
    source_payload_kind: str,
    collection_complete: bool,
    stop_reason: str,
    continuation_cursor: str | None,
    collection_errors: tuple[str, ...],
    source_collection_manifest: Mapping[str, Any],
) -> tuple[Path, Path]:
    """Atomically publish one immutable evidence artifact and v2 manifest directory."""
    normalized_cursor = _text(continuation_cursor) or None
    normalized_errors = tuple(
        sorted({_text(value) for value in collection_errors if _text(value)})
    )
    expected_collection_state = {
        "venue": venue,
        "source_endpoint": source_endpoint,
        "payload_scope": payload_scope,
        "collection_complete": collection_complete,
        "stop_reason": stop_reason,
        "continuation_cursor": normalized_cursor,
        "collection_errors": list(normalized_errors),
    }
    source_state = dict(source_collection_manifest)
    source_state["continuation_cursor"] = (
        _text(source_state.get("continuation_cursor")) or None
    )
    raw_source_errors = source_state.get("collection_errors")
    if isinstance(raw_source_errors, list):
        source_state["collection_errors"] = sorted(
            {_text(value) for value in raw_source_errors if _text(value)}
        )
    mismatched = [
        key
        for key, expected in expected_collection_state.items()
        if source_state.get(key) != expected
    ]
    if mismatched:
        raise ValueError(
            "source collection manifest disagrees with publication state: "
            + ", ".join(mismatched)
        )
    canonical_source_manifest = dict(source_collection_manifest)
    canonical_source_manifest.update(expected_collection_state)
    requested_artifact = Path(artifact_path)
    destination = contract_evidence_bundle_path(requested_artifact)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise FileExistsError(
            f"contract evidence bundle destination is immutable: {destination}"
        )
    staging = Path(
        tempfile.mkdtemp(prefix=f".{destination.name}.staging-", dir=destination.parent)
    )
    try:
        staged_artifact = write_parquet(
            frame,
            staging / requested_artifact.name,
            overwrite=False,
            schema=CONTRACT_EVIDENCE_SCHEMA_VERSION,
            strict=True,
        )
        source_manifest_path = staging / "source_collection_manifest.json"
        source_manifest_path.write_text(
            json.dumps(
                canonical_source_manifest,
                indent=2,
                sort_keys=True,
                default=str,
            )
            + "\n",
            encoding="utf-8",
        )
        staged_manifest = write_contract_evidence_manifest(
            frame,
            artifact_path=staged_artifact,
            venue=venue,
            source_endpoint=source_endpoint,
            payload_scope=payload_scope,
            observation_time_source=observation_time_source,
            source_payload_kind=source_payload_kind,
            collection_complete=collection_complete,
            stop_reason=stop_reason,
            continuation_cursor=normalized_cursor,
            collection_errors=normalized_errors,
            source_manifest_sha256=file_sha256(source_manifest_path),
            manifest_path=staging / "manifest.json",
        )
        staging.replace(destination)
        return destination / staged_artifact.name, destination / staged_manifest.name
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


def build_contract_evidence_manifest(
    frame: pd.DataFrame,
    *,
    artifact_path: str | Path,
    venue: str,
    source_endpoint: str,
    payload_scope: str,
    observation_time_source: str,
    source_payload_kind: str = "unknown",
    collection_complete: bool = False,
    stop_reason: str = "unknown",
    continuation_cursor: str | None = None,
    collection_errors: tuple[str, ...] = (),
    source_manifest_sha256: str | None = None,
) -> dict[str, Any]:
    artifact = Path(artifact_path)
    if not artifact.is_file():
        raise FileNotFoundError(f"contract evidence artifact not found: {artifact}")
    observations = sorted(_text(value) for value in frame["observed_at_utc"].tolist())
    evidence_ids = sorted({_text(value) for value in frame["evidence_id"].tolist()})
    payload_hashes = sorted(
        {_text(value) for value in frame["raw_payload_hash"].tolist() if _text(value)}
    )
    return build_contract_evidence_manifest_from_summary(
        artifact_path=artifact,
        venue=venue,
        source_endpoint=source_endpoint,
        payload_scope=payload_scope,
        observation_time_source=observation_time_source,
        source_payload_kind=source_payload_kind,
        collection_complete=collection_complete,
        stop_reason=stop_reason,
        continuation_cursor=continuation_cursor,
        collection_errors=collection_errors,
        source_manifest_sha256=source_manifest_sha256,
        row_count=len(frame),
        observation_min_utc=observations[0] if observations else None,
        observation_max_utc=observations[-1] if observations else None,
        evidence_ids=evidence_ids,
        payload_hashes=payload_hashes,
        payload_authority_valid=_frame_payload_authority_valid(
            frame, _text(source_payload_kind)
        ),
    )


def build_contract_evidence_manifest_from_summary(
    *,
    artifact_path: str | Path,
    venue: str,
    source_endpoint: str,
    payload_scope: str,
    observation_time_source: str,
    source_payload_kind: str,
    collection_complete: bool,
    stop_reason: str,
    continuation_cursor: str | None,
    collection_errors: tuple[str, ...],
    source_manifest_sha256: str | None,
    row_count: int,
    observation_min_utc: str | None,
    observation_max_utc: str | None,
    evidence_ids: list[str] | set[str] | tuple[str, ...],
    payload_hashes: list[str] | set[str] | tuple[str, ...],
    payload_authority_valid: bool,
) -> dict[str, Any]:
    artifact = Path(artifact_path)
    if not artifact.is_file():
        raise FileNotFoundError(f"contract evidence artifact not found: {artifact}")
    normalized_errors = sorted(
        {_text(value) for value in collection_errors if _text(value)}
    )
    source_manifest_hash = _text(source_manifest_sha256).lower() or None
    normalized_evidence_ids = sorted({_text(value) for value in evidence_ids})
    normalized_payload_hashes = sorted(
        {_text(value) for value in payload_hashes if _text(value)}
    )
    authoritative_complete = bool(
        _collection_state_is_authoritative(
            collection_complete=collection_complete,
            stop_reason=stop_reason,
            continuation_cursor=continuation_cursor,
            collection_errors=normalized_errors,
        )
        and source_manifest_hash
        and _text(source_payload_kind) in {"raw_json", "venue_api_response"}
        and payload_authority_valid
    )
    return {
        "manifest_version": CONTRACT_EVIDENCE_MANIFEST_VERSION,
        "schema_version": CONTRACT_EVIDENCE_SCHEMA_VERSION,
        "venue": _text(venue),
        "source_endpoint": _text(source_endpoint),
        "payload_scope": _text(payload_scope),
        "observation_time_source": _text(observation_time_source),
        "source_payload_kind": _text(source_payload_kind) or "unknown",
        "collection_complete": bool(collection_complete),
        "stop_reason": _text(stop_reason) or "unknown",
        "continuation_cursor": _text(continuation_cursor) or None,
        "collection_errors": normalized_errors,
        "source_manifest_sha256": source_manifest_hash,
        "payload_hashes_sha256": _sha256_json(normalized_payload_hashes),
        "authoritative_complete": authoritative_complete,
        "row_count": int(row_count),
        "observation_min_utc": _text(observation_min_utc) or None,
        "observation_max_utc": _text(observation_max_utc) or None,
        "artifact_path": artifact.name,
        "artifact_sha256": file_sha256(artifact),
        "evidence_ids_sha256": _sha256_json(normalized_evidence_ids),
    }


def write_contract_evidence_manifest(
    frame: pd.DataFrame,
    *,
    artifact_path: str | Path,
    venue: str,
    source_endpoint: str,
    payload_scope: str,
    observation_time_source: str,
    source_payload_kind: str = "unknown",
    collection_complete: bool = False,
    stop_reason: str = "unknown",
    continuation_cursor: str | None = None,
    collection_errors: tuple[str, ...] = (),
    source_manifest_sha256: str | None = None,
    manifest_path: str | Path | None = None,
) -> Path:
    destination = (
        Path(manifest_path)
        if manifest_path is not None
        else contract_evidence_manifest_path(artifact_path)
    )
    payload = build_contract_evidence_manifest(
        frame,
        artifact_path=artifact_path,
        venue=venue,
        source_endpoint=source_endpoint,
        payload_scope=payload_scope,
        observation_time_source=observation_time_source,
        source_payload_kind=source_payload_kind,
        collection_complete=collection_complete,
        stop_reason=stop_reason,
        continuation_cursor=continuation_cursor,
        collection_errors=collection_errors,
        source_manifest_sha256=source_manifest_sha256,
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return destination


def verify_contract_evidence_manifest(
    frame: pd.DataFrame,
    *,
    artifact_path: str | Path,
    manifest_path: str | Path,
    expected_venue: str,
    expected_source_endpoint: str,
    expected_payload_scope: str,
    expected_observation_time_source: str,
    expected_source_payload_kind: str | None = None,
    expected_manifest_sha256: str | None = None,
) -> dict[str, Any]:
    manifest_file = Path(manifest_path)
    if expected_manifest_sha256 is not None:
        expected = _text(expected_manifest_sha256).lower()
        actual = file_sha256(manifest_file)
        if expected != actual:
            raise ValueError("contract evidence manifest sha256 mismatch")
    payload = json.loads(manifest_file.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("contract evidence manifest must be a JSON object")
    errors: list[str] = []
    manifest_version = payload.get("manifest_version")
    if manifest_version not in {
        CONTRACT_EVIDENCE_MANIFEST_VERSION_V1,
        CONTRACT_EVIDENCE_MANIFEST_VERSION,
    }:
        errors.append("manifest_version")
    if payload.get("schema_version") != CONTRACT_EVIDENCE_SCHEMA_VERSION:
        errors.append("schema_version")
    if payload.get("artifact_path") != Path(artifact_path).name:
        errors.append("artifact_path")
    venues = {_text(value) for value in frame["venue"].tolist()}
    endpoints = {_text(value) for value in frame["source_endpoint"].tolist()}
    scopes = {_text(value) for value in frame["payload_scope"].tolist()}
    if payload.get("venue") != _text(expected_venue) or (
        venues and venues != {_text(expected_venue)}
    ):
        errors.append("venue")
    if payload.get("source_endpoint") != _text(expected_source_endpoint) or (
        endpoints and endpoints != {_text(expected_source_endpoint)}
    ):
        errors.append("source_endpoint")
    if payload.get("payload_scope") != _text(expected_payload_scope) or (
        scopes and scopes != {_text(expected_payload_scope)}
    ):
        errors.append("payload_scope")
    if payload.get("observation_time_source") != _text(
        expected_observation_time_source
    ):
        errors.append("observation_time_source")
    if manifest_version == CONTRACT_EVIDENCE_MANIFEST_VERSION:
        if expected_source_payload_kind is not None and payload.get(
            "source_payload_kind"
        ) != _text(expected_source_payload_kind):
            errors.append("source_payload_kind")
        if not isinstance(payload.get("collection_complete"), bool):
            errors.append("collection_complete")
        if not _text(payload.get("stop_reason")):
            errors.append("stop_reason")
        collection_errors = payload.get("collection_errors")
        if not isinstance(collection_errors, list) or not all(
            isinstance(value, str) for value in collection_errors
        ):
            errors.append("collection_errors")
            collection_errors = []
        payload_hashes = sorted(
            {
                _text(value)
                for value in frame["raw_payload_hash"].tolist()
                if _text(value)
            }
        )
        if payload.get("payload_hashes_sha256") != _sha256_json(payload_hashes):
            errors.append("payload_hashes_sha256")
        source_manifest_hash = _text(payload.get("source_manifest_sha256"))
        source_manifest_valid = False
        if source_manifest_hash:
            source_manifest_path = Path(manifest_path).with_name(
                "source_collection_manifest.json"
            )
            if (
                not source_manifest_path.is_file()
                or file_sha256(source_manifest_path) != source_manifest_hash
            ):
                errors.append("source_manifest_sha256")
            else:
                try:
                    source_manifest_payload = json.loads(
                        source_manifest_path.read_text(encoding="utf-8")
                    )
                except (OSError, json.JSONDecodeError):
                    source_manifest_payload = None
                expected_source_state = {
                    "venue": payload.get("venue"),
                    "source_endpoint": payload.get("source_endpoint"),
                    "payload_scope": payload.get("payload_scope"),
                    "collection_complete": payload.get("collection_complete"),
                    "stop_reason": payload.get("stop_reason"),
                    "continuation_cursor": payload.get("continuation_cursor"),
                    "collection_errors": collection_errors,
                }
                source_manifest_valid = isinstance(
                    source_manifest_payload, Mapping
                ) and all(
                    source_manifest_payload.get(key) == expected
                    for key, expected in expected_source_state.items()
                )
                if not source_manifest_valid:
                    errors.append("source_manifest_state")
        expected_authoritative = bool(
            _collection_state_is_authoritative(
                collection_complete=payload.get("collection_complete"),
                stop_reason=payload.get("stop_reason"),
                continuation_cursor=payload.get("continuation_cursor"),
                collection_errors=collection_errors,
            )
            and source_manifest_hash
            and source_manifest_valid
            and payload.get("source_payload_kind") in {"raw_json", "venue_api_response"}
            and _frame_payload_authority_valid(
                frame, _text(payload.get("source_payload_kind"))
            )
        )
        if payload.get("authoritative_complete") is not expected_authoritative:
            errors.append("authoritative_complete")
    if int(payload.get("row_count", -1)) != len(frame):
        errors.append("row_count")
    if payload.get("artifact_sha256") != file_sha256(artifact_path):
        errors.append("artifact_sha256")
    evidence_ids = sorted({_text(value) for value in frame["evidence_id"].tolist()})
    if payload.get("evidence_ids_sha256") != _sha256_json(evidence_ids):
        errors.append("evidence_ids_sha256")
    observations = sorted(_text(value) for value in frame["observed_at_utc"].tolist())
    minimum = observations[0] if observations else None
    maximum = observations[-1] if observations else None
    if payload.get("observation_min_utc") != minimum:
        errors.append("observation_min_utc")
    if payload.get("observation_max_utc") != maximum:
        errors.append("observation_max_utc")
    if errors:
        raise ValueError(
            "contract evidence manifest verification failed: " + ", ".join(errors)
        )
    result = dict(payload)
    if manifest_version == CONTRACT_EVIDENCE_MANIFEST_VERSION_V1:
        result["authoritative_complete"] = False
    return result


def _sha256_json(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _frame_payload_authority_valid(frame: pd.DataFrame, payload_kind: str) -> bool:
    if frame.empty:
        return True
    for provenance in frame["field_provenance_json"].tolist():
        if isinstance(provenance, str):
            try:
                provenance = json.loads(provenance)
            except json.JSONDecodeError:
                return False
        if not isinstance(provenance, Mapping):
            return False
        source_payload = provenance.get("_source_payload")
        if not isinstance(source_payload, Mapping):
            return False
        if source_payload.get("authoritative") is not True:
            return False
        if _text(source_payload.get("kind")) != payload_kind:
            return False
    return True


def _collection_state_is_authoritative(
    *,
    collection_complete: object,
    stop_reason: object,
    continuation_cursor: object,
    collection_errors: list[str],
) -> bool:
    return bool(
        collection_complete is True
        and _text(stop_reason) in _AUTHORITATIVE_TERMINAL_STOP_REASONS
        and not _text(continuation_cursor)
        and not collection_errors
    )


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


__all__ = [
    "CONTRACT_EVIDENCE_MANIFEST_VERSION",
    "CONTRACT_EVIDENCE_MANIFEST_VERSION_V1",
    "build_contract_evidence_manifest",
    "build_contract_evidence_manifest_from_summary",
    "contract_evidence_bundle_path",
    "contract_evidence_manifest_path",
    "file_sha256",
    "verify_contract_evidence_manifest",
    "write_contract_evidence_bundle",
    "write_contract_evidence_manifest",
]
