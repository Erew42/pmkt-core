from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Mapping, Sequence, TypeAlias

RUN_STATE_FORMAT = "run_state.v1"
CAPTURE_COMMIT_JOURNAL_V1_FORMAT = "capture_commit_journal.v1"
CAPTURE_COMMIT_JOURNAL_V2_FORMAT = "capture_commit_journal.v2"
CAPTURE_COMMIT_JOURNAL_FORMAT = CAPTURE_COMMIT_JOURNAL_V2_FORMAT
LEGACY_UNKNOWN_COMMIT_CAUSE = "legacy_unknown"


class CaptureCommitCause(str, Enum):
    THRESHOLD_ROWS = "threshold_rows"
    THRESHOLD_TIME = "threshold_time"
    CHECKPOINT_STARTUP = "checkpoint_startup"
    CHECKPOINT_RESYNC = "checkpoint_resync"
    CHECKPOINT_PERIODIC = "checkpoint_periodic"
    RECOVERY_TOPBOOK = "recovery_topbook"
    INVALIDATION = "invalidation"
    TERMINATION = "termination"
    CLEAN_SHUTDOWN = "clean_shutdown"


COALESCIBLE_COMMIT_CAUSES = frozenset({CaptureCommitCause.INVALIDATION})


def _canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _require_text(mapping: Mapping[str, Any], key: str) -> str:
    value = mapping.get(key)
    if value is None or not str(value).strip():
        raise ValueError(f"{key} must be non-empty text")
    return str(value)


def _require_nonnegative_int(mapping: Mapping[str, Any], key: str) -> int:
    value = mapping.get(key)
    if value is None or isinstance(value, bool):
        raise ValueError(f"{key} must be a nonnegative integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be a nonnegative integer") from None
    if parsed < 0 or str(value).strip() not in {str(parsed), f"+{parsed}"}:
        raise ValueError(f"{key} must be a nonnegative integer")
    return parsed


def _require_sha256(mapping: Mapping[str, Any], key: str) -> str:
    value = _require_text(mapping, key)
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{key} must be lowercase sha256")
    return value


def _require_utc(mapping: Mapping[str, Any], key: str) -> str:
    value = _require_text(mapping, key)
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        raise ValueError(f"{key} must be an explicit UTC timestamp") from None
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{key} must be an explicit UTC timestamp")
    return value


def canonical_run_relative_path(value: Any, *, key: str = "path") -> str:
    text = str(value)
    normalized = text.replace("\\", "/")
    posix = PurePosixPath(normalized)
    windows = PureWindowsPath(text)
    if (
        not text.strip()
        or posix.is_absolute()
        or windows.is_absolute()
        or bool(windows.drive)
        or bool(windows.root)
        or any(part in {".", ".."} for part in posix.parts)
        or normalized != posix.as_posix()
    ):
        raise ValueError(
            f"{key} must be a canonical relative path within the run directory"
        )
    return normalized


def resolve_run_relative_path(
    run_dir: str | Path, value: Any, *, key: str = "path"
) -> Path:
    root = Path(run_dir).resolve()
    relative = canonical_run_relative_path(value, key=key)
    candidate = (root / relative).resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        raise ValueError(f"{key} escapes run directory: {value}") from None
    return candidate


@dataclass(frozen=True)
class RunStateV1:
    run_id: str
    profile_name: str
    profile_version: str
    expected_role_paths: Mapping[str, str]
    shard_plan: Mapping[str, Any]
    started_at_utc: str
    status: str = "recording"
    format: str = RUN_STATE_FORMAT
    storage_profile: Mapping[str, Any] | None = None
    adapter_settings_by_venue: Mapping[str, Mapping[str, Any]] | None = None
    capture_durability: Mapping[str, Any] | None = None
    capture_storage: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.format != RUN_STATE_FORMAT:
            raise ValueError(f"format must equal {RUN_STATE_FORMAT}")
        if self.status not in {"recording", "finalized"}:
            raise ValueError("run state status must be recording or finalized")
        if not self.expected_role_paths:
            raise ValueError("expected_role_paths must not be empty")
        if any(
            not str(role).strip() or not str(path).strip()
            for role, path in self.expected_role_paths.items()
        ):
            raise ValueError("expected_role_paths keys and values must be non-empty")
        normalized_paths = [
            canonical_run_relative_path(path, key=f"expected_role_paths.{role}")
            for role, path in self.expected_role_paths.items()
        ]
        if len(normalized_paths) != len(set(normalized_paths)):
            raise ValueError("expected_role_paths values must be unique")
        _require_utc({"started_at_utc": self.started_at_utc}, "started_at_utc")
        if self.storage_profile is not None:
            if not isinstance(self.storage_profile, Mapping):
                raise ValueError("storage_profile must be a JSON object")
            if _require_text(self.storage_profile, "name") != self.profile_name:
                raise ValueError("storage_profile name must match profile_name")
            if (
                _require_text(self.storage_profile, "profile_version")
                != self.profile_version
            ):
                raise ValueError("storage_profile version must match profile_version")
        if self.adapter_settings_by_venue is not None:
            if not isinstance(self.adapter_settings_by_venue, Mapping):
                raise ValueError("adapter_settings_by_venue must be a JSON object")
            if any(
                not str(venue).strip() or not isinstance(settings, Mapping)
                for venue, settings in self.adapter_settings_by_venue.items()
            ):
                raise ValueError(
                    "adapter_settings_by_venue must map venue names to JSON objects"
                )
        if self.capture_durability is not None and not isinstance(
            self.capture_durability, Mapping
        ):
            raise ValueError("capture_durability must be a JSON object")
        if self.capture_storage is not None and not isinstance(
            self.capture_storage, Mapping
        ):
            raise ValueError("capture_storage must be a JSON object")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "format": self.format,
            "run_id": self.run_id,
            "profile_name": self.profile_name,
            "profile_version": self.profile_version,
            "expected_role_paths": dict(sorted(self.expected_role_paths.items())),
            "shard_plan": dict(self.shard_plan),
            "started_at_utc": self.started_at_utc,
            "status": self.status,
            "storage_profile": dict(self.storage_profile)
            if self.storage_profile
            else None,
            "capture_durability": (
                dict(self.capture_durability)
                if self.capture_durability is not None
                else None
            ),
            "capture_storage": (
                dict(self.capture_storage)
                if self.capture_storage is not None
                else None
            ),
            "adapter_settings_by_venue": (
                {
                    str(venue): dict(settings)
                    for venue, settings in self.adapter_settings_by_venue.items()
                }
                if self.adapter_settings_by_venue is not None
                else None
            ),
        }

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "RunStateV1":
        _reject_unknown_keys(
            payload,
            {
                "format",
                "run_id",
                "profile_name",
                "profile_version",
                "expected_role_paths",
                "shard_plan",
                "started_at_utc",
                "status",
                "storage_profile",
                "adapter_settings_by_venue",
                "capture_durability",
                "capture_storage",
            },
        )
        paths = payload.get("expected_role_paths")
        shards = payload.get("shard_plan")
        if not isinstance(paths, Mapping) or not isinstance(shards, Mapping):
            raise ValueError("expected_role_paths and shard_plan must be JSON objects")
        profile = payload.get("storage_profile")
        if profile is not None and not isinstance(profile, Mapping):
            raise ValueError("storage_profile must be a JSON object or null")
        adapter_settings = payload.get("adapter_settings_by_venue")
        capture_durability = payload.get("capture_durability")
        capture_storage = payload.get("capture_storage")
        if capture_durability is not None and not isinstance(
            capture_durability, Mapping
        ):
            raise ValueError("capture_durability must be a JSON object or null")
        if capture_storage is not None and not isinstance(capture_storage, Mapping):
            raise ValueError("capture_storage must be a JSON object or null")
        if adapter_settings is not None and not isinstance(adapter_settings, Mapping):
            raise ValueError("adapter_settings_by_venue must be a JSON object or null")
        if isinstance(adapter_settings, Mapping) and any(
            not str(venue).strip() or not isinstance(settings, Mapping)
            for venue, settings in adapter_settings.items()
        ):
            raise ValueError(
                "adapter_settings_by_venue must map venue names to JSON objects"
            )

        return cls(
            format=_require_text(payload, "format"),
            run_id=_require_text(payload, "run_id"),
            profile_name=_require_text(payload, "profile_name"),
            profile_version=_require_text(payload, "profile_version"),
            expected_role_paths={str(key): str(value) for key, value in paths.items()},
            shard_plan=dict(shards),
            started_at_utc=_require_utc(payload, "started_at_utc"),
            status=_require_text(payload, "status"),
            storage_profile=dict(profile) if isinstance(profile, Mapping) else None,
            capture_durability=(
                dict(capture_durability)
                if isinstance(capture_durability, Mapping)
                else None
            ),
            capture_storage=(
                dict(capture_storage)
                if isinstance(capture_storage, Mapping)
                else None
            ),
            adapter_settings_by_venue=(
                {
                    str(venue): dict(settings)
                    for venue, settings in adapter_settings.items()
                }
                if isinstance(adapter_settings, Mapping)
                else None
            ),
        )


@dataclass(frozen=True)
class CaptureCommitArtifactV1:
    role: str
    path: str
    sha256: str
    row_count: int
    first_local_sequence: int
    last_local_sequence: int

    def __post_init__(self) -> None:
        if not self.role.strip() or not self.path.strip():
            raise ValueError("commit artifact role and path must be non-empty")
        canonical_run_relative_path(self.path, key="commit artifact path")
        _require_sha256({"sha256": self.sha256}, "sha256")
        for key, value in (
            ("row_count", self.row_count),
            ("first_local_sequence", self.first_local_sequence),
            ("last_local_sequence", self.last_local_sequence),
        ):
            _require_nonnegative_int({key: value}, key)
        if self.last_local_sequence < self.first_local_sequence:
            raise ValueError("last_local_sequence must be >= first_local_sequence")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "path": self.path,
            "sha256": self.sha256,
            "row_count": self.row_count,
            "first_local_sequence": self.first_local_sequence,
            "last_local_sequence": self.last_local_sequence,
        }

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "CaptureCommitArtifactV1":
        _reject_unknown_keys(
            payload,
            {
                "role",
                "path",
                "sha256",
                "row_count",
                "first_local_sequence",
                "last_local_sequence",
            },
        )
        return cls(
            role=_require_text(payload, "role"),
            path=_require_text(payload, "path"),
            sha256=_require_sha256(payload, "sha256"),
            row_count=_require_nonnegative_int(payload, "row_count"),
            first_local_sequence=_require_nonnegative_int(
                payload, "first_local_sequence"
            ),
            last_local_sequence=_require_nonnegative_int(
                payload, "last_local_sequence"
            ),
        )


@dataclass(frozen=True)
class CaptureCommitRecordV1:
    group_id: str
    committed_at_utc: str
    artifacts: tuple[CaptureCommitArtifactV1, ...]
    checksum_sha256: str
    format: str = CAPTURE_COMMIT_JOURNAL_V1_FORMAT

    def __post_init__(self) -> None:
        if self.format != CAPTURE_COMMIT_JOURNAL_V1_FORMAT:
            raise ValueError(f"format must equal {CAPTURE_COMMIT_JOURNAL_V1_FORMAT}")
        _require_sha256({"group_id": self.group_id}, "group_id")
        _require_utc({"committed_at_utc": self.committed_at_utc}, "committed_at_utc")
        if not self.artifacts:
            raise ValueError("commit record artifacts must not be empty")
        roles = [artifact.role for artifact in self.artifacts]
        paths = [artifact.path for artifact in self.artifacts]
        if len(roles) != len(set(roles)) or len(paths) != len(set(paths)):
            raise ValueError("commit record artifact roles and paths must be unique")
        _require_sha256({"checksum_sha256": self.checksum_sha256}, "checksum_sha256")
        if self.checksum_sha256 != self.expected_checksum():
            raise ValueError("commit record checksum does not match canonical payload")

    def payload_without_checksum(self) -> dict[str, Any]:
        return {
            "format": self.format,
            "group_id": self.group_id,
            "committed_at_utc": self.committed_at_utc,
            "artifacts": [artifact.to_mapping() for artifact in self.artifacts],
        }

    def expected_checksum(self) -> str:
        return hashlib.sha256(
            _canonical_json_bytes(self.payload_without_checksum())
        ).hexdigest()

    def to_mapping(self) -> dict[str, Any]:
        return {
            **self.payload_without_checksum(),
            "checksum_sha256": self.checksum_sha256,
        }

    @property
    def cause(self) -> str:
        return LEGACY_UNKNOWN_COMMIT_CAUSE

    @classmethod
    def create(
        cls,
        *,
        group_id: str,
        committed_at_utc: str,
        artifacts: Sequence[CaptureCommitArtifactV1],
    ) -> "CaptureCommitRecordV1":
        payload = {
            "format": CAPTURE_COMMIT_JOURNAL_V1_FORMAT,
            "group_id": group_id,
            "committed_at_utc": committed_at_utc,
            "artifacts": [artifact.to_mapping() for artifact in artifacts],
        }
        checksum = hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()
        return cls(
            group_id=group_id,
            committed_at_utc=committed_at_utc,
            artifacts=tuple(artifacts),
            checksum_sha256=checksum,
        )

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "CaptureCommitRecordV1":
        _reject_unknown_keys(
            payload,
            {"format", "group_id", "committed_at_utc", "artifacts", "checksum_sha256"},
        )
        raw_artifacts = payload.get("artifacts")
        if not isinstance(raw_artifacts, list) or not all(
            isinstance(item, Mapping) for item in raw_artifacts
        ):
            raise ValueError("artifacts must be a JSON array of objects")
        return cls(
            format=_require_text(payload, "format"),
            group_id=_require_sha256(payload, "group_id"),
            committed_at_utc=_require_utc(payload, "committed_at_utc"),
            artifacts=tuple(
                CaptureCommitArtifactV1.from_mapping(item) for item in raw_artifacts
            ),
            checksum_sha256=_require_sha256(payload, "checksum_sha256"),
        )


@dataclass(frozen=True)
class CaptureCommitRecordV2:
    group_id: str
    group_index: int
    cause: CaptureCommitCause | str
    accepted_at_utc: str
    committed_at_utc: str
    artifacts: tuple[CaptureCommitArtifactV1, ...]
    checksum_sha256: str
    format: str = CAPTURE_COMMIT_JOURNAL_V2_FORMAT

    def __post_init__(self) -> None:
        if self.format != CAPTURE_COMMIT_JOURNAL_V2_FORMAT:
            raise ValueError(f"format must equal {CAPTURE_COMMIT_JOURNAL_V2_FORMAT}")
        _require_sha256({"group_id": self.group_id}, "group_id")
        _require_nonnegative_int({"group_index": self.group_index}, "group_index")
        cause = CaptureCommitCause(self.cause)
        object.__setattr__(self, "cause", cause)
        accepted = _require_utc(
            {"accepted_at_utc": self.accepted_at_utc}, "accepted_at_utc"
        )
        committed = _require_utc(
            {"committed_at_utc": self.committed_at_utc}, "committed_at_utc"
        )
        if _parse_utc(committed) < _parse_utc(accepted):
            raise ValueError("committed_at_utc must not precede accepted_at_utc")
        if not self.artifacts:
            raise ValueError("commit record artifacts must not be empty")
        roles = [artifact.role for artifact in self.artifacts]
        paths = [artifact.path for artifact in self.artifacts]
        if len(roles) != len(set(roles)) or len(paths) != len(set(paths)):
            raise ValueError("commit record artifact roles and paths must be unique")
        _require_sha256({"checksum_sha256": self.checksum_sha256}, "checksum_sha256")
        if self.checksum_sha256 != self.expected_checksum():
            raise ValueError("commit record checksum does not match canonical payload")

    def payload_without_checksum(self) -> dict[str, Any]:
        return {
            "format": self.format,
            "group_id": self.group_id,
            "group_index": self.group_index,
            "cause": CaptureCommitCause(self.cause).value,
            "accepted_at_utc": self.accepted_at_utc,
            "committed_at_utc": self.committed_at_utc,
            "artifacts": [artifact.to_mapping() for artifact in self.artifacts],
        }

    def expected_checksum(self) -> str:
        return hashlib.sha256(
            _canonical_json_bytes(self.payload_without_checksum())
        ).hexdigest()

    def to_mapping(self) -> dict[str, Any]:
        return {
            **self.payload_without_checksum(),
            "checksum_sha256": self.checksum_sha256,
        }

    @classmethod
    def create(
        cls,
        *,
        group_id: str,
        group_index: int,
        cause: CaptureCommitCause | str,
        accepted_at_utc: str,
        committed_at_utc: str,
        artifacts: Sequence[CaptureCommitArtifactV1],
    ) -> "CaptureCommitRecordV2":
        normalized_cause = CaptureCommitCause(cause)
        payload = {
            "format": CAPTURE_COMMIT_JOURNAL_V2_FORMAT,
            "group_id": group_id,
            "group_index": group_index,
            "cause": normalized_cause.value,
            "accepted_at_utc": accepted_at_utc,
            "committed_at_utc": committed_at_utc,
            "artifacts": [artifact.to_mapping() for artifact in artifacts],
        }
        checksum = hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()
        return cls(
            group_id=group_id,
            group_index=group_index,
            cause=normalized_cause,
            accepted_at_utc=accepted_at_utc,
            committed_at_utc=committed_at_utc,
            artifacts=tuple(artifacts),
            checksum_sha256=checksum,
        )

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "CaptureCommitRecordV2":
        _reject_unknown_keys(
            payload,
            {
                "format",
                "group_id",
                "group_index",
                "cause",
                "accepted_at_utc",
                "committed_at_utc",
                "artifacts",
                "checksum_sha256",
            },
        )
        raw_artifacts = payload.get("artifacts")
        if not isinstance(raw_artifacts, list) or not all(
            isinstance(item, Mapping) for item in raw_artifacts
        ):
            raise ValueError("artifacts must be a JSON array of objects")
        return cls(
            format=_require_text(payload, "format"),
            group_id=_require_sha256(payload, "group_id"),
            group_index=_require_nonnegative_int(payload, "group_index"),
            cause=_require_text(payload, "cause"),
            accepted_at_utc=_require_utc(payload, "accepted_at_utc"),
            committed_at_utc=_require_utc(payload, "committed_at_utc"),
            artifacts=tuple(
                CaptureCommitArtifactV1.from_mapping(item) for item in raw_artifacts
            ),
            checksum_sha256=_require_sha256(payload, "checksum_sha256"),
        )


CaptureCommitRecord: TypeAlias = CaptureCommitRecordV1 | CaptureCommitRecordV2


def parse_capture_commit_record(
    payload: Mapping[str, Any],
) -> CaptureCommitRecord:
    format_name = _require_text(payload, "format")
    if format_name == CAPTURE_COMMIT_JOURNAL_V1_FORMAT:
        return CaptureCommitRecordV1.from_mapping(payload)
    if format_name == CAPTURE_COMMIT_JOURNAL_V2_FORMAT:
        return CaptureCommitRecordV2.from_mapping(payload)
    raise ValueError(f"unsupported capture commit journal format: {format_name}")


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(
        value[:-1] + "+00:00" if value.endswith("Z") else value
    )


def _reject_unknown_keys(payload: Mapping[str, Any], allowed: set[str]) -> None:
    extra = sorted(str(key) for key in payload if str(key) not in allowed)
    if extra:
        raise ValueError(f"unknown fields: {', '.join(extra)}")


__all__ = [
    "CAPTURE_COMMIT_JOURNAL_FORMAT",
    "CAPTURE_COMMIT_JOURNAL_V1_FORMAT",
    "CAPTURE_COMMIT_JOURNAL_V2_FORMAT",
    "COALESCIBLE_COMMIT_CAUSES",
    "LEGACY_UNKNOWN_COMMIT_CAUSE",
    "RUN_STATE_FORMAT",
    "CaptureCommitArtifactV1",
    "CaptureCommitCause",
    "CaptureCommitRecord",
    "CaptureCommitRecordV1",
    "CaptureCommitRecordV2",
    "RunStateV1",
    "canonical_run_relative_path",
    "parse_capture_commit_record",
    "resolve_run_relative_path",
]
