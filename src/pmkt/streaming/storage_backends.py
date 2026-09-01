from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

from pmkt.streaming.recovery_contracts import RunStateV1

CAPTURE_STORAGE_FORMAT = "capture_storage.v1"
SQLITE_CAPTURE_NAME = "capture.sqlite3"


class CaptureStorageBackend(str, Enum):
    PARQUET_SEGMENTS = "parquet_segments"
    SQLITE_WAL = "sqlite_wal_v1"


@dataclass(frozen=True)
class CaptureStorageSettings:
    backend: CaptureStorageBackend | str = CaptureStorageBackend.PARQUET_SEGMENTS
    authoritative_path: str | None = None
    promotion_mode: str = "none"
    format: str = CAPTURE_STORAGE_FORMAT

    def __post_init__(self) -> None:
        if self.format != CAPTURE_STORAGE_FORMAT:
            raise ValueError(f"format must equal {CAPTURE_STORAGE_FORMAT}")
        backend = CaptureStorageBackend(self.backend)
        object.__setattr__(self, "backend", backend)
        if backend is CaptureStorageBackend.PARQUET_SEGMENTS:
            if self.authoritative_path is not None:
                raise ValueError(
                    "parquet_segments storage must not declare an authoritative path"
                )
            if self.promotion_mode != "none":
                raise ValueError(
                    "parquet_segments storage promotion_mode must equal 'none'"
                )
            return
        if self.authoritative_path is None:
            raise ValueError("sqlite_wal_v1 storage requires authoritative_path")
        normalized = self.authoritative_path.replace("\\", "/")
        posix = PurePosixPath(normalized)
        windows = PureWindowsPath(self.authoritative_path)
        if (
            posix.is_absolute()
            or windows.is_absolute()
            or bool(windows.drive)
            or bool(windows.root)
            or any(part in {".", ".."} for part in posix.parts)
            or normalized != posix.as_posix()
        ):
            raise ValueError(
                "authoritative_path must be a canonical run-relative path"
            )
        object.__setattr__(self, "authoritative_path", normalized)
        if self.promotion_mode != "parquet_on_finalize":
            raise ValueError(
                "sqlite_wal_v1 storage promotion_mode must equal "
                "'parquet_on_finalize'"
            )

    @classmethod
    def for_backend(
        cls, backend: CaptureStorageBackend | str
    ) -> "CaptureStorageSettings":
        normalized = CaptureStorageBackend(backend)
        if normalized is CaptureStorageBackend.SQLITE_WAL:
            return cls(
                backend=normalized,
                authoritative_path=SQLITE_CAPTURE_NAME,
                promotion_mode="parquet_on_finalize",
            )
        return cls(backend=normalized)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "format": self.format,
            "backend": CaptureStorageBackend(self.backend).value,
            "authoritative_path": self.authoritative_path,
            "promotion_mode": self.promotion_mode,
        }

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "CaptureStorageSettings":
        allowed = {
            "format",
            "backend",
            "authoritative_path",
            "promotion_mode",
        }
        missing = sorted(allowed - {str(key) for key in payload})
        if missing:
            raise ValueError("missing capture storage fields: " + ", ".join(missing))
        extra = sorted(str(key) for key in payload if str(key) not in allowed)
        if extra:
            raise ValueError("unknown capture storage fields: " + ", ".join(extra))
        path = payload.get("authoritative_path")
        if path is not None and not isinstance(path, str):
            raise ValueError("capture storage authoritative_path must be text or null")
        return cls(
            format=str(payload["format"]),
            backend=str(payload["backend"]),
            authoritative_path=path,
            promotion_mode=str(payload["promotion_mode"]),
        )


@runtime_checkable
class CaptureCoordinator(Protocol):
    state: RunStateV1
    segment_row_limit: int

    def add(self, role: str, row: Mapping[str, Any]) -> None: ...

    def add_rows_bounded(
        self,
        role: str,
        rows: Any,
        *,
        max_rows_per_commit: int,
        cause: Any,
    ) -> int: ...

    @property
    def has_pending_rows(self) -> bool: ...

    def due_cause(self) -> Any: ...

    def barrier_due(self) -> bool: ...

    def commit(self, *, cause: Any = None, force: bool = False) -> Any: ...

    def finalize_segments(self) -> None: ...

    def mark_finalized(self) -> None: ...

    def finalize(self) -> None: ...

    @property
    def row_counts(self) -> dict[str, int]: ...

    @property
    def committed_roles(self) -> frozenset[str]: ...

    @property
    def segments_finalized(self) -> bool: ...

    def durability_manifest(self) -> dict[str, Any]: ...

    def storage_manifest(self) -> dict[str, Any]: ...

    def dataset_artifacts(self) -> dict[str, dict[str, Any]]: ...


def sample_summary(values: Sequence[int | float]) -> dict[str, int | float | None]:
    normalized = sorted(float(value) for value in values)
    if not normalized:
        return {
            "sample_count": 0,
            "total": 0,
            "minimum": None,
            "p50": None,
            "p95": None,
            "p99": None,
            "maximum": None,
        }

    def percentile(quantile: float) -> float:
        position = (len(normalized) - 1) * quantile
        lower = int(position)
        upper = min(lower + 1, len(normalized) - 1)
        weight = position - lower
        return normalized[lower] * (1.0 - weight) + normalized[upper] * weight

    return {
        "sample_count": len(normalized),
        "total": sum(normalized),
        "minimum": normalized[0],
        "p50": percentile(0.50),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "maximum": normalized[-1],
    }


__all__ = [
    "CAPTURE_STORAGE_FORMAT",
    "CaptureCoordinator",
    "CaptureStorageBackend",
    "CaptureStorageSettings",
    "SQLITE_CAPTURE_NAME",
    "sample_summary",
]
