from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from math import isfinite
from typing import Any, Mapping, Sequence

from pmkt.streaming.recovery_contracts import CAPTURE_COMMIT_JOURNAL_V2_FORMAT

DEFAULT_SEGMENT_ROWS = 50_000
DEFAULT_SEGMENT_SECONDS = 30.0
MAX_SEGMENT_SECONDS = 300.0
DEFAULT_BARRIER_COALESCE_SECONDS = 1.0
MAX_BARRIER_COALESCE_SECONDS = 30.0
DEFAULT_PUBLICATION_DEADLINE_SECONDS = 15.0
DEFAULT_MAX_PENDING_PUBLISH_GROUPS = 8


class PublicationMode(str, Enum):
    INLINE = "inline"
    ASYNC = "async"


@dataclass(frozen=True)
class CaptureDurabilitySettings:
    publication_mode: PublicationMode | str = PublicationMode.INLINE
    barrier_coalesce_seconds: float = DEFAULT_BARRIER_COALESCE_SECONDS
    publication_deadline_seconds: float = DEFAULT_PUBLICATION_DEADLINE_SECONDS
    max_pending_publish_groups: int = DEFAULT_MAX_PENDING_PUBLISH_GROUPS
    requested_segment_rows: int | None = None
    effective_segment_rows: int = DEFAULT_SEGMENT_ROWS
    requested_segment_seconds: float | None = None
    effective_segment_seconds: float = DEFAULT_SEGMENT_SECONDS
    journal_version: str = CAPTURE_COMMIT_JOURNAL_V2_FORMAT
    segment_limit_adjustments: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        mode = PublicationMode(self.publication_mode)
        object.__setattr__(self, "publication_mode", mode)
        _require_finite_number(
            self.barrier_coalesce_seconds, "barrier_coalesce_seconds"
        )
        coalesce = float(self.barrier_coalesce_seconds)
        if not 0 <= coalesce <= MAX_BARRIER_COALESCE_SECONDS:
            raise ValueError(
                "barrier_coalesce_seconds must be between 0 and 30 inclusive"
            )
        object.__setattr__(self, "barrier_coalesce_seconds", coalesce)
        _require_finite_number(
            self.publication_deadline_seconds, "publication_deadline_seconds"
        )
        deadline = float(self.publication_deadline_seconds)
        if deadline != DEFAULT_PUBLICATION_DEADLINE_SECONDS:
            raise ValueError(
                "publication_deadline_seconds must equal the acceptance value 15"
            )
        object.__setattr__(self, "publication_deadline_seconds", deadline)
        object.__setattr__(
            self,
            "max_pending_publish_groups",
            _require_positive_int(
                self.max_pending_publish_groups, "max_pending_publish_groups"
            ),
        )
        object.__setattr__(
            self,
            "effective_segment_rows",
            _require_positive_int(
                self.effective_segment_rows, "effective_segment_rows"
            ),
        )
        if self.requested_segment_rows is not None:
            object.__setattr__(
                self,
                "requested_segment_rows",
                _require_positive_int(
                    self.requested_segment_rows, "requested_segment_rows"
                ),
            )
        if self.requested_segment_seconds is not None:
            _require_finite_number(
                self.requested_segment_seconds, "requested_segment_seconds"
            )
            requested_seconds = float(self.requested_segment_seconds)
            if requested_seconds <= 0:
                raise ValueError(
                    "requested_segment_seconds must be positive when provided"
                )
            object.__setattr__(self, "requested_segment_seconds", requested_seconds)
        _require_finite_number(
            self.effective_segment_seconds, "effective_segment_seconds"
        )
        effective_seconds = float(self.effective_segment_seconds)
        if not 0 < effective_seconds <= MAX_SEGMENT_SECONDS:
            raise ValueError(
                "effective_segment_seconds must be greater than 0 and at most 300"
            )
        object.__setattr__(self, "effective_segment_seconds", effective_seconds)
        if self.journal_version != CAPTURE_COMMIT_JOURNAL_V2_FORMAT:
            raise ValueError(
                "new capture durability settings require capture_commit_journal.v2"
            )
        adjustments = tuple(dict(item) for item in self.segment_limit_adjustments)
        for item in adjustments:
            if set(item) != {"field", "requested", "effective", "reason"}:
                raise ValueError(
                    "segment_limit_adjustments entries require exactly field, "
                    "requested, effective, and reason"
                )
            if not item.get("field") or not item.get("reason"):
                raise ValueError(
                    "segment_limit_adjustments require non-empty field and reason"
                )
            _require_finite_number(item["requested"], "adjustment.requested")
            _require_finite_number(item["effective"], "adjustment.effective")
        object.__setattr__(self, "segment_limit_adjustments", adjustments)

    @classmethod
    def resolve(
        cls,
        *,
        requested_segment_rows: int | None,
        requested_segment_seconds: float | None,
        publication_mode: PublicationMode | str = PublicationMode.INLINE,
        barrier_coalesce_seconds: float = DEFAULT_BARRIER_COALESCE_SECONDS,
        publication_deadline_seconds: float = DEFAULT_PUBLICATION_DEADLINE_SECONDS,
        max_pending_publish_groups: int = DEFAULT_MAX_PENDING_PUBLISH_GROUPS,
    ) -> "CaptureDurabilitySettings":
        effective_rows = requested_segment_rows or DEFAULT_SEGMENT_ROWS
        requested_seconds = (
            float(requested_segment_seconds)
            if requested_segment_seconds is not None
            else None
        )
        effective_seconds = min(
            requested_seconds or DEFAULT_SEGMENT_SECONDS,
            MAX_SEGMENT_SECONDS,
        )
        adjustments: list[dict[str, Any]] = []
        if requested_seconds is not None and requested_seconds != effective_seconds:
            adjustments.append(
                {
                    "field": "segment_seconds",
                    "requested": requested_seconds,
                    "effective": effective_seconds,
                    "reason": "bounded_maximum_uncommitted_interval",
                }
            )
        return cls(
            publication_mode=publication_mode,
            barrier_coalesce_seconds=barrier_coalesce_seconds,
            publication_deadline_seconds=publication_deadline_seconds,
            max_pending_publish_groups=max_pending_publish_groups,
            requested_segment_rows=requested_segment_rows,
            effective_segment_rows=effective_rows,
            requested_segment_seconds=requested_segment_seconds,
            effective_segment_seconds=effective_seconds,
            segment_limit_adjustments=tuple(adjustments),
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "publication_mode": PublicationMode(self.publication_mode).value,
            "barrier_coalesce_seconds": self.barrier_coalesce_seconds,
            "publication_deadline_seconds": self.publication_deadline_seconds,
            "max_pending_publish_groups": self.max_pending_publish_groups,
            "requested_segment_rows": self.requested_segment_rows,
            "effective_segment_rows": self.effective_segment_rows,
            "requested_segment_seconds": self.requested_segment_seconds,
            "effective_segment_seconds": self.effective_segment_seconds,
            "journal_version": self.journal_version,
            "segment_limit_adjustments": [
                dict(item) for item in self.segment_limit_adjustments
            ],
        }

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "CaptureDurabilitySettings":
        allowed = {
            "publication_mode",
            "barrier_coalesce_seconds",
            "publication_deadline_seconds",
            "max_pending_publish_groups",
            "requested_segment_rows",
            "effective_segment_rows",
            "requested_segment_seconds",
            "effective_segment_seconds",
            "journal_version",
            "segment_limit_adjustments",
        }
        missing = sorted(allowed - {str(key) for key in payload})
        if missing:
            raise ValueError("missing capture durability fields: " + ", ".join(missing))
        extra = sorted(str(key) for key in payload if str(key) not in allowed)
        if extra:
            raise ValueError("unknown capture durability fields: " + ", ".join(extra))
        raw_adjustments = payload.get("segment_limit_adjustments", ())
        if not isinstance(raw_adjustments, Sequence) or isinstance(
            raw_adjustments, (str, bytes)
        ):
            raise ValueError("segment_limit_adjustments must be a JSON array")
        if any(not isinstance(item, Mapping) for item in raw_adjustments):
            raise ValueError("segment_limit_adjustments must contain JSON objects")
        return cls(
            publication_mode=payload["publication_mode"],
            barrier_coalesce_seconds=payload["barrier_coalesce_seconds"],
            publication_deadline_seconds=payload["publication_deadline_seconds"],
            max_pending_publish_groups=payload["max_pending_publish_groups"],
            requested_segment_rows=payload.get("requested_segment_rows"),
            effective_segment_rows=payload["effective_segment_rows"],
            requested_segment_seconds=payload.get("requested_segment_seconds"),
            effective_segment_seconds=payload["effective_segment_seconds"],
            journal_version=payload["journal_version"],
            segment_limit_adjustments=tuple(dict(item) for item in raw_adjustments),
        )


def _require_positive_int(value: Any, field: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _require_finite_number(value: Any, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a finite number")
    if not isfinite(float(value)):
        raise ValueError(f"{field} must be a finite number")


__all__ = [
    "CaptureDurabilitySettings",
    "PublicationMode",
]
