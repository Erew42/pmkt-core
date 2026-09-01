from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def run_name() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%SZ")


def as_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def raw_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


class CaptureObservationClock:
    def __init__(self) -> None:
        self.last_capture_observed_at: datetime | None = None

    def parse(self, observed_at_utc: str) -> datetime:
        return datetime.fromisoformat(
            observed_at_utc[:-1] + "+00:00"
            if observed_at_utc.endswith("Z")
            else observed_at_utc
        ).astimezone(timezone.utc)

    def record(self, observed_at_utc: str) -> None:
        observed_at = self.parse(observed_at_utc)
        if (
            self.last_capture_observed_at is None
            or observed_at > self.last_capture_observed_at
        ):
            self.last_capture_observed_at = observed_at

    def reserve(self, candidate: datetime) -> datetime:
        resolved = candidate.astimezone(timezone.utc)
        if (
            self.last_capture_observed_at is not None
            and resolved <= self.last_capture_observed_at
        ):
            resolved = self.last_capture_observed_at + timedelta(microseconds=1)
        self.last_capture_observed_at = resolved
        return resolved
