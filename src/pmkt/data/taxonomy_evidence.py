from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from pmkt.data.registry import (
    MARKET_TAXONOMY_EVIDENCE_SCHEMA_VERSION,
    get_table_spec,
)
from pmkt.data.validation import convert_frame_strict


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_VENUES = {"polymarket", "kalshi"}


def _required_text(value: Any, name: str) -> str:
    text = "" if value is None else str(value).strip()
    if not text:
        raise ValueError(f"{name} must be nonempty")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _sha256(value: Any, name: str, *, nullable: bool = False) -> str | None:
    text = _optional_text(value)
    if text is None and nullable:
        return None
    if text is None or not _SHA256_RE.fullmatch(text):
        raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")
    return text


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def build_market_taxonomy_evidence_row(
    *,
    venue: str,
    market_key: str,
    event_key: str,
    requested_at_utc: str,
    observed_at_utc: str,
    source_endpoint: str,
    source_payload_sha256: str,
    native_category: str | None = None,
    native_tags: Sequence[str] = (),
    native_series: Sequence[str] = (),
    series_ticker: str | None = None,
    structured_sport: Mapping[str, Any] | None = None,
    game_id: str | None = None,
    sports_market_type: str | None = None,
    snapshot_raw_json_sha256: str | None = None,
    issues: Sequence[str] = (),
) -> dict[str, Any]:
    """Build one source-grounded market taxonomy evidence sidecar row.

    This records native metadata only. It deliberately does not assign a
    repository taxonomy or imply that two contracts are equivalent.
    """

    normalized_venue = _required_text(venue, "venue").casefold()
    if normalized_venue not in _VENUES:
        raise ValueError(f"unsupported venue {venue!r}")
    return {
        "schema_version": MARKET_TAXONOMY_EVIDENCE_SCHEMA_VERSION,
        "venue": normalized_venue,
        "market_key": _required_text(market_key, "market_key"),
        "event_key": _required_text(event_key, "event_key"),
        "native_category": _optional_text(native_category),
        "native_tags_json": _json(sorted({str(value).strip() for value in native_tags if str(value).strip()})),
        "native_series_json": _json(sorted({str(value).strip() for value in native_series if str(value).strip()})),
        "series_ticker": _optional_text(series_ticker),
        "structured_sport_json": _json(dict(structured_sport)) if structured_sport else None,
        "game_id": _optional_text(game_id),
        "sports_market_type": _optional_text(sports_market_type),
        "requested_at_utc": _required_text(requested_at_utc, "requested_at_utc"),
        "observed_at_utc": _required_text(observed_at_utc, "observed_at_utc"),
        "source_endpoint": _required_text(source_endpoint, "source_endpoint"),
        "source_payload_sha256": _sha256(source_payload_sha256, "source_payload_sha256"),
        "snapshot_raw_json_sha256": _sha256(
            snapshot_raw_json_sha256,
            "snapshot_raw_json_sha256",
            nullable=True,
        ),
        "issues_json": _json(sorted({str(value).strip() for value in issues if str(value).strip()})),
    }


def market_taxonomy_evidence_frame(rows: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    """Strictly convert a duplicate-free taxonomy evidence sidecar frame."""

    spec = get_table_spec(MARKET_TAXONOMY_EVIDENCE_SCHEMA_VERSION)
    frame = pd.DataFrame([dict(row) for row in rows])
    if frame.empty and not len(frame.columns):
        frame = pd.DataFrame(columns=spec.columns)
    try:
        frame = convert_frame_strict(
            frame,
            MARKET_TAXONOMY_EVIDENCE_SCHEMA_VERSION,
        )
    except ValueError as exc:
        raise ValueError(f"invalid market taxonomy evidence: {exc}") from exc
    return frame.sort_values(["venue", "market_key"], kind="mergesort").reset_index(drop=True)


__all__ = [
    "build_market_taxonomy_evidence_row",
    "market_taxonomy_evidence_frame",
]
