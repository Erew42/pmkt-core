from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from pmkt.data.registry import (
    EVENT_TAXONOMY_PREDICTION_SCHEMA_VERSION,
    get_table_spec,
)
from pmkt.data.validation import coerce_frame, validate_frame


HYBRID_TAXONOMY_DOMAINS = frozenset(
    {
        "politics_government",
        "sports",
        "economics_financial_markets",
        "corporate_business",
        "science_technology_health",
        "weather_climate_environment",
        "culture_entertainment",
        "geopolitics_security",
    }
)
ACCEPTED_TAXONOMY_STATUSES = frozenset({"accepted_model", "accepted_structural"})
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


def _sha256(value: Any, name: str) -> str:
    text = _required_text(value, name)
    if not _SHA256_RE.fullmatch(text):
        raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")
    return text


def _probability(value: Any, name: str) -> float:
    number = float(value)
    if not math.isfinite(number) or not 0.0 <= number <= 1.0:
        raise ValueError(f"{name} must be finite and between 0 and 1")
    return number


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def build_event_taxonomy_prediction_row(
    *,
    venue: str,
    event_key: str,
    primary_domain: str | None,
    model_primary_domain: str,
    domain_confidence: float,
    domain_margin: float,
    domain_scores: Mapping[str, float],
    prediction_status: str,
    model_sha256: str,
    evidence_sha256: str,
    classified_at_utc: str,
    family_shadow: str | None = None,
    family_confidence: float | None = None,
    issues: Sequence[str] = (),
) -> dict[str, Any]:
    """Build a provenance-bound event taxonomy prediction sidecar row."""

    normalized_venue = _required_text(venue, "venue").casefold()
    if normalized_venue not in _VENUES:
        raise ValueError(f"unsupported venue {venue!r}")
    normalized_status = _required_text(prediction_status, "prediction_status")
    selected = _optional_text(primary_domain)
    model_selected = _required_text(model_primary_domain, "model_primary_domain")
    if model_selected not in HYBRID_TAXONOMY_DOMAINS:
        raise ValueError(f"unsupported model_primary_domain {model_selected!r}")
    if selected is not None and selected not in HYBRID_TAXONOMY_DOMAINS:
        raise ValueError(f"unsupported primary_domain {selected!r}")
    accepted = normalized_status in ACCEPTED_TAXONOMY_STATUSES
    if accepted != (selected is not None):
        raise ValueError("accepted statuses require primary_domain; abstentions require null primary_domain")
    if normalized_status == "accepted_model" and selected != model_selected:
        raise ValueError("accepted_model primary_domain must match model_primary_domain")
    if normalized_status == "accepted_structural" and selected != "sports":
        raise ValueError("accepted_structural currently requires primary_domain='sports'")
    if not accepted and not normalized_status.startswith("abstain_"):
        raise ValueError("non-accepted prediction_status must be an explicit abstain_* reason")

    normalized_scores = {str(key): _probability(value, f"domain_scores[{key!r}]") for key, value in domain_scores.items()}
    if set(normalized_scores) != HYBRID_TAXONOMY_DOMAINS:
        raise ValueError("domain_scores must contain exactly the frozen domain set")
    if not math.isclose(sum(normalized_scores.values()), 1.0, rel_tol=0.0, abs_tol=1e-6):
        raise ValueError("domain_scores must sum to 1")
    confidence = _probability(domain_confidence, "domain_confidence")
    margin = _probability(domain_margin, "domain_margin")
    if not math.isclose(confidence, normalized_scores[model_selected], rel_tol=0.0, abs_tol=1e-9):
        raise ValueError("domain_confidence must match model_primary_domain score")

    return {
        "schema_version": EVENT_TAXONOMY_PREDICTION_SCHEMA_VERSION,
        "venue": normalized_venue,
        "event_key": _required_text(event_key, "event_key"),
        "primary_domain": selected,
        "model_primary_domain": model_selected,
        "domain_confidence": confidence,
        "domain_margin": margin,
        "domain_scores_json": _json(normalized_scores),
        "prediction_status": normalized_status,
        "family_shadow": _optional_text(family_shadow),
        "family_confidence": None if family_confidence is None else _probability(family_confidence, "family_confidence"),
        "model_sha256": _sha256(model_sha256, "model_sha256"),
        "evidence_sha256": _sha256(evidence_sha256, "evidence_sha256"),
        "classified_at_utc": _required_text(classified_at_utc, "classified_at_utc"),
        "issues_json": _json(sorted({str(value).strip() for value in issues if str(value).strip()})),
    }


def event_taxonomy_prediction_frame(rows: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    """Coerce and strictly validate a duplicate-free event prediction sidecar."""

    spec = get_table_spec(EVENT_TAXONOMY_PREDICTION_SCHEMA_VERSION)
    built = [build_event_taxonomy_prediction_row(
        venue=str(row.get("venue") or ""),
        event_key=str(row.get("event_key") or ""),
        primary_domain=row.get("primary_domain"),
        model_primary_domain=str(row.get("model_primary_domain") or ""),
        domain_confidence=float(row.get("domain_confidence", float("nan"))),
        domain_margin=float(row.get("domain_margin", float("nan"))),
        domain_scores=(json.loads(str(row.get("domain_scores_json"))) if isinstance(row.get("domain_scores_json"), str) else dict(row.get("domain_scores") or {})),
        prediction_status=str(row.get("prediction_status") or ""),
        family_shadow=row.get("family_shadow"),
        family_confidence=row.get("family_confidence"),
        model_sha256=str(row.get("model_sha256") or ""),
        evidence_sha256=str(row.get("evidence_sha256") or ""),
        classified_at_utc=str(row.get("classified_at_utc") or ""),
        issues=(json.loads(str(row.get("issues_json"))) if isinstance(row.get("issues_json"), str) else tuple(row.get("issues") or ())),
    ) for row in rows]
    frame = pd.DataFrame(built, columns=spec.columns)
    frame = coerce_frame(frame, EVENT_TAXONOMY_PREDICTION_SCHEMA_VERSION)
    report = validate_frame(frame, EVENT_TAXONOMY_PREDICTION_SCHEMA_VERSION, strict=True)
    if not report.ok:
        raise ValueError("invalid event taxonomy predictions: " + "; ".join(report.errors))
    return frame.sort_values(["venue", "event_key"], kind="mergesort").reset_index(drop=True)


__all__ = [
    "ACCEPTED_TAXONOMY_STATUSES",
    "HYBRID_TAXONOMY_DOMAINS",
    "build_event_taxonomy_prediction_row",
    "event_taxonomy_prediction_frame",
]
