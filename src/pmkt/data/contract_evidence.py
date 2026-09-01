from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Literal

import pandas as pd

from pmkt.data.canonical import contract_evidence_row
from pmkt.data.registry import (
    CONTRACT_EVIDENCE_COLUMNS,
    CONTRACT_EVIDENCE_SCHEMA_VERSION,
)

Venue = Literal["polymarket", "kalshi"]
SourcePayloadKind = Literal["raw_json", "venue_api_response", "normalized_snapshot"]


def contract_evidence_dataframe(
    markets: Sequence[Mapping[str, Any]],
    *,
    venue: Venue,
    source_endpoint: str,
    payload_scope: str,
    observed_at_utc: str | None = None,
    derived_at_utc: str | None = None,
    source_payload_kind: SourcePayloadKind | None = None,
) -> pd.DataFrame:
    """Project venue market payloads into registered contract evidence rows.

    The function is deliberately pure and never performs venue requests. A raw
    payload embedded in ``raw_json`` is verified before it is used; a normalized
    snapshot without raw JSON remains an explicitly incomplete projection.
    """
    if venue not in {"polymarket", "kalshi"}:
        raise ValueError(f"unsupported contract evidence venue: {venue!r}")
    endpoint = str(source_endpoint).strip()
    scope = str(payload_scope).strip()
    if not endpoint or not scope:
        raise ValueError("source_endpoint and payload_scope are required")
    derived = derived_at_utc or datetime.now(timezone.utc).isoformat()
    if not _explicit_utc(derived):
        raise ValueError("derived_at_utc must be an explicit UTC timestamp")
    rows = [
        _project_market(
            market,
            venue=venue,
            source_endpoint=endpoint,
            payload_scope=scope,
            observed_at_utc=observed_at_utc,
            derived_at_utc=derived,
            source_payload_kind=source_payload_kind,
        )
        for market in markets
    ]
    frame = pd.DataFrame(rows, columns=CONTRACT_EVIDENCE_COLUMNS)
    if not frame.empty:
        frame = frame.sort_values("evidence_id").reset_index(drop=True)
    return frame


def stable_source_row_hash(row: Mapping[str, Any]) -> str:
    """Hash a normalized source row without raw or transient/internal fields."""
    payload = {
        str(key): _json_safe(value)
        for key, value in row.items()
        if str(key)
        not in {
            "raw_json",
            "raw_json_sha256",
            "raw_payload_hash",
            "raw_json_hash",
            "raw_sha256",
            "source_row_hash",
            "evidence_projection_hash",
            "evidence_id",
            "schema_version",
            "source_endpoint",
            "payload_scope",
            "observed_at_utc",
            "derived_at_utc",
        }
        and not str(key).startswith("_")
    }
    return _sha256_json(payload)


def stable_evidence_projection_hash(projection: Mapping[str, Any]) -> str:
    """Hash only versioned semantic evidence, excluding observation provenance."""
    json_fields = {
        "instrument_mapping_json",
        "contract_fields_json",
        "field_provenance_json",
        "completeness_reasons_json",
    }
    fields = {
        key: (
            _semantic_json_value(projection.get(key))
            if key in json_fields
            else projection.get(key)
        )
        for key in (
            "venue",
            "market_key",
            "venue_event_key",
            "question",
            "title",
            "rules_text",
            "resolution_source",
            "close_time",
            "status",
            "instrument_mapping_json",
            "contract_fields_json",
            "field_provenance_json",
            "identity_complete",
            "rules_complete",
            "instrument_mapping_complete",
            "completeness_reasons_json",
        )
    }
    return _sha256_json({"schema_version": CONTRACT_EVIDENCE_SCHEMA_VERSION, **fields})


def _semantic_json_value(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return value
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


def stable_contract_evidence_id(evidence: Mapping[str, Any]) -> str:
    """Hash one explicit source observation identity."""
    source_hash = _text(evidence.get("raw_payload_hash")) or _text(
        evidence.get("source_row_hash")
    )
    return _sha256_json(
        {
            "venue": _text(evidence.get("venue")),
            "market_key": _text(evidence.get("market_key")),
            "source_endpoint": _text(evidence.get("source_endpoint")),
            "payload_scope": _text(evidence.get("payload_scope")),
            "observed_at_utc": _text(evidence.get("observed_at_utc")),
            "source_hash": source_hash,
        }
    )


def _project_market(
    source: Mapping[str, Any],
    *,
    venue: Venue,
    source_endpoint: str,
    payload_scope: str,
    observed_at_utc: str | None,
    derived_at_utc: str,
    source_payload_kind: SourcePayloadKind | None,
) -> dict[str, Any]:
    source_row = {str(key): value for key, value in source.items()}
    payload, raw_payload_hash, has_authoritative_raw, resolved_payload_kind = (
        _source_payload(source_row, source_payload_kind=source_payload_kind)
    )
    normalized = _normalized_source_row(payload, venue=venue)
    source_row_hash = stable_source_row_hash(normalized)
    if venue == "polymarket":
        semantic = _polymarket_semantics(payload, normalized)
    else:
        semantic = _kalshi_semantics(payload, normalized)
    market_key = _text(semantic.get("market_key"))
    if not market_key:
        raise ValueError(f"{venue} contract evidence requires a market key")
    observed = _text(observed_at_utc) or _text(source_row.get("observed_at_utc"))
    if not observed:
        raise ValueError("contract evidence requires observed_at_utc")
    if not _explicit_utc(observed):
        raise ValueError("observed_at_utc must be an explicit UTC timestamp")

    identity_complete = bool(market_key and _text(semantic.get("venue_event_key")))
    rules_complete = bool(
        _text(semantic.get("rules_text")) and semantic.get("rules_authoritative")
    )
    instrument_mapping = semantic.get("instrument_mapping_json") or []
    instrument_mapping_complete = bool(semantic.get("instrument_mapping_complete"))
    reasons: list[str] = []
    if not identity_complete:
        reasons.append("identity_incomplete")
        if not _text(semantic.get("venue_event_key")):
            reasons.append("venue_event_identity_incomplete")
    if not rules_complete:
        reasons.append("rules_incomplete")
    if not instrument_mapping_complete:
        reasons.append("instrument_mapping_incomplete")
    if not has_authoritative_raw:
        reasons.append("raw_payload_unavailable")

    field_provenance = dict(semantic.get("field_provenance_json") or {})
    field_provenance["_source_payload"] = {
        "kind": resolved_payload_kind,
        "authoritative": has_authoritative_raw,
    }
    projection: dict[str, Any] = {
        "venue": venue,
        "market_key": market_key,
        "venue_event_key": _text(semantic.get("venue_event_key")),
        "question": _text(semantic.get("question")),
        "title": _text(semantic.get("title")),
        "rules_text": _text(semantic.get("rules_text")),
        "resolution_source": _text(semantic.get("resolution_source")),
        "close_time": _text(semantic.get("close_time")),
        "status": _text(semantic.get("status")),
        "instrument_mapping_json": instrument_mapping,
        "contract_fields_json": semantic.get("contract_fields_json") or {},
        "field_provenance_json": field_provenance,
        "identity_complete": identity_complete,
        "rules_complete": rules_complete,
        "instrument_mapping_complete": instrument_mapping_complete,
        "completeness_reasons_json": reasons,
    }
    projection_hash = stable_evidence_projection_hash(projection)
    observation = {
        **projection,
        "source_endpoint": source_endpoint,
        "payload_scope": payload_scope,
        "observed_at_utc": observed,
        "derived_at_utc": derived_at_utc,
        "source_row_hash": source_row_hash,
        "raw_payload_hash": raw_payload_hash,
        "evidence_projection_hash": projection_hash,
    }
    return contract_evidence_row(
        evidence_id=stable_contract_evidence_id(observation),
        **observation,
    )


def _source_payload(
    source: Mapping[str, Any],
    *,
    source_payload_kind: SourcePayloadKind | None,
) -> tuple[dict[str, Any], str | None, bool, str]:
    requested_kind = _text(source_payload_kind) or ""
    if requested_kind and requested_kind not in {
        "raw_json",
        "venue_api_response",
        "normalized_snapshot",
    }:
        raise ValueError(f"unsupported source_payload_kind: {requested_kind!r}")
    raw_json = source.get("raw_json")
    if _text(raw_json):
        if requested_kind not in {"", "raw_json"}:
            raise ValueError("raw_json payload requires source_payload_kind='raw_json'")
        raw_text = str(raw_json)
        actual = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
        expected = _text(source.get("raw_json_sha256"))
        if expected and expected != actual:
            raise ValueError("raw_json_sha256 does not match raw_json")
        try:
            decoded = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise ValueError("raw_json is not valid JSON") from exc
        if not isinstance(decoded, dict):
            raise ValueError("raw_json must decode to an object")
        return decoded, _sha256_json(decoded), True, "raw_json"
    if requested_kind == "raw_json":
        raise ValueError("source_payload_kind='raw_json' requires raw_json")
    schema_version = _text(source.get("schema_version"))
    if requested_kind == "normalized_snapshot" or (
        schema_version and "market_snapshot" in schema_version
    ):
        return dict(source), None, False, "normalized_snapshot"
    payload = {str(key): _json_safe(value) for key, value in source.items()}
    return (
        payload,
        _sha256_json(payload),
        requested_kind == "venue_api_response",
        requested_kind or "unknown",
    )


def _normalized_source_row(payload: dict[str, Any], *, venue: Venue) -> dict[str, Any]:
    if venue == "polymarket":
        from pmkt.data.normalize import extract_market_rows

        rows = extract_market_rows([payload])
        if rows:
            return rows[0]
    else:
        from pmkt.data.normalize_kalshi import normalize_kalshi_market

        return normalize_kalshi_market(payload)
    return {str(key): _json_safe(value) for key, value in payload.items()}


def _polymarket_semantics(
    payload: Mapping[str, Any], normalized: Mapping[str, Any]
) -> dict[str, Any]:
    event = _first_event(payload)
    question, question_path = _first_text_with_path(payload, ("question", "title"))
    if not question:
        question = _text(normalized.get("question"))
        question_path = "normalized.question" if question else None
    title, title_path = _first_text_with_path(payload, ("title",))
    if not title:
        title, event_title_path = _first_text_with_path(event, ("title",))
        title_path = f"event.{event_title_path}" if event_title_path else None
    description, description_path = _first_text_with_path(payload, ("description",))
    resolution_source, resolution_path = _first_text_with_path(
        payload, ("resolutionSource", "resolution_source")
    )
    if not resolution_source:
        resolution_source, event_resolution_path = _first_text_with_path(
            event, ("resolutionSource", "resolution_source")
        )
        resolution_path = (
            f"event.{event_resolution_path}" if event_resolution_path else None
        )
    outcomes = _sequence(
        payload.get("outcomes")
        if payload.get("outcomes") is not None
        else payload.get("outcome_labels_json")
        if payload.get("outcome_labels_json") is not None
        else payload.get("outcome_labels")
        if payload.get("outcome_labels") is not None
        else normalized.get("outcome_labels_json")
    )
    tokens = _sequence(
        payload.get("clobTokenIds")
        if payload.get("clobTokenIds") is not None
        else payload.get("clob_token_ids")
        if payload.get("clob_token_ids") is not None
        else payload.get("token_ids")
        if payload.get("token_ids") is not None
        else normalized.get("token_ids")
    )
    normalized_tokens = [str(token).strip() for token in tokens]
    normalized_outcomes = [str(outcome).strip() for outcome in outcomes]
    mapping_complete = bool(
        normalized_outcomes
        and len(normalized_outcomes) == len(normalized_tokens)
        and all(normalized_tokens)
        and all(normalized_outcomes)
        and len(set(normalized_tokens)) == len(normalized_tokens)
        and len({value.casefold() for value in normalized_outcomes})
        == len(normalized_outcomes)
    )
    mapping = (
        [
            {"instrument_key": token, "outcome": outcome}
            for token, outcome in zip(
                normalized_tokens, normalized_outcomes, strict=True
            )
        ]
        if mapping_complete
        else []
    )
    market_key = _first_text(payload, ("id", "market_id", "market_key")) or _text(
        normalized.get("market_id")
    )
    event_key = (
        _first_text(payload, ("event_id", "eventId"))
        or _first_text(event, ("id", "event_id", "eventId", "slug"))
        or _text(normalized.get("event_id"))
    )
    close_time = _first_text(payload, ("endDate", "end_date", "close_time")) or _text(
        normalized.get("close_time")
    )
    status = _first_text(payload, ("status",))
    if not status and payload.get("closed") is not None:
        status = "closed" if bool(payload.get("closed")) else "open"
    fields = _compact(
        {
            "market_key": market_key,
            "event_id": event_key,
            "condition_id": _first_text(payload, ("conditionId", "condition_id"))
            or _text(normalized.get("condition_id")),
            "question": question,
            "title": title,
            "description": description,
            "scan_category": _first_text(
                payload, ("scan_category", "scanCategory")
            )
            or _text(normalized.get("scan_category")),
            "category": _first_text(payload, ("category",))
            or _text(normalized.get("category")),
            "market_family": _first_text(
                payload, ("market_family", "marketFamily")
            )
            or _text(normalized.get("market_family")),
            "sport": _first_text(payload, ("sport",))
            or _text(normalized.get("sport")),
            "rules": description,
            "resolution_source": resolution_source,
            "close_time": close_time,
            "status": status,
            "token_ids": [str(value) for value in tokens],
            "outcome_labels_json": [str(value) for value in outcomes],
        }
    )
    provenance = _compact(
        {
            "market_key": [
                _selected_key(payload, ("id", "market_id", "market_key"))
                or "normalized.market_id"
            ],
            "venue_event_key": [_polymarket_event_path(payload, event)],
            "question": [question_path] if question_path else None,
            "title": [title_path] if title_path else None,
            "scan_category": (
                [
                    _selected_key(payload, ("scan_category", "scanCategory"))
                    or "normalized.scan_category"
                ]
                if fields.get("scan_category")
                else None
            ),
            "category": (
                [_selected_key(payload, ("category",)) or "normalized.category"]
                if fields.get("category")
                else None
            ),
            "market_family": (
                [
                    _selected_key(payload, ("market_family", "marketFamily"))
                    or "normalized.market_family"
                ]
                if fields.get("market_family")
                else None
            ),
            "sport": (
                [_selected_key(payload, ("sport",)) or "normalized.sport"]
                if fields.get("sport")
                else None
            ),
            "rules_text": [description_path] if description_path else None,
            "resolution_source": [resolution_path] if resolution_path else None,
            "instrument_mapping_json": (
                ["clobTokenIds", "outcomes"] if mapping_complete else None
            ),
        }
    )
    return {
        "market_key": market_key,
        "venue_event_key": event_key,
        "question": question,
        "title": title,
        "rules_text": description,
        "rules_authoritative": bool(description),
        "resolution_source": resolution_source,
        "close_time": close_time,
        "status": status,
        "instrument_mapping_json": mapping,
        "instrument_mapping_complete": mapping_complete,
        "contract_fields_json": fields,
        "field_provenance_json": provenance,
    }


def _kalshi_semantics(
    payload: Mapping[str, Any], normalized: Mapping[str, Any]
) -> dict[str, Any]:
    market_key = _first_text(payload, ("ticker", "market_key")) or _text(
        normalized.get("market_key")
    )
    question = _first_text(payload, ("question", "title", "subtitle")) or _text(
        normalized.get("question")
    )
    title = _first_text(payload, ("title",)) or _text(normalized.get("title"))
    description = _first_text(payload, ("description",)) or _text(
        normalized.get("description")
    )
    primary, primary_path = _first_text_with_path(
        payload, ("rules_primary", "rulesPrimary", "rules", "market_rules")
    )
    if not primary:
        primary = _text(normalized.get("rules_primary"))
        primary_path = "normalized.rules_primary" if primary else None
    secondary, secondary_path = _first_text_with_path(
        payload, ("rules_secondary", "rulesSecondary")
    )
    if not secondary:
        secondary = _text(normalized.get("rules_secondary"))
        secondary_path = "normalized.rules_secondary" if secondary else None
    rules_text = "\n".join(value for value in (primary, secondary) if value)
    event_key = _first_text(payload, ("event_ticker", "eventTicker")) or _text(
        normalized.get("event_ticker")
    )
    close_time = _first_text(payload, ("close_time", "expiration_time")) or _text(
        normalized.get("close_time")
    )
    status = _first_text(payload, ("status",)) or _text(normalized.get("status"))
    mapping = (
        [
            {"instrument_key": f"{market_key}:YES", "outcome": "YES"},
            {"instrument_key": f"{market_key}:NO", "outcome": "NO"},
        ]
        if market_key
        else []
    )
    fields = _compact(
        {
            "market_key": market_key,
            "event_ticker": event_key,
            "instrument_key": f"{market_key}:YES" if market_key else None,
            "question": question,
            "title": title,
            "subtitle": _first_text(payload, ("subtitle",))
            or _text(normalized.get("subtitle")),
            "category": _first_text(payload, ("category",))
            or _text(normalized.get("category")),
            "scan_category": _first_text(
                payload, ("scan_category", "scanCategory")
            )
            or _text(normalized.get("scan_category")),
            "market_family": _first_text(
                payload, ("market_family", "marketFamily")
            )
            or _text(normalized.get("market_family")),
            "sport": _first_text(payload, ("sport",))
            or _text(normalized.get("sport")),
            "description": description,
            "rules": rules_text,
            "rules_primary": primary,
            "rules_secondary": secondary,
            "resolution_source": _first_text(
                payload, ("resolution_source", "resolutionSource")
            ),
            "close_time": close_time,
            "status": status,
        }
    )
    rules_paths = [path for path in (primary_path, secondary_path) if path]
    provenance = _compact(
        {
            "market_key": [
                _selected_key(payload, ("ticker", "market_key"))
                or "normalized.market_key"
            ],
            "venue_event_key": [
                _selected_key(payload, ("event_ticker", "eventTicker"))
                or "normalized.event_ticker"
            ],
            "question": [
                _selected_key(payload, ("question", "title", "subtitle"))
                or "normalized.question"
            ],
            "title": (
                [_selected_key(payload, ("title",)) or "normalized.title"]
                if title
                else None
            ),
            "scan_category": (
                [
                    _selected_key(payload, ("scan_category", "scanCategory"))
                    or "normalized.scan_category"
                ]
                if fields.get("scan_category")
                else None
            ),
            "category": (
                [_selected_key(payload, ("category",)) or "normalized.category"]
                if fields.get("category")
                else None
            ),
            "market_family": (
                [
                    _selected_key(payload, ("market_family", "marketFamily"))
                    or "normalized.market_family"
                ]
                if fields.get("market_family")
                else None
            ),
            "sport": (
                [_selected_key(payload, ("sport",)) or "normalized.sport"]
                if fields.get("sport")
                else None
            ),
            "rules_text": rules_paths or None,
            "resolution_source": (
                [_selected_key(payload, ("resolution_source", "resolutionSource"))]
                if fields.get("resolution_source")
                else None
            ),
            "instrument_mapping_json": (
                ["ticker", "binary:YES_NO"] if mapping else None
            ),
        }
    )
    return {
        "market_key": market_key,
        "venue_event_key": event_key,
        "question": question,
        "title": title,
        "rules_text": rules_text or None,
        "rules_authoritative": bool(rules_text),
        "resolution_source": fields.get("resolution_source"),
        "close_time": close_time,
        "status": status,
        "instrument_mapping_json": mapping,
        "instrument_mapping_complete": bool(mapping),
        "contract_fields_json": fields,
        "field_provenance_json": provenance,
    }


def _first_event(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    market_refs = {
        _text(payload.get(key))
        for key in (
            "event_id",
            "eventId",
            "event_slug",
            "eventSlug",
            "event_ticker",
        )
        if _text(payload.get(key))
    }

    def validate(candidate: Mapping[str, Any]) -> Mapping[str, Any]:
        candidate_refs = {
            _text(candidate.get(key))
            for key in ("id", "event_id", "eventId", "slug", "ticker")
            if _text(candidate.get(key))
        }
        if market_refs and candidate_refs and not market_refs.intersection(
            candidate_refs
        ):
            raise ValueError("conflicting polymarket event association")
        return candidate

    event = payload.get("event")
    if isinstance(event, Mapping):
        return validate(event)
    events = payload.get("events")
    if isinstance(events, Sequence) and not isinstance(events, (str, bytes)):
        candidates = [item for item in events if isinstance(item, Mapping)]
        if len(candidates) == 1:
            return validate(candidates[0])
        if len(candidates) > 1:
            associated = [
                candidate
                for candidate in candidates
                if market_refs.intersection(
                    {
                        _text(candidate.get(key))
                        for key in (
                            "id",
                            "event_id",
                            "eventId",
                            "slug",
                            "ticker",
                        )
                        if _text(candidate.get(key))
                    }
                )
            ]
            if len(associated) == 1:
                return associated[0]
            raise ValueError("ambiguous polymarket event association")
    return {}


def _first_text(payload: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    for key in keys:
        text = _text(payload.get(key))
        if text:
            return text
    return None


def _first_text_with_path(
    payload: Mapping[str, Any], keys: Sequence[str]
) -> tuple[str | None, str | None]:
    for key in keys:
        text = _text(payload.get(key))
        if text:
            return text, key
    return None, None


def _selected_key(payload: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    for key in keys:
        if _text(payload.get(key)):
            return key
    return None


def _polymarket_event_path(
    payload: Mapping[str, Any], event: Mapping[str, Any]
) -> str | None:
    direct = _selected_key(payload, ("event_id", "eventId"))
    if direct:
        return direct
    nested = _selected_key(event, ("id", "event_id", "eventId", "slug"))
    if not nested:
        return None
    if isinstance(payload.get("event"), Mapping):
        prefix = "event"
    else:
        events = payload.get("events")
        index = next(
            (
                position
                for position, candidate in enumerate(events)
                if isinstance(candidate, Mapping) and candidate is event
            ),
            None,
        ) if isinstance(events, Sequence) and not isinstance(
            events, (str, bytes)
        ) else None
        prefix = f"events[{index}]" if index is not None else "events"
    return f"{prefix}.{nested}"


def _sequence(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return []
        return list(parsed) if isinstance(parsed, list) else []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    if hasattr(value, "tolist"):
        return _sequence(value.tolist())
    return []


def _compact(values: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): _json_safe(value)
        for key, value in values.items()
        if not _missing(value)
    }


def _missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        text = value.strip()
        return not text or text.casefold() in {"nan", "none", "<na>", "nat"}
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    if isinstance(missing, bool) or type(missing).__name__ == "bool_":
        return bool(missing)
    return False


def _text(value: Any) -> str | None:
    if _missing(value):
        return None
    return str(value).strip()


def _explicit_utc(value: str) -> bool:
    if not value or not (value.endswith("Z") or value.endswith("+00:00")):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    offset = parsed.utcoffset()
    return offset is not None and offset.total_seconds() == 0


def _sha256_json(value: Any) -> str:
    encoded = json.dumps(
        _json_safe(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {
            str(key): _json_safe(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
            if not _missing(item)
        }
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return sorted((_json_safe(item) for item in value), key=lambda item: str(item))
    if hasattr(value, "tolist"):
        return _json_safe(value.tolist())
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


__all__ = [
    "contract_evidence_dataframe",
    "stable_contract_evidence_id",
    "stable_evidence_projection_hash",
    "stable_source_row_hash",
]
