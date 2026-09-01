from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from pmkt.data.contract_evidence import (
    contract_evidence_dataframe,
    stable_evidence_projection_hash,
)
from pmkt.data.contract_evidence_manifest import (
    CONTRACT_EVIDENCE_MANIFEST_VERSION_V1,
    build_contract_evidence_manifest,
    contract_evidence_manifest_path,
    verify_contract_evidence_manifest,
    write_contract_evidence_bundle,
    write_contract_evidence_manifest,
)
from pmkt.data.registry import (
    CONTRACT_EVIDENCE_COLUMNS,
    CONTRACT_EVIDENCE_SCHEMA_VERSION,
)
from pmkt.data.validation import coerce_frame, validate_frame

DERIVED = "2026-07-10T18:00:00+00:00"
OBSERVED = "2026-07-10T17:59:00+00:00"


def _polymarket_raw(*, noise: str = "a") -> dict[str, object]:
    return {
        "id": "pm-1",
        "question": "France vs. Morocco: O/U 9.5 Total Corners",
        "description": "Only regulation and stoppage-time corners count.",
        "scan_category": "sports",
        "category": "Sports",
        "market_family": "soccer_totals",
        "sport": "soccer",
        "conditionId": "0xcondition",
        "endDate": "2026-07-09T22:00:00Z",
        "closed": True,
        "outcomes": '["Over", "Under"]',
        "clobTokenIds": '["pm-over", "pm-under"]',
        "events": [{"id": "event-1", "title": "France vs Morocco"}],
        "volatile_extra": noise,
    }


def _kalshi_raw(*, include_rules: bool = True) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": "KXWCCORNERS-26JUL09FRAMAR-10",
        "event_ticker": "KXWCCORNERS-26JUL09FRAMAR",
        "title": "10+ corners?",
        "scan_category": "sports",
        "category": "Sports",
        "market_family": "soccer_totals",
        "sport": "soccer",
        "close_time": "2026-07-09T21:59:51Z",
        "status": "finalized",
    }
    if include_rules:
        row["rules_primary"] = "At least 10 corners during the entire game."
        row["rules_secondary"] = "Extra time counts; shootout attempts do not."
    return row


def test_polymarket_projection_is_registered_and_preserves_orientation() -> None:
    frame = contract_evidence_dataframe(
        [_polymarket_raw()],
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
    )

    assert list(frame.columns) == CONTRACT_EVIDENCE_COLUMNS
    assert validate_frame(frame, CONTRACT_EVIDENCE_SCHEMA_VERSION, strict=True).ok
    row = frame.iloc[0]
    assert row["market_key"] == "pm-1"
    assert row["venue_event_key"] == "event-1"
    assert row["rules_text"] == "Only regulation and stoppage-time corners count."
    assert row["rules_complete"] is True or bool(row["rules_complete"])
    assert row["instrument_mapping_json"] == [
        {"instrument_key": "pm-over", "outcome": "Over"},
        {"instrument_key": "pm-under", "outcome": "Under"},
    ]
    assert row["field_provenance_json"]["rules_text"][0] == "description"
    assert row["contract_fields_json"]["scan_category"] == "sports"
    assert row["contract_fields_json"]["market_family"] == "soccer_totals"
    assert row["field_provenance_json"]["sport"] == ["sport"]


def test_kalshi_projection_joins_rules_and_maps_yes_no() -> None:
    frame = contract_evidence_dataframe(
        [_kalshi_raw()],
        venue="kalshi",
        source_endpoint="kalshi:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
    )
    row = frame.iloc[0]

    assert row["rules_text"] == (
        "At least 10 corners during the entire game.\n"
        "Extra time counts; shootout attempts do not."
    )
    assert row["instrument_mapping_json"] == [
        {
            "instrument_key": "KXWCCORNERS-26JUL09FRAMAR-10:YES",
            "outcome": "YES",
        },
        {
            "instrument_key": "KXWCCORNERS-26JUL09FRAMAR-10:NO",
            "outcome": "NO",
        },
    ]
    assert row["contract_fields_json"]["scan_category"] == "sports"
    assert row["contract_fields_json"]["market_family"] == "soccer_totals"
    assert row["field_provenance_json"]["sport"] == ["sport"]
    assert validate_frame(frame, CONTRACT_EVIDENCE_SCHEMA_VERSION, strict=True).ok


def test_projection_hash_survives_parquet_null_struct_keys(tmp_path: Path) -> None:
    complete = _kalshi_raw()
    incomplete = _kalshi_raw(include_rules=False)
    incomplete["ticker"] = "KXWCCORNERS-26JUL09FRAMAR-11"
    frame = contract_evidence_dataframe(
        [complete, incomplete],
        venue="kalshi",
        source_endpoint="kalshi:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
    )
    artifact = tmp_path / "contract_evidence.parquet"
    frame.to_parquet(artifact, index=False)

    restored = coerce_frame(
        pd.read_parquet(artifact),
        CONTRACT_EVIDENCE_SCHEMA_VERSION,
    )

    for _, row in restored.iterrows():
        assert row["evidence_projection_hash"] == stable_evidence_projection_hash(
            row.to_dict()
        )


def test_incomplete_list_payload_stays_fail_closed() -> None:
    frame = contract_evidence_dataframe(
        [_kalshi_raw(include_rules=False)],
        venue="kalshi",
        source_endpoint="kalshi:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
    )
    row = frame.iloc[0]

    assert bool(row["identity_complete"]) is True
    assert bool(row["rules_complete"]) is False
    assert "rules_incomplete" in row["completeness_reasons_json"]


def test_semantic_hash_ignores_raw_payload_noise() -> None:
    left = contract_evidence_dataframe(
        [_polymarket_raw(noise="a")],
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
    ).iloc[0]
    right = contract_evidence_dataframe(
        [_polymarket_raw(noise="b")],
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
    ).iloc[0]

    assert left["raw_payload_hash"] != right["raw_payload_hash"]
    assert left["evidence_id"] != right["evidence_id"]
    assert left["source_row_hash"] == right["source_row_hash"]
    assert left["evidence_projection_hash"] == right["evidence_projection_hash"]


def test_raw_json_hash_mismatch_is_rejected() -> None:
    raw_json = json.dumps(_polymarket_raw(), sort_keys=True, separators=(",", ":"))
    source = {
        "schema_version": "polymarket_market_snapshot.v1",
        "market_id": "pm-1",
        "raw_json": raw_json,
        "raw_json_sha256": "0" * 64,
    }

    try:
        contract_evidence_dataframe(
            [source],
            venue="polymarket",
            source_endpoint="snapshot:raw_json",
            payload_scope="snapshot",
            observed_at_utc=OBSERVED,
            derived_at_utc=DERIVED,
        )
    except ValueError as exc:
        assert "raw_json_sha256" in str(exc)
    else:
        raise AssertionError("expected raw JSON hash mismatch")

    source["raw_json_sha256"] = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
    frame = contract_evidence_dataframe(
        [source],
        venue="polymarket",
        source_endpoint="snapshot:raw_json",
        payload_scope="snapshot",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
    )
    canonical = json.dumps(
        json.loads(raw_json), ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )
    assert (
        frame.iloc[0]["raw_payload_hash"]
        == hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    )


def test_normalized_snapshot_without_raw_json_marks_raw_payload_unavailable() -> None:
    source = pd.Series(
        {
            "schema_version": "polymarket_market_snapshot.v1",
            "market_id": "pm-1",
            "question": "Will France win?",
            "token_ids": ["yes", "no"],
            "outcome_labels_json": ["Yes", "No"],
            "raw_json": None,
            "raw_json_sha256": None,
        }
    ).to_dict()
    frame = contract_evidence_dataframe(
        [source],
        venue="polymarket",
        source_endpoint="snapshot:normalized",
        payload_scope="snapshot",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
    )
    row = frame.iloc[0]
    assert row["raw_payload_hash"] is None
    assert row["instrument_mapping_json"] == [
        {"instrument_key": "yes", "outcome": "Yes"},
        {"instrument_key": "no", "outcome": "No"},
    ]
    assert bool(row["instrument_mapping_complete"]) is True
    assert "raw_payload_unavailable" in row["completeness_reasons_json"]


def test_normalized_snapshot_preserves_outcome_orientation() -> None:
    source = {
        "schema_version": "polymarket_market_snapshot.v1",
        "market_id": "pm-1",
        "question": "Will France win?",
        "token_ids": ["yes", "no"],
        "outcome_labels_json": ["Yes", "No"],
        "raw_json": None,
        "raw_json_sha256": None,
    }

    row = contract_evidence_dataframe(
        [source],
        venue="polymarket",
        source_endpoint="snapshot:normalized",
        payload_scope="snapshot",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
    ).iloc[0]

    assert row["instrument_mapping_json"] == [
        {"instrument_key": "yes", "outcome": "Yes"},
        {"instrument_key": "no", "outcome": "No"},
    ]


def test_arbitrary_mapping_requires_explicit_approved_payload_kind(
    tmp_path: Path,
) -> None:
    review_only = contract_evidence_dataframe(
        [_polymarket_raw()],
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
    ).iloc[0]
    authoritative = contract_evidence_dataframe(
        [_polymarket_raw()],
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
        source_payload_kind="venue_api_response",
    ).iloc[0]

    assert "raw_payload_unavailable" in review_only["completeness_reasons_json"]
    assert review_only["field_provenance_json"]["_source_payload"] == {
        "kind": "unknown",
        "authoritative": False,
    }
    assert "raw_payload_unavailable" not in authoritative["completeness_reasons_json"]
    assert authoritative["field_provenance_json"]["_source_payload"] == {
        "kind": "venue_api_response",
        "authoritative": True,
    }

    artifact = tmp_path / "evidence.parquet"
    pd.DataFrame([review_only]).to_parquet(artifact, index=False)
    manifest = build_contract_evidence_manifest(
        pd.DataFrame([review_only]),
        artifact_path=artifact,
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observation_time_source="argument",
        source_payload_kind="venue_api_response",
        collection_complete=True,
        stop_reason="cursor_exhausted",
        source_manifest_sha256="a" * 64,
    )
    assert manifest["authoritative_complete"] is False


def test_polymarket_event_association_requires_one_stable_match() -> None:
    ambiguous = _polymarket_raw()
    ambiguous["events"] = [
        {"id": "event-1", "title": "First"},
        {"id": "event-2", "title": "Second"},
    ]
    with pytest.raises(ValueError, match="ambiguous polymarket event association"):
        contract_evidence_dataframe(
            [ambiguous],
            venue="polymarket",
            source_endpoint="gamma:/markets",
            payload_scope="list",
            observed_at_utc=OBSERVED,
            derived_at_utc=DERIVED,
            source_payload_kind="venue_api_response",
        )

    associated = dict(ambiguous, eventId="event-2")
    row = contract_evidence_dataframe(
        [associated],
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
        source_payload_kind="venue_api_response",
    ).iloc[0]
    assert row["venue_event_key"] == "event-2"


def test_polymarket_event_association_rejects_single_stable_id_conflict() -> None:
    conflicting = _polymarket_raw()
    conflicting["event_id"] = "event-A"
    conflicting["events"] = [{"id": "event-B", "title": "Wrong event"}]

    with pytest.raises(ValueError, match="conflicting polymarket event association"):
        contract_evidence_dataframe(
            [conflicting],
            venue="polymarket",
            source_endpoint="gamma:/markets",
            payload_scope="list",
            observed_at_utc=OBSERVED,
            derived_at_utc=DERIVED,
            source_payload_kind="venue_api_response",
        )


def test_polymarket_event_provenance_uses_selected_event_index() -> None:
    associated = _polymarket_raw()
    associated["event_slug"] = "selected"
    associated["events"] = [
        {"id": "event-1", "slug": "other", "title": "Other"},
        {"id": "event-2", "slug": "selected", "title": "Selected"},
    ]

    row = contract_evidence_dataframe(
        [associated],
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
        source_payload_kind="venue_api_response",
    ).iloc[0]

    assert row["venue_event_key"] == "event-2"
    assert row["field_provenance_json"]["venue_event_key"] == ["events[1].id"]


def test_canonical_raw_hash_and_identity_ignore_json_formatting() -> None:
    payload = _polymarket_raw()
    compact = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    pretty = json.dumps(payload, indent=2, sort_keys=False)
    rows = []
    for raw_json in (compact, pretty):
        rows.append(
            {
                "schema_version": "polymarket_market_snapshot.v1",
                "market_id": "pm-1",
                "raw_json": raw_json,
                "raw_json_sha256": hashlib.sha256(raw_json.encode()).hexdigest(),
            }
        )

    left = contract_evidence_dataframe(
        [rows[0]],
        venue="polymarket",
        source_endpoint="snapshot:raw_json",
        payload_scope="snapshot",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
    ).iloc[0]
    right = contract_evidence_dataframe(
        [rows[1]],
        venue="polymarket",
        source_endpoint="snapshot:raw_json",
        payload_scope="snapshot",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
    ).iloc[0]

    assert left["raw_payload_hash"] == right["raw_payload_hash"]
    assert left["evidence_id"] == right["evidence_id"]


def test_observation_identity_distinguishes_history_and_rejects_duplicates() -> None:
    first = contract_evidence_dataframe(
        [_polymarket_raw()],
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
    )
    second = contract_evidence_dataframe(
        [_polymarket_raw()],
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observed_at_utc="2026-07-10T18:01:00+00:00",
        derived_at_utc="2026-07-10T18:02:00+00:00",
    )

    history = pd.concat([first, second], ignore_index=True)
    assert history["evidence_id"].nunique() == 2
    assert validate_frame(history, CONTRACT_EVIDENCE_SCHEMA_VERSION, strict=True).ok
    duplicate = pd.concat([first, first], ignore_index=True)
    result = validate_frame(duplicate, CONTRACT_EVIDENCE_SCHEMA_VERSION, strict=True)
    assert not result.ok
    assert any("duplicate" in error.lower() for error in result.errors)


def test_observation_time_is_required_and_must_be_explicit_utc() -> None:
    kwargs = {
        "venue": "polymarket",
        "source_endpoint": "gamma:/markets",
        "payload_scope": "list",
        "derived_at_utc": DERIVED,
    }
    for observed in (None, "2026-07-10T17:59:00", "2026-07-10T19:59:00+02:00"):
        with pytest.raises(ValueError, match="observed_at_utc"):
            contract_evidence_dataframe(
                [_polymarket_raw()], observed_at_utc=observed, **kwargs
            )


def test_contract_evidence_manifest_detects_artifact_and_row_tampering(
    tmp_path: Path,
) -> None:
    frame = contract_evidence_dataframe(
        [_polymarket_raw()],
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
    )
    artifact = tmp_path / "contract_evidence.parquet"
    frame.to_parquet(artifact, index=False)
    manifest = write_contract_evidence_manifest(
        frame,
        artifact_path=artifact,
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observation_time_source="argument",
    )
    assert manifest == contract_evidence_manifest_path(artifact)
    verify_contract_evidence_manifest(
        frame,
        artifact_path=artifact,
        manifest_path=manifest,
        expected_venue="polymarket",
        expected_source_endpoint="gamma:/markets",
        expected_payload_scope="list",
        expected_observation_time_source="argument",
    )

    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["source_endpoint"] = "tampered:/endpoint"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="source_endpoint"):
        verify_contract_evidence_manifest(
            frame,
            artifact_path=artifact,
            manifest_path=manifest,
            expected_venue="polymarket",
            expected_source_endpoint="gamma:/markets",
            expected_payload_scope="list",
            expected_observation_time_source="argument",
        )
    manifest = write_contract_evidence_manifest(
        frame,
        artifact_path=artifact,
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observation_time_source="argument",
    )

    with pytest.raises(ValueError, match="row_count|evidence_ids"):
        verify_contract_evidence_manifest(
            pd.concat([frame, frame], ignore_index=True),
            artifact_path=artifact,
            manifest_path=manifest,
            expected_venue="polymarket",
            expected_source_endpoint="gamma:/markets",
            expected_payload_scope="list",
            expected_observation_time_source="argument",
        )
    artifact.write_bytes(artifact.read_bytes() + b"tamper")
    with pytest.raises(ValueError, match="artifact_sha256"):
        verify_contract_evidence_manifest(
            frame,
            artifact_path=artifact,
            manifest_path=manifest,
            expected_venue="polymarket",
            expected_source_endpoint="gamma:/markets",
            expected_payload_scope="list",
            expected_observation_time_source="argument",
        )


def test_v1_manifest_remains_readable_but_never_authoritative(tmp_path: Path) -> None:
    frame = contract_evidence_dataframe(
        [_polymarket_raw()],
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
    )
    artifact = tmp_path / "contract_evidence.parquet"
    frame.to_parquet(artifact, index=False)
    manifest = write_contract_evidence_manifest(
        frame,
        artifact_path=artifact,
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observation_time_source="argument",
    )
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["manifest_version"] = CONTRACT_EVIDENCE_MANIFEST_VERSION_V1
    for key in (
        "source_payload_kind",
        "collection_complete",
        "stop_reason",
        "continuation_cursor",
        "collection_errors",
        "source_manifest_sha256",
        "payload_hashes_sha256",
        "authoritative_complete",
    ):
        payload.pop(key, None)
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    verified = verify_contract_evidence_manifest(
        frame,
        artifact_path=artifact,
        manifest_path=manifest,
        expected_venue="polymarket",
        expected_source_endpoint="gamma:/markets",
        expected_payload_scope="list",
        expected_observation_time_source="argument",
    )
    assert verified["authoritative_complete"] is False


def test_v2_bundle_binds_collection_state_and_rejects_overwrite(tmp_path: Path) -> None:
    frame = contract_evidence_dataframe(
        [_polymarket_raw()],
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
        source_payload_kind="venue_api_response",
    )
    requested = tmp_path / "contract_evidence.parquet"
    collection = {
        "venue": "polymarket",
        "source_endpoint": "gamma:/markets",
        "payload_scope": "list",
        "page_count": 1,
        "collection_complete": True,
        "stop_reason": "cursor_exhausted",
        "continuation_cursor": None,
        "collection_errors": [],
    }
    artifact, manifest = write_contract_evidence_bundle(
        frame,
        artifact_path=requested,
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observation_time_source="capture_clock",
        source_payload_kind="venue_api_response",
        collection_complete=True,
        stop_reason="cursor_exhausted",
        continuation_cursor=None,
        collection_errors=(),
        source_collection_manifest=collection,
    )
    verified = verify_contract_evidence_manifest(
        frame,
        artifact_path=artifact,
        manifest_path=manifest,
        expected_venue="polymarket",
        expected_source_endpoint="gamma:/markets",
        expected_payload_scope="list",
        expected_observation_time_source="capture_clock",
        expected_source_payload_kind="venue_api_response",
    )
    assert verified["authoritative_complete"] is True
    original_manifest = manifest.read_bytes()

    with pytest.raises(FileExistsError, match="destination is immutable"):
        write_contract_evidence_bundle(
            frame,
            artifact_path=requested,
            venue="polymarket",
            source_endpoint="gamma:/markets",
            payload_scope="list",
            observation_time_source="capture_clock",
            source_payload_kind="venue_api_response",
            collection_complete=False,
            stop_reason="max_pages_reached",
            continuation_cursor="next",
            collection_errors=(),
            source_collection_manifest={
                "venue": "polymarket",
                "source_endpoint": "gamma:/markets",
                "payload_scope": "list",
                "page_count": 1,
                "collection_complete": False,
                "stop_reason": "max_pages_reached",
                "continuation_cursor": "next",
                "collection_errors": [],
            },
        )
    assert artifact.is_file()
    assert manifest.read_bytes() == original_manifest


def test_v2_bundle_canonicalizes_exhausted_cursor_and_collection_errors(
    tmp_path: Path,
) -> None:
    frame = contract_evidence_dataframe(
        [_polymarket_raw()],
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
        source_payload_kind="venue_api_response",
    )
    artifact, manifest = write_contract_evidence_bundle(
        frame,
        artifact_path=tmp_path / "contract_evidence.parquet",
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observation_time_source="capture_clock",
        source_payload_kind="venue_api_response",
        collection_complete=True,
        stop_reason="cursor_exhausted",
        continuation_cursor="",
        collection_errors=(" warning ", "", "warning"),
        source_collection_manifest={
            "venue": "polymarket",
            "source_endpoint": "gamma:/markets",
            "payload_scope": "list",
            "collection_complete": True,
            "stop_reason": "cursor_exhausted",
            "continuation_cursor": "",
            "collection_errors": [" warning ", "", "warning"],
        },
    )

    source_manifest = json.loads(
        manifest.with_name("source_collection_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    outer_manifest = json.loads(manifest.read_text(encoding="utf-8"))
    assert source_manifest["continuation_cursor"] is None
    assert source_manifest["collection_errors"] == ["warning"]
    assert outer_manifest["continuation_cursor"] is None
    assert outer_manifest["collection_errors"] == ["warning"]
    verified = verify_contract_evidence_manifest(
        frame,
        artifact_path=artifact,
        manifest_path=manifest,
        expected_venue="polymarket",
        expected_source_endpoint="gamma:/markets",
        expected_payload_scope="list",
        expected_observation_time_source="capture_clock",
        expected_source_payload_kind="venue_api_response",
    )
    assert verified["authoritative_complete"] is False


def test_v2_verifier_rejects_inner_collection_state_contradiction(
    tmp_path: Path,
) -> None:
    frame = contract_evidence_dataframe(
        [_polymarket_raw()],
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
        source_payload_kind="venue_api_response",
    )
    artifact, manifest = write_contract_evidence_bundle(
        frame,
        artifact_path=tmp_path / "contract_evidence.parquet",
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observation_time_source="capture_clock",
        source_payload_kind="venue_api_response",
        collection_complete=True,
        stop_reason="cursor_exhausted",
        continuation_cursor=None,
        collection_errors=(),
        source_collection_manifest={
            "venue": "polymarket",
            "source_endpoint": "gamma:/markets",
            "payload_scope": "list",
            "collection_complete": True,
            "stop_reason": "cursor_exhausted",
            "continuation_cursor": None,
            "collection_errors": [],
        },
    )
    source_manifest = manifest.with_name("source_collection_manifest.json")
    inner = json.loads(source_manifest.read_text(encoding="utf-8"))
    inner.update(
        {
            "collection_complete": False,
            "stop_reason": "max_pages_reached",
            "continuation_cursor": "next",
        }
    )
    source_manifest.write_text(json.dumps(inner), encoding="utf-8")
    outer = json.loads(manifest.read_text(encoding="utf-8"))
    outer["source_manifest_sha256"] = hashlib.sha256(
        source_manifest.read_bytes()
    ).hexdigest()
    manifest.write_text(json.dumps(outer), encoding="utf-8")

    with pytest.raises(ValueError, match="source_manifest_state"):
        verify_contract_evidence_manifest(
            frame,
            artifact_path=artifact,
            manifest_path=manifest,
            expected_venue="polymarket",
            expected_source_endpoint="gamma:/markets",
            expected_payload_scope="list",
            expected_observation_time_source="capture_clock",
            expected_source_payload_kind="venue_api_response",
        )


@pytest.mark.parametrize("stop_reason", ["max_pages_reached", "page_limit_exhausted"])
def test_v2_manifest_rejects_inconsistent_complete_collection_state(
    tmp_path: Path, stop_reason: str
) -> None:
    frame = contract_evidence_dataframe(
        [_polymarket_raw()],
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observed_at_utc=OBSERVED,
        derived_at_utc=DERIVED,
        source_payload_kind="venue_api_response",
    )
    artifact = tmp_path / "contract_evidence.parquet"
    artifact.write_bytes(b"artifact")

    manifest = build_contract_evidence_manifest(
        frame,
        artifact_path=artifact,
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="list",
        observation_time_source="capture_clock",
        source_payload_kind="venue_api_response",
        collection_complete=True,
        stop_reason=stop_reason,
        continuation_cursor="next",
        collection_errors=(),
        source_manifest_sha256="a" * 64,
    )

    assert manifest["authoritative_complete"] is False
