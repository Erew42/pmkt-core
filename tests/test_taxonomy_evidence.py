from __future__ import annotations

import json

import pytest

from pmkt.data.registry import (
    MARKET_TAXONOMY_EVIDENCE_SCHEMA_VERSION,
    get_table_spec,
)
from pmkt.data.taxonomy_evidence import (
    build_market_taxonomy_evidence_row,
    market_taxonomy_evidence_frame,
)
from pmkt.data.validation import validate_frame


SHA = "a" * 64


def _row(*, venue: str = "polymarket", market_key: str = "123") -> dict[str, object]:
    return build_market_taxonomy_evidence_row(
        venue=venue,
        market_key=market_key,
        event_key="456",
        native_category="Sports" if venue == "kalshi" else None,
        native_tags=["Soccer", "Sports", "Soccer"],
        native_series=["World Cup"],
        series_ticker="KXSOCCER" if venue == "kalshi" else None,
        structured_sport={"name": "soccer"},
        game_id="game-1",
        sports_market_type="moneyline",
        requested_at_utc="2026-08-12T10:00:00+00:00",
        observed_at_utc="2026-08-12T10:00:01+00:00",
        source_endpoint="gamma:/events/456",
        source_payload_sha256=SHA,
        snapshot_raw_json_sha256="b" * 64,
        issues=["snapshot_missing_tags", "snapshot_missing_tags"],
    )


def test_taxonomy_evidence_schema_is_registered() -> None:
    spec = get_table_spec(MARKET_TAXONOMY_EVIDENCE_SCHEMA_VERSION)
    assert spec.name == "market_taxonomy_evidence"
    assert spec.primary_key == ("venue", "market_key")
    assert "native_tags_json" in spec.columns
    assert "source_payload_sha256" in spec.columns


def test_taxonomy_evidence_builder_preserves_native_metadata_and_provenance() -> None:
    row = _row()
    assert row["schema_version"] == MARKET_TAXONOMY_EVIDENCE_SCHEMA_VERSION
    assert json.loads(str(row["native_tags_json"])) == ["Soccer", "Sports"]
    assert json.loads(str(row["native_series_json"])) == ["World Cup"]
    assert json.loads(str(row["structured_sport_json"])) == {"name": "soccer"}
    assert json.loads(str(row["issues_json"])) == ["snapshot_missing_tags"]
    assert "primary_domain" not in row


def test_taxonomy_evidence_frame_validates_and_sorts() -> None:
    frame = market_taxonomy_evidence_frame(
        [
            _row(venue="polymarket", market_key="z"),
            _row(venue="kalshi", market_key="a"),
        ]
    )
    assert list(zip(frame["venue"], frame["market_key"])) == [
        ("kalshi", "a"),
        ("polymarket", "z"),
    ]
    assert validate_frame(
        frame,
        MARKET_TAXONOMY_EVIDENCE_SCHEMA_VERSION,
        strict=True,
    ).ok


def test_taxonomy_evidence_rejects_duplicate_market_keys() -> None:
    with pytest.raises(ValueError, match="primary key"):
        market_taxonomy_evidence_frame([_row(), _row()])


def test_taxonomy_evidence_strict_converter_rejects_extra_fields() -> None:
    row = _row()
    row["legacy_debug_note"] = "must-not-be-projected-away"

    with pytest.raises(ValueError, match="extra columns: legacy_debug_note"):
        market_taxonomy_evidence_frame([row])


def test_taxonomy_evidence_strict_converter_rejects_invalid_json() -> None:
    row = _row()
    row["issues_json"] = "not-json"

    with pytest.raises(ValueError, match="issues_json: 1 values are not valid JSON arrays"):
        market_taxonomy_evidence_frame([row])


def test_taxonomy_evidence_strict_converter_rejects_naive_timestamp() -> None:
    row = _row()
    row["observed_at_utc"] = "2026-08-12T10:00:01"

    with pytest.raises(
        ValueError,
        match="observed_at_utc: 1 values are not explicit UTC timestamps",
    ):
        market_taxonomy_evidence_frame([row])


def test_taxonomy_evidence_accepts_retained_json_null_optional_object() -> None:
    row = _row()
    row["structured_sport_json"] = "null"

    frame = market_taxonomy_evidence_frame([row])

    assert frame.loc[0, "structured_sport_json"] == "null"


def test_taxonomy_evidence_strict_converter_accepts_empty_frame() -> None:
    frame = market_taxonomy_evidence_frame([])

    assert frame.empty
    assert tuple(frame.columns) == get_table_spec(
        MARKET_TAXONOMY_EVIDENCE_SCHEMA_VERSION
    ).columns


@pytest.mark.parametrize("value", ["bad", "A" * 64, "a" * 63])
def test_taxonomy_evidence_rejects_invalid_hashes(value: str) -> None:
    with pytest.raises(ValueError, match="SHA-256"):
        build_market_taxonomy_evidence_row(
            venue="kalshi",
            market_key="KXTEST",
            event_key="KXEVENT",
            requested_at_utc="2026-08-12T10:00:00+00:00",
            observed_at_utc="2026-08-12T10:00:01+00:00",
            source_endpoint="kalshi:/events/KXEVENT",
            source_payload_sha256=value,
        )
