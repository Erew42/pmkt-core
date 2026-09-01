import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from pmkt.data.contract_evidence_manifest import verify_contract_evidence_manifest
from pmkt.data.registry import (
    CONTRACT_EVIDENCE_SCHEMA_VERSION,
    KALSHI_MARKET_SNAPSHOT_COLUMNS,
    KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    POLYMARKET_MARKET_SNAPSHOT_COLUMNS,
    POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
)
from pmkt.data.validation import validate_frame
from scripts.derive_market_snapshots_from_raw_json import (
    derive_market_snapshots_from_raw_json,
)


def test_derives_current_snapshot_columns_and_matching_projections(
    tmp_path: Path,
) -> None:
    polymarket_source = _write_raw_source(
        tmp_path / "polymarket_source.parquet", [_polymarket_raw()]
    )
    kalshi_source = _write_raw_source(
        tmp_path / "kalshi_source.parquet", [_kalshi_raw()]
    )
    output_dir = tmp_path / "derived"

    result = derive_market_snapshots_from_raw_json(
        polymarket_source=polymarket_source,
        kalshi_source=kalshi_source,
        output_dir=output_dir,
        batch_size=1,
        observed_at_utc="2026-07-10T17:59:00+00:00",
    )

    polymarket = pd.read_parquet(result.polymarket.snapshot_path)
    kalshi = pd.read_parquet(result.kalshi.snapshot_path)
    polymarket_projection = pd.read_parquet(result.polymarket.projection_path)
    kalshi_projection = pd.read_parquet(result.kalshi.projection_path)
    polymarket_evidence = pd.read_parquet(result.polymarket.contract_evidence_path)
    kalshi_evidence = pd.read_parquet(result.kalshi.contract_evidence_path)

    assert list(polymarket.columns) == POLYMARKET_MARKET_SNAPSHOT_COLUMNS
    assert list(kalshi.columns) == KALSHI_MARKET_SNAPSHOT_COLUMNS
    assert "raw_json" not in polymarket_projection.columns
    assert "raw_json" not in kalshi_projection.columns
    assert "raw_json_sha256" in polymarket_projection.columns
    assert "raw_json_sha256" in kalshi_projection.columns
    assert "condition_id" in polymarket_projection.columns
    assert "result" in kalshi_projection.columns
    assert polymarket.loc[0, "condition_id"] == "0xabc"
    assert polymarket.loc[0, "question_id"] == "question-1"
    assert kalshi.loc[0, "result"] == "yes"
    assert kalshi.loc[0, "rules_primary"] == "Primary settlement rule"
    assert validate_frame(
        polymarket, POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION, strict=True
    ).ok
    assert validate_frame(kalshi, KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION, strict=True).ok
    assert validate_frame(
        polymarket_evidence, CONTRACT_EVIDENCE_SCHEMA_VERSION, strict=True
    ).ok
    assert validate_frame(
        kalshi_evidence, CONTRACT_EVIDENCE_SCHEMA_VERSION, strict=True
    ).ok
    assert polymarket_evidence.loc[0, "rules_text"] == "Polymarket settlement rule"
    assert kalshi_evidence.loc[0, "rules_text"] == (
        "Primary settlement rule\nSecondary settlement rule"
    )

    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert summary["polymarket"]["row_count"] == 1
    assert summary["kalshi"]["row_count"] == 1
    assert summary["polymarket"]["derivation_mode"] == "raw_json"
    assert summary["kalshi"]["derivation_mode"] == "raw_json"
    assert manifest["mode"] == "derive_market_snapshots_from_raw_json"
    assert manifest["row_counts"]["polymarket_matching_projection"] == 1
    assert manifest["row_counts"]["polymarket_contract_evidence"] == 1
    assert manifest["row_counts"]["kalshi_contract_evidence"] == 1
    assert (
        manifest["schema_versions"]["polymarket_contract_evidence"]
        == CONTRACT_EVIDENCE_SCHEMA_VERSION
    )
    verify_contract_evidence_manifest(
        polymarket_evidence,
        artifact_path=result.polymarket.contract_evidence_path,
        manifest_path=result.polymarket.contract_evidence_manifest_path,
        expected_venue="polymarket",
        expected_source_endpoint="snapshot:raw_json",
        expected_payload_scope="snapshot",
        expected_observation_time_source="cli_override",
    )
    verify_contract_evidence_manifest(
        kalshi_evidence,
        artifact_path=result.kalshi.contract_evidence_path,
        manifest_path=result.kalshi.contract_evidence_manifest_path,
        expected_venue="kalshi",
        expected_source_endpoint="snapshot:raw_json",
        expected_payload_scope="snapshot",
        expected_observation_time_source="cli_override",
    )
    evidence_manifest = json.loads(
        result.polymarket.contract_evidence_manifest_path.read_text(encoding="utf-8")
    )
    assert evidence_manifest["observation_time_source"] == "cli_override"


def test_derivation_fails_when_observation_time_is_unavailable(tmp_path: Path) -> None:
    polymarket_source = _write_raw_source(
        tmp_path / "polymarket_source.parquet", [_polymarket_raw()]
    )
    kalshi_source = _write_raw_source(
        tmp_path / "kalshi_source.parquet", [_kalshi_raw()]
    )

    with pytest.raises(ValueError, match="observed_at_utc"):
        derive_market_snapshots_from_raw_json(
            polymarket_source=polymarket_source,
            kalshi_source=kalshi_source,
            output_dir=tmp_path / "derived",
        )


def test_derivation_preserves_row_observation_and_raw_json_formatting(
    tmp_path: Path,
) -> None:
    observed = "2026-07-10T17:58:00+00:00"
    pm_raw = json.dumps(_polymarket_raw(), indent=2, sort_keys=False)
    kx_raw = json.dumps(_kalshi_raw(), separators=(",", ":"), sort_keys=False)
    polymarket_source = _write_raw_rows(
        tmp_path / "polymarket_source.parquet",
        [
            {
                "raw_json": pm_raw,
                "raw_json_sha256": _sha256_text(pm_raw),
                "observed_at_utc": observed,
            }
        ],
    )
    kalshi_source = _write_raw_rows(
        tmp_path / "kalshi_source.parquet",
        [
            {
                "raw_json": kx_raw,
                "raw_json_sha256": _sha256_text(kx_raw),
                "observed_at_utc": observed,
            }
        ],
    )

    result = derive_market_snapshots_from_raw_json(
        polymarket_source=polymarket_source,
        kalshi_source=kalshi_source,
        output_dir=tmp_path / "derived",
    )

    pm_snapshot = pd.read_parquet(result.polymarket.snapshot_path)
    pm_evidence = pd.read_parquet(result.polymarket.contract_evidence_path)
    assert pm_snapshot.loc[0, "raw_json"] == pm_raw
    assert pm_snapshot.loc[0, "raw_json_sha256"] == _sha256_text(pm_raw)
    assert pm_evidence.loc[0, "observed_at_utc"] == observed
    manifest = json.loads(
        result.polymarket.contract_evidence_manifest_path.read_text(encoding="utf-8")
    )
    assert manifest["observation_time_source"] == "source_row"


def test_derivation_falls_back_to_normalized_snapshots_when_raw_json_is_null(
    tmp_path: Path,
) -> None:
    polymarket_source = tmp_path / "polymarket_source.parquet"
    kalshi_source = tmp_path / "kalshi_source.parquet"
    pd.DataFrame(
        [
            {
                "schema_version": POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
                "market_id": "pm-rain",
                "question": "Will it rain in Berlin?",
                "raw_json": None,
                "raw_json_sha256": "a" * 64,
            }
        ]
    ).to_parquet(polymarket_source, index=False)
    pd.DataFrame(
        [
            {
                "schema_version": KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
                "exchange": "kalshi",
                "market_key": "KXRAIN",
                "instrument_key": "KXRAIN:YES",
                "question": "Will it rain in Berlin?",
                "raw_json": None,
                "raw_json_sha256": "b" * 64,
            }
        ]
    ).to_parquet(kalshi_source, index=False)

    result = derive_market_snapshots_from_raw_json(
        polymarket_source=polymarket_source,
        kalshi_source=kalshi_source,
        output_dir=tmp_path / "derived",
        observed_at_utc="2026-07-10T17:59:00+00:00",
    )

    polymarket = pd.read_parquet(result.polymarket.snapshot_path)
    kalshi = pd.read_parquet(result.kalshi.snapshot_path)
    polymarket_evidence = pd.read_parquet(result.polymarket.contract_evidence_path)
    kalshi_evidence = pd.read_parquet(result.kalshi.contract_evidence_path)
    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))

    assert list(polymarket.columns) == POLYMARKET_MARKET_SNAPSHOT_COLUMNS
    assert list(kalshi.columns) == KALSHI_MARKET_SNAPSHOT_COLUMNS
    assert pd.isna(polymarket.loc[0, "condition_id"])
    assert pd.isna(kalshi.loc[0, "result"])
    assert summary["polymarket"]["derivation_mode"] == "normalized_snapshot_fallback"
    assert summary["kalshi"]["derivation_mode"] == "normalized_snapshot_fallback"
    assert pd.isna(polymarket_evidence.loc[0, "raw_payload_hash"])
    assert pd.isna(kalshi_evidence.loc[0, "raw_payload_hash"])
    assert (
        "raw_payload_unavailable"
        in polymarket_evidence.loc[0, "completeness_reasons_json"]
    )
    assert validate_frame(
        polymarket, POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION, strict=True
    ).ok
    assert validate_frame(kalshi, KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION, strict=True).ok


def test_derivation_rejects_invalid_raw_json(tmp_path: Path) -> None:
    bad_raw = "{not valid json"
    polymarket_source = _write_raw_rows(
        tmp_path / "polymarket_source.parquet",
        [{"raw_json": bad_raw, "raw_json_sha256": _sha256_text(bad_raw)}],
    )
    kalshi_source = _write_raw_source(
        tmp_path / "kalshi_source.parquet", [_kalshi_raw()]
    )

    with pytest.raises(ValueError, match="invalid raw_json"):
        derive_market_snapshots_from_raw_json(
            polymarket_source=polymarket_source,
            kalshi_source=kalshi_source,
            output_dir=tmp_path / "derived",
            observed_at_utc="2026-07-10T17:59:00+00:00",
        )


def test_derivation_rejects_raw_hash_mismatch(tmp_path: Path) -> None:
    polymarket_source = _write_raw_rows(
        tmp_path / "polymarket_source.parquet",
        [
            {
                "raw_json": _stable_raw_json(_polymarket_raw()),
                "raw_json_sha256": "0" * 64,
            }
        ],
    )
    kalshi_source = _write_raw_source(
        tmp_path / "kalshi_source.parquet", [_kalshi_raw()]
    )

    with pytest.raises(ValueError, match="raw_json_sha256 mismatch"):
        derive_market_snapshots_from_raw_json(
            polymarket_source=polymarket_source,
            kalshi_source=kalshi_source,
            output_dir=tmp_path / "derived",
            observed_at_utc="2026-07-10T17:59:00+00:00",
        )


def test_derivation_rejects_duplicate_primary_keys(tmp_path: Path) -> None:
    polymarket_source = _write_raw_source(
        tmp_path / "polymarket_source.parquet",
        [_polymarket_raw(), _polymarket_raw()],
    )
    kalshi_source = _write_raw_source(
        tmp_path / "kalshi_source.parquet", [_kalshi_raw()]
    )

    with pytest.raises(ValueError, match="duplicate market_id"):
        derive_market_snapshots_from_raw_json(
            polymarket_source=polymarket_source,
            kalshi_source=kalshi_source,
            output_dir=tmp_path / "derived",
            batch_size=1,
            observed_at_utc="2026-07-10T17:59:00+00:00",
        )


def test_derivation_rejects_existing_output_without_overwrite(tmp_path: Path) -> None:
    polymarket_source = _write_raw_source(
        tmp_path / "polymarket_source.parquet", [_polymarket_raw()]
    )
    kalshi_source = _write_raw_source(
        tmp_path / "kalshi_source.parquet", [_kalshi_raw()]
    )
    output_dir = tmp_path / "derived"
    output_dir.mkdir()
    (output_dir / "sentinel.txt").write_text("exists", encoding="utf-8")

    with pytest.raises(FileExistsError, match="not empty"):
        derive_market_snapshots_from_raw_json(
            polymarket_source=polymarket_source,
            kalshi_source=kalshi_source,
            output_dir=output_dir,
            observed_at_utc="2026-07-10T17:59:00+00:00",
        )


def _write_raw_source(path: Path, payloads: list[dict]) -> Path:
    rows = []
    for payload in payloads:
        raw_json = _stable_raw_json(payload)
        rows.append({"raw_json": raw_json, "raw_json_sha256": _sha256_text(raw_json)})
    return _write_raw_rows(path, rows)


def _write_raw_rows(path: Path, rows: list[dict]) -> Path:
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def _stable_raw_json(value: dict) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, default=str)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _polymarket_raw() -> dict:
    return {
        "id": "pm-rain",
        "question": "Will it rain in Berlin?",
        "description": "Polymarket settlement rule",
        "event": {"id": "event-rain", "slug": "berlin-rain"},
        "closed": False,
        "clobTokenIds": ["pm-yes", "pm-no"],
        "conditionId": "0xabc",
        "questionID": "question-1",
        "outcomes": ["Yes", "No"],
        "outcomePrices": ["0.7", "0.3"],
        "umaResolutionStatus": "resolved",
        "resolvedBy": "uma",
        "resolutionSource": "oracle",
        "openTime": "2026-01-01T00:00:00Z",
        "startTime": "2026-01-02T00:00:00Z",
        "closeTime": "2026-06-01T00:00:00Z",
        "volume": "100",
        "liquidity": "10",
        "bestBid": "0.60",
        "bestAsk": "0.80",
        "lastTradePrice": "0.75",
    }


def _kalshi_raw() -> dict:
    return {
        "ticker": "KXRAIN",
        "event_ticker": "KXRAIN-EVENT",
        "title": "Will it rain in Berlin?",
        "subtitle": "Weather",
        "category": "Weather",
        "series_ticker": "KXWX",
        "status": "settled",
        "result": "yes",
        "settlement_value_dollars": "1",
        "settlement_ts": "2026-06-01T01:00:00Z",
        "expiration_value": "yes",
        "is_provisional": False,
        "rules_primary": "Primary settlement rule",
        "rules_secondary": "Secondary settlement rule",
        "open_time": "2026-01-01T00:00:00Z",
        "close_time": "2026-06-01T00:00:00Z",
        "yes_bid_dollars": "0.60",
        "yes_ask_dollars": "0.80",
        "no_bid_dollars": "0.20",
        "no_ask_dollars": "0.40",
        "fee_type": "quadratic",
        "fee_multiplier": "1",
        "volume_fp": "100",
        "volume_24h_fp": "5",
        "liquidity_dollars": "10",
        "open_interest_fp": "20",
        "last_price_dollars": "0.7",
    }
