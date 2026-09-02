from __future__ import annotations

import hashlib
import json

import pandas as pd
import pytest
from typer.testing import CliRunner

from pmkt.cli.app import app
from pmkt.data.canonical import POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION
from pmkt.data.manifests import build_run_manifest, validate_run_manifest, write_manifest
from pmkt.data.schemas import topbook_row
from pmkt.data.validation import validate_frame


def test_run_manifest_records_core_and_caller_provenance(tmp_path) -> None:
    manifest = build_run_manifest(
        run_id="run-provenance",
        run_dir=tmp_path,
        started_at_utc="2026-05-26T00:00:00Z",
        ended_at_utc="2026-05-26T00:01:00Z",
        status="success",
        command="pmkt collect-books",
        dataset_paths={},
        schema_versions={},
        row_counts={},
        git_commit="caller-commit",
    )

    assert manifest["caller_git_commit"] == "caller-commit"
    assert manifest["pmkt_core_version"] == "0.1.0"
    assert manifest["pmkt_core_commit"]


def test_build_run_manifest_supports_success_partial_and_failed_statuses(tmp_path) -> None:
    rows = []
    for status in ("success", "partial", "failed"):
        manifest = build_run_manifest(
            run_id=f"run-{status}",
            run_dir=tmp_path / status,
            started_at_utc="2026-05-26T00:00:00Z",
            ended_at_utc="2026-05-26T00:01:00Z",
            status=status,
            command="pmkt stream-books",
            dataset_paths={"topbook": "topbook_v1.parquet"},
            schema_versions={"topbook": "topbook.v1"},
            row_counts={"topbook": 1},
            quality_flag_counts={"seq_gap": 1} if status == "partial" else {},
            venue_counts={"polymarket": 1},
            instrument_counts={"token-1": 1},
            reconnect_count=1 if status == "partial" else 0,
            sequence_gap_count=1 if status == "partial" else 0,
            resync_event_count=2 if status == "partial" else 0,
            error_type="RuntimeError" if status == "failed" else None,
            error_message="fixture failure" if status == "failed" else None,
        )
        path = write_manifest(tmp_path / status / "manifest.json", manifest)
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows.append(payload)

    report = validate_frame(pd.DataFrame(rows), "run_manifest.v1")

    assert report.ok
    assert [row["status"] for row in rows] == ["success", "partial", "failed"]
    assert rows[1]["quality_flag_counts"] == {"seq_gap": 1}
    assert rows[1]["venue_counts"] == {"polymarket": 1}
    assert rows[1]["instrument_counts"] == {"token-1": 1}
    assert rows[1]["reconnect_count"] == 1
    assert rows[1]["sequence_gap_count"] == 1
    assert rows[1]["resync_event_count"] == 2
    assert rows[2]["error_type"] == "RuntimeError"


def _write_topbook(path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            topbook_row(
                exchange="polymarket",
                instrument_id="token-1",
                received_at_utc="2026-05-26T00:00:00+00:00",
                best_bid_dollars=0.4,
                best_ask_dollars=0.6,
                spread_dollars=0.2,
                valid_state=True,
                quality_flags=[],
            )
        ]
    ).to_parquet(path, index=False)


def _sha256(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_validate_run_manifest_accepts_canonical_dataset_manifest(tmp_path) -> None:
    run_dir = tmp_path / "run"
    dataset_path = run_dir / "topbook_v1.parquet"
    _write_topbook(dataset_path)
    manifest_path = write_manifest(
        run_dir / "manifest.json",
        build_run_manifest(
            run_id="run-1",
            run_dir=run_dir,
            started_at_utc="2026-05-26T00:00:00Z",
            ended_at_utc="2026-05-26T00:01:00Z",
            status="success",
            command="pmkt stream-books",
            dataset_paths={"topbook_v1_parquet": "topbook_v1.parquet"},
            schema_versions={"topbook": "topbook.v1"},
            row_counts={"topbook": 1},
            extra={"dataset_hashes": {"topbook": _sha256(dataset_path)}},
        ),
    )

    report = validate_run_manifest(manifest_path)

    assert report.ok, report.all_errors
    assert report.datasets[0].dataset_key == "topbook_v1_parquet"
    assert report.datasets[0].row_count == 1
    assert report.datasets[0].actual_sha256 == _sha256(dataset_path)


def test_validate_run_manifest_accepts_parquet_directory_dataset(tmp_path) -> None:
    run_dir = tmp_path / "run"
    dataset_path = run_dir / "topbook_v1.parquet"
    dataset_path.mkdir(parents=True)
    _write_topbook(dataset_path / "part-000000.parquet")
    manifest_path = write_manifest(
        run_dir / "manifest.json",
        build_run_manifest(
            run_id="run-1",
            run_dir=run_dir,
            started_at_utc="2026-05-26T00:00:00Z",
            ended_at_utc="2026-05-26T00:01:00Z",
            status="success",
            command="pmkt stream-books",
            dataset_paths={"topbook_v1_parquet": "topbook_v1.parquet"},
            schema_versions={"topbook": "topbook.v1"},
            row_counts={"topbook": 1},
        ),
    )

    report = validate_run_manifest(manifest_path)

    assert report.ok, report.all_errors
    assert report.datasets[0].row_count == 1


def test_validate_run_manifest_requires_schema_for_dataset_paths(tmp_path) -> None:
    dataset_path = tmp_path / "topbook.parquet"
    _write_topbook(dataset_path)
    manifest_path = write_manifest(
        tmp_path / "manifest.json",
        build_run_manifest(
            run_id="run-1",
            run_dir=tmp_path,
            started_at_utc="2026-05-26T00:00:00Z",
            ended_at_utc="2026-05-26T00:01:00Z",
            status="success",
            command="pmkt stream-books",
            dataset_paths={"topbook": "topbook.parquet"},
            schema_versions={},
            row_counts={"topbook": 1},
        ),
    )

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert any(
        "schema version is required for dataset_paths entries" in error
        for error in report.all_errors
    )


def test_validate_run_manifest_requires_row_count_for_dataset_paths(tmp_path) -> None:
    dataset_path = tmp_path / "topbook.parquet"
    _write_topbook(dataset_path)
    manifest_path = write_manifest(
        tmp_path / "manifest.json",
        build_run_manifest(
            run_id="run-1",
            run_dir=tmp_path,
            started_at_utc="2026-05-26T00:00:00Z",
            ended_at_utc="2026-05-26T00:01:00Z",
            status="success",
            command="pmkt stream-books",
            dataset_paths={"topbook": "topbook.parquet"},
            schema_versions={"topbook": "topbook.v1"},
            row_counts={},
        ),
    )

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert any(
        "row count is required for dataset_paths entries" in error
        for error in report.all_errors
    )


def test_validate_run_manifest_rejects_unparseable_row_count(tmp_path) -> None:
    dataset_path = tmp_path / "topbook.parquet"
    _write_topbook(dataset_path)
    manifest_path = write_manifest(
        tmp_path / "manifest.json",
        build_run_manifest(
            run_id="run-1",
            run_dir=tmp_path,
            started_at_utc="2026-05-26T00:00:00Z",
            ended_at_utc="2026-05-26T00:01:00Z",
            status="success",
            command="pmkt stream-books",
            dataset_paths={"topbook": "topbook.parquet"},
            schema_versions={"topbook": "topbook.v1"},
            row_counts={"topbook": "one"},
        ),
    )

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert any(
        "row count is not a valid nonnegative integer" in error
        for error in report.all_errors
    )


@pytest.mark.parametrize("row_count", [True, 1.5, -1, "1.0", "1e0"])
def test_validate_run_manifest_rejects_noncanonical_or_negative_row_counts(
    tmp_path,
    row_count,
) -> None:
    dataset_path = tmp_path / "topbook.parquet"
    _write_topbook(dataset_path)
    manifest_path = write_manifest(
        tmp_path / "manifest.json",
        build_run_manifest(
            run_id="run-1",
            run_dir=tmp_path,
            started_at_utc="2026-05-26T00:00:00Z",
            ended_at_utc="2026-05-26T00:01:00Z",
            status="success",
            command="pmkt stream-books",
            dataset_paths={"topbook": "topbook.parquet"},
            schema_versions={"topbook": "topbook.v1"},
            row_counts={"topbook": row_count},
        ),
    )

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert any(
        "row count is not a valid nonnegative integer" in error
        for error in report.all_errors
    )


@pytest.mark.parametrize("row_count", [1, 1.0, "1", "+1"])
def test_validate_run_manifest_accepts_exact_one_row_count_forms(
    tmp_path,
    row_count,
) -> None:
    dataset_path = tmp_path / "topbook.parquet"
    _write_topbook(dataset_path)
    manifest_path = write_manifest(
        tmp_path / "manifest.json",
        build_run_manifest(
            run_id="run-1",
            run_dir=tmp_path,
            started_at_utc="2026-05-26T00:00:00Z",
            ended_at_utc="2026-05-26T00:01:00Z",
            status="success",
            command="pmkt stream-books",
            dataset_paths={"topbook": "topbook.parquet"},
            schema_versions={"topbook": "topbook.v1"},
            row_counts={"topbook": row_count},
        ),
    )

    report = validate_run_manifest(manifest_path)

    assert report.ok, report.all_errors
    assert report.datasets[0].expected_row_count == 1


def test_validate_run_manifest_accepts_inline_dataset_metadata(tmp_path) -> None:
    dataset_path = tmp_path / "topbook.parquet"
    _write_topbook(dataset_path)
    manifest_path = write_manifest(
        tmp_path / "manifest.json",
        build_run_manifest(
            run_id="run-1",
            run_dir=tmp_path,
            started_at_utc="2026-05-26T00:00:00Z",
            ended_at_utc="2026-05-26T00:01:00Z",
            status="success",
            command="pmkt stream-books",
            dataset_paths={
                "topbook": {
                    "path": "topbook.parquet",
                    "schema_version": "topbook.v1",
                    "row_count": "1",
                    "sha256": _sha256(dataset_path),
                },
            },
            schema_versions={},
            row_counts={},
        ),
    )

    report = validate_run_manifest(manifest_path)

    assert report.ok, report.all_errors
    assert report.datasets[0].schema_version == "topbook.v1"
    assert report.datasets[0].row_count == 1
    assert report.datasets[0].actual_sha256 == _sha256(dataset_path)


def test_validate_run_manifest_inline_path_can_use_top_level_metadata(tmp_path) -> None:
    dataset_path = tmp_path / "topbook.parquet"
    _write_topbook(dataset_path)
    manifest_path = write_manifest(
        tmp_path / "manifest.json",
        build_run_manifest(
            run_id="run-1",
            run_dir=tmp_path,
            started_at_utc="2026-05-26T00:00:00Z",
            ended_at_utc="2026-05-26T00:01:00Z",
            status="success",
            command="pmkt stream-books",
            dataset_paths={"topbook": {"path": "topbook.parquet"}},
            schema_versions={"topbook": "topbook.v1"},
            row_counts={"topbook": 1},
        ),
    )

    report = validate_run_manifest(manifest_path)

    assert report.ok, report.all_errors
    assert report.datasets[0].schema_version == "topbook.v1"
    assert report.datasets[0].row_count == 1


def test_validate_run_manifest_reports_missing_dataset(tmp_path) -> None:
    manifest_path = write_manifest(
        tmp_path / "manifest.json",
        build_run_manifest(
            run_id="run-1",
            run_dir=tmp_path,
            started_at_utc="2026-05-26T00:00:00Z",
            ended_at_utc="2026-05-26T00:01:00Z",
            status="success",
            command="pmkt stream-books",
            dataset_paths={"topbook": "missing.parquet"},
            schema_versions={"topbook": "topbook.v1"},
            row_counts={"topbook": 1},
        ),
    )

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert any("dataset does not exist" in error for error in report.all_errors)


def test_validate_run_manifest_reports_wrong_row_count_and_schema(tmp_path) -> None:
    dataset_path = tmp_path / "topbook.parquet"
    _write_topbook(dataset_path)
    manifest_path = write_manifest(
        tmp_path / "manifest.json",
        build_run_manifest(
            run_id="run-1",
            run_dir=tmp_path,
            started_at_utc="2026-05-26T00:00:00Z",
            ended_at_utc="2026-05-26T00:01:00Z",
            status="success",
            command="pmkt stream-books",
            dataset_paths={"topbook": "topbook.parquet"},
            schema_versions={"topbook": "depth.v1"},
            row_counts={"topbook": 2},
        ),
    )

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert any("row count mismatch: expected 2, got 1" in error for error in report.all_errors)
    assert any("schema depth.v1" in error for error in report.all_errors)


def test_validate_run_manifest_reports_stale_hash(tmp_path) -> None:
    dataset_path = tmp_path / "topbook.parquet"
    _write_topbook(dataset_path)
    manifest_path = write_manifest(
        tmp_path / "manifest.json",
        build_run_manifest(
            run_id="run-1",
            run_dir=tmp_path,
            started_at_utc="2026-05-26T00:00:00Z",
            ended_at_utc="2026-05-26T00:01:00Z",
            status="success",
            command="pmkt stream-books",
            dataset_paths={"topbook": "topbook.parquet"},
            schema_versions={"topbook": "topbook.v1"},
            row_counts={"topbook": 1},
            extra={"dataset_hashes": {"topbook": "0" * 64}},
        ),
    )

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert any("sha256 mismatch" in error for error in report.all_errors)


def test_validate_run_manifest_supports_single_output_collection_manifest(tmp_path) -> None:
    output_path = tmp_path / "markets.parquet"
    pd.DataFrame([{"market_id": "pm-1"}, {"market_id": "pm-2"}]).to_parquet(
        output_path,
        index=False,
    )
    manifest_path = write_manifest(
        tmp_path / "manifest.json",
        {
            "mode": "gamma_markets_keyset",
            "row_count": 2,
            "output_path": str(output_path),
        },
    )

    report = validate_run_manifest(manifest_path)

    assert report.ok, report.all_errors
    assert report.datasets[0].dataset_key == "output"
    assert report.datasets[0].row_count == 2
    assert report.all_warnings == ("output: schema version not declared",)


def test_validate_run_manifest_accepts_collection_manifest_with_schema_metadata(tmp_path) -> None:
    output_path = tmp_path / "markets.parquet"
    pd.DataFrame(
        [
            {
                "schema_version": POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
                "market_id": "pm-1",
                "event_id": None,
                "event_slug": None,
                "slug": "pm-1",
                "question": "Will it rain?",
                "open_time": None,
                "start_time": None,
                "close_time": None,
                "closed": False,
                "volume": None,
                "liquidity": None,
                "enable_orderbook": True,
                "token_ids": ["token-yes", "token-no"],
                "yes_bid": 0.4,
                "yes_ask": 0.6,
                "mid": 0.5,
                "spread": 0.2,
                "last_trade_price": 0.5,
                "condition_id": "0xabc",
                "question_id": "q-1",
                "outcome_labels_json": ["yes", "no"],
                "outcome_prices_json": ["0.4", "0.6"],
                "uma_resolution_status": None,
                "resolved_by": None,
                "resolution_source": None,
                "raw_json": "{}",
                "raw_json_sha256": "0" * 64,
            }
        ]
    ).to_parquet(output_path, index=False)
    manifest_path = write_manifest(
        tmp_path / "manifest.json",
        {
            "mode": "gamma_markets_keyset",
            "row_count": 1,
            "output_path": str(output_path),
            "dataset_paths": {"output": str(output_path)},
            "schema_versions": {
                "output": POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
            },
            "row_counts": {"output": 1},
        },
    )

    report = validate_run_manifest(manifest_path)

    assert report.ok, report.all_errors
    assert report.all_warnings == ()
    assert report.datasets[0].schema_version == POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION
    assert report.datasets[0].row_count == 1


def test_dataset_validate_manifest_cli_reports_ok_and_failures(tmp_path) -> None:
    dataset_path = tmp_path / "topbook.parquet"
    _write_topbook(dataset_path)
    valid_manifest_path = write_manifest(
        tmp_path / "valid_manifest.json",
        build_run_manifest(
            run_id="run-1",
            run_dir=tmp_path,
            started_at_utc="2026-05-26T00:00:00Z",
            ended_at_utc="2026-05-26T00:01:00Z",
            status="success",
            command="pmkt stream-books",
            dataset_paths={"topbook": "topbook.parquet"},
            schema_versions={"topbook": "topbook.v1"},
            row_counts={"topbook": 1},
        ),
    )
    invalid_manifest_path = write_manifest(
        tmp_path / "invalid_manifest.json",
        build_run_manifest(
            run_id="run-2",
            run_dir=tmp_path,
            started_at_utc="2026-05-26T00:00:00Z",
            ended_at_utc="2026-05-26T00:01:00Z",
            status="success",
            command="pmkt stream-books",
            dataset_paths={"topbook": "missing.parquet"},
            schema_versions={"topbook": "topbook.v1"},
            row_counts={"topbook": 1},
        ),
    )

    runner = CliRunner()
    valid = runner.invoke(app, ["dataset", "validate-manifest", str(valid_manifest_path)])
    invalid = runner.invoke(app, ["dataset", "validate-manifest", str(invalid_manifest_path)])

    assert valid.exit_code == 0
    assert "OK manifest: 1 datasets" in valid.output
    assert invalid.exit_code == 1
    assert "dataset does not exist" in invalid.output
