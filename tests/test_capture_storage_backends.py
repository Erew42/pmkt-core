from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

import pmkt.streaming.durability as durability_module
from pmkt.data.canonical import book_tape_control_row
from pmkt.data.manifests import validate_run_manifest
from pmkt.data.registry import (
    BOOK_TAPE_CONTROL_SCHEMA_VERSION,
    FEED_HEALTH_SCHEMA_VERSION,
    TOPBOOK_SCHEMA_VERSION,
)
from pmkt.data.schemas import topbook_evidence_id, topbook_row
from pmkt.exchanges.polymarket.order_book_stream import STREAM_DATASETS
from pmkt.streaming.supervisor import FeedShardHealth, LiveFeedSupervisor
from pmkt.streaming.datasets import merge_profile_dataset_specs
from pmkt.streaming.durability import RUN_STATE_NAME, DurableCaptureCoordinator
from pmkt.streaming.durability_settings import CaptureDurabilitySettings
from pmkt.streaming.profile_runtime import create_profile_runtime
from pmkt.streaming.profiles import resolve_dataset_specs, select_storage_profile
from pmkt.streaming.recovery import recover_stream_run
from pmkt.streaming.recovery_contracts import RunStateV1
from pmkt.streaming.sqlite_durability import (
    SQLiteCaptureCoordinator,
    inspect_sqlite_capture,
    promote_sqlite_capture,
)
from pmkt.streaming.storage_backends import (
    CAPTURE_STORAGE_FORMAT,
    CaptureStorageBackend,
    CaptureStorageSettings,
)

_UTC = "2026-08-25T10:00:00.000000Z"


def _sqlite_coordinator(tmp_path) -> SQLiteCaptureCoordinator:
    durability = CaptureDurabilitySettings.resolve(
        requested_segment_rows=100,
        requested_segment_seconds=30.0,
    )
    storage = CaptureStorageSettings.for_backend(CaptureStorageBackend.SQLITE_WAL)
    state = RunStateV1(
        run_id="run-1",
        profile_name="full",
        profile_version="2",
        expected_role_paths={"health": "datasets/health"},
        shard_plan={"shard-0": ["token-1"]},
        started_at_utc=_UTC,
        capture_durability=durability.to_mapping(),
        capture_storage=storage.to_mapping(),
    )
    return SQLiteCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={"health": FEED_HEALTH_SCHEMA_VERSION},
        segment_row_limit=100,
        commit_interval_seconds=30.0,
        durability_settings=durability,
    )


def _health_row(sequence: int = 1) -> dict[str, object]:
    supervisor = LiveFeedSupervisor(
        [
            FeedShardHealth(
                venue="polymarket",
                shard_id="shard-0",
                subscribed_instruments=("token-1",),
            )
        ]
    )
    return supervisor.feed_health_rows(
        now_monotonic_ns=1_000,
        observed_at_utc=_UTC,
        local_sequence=sequence,
    )[0]


def test_capture_storage_settings_are_strict_and_round_trip() -> None:
    parquet = CaptureStorageSettings.for_backend(CaptureStorageBackend.PARQUET_SEGMENTS)
    sqlite = CaptureStorageSettings.for_backend(CaptureStorageBackend.SQLITE_WAL)

    assert CaptureStorageSettings.from_mapping(parquet.to_mapping()) == parquet
    assert CaptureStorageSettings.from_mapping(sqlite.to_mapping()) == sqlite
    assert sqlite.to_mapping() == {
        "format": CAPTURE_STORAGE_FORMAT,
        "backend": "sqlite_wal_v1",
        "authoritative_path": "capture.sqlite3",
        "promotion_mode": "parquet_on_finalize",
    }

    with pytest.raises(ValueError, match="unknown capture storage fields"):
        CaptureStorageSettings.from_mapping({**parquet.to_mapping(), "extra": True})
    with pytest.raises(ValueError, match="canonical run-relative"):
        CaptureStorageSettings(
            backend=CaptureStorageBackend.SQLITE_WAL,
            authoritative_path="../capture.sqlite3",
            promotion_mode="parquet_on_finalize",
        )


def test_parquet_storage_manifest_reports_group_and_file_metrics(
    tmp_path, monkeypatch
) -> None:
    storage = CaptureStorageSettings.for_backend(CaptureStorageBackend.PARQUET_SEGMENTS)
    state = RunStateV1(
        run_id="run-1",
        profile_name="full",
        profile_version="2",
        expected_role_paths={"health": "datasets/health"},
        shard_plan={"shard-0": ["token-1"]},
        started_at_utc=_UTC,
        capture_storage=storage.to_mapping(),
    )
    coordinator = DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={"health": FEED_HEALTH_SCHEMA_VERSION},
        segment_row_limit=100,
    )
    serialized_values: list[object] = []
    canonical_json_bytes = durability_module.canonical_json_bytes

    def record_serialization(value):
        serialized_values.append(value)
        return canonical_json_bytes(value)

    monkeypatch.setattr(durability_module, "canonical_json_bytes", record_serialization)
    coordinator.add("health", _health_row())

    coordinator.finalize_segments()
    manifest = coordinator.storage_manifest()

    assert manifest["configuration"] == storage.to_mapping()
    metrics = manifest["metrics"]
    assert metrics["logical_groups_committed"] == 1
    assert metrics["cause_counts"] == {"clean_shutdown": 1}
    assert metrics["group_rows"]["total"] == 1.0
    assert metrics["canonical_input_bytes"]["sample_count"] == 0
    assert len(serialized_values) == 1
    assert isinstance(serialized_values[0], dict)
    assert "artifacts" in serialized_values[0]
    assert metrics["durable_files"]["total"] == 1.0
    assert metrics["durable_bytes"]["total"] > 0
    assert metrics["minimum_disk_free_bytes"] > 0
    assert metrics["commit_latency_ms_by_cause"]["clean_shutdown"]["sample_count"] == 1

    persisted = json.loads((tmp_path / RUN_STATE_NAME).read_text(encoding="utf-8"))
    assert persisted["capture_storage"] == storage.to_mapping()


def test_finalized_sqlite_recovery_uses_promoted_parquet_journal(tmp_path) -> None:
    coordinator = _sqlite_coordinator(tmp_path)
    coordinator.add("health", _health_row())
    coordinator.finalize_segments()
    coordinator.mark_finalized()

    report = recover_stream_run(
        tmp_path,
        payload_validation="integrity",
        artifact_roles={"health"},
    )

    assert report.state_status == "finalized"
    assert report.journal_version == "capture_commit_journal.v2"
    assert report.journal_errors == ()
    assert report.orphan_paths == ()
    assert report.valid_group_count == 1
    assert report.committed_role_counts == {"health": 1}
    assert len(report.validated_records) == 1


def test_sqlite_groups_are_durable_before_parquet_promotion(tmp_path) -> None:
    coordinator = _sqlite_coordinator(tmp_path)
    coordinator.add("health", _health_row())
    coordinator.commit(cause="invalidation", force=True)

    assert not list(tmp_path.rglob("*.parquet"))
    assert coordinator.database_path.exists()
    inspection = coordinator.inspect()
    assert inspection.errors == ()
    assert inspection.group_count == 1
    assert inspection.role_row_counts == {"health": 1}
    assert inspection.cause_counts == {"invalidation": 1}
    metrics = coordinator.storage_manifest()["metrics"]
    assert metrics["logical_groups_committed"] == 1
    assert metrics["database_bytes"] > 0
    assert metrics["wal_peak_bytes"] > 0
    coordinator.close()


def test_sqlite_post_commit_metrics_failure_does_not_replay_group(
    tmp_path, monkeypatch
) -> None:
    coordinator = _sqlite_coordinator(tmp_path)
    coordinator.add("health", _health_row(sequence=1))

    def fail_resource_metrics() -> None:
        raise OSError("simulated disk metrics failure")

    monkeypatch.setattr(coordinator, "_update_resource_metrics", fail_resource_metrics)
    coordinator.commit(cause="invalidation", force=True)
    coordinator.add("health", _health_row(sequence=2))
    coordinator.commit(cause="invalidation", force=True)

    inspection = coordinator.inspect()
    assert inspection.errors == ()
    assert inspection.group_count == 2
    assert inspection.role_row_counts == {"health": 2}
    metrics = coordinator.storage_manifest()["metrics"]
    assert metrics["logical_groups_committed"] == 2
    assert metrics["group_rows"]["total"] == 2.0
    assert metrics["resource_metrics"] == {
        "status": "degraded",
        "error_count": 2,
        "last_error": "OSError: simulated disk metrics failure",
    }
    coordinator.close()


def test_sqlite_crash_recovery_can_promote_without_existing_parquet(tmp_path) -> None:
    coordinator = _sqlite_coordinator(tmp_path)
    coordinator.add("health", _health_row())
    coordinator.commit(cause="termination", force=True)
    coordinator.close()

    report = recover_stream_run(tmp_path)
    assert report.journal_errors == ()
    assert report.valid_group_count == 1
    assert report.committed_role_counts == {"health": 1}
    assert not list(tmp_path.rglob("*.parquet"))

    state = RunStateV1.from_mapping(
        json.loads((tmp_path / RUN_STATE_NAME).read_text(encoding="utf-8"))
    )
    promoted = promote_sqlite_capture(tmp_path, state)
    promoted.close()
    assert len(list((tmp_path / "datasets/health").glob("*.parquet"))) == 1
    assert (tmp_path / "capture_commit_journal.v2.jsonl").exists()


def test_sqlite_clean_finalize_is_idempotently_promotable(tmp_path) -> None:
    coordinator = _sqlite_coordinator(tmp_path)
    coordinator.add("health", _health_row())
    coordinator.finalize_segments()

    assert coordinator.segments_finalized
    assert coordinator.row_counts == {"health": 1}
    artifact = coordinator.dataset_artifacts()["health"]
    first_hash = artifact["segment_manifest_hash"]
    parquet_path = next((tmp_path / artifact["path"]).glob("*.parquet"))
    parquet_hash = parquet_path.read_bytes()
    segment_manifest_path = tmp_path / artifact["segment_manifest_path"]
    coordinator.close()
    segment_manifest_path.unlink()

    resumed = promote_sqlite_capture(
        tmp_path,
        RunStateV1.from_mapping(
            json.loads((tmp_path / RUN_STATE_NAME).read_text(encoding="utf-8"))
        ),
    )
    assert resumed.dataset_artifacts()["health"]["segment_manifest_hash"] == first_hash
    assert parquet_path.read_bytes() == parquet_hash
    assert resumed.storage_manifest()["metrics"]["promotion"]["attempt_count"] == 1
    resumed.close()


def test_sqlite_promotion_preserves_recurring_decimal_evidence_identity(
    tmp_path,
) -> None:
    durability = CaptureDurabilitySettings.resolve(
        requested_segment_rows=100,
        requested_segment_seconds=30.0,
    )
    storage = CaptureStorageSettings.for_backend(CaptureStorageBackend.SQLITE_WAL)
    state = RunStateV1(
        run_id="run-1",
        profile_name="mm-compact",
        profile_version="2",
        expected_role_paths={
            "topbook_main": "datasets/topbook_main",
            "tape_control": "datasets/tape_control",
        },
        shard_plan={"shard-0": ["KX-1"]},
        started_at_utc=_UTC,
        capture_durability=durability.to_mapping(),
        capture_storage=storage.to_mapping(),
    )
    coordinator = SQLiteCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={
            "topbook_main": TOPBOOK_SCHEMA_VERSION,
            "tape_control": BOOK_TAPE_CONTROL_SCHEMA_VERSION,
        },
        segment_row_limit=100,
        commit_interval_seconds=30.0,
        durability_settings=durability,
    )
    topbook = topbook_row(
        collector_run_id="run-1",
        exchange="kalshi",
        venue_market_id="KX-1",
        instrument_id="KX-1",
        received_at_utc=_UTC,
        received_at_monotonic_ns=1,
        local_sequence=1,
        best_bid_dollars="0.055",
        best_ask_dollars="0.925",
        spread_bps="15963.302752293577",
        valid_state=True,
        quality_flags=[],
    )
    control = book_tape_control_row(
        collector_run_id="run-1",
        control_id="a" * 64,
        venue="kalshi",
        venue_market_id="KX-1",
        venue_book_id="KX-1",
        control_type="book_recovered",
        reason="topbook_validated",
        valid_after=True,
        received_at_utc=_UTC,
        received_at_monotonic_ns=1,
        local_sequence=1,
        subsequence=1,
        evidence_role="topbook_main",
        evidence_id=topbook_evidence_id(topbook),
        quality_flags_json="[]",
    )
    coordinator.add("topbook_main", topbook)
    coordinator.add("tape_control", control)

    coordinator.finalize_segments()
    coordinator.mark_finalized()
    report = recover_stream_run(tmp_path)

    assert report.journal_errors == ()
    assert report.valid_group_count == 1


def test_sqlite_recovery_rejects_payload_corruption(tmp_path) -> None:
    coordinator = _sqlite_coordinator(tmp_path)
    coordinator.add("health", _health_row())
    coordinator.commit(cause="invalidation", force=True)
    database_path = coordinator.database_path
    state = coordinator.state
    coordinator.close()

    connection = sqlite3.connect(database_path)
    connection.execute(
        "UPDATE capture_rows SET payload = ? WHERE group_index = 0",
        (b'{"corrupt":true}',),
    )
    connection.commit()
    connection.close()

    inspection = inspect_sqlite_capture(tmp_path, state)
    assert inspection.group_count == 0
    assert inspection.errors


def test_sqlite_group_transaction_rolls_back_and_can_retry(tmp_path) -> None:
    coordinator = _sqlite_coordinator(tmp_path)
    coordinator.add("health", _health_row())
    blocker = sqlite3.connect(coordinator.database_path)
    blocker.execute(
        """
        CREATE TRIGGER reject_capture_row BEFORE INSERT ON capture_rows
        BEGIN
            SELECT RAISE(ABORT, 'injected row failure');
        END
        """
    )
    blocker.commit()
    blocker.close()

    with pytest.raises(sqlite3.IntegrityError, match="injected row failure"):
        coordinator.commit(cause="termination", force=True)
    inspection = coordinator.inspect()
    assert inspection.errors == ()
    assert inspection.group_count == 0
    assert coordinator.has_pending_rows

    unblocker = sqlite3.connect(coordinator.database_path)
    unblocker.execute("DROP TRIGGER reject_capture_row")
    unblocker.commit()
    unblocker.close()
    coordinator.commit(cause="termination", force=True)
    assert coordinator.inspect().group_count == 1
    coordinator.close()


def test_sqlite_promotion_failure_retains_recoverable_authority(
    tmp_path, monkeypatch
) -> None:
    coordinator = _sqlite_coordinator(tmp_path)
    coordinator.add("health", _health_row())

    def fail_promotion() -> None:
        raise OSError("injected promotion failure")

    monkeypatch.setattr(coordinator, "_write_promoted_parquet", fail_promotion)
    with pytest.raises(OSError, match="injected promotion failure"):
        coordinator.finalize_segments()
    metrics = coordinator.storage_manifest()["metrics"]
    assert metrics["promotion"]["attempt_count"] == 1
    assert metrics["promotion"]["failure_count"] == 1
    assert metrics["unpromoted_sealed_database_count"] == 1
    assert not (tmp_path / "capture_commit_journal.v2.jsonl").exists()
    state = coordinator.state
    coordinator.close()

    inspection = inspect_sqlite_capture(tmp_path, state)
    assert inspection.errors == ()
    assert inspection.group_count == 1
    promoted = promote_sqlite_capture(tmp_path, state)
    assert promoted.storage_manifest()["metrics"]["promotion"]["attempt_count"] == 2
    assert promoted.storage_manifest()["metrics"]["promotion"]["failure_count"] == 1
    promoted.close()


def test_full_profile_crash_recovery_restores_legacy_arrow_schemas(tmp_path) -> None:
    selection = select_storage_profile("full")
    specs = resolve_dataset_specs(
        selection,
        merge_profile_dataset_specs(STREAM_DATASETS),
    )
    durability = CaptureDurabilitySettings.resolve(
        requested_segment_rows=100,
        requested_segment_seconds=30.0,
    )
    runtime = create_profile_runtime(
        run_dir=tmp_path,
        selection=selection,
        specs=specs,
        shard_plan={"shard-0": ["token-1"]},
        adapter_settings_by_venue={"polymarket": {}},
        started_at_utc=_UTC,
        durability_settings=durability,
        storage_backend=CaptureStorageBackend.SQLITE_WAL,
    )
    runtime.coordinator.add("health", _health_row())
    runtime.coordinator.commit(cause="termination", force=True)
    assert isinstance(runtime.coordinator, SQLiteCaptureCoordinator)
    runtime.coordinator.close()

    recovered = recover_stream_run(tmp_path, finalize=True)

    assert recovered.finalized_manifest_path is not None
    manifest_path = tmp_path / "run_manifest.v1.json"
    assert Path(recovered.finalized_manifest_path) == manifest_path
    validation = validate_run_manifest(manifest_path)
    assert validation.ok, validation.all_errors
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "partial"
    assert manifest["capture_storage"]["configuration"]["backend"] == ("sqlite_wal_v1")
    assert manifest["capture_storage"]["metrics"]["promotion"]["attempt_count"] == 1
    assert all(
        len(list((tmp_path / artifact["path"]).glob("*.parquet"))) == 1
        for artifact in manifest["dataset_artifacts"].values()
    )
