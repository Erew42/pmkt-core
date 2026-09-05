from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import pmkt.data.manifests as manifests_module
import pmkt.data.validation as validation_module

from pmkt.data.canonical import (
    book_tape_control_row,
    book_tape_event_row,
    book_tape_level_row,
    stream_lifecycle_row,
    trade_row,
)
from pmkt.data.manifests import (
    build_run_manifest,
    validate_run_manifest,
    write_manifest,
)
from pmkt.data.registry import arrow_schema, get_table_spec
from pmkt.data.schemas import topbook_evidence_id, topbook_row
from pmkt.data.validation import (
    validate_book_control_evidence,
    validate_book_tape_bundle,
    validate_frame,
)
from pmkt.streaming.capture import CaptureRouter, CaptureWriteIntent
from pmkt.streaming.collector import StreamDatasetSpec, StreamRunOutputs
from pmkt.streaming.datasets import merge_profile_dataset_specs
from pmkt.streaming.profiles import (
    PROFILE_DEFINITIONS,
    PROFILE_DEFINITIONS_BY_VERSION,
    get_storage_profile_definition,
    DatasetRole,
    StorageProfileOverrides,
    resolve_dataset_specs,
    select_storage_profile,
)
from pmkt.streaming.recovery_contracts import (
    CaptureCommitArtifactV1,
    CaptureCommitRecordV1,
    RunStateV1,
)

_UTC = "2026-07-19T10:00:00.000000Z"
_EVENT_ID = "1" * 64
_EPOCH_ID = "2" * 64


def _file_sha256(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _refresh_artifact_segment_manifest(artifacts, tmp_path, role: str) -> None:
    entry = artifacts[role]
    artifact_path = tmp_path / entry["path"]
    if artifact_path.is_dir():
        segment_manifest_path = artifact_path / "_segments.json"
        segment_paths = sorted(
            path
            for path in artifact_path.iterdir()
            if path.is_file() and path != segment_manifest_path
        )
    else:
        segment_manifest_path = artifact_path.with_name(
            f"{artifact_path.name}.segments.json"
        )
        segment_paths = [artifact_path]
    completed_segments = []
    for index, segment_path in enumerate(segment_paths):
        completed_segments.append(
            {
                "index": index,
                "path": segment_path.name,
                "row_count": len(pd.read_parquet(segment_path)),
                "sha256": _file_sha256(segment_path),
            }
        )
    payload = {
        "format": "pmkt.capture_segments.v1",
        "status": "closed",
        "row_count": sum(item["row_count"] for item in completed_segments),
        "completed_segments": completed_segments,
        "journal_path": "capture_commit_journal.v1.jsonl",
    }
    segment_manifest_path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    entry["segment_manifest_path"] = segment_manifest_path.relative_to(
        tmp_path
    ).as_posix()
    entry["segment_manifest_hash"] = _file_sha256(segment_manifest_path)


def _event(**overrides):
    values = {
        "collector_run_id": "run-1",
        "event_id": _EVENT_ID,
        "venue": "polymarket",
        "venue_market_id": "market-1",
        "venue_book_id": "token-1",
        "event_kind": "checkpoint",
        "epoch_id": _EPOCH_ID,
        "checkpoint_reason": "startup",
        "received_at_utc": _UTC,
        "received_at_monotonic_ns": 100,
        "local_sequence": 1,
        "subsequence": 0,
        "expected_level_row_count": 1,
        "side_counts_json": '{"ask":0,"bid":1}',
        "valid_state": True,
        "reconstructible": True,
        "quality_flags_json": "[]",
        "event_payload_hash": "3" * 64,
        "encoding_version": "book-tape.v1",
    }
    values.update(overrides)
    return book_tape_event_row(**values)


def _level(**overrides):
    values = {
        "collector_run_id": "run-1",
        "event_id": _EVENT_ID,
        "venue": "polymarket",
        "venue_book_id": "token-1",
        "epoch_id": _EPOCH_ID,
        "source_side": "bid",
        "price_key": "0.4",
        "price_dollars": 0.4,
        "size_after_contracts": 3.0,
        "level_ordinal": 0,
    }
    values.update(overrides)
    return book_tape_level_row(**values)


def _control() -> dict[str, object]:
    return book_tape_control_row(
        collector_run_id="run-1",
        control_id="4" * 64,
        venue="polymarket",
        venue_market_id="market-1",
        venue_book_id="token-1",
        control_type="book_recovered",
        reason="startup_snapshot",
        valid_after=True,
        received_at_utc=_UTC,
        received_at_monotonic_ns=100,
        local_sequence=1,
        subsequence=1,
        epoch_id=_EPOCH_ID,
        evidence_role="tape_event",
        evidence_id=_EVENT_ID,
        quality_flags_json="[]",
    )


def _lifecycle() -> dict[str, object]:
    return stream_lifecycle_row(
        collector_run_id="run-1",
        lifecycle_event_id="5" * 64,
        venue="kalshi",
        venue_market_id="KX-1",
        event_type="activated",
        received_at_utc=_UTC,
        received_at_monotonic_ns=10,
        local_sequence=1,
        subsequence=0,
        quality_flags_json="[]",
    )


def _trade() -> dict[str, object]:
    return trade_row(
        collector_run_id="run-1",
        venue="polymarket",
        venue_trade_id="trade-1",
        venue_market_id="market-1",
        instrument_id="token-1",
        outcome="YES",
        trade_ts_utc=_UTC,
        received_at_utc=_UTC,
        received_at_monotonic_ns=1,
        local_sequence=1,
        subsequence=0,
        price_dollars=0.5,
        size_contracts=2.0,
        notional_dollars=1.0,
        aggressor_side="buy",
        raw_json="{}",
        raw_json_sha256=hashlib.sha256(b"{}").hexdigest(),
    )


def _write_complete_profile_manifest(tmp_path):
    selection = select_storage_profile("mm-compact", profile_version="1")
    artifacts = {}
    for role in sorted(selection.enabled_roles, key=lambda item: item.value):
        assert role is not DatasetRole.RAW_JSONL
        versions = selection.definition.role_schema_versions[role]
        assert len(versions) == 1
        schema_version = next(iter(versions))
        spec = get_table_spec(schema_version)
        path = tmp_path / f"{role.value}.parquet"
        pq.write_table(
            pa.Table.from_pylist([], schema=arrow_schema(spec)),
            path,
        )
        artifacts[role.value] = {
            "path": path.name,
            "dataset_key": spec.name,
            "schema_version": schema_version,
            "row_count": 0,
            "segment_manifest_path": None,
            "segment_manifest_hash": None,
            "completion_status": "closed",
        }
    for role in artifacts:
        _refresh_artifact_segment_manifest(artifacts, tmp_path, role)
    profile = {
        **selection.to_manifest_mapping(),
        "successfully_committed_roles": sorted(artifacts),
        "terminal_completeness": "complete",
    }
    manifest = build_run_manifest(
        run_id="run-1",
        run_dir=tmp_path,
        started_at_utc=_UTC,
        ended_at_utc=_UTC,
        status="success",
        command="fixture",
        dataset_paths={"topbook": "wrong-legacy-alias.parquet"},
        schema_versions={"topbook": "depth.v1"},
        row_counts={"topbook": 999},
        extra={"dataset_artifacts": artifacts, "storage_profile": profile},
    )
    manifest_path = write_manifest(tmp_path / "manifest.json", manifest)
    return manifest, manifest_path


def test_cr18_schemas_are_registered_with_exact_arrow_types() -> None:
    event_spec = get_table_spec("book_tape_event.v1")
    event_fields = {field.name: field for field in event_spec.fields}
    event_arrow = arrow_schema(event_spec)

    assert event_spec.primary_key == ("collector_run_id", "event_id")
    assert event_fields["subsequence"].dtype == "int32"
    assert event_arrow.field("subsequence").type == pa.int32()
    assert event_arrow.field("quality_flags_json").type == pa.large_string()
    assert get_table_spec("book_tape_level.v1").primary_key == (
        "collector_run_id",
        "event_id",
        "source_side",
        "price_key",
    )
    assert get_table_spec("book_tape_control.v1").primary_key == (
        "collector_run_id",
        "control_id",
    )
    assert get_table_spec("stream_lifecycle.v1").primary_key == (
        "collector_run_id",
        "lifecycle_event_id",
    )


def test_tape_rows_strict_validate_and_bundle_resolves_foreign_keys() -> None:
    events = pd.DataFrame([_event()])
    levels = pd.DataFrame([_level()])
    control = book_tape_control_row(
        collector_run_id="run-1",
        control_id="4" * 64,
        venue="polymarket",
        venue_market_id="market-1",
        venue_book_id="token-1",
        control_type="book_recovered",
        reason="startup_snapshot",
        valid_after=True,
        received_at_utc=_UTC,
        received_at_monotonic_ns=100,
        local_sequence=1,
        subsequence=1,
        epoch_id=_EPOCH_ID,
        evidence_role="tape_event",
        evidence_id=_EVENT_ID,
        quality_flags_json="[]",
    )
    controls = pd.DataFrame([control])

    assert validate_frame(events, "book_tape_event.v1", strict=True).ok
    assert validate_frame(levels, "book_tape_level.v1", strict=True).ok
    assert validate_frame(controls, "book_tape_control.v1", strict=True).ok
    assert validate_book_tape_bundle(events, levels, controls).ok


def test_tape_bundle_accepts_pandas_string_identity_columns() -> None:
    events = pd.DataFrame([_event()])
    levels = pd.DataFrame([_level()])
    for frame in (events, levels):
        frame["collector_run_id"] = frame["collector_run_id"].astype("string")
        frame["event_id"] = frame["event_id"].astype("string")

    report = validate_book_tape_bundle(events, levels)

    assert report.ok, report.errors


def test_tape_bundle_rejects_non_string_relational_identity_before_join() -> None:
    events = pd.DataFrame([_event()])
    levels = pd.DataFrame([_level()])
    events["collector_run_id"] = pd.Series([1], dtype="object")
    levels["collector_run_id"] = pd.Series([1.0], dtype="object")

    report = validate_book_tape_bundle(events, levels)

    assert report.errors == (
        "book_tape_event.v1: collector_run_id: 1 values must be strings",
        "book_tape_level.v1: collector_run_id: 1 values must be strings",
    )


def test_tape_bundle_skips_side_aggregation_without_checkpoints(monkeypatch) -> None:
    event = _event(
        event_kind="delta",
        checkpoint_reason=None,
        side_counts_json=None,
    )

    original_assign = pd.DataFrame.assign
    original_itertuples = pd.DataFrame.itertuples

    def guarded_assign(frame, **kwargs):
        if "source_side" in kwargs:
            raise AssertionError("side aggregation ran without checkpoints")
        return original_assign(frame, **kwargs)

    def guarded_itertuples(frame, *args, **kwargs):
        if list(frame.columns) == ["collector_run_id", "event_id", "source_side"]:
            raise AssertionError("side aggregation ran without checkpoints")
        return original_itertuples(frame, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "assign", guarded_assign)
    monkeypatch.setattr(pd.DataFrame, "itertuples", guarded_itertuples)

    report = validate_book_tape_bundle(
        pd.DataFrame([event]),
        pd.DataFrame([_level()]),
    )

    assert report.ok, report.errors


@pytest.mark.parametrize(
    ("control_type", "valid_after", "evidence_role", "expected_error"),
    [
        ("book_recovered", True, "arbitrary_role", "evidence_role"),
        ("stream_ended", True, None, "stream_ended must close valid state"),
    ],
)
def test_tape_controls_close_evidence_and_terminal_invariants(
    control_type: str,
    valid_after: bool,
    evidence_role: str | None,
    expected_error: str,
) -> None:
    row = book_tape_control_row(
        collector_run_id="run-1",
        control_id="8" * 64,
        venue="polymarket",
        venue_market_id="market-1",
        venue_book_id="token-1",
        control_type=control_type,
        reason="test",
        valid_after=valid_after,
        received_at_utc=_UTC,
        received_at_monotonic_ns=101,
        local_sequence=1,
        subsequence=1,
        epoch_id=_EPOCH_ID,
        evidence_role=evidence_role,
        evidence_id=_EVENT_ID if evidence_role is not None else None,
        quality_flags_json="[]",
    )

    report = validate_frame(pd.DataFrame([row]), "book_tape_control.v1", strict=True)

    assert not report.ok
    assert any(expected_error in error for error in report.errors)


@pytest.mark.parametrize(
    ("event_overrides", "control_overrides", "expected_error"),
    [
        ({}, {"venue": "kalshi"}, "venue disagrees"),
        ({}, {"venue_market_id": "other-market"}, "venue_market_id disagrees"),
        ({}, {"venue_book_id": "other-book"}, "venue_book_id disagrees"),
        ({}, {"epoch_id": "9" * 64}, "epoch_id disagrees"),
        ({}, {"received_at_utc": "2026-07-19T10:00:01Z"}, "received_at_utc disagrees"),
        ({}, {"received_at_monotonic_ns": 999}, "received_at_monotonic_ns disagrees"),
        ({}, {"exchange_at_utc": _UTC}, "exchange_at_utc disagrees"),
        ({}, {"local_sequence": 2}, "local_sequence disagrees"),
        ({}, {"subsequence": 2}, "subsequence disagrees"),
        ({}, {"venue_sequence": 9}, "venue_sequence disagrees"),
        ({"valid_state": False}, {}, "must have valid state"),
        ({"reconstructible": False}, {}, "must be reconstructible"),
    ],
)
def test_tape_recovery_evidence_must_match_a_valid_checkpoint(
    event_overrides: dict[str, object],
    control_overrides: dict[str, object],
    expected_error: str,
) -> None:
    event = _event(**event_overrides)
    control_values = {
        "collector_run_id": "run-1",
        "control_id": "4" * 64,
        "venue": "polymarket",
        "venue_market_id": "market-1",
        "venue_book_id": "token-1",
        "control_type": "book_recovered",
        "reason": "startup_snapshot",
        "valid_after": True,
        "received_at_utc": _UTC,
        "received_at_monotonic_ns": 100,
        "local_sequence": 1,
        "subsequence": 1,
        "epoch_id": _EPOCH_ID,
        "evidence_role": "tape_event",
        "evidence_id": _EVENT_ID,
        "quality_flags_json": "[]",
    }
    control_values.update(control_overrides)
    report = validate_book_tape_bundle(
        pd.DataFrame([event]),
        pd.DataFrame([_level()]),
        pd.DataFrame([book_tape_control_row(**control_values)]),
    )

    assert not report.ok
    assert any(expected_error in error for error in report.errors)


def _timestamp_bound_evidence_report(
    role: str,
    *,
    control_received: object = "2026-07-19T10:00:00.123456789Z",
    evidence_received: object = "2026-07-19T10:00:00.123456789Z",
    control_exchange: object = "2026-07-19T09:59:59.987654321Z",
    evidence_exchange: object = "2026-07-19T09:59:59.987654321Z",
):
    if role == "tape_event":
        event = _event(
            received_at_utc=evidence_received,
            exchange_at_utc=evidence_exchange,
        )
        control = book_tape_control_row(
            collector_run_id="run-1",
            control_id="4" * 64,
            venue="polymarket",
            venue_market_id="market-1",
            venue_book_id="token-1",
            control_type="book_recovered",
            reason="startup_snapshot",
            valid_after=True,
            received_at_utc=control_received,
            received_at_monotonic_ns=100,
            exchange_at_utc=control_exchange,
            local_sequence=1,
            subsequence=1,
            epoch_id=_EPOCH_ID,
            evidence_role=role,
            evidence_id=_EVENT_ID,
            quality_flags_json="[]",
        )
        return validate_book_tape_bundle(
            pd.DataFrame([event]),
            pd.DataFrame([_level()]),
            pd.DataFrame([control]),
        )

    topbook, control = _topbook_recovery_rows(role=role)
    topbook["received_at_utc"] = evidence_received
    topbook["exchange_ts_utc"] = evidence_exchange
    control["received_at_utc"] = control_received
    control["exchange_at_utc"] = control_exchange
    control["evidence_id"] = topbook_evidence_id(topbook)
    return validate_book_control_evidence(
        pd.DataFrame([control]),
        **{role: pd.DataFrame([topbook])},
    )


@pytest.mark.parametrize("role", ["tape_event", "topbook_main", "topbook_checkpoint"])
def test_causal_timestamps_compare_as_exact_utc_instants(role: str) -> None:
    equivalent_spelling = _timestamp_bound_evidence_report(
        role,
        control_received="2026-07-19T10:00:00.123456789+00:00",
        control_exchange="2026-07-19T09:59:59.987654321+00:00",
    )
    equivalent_fraction_width = _timestamp_bound_evidence_report(
        role,
        control_received="2026-07-19T10:00:00.100000000Z",
        evidence_received="2026-07-19T10:00:00.1+00:00",
        control_exchange="2026-07-19T09:59:59Z",
        evidence_exchange="2026-07-19T09:59:59.000000000+00:00",
    )

    assert equivalent_spelling.ok, equivalent_spelling.errors
    assert equivalent_fraction_width.ok, equivalent_fraction_width.errors


@pytest.mark.parametrize("role", ["tape_event", "topbook_main", "topbook_checkpoint"])
@pytest.mark.parametrize(
    ("overrides", "expected_error"),
    [
        (
            {"control_received": "2026-07-19T10:00:00.123456788Z"},
            "received_at_utc disagrees",
        ),
        (
            {"control_exchange": "2026-07-19T09:59:59.987654320Z"},
            "exchange_at_utc disagrees",
        ),
        (
            {
                "control_received": "not-a-timestamp",
                "evidence_received": "not-a-timestamp",
            },
            "explicit UTC timestamp",
        ),
        (
            {
                "control_received": "2026-07-19T10:00:00.123456789",
                "evidence_received": "2026-07-19T10:00:00.123456789",
            },
            "explicit UTC timestamp",
        ),
        (
            {
                "control_received": "2026-07-19T10:00:00.1234567890Z",
                "evidence_received": "2026-07-19T10:00:00.1234567890Z",
            },
            "explicit UTC timestamp",
        ),
        (
            {
                "control_received": "2026-07-19T12:00:00.123456789+02:00",
                "evidence_received": "2026-07-19T12:00:00.123456789+02:00",
            },
            "explicit UTC timestamp",
        ),
        (
            {"control_received": None, "evidence_received": None},
            "null values",
        ),
        (
            {"control_exchange": "", "evidence_exchange": ""},
            "explicit UTC timestamp",
        ),
    ],
)
def test_causal_timestamp_validation_fails_closed(
    role: str,
    overrides: dict[str, object],
    expected_error: str,
) -> None:
    report = _timestamp_bound_evidence_report(role, **overrides)

    assert not report.ok
    assert any(expected_error in error for error in report.errors)


@pytest.mark.parametrize("role", ["tape_event", "topbook_main", "topbook_checkpoint"])
def test_optional_exchange_timestamps_have_explicit_null_semantics(role: str) -> None:
    both_null = _timestamp_bound_evidence_report(
        role,
        control_exchange=None,
        evidence_exchange=None,
    )
    control_only = _timestamp_bound_evidence_report(
        role,
        control_exchange="2026-07-19T09:59:59Z",
        evidence_exchange=None,
    )
    evidence_only = _timestamp_bound_evidence_report(
        role,
        control_exchange=None,
        evidence_exchange="2026-07-19T09:59:59Z",
    )

    assert both_null.ok, both_null.errors
    assert not control_only.ok
    assert any("exchange_at_utc disagrees" in error for error in control_only.errors)
    assert not evidence_only.ok
    assert any("exchange_at_utc disagrees" in error for error in evidence_only.errors)


def test_tape_validation_rejects_incomplete_or_wrong_venue_evidence() -> None:
    bad_event = _event(side_counts_json='{"ask":0,"bid":0}')
    bad_level = _level(source_side="yes")

    event_report = validate_frame(
        pd.DataFrame([bad_event]), "book_tape_event.v1", strict=True
    )
    level_report = validate_frame(
        pd.DataFrame([bad_level]), "book_tape_level.v1", strict=True
    )
    bundle = validate_book_tape_bundle(
        pd.DataFrame(
            [_event(expected_level_row_count=2, side_counts_json='{"ask":0,"bid":2}')]
        ),
        pd.DataFrame([_level()]),
    )

    assert not event_report.ok
    assert any("side counts" in error for error in event_report.errors)
    assert not level_report.ok
    assert any("does not allow" in error for error in level_report.errors)
    assert not bundle.ok
    assert any("expected 2 levels" in error for error in bundle.errors)


def test_tape_level_rejects_noncanonical_price_key_spelling() -> None:
    report = validate_frame(
        pd.DataFrame([_level(price_key="0.40")]),
        "book_tape_level.v1",
        strict=True,
    )

    assert not report.ok
    assert any("price_key" in error for error in report.errors)


def test_stream_lifecycle_enforces_venue_event_mapping() -> None:
    row = stream_lifecycle_row(
        collector_run_id="run-1",
        lifecycle_event_id="5" * 64,
        venue="kalshi",
        venue_market_id="KX-1",
        event_type="activated",
        received_at_utc=_UTC,
        received_at_monotonic_ns=10,
        local_sequence=1,
        subsequence=0,
        quality_flags_json="[]",
    )
    assert validate_frame(pd.DataFrame([row]), "stream_lifecycle.v1", strict=True).ok
    row["event_type"] = "new_market"
    report = validate_frame(pd.DataFrame([row]), "stream_lifecycle.v1", strict=True)
    assert not report.ok
    assert any("kalshi does not allow" in error for error in report.errors)


@pytest.mark.parametrize(
    ("schema_version", "row", "column"),
    [
        *[
            ("book_tape_event.v1", _event(), column)
            for column in (
                "event_kind",
                "epoch_id",
                "checkpoint_reason",
                "side_counts_json",
                "reconstructible",
            )
        ],
        *[
            ("book_tape_level.v1", _level(), column)
            for column in ("venue", "source_side", "price_key", "price_dollars")
        ],
        *[
            ("book_tape_control.v1", _control(), column)
            for column in (
                "control_type",
                "valid_after",
                "evidence_role",
                "evidence_id",
            )
        ],
        *[
            ("stream_lifecycle.v1", _lifecycle(), column)
            for column in ("venue", "event_type")
        ],
        *[
            ("trade.v1", _trade(), column)
            for column in (
                "collector_run_id",
                "received_at_monotonic_ns",
                "local_sequence",
                "subsequence",
            )
        ],
    ],
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_vectorized_invariant_hooks_report_missing_columns_without_key_error(
    schema_version: str,
    row: dict[str, object],
    column: str,
) -> None:
    frame = pd.DataFrame([row]).drop(columns=[column])

    report = validate_frame(frame, schema_version, strict=True)

    assert column in report.missing_columns
    assert not report.ok


def test_vectorized_event_invariants_preserve_base_per_row_error_order() -> None:
    frame = pd.DataFrame(
        [
            _event(
                epoch_id=None,
                checkpoint_reason=None,
                side_counts_json=None,
            ),
            _event(
                event_id="9" * 64,
                event_kind="delta",
                epoch_id=None,
                checkpoint_reason="periodic",
                side_counts_json='{"ask":0,"bid":1}',
            ),
        ]
    )

    errors = validation_module._book_tape_event_invariant_errors(frame)

    assert errors[-7:] == [
        "epoch_id: checkpoint requires an epoch id",
        "checkpoint_reason: checkpoint requires a reason",
        "side_counts_json: checkpoint requires explicit side counts",
        "epoch_id: reconstructible event requires an open epoch",
        "checkpoint_reason: delta must not declare a checkpoint reason",
        "side_counts_json: delta must not declare checkpoint side counts",
        "epoch_id: reconstructible event requires an open epoch",
    ]


def test_storage_profiles_are_additive_and_fail_closed() -> None:
    full = select_storage_profile("full")
    compact = select_storage_profile(
        "mm-compact",
        overrides=StorageProfileOverrides(
            keep_raw_jsonl=True,
            emit_full_depth=True,
            emit_legacy_book_artifacts=True,
        ),
    )

    assert full.definition.contract_status.value == "stable"
    assert DatasetRole.TAPE_EVENT in full.required_roles
    assert DatasetRole.TAPE_EVENT not in compact.enabled_roles
    assert DatasetRole.RAW_JSONL in compact.enabled_roles
    assert DatasetRole.DEPTH_MAIN in compact.enabled_roles
    assert DatasetRole.LEGACY_LEVEL in compact.enabled_roles
    with pytest.raises(ValueError, match="unknown storage profile"):
        select_storage_profile("missing")
    with pytest.raises(ValueError, match="finite and > 0"):
        select_storage_profile("full", feed_health_interval_seconds=float("nan"))


def test_profile_authority_is_deeply_immutable_and_schema_exact() -> None:
    full = select_storage_profile("full")
    with pytest.raises(TypeError):
        PROFILE_DEFINITIONS["replacement"] = full.definition  # type: ignore[index]
    with pytest.raises(TypeError):
        full.definition.role_schema_versions[DatasetRole.HEALTH] = frozenset(  # type: ignore[index]
            {"trade.v1"}
        )

    from pmkt.exchanges.polymarket.order_book_stream import STREAM_DATASETS

    selection = select_storage_profile("mm-compact")
    specs = list(merge_profile_dataset_specs(STREAM_DATASETS))
    health_index = next(
        index
        for index, spec in enumerate(specs)
        if spec.role == DatasetRole.HEALTH.value
    )
    health = specs[health_index]

    specs[health_index] = replace(health, schema_version="trade.v1")
    with pytest.raises(ValueError, match="schema version"):
        resolve_dataset_specs(selection, specs)

    specs[health_index] = replace(
        health,
        schema=arrow_schema(get_table_spec("trade.v1")),
    )
    with pytest.raises(ValueError, match="Arrow schema"):
        resolve_dataset_specs(selection, specs)


def test_both_venue_catalogs_resolve_every_named_profile_role() -> None:
    from pmkt.exchanges.kalshi.order_book_stream import (
        STREAM_DATASETS as KALSHI_STREAM_DATASETS,
    )
    from pmkt.exchanges.polymarket.order_book_stream import (
        STREAM_DATASETS as POLYMARKET_STREAM_DATASETS,
    )

    for venue_specs in (POLYMARKET_STREAM_DATASETS, KALSHI_STREAM_DATASETS):
        complete_specs = merge_profile_dataset_specs(venue_specs)
        for name in ("full", "book-tape", "mm-compact"):
            selection = select_storage_profile(name)
            resolved = resolve_dataset_specs(selection, complete_specs)
            assert {spec.role for spec in resolved} == {
                role.value
                for role in selection.enabled_roles
                if role is not DatasetRole.RAW_JSONL
            }


@pytest.mark.asyncio
async def test_capture_router_noops_disabled_roles_and_tracks_exact_outputs(
    tmp_path,
) -> None:
    selection = select_storage_profile("mm-compact")
    schema = pa.schema([("value", pa.string())])
    specs = tuple(
        StreamDatasetSpec(
            file_key=role.value,
            filename=f"{role.value}.parquet",
            schema=schema,
            role=role.value,
        )
        for role in sorted(
            selection.enabled_roles - {DatasetRole.RAW_JSONL},
            key=lambda item: item.value,
        )
    )
    outputs = StreamRunOutputs(
        run_dir=tmp_path,
        datasets=specs,
        include_raw_jsonl=False,
        parquet_segment_rows=None,
        parquet_segment_seconds=None,
    )
    router = CaptureRouter(selection=selection, outputs=outputs)

    async with router:
        assert not await router.write(
            CaptureWriteIntent(DatasetRole.DEPTH_MAIN, {"value": "ignored"})
        )
        assert await router.write_health({"value": "health"})

    assert "raw_events_jsonl" not in outputs.files
    assert set(outputs.dataset_specs_by_role) == {
        role.value for role in selection.enabled_roles
    }
    assert router.completeness().complete
    assert router.completeness().row_counts == {"health": 1}


def test_recovery_json_contracts_are_strict_and_checksummed() -> None:
    state = RunStateV1(
        run_id="run-1",
        profile_name="book-tape",
        profile_version="1",
        expected_role_paths={"tape_event": "book_tape_event.parquet"},
        shard_plan={"kalshi-0": ["KX-1"]},
        started_at_utc=_UTC,
    )
    assert RunStateV1.from_mapping(state.to_mapping()) == state

    artifact = CaptureCommitArtifactV1(
        role="tape_event",
        path="book_tape_event.parquet/part-000000.parquet",
        sha256="6" * 64,
        row_count=1,
        first_local_sequence=1,
        last_local_sequence=1,
    )
    record = CaptureCommitRecordV1.create(
        group_id="7" * 64,
        committed_at_utc=_UTC,
        artifacts=[artifact],
    )
    assert CaptureCommitRecordV1.from_mapping(record.to_mapping()) == record
    tampered = record.to_mapping()
    tampered["artifacts"][0]["row_count"] = 2
    with pytest.raises(ValueError, match="checksum"):
        CaptureCommitRecordV1.from_mapping(tampered)


@pytest.mark.parametrize(
    "unsafe_path",
    ("../escape", "/absolute/path", "C:/escape", r"C:\\escape", r"\\server\share\x"),
)
def test_recovery_contracts_reject_paths_outside_run(unsafe_path: str) -> None:
    with pytest.raises(ValueError, match="canonical relative path"):
        RunStateV1(
            run_id="run-1",
            profile_name="full",
            profile_version="1",
            expected_role_paths={"health": unsafe_path},
            shard_plan={},
            started_at_utc=_UTC,
        )
    with pytest.raises(ValueError, match="canonical relative path"):
        CaptureCommitArtifactV1(
            role="health",
            path=unsafe_path,
            sha256="6" * 64,
            row_count=0,
            first_local_sequence=0,
            last_local_sequence=0,
        )


def test_named_profile_manifest_cannot_self_authorize_reduced_roles(tmp_path) -> None:
    paths = {}
    artifacts = {}
    for role, sequence in (("topbook_main", 1), ("topbook_checkpoint", 2)):
        path = tmp_path / f"{role}.parquet"
        pd.DataFrame(
            [
                topbook_row(
                    collector_run_id="run-1",
                    exchange="polymarket",
                    instrument_id="token-1",
                    received_at_utc=f"2026-07-19T10:00:0{sequence}Z",
                    local_sequence=sequence,
                    valid_state=True,
                    quality_flags=[],
                )
            ]
        ).to_parquet(path, index=False)
        paths[role] = str(path)
        artifacts[role] = {
            "path": path.name,
            "dataset_key": "topbook",
            "schema_version": "topbook.v1",
            "row_count": 1,
            "segment_manifest_path": None,
            "segment_manifest_hash": None,
            "completion_status": "closed",
        }
    profile = {
        "name": "book-tape",
        "profile_version": "1",
        "required_roles": list(artifacts),
        "enabled_roles": list(artifacts),
        "disabled_roles": [],
        "successfully_committed_roles": list(artifacts),
        "terminal_completeness": "complete",
    }
    manifest = build_run_manifest(
        run_id="run-1",
        run_dir=tmp_path,
        started_at_utc=_UTC,
        ended_at_utc=_UTC,
        status="success",
        command="fixture",
        dataset_paths={"topbook": "wrong-legacy-alias.parquet"},
        schema_versions={"topbook": "depth.v1"},
        row_counts={"topbook": 999},
        extra={"dataset_artifacts": artifacts, "storage_profile": profile},
    )
    manifest_path = write_manifest(tmp_path / "manifest.json", manifest)

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert any(
        "does not match named profile authority" in error for error in report.all_errors
    )


def test_exact_dataset_artifacts_override_fuzzy_manifest_aliases(tmp_path) -> None:
    manifest, manifest_path = _write_complete_profile_manifest(tmp_path)

    report = validate_run_manifest(manifest_path)

    assert report.ok, report.all_errors
    assert {dataset.dataset_key for dataset in report.datasets} == set(
        manifest["dataset_artifacts"]
    )
    assert {dataset.row_count for dataset in report.datasets} == {0}

    manifest["dataset_artifacts"]["topbook_main"]["dataset_key"] = "trade"
    invalid_path = write_manifest(tmp_path / "invalid-manifest.json", manifest)
    invalid = validate_run_manifest(invalid_path)
    assert not invalid.ok
    assert any(
        "dataset_key does not match schema identity" in error
        for error in invalid.all_errors
    )


def test_exact_artifact_segment_hashes_authenticate_physical_contents(tmp_path) -> None:
    manifest, manifest_path = _write_complete_profile_manifest(tmp_path)
    path = tmp_path / "topbook_main.parquet"
    frame = pd.DataFrame(
        [
            topbook_row(
                collector_run_id="run-1",
                exchange="polymarket",
                instrument_id="token-1",
                received_at_utc=_UTC,
                local_sequence=1,
                valid_state=True,
                quality_flags=[],
            )
        ]
    )
    frame.to_parquet(path, index=False, compression="snappy")
    manifest["dataset_artifacts"]["topbook_main"]["row_count"] = 1
    _refresh_artifact_segment_manifest(
        manifest["dataset_artifacts"], tmp_path, "topbook_main"
    )
    write_manifest(manifest_path, manifest)

    baseline = validate_run_manifest(manifest_path)
    assert baseline.ok, baseline.all_errors

    frame.to_parquet(path, index=False, compression="gzip")
    tampered = validate_run_manifest(manifest_path)
    assert not tampered.ok
    assert any(
        "completed_segments[0] sha256 mismatch" in error
        for error in tampered.all_errors
    )


@pytest.mark.parametrize(
    "bad_path",
    ("../escape.parquet", "C:/outside/health.parquet", "/outside/health.parquet"),
)
def test_exact_artifacts_reject_paths_outside_the_manifest_run(
    tmp_path, bad_path: str
) -> None:
    manifest, manifest_path = _write_complete_profile_manifest(tmp_path)
    manifest["dataset_artifacts"]["health"]["path"] = bad_path
    write_manifest(manifest_path, manifest)

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert any("canonical path relative" in error for error in report.all_errors)


@pytest.mark.parametrize("bad_count", (True, 0.5, "0"))
def test_exact_artifact_row_counts_require_json_integers(
    tmp_path, bad_count: object
) -> None:
    manifest, manifest_path = _write_complete_profile_manifest(tmp_path)
    manifest["dataset_artifacts"]["health"]["row_count"] = bad_count
    write_manifest(manifest_path, manifest)

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert any(
        "row_count must be a nonnegative integer" in error
        for error in report.all_errors
    )


def test_exact_manifest_run_dir_must_be_its_own_directory(tmp_path) -> None:
    manifest, manifest_path = _write_complete_profile_manifest(tmp_path)
    manifest["run_dir"] = str(tmp_path.parent)
    write_manifest(manifest_path, manifest)

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert any("directory containing" in error for error in report.all_errors)


@pytest.mark.parametrize("invalid", [None, "escape", "run_binding"])
def test_exact_run_directory_hook_does_not_relax_artifact_authority(tmp_path, invalid) -> None:
    manifest, manifest_path = _write_complete_profile_manifest(tmp_path)
    old = tmp_path / "absent-original-directory"
    manifest["run_dir"] = str(old)
    if invalid == "escape":
        manifest["dataset_artifacts"]["health"]["path"] = "../health.parquet"
    elif invalid == "run_binding":
        pd.DataFrame([_trade()]).to_parquet(tmp_path / "trade.parquet", index=False)
        manifest["dataset_artifacts"]["trade"]["row_count"] = 1
        _refresh_artifact_segment_manifest(manifest["dataset_artifacts"], tmp_path, "trade")
        manifest["run_id"] = "foreign-run"
    write_manifest(manifest_path, manifest)
    before = manifest_path.read_bytes()
    observed = []

    def resolve(value):
        observed.append(value)
        return tmp_path if value == old else value

    report = validate_run_manifest(manifest_path, path_resolver=resolve)
    assert report.ok == (invalid is None), report.all_errors
    assert all(value == old for value in observed)
    assert manifest_path.read_bytes() == before


def test_profile_artifacts_bind_run_id_and_keep_topbook_roles_disjoint(
    tmp_path,
) -> None:
    manifest, manifest_path = _write_complete_profile_manifest(tmp_path)
    main_path = tmp_path / "topbook_main.parquet"
    checkpoint_path = tmp_path / "topbook_checkpoint.parquet"
    foreign = topbook_row(
        collector_run_id="foreign-run",
        exchange="polymarket",
        instrument_id="token-1",
        received_at_utc=_UTC,
        local_sequence=1,
        valid_state=True,
        quality_flags=[],
    )
    pd.DataFrame([foreign]).to_parquet(main_path, index=False)
    manifest["dataset_artifacts"]["topbook_main"]["row_count"] = 1
    write_manifest(manifest_path, manifest)

    foreign_report = validate_run_manifest(manifest_path)

    assert not foreign_report.ok
    assert any(
        "do not match manifest run_id" in error for error in foreign_report.all_errors
    )

    shared = topbook_row(
        collector_run_id="run-1",
        exchange="polymarket",
        instrument_id="token-1",
        received_at_utc=_UTC,
        local_sequence=1,
        valid_state=True,
        quality_flags=[],
    )
    pd.DataFrame([shared]).to_parquet(main_path, index=False)
    pd.DataFrame([shared]).to_parquet(checkpoint_path, index=False)
    manifest["dataset_artifacts"]["topbook_checkpoint"]["row_count"] = 1
    write_manifest(manifest_path, manifest)

    overlap_report = validate_run_manifest(manifest_path)

    assert not overlap_report.ok
    assert any(
        "primary keys must be disjoint" in error for error in overlap_report.all_errors
    )


def test_named_profile_versions_and_compatibility_fields_are_authoritative(
    tmp_path,
) -> None:
    manifest, manifest_path = _write_complete_profile_manifest(tmp_path)
    manifest["storage_profile"]["profile_version"] = "999"
    manifest["storage_profile"]["replay_evidence_version"] = "future"
    write_manifest(manifest_path, manifest)

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert any("profile_version" in error for error in report.all_errors)
    assert any("replay_evidence_version" in error for error in report.all_errors)


@pytest.mark.parametrize("acknowledgement", ("missing", None, "true", 1))
def test_exact_profile_acknowledgement_requires_json_boolean(
    tmp_path,
    acknowledgement: object,
) -> None:
    manifest, manifest_path = _write_complete_profile_manifest(tmp_path)
    if acknowledgement == "missing":
        del manifest["storage_profile"]["experimental_profile_acknowledged"]
    else:
        manifest["storage_profile"]["experimental_profile_acknowledged"] = (
            acknowledgement
        )
    write_manifest(manifest_path, manifest)

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert any(
        "experimental_profile_acknowledged must be a JSON boolean" in error
        for error in report.all_errors
    )


def test_commit_checksum_is_canonical_json_sha256() -> None:
    artifact = CaptureCommitArtifactV1(
        role="health",
        path="health/part-000000.parquet",
        sha256="8" * 64,
        row_count=2,
        first_local_sequence=1,
        last_local_sequence=2,
    )
    record = CaptureCommitRecordV1.create(
        group_id="9" * 64,
        committed_at_utc=_UTC,
        artifacts=[artifact],
    )
    encoded = json.dumps(
        record.payload_without_checksum(),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    assert record.checksum_sha256 == hashlib.sha256(encoded).hexdigest()


def test_profile_registry_resolves_exact_historical_versions() -> None:
    current = select_storage_profile("full")
    exact = select_storage_profile("full", profile_version="1")

    assert current.definition.profile_version == "2"
    assert exact.definition.profile_version == "1"
    assert exact.definition != current.definition
    assert get_storage_profile_definition("full", "2") is PROFILE_DEFINITIONS["full"]
    assert (
        PROFILE_DEFINITIONS_BY_VERSION[("full", "2")] is PROFILE_DEFINITIONS["full"]
    )
    assert DatasetRole.INSTRUMENT_EVIDENCE not in exact.enabled_roles
    assert DatasetRole.INSTRUMENT_EVIDENCE not in exact.disabled_roles
    assert exact.enabled_roles | exact.disabled_roles == (
        frozenset(DatasetRole) - {DatasetRole.INSTRUMENT_EVIDENCE}
    )
    with pytest.raises(TypeError):
        PROFILE_DEFINITIONS_BY_VERSION[("full", "1")] = current.definition  # type: ignore[index]
    with pytest.raises(ValueError, match="unknown storage profile version"):
        select_storage_profile("full", profile_version="999")



def test_structure_only_manifest_validation_avoids_global_materialization(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, manifest_path = _write_complete_profile_manifest(tmp_path)

    def reject_materialization(*args, **kwargs):
        del args, kwargs
        raise AssertionError("structure validation must not materialize datasets")

    monkeypatch.setattr(
        manifests_module,
        "_read_dataset_frame",
        reject_materialization,
    )

    report = validate_run_manifest(
        manifest_path,
        exact_artifact_validation="structure",
    )

    assert report.ok, report.all_errors
    assert all(dataset.exists for dataset in report.datasets)


def test_manifest_validation_rejects_unknown_exact_artifact_mode(tmp_path) -> None:
    _, manifest_path = _write_complete_profile_manifest(tmp_path)

    with pytest.raises(ValueError, match="exact_artifact_validation"):
        validate_run_manifest(manifest_path, exact_artifact_validation="unknown")  # type: ignore[arg-type]

@pytest.mark.parametrize("artifact_value", ["missing", None, [], {}])
def test_exact_authority_never_falls_back_to_legacy_artifacts(
    tmp_path, artifact_value: object
) -> None:
    manifest, manifest_path = _write_complete_profile_manifest(tmp_path)
    if artifact_value == "missing":
        del manifest["dataset_artifacts"]
    else:
        manifest["dataset_artifacts"] = artifact_value
    write_manifest(manifest_path, manifest)

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert report.datasets == ()
    assert any("dataset_artifacts must be" in error for error in report.all_errors)


@pytest.mark.parametrize("run_id", ["   ", " run-1", "run-1 ", 123, None])
def test_exact_authority_requires_exact_nonempty_string_run_id(
    tmp_path, run_id: object
) -> None:
    manifest, manifest_path = _write_complete_profile_manifest(tmp_path)
    manifest["run_id"] = run_id
    write_manifest(manifest_path, manifest)

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert any(
        "run_id must be a non-empty string" in error for error in report.all_errors
    )


def test_nonempty_exact_artifact_requires_row_run_authority(tmp_path) -> None:
    manifest, manifest_path = _write_complete_profile_manifest(tmp_path)
    trade = trade_row(
        venue="polymarket",
        venue_trade_id="trade-1",
        venue_market_id="market-1",
        instrument_id="token-1",
        outcome="YES",
        trade_ts_utc=_UTC,
        received_at_utc=_UTC,
        price_dollars=0.5,
        size_contracts=2.0,
        notional_dollars=1.0,
        aggressor_side="buy",
        raw_json="{}",
        raw_json_sha256=hashlib.sha256(b"{}").hexdigest(),
    )
    pq.write_table(
        pa.Table.from_pylist([trade], schema=arrow_schema(get_table_spec("trade.v1"))),
        tmp_path / "trade.parquet",
    )
    manifest["dataset_artifacts"]["trade"]["row_count"] = 1
    write_manifest(manifest_path, manifest)

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert any(
        "collector_run_id contains 1 null or blank values" in error
        for error in report.all_errors
    )


@pytest.mark.parametrize("collector_run_id", [None, ""])
def test_exact_artifacts_reject_null_or_blank_collector_run_ids(
    tmp_path, collector_run_id: str | None
) -> None:
    manifest, manifest_path = _write_complete_profile_manifest(tmp_path)
    path = tmp_path / "topbook_main.parquet"
    pd.DataFrame(
        [
            topbook_row(
                collector_run_id=collector_run_id,
                exchange="polymarket",
                venue_market_id="market-1",
                instrument_id="token-1",
                received_at_utc=_UTC,
                received_at_monotonic_ns=100,
                local_sequence=1,
                valid_state=True,
                quality_flags=[],
            )
        ]
    ).to_parquet(path, index=False)
    manifest["dataset_artifacts"]["topbook_main"]["row_count"] = 1
    write_manifest(manifest_path, manifest)

    report = validate_run_manifest(manifest_path)

    assert not report.ok
    assert any("null or blank values" in error for error in report.all_errors)


def _topbook_recovery_rows(
    *, role: str = "topbook_main", valid_state: bool = True
) -> tuple[dict[str, object], dict[str, object]]:
    topbook = topbook_row(
        collector_run_id="run-1",
        exchange="polymarket",
        venue_market_id="market-1",
        instrument_id="token-1",
        received_at_utc=_UTC,
        received_at_monotonic_ns=100,
        local_sequence=1,
        venue_sequence=7,
        best_bid_dollars=0.4,
        best_ask_dollars=0.6,
        valid_state=valid_state,
        quality_flags=[],
    )
    control = book_tape_control_row(
        collector_run_id="run-1",
        control_id="a" * 64,
        venue="polymarket",
        venue_market_id="market-1",
        venue_book_id="token-1",
        control_type="book_recovered",
        reason="topbook_validated",
        valid_after=True,
        received_at_utc=_UTC,
        received_at_monotonic_ns=100,
        local_sequence=1,
        subsequence=4,
        venue_sequence="7",
        evidence_role=role,
        evidence_id=topbook_evidence_id(topbook),
        quality_flags_json="[]",
    )
    return topbook, control


@pytest.mark.parametrize("role", ["topbook_main", "topbook_checkpoint"])
@pytest.mark.parametrize(
    ("field", "value", "expected_error"),
    [
        ("venue", "kalshi", "venue disagrees"),
        ("venue_market_id", "other-market", "venue_market_id disagrees"),
        ("venue_book_id", "other-book", "venue_book_id disagrees"),
        ("received_at_utc", "2026-07-19T10:00:01Z", "received_at_utc disagrees"),
        ("received_at_monotonic_ns", 999, "received_at_monotonic_ns disagrees"),
        ("exchange_at_utc", _UTC, "exchange_at_utc disagrees"),
        ("local_sequence", 2, "local_sequence disagrees"),
        ("venue_sequence", 8, "venue_sequence disagrees"),
        ("epoch_id", _EPOCH_ID, "epoch_id must be absent"),
    ],
)
def test_topbook_recovery_evidence_is_hash_and_coordinate_bound(
    role: str, field: str, value: object, expected_error: str
) -> None:
    topbook, control = _topbook_recovery_rows(role=role)
    kwargs = {role: pd.DataFrame([topbook])}

    report = validate_book_control_evidence(
        pd.DataFrame([control]),
        **kwargs,
    )
    assert report.ok, report.errors

    control[field] = value
    mismatch = validate_book_control_evidence(
        pd.DataFrame([control]),
        **kwargs,
    )
    assert not mismatch.ok
    assert any(expected_error in error for error in mismatch.errors)


def test_topbook_recovery_evidence_requires_valid_state() -> None:
    topbook, control = _topbook_recovery_rows(valid_state=False)

    report = validate_book_control_evidence(
        pd.DataFrame([control]),
        topbook_main=pd.DataFrame([topbook]),
    )

    assert not report.ok
    assert any("must have valid state" in error for error in report.errors)


def test_exact_manifest_resolves_topbook_recovery_evidence(tmp_path) -> None:
    manifest, manifest_path = _write_complete_profile_manifest(tmp_path)
    topbook, control = _topbook_recovery_rows()
    topbook["received_at_utc"] = "2026-07-19T10:00:00.123456789Z"
    control["received_at_utc"] = "2026-07-19T10:00:00.123456789+00:00"
    control["evidence_id"] = topbook_evidence_id(topbook)
    pd.DataFrame([topbook]).to_parquet(tmp_path / "topbook_main.parquet", index=False)
    pd.DataFrame([control]).to_parquet(tmp_path / "tape_control.parquet", index=False)
    manifest["dataset_artifacts"]["topbook_main"]["row_count"] = 1
    manifest["dataset_artifacts"]["tape_control"]["row_count"] = 1
    _refresh_artifact_segment_manifest(
        manifest["dataset_artifacts"], tmp_path, "topbook_main"
    )
    _refresh_artifact_segment_manifest(
        manifest["dataset_artifacts"], tmp_path, "tape_control"
    )
    write_manifest(manifest_path, manifest)

    report = validate_run_manifest(manifest_path)
    assert report.ok, report.all_errors

    control["evidence_id"] = "b" * 64
    pd.DataFrame([control]).to_parquet(tmp_path / "tape_control.parquet", index=False)
    tampered = validate_run_manifest(manifest_path)
    assert not tampered.ok
    assert any(
        "unresolved topbook_main evidence" in error for error in tampered.all_errors
    )
