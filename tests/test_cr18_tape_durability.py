from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import pmkt.streaming.durability as durability_module
import pmkt.streaming.recovery as recovery_module

from pmkt.data.manifests import (
    _durability_latency_metric_errors,
    validate_run_manifest,
)
from pmkt.data.kalshi_quotes import KALSHI_QUOTE_NORMALIZATION_POLICY_LEGACY
from pmkt.data.registry import (
    BOOK_TAPE_EVENT_SCHEMA_VERSION,
    BOOK_TAPE_LEVEL_SCHEMA_VERSION,
    TOPBOOK_SCHEMA_VERSION,
)
from pmkt.data.schemas import topbook_row
from pmkt.data.validation import validate_book_tape_bundle
from pmkt.exchanges.kalshi.ws import KalshiOrderBookState
from pmkt.exchanges.polymarket.ws import MarketBookState
from pmkt.streaming.capture import TapeBatchIntent
from pmkt.streaming.durability import (
    COMMIT_JOURNAL_NAME,
    COMMIT_JOURNAL_V1_NAME,
    COMMIT_JOURNAL_V2_NAME,
    RUN_STATE_NAME,
    DurableCaptureCoordinator,
    file_sha256,
)
from pmkt.streaming.durability_settings import CaptureDurabilitySettings
from pmkt.streaming.recovery import (
    recover_stream_run,
    resolve_commit_journal_path,
    validate_commit_journal,
)
from pmkt.streaming.recovery_contracts import (
    CAPTURE_COMMIT_JOURNAL_V1_FORMAT,
    CAPTURE_COMMIT_JOURNAL_V2_FORMAT,
    COALESCIBLE_COMMIT_CAUSES,
    LEGACY_UNKNOWN_COMMIT_CAUSE,
    CaptureCommitArtifactV1,
    CaptureCommitCause,
    CaptureCommitRecordV1,
    CaptureCommitRecordV2,
    RunStateV1,
)
from pmkt.streaming.profiles import select_storage_profile
from pmkt.streaming.tape import (
    CaptureCoordinate,
    NativeBookLevel,
    build_tape_batch,
    canonical_decimal,
    canonical_json_bytes,
    deterministic_merge_key,
    epoch_id,
    post_book_hash,
    recompute_tape_event_id,
    recompute_tape_event_payload_hash,
)
from pmkt.streaming.tape_producers import (
    CompactValidityProducer,
    KalshiTapeProducer,
    PolymarketTapeProducer,
)
from pmkt.streaming.venue_tape import (
    kalshi_book_levels,
    kalshi_delta_levels,
    polymarket_book_levels,
    polymarket_delta_levels,
)

_UTC = "2026-07-19T10:00:00.000000Z"


def _coordinate(*, sequence: int = 1, subsequence: int = 1) -> CaptureCoordinate:
    return CaptureCoordinate(
        collector_run_id="run-1",
        shard_id="shard-0",
        received_at_utc=_UTC,
        received_at_monotonic_ns=100,
        local_sequence=sequence,
        subsequence=subsequence,
    )


def _checkpoint_batch(*, encoding_version: str = "book-tape.v1") -> object:
    state = MarketBookState(asset_id="token-1", market="market-1")
    state.apply_book(
        {
            "event_type": "book",
            "asset_id": "token-1",
            "market": "market-1",
            "bids": [{"price": "0.40", "size": "10"}],
            "asks": [{"price": "0.60", "size": "11"}],
            "timestamp": "2026-07-19T10:00:00Z",
        }
    )
    coordinate = _coordinate()
    epoch = epoch_id(coordinate, venue_book_id="token-1", epoch_generation=0)
    levels = polymarket_book_levels(state)
    return build_tape_batch(
        coordinate=coordinate,
        venue="polymarket",
        venue_market_id="market-1",
        venue_book_id="token-1",
        event_kind="checkpoint",
        checkpoint_reason="startup",
        epoch=epoch,
        levels=levels,
        full_book_levels=levels,
        allowed_source_sides=("bid", "ask"),
        valid_state=state.valid_state,
        reconstructible=True,
        quality_flags=state.quality_flags,
        encoding_version=encoding_version,
    )


def _tape_coordinator(
    tmp_path: Path,
    *,
    venue_book_id: str = "token-1",
    adapter_settings_by_venue: dict[str, dict[str, object]] | None = None,
) -> DurableCaptureCoordinator:
    state = RunStateV1(
        run_id="run-1",
        profile_name="book-tape",
        profile_version="1",
        expected_role_paths={
            "tape_level": "datasets/tape_level",
            "tape_event": "datasets/tape_event",
        },
        shard_plan={"shard-0": [venue_book_id]},
        started_at_utc=_UTC,
    )
    return DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={
            "tape_level": BOOK_TAPE_LEVEL_SCHEMA_VERSION,
            "tape_event": BOOK_TAPE_EVENT_SCHEMA_VERSION,
        },
        segment_row_limit=100,
        adapter_settings_by_venue=adapter_settings_by_venue,
    )


def _add_tape_batch(
    coordinator: DurableCaptureCoordinator, batch: TapeBatchIntent
) -> None:
    for row in batch.levels:
        coordinator.add("tape_level", row)
    coordinator.add("tape_event", batch.event)


@pytest.mark.parametrize("write_statistics", [True, False])
def test_parquet_sequence_bounds_preserve_exact_min_max(
    tmp_path: Path,
    *,
    write_statistics: bool,
) -> None:
    path = tmp_path / f"sequence-stats-{write_statistics}.parquet"
    pq.write_table(
        pa.table({"local_sequence": pa.array([9, 2, 7, 4], type=pa.int64())}),
        path,
        row_group_size=2,
        write_statistics=write_statistics,
    )

    parquet_file = pq.ParquetFile(path)

    assert recovery_module._parquet_sequence_bounds(
        parquet_file,
        "local_sequence",
    ) == (2, 9)

def test_canonical_decimal_json_and_merge_order_are_stable() -> None:
    assert canonical_decimal("1.2300") == "1.23"
    assert canonical_decimal(-0.0) == "0"
    assert canonical_json_bytes({"z": 1.2, "a": ["x", 2.0]}) == (
        b'{"a":["x","2"],"z":"1.2"}'
    )
    earlier = {"received_at_utc": _UTC, "local_sequence": 4, "subsequence": 0}
    later = {"received_at_utc": _UTC, "local_sequence": 4, "subsequence": 1}
    assert deterministic_merge_key(
        earlier, shard_id="a", family="invalidation_control"
    ) < deterministic_merge_key(later, shard_id="a", family="book_event")


def test_checkpoint_builder_hashes_and_validates_as_one_bundle() -> None:
    batch = _checkpoint_batch()
    report = validate_book_tape_bundle(
        pd.DataFrame([batch.event]), pd.DataFrame(batch.levels)
    )
    assert report.ok, report.errors
    assert batch.event["expected_level_row_count"] == 2
    assert json.loads(batch.event["side_counts_json"]) == {"ask": 1, "bid": 1}
    assert len(batch.event["event_payload_hash"]) == 64
    assert len(batch.event["post_book_hash"]) == 64


def test_bundle_validation_can_bind_events_to_profile_encoding() -> None:
    batch = _checkpoint_batch(encoding_version="book-tape.v2")

    unbound_report = validate_book_tape_bundle(
        pd.DataFrame([batch.event]), pd.DataFrame(batch.levels)
    )
    bound_report = validate_book_tape_bundle(
        pd.DataFrame([batch.event]),
        pd.DataFrame(batch.levels),
        expected_encoding_version="book-tape.v1",
    )

    assert unbound_report.ok, unbound_report.errors
    assert not bound_report.ok
    assert any(
        "encoding_version must equal 'book-tape.v1'" in error
        for error in bound_report.errors
    )


def test_tape_shard_index_preserves_bindings_after_state_rebind(
    tmp_path: Path,
) -> None:
    coordinator = _tape_coordinator(tmp_path)
    event = {"venue_book_id": "token-1"}

    assert coordinator._resolve_tape_shard(event) == "shard-0"
    first_cache = coordinator._tape_shard_index_cache

    coordinator.state = replace(coordinator.state, shard_plan={"shard-1": ["token-1"]})

    assert coordinator._resolve_tape_shard(event) == "shard-1"
    assert coordinator._tape_shard_index_cache is not first_cache


def test_checkpoint_side_counts_must_match_actual_level_sides() -> None:
    batch = _checkpoint_batch()
    event = dict(batch.event)
    event["side_counts_json"] = '{"ask":2,"bid":0}'

    report = validate_book_tape_bundle(
        pd.DataFrame([event]), pd.DataFrame(batch.levels)
    )

    assert not report.ok
    assert any(
        "side counts disagree with level rows" in error for error in report.errors
    )


def test_commit_rejects_unknown_event_fields_before_serialization(
    tmp_path: Path,
) -> None:
    batch = _checkpoint_batch()
    event = dict(batch.event)
    event["unregistered_payload_field"] = "must-not-be-dropped"
    coordinator = _tape_coordinator(tmp_path)
    for row in batch.levels:
        coordinator.add("tape_level", row)
    coordinator.add("tape_event", event)

    with pytest.raises(ValueError, match="unknown fields unregistered_payload_field"):
        coordinator.commit(cause="checkpoint_startup", force=True)

    assert not list(tmp_path.rglob("part-*.parquet"))
    assert not (tmp_path / COMMIT_JOURNAL_NAME).exists()


def test_commit_rejects_mutated_payload_before_writing_artifacts(
    tmp_path: Path,
) -> None:
    batch = _checkpoint_batch()
    event = dict(batch.event)
    event["valid_state"] = False
    coordinator = _tape_coordinator(tmp_path)
    for row in batch.levels:
        coordinator.add("tape_level", row)
    coordinator.add("tape_event", event)

    with pytest.raises(ValueError, match="event_payload_hash mismatch"):
        coordinator.commit(cause="checkpoint_startup", force=True)

    assert not list(tmp_path.rglob("part-*.parquet"))
    assert not (tmp_path / COMMIT_JOURNAL_NAME).exists()


def test_commit_rejects_cross_run_rows_before_writing_artifacts(tmp_path: Path) -> None:
    batch = _checkpoint_batch()
    coordinator = _tape_coordinator(tmp_path)
    for row in batch.levels:
        coordinator.add("tape_level", row)
    event = dict(batch.event)
    event["collector_run_id"] = "other-run"
    coordinator.add("tape_event", event)

    with pytest.raises(ValueError, match="collector_run_id must equal 'run-1'"):
        coordinator.commit(cause="checkpoint_startup", force=True)

    assert not list(tmp_path.rglob("part-*.parquet"))
    assert not (tmp_path / COMMIT_JOURNAL_NAME).exists()


def test_commit_recomputes_event_id_after_foreign_keys_are_rewritten(
    tmp_path: Path,
) -> None:
    batch = _checkpoint_batch()
    event = dict(batch.event)
    event["event_id"] = "a" * 64
    levels = [dict(row, event_id=event["event_id"]) for row in batch.levels]
    coordinator = _tape_coordinator(tmp_path)
    for row in levels:
        coordinator.add("tape_level", row)
    coordinator.add("tape_event", event)

    with pytest.raises(ValueError, match="event_id mismatch"):
        coordinator.commit(cause="checkpoint_startup", force=True)

    assert not list(tmp_path.rglob("part-*.parquet"))
    assert not (tmp_path / COMMIT_JOURNAL_NAME).exists()


def test_commit_recomputes_post_book_hash_from_committed_levels(tmp_path: Path) -> None:
    batch = _checkpoint_batch()
    event = dict(batch.event)
    event["post_book_hash"] = "0" * 64
    payload_hash = recompute_tape_event_payload_hash(event, batch.levels)
    event["event_payload_hash"] = payload_hash
    event["event_id"] = recompute_tape_event_id(
        event, shard_id="shard-0", payload_hash=payload_hash
    )
    levels = [dict(row, event_id=event["event_id"]) for row in batch.levels]
    coordinator = _tape_coordinator(tmp_path)
    for row in levels:
        coordinator.add("tape_level", row)
    coordinator.add("tape_event", event)

    with pytest.raises(ValueError, match="post_book_hash mismatch"):
        coordinator.commit(cause="checkpoint_startup", force=True)

    assert not list(tmp_path.rglob("part-*.parquet"))
    assert not (tmp_path / COMMIT_JOURNAL_NAME).exists()


def test_commit_revalidates_persisted_arrow_values_before_journal(
    tmp_path: Path,
) -> None:
    batch = _checkpoint_batch()
    event = dict(batch.event)
    event.pop("exchange_at_utc")
    payload_hash = recompute_tape_event_payload_hash(event, batch.levels)
    event["event_payload_hash"] = payload_hash
    event["event_id"] = recompute_tape_event_id(
        event, shard_id="shard-0", payload_hash=payload_hash
    )
    levels = [dict(row, event_id=event["event_id"]) for row in batch.levels]
    coordinator = _tape_coordinator(tmp_path)
    for row in levels:
        coordinator.add("tape_level", row)
    coordinator.add("tape_event", event)

    with pytest.raises(ValueError, match="event_payload_hash mismatch"):
        coordinator.commit(cause="checkpoint_startup", force=True)

    assert list(tmp_path.rglob("part-*.parquet"))
    assert not (tmp_path / COMMIT_JOURNAL_NAME).exists()


def test_kalshi_commit_requires_exact_adapter_settings(tmp_path: Path) -> None:
    state = KalshiOrderBookState("KX-1")
    state.apply_snapshot(
        {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "KX-1",
                "yes_dollars": [["0.40", "3"]],
                "no_dollars": [["0.30", "4"]],
            },
        }
    )
    levels = kalshi_book_levels(state)
    coordinate = _coordinate()
    batch = build_tape_batch(
        coordinate=coordinate,
        venue="kalshi",
        venue_market_id="KX-1",
        venue_book_id="KX-1",
        event_kind="checkpoint",
        checkpoint_reason="startup",
        epoch=epoch_id(coordinate, venue_book_id="KX-1", epoch_generation=0),
        levels=levels,
        full_book_levels=levels,
        allowed_source_sides=("yes", "no"),
        valid_state=True,
        reconstructible=True,
        adapter_settings={"use_yes_price": True},
    )
    rejected = _tape_coordinator(tmp_path / "rejected", venue_book_id="KX-1")
    _add_tape_batch(rejected, batch)
    with pytest.raises(ValueError, match="requires explicit adapter settings"):
        rejected.commit(cause="checkpoint_startup", force=True)

    accepted = _tape_coordinator(
        tmp_path / "accepted",
        venue_book_id="KX-1",
        adapter_settings_by_venue={"kalshi": {"use_yes_price": True}},
    )
    _add_tape_batch(accepted, batch)
    assert accepted.commit(cause="checkpoint_startup", force=True) is not None
    persisted_state = json.loads((tmp_path / "accepted" / RUN_STATE_NAME).read_text())
    assert persisted_state["adapter_settings_by_venue"] == {
        "kalshi": {"use_yes_price": True}
    }
    explicitly_empty = replace(accepted.state, adapter_settings_by_venue={})
    with pytest.raises(ValueError, match="persisted run-state authority"):
        DurableCaptureCoordinator(
            run_dir=tmp_path / "explicit-empty",
            run_state=explicitly_empty,
            role_schema_versions=accepted.role_schema_versions,
            segment_row_limit=100,
            adapter_settings_by_venue={"kalshi": {"use_yes_price": True}},
        )


def test_delta_post_hash_uses_prior_committed_checkpoint_state(tmp_path: Path) -> None:
    checkpoint = _checkpoint_batch()
    coordinator = _tape_coordinator(tmp_path)
    _add_tape_batch(coordinator, checkpoint)
    assert coordinator.commit(cause="checkpoint_startup", force=True) is not None

    changed_bid = NativeBookLevel(
        source_side="bid",
        price="0.40",
        size_after_contracts="7",
        size_delta_contracts="-3",
    )
    full_book = (
        NativeBookLevel(source_side="bid", price="0.40", size_after_contracts="7"),
        NativeBookLevel(source_side="ask", price="0.60", size_after_contracts="11"),
    )
    delta = build_tape_batch(
        coordinate=_coordinate(sequence=2),
        venue="polymarket",
        venue_market_id="market-1",
        venue_book_id="token-1",
        event_kind="delta",
        epoch=checkpoint.event["epoch_id"],
        levels=(changed_bid,),
        full_book_levels=full_book,
        allowed_source_sides=("bid", "ask"),
        valid_state=True,
        reconstructible=True,
    )
    _add_tape_batch(coordinator, delta)

    assert coordinator.commit(cause="checkpoint_startup", force=True) is not None


def test_polymarket_repeated_mutations_collapse_to_final_absolute_size() -> None:
    state = MarketBookState(asset_id="token-1")
    state.apply_book(
        {
            "event_type": "book",
            "asset_id": "token-1",
            "bids": [{"price": "0.4", "size": "2"}],
            "asks": [{"price": "0.6", "size": "3"}],
        }
    )
    message = {
        "event_type": "price_change",
        "asset_id": "token-1",
        "price_changes": [
            {"side": "BUY", "price": "0.4", "size": "5"},
            {"side": "BUY", "price": "0.4", "size": "7"},
        ],
    }
    state.apply_price_change(message["price_changes"][0], message)
    state.apply_price_change(message["price_changes"][1], message)
    levels = polymarket_delta_levels(state, message)
    assert len(levels) == 1
    assert levels[0].source_side == "bid"
    assert levels[0].size_after_contracts == 7.0


def test_polymarket_batched_mutations_are_scoped_to_their_asset() -> None:
    first = MarketBookState(asset_id="token-1")
    second = MarketBookState(asset_id="token-2")
    for state, price in ((first, "0.4"), (second, "0.3")):
        state.apply_book(
            {
                "event_type": "book",
                "asset_id": state.asset_id,
                "bids": [{"price": price, "size": "2"}],
                "asks": [{"price": "0.6", "size": "3"}],
            }
        )
    message = {
        "event_type": "price_change",
        "price_changes": [
            {
                "asset_id": "token-1",
                "side": "BUY",
                "price": "0.4",
                "size": "5",
            },
            {
                "asset_id": "token-2",
                "side": "BUY",
                "price": "0.3",
                "size": "7",
            },
        ],
    }
    first.apply_price_change(message["price_changes"][0], message)
    second.apply_price_change(message["price_changes"][1], message)

    first_levels = polymarket_delta_levels(first, message)
    second_levels = polymarket_delta_levels(second, message)

    assert [
        (level.price_key, level.size_after_contracts) for level in first_levels
    ] == [("0.4", 5.0)]
    assert [
        (level.price_key, level.size_after_contracts) for level in second_levels
    ] == [("0.3", 7.0)]


def test_kalshi_adapter_retains_native_yes_no_ladders() -> None:
    state = KalshiOrderBookState("KX-1")
    state.apply_snapshot(
        {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "KX-1",
                "yes_dollars": [["0.40", "3"]],
                "no_dollars": [["0.30", "4"]],
            },
        }
    )
    message = {
        "type": "orderbook_delta",
        "sid": 1,
        "seq": 2,
        "msg": {"market_ticker": "KX-1", "side": "yes", "price": "0.40", "delta": "2"},
    }
    state.apply_delta(message)
    mutation = kalshi_delta_levels(state, message)
    assert mutation[0].source_side == "yes"
    assert mutation[0].size_after_contracts == 5.0
    assert {level.source_side for level in kalshi_book_levels(state)} == {"yes", "no"}
    assert post_book_hash(
        venue="kalshi",
        venue_book_id="KX-1",
        levels=kalshi_book_levels(state),
        adapter_settings={"use_yes_price": True},
    ) != post_book_hash(
        venue="kalshi",
        venue_book_id="KX-1",
        levels=kalshi_book_levels(state),
        adapter_settings={"use_yes_price": False},
    )


def test_kalshi_tape_producer_rejects_mismatched_quote_policy() -> None:
    state = KalshiOrderBookState(
        "KX-1",
        quote_normalization_policy=KALSHI_QUOTE_NORMALIZATION_POLICY_LEGACY,
    )
    message = {
        "type": "orderbook_snapshot",
        "sid": 1,
        "seq": 1,
        "msg": {
            "market_ticker": "KX-1",
            "yes_dollars": [["0.40", "3"]],
            "no_dollars": [["0.30", "4"]],
        },
    }
    state.apply_snapshot(message)
    producer = KalshiTapeProducer(
        collector_run_id="run-1",
        shard_id="kalshi-0",
        use_yes_price=True,
    )

    with pytest.raises(ValueError, match="adapter settings differ"):
        producer.observe(
            message=message,
            states={"KX-1": state},
            received_at_utc=_UTC,
            received_at_monotonic_ns=1,
            local_sequence=1,
        )


def test_durable_group_is_reportable_and_finalize_promotes_only_journaled(
    tmp_path: Path,
) -> None:
    batch = _checkpoint_batch()
    selection = select_storage_profile("book-tape", profile_version="1")
    enabled_roles = sorted(role.value for role in selection.enabled_roles)
    role_schema_versions = {
        role.value: next(iter(selection.definition.role_schema_versions[role]))
        for role in selection.enabled_roles
    }
    state = RunStateV1(
        run_id="run-1",
        profile_name="book-tape",
        profile_version="1",
        expected_role_paths={role: f"datasets/{role}" for role in enabled_roles},
        shard_plan={"shard-0": ["token-1"]},
        started_at_utc=_UTC,
        storage_profile=selection.to_manifest_mapping(),
    )
    coordinator = DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions=role_schema_versions,
        segment_row_limit=100,
    )
    for row in batch.levels:
        coordinator.add("tape_level", row)
    coordinator.add("tape_event", batch.event)
    record = coordinator.commit(cause="checkpoint_startup", force=True)
    assert record is not None
    assert [artifact.role for artifact in record.artifacts] == [
        "tape_level",
        "tape_event",
    ]
    orphan = tmp_path / "datasets" / "tape_event" / "part-999999.parquet"
    orphan.write_bytes((tmp_path / record.artifacts[1].path).read_bytes())

    report = recover_stream_run(tmp_path)
    assert report.valid_group_count == 1
    assert report.orphan_paths == ("datasets/tape_event/part-999999.parquet",)
    assert json.loads((tmp_path / RUN_STATE_NAME).read_text())["status"] == "recording"

    finalized = recover_stream_run(tmp_path, finalize=True)
    assert finalized.finalized_manifest_path is not None
    manifest = json.loads(Path(finalized.finalized_manifest_path).read_text())
    assert manifest["status"] == "partial"
    assert manifest["capture_termination"] == "crashed"
    assert not orphan.exists()
    assert (tmp_path / "_orphans" / "datasets" / "tape_event" / orphan.name).exists()
    assert json.loads((tmp_path / RUN_STATE_NAME).read_text())["status"] == "finalized"
    validation = validate_run_manifest(finalized.finalized_manifest_path)
    assert validation.ok, validation.all_errors
    profile = manifest["storage_profile"]
    assert profile["contract_status"] == "experimental"
    assert profile["effective_overrides"] == {
        "emit_full_depth": False,
        "emit_legacy_book_artifacts": False,
        "keep_raw_jsonl": False,
        "topbook_emission_per_event": False,
    }
    assert profile["successfully_committed_roles"] == [
        "tape_event",
        "tape_level",
    ]
    assert profile["terminal_completeness"] == "partial"
    assert manifest["dataset_artifacts"]["health"]["completion_status"] == "failed"

    durability_metrics = manifest["capture_durability"]["metrics"]
    assert durability_metrics["groups_accepted"] is None
    assert durability_metrics["groups_published"] == 1
    assert durability_metrics["groups_discarded"] is None
    assert durability_metrics["cause_counts"] == {"checkpoint_startup": 1}
    assert durability_metrics["acceptance_to_journal_latency_ms"] == {
        "sample_count": 0,
        "p50": None,
        "p95": None,
        "p99": None,
        "maximum": None,
    }

    durability_metrics["acceptance_to_journal_latency_ms"]["p50"] = 0.0
    manifest_path = Path(finalized.finalized_manifest_path)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    unavailable_latency = validate_run_manifest(manifest_path)
    assert not unavailable_latency.ok
    assert any(
        "must be unavailable after process-loss recovery" in error
        for error in unavailable_latency.all_errors
    )
    durability_metrics["acceptance_to_journal_latency_ms"]["p50"] = None
    durability_metrics["cause_counts"] = {}
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    invalid_metrics = validate_run_manifest(manifest_path)
    assert not invalid_metrics.ok
    assert any(
        "cause_counts must exactly reconcile with the journal" in error
        for error in invalid_metrics.all_errors
    )


def test_recovery_with_all_roles_but_no_clean_terminal_marker_stays_partial(
    tmp_path: Path,
) -> None:
    selection = select_storage_profile("book-tape", profile_version="1")
    enabled_roles = sorted(role.value for role in selection.enabled_roles)
    role_schema_versions = {
        role.value: next(iter(selection.definition.role_schema_versions[role]))
        for role in selection.enabled_roles
    }
    state = RunStateV1(
        run_id="run-1",
        profile_name="book-tape",
        profile_version="1",
        expected_role_paths={
            role: f"datasets/{role}.parquet" for role in enabled_roles
        },
        shard_plan={"shard-0": ["token-1"]},
        started_at_utc=_UTC,
        storage_profile=selection.to_manifest_mapping(),
    )
    coordinator = DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions=role_schema_versions,
        segment_row_limit=100,
    )
    coordinator.finalize_segments()

    finalized = recover_stream_run(tmp_path, finalize=True)

    assert finalized.finalized_manifest_path is not None
    manifest = json.loads(Path(finalized.finalized_manifest_path).read_text())
    assert manifest["status"] == "partial"
    assert manifest["storage_profile"]["terminal_completeness"] == "partial"
    assert manifest["storage_profile"]["successfully_committed_roles"] == enabled_roles
    validation = validate_run_manifest(finalized.finalized_manifest_path)
    assert validation.ok, validation.all_errors
    manifest_path = Path(finalized.finalized_manifest_path)
    trade_entry = manifest["dataset_artifacts"]["trade"]
    assert trade_entry["segment_manifest_path"] == (
        "datasets/trade.parquet/_segments.json"
    )
    assert (tmp_path / trade_entry["segment_manifest_path"]).is_file()

    original_interval = manifest["storage_profile"]["feed_health_interval_seconds"]
    manifest["storage_profile"]["feed_health_interval_seconds"] = 5.0
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    profile_mismatch = validate_run_manifest(manifest_path)
    assert not profile_mismatch.ok
    assert any(
        "storage_profile must exactly match manifest capture-time profile" in error
        for error in profile_mismatch.all_errors
    )
    manifest["storage_profile"]["feed_health_interval_seconds"] = original_interval
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    trade_dir = tmp_path / trade_entry["path"]
    journaled_empty_part = next(trade_dir.glob("part-*.parquet"))
    unjournaled_part = trade_dir / "part-999999.parquet"
    unjournaled_part.write_bytes(journaled_empty_part.read_bytes())
    tampered = validate_run_manifest(manifest_path)
    assert not tampered.ok
    assert any(
        "unjournaled physical artifacts" in error for error in tampered.all_errors
    )
    unjournaled_part.unlink()

    symlink_part = trade_dir / "part-999998.parquet"
    try:
        symlink_part.symlink_to(journaled_empty_part.name)
    except OSError:
        pass
    else:
        symlinked = validate_run_manifest(manifest_path)
        assert not symlinked.ok
        assert any(
            "unjournaled physical artifacts" in error for error in symlinked.all_errors
        )


def test_recovery_without_commits_publishes_valid_failed_manifest(
    tmp_path: Path,
) -> None:
    selection = select_storage_profile("book-tape", profile_version="1")
    enabled_roles = sorted(role.value for role in selection.enabled_roles)
    role_schema_versions = {
        role.value: next(iter(selection.definition.role_schema_versions[role]))
        for role in selection.enabled_roles
    }
    state = RunStateV1(
        run_id="run-1",
        profile_name="book-tape",
        profile_version="1",
        expected_role_paths={role: f"datasets/{role}" for role in enabled_roles},
        shard_plan={"shard-0": ["token-1"]},
        started_at_utc=_UTC,
        storage_profile=selection.to_manifest_mapping(),
    )
    DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions=role_schema_versions,
        segment_row_limit=100,
    )

    finalized = recover_stream_run(tmp_path, finalize=True)

    assert finalized.finalized_manifest_path is not None
    validation = validate_run_manifest(finalized.finalized_manifest_path)
    assert validation.ok, validation.all_errors
    manifest = json.loads(Path(finalized.finalized_manifest_path).read_text())
    assert manifest["status"] == "failed"
    assert manifest["storage_profile"]["terminal_completeness"] == "failed"
    assert manifest["storage_profile"]["successfully_committed_roles"] == []
    assert {
        artifact["completion_status"]
        for artifact in manifest["dataset_artifacts"].values()
    } == {"failed"}
    failed_artifact = manifest["dataset_artifacts"][enabled_roles[0]]
    segment_manifest_path = tmp_path / failed_artifact["segment_manifest_path"]
    segment_manifest = json.loads(segment_manifest_path.read_text(encoding="utf-8"))
    segment_manifest["status"] = "closed"
    segment_manifest_path.write_text(
        json.dumps(segment_manifest, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    failed_artifact["segment_manifest_hash"] = file_sha256(segment_manifest_path)
    Path(finalized.finalized_manifest_path).write_text(
        json.dumps(manifest, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    invalid = validate_run_manifest(finalized.finalized_manifest_path)
    assert not invalid.ok
    assert any(
        "segment manifest must exactly match journal paths, hashes, and counts" in error
        for error in invalid.all_errors
    )


def test_recovery_revalidates_tape_post_book_hash_after_journal_rewrite(
    tmp_path: Path,
) -> None:
    batch = _checkpoint_batch()
    coordinator = _tape_coordinator(tmp_path)
    _add_tape_batch(coordinator, batch)
    original = coordinator.commit(cause="checkpoint_startup", force=True)
    assert original is not None
    artifacts_by_role = {artifact.role: artifact for artifact in original.artifacts}
    event_path = tmp_path / artifacts_by_role["tape_event"].path
    level_path = tmp_path / artifacts_by_role["tape_level"].path

    event_table = pq.read_table(event_path)
    level_table = pq.read_table(level_path)
    event_rows = event_table.to_pylist()
    level_rows = level_table.to_pylist()
    event_rows[0]["post_book_hash"] = "0" * 64
    payload_hash = recompute_tape_event_payload_hash(event_rows[0], level_rows)
    event_rows[0]["event_payload_hash"] = payload_hash
    rewritten_event_id = recompute_tape_event_id(
        event_rows[0], shard_id="shard-0", payload_hash=payload_hash
    )
    event_rows[0]["event_id"] = rewritten_event_id
    for level in level_rows:
        level["event_id"] = rewritten_event_id
    pq.write_table(
        pa.Table.from_pylist(event_rows, schema=event_table.schema), event_path
    )
    pq.write_table(
        pa.Table.from_pylist(level_rows, schema=level_table.schema), level_path
    )

    rewritten_artifacts = tuple(
        replace(artifact, sha256=file_sha256(tmp_path / artifact.path))
        for artifact in original.artifacts
    )
    rewritten = CaptureCommitRecordV2.create(
        group_id=original.group_id,
        group_index=original.group_index,
        cause=original.cause,
        accepted_at_utc=original.accepted_at_utc,
        committed_at_utc=original.committed_at_utc,
        artifacts=rewritten_artifacts,
    )
    (tmp_path / COMMIT_JOURNAL_NAME).write_text(
        json.dumps(rewritten.to_mapping()) + "\n", encoding="utf-8"
    )

    report = recover_stream_run(tmp_path)

    assert report.valid_group_count == 0
    assert any("post_book_hash mismatch" in error for error in report.journal_errors)


def test_recovery_rejects_self_checksummed_wrong_sequence_bounds(
    tmp_path: Path,
) -> None:
    batch = _checkpoint_batch()
    coordinator = _tape_coordinator(tmp_path)
    _add_tape_batch(coordinator, batch)
    original = coordinator.commit(cause="checkpoint_startup", force=True)
    assert original is not None
    event_artifact = next(
        artifact for artifact in original.artifacts if artifact.role == "tape_event"
    )
    rewritten_artifacts = tuple(
        replace(artifact, first_local_sequence=0, last_local_sequence=0)
        if artifact is event_artifact
        else artifact
        for artifact in original.artifacts
    )
    rewritten = CaptureCommitRecordV2.create(
        group_id=original.group_id,
        group_index=original.group_index,
        cause=original.cause,
        accepted_at_utc=original.accepted_at_utc,
        committed_at_utc=original.committed_at_utc,
        artifacts=rewritten_artifacts,
    )
    (tmp_path / COMMIT_JOURNAL_NAME).write_text(
        json.dumps(rewritten.to_mapping()) + "\n", encoding="utf-8"
    )

    report = recover_stream_run(tmp_path)

    assert report.valid_group_count == 0
    assert any("sequence bounds mismatch" in error for error in report.journal_errors)


def test_corrupt_journal_record_is_never_recovered(tmp_path: Path) -> None:
    state = RunStateV1(
        run_id="run-1",
        profile_name="full",
        profile_version="1",
        expected_role_paths={"tape_event": "datasets/tape_event"},
        shard_plan={"shard-0": []},
        started_at_utc=_UTC,
    )
    DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={"tape_event": BOOK_TAPE_EVENT_SCHEMA_VERSION},
        segment_row_limit=1,
    )
    journal = tmp_path / COMMIT_JOURNAL_NAME
    journal.write_text('{"format":"capture_commit_journal.v2","bad":true}\n')
    report = recover_stream_run(tmp_path)
    assert report.valid_group_count == 0
    assert report.journal_errors
    with pytest.raises(ValueError, match="invalid journal evidence"):
        recover_stream_run(tmp_path, finalize=True)


def test_recovery_rejects_duplicate_and_reordered_journal_groups(
    tmp_path: Path,
) -> None:
    state = RunStateV1(
        run_id="run-1",
        profile_name="full",
        profile_version="1",
        expected_role_paths={"probe": "datasets/probe"},
        shard_plan={"shard-0": []},
        started_at_utc=_UTC,
    )
    coordinator = DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={"probe": "legacy.probe.v1"},
        role_schemas={
            "probe": pa.schema([pa.field("value", pa.int64(), nullable=False)])
        },
        segment_row_limit=1,
    )
    coordinator.add("probe", {"value": 1})
    coordinator.commit(cause="checkpoint_startup", force=True)
    coordinator.add("probe", {"value": 2})
    coordinator.commit(cause="checkpoint_startup", force=True)
    journal = tmp_path / COMMIT_JOURNAL_NAME
    lines = journal.read_text(encoding="utf-8").splitlines()

    journal.write_text("\n".join([lines[0], lines[0]]) + "\n", encoding="utf-8")
    duplicate = recover_stream_run(tmp_path)
    assert any("duplicate group_id" in error for error in duplicate.journal_errors)

    journal.write_text("\n".join(reversed(lines)) + "\n", encoding="utf-8")
    reordered = recover_stream_run(tmp_path)
    assert any("group_index" in error for error in reordered.journal_errors)


@pytest.mark.parametrize(
    ("replacement_role", "error"),
    [
        ("unknown", "artifact role is not present in run state"),
        ("other", "disagrees with expected path role"),
    ],
)
def test_recovery_binds_journal_artifacts_to_run_state_roles(
    tmp_path: Path,
    replacement_role: str,
    error: str,
) -> None:
    state = RunStateV1(
        run_id="run-1",
        profile_name="full",
        profile_version="1",
        expected_role_paths={
            "probe": "datasets/probe",
            "other": "datasets/other",
        },
        shard_plan={"shard-0": []},
        started_at_utc=_UTC,
    )
    schema = pa.schema([pa.field("value", pa.int64(), nullable=False)])
    coordinator = DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={
            "probe": "legacy.probe.v1",
            "other": "legacy.probe.v1",
        },
        role_schemas={"probe": schema, "other": schema},
        segment_row_limit=1,
    )
    coordinator.add("probe", {"value": 1})
    original = coordinator.commit(cause="checkpoint_startup", force=True)
    assert original is not None
    source = original.artifacts[0]
    replacement = CaptureCommitArtifactV1(
        role=replacement_role,
        path=source.path,
        sha256=source.sha256,
        row_count=source.row_count,
        first_local_sequence=source.first_local_sequence,
        last_local_sequence=source.last_local_sequence,
    )
    rewritten = CaptureCommitRecordV2.create(
        group_id=original.group_id,
        group_index=original.group_index,
        cause=original.cause,
        accepted_at_utc=original.accepted_at_utc,
        committed_at_utc=original.committed_at_utc,
        artifacts=(replacement,),
    )
    (tmp_path / COMMIT_JOURNAL_NAME).write_text(
        json.dumps(rewritten.to_mapping()) + "\n", encoding="utf-8"
    )

    report = recover_stream_run(tmp_path)
    assert any(error in item for item in report.journal_errors)
    with pytest.raises(ValueError, match=error):
        validate_commit_journal(tmp_path)


def test_strict_journal_validation_rejects_missing_and_empty_evidence(
    tmp_path: Path,
) -> None:
    state = RunStateV1(
        run_id="run-1",
        profile_name="full",
        profile_version="1",
        expected_role_paths={"probe": "datasets/probe"},
        shard_plan={},
        started_at_utc=_UTC,
    )
    DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={"probe": "legacy.probe.v1"},
        role_schemas={
            "probe": pa.schema([pa.field("value", pa.int64(), nullable=False)])
        },
        segment_row_limit=1,
    )

    with pytest.raises(ValueError, match="does not exist"):
        validate_commit_journal(tmp_path)
    (tmp_path / COMMIT_JOURNAL_NAME).write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="contains no records"):
        validate_commit_journal(tmp_path)


def test_recovery_normalizes_list_columns_from_persisted_parquet(
    tmp_path: Path,
) -> None:
    state = RunStateV1(
        run_id="run-1",
        profile_name="book-tape",
        profile_version="1",
        expected_role_paths={"topbook_main": "datasets/topbook_main"},
        shard_plan={"shard-0": ["token-1"]},
        started_at_utc=_UTC,
    )
    coordinator = DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={"topbook_main": TOPBOOK_SCHEMA_VERSION},
        segment_row_limit=1,
    )
    coordinator.add(
        "topbook_main",
        topbook_row(
            collector_run_id="run-1",
            exchange="polymarket",
            venue_market_id="market-1",
            instrument_id="token-1",
            received_at_utc=_UTC,
            received_at_monotonic_ns=100,
            local_sequence=1,
            valid_state=True,
            quality_flags=["startup_restatement", "valid"],
        ),
    )

    coordinator.commit(cause="checkpoint_startup", force=True)

    assert len(validate_commit_journal(tmp_path)) == 1


def test_profile_v1_recovery_repairs_legacy_character_flag_arrays(
    tmp_path: Path,
) -> None:
    state = RunStateV1(
        run_id="run-1",
        profile_name="book-tape",
        profile_version="1",
        expected_role_paths={"topbook_main": "datasets/topbook_main"},
        shard_plan={"shard-0": ["token-1"]},
        started_at_utc=_UTC,
    )
    coordinator = DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={"topbook_main": TOPBOOK_SCHEMA_VERSION},
        segment_row_limit=1,
    )
    coordinator.add(
        "topbook_main",
        topbook_row(
            collector_run_id="run-1",
            exchange="polymarket",
            venue_market_id="market-1",
            instrument_id="token-1",
            received_at_utc=_UTC,
            received_at_monotonic_ns=100,
            local_sequence=1,
            valid_state=True,
            quality_flags=["missing_instrument_books"],
        ),
    )
    receipt = coordinator.commit(cause="checkpoint_startup", force=True)
    assert receipt is not None
    current = receipt
    artifact = current.artifacts[0]
    segment_path = tmp_path / artifact.path
    table = pq.read_table(segment_path)
    field_index = table.schema.get_field_index("quality_flags")
    field = table.schema.field(field_index)
    table = table.set_column(
        field_index,
        field,
        pa.array([list("missing_instrument_books")], type=field.type),
    )
    pq.write_table(table, segment_path)
    replacement = replace(artifact, sha256=file_sha256(segment_path))
    legacy = CaptureCommitRecordV1.create(
        group_id=current.group_id,
        committed_at_utc=current.committed_at_utc,
        artifacts=(replacement,),
    )
    (tmp_path / COMMIT_JOURNAL_NAME).unlink()
    (tmp_path / COMMIT_JOURNAL_V1_NAME).write_text(
        json.dumps(legacy.to_mapping()) + "\n",
        encoding="utf-8",
    )
    state_path = tmp_path / RUN_STATE_NAME
    state_payload = json.loads(state_path.read_text(encoding="utf-8"))
    state_payload.pop("capture_durability")
    state_path.write_text(json.dumps(state_payload) + "\n", encoding="utf-8")

    records = validate_commit_journal(tmp_path)

    assert len(records) == 1
    assert records[0].cause == LEGACY_UNKNOWN_COMMIT_CAUSE


def test_recovery_materializes_each_parquet_artifact_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = RunStateV1(
        run_id="run-1",
        profile_name="book-tape",
        profile_version="1",
        expected_role_paths={"topbook_main": "datasets/topbook_main"},
        shard_plan={"shard-0": ["token-1"]},
        started_at_utc=_UTC,
    )
    coordinator = DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={"topbook_main": TOPBOOK_SCHEMA_VERSION},
        segment_row_limit=1,
    )
    coordinator.add(
        "topbook_main",
        topbook_row(
            collector_run_id="run-1",
            exchange="polymarket",
            venue_market_id="market-1",
            instrument_id="token-1",
            received_at_utc=_UTC,
            received_at_monotonic_ns=100,
            local_sequence=1,
            valid_state=True,
            quality_flags=["valid"],
        ),
    )
    receipt = coordinator.commit(cause="checkpoint_startup", force=True)
    assert receipt is not None
    record = receipt

    original_read_parquet = recovery_module.pd.read_parquet
    calls: list[Path] = []

    def counted_read_parquet(path: Path) -> pd.DataFrame:
        calls.append(Path(path))
        return original_read_parquet(path)

    monkeypatch.setattr(recovery_module.pd, "read_parquet", counted_read_parquet)

    report = recover_stream_run(tmp_path)
    assert len(report.validated_records) == 1
    expected_paths = [
        tmp_path / artifact.path
        for artifact in record.artifacts
        if Path(artifact.path).suffix.lower() == ".parquet"
    ]
    assert calls == expected_paths

    calls.clear()
    integrity_report = recover_stream_run(
        tmp_path,
        payload_validation="integrity",
    )

    assert len(integrity_report.validated_records) == 1
    assert calls == []
    with pytest.raises(ValueError, match="requires full"):
        recover_stream_run(
            tmp_path, finalize=True, payload_validation="integrity"
        )


def test_recovery_integrity_can_scope_artifact_payloads(tmp_path: Path) -> None:
    state = RunStateV1(
        run_id="run-1",
        profile_name="full",
        profile_version="1",
        expected_role_paths={
            "probe_a": "datasets/probe_a",
            "probe_b": "datasets/probe_b",
        },
        shard_plan={},
        started_at_utc=_UTC,
    )
    probe_schema = pa.schema([pa.field("value", pa.int64(), nullable=False)])
    coordinator = DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={
            "probe_a": "legacy.probe.v1",
            "probe_b": "legacy.probe.v1",
        },
        role_schemas={
            "probe_a": probe_schema,
            "probe_b": probe_schema,
        },
        segment_row_limit=100,
    )
    coordinator.add("probe_a", {"value": 1})
    coordinator.add("probe_b", {"value": 2})
    receipt = coordinator.commit(cause="checkpoint_startup", force=True)
    assert receipt is not None
    record = receipt

    scoped = recover_stream_run(
        tmp_path,
        payload_validation="integrity",
        artifact_roles={"probe_a"},
    )

    artifacts = {artifact.role: artifact for artifact in record.artifacts}
    assert scoped.journal_errors == ()
    assert set(scoped.validated_artifact_fingerprints) == {
        artifacts["probe_a"].path
    }
    assert scoped.to_mapping()["validated_artifact_count"] == 1

    unselected_path = tmp_path / artifacts["probe_b"].path
    unselected_path.write_bytes(unselected_path.read_bytes() + b"corrupt")
    still_scoped = recover_stream_run(
        tmp_path,
        payload_validation="integrity",
        artifact_roles={"probe_a"},
    )
    assert still_scoped.journal_errors == ()

    unscoped = recover_stream_run(tmp_path, payload_validation="integrity")
    assert any("artifact hash mismatch" in error for error in unscoped.journal_errors)
    with pytest.raises(ValueError, match="requires integrity-only"):
        recover_stream_run(tmp_path, artifact_roles={"probe_a"})


def test_recovery_refuses_to_reclassify_a_finalized_run(tmp_path: Path) -> None:
    state = RunStateV1(
        run_id="run-1",
        profile_name="full",
        profile_version="1",
        expected_role_paths={"probe": "datasets/probe"},
        shard_plan={},
        started_at_utc=_UTC,
    )
    coordinator = DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={"probe": "legacy.probe.v1"},
        role_schemas={
            "probe": pa.schema([pa.field("value", pa.int64(), nullable=False)])
        },
        segment_row_limit=1,
    )
    coordinator.finalize()

    with pytest.raises(ValueError, match="already finalized"):
        recover_stream_run(tmp_path, finalize=True)
    assert not (tmp_path / "run_manifest.v1.json").exists()


def test_journal_binding_supports_segment_directories_with_suffixes(
    tmp_path: Path,
) -> None:
    state = RunStateV1(
        run_id="run-1",
        profile_name="full",
        profile_version="1",
        expected_role_paths={"probe": "datasets/book.v1"},
        shard_plan={},
        started_at_utc=_UTC,
    )
    coordinator = DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={"probe": "legacy.probe.v1"},
        role_schemas={
            "probe": pa.schema([pa.field("value", pa.int64(), nullable=False)])
        },
        segment_row_limit=1,
    )
    coordinator.add("probe", {"value": 1})
    coordinator.commit(cause="checkpoint_startup", force=True)

    assert len(validate_commit_journal(tmp_path)) == 1


def test_journal_binding_rejects_ambiguous_nested_role_roots(tmp_path: Path) -> None:
    state = RunStateV1(
        run_id="run-1",
        profile_name="full",
        profile_version="1",
        expected_role_paths={
            "parent": "datasets",
            "child": "datasets/child",
        },
        shard_plan={},
        started_at_utc=_UTC,
    )
    schema = pa.schema([pa.field("value", pa.int64(), nullable=False)])
    coordinator = DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={
            "parent": "legacy.probe.v1",
            "child": "legacy.probe.v1",
        },
        role_schemas={"parent": schema, "child": schema},
        segment_row_limit=1,
    )
    coordinator.add("child", {"value": 1})
    coordinator.commit(cause="checkpoint_startup", force=True)

    report = recover_stream_run(tmp_path)

    assert any(
        "ambiguous expected role roots" in error for error in report.journal_errors
    )


def test_journal_binding_rejects_noncanonical_segment_names(tmp_path: Path) -> None:
    state = RunStateV1(
        run_id="run-1",
        profile_name="full",
        profile_version="1",
        expected_role_paths={"probe": "datasets/probe"},
        shard_plan={},
        started_at_utc=_UTC,
    )
    coordinator = DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={"probe": "legacy.probe.v1"},
        role_schemas={
            "probe": pa.schema([pa.field("value", pa.int64(), nullable=False)])
        },
        segment_row_limit=1,
    )
    coordinator.add("probe", {"value": 1})
    original = coordinator.commit(cause="checkpoint_startup", force=True)
    assert original is not None
    source = original.artifacts[0]
    copied_path = "datasets/probe/copied.parquet"
    (tmp_path / copied_path).write_bytes((tmp_path / source.path).read_bytes())
    copied = CaptureCommitArtifactV1(
        role=source.role,
        path=copied_path,
        sha256=source.sha256,
        row_count=source.row_count,
        first_local_sequence=source.first_local_sequence,
        last_local_sequence=source.last_local_sequence,
    )
    rewritten = CaptureCommitRecordV2.create(
        group_id=original.group_id,
        group_index=original.group_index,
        cause=original.cause,
        accepted_at_utc=original.accepted_at_utc,
        committed_at_utc=original.committed_at_utc,
        artifacts=(copied,),
    )
    (tmp_path / COMMIT_JOURNAL_NAME).write_text(
        json.dumps(rewritten.to_mapping()) + "\n", encoding="utf-8"
    )

    report = recover_stream_run(tmp_path)

    assert any(
        "segment path is not canonical" in error for error in report.journal_errors
    )


def test_external_file_directory_is_synced_before_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = RunStateV1(
        run_id="run-1",
        profile_name="full",
        profile_version="1",
        expected_role_paths={"raw_jsonl": "nested/raw_events.jsonl"},
        shard_plan={},
        started_at_utc=_UTC,
    )
    calls: list[tuple[str, str]] = []
    coordinator = DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={"raw_jsonl": "legacy.raw_jsonl.v1"},
        external_file_roles=("raw_jsonl",),
        segment_row_limit=1,
    )
    monkeypatch.setattr(
        durability_module,
        "_fsync_directory",
        lambda path: calls.append(("directory", Path(path).name)),
    )
    original_append = coordinator._append_journal_record

    def append_and_record(record: CaptureCommitRecordV2) -> None:
        calls.append(("journal", record.group_id))
        original_append(record)

    monkeypatch.setattr(coordinator, "_append_journal_record", append_and_record)

    coordinator.finalize_segments()

    directory_index = next(
        index for index, call in enumerate(calls) if call == ("directory", "nested")
    )
    journal_index = next(
        index for index, call in enumerate(calls) if call[0] == "journal"
    )
    assert directory_index < journal_index


@pytest.mark.parametrize(
    ("crash_point", "valid_groups", "orphan_count"),
    [("before_journal", 0, 1), ("after_journal_fsync", 1, 0)],
)
def test_real_child_process_crash_respects_journal_boundary(
    tmp_path: Path,
    crash_point: str,
    valid_groups: int,
    orphan_count: int,
) -> None:
    run_dir = tmp_path / crash_point
    script = textwrap.dedent(
        """
        import os
        import sys
        import pyarrow as pa
        from pmkt.streaming.durability import DurableCaptureCoordinator
        from pmkt.streaming.recovery_contracts import RunStateV1

        run_dir, crash_point = sys.argv[1:]
        state = RunStateV1(
            run_id="crash-run",
            profile_name="full",
            profile_version="1",
            expected_role_paths={"probe": "datasets/probe"},
            shard_plan={"probe-0": []},
            started_at_utc="2026-07-19T10:00:00.000000Z",
        )
        coordinator = DurableCaptureCoordinator(
            run_dir=run_dir,
            run_state=state,
            role_schema_versions={"probe": "legacy.probe.v1"},
            role_schemas={"probe": pa.schema([pa.field("value", pa.int64(), nullable=False)])},
            segment_row_limit=1,
        )
        coordinator.add("probe", {"value": 1})
        if crash_point == "before_journal":
            coordinator._append_journal_record = lambda record: os._exit(91)
        coordinator.commit(cause="checkpoint_startup", force=True)
        os._exit(92)
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script, str(run_dir), crash_point],
        cwd=Path.cwd(),
        check=False,
    )
    assert completed.returncode in {91, 92}
    report = recover_stream_run(run_dir)
    assert report.valid_group_count == valid_groups
    assert len(report.orphan_paths) == orphan_count


def test_run_state_finalizes_only_after_segments_and_manifest_boundary(
    tmp_path: Path,
) -> None:
    state = RunStateV1(
        run_id="run-1",
        profile_name="full",
        profile_version="1",
        expected_role_paths={"probe": "datasets/probe"},
        shard_plan={"probe-0": []},
        started_at_utc=_UTC,
    )
    coordinator = DurableCaptureCoordinator(
        run_dir=tmp_path,
        run_state=state,
        role_schema_versions={"probe": "legacy.probe.v1"},
        role_schemas={
            "probe": pa.schema([pa.field("value", pa.int64(), nullable=False)])
        },
        segment_row_limit=1,
    )
    coordinator.finalize_segments()
    assert json.loads((tmp_path / RUN_STATE_NAME).read_text())["status"] == "recording"
    (tmp_path / "manifest.json").write_text("{}\n", encoding="utf-8")
    coordinator.mark_finalized()
    assert json.loads((tmp_path / RUN_STATE_NAME).read_text())["status"] == "finalized"


def test_coordinate_rejects_naive_timestamp() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        CaptureCoordinate(
            collector_run_id="run",
            shard_id="s",
            received_at_utc=datetime(2026, 7, 19).isoformat(),
            received_at_monotonic_ns=0,
            local_sequence=0,
            subsequence=0,
        )


def test_polymarket_producer_opens_invalidates_and_reopens_epochs() -> None:
    producer = PolymarketTapeProducer(collector_run_id="run-1", shard_id="poly-0")
    state = MarketBookState(asset_id="token-1")
    states = {"token-1": state}
    snapshot = {
        "event_type": "book",
        "asset_id": "token-1",
        "market": "market-1",
        "bids": [{"price": "0.4", "size": "2"}],
        "asks": [{"price": "0.6", "size": "3"}],
    }
    state.apply_book(snapshot)
    opened = producer.observe(
        message=snapshot,
        states=states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=1,
        local_sequence=1,
    )
    assert opened.barrier_cause is CaptureCommitCause.CHECKPOINT_STARTUP
    assert opened.batches[0].event["reconstructible"] is True
    first_epoch = opened.batches[0].event["epoch_id"]
    assert opened.controls[0]["control_type"] == "book_recovered"

    reconnect = producer.reconnect(
        states=states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=2,
        local_sequence=2,
    )
    state.mark_reconnect()
    assert reconnect.controls[0]["epoch_id"] == first_epoch
    assert reconnect.controls[0]["control_type"] == "book_invalidated"

    delta = {
        "event_type": "price_change",
        "asset_id": "token-1",
        "price_changes": [
            {"asset_id": "token-1", "side": "BUY", "price": "0.4", "size": "5"}
        ],
    }
    state.apply_price_change(delta["price_changes"][0], delta)
    audit = producer.observe(
        message=delta,
        states=states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=3,
        local_sequence=3,
    )
    assert audit.batches[0].event["epoch_id"] is None
    assert audit.batches[0].event["reconstructible"] is False

    state.apply_book(snapshot)
    reopened = producer.observe(
        message=snapshot,
        states=states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=4,
        local_sequence=4,
    )
    assert reopened.batches[0].event["epoch_id"] != first_epoch
    assert reopened.batches[0].event["checkpoint_reason"] == "resync"


def test_polymarket_sticky_invalidation_emits_one_control_and_barrier() -> None:
    producer = PolymarketTapeProducer(collector_run_id="run-1", shard_id="poly-0")
    state = MarketBookState(asset_id="token-1")
    states = {"token-1": state}
    snapshot = {
        "event_type": "book",
        "asset_id": "token-1",
        "market": "market-1",
        "bids": [{"price": "0.4", "size": "2"}],
        "asks": [{"price": "0.6", "size": "3"}],
    }
    state.apply_book(snapshot)
    producer.observe(
        message=snapshot,
        states=states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=1,
        local_sequence=1,
    )
    state.valid_state = False
    state.quality_flags.add("hash_mismatch")
    delta = {
        "event_type": "price_change",
        "asset_id": "token-1",
        "price_changes": [
            {"asset_id": "token-1", "side": "BUY", "price": "0.4", "size": "5"}
        ],
    }

    first = producer.observe(
        message=delta,
        states=states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=2,
        local_sequence=2,
    )
    repeated = producer.observe(
        message=delta,
        states=states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=3,
        local_sequence=3,
    )

    assert len(first.controls) == 1
    assert first.barrier_cause == "invalidation"
    assert repeated.controls == ()
    assert repeated.barrier_cause is None


def test_tape_producers_invalidate_when_book_becomes_empty() -> None:
    polymarket = PolymarketTapeProducer(collector_run_id="run-1", shard_id="poly-0")
    poly_state = MarketBookState(asset_id="token-1")
    poly_snapshot = {
        "event_type": "book",
        "asset_id": "token-1",
        "market": "market-1",
        "bids": [{"price": "0.4", "size": "2"}],
        "asks": [{"price": "0.6", "size": "3"}],
    }
    poly_state.apply_book(poly_snapshot)
    polymarket.observe(
        message=poly_snapshot,
        states={"token-1": poly_state},
        received_at_utc=_UTC,
        received_at_monotonic_ns=1,
        local_sequence=1,
    )
    poly_state.valid_state = False
    poly_state.quality_flags.add("empty_bid")
    poly_delta = {
        "event_type": "price_change",
        "asset_id": "token-1",
        "price_changes": [
            {
                "asset_id": "token-1",
                "side": "BUY",
                "price": "0.4",
                "size": "0",
            }
        ],
    }
    poly_emission = polymarket.observe(
        message=poly_delta,
        states={"token-1": poly_state},
        received_at_utc=_UTC,
        received_at_monotonic_ns=2,
        local_sequence=2,
    )

    kalshi = KalshiTapeProducer(
        collector_run_id="run-1", shard_id="kalshi-0", use_yes_price=True
    )
    kalshi_state = KalshiOrderBookState("KX-1")
    kalshi_snapshot = {
        "type": "orderbook_snapshot",
        "sid": 1,
        "seq": 1,
        "msg": {
            "market_ticker": "KX-1",
            "yes_dollars": [["0.4", "2"]],
            "no_dollars": [["0.6", "3"]],
        },
    }
    kalshi_state.apply_snapshot(kalshi_snapshot)
    kalshi.observe(
        message=kalshi_snapshot,
        states={"KX-1": kalshi_state},
        received_at_utc=_UTC,
        received_at_monotonic_ns=1,
        local_sequence=1,
    )
    kalshi_state.valid_state = False
    kalshi_state.quality_flags.add("empty_bid")
    kalshi_delta = {
        "type": "orderbook_delta",
        "sid": 1,
        "seq": 2,
        "msg": {
            "market_ticker": "KX-1",
            "side": "yes",
            "price_dollars": "0.4",
            "delta_fp": "-2",
        },
    }
    kalshi_emission = kalshi.observe(
        message=kalshi_delta,
        states={"KX-1": kalshi_state},
        received_at_utc=_UTC,
        received_at_monotonic_ns=2,
        local_sequence=2,
    )

    for emission in (poly_emission, kalshi_emission):
        assert emission.barrier_cause == "invalidation"
        assert emission.controls[0]["reason"] == "invalid_state"
        assert emission.batches[0].event["epoch_id"] is None
        assert emission.batches[0].event["reconstructible"] is False


def test_invalid_snapshots_do_not_emit_recovery_controls() -> None:
    polymarket = PolymarketTapeProducer(collector_run_id="run-1", shard_id="poly-0")
    poly_state = MarketBookState(asset_id="token-1")
    poly_message = {
        "event_type": "book",
        "asset_id": "token-1",
        "market": "market-1",
        "bids": [],
        "asks": [],
    }
    poly_state.apply_book(poly_message)
    poly_emission = polymarket.observe(
        message=poly_message,
        states={"token-1": poly_state},
        received_at_utc=_UTC,
        received_at_monotonic_ns=1,
        local_sequence=1,
    )

    kalshi = KalshiTapeProducer(
        collector_run_id="run-1", shard_id="kalshi-0", use_yes_price=True
    )
    kalshi_state = KalshiOrderBookState("KX-1")
    kalshi_message = {
        "type": "orderbook_snapshot",
        "sid": 1,
        "seq": 1,
        "msg": {"market_ticker": "KX-1", "yes_dollars": [], "no_dollars": []},
    }
    kalshi_state.apply_snapshot(kalshi_message)
    kalshi_emission = kalshi.observe(
        message=kalshi_message,
        states={"KX-1": kalshi_state},
        received_at_utc=_UTC,
        received_at_monotonic_ns=1,
        local_sequence=1,
    )

    for emission in (poly_emission, kalshi_emission):
        assert emission.batches and not emission.controls
        assert emission.batches[0].event["epoch_id"] is not None
        assert emission.batches[0].event["reconstructible"] is False

    poly_delta = {
        "event_type": "price_change",
        "asset_id": "token-1",
        "price_changes": [
            {"asset_id": "token-1", "side": "BUY", "price": "0.4", "size": "1"}
        ],
    }
    poly_state.apply_price_change(poly_delta["price_changes"][0], poly_delta)
    poly_audit = polymarket.observe(
        message=poly_delta,
        states={"token-1": poly_state},
        received_at_utc=_UTC,
        received_at_monotonic_ns=2,
        local_sequence=2,
    )
    assert poly_audit.batches[0].event["epoch_id"] is None
    assert poly_audit.batches[0].event["reconstructible"] is False

    kalshi_delta = {
        "type": "orderbook_delta",
        "sid": 1,
        "seq": 2,
        "msg": {
            "market_ticker": "KX-1",
            "side": "yes",
            "price": "0.4",
            "delta": "1",
        },
    }
    kalshi_state.apply_delta(kalshi_delta)
    kalshi_audit = kalshi.observe(
        message=kalshi_delta,
        states={"KX-1": kalshi_state},
        received_at_utc=_UTC,
        received_at_monotonic_ns=2,
        local_sequence=2,
    )
    assert kalshi_audit.batches[0].event["epoch_id"] is None
    assert kalshi_audit.batches[0].event["reconstructible"] is False

    poly_checkpoint = polymarket.checkpoint_states(
        states={"token-1": poly_state},
        received_at_utc=_UTC,
        received_at_monotonic_ns=3,
        local_sequence=3,
    )
    kalshi_checkpoint = kalshi.checkpoint_states(
        states={"KX-1": kalshi_state},
        received_at_utc=_UTC,
        received_at_monotonic_ns=3,
        local_sequence=3,
    )
    for emission in (poly_checkpoint, kalshi_checkpoint):
        assert emission.batches and not emission.controls
        assert emission.batches[0].event["epoch_id"] is not None
        assert emission.batches[0].event["reconstructible"] is False


def test_delta_that_restores_valid_book_opens_reconstructible_checkpoint() -> None:
    polymarket = PolymarketTapeProducer(collector_run_id="run-1", shard_id="poly-0")
    poly_state = MarketBookState(asset_id="token-1")
    poly_snapshot = {
        "event_type": "book",
        "asset_id": "token-1",
        "market": "market-1",
        "bids": [],
        "asks": [{"price": "0.6", "size": "3"}],
    }
    poly_state.apply_book(poly_snapshot)
    polymarket.observe(
        message=poly_snapshot,
        states={"token-1": poly_state},
        received_at_utc=_UTC,
        received_at_monotonic_ns=1,
        local_sequence=1,
    )
    poly_delta = {
        "event_type": "price_change",
        "price_changes": [
            {"asset_id": "token-1", "side": "BUY", "price": "0.4", "size": "1"}
        ],
    }
    poly_state.apply_price_change(poly_delta["price_changes"][0], poly_delta)
    poly_recovery = polymarket.observe(
        message=poly_delta,
        states={"token-1": poly_state},
        received_at_utc=_UTC,
        received_at_monotonic_ns=2,
        local_sequence=2,
    )

    kalshi = KalshiTapeProducer(
        collector_run_id="run-1", shard_id="kalshi-0", use_yes_price=True
    )
    kalshi_state = KalshiOrderBookState("KX-1")
    kalshi_snapshot = {
        "type": "orderbook_snapshot",
        "sid": 1,
        "seq": 1,
        "msg": {
            "market_ticker": "KX-1",
            "yes_dollars": [],
            "no_dollars": [["0.6", "3"]],
        },
    }
    kalshi_state.apply_snapshot(kalshi_snapshot)
    kalshi.observe(
        message=kalshi_snapshot,
        states={"KX-1": kalshi_state},
        received_at_utc=_UTC,
        received_at_monotonic_ns=1,
        local_sequence=1,
    )
    kalshi_delta = {
        "type": "orderbook_delta",
        "sid": 1,
        "seq": 2,
        "msg": {
            "market_ticker": "KX-1",
            "side": "yes",
            "price_dollars": "0.4",
            "delta_fp": "1",
        },
    }
    kalshi_state.apply_delta(kalshi_delta)
    kalshi_recovery = kalshi.observe(
        message=kalshi_delta,
        states={"KX-1": kalshi_state},
        received_at_utc=_UTC,
        received_at_monotonic_ns=2,
        local_sequence=2,
    )

    for emission in (poly_recovery, kalshi_recovery):
        assert emission.barrier_cause is CaptureCommitCause.CHECKPOINT_RESYNC
        assert emission.batches[0].event["event_kind"] == "checkpoint"
        assert emission.batches[0].event["checkpoint_reason"] == "resync"
        assert emission.batches[0].event["reconstructible"] is True
        assert emission.controls[0]["control_type"] == "book_recovered"
        assert emission.controls[0]["epoch_id"] == emission.batches[0].event["epoch_id"]


def test_invalid_snapshots_close_prior_epochs_once_for_both_venues() -> None:
    polymarket = PolymarketTapeProducer(collector_run_id="run-1", shard_id="poly-0")
    poly_state = MarketBookState(asset_id="token-1")
    poly_states = {"token-1": poly_state}
    poly_valid = {
        "event_type": "book",
        "asset_id": "token-1",
        "market": "market-1",
        "bids": [{"price": "0.4", "size": "2"}],
        "asks": [{"price": "0.6", "size": "3"}],
    }
    poly_state.apply_book(poly_valid)
    poly_opened = polymarket.observe(
        message=poly_valid,
        states=poly_states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=1,
        local_sequence=1,
    )
    poly_epoch = poly_opened.batches[0].event["epoch_id"]
    poly_invalid = {
        **poly_valid,
        "bids": [],
        "asks": [],
    }
    poly_state.apply_book(poly_invalid)
    poly_closed = polymarket.observe(
        message=poly_invalid,
        states=poly_states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=2,
        local_sequence=2,
    )
    assert len(poly_closed.controls) == 1
    assert poly_closed.controls[0]["control_type"] == "book_invalidated"
    assert poly_closed.controls[0]["epoch_id"] == poly_epoch
    assert poly_closed.batches[0].event["reconstructible"] is False
    poly_repeated = polymarket.checkpoint_states(
        states=poly_states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=3,
        local_sequence=3,
    )
    assert not poly_repeated.controls
    poly_delta = {
        "event_type": "price_change",
        "asset_id": "token-1",
        "price_changes": [
            {"asset_id": "token-1", "side": "BUY", "price": "0.4", "size": "1"}
        ],
    }
    poly_state.apply_price_change(poly_delta["price_changes"][0], poly_delta)
    poly_audit = polymarket.observe(
        message=poly_delta,
        states=poly_states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=4,
        local_sequence=4,
    )
    assert poly_audit.batches[0].event["epoch_id"] is None
    assert poly_audit.batches[0].event["reconstructible"] is False

    kalshi = KalshiTapeProducer(
        collector_run_id="run-1", shard_id="kalshi-0", use_yes_price=True
    )
    kalshi_state = KalshiOrderBookState("KX-1")
    kalshi_states = {"KX-1": kalshi_state}
    kalshi_valid = {
        "type": "orderbook_snapshot",
        "sid": 1,
        "seq": 1,
        "msg": {
            "market_ticker": "KX-1",
            "yes_dollars": [["0.4", "2"]],
            "no_dollars": [["0.6", "3"]],
        },
    }
    kalshi_state.apply_snapshot(kalshi_valid)
    kalshi_opened = kalshi.observe(
        message=kalshi_valid,
        states=kalshi_states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=1,
        local_sequence=1,
    )
    kalshi_epoch = kalshi_opened.batches[0].event["epoch_id"]
    kalshi_invalid = {
        "type": "orderbook_snapshot",
        "sid": 1,
        "seq": 2,
        "msg": {"market_ticker": "KX-1", "yes_dollars": [], "no_dollars": []},
    }
    kalshi_state.apply_snapshot(kalshi_invalid)
    kalshi_closed = kalshi.observe(
        message=kalshi_invalid,
        states=kalshi_states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=2,
        local_sequence=2,
    )
    assert len(kalshi_closed.controls) == 1
    assert kalshi_closed.controls[0]["control_type"] == "book_invalidated"
    assert kalshi_closed.controls[0]["epoch_id"] == kalshi_epoch
    assert kalshi_closed.batches[0].event["reconstructible"] is False
    kalshi_repeated = kalshi.checkpoint_states(
        states=kalshi_states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=3,
        local_sequence=3,
    )
    assert not kalshi_repeated.controls
    kalshi_delta = {
        "type": "orderbook_delta",
        "sid": 1,
        "seq": 3,
        "msg": {
            "market_ticker": "KX-1",
            "side": "yes",
            "price": "0.4",
            "delta": "1",
        },
    }
    kalshi_state.apply_delta(kalshi_delta)
    kalshi_audit = kalshi.observe(
        message=kalshi_delta,
        states=kalshi_states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=4,
        local_sequence=4,
    )
    assert kalshi_audit.batches[0].event["epoch_id"] is None
    assert kalshi_audit.batches[0].event["reconstructible"] is False


def test_kalshi_producer_fails_closed_after_sequence_gap() -> None:
    producer = KalshiTapeProducer(
        collector_run_id="run-1", shard_id="kalshi-0", use_yes_price=True
    )
    state = KalshiOrderBookState("KX-1")
    states = {"KX-1": state}
    snapshot = {
        "type": "orderbook_snapshot",
        "sid": 1,
        "seq": 1,
        "msg": {
            "market_ticker": "KX-1",
            "yes_dollars": [["0.4", "3"]],
            "no_dollars": [["0.6", "4"]],
        },
    }
    state.apply_snapshot(snapshot)
    opened = producer.observe(
        message=snapshot,
        states=states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=1,
        local_sequence=1,
    )
    assert opened.batches[0].event["reconstructible"] is True
    state.mark_sequence_gap()
    delta = {
        "type": "orderbook_delta",
        "sid": 1,
        "seq": 3,
        "msg": {
            "market_ticker": "KX-1",
            "side": "yes",
            "price": "0.4",
            "delta": "1",
        },
    }
    state.apply_delta(delta)
    invalid = producer.observe(
        message=delta,
        states=states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=2,
        local_sequence=2,
    )
    assert invalid.barrier_cause == "invalidation"
    assert invalid.controls[0]["reason"] == "sequence_gap"
    assert invalid.batches[0].event["reconstructible"] is False
    repeated = producer.observe(
        message=delta,
        states=states,
        received_at_utc=_UTC,
        received_at_monotonic_ns=3,
        local_sequence=3,
    )
    assert repeated.controls == ()
    assert repeated.barrier_cause is None


def _probe_coordinator(
    run_dir: Path,
    *,
    segment_row_limit: int = 1,
    commit_interval_seconds: float = 30.0,
    durability_settings: CaptureDurabilitySettings | None = None,
) -> DurableCaptureCoordinator:
    state = RunStateV1(
        run_id=run_dir.name,
        profile_name="full",
        profile_version="1",
        expected_role_paths={"probe": "datasets/probe"},
        shard_plan={"probe-0": []},
        started_at_utc=_UTC,
    )
    return DurableCaptureCoordinator(
        run_dir=run_dir,
        run_state=state,
        role_schema_versions={"probe": "legacy.probe.v1"},
        role_schemas={
            "probe": pa.schema([pa.field("value", pa.int64(), nullable=False)])
        },
        segment_row_limit=segment_row_limit,
        commit_interval_seconds=commit_interval_seconds,
        durability_settings=durability_settings,
    )


def test_v2_record_covers_index_cause_acceptance_and_complete_checksum(
    tmp_path: Path,
) -> None:
    coordinator = _probe_coordinator(tmp_path)
    coordinator.add("probe", {"value": 1})

    record = coordinator.commit(
        cause=CaptureCommitCause.CHECKPOINT_STARTUP,
        force=True,
    )

    assert record is not None
    assert record.format == CAPTURE_COMMIT_JOURNAL_V2_FORMAT
    assert record.group_index == 0
    assert record.cause is CaptureCommitCause.CHECKPOINT_STARTUP
    assert record.accepted_at_utc <= record.committed_at_utc
    mapping = record.to_mapping()
    assert record.expected_checksum() == mapping["checksum_sha256"]
    assert set(mapping) == {
        "format",
        "group_id",
        "group_index",
        "cause",
        "accepted_at_utc",
        "committed_at_utc",
        "artifacts",
        "checksum_sha256",
    }
    tampered = {**mapping, "cause": CaptureCommitCause.INVALIDATION.value}
    with pytest.raises(ValueError, match="checksum"):
        CaptureCommitRecordV2.from_mapping(tampered)
    with pytest.raises(ValueError, match="not a valid CaptureCommitCause"):
        CaptureCommitRecordV2.from_mapping({**mapping, "cause": "not-canonical"})


def test_only_invalidation_is_coalescible() -> None:
    assert COALESCIBLE_COMMIT_CAUSES == {CaptureCommitCause.INVALIDATION}


def test_row_threshold_wins_and_inline_metrics_reconcile(tmp_path: Path) -> None:
    coordinator = _probe_coordinator(tmp_path, segment_row_limit=1)
    coordinator.add("probe", {"value": 1})

    assert coordinator.due_cause() is CaptureCommitCause.THRESHOLD_ROWS
    record = coordinator.commit()

    assert record is not None
    assert record.cause is CaptureCommitCause.THRESHOLD_ROWS
    durability = coordinator.durability_manifest()
    assert durability["configuration"]["journal_version"] == (
        CAPTURE_COMMIT_JOURNAL_V2_FORMAT
    )
    metrics = durability["metrics"]
    assert metrics["groups_accepted"] == 1
    assert metrics["groups_published"] == 1
    assert metrics["groups_discarded"] == 0
    assert metrics["cause_counts"] == {"threshold_rows": 1}
    assert metrics["maximum_queue_depth"] == 0
    assert metrics["queue_full_wait_count"] == 0
    assert metrics["acceptance_to_journal_latency_ms"]["sample_count"] == 1


def test_bounded_row_staging_does_not_retain_commit_records(tmp_path: Path) -> None:
    coordinator = _probe_coordinator(tmp_path, segment_row_limit=100)
    added = coordinator.add_rows_bounded(
        "probe",
        ({"value": value} for value in range(10)),
        max_rows_per_commit=3,
        cause="termination",
    )

    assert added == 10
    assert coordinator._buffered_rows == 1
    assert not hasattr(coordinator, "_records")
    assert len(validate_commit_journal(tmp_path)) == 3

    coordinator.finalize_segments()
    segment_manifest = json.loads(
        (tmp_path / "datasets" / "probe" / "_segments.json").read_text()
    )
    assert segment_manifest["row_count"] == 10
    assert len(segment_manifest["completed_segments"]) == 4


def test_mm_compact_recovery_controls_coalesce_but_invalidation_is_forced() -> None:
    producer = CompactValidityProducer(
        collector_run_id="run-1",
        shard_id="polymarket-0",
        venue="polymarket",
    )
    row = topbook_row(
        collector_run_id="run-1",
        exchange="polymarket",
        venue_market_id="market-1",
        instrument_id="token-1",
        received_at_utc=_UTC,
        received_at_monotonic_ns=100,
        local_sequence=1,
        valid_state=True,
    )
    recovered = producer.observe_topbook(
        row=row,
        coordinate=_coordinate(),
        venue_book_id="token-1",
        venue_market_id="market-1",
    )
    invalidated = producer.observe_topbook(
        row={**row, "valid_state": False, "local_sequence": 2},
        coordinate=_coordinate(sequence=2),
        venue_book_id="token-1",
        venue_market_id="market-1",
    )

    assert recovered.controls
    assert recovered.barrier_cause is None
    assert invalidated.barrier_cause is CaptureCommitCause.INVALIDATION


def test_inline_latency_summaries_are_validated(
    tmp_path: Path,
) -> None:
    coordinator = _probe_coordinator(tmp_path, segment_row_limit=1)
    coordinator.add("probe", {"value": 1})
    record = coordinator.commit()
    assert record is not None
    latency = coordinator.durability_manifest()["metrics"][
        "acceptance_to_journal_latency_ms"
    ]

    assert not _durability_latency_metric_errors(
        latency,
        records=(record,),
        recovered=False,
    )
    non_monotonic = {
        **latency,
        "p50": float(latency["maximum"]) + 1.0,
    }
    assert any(
        "must be monotonic" in error
        for error in _durability_latency_metric_errors(
            non_monotonic,
            records=(record,),
            recovered=False,
        )
    )
    negative = {**latency, "p95": -1.0}
    assert any(
        ".p95 must be a nonnegative finite number" in error
        for error in _durability_latency_metric_errors(
            negative,
            records=(record,),
            recovered=False,
        )
    )


def test_time_threshold_is_reported_when_row_threshold_is_not_due(
    tmp_path: Path,
) -> None:
    coordinator = _probe_coordinator(
        tmp_path,
        segment_row_limit=10,
        commit_interval_seconds=0.001,
    )
    coordinator.add("probe", {"value": 1})
    coordinator._last_commit_monotonic -= 1.0

    assert coordinator.due_cause() is CaptureCommitCause.THRESHOLD_TIME
    record = coordinator.commit()
    assert record is not None
    assert record.cause is CaptureCommitCause.THRESHOLD_TIME


def test_durability_settings_persist_requested_effective_adjustment() -> None:
    settings = CaptureDurabilitySettings.resolve(
        requested_segment_rows=123,
        requested_segment_seconds=45,
    )

    assert settings.requested_segment_rows == 123
    assert settings.effective_segment_rows == 123
    assert settings.requested_segment_seconds == 45
    assert settings.effective_segment_seconds == 45
    assert settings.segment_limit_adjustments == ()
    assert CaptureDurabilitySettings.from_mapping(settings.to_mapping()) == settings

    bounded = CaptureDurabilitySettings.resolve(
        requested_segment_rows=123,
        requested_segment_seconds=301,
    )
    assert bounded.effective_segment_seconds == 300
    assert bounded.segment_limit_adjustments == (
        {
            "field": "segment_seconds",
            "requested": 301.0,
            "effective": 300.0,
            "reason": "bounded_maximum_uncommitted_interval",
        },
    )
    with pytest.raises(ValueError, match="between 0 and 30"):
        CaptureDurabilitySettings(barrier_coalesce_seconds=31)
    with pytest.raises(ValueError, match="acceptance value 15"):
        CaptureDurabilitySettings(publication_deadline_seconds=14)
    with pytest.raises(ValueError, match="positive integer"):
        CaptureDurabilitySettings(max_pending_publish_groups=1.5)  # type: ignore[arg-type]
    incomplete = settings.to_mapping()
    incomplete.pop("publication_mode")
    with pytest.raises(ValueError, match="missing capture durability fields"):
        CaptureDurabilitySettings.from_mapping(incomplete)


def test_async_publication_cannot_be_claimed_before_canary_acceptance(
    tmp_path: Path,
) -> None:
    settings = CaptureDurabilitySettings.resolve(
        requested_segment_rows=1,
        requested_segment_seconds=30,
        publication_mode="async",
    )
    with pytest.raises(ValueError, match="async publication is disabled"):
        _probe_coordinator(
            tmp_path,
            durability_settings=settings,
        )


def test_v1_journal_remains_readable_and_reports_legacy_unknown(
    tmp_path: Path,
) -> None:
    coordinator = _probe_coordinator(tmp_path)
    coordinator.add("probe", {"value": 1})
    current = coordinator.commit(
        cause=CaptureCommitCause.CHECKPOINT_STARTUP,
        force=True,
    )
    assert current is not None
    legacy = CaptureCommitRecordV1.create(
        group_id=current.group_id,
        committed_at_utc=current.committed_at_utc,
        artifacts=current.artifacts,
    )
    (tmp_path / COMMIT_JOURNAL_V2_NAME).unlink()
    (tmp_path / COMMIT_JOURNAL_V1_NAME).write_text(
        json.dumps(legacy.to_mapping()) + "\n",
        encoding="utf-8",
    )
    state_path = tmp_path / RUN_STATE_NAME
    state_payload = json.loads(state_path.read_text(encoding="utf-8"))
    state_payload.pop("capture_durability", None)
    state_path.write_text(json.dumps(state_payload) + "\n", encoding="utf-8")

    report = recover_stream_run(tmp_path)
    records = validate_commit_journal(tmp_path)

    assert report.journal_errors == ()
    assert report.journal_version == CAPTURE_COMMIT_JOURNAL_V1_FORMAT
    assert report.commit_cause_counts == {LEGACY_UNKNOWN_COMMIT_CAUSE: 1}
    assert len(records) == 1
    assert records[0].cause == LEGACY_UNKNOWN_COMMIT_CAUSE
    assert resolve_commit_journal_path(tmp_path).name == COMMIT_JOURNAL_V1_NAME


def test_v2_recovery_requires_valid_persisted_durability_configuration(
    tmp_path: Path,
) -> None:
    coordinator = _probe_coordinator(tmp_path)
    coordinator.add("probe", {"value": 1})
    assert (
        coordinator.commit(
            cause=CaptureCommitCause.CHECKPOINT_STARTUP,
            force=True,
        )
        is not None
    )
    state_path = tmp_path / RUN_STATE_NAME
    state_payload = json.loads(state_path.read_text(encoding="utf-8"))
    state_payload.pop("capture_durability")
    state_path.write_text(json.dumps(state_payload) + "\n", encoding="utf-8")

    report = recover_stream_run(tmp_path)

    assert report.valid_group_count == 0
    assert any(
        "requires persisted capture_durability" in item
        for item in report.journal_errors
    )


def test_mixed_journal_files_and_records_fail_closed(tmp_path: Path) -> None:
    coordinator = _probe_coordinator(tmp_path)
    coordinator.add("probe", {"value": 1})
    current = coordinator.commit(
        cause=CaptureCommitCause.CHECKPOINT_STARTUP,
        force=True,
    )
    assert current is not None
    legacy = CaptureCommitRecordV1.create(
        group_id=current.group_id,
        committed_at_utc=current.committed_at_utc,
        artifacts=current.artifacts,
    )
    legacy_line = json.dumps(legacy.to_mapping()) + "\n"
    (tmp_path / COMMIT_JOURNAL_V1_NAME).write_text(legacy_line, encoding="utf-8")

    mixed_files = recover_stream_run(tmp_path)
    assert mixed_files.valid_group_count == 0
    assert any("mixed journal versions" in item for item in mixed_files.journal_errors)
    with pytest.raises(ValueError, match="mixed journal versions"):
        validate_commit_journal(tmp_path)

    (tmp_path / COMMIT_JOURNAL_V1_NAME).unlink()
    with (tmp_path / COMMIT_JOURNAL_V2_NAME).open("a", encoding="utf-8") as handle:
        handle.write(legacy_line)
    mixed_records = recover_stream_run(tmp_path)
    assert mixed_records.valid_group_count == 0
    assert any(
        "mixed journal versions" in item for item in mixed_records.journal_errors
    )
