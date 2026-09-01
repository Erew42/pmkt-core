from __future__ import annotations

from decimal import Decimal
from fractions import Fraction
from pathlib import Path
import re

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from pmkt.cli.app import app
import pmkt.data.registry as registry_module
from pmkt.data.registry import FieldSpec, TableSpec, get_table_spec, list_table_specs
from pmkt.data.canonical import (
    kalshi_market_snapshot_v2_row,
    order_intent_row,
    paper_fill_row,
    paper_position_row,
    polymarket_market_snapshot_v2_row,
    run_manifest_row,
    signal_row,
)
from pmkt.data.schemas import topbook_row
from pmkt.data.storage.parquet import write_parquet
from pmkt.data.types import parse_int
from pmkt.data.validation import (
    coerce_frame,
    coerce_snapshot_frame,
    convert_frame_strict,
    infer_and_validate_frame,
    validate_frame,
)


def test_schema_registry_includes_major_schema_families() -> None:
    versions = {spec.version for spec in list_table_specs()}

    assert {
        "event.v1",
        "market.v1",
        "polymarket_market_snapshot.v1",
        "kalshi_market_snapshot.v1",
        "polymarket_market_snapshot.v2",
        "kalshi_market_snapshot.v2",
        "instrument.v1",
        "topbook.v1",
        "depth.v1",
        "trade.v1",
        "market_match.v1",
        "market_match.v2",
        "tracking_match.v1",
        "match_relation.v1",
        "feed_health.v1",
        "tracking_health.v1",
        "signal.v1",
        "execution_sizing_plan.v1",
        "order_intent.v1",
        "order_state.v1",
        "paper_fill.v1",
        "paper_position.v1",
        "arbitrage_candidate.v1",
        "run_manifest.v1",
        "soak_run_plan.v1",
        "soak_run_report.v1",
        "co_resolution_observation.v1",
        "co_resolution_score.v1",
    }.issubset(versions)


def test_passive_output_schemas_use_contract_specific_venue_and_side_enums() -> None:
    quote_fields = {
        field.name: field
        for field in get_table_spec(
            registry_module.PASSIVE_QUOTE_EVALUATION_SCHEMA_VERSION
        ).fields
    }
    assert quote_fields["venue"].allowed_values is None
    assert quote_fields["side"].allowed_values is None

    for version in (
        registry_module.PASSIVE_FILL_SCHEMA_VERSION,
        registry_module.PASSIVE_MARKOUT_SCHEMA_VERSION,
    ):
        fields = {field.name: field for field in get_table_spec(version).fields}

        assert fields["venue"].allowed_values == ("polymarket", "kalshi")
        assert fields["side"].allowed_values is not None
        assert "bid" in fields["side"].allowed_values
        assert "ask" in fields["side"].allowed_values


def test_data_dictionary_covers_registered_schema_versions() -> None:
    docs = Path("docs/data_dictionary.md").read_text(encoding="utf-8")
    documented_versions = set(re.findall(r"`([a-z0-9_]+\.v\d+)`", docs))

    assert registry_module.schema_documentation_gaps(documented_versions) == {}


def test_schema_specs_carry_source_truth_metadata() -> None:
    for spec in list_table_specs():
        assert spec.source_truth == "pmkt.data.registry"
        assert spec.documentation_ref == "docs/data_dictionary.md"


def test_validate_topbook_catches_invalid_spread_and_prices() -> None:
    df = pd.DataFrame(
        [
            topbook_row(
                exchange="polymarket",
                instrument_id="token-1",
                received_at_utc="2026-05-26T00:00:00+00:00",
                best_bid_dollars=0.70,
                best_ask_dollars=0.60,
                spread_dollars=-0.10,
                valid_state=True,
                quality_flags=[],
            )
        ]
    )

    report = validate_frame(df, "topbook.v1")

    assert not report.ok
    assert any("negative spreads" in error for error in report.errors)


def test_validate_frame_requires_matching_schema_version() -> None:
    row = topbook_row(
        exchange="polymarket",
        instrument_id="token-1",
        received_at_utc="2026-05-26T00:00:00+00:00",
        best_bid_dollars=0.40,
        best_ask_dollars=0.60,
        spread_dollars=0.20,
        valid_state=True,
        quality_flags=[],
    )
    row["schema_version"] = "depth.v1"

    report = validate_frame(pd.DataFrame([row]), "topbook.v1")

    assert not report.ok
    assert any("schema_version" in error for error in report.errors)


def test_validate_frame_rejects_container_values_in_string_fields() -> None:
    row = topbook_row(
        exchange="polymarket",
        instrument_id={"unexpected": "mapping"},
        received_at_utc="2026-05-26T00:00:00+00:00",
        best_bid_dollars=0.40,
        best_ask_dollars=0.60,
        spread_dollars=0.20,
        valid_state=True,
        quality_flags=[],
    )

    report = validate_frame(pd.DataFrame([row]), "topbook.v1")

    assert not report.ok
    assert any(
        "instrument_id" in error and "string" in error for error in report.errors
    )


def test_validate_topbook_catches_kalshi_yes_no_complement_mismatch() -> None:
    df = pd.DataFrame(
        [
            topbook_row(
                collector_run_id="run-1",
                exchange="kalshi",
                venue_market_id="KXTEST",
                instrument_id="KXTEST:YES",
                outcome="YES",
                received_at_utc="2026-05-26T00:00:00+00:00",
                local_sequence=1,
                best_bid_dollars=0.40,
                best_ask_dollars=0.70,
                spread_dollars=0.30,
                valid_state=True,
                quality_flags=[],
            ),
            topbook_row(
                collector_run_id="run-1",
                exchange="kalshi",
                venue_market_id="KXTEST",
                instrument_id="KXTEST:NO",
                outcome="NO",
                received_at_utc="2026-05-26T00:00:00+00:00",
                local_sequence=1,
                best_bid_dollars=0.35,
                best_ask_dollars=0.60,
                spread_dollars=0.25,
                valid_state=True,
                quality_flags=[],
            ),
        ]
    )

    report = validate_frame(df, "topbook.v1")

    assert not report.ok
    assert any("YES asks do not equal 1 - NO bid" in error for error in report.errors)


def _valid_field_value(field: FieldSpec, spec: TableSpec):
    if field.name == "schema_version":
        return spec.version
    if field.name in {"venue", "exchange"} or field.name.endswith("_exchange"):
        return "polymarket"
    if field.name == "outcome":
        return "YES"
    if field.name == "side":
        return "bid"
    if field.allowed_values:
        return field.allowed_values[0]
    if field.dtype == "float64":
        return 0.0
    if field.dtype in {"int32", "int64"}:
        return 1
    if field.dtype == "bool":
        return False
    if field.dtype == "json":
        return {}
    if field.dtype == "list[string]":
        return []
    if field.dtype == "large_string":
        return "[]"
    return f"{field.name}-value"


def _valid_schema_row(spec: TableSpec) -> dict[str, object]:
    row = {field.name: _valid_field_value(field, spec) for field in spec.fields}
    if spec.version == "topbook.v1":
        row.update(
            {
                "best_bid_dollars": 0.4,
                "best_ask_dollars": 0.6,
                "spread_dollars": 0.2,
                "valid_state": True,
                "quality_flags": [],
            }
        )
    if spec.version == "depth.v1":
        row.update(
            {
                "price_dollars": 0.4,
                "size_contracts": 1.0,
                "cumulative_size_contracts": 1.0,
                "level_index": 0,
                "valid_state": True,
                "quality_flags": [],
            }
        )
    if spec.version == "arbitrage_candidate.v1":
        row.update(
            {
                "polymarket_bid_dollars": 0.4,
                "polymarket_ask_dollars": 0.6,
                "kalshi_bid_dollars": 0.4,
                "kalshi_ask_dollars": 0.6,
                "valid_state": True,
                "quality_flags": [],
                "is_research_candidate": True,
                "execution_ready": False,
            }
        )
    if spec.version == "maker_quote_plan.v1":
        row.update(
            {
                "quote_action": "buy",
                "quote_book_side": "bid",
                "quote_price": 0.49,
                "quote_top_bid": 0.48,
                "quote_top_ask": 0.50,
                "quote_size_contracts": 10.0,
                "requote_threshold": 0.01,
                "hedge_action": "sell",
                "hedge_book_side": "bid",
                "hedge_average_price": 0.59,
                "hedge_limit_price": 0.58,
                "hedge_size_contracts": 10.0,
                "maker_fee_dollars": 0.0,
                "hedge_taker_fee_dollars": 0.059,
                "gross_edge": 0.10,
                "slippage_allowance": 0.001,
                "net_edge": 0.0931,
                "post_only": True,
                "risk_flags": "",
                "current_quote_ref_json": {},
            }
        )
    if spec.version in {
        "polymarket_market_snapshot.v1",
        "polymarket_market_snapshot.v2",
    }:
        row.update(
            {
                "market_id": "pm-1",
                "question": "Will Candidate A win?",
                "token_ids": ["token-yes", "token-no"],
                "yes_bid": 0.4,
                "yes_ask": 0.6,
                "mid": 0.5,
                "spread": 0.2,
                "last_trade_price": 0.5,
            }
        )
    if spec.version == "polymarket_market_snapshot.v1":
        row.update(
            {
                "condition_id": "0xabc",
                "question_id": "q-1",
                "outcome_labels_json": ["yes", "no"],
                "outcome_prices_json": ["1", "0"],
                "uma_resolution_status": "resolved",
                "resolved_by": "uma",
                "resolution_source": "oracle",
            }
        )
    if spec.version in {"kalshi_market_snapshot.v1", "kalshi_market_snapshot.v2"}:
        row.update(
            {
                "exchange": "kalshi",
                "market_key": "KXTEST",
                "instrument_key": "KXTEST:YES",
                "question": "Will Candidate A win?",
                "yes_bid": 0.4,
                "yes_ask": 0.6,
                "no_bid": 0.4,
                "no_ask": 0.6,
                "mid": 0.5,
                "spread": 0.2,
                "last_price": 0.5,
            }
        )
    if spec.version == "kalshi_market_snapshot.v1":
        row.update(
            {
                "result": "yes",
                "settlement_value_dollars": "1",
                "settlement_ts": "2026-01-01T00:00:00Z",
                "expiration_value": "1",
                "is_provisional": True,
                "rules_primary": "Primary rule",
                "rules_secondary": "Secondary rule",
            }
        )
    if spec.version == "contract_evidence.v1":
        row.update(
            {
                "evidence_id": "a" * 64,
                "venue": "polymarket",
                "market_key": "pm-1",
                "venue_event_key": "event-1",
                "source_endpoint": "gamma:/markets",
                "payload_scope": "list",
                "observed_at_utc": "2026-07-10T17:59:00+00:00",
                "derived_at_utc": "2026-07-10T18:00:00+00:00",
                "source_row_hash": "b" * 64,
                "raw_payload_hash": "c" * 64,
                "evidence_projection_hash": "d" * 64,
                "question": "Will Candidate A win?",
                "rules_text": "Resolves from the official result.",
                "close_time": "2026-07-11T00:00:00+00:00",
                "instrument_mapping_json": [
                    {"instrument_key": "token-yes", "outcome": "Yes"}
                ],
                "contract_fields_json": {"question": "Will Candidate A win?"},
                "field_provenance_json": {"question": ["question"]},
                "identity_complete": True,
                "rules_complete": True,
                "instrument_mapping_complete": True,
                "completeness_reasons_json": [],
            }
        )
    if spec.version == "market_taxonomy_evidence.v1":
        row.update(
            {
                "native_tags_json": "[]",
                "native_series_json": "[]",
                "structured_sport_json": "{}",
                "requested_at_utc": "2026-08-12T10:00:00Z",
                "observed_at_utc": "2026-08-12T10:00:01Z",
                "source_payload_sha256": "a" * 64,
                "snapshot_raw_json_sha256": "b" * 64,
                "issues_json": "[]",
            }
        )
    if spec.version == "match_relation.v1":
        row.update(
            {
                "match_id": "pm:pm-1:token-1|kalshi:KXTEST:KXTEST:YES",
                "polymarket_market_key": "pm-1",
                "polymarket_instrument_key": "token-1",
                "relation_label": "same_event_different_cutoff",
                "is_trade_equivalent": False,
                "is_tracking_useful": True,
                "polymarket_token_id": "token-1",
                "kalshi_market_key": "KXTEST",
                "kalshi_instrument_key": "KXTEST:YES",
                "evidence_json": {
                    "source_row_hashes": {
                        "polymarket": "pm-source",
                        "kalshi": "kx-source",
                    },
                    "contract_fields": {
                        "polymarket": {"question": "Will Candidate A win?"},
                        "kalshi": {"question": "Candidate A to win?"},
                    },
                },
            }
        )
    if spec.version == "tracking_match.v1":
        row.update(
            {
                "tracking_pair_id": "pm:pm-1|kalshi:KXTEST",
                "match_tier": "track_event_related",
                "relation_type": "same_event_different_outcome",
                "polymarket_market_key": "pm-1",
                "polymarket_question": "Will Candidate A win?",
                "kalshi_market_key": "KXTEST",
                "kalshi_question": "Candidate A to win?",
                "confidence_score": 0.5,
                "title_similarity": 0.5,
            }
        )
    if spec.version == "tracking_health.v1":
        row.update(
            {
                "polymarket_quote_age_ms": 100,
                "kalshi_quote_age_ms": 100,
                "max_quote_age_ms": 1000,
                "polymarket_valid_book": True,
                "kalshi_valid_book": True,
                "tracking_ready": True,
                "health_status": "ready",
                "health_flags": "",
            }
        )
    if spec.version == "feed_health.v1":
        row.update(
            {
                "venue": "polymarket",
                "shard_id": "polymarket-0",
                "connection_state": "connected",
                "instrument_count": 1,
                "relation_count": 1,
                "reconnect_count": 0,
                "sequence_gap_count": 0,
                "resync_count": 0,
                "error_count": 0,
                "valid_book_count": 1,
                "invalid_book_count": 0,
                "valid_instrument_count": 1,
                "invalid_instrument_count": 0,
                "stale_instrument_count": 0,
                "missing_instrument_count": 0,
                "instrument_state_json": "[]",
                "quality_flags": [],
            }
        )
    if spec.version == "trade.v1":
        row.update(
            {
                "collector_run_id": "run-1",
                "trade_ts_utc": "2026-07-19T10:00:00Z",
                "received_at_utc": "2026-07-19T10:00:00Z",
                "received_at_monotonic_ns": 1,
                "local_sequence": 1,
                "subsequence": 0,
            }
        )
    if spec.version == "book_tape_event.v1":
        row.update(
            {
                "collector_run_id": "run-1",
                "event_id": "1" * 64,
                "venue": "polymarket",
                "venue_market_id": "market-1",
                "venue_book_id": "token-1",
                "event_kind": "checkpoint",
                "epoch_id": "2" * 64,
                "checkpoint_reason": "startup",
                "received_at_utc": "2026-07-19T10:00:00Z",
                "received_at_monotonic_ns": 1,
                "exchange_at_utc": "2026-07-19T10:00:00Z",
                "local_sequence": 1,
                "subsequence": 0,
                "expected_level_row_count": 0,
                "side_counts_json": '{"ask":0,"bid":0}',
                "post_book_hash": "3" * 64,
                "valid_state": True,
                "reconstructible": True,
                "quality_flags_json": "[]",
                "raw_event_hash": "4" * 64,
                "event_payload_hash": "5" * 64,
                "encoding_version": "book-tape.v1",
            }
        )
    if spec.version == "book_tape_level.v1":
        row.update(
            {
                "collector_run_id": "run-1",
                "event_id": "1" * 64,
                "venue": "polymarket",
                "venue_book_id": "token-1",
                "epoch_id": "2" * 64,
                "source_side": "bid",
                "price_key": "0.4",
                "price_dollars": 0.4,
                "size_after_contracts": 1.0,
                "size_delta_contracts": 1.0,
                "level_ordinal": 0,
            }
        )
    if spec.version == "book_tape_control.v1":
        row.update(
            {
                "collector_run_id": "run-1",
                "control_id": "6" * 64,
                "venue": "polymarket",
                "venue_market_id": "market-1",
                "venue_book_id": "token-1",
                "control_type": "book_recovered",
                "reason": "startup_snapshot",
                "valid_after": True,
                "received_at_utc": "2026-07-19T10:00:00Z",
                "received_at_monotonic_ns": 1,
                "exchange_at_utc": "2026-07-19T10:00:00Z",
                "local_sequence": 1,
                "subsequence": 0,
                "epoch_id": "2" * 64,
                "evidence_role": "tape_event",
                "evidence_id": "1" * 64,
                "quality_flags_json": "[]",
            }
        )
    if spec.version == "stream_lifecycle.v1":
        row.update(
            {
                "collector_run_id": "run-1",
                "lifecycle_event_id": "7" * 64,
                "venue": "polymarket",
                "venue_market_id": "market-1",
                "event_type": "new_market",
                "received_at_utc": "2026-07-19T10:00:00Z",
                "received_at_monotonic_ns": 1,
                "exchange_at_utc": "2026-07-19T10:00:00Z",
                "local_sequence": 1,
                "subsequence": 0,
                "previous_tick_size_dollars": 0.01,
                "new_tick_size_dollars": 0.01,
                "market_close_at_utc": "2026-07-20T10:00:00Z",
                "raw_event_hash": "8" * 64,
                "quality_flags_json": "[]",
            }
        )
    if spec.version == "signal.v1":
        row.update(
            {
                "relation_label": "same_event_different_outcome",
                "gross_edge": 0.01,
                "fee_estimate": 0.0,
                "slippage_estimate": 0.0,
                "net_edge": 0.01,
                "executable_size": 1.0,
                "quote_age_ms": 1,
                "risk_flags": "non_trade_equivalent_relation",
                "decision": "observe",
                "execution_allowed": False,
            }
        )
    if spec.version == "execution_sizing_plan.v1":
        row.update(
            {
                "plan_id": "plan-1",
                "plan_fingerprint": "fingerprint-1",
                "match_id": "match-1",
                "signal_id": "signal-1",
                "side_plan": "buy_polymarket_sell_kalshi",
                "sizing_source": "depth_adjusted",
                "execution_allowed": True,
                "risk_flags": "",
                "gross_edge": 0.15,
                "fee_estimate": 0.0,
                "depth_adjusted_net_edge": 0.15,
                "executable_size": 10.0,
                "cash_notional_dollars": 8.5,
                "max_loss_dollars": 8.5,
                "unhedged_first_leg_max_loss_dollars": 4.0,
                "gross_leg_notional_dollars": 9.5,
                "depth_levels_consumed": 1,
                "intended_orders_json": [
                    {
                        "venue": "polymarket",
                        "instrument_id": "pm-token",
                        "action": "buy",
                        "book_side": "ask",
                        "limit_price": "0.40",
                        "average_price": "0.40",
                        "size_contracts": "10",
                        "fees_dollars": "0",
                    }
                ],
                "metadata_snapshot_json": {},
                "book_state_refs_json": {},
                "depth_refs_json": {},
                "created_at_utc": "2026-06-05T12:00:00+00:00",
            }
        )
    if spec.version == "order_intent.v1":
        row.update(
            {
                "venue": "polymarket",
                "action": "buy",
                "book_side": "ask",
                "limit_price": 0.4,
                "size_contracts": 1.0,
                "post_only": False,
                "reduce_only": False,
                "risk_check_status": "passed",
                "risk_check_json": {},
                "mode": "paper",
            }
        )
    if spec.version == "order_state.v1":
        row.update(
            {
                "venue": "polymarket",
                "status": "filled",
                "filled_size_contracts": 1.0,
                "remaining_size_contracts": 0.0,
                "average_fill_price": 0.4,
                "fees_dollars": 0.0,
                "source": "paper",
                "reconcile_status": "simulated",
            }
        )
    if spec.version == "paper_fill.v1":
        row.update(
            {
                "venue": "polymarket",
                "action": "buy",
                "book_side": "ask",
                "fill_price_dollars": 0.4,
                "size_contracts": 1.0,
                "notional_dollars": 0.4,
                "fees_dollars": 0.0,
                "latency_ms": 1,
            }
        )
    if spec.version == "paper_position.v1":
        row.update(
            {
                "status": "open",
                "source_fill_ids": ["fill-1"],
                "filled_size_contracts": 1.0,
                "unmatched_leg_size_contracts": 0.0,
                "buy_notional_dollars": 0.4,
                "sell_notional_dollars": 0.5,
                "fees_dollars": 0.0,
                "gross_pnl_dollars": 0.1,
                "realized_pnl_dollars": 0.1,
                "unrealized_pnl_dollars": 0.0,
                "net_pnl_dollars": 0.1,
            }
        )
    if spec.version == "backtest_report.v1":
        row.update(
            {
                "strategy_family": "taker",
                "source_kind": "taker_batch",
                "fill_model": "optimistic",
                "fill_model_params_json": {"name": "optimistic"},
                "decision_count": 1,
                "attempted_count": 1,
                "fill_count": 1,
                "position_count": 1,
                "filled_size_contracts": 1.0,
                "capital_at_risk_dollars": 1.0,
                "max_concurrent_notional_dollars": 1.0,
                "theoretical_edge_dollars": 0.1,
                "fee_drag_dollars": 0.0,
                "markout_window_count": 0,
                "estimated_pnl_lower_dollars": 0.0,
                "estimated_pnl_upper_dollars": 0.0,
                "return_on_capital_lower": 0.0,
                "return_on_capital_upper": 0.0,
                "edge_capture_ratio": 0.0,
                "fee_drag_ratio": 0.0,
                "fill_rate": 1.0,
                "markout_pnl_lower_dollars": 0.0,
                "markout_pnl_upper_dollars": 0.0,
                "markout_decay_json": {},
                "data_quality_caveats": ["test_fixture"],
                "assumption_caveats": ["test_fixture"],
                "source_artifacts_json": {},
            }
        )
    if spec.version == "co_resolution_observation.v1":
        row.update(
            {
                "observation_run_id": "obs-run-1",
                "created_at_utc": "2026-06-30T00:00:00+00:00",
                "polymarket_market_key": "pm-1",
                "polymarket_instrument_key": "pm-1:YES",
                "kalshi_market_key": "KXTEST",
                "kalshi_instrument_key": "KXTEST:YES",
                "polymarket_terminal_label": "yes",
                "kalshi_terminal_label": "no",
                "polymarket_binary_yes": 1,
                "kalshi_binary_yes": 0,
                "binary_label_grain": "instrument",
                "both_terminal_known": True,
                "known_binary_pair": True,
                "same_terminal_outcome": False,
                "same_binary_outcome": False,
                "inverse_binary_outcome": True,
                "polymarket_marginal_probability": 0.6,
                "kalshi_marginal_probability": 0.4,
                "independent_same_probability": 0.48,
                "independent_inverse_probability": 0.52,
                "source_match_schema_version": "market_match.v2",
                "source_resolution_schema_version": "market_resolution.v1",
                "label_policy_id": "canonical_final_binary.v1",
                "feature_bucket_version": "co_resolution_feature_buckets.v1",
                "data_quality_flags": [],
                "included_in_fit": True,
                "exclusion_reason": None,
            }
        )
    if spec.version == "co_resolution_score.v1":
        row.update(
            {
                "experiment_id": "experiment-1",
                "manifest_hash": "hash-1",
                "model_version": "co_resolution_model.v1",
                "model_family": "beta_binomial_bucket.v1",
                "model_spec_id": "model-spec-1",
                "bucket_strategy_id": "co_resolution_bucket_strategy.v1",
                "feature_set_id": "co_resolution_features.v1",
                "label_policy_id": "canonical_final_binary.v1",
                "score_semantics": "binary_same_inverse_complement",
                "scorer_run_id": "score-run-1",
                "scored_at_utc": "2026-06-30T00:00:00+00:00",
                "polymarket_market_key": "pm-1",
                "polymarket_instrument_key": "pm-1:YES",
                "kalshi_market_key": "KXTEST",
                "kalshi_instrument_key": "KXTEST:YES",
                "source_match_schema_version": "market_match.v2",
                "co_resolution_probability": 0.75,
                "co_resolution_lower": 0.65,
                "co_resolution_upper": 0.85,
                "inverse_resolution_probability": 0.25,
                "inverse_resolution_lower": 0.15,
                "inverse_resolution_upper": 0.35,
                "independent_same_probability": 0.50,
                "independent_inverse_probability": 0.50,
                "co_resolution_lift": 0.25,
                "inverse_resolution_lift": -0.25,
                "baseline_probability_source": "match_mid",
                "baseline_probability_quality": "available",
                "terminal_evidence_quality": "fit_ready",
                "binary_label_grain": "instrument",
                "selected_bucket_level": "global",
                "selected_bucket_n": 100,
                "feature_bucket_version": "co_resolution_bucket_strategy.v1",
                "score_source": "beta_binomial_bucket.v1",
                "data_quality_flags": [],
                "risk_flags": "",
                "complement_residual": 0.0,
                "model_diagnostics_json": {
                    "posterior_n": 100,
                    "posterior_group_key_json": "__global__",
                },
                "research_only": True,
                "allowed_consumers_json": ["research_report", "manual_review"],
            }
        )
    return row


def test_all_major_schema_families_validate_valid_and_missing_required_frames() -> None:
    for spec in list_table_specs():
        row = _valid_schema_row(spec)
        valid_report = validate_frame(pd.DataFrame([row]), spec)
        assert valid_report.ok, (spec.version, valid_report.errors)

        required_column = next(
            column for column in spec.primary_key if column != "schema_version"
        )
        invalid_report = validate_frame(
            pd.DataFrame([row]).drop(columns=[required_column]), spec
        )
        assert not invalid_report.ok
        assert required_column in invalid_report.missing_columns


def test_coerce_frame_orders_columns_and_sets_schema_version() -> None:
    df = pd.DataFrame(
        [
            {
                "exchange": "kalshi",
                "instrument_id": "KXTEST:YES",
                "received_at_utc": "2026-05-26T00:00:00+00:00",
                "best_bid_dollars": "0.4",
                "valid_state": "true",
            }
        ]
    )

    coerced = coerce_frame(df, get_table_spec("topbook.v1"))

    assert list(coerced.columns) == list(get_table_spec("topbook.v1").columns)
    assert coerced.loc[0, "schema_version"] == "topbook.v1"
    assert coerced.loc[0, "best_bid_dollars"] == 0.4


@pytest.mark.parametrize(
    "value",
    [
        "not-json",
        "",
        "{",
        "NaN",
        "Infinity",
        '{"value": NaN}',
    ],
)
def test_strict_converter_rejects_bad_json_without_changing_compatibility(
    value: str,
) -> None:
    spec = TableSpec(
        name="strict_json",
        version="strict_json.v1",
        fields=(
            FieldSpec("schema_version", "string", False),
            FieldSpec("payload", "json", False),
        ),
    )
    frame = pd.DataFrame({"payload": [value]})

    # The broad validator/coercer remains a compatibility surface until its
    # retained JSON scan is practical. New strict writers do not inherit that
    # permissive string behavior.
    coerced = coerce_frame(frame, spec)

    assert coerced.loc[0, "payload"] == value
    with pytest.raises(ValueError, match="payload: 1 values incompatible with strict json"):
        convert_frame_strict(frame, spec)


@pytest.mark.parametrize(
    "value",
    [float("inf"), {1: "non-string-key"}, {"unordered", "set"}, object()],
)
def test_strict_converter_rejects_lossy_json_object_conversion(value: object) -> None:
    spec = TableSpec(
        name="strict_json",
        version="strict_json.v1",
        fields=(
            FieldSpec("schema_version", "string", False),
            FieldSpec("payload", "json", False),
        ),
    )

    with pytest.raises(ValueError, match="payload: 1 values incompatible with strict json"):
        convert_frame_strict(pd.DataFrame({"payload": [value]}), spec)


def test_convert_frame_strict_preserves_json_text_and_rejects_projection() -> None:
    spec = TableSpec(
        name="strict_json",
        version="strict_json.v1",
        fields=(
            FieldSpec("schema_version", "string", False),
            FieldSpec("payload", "json", False),
            FieldSpec("note", "string", True),
        ),
    )
    exact_json = ' { "b": 2, "a": 1 } '

    converted = convert_frame_strict(
        pd.DataFrame({"payload": [exact_json]}),
        spec,
    )

    assert converted.loc[0, "schema_version"] == spec.version
    assert converted.loc[0, "payload"] == exact_json
    assert pd.isna(converted.loc[0, "note"])

    with pytest.raises(ValueError, match="extra columns: debug"):
        convert_frame_strict(
            pd.DataFrame({"payload": [exact_json], "debug": ["do-not-drop"]}),
            spec,
        )


def test_convert_frame_strict_rejects_values_that_cleaning_would_null() -> None:
    spec = TableSpec(
        name="strict_integer",
        version="strict_integer.v1",
        fields=(
            FieldSpec("schema_version", "string", False),
            FieldSpec("count", "int64", True),
        ),
    )
    source = pd.DataFrame({"count": ["1.5"]})

    assert pd.isna(coerce_frame(source, spec).loc[0, "count"])
    with pytest.raises(ValueError, match="count: 1 values incompatible with int64"):
        convert_frame_strict(source, spec)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("stale_quote;sequence_gap", ["stale_quote", "sequence_gap"]),
        ("stale_quote,sequence_gap", ["stale_quote", "sequence_gap"]),
        ('["stale_quote", "sequence_gap"]', ["stale_quote", "sequence_gap"]),
    ],
)
def test_coerce_frame_normalizes_legacy_flag_containers(
    raw: str, expected: list[str]
) -> None:
    row = topbook_row(
        exchange="polymarket",
        instrument_id="token-1",
        received_at_utc="2026-05-26T00:00:00+00:00",
        best_bid_dollars=0.4,
        best_ask_dollars=0.6,
        spread_dollars=0.2,
        valid_state=True,
        quality_flags=[],
    )
    row["quality_flags"] = raw

    coerced = coerce_frame(pd.DataFrame([row]), "topbook.v1")

    assert coerced.loc[0, "quality_flags"] == expected
    assert validate_frame(coerced, "topbook.v1", strict=True).ok


def test_validation_accepts_nullable_flag_scalar() -> None:
    row = topbook_row(
        exchange="polymarket",
        instrument_id="token-1",
        received_at_utc="2026-05-26T00:00:00+00:00",
        best_bid_dollars=0.4,
        best_ask_dollars=0.6,
        spread_dollars=0.2,
        valid_state=True,
        quality_flags=[],
    )
    row["quality_flags"] = pd.NA

    report = validate_frame(pd.DataFrame([row]), "topbook.v1", strict=True)

    assert report.ok, report.errors


def test_strict_validation_rejects_duplicate_primary_keys() -> None:
    row = topbook_row(
        exchange="polymarket",
        instrument_id="token-1",
        received_at_utc="2026-05-26T00:00:00+00:00",
        best_bid_dollars=0.4,
        best_ask_dollars=0.6,
        spread_dollars=0.2,
        valid_state=True,
        quality_flags=[],
    )

    report = validate_frame(pd.DataFrame([row, dict(row)]), "topbook.v1", strict=True)

    assert not report.ok
    assert any("primary key" in error for error in report.errors)


def test_strict_validation_rejects_extra_columns_while_default_allows_them() -> None:
    row = topbook_row(
        exchange="polymarket",
        instrument_id="token-1",
        received_at_utc="2026-05-26T00:00:00+00:00",
        best_bid_dollars=0.4,
        best_ask_dollars=0.6,
        spread_dollars=0.2,
        valid_state=True,
        quality_flags=[],
    )
    row["debug_note"] = "legacy-export"
    df = pd.DataFrame([row])

    permissive_report = validate_frame(df, "topbook.v1")
    strict_report = validate_frame(df, "topbook.v1", strict=True)

    assert permissive_report.ok, permissive_report.errors
    assert permissive_report.extra_columns == ("debug_note",)
    assert not strict_report.ok
    assert strict_report.errors == ("extra columns: debug_note",)


def test_strict_inferred_validation_rejects_extra_columns() -> None:
    row = topbook_row(
        exchange="polymarket",
        instrument_id="token-1",
        received_at_utc="2026-05-26T00:00:00+00:00",
        best_bid_dollars=0.4,
        best_ask_dollars=0.6,
        spread_dollars=0.2,
        valid_state=True,
        quality_flags=[],
    )
    row["debug_note"] = "legacy-export"

    report = infer_and_validate_frame(pd.DataFrame([row]), strict=True)

    assert not report.ok
    assert report.errors == ("extra columns: debug_note",)


def test_write_parquet_strict_validation_rejects_extra_columns(tmp_path) -> None:
    row = topbook_row(
        exchange="polymarket",
        instrument_id="token-1",
        received_at_utc="2026-05-26T00:00:00+00:00",
        best_bid_dollars=0.4,
        best_ask_dollars=0.6,
        spread_dollars=0.2,
        valid_state=True,
        quality_flags=[],
    )
    row["debug_note"] = "legacy-export"

    with pytest.raises(ValueError, match="extra columns: debug_note"):
        write_parquet(
            pd.DataFrame([row]),
            tmp_path / "topbook.parquet",
            schema="topbook.v1",
            strict=True,
        )


def test_write_parquet_coerce_normalizes_before_strict_validation(tmp_path) -> None:
    path = tmp_path / "topbook.parquet"
    row = {
        "exchange": "polymarket",
        "instrument_id": "token-1",
        "received_at_utc": "2026-05-26T00:00:00+00:00",
        "best_bid_dollars": "0.4",
        "best_ask_dollars": "0.6",
        "spread_dollars": "0.2",
        "valid_state": "true",
        "quality_flags": [],
        "debug_note": "legacy-export",
    }

    write_parquet(
        pd.DataFrame([row]),
        path,
        schema="topbook.v1",
        coerce=True,
        strict=True,
    )

    written = pd.read_parquet(path)
    assert "debug_note" not in written.columns
    assert list(written.columns) == list(get_table_spec("topbook.v1").columns)


def test_schema_registry_uses_explicit_count_dtypes() -> None:
    fields = {field.name: field for field in get_table_spec("market_match.v2").fields}

    assert fields["polymarket_mid"].dtype == "float64"
    assert fields["kalshi_mid"].dtype == "float64"
    assert fields["kalshi_open_interest"].dtype == "float64"
    assert fields["polymarket_depth"].dtype == "int64"
    assert fields["kalshi_depth"].dtype == "int64"


def test_schema_registry_requires_explicit_dtype_for_new_columns() -> None:
    with pytest.raises(KeyError, match="no explicit dtype"):
        registry_module._field("new_unmapped_schema_column")


def test_source_market_snapshot_schema_dtypes() -> None:
    polymarket = {
        field.name: field
        for field in get_table_spec("polymarket_market_snapshot.v1").fields
    }
    kalshi = {
        field.name: field
        for field in get_table_spec("kalshi_market_snapshot.v1").fields
    }

    assert polymarket["token_ids"].dtype == "list[string]"
    assert polymarket["enable_orderbook"].dtype == "bool"
    assert polymarket["open_time"].dtype == "string"
    assert polymarket["start_time"].dtype == "string"
    assert polymarket["raw_json"].dtype == "json"
    assert polymarket["yes_bid"].dtype == "float64"
    assert polymarket["yes_ask"].dtype == "float64"
    assert polymarket["mid"].dtype == "float64"
    assert polymarket["spread"].dtype == "float64"
    assert polymarket["last_trade_price"].dtype == "float64"
    assert polymarket["volume"].dtype == "float64"
    assert polymarket["liquidity"].dtype == "float64"
    assert kalshi["fee_multiplier"].dtype == "float64"
    assert kalshi["yes_bid"].dtype == "float64"
    assert kalshi["yes_ask"].dtype == "float64"
    assert kalshi["no_bid"].dtype == "float64"
    assert kalshi["no_ask"].dtype == "float64"
    assert kalshi["mid"].dtype == "float64"
    assert kalshi["spread"].dtype == "float64"
    assert kalshi["volume"].dtype == "float64"
    assert kalshi["open_interest"].dtype == "float64"
    assert kalshi["raw_json"].dtype == "json"


def test_legacy_source_market_snapshot_v1_schemas_preserve_resolution_columns() -> None:
    polymarket_spec = get_table_spec("polymarket_market_snapshot.v1")
    kalshi_spec = get_table_spec("kalshi_market_snapshot.v1")
    polymarket = _valid_schema_row(polymarket_spec)
    kalshi = _valid_schema_row(kalshi_spec)

    assert "condition_id" in polymarket_spec.columns
    assert "outcome_labels_json" in polymarket_spec.columns
    assert "uma_resolution_status" in polymarket_spec.columns
    assert "result" in kalshi_spec.columns
    assert "settlement_value_dollars" in kalshi_spec.columns

    polymarket_report = validate_frame(
        pd.DataFrame([polymarket]),
        "polymarket_market_snapshot.v1",
        strict=True,
    )
    kalshi_report = validate_frame(
        pd.DataFrame([kalshi]),
        "kalshi_market_snapshot.v1",
        strict=True,
    )

    assert polymarket_report.ok, polymarket_report.errors
    assert kalshi_report.ok, kalshi_report.errors


def test_trimmed_source_market_snapshot_v2_schemas_validate_explicitly() -> None:
    polymarket = polymarket_market_snapshot_v2_row(
        market_id="pm-1",
        question="Will it rain?",
        condition_id="0xabc",
        question_id="q-1",
        outcome_labels_json=["yes", "no"],
        outcome_prices_json=["1", "0"],
        uma_resolution_status="resolved",
        resolved_by="uma",
        resolution_source="oracle",
        raw_json={"id": "pm-1"},
        raw_json_sha256="0" * 64,
    )
    kalshi = kalshi_market_snapshot_v2_row(
        exchange="kalshi",
        market_key="KXRAIN",
        instrument_key="KXRAIN:YES",
        question="Will it rain?",
        status="determined",
        result="yes",
        settlement_value_dollars="1",
        settlement_ts="2026-01-01T00:00:00Z",
        expiration_value="1",
        is_provisional=True,
        rules_primary="Primary rule",
        rules_secondary="Secondary rule",
        raw_json={"ticker": "KXRAIN"},
        raw_json_sha256="1" * 64,
    )

    assert "condition_id" not in polymarket
    assert "uma_resolution_status" not in polymarket
    assert "result" not in kalshi
    assert "settlement_value_dollars" not in kalshi

    polymarket_report = validate_frame(
        pd.DataFrame([polymarket]),
        "polymarket_market_snapshot.v2",
        strict=True,
    )
    kalshi_report = validate_frame(
        pd.DataFrame([kalshi]),
        "kalshi_market_snapshot.v2",
        strict=True,
    )

    assert polymarket_report.ok, polymarket_report.errors
    assert kalshi_report.ok, kalshi_report.errors


def test_snapshot_compatibility_coercion_preserves_identity_and_source_fields() -> None:
    legacy = pd.DataFrame(
        [
            {
                "schema_version": "polymarket_market_snapshot.v1",
                "market_id": "pm-1",
                "question": "Will it rain?",
                "condition_id": "0xabc",
                "outcome_labels_json": ["yes", "no"],
                "raw_json": {"id": "pm-1", "question": "Will it rain?"},
                "raw_json_sha256": "0" * 64,
            }
        ]
    )

    migrated = coerce_snapshot_frame(legacy, "polymarket_market_snapshot.v2")

    assert migrated.loc[0, "schema_version"] == "polymarket_market_snapshot.v2"
    assert migrated.loc[0, "market_id"] == "pm-1"
    assert migrated.loc[0, "question"] == "Will it rain?"
    assert migrated.loc[0, "raw_json"] == '{"id": "pm-1", "question": "Will it rain?"}'
    assert migrated.loc[0, "raw_json_sha256"] == "0" * 64
    assert "condition_id" not in migrated.columns
    assert "outcome_labels_json" not in migrated.columns
    assert validate_frame(migrated, "polymarket_market_snapshot.v2", strict=True).ok


def test_snapshot_compatibility_coercion_rejects_wrong_source_schema() -> None:
    wrong = pd.DataFrame(
        [
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "market_key": "KXRAIN",
                "exchange": "kalshi",
                "question": "Will it rain?",
            }
        ]
    )

    with pytest.raises(ValueError, match="incompatible source snapshot schema_version"):
        coerce_snapshot_frame(wrong, "polymarket_market_snapshot.v2")


def test_selected_snapshot_schema_rejects_unknown_and_malformed_required_fields() -> (
    None
):
    row = polymarket_market_snapshot_v2_row(
        market_id={"bad": "mapping"},
        question="Will it rain?",
        raw_json={},
        raw_json_sha256="0" * 64,
    )
    row["debug_note"] = "unexpected"

    report = validate_frame(
        pd.DataFrame([row]),
        "polymarket_market_snapshot.v2",
        strict=True,
    )

    assert not report.ok
    assert "extra columns: debug_note" in report.errors
    assert "market_id: 1 values incompatible with string" in report.errors


def test_source_market_snapshot_validation_rejects_non_numeric_quotes() -> None:
    polymarket = _valid_schema_row(get_table_spec("polymarket_market_snapshot.v1"))
    polymarket.update(
        {
            "yes_bid": "not-a-price",
            "yes_ask": "not-a-price",
            "mid": "not-a-price",
            "last_trade_price": "not-a-price",
        }
    )
    kalshi = _valid_schema_row(get_table_spec("kalshi_market_snapshot.v1"))
    kalshi.update(
        {
            "yes_bid": "not-a-price",
            "yes_ask": "not-a-price",
            "no_bid": "not-a-price",
            "no_ask": "not-a-price",
            "mid": "not-a-price",
        }
    )

    polymarket_report = validate_frame(
        pd.DataFrame([polymarket]),
        "polymarket_market_snapshot.v1",
        strict=True,
    )
    kalshi_report = validate_frame(
        pd.DataFrame([kalshi]),
        "kalshi_market_snapshot.v1",
        strict=True,
    )

    assert not polymarket_report.ok
    assert "yes_bid: 1 values incompatible with float64" in polymarket_report.errors
    assert "yes_ask: 1 values incompatible with float64" in polymarket_report.errors
    assert "mid: 1 values incompatible with float64" in polymarket_report.errors
    assert (
        "last_trade_price: 1 values incompatible with float64"
        in polymarket_report.errors
    )
    assert not kalshi_report.ok
    assert "yes_bid: 1 values incompatible with float64" in kalshi_report.errors
    assert "yes_ask: 1 values incompatible with float64" in kalshi_report.errors
    assert "no_bid: 1 values incompatible with float64" in kalshi_report.errors
    assert "no_ask: 1 values incompatible with float64" in kalshi_report.errors
    assert "mid: 1 values incompatible with float64" in kalshi_report.errors


def test_validate_market_match_rejects_out_of_range_probability_prices() -> None:
    row = _valid_schema_row(get_table_spec("market_match.v2"))
    row.update(
        {
            "polymarket_mid": 1.2,
            "polymarket_bid": -0.1,
            "polymarket_ask": 2.0,
            "kalshi_mid": -3.0,
            "kalshi_bid": 1.1,
            "kalshi_ask": -0.2,
        }
    )

    report = validate_frame(pd.DataFrame([row]), "market_match.v2")

    assert not report.ok
    assert "polymarket_mid: 1 prices outside [0, 1]" in report.errors
    assert "polymarket_bid: 1 prices outside [0, 1]" in report.errors
    assert "polymarket_ask: 1 prices outside [0, 1]" in report.errors
    assert "kalshi_mid: 1 prices outside [0, 1]" in report.errors
    assert "kalshi_bid: 1 prices outside [0, 1]" in report.errors
    assert "kalshi_ask: 1 prices outside [0, 1]" in report.errors


def test_validate_match_relation_rejects_event_related_trade_equivalence() -> None:
    row = _valid_schema_row(get_table_spec("match_relation.v1"))
    row.update(
        {
            "relation_label": "same_event_different_cutoff",
            "is_trade_equivalent": True,
        }
    )

    report = validate_frame(pd.DataFrame([row]), "match_relation.v1")

    assert not report.ok
    assert any(
        "same_event rows cannot be trade-equivalent" in error for error in report.errors
    )


def test_validate_empty_match_relation_frame() -> None:
    spec = get_table_spec("match_relation.v1")
    df = pd.DataFrame(columns=[field.name for field in spec.fields])

    report = validate_frame(df, "match_relation.v1")

    assert report.ok
    assert report.row_count == 0


def test_validate_match_relation_rejects_serialized_token_id() -> None:
    row = _valid_schema_row(get_table_spec("match_relation.v1"))
    row["polymarket_token_id"] = '["token-yes", "token-no"]'

    report = validate_frame(pd.DataFrame([row]), "match_relation.v1")

    assert not report.ok
    assert any("serialized/list-like token ids" in error for error in report.errors)


def test_validate_match_relation_requires_instrument_level_id() -> None:
    row = _valid_schema_row(get_table_spec("match_relation.v1"))
    row["match_id"] = "match-1"

    report = validate_frame(pd.DataFrame([row]), "match_relation.v1")

    assert not report.ok
    assert any(
        "match_id" in error and "instrument keys" in error for error in report.errors
    )


def test_validate_match_relation_requires_instrument_keys() -> None:
    row = _valid_schema_row(get_table_spec("match_relation.v1"))
    row["polymarket_instrument_key"] = None

    report = validate_frame(pd.DataFrame([row]), "match_relation.v1")

    assert not report.ok
    assert any("polymarket_instrument_key" in error for error in report.errors)


def test_validate_match_relation_requires_provenance_for_tracking_rows() -> None:
    row = _valid_schema_row(get_table_spec("match_relation.v1"))
    row["evidence_json"] = {"source_row_hashes": {"polymarket": "pm-source"}}

    report = validate_frame(pd.DataFrame([row]), "match_relation.v1")

    assert not report.ok
    assert any(
        "lack source hashes or contract fields" in error for error in report.errors
    )


def test_validate_tracking_match_rejects_inconsistent_pair_id() -> None:
    row = _valid_schema_row(get_table_spec("tracking_match.v1"))
    row.update(
        {
            "tracking_pair_id": "pm:wrong|kalshi:KXTEST",
            "polymarket_market_key": "pm-1",
            "kalshi_market_key": "KXTEST",
            "confidence_score": 0.5,
            "title_similarity": 0.5,
        }
    )

    report = validate_frame(pd.DataFrame([row]), "tracking_match.v1")

    assert not report.ok
    assert any("do not match venue market keys" in error for error in report.errors)


def test_validate_signal_rejects_allowed_non_strict_relation() -> None:
    row = signal_row(
        signal_id="signal-1",
        match_id="match-1",
        strategy_version="strategy-1",
        observed_at_utc="2026-05-30T00:00:00+00:00",
        relation_label="same_context_only",
        gross_edge=0.05,
        fee_estimate=0.0,
        slippage_estimate=0.0,
        net_edge=0.05,
        executable_size=1.0,
        quote_age_ms=1,
        risk_flags="",
        decision="allow",
        execution_allowed=True,
    )

    report = validate_frame(pd.DataFrame([row]), "signal.v1")

    assert not report.ok
    assert any("non-strict relation_label" in error for error in report.errors)


def test_validate_signal_rejects_bad_net_edge_accounting() -> None:
    row = signal_row(
        signal_id="signal-1",
        match_id="match-1",
        strategy_version="strategy-1",
        observed_at_utc="2026-05-30T00:00:00+00:00",
        relation_label="exact_equivalent",
        gross_edge=0.05,
        fee_estimate=0.01,
        slippage_estimate=0.005,
        net_edge=0.05,
        executable_size=1.0,
        quote_age_ms=1,
        risk_flags="",
        decision="allow",
        execution_allowed=True,
    )

    report = validate_frame(pd.DataFrame([row]), "signal.v1")

    assert not report.ok
    assert any("gross-fees-slippage" in error for error in report.errors)


def test_validate_order_intent_rejects_live_without_passed_risk() -> None:
    row = order_intent_row(
        order_intent_id="intent-1",
        signal_id="signal-1",
        venue="polymarket",
        instrument_id="token-1",
        action="buy",
        book_side="ask",
        limit_price=0.4,
        size_contracts=1.0,
        client_order_id="client-1",
        risk_check_status="blocked",
        created_at_utc="2026-05-30T00:00:00+00:00",
        mode="live",
    )

    report = validate_frame(pd.DataFrame([row]), "order_intent.v1")

    assert not report.ok
    assert any("live rows did not pass risk checks" in error for error in report.errors)


def test_validate_order_intent_rejects_conflicting_client_order_id() -> None:
    base = order_intent_row(
        order_intent_id="intent-1",
        signal_id="signal-1",
        venue="polymarket",
        instrument_id="token-1",
        action="buy",
        book_side="ask",
        limit_price=0.4,
        size_contracts=1.0,
        client_order_id="client-1",
        risk_check_status="passed",
        created_at_utc="2026-05-30T00:00:00+00:00",
        mode="paper",
    )
    conflicting = dict(base)
    conflicting["order_intent_id"] = "intent-2"
    conflicting["limit_price"] = 0.5

    report = validate_frame(pd.DataFrame([base, conflicting]), "order_intent.v1")

    assert not report.ok
    assert any("conflicting payloads" in error for error in report.errors)


def test_validate_paper_fill_rejects_bad_notional() -> None:
    row = paper_fill_row(
        paper_fill_id="fill-1",
        order_intent_id="intent-1",
        signal_id="signal-1",
        client_order_id="client-1",
        venue="polymarket",
        instrument_id="token-1",
        action="buy",
        book_side="ask",
        fill_price_dollars=0.4,
        size_contracts=2.0,
        notional_dollars=0.7,
        fees_dollars=0.0,
        filled_at_utc="2026-05-30T00:00:00+00:00",
        simulator_version="paper_execution.v1",
        fill_type="partial",
        latency_ms=1,
        topbook_ref="{}",
    )

    report = validate_frame(pd.DataFrame([row]), "paper_fill.v1")

    assert not report.ok
    assert any("fill_price*size" in error for error in report.errors)


def test_validate_paper_position_rejects_bad_pnl_accounting() -> None:
    row = paper_position_row(
        paper_position_id="position-1",
        signal_id="signal-1",
        match_id="match-1",
        opened_at_utc="2026-05-30T00:00:00+00:00",
        as_of_utc="2026-05-30T00:00:00+00:00",
        status="open",
        filled_size_contracts=1.0,
        unmatched_leg_size_contracts=0.0,
        buy_notional_dollars=0.4,
        sell_notional_dollars=0.5,
        fees_dollars=0.01,
        gross_pnl_dollars=0.1,
        realized_pnl_dollars=0.2,
        unrealized_pnl_dollars=0.0,
        net_pnl_dollars=0.09,
    )

    report = validate_frame(pd.DataFrame([row]), "paper_position.v1")

    assert not report.ok
    assert any("realized+unrealized" in error for error in report.errors)


def test_coerce_frame_reports_missing_required_columns() -> None:
    with pytest.raises(ValueError, match="missing required columns: exchange"):
        coerce_frame(pd.DataFrame([{"instrument_id": "token-1"}]), "topbook.v1")


def test_run_manifest_row_minimal_default_validates() -> None:
    report = validate_frame(
        pd.DataFrame([run_manifest_row(run_id="run-1", status="success")]),
        "run_manifest.v1",
    )

    assert report.ok, report.errors


def test_validation_tolerates_rounded_spread_and_non_default_index() -> None:
    df = pd.DataFrame(
        [
            topbook_row(
                exchange="polymarket",
                instrument_id="token-1",
                received_at_utc="2026-05-26T00:00:00+00:00",
                best_bid_dollars=0.333333,
                best_ask_dollars=0.666666,
                spread_dollars=0.333333,
                valid_state=True,
                quality_flags=[],
            )
        ],
        index=[42],
    )

    report = validate_frame(df, "topbook.v1")

    assert report.ok, report.errors


def test_validate_frame_rejects_bool_and_nonfinite_numeric_values() -> None:
    spec = get_table_spec("topbook.v1")
    rows = []
    for overrides in (
        {"best_bid_dollars": True},
        {"best_bid_dollars": float("inf")},
        {"local_sequence": False},
    ):
        row = _valid_schema_row(spec)
        row.update(overrides)
        rows.append(row)

    report = validate_frame(pd.DataFrame(rows), spec)

    assert not report.ok
    assert "best_bid_dollars: 2 values incompatible with float64" in report.errors
    assert "local_sequence: 1 values incompatible with int64" in report.errors


def test_parse_price_level_preserves_zero_price() -> None:
    from pmkt.data.books import parse_price_level

    level = parse_price_level({"price": 0.0, "price_dollars": 0.25, "size": 4})

    assert level is not None
    assert level.price == 0.0
    assert level.size == 4


def test_parse_price_level_rejects_negative_dict_and_list_prices() -> None:
    from pmkt.data.books import parse_price_level

    assert parse_price_level({"price": -0.01, "size": 4}) is None
    assert parse_price_level([-0.01, 4]) is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1, 1),
        (np.int64(5), 5),
        (Decimal("9007199254740993"), 9_007_199_254_740_993),
        (Fraction(6, 3), 2),
        (1.0, 1),
        (np.float16(2_047), 2_047),
        (np.float32(16_777_215), 16_777_215),
        (" 1 ", 1),
        ("+1.0", 1),
        ("-1", -1),
        ("1e3", 1_000),
        ("1_000", 1_000),
        ("9007199254740993", 9_007_199_254_740_993),
        ("-9223372036854775808", -(1 << 63)),
        ("9223372036854775807", (1 << 63) - 1),
    ],
)
def test_parse_int_preserves_exact_integral_values(value: object, expected: int) -> None:
    assert parse_int(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        "",
        "   ",
        1.5,
        Decimal("1.5"),
        Fraction((1 << 60) + 1, 1 << 60),
        float("nan"),
        float("inf"),
        Decimal("NaN"),
        "0x10",
        float(9_007_199_254_740_993),
        np.float16(2_048),
        np.float32(16_777_216),
        "1" * 4_301,
        "1e4300",
        "0" * 9_000,
    ],
)
def test_parse_int_rejects_nonintegral_unsafe_or_pathological_values(
    value: object,
) -> None:
    assert parse_int(value) is None


@pytest.mark.parametrize("value", ["1__0", "_10", "10_", "1e_3"])
def test_integer_schema_rejects_malformed_numeric_text(value: str) -> None:
    spec = TableSpec(
        name="malformed_integer",
        version="malformed_integer.v1",
        fields=(FieldSpec("value", "int64", True),),
    )

    report = validate_frame(pd.DataFrame({"value": [value]}), spec)
    coerced = coerce_frame(pd.DataFrame({"value": [value]}), spec)

    assert parse_int(value) is None
    assert not report.ok
    assert "value: 1 values incompatible with int64" in report.errors
    assert pd.isna(coerced.loc[0, "value"])


def test_integer_schema_validation_enforces_signed_bounds_exactly() -> None:
    spec = TableSpec(
        name="integer_bounds",
        version="integer_bounds.v1",
        fields=(
            FieldSpec("int32_value", "int32", False),
            FieldSpec("int64_value", "int64", False),
        ),
    )
    valid = pd.DataFrame(
        {
            "int32_value": [-(1 << 31), (1 << 31) - 1],
            "int64_value": [str(-(1 << 63)), str((1 << 63) - 1)],
        }
    )
    invalid = pd.DataFrame(
        {
            "int32_value": [-(1 << 31) - 1, 1 << 31],
            "int64_value": [str(-(1 << 63) - 1), str(1 << 63)],
        }
    )

    valid_report = validate_frame(valid, spec)
    invalid_report = validate_frame(invalid, spec)

    assert valid_report.ok, valid_report.errors
    assert not invalid_report.ok
    assert "int32_value: 2 values incompatible with int32" in invalid_report.errors
    assert "int64_value: 2 values incompatible with int64" in invalid_report.errors


def test_integer_coercion_preserves_large_values_without_float_rounding() -> None:
    spec = TableSpec(
        name="integer_coercion",
        version="integer_coercion.v1",
        fields=(
            FieldSpec("int32_value", "int32", True),
            FieldSpec("int64_value", "int64", True),
        ),
    )
    frame = pd.DataFrame(
        {
            "int32_value": ["2147483647", "2147483648", None, "1.0"],
            "int64_value": [
                "9007199254740993",
                "9223372036854775807",
                "9223372036854775808",
                "1.5",
            ],
        }
    )

    coerced = coerce_frame(frame, spec)

    assert str(coerced["int32_value"].dtype) == "Int32"
    assert str(coerced["int64_value"].dtype) == "Int64"
    assert coerced["int32_value"].tolist()[0] == (1 << 31) - 1
    assert pd.isna(coerced["int32_value"].tolist()[1])
    assert pd.isna(coerced["int32_value"].tolist()[2])
    assert coerced["int32_value"].tolist()[3] == 1
    assert coerced["int64_value"].tolist()[0] == 9_007_199_254_740_993
    assert coerced["int64_value"].tolist()[1] == (1 << 63) - 1
    assert pd.isna(coerced["int64_value"].tolist()[2])
    assert pd.isna(coerced["int64_value"].tolist()[3])



def test_schema_inference_rejects_projected_partial_frames() -> None:
    df = pd.DataFrame(columns=["exchange", "instrument_id", "received_at_utc"])

    with pytest.raises(KeyError, match="could not infer schema"):
        infer_and_validate_frame(df)


def test_schema_inference_rejects_ambiguous_full_frames() -> None:
    columns = list(get_table_spec("topbook.v1").columns) + [
        column
        for column in get_table_spec("depth.v1").columns
        if column not in get_table_spec("topbook.v1").columns
    ]
    df = pd.DataFrame(columns=columns)

    with pytest.raises(KeyError, match="ambiguous schema inference"):
        infer_and_validate_frame(df)


def test_run_manifest_status_is_restricted_to_known_states() -> None:
    report = validate_frame(
        pd.DataFrame([run_manifest_row(run_id="run-1", status="unknown")]),
        "run_manifest.v1",
    )

    assert not report.ok
    assert any("unsupported values" in error for error in report.errors)


def test_schema_and_dataset_cli_validate(tmp_path) -> None:
    path = tmp_path / "topbook.parquet"
    row = topbook_row(
        exchange="polymarket",
        instrument_id="token-1",
        received_at_utc="2026-05-26T00:00:00+00:00",
        best_bid_dollars=0.4,
        best_ask_dollars=0.6,
        spread_dollars=0.2,
        valid_state=True,
        quality_flags=[],
    )
    pd.DataFrame([row]).to_parquet(path, index=False)

    extra_path = tmp_path / "topbook-extra.parquet"
    extra_row = dict(row)
    extra_row["debug_note"] = "legacy-export"
    pd.DataFrame([extra_row]).to_parquet(extra_path, index=False)

    runner = CliRunner()
    listed = runner.invoke(app, ["schema", "list"])
    validated = runner.invoke(
        app, ["dataset", "validate", str(path), "--schema", "topbook.v1"]
    )
    permissive_extra = runner.invoke(
        app,
        [
            "dataset",
            "validate",
            str(extra_path),
            "--schema",
            "topbook.v1",
        ],
    )
    strict_extra = runner.invoke(
        app,
        [
            "dataset",
            "validate",
            str(extra_path),
            "--schema",
            "topbook.v1",
            "--strict",
        ],
    )

    assert listed.exit_code == 0
    assert "topbook.v1" in listed.output
    assert validated.exit_code == 0
    assert "OK topbook.v1" in validated.output
    assert permissive_extra.exit_code == 0
    assert strict_extra.exit_code == 1
    assert "extra columns: debug_note" in strict_extra.output


def test_validate_match_relation_accepts_settlement_scope_tracking_label() -> None:
    row = _valid_schema_row(get_table_spec("match_relation.v1"))
    row.update(
        {
            "relation_label": "same_event_different_settlement_scope",
            "is_trade_equivalent": False,
            "is_tracking_useful": True,
        }
    )

    report = validate_frame(pd.DataFrame([row]), "match_relation.v1")

    assert report.ok
    row["is_trade_equivalent"] = True

    rejected = validate_frame(pd.DataFrame([row]), "match_relation.v1")

    assert not rejected.ok
    assert any(
        "same_event rows cannot be trade-equivalent" in error
        for error in rejected.errors
    )



@pytest.mark.parametrize(
    "schema",
    [
        "topbook.v1",
        "depth.v1",
        "trade.v1",
        "book_tape_event.v1",
        "book_tape_level.v1",
        "book_tape_control.v1",
        "stream_lifecycle.v1",
    ],
)
@pytest.mark.parametrize("dtype", ["string", "object"])
def test_validate_frame_reports_on_empty_frames(schema: str, dtype: str) -> None:
    """Empty frames must validate, not raise.

    The vectorized invariant hooks build masks with ``Series.map``, which
    preserves the source dtype when there are no rows. Without an explicit
    boolean cast a string-dtype column produced a string mask and the boolean
    reductions raised TypeError instead of returning a report.
    """
    spec = get_table_spec(schema)
    frame = pd.DataFrame({column: pd.Series(dtype=dtype) for column in spec.columns})

    report = validate_frame(frame, spec)

    assert report.row_count == 0
    assert not report.missing_columns
