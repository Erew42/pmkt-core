from __future__ import annotations

import json

import pandas as pd

from pmkt.resolution.models import (
    CONFIDENCE_CANONICAL,
    CONFIDENCE_PLATFORM_CONFIRMED,
    RESOLVER_VERSION,
    RESULT_TYPE_BINARY,
    RESULT_TYPE_SCALAR,
    STATE_FINAL,
    STATE_INCONSISTENT,
)
from pmkt.resolution.terminal_labels import (
    ResolutionLabelPolicy,
    binary_yes_outcome_from_resolution,
    is_authoritative_final_resolution,
    is_known_terminal_label,
    json_array,
    resolution_training_label,
    scalar_terminal_from_value,
    terminal_from_kalshi_settlement,
    terminal_from_payouts,
    terminal_label_from_resolution,
)


def _record(**overrides: object) -> dict[str, object]:
    record: dict[str, object] = {
        "platform": "polymarket",
        "market_key": "pm-1",
        "resolution_state": STATE_FINAL,
        "confidence": CONFIDENCE_CANONICAL,
        "canonical_source": "polygon_ctf",
        "resolver_version": RESOLVER_VERSION,
        "result_type": RESULT_TYPE_BINARY,
        "payouts_json": [
            {"outcome": "yes", "payout": "1"},
            {"outcome": "no", "payout": "0"},
        ],
    }
    record.update(overrides)
    return record


def test_polymarket_canonical_binary_labels_are_training_eligible() -> None:
    yes = _record()
    no = _record(
        payouts_json=[
            {"outcome": "yes", "payout": "0"},
            {"outcome": "no", "payout": "1"},
        ]
    )

    yes_label = resolution_training_label(yes)
    no_label = resolution_training_label(no)

    assert is_authoritative_final_resolution(yes)
    assert terminal_label_from_resolution(yes) == "yes"
    assert binary_yes_outcome_from_resolution(yes) == 1
    assert yes_label.terminal_label == "yes"
    assert yes_label.binary_yes == 1
    assert yes_label.quality == "canonical_binary"
    assert yes_label.exclusion_reason is None
    assert terminal_label_from_resolution(no) == "no"
    assert no_label.binary_yes == 0
    assert no_label.quality == "canonical_binary"
    assert no_label.exclusion_reason is None


def test_polymarket_refund_and_fractional_payouts_are_nonbinary() -> None:
    refund = _record(
        payouts_json=json.dumps(
            [
                {"outcome": "yes", "numerator": "1", "denominator": "1"},
                {"outcome": "no", "numerator": "1", "denominator": "1"},
            ]
        )
    )
    fractional = _record(
        payouts_json=[
            {"outcome": "yes", "payout": "1/2"},
            {"outcome": "no", "payout": "1/2"},
        ]
    )

    refund_label = resolution_training_label(refund)
    fractional_label = resolution_training_label(fractional)

    assert terminal_label_from_resolution(refund) == "refund"
    assert refund_label.binary_yes is None
    assert refund_label.quality == "canonical_nonbinary"
    assert refund_label.exclusion_reason == "nonbinary_terminal_label"
    assert terminal_label_from_resolution(fractional) == "payout:yes=1/2,no=1/2"
    assert fractional_label.binary_yes is None
    assert fractional_label.quality == "canonical_nonbinary"
    assert fractional_label.exclusion_reason == "nonbinary_terminal_label"


def test_malformed_exact_terminal_labels_are_not_known() -> None:
    assert terminal_from_payouts(
        [
            {"outcome": "yes", "payout": "-1/2"},
            {"outcome": "no", "payout": "3/2"},
        ]
    ) is None
    assert is_known_terminal_label("payout:anything") is False
    assert is_known_terminal_label("payout:yes=3/2,no=0") is False
    assert is_known_terminal_label("payout:yes=1/2,no=0") is False
    assert is_known_terminal_label("payout:yes=0,no=0") is False
    assert is_known_terminal_label("payout:no=1/2,yes=1/2") is False
    assert is_known_terminal_label("scalar:not-a-number") is False
    assert is_known_terminal_label("scalar:-1/2") is False
    assert is_known_terminal_label("payout:yes=1/2,no=1/2")
    assert is_known_terminal_label("payout:yes=1,no=1")
    assert is_known_terminal_label("scalar:1/2")


def test_kalshi_binary_settlements_are_training_eligible() -> None:
    yes = _record(
        platform="kalshi",
        market_key="ka-yes",
        canonical_source="kalshi_rest",
        payouts_json=[],
        settlement_value_dollars="1.0000",
    )
    no = _record(
        platform="kalshi",
        market_key="ka-no",
        canonical_source="kalshi_historical_rest",
        payouts_json=[],
        settlement_value_dollars="0",
    )

    assert terminal_from_kalshi_settlement(yes) == "yes"
    assert terminal_label_from_resolution(yes) == "yes"
    assert resolution_training_label(yes).binary_yes == 1
    assert resolution_training_label(yes).quality == "canonical_binary"
    assert terminal_from_kalshi_settlement(no) == "no"
    assert terminal_label_from_resolution(no) == "no"
    assert resolution_training_label(no).binary_yes == 0
    assert resolution_training_label(no).quality == "canonical_binary"


def test_kalshi_scalar_settlements_remain_nonbinary() -> None:
    scalar = _record(
        platform="kalshi",
        market_key="ka-scalar",
        canonical_source="kalshi_rest",
        result_type=RESULT_TYPE_SCALAR,
        payouts_json=[],
        settlement_value_dollars="0.5000",
    )

    label = resolution_training_label(scalar)

    assert terminal_label_from_resolution(scalar) == "scalar:1/2"
    assert label.terminal_label == "scalar:1/2"
    assert label.binary_yes is None
    assert label.quality == "canonical_nonbinary"
    assert label.exclusion_reason == "nonbinary_terminal_label"


def test_default_training_policy_rejects_noncanonical_and_unsafe_rows() -> None:
    noncanonical = _record(confidence=CONFIDENCE_PLATFORM_CONFIRMED)
    stale = _record(resolver_version="market_resolution_resolver.v0")
    resolver_error = _record(error_type="MarketResolutionCacheConflict")
    wrong_source = _record(canonical_source="manual_review")
    missing_platform = _record(platform="")
    inconsistent = _record(
        resolution_state=STATE_INCONSISTENT,
        confidence="inconsistent",
    )

    assert terminal_label_from_resolution(noncanonical, require_canonical=False) == "yes"
    assert terminal_label_from_resolution(noncanonical) is None
    assert resolution_training_label(noncanonical).exclusion_reason == (
        "noncanonical_or_nonfinal_resolution"
    )
    assert resolution_training_label(stale).exclusion_reason == (
        "resolver_version_mismatch"
    )
    assert resolution_training_label(resolver_error).quality == "error"
    assert resolution_training_label(resolver_error).exclusion_reason == "resolver_error"
    assert resolution_training_label(wrong_source).exclusion_reason == (
        "canonical_source_not_allowed"
    )
    assert resolution_training_label(missing_platform).exclusion_reason == (
        "platform_missing"
    )
    assert resolution_training_label(inconsistent).quality == "conflict"
    assert resolution_training_label(inconsistent).exclusion_reason == (
        "inconsistent_resolution"
    )
    assert not is_authoritative_final_resolution(noncanonical)
    assert not is_authoritative_final_resolution(stale)
    assert not is_authoritative_final_resolution(resolver_error)


def test_training_labels_ignore_diagnostic_winner_result_fallbacks() -> None:
    polymarket = _record(
        payouts_json=[],
        winner="yes",
        result="yes",
    )
    kalshi = _record(
        platform="kalshi",
        market_key="ka-no-settlement",
        canonical_source="kalshi_rest",
        payouts_json=[],
        settlement_value_dollars=None,
        winner="yes",
        result="yes",
    )

    assert terminal_label_from_resolution(polymarket) == "yes"
    assert terminal_label_from_resolution(kalshi) == "yes"
    assert resolution_training_label(polymarket).terminal_label is None
    assert resolution_training_label(polymarket).exclusion_reason == (
        "terminal_label_unavailable"
    )
    assert resolution_training_label(kalshi).terminal_label is None
    assert resolution_training_label(kalshi).exclusion_reason == (
        "terminal_label_unavailable"
    )
    assert not is_authoritative_final_resolution(polymarket)
    assert not is_authoritative_final_resolution(kalshi)


def test_invalid_payout_vectors_are_excluded_from_training() -> None:
    invalid_vectors = [
        [
            {"outcome": "yes", "numerator": "2", "denominator": "1"},
            {"outcome": "no", "numerator": "0", "denominator": "1"},
        ],
        [
            {"outcome": "yes", "numerator": "1", "denominator": "3"},
            {"outcome": "no", "numerator": "1", "denominator": "3"},
        ],
        [
            {"outcome": "yes", "payout": "0"},
            {"outcome": "no", "payout": "0"},
        ],
        [
            {"outcome": "yes", "payout": "1/2"},
            {"outcome": "no", "payout": "0"},
        ],
    ]

    for payouts in invalid_vectors:
        record = _record(payouts_json=payouts, winner="yes")
        label = resolution_training_label(record)

        assert terminal_from_payouts(payouts) is None
        assert label.terminal_label is None
        assert label.exclusion_reason == "terminal_label_unavailable"
        assert not is_authoritative_final_resolution(record)


def test_custom_policy_can_include_platform_confirmed_rows() -> None:
    row = _record(confidence=CONFIDENCE_PLATFORM_CONFIRMED)
    policy = ResolutionLabelPolicy(allow_platform_confirmed=True)

    label = resolution_training_label(row, policy=policy)

    assert is_authoritative_final_resolution(row, policy=policy)
    assert label.terminal_label == "yes"
    assert label.binary_yes == 1
    assert label.exclusion_reason is None


def test_missing_scalars_and_json_array_values_are_handled() -> None:
    assert scalar_terminal_from_value(pd.NA) is None
    assert scalar_terminal_from_value("") is None
    assert scalar_terminal_from_value("0.25") == "scalar:1/4"
    assert json_array(pd.NA) == []
    assert json_array("") == []
    assert json_array('[{"outcome": "yes", "payout": "1"}]') == [
        {"outcome": "yes", "payout": "1"}
    ]
