from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from pmkt.data.books import PriceLevel, parse_price_level
from pmkt.data import normalize_books
from pmkt.data.kalshi_quotes import KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT
from pmkt.data.normalize_books import (
    kalshi_orderbook_to_topbook,
    kalshi_ws_snapshot_to_topbook,
    polymarket_book_to_topbook,
    polymarket_ws_snapshot_to_topbook,
)
from pmkt.data.schemas import (
    TOPBOOK_COLUMNS,
    TOPBOOK_SCHEMA_VERSION,
    topbook_evidence_id,
)
from pmkt.exchanges.kalshi.ws import apply_kalshi_orderbook_message
from pmkt.streaming.topbook_emission import topbook_state_fingerprint


_FIXTURE_ROOT = Path(__file__).with_name("fixtures")

_NORMALIZER_CASES = (
    pytest.param(
        polymarket_book_to_topbook,
        {"asset_id": "token-1", "bids": [], "asks": []},
        {"token_id": "token-1"},
        id="polymarket-rest",
    ),
    pytest.param(
        polymarket_ws_snapshot_to_topbook,
        {"asset_id": "token-1"},
        {},
        id="polymarket-ws",
    ),
    pytest.param(
        kalshi_orderbook_to_topbook,
        {"orderbook_fp": {"yes_dollars": [], "no_dollars": []}},
        {"market_ticker": "KXTEST"},
        id="kalshi-rest",
    ),
    pytest.param(
        kalshi_ws_snapshot_to_topbook,
        {"market_ticker": "KXTEST"},
        {},
        id="kalshi-ws",
    ),
)


def _as_rows(result: object) -> list[dict[str, object]]:
    if isinstance(result, list):
        return result
    assert isinstance(result, dict)
    return [result]


def _load_kalshi_fixture(name: str) -> dict[str, Any]:
    return json.loads((_FIXTURE_ROOT / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize("normalizer,payload,kwargs", _NORMALIZER_CASES)
@pytest.mark.parametrize(
    "received_at_utc",
    ["2026-05-07T00:00:00+00:00", ""],
    ids=["valid-utc", "invalid-empty-utc"],
)
def test_book_normalizers_preserve_explicit_capture_clocks(
    monkeypatch: pytest.MonkeyPatch,
    normalizer: object,
    payload: dict[str, object],
    kwargs: dict[str, object],
    received_at_utc: str,
) -> None:
    def _unexpected_clock_read() -> object:
        raise AssertionError("explicit capture clocks must not be regenerated")

    monkeypatch.setattr(normalize_books, "_utc_now", _unexpected_clock_read)
    monkeypatch.setattr(normalize_books.time, "monotonic_ns", _unexpected_clock_read)

    rows = _as_rows(
        normalizer(  # type: ignore[operator]
            payload,
            received_at_utc=received_at_utc,
            received_at_monotonic_ns=0,
            **kwargs,
        )
    )

    assert rows
    assert all(row["received_at_utc"] == received_at_utc for row in rows)
    assert all(row["received_at_monotonic_ns"] == 0 for row in rows)


@pytest.mark.parametrize("normalizer,payload,kwargs", _NORMALIZER_CASES)
def test_book_normalizers_generate_only_missing_capture_clocks(
    monkeypatch: pytest.MonkeyPatch,
    normalizer: object,
    payload: dict[str, object],
    kwargs: dict[str, object],
) -> None:
    generated_utc = "2026-08-20T12:34:56+00:00"
    generated_monotonic_ns = 987_654_321
    monkeypatch.setattr(normalize_books, "_utc_now", lambda: generated_utc)
    monkeypatch.setattr(
        normalize_books.time,
        "monotonic_ns",
        lambda: generated_monotonic_ns,
    )

    rows = _as_rows(
        normalizer(  # type: ignore[operator]
            payload,
            received_at_utc=None,
            received_at_monotonic_ns=None,
            **kwargs,
        )
    )

    assert rows
    assert all(row["received_at_utc"] == generated_utc for row in rows)
    assert all(
        row["received_at_monotonic_ns"] == generated_monotonic_ns for row in rows
    )


def test_topbook_evidence_identity_uses_persisted_float64_values() -> None:
    decimal_row = {
        column: None for column in TOPBOOK_COLUMNS
    }
    decimal_row.update(
        {
            "schema_version": TOPBOOK_SCHEMA_VERSION,
            "collector_run_id": "run-1",
            "exchange": "kalshi",
            "instrument_id": "KX-1",
            "received_at_utc": "2026-08-25T10:00:00Z",
            "received_at_monotonic_ns": 1,
            "local_sequence": 1,
            "spread_bps": Decimal("15963.302752293577"),
            "valid_state": True,
            "quality_flags": [],
        }
    )
    persisted_row = dict(decimal_row)
    persisted_row["spread_bps"] = float(decimal_row["spread_bps"])

    assert topbook_evidence_id(decimal_row) == topbook_evidence_id(persisted_row)


def test_polymarket_book_to_topbook_emits_canonical_contract() -> None:
    row = polymarket_book_to_topbook(
        {
            "hash": "book-hash",
            "market": "0xmarket",
            "asset_id": "token-1",
            "timestamp": "1766789469000",
            "min_order_size": "5",
            "tick_size": "0.01",
            "bids": [{"price": "0.40", "size": "10"}],
            "asks": [{"price": "0.60", "size": "5"}],
        },
        token_id="token-1",
        received_at_utc="2026-05-07T00:00:00+00:00",
        received_at_monotonic_ns=123,
    )

    assert list(row.keys()) == TOPBOOK_COLUMNS
    assert row["schema_version"] == TOPBOOK_SCHEMA_VERSION
    assert row["exchange"] == "polymarket"
    assert row["instrument_id"] == "token-1"
    assert row["best_bid_dollars"] == pytest.approx(0.4)
    assert row["best_ask_dollars"] == pytest.approx(0.6)
    assert row["best_bid_source"] == "direct"
    assert row["best_ask_source"] == "direct"
    assert row["spread_bps"] == pytest.approx(4000)
    assert row["valid_state"] is True
    assert row["quality_flags"] == []


def test_polymarket_topbook_flags_crossed_book() -> None:
    row = polymarket_book_to_topbook(
        {
            "asset_id": "token-1",
            "bids": [{"price": "0.70", "size": "10"}],
            "asks": [{"price": "0.60", "size": "5"}],
        },
        token_id="token-1",
    )

    assert row["valid_state"] is False
    assert row["quality_flags"] == ["crossed_book", "negative_spread"]


def test_parse_price_level_rejects_missing_size_without_raising() -> None:
    assert parse_price_level(PriceLevel(0.5, None)) is None


def test_kalshi_orderbook_to_topbook_emits_yes_and_no_rows() -> None:
    rows = kalshi_orderbook_to_topbook(
        {
            "orderbook_fp": {
                "yes_dollars": [["0.40", "10"]],
                "no_dollars": [["0.35", "5"]],
            }
        },
        market_ticker="KXTEST",
    )

    assert [row["instrument_id"] for row in rows] == ["KXTEST:YES", "KXTEST:NO"]
    assert [row["outcome"] for row in rows] == ["YES", "NO"]
    assert rows[0]["best_bid_dollars"] == pytest.approx(0.4)
    assert rows[0]["best_ask_dollars"] == pytest.approx(0.65)
    assert rows[1]["best_bid_dollars"] == pytest.approx(0.35)
    assert rows[1]["best_ask_dollars"] == pytest.approx(0.6)
    assert rows[0]["best_bid_source"] == "direct"
    assert rows[0]["best_ask_source"] == "complement_derived"
    assert rows[1]["best_bid_source"] == "direct"
    assert rows[1]["best_ask_source"] == "complement_derived"


def test_ws_snapshot_normalizers_preserve_quality_flags() -> None:
    poly = polymarket_ws_snapshot_to_topbook(
        {
            "asset_id": "token-1",
            "best_bid": "0.40",
            "best_ask": "0.60",
            "quality_flags": ["reconnect"],
        }
    )
    kalshi = kalshi_ws_snapshot_to_topbook(
        {
            "market_ticker": "KXTEST",
            "yes_bid": 0.4,
            "yes_ask": 0.6,
            "no_bid": 0.35,
            "no_ask": 0.6,
            "quality_flags": ["seq_gap"],
        }
    )

    assert poly["valid_state"] is False
    assert poly["quality_flags"] == ["reconnect"]
    assert [row["valid_state"] for row in kalshi] == [False, False]
    assert [row["quality_flags"] for row in kalshi] == [["seq_gap"], ["seq_gap"]]
    assert [row["best_ask_source"] for row in kalshi] == ["direct", "direct"]


@pytest.mark.parametrize(
    "fixture_name",
    ["kalshi_orderbook_yes_price.json", "kalshi_orderbook_no_price.json"],
)
def test_kalshi_transcript_preserves_raw_to_topbook_quote_provenance(
    fixture_name: str,
) -> None:
    fixture = _load_kalshi_fixture(fixture_name)
    states = {}
    snapshots = []
    for message in fixture["messages"]:
        snapshots.extend(
            apply_kalshi_orderbook_message(
                states,
                message,
                use_yes_price=fixture["use_yes_price"],
            )
        )

    serialized = snapshots[-1].as_dict()
    assert serialized["use_yes_price"] is fixture["use_yes_price"]
    assert (
        serialized["quote_normalization_policy"]
        == KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT
    )

    rows = kalshi_ws_snapshot_to_topbook(
        serialized,
        collector_run_id="fixture-run",
        received_at_utc="2026-01-01T00:00:00+00:00",
        received_at_monotonic_ns=1,
        local_sequence=2,
    )
    by_outcome = {row["outcome"]: row for row in rows}
    for outcome, expected in fixture["expected"].items():
        actual = by_outcome[outcome]
        for field, value in expected.items():
            if isinstance(value, float):
                assert actual[field] == pytest.approx(value)
            else:
                assert actual[field] == value


def test_missing_policy_preserves_legacy_opposite_bid_size_projection() -> None:
    payload = {
        "market_ticker": "KXLEGACY",
        "yes_bid": 0.4,
        "yes_ask": 0.6,
        "no_bid": 0.35,
        "no_ask": 0.65,
        "yes_bid_size": 10.0,
        "yes_ask_size": 11.0,
        "no_bid_size": 5.0,
        "no_ask_size": 6.0,
    }
    legacy = kalshi_ws_snapshot_to_topbook(payload)
    corrected = kalshi_ws_snapshot_to_topbook(
        {
            **payload,
            "quote_normalization_policy": (
                KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT
            ),
        }
    )

    assert [row["ask_size_contracts"] for row in legacy] == [5.0, 10.0]
    assert [row["ask_size_contracts"] for row in corrected] == [11.0, 6.0]


def test_unknown_kalshi_policy_is_not_treated_as_legacy_evidence() -> None:
    with pytest.raises(ValueError, match="unsupported Kalshi"):
        kalshi_ws_snapshot_to_topbook(
            {
                "market_ticker": "KXUNKNOWN",
                "quote_normalization_policy": "kalshi_quote_normalization.v99",
            }
        )


@pytest.mark.parametrize("include_explicit", [False, True], ids=["absent", "null"])
def test_current_kalshi_policy_falls_back_only_for_missing_explicit_ask_size(
    include_explicit: bool,
) -> None:
    payload = {
        "market_ticker": "KXFALLBACK",
        "yes_bid": 0.4,
        "no_bid": 0.35,
        "yes_bid_size": 10.0,
        "no_bid_size": 5.0,
        "quote_normalization_policy": KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT,
    }
    if include_explicit:
        payload.update({"yes_ask_size": None, "no_ask_size": None})

    rows = kalshi_ws_snapshot_to_topbook(payload)

    assert [row["ask_size_contracts"] for row in rows] == [5.0, 10.0]


def test_current_kalshi_policy_accepts_positive_textual_explicit_ask_sizes() -> None:
    rows = kalshi_ws_snapshot_to_topbook(
        {
            "market_ticker": "KXEXPLICIT",
            "yes_bid": 0.4,
            "no_bid": 0.35,
            "yes_bid_size": 10.0,
            "no_bid_size": 5.0,
            "yes_ask_size": "11.5",
            "no_ask_size": "6.5",
            "quote_normalization_policy": KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT,
        }
    )

    assert [row["ask_size_contracts"] for row in rows] == [11.5, 6.5]


@pytest.mark.parametrize(
    "field,value",
    [
        pytest.param("yes_ask_size", "", id="yes-blank"),
        pytest.param("yes_ask_size", "bad", id="yes-malformed"),
        pytest.param("yes_ask_size", True, id="yes-boolean"),
        pytest.param("yes_ask_size", 0, id="yes-zero"),
        pytest.param("yes_ask_size", -1, id="yes-negative"),
        pytest.param("yes_ask_size", float("inf"), id="yes-infinite"),
        pytest.param("no_ask_size", "", id="no-blank"),
        pytest.param("no_ask_size", "bad", id="no-malformed"),
        pytest.param("no_ask_size", False, id="no-boolean"),
        pytest.param("no_ask_size", 0, id="no-zero"),
        pytest.param("no_ask_size", -1, id="no-negative"),
        pytest.param("no_ask_size", float("nan"), id="no-nan"),
    ],
)
def test_current_kalshi_policy_rejects_malformed_explicit_ask_size(
    field: str,
    value: object,
) -> None:
    payload = {
        "market_ticker": "KXMALFORMED",
        "yes_bid": 0.4,
        "no_bid": 0.35,
        "yes_bid_size": 10.0,
        "no_bid_size": 5.0,
        "quote_normalization_policy": KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT,
        field: value,
    }

    with pytest.raises(ValueError, match=field):
        kalshi_ws_snapshot_to_topbook(payload)


def test_legacy_kalshi_policy_ignores_explicit_ask_size_fields() -> None:
    rows = kalshi_ws_snapshot_to_topbook(
        {
            "market_ticker": "KXLEGACY",
            "yes_bid": 0.4,
            "no_bid": 0.35,
            "yes_bid_size": 10.0,
            "no_bid_size": 5.0,
            "yes_ask_size": "malformed-but-not-legacy-authority",
            "no_ask_size": "malformed-but-not-legacy-authority",
        }
    )

    assert [row["ask_size_contracts"] for row in rows] == [5.0, 10.0]


def test_corrected_source_lineage_changes_evidence_and_on_change_identity() -> None:
    fixture = _load_kalshi_fixture("kalshi_orderbook_yes_price.json")
    states = {}
    snapshots = []
    for message in fixture["messages"]:
        snapshots.extend(
            apply_kalshi_orderbook_message(
                states,
                message,
                use_yes_price=True,
            )
        )
    corrected_no = kalshi_ws_snapshot_to_topbook(
        snapshots[-1].as_dict(),
        received_at_utc="2026-01-01T00:00:00+00:00",
        received_at_monotonic_ns=1,
    )[1]
    legacy_no = dict(corrected_no)
    legacy_no["best_bid_source"] = "direct"
    legacy_no["best_ask_source"] = "direct"

    assert topbook_evidence_id(corrected_no) != topbook_evidence_id(legacy_no)
    assert topbook_state_fingerprint(corrected_no) != topbook_state_fingerprint(
        legacy_no
    )
