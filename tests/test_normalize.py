import hashlib
import json

import pandas as pd
import pytest

from pmkt.data.canonical import POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION
from pmkt.data.normalize import (
    extract_close_time,
    extract_open_time,
    extract_start_time,
    extract_market_rows,
    extract_mid_price,
    extract_yes_outcome_price,
    first_numeric,
    markets_dataframe,
    parse_float,
)
from pmkt.data.validation import validate_frame


def test_parse_float_and_first_numeric() -> None:
    payload = {"a": "1.5", "b": None, "c": "bad"}

    assert parse_float("2.5") == 2.5
    assert parse_float(True) is None
    assert first_numeric(payload, ("b", "a", "c")) == 1.5


def test_first_numeric_skips_non_finite_values() -> None:
    payload = {"nan": "NaN", "inf": float("inf"), "fallback": "3.25"}

    assert parse_float("NaN") is None
    assert parse_float(float("inf")) is None
    assert first_numeric(payload, ("nan", "inf", "fallback")) == pytest.approx(3.25)


def test_extract_market_rows_tokens_and_fields() -> None:
    markets = [
        {
            "id": 123,
            "slug": "test-market",
            "question": "Will it rain?",
            "startDate": "2026-01-01T00:00:00Z",
            "createdAt": "2025-12-01T00:00:00Z",
            "endDate": "2026-06-01T00:00:00Z",
            "closed": False,
            "volume": "10.5",
            "liquidityUsd": "42",
            "enableOrderBook": True,
            "outcomes": [
                {"token_id": "token-1"},
                {"token_id": "token-2"},
            ],
        }
    ]

    rows = extract_market_rows(markets)

    assert len(rows) == 1
    row = rows[0]
    assert row["market_id"] == "123"
    assert row["slug"] == "test-market"
    assert row["question"] == "Will it rain?"
    assert row["open_time"] == "2026-01-01T00:00:00Z"
    assert row["start_time"] == "2026-01-01T00:00:00Z"
    assert row["close_time"] == "2026-06-01T00:00:00Z"
    assert row["closed"] is False
    assert row["volume"] == pytest.approx(10.5)
    assert row["liquidity"] == pytest.approx(42.0)
    assert row["enable_orderbook"] is True
    assert set(row["token_ids"]) == {"token-1", "token-2"}
    assert row["schema_version"] == POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION
    assert row["raw_json"] == json.dumps(markets[0], ensure_ascii=True, sort_keys=True, default=str)
    assert row["raw_json_sha256"] == hashlib.sha256(
        row["raw_json"].encode("utf-8")
    ).hexdigest()
    assert validate_frame(
        pd.DataFrame(rows),
        POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    ).ok


def test_markets_dataframe_preserves_legacy_v1_resolution_fields() -> None:
    df = markets_dataframe(
        [
            {
                "id": "pm-1",
                "question": "Will it rain?",
                "conditionId": "0xabc",
                "questionID": "q-1",
                "outcomes": '["Yes", "No"]',
                "outcomePrices": '["1", "0"]',
                "umaResolutionStatus": "resolved",
                "resolvedBy": "uma",
                "resolutionSource": "oracle",
            }
        ]
    )

    assert df.loc[0, "schema_version"] == POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION
    assert df.loc[0, "condition_id"] == "0xabc"
    assert df.loc[0, "question_id"] == "q-1"
    assert df.loc[0, "outcome_labels_json"] == ["yes", "no"]
    assert df.loc[0, "outcome_prices_json"] == ["1", "0"]
    assert df.loc[0, "uma_resolution_status"] == "resolved"
    assert df.loc[0, "resolved_by"] == "uma"
    assert df.loc[0, "resolution_source"] == "oracle"
    assert validate_frame(
        df,
        POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    ).ok


def test_extract_polymarket_open_and_start_times_from_nested_event() -> None:
    market = {
        "id": 1,
        "question": "Will it happen?",
        "event": {
            "startDate": "2026-02-01T00:00:00Z",
            "createdAt": "2026-01-15T00:00:00Z",
        },
    }

    assert extract_open_time(market) == "2026-02-01T00:00:00Z"
    assert extract_start_time(market) == "2026-02-01T00:00:00Z"


def test_extract_polymarket_open_time_prefers_accepting_orders_timestamp() -> None:
    market = {
        "id": 1,
        "question": "Will it happen?",
        "createdAt": "2026-01-01T00:00:00Z",
        "startDate": "2026-01-02T00:00:00Z",
        "acceptingOrdersTimestamp": "2026-01-03T00:00:00Z",
    }

    assert extract_open_time(market) == "2026-01-03T00:00:00Z"


def test_extract_polymarket_start_time_prefers_event_start_over_market_start() -> None:
    market = {
        "id": 1,
        "question": "Will it happen?",
        "startDate": "2026-01-02T00:00:00Z",
        "eventStartTime": "2026-01-04T00:00:00Z",
        "events": [{"startTime": "2026-01-05T00:00:00Z"}],
    }

    assert extract_start_time(market) == "2026-01-04T00:00:00Z"


def test_extract_polymarket_start_time_uses_nested_event_before_market_start_date() -> None:
    market = {
        "id": 1,
        "question": "Will it happen?",
        "startDate": "2026-01-02T00:00:00Z",
        "events": [{"startTime": "2026-01-05T00:00:00Z"}],
    }

    assert extract_open_time(market) == "2026-01-02T00:00:00Z"
    assert extract_start_time(market) == "2026-01-05T00:00:00Z"


def test_extract_close_time_skips_empty_aliases_but_preserves_zero() -> None:
    assert extract_close_time(
        {
            "closeTime": None,
            "close_time": "",
            "endDate": 0,
            "event": {"endDate": "2026-06-01T00:00:00Z"},
        }
    ) == 0


def test_extract_close_time_uses_nested_value_after_empty_top_level_values() -> None:
    assert extract_close_time(
        {
            "closeTime": "",
            "endDate": None,
            "events": [{"endDate": "2026-06-01T00:00:00Z"}],
        }
    ) == "2026-06-01T00:00:00Z"


def test_extract_close_time_does_not_hide_malformed_nonempty_priority_value() -> None:
    assert extract_close_time(
        {
            "closeTime": "not-a-timestamp",
            "endDate": "2026-06-01T00:00:00Z",
        }
    ) == "not-a-timestamp"


def test_extract_market_rows_event_and_orderbook() -> None:
    markets = [
        {
            "id": 999,
            "slug": "event-market",
            "question": "Test market?",
            "closed": "false",
            "enableOrderBook": "true",
            "clobTokenIds": ["alpha", "beta"],
            "events": [{"id": 1234, "slug": "event-slug"}],
        }
    ]

    rows = extract_market_rows(markets)
    assert len(rows) == 1
    row = rows[0]
    assert row["event_id"] == "1234"
    assert row["event_slug"] == "event-slug"
    assert row["enable_orderbook"] is True
    assert row["token_ids"] == ["alpha", "beta"]


def test_extract_market_rows_keeps_gamma_best_quote_prices() -> None:
    markets = [
        {
            "id": 540817,
            "slug": "new-rhianna-album-before-gta-vi-926",
            "question": "New Rihanna Album before GTA VI?",
            "closed": False,
            "clobTokenIds": '["yes-token", "no-token"]',
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.515", "0.485"]',
            "bestBid": 0.51,
            "bestAsk": 0.52,
            "lastTradePrice": 0.53,
        }
    ]

    rows = extract_market_rows(markets)

    assert len(rows) == 1
    row = rows[0]
    assert row["yes_bid"] == pytest.approx(0.51)
    assert row["yes_ask"] == pytest.approx(0.52)
    assert row["mid"] == pytest.approx(0.515)
    assert row["spread"] == pytest.approx(0.01)
    assert row["last_trade_price"] == pytest.approx(0.53)


def test_extract_mid_price_falls_back_to_yes_outcome_price() -> None:
    market = {
        "outcomes": '["Yes", "No"]',
        "outcomePrices": '["0.375", "0.625"]',
        "lastTradePrice": 0.41,
    }

    assert extract_yes_outcome_price(market) == pytest.approx(0.375)
    assert extract_mid_price(market) == pytest.approx(0.375)
