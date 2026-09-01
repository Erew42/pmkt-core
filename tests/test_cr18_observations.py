from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from pmkt.data.registry import STREAM_LIFECYCLE_SCHEMA_VERSION, TRADE_SCHEMA_VERSION
from pmkt.data.storage.duckdb import query_parquet
from pmkt.data.storage.parquet import write_parquet
from pmkt.data.validation import validate_frame
from pmkt.streaming.observations import (
    KALSHI_LIFECYCLE_EVENTS,
    ObservationValidationError,
    StreamObservationProducer,
)
from pmkt.streaming.tape import CaptureCoordinate, deterministic_merge_key

_UTC = "2026-07-19T10:00:00.000000Z"


def _coordinate(sequence: int = 1) -> CaptureCoordinate:
    return CaptureCoordinate("run-1", "shard-0", _UTC, 10, sequence, 3)


def test_polymarket_trade_observation_is_strict_and_within_run_stable() -> None:
    producer = StreamObservationProducer(collector_run_id="run-1")
    message = {
        "event_type": "last_trade_price",
        "asset_id": "token-1",
        "market": "market-1",
        "timestamp": 1_753_002_000_000,
        "price": "0.42",
        "size": "5",
        "side": "BUY",
    }
    first = producer.polymarket(message, _coordinate()).trades[0]
    same = producer.polymarket(message, _coordinate()).trades[0]
    different_coordinate = producer.polymarket(message, _coordinate(2)).trades[0]
    assert first["venue_trade_id"] == same["venue_trade_id"]
    assert first["venue_trade_id"] != different_coordinate["venue_trade_id"]
    assert first["notional_dollars"] == pytest.approx(2.1)
    assert first["collector_run_id"] == "run-1"
    assert first["received_at_monotonic_ns"] == 10
    assert first["local_sequence"] == 1
    assert first["subsequence"] == 3
    report = validate_frame(pd.DataFrame([first]), TRADE_SCHEMA_VERSION, strict=True)
    assert report.ok, report.errors


def test_polymarket_trade_trigger_allows_missing_side_and_size() -> None:
    producer = StreamObservationProducer(collector_run_id="run-1")
    row = producer.polymarket(
        {
            "event_type": "last_trade_price",
            "asset_id": "token-1",
            "market": "market-1",
            "timestamp": 1_753_002_000_000,
            "price": "0.42",
        },
        _coordinate(),
    ).trades[0]

    assert row["size_contracts"] is None
    assert row["notional_dollars"] is None
    assert row["aggressor_side"] is None
    report = validate_frame(pd.DataFrame([row]), TRADE_SCHEMA_VERSION, strict=True)
    assert report.ok, report.errors


def test_kalshi_native_trade_id_deduplicates_duplicate_delivery() -> None:
    producer = StreamObservationProducer(collector_run_id="run-1")
    message = {
        "type": "trade",
        "msg": {
            "trade_id": "native-1",
            "market_ticker": "KX-1",
            "yes_price": 41,
            "no_price": 59,
            "count_fp": "7",
            "taker_side": "yes",
            "created_time": "2026-07-19T10:00:00Z",
        },
    }
    first = producer.kalshi(message, _coordinate())
    duplicate = producer.kalshi(message, _coordinate(2))
    assert len(first.trades) == 1
    assert duplicate.trades == ()
    row = first.trades[0]
    assert row["venue_trade_id"] == "native-1"
    assert row["price_dollars"] == pytest.approx(0.41)
    assert row["size_contracts"] == pytest.approx(7.0)
    assert row["collector_run_id"] == "run-1"
    assert row["received_at_monotonic_ns"] == 10
    assert row["local_sequence"] == 1
    assert row["subsequence"] == 3
    report = validate_frame(pd.DataFrame([row]), TRADE_SCHEMA_VERSION, strict=True)
    assert report.ok, report.errors


def test_invalid_kalshi_delivery_does_not_poison_native_id_deduplication() -> None:
    producer = StreamObservationProducer(collector_run_id="run-1")
    invalid = {
        "type": "trade",
        "msg": {"trade_id": "native-1", "market_ticker": "KX-1"},
    }
    with pytest.raises(ObservationValidationError, match="price and count"):
        producer.kalshi(invalid, _coordinate())

    corrected = {
        "type": "trade",
        "msg": {
            "trade_id": "native-1",
            "market_ticker": "KX-1",
            "yes_price": 41,
            "count": 2,
        },
    }
    assert len(producer.kalshi(corrected, _coordinate(2)).trades) == 1


@pytest.mark.parametrize(
    ("price_fields", "expected"),
    [
        ({"yes_price_dollars": "0.0100"}, 0.01),
        ({"yes_price_dollars_fp": "0.415"}, 0.415),
        ({"yes_price": 1}, 0.01),
        ({"yes_price": 41}, 0.41),
        ({"yes_price_dollars": "0.41", "yes_price": 41}, 0.41),
    ],
)
def test_kalshi_trade_price_fields_have_explicit_units(
    price_fields: dict[str, object], expected: float
) -> None:
    payload: dict[str, object] = {
        "trade_id": "native-1",
        "market_ticker": "KX-1",
        "count_fp": "2",
        **price_fields,
    }
    row = StreamObservationProducer(collector_run_id="run-1").kalshi(
        {"type": "trade", "msg": payload}, _coordinate()
    ).trades[0]

    assert row["price_dollars"] == pytest.approx(expected)


@pytest.mark.parametrize(
    ("price_fields", "error"),
    [
        ({"price": 1}, "ambiguous"),
        (
            {"yes_price_dollars": "0.41", "yes_price": 42},
            "prices disagree",
        ),
    ],
)
def test_kalshi_trade_price_ambiguity_fails_closed(
    price_fields: dict[str, object], error: str
) -> None:
    payload: dict[str, object] = {
        "trade_id": "native-1",
        "market_ticker": "KX-1",
        "count_fp": "2",
        **price_fields,
    }

    with pytest.raises(ObservationValidationError, match=error):
        StreamObservationProducer(collector_run_id="run-1").kalshi(
            {"type": "trade", "msg": payload}, _coordinate()
        )


def test_partial_trade_capture_coordinate_fails_strict_validation() -> None:
    row = StreamObservationProducer(collector_run_id="run-1").kalshi(
        {
            "type": "trade",
            "msg": {
                "trade_id": "native-1",
                "market_ticker": "KX-1",
                "yes_price_dollars": "0.41",
                "count_fp": "2",
            },
        },
        _coordinate(),
    ).trades[0]
    row["local_sequence"] = None

    report = validate_frame(pd.DataFrame([row]), TRADE_SCHEMA_VERSION, strict=True)

    assert not report.ok
    assert any("capture coordinate" in error for error in report.errors)


@pytest.mark.parametrize("event_type", sorted(KALSHI_LIFECYCLE_EVENTS))
def test_every_supported_kalshi_lifecycle_event_maps_strictly(event_type: str) -> None:
    producer = StreamObservationProducer(collector_run_id="run-1")
    observation = producer.kalshi(
        {
            "type": "market_lifecycle_v2",
            "seq": 12,
            "msg": {
                "event_type": event_type,
                "market_ticker": "KX-1",
                "status": "active",
                "ts": "2026-07-19T10:00:00Z",
            },
        },
        _coordinate(),
    )
    row = observation.lifecycle[0]
    assert row["event_type"] == event_type
    report = validate_frame(
        pd.DataFrame([row]), STREAM_LIFECYCLE_SCHEMA_VERSION, strict=True
    )
    assert report.ok, report.errors


@pytest.mark.parametrize(
    ("event_type", "expected_status"),
    [
        ("created", "initialized"),
        ("activated", "active"),
        ("deactivated", "inactive"),
        ("determined", "determined"),
        ("settled", "finalized"),
        ("close_date_updated", None),
        ("metadata_updated", None),
    ],
)
def test_kalshi_lifecycle_derives_canonical_status_without_status_field(
    event_type: str,
    expected_status: str | None,
) -> None:
    row = StreamObservationProducer(collector_run_id="run-1").kalshi(
        {
            "type": "market_lifecycle_v2",
            "seq": 12,
            "msg": {
                "event_type": event_type,
                "market_ticker": "KX-1",
                "previous_status": "open",
                "ts": "2026-07-19T10:00:00Z",
            },
        },
        _coordinate(),
    ).lifecycle[0]

    assert row["previous_status"] == "active"
    assert row["new_status"] == expected_status
    report = validate_frame(
        pd.DataFrame([row]), STREAM_LIFECYCLE_SCHEMA_VERSION, strict=True
    )
    assert report.ok, report.errors


def test_kalshi_lifecycle_explicit_status_wins_and_is_canonicalized() -> None:
    row = StreamObservationProducer(collector_run_id="run-1").kalshi(
        {
            "type": "market_lifecycle_v2",
            "seq": 12,
            "msg": {
                "event_type": "close_date_updated",
                "market_ticker": "KX-1",
                "new_status": "open",
                "ts": "2026-07-19T10:00:00Z",
            },
        },
        _coordinate(),
    ).lifecycle[0]

    assert row["new_status"] == "active"


def test_kalshi_lifecycle_ws_row_round_trips_through_parquet_and_duckdb(
    tmp_path: Path,
) -> None:
    row = StreamObservationProducer(collector_run_id="run-1").kalshi(
        {
            "type": "market_lifecycle_v2",
            "seq": 12,
            "msg": {
                "event_type": "settled",
                "market_ticker": "KX-1",
                "previous_status": "determined",
                "ts": "2026-07-19T10:00:00Z",
            },
        },
        _coordinate(),
    ).lifecycle[0]
    path = tmp_path / "stream_lifecycle.parquet"
    write_parquet(
        pd.DataFrame([row]),
        path,
        schema=STREAM_LIFECYCLE_SCHEMA_VERSION,
        coerce=True,
        strict=True,
    )

    persisted = query_parquet(
        "SELECT previous_status, new_status FROM lifecycle",
        {"lifecycle": path},
    )

    assert persisted.to_dict("records") == [
        {"previous_status": "determined", "new_status": "finalized"}
    ]


@pytest.mark.parametrize(
    "event_type", ["new_market", "market_resolved", "tick_size_change"]
)
def test_supported_polymarket_lifecycle_maps_strictly(event_type: str) -> None:
    producer = StreamObservationProducer(collector_run_id="run-1")
    observation = producer.polymarket(
        {
            "event_type": event_type,
            "market": "market-1",
            "asset_id": "token-1",
            "timestamp": "2026-07-19T10:00:00Z",
            "new_tick_size": "0.01",
        },
        _coordinate(),
    )
    report = validate_frame(
        pd.DataFrame(observation.lifecycle),
        STREAM_LIFECYCLE_SCHEMA_VERSION,
        strict=True,
    )
    assert report.ok, report.errors


def test_mve_lifecycle_is_explicitly_unsupported() -> None:
    producer = StreamObservationProducer(collector_run_id="run-1")
    with pytest.raises(ValueError, match="explicitly unsupported"):
        producer.kalshi(
            {"type": "mve_market_lifecycle", "msg": {"market_ticker": "KX-1"}},
            _coordinate(),
        )


def test_observation_producer_binds_nonempty_run_coordinates() -> None:
    with pytest.raises(
        ObservationValidationError, match="collector_run_id is required"
    ):
        StreamObservationProducer(collector_run_id=" ")

    producer = StreamObservationProducer(collector_run_id="run-1")
    with pytest.raises(ObservationValidationError, match="does not match producer run"):
        producer.polymarket(
            {
                "event_type": "last_trade_price",
                "asset_id": "token-1",
                "price": "0.42",
            },
            CaptureCoordinate("foreign-run", "shard-0", _UTC, 10, 1, 3),
        )


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("price", "nan", "finite"),
        ("price", "1.1", "between 0 and 1"),
        ("size", "bad", "numeric"),
        ("size", 0, "positive"),
        ("timestamp", "not-a-time", "valid UTC timestamp"),
    ],
)
def test_polymarket_explicit_malformed_trade_fields_fail_closed(
    field: str, value: object, error: str
) -> None:
    message: dict[str, object] = {
        "event_type": "last_trade_price",
        "asset_id": "token-1",
        "market": "market-1",
        "timestamp": "2026-07-19T10:00:00Z",
        "price": "0.42",
        "size": "2",
    }
    message[field] = value

    producer = StreamObservationProducer(collector_run_id="run-1")
    with pytest.raises(ObservationValidationError, match=error):
        producer.polymarket(message, _coordinate())


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("yes_price", 101, "between 0 and 100"),
        ("count", 0, "positive"),
        ("created_time", "not-a-time", "valid UTC timestamp"),
    ],
)
def test_kalshi_explicit_malformed_trade_fields_fail_closed(
    field: str, value: object, error: str
) -> None:
    payload: dict[str, object] = {
        "trade_id": "native-1",
        "market_ticker": "KX-1",
        "yes_price": 41,
        "count": 2,
        "created_time": "2026-07-19T10:00:00Z",
    }
    payload[field] = value

    producer = StreamObservationProducer(collector_run_id="run-1")
    with pytest.raises(ObservationValidationError, match=error):
        producer.kalshi({"type": "trade", "msg": payload}, _coordinate())


def test_kalshi_lifecycle_rejects_unknown_subtype_and_keeps_nested_times() -> None:
    producer = StreamObservationProducer(collector_run_id="run-1")
    with pytest.raises(ObservationValidationError, match="missing or unsupported"):
        producer.kalshi(
            {
                "type": "market_lifecycle_v2",
                "msg": {"event_type": "future_event", "market_ticker": "KX-1"},
            },
            _coordinate(),
        )

    row = producer.kalshi(
        {
            "type": "market_lifecycle_v2",
            "seq": 12,
            "msg": {
                "event_type": "settled",
                "market_ticker": "KX-1",
                "ts": "2026-07-19T10:00:00Z",
                "close_time": "2026-07-19T09:59:00Z",
            },
        },
        _coordinate(2),
    ).lifecycle[0]

    assert row["exchange_at_utc"] == "2026-07-19T10:00:00.000000Z"
    assert row["market_close_at_utc"] == "2026-07-19T09:59:00.000000Z"


def test_malformed_lifecycle_tick_fails_closed() -> None:
    producer = StreamObservationProducer(collector_run_id="run-1")
    with pytest.raises(
        ObservationValidationError, match="new_tick_size must be numeric"
    ):
        producer.polymarket(
            {
                "event_type": "tick_size_change",
                "market": "market-1",
                "new_tick_size": "bad",
            },
            _coordinate(),
        )


def test_terminal_lifecycle_sorts_after_same_cause_invalidation() -> None:
    producer = StreamObservationProducer(collector_run_id="run-1")
    lifecycle = producer.polymarket(
        {"event_type": "market_resolved", "market": "market-1"},
        _coordinate(9),
    ).lifecycle[0]
    invalidation = {
        "received_at_utc": lifecycle["received_at_utc"],
        "local_sequence": lifecycle["local_sequence"],
        "subsequence": 0,
        "control_id": "0" * 64,
    }

    invalidation_key = deterministic_merge_key(
        invalidation, shard_id="shard-0", family="invalidation_control"
    )
    lifecycle_key = deterministic_merge_key(
        lifecycle, shard_id="shard-0", family="trade_lifecycle"
    )
    assert invalidation_key < lifecycle_key
