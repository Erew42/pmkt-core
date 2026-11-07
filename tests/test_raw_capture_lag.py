"""Sampled lag statistics use only public replay data and deterministic clocks."""

import gzip
import json
from pathlib import Path

import pytest

from pmkt.streaming import raw_capture as rr


@pytest.mark.parametrize("venue", ["kalshi", "polymarket"])
def test_exchange_lag_on_every_recorded_text_payload(venue):
    payloads, kinds, sampled = [], set(), 0
    with gzip.open(
        Path(__file__).parent / "fixtures" / "raw_capture" / f"{venue}_00.jsonl.gz",
        "rt",
    ) as source:
        for line in source:
            row = json.loads(line)
            if row["frame_type"] != "text":
                continue
            raw = row["payload_raw"]
            payload = json.loads(raw)
            events = payload if isinstance(payload, list) else [payload]
            kinds.update(event.get("event_type", event.get("type")) for event in events)
            exchange_ms = (
                payload.get("sending_ts_ms")
                if venue == "kalshi"
                else next(
                    (
                        int(event["timestamp"])
                        for event in events
                        if "timestamp" in event
                    ),
                    None,
                )
            )
            lag = rr.LagMonitor(venue)
            lag.raw, lag.received_ns = raw, (exchange_ms or 0) * 1_000_000 + 123_125_000
            lag.sample(1, 1)
            if exchange_ms is not None:
                sampled += 1
                assert lag.stats(1)["exchange_lag_ms_last"] == 123.125
            assert lag.stats(1)["lag_samples_60s"] == int(exchange_ms is not None)
            if exchange_ms is not None and len(payloads) < 2:
                payloads.append(raw)
    assert sampled > 100
    if venue == "polymarket":
        assert "book" in kinds
        lag.raw = "[" + ",".join(payloads) + "]"
        lag.received_ns = (
            int(json.loads(payloads[0])["timestamp"]) * 1_000_000 + 456_000_000
        )
        lag.sample(2, 2)
        assert lag.stats(2)["exchange_lag_ms_last"] == 456


@pytest.mark.parametrize(
    ("venue", "raw", "exchange_ms"),
    [
        ("kalshi", '{"msg":{"ts_ms":999},"sending_ts_ms":1000}', 1000),
        ("kalshi", '{ "sending_ts_ms" : 1000, "msg":{"ts_ms":999} }', 1000),
        ("polymarket", '[{"timestamp":"1000"},{"timestamp":"9000"}]', 1000),
        ("polymarket", '[{"event_type":"new_market"},{"timestamp" : "1000"}]', 1000),
        ("polymarket", '{"event_type":"book", "timestamp" : "1000"}', 1000),
    ],
)
def test_exchange_lag_keeps_negative_values_and_first_timestamp(
    venue, raw, exchange_ms, monkeypatch
):
    lag = rr.LagMonitor(venue)
    lag.raw, lag.received_ns = raw, (exchange_ms - 25) * 1_000_000 - 125_000

    def unexpected_parse(*args):
        pytest.fail("lag sampling used JSON parsing")

    monkeypatch.setattr(rr.json, "loads", unexpected_parse)
    lag.sample(1, 1)
    assert lag.stats(1) == dict(
        exchange_lag_ms_last=-25.125,
        exchange_lag_ms_p50_60s=-25.125,
        exchange_lag_ms_max_60s=-25.125,
        loop_lag_ms_max_60s=0,
        lag_samples_60s=1,
    )
    lag.sample(2, 2)
    assert lag.stats(2)["lag_samples_60s"] == 1


@pytest.mark.parametrize("venue", ["kalshi", "polymarket"])
def test_timestamp_free_and_malformed_numbers_are_skipped(venue):
    lag = rr.LagMonitor(venue)
    payloads = [
        "PING",
        "PONG",
        "",
        "{",
        "[]",
        '{"event_type":"new_market"}',
        '{"msg":{"ts_ms":1000}}',
    ]
    numbers = ["", "null", "true", "-1", "+1", "1.5", "1e3", "123bad", "1" * 5000]
    if venue == "kalshi":
        payloads += [
            '{"sending_ts_ms":' + number + "}" for number in numbers + ["01", '"123"']
        ]
    else:
        payloads += ['{"timestamp":"' + number + '"}' for number in numbers]
        payloads += ['{"timestamp":123}']
    for raw in payloads:
        lag.raw, lag.received_ns = raw, 1_000_000_000
        lag.sample(1, 1)
    assert lag.stats(1)["lag_samples_60s"] == 0
    assert lag.stats(1)["exchange_lag_ms_last"] is None


def test_lag_window_expires_samples_and_uses_even_and_odd_medians():
    lag = rr.LagMonitor("polymarket")
    empty = dict(
        exchange_lag_ms_last=None,
        exchange_lag_ms_p50_60s=None,
        exchange_lag_ms_max_60s=None,
        loop_lag_ms_max_60s=None,
        lag_samples_60s=0,
    )
    assert lag.stats(0) == empty
    for now, value, oversleep in [
        (1, 100, 0.5),
        (2, -20, 0.125),
        (3, 40, 0),
        (4, 10, 0.25),
    ]:
        lag.raw, lag.received_ns = '{"timestamp":"1000"}', (1000 + value) * 1_000_000
        lag.sample(now, now - oversleep)
    assert lag.stats(4) == dict(
        exchange_lag_ms_last=10,
        exchange_lag_ms_p50_60s=25,
        exchange_lag_ms_max_60s=100,
        loop_lag_ms_max_60s=500,
        lag_samples_60s=4,
    )
    assert lag.stats(61) == dict(
        exchange_lag_ms_last=10,
        exchange_lag_ms_p50_60s=10,
        exchange_lag_ms_max_60s=40,
        loop_lag_ms_max_60s=250,
        lag_samples_60s=3,
    )
    assert lag.stats(64) == empty
    lag.sample(65, 66)  # An early supervisor wake is not an oversleep.
    assert lag.stats(65)["loop_lag_ms_max_60s"] == 0
    assert lag.stats(65)["lag_samples_60s"] == 0
