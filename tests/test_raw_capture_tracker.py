"""Tracker optimizations must agree with the original decoder after every frame."""

import gzip
import json
from pathlib import Path

import pytest

from pmkt.streaming import raw_capture as rr


class ReferenceTracker(rr.Tracker):
    def observe(self, raw):
        try:
            events = json.loads(raw)
        except (ValueError, RecursionError):
            return
        for event in events if isinstance(events, list) else [events]:
            if not isinstance(event, dict):
                continue
            if self.venue == "polymarket":
                if (
                    event.get("event_type") == "book"
                    and event.get("asset_id") in self.expected
                ):
                    self.books.add(event["asset_id"])
                continue
            kind, msg = event.get("type"), event.get("msg")
            msg = msg if isinstance(msg, dict) else {}
            if kind == "subscribed" and msg.get("channel"):
                self.acks.add(str(msg["channel"]))
            if kind in ("orderbook_snapshot", "orderbook_delta"):
                if (
                    kind == "orderbook_snapshot"
                    and msg.get("market_ticker") in self.expected
                ):
                    self.books.add(msg["market_ticker"])
                sid, seq = event.get("sid"), event.get("seq")
                if isinstance(sid, int) and isinstance(seq, int):
                    last = self.seqs.get(sid)
                    if last is not None and seq != last + 1 and self.gap is None:
                        self.gap = dict(sid=sid, expected=last + 1, got=seq)
                    self.seqs[sid] = seq


def assert_replay(venue, instruments, payloads):
    reference, tracker = (
        ReferenceTracker(venue, instruments),
        rr.Tracker(venue, instruments),
    )
    for index, raw in enumerate(payloads):
        reference.observe(raw)
        tracker.observe(raw)
        assert (tracker.books, tracker.acks, tracker.seqs, tracker.gap) == (
            reference.books,
            reference.acks,
            reference.seqs,
            reference.gap,
        ), (index, raw[:200])


@pytest.mark.parametrize(("venue", "count"), [("polymarket", 202), ("kalshi", 202)])
def test_every_recorded_text_frame_matches_original_tracker(venue, count):
    with gzip.open(
        Path(__file__).parent / "fixtures" / "raw_capture" / f"{venue}_00.jsonl.gz",
        "rt",
    ) as source:
        payloads = [
            row["payload_raw"]
            for line in source
            if (row := json.loads(line))["frame_type"] == "text"
        ]
    assert len(payloads) == count
    instruments = set()
    for raw in payloads:
        events = json.loads(raw)
        for event in events if isinstance(events, list) else [events]:
            instrument = (
                event.get("asset_id")
                if venue == "polymarket"
                else event.get("msg", {}).get("market_ticker")
            )
            if instrument:
                instruments.add(instrument)
    assert_replay(venue, sorted(instruments)[::2], payloads)


def test_polymarket_unusual_and_malformed_payloads_match_original():
    payloads = [
        "PING",
        "PONG",
        "",
        "{",
        "null",
        "42",
        "[]",
        '{"event_type":"price_change","question":"Will the book sell?"}',
        '[{"event_type":"price_change"},{"event_type":"book","asset_id":"a"}]',
        '{"event_type" : "book", "asset_id" : "b"}',
        r'{"event_type":"bo\u006fk","asset_id":"c"}',
        r'{"event_t\u0079pe":"\u0062\u006f\u006f\u006b","asset_id":"d"}',
        '{"event_type":"book","asset_id":"other"}',
        '[null,42,"book",{"event_type":"book","asset_id":"e"}]',
        '{"event_type":"book","asset_id":"bad"',
        '{"event_type":"book","asset_id":"bad"} trailing',
        '{"event_type":"book","asset_id":"bad",}',
    ]
    assert_replay("polymarket", list("abcde") + ["bad"], payloads)


def test_kalshi_unusual_and_malformed_payloads_match_original():
    payloads = [
        "PING",
        "PONG",
        "",
        "{",
        "null",
        "42",
        "[]",
        '{"type":"subscribed","msg":{"channel":"orderbook_delta"}}',
        '{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"K"}}',
        '{"type":"orderbook_delta","sid":1,"seq":2,"msg":{}}',
        '{"msg":{"market_ticker":"K"},"seq":3,"sid":1,"type":"orderbook_delta"}',
        '{"type": "orderbook_delta", "sid": 1, "seq": 4, "msg": {}}',
        '{"type"\t:\n"orderbook_delta","sid":1,"seq":5,"msg":{}}',
        '{"type":"orderbook_delta","sid":1,"seq":6,"msg":{"side":"yes","clock":42},"sending_ts_ms":123}\n',
        '{"type":"orderbook_delta","sid":1,"seq":9,"msg":{}}',
        '{"type":"orderbook_delta","sid":1,"seq":12,"msg":{}}',
        '{"type":"orderbook_snapshot","sid":2,"seq":1,"msg":{"market_ticker":"OTHER"}}',
        '[{"type":"subscribed","msg":{"channel":7}},'
        '{"type":"orderbook_snapshot","sid":2,"seq":2,"msg":{"market_ticker":"K2"}}]',
        '{"type":"orderbook_delta","sid":2,"seq":3,"msg":{},"seq":4}',
        '{"type":"orderbook_delta","sid":2,"seq":99,"msg":{},"type":"trade"}',
        '{"type":"orderbook_delta","sid":2,"seq":5,"msg":{"seq":999,"type":"trade"}}',
        '{"type":"orderbook_delta","sid":2,"seq":6,"msg":{"nested":{"seq":999}}}',
        r'{"type":"orderbook_delta","sid":2,"seq":7,"msg":{"text":"quote\" and \\ slash"}}',
        '{"type":"orderbook_delta","sid":2,"seq":8,"msg":null}',
        '{"type":"trade","sid":2,"seq":100,"msg":{}}',
    ]
    for sid, seq in [
        ("2", 9),
        (2, "9"),
        (2.0, 9),
        (2, 9.0),
        (None, 9),
        (2, None),
        (True, 10),
        (False, False),
        (-3, -5),
        (2**80, 2**80),
    ]:
        payloads.append(
            json.dumps(
                dict(type="orderbook_delta", sid=sid, seq=seq, msg={}),
                separators=(",", ":"),
            )
        )
    head = '{"type":"orderbook_delta","sid":2,"seq":99,"msg":'
    payloads += [
        head + tail
        for tail in (
            "{",
            "{}",
            "{}} trailing",
            "{},}",
            '{"x":01}}',
            '{"x":1,}}',
            '{"x":"bad\nstring"}}',
            r'{"x":"bad\q"}}',
            '{"x":[' + "[" * 1100 + "]}}",
            '{"x":' + "1" * 5000 + "}}",
        )
    ]
    payloads += [
        '{"type":"orderbook_delta","sid":01,"seq":99,"msg":{}}',
        '{"type":"orderbook_delta","sid":2,"seq":099,"msg":{}}',
        head + '{},"sending_ts_ms":01}',
        head + '{},"sending_ts_ms":' + "1" * 5000 + "}",
    ]
    assert_replay("kalshi", ["K", "K2"], payloads)


def test_common_frames_do_not_call_json_loads(monkeypatch):
    def unexpected_parse(raw):
        pytest.fail("common frame used json.loads")

    monkeypatch.setattr(rr.json, "loads", unexpected_parse)
    rr.Tracker("polymarket", ["a"]).observe(
        '{"event_type":"price_change","asset_id":"a"}'
    )
    tracker = rr.Tracker("kalshi", ["K"])
    tracker.observe(
        '{"type":"orderbook_delta","sid":1,"seq":2,"msg":{"market_ticker":"K","clock":42}}'
    )
    tracker.observe(
        '{"type":"orderbook_delta","sid":1,"seq":4,"msg":{},"sending_ts_ms":123}\n'
    )
    assert tracker.seqs == {1: 4} and tracker.gap == {"sid": 1, "expected": 3, "got": 4}
