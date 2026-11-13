"""Merging recorded copies: small hand-checkable inputs for each merge rule."""

from __future__ import annotations

import itertools
import json
from datetime import datetime, timedelta, timezone

import pytest

from pmkt.streaming import raw_capture
from pmkt.streaming.copy_coverage import (
    CoverageTracker,
    uncovered_intervals,
    utc_microseconds,
)
from pmkt.streaming.copy_merge import (
    CopyMerge,
    CopyMergeError,
    Occurrence,
    connection_occurrences,
    discover_sessions,
    message_key,
)
from pmkt.streaming.copy_merge_books import check_merged_books
from pmkt.streaming.raw_capture import ChunkWriter

T0 = 1_793_673_000_000  # 2026-11-03T02:30:00Z in milliseconds


# -- message builders ------------------------------------------------------------


def change(ts, price, size, *, market="M", token="yes", side="BUY"):
    """One Polymarket price_change setting one level, as the venue states it."""
    return {
        "market": market,
        "price_changes": [
            {"asset_id": token, "price": price, "size": size, "side": side, "hash": "h"}
        ],
        "timestamp": str(ts),
        "event_type": "price_change",
    }


def book(ts, bids, asks=(), *, market="M", token="yes"):
    return {
        "market": market,
        "asset_id": token,
        "timestamp": str(ts),
        "hash": f"book-{ts}",
        "bids": [{"price": p, "size": s} for p, s in bids],
        "asks": [{"price": p, "size": s} for p, s in asks],
        "event_type": "book",
    }


def delta(ts, price, amount, *, sid=1, seq=1, sent=None, ticker="K", clock=None):
    return {
        "type": "orderbook_delta",
        "sid": sid,
        "seq": seq,
        "msg": {
            "market_ticker": ticker,
            "clock": ts if clock is None else clock,
            "price_dollars": price,
            "delta_fp": amount,
            "side": "yes",
            "ts_ms": ts,
        },
        "sending_ts_ms": ts + 5 if sent is None else sent,
    }


def stream(copy, messages, *, connection="polymarket_00", lag_ms=20, session="s1"):
    """Occurrences of one stream; each message received ``lag_ms`` after its time.

    ``messages`` items are a message or a (received_ms, message) pair.
    """
    out = []
    for seq, item in enumerate(messages, 1):
        received, message = item if isinstance(item, tuple) else (None, item)
        if received is None:
            key = message_key("kalshi" if "type" in message else "polymarket", message)
            received = (key.venue_ts_ms or key.sent_ms) + lag_ms
        out.append(
            Occurrence(copy, connection, session, 1, seq, None, received * 1000, message)
        )
    return out


def merge(streams, venue="polymarket", **options):
    merger = CopyMerge(venue, **options)
    return list(merger.merge(streams)), merger.stats


def contents(merged):
    return [m.message for m in merged]


# -- rule 1: union by content ------------------------------------------------------


def test_identical_copies_give_each_message_once_with_both_copies():
    messages = [change(T0, "0.40", "10"), change(T0 + 1, "0.41", "5"), book(T0 + 2, [("0.41", "5")])]
    merged, stats = merge([stream("pc", messages), stream("vm1", messages, lag_ms=45)])
    assert contents(merged) == messages
    assert all(m.copies == ("pc", "vm1") for m in merged)
    # The earlier receive time wins, and only that copy's origin is "first".
    assert [m.first_copy for m in merged] == ["pc", "pc", "pc"]
    assert merged[0].first_received_us == (T0 + 20) * 1000
    assert len(merged[0].occurrences) == 2
    assert stats.messages == 3 and stats.repeats_merged == 3
    assert stats.order_conflicts == stats.ambiguous_choices == 0


def test_hole_in_one_copy_is_filled_from_another():
    messages = [change(T0 + i * 1000, f"0.{40 + i}", "1") for i in range(6)]
    with_hole = messages[:2] + messages[4:]
    merged, _ = merge([stream("pc", with_hole), stream("vm1", messages)])
    assert contents(merged) == messages
    assert [m.copies for m in merged] == [("pc", "vm1")] * 2 + [("vm1",)] * 2 + [("pc", "vm1")] * 2


def test_exact_repeats_keep_their_multiplicity():
    tick = {"market": "M", "asset_id": "yes", "old_tick_size": "0.01",
            "new_tick_size": "0.001", "timestamp": str(T0), "event_type": "tick_size_change"}
    x = change(T0 + 5, "0.40", "10")
    # pc received the tick-size change twice (the venue sends it twice) and x
    # twice; vm1 lost one of each. The merge keeps the maximum: two of each.
    merged, stats = merge([stream("pc", [tick, tick, x, x]), stream("vm1", [tick, x])])
    assert contents(merged) == [tick, tick, x, x]
    assert [m.copies for m in merged] == [("pc", "vm1"), ("pc",), ("pc", "vm1"), ("pc",)]
    assert stats.order_conflicts == 0


def test_same_market_on_two_connections_of_one_copy_is_kept_once():
    # The Yes-token and No-token connections both receive the market's changes.
    messages = [change(T0, "0.40", "10"), change(T0 + 3, "0.60", "4", token="no", side="SELL")]
    merged, _ = merge(
        [
            stream("vm1", messages, connection="polymarket_00"),
            stream("vm1", messages, connection="polymarket_02", lag_ms=21),
        ]
    )
    assert contents(merged) == messages
    assert [m.copies for m in merged] == [("vm1",), ("vm1",)]
    assert [{o.connection for o in m.occurrences} for m in merged] == [
        {"polymarket_00", "polymarket_02"}
    ] * 2


def test_kalshi_identity_ignores_per_connection_fields():
    a = [delta(T0, "0.40", "5.00", sid=1, seq=10, sent=T0 + 4), delta(T0 + 7, "0.41", "-2.00", sid=1, seq=11)]
    b = [delta(T0, "0.40", "5.00", sid=3, seq=900, sent=T0 + 9), delta(T0 + 7, "0.41", "-2.00", sid=3, seq=901)]
    merged, stats = merge(
        [stream("pc", a, connection="kalshi_00"), stream("vm1", b, connection="kalshi_00", lag_ms=10)],
        venue="kalshi",
    )
    assert [m.copies for m in merged] == [("pc", "vm1")] * 2
    # The output keeps the envelope of the earliest-received occurrence.
    assert merged[0].message["seq"] == 900 and merged[0].first_copy == "vm1"
    assert stats.messages == 2
    # A different inner message is a different message.
    other = delta(T0, "0.40", "6.00")
    assert message_key("kalshi", other).digest != message_key("kalshi", a[0]).digest


# -- rules 2 and 3: order ----------------------------------------------------------


def test_order_is_by_venue_time_not_receive_time():
    early, late = change(T0, "0.40", "1", market="A"), change(T0 + 50, "0.40", "2", market="B")
    # Received in the opposite order (B arrived first).
    merged, _ = merge([stream("pc", [(T0 + 100, late), (T0 + 120, early)])])
    assert contents(merged) == [early, late]
    assert [m.order_ms for m in merged] == [T0, T0 + 50]


def test_shared_timestamp_keeps_received_order_complete_in_one_copy_partial_in_other():
    # Four changes in one millisecond; 1 and 3 set the same level to different
    # sizes, so their order decides the book. pc has the whole group, vm1 lost
    # the first message, vm3 lost the middle, vm4 the last.
    group = [
        change(T0, "0.40", "10"),
        change(T0, "0.39", "7"),
        change(T0, "0.40", "0"),
        change(T0, "0.38", "3"),
    ]
    partials = {"vm1": group[1:], "vm3": [group[0], group[1], group[3]], "vm4": group[:3]}
    streams = [stream("pc", group)] + [stream(copy, part) for copy, part in partials.items()]
    merged, stats = merge(streams)
    assert contents(merged) == group
    assert stats.order_conflicts == stats.ambiguous_choices == 0
    assert merged[0].copies == ("pc", "vm3", "vm4")


def test_group_split_between_copies_is_joined_in_order():
    group = [change(T0, "0.40", str(size)) for size in (1, 2, 3, 4)]
    # Neither copy holds the whole group: pc has 1,2,3 and vm1 has 2,3,4.
    merged, stats = merge([stream("pc", group[:3]), stream("vm1", group[1:])])
    assert contents(merged) == group
    assert stats.ambiguous_choices == 0


def test_order_disagreement_inside_a_timestamp_is_counted_and_base_order_wins():
    x, y, z = (change(T0, "0.40", size) for size in ("1", "2", "3"))
    merged, stats = merge([stream("pc", [x, y, z]), stream("vm1", [y, x])])
    # pc holds more of the group, so its order is the base.
    assert contents(merged) == [x, y, z]
    assert stats.order_conflicts == 1
    assert stats.conflict_examples[0]["copy"] == "vm1"
    # vm1's x is still the same message; only its order constraint is ignored.
    assert merged[0].copies == ("pc", "vm1")
    with pytest.raises(CopyMergeError, match="disagree"):
        merge([stream("pc", [x, y, z]), stream("vm1", [y, x])], strict_order=True)


def test_messages_each_copy_lacks_in_one_gap_are_ordered_by_convention():
    a, b, c, d = (change(T0, "0.40", size) for size in ("1", "2", "3", "4"))
    # pc: a b d, vm1: a c d. The order of b and c is unknown; base first.
    merged, stats = merge([stream("pc", [a, b, d]), stream("vm1", [a, c, d])])
    assert contents(merged) == [a, b, c, d]
    assert stats.ambiguous_choices == 1 and stats.order_conflicts == 0


def test_order_evidence_from_all_streams_is_combined():
    a, b, c, d, e = (change(T0, "0.40", size) for size in "12345")
    # Each stream is compatible with a c b d e; no stream holds all of it.
    streams = [stream("pc", [a, b, d, e]), stream("vm1", [a, c, d]), stream("vm3", [a, c, b])]
    merged, stats = merge(streams, strict_order=True)
    assert contents(merged) == [a, c, b, d, e]
    assert stats.order_conflicts == 0


def test_a_rejected_order_constraint_does_not_invent_another():
    a, b, c, d, e = (change(T0, "0.40", size) for size in "12345")
    # vm1's b -> a contradicts pc; skipping it must leave vm1's a -> c, not
    # create b -> c (which would contradict vm3's c -> b).
    streams = [stream("pc", [a, b, d, e]), stream("vm1", [b, a, c]), stream("vm3", [a, c, b])]
    merged, stats = merge(streams)
    assert contents(merged) == [a, c, b, d, e]
    assert stats.order_conflicts == 1


def test_repeats_match_by_occurrence_number_with_or_without_other_content():
    x, y, z = change(T0, "0.40", "1"), change(T0, "0.40", "2"), change(T0, "0.40", "3")
    for extra in ([], [stream("vm3", [z])]):
        # vm1's x is the first x of the group, so its order contradicts pc's
        # whether or not an unrelated stream is present.
        merged, stats = merge([stream("pc", [x, y, x]), stream("vm1", [y, x])] + extra)
        assert contents(merged)[:3] == [x, y, x]
        assert stats.order_conflicts == 1
        assert [m.copies for m in merged[:3]] == [("pc", "vm1"), ("pc", "vm1"), ("pc",)]


def test_untimed_and_timed_messages_of_one_millisecond_are_ordered_together():
    # Kalshi snapshots without ts_ms are placed at sending_ts_ms. Here a
    # snapshot at T is followed by a delta stamped T: the received order holds.
    snap = {"type": "orderbook_snapshot", "sid": 1, "seq": 1, "sending_ts_ms": T0,
            "msg": {"market_ticker": "K", "yes_dollars_fp": [["0.40", "5.00"]], "no_dollars_fp": []}}
    later = {**snap, "seq": 3, "sending_ts_ms": T0 + 1,
             "msg": {**snap["msg"], "yes_dollars_fp": [["0.40", "3.00"]]}}
    change_ = delta(T0, "0.40", "-2.00", seq=2)
    merged, _ = merge(
        [stream("pc", [(T0 + 5, snap), (T0 + 6, change_), (T0 + 7, later)], connection="kalshi_00")],
        venue="kalshi",
    )
    assert contents(merged) == [snap, change_, later]
    assert merged[0].venue_ts_ms is None and merged[1].venue_ts_ms == T0
    result = check_merged_books("kalshi", merged)
    assert (result.compared, result.equal) == (1, 1)


def test_snapshot_follows_a_change_of_its_timestamp_held_only_by_another_copy():
    # Copy "a" reconnected and holds only the snapshot (size 3, including the
    # delta); copy "m" holds only the delta 5 -> 3. Whatever the copy names, the
    # checkpoint comes after the change it includes.
    first = {"type": "orderbook_snapshot", "sid": 1, "seq": 1, "sending_ts_ms": T0 - 50,
             "msg": {"market_ticker": "K", "yes_dollars_fp": [["0.40", "5.00"]], "no_dollars_fp": [], "ts_ms": T0 - 50}}
    change_ = delta(T0, "0.40", "-2.00", seq=2)
    snapshot = {**first, "seq": 1, "msg": {**first["msg"], "yes_dollars_fp": [["0.40", "3.00"]], "ts_ms": T0}}
    merged, _ = merge(
        [
            stream("a", [snapshot], connection="kalshi_00"),
            stream("m", [first, change_], connection="kalshi_00"),
        ],
        venue="kalshi",
    )
    assert contents(merged) == [first, change_, snapshot]
    result = check_merged_books("kalshi", merged)
    assert (result.compared, result.equal) == (1, 1)


def test_input_order_and_scope_split_do_not_change_the_output():
    def copy_streams():
        pc = [change(T0, "0.40", "1", market="A"), change(T0, "0.50", "1", market="B"),
              change(T0, "0.41", "2", market="A"), book(T0 + 3, [("0.41", "2")], market="A")]
        vm1 = [change(T0, "0.50", "1", market="B"), change(T0, "0.40", "1", market="A"),
               change(T0 + 9, "0.51", "6", market="B")]
        vm3 = [change(T0, "0.41", "2", market="A"), change(T0 + 9, "0.51", "6", market="B")]
        return {"pc": pc, "vm1": vm1, "vm3": vm3}

    def run(order, keep=None):
        data = copy_streams()
        streams = [
            stream(copy, [m for m in data[copy] if keep is None or m["market"] == keep])
            for copy in order
        ]
        return [(m.content_digest, m.copies, m.order_ms) for m in merge(streams)[0]]

    reference = run(["pc", "vm1", "vm3"])
    for order in itertools.permutations(["pc", "vm1", "vm3"]):
        assert run(list(order)) == reference
    # Cross-market order inside a millisecond is by scope (A before B), so
    # merging each market alone and interleaving by (order_ms, scope) agrees
    # (no message here is late).
    split = sorted(run(["pc", "vm1", "vm3"], "A") + run(["pc", "vm1", "vm3"], "B"),
                   key=lambda row: row[2])
    assert sorted(reference, key=lambda row: row[2]) == split


# -- rule 4: reconnect snapshots and late messages --------------------------------


def test_reconnect_snapshot_is_placed_at_its_own_timestamp():
    before = change(T0, "0.40", "10")
    after = change(T0 + 1_000, "0.39", "5")
    snapshot = book(T0, [("0.40", "10")])
    # vm1 reconnects; the venue's snapshot carries the time of the last change
    # (T0) and arrives 3 s later, after pc already received ``after``.
    pc = stream("pc", [before, after])
    vm1 = stream("vm1", [(T0 + 3_000, snapshot)])
    vm1 = [Occurrence(**{**_fields(o), "reconnect_snapshot": True}) for o in vm1]
    merged, stats = merge([pc, vm1])
    assert contents(merged) == [before, snapshot, after]
    assert merged[1].checkpoint and merged[1].reconnect_snapshot and not merged[1].late
    assert stats.late_messages == 0


def _fields(occurrence):
    return {name: getattr(occurrence, name) for name in Occurrence.__slots__}


def test_snapshot_older_than_lateness_is_flagged_late_and_kept_once():
    quiet = book(T0, [("0.40", "10")])
    later = change(T0 + 200_000, "0.39", "5")
    # Both copies reconnect long after the token last changed (T0): each gets
    # the same snapshot, 150 s and 170 s after its timestamp.
    merged, stats = merge(
        [
            stream("pc", [(T0 + 150_000, quiet), later]),
            stream("vm1", [(T0 + 170_000, quiet)]),
        ],
        lateness_ms=60_000,
    )
    assert contents(merged) == [quiet, later]
    first = merged[0]
    assert first.late and first.venue_ts_ms == T0
    # Placed where the merge could still place it: receive time minus lateness.
    assert first.order_ms == T0 + 150_000 - 60_000
    assert stats.late_messages == 1 and stats.repeats_after_emit == 1
    assert stats.late_by_copy == {"pc": 1}


def test_untimed_kalshi_messages_match_by_content_and_sending_time():
    def lifecycle(sid, seq, sent):
        return {"type": "market_lifecycle_v2", "sid": sid, "seq": seq,
                "msg": {"market_ticker": "K", "event_type": "settled", "settled_ts": 1},
                "sending_ts_ms": sent}

    pc = stream("pc", [lifecycle(3, 1, T0), lifecycle(3, 2, T0 + 300)], connection="kalshi_00")
    vm1 = stream("vm1", [lifecycle(5, 7, T0 + 2)], connection="kalshi_00")
    merged, stats = merge([pc, vm1], venue="kalshi")
    # pc received it twice (a venue repeat), vm1 once: kept twice.
    assert [m.copies for m in merged] == [("pc", "vm1"), ("pc",)]
    assert [m.order_ms for m in merged] == [T0, T0 + 300]
    assert all(m.venue_ts_ms is None for m in merged)
    assert stats.untimed_messages == 2


def lifecycle(sid, seq, sent, event="settled"):
    return {"type": "market_lifecycle_v2", "sid": sid, "seq": seq,
            "msg": {"market_ticker": "K", "event_type": event}, "sending_ts_ms": sent}


def test_untimed_messages_at_one_time_keep_received_order():
    x, y = lifecycle(3, 1, T0, "determined"), lifecycle(3, 2, T0, "settled")
    pc = stream("pc", [(T0 + 10, x), (T0 + 11, y)], connection="kalshi_00")
    # vm1 lost x and received y earlier than pc received x.
    vm1 = stream("vm1", [(T0 + 1, {**y, "sid": 9, "seq": 4})], connection="kalshi_00")
    merged, stats = merge([pc, vm1], venue="kalshi")
    assert [m.message["msg"]["event_type"] for m in merged] == ["determined", "settled"]
    assert merged[1].copies == ("pc", "vm1") and stats.order_conflicts == 0


def test_untimed_repeat_after_its_first_copy_was_emitted_is_kept():
    # pc's x was emitted before vm1's two occurrences arrived; vm1's first one
    # is the same message, its second is a genuine repeat.
    pc = stream("pc", [(T0, lifecycle(3, 1, T0))], connection="kalshi_00")
    vm1 = stream("vm1", [(T0 + 21, lifecycle(5, 1, T0 + 20)), (T0 + 22, lifecycle(5, 2, T0 + 21))],
                 connection="kalshi_00")
    merged, stats = merge([pc, vm1], venue="kalshi", lateness_ms=10)
    assert len(merged) == 2 and stats.repeats_after_emit == 1


def test_late_untimed_copies_match_by_sending_time():
    pc = stream("pc", [(T0 + 150_000, lifecycle(3, 1, T0))], connection="kalshi_00")
    vm1 = stream("vm1", [(T0 + 150_010, lifecycle(5, 1, T0 + 2))], connection="kalshi_00")
    merged, stats = merge([pc, vm1], venue="kalshi")
    assert len(merged) == 1 and merged[0].late and stats.repeats_after_emit == 1


def test_future_timestamps_are_handled_like_late_messages():
    # pc's clock runs far behind: every message looks an hour in the future.
    ahead = [change(T0 + 3_600_000 + i, "0.40", str(i)) for i in range(50)]
    pc = stream("pc", [(T0 + i, m) for i, m in enumerate(ahead)])
    vm1 = stream("vm1", ahead[:5])  # received on time, an hour later
    merged, stats = merge([pc, vm1], lateness_ms=1_000)
    assert contents(merged) == ahead
    assert all(m.late for m in merged) and stats.future_messages == 50
    assert stats.max_pending_occurrences == 0 and stats.repeats_after_emit == 5


# -- coverage ---------------------------------------------------------------------


def _utc(ms):
    return (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=ms)).isoformat()


def record(root, monkeypatch, venue, frames, *, seal_after=()):
    """Write a session with ChunkWriter; frames are (ms, frame_type, payload, generation, direction)."""
    root.mkdir(parents=True)
    writer = ChunkWriter(root, "rec-" + root.name, venue, root.parent.name)
    times = iter([_utc(ms) for ms, *_ in frames])
    monkeypatch.setattr(raw_capture, "utc", lambda: next(times))
    for index, (_, frame_type, payload, generation, direction) in enumerate(frames):
        writer.write(frame_type, payload, generation, direction)
        if index in seal_after:
            writer.seal()
    writer.seal()


def text(ms, message, generation=1):
    return (ms, "text", json.dumps(message, separators=(",", ":")), generation, "received")


def test_recordings_with_a_hole_in_every_copy_report_uncovered_time(tmp_path, monkeypatch):
    sub = '{"assets_ids":["yes"],"type":"market"}'
    c = [change(T0 + i * 1_000, "0.40", str(i + 1)) for i in range(10)]
    snap = book(T0 + 6_500, [("0.40", "7")])
    # pc: connected 0-4 s, transport error, reconnects at 6.5 s (snapshot).
    record(tmp_path / "pc" / "polymarket_00" / "20261103T000000Z_aaaa0001", monkeypatch, "polymarket", [
        (T0 - 100, "subscription", sub, 1, "sent"),
        *[text(T0 + i * 1_000 + 20, c[i]) for i in range(5)],
        (T0 + 4_500, "transport_error", "ConnectionClosed", 1, "local"),
        (T0 + 6_000, "subscription", sub, 2, "sent"),
        text(T0 + 6_600, snap, 2),
        *[text(T0 + i * 1_000 + 20, c[i], 2) for i in range(7, 10)],
    ])
    # vm1: chunk 2 (c[3]..c[6]) is lost.
    vm1 = tmp_path / "vm1" / "polymarket_00" / "20261103T000000Z_bbbb0001"
    record(vm1, monkeypatch, "polymarket", [
        (T0 - 100, "subscription", sub, 1, "sent"),
        *[text(T0 + i * 1_000 + 40, c[i]) for i in range(10)],
    ], seal_after=(3, 7))
    (vm1 / "chunk_000002.jsonl.gz").unlink()
    (vm1 / "chunk_000002.jsonl.gz.manifest.json").unlink()

    coverage = []
    found = discover_sessions({"vm1": tmp_path / "vm1", "pc": tmp_path / "pc"})
    streams = [
        connection_occurrences(copy, connection, sessions, coverage=coverage)
        for (copy, connection), sessions in found.items()
    ]
    merged, stats = merge(streams)
    # c[5] and c[6] were sent while neither copy was connected.
    assert contents(merged) == c[:5] + [snap] + c[7:]
    assert merged[5].reconnect_snapshot and merged[5].copies == ("pc",)
    assert [m.copies for m in merged[:5]] == [("pc", "vm1")] * 3 + [("pc",)] * 2

    by_copy = {s.copy: s for s in coverage}
    us = 1_000
    assert [(i.start_us, i.end_us, i.end_reason) for i in by_copy["pc"].intervals] == [
        ((T0 + 20) * us, (T0 + 4_020) * us, "transport_error"),
        ((T0 + 6_600) * us, (T0 + 9_020) * us, "session_end"),
    ]
    assert [(i.start_us, i.end_us, i.end_reason, i.start_reason) for i in by_copy["vm1"].intervals] == [
        ((T0 + 40) * us, (T0 + 2_040) * us, "chunk_gap", "session_start"),
        ((T0 + 7_040) * us, (T0 + 9_040) * us, "session_end", "after_gap"),
    ]
    assert by_copy["vm1"].chunk_gaps == 1 and by_copy["pc"].transport_errors == 1
    holes = uncovered_intervals(
        [i for s in coverage for i in s.intervals], T0 * us, (T0 + 10_000) * us
    )
    assert holes == [
        (T0 * us, (T0 + 20) * us),
        ((T0 + 4_020) * us, (T0 + 6_600) * us),
        ((T0 + 9_040) * us, (T0 + 10_000) * us),
    ]


def test_empty_list_frame_still_extends_coverage(tmp_path, monkeypatch):
    session = tmp_path / "pc" / "polymarket_00" / "20261103T000000Z_cccc0001"
    record(session, monkeypatch, "polymarket", [text(T0, change(T0, "0.40", "1")), (T0 + 1_000, "text", "[]", 1, "received")])
    coverage = []
    occurrences = list(connection_occurrences("pc", "polymarket_00", [session], coverage=coverage))
    assert len(occurrences) == 1
    assert [(i.start_us, i.end_us) for i in coverage[0].intervals] == [(T0 * 1_000, (T0 + 1_000) * 1_000)]


def test_kalshi_sequence_gap_splits_coverage():
    tracker = CoverageTracker("pc", "kalshi_00", "s1")
    for ms, seq in ((0, 1), (10, 2), (20, 5), (30, 6)):
        tracker.frame(ms * 1_000, 1, "text", "received")
        tracker.sequence(1, seq)
    tracker.frame(40_000, 2, "text", "received")
    tracker.sequence(1, 1)  # new generation restarts the counter
    result = tracker.finish()
    assert [(i.start_us, i.end_us, i.end_reason) for i in result.intervals] == [
        (0, 10_000, "seq_gap"),
        (20_000, 30_000, "new_generation"),
        (40_000, 40_000, "session_end"),
    ]
    assert result.seq_gaps == 1


def test_utc_microseconds_is_exact():
    assert utc_microseconds("2026-11-03T02:34:43.273921+00:00") == 1_793_673_283_273_921


# -- book check -------------------------------------------------------------------


def test_book_check_rebuilds_books_and_compares_snapshots():
    messages = [
        book(T0, [("0.40", "10")], [("0.60", "5")]),
        change(T0 + 1, "0.40", "0"),
        change(T0 + 1, "0.41", "3"),
        book(T0 + 1, [("0.41", "3")], [("0.60", "5")]),
        book(T0 + 2, [("0.41", "4")], [("0.60", "5")]),  # wrong on purpose
    ]
    merged, _ = merge([stream("pc", messages)])
    result = check_merged_books("polymarket", merged)
    assert (result.full_books, result.compared, result.equal) == (3, 2, 1)
    assert result.mismatch_examples[0]["differing_levels"] == 1

    snapshot = {"type": "orderbook_snapshot", "sid": 1, "seq": 1, "sending_ts_ms": T0,
                "msg": {"market_ticker": "K", "yes_dollars_fp": [["0.40", "5.00"]], "no_dollars_fp": [], "ts_ms": T0}}
    again = {**snapshot, "seq": 3, "msg": {**snapshot["msg"], "yes_dollars_fp": [["0.40", "3.00"]], "ts_ms": T0 + 9}}
    merged, _ = merge(
        [stream("pc", [snapshot, delta(T0 + 5, "0.40", "-2.00", seq=2), again], connection="kalshi_00")],
        venue="kalshi",
    )
    result = check_merged_books("kalshi", merged)
    assert (result.full_books, result.compared, result.equal) == (2, 1, 1)


def test_streams_must_be_unique_and_consistent():
    x = change(T0, "0.40", "1")
    with pytest.raises(CopyMergeError, match="two input streams"):
        merge([stream("pc", [x]), stream("pc", [x])])
    mixed = stream("pc", [x]) + stream("vm1", [change(T0 + 1, "0.40", "2")])
    with pytest.raises(CopyMergeError, match="yielded an occurrence"):
        merge([mixed])


# -- superseded untimed placements --------------------------------------------------


def test_untimed_message_moved_earlier_leaves_no_empty_group():
    # The copy that sent it earlier received it later: the pending message moves
    # to the earlier sending time, and its first placement must be skipped.
    x = lifecycle(3, 1, T0 + 100, "created")
    merged, stats = merge(
        [
            stream("a", [(T0 + 110, x)], connection="kalshi_00"),
            stream("b", [(T0 + 120, {**x, "sid": 5, "sending_ts_ms": T0 + 90})], connection="kalshi_00"),
        ],
        venue="kalshi",
    )
    assert len(merged) == 1 and merged[0].order_ms == T0 + 90
    assert merged[0].copies == ("a", "b") and stats.untimed_messages == 1


def test_untimed_message_moved_earlier_keeps_its_neighbours():
    # Other messages share both the old and the new (millisecond, market).
    x = lifecycle(3, 1, T0 + 100, "created")
    y = lifecycle(3, 2, T0 + 100, "activated")
    n = lifecycle(5, 1, T0 + 90, "deactivated")
    trade = {"type": "trade", "sid": 2, "seq": 1, "sending_ts_ms": T0 + 101,
             "msg": {"market_ticker": "K", "yes_price_dollars": "0.40", "count_fp": "1.00",
                     "taker_side": "yes", "ts_ms": T0 + 100}}
    a = stream("a", [(T0 + 110, x), (T0 + 111, trade), (T0 + 112, y)], connection="kalshi_00")
    b = stream("b", [(T0 + 115, n), (T0 + 120, {**x, "sid": 5, "seq": 2, "sending_ts_ms": T0 + 90})],
               connection="kalshi_00")
    merged, stats = merge([a, b], venue="kalshi")
    assert [(m.order_ms - T0, m.message["msg"].get("event_type", "trade")) for m in merged] == [
        (90, "deactivated"), (90, "created"), (100, "trade"), (100, "activated"),
    ]
    assert merged[1].copies == ("a", "b") and stats.messages == 4


# -- Kalshi snapshots by clock ------------------------------------------------------


def ksnap(sent, levels, clock, *, seq=1, sid=1, ticker="K"):
    """A Kalshi snapshot as recorded: no ts_ms, only the sending time and clock."""
    body = {"market_ticker": ticker, "yes_dollars_fp": [[p, s] for p, s in levels]}
    if clock is not None:
        body["clock"] = clock
    return {"type": "orderbook_snapshot", "sid": sid, "seq": seq, "sending_ts_ms": sent, "msg": body}


def test_snapshot_comes_before_an_earlier_stamped_delta_with_a_higher_clock():
    # One connection: snapshot (clock 10, sent T+110), then a delta stamped
    # T+105 with clock 11 (not in the snapshot), then a snapshot with clock 11.
    first, change_ = ksnap(T0 + 110, [("0.50", "5")], 10), delta(T0 + 105, "0.50", "3", seq=2, clock=11, sent=T0 + 120)
    second = ksnap(T0 + 200, [("0.50", "8")], 11, seq=3)
    merged, stats = merge(
        [stream("a", [(T0 + 111, first), (T0 + 121, change_), (T0 + 201, second)], connection="kalshi_00")],
        venue="kalshi",
    )
    assert contents(merged) == [first, change_, second]
    assert [m.order_ms - T0 for m in merged] == [105, 105, 200]
    assert stats.snapshots_by_clock == 2 and stats.clock_regressions == 0
    assert (check_merged_books("kalshi", merged).compared, check_merged_books("kalshi", merged).equal) == (1, 1)


def test_reconnect_snapshot_of_one_copy_sits_between_the_deltas_of_another_by_clock():
    deltas = [
        delta(T0 + 100, "0.40", "5", seq=1, clock=5),
        delta(T0 + 105, "0.40", "2", seq=2, clock=6),
        delta(T0 + 105, "0.41", "1", seq=3, clock=7),
        delta(T0 + 108, "0.40", "-1", seq=4, clock=8),
    ]
    # Copy b reconnects: its snapshot includes clock 6 and is sent at T+110,
    # after the deltas with clocks 7 and 8, which it received after it.
    snap = ksnap(T0 + 110, [("0.40", "7")], 6)
    a = stream("a", deltas, connection="kalshi_00")
    b = stream("b", [(T0 + 111, snap), (T0 + 115, {**deltas[2], "sid": 4}), (T0 + 116, {**deltas[3], "sid": 4})],
               connection="kalshi_00")
    b = [Occurrence(**{**_fields(o), "reconnect_snapshot": o.message is snap}) for o in b]
    merged, stats = merge([a, b], venue="kalshi")
    assert [m.message["msg"] for m in merged] == [m["msg"] for m in deltas[:2] + [snap] + deltas[2:]]
    assert merged[2].order_ms == T0 + 105 and merged[2].snapshot_class == "reconnect"
    assert stats.snapshots_by_clock == 1 and stats.order_conflicts == 0
    # Without its clock the same snapshot would sit at its sending time.
    unclocked = {**snap, "msg": {k: v for k, v in snap["msg"].items() if k != "clock"}}
    b2 = [Occurrence(**{**_fields(o), "message": unclocked}) if o.message is snap else o for o in b]
    merged, stats = merge([a, b2], venue="kalshi")
    assert [m.message["msg"] for m in merged] == [m["msg"] for m in deltas + [unclocked]]
    assert stats.snapshot_clock_fallbacks == {"missing": 1}


def test_snapshot_clock_fallbacks_are_counted():
    # Clock 0: an empty book of a market without changes, placed at sending time.
    empty = ksnap(T0 + 50, [], 0)
    later = delta(T0 + 10, "0.40", "1", clock=3)
    merged, stats = merge([stream("a", [(T0 + 60, empty), later], connection="kalshi_00")], venue="kalshi")
    assert contents(merged) == [later, empty]
    assert stats.snapshot_clock_fallbacks == {"zero": 1} and stats.snapshots_by_clock == 0
    assert merged[1].snapshot_class == "empty"

    # A clock below the last emitted delta of the market (e.g. maintenance).
    old = delta(T0, "0.40", "1", clock=50)
    snap = ksnap(T0 + 2_000, [("0.40", "1")], 3)
    after = delta(T0 + 2_005, "0.40", "1", clock=4, seq=2)
    merged, stats = merge(
        [stream("a", [old, (T0 + 2_001, snap), after], connection="kalshi_00")],
        venue="kalshi", lateness_ms=1_000,
    )
    assert contents(merged) == [old, snap, after]
    assert stats.snapshot_clock_fallbacks == {"decreased": 1} and stats.clock_regressions == 1


def test_delta_included_in_a_snapshot_but_stamped_after_it_is_counted():
    snap = ksnap(T0 + 110, [("0.40", "1")], 6)
    included = delta(T0 + 115, "0.40", "1", clock=6)
    merged, stats = merge([stream("a", [(T0 + 111, snap), included], connection="kalshi_00")], venue="kalshi")
    assert contents(merged) == [snap, included]
    assert stats.clock_regressions == 1


def test_clock_placement_does_not_depend_on_other_markets():
    # With a 10 ms lateness window, J's messages move the flush point past K's
    # delta before K's snapshot arrives; merging K alone must give the same.
    k = [(T0 + 105, delta(T0 + 100, "0.40", "1", clock=7)),
         (T0 + 113, ksnap(T0 + 103, [("0.40", "1")], 6, seq=2)),
         (T0 + 125, delta(T0 + 120, "0.40", "1", clock=8, seq=3))]
    j = [(T0 + 111, delta(T0 + 101, "0.30", "1", clock=1, ticker="J")),
         (T0 + 112, ksnap(T0 + 112, [("0.30", "1")], 1, ticker="J", seq=2)),
         (T0 + 114, delta(T0 + 110, "0.30", "1", clock=2, ticker="J", seq=3))]

    def rows(*tickers):
        items = sorted((item for t in tickers for item in {"K": k, "J": j}[t]), key=lambda i: i[0])
        merged, stats = merge([stream("a", items, connection="kalshi_00")], venue="kalshi", lateness_ms=10)
        return [(m.scope, m.kind, m.order_ms - T0) for m in merged], stats

    together, stats = rows("K", "J")
    assert [r for r in together if r[0] == "J"] == [
        ("J", "orderbook_delta", 101), ("J", "orderbook_snapshot", 110), ("J", "orderbook_delta", 110)]
    assert stats.snapshot_clock_fallbacks == {"decreased": 1} and stats.snapshots_by_clock == 1
    for ticker in ("K", "J"):
        assert [r for r in together if r[0] == ticker] == rows(ticker)[0]


def test_kalshi_reconnect_snapshots_are_the_first_per_ticker_after_subscribing(tmp_path, monkeypatch):
    sub = '{"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":["K","J"],"use_yes_price":true}}'
    session = tmp_path / "pc" / "kalshi_00" / "20261103T000000Z_dddd0001"
    record(session, monkeypatch, "kalshi", [
        (T0, "subscription", sub, 1, "sent"),
        text(T0 + 10, ksnap(T0 + 9, [("0.40", "1")], 4)),
        text(T0 + 11, ksnap(T0 + 10, [("0.30", "1")], 2, ticker="J", seq=2)),
        text(T0 + 20, ksnap(T0 + 19, [("0.40", "2")], 5, seq=3)),
        text(T0 + 30, ksnap(T0 + 29, [], 0, seq=4)),
        (T0 + 40, "subscription", sub, 1, "sent"),
        text(T0 + 50, ksnap(T0 + 49, [("0.40", "2")], 5, seq=5)),
    ])
    occurrences = list(connection_occurrences("pc", "kalshi_00", [session]))
    assert [o.reconnect_snapshot for o in occurrences] == [True, True, False, False, True]
    merged, _ = merge([occurrences], venue="kalshi")
    assert [m.snapshot_class for m in merged] == ["reconnect", "reconnect", "later", "empty", "reconnect"]

    conflated = tmp_path / "pc" / "kalshi_01" / "20261103T000000Z_eeee0001"
    record(conflated, monkeypatch, "kalshi", [
        (T0, "subscription", sub.replace('"use_yes_price"', '"update_interval_ms":500,"use_yes_price"'), 1, "sent"),
    ])
    with pytest.raises(CopyMergeError, match="conflated"):
        list(connection_occurrences("pc", "kalshi_01", [conflated]))


# -- book-change order before trade placement ---------------------------------------


def test_trade_constraints_cannot_overturn_the_order_of_book_changes():
    x, y = change(T0, "0.50", "10", side="SELL"), change(T0, "0.50", "20", side="SELL")
    trade = {"event_type": "last_trade_price", "market": "M", "asset_id": "yes", "price": "0.5",
             "size": "1", "side": "BUY", "timestamp": str(T0)}
    # a: x, trade; b: trade, y; together they imply x before y, but c received
    # y before x, the only direct evidence on the two book changes.
    merged, stats = merge([stream("a", [x, trade]), stream("b", [trade, y]), stream("c", [y, x])])
    assert [m.message.get("price_changes", [{}])[0].get("size", "trade") for m in merged] == ["20", "10", "trade"]
    assert (stats.book_order_conflicts, stats.other_order_conflicts, stats.order_conflicts) == (0, 1, 1)
    assert stats.conflict_examples[0]["book_changes"] is False


def test_disagreeing_book_changes_are_counted_as_book_conflicts():
    x, y = change(T0, "0.40", "1"), change(T0, "0.40", "2")
    _, stats = merge([stream("a", [x, y]), stream("b", [y, x])])
    assert (stats.book_order_conflicts, stats.other_order_conflicts) == (1, 0)


# -- caller preference among copies -------------------------------------------------


def test_preferred_copies_decide_remaining_disagreements_and_free_choices():
    x = change(T0, "0.40", "1")
    trade = {"event_type": "last_trade_price", "market": "M", "asset_id": "yes", "price": "0.4",
             "size": "1", "side": "BUY", "timestamp": str(T0)}
    streams = [stream("a", [x, trade]), stream("b", [trade, x])]

    def kinds(**options):
        return [m.kind for m in merge(streams, **options)[0]]

    assert kinds() == ["price_change", "last_trade_price"]  # name tie-break: a
    assert kinds(preferred_copies=("b", "a")) == ["last_trade_price", "price_change"]
    assert kinds(preferred_copies=("b",)) == ["last_trade_price", "price_change"]
    # A stream holding more of the group still ranks first.
    y = change(T0, "0.41", "1")
    fuller = [stream("a", [x, trade, y]), stream("b", [trade, x])]
    assert [m.kind for m in merge(fuller, preferred_copies=("b",))[0]][:2] == ["price_change", "last_trade_price"]
    # Book-change order is evidence, not preference.
    z = change(T0, "0.40", "2")
    book_streams = [stream("a", [x, z]), stream("b", [z, x])]
    assert contents(merge(book_streams, preferred_copies=("b",))[0]) == [z, x]
    with pytest.raises(ValueError, match="distinct"):
        CopyMerge("polymarket", preferred_copies=("a", "a"))
    with pytest.raises(ValueError, match="distinct"):
        CopyMerge("polymarket", preferred_copies="ab")
