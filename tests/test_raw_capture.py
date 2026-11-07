"""Offline synthetic checks against local websocket servers; never touches a venue or credentials."""

import asyncio
import base64
import contextlib
import gzip
import hashlib
import itertools
import json
import logging
import os
import signal
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from pmkt.streaming import raw_capture as rr

PM_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
K_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"


def market_list(tmp_path, venue, ids, name=None):
    name = name or f"{venue}_00"
    path = tmp_path / "lists" / f"{name}.json"
    path.parent.mkdir(exist_ok=True)
    path.write_text(
        json.dumps(
            dict(
                venue=venue,
                name=name,
                ws_url=PM_URL if venue == "polymarket" else K_URL,
                instruments=ids,
            )
        )
    )
    return path


def frames(session):
    out = []
    for chunk in sorted(session.glob("chunk_*.jsonl.gz")):
        out += [
            json.loads(line)
            for line in gzip.decompress(chunk.read_bytes()).splitlines()
        ]
    return out


def manifests(session):
    return [json.loads(p.read_text()) for p in sorted(session.glob("*.manifest.json"))]


def end_in(seconds):
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


@contextlib.asynccontextmanager
async def local_server(handler):
    async with serve(handler, "127.0.0.1", 0) as server:
        yield "ws://127.0.0.1:%d" % server.sockets[0].getsockname()[1]


def connector_to(local, calls=None):
    def connector(url, **kwargs):
        if calls is not None:
            calls.append((url, kwargs))
        return connect(local, **kwargs)

    return connector


FAST = dict(
    chunk_seconds=0.15,
    backoff_start=0.01,
    backoff_max=0.05,
    tick_seconds=0.05,
    supervise_seconds=0.02,
)


class CaptureClock:
    def __init__(self):
        self.offset = 0

    def monotonic(self):
        return time.monotonic() + self.offset

    monotonic_ns = staticmethod(time.monotonic_ns)
    time_ns = staticmethod(time.time_ns)


class FakeProvider:
    def __init__(self):
        self.paths = []

    def headers_for_get(self, path):
        self.paths.append(path)
        return {"X-Synthetic-Signature": "sig-%d" % len(self.paths)}


class FakeFeed:
    def __init__(self, payloads=(), *, delay=0, on_exhausted=None):
        self.payloads, self.delay, self.on_exhausted = (
            iter(payloads),
            delay,
            on_exhausted,
        )
        self.sent, self.recv_tasks = [], set()
        self.received = self.active_recv = self.cancelled = 0
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        assert self.active_recv == 0
        self.closed = True
        return False

    async def send(self, payload):
        self.sent.append(payload)

    async def recv(self):
        assert self.active_recv == 0
        self.active_recv += 1
        self.recv_tasks.add(asyncio.current_task())
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            try:
                raw = next(self.payloads)
            except StopIteration:
                if self.on_exhausted:
                    self.on_exhausted()
                await asyncio.Future()
            self.received += 1
            return raw
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        finally:
            self.active_recv -= 1


def capture_signals(monkeypatch):
    handlers = {}
    monkeypatch.setattr(
        asyncio.get_running_loop(),
        "add_signal_handler",
        lambda sig, callback: handlers.update({sig: callback}),
    )
    return handlers


# ---------------------------------------------------------------- tracker / config


def test_polymarket_tracker_counts_only_expected_books_and_resets():
    tracker = rr.Tracker("polymarket", ["a", "b"])
    for raw in (
        "PING",
        "{",
        "null",
        "42",
        json.dumps({"event_type": "book", "asset_id": "z"}),
        json.dumps({"event_type": "price_change", "asset_id": "a"}),
    ):
        tracker.observe(raw)
    assert tracker.books == set() and not tracker.ready
    tracker.observe(
        json.dumps(
            [
                {"event_type": "book", "asset_id": "a"},
                {"event_type": "book", "asset_id": "b"},
            ]
        )
    )
    assert tracker.ready
    tracker.reset(2)
    assert tracker.generation == 2 and not tracker.ready


def test_kalshi_tracker_snapshots_acks_and_sequence_gaps():
    tracker = rr.Tracker("kalshi", ["K-1", "K-2"])
    tracker.observe(
        json.dumps(
            {
                "type": "subscribed",
                "id": 1,
                "msg": {"channel": "orderbook_delta", "sid": 7},
            }
        )
    )
    tracker.observe(
        json.dumps(
            {
                "type": "orderbook_snapshot",
                "sid": 7,
                "seq": 1,
                "msg": {"market_ticker": "K-1"},
            }
        )
    )
    tracker.observe(
        json.dumps(
            {
                "type": "orderbook_snapshot",
                "sid": 7,
                "seq": 2,
                "msg": {"market_ticker": "OTHER"},
            }
        )
    )
    tracker.observe(
        json.dumps(
            {
                "type": "orderbook_delta",
                "sid": 7,
                "seq": 3,
                "msg": {"market_ticker": "K-2"},
            }
        )
    )
    tracker.observe(
        json.dumps(
            {"type": "trade", "sid": 8, "seq": 99, "msg": {"market_ticker": "K-2"}}
        )
    )
    assert (
        tracker.acks == {"orderbook_delta"}
        and tracker.books == {"K-1"}
        and tracker.gap is None
    )
    tracker.observe(
        json.dumps(
            {
                "type": "orderbook_snapshot",
                "sid": 7,
                "seq": 4,
                "msg": {"market_ticker": "K-2"},
            }
        )
    )
    assert tracker.ready
    tracker.observe(
        json.dumps({"type": "orderbook_delta", "sid": 7, "seq": 9, "msg": {}})
    )
    assert tracker.gap == {"sid": 7, "expected": 5, "got": 9}
    tracker.reset(5)
    assert tracker.gap is None and tracker.seqs == {} and tracker.acks == set()


def test_kalshi_subscriptions_use_core_payload_one_command_per_channel():
    payloads = [json.loads(p) for p in rr.subscriptions("kalshi", ["A", "B"])]
    assert [p["params"]["channels"] for p in payloads] == [
        ["orderbook_delta"],
        ["trade"],
        ["market_lifecycle_v2"],
    ]
    assert [p["id"] for p in payloads] == [1, 2, 3]
    assert all(
        p["cmd"] == "subscribe" and p["params"]["market_tickers"] == ["A", "B"]
        for p in payloads
    )
    assert (
        payloads[0]["params"]["use_yes_price"] is True
        and "use_yes_price" not in payloads[1]["params"]
    )
    pm = json.loads(rr.subscriptions("polymarket", ["x"])[0])
    assert pm == {"assets_ids": ["x"], "type": "market", "custom_feature_enabled": True}


def test_market_list_validation(tmp_path):
    assert rr.load_markets(
        market_list(tmp_path, "polymarket", ["1", "2"]), "polymarket"
    )["instruments"] == ["1", "2"]
    bad = [
        ("polymarket", ["1", "1"]),
        ("polymarket", []),
        ("kalshi", ["X:YES"]),
        ("kalshi", [" X"]),
    ]
    for venue, ids in bad:
        with pytest.raises(ValueError):
            rr.load_markets(market_list(tmp_path, venue, ids), venue)
    with pytest.raises(ValueError):
        rr.load_markets(market_list(tmp_path, "polymarket", ["1"]), "kalshi")
    path = market_list(tmp_path, "kalshi", ["K"])
    path.write_text(path.read_text().replace(K_URL, "wss://example.com/ws"))
    with pytest.raises(ValueError):
        rr.load_markets(path, "kalshi")


@pytest.mark.parametrize("venue,count", [("polymarket", 381), ("kalshi", 1001)])
def test_market_lists_above_former_caps_are_preserved(tmp_path, venue, count):
    ids = [str(i) for i in range(count)]
    assert (
        rr.load_markets(market_list(tmp_path, venue, ids), venue)["instruments"] == ids
    )
    for raw in rr.subscriptions(venue, ids):
        payload = json.loads(raw)
        assert (
            payload["assets_ids"]
            if venue == "polymarket"
            else payload["params"]["market_tickers"]
        ) == ids


def test_roots_must_be_runs_run_instance(tmp_path):
    runs = tmp_path / "runs"
    assert rr.validate_root(runs / "run1" / "polymarket_00", runs)
    for root in (
        Path("relative/x/y"),
        runs,
        runs / "run1",
        runs / "a" / "b" / "c",
        runs / "run1" / ".." / "x",
        runs / "run 1" / "x",
        tmp_path / "elsewhere" / "a",
    ):
        with pytest.raises(ValueError):
            rr.validate_root(root, runs)


def test_parse_end_requires_explicit_utc():
    assert rr.parse_end("2026-11-06T12:00:00Z") == datetime(
        2026, 11, 6, 12, tzinfo=timezone.utc
    )
    for value in ("2026-11-06T12:00:00", "2026-11-06T12:00:00+01:00", "nonsense"):
        with pytest.raises(ValueError):
            rr.parse_end(value)


@pytest.mark.parametrize("link_depth", [0, 1, 2, 3])
def test_root_rejects_symlinks_at_every_level(tmp_path, link_depth):
    parts = ["ancestor", "runs", "run1", "instance"]
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path.joinpath(*parts[: link_depth + 1])
    link.parent.mkdir(parents=True, exist_ok=True)
    link.symlink_to(target, target_is_directory=True)
    runs_root = tmp_path / "ancestor/runs"
    with pytest.raises(ValueError, match="non-symlink"):
        rr.validate_root(runs_root / "run1/instance", runs_root)


# ---------------------------------------------------------------- chunk writer


def test_writer_is_lossless_hashes_and_chains(tmp_path):
    writer = rr.ChunkWriter(tmp_path, "rec", "kalshi", "kalshi_00")
    payloads = [
        ' [ {"type":"orderbook_delta", "n":1.000} ] \n',
        '{"unicode":"SÃƒÂ£o Paulo", "escape":"\\u00e3"}',
    ]
    for raw in payloads:
        writer.write("text", raw, 3)
    first = writer.seal()
    assert (
        first["schema"] == "kalshi.public_raw_chunk.v1" and first["venue"] == "kalshi"
    )
    assert first["instance"] == "kalshi_00" and first["previous_chunk_sha256"] is None
    data = (tmp_path / first["filename"]).read_bytes()
    assert (
        first["sha256"] == hashlib.sha256(data).hexdigest()
        and first["md5"] == hashlib.md5(data).hexdigest()
    )
    envelopes = [json.loads(x) for x in gzip.decompress(data).splitlines()]
    assert [e["local_frame_seq"] for e in envelopes] == [1, 2]
    assert all(
        e["connection_generation"] == 3
        and e["frame_type"] == "text"
        and e["direction"] == "received"
        for e in envelopes
    )
    assert [e["payload_raw"] for e in envelopes] == payloads
    assert all(
        e["venue"] == "kalshi" and e["schema"] == "kalshi.public_raw_frame.v1"
        for e in envelopes
    )
    # The uploader deletes sealed chunks: the chain must continue from memory, never from disk.
    (tmp_path / first["filename"]).unlink()
    (tmp_path / (first["filename"] + ".manifest.json")).unlink()
    writer.write("text", "x", 4)
    second = writer.seal("shutdown")
    assert (
        second["previous_chunk_sha256"] == first["sha256"]
        and second["first_frame_seq"] == 3
    )
    assert writer.previous == second["sha256"] and writer.sealed_count == 2
    assert not list(tmp_path.glob("*.part"))


def test_writer_pause_drops_then_records_gap(tmp_path):
    writer = rr.ChunkWriter(tmp_path, "rec", "polymarket", "polymarket_00")
    writer.write("text", "a", 1)
    writer.pause()
    assert writer.write("text", "b", 1) is None and writer.write("text", "c", 1) is None
    writer.resume(1)
    writer.write("text", "d", 2)
    writer.seal()
    got = frames(tmp_path)
    assert [f["payload_raw"] for f in got] == ["a", '{"dropped_frames": 2}', "d"]
    assert got[1]["frame_type"] == "disk_pause_gap" and got[1]["direction"] == "local"
    assert [f["local_frame_seq"] for f in got] == [
        1,
        2,
        3,
    ] and writer.dropped_total == 2


# ---------------------------------------------------------------- end-to-end against local servers


@pytest.mark.asyncio
async def test_polymarket_reconnects_with_new_generation_and_records_exact_frames(
    tmp_path,
):
    ids = ["111", "222", "333"]
    exact = '[{"event_type":"price_change", "asset_id":"111",  "price":"0.50"}]'
    subs = []

    async def handler(ws):
        subs.append(json.loads(await ws.recv()))
        await ws.send(json.dumps([{"event_type": "book", "asset_id": a} for a in ids]))
        await ws.send(exact)
        await ws.ping(b"srv")
        await ws.send(b"\x00\x01binary")
        if len(subs) == 1:
            await asyncio.sleep(0.1)
            await ws.close()
            return
        await ws.wait_closed()

    root = tmp_path / "runs" / "run1" / "polymarket_00"
    calls = []
    async with local_server(handler) as local:
        reason = await rr.capture_raw_ws(
            root,
            end_in(1.2),
            "polymarket",
            market_list(tmp_path, "polymarket", ids),
            connector=connector_to(local, calls),
            runs_root=tmp_path / "runs",
            **FAST,
        )
    assert reason == "deadline"
    assert (
        calls[0][0] == PM_URL
        and calls[0][1]["ping_interval"] is None
        and "additional_headers" not in calls[0][1]
    )
    assert len(subs) >= 2 and all(s["assets_ids"] == ids for s in subs)
    [session] = root.iterdir()
    got = frames(session)
    received = [f for f in got if f["frame_type"] == "text"]
    assert [f["payload_raw"] for f in received if f["payload_raw"] == exact] == [
        exact
    ] * len(subs)
    assert {f["connection_generation"] for f in received} >= {1, 2}
    kinds = {f["frame_type"] for f in got}
    assert {
        "subscription",
        "ws_ping",
        "ws_close",
        "binary_base64",
        "transport_error",
    } <= kinds
    assert [f["local_frame_seq"] for f in got] == list(range(1, len(got) + 1))
    ms = manifests(session)
    assert len(ms) >= 2 and all(m["venue"] == "polymarket" for m in ms)
    assert [m["previous_chunk_sha256"] for m in ms[1:]] == [
        m["sha256"] for m in ms[:-1]
    ]
    final = json.loads((session / "FINAL.json").read_text())
    assert (
        final["reason"] == "deadline"
        and not final["partial"]
        and final["final_chunk_sha256"] == ms[-1]["sha256"]
    )
    assert final["reconnects"] >= 1 and final["connection_generation"] >= 2
    assert not list(session.glob("*.part")) and not list(session.glob("*.tmp"))


@pytest.mark.asyncio
async def test_kalshi_signed_subscriptions_errors_and_gap_reconnect(tmp_path):
    tickers = ["K-A", "K-B"]
    seen = []

    async def handler(ws):
        seen.append(
            dict(
                sig=ws.request.headers.get("X-Synthetic-Signature"),
                subs=[json.loads(await ws.recv()) for _ in range(3)],
            )
        )
        for sub in seen[-1]["subs"]:
            channel = sub["params"]["channels"][0]
            if channel == "market_lifecycle_v2":
                await ws.send(
                    json.dumps(
                        {
                            "id": sub["id"],
                            "type": "error",
                            "msg": {"code": 8, "msg": "synthetic"},
                        }
                    )
                )
            else:
                await ws.send(
                    json.dumps(
                        {
                            "id": sub["id"],
                            "type": "subscribed",
                            "msg": {"channel": channel, "sid": sub["id"]},
                        }
                    )
                )
        for seq, ticker in enumerate(tickers, 1):
            await ws.send(
                '{"type":"orderbook_snapshot","sid":1,"seq":%d,"msg":{"market_ticker":"%s","yes":[[50, 10]]}}'
                % (seq, ticker)
            )
        await ws.send(
            '{"type":"orderbook_delta","sid":1,"seq":%d,"msg":{"market_ticker":"K-A"}}'
            % (7 if len(seen) == 1 else 3)
        )
        await ws.wait_closed()

    provider = FakeProvider()

    root = tmp_path / "runs" / "run1" / "kalshi_00"
    calls = []
    async with local_server(handler) as local:
        reason = await rr.capture_raw_ws(
            root,
            end_in(1.0),
            "kalshi",
            market_list(tmp_path, "kalshi", tickers),
            connector=connector_to(local, calls),
            auth=provider,
            runs_root=tmp_path / "runs",
            **FAST,
        )
    assert reason == "deadline"
    # Exactly one forced reconnect for the gap; a fresh GET signature per connection.
    assert len(seen) == 2 and [s["sig"] for s in seen] == ["sig-1", "sig-2"]
    assert provider.paths == ["/trade-api/ws/v2"] * 2 and calls[0][0] == K_URL
    assert calls[0][1]["ping_interval"] == 20
    [session] = root.iterdir()
    got = frames(session)
    assert sum(f["frame_type"] == "seq_gap" for f in got) == 1
    assert any(
        '"type": "error"' in f["payload_raw"] for f in got if f["frame_type"] == "text"
    )
    assert any(
        f["payload_raw"].startswith('{"type":"orderbook_snapshot","sid":1,"seq":1')
        for f in got
    )
    final = json.loads((session / "FINAL.json").read_text())
    assert (
        final["seq_gaps"] == 1
        and final["gap_reconnects"] == 1
        and final["ready_generations"] == 2
    )
    assert final["subscribed_channels"] == ["orderbook_delta", "trade"]
    provenance = json.loads((session / "PROVENANCE.json").read_text())
    assert provenance["venue"] == "kalshi" and provenance["instruments"] == tickers
    assert "sig-" not in "".join(p.read_text() for p in session.glob("*.json"))


@pytest.mark.asyncio
async def test_kalshi_refuses_redirects_that_would_carry_signed_headers(tmp_path):
    reached = []

    async def target(ws):
        reached.append(ws.request.headers.get("X-Synthetic-Signature"))
        await ws.close()

    async with local_server(target) as target_url:

        def redirect(connection, request):
            response = connection.respond(302, "")
            response.headers["Location"] = target_url
            return response

        async with serve(
            lambda ws: None, "127.0.0.1", 0, process_request=redirect
        ) as server:
            redirect_url = "ws://127.0.0.1:%d" % server.sockets[0].getsockname()[1]
            # Plain websockets follows the redirect and forwards the headers.
            async with connect(
                redirect_url, additional_headers={"X-Synthetic-Signature": "probe"}
            ):
                pass
            assert reached == ["probe"]
            root = tmp_path / "runs" / "run1" / "kalshi_00"
            reason = await rr.capture_raw_ws(
                root,
                end_in(0.6),
                "kalshi",
                market_list(tmp_path, "kalshi", ["K-A"]),
                connector=lambda url, **kwargs: rr.NoRedirectConnect(
                    redirect_url, **kwargs
                ),
                auth=FakeProvider(),
                runs_root=tmp_path / "runs",
                **FAST,
            )
    assert reason == "deadline" and reached == ["probe"]
    [session] = root.iterdir()
    errors = [
        f["payload_raw"]
        for f in frames(session)
        if f["frame_type"] == "transport_error"
    ]
    assert errors and all("302" in error for error in errors)


@pytest.mark.asyncio
async def test_kalshi_connection_errors_redact_sent_header_values(tmp_path, caplog):
    def connector(url, **kwargs):
        raise OSError("echoed " + kwargs["additional_headers"]["X-Synthetic-Signature"])

    root = tmp_path / "runs" / "run1" / "kalshi_00"
    reason = await rr.capture_raw_ws(
        root,
        end_in(0.4),
        "kalshi",
        market_list(tmp_path, "kalshi", ["K-A"]),
        connector=connector,
        auth=FakeProvider(),
        runs_root=tmp_path / "runs",
        **FAST,
    )
    assert reason == "deadline"
    [session] = root.iterdir()
    errors = [
        f["payload_raw"]
        for f in frames(session)
        if f["frame_type"] == "transport_error"
    ]
    assert errors and all(error == "OSError: echoed <redacted>" for error in errors)
    logs = [
        r
        for r in caplog.records
        if r.name == rr.__name__ and r.getMessage() == "transport_error"
    ]
    assert logs and all(
        r.levelno == logging.WARNING and r.error == errors[0] for r in logs
    )
    recorded = "".join(p.read_text() for p in session.glob("*.json")) + caplog.text
    recorded += repr([r.__dict__ for r in logs])
    assert "<redacted>" in recorded and "sig-" not in recorded


@pytest.mark.asyncio
async def test_debug_logging_does_not_emit_signed_handshake_headers(tmp_path, caplog):
    caplog.set_level(logging.DEBUG)

    async def handler(ws):
        await ws.close()

    async with local_server(handler) as local:
        reason = await rr.capture_raw_ws(
            tmp_path / "runs" / "run1" / "kalshi_00",
            end_in(0.3),
            "kalshi",
            market_list(tmp_path, "kalshi", ["K-A"]),
            connector=connector_to(local),
            auth=FakeProvider(),
            runs_root=tmp_path / "runs",
            **FAST,
        )
    assert reason == "deadline"
    client = [r for r in caplog.records if not r.name.startswith("websockets.server")]
    assert not any("sig-" in r.getMessage() for r in client)


@pytest.mark.asyncio
async def test_offline_connector_retries_until_deadline(tmp_path, monkeypatch):
    attempts = []
    clock = CaptureClock()
    monkeypatch.setattr(rr, "time", clock)

    class Offline:
        async def __aenter__(self):
            attempts.append(1)
            if len(attempts) == 3:
                clock.offset = 6  # Expire only after retries have actually happened.
            raise OSError("synthetic offline")

        async def __aexit__(self, *args):
            return False

    root = tmp_path / "runs" / "run1" / "polymarket_00"
    reason = await asyncio.wait_for(
        rr.capture_raw_ws(
            root,
            end_in(5),
            "polymarket",
            market_list(tmp_path, "polymarket", ["1"]),
            connector=lambda url, **kw: Offline(),
            runs_root=tmp_path / "runs",
            **FAST,
        ),
        timeout=3,
    )
    [session] = root.iterdir()
    final = json.loads((session / "FINAL.json").read_text())
    assert (
        reason == "deadline" and final["reason"] == "deadline" and not final["partial"]
    )
    assert len(attempts) == final["errors"] == 3 and final["reconnects"] == 2
    assert "synthetic offline" in final["last_error"]


@pytest.mark.asyncio
async def test_disk_critical_pauses_then_resumes_with_fresh_generation(
    tmp_path, monkeypatch, caplog
):
    caplog.set_level(logging.INFO, logger=rr.__name__)
    handlers = capture_signals(monkeypatch)
    readings = iter([10 * rr.GiB] * 3 + [100 * rr.MiB] * 4 + [rr.GiB] * 4)
    connections = []

    def free_bytes():
        free = next(readings, 10 * rr.GiB)
        if len(connections) >= 2 and free == 10 * rr.GiB:
            handlers[signal.SIGTERM]()
        return free

    async def handler(ws):
        await ws.recv()
        connections.append(ws)
        while True:
            await ws.send('{"event_type":"price_change"}')
            await asyncio.sleep(0.01)

    root = tmp_path / "runs" / "run1" / "polymarket_00"
    async with local_server(handler) as local:
        await asyncio.wait_for(
            rr.capture_raw_ws(
                root,
                end_in(5),
                "polymarket",
                market_list(tmp_path, "polymarket", ["1"]),
                connector=connector_to(local),
                runs_root=tmp_path / "runs",
                free_bytes=free_bytes,
                **dict(FAST, chunk_seconds=rr.CHUNK_SECONDS),
            ),
            timeout=3,
        )
    [session] = root.iterdir()
    got = frames(session)
    gaps = [f for f in got if f["frame_type"] == "disk_pause_gap"]
    assert len(gaps) == 1 and json.loads(gaps[0]["payload_raw"])["dropped_frames"] > 0
    assert (
        max(f["connection_generation"] for f in got) > gaps[0]["connection_generation"]
    )
    final = json.loads((session / "FINAL.json").read_text())
    assert final["dropped_frames_total"] > 0 and final["disk"] == "ok"
    disk_logs = [
        r
        for r in caplog.records
        if r.name == rr.__name__ and r.getMessage() == "disk_state"
    ]
    assert {r.disk: r.levelno for r in disk_logs} == {
        "critical": logging.ERROR,
        "low": logging.WARNING,
        "ok": logging.INFO,
    }
    assert all(
        r.instance == "polymarket_00" and isinstance(r.free_bytes, int)
        for r in disk_logs
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("free,disk,dropped", [(150, "low", 0), (50, "critical", 2)])
async def test_custom_disk_thresholds_warn_or_pause_writes(
    tmp_path, monkeypatch, free, disk, dropped
):
    handlers = capture_signals(monkeypatch)
    feed = FakeFeed(
        ['{"event_type":"book","asset_id":"1"}'],
        on_exhausted=lambda: handlers[signal.SIGTERM](),
    )
    root = tmp_path / "runs/run1/polymarket_00"
    await rr.capture_raw_ws(
        root,
        end_in(5),
        "polymarket",
        market_list(tmp_path, "polymarket", ["1"]),
        runs_root=tmp_path / "runs",
        connector=lambda url, **kwargs: feed,
        free_bytes=lambda: free,
        low_free=200,
        critical_free=100,
        **FAST,
    )
    [session] = root.iterdir()
    final = json.loads((session / "FINAL.json").read_text())
    assert final["disk"] == disk and final["dropped_frames_total"] == dropped
    assert len(frames(session)) == 2 - dropped


@pytest.mark.asyncio
async def test_multiple_connections_are_independent(tmp_path):
    """Two Polymarket slices and one Kalshi slice concurrently; a dead endpoint for one slice harms nobody."""

    async def pm(ws):
        sub = json.loads(await ws.recv())
        await ws.send(
            json.dumps(
                [{"event_type": "book", "asset_id": a} for a in sub["assets_ids"]]
            )
        )
        await ws.wait_closed()

    async def kalshi(ws):
        for _ in range(3):
            await ws.recv()
        await ws.send(
            '{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"K"}}'
        )
        await ws.wait_closed()

    provider = FakeProvider()

    runs = tmp_path / "runs"
    async with local_server(pm) as pm_local, local_server(kalshi) as k_local:
        end = end_in(2.0)
        jobs = [
            rr.capture_raw_ws(
                runs / "r" / "polymarket_00",
                end,
                "polymarket",
                market_list(tmp_path, "polymarket", ["1", "2"], "polymarket_00"),
                connector=connector_to(pm_local),
                runs_root=runs,
                **FAST,
            ),
            rr.capture_raw_ws(
                runs / "r" / "polymarket_01",
                end,
                "polymarket",
                market_list(tmp_path, "polymarket", ["3"], "polymarket_01"),
                connector=connector_to("ws://127.0.0.1:9"),
                runs_root=runs,
                **FAST,
            ),
            rr.capture_raw_ws(
                runs / "r" / "kalshi_00",
                end,
                "kalshi",
                market_list(tmp_path, "kalshi", ["K"], "kalshi_00"),
                connector=connector_to(k_local),
                auth=provider,
                runs_root=runs,
                **FAST,
            ),
        ]
        assert await asyncio.gather(*jobs) == ["deadline"] * 3
    state = {
        p.parent.parent.name: json.loads(p.read_text())
        for p in runs.glob("r/*/*/FINAL.json")
    }
    assert (
        state["polymarket_00"]["ready_generations"] == 1
        and state["polymarket_00"]["errors"] == 0
    )
    assert (
        state["kalshi_00"]["ready_generations"] == 1
        and state["kalshi_00"]["errors"] == 0
    )
    assert (
        state["polymarket_01"]["ready_generations"] == 0
        and state["polymarket_01"]["errors"] >= 2
    )


@pytest.mark.asyncio
async def test_end_in_past_exits_cleanly_without_session(tmp_path):
    root = tmp_path / "runs" / "run1" / "polymarket_00"
    reason = await rr.capture_raw_ws(
        root,
        end_in(-5),
        "polymarket",
        market_list(tmp_path, "polymarket", ["1"]),
        runs_root=tmp_path / "runs",
    )
    assert reason == "end_already_passed" and not root.exists()


# ---------------------------------------------------------------- in-memory receive path


@pytest.mark.asyncio
async def test_burst_uses_one_receiver_and_preserves_every_frame_in_order(
    tmp_path, monkeypatch
):
    handlers = capture_signals(monkeypatch)
    payloads = [' [{"event_type":"book", "asset_id":"1"}] \n']
    payloads += [f' {{"event_type":"price_change", "n":{i}}} ' for i in range(4096)]
    payloads[129:129] = ["PING", "PONG", b"\x00\xffbinary"]
    feed = FakeFeed(payloads, on_exhausted=lambda: handlers[signal.SIGTERM]())
    root = tmp_path / "runs" / "run1" / "polymarket_00"
    reason = await asyncio.wait_for(
        rr.capture_raw_ws(
            root,
            end_in(10),
            "polymarket",
            market_list(tmp_path, "polymarket", ["1"]),
            connector=lambda url, **kwargs: feed,
            runs_root=tmp_path / "runs",
            **FAST,
        ),
        timeout=5,
    )
    [session] = root.iterdir()
    got = frames(session)
    received = [f for f in got if f["direction"] == "received"]
    assert [f["payload_raw"] for f in received] == [
        raw if isinstance(raw, str) else base64.b64encode(raw).decode()
        for raw in payloads
    ]
    assert [f["frame_type"] for f in received[129:132]] == [
        "app_ping",
        "app_pong",
        "binary_base64",
    ]
    ping_index = next(i for i, f in enumerate(got) if f["frame_type"] == "app_ping")
    assert (got[ping_index + 1]["frame_type"], got[ping_index + 1]["direction"]) == (
        "app_pong",
        "sent",
    )
    assert feed.sent[-1] == "PONG" and len(feed.recv_tasks) == 1
    assert feed.closed and feed.cancelled == 1 and feed.received == len(payloads)
    assert [f["local_frame_seq"] for f in got] == list(range(1, len(got) + 1))
    final = json.loads((session / "FINAL.json").read_text())
    assert (
        reason == "signal_or_control_storage_error"
        and final["ready_generations"] == 1
        and final["errors"] == 0
    )


@pytest.mark.asyncio
async def test_polymarket_app_ping_fires_after_ten_seconds_during_continuous_frames(
    tmp_path, monkeypatch
):
    handlers = capture_signals(monkeypatch)
    ping_times = []

    class Streaming(FakeFeed):
        async def send(self, payload):
            await super().send(payload)
            if payload == "PING":
                ping_times.append(time.monotonic())
                handlers[signal.SIGTERM]()

    feed = Streaming(
        itertools.repeat('{"event_type":"book","asset_id":"1"}'), delay=0.005
    )
    root = tmp_path / "runs" / "run1" / "polymarket_00"
    started = time.monotonic()
    await asyncio.wait_for(
        rr.capture_raw_ws(
            root,
            end_in(12),
            "polymarket",
            market_list(tmp_path, "polymarket", ["1"]),
            connector=lambda url, **kwargs: feed,
            runs_root=tmp_path / "runs",
            **FAST,
        ),
        timeout=14,
    )
    [session] = root.iterdir()
    got = frames(session)
    pings = [
        f for f in got if f["frame_type"] == "app_ping" and f["direction"] == "sent"
    ]
    assert len(pings) == len(ping_times) == 1 and 10 <= ping_times[0] - started < 12
    received = [f for f in got if f["frame_type"] == "text"]
    assert len(received) > 100
    assert received[-1]["received_monotonic_ns"] / 1e9 >= ping_times[0] - 0.1
    assert json.loads((session / "FINAL.json").read_text())["errors"] == 0


@pytest.mark.asyncio
async def test_silent_receive_times_out_and_reconnects(tmp_path, monkeypatch):
    handlers = capture_signals(monkeypatch)
    feeds = []

    def connector(url, **kwargs):
        feed = FakeFeed(
            on_exhausted=(lambda: handlers[signal.SIGTERM]()) if feeds else None
        )
        feeds.append(feed)
        return feed

    root = tmp_path / "runs" / "run1" / "polymarket_00"
    reason = await asyncio.wait_for(
        rr.capture_raw_ws(
            root,
            end_in(5),
            "polymarket",
            market_list(tmp_path, "polymarket", ["1"]),
            connector=connector,
            runs_root=tmp_path / "runs",
            receive_timeout=0.1,
            **FAST,
        ),
        timeout=3,
    )
    [session] = root.iterdir()
    final = json.loads((session / "FINAL.json").read_text())
    assert reason == "signal_or_control_storage_error" and len(feeds) == 2
    assert final["errors"] == final["reconnects"] == 1
    assert final["last_error"] == "TimeoutError: 0.1 second receive timeout"
    assert all(
        feed.closed and feed.cancelled == 1 and len(feed.recv_tasks) == 1
        for feed in feeds
    )
    assert sum(f["frame_type"] == "transport_error" for f in frames(session)) == 1


@pytest.mark.asyncio
async def test_control_frames_extend_receive_timeout_without_text_frames(
    tmp_path, monkeypatch
):
    handlers = capture_signals(monkeypatch)

    async def handler(ws):
        await ws.recv()
        for _ in range(12):
            await (await ws.ping(b"activity"))
            await asyncio.sleep(0.05)
        handlers[signal.SIGTERM]()
        await ws.wait_closed()

    root = tmp_path / "runs" / "run1" / "polymarket_00"
    async with local_server(handler) as local:
        await asyncio.wait_for(
            rr.capture_raw_ws(
                root,
                end_in(5),
                "polymarket",
                market_list(tmp_path, "polymarket", ["1"]),
                connector=connector_to(local),
                runs_root=tmp_path / "runs",
                receive_timeout=0.2,
                **dict(FAST, chunk_seconds=rr.CHUNK_SECONDS),
            ),
            timeout=3,
        )
    [session] = root.iterdir()
    final = json.loads((session / "FINAL.json").read_text())
    assert final["errors"] == final["reconnects"] == 0
    got = frames(session)
    assert sum(f["frame_type"] == "ws_ping" for f in got) == 12
    assert not any(f["frame_type"] == "text" for f in got)


@pytest.mark.asyncio
async def test_buffered_kalshi_gap_reconnects_immediately_and_throttles_later_gaps(
    tmp_path, monkeypatch
):
    handlers = capture_signals(monkeypatch)
    snapshot = (
        '{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"K"}}'
    )
    gap = '{"type":"orderbook_delta","sid":1,"seq":4,"msg":{"market_ticker":"K"}}'
    after_gap = '{"type":"trade","msg":{"market_ticker":"K"}}'
    feeds = [
        FakeFeed([snapshot, gap, "must not read past reconnecting gap"]),
        FakeFeed(
            [snapshot, gap, after_gap], on_exhausted=lambda: handlers[signal.SIGTERM]()
        ),
    ]
    calls = []

    def connector(url, **kwargs):
        calls.append(kwargs)
        if len(calls) == 2:
            assert feeds[0].closed
        return feeds[len(calls) - 1]

    provider = FakeProvider()

    root = tmp_path / "runs" / "run1" / "kalshi_00"
    await asyncio.wait_for(
        rr.capture_raw_ws(
            root,
            end_in(5),
            "kalshi",
            market_list(tmp_path, "kalshi", ["K"]),
            connector=connector,
            auth=provider,
            runs_root=tmp_path / "runs",
            **FAST,
        ),
        timeout=3,
    )
    [session] = root.iterdir()
    got = frames(session)
    assert [f["payload_raw"] for f in got if f["frame_type"] == "text"] == [
        snapshot,
        gap,
        snapshot,
        gap,
        after_gap,
    ]
    assert feeds[0].received == 2 and all(
        feed.closed and len(feed.recv_tasks) == 1 for feed in feeds
    )
    assert all(
        c["max_queue"] == 128 and c["ping_interval"] == c["ping_timeout"] == 20
        for c in calls
    )
    final = json.loads((session / "FINAL.json").read_text())
    assert (
        final["seq_gaps"] == 2
        and final["gap_reconnects"] == final["reconnects"] == final["errors"] == 1
    )
    assert final["ready_generations"] == 2
    assert [
        (f["connection_generation"], f["direction"])
        for f in got
        if f["frame_type"] == "seq_gap"
    ] == [(1, "local"), (2, "local")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "interval,advance,reconnects", [(10, 9, 1), (10, 10, 2), (0, 0, 2)]
)
async def test_gap_reconnect_interval_controls_when_fresh_snapshots_are_requested(
    tmp_path, monkeypatch, interval, advance, reconnects
):
    handlers = capture_signals(monkeypatch)
    clock = CaptureClock()
    monkeypatch.setattr(rr, "time", clock)
    snapshot = (
        '{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"K"}}'
    )
    gap = '{"type":"orderbook_delta","sid":1,"seq":4,"msg":{"market_ticker":"K"}}'

    class LaterGap(FakeFeed):
        async def recv(self):
            if self.received == 1:
                clock.offset = advance
            return await super().recv()

    feeds = [
        FakeFeed([snapshot, gap]),
        LaterGap([snapshot, gap], on_exhausted=lambda: handlers[signal.SIGTERM]()),
        FakeFeed([snapshot], on_exhausted=lambda: handlers[signal.SIGTERM]()),
    ]
    connections = []

    def connector(url, **kwargs):
        feed = feeds[len(connections)]
        connections.append(feed)
        return feed

    root = tmp_path / "runs/run1/kalshi_00"
    await asyncio.wait_for(
        rr.capture_raw_ws(
            root,
            end_in(30),
            "kalshi",
            market_list(tmp_path, "kalshi", ["K"]),
            runs_root=tmp_path / "runs",
            auth=FakeProvider(),
            connector=connector,
            gap_reconnect_min_interval=interval,
            **FAST,
        ),
        timeout=3,
    )
    [session] = root.iterdir()
    final = json.loads((session / "FINAL.json").read_text())
    assert final["seq_gaps"] == 2
    assert final["gap_reconnects"] == final["reconnects"] == reconnects
    assert len(connections) == reconnects + 1
    assert sum(f["frame_type"] == "seq_gap" for f in frames(session)) == 2
    assert all(feed.closed for feed in connections)


@pytest.mark.asyncio
async def test_disk_recovery_cancels_pending_receive_before_reconnecting(
    tmp_path, monkeypatch
):
    handlers = capture_signals(monkeypatch)
    book = '{"event_type":"book","asset_id":"1"}'
    feeds = [
        FakeFeed([book]),
        FakeFeed([book], on_exhausted=lambda: handlers[signal.SIGTERM]()),
    ]
    calls = []
    readings = iter([10 * rr.GiB, 100 * rr.MiB, 10 * rr.GiB])

    def connector(url, **kwargs):
        calls.append(kwargs)
        if len(calls) == 2:
            assert feeds[0].closed and feeds[0].cancelled == 1
        return feeds[len(calls) - 1]

    root = tmp_path / "runs" / "run1" / "polymarket_00"
    await asyncio.wait_for(
        rr.capture_raw_ws(
            root,
            end_in(5),
            "polymarket",
            market_list(tmp_path, "polymarket", ["1"]),
            connector=connector,
            runs_root=tmp_path / "runs",
            free_bytes=lambda: next(readings, 10 * rr.GiB),
            **FAST,
        ),
        timeout=3,
    )
    [session] = root.iterdir()
    final = json.loads((session / "FINAL.json").read_text())
    assert final["connection_generation"] == 2 and final["ready_generations"] == 2
    assert (
        final["reconnects"] == final["errors"] == 1
        and "ForcedReconnect: disk pause ended" in final["last_error"]
    )
    assert all(
        feed.closed and feed.cancelled == 1 and len(feed.recv_tasks) == 1
        for feed in feeds
    )
    assert sum(f["frame_type"] == "disk_pause_gap" for f in frames(session)) == 1


@pytest.mark.parametrize("ending", ["signal", "deadline"])
@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.asyncio
async def test_signal_and_deadline_finish_promptly_and_seal_pending_or_busy_receive(
    tmp_path, monkeypatch, ending, streaming
):
    loop = asyncio.get_running_loop()
    clock = CaptureClock()
    monkeypatch.setattr(rr, "time", clock)
    timer = ended_at = None

    def finish():
        nonlocal ended_at
        ended_at = time.monotonic()
        if ending == "signal":
            os.kill(os.getpid(), signal.SIGTERM)
        else:
            clock.offset = 6

    class Receiving(FakeFeed):
        async def recv(self):
            nonlocal timer
            if self.received == 1:
                # Arm shutdown after the first book has been written and observed.
                timer = loop.call_later(0.15, finish)
            return await super().recv()

    book = '{"event_type":"book","asset_id":"1"}'
    feed = Receiving(itertools.repeat(book) if streaming else [book])
    root = tmp_path / "runs" / "run1" / "polymarket_00"
    try:
        reason = await asyncio.wait_for(
            rr.capture_raw_ws(
                root,
                end_in(5),
                "polymarket",
                market_list(tmp_path, "polymarket", ["1"]),
                connector=lambda url, **kwargs: feed,
                runs_root=tmp_path / "runs",
                **dict(FAST, chunk_seconds=rr.CHUNK_SECONDS),
            ),
            timeout=3,
        )
    finally:
        if timer is not None:
            timer.cancel()
    assert ended_at is not None and time.monotonic() - ended_at < 1
    [session] = root.iterdir()
    final = json.loads((session / "FINAL.json").read_text())
    assert reason == (
        "signal_or_control_storage_error" if ending == "signal" else "deadline"
    )
    assert final["partial"] == (ending == "signal") and final["closed_chunk_count"] >= 1
    assert (
        final["errors"] == final["reconnects"] == 0 and final["ready_generations"] == 1
    )
    assert final["final_chunk_sha256"] == manifests(session)[-1]["sha256"]
    assert (
        feed.closed
        and len(feed.recv_tasks) == 1
        and next(iter(feed.recv_tasks)).cancelled()
    )
    assert not list(session.glob("*.part"))


# ---------------------------------------------------------------- sampled lag supervision


async def scripted_lag_capture(tmp_path, monkeypatch, caplog, steps, **options):
    caplog.set_level(logging.INFO, logger=rr.__name__)
    handlers = capture_signals(monkeypatch)
    clock = CaptureClock()
    clock.monotonic = lambda: 1000 + clock.offset
    wall_ns = 1_000_000_000_000
    clock.time_ns = lambda: wall_ns
    monkeypatch.setattr(rr, "time", clock)
    inbox, delivered = asyncio.Queue(), asyncio.Event()
    logs = []

    class CollectLogs(logging.Handler):
        def emit(self, record):
            logs.append(
                dict(record.__dict__, event=record.getMessage(), at=clock.offset)
            )

    monkeypatch.setattr(rr.logger, "handlers", [CollectLogs()])

    class ScriptedFeed(FakeFeed):
        async def recv(self):
            raw = await inbox.get()
            delivered.set()
            return raw

    feed = ScriptedFeed()
    original_wait = asyncio.wait
    script = iter(steps)

    async def scripted_wait(tasks, *, timeout, return_when):
        nonlocal wall_ns
        if len(tasks) != 2:
            return await original_wait(tasks, timeout=timeout, return_when=return_when)
        elapsed, payloads = next(script)
        clock.offset += elapsed
        wall_ns += int(elapsed * 1_000_000_000)
        for raw in payloads:
            delivered.clear()
            inbox.put_nowait(raw(wall_ns) if callable(raw) else raw)
            await delivered.wait()
        await asyncio.sleep(0)
        if clock.offset == total_elapsed:
            handlers[signal.SIGTERM]()
        return set(), set(tasks)

    async def inline_thread(func, *args):
        return func(*args)

    total_elapsed = sum(elapsed for elapsed, _ in steps)
    monkeypatch.setattr(asyncio, "wait", scripted_wait)
    monkeypatch.setattr(asyncio, "to_thread", inline_thread)
    root = tmp_path / "runs" / "run1" / "polymarket_00"
    reason = await rr.capture_raw_ws(
        root,
        end_in(600),
        "polymarket",
        market_list(tmp_path, "polymarket", ["1"]),
        connector=lambda url, **kwargs: feed,
        runs_root=tmp_path / "runs",
        tick_seconds=0,
        supervise_seconds=1,
        receive_timeout=600,
        **options,
    )
    assert reason == "signal_or_control_storage_error"
    [session] = root.iterdir()
    return session, logs


def lag_payload(lag_ms):
    return lambda wall_ns: json.dumps(
        dict(event_type="price_change", timestamp=str(wall_ns // 1_000_000 - lag_ms))
    )


@pytest.mark.asyncio
async def test_lag_warnings_need_ten_samples_rate_limit_and_recover_once(
    tmp_path, monkeypatch, caplog
):
    steps = [(1, [lag_payload(8000)]) for _ in range(75)]
    steps += [(1, [lag_payload(100)]) for _ in range(40)]
    session, logs = await scripted_lag_capture(tmp_path, monkeypatch, caplog, steps)
    warnings = [entry for entry in logs if entry["event"] == "lag_warning"]
    assert [entry["at"] for entry in warnings] == [10, 70]
    assert all(
        entry["levelno"] == logging.WARNING and entry["exchange_lag_ms_p50_60s"] == 8000
        for entry in warnings
    )
    assert [entry["lag_samples_60s"] for entry in warnings] == [10, 60]
    recovered = [entry for entry in logs if entry["event"] == "lag_recovered"]
    assert len(recovered) == 1 and recovered[0]["at"] == 105
    assert recovered[0]["exchange_lag_ms_p50_60s"] == 4050
    assert json.loads((session / "FINAL.json").read_text())["lag_warnings"] == 2
    summaries = [
        entry for entry in logs if entry["event"] == "summary" and entry["at"] >= 60
    ]
    assert summaries and all(
        entry["exchange_lag_ms_p50_60s"] == entry["exchange_lag_ms_max_60s"] == 8000
        and entry["loop_lag_ms_max_60s"] == 0
        for entry in summaries
    )


@pytest.mark.asyncio
async def test_lag_threshold_is_strict_and_silence_is_not_recovery(
    tmp_path, monkeypatch, caplog
):
    steps = [(1, [lag_payload(100)]) for _ in range(10)]
    steps += [(1, [lag_payload(101)]) for _ in range(10)]
    steps += [(61, []), (1, []), (1, [lag_payload(-25)]), (1, [])]
    session, logs = await scripted_lag_capture(
        tmp_path, monkeypatch, caplog, steps, lag_warn_ms=100
    )
    warnings = [entry for entry in logs if entry["event"] == "lag_warning"]
    assert len(warnings) == 1 and warnings[0]["at"] == 20
    recovered = [entry for entry in logs if entry["event"] == "lag_recovered"]
    assert len(recovered) == 1 and recovered[0]["at"] == 83
    assert json.loads((session / "PROVENANCE.json").read_text())["lag_warn_ms"] == 100


@pytest.mark.asyncio
async def test_loop_stall_latest_frame_sampling_and_provenance_only_disclaimers(
    tmp_path, monkeypatch, caplog
):
    steps = [
        (1, [lag_payload(9999), lag_payload(-25), b"\x00\xff"]),
        (1.25, []),
        (1, []),
    ]
    session, logs = await scripted_lag_capture(tmp_path, monkeypatch, caplog, steps)
    live = json.loads((session / "live_state.json").read_text())
    final = json.loads((session / "FINAL.json").read_text())
    lag_values = dict(
        exchange_lag_ms_last=-25,
        exchange_lag_ms_p50_60s=-25,
        exchange_lag_ms_max_60s=-25,
        loop_lag_ms_max_60s=250,
        lag_samples_60s=1,
        lag_warnings=0,
    )
    assert {key: live[key] for key in lag_values} == lag_values
    assert {key: final[key] for key in lag_values} == lag_values
    original_keys = set(
        "recording_id venue instance session updated_utc connected ready connection_generation "
        "book_count_current_generation expected_book_count subscribed_channels frame_count sealed_chunks "
        "disk free_bytes paused dropped_frames_total last_error errors reconnects seq_gaps gap_reconnects "
        "ready_generations".split()
    )
    assert set(live) == original_keys | lag_values.keys()
    final_keys = set(
        "reason partial final_chunk_sha256 closed_chunk_count active_partial_filename end_utc source_hashes".split()
    )
    assert set(final) == set(live) | final_keys
    assert all(final[key] == value for key, value in live.items())
    assert live["venue"] == "polymarket" and live["instance"] == "polymarket_00"
    assert live["connected"] is live["ready"] is live["paused"] is False
    assert (
        live["connection_generation"]
        == live["expected_book_count"]
        == live["sealed_chunks"]
        == 1
    )
    assert live["frame_count"] == len(frames(session)) == 4
    assert (
        live["free_bytes"] is not None
        and live["disk"] == "ok"
        and live["last_error"] is None
    )
    assert all(
        live[key] == 0
        for key in (
            "book_count_current_generation",
            "dropped_frames_total",
            "errors",
            "reconnects",
            "seq_gaps",
            "gap_reconnects",
            "ready_generations",
        )
    )
    assert live["subscribed_channels"] == []
    provenance = json.loads((session / "PROVENANCE.json").read_text())
    assert provenance["lag_warn_ms"] == rr.LAG_WARN_MS
    assert provenance["gzip_level"] == rr.GZIP_LEVEL
    assert {key: provenance[key] for key in rr.DISCLAIMER} == rr.DISCLAIMER
    for path in session.glob("*.json"):
        if path.name != "PROVENANCE.json":
            assert rr.DISCLAIMER.keys().isdisjoint(json.loads(path.read_text()))
    assert not any(entry["event"] == "lag_warning" for entry in logs)


@pytest.mark.asyncio
async def test_loop_lag_detects_stall_without_any_frames(tmp_path, monkeypatch, caplog):
    session, _ = await scripted_lag_capture(
        tmp_path, monkeypatch, caplog, [(1.5, []), (1, [])]
    )
    final = json.loads((session / "FINAL.json").read_text())
    assert final["loop_lag_ms_max_60s"] == 500 and final["lag_samples_60s"] == 0
    assert all(
        final[key] is None
        for key in (
            "exchange_lag_ms_last",
            "exchange_lag_ms_p50_60s",
            "exchange_lag_ms_max_60s",
        )
    )


@pytest.mark.asyncio
async def test_read_auth_failures_do_not_persist_provider_exception_text(
    tmp_path, monkeypatch, caplog
):
    clock = CaptureClock()
    monkeypatch.setattr(rr, "time", clock)

    class BrokenProvider:
        calls = 0

        def headers_for_get(self, path):
            self.calls += 1
            if self.calls == 2:
                clock.offset = 6
            raise ValueError("synthetic-sensitive-header")

    def unexpected_connect(*args, **kwargs):
        pytest.fail("failed auth reached network")

    root = tmp_path / "runs/run1/kalshi_00"
    await rr.capture_raw_ws(
        root,
        end_in(5),
        "kalshi",
        market_list(tmp_path, "kalshi", ["K"]),
        runs_root=tmp_path / "runs",
        auth=BrokenProvider(),
        connector=unexpected_connect,
        **FAST,
    )
    [session] = root.iterdir()
    final = json.loads((session / "FINAL.json").read_text())
    assert final["errors"] == 2
    assert (
        final["last_error"]
        == "ReadAuthenticationRequiredError: Kalshi read-auth provider failed"
    )
    assert "synthetic-sensitive-header" not in json.dumps(frames(session))
    assert "synthetic-sensitive-header" not in "".join(
        path.read_text() for path in session.glob("*.json")
    )
    assert "synthetic-sensitive-header" not in caplog.text
    assert "synthetic-sensitive-header" not in repr(
        [r.__dict__ for r in caplog.records]
    )


@pytest.mark.asyncio
async def test_fatal_frame_write_retains_partial_chunk_without_manifest(
    tmp_path, monkeypatch
):
    original_write = rr.ChunkWriter.write

    def write(self, frame_type, *args, **kwargs):
        if frame_type != "subscription":
            raise OSError("synthetic storage failure")
        return original_write(self, frame_type, *args, **kwargs)

    monkeypatch.setattr(rr.ChunkWriter, "write", write)
    feed = FakeFeed(['{"event_type":"book","asset_id":"1"}'])
    root = tmp_path / "runs/run1/polymarket_00"
    with pytest.raises(OSError, match="synthetic storage failure"):
        await rr.capture_raw_ws(
            root,
            end_in(5),
            "polymarket",
            market_list(tmp_path, "polymarket", ["1"]),
            runs_root=tmp_path / "runs",
            connector=lambda *args, **kwargs: feed,
            **FAST,
        )
    [session] = root.iterdir()
    [partial] = session.glob("*.part")
    final = json.loads((session / "FINAL.json").read_text())
    assert final["partial"] and final["reason"] == "partial_OSError"
    assert final["active_partial_filename"] == partial.name
    assert final["final_chunk_sha256"] is None and final["closed_chunk_count"] == 0
    assert not list(session.glob("*.manifest.json"))
    assert feed.closed and feed.active_recv == 0


@pytest.mark.asyncio
async def test_startup_storage_error_removes_signal_handlers(tmp_path, monkeypatch):
    loop = asyncio.get_running_loop()
    registered, removed = [], []
    monkeypatch.setattr(
        loop, "add_signal_handler", lambda sig, callback: registered.append(sig)
    )
    monkeypatch.setattr(loop, "remove_signal_handler", lambda sig: removed.append(sig))

    def fail(*args, **kwargs):
        raise OSError("synthetic provenance failure")

    monkeypatch.setattr(rr, "atomic", fail)
    with pytest.raises(OSError, match="synthetic provenance failure"):
        await rr.capture_raw_ws(
            tmp_path / "runs/run1/polymarket_00",
            end_in(5),
            "polymarket",
            market_list(tmp_path, "polymarket", ["1"]),
            runs_root=tmp_path / "runs",
        )
    assert registered == [signal.SIGTERM, signal.SIGINT]
    assert removed == registered[::-1]
