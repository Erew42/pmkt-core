"""Exercise the CLI with local TLS, synthetic certificates, fsync and signals."""

import asyncio
import gzip
import json
import logging
import os
import runpy
import signal
import ssl
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pytest
from typer.testing import CliRunner
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from pmkt.cli.app import app
from pmkt.streaming import raw_capture as rr
from raw_capture_helpers import validate_chunk


@pytest.mark.parametrize("use_uvloop", [False, True])
@pytest.mark.parametrize(
    "stop_signal,gzip_level,lag_warn_ms",
    [(signal.SIGTERM, 6, 5000), (signal.SIGINT, 9, 2500)],
)
def test_cli_tls_fsync_and_signal_seals_chunk(
    tmp_path, monkeypatch, caplog, stop_signal, gzip_level, lag_warn_ms, use_uvloop
):
    caplog.set_level(logging.INFO, logger=rr.__name__)
    uvloop = pytest.importorskip("uvloop") if use_uvloop else None
    monkeypatch.setattr(rr, "uvloop", uvloop)
    cert_path, key_path = tmp_path / "cert.pem", tmp_path / "synthetic.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-keyout",
            str(key_path),
            "-out",
            str(cert_path),
        ],
        check=True,
        capture_output=True,
    )
    server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_tls.load_cert_chain(cert_path, key_path)
    client_tls = ssl.create_default_context(cafile=str(cert_path))
    root = tmp_path / "runs" / "run1" / "polymarket_00"
    markets = tmp_path / "markets.json"
    markets.write_text(
        json.dumps(
            dict(
                venue="polymarket",
                name="polymarket_00",
                instruments=["1"],
                ws_url="wss://ws-subscriptions-clob.polymarket.com/ws/market",
            )
        )
    )
    raw = ' [{"event_type":"book", "asset_id":"1"}] \n'
    capture, to_thread = rr.capture_raw_ws, asyncio.to_thread
    sync_calls = []

    async def checked_to_thread(func, *args):
        result = await to_thread(func, *args)
        sync_calls.append(func)
        synced.set()
        return result

    async def handler(ws):
        await ws.recv()
        await ws.send(raw)
        await (await ws.ping(b"local-tls"))
        await asyncio.wait_for(synced.wait(), timeout=12)
        os.kill(os.getpid(), stop_signal)
        await ws.wait_closed()

    async def local_capture(root, end, venue, markets, **kwargs):
        nonlocal synced
        synced = asyncio.Event()
        loop = asyncio.get_running_loop()
        assert (
            (uvloop is not None and isinstance(loop, uvloop.Loop))
            if use_uvloop
            else rr.event_loop_name(loop) == "asyncio"
        )
        async with serve(handler, "127.0.0.1", 0, ssl=server_tls) as server:
            local = "wss://127.0.0.1:%d" % server.sockets[0].getsockname()[1]
            return await capture(
                root,
                end,
                venue,
                markets,
                connector=lambda url, **kw: connect(local, ssl=client_tls, **kw),
                free_bytes=lambda: 10 * rr.GiB,
                tick_seconds=0.05,
                supervise_seconds=0.02,
                **kwargs,
            )

    synced = None
    monkeypatch.setattr(rr, "capture_raw_ws", local_capture)
    monkeypatch.setattr(asyncio, "to_thread", checked_to_thread)
    result = CliRunner().invoke(
        app,
        [
            "record-raw-ws",
            "--venue",
            "polymarket",
            "--markets",
            str(markets),
            "--runs-root",
            str(tmp_path / "runs"),
            "--root",
            str(root),
            "--end-utc",
            (datetime.now(timezone.utc) + timedelta(seconds=15)).isoformat(),
            "--gzip-level",
            str(gzip_level),
            "--lag-warn-ms",
            str(lag_warn_ms),
        ],
    )
    assert result.exit_code == 1, result.output
    [session] = root.iterdir()
    [chunk] = session.glob("*.jsonl.gz")
    assert validate_chunk(chunk, str(chunk) + ".manifest.json")["ok"]
    frames = [
        json.loads(line) for line in gzip.decompress(chunk.read_bytes()).splitlines()
    ]
    assert any(f["payload_raw"] == raw and f["frame_type"] == "text" for f in frames)
    assert any(f["frame_type"] == "ws_ping" for f in frames)
    assert sync_calls and all(func is os.fsync for func in sync_calls)
    provenance = json.loads((session / "PROVENANCE.json").read_text())
    assert provenance["event_loop"] == (
        f"uvloop {uvloop.__version__}" if use_uvloop else "asyncio"
    )
    assert (
        provenance["gzip_level"] == gzip_level
        and provenance["lag_warn_ms"] == lag_warn_ms
    )
    final = json.loads((session / "FINAL.json").read_text())
    assert final["reason"] == "signal_or_control_storage_error" and final["partial"]
    assert final["closed_chunk_count"] == 1 and final["errors"] == 0
    assert not list(session.glob("*.part"))
    events = [r for r in caplog.records if r.name == rr.__name__]
    assert [
        r.implementation for r in events if r.getMessage().startswith("event_loop ")
    ] == [provenance["event_loop"]]


def test_run_falls_back_when_uvloop_cannot_be_imported(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="<run_path>")
    monkeypatch.setitem(sys.modules, "uvloop", None)
    module = runpy.run_path(rr.__file__)
    assert module["uvloop"] is None
    loops, cancelled, generators_closed = [], [], []

    async def pending():
        try:
            await asyncio.Future()
        finally:
            cancelled.append(True)

    async def values():
        try:
            yield 1
        finally:
            generators_closed.append(True)

    async def capture():
        loops.append(asyncio.get_running_loop())
        assert module["event_loop_name"](loops[-1]) == "asyncio"
        asyncio.create_task(pending())
        iterator = values()
        await iterator.__anext__()
        await asyncio.to_thread(lambda: None)
        return 42

    policy = asyncio.get_event_loop_policy()
    assert module["run"](capture()) == 42
    assert asyncio.get_event_loop_policy() is policy
    assert len(loops) == 1 and loops[0].is_closed()
    assert cancelled == generators_closed == [True]
    events = [r for r in caplog.records if r.name == "<run_path>"]
    assert [
        r.implementation for r in events if r.getMessage().startswith("event_loop ")
    ] == ["asyncio"]


def test_run_closes_loop_on_error(monkeypatch):
    monkeypatch.setattr(rr, "uvloop", None)
    loops = []

    async def fail():
        loops.append(asyncio.get_running_loop())
        raise ValueError("synthetic failure")

    with pytest.raises(ValueError, match="synthetic failure"):
        rr.run(fail())
    assert loops[0].is_closed()
