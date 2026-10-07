from __future__ import annotations

import json
import fcntl
import os
from pathlib import Path
import signal
import sqlite3
import sys
import time

import httpx
import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import collect_polymarket_wallets_rpc as census  # noqa: E402
import wallet_registry_export as exporter  # noqa: E402


def options(output: Path, *, blocks: int = 3) -> list[str]:
    return ["--output", str(output), "--rpc-url", "https://rpc.test",
            "--streams", "factory", "--request-delay", "0", "--max-blocks", "1",
            "--end-block", str(census.FACTORY_START + blocks - 1)]


def result_for(payload: dict):
    if payload["method"] == "eth_chainId":
        return "0x89"
    if payload["method"] == "eth_getBlockByNumber":
        return {"number": payload["params"][0], "hash": "0x" + "12" * 32, "timestamp": "0x1"}
    assert payload["method"] == "eth_getLogs"
    return []


def install_rpc(monkeypatch, handler):
    real_rpc = census.Rpc

    def build(store, url, **kwargs):
        def transport(request):
            payload = json.loads(request.content)
            result = handler(payload)
            if isinstance(result, httpx.Response):
                return result
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], "result": result})
        return real_rpc(store, url, transport=httpx.MockTransport(transport), **kwargs)

    monkeypatch.setattr(census, "Rpc", build)


def factory_status(status: dict) -> dict:
    return next(stream for stream in status["streams"] if stream["name"] == "factory")


class Clock:
    elapsed = 0.0

    def monotonic(self):
        return self.elapsed

    def sleep(self, seconds):
        self.elapsed += seconds


@pytest.mark.parametrize("mode", ["sync", "snapshot"])
@pytest.mark.parametrize("reason", ["startup", "budget", "stop"])
def test_noncompletion_exits_preserve_checkpoint_without_export(tmp_path, monkeypatch, mode, reason):
    output = tmp_path / "output"
    output.mkdir()
    previous = {"wallets.csv": b"previous CSV\n", "wallet-evidence.jsonl": b"{}\n",
                "manifest.json": b'{"previous":true}\n'}
    for name, content in previous.items():
        (output / name).write_bytes(content)
    log_calls = 0

    def handler(payload):
        nonlocal log_calls
        if reason == "startup" and payload["method"] == "eth_chainId":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"] + 1, "result": "0x89"})
        if payload["method"] == "eth_getLogs":
            log_calls += 1
            if reason == "stop" and log_calls == 2:
                signal.raise_signal(signal.SIGTERM)
        return result_for(payload)

    def unexpected_export(*_args, **_kwargs):
        pytest.fail("Stopping or startup failure initiated a full export")

    install_rpc(monkeypatch, handler)
    monkeypatch.setattr(census, "export", unexpected_export)
    monkeypatch.setattr(exporter.ExportManager, "start", unexpected_export)
    args = options(output) + ["--export-mode", mode]
    if mode == "snapshot":
        args += ["--export-scratch-dir", str(tmp_path / "scratch")]
    if reason == "budget":
        args += ["--max-requests", "3"]
    assert census.main(args) == (1 if reason == "startup" else 0)
    status = json.loads((output / "status.json").read_text())
    assert status["state"] == ("failed" if reason == "startup" else "partial")
    assert status["error"]
    with sqlite3.connect(output / "registry.sqlite3") as db:
        assert db.execute("SELECT count(*) FROM ranges").fetchone() == (0 if reason == "startup" else 1,)
    if reason != "startup":
        assert factory_status(status)["next_block"] == census.FACTORY_START + 1
    assert all((output / name).read_bytes() == content for name, content in previous.items())
    assert not (output / "latest-export.json").exists()


@pytest.fixture
def child_network_guard(tmp_path, monkeypatch):
    guard = tmp_path / "guard"
    guard.mkdir()
    loaded, attempted = guard / "loaded", guard / "attempted"
    (guard / "sitecustomize.py").write_text(
        "import socket\nfrom pathlib import Path\n"
        f"Path({str(loaded)!r}).write_text('loaded')\n"
        "def forbidden(*args, **kwargs):\n"
        f"    Path({str(attempted)!r}).write_text('attempted')\n"
        "    raise RuntimeError('Network forbidden in export worker test')\n"
        "socket.socket.connect = forbidden\nsocket.socket.connect_ex = forbidden\n"
    )
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(guard), os.environ.get("PYTHONPATH", "")]))
    return loaded, attempted


def test_actual_snapshot_worker_exports_final_checkpoint_without_rpc(tmp_path, monkeypatch, child_network_guard):
    output, scratch = tmp_path / "output", tmp_path / "scratch"
    install_rpc(monkeypatch, result_for)
    args = options(output) + ["--export-mode", "snapshot", "--export-scratch-dir", str(scratch),
                              "--snapshot-min-free-bytes", "0", "--snapshot-pause-s", "0"]
    assert census.main(args) == 0
    live = json.loads((output / "status.json").read_text())
    pointer = json.loads((output / "latest-export.json").read_text())
    manifest = json.loads((output / pointer["files"]["manifest.json"]).read_text())
    assert factory_status(live)["accepted_ranges"] == 3
    assert factory_status(pointer["snapshot_status"])["next_block"] == census.FACTORY_START + 3
    assert manifest["status"] == pointer["snapshot_status"]
    assert live["export"]["stage"] == "succeeded" and live["export"]["worker_exit_code"] == 0
    assert live["export"]["worker_pid"] != os.getpid()
    assert "phase_timings_s" in live and "phase_timings_s" not in pointer["snapshot_status"]
    requests = live["requests"]
    monkeypatch.setattr(census, "Rpc", lambda *_a, **_kw: pytest.fail("Export-only constructed an RPC client"))
    previous = pointer["generation"]
    for mode in ("sync", "snapshot"):
        assert census.main(args + ["--export-only", "--export-mode", mode]) == 0
        exported = json.loads((output / "status.json").read_text())
        latest = json.loads((output / "latest-export.json").read_text())
        assert exported["state"] == "exported" and exported["requests"] == requests
        assert latest["generation"] != previous and latest["previous_generation"] == previous
        assert latest["snapshot_status"]["state"] == "exported"
        assert all((output / name).read_bytes() == (output / path).read_bytes()
                   for name, path in latest["files"].items())
        previous = latest["generation"]
    assert child_network_guard[0].exists() and not child_network_guard[1].exists()
    assert not list(scratch.iterdir())


def test_startup_throttle_honors_retry_after_and_spaces_every_attempt(tmp_path, monkeypatch):
    clock = Clock()
    starts = []
    throttled = False

    def handler(payload):
        nonlocal throttled
        starts.append((payload["method"], clock.elapsed))
        if not throttled:
            throttled = True
            assert payload["method"] == "eth_chainId"
            return httpx.Response(429, headers={"Retry-After": "7"})
        return result_for(payload)

    monkeypatch.setattr(census, "time", clock)
    install_rpc(monkeypatch, handler)
    assert census.main(options(tmp_path, blocks=1) + ["--request-delay", "1"]) == 0
    assert starts[:2] == [("eth_chainId", 1.0), ("eth_chainId", 8.0)]
    assert all(second[1] - first[1] >= 1 for first, second in zip(starts, starts[1:]))
    status = json.loads((tmp_path / "status.json").read_text())
    assert status["phase_timings_s"]["rpc"]["retries"] == 1
    assert factory_status(status)["accepted_ranges"] == 1
    with sqlite3.connect(tmp_path / "registry.sqlite3") as db:
        attempts = db.execute("SELECT http_status,error FROM requests ORDER BY id").fetchall()
    assert attempts[0][0] == 429 and attempts[0][1]
    assert all(row == (200, None) for row in attempts[1:])


@pytest.mark.parametrize("payload", [None, [], {"jsonrpc": "2.0", "id": -1, "result": []},
                                     {"jsonrpc": "1.0", "id": 1, "result": []}])
def test_invalid_rpc_response_never_advances_or_bisects_range(tmp_path, payload):
    store = census.Store(tmp_path / "registry.sqlite3")
    start = census.FACTORY_START
    store.initialize({"end_block": start + 7, "max_blocks": 8, "starts": {"factory": start}}, {})
    rpc = census.Rpc(store, "https://rpc.test", delay=0,
                     transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=payload)))
    try:
        with pytest.raises(census.RpcError):
            census.Collector(store, rpc).step("factory")
        assert rpc.calls == 1
        assert store.db.execute("SELECT next_block,window FROM streams").fetchone() == (start, 8)
        assert store.db.execute("SELECT count(*) FROM ranges").fetchone() == (0,)
        assert store.db.execute("SELECT error FROM requests").fetchone()[0]
    finally:
        rpc.client.close()
        store.db.close()


def test_periodic_worker_launch_failure_does_not_stop_collection(tmp_path, monkeypatch):
    clock = Clock()
    launches = 0

    def handler(payload):
        if payload["method"] == "eth_getLogs":
            clock.elapsed += 1
        return result_for(payload)

    def launch_failure(*_args, **_kwargs):
        nonlocal launches
        launches += 1
        raise OSError("Unable to spawn exporter")

    monkeypatch.setattr(census, "time", clock)
    install_rpc(monkeypatch, handler)
    monkeypatch.setattr(exporter.subprocess, "Popen", launch_failure)
    assert census.main(options(tmp_path) + ["--export-mode", "snapshot", "--export-scratch-dir",
                                          str(tmp_path / "scratch"), "--export-interval-s", "1",
                                          "--max-requests", "4"]) == 0
    status = json.loads((tmp_path / "status.json").read_text())
    assert status["state"] == "partial"
    assert factory_status(status)["accepted_ranges"] == 2
    assert launches >= 1 and status["export"]["stage"] == "failed"


def test_export_due_at_last_range_produces_only_final_generation(tmp_path, monkeypatch):
    clock = Clock()
    completed_exports = []
    original_export = census.export

    def handler(payload):
        if payload["method"] == "eth_getLogs":
            clock.elapsed += 1
        return result_for(payload)

    def record_export(*args, **kwargs):
        status = original_export(*args, **kwargs)
        completed_exports.append((status["state"], factory_status(status)["accepted_ranges"]))
        return status

    monkeypatch.setattr(census, "time", clock)
    monkeypatch.setattr(census, "export", record_export)
    install_rpc(monkeypatch, handler)
    assert census.main(options(tmp_path, blocks=2) + ["--export-interval-s", "2"]) == 0
    assert completed_exports == [("partial", 2)]


def test_held_publisher_defers_periodic_export_while_collection_advances(tmp_path, monkeypatch):
    clock = Clock()

    def handler(payload):
        if payload["method"] == "eth_getLogs":
            clock.elapsed += 1
        return result_for(payload)

    monkeypatch.setattr(census, "time", clock)
    install_rpc(monkeypatch, handler)
    assert census.main(options(tmp_path, blocks=4) + ["--max-requests", "3"]) == 0
    with (tmp_path / ".export.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert census.main(options(tmp_path, blocks=4) + ["--max-requests", "3", "--export-interval-s", "1"]) == 0
    live = json.loads((tmp_path / "status.json").read_text())
    assert live["state"] == "partial" and factory_status(live)["accepted_ranges"] == 2
    assert not (tmp_path / "latest-export.json").exists()


def test_stop_immediately_after_sync_publication_preserves_complete_generation(tmp_path, monkeypatch):
    original_atomic = exporter._atomic_json
    publications = 0

    def stop_after_commit(path, payload):
        nonlocal publications
        original_atomic(path, payload)
        if path.name == "latest-export.json":
            publications += 1
            signal.raise_signal(signal.SIGTERM)

    install_rpc(monkeypatch, result_for)
    monkeypatch.setattr(exporter, "_atomic_json", stop_after_commit)
    assert census.main(options(tmp_path)) == 0
    pointer = json.loads((tmp_path / "latest-export.json").read_text())
    live = json.loads((tmp_path / "status.json").read_text())
    assert publications == 1
    assert all((tmp_path / path).is_file() for path in pointer["files"].values())
    assert factory_status(pointer["snapshot_status"])["accepted_ranges"] == 3
    assert live["state"] == "partial" and live["error"]


@pytest.mark.parametrize("finish", [False, True])
def test_collector_commits_while_real_worker_serializes(tmp_path, monkeypatch, child_network_guard, finish):
    output, scratch = tmp_path / "output", tmp_path / "scratch"
    serializing = tmp_path / "serializing"
    child = tmp_path / "slow-worker.py"
    child.write_text(
        "import os, sys, time\nfrom pathlib import Path\n"
        f"sys.path.insert(0, {str(SCRIPTS_DIR)!r})\n"
        "import collect_polymarket_wallets_rpc as census\n"
        "original_export = census.export_snapshot\n"
        "def slow_export(snapshot, directory, state, error=None):\n"
        "    if state != 'running':\n"
        "        return original_export(snapshot, directory, state, error)\n"
        f"    Path({str(serializing)!r}).write_text(str(os.getpid()))\n"
        "    time.sleep(30)\n"
        "    raise AssertionError('test must stop this exporter')\n"
        "census.export_snapshot = slow_export\nsys.exit(census.main())\n"
    )
    real_manager = exporter.ExportManager

    def manager(output, scratch, _collector_path, **kwargs):
        return real_manager(output, scratch, child, **kwargs)

    clock = Clock()
    log_calls = 0

    def handler(payload):
        nonlocal log_calls
        if payload["method"] == "eth_getLogs":
            log_calls += 1
            if log_calls == 2:
                deadline = time.monotonic() + 5
                while not serializing.exists() and time.monotonic() < deadline:
                    time.sleep(0.005)
                assert serializing.exists(), "Exporter never reached serialization"
            clock.elapsed += 1
        return result_for(payload)

    monkeypatch.setattr(exporter, "ExportManager", manager)
    monkeypatch.setattr(census, "time", clock)
    install_rpc(monkeypatch, handler)
    args = options(output) + ["--export-mode", "snapshot", "--export-scratch-dir", str(scratch),
                              "--snapshot-min-free-bytes", "0", "--snapshot-pause-s", "0",
                              "--export-interval-s", "1"]
    if not finish:
        args += ["--max-requests", "4"]
    assert census.main(args) == 0
    live = json.loads((output / "status.json").read_text())
    assert factory_status(live)["accepted_ranges"] == (3 if finish else 2)
    assert live["state"] == "partial" and live["export"]["stage"] == ("succeeded" if finish else "cancelled")
    if finish:
        pointer = json.loads((output / "latest-export.json").read_text())
        assert factory_status(pointer["snapshot_status"])["accepted_ranges"] == 3
        assert live["export"]["worker_pid"] != int(serializing.read_text())
    assert not live["export"]["worker_running"]
    assert child_network_guard[0].exists() and not child_network_guard[1].exists()
    assert not list(scratch.iterdir())
