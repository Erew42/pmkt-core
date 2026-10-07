from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from types import SimpleNamespace

import httpx
import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import collect_polymarket_wallets_rpc as census  # noqa: E402

ALICE = "0x" + "11" * 20
BOB = "0x" + "22" * 20
POOL = "0x" + "33" * 20
OLD_EXCHANGE = next(iter(census.EXCHANGES))
NEW_EXCHANGE = next(a for a, name in census.EXCHANGES.items() if name == "ctf_v2")


def word(n: int) -> str:
    return n.to_bytes(32, "big").hex()


def addr_topic(address: str) -> str:
    return "0x" + "0" * 24 + address[2:]


def event(topic: str = census.FILLED_V1, *, block: int = census.EXCHANGE_START,
          index: int = 0, counterparty: str = BOB) -> dict:
    log = {"address": OLD_EXCHANGE, "blockNumber": hex(block), "blockHash": "0x" + word(block),
           "transactionHash": "0x" + word(block + 100), "transactionIndex": "0x0",
           "logIndex": hex(index), "removed": False,
           "topics": [topic, "0x" + word(123), addr_topic(ALICE), addr_topic(counterparty)],
           "data": "0x" + "".join(map(word, [0, 42, 2, 3, 0]))}
    if topic == census.FILLED_V2:
        log["address"] = NEW_EXCHANGE
        log["data"] = "0x" + "".join(map(word, [0, 42, 2, 3, 0, 0, 0]))
    elif topic in (census.BUY, census.SELL):
        log["address"] = POOL
        log["topics"] = [topic, addr_topic(ALICE), "0x" + word(0)]
        log["data"] = "0x" + "".join(map(word, [2, 0, 3]))
    elif topic == census.CREATION:
        log["address"] = census.FACTORY
        log["topics"] = [topic, addr_topic(BOB), addr_topic(census.CTF), addr_topic(ALICE)]
        log["data"] = "0x" + "".join(map(word, [int(POOL, 16), 96, 0, 1, 42]))
    return log


@pytest.fixture
def store(tmp_path):
    db = census.Store(tmp_path / "registry.sqlite3")
    config = {"chain_id": 137, "rpc": "https://rpc.test", "end_block": census.EXCHANGE_START + 7,
              "max_blocks": 8, "starts": {"factory": census.FACTORY_START,
                                           "amm": census.FACTORY_START, "exchange": census.EXCHANGE_START}}
    db.initialize(config, {"number": config["end_block"], "hash": "0x" + word(9), "timestamp_s": 1})
    yield db
    db.db.close()


def rpc_for(store, handler, budget=None):
    def wrapper(request):
        assert "authorization" not in request.headers
        assert "cookie" not in request.headers
        payload = json.loads(request.content)
        result = handler(payload)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], "result": result},
                              headers={"set-cookie": "anonymous=discard-me; Path=/"})
    return census.Rpc(store, "https://rpc.test", delay=0, budget=budget, transport=httpx.MockTransport(wrapper))


@pytest.mark.parametrize("topic", [census.FILLED_V1, census.FILLED_V2])
def test_order_wallets_and_structural_counterparty(topic):
    assert census.decode_traders(event(topic)) == [(ALICE, 1), (BOB, 2)]
    assert census.decode_traders(event(topic, counterparty=OLD_EXCHANGE)) == [(ALICE, 1)]
    # Do not infer the maker's EOA/owner from a contract address.
    log = event(topic)
    log["topics"][2] = addr_topic(POOL)
    assert (POOL, 1) in census.decode_traders(log)


@pytest.mark.parametrize("topic,role", [(census.BUY, 4), (census.SELL, 8)])
def test_amm_requires_factory_attribution_and_accepts_early_collateral(topic, role):
    pool = census.decode_pool(event(census.CREATION, block=census.FACTORY_START))
    assert pool["collateral"] == ALICE  # No assumption that only USDC was used.
    log = event(topic, block=census.FACTORY_START + 1)
    assert census.decode_traders(log, pool) == [(ALICE, role)]
    assert census.decode_traders(log) == []
    with pytest.raises(ValueError, match="predates"):
        census.decode_traders(event(topic, block=census.FACTORY_START - 1), pool)


def test_reject_bad_factory_offset_and_foreign_ctf():
    log = event(census.CREATION)
    log["data"] = "0x" + "".join(map(word, [int(POOL, 16), 128, 0, 1, 42]))
    with pytest.raises(ValueError, match="dynamic array"):
        census.decode_pool(log)
    log = event(census.CREATION)
    log["topics"][2] = addr_topic(BOB)
    assert census.decode_pool(log) is None


def test_zero_payment_is_trade_but_zero_token_fill_is_not():
    log = event()
    log["data"] = "0x" + "".join(map(word, [0, 42, 0, 3, 0]))
    assert census.decode_traders(log)
    log["data"] = "0x" + "".join(map(word, [0, 42, 3, 0, 0]))
    assert census.decode_traders(log) == []


@pytest.mark.parametrize("mutation", [
    lambda x: x.update(removed=True),
    lambda x: x.update(blockNumber=hex(census.EXCHANGE_START - 1)),
    lambda x: x.update(transactionHash="0x1234"),
    lambda x: x.update(address=POOL),
    lambda x: x["topics"].__setitem__(0, census.BUY),
])
def test_invalid_rpc_logs_fail_closed(mutation):
    log = event()
    mutation(log)
    with pytest.raises(ValueError):
        census.validate_logs([log], census.EXCHANGE_START, census.EXCHANGE_START, {census.FILLED_V1}, {OLD_EXCHANGE})


def test_event_identity_is_not_transaction_hash():
    logs = [event(index=0), event(index=1)]
    assert len(census.validate_logs(logs + [copy.deepcopy(logs[0])], census.EXCHANGE_START,
                                   census.EXCHANGE_START, {census.FILLED_V1}, {OLD_EXCHANGE})) == 2
    bad = copy.deepcopy(logs[0])
    bad["data"] = "0x" + "".join(map(word, [0, 42, 2, 4, 0]))
    with pytest.raises(ValueError, match="duplicate"):
        census.validate_logs([logs[0], bad], census.EXCHANGE_START, census.EXCHANGE_START,
                             {census.FILLED_V1}, {OLD_EXCHANGE})


def test_conflicting_block_hashes_and_address_padding():
    first, second = event(index=0), event(index=1)
    second["blockHash"] = "0x" + word(99)
    with pytest.raises(ValueError, match="block hashes"):
        census.validate_logs([first, second], census.EXCHANGE_START, census.EXCHANGE_START,
                             {census.FILLED_V1}, {OLD_EXCHANGE})
    first["topics"][2] = "0x01" + first["topics"][2][4:]
    with pytest.raises(ValueError, match="padding"):
        census.decode_traders(first)


def test_write_rejected_before_transport_and_credentials_not_loaded(store, monkeypatch):
    monkeypatch.setenv("PMKT_POLYMARKET_PRIVATE_KEY_PATH", "/never/load/this")
    rpc = rpc_for(store, lambda payload: "0x89")
    with pytest.raises(ValueError, match="allowlist"):
        rpc.call("eth_sendRawTransaction", ["0xdead"])
    assert rpc.calls == 0
    assert store.db.execute("SELECT COUNT(*) FROM requests").fetchone()[0] == 0
    assert rpc.call("eth_chainId", [])[0] == "0x89"
    assert rpc.call("eth_chainId", [])[0] == "0x89"  # No anonymous CDN cookie forwarded.
    rpc.client.close()


@pytest.mark.parametrize("url", ["http://rpc.test", "https://user:secret@rpc.test", "https://rpc.test?key=secret"])
def test_reject_credential_urls(store, url):
    with pytest.raises(ValueError, match="public HTTPS"):
        census.Rpc(store, url)


def test_adaptive_range_split_and_durable_resume(store, tmp_path):
    seen = []
    def handler(payload):
        query = payload["params"][0]
        start, end = int(query["fromBlock"], 16), int(query["toBlock"], 16)
        seen.append((start, end))
        return [event(block=start, index=i) for i in range(3 if end - start >= 2 else 1)]
    rpc = rpc_for(store, handler)
    collector = census.Collector(store, rpc, log_cap=3)
    assert collector.step("exchange")
    assert seen[:3] == [(census.EXCHANGE_START, census.EXCHANGE_START + 7),
                        (census.EXCHANGE_START, census.EXCHANGE_START + 3),
                        (census.EXCHANGE_START, census.EXCHANGE_START + 1)]
    assert store.db.execute("SELECT COUNT(*) FROM ranges").fetchone()[0] == 1
    assert store.db.execute("SELECT COUNT(*) FROM wallets").fetchone()[0] == 2
    rpc.client.close()
    # A new process/connection starts exactly after the committed range.
    other = census.Store(tmp_path / "registry.sqlite3")
    rpc = rpc_for(other, handler)
    assert census.Collector(other, rpc, log_cap=3).step("exchange")
    assert seen[-1][0] == census.EXCHANGE_START + 2
    other.db.close()
    rpc.client.close()


def test_bad_abi_does_not_advance_checkpoint_or_partially_store_wallets(store):
    first, bad = event(), event(index=1)
    bad["data"] = "0x00"
    rpc = rpc_for(store, lambda payload: [first, bad])
    with pytest.raises(ValueError, match="ABI"):
        census.Collector(store, rpc).step("exchange")
    assert store.db.execute("SELECT next_block FROM streams WHERE name='exchange'").fetchone()[0] == census.EXCHANGE_START
    assert store.db.execute("SELECT COUNT(*) FROM wallets").fetchone()[0] == 0
    assert store.db.execute("SELECT COUNT(*) FROM ranges").fetchone()[0] == 0
    assert store.db.execute("SELECT COUNT(*) FROM requests").fetchone()[0] == 1
    rpc.client.close()


def test_factory_barrier_and_earlier_amm_witness_replaces_clob(store, tmp_path):
    def handler(payload):
        query = payload["params"][0]
        topic = query["topics"][0][0]
        start = int(query["fromBlock"], 16)
        return [event(census.CREATION if topic == census.CREATION else
                      census.BUY if topic in (census.BUY, census.SELL) else census.FILLED_V1, block=start)]
    rpc = rpc_for(store, handler)
    collector = census.Collector(store, rpc)
    assert not collector.step("amm")
    assert rpc.calls == 0
    assert collector.step("exchange")
    assert collector.step("factory")
    assert collector.step("amm")
    first, last, roles, raw = store.db.execute("SELECT first_block,last_block,roles,evidence FROM wallets WHERE address=?", (ALICE,)).fetchone()
    assert first == census.FACTORY_START and last == census.EXCHANGE_START
    assert roles == 5
    assert json.loads(raw)["role"] == "amm_buyer"
    status = census.export(store, tmp_path, "partial")
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert status["wallets"] == 2
    assert not manifest["coverage"]["lifetime_coverage_proven"]
    for name, digest in manifest["artifact_sha256"].items():
        assert hashlib.sha256((tmp_path / name).read_bytes()).hexdigest() == digest
    evidence = [json.loads(line) for line in (tmp_path / "wallet-evidence.jsonl").read_text().splitlines()]
    alice = next(row for row in evidence if row["wallet_address"] == ALICE)
    assert alice["pool"]["creation_log"]["topics"][0] == census.CREATION
    assert alice["request"]["response_sha256"]
    rpc.client.close()


def test_reject_resume_range_or_anchor_change(store):
    config, anchor = store.get("config"), store.get("anchor")
    store.initialize(config, anchor)
    with pytest.raises(ValueError, match="configuration"):
        store.initialize({**config, "end_block": config["end_block"] + 1}, anchor)
    with pytest.raises(ValueError, match="anchor changed"):
        store.initialize(config, {**anchor, "hash": "0x" + word(10)})


def test_rpc_error_recorded_and_single_block_cap_not_accepted(store):
    def error_handler(request):
        p = json.loads(request.content)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": p["id"], "error": {"code": -1, "message": "range limit"}})
    rpc = census.Rpc(store, "https://rpc.test", delay=0, transport=httpx.MockTransport(error_handler))
    with pytest.raises(census.RpcError, match="range limit"):
        rpc.call("eth_getLogs", [{}])
    assert "range limit" in store.db.execute("SELECT error FROM requests").fetchone()[0]
    rpc.client.close()
    rpc = rpc_for(store, lambda payload: [event(index=0), event(index=1)])
    with pytest.raises(ValueError, match="Single block"):
        census.Collector(store, rpc, log_cap=2).step("exchange")
    assert store.db.execute("SELECT COUNT(*) FROM ranges").fetchone()[0] == 0
    rpc.client.close()


def test_request_budget_keeps_last_committed_checkpoint(store):
    rpc = rpc_for(store, lambda payload: [event()], budget=1)
    collector = census.Collector(store, rpc)
    assert collector.step("exchange")
    with pytest.raises(census.RequestBudgetReached):
        collector.step("factory")
    assert store.db.execute("SELECT next_block FROM streams WHERE name='exchange'").fetchone()[0] == census.EXCHANGE_START + 8
    rpc.client.close()


def test_cli_budget_resume_and_offline_export(tmp_path, monkeypatch):
    real_rpc = census.Rpc
    def handler(payload):
        method = payload["method"]
        if method == "eth_chainId":
            return "0x89"
        if method == "eth_blockNumber":
            return hex(census.FACTORY_START + 300)
        if method == "eth_getBlockByNumber":
            return {"number": payload["params"][0], "hash": "0x" + word(9), "timestamp": "0x1"}
        query = payload["params"][0]
        topic = query["topics"][0][0]
        return [event(census.CREATION if topic == census.CREATION else census.BUY,
                      block=census.FACTORY_START)]
    def build_rpc(store, url, **kwargs):
        def transport(request):
            payload = json.loads(request.content)
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], "result": handler(payload)})
        return real_rpc(store, url, transport=httpx.MockTransport(transport), **kwargs)
    monkeypatch.setattr(census, "Rpc", build_rpc)
    options = ["--output", str(tmp_path), "--request-delay", "0"]
    assert census.main(options + ["--max-requests", "4", "--export-interval-s", "300"]) == 0
    partial = json.loads((tmp_path / "status.json").read_text())
    assert partial["state"] == "partial" and partial["wallets"] == 0
    assert partial["pools"] == 1
    assert not (tmp_path / "manifest.json").exists()
    assert census.main(options) == 0
    finished = json.loads((tmp_path / "status.json").read_text())
    assert finished["wallets"] == 1 and finished["configured_ranges_scanned"]
    assert not finished["lifetime_coverage_proven"]
    versions = json.loads((tmp_path / "manifest.json").read_text())["collector_versions"]
    assert [v["export_interval_s"] for v in versions] == [300, 43200]
    assert "export_interval_s" not in finished["config"]
    monkeypatch.setattr(census, "Rpc", lambda *a, **k: pytest.fail("Export made a network client"))
    assert census.main(options + ["--export-only"]) == 0
    assert json.loads((tmp_path / "status.json").read_text())["state"] == "exported"


@pytest.mark.parametrize("options,expected_ranges", [
    ([], []),
    (["--export-interval-s", "1200"], [2, 4]),
])
def test_periodic_exports_follow_interval(tmp_path, monkeypatch, options, expected_ranges):
    elapsed = 0
    real_rpc, real_export = census.Rpc, census.export
    exports = []
    def transport(request):
        nonlocal elapsed
        payload = json.loads(request.content)
        if payload["method"] == "eth_chainId":
            result = "0x89"
        elif payload["method"] == "eth_getBlockByNumber":
            result = {"number": payload["params"][0], "hash": "0x" + word(9), "timestamp": "0x1"}
        else:
            assert payload["method"] == "eth_getLogs"
            elapsed += 600
            result = []
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], "result": result})
    def build_rpc(store, url, **kwargs):
        return real_rpc(store, url, transport=httpx.MockTransport(transport), **kwargs)
    def record_export(store, directory, state, error=None, **kwargs):
        status = real_export(store, directory, state, error, **kwargs)
        factory = next(s for s in status["streams"] if s["name"] == "factory")
        exports.append((state, factory["accepted_ranges"]))
        return status
    monkeypatch.setattr(census, "time", SimpleNamespace(monotonic=lambda: elapsed, sleep=lambda _: None))
    monkeypatch.setattr(census, "Rpc", build_rpc)
    monkeypatch.setattr(census, "export", record_export)
    assert census.main(["--output", str(tmp_path), "--request-delay", "0",
                        "--streams", "factory", "--max-blocks", "1",
                        "--end-block", str(census.FACTORY_START + 5), *options]) == 0
    assert exports == [("running", n) for n in expected_ranges] + [("partial", 6)]


@pytest.mark.parametrize("interval", ["0", "-1"])
def test_reject_invalid_export_interval_before_opening_dataset(tmp_path, interval):
    output = tmp_path / "registry"
    with pytest.raises(SystemExit) as exc:
        census.main(["--output", str(output), "--export-interval-s", interval])
    assert exc.value.code == 2
    assert not output.exists()


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, delay):
        self.sleeps.append(delay)
        self.now += delay


@pytest.mark.parametrize("status", [429, 503])
def test_non_json_throttle_retries_same_range_with_cooldown_and_limiter(store, monkeypatch, status):
    clock, queries, starts = Clock(), [], []
    monkeypatch.setattr(census, "time", clock)

    def transport(request):
        payload = json.loads(request.content)
        queries.append(payload["params"])
        starts.append(clock.now)
        if len(queries) == 1:
            return httpx.Response(status, text="provider unavailable", headers={"Retry-After": "3"})
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], "result": [event()]})

    rpc = census.Rpc(store, "https://rpc.test", delay=1, transport=httpx.MockTransport(transport))
    try:
        assert census.Collector(store, rpc).step("exchange")
        assert queries[0] == queries[1]
        assert starts[1] - starts[0] >= 3
        assert all(b - a >= 1 for a, b in zip(starts, starts[1:]))
        assert store.db.execute("SELECT next_block FROM streams WHERE name='exchange'").fetchone()[0] == census.EXCHANGE_START + 8
        rows = store.db.execute("SELECT http_status,error FROM requests ORDER BY id").fetchall()
        assert rows[0][0] == status and rows[0][1]
        assert rows[1] == (200, None)
    finally:
        rpc.client.close()


@pytest.mark.parametrize("status", [200, 400])
def test_only_recognized_range_limit_bisects(store, monkeypatch, status):
    clock, spans = Clock(), []
    monkeypatch.setattr(census, "time", clock)

    def transport(request):
        payload = json.loads(request.content)
        query = payload["params"][0]
        span = int(query["toBlock"], 16) - int(query["fromBlock"], 16) + 1
        spans.append(span)
        if span > 4:
            return httpx.Response(status, json={"jsonrpc": "2.0", "id": payload["id"],
                                               "error": {"code": -32005, "message": "query returned more than 3000 results"}})
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], "result": [event()]})

    rpc = census.Rpc(store, "https://rpc.test", delay=1, transport=httpx.MockTransport(transport))
    try:
        assert census.Collector(store, rpc).step("exchange")
        assert spans == [8, 4]
        assert clock.now >= 2
        assert store.db.execute("SELECT end_block FROM ranges WHERE stream='exchange'").fetchone()[0] == census.EXCHANGE_START + 3
    finally:
        rpc.client.close()


@pytest.mark.parametrize("message", ["rate limit exceeded", "too many requests"])
def test_rpc_rate_limit_code_does_not_bisect(store, monkeypatch, message):
    clock, params = Clock(), []
    monkeypatch.setattr(census, "time", clock)

    def transport(request):
        payload = json.loads(request.content)
        params.append(payload["params"])
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"],
                                        "error": {"code": -32005, "message": message}})

    rpc = census.Rpc(store, "https://rpc.test", delay=1, retry_attempts=3,
                     transport=httpx.MockTransport(transport))
    try:
        with pytest.raises(census.RpcError) as exc:
            census.Collector(store, rpc).step("exchange")
        assert exc.value.code == -32005 and exc.value.retryable
        assert len(params) == 3 and params[0] == params[1] == params[2]
        assert store.db.execute("SELECT next_block,window FROM streams WHERE name='exchange'").fetchone() == (census.EXCHANGE_START, 8)
        assert store.db.execute("SELECT COUNT(*) FROM ranges").fetchone()[0] == 0
    finally:
        rpc.client.close()


def test_unrecognized_provider_error_fails_without_range_retries(store):
    def transport(request):
        payload = json.loads(request.content)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"],
                                        "error": {"code": -32602, "message": "invalid parameter"}})

    rpc = census.Rpc(store, "https://rpc.test", delay=0, transport=httpx.MockTransport(transport))
    try:
        with pytest.raises(census.RpcError) as exc:
            census.Collector(store, rpc).step("exchange")
        assert exc.value.code == -32602 and not exc.value.retryable and not exc.value.range_limit
        assert rpc.calls == 1
        assert store.db.execute("SELECT COUNT(*) FROM ranges").fetchone()[0] == 0
    finally:
        rpc.client.close()


def test_provider_cooldown_survives_restart_after_exhausted_attempt(store, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(census, "time", clock)
    rpc = census.Rpc(store, "https://rpc.test", delay=1, retry_attempts=1,
                     transport=httpx.MockTransport(lambda _: httpx.Response(429, text="throttled", headers={"Retry-After": "5"})))
    with pytest.raises(census.RpcError):
        rpc.call("eth_chainId", [])
    rpc.client.close()
    assert store.get("rpc_retry_after_until")
    recorded = json.loads(store.db.execute("SELECT error FROM requests").fetchone()[0])
    assert recorded["retry_after_s"] == 5 and recorded["http_status"] == 429
    starts = []

    def transport(request):
        starts.append(clock.now)
        payload = json.loads(request.content)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], "result": "0x89"})

    before = clock.now
    resumed = census.Rpc(store, "https://rpc.test", delay=1, transport=httpx.MockTransport(transport))
    try:
        assert resumed.call("eth_chainId", [])[0] == "0x89"
        assert 4 <= starts[0] - before <= 5
    finally:
        resumed.client.close()


def test_interrupted_request_is_recorded_without_accepting_range(store):
    def transport(_request):
        raise KeyboardInterrupt

    rpc = census.Rpc(store, "https://rpc.test", delay=0, transport=httpx.MockTransport(transport))
    try:
        with pytest.raises(KeyboardInterrupt):
            census.Collector(store, rpc).step("exchange")
        assert "Interrupted" in store.db.execute("SELECT error FROM requests").fetchone()[0]
        assert store.db.execute("SELECT COUNT(*) FROM ranges").fetchone()[0] == 0
    finally:
        rpc.client.close()


def test_export_preserves_exact_csv_bytes_and_avoids_artifact_rereads(store, tmp_path, monkeypatch):
    rpc = rpc_for(store, lambda payload: [event()])
    assert census.Collector(store, rpc).step("exchange")
    rpc.client.close()
    original_open = Path.open

    def guarded_open(path, mode="r", *args, **kwargs):
        if path.name in ("wallets.csv", "wallet-evidence.jsonl") and "r" in mode:
            pytest.fail("Export reread an artifact solely for hashing")
        return original_open(path, mode, *args, **kwargs)

    with monkeypatch.context() as context:
        context.setattr(Path, "open", guarded_open)
        census.export(store, tmp_path, "partial")
    transaction = event()["transactionHash"]
    expected = ("chain_id,wallet_address,first_observed_block,last_observed_block,observed_roles,evidence_transaction_hash,evidence_log_index\r\n"
                f"137,{ALICE},{census.EXCHANGE_START},{census.EXCHANGE_START},clob_order_maker,{transaction},0\r\n"
                f"137,{BOB},{census.EXCHANGE_START},{census.EXCHANGE_START},clob_fill_counterparty,{transaction},0\r\n").encode()
    assert (tmp_path / "wallets.csv").read_bytes() == expected
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["request_provenance_cache"]["misses"] == 1
    assert manifest["request_provenance_cache"]["hits"] == 1
    for name, digest in manifest["artifact_sha256"].items():
        assert hashlib.sha256((tmp_path / name).read_bytes()).hexdigest() == digest


def test_resume_does_not_postpone_due_export_for_another_interval(tmp_path, monkeypatch):
    clock, real_rpc = Clock(), census.Rpc
    monkeypatch.setattr(census, "time", clock)

    def transport(request):
        payload = json.loads(request.content)
        if payload["method"] == "eth_chainId":
            result = "0x89"
        elif payload["method"] == "eth_getBlockByNumber":
            result = {"number": payload["params"][0], "hash": "0x" + word(9), "timestamp": "0x1"}
        else:
            assert payload["method"] == "eth_getLogs"
            result = []
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], "result": result})

    monkeypatch.setattr(census, "Rpc", lambda store, url, **kwargs: real_rpc(
        store, url, transport=httpx.MockTransport(transport), **kwargs))
    options = ["--output", str(tmp_path), "--request-delay", "0", "--streams", "factory",
               "--max-blocks", "1", "--end-block", str(census.FACTORY_START + 3)]
    assert census.main(options + ["--max-requests", "2"]) == 0
    assert census.main(options + ["--export-only"]) == 0
    pointer_path = tmp_path / "latest-export.json"
    pointer = json.loads(pointer_path.read_text())
    pointer["published_at"] = (datetime.now(timezone.utc) - timedelta(hours=13)).isoformat()
    pointer_path.write_text(json.dumps(pointer))
    real_export, exports = census.export, []

    def record_export(store, directory, state, error=None, **kwargs):
        status = real_export(store, directory, state, error, **kwargs)
        exports.append(next(s["accepted_ranges"] for s in status["streams"] if s["name"] == "factory"))
        return status

    monkeypatch.setattr(census, "export", record_export)
    assert census.main(options + ["--max-requests", "4"]) == 0
    assert exports == [1]  # Only one range elapsed; the old publication made it due.
    assert json.loads(pointer_path.read_text())["generation"] != pointer["generation"]
