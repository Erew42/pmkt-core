from __future__ import annotations

import copy
from datetime import datetime
import hashlib
import json
import sqlite3

import httpx
import pytest

import test_wallet_registry_rpc as fixtures

census = fixtures.census


class ReviewClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        assert 0 <= seconds <= 900, "RPC attempted an unbounded provider wait"
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch):
    clock = ReviewClock()

    class ReplayDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.fromtimestamp(1704067200 + clock.now, tz=tz)

    monkeypatch.setattr(census, "time", clock)
    monkeypatch.setattr(census, "datetime", ReplayDateTime)
    return clock


@pytest.fixture
def store(tmp_path):
    store = census.Store(tmp_path / "registry.sqlite3")
    end = census.EXCHANGE_START + 7
    store.initialize(
        {"chain_id": 137, "rpc": "https://rpc.test", "end_block": end,
         "max_blocks": 8,
         "starts": {"factory": census.FACTORY_START,
                    "amm": census.FACTORY_START,
                    "exchange": census.EXCHANGE_START}},
        {"number": end, "hash": "0x" + fixtures.word(9), "timestamp_s": 1},
    )
    yield store
    store.db.close()


def stream_checkpoint(store, stream="exchange"):
    return store.db.execute(
        "SELECT next_block,window FROM streams WHERE name=?", (stream,),
    ).fetchone()


def rpc_result(request, result):
    payload = json.loads(request.content)
    return httpx.Response(
        200, json={"jsonrpc": "2.0", "id": payload["id"], "result": result},
    )


@pytest.mark.parametrize("malformation", ["data", "topics", "padding"])
@pytest.mark.parametrize("known_pool", [False, True])
def test_amm_abi_validation_applies_only_to_attributed_pools(
    store, clock, malformation, known_pool,
):
    with store.db:
        store.db.execute(
            "UPDATE streams SET next_block=? WHERE name='factory'",
            (census.FACTORY_START + 8,),
        )
        if known_pool:
            pool = census.decode_pool(
                fixtures.event(census.CREATION, block=census.FACTORY_START),
            )
            store.db.execute(
                "INSERT INTO pools VALUES (?,?)", (pool["address"], census.dumps(pool)),
            )
    log = fixtures.event(census.BUY, block=census.FACTORY_START)
    if malformation == "data":
        log["data"] = "0x00"  # Valid RPC bytes, wrong AMM ABI length.
    elif malformation == "topics":
        log["topics"] = [census.BUY]  # Valid envelope, wrong AMM topic count.
    else:
        log["topics"][1] = "0x01" + log["topics"][1][4:]
    rpc = census.Rpc(
        store, "https://rpc.test", delay=0,
        transport=httpx.MockTransport(lambda request: rpc_result(request, [log])),
    )
    try:
        collector = census.Collector(store, rpc)
        if known_pool:
            with pytest.raises(ValueError):
                collector.step("amm")
            assert stream_checkpoint(store, "amm") == (census.FACTORY_START, 8)
            assert store.db.execute("SELECT COUNT(*) FROM ranges").fetchone()[0] == 0
        else:
            assert collector.step("amm")
            assert stream_checkpoint(store, "amm")[0] == census.FACTORY_START + 8
            assert store.db.execute(
                "SELECT qualified_logs FROM ranges WHERE stream='amm'",
            ).fetchone()[0] == 0
        assert store.db.execute("SELECT COUNT(*) FROM wallets").fetchone()[0] == 0
    finally:
        rpc.client.close()


def test_unrelated_amm_still_requires_a_valid_rpc_envelope(store, clock):
    with store.db:
        store.db.execute(
            "UPDATE streams SET next_block=? WHERE name='factory'",
            (census.FACTORY_START + 8,),
        )
    log = fixtures.event(census.BUY, block=census.FACTORY_START)
    log["removed"] = True
    rpc = census.Rpc(
        store, "https://rpc.test", delay=0,
        transport=httpx.MockTransport(lambda request: rpc_result(request, [log])),
    )
    try:
        with pytest.raises(ValueError, match="removed"):
            census.Collector(store, rpc).step("amm")
        assert stream_checkpoint(store, "amm") == (census.FACTORY_START, 8)
        assert store.db.execute("SELECT COUNT(*) FROM ranges").fetchone()[0] == 0
    finally:
        rpc.client.close()


@pytest.mark.parametrize("failure", ["read_timeout", 502, 504])
def test_repeated_capacity_failure_recovers_by_reducing_getlogs_span(
    store, clock, failure,
):
    queries = []

    def transport(request):
        payload = json.loads(request.content)
        query = payload["params"][0]
        queries.append(copy.deepcopy(query))
        span = int(query["toBlock"], 16) - int(query["fromBlock"], 16) + 1
        if span > 4:
            if failure == "read_timeout":
                raise httpx.ReadTimeout("wide query timed out", request=request)
            return httpx.Response(failure, text="gateway could not serve wide query")
        return rpc_result(request, [])

    rpc = census.Rpc(
        store, "https://rpc.test", delay=1,
        transport=httpx.MockTransport(transport),
    )
    try:
        assert census.Collector(store, rpc).step("exchange")
        assert [int(q["toBlock"], 16) - int(q["fromBlock"], 16) + 1
                for q in queries] == [8, 8, 4]
        assert {q["fromBlock"] for q in queries} == {hex(census.EXCHANGE_START)}
        assert store.db.execute(
            "SELECT start_block,end_block FROM ranges WHERE stream='exchange'",
        ).fetchone() == (census.EXCHANGE_START, census.EXCHANGE_START + 3)
    finally:
        rpc.client.close()


def test_reduced_capacity_window_survives_budget_exit_and_reopen(store, clock):
    queries = []

    def transport(request):
        queries.append(json.loads(request.content)["params"][0])
        raise httpx.ReadTimeout("wide query timed out", request=request)

    rpc = census.Rpc(
        store, "https://rpc.test", delay=0, budget=2,
        transport=httpx.MockTransport(transport),
    )
    try:
        with pytest.raises(census.RequestBudgetReached):
            census.Collector(store, rpc).step("exchange")
        assert len(queries) == 2
        assert stream_checkpoint(store) == (census.EXCHANGE_START, 4)
        assert store.db.execute("SELECT COUNT(*) FROM ranges").fetchone()[0] == 0
    finally:
        rpc.client.close()
    resumed = census.Store(store.path)
    try:
        assert stream_checkpoint(resumed) == (census.EXCHANGE_START, 4)
    finally:
        resumed.db.close()


def test_capacity_reduction_stops_at_single_block_without_advancing_checkpoint(store, clock):
    spans = []

    def transport(request):
        query = json.loads(request.content)["params"][0]
        spans.append(int(query["toBlock"], 16) - int(query["fromBlock"], 16) + 1)
        raise httpx.ReadTimeout("even one block timed out", request=request)

    rpc = census.Rpc(
        store, "https://rpc.test", delay=1,
        transport=httpx.MockTransport(transport),
    )
    try:
        with pytest.raises(census.RpcError):
            census.Collector(store, rpc).step("exchange")
        assert spans == [8, 8, 4, 4, 2, 2, 1, 1]
        assert stream_checkpoint(store) == (census.EXCHANGE_START, 1)
        assert store.db.execute("SELECT COUNT(*) FROM ranges").fetchone()[0] == 0
    finally:
        rpc.client.close()


def test_connection_timeouts_retry_without_shrinking_query(store, clock):
    queries = []

    def transport(request):
        queries.append(json.loads(request.content)["params"])
        if len(queries) < 3:
            raise httpx.ConnectTimeout("could not connect", request=request)
        return rpc_result(request, [])

    rpc = census.Rpc(
        store, "https://rpc.test", delay=1,
        transport=httpx.MockTransport(transport),
    )
    try:
        assert census.Collector(store, rpc).step("exchange")
        assert len(queries) == 3 and queries[0] == queries[1] == queries[2]
        assert stream_checkpoint(store)[0] == census.EXCHANGE_START + 8
    finally:
        rpc.client.close()


@pytest.mark.parametrize("status", [200, 400, 429, 502, 504])
def test_retry_after_overrides_conflicting_range_errors(store, clock, status):
    queries, starts = [], []

    def transport(request):
        payload = json.loads(request.content)
        queries.append(payload["params"])
        starts.append(clock.now)
        if len(queries) <= 2:
            return httpx.Response(
                status, headers={"Retry-After": "3"},
                json={"jsonrpc": "2.0", "id": payload["id"],
                      "error": {"code": -32005,
                                "message": "query returned more than 3000 results"}},
            )
        return rpc_result(request, [])

    rpc = census.Rpc(
        store, "https://rpc.test", delay=1,
        transport=httpx.MockTransport(transport),
    )
    try:
        assert census.Collector(store, rpc).step("exchange")
        assert len(queries) == 3 and queries[0] == queries[1] == queries[2]
        assert all(b - a >= 3 for a, b in zip(starts, starts[1:]))
        assert stream_checkpoint(store)[0] == census.EXCHANGE_START + 8
    finally:
        rpc.client.close()


@pytest.mark.parametrize("retry_after", ["3600", "Mon, 01 Jan 2024 01:00:00 GMT"])
def test_long_retry_after_is_preserved_without_sleep_or_additional_requests(
    store, clock, retry_after,
):
    requests = []

    def transport(request):
        requests.append(request)
        return httpx.Response(429, text="quota exhausted", headers={"Retry-After": retry_after})

    rpc = census.Rpc(
        store, "https://rpc.test", delay=0,
        transport=httpx.MockTransport(transport),
    )
    try:
        with pytest.raises(census.RpcError):
            rpc.call("eth_chainId", [])
        assert len(requests) == 1 and clock.now == 0
        assert store.get("rpc_retry_after_until") == 1704067200 + 3600
        recorded = json.loads(store.db.execute("SELECT error FROM requests").fetchone()[0])
        assert recorded["retry_after_s"] == 3600 and recorded["http_status"] == 429
    finally:
        rpc.client.close()


def test_resume_long_provider_cooldown_fails_before_transport(store, clock):
    store.put("rpc_retry_after_until", 1704067200 + 3600)

    def transport(_request):
        pytest.fail("Resume bypassed the provider cooldown")

    rpc = census.Rpc(
        store, "https://rpc.test", delay=0,
        transport=httpx.MockTransport(transport),
    )
    try:
        with pytest.raises(census.RpcError):
            rpc.call("eth_chainId", [])
        assert clock.now == 0 and rpc.calls == 0
        assert store.get("rpc_retry_after_until") == 1704067200 + 3600
        assert store.db.execute("SELECT COUNT(*) FROM requests").fetchone()[0] == 0
    finally:
        rpc.client.close()


def test_rejected_resumed_cooldown_preserves_original_absolute_deadline(store, clock):
    deadline = 1704067200 + 3600
    store.put("rpc_retry_after_until", deadline)

    def transport(_request):
        pytest.fail("Resume bypassed the provider cooldown")

    rpc = census.Rpc(
        store, "https://rpc.test", delay=0,
        transport=httpx.MockTransport(transport),
    )
    clock.now = 1
    try:
        with pytest.raises(census.RpcError):
            rpc.call("eth_chainId", [])
        assert store.get("rpc_retry_after_until") == deadline
        assert rpc.calls == 0 and clock.now == 1
        assert store.db.execute("SELECT COUNT(*) FROM requests").fetchone()[0] == 0
    finally:
        rpc.client.close()


def test_resume_short_remaining_cooldown_waits_until_full_provider_deadline(store, clock):
    store.put("rpc_retry_after_until", 1704067200 + 3600)
    clock.now = 2800
    starts = []

    def transport(request):
        starts.append(clock.now)
        return rpc_result(request, "0x89")

    rpc = census.Rpc(
        store, "https://rpc.test", delay=0,
        transport=httpx.MockTransport(transport),
    )
    try:
        assert rpc.call("eth_chainId", [])[0] == "0x89"
        assert starts == [3600] and rpc.calls == 1
    finally:
        rpc.client.close()


def test_compute_unit_throttle_retries_identical_query(store, clock):
    # Provider example: https://www.alchemy.com/docs/reference/throughput
    params = []

    def transport(request):
        payload = json.loads(request.content)
        params.append(payload["params"])
        if len(params) < 3:
            return httpx.Response(
                200, json={"jsonrpc": "2.0", "id": payload["id"],
                           "error": {"code": 429, "message":
                                     "Your app has exceeded its compute units per second capacity."}},
            )
        return rpc_result(request, [])

    rpc = census.Rpc(
        store, "https://rpc.test", delay=1,
        transport=httpx.MockTransport(transport),
    )
    try:
        assert census.Collector(store, rpc).step("exchange")
        assert len(params) == 3 and params[0] == params[1] == params[2]
        assert stream_checkpoint(store)[0] == census.EXCHANGE_START + 8
        errors = store.db.execute("SELECT error FROM requests WHERE error IS NOT NULL").fetchall()
        assert [json.loads(row[0])["rpc_code"] for row in errors] == [429, 429]
    finally:
        rpc.client.close()


def test_documented_tenderly_result_limit_splits_despite_invalid_params_code(store, clock):
    # https://docs.tenderly.co/node-rpc/overview#public-endpoint-limits
    spans = []

    def transport(request):
        payload = json.loads(request.content)
        query = payload["params"][0]
        span = int(query["toBlock"], 16) - int(query["fromBlock"], 16) + 1
        spans.append(span)
        if span > 4:
            return httpx.Response(
                200, json={"jsonrpc": "2.0", "id": payload["id"],
                           "error": {"code": -32602,
                                     "message": "Query returned more than 3000 results."}},
            )
        return rpc_result(request, [])

    rpc = census.Rpc(
        store, "https://rpc.test", delay=0,
        transport=httpx.MockTransport(transport),
    )
    try:
        assert census.Collector(store, rpc).step("exchange")
        assert spans == [8, 4]
        assert stream_checkpoint(store)[0] == census.EXCHANGE_START + 4
    finally:
        rpc.client.close()


@pytest.mark.parametrize("code,message", [
    (-32600, "invalid request"),
    (-32601, "method not found"),
    (-32602, "invalid parameter"),
])
def test_explicit_invalid_request_fails_but_retains_retry_after(store, clock, code, message):
    queries = []

    def transport(request):
        payload = json.loads(request.content)
        queries.append(payload["params"])
        return httpx.Response(
            200, headers={"Retry-After": "4"},
            json={"jsonrpc": "2.0", "id": payload["id"],
                  "error": {"code": code, "message": message}},
        )

    rpc = census.Rpc(
        store, "https://rpc.test", delay=0,
        transport=httpx.MockTransport(transport),
    )
    try:
        with pytest.raises(census.RpcError):
            census.Collector(store, rpc).step("exchange")
        assert len(queries) == 1 and clock.now == 0
        assert stream_checkpoint(store) == (census.EXCHANGE_START, 8)
        assert store.get("rpc_retry_after_until") == 1704067200 + 4
        recorded = json.loads(store.db.execute("SELECT error FROM requests").fetchone()[0])
        assert recorded["rpc_code"] == code and recorded["retry_after_s"] == 4
    finally:
        rpc.client.close()


def test_throttle_resets_consecutive_capacity_failure_streak(store, clock):
    queries = []

    def transport(request):
        payload = json.loads(request.content)
        queries.append(payload["params"])
        if len(queries) in (1, 3):
            return httpx.Response(502, text="gateway query timeout")
        if len(queries) == 2:
            return httpx.Response(429, text="throttled", headers={"Retry-After": "3"})
        return rpc_result(request, [])

    rpc = census.Rpc(
        store, "https://rpc.test", delay=1,
        transport=httpx.MockTransport(transport),
    )
    try:
        assert census.Collector(store, rpc).step("exchange")
        assert len(queries) == 4 and all(q == queries[0] for q in queries)
        assert stream_checkpoint(store)[0] == census.EXCHANGE_START + 8
    finally:
        rpc.client.close()


@pytest.mark.parametrize("method", ["eth_chainId", "eth_getBlockByNumber"])
def test_startup_transient_failures_retry_without_range_adaptation(store, clock, method):
    queries = []

    def transport(request):
        payload = json.loads(request.content)
        queries.append((payload["method"], payload["params"]))
        if len(queries) <= 2:
            return httpx.Response(504, text="gateway timeout")
        return rpc_result(request, "0x89")

    rpc = census.Rpc(
        store, "https://rpc.test", delay=0,
        transport=httpx.MockTransport(transport),
    )
    params = [] if method == "eth_chainId" else ["0x1", False]
    try:
        assert rpc.call(method, params)[0] == "0x89"
        assert queries == [(method, params)] * 3
        assert stream_checkpoint(store) == (census.EXCHANGE_START, 8)
    finally:
        rpc.client.close()


@pytest.mark.parametrize("existing", [
    "missing", "directory", "uninitialized_database", "empty_registry_schema",
])
def test_invalid_offline_export_does_not_create_or_modify_dataset(
    tmp_path, monkeypatch, existing,
):
    output = tmp_path / "registry"
    if existing != "missing":
        output.mkdir()
    if existing == "uninitialized_database":
        db = sqlite3.connect(output / "registry.sqlite3")
        try:
            db.execute("CREATE TABLE unrelated (id INTEGER)")
            db.commit()
        finally:
            db.close()
    elif existing == "empty_registry_schema":
        empty = census.Store(output / "registry.sqlite3")
        empty.db.close()
    before = {path.relative_to(tmp_path): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in tmp_path.rglob("*") if path.is_file()}
    monkeypatch.setattr(census, "Rpc", lambda *a, **k: pytest.fail("Offline export constructed RPC"))
    try:
        result = census.main(["--output", str(output), "--export-only"])
    except SystemExit as exc:
        result = exc.code
    assert result != 0
    assert {path.relative_to(tmp_path): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in tmp_path.rglob("*") if path.is_file()} == before
    if existing == "missing":
        assert not output.exists()


def test_single_block_tail_capacity_failure_does_not_repeat_a_nominally_wide_window(store, clock):
    config = store.get("config")
    config["end_block"] = census.EXCHANGE_START
    store.put("config", config)
    spans = []

    def transport(request):
        query = json.loads(request.content)["params"][0]
        spans.append(int(query["toBlock"], 16) - int(query["fromBlock"], 16) + 1)
        raise httpx.ReadTimeout("single block timed out", request=request)

    rpc = census.Rpc(store, "https://rpc.test", delay=0, transport=httpx.MockTransport(transport))
    try:
        with pytest.raises(census.RpcError):
            census.Collector(store, rpc).step("exchange")
        assert spans == [1, 1]
        assert stream_checkpoint(store) == (census.EXCHANGE_START, 8)
        assert store.db.execute("SELECT COUNT(*) FROM ranges").fetchone()[0] == 0
    finally:
        rpc.client.close()


def test_offline_export_preflight_reads_configuration_from_pending_wal(tmp_path, monkeypatch):
    output = tmp_path / "registry"
    output.mkdir()
    store = census.Store(output / "registry.sqlite3")
    store.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    end = census.EXCHANGE_START + 7
    store.initialize(
        {"chain_id": 137, "rpc": "https://rpc.test", "end_block": end,
         "max_blocks": 8,
         "starts": {"factory": census.FACTORY_START,
                    "amm": census.FACTORY_START,
                    "exchange": census.EXCHANGE_START}},
        {"number": end, "hash": "0x" + fixtures.word(9), "timestamp_s": 1},
    )
    assert (output / "registry.sqlite3-wal").stat().st_size > 0
    # Prove an immutable main-file-only probe would miss this valid configuration.
    main_file = sqlite3.connect(store.path.resolve().as_uri() + "?immutable=1", uri=True)
    try:
        assert main_file.execute("SELECT value FROM metadata WHERE key='config'").fetchone() is None
    finally:
        main_file.close()
    monkeypatch.setattr(census, "Rpc", lambda *a, **k: pytest.fail("Offline export constructed RPC"))
    try:
        assert census.main(["--output", str(output), "--export-only"]) == 0
        pointer = json.loads((output / "latest-export.json").read_text())
        manifest_path = output / pointer["files"]["manifest.json"]
        assert json.loads(manifest_path.read_text())["status"]["config"]["end_block"] == end
    finally:
        store.db.close()
