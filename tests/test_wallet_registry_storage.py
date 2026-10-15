from __future__ import annotations

import json
import sqlite3
import sys
import time
from pathlib import Path

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import collect_polymarket_wallets_rpc as census  # noqa: E402
import wallet_registry_export as exporter  # noqa: E402


@pytest.fixture
def store(tmp_path):
    result = census.Store(tmp_path / "registry.sqlite3")
    config = {"chain_id": 137, "rpc": "https://rpc.test", "end_block": census.EXCHANGE_START + 100,
              "max_blocks": 8, "starts": {"factory": census.FACTORY_START,
                                           "amm": census.FACTORY_START, "exchange": census.EXCHANGE_START}}
    result.initialize(config, {"number": config["end_block"], "hash": "0x" + "ab" * 32, "timestamp_s": 1})
    yield result
    result.db.close()


def exchange_log(block, *, maker="11", taker="22"):
    return {"address": next(iter(census.EXCHANGES)), "blockNumber": hex(block),
            "blockHash": "0x" + "ab" * 32, "transactionHash": "0x" + "cd" * 32,
            "transactionIndex": "0x0", "logIndex": "0x0", "removed": False,
            "topics": [census.FILLED_V1, "0x" + "00" * 32,
                       "0x" + "00" * 12 + maker * 20, "0x" + "00" * 12 + taker * 20],
            "data": "0x" + "".join(f"{value:064x}" for value in (0, 42, 2, 3, 0))}


def request(store):
    return store.record_request("https://rpc.test", "eth_getLogs", [{"fromBlock": "0x1"}], "2026-10-07T00:00:00Z")


def commit(store, block, *, maker="11", taker="22"):
    ident = request(store)
    log = exchange_log(block, maker=maker, taker=taker)
    store.commit_range("exchange", block, block, 8, ident, 1, [log])


def assert_exact_counts(store):
    status = store.status("running")
    for table in ("wallets", "pools", "requests"):
        assert status[table] == store.db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    totals = {row[0]: row[1:] for row in store.db.execute(
        "SELECT stream,count(*),sum(unique_logs),sum(qualified_logs) FROM ranges GROUP BY stream")}
    for stream in status["streams"]:
        expected = totals.get(stream["name"], (0, 0, 0))
        assert (stream["accepted_ranges"], stream["unique_logs_observed"], stream["qualified_logs_observed"]) == expected


def test_status_keeps_exact_counters_without_repeated_full_table_scans(store):
    statements = []
    store.db.set_trace_callback(statements.append)
    start = census.EXCHANGE_START
    commit(store, start)
    commit(store, start + 1, taker="33")
    for _ in range(5):
        status = store.status("running")
        assert (status["wallets"], status["requests"]) == (3, 2)
    assert not any("COUNT(" in statement.upper() or "SUM(" in statement.upper() for statement in statements)
    assert_exact_counts(store)
    row = store.db.execute("SELECT first_block,last_block,roles FROM wallets WHERE address=?", ("0x" + "11" * 20,)).fetchone()
    assert row == (start, start + 1, 1)


def test_factory_pool_counter_and_earlier_amm_witness_remain_exact(store):
    pool = "0x" + "33" * 20
    creation = {"address": census.FACTORY, "blockNumber": hex(census.FACTORY_START),
                "topics": [census.CREATION, "0x" + "00" * 12 + "11" * 20,
                           "0x" + "00" * 12 + census.CTF[2:], "0x" + "00" * 12 + "22" * 20],
                "data": "0x" + "".join(f"{value:064x}" for value in (int(pool, 16), 96, 0, 1, 42))}
    store.commit_range("factory", census.FACTORY_START, census.FACTORY_START, 8, request(store), 1, [creation])
    commit(store, census.EXCHANGE_START)
    trade = {"address": pool, "blockNumber": hex(census.FACTORY_START),
             "transactionHash": "0x" + "ef" * 32, "logIndex": "0x0",
             "topics": [census.BUY, "0x" + "00" * 12 + "11" * 20, "0x" + "00" * 32],
             "data": "0x" + "".join(f"{value:064x}" for value in (2, 0, 3))}
    store.commit_range("amm", census.FACTORY_START, census.FACTORY_START, 8, request(store), 1, [trade])
    assert_exact_counts(store)
    row = store.db.execute("SELECT first_block,last_block,roles,evidence FROM wallets WHERE address=?", ("0x" + "11" * 20,)).fetchone()
    assert row[:3] == (census.FACTORY_START, census.EXCHANGE_START, 5)
    assert json.loads(row[3])["log"]["transactionHash"] == trade["transactionHash"]
    assert store.status("running")["pools"] == 1


def test_multiple_lookup_batches_count_overlapping_and_new_wallets_and_preserve_witness(store):
    statements = []
    store.db.set_trace_callback(statements.append)
    start = census.EXCHANGE_START
    for block, identifiers in ((start, range(1, 1101)), (start + 1, range(551, 1651))):
        logs = []
        for index, identifier in enumerate(identifiers):
            log = exchange_log(block)
            log["topics"][2] = f"0x{identifier:064x}"
            log["topics"][3] = "0x" + "00" * 12 + next(iter(census.EXCHANGES))[2:]
            log["logIndex"] = hex(index)
            log["transactionHash"] = f"0x{(block << 20) + index:064x}"
            logs.append(log)
        store.commit_range("exchange", block, block, 8, request(store), len(logs), logs)
    assert_exact_counts(store)
    status = store.status("running")
    assert (status["wallets"], status["requests"]) == (1650, 2)
    row = store.db.execute("SELECT first_block,last_block,roles,evidence FROM wallets WHERE address=?",
                           (f"0x{600:040x}",)).fetchone()
    assert row[:3] == (start, start + 1, 1)
    assert json.loads(row[3])["log"]["logIndex"] == hex(599)
    assert json.loads(row[3])["log"]["blockNumber"] == hex(start)
    newcomer = store.db.execute("SELECT first_block,last_block FROM wallets WHERE address=?",
                                (f"0x{1500:040x}",)).fetchone()
    assert newcomer == (start + 1, start + 1)
    probes = [statement for statement in statements if statement.startswith("SELECT address FROM wallets WHERE address IN")]
    assert len(probes) == 6
    assert max(statement.count(",") + 1 for statement in probes) == 500


def test_failed_range_transaction_does_not_advance_counters_or_checkpoint(store):
    store.db.execute("CREATE TRIGGER reject_range BEFORE INSERT ON ranges BEGIN SELECT RAISE(ABORT,'fixture rollback'); END")
    with pytest.raises(sqlite3.IntegrityError, match="fixture rollback"):
        commit(store, census.EXCHANGE_START)
    assert_exact_counts(store)
    assert store.status("running")["wallets"] == 0
    assert store.db.execute("SELECT next_block FROM streams WHERE name='exchange'").fetchone() == (census.EXCHANGE_START,)
    store.db.execute("DROP TRIGGER reject_range")
    commit(store, census.EXCHANGE_START)
    assert_exact_counts(store)


def test_failed_request_transaction_does_not_increment_count(store):
    store.db.execute("CREATE TRIGGER reject_request BEFORE INSERT ON requests BEGIN SELECT RAISE(ABORT,'fixture rollback'); END")
    with pytest.raises(sqlite3.IntegrityError):
        request(store)
    assert store.status("running")["requests"] == 0
    store.db.execute("DROP TRIGGER reject_request")
    request(store)
    assert_exact_counts(store)


@pytest.mark.parametrize("operation,key", [("request", "requests"), ("range", "wallets")])
def test_interruption_after_durable_commit_recounts_once(store, operation, key):
    class InterruptedCounts(dict):
        def __setitem__(self, name, value):
            if name == key:
                raise KeyboardInterrupt
            return super().__setitem__(name, value)

    if operation == "range":
        ident = request(store)
    store._counts = InterruptedCounts(store._counts)
    with pytest.raises(KeyboardInterrupt):
        if operation == "request":
            request(store)
        else:
            store.commit_range("exchange", census.EXCHANGE_START, census.EXCHANGE_START, 8, ident, 1,
                               [exchange_log(census.EXCHANGE_START)])
    assert_exact_counts(store)
    statements = []
    store.db.set_trace_callback(statements.append)
    store.status("partial")
    assert not any("COUNT(" in statement.upper() for statement in statements)


def test_reopen_and_readonly_snapshot_seed_current_counts(store):
    commit(store, census.EXCHANGE_START)
    for readonly in (False, True):
        reopened = census.Store(store.path, readonly=readonly)
        try:
            assert_exact_counts(reopened)
            if readonly:
                assert reopened.maintain_wal()["state"] == "readonly"
                with pytest.raises(sqlite3.OperationalError):
                    request(reopened)
                assert_exact_counts(reopened)
        finally:
            reopened.db.close()


def test_malformed_range_preserves_exact_status(store):
    ident = request(store)
    log = exchange_log(census.EXCHANGE_START)
    log["topics"] = [census.FILLED_V1]
    with pytest.raises(ValueError):
        store.commit_range("exchange", census.EXCHANGE_START, census.EXCHANGE_START, 8, ident, 1, [log])
    assert_exact_counts(store)
    assert store.status("running")["wallets"] == 0


def fill_wal_payload(store):
    with store.db:
        store.db.execute("CREATE TABLE payloads(value BLOB)")
        store.db.executemany("INSERT INTO payloads VALUES (?)", [(bytes(4096),) for _ in range(100)])
    store.db.execute("PRAGMA wal_autocheckpoint=0")
    store.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")


def grow_wal(store):
    with store.db:
        store.db.execute("UPDATE payloads SET value=?", (b"updated" * 600,))


def test_wal_maintenance_defers_active_reader_without_waiting_then_reclaims(store):
    fill_wal_payload(store)
    reader = sqlite3.connect(store.path.resolve().as_uri() + "?mode=ro", uri=True)
    reader.execute("BEGIN")
    reader.execute("SELECT count(*) FROM payloads").fetchone()
    grow_wal(store)
    store.db.execute("PRAGMA busy_timeout=10000")
    started = time.monotonic()
    result = store.maintain_wal(max_bytes=65536)
    assert time.monotonic() - started < 1
    assert result["state"] == "deferred" and result["checkpoint_busy"] == 1
    assert store.db.execute("PRAGMA busy_timeout").fetchone() == (10000,)
    grow_wal(store)  # The writer still commits with the old snapshot pinned.
    reader.close()
    result = store.maintain_wal(max_bytes=65536)
    assert result["state"] == "reclaimed" and result["wal_after_bytes"] == 0


@pytest.mark.parametrize("failure", ["budget", "cancel"])
def test_snapshot_abort_releases_reader_and_writer_reclaims_for_next_copy(store, tmp_path, monkeypatch, failure):
    fill_wal_payload(store)
    monkeypatch.setattr(exporter, "SNAPSHOT_PAGES", 1)
    calls = 0

    def cancel():
        nonlocal calls
        calls += 1
        if calls == 2:
            grow_wal(store)
        return failure == "cancel" and calls >= 2

    expected = exporter.ExportCancelled if failure == "cancel" else exporter.SnapshotBudgetExceeded
    with pytest.raises(expected):
        exporter.snapshot_database(store.path, tmp_path / "aborted.sqlite3", max_wal_bytes=65536,
                                   min_free_bytes=0, pause_s=0, cancelled=cancel)
    assert not (tmp_path / "aborted.sqlite3").exists()
    assert store.maintain_wal(max_bytes=65536)["state"] == "reclaimed"
    exporter.snapshot_database(store.path, tmp_path / "next.sqlite3", max_wal_bytes=65536,
                               min_free_bytes=0, pause_s=0)
    assert (tmp_path / "next.sqlite3").is_file()


def test_journal_size_limit_reclaims_capacity_on_normal_reset(store):
    assert store.db.execute("PRAGMA journal_size_limit").fetchone() == (64 * 1024**2,)
    fill_wal_payload(store)
    store.db.execute("PRAGMA journal_size_limit=65536")
    reader = sqlite3.connect(store.path)
    reader.execute("BEGIN")
    reader.execute("SELECT count(*) FROM payloads").fetchone()
    grow_wal(store)
    assert Path(str(store.path) + "-wal").stat().st_size > 65536
    reader.close()
    store.db.execute("PRAGMA wal_autocheckpoint=1")
    for number in range(2):
        with store.db:
            store.db.execute("UPDATE metadata SET value=? WHERE key='created_at'", (json.dumps(f"reset-{number}"),))
    assert Path(str(store.path) + "-wal").stat().st_size <= 65536


def test_startup_reclaims_retained_capacity_without_new_collection_data(store, monkeypatch):
    fill_wal_payload(store)
    grow_wal(store)
    store.db.execute("PRAGMA wal_checkpoint(PASSIVE)")
    assert Path(str(store.path) + "-wal").stat().st_size > 65536
    real = census.Store.maintain_wal
    monkeypatch.setattr(census.Store, "maintain_wal", lambda self, max_bytes=65536: real(self, max_bytes=65536))
    reopened = census.Store(store.path)
    try:
        assert Path(str(store.path) + "-wal").stat().st_size == 0
        assert_exact_counts(reopened)
    finally:
        reopened.db.close()


def test_bounded_provenance_preload_preserves_exact_artifacts_and_fallback(store, tmp_path):
    commit(store, census.EXCHANGE_START)
    artifacts = {}
    for budget in (0, 64, 32 * 1024**2):
        destination = tmp_path / str(budget)
        destination.mkdir()
        census.export(store, destination, "running", provenance_preload_bytes=budget)
        artifacts[budget] = tuple((destination / name).read_bytes() for name in exporter.ARTIFACTS[:2])
        manifest = json.loads((destination / "manifest.json").read_text())
        preload = manifest["request_provenance_preload"]
        assert preload["estimated_bytes"] <= budget
        if budget < 100:
            assert preload["entries"] == 0 and preload["database_lookup_misses"] == 1
        else:
            assert preload["entries"] == 1 and preload["database_lookup_misses"] == 0
    assert artifacts[0] == artifacts[64] == artifacts[32 * 1024**2]
