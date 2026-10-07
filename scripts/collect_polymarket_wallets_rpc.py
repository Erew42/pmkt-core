#!/usr/bin/env python3
"""Resumable, public Polygon trade-event wallet census (not a trade ledger)."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import shutil
import signal
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import urlsplit

import httpx

CHAIN_ID = 137
FACTORY_START = 4_023_693
EXCHANGE_START = 33_605_403
FACTORY = "0x8b9805a2f595b6705e74f7310829f2d299d21522"
CTF = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
EXCHANGES = {
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e": "clob_v1",
    "0xc5d563a36ae78145c45a50134d48a1215220f80a": "clob_v1_neg_risk",
    "0xe111180000d2663c0091e4f400237545b87b996b": "ctf_v2",
    "0xe2222d279d744050d28e00520010520000310f59": "ctf_v2_neg_risk",
    "0xe3333700ca9d93003f00f0f71f8515005f6c00aa": "combo_v2",
}
# These are structural counterparties, not trading-wallet identities. Contract
# wallets in general remain eligible; neither EOAs nor owners are inferred.
INFRASTRUCTURE = set(EXCHANGES) | {
    FACTORY, CTF,
    "0x12121212006e4cd160d18e3f00711da5c3372600",
    "0xada100db00ca00073811820692005400218fce1f",
    "0xada2005600dec949baf300f4c6120000bdb6eaab",
    "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296",
    "0x006f54f7f9a22e0000cc2ab60031000000ae9fef",
}
ZERO = "0x" + "0" * 40
FILLED_V1 = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
FILLED_V2 = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
BUY = "0x4f62630f51608fc8a7603a9391a5101e58bd7c276139366fc107dc3b67c3dcf8"
SELL = "0xadcf2a240ed9300d681d9a3f5382b6c1beed1b7e46643e0c7b42cbe6e2d766b4"
CREATION = "0x92e0912d3d7f3192cad5c7ae3b47fb97f9c465c1dd12a5c24fd901ddb3905f43"
SIGNATURES = {
    FILLED_V1: "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)",
    FILLED_V2: "OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)",
    BUY: "FPMMBuy(address,uint256,uint256,uint256,uint256)",
    SELL: "FPMMSell(address,uint256,uint256,uint256,uint256)",
    CREATION: "FixedProductMarketMakerCreation(address,address,address,address,bytes32[],uint256)",
}
SOURCES = [
    "https://docs.polymarket.com/resources/contracts",
    "https://github.com/Polymarket/polymarket-subgraph/blob/main/networks.yaml",
    "https://github.com/Polymarket/polymarket-subgraph/tree/main/abis",
    "https://github.com/Polymarket/ctf-exchange/blob/main/src/exchange/interfaces/ITrading.sol",
    "https://github.com/Polymarket/ctf-exchange-v2/blob/ccc0596074f4dfd62c944fbca4de252893b82b4b/src/exchange/interfaces/ITrading.sol",
    "https://polygonscan.com/address/0x7345C6842b244926125ed4054905cAc49620B5dc#code",
]
ROLES = {1: "clob_order_maker", 2: "clob_fill_counterparty", 4: "amm_buyer", 8: "amm_seller"}
READ_METHODS = {"eth_chainId", "eth_blockNumber", "eth_getBlockByNumber", "eth_getLogs"}
MAX_RETRY_AFTER_S = 900.0
COLLECTOR_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def hex_bytes(value: Any, size: int | None = None) -> bytes:
    if not isinstance(value, str) or not re.fullmatch(r"0x(?:[0-9a-fA-F]{2})*", value):
        raise ValueError("Invalid hex byte string")
    result = bytes.fromhex(value[2:])
    if size is not None and len(result) != size:
        raise ValueError(f"Expected {size} bytes")
    return result


def quantity(value: Any) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)", value):
        raise ValueError("Invalid RPC quantity")
    return int(value, 16)


def address_word(value: str) -> str:
    word = hex_bytes(value, 32)
    if any(word[:12]):
        raise ValueError("Nonzero ABI address padding")
    return "0x" + word[12:].hex()


def words(value: str, count: int | None = None) -> list[int]:
    raw = hex_bytes(value)
    if len(raw) % 32 or (count is not None and len(raw) != count * 32):
        raise ValueError("Invalid ABI data length")
    return [int.from_bytes(raw[i:i + 32], "big") for i in range(0, len(raw), 32)]


def validate_logs(result: Any, start: int, end: int, topics: set[str],
                  addresses: set[str] | None) -> list[dict[str, Any]]:
    if not isinstance(result, list):
        raise ValueError("eth_getLogs did not return a list")
    unique: dict[tuple[str, int], dict[str, Any]] = {}
    blocks: dict[int, str] = {}
    for log in result:
        if not isinstance(log, dict) or log.get("removed") is not False:
            raise ValueError("Missing/removed log")
        address = "0x" + hex_bytes(log.get("address"), 20).hex()
        block = quantity(log.get("blockNumber"))
        block_hash = "0x" + hex_bytes(log.get("blockHash"), 32).hex()
        hex_bytes(log.get("transactionHash"), 32)
        quantity(log.get("transactionIndex"))
        index = quantity(log.get("logIndex"))
        hex_bytes(log.get("data"))
        event_topics = log.get("topics")
        if not isinstance(event_topics, list) or not event_topics:
            raise ValueError("Missing event topics")
        for topic in event_topics:
            hex_bytes(topic, 32)
        if not start <= block <= end or event_topics[0].lower() not in topics:
            raise ValueError("RPC log outside requested range/topics")
        if addresses is not None and address not in addresses:
            raise ValueError("RPC log outside requested addresses")
        if block in blocks and blocks[block] != block_hash:
            raise ValueError("Conflicting block hashes")
        blocks[block] = block_hash
        # A log index is unique within a canonical block, including across txs.
        key = (block_hash, index)
        if key in unique and unique[key] != log:
            raise ValueError("Conflicting duplicate log")
        unique[key] = log
    return sorted(unique.values(), key=lambda x: (quantity(x["blockNumber"]), quantity(x["logIndex"])))


def decode_pool(log: dict[str, Any]) -> dict[str, Any] | None:
    topics = log["topics"]
    if len(topics) != 4 or topics[0].lower() != CREATION or log["address"].lower() != FACTORY:
        raise ValueError("Invalid factory event")
    creator, ctf, collateral = (address_word(t) for t in topics[1:])
    data = words(log["data"])
    # Canonical ABI head: pool, offset(96), fee; then array length and IDs.
    if len(data) < 5 or data[1] != 96 or data[3] < 1 or len(data) != 4 + data[3]:
        raise ValueError("Invalid factory dynamic array")
    pool = address_word("0x" + data[0].to_bytes(32, "big").hex())
    if pool == ZERO or collateral == ZERO or creator == ZERO:
        raise ValueError("Zero factory event address")
    if ctf != CTF:
        return None
    return {"address": pool, "creation_block": quantity(log["blockNumber"]),
            "creator": creator, "collateral": collateral,
            "condition_ids": ["0x" + n.to_bytes(32, "big").hex() for n in data[4:]],
            "creation_log": log}


def decode_traders(log: dict[str, Any], pool: dict[str, Any] | None = None) -> list[tuple[str, int]]:
    topic = log["topics"][0].lower()
    address = log["address"].lower()
    if topic in (FILLED_V1, FILLED_V2):
        if address not in EXCHANGES or len(log["topics"]) != 4:
            raise ValueError("Invalid exchange event")
        expected = FILLED_V1 if EXCHANGES[address].startswith("clob_v1") else FILLED_V2
        if topic != expected:
            raise ValueError("Unexpected ABI for exchange")
        data = words(log["data"], 5 if topic == FILLED_V1 else 7)
        if topic == FILLED_V1:
            # Zero collateral payment can be valid. Require traded token quantity.
            if (data[0] == 0) == (data[1] == 0):
                raise ValueError("Invalid exchange asset pair")
            token_amount = data[2] if data[0] != 0 else data[3]
        else:
            if data[0] not in (0, 1):
                raise ValueError("Invalid exchange side")
            token_amount = data[3] if data[0] == 0 else data[2]
        if token_amount == 0:
            return []
        traders = [(address_word(log["topics"][2]), 1), (address_word(log["topics"][3]), 2)]
    elif topic in (BUY, SELL):
        if pool is None:
            return []  # Unrelated contracts do not share the factory pool ABI.
        if len(log["topics"]) != 3:
            raise ValueError("Invalid AMM event topics")
        trader = address_word(log["topics"][1])
        data = words(log["data"], 3)
        if quantity(log["blockNumber"]) < pool["creation_block"]:
            raise ValueError("AMM trade predates pool creation")
        traders = [(trader, 4 if topic == BUY else 8)] if data[2] > 0 else []
    else:
        raise ValueError("Unsupported trade event")
    return [(wallet, role) for wallet, role in traders if wallet != ZERO and wallet not in INFRASTRUCTURE]


class Store:
    def __init__(self, path: Path, *, readonly: bool = False):
        self.path = path
        self.readonly = readonly
        if readonly:
            self.db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
            self.db.execute("PRAGMA query_only=ON")
            self.db.execute("PRAGMA cache_size=-65536")
            self._seed_status_totals()
            return
        self.db = sqlite3.connect(path)
        if self.db.execute("PRAGMA journal_mode=WAL").fetchone()[0].lower() != "wal":
            self.db.close()
            raise ValueError("Wallet collection requires SQLite WAL mode")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA cache_size=-65536")
        # Reclaim allocated WAL capacity at the next ordinary reset. A snapshot
        # reader can grow the WAL past its budget; keeping that capacity forever
        # would otherwise reject every later snapshot even after it is released.
        self.db.execute("PRAGMA journal_size_limit=67108864")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY, rpc TEXT, method TEXT, params TEXT,
                started_at TEXT, completed_at TEXT, http_status INTEGER,
                response_sha256 TEXT, response_bytes INTEGER, error TEXT);
            CREATE TABLE IF NOT EXISTS streams (
                name TEXT PRIMARY KEY, start_block INTEGER, next_block INTEGER, window INTEGER);
            CREATE TABLE IF NOT EXISTS ranges (
                stream TEXT, start_block INTEGER, end_block INTEGER, request_id INTEGER,
                returned_logs INTEGER, unique_logs INTEGER, qualified_logs INTEGER,
                PRIMARY KEY(stream, start_block));
            CREATE TABLE IF NOT EXISTS pools (address TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS wallets (
                address TEXT PRIMARY KEY, first_block INTEGER, last_block INTEGER,
                roles INTEGER, evidence TEXT NOT NULL);
        """)
        self.maintain_wal()
        self._seed_status_totals()

    def maintain_wal(self, max_bytes: int = 64 * 1024**2) -> dict[str, Any]:
        """Reclaim large retained WAL capacity without waiting on reader locks.

        Only the collector writer calls this at startup or after worker release.
        A busy reader defers reclamation; ordinary writes still reset the WAL
        under journal_size_limit later. Checkpoint transfer I/O can take time,
        but busy_timeout=0 prevents an unbounded lock wait behind an exporter.
        """
        if self.readonly:
            return {"state": "readonly"}
        wal = Path(str(self.path) + "-wal")
        try:
            before = wal.stat().st_size
        except FileNotFoundError:
            before = 0
        result = {"state": "not_needed", "wal_before_bytes": before, "wal_after_bytes": before}
        if before <= max_bytes:
            return result
        if self.db.in_transaction:
            return {**result, "state": "deferred", "reason": "Active writer transaction"}
        previous_timeout = self.db.execute("PRAGMA busy_timeout").fetchone()[0]
        try:
            self.db.execute("PRAGMA busy_timeout=0")
            busy, frames, checkpointed = self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            try:
                after = wal.stat().st_size
            except FileNotFoundError:
                after = 0
            return {"state": "reclaimed" if not busy and after <= max_bytes else "deferred",
                    "wal_before_bytes": before, "wal_after_bytes": after,
                    "checkpoint_busy": busy, "log_frames": frames, "checkpointed_frames": checkpointed}
        except sqlite3.OperationalError as exc:
            return {**result, "state": "deferred", "reason": str(exc)}
        finally:
            self.db.execute(f"PRAGMA busy_timeout={previous_timeout}")

    def _seed_status_totals(self) -> None:
        # This Store owns all production mutations. Resume (and immutable export
        # copies) pay the full scans once, rather than after every accepted range.
        self._counts = {table: self.db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                        for table in ("wallets", "pools", "requests")}
        self._range_totals = {row[0]: row[1:] for row in self.db.execute(
            "SELECT stream,COUNT(*),COALESCE(SUM(unique_logs),0),COALESCE(SUM(qualified_logs),0) "
            "FROM ranges GROUP BY stream")}
        self._status_totals_valid = True

    def _ensure_status_totals(self) -> None:
        if not self._status_totals_valid:
            self._seed_status_totals()

    def record_request(self, rpc: str, method: str, params: list[Any], started_at: str) -> int:
        self._ensure_status_totals()
        self._status_totals_valid = False
        with self.db:
            cursor = self.db.execute("INSERT INTO requests (rpc,method,params,started_at) VALUES (?,?,?,?)",
                                     (rpc, method, dumps(params), started_at))
            request_id = int(cursor.lastrowid or 0)
        self._counts["requests"] += 1
        self._status_totals_valid = True
        return request_id

    def get(self, key: str) -> Any:
        row = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def put(self, key: str, value: Any) -> None:
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES (?,?)", (key, dumps(value)))

    def initialize(self, config: dict[str, Any], anchor: dict[str, Any]) -> None:
        existing = self.get("config")
        if existing is not None:
            if existing != config:
                raise ValueError("Resume configuration differs; use the original options or a new output directory")
            if self.get("anchor") != anchor:
                raise ValueError("End-block anchor changed; canonical history must be reviewed")
            return
        with self.db:
            for key, value in (("config", config), ("anchor", anchor), ("created_at", utc_now())):
                self.db.execute("INSERT INTO metadata VALUES (?,?)", (key, dumps(value)))
            for name, start in config["starts"].items():
                self.db.execute("INSERT INTO streams VALUES (?,?,?,?)", (name, start, start, config["max_blocks"]))

    def commit_range(self, stream: str, start: int, end: int, window: int,
                     request_id: int, returned: int, logs: list[dict[str, Any]]) -> None:
        # Decode everything before the transaction: no checkpoint after a bad log.
        pools: list[dict[str, Any]] = []
        observations: dict[str, dict[str, Any]] = {}
        qualified = 0
        pool_cache: dict[str, Any] = {}
        for log in logs:
            if stream == "factory":
                pool = decode_pool(log)
                if pool:
                    pool["request_id"] = request_id
                    pools.append(pool)
                    qualified += 1
                continue
            pool = None
            if stream == "amm":
                address = log["address"].lower()
                if address not in pool_cache:
                    row = self.db.execute("SELECT payload FROM pools WHERE address=?", (address,)).fetchone()
                    pool_cache[address] = json.loads(row[0]) if row else None
                pool = pool_cache[address]
            traders = decode_traders(log, pool)
            qualified += bool(traders)
            block = quantity(log["blockNumber"])
            for wallet, role in traders:
                if wallet not in observations:
                    evidence = {"wallet_address": wallet, "role": ROLES[role], "chain_id": CHAIN_ID,
                                "request_id": request_id, "log": log}
                    if pool:
                        evidence["pool"] = pool
                    observations[wallet] = {"first": block, "last": block, "roles": role, "evidence": evidence}
                else:
                    observations[wallet]["last"] = max(observations[wallet]["last"], block)
                    observations[wallet]["roles"] |= role
        self._ensure_status_totals()
        self._status_totals_valid = False
        new_wallets = new_pools = 0
        with self.db:
            row = self.db.execute("SELECT next_block FROM streams WHERE name=?", (stream,)).fetchone()
            if row is None or row[0] != start:
                raise ValueError("Noncontiguous checkpoint")
            if stream == "amm":
                factory_end = self.db.execute("SELECT next_block-1 FROM streams WHERE name='factory'").fetchone()[0]
                if end > factory_end:
                    raise ValueError("Factory discovery must precede AMM scanning")
            for pool in pools:
                old = self.db.execute("SELECT payload FROM pools WHERE address=?", (pool["address"],)).fetchone()
                if old and json.loads(old[0])["creation_log"] != pool["creation_log"]:
                    raise ValueError("Conflicting pool creation")
                inserted = self.db.execute("INSERT OR IGNORE INTO pools VALUES (?,?)", (pool["address"], dumps(pool)))
                new_pools += inserted.rowcount
            entries = [(wallet, item["first"], item["last"], item["roles"], dumps(item["evidence"]))
                       for wallet, item in observations.items()]
            for offset in range(0, len(entries), 500):
                batch = entries[offset:offset + 500]
                # Covered primary-key lookups count genuinely new wallets while
                # preserving the original UPSERT and its witness/role semantics.
                placeholders = ",".join("?" for _ in batch)
                existing = len(self.db.execute(
                    f"SELECT address FROM wallets WHERE address IN ({placeholders})",
                    [item[0] for item in batch]).fetchall())
                new_wallets += len(batch) - existing
                self.db.executemany("""
                    INSERT INTO wallets VALUES (?,?,?,?,?) ON CONFLICT(address) DO UPDATE SET
                      evidence=CASE WHEN excluded.first_block < wallets.first_block
                                    THEN excluded.evidence ELSE wallets.evidence END,
                      first_block=MIN(wallets.first_block, excluded.first_block),
                      last_block=MAX(wallets.last_block, excluded.last_block),
                      roles=wallets.roles | excluded.roles
                    """, batch)
            self.db.execute("INSERT INTO ranges VALUES (?,?,?,?,?,?,?)",
                            (stream, start, end, request_id, returned, len(logs), qualified))
            self.db.execute("UPDATE streams SET next_block=?, window=? WHERE name=?", (end + 1, window, stream))
        # Only update invocation counters after SQLite has durably committed.
        self._counts["wallets"] += new_wallets
        self._counts["pools"] += new_pools
        ranges, unique, qualified_total = self._range_totals.get(stream, (0, 0, 0))
        self._range_totals[stream] = (ranges + 1, unique + len(logs), qualified_total + qualified)
        self._status_totals_valid = True

    def status(self, state: str, error: str | None = None) -> dict[str, Any]:
        self._ensure_status_totals()
        config = self.get("config")
        streams = []
        for name, start, next_block, window in self.db.execute("SELECT * FROM streams ORDER BY name"):
            totals = self._range_totals.get(name, (0, 0, 0))
            streams.append({"name": name, "start_block": start, "next_block": next_block,
                            "last_scanned_block": next_block - 1 if next_block > start else None,
                            "window_blocks": window, "accepted_ranges": totals[0],
                            "unique_logs_observed": totals[1], "qualified_logs_observed": totals[2]})
        return {"state": state, "updated_at": utc_now(), "error": error,
                "wallets": self._counts["wallets"], "pools": self._counts["pools"],
                "requests": self._counts["requests"],
                "streams": streams, "config": config, "anchor": self.get("anchor"),
                "configured_ranges_scanned": bool(config) and all(s["next_block"] > config["end_block"] for s in streams),
                "lifetime_coverage_proven": False}


class RpcError(RuntimeError):
    def __init__(self, message: str, *, http_status: int | None = None,
                 code: int | None = None, retry_after: float | None = None,
                 retryable: bool = False, range_limit: bool = False,
                 capacity_failure: bool = False):
        super().__init__(message)
        self.http_status, self.code = http_status, code
        self.retry_after, self.retryable, self.range_limit = retry_after, retryable, range_limit
        self.capacity_failure = capacity_failure


class RpcCooldownExceeded(RpcError):
    """Stop without making an early request or sleeping for an excessive header."""


class RequestBudgetReached(RuntimeError):
    pass


def retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        if value.strip().isdigit():
            result = float(value)
            if not math.isfinite(result):
                raise RpcCooldownExceeded("Retry-After exceeds the supported duration")
            return result
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())
    except (ValueError, TypeError, OverflowError):
        return None


def is_range_limit(message: str) -> bool:
    text = message.lower()
    # Generic rate/compute limits must not turn into repeated range bisections.
    if is_throttle(message):
        return False
    return (any(s in text for s in ("query returned more than", "too many results", "response size")) or
            ("range" in text and any(s in text for s in ("limit", "exceed", "too large", "maximum"))) or
            ("result" in text and any(s in text for s in ("limit exceeded", "maximum", "too large"))))


def is_throttle(message: str, code: int | None = None) -> bool:
    text = message.lower()
    return code == 429 or any(s in text for s in (
        "rate limit", "too many requests", "requests per", "compute units per second",
        "compute units/s", "throughput limit"))


class Rpc:
    def __init__(self, store: Store, url: str, *, delay: float = 1.0,
                 budget: int | None = None, transport: httpx.BaseTransport | None = None,
                 retry_attempts: int = 6):
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("Use a public HTTPS RPC URL without credentials/query/fragment")
        if not math.isfinite(delay) or delay < 0 or retry_attempts < 1:
            raise ValueError("Invalid RPC pacing/retry bounds")
        self.store, self.url, self.delay, self.budget = store, url, delay, budget
        # No environment profiles, proxy credentials, cookies or auth providers.
        self.client = httpx.Client(timeout=45, transport=transport, trust_env=False, follow_redirects=False)
        self.calls = 0
        self.last_call = 0.0
        self.retry_attempts = retry_attempts
        cooldown = store.get("rpc_retry_after_until")
        self.resume_wait_s = (max(0.0, cooldown - datetime.now(timezone.utc).timestamp())
                              if isinstance(cooldown, (int, float)) else 0.0)
        self.wait_state: dict[str, Any] = {}
        self.on_wait: Any = None
        self.timings = {"limiter_wait_s": 0.0, "network_s": 0.0, "retry_wait_s": 0.0,
                        "retries": 0}

    def call(self, method: str, params: list[Any]) -> tuple[Any, int]:
        if method not in READ_METHODS:
            raise ValueError("RPC method is outside the public read allowlist")
        capacity_failures = 0
        for attempt in range(self.retry_attempts):
            try:
                return self._call_once(method, params)
            except RpcError as exc:
                if isinstance(exc, RpcCooldownExceeded):
                    raise  # A saved deadline must not be extended on resume.
                if not exc.retryable and exc.retry_after is None:
                    raise
                capacity_failures = capacity_failures + 1 if exc.capacity_failure else 0
                wait = exc.retry_after if exc.retry_after is not None else min(60.0, 2.0 ** attempt)
                # Persist cooldown before a budget exit, interruption, or service
                # restart; a new invocation must not bypass the last 429 header.
                self.store.put("rpc_retry_after_until", datetime.now(timezone.utc).timestamp() + wait)
                self.resume_wait_s = wait
                self.wait_state = {"reason": "provider_cooldown" if exc.retry_after is not None else "backoff",
                                   "remaining_s": wait, "not_before": self.store.get("rpc_retry_after_until")}
                if wait > MAX_RETRY_AFTER_S:
                    raise RpcCooldownExceeded(
                        f"Provider Retry-After {wait:g}s exceeds the {MAX_RETRY_AFTER_S:g}s wait limit; "
                        "full cooldown retained, no early retry", http_status=exc.http_status,
                        code=exc.code, retry_after=wait) from exc
                if not exc.retryable:
                    raise
                if method == "eth_getLogs" and capacity_failures >= 2:
                    exc.range_limit = True
                    raise
                if attempt + 1 >= self.retry_attempts:
                    raise
                # An invocation budget includes every attempt; don't wait if exhausted.
                if self.budget is not None and self.calls >= self.budget:
                    raise RequestBudgetReached("Invocation request budget reached") from exc
                started = time.monotonic()
                if self.on_wait:
                    self.on_wait()
                time.sleep(wait)
                self.resume_wait_s = 0.0
                self.wait_state = {}
                self.timings["retry_wait_s"] += time.monotonic() - started
                self.timings["retries"] += 1
        raise ValueError("RPC retry attempts must be positive")

    def _call_once(self, method: str, params: list[Any]) -> tuple[Any, int]:
        if self.budget is not None and self.calls >= self.budget:
            raise RequestBudgetReached("Invocation request budget reached")
        waited = time.monotonic()
        if self.resume_wait_s:
            self.wait_state = {"reason": "resumed_cooldown", "remaining_s": self.resume_wait_s,
                               "not_before": self.store.get("rpc_retry_after_until")}
            if self.resume_wait_s > MAX_RETRY_AFTER_S:
                raise RpcCooldownExceeded(
                    f"Remaining provider cooldown {self.resume_wait_s:g}s exceeds the {MAX_RETRY_AFTER_S:g}s wait limit; "
                    "no RPC request made", retry_after=self.resume_wait_s)
            if self.on_wait:
                self.on_wait()
            time.sleep(self.resume_wait_s)
            self.resume_wait_s = 0.0
            self.wait_state = {}
        time.sleep(max(0, self.delay - (time.monotonic() - self.last_call)))
        self.timings["limiter_wait_s"] += time.monotonic() - waited
        request_id = self.store.record_request(self.url, method, params, utc_now())
        self.calls += 1
        self.last_call = time.monotonic()
        status, digest, length, error = None, None, None, None
        network_started = time.monotonic()
        try:
            self.client.cookies.clear()
            response = self.client.post(self.url, json={"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
            status, length = response.status_code, len(response.content)
            digest = hashlib.sha256(response.content).hexdigest()
            if status != 200:
                # Error pages need not be JSON. Recognized range limits may also
                # use HTTP 400, while throttling/server failures retain priority.
                detail = None
                try:
                    body = response.json()
                    detail = body.get("error") if isinstance(body, dict) else None
                except ValueError:
                    pass
                message = str(detail.get("message", "")) if isinstance(detail, dict) else ""
                code = detail.get("code") if isinstance(detail, dict) else None
                header_present = "retry-after" in response.headers
                throttle = status == 429 or is_throttle(message, code)
                transient = status in (408, 425, 429, 500, 502, 503, 504) or throttle or header_present
                invalid_request = code in (-32600, -32601, -32602) and not is_range_limit(message) and not throttle
                raise RpcError(f"RPC HTTP {status}: {message[:500]}" if message else f"RPC HTTP {status}",
                               http_status=status, code=code if isinstance(code, int) else None,
                               retry_after=retry_after_seconds(response.headers.get("retry-after")),
                               retryable=transient and not invalid_request,
                               range_limit=not transient and method == "eth_getLogs" and is_range_limit(message),
                               capacity_failure=method == "eth_getLogs" and status in (502, 504)
                               and not throttle and not header_present)
            payload = response.json()
            if not isinstance(payload, dict) or payload.get("jsonrpc") != "2.0" or payload.get("id") != request_id:
                raise RpcError(f"Invalid RPC response (HTTP {status})")
            if payload.get("error") is not None or "result" not in payload:
                detail = payload.get("error")
                message = str(detail.get("message", "")) if isinstance(detail, dict) else ""
                code = detail.get("code") if isinstance(detail, dict) else None
                text = message.lower()
                header_present = "retry-after" in response.headers
                throttle = is_throttle(message, code) or (code == -32005 and not is_range_limit(message))
                timeout = any(s in text for s in ("timeout", "timed out"))
                range_limit = method == "eth_getLogs" and is_range_limit(message) and not header_present and not throttle
                invalid_request = code in (-32600, -32601, -32602) and not is_range_limit(message) and not throttle
                retryable = not range_limit and not invalid_request and (header_present or throttle or code == -32603 or any(s in text for s in (
                    "rate limit", "too many requests", "requests per", "temporarily unavailable",
                    "timeout", "timed out", "server busy", "try again")))
                raise RpcError(f"RPC error: {dumps(detail)[:500]}", http_status=status,
                               code=code if isinstance(code, int) else None,
                               retry_after=retry_after_seconds(response.headers.get("retry-after")),
                               retryable=retryable, range_limit=range_limit,
                               capacity_failure=method == "eth_getLogs" and timeout and not header_present and not throttle)
            return payload["result"], request_id
        except RpcError as exc:
            error = dumps({"message": str(exc)[:600], "http_status": exc.http_status,
                           "rpc_code": exc.code, "retry_after_s": exc.retry_after,
                           "retryable": exc.retryable, "range_limit": exc.range_limit,
                           "capacity_failure": exc.capacity_failure})
            raise
        except KeyboardInterrupt:
            error = "Interrupted during RPC request"
            raise
        except (httpx.HTTPError, ValueError) as exc:
            error = str(exc)[:600]
            raise RpcError(error, http_status=status, retryable=isinstance(exc, httpx.TransportError),
                           capacity_failure=method == "eth_getLogs" and isinstance(exc, httpx.ReadTimeout)) from exc
        finally:
            self.timings["network_s"] += time.monotonic() - network_started
            with self.store.db:
                self.store.db.execute("UPDATE requests SET completed_at=?,http_status=?,response_sha256=?,response_bytes=?,error=? WHERE id=?",
                                      (utc_now(), status, digest, length, error, request_id))

    def block(self, number: int) -> dict[str, Any]:
        block, _ = self.call("eth_getBlockByNumber", [hex(number), False])
        if not isinstance(block, dict) or quantity(block.get("number")) != number:
            raise ValueError("Missing/wrong anchor block")
        return {"number": number, "hash": "0x" + hex_bytes(block.get("hash"), 32).hex(),
                "timestamp_s": quantity(block.get("timestamp"))}


class Collector:
    def __init__(self, store: Store, rpc: Rpc, *, log_cap: int = 5000):
        self.store, self.rpc, self.log_cap = store, rpc, log_cap
        self.processing_s = 0.0

    def step(self, stream: str) -> bool:
        config = self.store.get("config")
        start, window = self.store.db.execute("SELECT next_block,window FROM streams WHERE name=?", (stream,)).fetchone()
        limit = config["end_block"]
        if stream == "amm":
            limit = min(limit, self.store.db.execute("SELECT next_block-1 FROM streams WHERE name='factory'").fetchone()[0])
        if start > limit:
            return False
        topics = {CREATION} if stream == "factory" else ({BUY, SELL} if stream == "amm" else {FILLED_V1, FILLED_V2})
        addresses = {FACTORY} if stream == "factory" else (None if stream == "amm" else set(EXCHANGES))
        while True:
            end = min(start + window - 1, limit)
            query: dict[str, Any] = {"fromBlock": hex(start), "toBlock": hex(end), "topics": [sorted(topics)]}
            if addresses is not None:
                query["address"] = sorted(addresses)
            try:
                result, request_id = self.rpc.call("eth_getLogs", [query])
            except RpcError as exc:
                if not exc.range_limit or end == start:
                    raise
                window = max(1, (end - start + 1) // 2)
                with self.store.db:
                    self.store.db.execute("UPDATE streams SET window=? WHERE name=? AND next_block=?", (window, stream, start))
                continue
            if not isinstance(result, list):
                raise ValueError("eth_getLogs did not return a list")
            if len(result) >= self.log_cap:
                if end == start:
                    raise ValueError("Single block reached the log cap; cannot rule out truncation")
                window = max(1, (end - start + 1) // 2)
                with self.store.db:
                    self.store.db.execute("UPDATE streams SET window=? WHERE name=? AND next_block=?", (window, stream, start))
                continue
            processing_started = time.monotonic()
            logs = validate_logs(result, start, end, topics, addresses)
            next_window = min(config["max_blocks"], window * 2) if len(logs) < self.log_cap // 4 else window
            self.store.commit_range(stream, start, end, next_window, request_id, len(result), logs)
            self.processing_s += time.monotonic() - processing_started
            return True


def atomic_json(path: Path, payload: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


class HashingTextWriter:
    """Hash the exact UTF-8 bytes passed to the binary artifact, including CRLF."""

    def __init__(self, handle: BinaryIO):
        self.handle = handle
        self.digest = hashlib.sha256()

    def write(self, value: str) -> int:
        encoded = value.encode("utf-8")
        self.handle.write(encoded)
        self.digest.update(encoded)
        return len(value)


def export(store: Store, directory: Path, state: str, error: str | None = None,
           *, publish_status: bool = True, provenance_preload_bytes: int = 32 * 1024**2) -> dict[str, Any]:
    # The caller owns the writer lock or supplies an immutable snapshot copy.
    export_started = time.monotonic()
    status = store.status(state, error)
    hashes = {}
    csv_path, evidence_path = directory / "wallets.csv", directory / "wallet-evidence.jsonl"

    def decode_provenance(row: tuple[Any, ...]) -> dict[str, Any]:
        request = dict(zip(("rpc", "method", "params", "started_at", "completed_at", "response_sha256"), row))
        request["params"] = json.loads(request["params"])
        return request

    def estimated_size(value: Any) -> int:
        size = sys.getsizeof(value)
        if isinstance(value, dict):
            size += sum(estimated_size(key) + estimated_size(item) for key, item in value.items())
        elif isinstance(value, (list, tuple)):
            size += sum(estimated_size(item) for item in value)
        return size

    # Wallet addresses randomize witness IDs: a small LRU alone repeatedly seeks
    # the same old requests. One sequential pass preloads a bounded prefix, with
    # conservative Python object/container overhead included in the 32 MiB cap.
    if provenance_preload_bytes < 0:
        raise ValueError("Provenance preload budget cannot be negative")
    preload_budget = provenance_preload_bytes
    preloaded: dict[int, dict[str, Any]] = {}
    preload_bytes = database_lookup_misses = 0
    if preload_budget and status["wallets"]:
        provenance_rows = store.db.execute("SELECT id,rpc,method,params,started_at,completed_at,response_sha256 FROM requests ORDER BY id")
        try:
            for row in provenance_rows:
                try:
                    request = decode_provenance(row[1:])
                except (ValueError, TypeError):
                    # Unused incomplete request records do not invalidate witnesses.
                    # A witness referencing one still fails in the lookup below.
                    continue
                size = estimated_size(request) + sys.getsizeof(row[0]) + 96
                if preload_bytes + size > preload_budget:
                    break
                preloaded[row[0]] = request
                preload_bytes += size
        finally:
            provenance_rows.close()

    @lru_cache(maxsize=4096)
    def request_provenance(request_id: int) -> dict[str, Any]:
        nonlocal database_lookup_misses
        if request_id in preloaded:
            return preloaded[request_id]
        row = store.db.execute("SELECT rpc,method,params,started_at,completed_at,response_sha256 FROM requests WHERE id=?",
                               (request_id,)).fetchone()
        if row is None:
            raise ValueError(f"Missing witness request {request_id}")
        database_lookup_misses += 1
        return decode_provenance(row)

    with csv_path.with_suffix(".csv.tmp").open("wb") as csv_raw, evidence_path.with_suffix(".jsonl.tmp").open("wb") as evidence_raw:
        csv_file, evidence_file = HashingTextWriter(csv_raw), HashingTextWriter(evidence_raw)
        writer = csv.writer(csv_file)
        writer.writerow(["chain_id", "wallet_address", "first_observed_block", "last_observed_block", "observed_roles", "evidence_transaction_hash", "evidence_log_index"])
        for address, first, last, roles, raw in store.db.execute("SELECT * FROM wallets ORDER BY address"):
            evidence = json.loads(raw)
            log = evidence["log"]
            evidence["request"] = request_provenance(evidence["request_id"])
            evidence_file.write(dumps(evidence) + "\n")
            writer.writerow([CHAIN_ID, address, first, last, "|".join(v for k, v in ROLES.items() if roles & k), log["transactionHash"], quantity(log["logIndex"])])
        for handle in (csv_raw, evidence_raw):
            handle.flush()
            os.fsync(handle.fileno())
        hashes = {csv_path.name: csv_file.digest.hexdigest(), evidence_path.name: evidence_file.digest.hexdigest()}
    for path in (csv_path, evidence_path):
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.replace(path)
    manifest = {"dataset": "polymarket_polygon_wallet_registry.rpc.v1", "created_at": store.get("created_at"),
                "status": status, "artifact_sha256": hashes, "event_signatures": SIGNATURES,
                "contracts": {"exchanges": EXCHANGES, "factory": FACTORY, "conditional_tokens": CTF},
                "sources": SOURCES, "source_inventory_checked_at": "2026-10-01",
                "collector_versions": store.get("collector_versions"),
                "grain": "One observed trading-contract wallet address per chain; not a person or signer",
                "evidence": "One earliest observed raw trade log per wallet; AMM witnesses include factory creation logs. Request bodies and response hashes are retained in registry.sqlite3; full RPC responses are not retained.",
                "coverage": {"lifetime_coverage_proven": False, "assurance": "provider_observed",
                             "limitations": ["RPC providers may silently omit logs; independent full-history coverage is not proven.",
                                             "Only the listed contract inventory and event ABIs are covered.",
                                             "AMM pools must originate from the listed factory using the listed CTF; listing on the Polymarket website is not established.",
                                             "First/last blocks are observed bounds within scanned ranges, not lifetime dates.",
                                             "Fill counterparties may be operators/contracts; observed roles distinguish them from signed-order makers.",
                                             "Wallet identity is not resolved to controlling EOAs or linked accounts."]},
                "export_timings_s": {"status_and_serialization_s": time.monotonic() - export_started},
                "request_provenance_cache": request_provenance.cache_info()._asdict(),
                "request_provenance_preload": {"entries": len(preloaded), "estimated_bytes": preload_bytes,
                                               "budget_bytes": preload_budget,
                                               "database_lookup_misses": database_lookup_misses}}
    atomic_json(directory / "manifest.json", manifest)
    if publish_status:
        atomic_json(directory / "status.json", status)
    return status


def export_snapshot(snapshot: Path, directory: Path, state: str,
                    error: str | None = None) -> dict[str, Any]:
    store = Store(snapshot, readonly=True)
    try:
        return export(store, directory, state, error, publish_status=False)
    finally:
        store.db.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Ignored local dataset directory")
    parser.add_argument("--rpc-url", default="https://polygon.gateway.tenderly.co")
    parser.add_argument("--end-block", type=int, help="Frozen inclusive end; default latest minus 256 blocks")
    parser.add_argument("--exchange-start-block", type=int, default=EXCHANGE_START, help="Override only for explicitly partial pilots")
    parser.add_argument("--max-blocks", type=int, default=100_000)
    parser.add_argument("--log-cap", type=int, default=5000, help="Split responses at/above this suspected truncation threshold")
    parser.add_argument("--request-delay", type=float, default=1.0, help="Minimum seconds between request starts (default: 1)")
    parser.add_argument("--export-interval-s", type=int, default=43200, help="Full export interval in seconds (default: 43200 / 12 hours)")
    parser.add_argument("--export-mode", choices=("sync", "snapshot"), default="sync")
    parser.add_argument("--export-scratch-dir", type=Path, help="Snapshot scratch directory on SSD outside this checkout")
    parser.add_argument("--snapshot-timeout-s", type=float, default=900)
    parser.add_argument("--snapshot-max-wal-bytes", type=int, default=2 * 1024**3)
    parser.add_argument("--snapshot-min-free-bytes", type=int, default=10 * 1024**3)
    parser.add_argument("--snapshot-pause-s", type=float, default=0.01)
    parser.add_argument("--max-requests", type=int, help="Per invocation network budget; checkpoint survives exit")
    parser.add_argument("--streams", default="factory,amm,exchange")
    parser.add_argument("--export-only", action="store_true", help="Export committed registry without any network calls")
    parser.add_argument("--_export-worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--worker-state", default="running", help=argparse.SUPPRESS)
    parser.add_argument("--worker-error", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    streams = args.streams.split(",")
    if (not set(streams) <= {"factory", "amm", "exchange"} or not streams or
            len(streams) != len(set(streams)) or ("amm" in streams and "factory" not in streams)):
        parser.error("Choose distinct factory,amm,exchange streams; AMM requires factory")
    if (args.max_blocks < 1 or args.log_cap < 2 or not math.isfinite(args.request_delay) or
            args.request_delay < 0 or args.export_interval_s < 1 or args.exchange_start_block < EXCHANGE_START or
            (args.max_requests is not None and args.max_requests < 1) or
            not math.isfinite(args.snapshot_timeout_s) or args.snapshot_timeout_s <= 0 or
            args.snapshot_max_wal_bytes < 1 or args.snapshot_min_free_bytes < 0 or
            not math.isfinite(args.snapshot_pause_s) or args.snapshot_pause_s < 0):
        parser.error("Invalid collection bounds")
    if args.export_mode == "snapshot" or args._export_worker:
        if args.export_scratch_dir is None:
            parser.error("Snapshot exports require --export-scratch-dir on SSD outside the checkout")
        checkout = Path(__file__).resolve().parents[1]
        if args.export_scratch_dir.resolve().is_relative_to(checkout):
            parser.error("Snapshot scratch must be outside the checkout")
    from wallet_registry_export import ExportManager, publish_generation, worker_main

    snapshot_options = {"snapshot_timeout_s": args.snapshot_timeout_s,
                        "snapshot_max_wal_bytes": args.snapshot_max_wal_bytes,
                        "snapshot_min_free_bytes": args.snapshot_min_free_bytes,
                        "snapshot_pause_s": args.snapshot_pause_s}
    if args.export_only and not args._export_worker:
        database = args.output / "registry.sqlite3"
        initialized = False
        if database.is_file():
            probe = None
            try:
                wal = database.with_name(database.name + "-wal")
                # SQLite's ordinary read-only WAL connections may create sidecars.
                # With no pending WAL frames the immutable main file is enough;
                # otherwise retain normal WAL visibility for an active writer.
                pending_wal = wal.is_file() and wal.stat().st_size > 0
                uri = database.resolve().as_uri() + ("?mode=ro" if pending_wal else "?mode=ro&immutable=1")
                probe = sqlite3.connect(uri, uri=True)
                probe.execute("PRAGMA query_only=ON")
                tables = {row[0] for row in probe.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if {"metadata", "requests", "streams", "ranges", "pools", "wallets"} <= tables:
                    row = probe.execute("SELECT value FROM metadata WHERE key='config'").fetchone()
                    config = json.loads(row[0]) if row is not None else None
                    initialized = isinstance(config, dict) and bool(config)
            except (sqlite3.DatabaseError, ValueError):
                pass
            finally:
                if probe is not None:
                    probe.close()
        if not initialized:
            print("No initialized registry to export; collection must initialize it first", file=sys.stderr)
            return 1
    args.output.mkdir(parents=True, exist_ok=True)
    if args._export_worker:
        return worker_main(args.output, args.export_scratch_dir, args.worker_state,
                           args.worker_error, export_snapshot, **snapshot_options)
    # Linux research runner: OS lock is released on exit/crash, with no stale PID lock.
    import fcntl
    with (args.output / ".collector.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("This output directory already has an active collector/exporter")
        store = Store(args.output / "registry.sqlite3")
        rpc = None
        collector = None
        manager = (ExportManager(args.output, args.export_scratch_dir, Path(__file__), **snapshot_options)
                   if args.export_mode == "snapshot" else None)
        state, error = "partial", None
        status_time = 0.0
        sync_export = {"mode": "sync", "stage": "idle", "snapshot_updated_at": None}
        previous_export: dict[str, Any] = {}
        wal_maintenance: dict[str, Any] = {"state": "not_requested"}
        previous_worker_running = False
        try:
            previous_export = json.loads((args.output / "latest-export.json").read_text())
            sync_export.update(snapshot_updated_at=previous_export["snapshot"]["started_at"],
                               latest_generation=previous_export["generation"])
        except (OSError, ValueError, KeyError, TypeError):
            previous_export = {}

        def publish_live() -> dict[str, Any]:
            nonlocal status_time, wal_maintenance, previous_worker_running
            started = time.monotonic()
            live = store.status(state, error)
            live["export"] = manager.poll() if manager else sync_export.copy()
            worker_running = bool(live["export"].get("worker_running"))
            if previous_worker_running and not worker_running and state == "running":
                wal_maintenance = store.maintain_wal()
            previous_worker_running = worker_running
            live["wal_maintenance"] = wal_maintenance
            live["rpc_wait"] = rpc.wait_state.copy() if rpc else {}
            if not manager and sync_export["snapshot_updated_at"]:
                try:
                    live["export"]["snapshot_age_s"] = max(0.0, (datetime.now(timezone.utc) - datetime.fromisoformat(
                        str(sync_export["snapshot_updated_at"]))).total_seconds())
                except (ValueError, TypeError):
                    pass
            live["phase_timings_s"] = {"status_s": status_time,
                                      "validation_and_commit_s": collector.processing_s if collector else 0.0,
                                      "rpc": rpc.timings.copy() if rpc else {}}
            atomic_json(args.output / "status.json", live)
            status_time += time.monotonic() - started
            return live

        def synchronous_export() -> bool:
            # A surviving worker from an interrupted parent still owns this
            # publication lock. A new sync invocation must not race its files.
            with (args.output / ".export.lock").open("a") as export_lock:
                try:
                    fcntl.flock(export_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    sync_export.update(stage="deferred", error="Another exporter holds the publication lock")
                    return False
                generation = args.output / "export-generations" / ("generation-" + uuid.uuid4().hex)
                generation.mkdir(parents=True)
                captured_at = utc_now()
                started = time.monotonic()
                try:
                    snapshot = export(store, generation, state, error, publish_status=False)
                    pointer = publish_generation(args.output, generation,
                                                 {"mode": "sync", "started_at": captured_at,
                                                  "completed_at": utc_now(), "elapsed_s": time.monotonic() - started},
                                                 snapshot)
                    sync_export.update(stage="succeeded", error=pointer.get("publication_warning"),
                                       snapshot_updated_at=captured_at, latest_generation=pointer["generation"])
                    return True
                finally:
                    # A signal just after atomic publication must not delete the
                    # generation that readers have already been told to open.
                    try:
                        published = json.loads((args.output / "latest-export.json").read_text()).get("generation")
                        if published != str(generation.relative_to(args.output)):
                            shutil.rmtree(generation, ignore_errors=True)
                    except FileNotFoundError:
                        shutil.rmtree(generation, ignore_errors=True)
                    except (OSError, ValueError):
                        pass  # Retain rather than delete a potentially published bundle.

        def full_export() -> None:
            nonlocal wal_maintenance
            if manager:
                # An older snapshot must not stand in for the final checkpoint.
                manager.cancel()
                wal_maintenance = store.maintain_wal()
                if not manager.start(state, error):
                    raise RuntimeError("Could not start final snapshot export")
                result = manager.wait()
                wal_maintenance = store.maintain_wal()
                if result.get("stage") != "succeeded" or result.get("worker_exit_code") != 0:
                    raise RuntimeError(result.get("error") or "Final snapshot export failed")
            else:
                if not synchronous_export():
                    raise RuntimeError("Final export blocked by another publisher")

        def stop_requested(_signum: int, _frame: Any) -> None:
            raise KeyboardInterrupt

        old_sigterm = signal.signal(signal.SIGTERM, stop_requested)
        try:
            if args.export_only:
                if store.get("config") is None:
                    raise ValueError("No initialized registry to export")
                state = "exported"
                full_export()
                return 0
            rpc = Rpc(store, args.rpc_url, delay=args.request_delay, budget=args.max_requests)
            rpc.on_wait = publish_live
            chain, _ = rpc.call("eth_chainId", [])
            if quantity(chain) != CHAIN_ID:
                raise ValueError("RPC is not Polygon mainnet")
            old = store.get("config")
            end = args.end_block if args.end_block is not None else (old["end_block"] if old else quantity(rpc.call("eth_blockNumber", [])[0]) - 256)
            if end < FACTORY_START:
                raise ValueError("End block predates the factory")
            config = {"chain_id": CHAIN_ID, "rpc": args.rpc_url, "end_block": end,
                      "max_blocks": args.max_blocks, "log_cap": args.log_cap,
                      "inventory_sha256": hashlib.sha256(dumps([EXCHANGES, FACTORY, CTF, SIGNATURES, sorted(INFRASTRUCTURE)]).encode()).hexdigest(),
                      "starts": {"factory": FACTORY_START, "amm": FACTORY_START, "exchange": args.exchange_start_block}}
            store.initialize(config, rpc.block(end))
            versions = store.get("collector_versions") or []
            versions.append({"started_at": utc_now(), "sha256": COLLECTOR_SHA256,
                             "exporter_sha256": hashlib.sha256(Path(__file__).with_name("wallet_registry_export.py").read_bytes()).hexdigest(),
                             "export_interval_s": args.export_interval_s, "export_mode": args.export_mode,
                             "request_delay_s": args.request_delay,
                             "snapshot_options": snapshot_options if manager else None})
            store.put("collector_versions", versions)
            collector = Collector(store, rpc, log_cap=args.log_cap)
            last_export = time.monotonic()
            previous_time = (previous_export.get("snapshot", {}).get("started_at") if manager else
                             previous_export.get("published_at"))
            if isinstance(previous_time, str):
                try:
                    last_export -= max(0.0, (datetime.now(timezone.utc) - datetime.fromisoformat(previous_time)).total_seconds())
                except (ValueError, TypeError):
                    pass
            state = "running"
            publish_live()
            while True:
                progressed = False
                for stream in streams:
                    progressed |= collector.step(stream)
                if not progressed:
                    if rpc.block(end) != store.get("anchor"):
                        raise ValueError("End-block anchor changed during collection")
                    state = "configured_ranges_scanned" if store.status("done")["configured_ranges_scanned"] else "partial"
                    full_export()
                    break
                selected_pending = any(next_block <= end for name, next_block in store.db.execute(
                    "SELECT name,next_block FROM streams") if name in streams)
                if selected_pending and time.monotonic() - last_export >= args.export_interval_s:
                    if manager:
                        if not manager.poll().get("worker_running"):
                            wal_maintenance = store.maintain_wal()
                        if manager.start("running") or not manager.poll().get("worker_running"):
                            last_export = time.monotonic()
                    else:
                        synchronous_export()
                        last_export = time.monotonic()
                publish_live()
        except RequestBudgetReached as exc:
            state, error = "partial", str(exc)
        except KeyboardInterrupt:
            state, error = "partial", "Interrupted; committed checkpoints retained"
        except Exception as exc:
            state, error = "failed", str(exc)
        finally:
            # Stop and startup failure retain committed ranges without re-exporting.
            if manager:
                manager.cancel()
            if rpc:
                rpc.client.close()
            try:
                print(dumps(publish_live()), flush=True)
            finally:
                store.db.close()
                signal.signal(signal.SIGTERM, old_sigterm)
        return 1 if state == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
