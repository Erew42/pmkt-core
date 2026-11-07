from __future__ import annotations

from pathlib import Path
import re

import pytest

from typer.testing import CliRunner

from pmkt.cli.app import app


PUBLIC_COMMANDS = {
    "backfill-venue-history",
    "collect-books",
    "collect-kalshi-books",
    "compute-features",
    "dataset",
    "ingest-kalshi-markets",
    "ingest-kalshi-historical-markets",
    "ingest-markets",
    "ingest-markets-keyset",
    "markets",
    "query",
    "reconstruct-book-tape",
    "record-topbooks",
    "record-raw-ws",
    "recover-stream-run",
    "resolve-market-resolutions",
    "schema",
    "stream-books",
    "stream-kalshi-books",
}

PRIVATE_COMMANDS = {
    "alerts",
    "canary-submit",
    "deployment",
    "drift-probe",
    "kill-switch",
    "ledger",
    "live-ladder",
    "match",
    "match-markets",
    "polymarket-auth",
    "runtime-backup",
    "soak",
}


def test_public_cli_registers_only_core_command_families() -> None:
    registered = {command.name for command in app.registered_commands}
    registered.update(group.name for group in app.registered_groups)

    assert PUBLIC_COMMANDS <= registered
    assert PRIVATE_COMMANDS.isdisjoint(registered)


def test_public_cli_help_is_available() -> None:
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0, result.output
    for command in PUBLIC_COMMANDS:
        assert command in result.output


@pytest.mark.parametrize("closed,cap,scenario,complete", [
    (None, None, "normal", True),
    ("any", None, "normal", True),
    ("any", 2, "normal", True),
    ("open", None, "normal", True),
    ("closed", None, "normal", True),
    ("any", 1, "cap_open", False),
    ("any", 2, "cap_closed", False),
    ("any", None, "duplicate", False),
    ("any", None, "failed_closed", False),
    ("any", None, "empty_cursor", True),
])
def test_keyset_closed_partitions(monkeypatch, tmp_path, closed, cap, scenario, complete):
    import json
    import httpx
    import pandas as pd
    import pmkt.cli.ingest as ingest
    from pmkt.exchanges.polymarket.gamma import AsyncGammaClient

    seen = []
    def handler(request):
        status = request.url.params.get("closed")
        assert status in {"true", "false"}
        seen.append(status)
        cursor = None
        if scenario == "failed_closed" and status == "true":
            return httpx.Response(200, json={"markets": "bad"})
        if scenario == "empty_cursor" and status == "false" and not request.url.params.get("after_cursor"):
            return httpx.Response(200, json={"markets": [], "next_cursor": "next"})
        if (scenario == "cap_open" and status == "false") or (scenario == "cap_closed" and status == "true"):
            cursor = "next"
        market_id = "same" if scenario == "duplicate" else status
        return httpx.Response(200, json={"markets": [{
            "id": market_id, "conditionId": "condition-" + market_id,
            "question": "Test?", "closed": status == "true",
            "outcomes": '["Yes", "No"]',
            "clobTokenIds": '["yes", "no"]',
        }], "next_cursor": cursor})

    monkeypatch.setattr(ingest, "AsyncGammaClient", lambda **kw: AsyncGammaClient(
        base_url="https://gamma.example", transport=httpx.MockTransport(handler)))
    out = tmp_path / "markets.parquet"
    manifest = tmp_path / "manifest.json"
    evidence = tmp_path / "evidence.parquet"
    args = ["ingest-markets-keyset", "--out", str(out), "--manifest-out", str(manifest),
            "--contract-evidence-out", str(evidence)]
    if closed is not None:
        args += ["--closed", closed]
    if cap is not None:
        args += ["--max-pages", str(cap)]
    result = CliRunner().invoke(app, args)
    assert (result.exit_code == 0) == (scenario not in {"duplicate", "failed_closed"}), result.exception
    metadata = json.loads(manifest.read_text())
    assert metadata["collection_complete"] is complete
    partitions = metadata["partitions"]
    assert [p["closed"] for p in partitions] == ([False] if closed == "open" else [True] if closed == "closed" else [False, True])
    source = json.loads((tmp_path / "evidence.parquet.bundle" / "source_collection_manifest.json").read_text())
    assert source["partitions"] == partitions
    assert source["collection_complete"] is complete
    if complete:
        expected = {"false"} if closed == "open" else {"true"} if closed == "closed" else {"false", "true"}
        assert set(pd.read_parquet(out)["market_id"]) == expected
        assert all(p["complete"] for p in partitions)
    if scenario == "cap_open":
        assert partitions[0]["continuation_cursor"] == "next"
        assert partitions[1]["stop_reason"] == "not_started"
    if scenario == "cap_closed":
        assert partitions[0]["complete"] is True
        assert partitions[1]["continuation_cursor"] == "next"
    if scenario in {"duplicate", "failed_closed"}:
        assert partitions[1]["stop_reason"] == "error"


@pytest.mark.asyncio
@pytest.mark.parametrize("closed_pages", [0, 1])
async def test_keyset_cancellation_keeps_closed_partition_incomplete(
    monkeypatch, tmp_path, closed_pages
):
    import asyncio
    import json

    import httpx
    import pandas as pd
    import pmkt.cli.ingest as ingest
    from pmkt.exchanges.polymarket.gamma import AsyncGammaClient

    waiting = asyncio.Event()
    closed_calls = 0

    async def handler(request):
        nonlocal closed_calls
        closed = request.url.params["closed"] == "true"
        if closed:
            closed_calls += 1
            if closed_calls > closed_pages:
                waiting.set()
                await asyncio.Event().wait()
        market_id = "closed" if closed else "open"
        return httpx.Response(200, json={
            "markets": [{
                "id": market_id, "conditionId": "condition-" + market_id,
                "question": "Test?", "closed": closed,
                "outcomes": '["Yes", "No"]', "clobTokenIds": '["yes", "no"]',
            }],
            "next_cursor": "next" if closed else None,
        })

    monkeypatch.setattr(ingest, "AsyncGammaClient", lambda **kw: AsyncGammaClient(
        base_url="https://gamma.example", transport=httpx.MockTransport(handler)))
    out = tmp_path / "markets.parquet"
    manifest = tmp_path / "manifest.json"
    evidence = tmp_path / "evidence.parquet"
    task = asyncio.create_task(ingest._ingest_markets_keyset_async(
        out, limit=100, closed=None, max_pages=None,
        manifest_out=manifest, contract_evidence_out=evidence,
    ))
    try:
        await asyncio.wait_for(waiting.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5)
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    metadata = json.loads(manifest.read_text())
    source = json.loads((tmp_path / "evidence.parquet.bundle"
                         / "source_collection_manifest.json").read_text())
    for document in (metadata, source):
        assert document["collection_complete"] is False
        assert document["stop_reason"] == "cancelled"
        assert document["partitions"][0]["complete"] is True
        assert document["partitions"][1]["complete"] is False
        assert document["partitions"][1]["stop_reason"] == "cancelled"
        assert document["partitions"][1]["page_count"] == closed_pages
    assert set(pd.read_parquet(out)["market_id"]) == (
        {"open", "closed"} if closed_pages else {"open"}
    )


def test_cli_commands_doc_covers_root_inventory() -> None:
    docs = Path("CLI_COMMANDS.md").read_text(encoding="utf-8")
    registered = {command.name for command in app.registered_commands}
    registered.update(group.name for group in app.registered_groups)
    # Groups are documented with their subcommands, e.g. `schema list|show`.
    missing = [
        name
        for name in sorted(str(name) for name in registered)
        if re.search(rf"`{re.escape(name)}[` ]", docs) is None
    ]

    assert missing == []



@pytest.mark.parametrize("explicit", [False, True])
def test_feature_cli_only_forwards_supplied_windows(monkeypatch, tmp_path, explicit):
    import pandas as pd
    from pmkt.cli import features as cli
    calls = []

    def compute(frame, **kwargs):
        calls.append(kwargs)
        return pd.DataFrame()

    monkeypatch.setattr(cli, "read_parquet", lambda path: pd.DataFrame())
    monkeypatch.setattr(cli, "write_parquet", lambda frame, path, **kwargs: path)
    monkeypatch.setattr(cli, "compute_features", compute)
    args = ["compute-features", "--books", str(tmp_path / "unused"), "--out", str(tmp_path / "out")]
    if explicit:
        args += ["--bar-seconds", "0", "--window-seconds", "0"]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    assert calls == ([{"bar_seconds": 0, "window_seconds": 0}] if explicit else [{}])


@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
@pytest.mark.parametrize("explicit", [False, True])
def test_history_cli_only_forwards_supplied_settings(monkeypatch, tmp_path, venue, explicit):
    from types import SimpleNamespace
    from pmkt.cli import history as cli
    calls = []

    class Client:
        def __init__(self, **kwargs):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass

    async def backfill(*args, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(summary={"historical_price_rows": 0, "trade_rows": 0, "gap_rows": 0})

    monkeypatch.setattr(cli, "AsyncClobClient", Client)
    monkeypatch.setattr(cli, "AsyncKalshiClient", Client)
    monkeypatch.setattr(cli.PmktConfig, "from_env", lambda: None)
    monkeypatch.setattr(cli, "backfill_venue_history", backfill)
    monkeypatch.setattr(cli, "write_historical_backfill_result", lambda *args: {})
    args = ["backfill-venue-history", "--venue", venue, "--instrument", "a", "--out-dir", str(tmp_path)]
    expected = {}
    if explicit:
        args += ["--start-ts", "0", "--end-ts", "1", "--run-id", ""]
        expected = {"start_ts": 0, "end_ts": 1, "run_id": ""}
        if venue == "polymarket":
            args += ["--interval", "", "--fidelity", "0"]
            expected.update(interval="", fidelity=0)
        else:
            args += ["--period-interval", "0", "--skip-trades", "--live", "--max-trade-pages", "0", "--trade-limit", "0"]
            expected.update(period_interval=0, include_trades=False, use_historical=False,
                            max_trade_pages=0, trade_limit=0)
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    actual = {key: value for key, value in calls[0].items() if key not in {"polymarket_client", "kalshi_client"}}
    assert actual == expected


@pytest.mark.parametrize("force_color", [False, True])
def test_history_help_explains_sampling_and_candle_units(monkeypatch, force_color):
    if force_color:
        monkeypatch.setenv("FORCE_COLOR", "1")
    else:
        monkeypatch.delenv("FORCE_COLOR", raising=False)
    result = CliRunner().invoke(app, ["backfill-venue-history", "--help"], color=False)
    assert result.exit_code == 0
    plain_output = re.sub(r"\x1b\[[0-9;]*m", "", result.output)
    output = " ".join(plain_output.replace(chr(0x2502), " ").split())
    assert "sampling resolution in minutes" in output
    assert "candlestick period in minutes" in output
