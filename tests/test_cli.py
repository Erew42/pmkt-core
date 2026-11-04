from __future__ import annotations

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
