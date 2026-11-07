import json
import sys
import types
from datetime import datetime, timedelta, timezone

import pytest
from typer.testing import CliRunner

from pmkt.cli.app import app
from pmkt.streaming import raw_capture as rr


def arguments(tmp_path, venue="polymarket"):
    return [
        "record-raw-ws",
        "--venue",
        venue,
        "--markets",
        str(tmp_path / "markets.json"),
        "--runs-root",
        str(tmp_path / "runs"),
        "--root",
        str(tmp_path / "runs/run1/instance"),
        "--end-utc",
        (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat(),
    ]


@pytest.mark.parametrize(
    "reason,exit_code",
    [
        ("deadline", 0),
        ("end_already_passed", 0),
        ("signal_or_control_storage_error", 1),
        ("partial_seal_OSError", 1),
    ],
)
def test_cli_wires_capture_options_and_terminal_status(
    tmp_path, monkeypatch, reason, exit_code
):
    calls = []

    async def capture(*args, **kwargs):
        calls.append((args, kwargs))
        return reason

    monkeypatch.setattr(rr, "capture_raw_ws", capture)
    result = CliRunner().invoke(app, arguments(tmp_path))
    assert result.exit_code == exit_code, result.output
    assert reason in result.output
    assert calls[0][0][2] == "polymarket"
    assert calls[0][1] == dict(
        runs_root=tmp_path / "runs",
        auth=None,
        chunk_seconds=60,
        gzip_level=6,
        lag_warn_ms=5000,
    )


@pytest.mark.parametrize("factory", [False, True])
def test_cli_reuses_injected_read_auth_loader(tmp_path, monkeypatch, factory):
    class Provider:
        def headers_for_get(self, path):
            pytest.fail("CLI must leave signing to the connection")

    provider = Provider()
    module = types.ModuleType("synthetic_raw_auth")
    module.provider = (lambda: provider) if factory else provider
    monkeypatch.setitem(sys.modules, module.__name__, module)
    calls = []

    async def capture(*args, **kwargs):
        calls.append(kwargs["auth"])
        return "deadline"

    monkeypatch.setattr(rr, "capture_raw_ws", capture)
    result = CliRunner().invoke(
        app,
        arguments(tmp_path, "kalshi")
        + ["--header-provider", module.__name__ + ":provider"],
    )
    assert result.exit_code == 0, result.output
    assert calls == [provider]


def test_cli_requires_kalshi_header_provider_before_creating_output(tmp_path):
    result = CliRunner().invoke(app, arguments(tmp_path, "kalshi"))
    assert result.exit_code != 0 and "--header-provider" in result.output
    assert not (tmp_path / "runs").exists()


@pytest.mark.parametrize(
    "option,value",
    [
        ("--gzip-level", "0"),
        ("--gzip-level", "10"),
        ("--gzip-level", "-1"),
        ("--gzip-level", "6.5"),
        ("--gzip-level", "invalid"),
        ("--chunk-seconds", "0"),
        ("--chunk-seconds", "nan"),
        ("--lag-warn-ms", "-1"),
        ("--lag-warn-ms", "nan"),
        ("--end-utc", "2026-11-06T12:00:00"),
    ],
)
def test_invalid_cli_options_fail_before_capture_creates_output(
    tmp_path, option, value
):
    result = CliRunner().invoke(app, arguments(tmp_path) + [option, value])
    assert result.exit_code != 0, result.output
    assert not (tmp_path / "runs").exists()


@pytest.mark.asyncio
async def test_library_requires_auth_before_session_or_network(tmp_path):
    markets = tmp_path / "markets.json"
    markets.write_text(
        json.dumps(
            dict(
                venue="kalshi",
                name="instance",
                instruments=["K"],
                ws_url="wss://external-api-ws.kalshi.com/trade-api/ws/v2",
            )
        )
    )
    from pmkt.exchanges.read_auth import ReadAuthenticationRequiredError

    with pytest.raises(ReadAuthenticationRequiredError):
        await rr.capture_raw_ws(
            tmp_path / "runs/run1/instance",
            datetime.now(timezone.utc) + timedelta(minutes=1),
            "kalshi",
            markets,
            runs_root=tmp_path / "runs",
        )
    assert not (tmp_path / "runs").exists()


@pytest.mark.asyncio
async def test_capture_rejects_platform_without_directory_fsync(tmp_path, monkeypatch):
    monkeypatch.delattr(rr.os, "O_DIRECTORY")
    with pytest.raises(RuntimeError, match="requires POSIX"):
        await rr.capture_raw_ws(
            tmp_path / "runs/run1/instance",
            datetime.now(timezone.utc),
            "polymarket",
            tmp_path / "missing.json",
            runs_root=tmp_path / "runs",
        )
    assert not (tmp_path / "runs").exists()
