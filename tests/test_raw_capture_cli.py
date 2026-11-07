import json
import re
import subprocess
import sys
import types
from datetime import datetime, timedelta, timezone

import pytest
from typer.testing import CliRunner

from pmkt.cli.app import app
from pmkt.streaming import raw_capture as rr

_ANSI_ESCAPE_RE = re.compile(r"\[[0-?]*[ -/]*[@-~]")


def _normalized_cli_output(value: str) -> str:
    return "".join(_ANSI_ESCAPE_RE.sub("", value).split())


@pytest.fixture(autouse=True)
def stable_terminal(monkeypatch):
    monkeypatch.setenv("COLUMNS", "160")
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    monkeypatch.setenv("NO_COLOR", "1")


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
    assert calls[0][1] == dict(runs_root=tmp_path / "runs")


@pytest.mark.parametrize(
    "name,value",
    [
        ("chunk_seconds", 12.5),
        ("gzip_level", 9),
        ("lag_warn_ms", 0),
        ("low_free", 4 * rr.GiB),
        ("critical_free", 64 * rr.MiB),
        ("gap_reconnect_min_interval", 12.5),
        ("gap_reconnect_min_interval", 0),
    ],
)
def test_cli_passes_only_supplied_tuning_options(tmp_path, monkeypatch, name, value):
    calls = []

    async def capture(*args, **kwargs):
        calls.append(kwargs)
        return "deadline"

    monkeypatch.setattr(rr, "capture_raw_ws", capture)
    result = CliRunner().invoke(
        app, arguments(tmp_path) + ["--" + name.replace("_", "-"), str(value)]
    )
    assert result.exit_code == 0, result.output
    assert calls == [dict(runs_root=tmp_path / "runs", **{name: value})]


def test_cli_emits_info_events_on_stderr_with_standard_logging(tmp_path):
    markets = tmp_path / "markets.json"
    markets.write_text(
        json.dumps(
            dict(
                venue="polymarket",
                name="instance",
                instruments=["1"],
                ws_url="wss://ws-subscriptions-clob.polymarket.com/ws/market",
            )
        )
    )
    args = arguments(tmp_path)
    args[-1] = "2000-01-01T00:00:00Z"
    result = subprocess.run(
        [sys.executable, "-c", "from pmkt.cli.entrypoint import main; main()", *args],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "INFO:pmkt.streaming.raw_capture:event_loop" in result.stderr
    assert "INFO:pmkt.streaming.raw_capture:end_already_passed" in result.stderr
    assert "Raw capture: end_already_passed" in result.stdout
    assert not (tmp_path / "runs").exists()


def test_cli_import_and_help_leave_streaming_extra_optional(tmp_path):
    script = """
import importlib.abc
import sys

class WithoutWebsockets(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] == 'websockets':
            raise ModuleNotFoundError('synthetic missing websockets', name='websockets')

sys.meta_path.insert(0, WithoutWebsockets())
from typer.testing import CliRunner
from pmkt.cli.app import app

runner = CliRunner()
help_result = runner.invoke(app, ['record-raw-ws', '--help'])
assert help_result.exit_code == 0, help_result.output
assert 'pmkt.streaming.raw_capture' not in sys.modules
result = runner.invoke(app, sys.argv[1:])
assert result.exit_code != 0
assert 'install pmkt[streaming]' in result.output, result.output
"""
    result = subprocess.run(
        [sys.executable, "-c", script, *arguments(tmp_path)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert not (tmp_path / "runs").exists()


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


def test_cli_masks_read_auth_initializer_errors(tmp_path, monkeypatch):
    def provider():
        raise RuntimeError("synthetic-secret-marker")

    module = types.ModuleType("synthetic_failing_raw_auth")
    module.provider = provider
    monkeypatch.setitem(sys.modules, module.__name__, module)
    result = CliRunner().invoke(
        app,
        arguments(tmp_path, "kalshi")
        + ["--header-provider", module.__name__ + ":provider"],
    )
    assert result.exit_code != 0
    assert "RuntimeError" in _normalized_cli_output(result.output)
    assert "synthetic-secret-marker" not in _normalized_cli_output(result.output)


def test_cli_requires_kalshi_header_provider_before_creating_output(tmp_path):
    result = CliRunner().invoke(app, arguments(tmp_path, "kalshi"))
    assert result.exit_code != 0
    assert "--header-provider" in _normalized_cli_output(result.output)
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
        ("--low-free", "-1"),
        ("--critical-free", "-1"),
        ("--low-free", "1"),
        ("--gap-reconnect-min-interval", "-1"),
        ("--gap-reconnect-min-interval", "nan"),
        ("--gap-reconnect-min-interval", "inf"),
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
