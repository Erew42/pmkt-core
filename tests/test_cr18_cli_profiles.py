from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any

from typer.testing import CliRunner
import typer.main

from pmkt.cli.app import app
from pmkt.exchanges.ws_transport import WS_MAX_QUEUE_FRAMES, WS_MAX_SIZE_BYTES
import pmkt.cli.streaming as streaming_cli
from pmkt.streaming.capture_completeness import CaptureIntent
from pmkt.streaming.profiles import DatasetRole


_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def _normalized_cli_output(value: str) -> str:
    return "".join(_ANSI_ESCAPE_RE.sub("", value).split())


def test_stream_profile_validation_precedes_output_side_effects(tmp_path: Path) -> None:
    output = tmp_path / "runs"
    result = CliRunner().invoke(
        app,
        [
            "stream-books",
            "--token-id",
            "token-1",
            "--output-dir",
            str(output),
            "--storage-profile",
            "unknown",
        ],
    )
    assert result.exit_code != 0
    assert "unknown storage profile" in result.output
    assert not output.exists()


def test_reduced_profile_requires_explicit_acknowledgement(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "stream-books",
            "--token-id",
            "token-1",
            "--output-dir",
            str(tmp_path / "runs"),
            "--storage-profile",
            "book-tape",
        ],
    )
    assert result.exit_code != 0
    assert "experimental" in result.output.lower()
    assert not (tmp_path / "runs").exists()


def test_stream_books_passes_default_full_profile(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, Any] = {}

    async def fake_stream(*args, **kwargs):
        captured.update(kwargs)
        return {
            "run_dir": str(tmp_path / "runs" / "run"),
            "counts": {"events": 0, "snapshots": 0, "levels": 0},
        }

    monkeypatch.setattr(streaming_cli, "stream_order_book_data", fake_stream)
    result = CliRunner().invoke(
        app,
        ["stream-books", "--token-id", "token-1", "--output-dir", str(tmp_path / "runs")],
    )
    assert result.exit_code == 0, result.output
    selection = captured["storage_profile"]
    assert selection.definition.name == "full"
    assert DatasetRole.RAW_JSONL not in selection.enabled_roles
    assert captured["capture_intent"] is CaptureIntent.OPERATIONAL
    assert captured["websocket_max_size_bytes"] == WS_MAX_SIZE_BYTES
    assert captured["websocket_max_queue_frames"] == WS_MAX_QUEUE_FRAMES


def test_subscription_metadata_uses_source_validation_timestamps(
    tmp_path: Path,
) -> None:
    plan_path = tmp_path / "plan.json"
    plan = {
        "schema_version": "subscription_plan.v1",
        "plan_id": "plan-1",
        "created_at_utc": "2026-07-27T10:00:00+00:00",
        "polymarket": {"assets_ids": ["token-active", "token-inactive"]},
        "kalshi": {"market_tickers": ["KXACTIVE"]},
        "polymarket_assets": [
            {
                "asset_id": "token-active",
                "active": True,
                "validated_at_utc": "2026-07-27T10:01:00+00:00",
            },
            {
                "asset_id": "token-inactive",
                "active": False,
                "validated_at_utc": "2026-07-27T10:02:00+00:00",
            },
        ],
        "kalshi_market_tickers": [
            {
                "market_ticker": "KXACTIVE",
                "active": True,
                "validated_at_utc": "2026-07-27T10:03:00+00:00",
            }
        ],
    }
    plan_path.write_text(json.dumps(plan), encoding="utf-8")

    metadata = streaming_cli._subscription_plan_metadata(plan_path, plan)
    polymarket = metadata["instrument_eligibility"]["polymarket"]
    kalshi = metadata["instrument_eligibility"]["kalshi"]
    assert polymarket["token-active"]["observed_at_utc"] == (
        "2026-07-27T10:01:00+00:00"
    )
    assert polymarket["token-inactive"]["observed_at_utc"] == (
        "2026-07-27T10:02:00+00:00"
    )
    assert polymarket["token-inactive"]["status"] == "ineligible"
    assert polymarket["token-inactive"]["reason"] == "source_inactive"
    assert kalshi["KXACTIVE"]["observed_at_utc"] == "2026-07-27T10:03:00+00:00"


def test_experimental_profile_additive_overrides_are_passed(
    monkeypatch, tmp_path: Path
) -> None:
    captured: dict[str, Any] = {}

    async def fake_stream(*args, **kwargs):
        captured.update(kwargs)
        return {
            "run_dir": str(tmp_path / "runs" / "run"),
            "counts": {"events": 0, "snapshots": 0, "levels": 0},
        }

    monkeypatch.setattr(streaming_cli, "stream_order_book_data", fake_stream)
    result = CliRunner().invoke(
        app,
        [
            "stream-books",
            "--token-id",
            "token-1",
            "--storage-profile",
            "book-tape",
            "--acknowledge-experimental-profile",
            "--keep-raw-jsonl",
            "--emit-full-depth",
        ],
    )
    assert result.exit_code == 0, result.output
    selection = captured["storage_profile"]
    assert DatasetRole.RAW_JSONL in selection.enabled_roles
    assert DatasetRole.DEPTH_MAIN in selection.enabled_roles
    assert selection.experimental_profile_acknowledged is True
    assert "Warning: using experimental storage profile" in result.stderr


def test_invalid_capture_inputs_do_not_emit_experimental_warning(
    tmp_path: Path,
) -> None:
    output = tmp_path / "runs"
    result = CliRunner().invoke(
        app,
        [
            "stream-books",
            "--token-id",
            "token-1",
            "--output-dir",
            str(output),
            "--duration",
            "0",
            "--storage-profile",
            "book-tape",
            "--acknowledge-experimental-profile",
        ],
    )
    assert result.exit_code != 0
    assert "Warning:" not in result.stderr
    assert not output.exists()


def test_kalshi_experimental_warning_follows_header_provider_validation(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "stream-kalshi-books",
            "--ticker",
            "KXTEST",
            "--output-dir",
            str(tmp_path / "runs"),
            "--storage-profile",
            "mm-compact",
            "--acknowledge-experimental-profile",
        ],
    )
    assert result.exit_code != 0
    assert "--header-provider" in _normalized_cli_output(result.output)
    assert "Warning:" not in result.stderr


def test_kalshi_experimental_acknowledgement_is_passed_and_warned(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, Any] = {}
    provider = object()

    async def fake_stream(*args, **kwargs):
        captured.update(kwargs)
        return {
            "run_dir": str(tmp_path / "runs" / "run"),
            "counts": {"events": 0, "snapshots": 0, "levels": 0},
        }

    monkeypatch.setattr(
        streaming_cli,
        "_load_read_auth_header_provider",
        lambda _: provider,
    )
    monkeypatch.setattr(streaming_cli, "stream_kalshi_order_book_data", fake_stream)
    result = CliRunner().invoke(
        app,
        [
            "stream-kalshi-books",
            "--ticker",
            "KXTEST",
            "--output-dir",
            str(tmp_path / "runs"),
            "--header-provider",
            "test_support:provider",
            "--storage-profile",
            "mm-compact",
            "--acknowledge-experimental-profile",
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured["auth"] is provider
    assert captured["storage_profile"].experimental_profile_acknowledged is True
    assert "Warning: using experimental storage profile" in result.stderr


def test_both_stream_commands_expose_profile_controls() -> None:
    for command in ("stream-books", "stream-kalshi-books"):
        command_info = typer.main.get_command(app).commands[command]
        options = {
            option
            for parameter in command_info.params
            for option in (*getattr(parameter, "opts", ()), *getattr(parameter, "secondary_opts", ()))
        }
        for flag in (
            "--storage-profile",
            "--capture-storage-backend",
            "--acknowledge-experimental-profile",
            "--feed-health-interval-seconds",
            "--topbook-checkpoint-interval-seconds",
            "--book-checkpoint-interval-seconds",
            "--keep-raw-jsonl",
            "--topbook-emission-per-event",
            "--emit-full-depth",
            "--emit-legacy-book-artifacts",
            "--capture-intent",
            "--websocket-max-size-bytes",
            "--websocket-max-queue-frames",
        ):
            assert flag in options
