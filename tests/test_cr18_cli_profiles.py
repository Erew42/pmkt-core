from __future__ import annotations

from pathlib import Path
import re
from typing import Any

import pytest
import typer
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


@pytest.mark.parametrize("key", ["capture_summary", "capture_completeness"])
def test_capture_summary_separates_eligibility_and_coverage(key):
    summary = {"requested_instrument_count": 3, "initial_snapshot_count": 1,
               "reconnect_count": 0, "eligibility_evaluation_status": "partial",
               "unknown_instrument_count": 1, "eligible_instrument_count": 2,
               "eligible_initial_snapshot_count": 1}
    suffix = streaming_cli._capture_summary_suffix({key: summary})
    assert "1/3 initial snapshots" in suffix
    assert "eligibility partial (1 unknown)" in suffix
    assert "1/2 eligible initial snapshots" in suffix
    del summary["eligibility_evaluation_status"]
    assert streaming_cli._capture_summary_suffix({key: summary}) == (
        ", 1/3 initial snapshots, 0 reconnects")


@pytest.mark.parametrize("schema", ["topbook.v1", "depth.v1"])
@pytest.mark.parametrize("flag", [
    "seq_gap", "no_initial_snapshot", "malformed_book", "missing_sequence",
    "sid_changed", "delta_before_snapshot", "hash_mismatch", "reconnect",
    "crossed_book", "negative_spread", "empty_bid", "empty_ask", "stale_quotes",
])
def test_integrity_projection_preserves_upstream_failures(schema, flag):
    from pmkt.streaming.profiles import add_book_integrity, select_storage_profile
    row = {"schema_version": schema, "quality_flags": [flag]}
    selection = select_storage_profile("full", profile_version="3")
    assert not add_book_integrity(row, integrity=False, selection=selection)["book_integrity_valid"]
    projected = add_book_integrity(row, integrity=True, selection=selection)
    assert projected["book_integrity_valid"] == (flag in {"empty_bid", "empty_ask", "stale_quotes"})
    assert add_book_integrity(row, integrity=True, selection=None) == row
    assert add_book_integrity(row, integrity=True,
        selection=select_storage_profile("full", profile_version="2")) == row


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


def test_read_auth_provider_loader_rejects_invalid_factory_shape() -> None:
    with pytest.raises(typer.BadParameter, match="could not initialize"):
        streaming_cli._load_read_auth_header_provider("json:dumps")


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


@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
@pytest.mark.parametrize("version", [None, "3", "99"])
def test_explicit_profile_version_selection(monkeypatch, tmp_path, venue, version):
    captured = {}

    async def capture(*args, **kwargs):
        captured.update(kwargs)
        return {"run_dir": str(tmp_path / "run"), "counts": {}}

    monkeypatch.setattr(streaming_cli, "stream_order_book_data", capture)
    monkeypatch.setattr(streaming_cli, "stream_kalshi_order_book_data", capture)
    monkeypatch.setattr(streaming_cli, "_load_read_auth_header_provider", lambda _: object())
    command = (["stream-books", "--token-id", "test"] if venue == "polymarket"
               else ["stream-kalshi-books", "--ticker", "TEST", "--header-provider", "test:provider"])
    command += ["--output-dir", str(tmp_path / "runs")]
    if version is not None:
        command += ["--profile-version", version]
    result = CliRunner().invoke(app, command)
    if version == "99":
        assert result.exit_code != 0
        assert not captured
        assert not (tmp_path / "runs").exists()
    else:
        assert result.exit_code == 0, result.output
        assert captured["storage_profile"].definition.profile_version == (version or "2")
