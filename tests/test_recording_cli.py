import pytest

from typer.testing import CliRunner

from pmkt.cli.app import app
from pmkt.cli import streaming


def test_price_only_cli_passes_explicit_none_and_raw_flag(monkeypatch, tmp_path):
    calls = []

    async def capture(instruments, **kwargs):
        calls.append((instruments, kwargs))
        return {"status": "complete", "run_dir": str(tmp_path)}

    monkeypatch.setattr(streaming, "stream_order_book_data", capture)
    result = CliRunner().invoke(
        app,
        [
            "stream-books",
            "--token-id",
            "a",
            "--depth-check-interval-s",
            "off",
            "--depth-on-best-price-change",
            "--raw-messages",
        ],
    )
    assert result.exit_code == 0, result.output
    assert calls[0][0] == ["a"]
    assert calls[0][1]["depth_check_interval_s"] is None
    assert calls[0][1]["depth_on_best_price_change"] is True
    assert calls[0][1]["raw_messages"] is True
    assert "storage_profile" not in calls[0][1]


def test_removed_profile_flags_are_not_silently_accepted():
    result = CliRunner().invoke(
        app, ["stream-books", "--token-id", "a", "--storage-profile", "mm-compact"]
    )
    assert result.exit_code != 0


def test_partial_run_has_nonzero_cli_exit(monkeypatch):
    async def capture(*args, **kwargs):
        return {"status": "partial", "run_dir": "partial-run"}

    monkeypatch.setattr(streaming, "stream_order_book_data", capture)
    result = CliRunner().invoke(app, ["stream-books", "--token-id", "a"])
    assert result.exit_code == 1
    assert "partial" in result.output


def test_invalid_configuration_does_not_create_output(tmp_path):
    result = CliRunner().invoke(
        app,
        [
            "stream-books",
            "--token-id",
            "a",
            "--depth-check-interval-s",
            "off",
            "--output-dir",
            str(tmp_path / "absent"),
        ],
    )
    assert result.exit_code != 0
    assert not (tmp_path / "absent").exists()



@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("command,attribute,flag", [
    ("stream-books", "stream_order_book_data", "--token-id"),
    ("stream-kalshi-books", "stream_kalshi_order_book_data", "--ticker"),
])
def test_recording_cli_inherits_connection_settings(monkeypatch, explicit, command, attribute, flag):
    calls = []

    async def capture(*args, **kwargs):
        calls.append(kwargs)
        return {"status": "complete", "run_dir": "test"}

    monkeypatch.setattr(streaming, attribute, capture)
    monkeypatch.setattr(streaming, "_load_read_auth_header_provider", lambda value: object())
    args = [command, flag, "a"]
    if explicit:
        args += ["--max-reconnects", "0", "--websocket-max-size-bytes", "0", "--websocket-max-queue-frames", "0"]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    actual = {key: value for key, value in calls[0].items() if key in {
        "max_reconnects", "websocket_max_size_bytes", "websocket_max_queue_frames"
    }}
    assert actual == ({"max_reconnects": 0, "websocket_max_size_bytes": 0, "websocket_max_queue_frames": 0} if explicit else {})
