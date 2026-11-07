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


@pytest.mark.parametrize("command,attribute", [
    ("stream-books", "stream_order_book_data"),
    ("stream-kalshi-books", "stream_kalshi_order_book_data"),
])
@pytest.mark.parametrize("explicit", [False, True])
def test_recording_cli_only_forwards_supplied_options(monkeypatch, command, attribute, explicit):
    calls = []

    async def capture(instruments, **kwargs):
        calls.append(kwargs)
        return {"status": "complete", "run_dir": "test"}

    monkeypatch.setattr(streaming, attribute, capture)
    monkeypatch.setattr(streaming, "_load_read_auth_header_provider", lambda value: object())
    instrument_flag = "--token-id" if command == "stream-books" else "--ticker"
    args = [command, instrument_flag, "a"]
    if explicit:
        args += ["--duration", "0", "--max-messages", "1", "--mode", "topbook",
                 "--depth-check-interval-s", "off", "--no-depth-on-best-price-change",
                 "--no-raw-messages"]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    actual = {key: value for key, value in calls[0].items() if key in {
        "duration_s", "mode", "depth_check_interval_s", "depth_on_best_price_change", "raw_messages"
    }}
    assert actual == ({
        "duration_s": 0.0, "mode": "topbook", "depth_check_interval_s": None,
        "depth_on_best_price_change": False, "raw_messages": False,
    } if explicit else {})


@pytest.mark.parametrize("overrides,expected", [
    ([], {"duration_s": 60.0}),
    (["--duration", "0", "--interval", "0", "--max-stale-seconds", "0"],
     {"duration_s": 0.0, "interval_s": 0.0, "max_stale_seconds": 0.0}),
    (["--max-stale-seconds", "-1"], {"duration_s": 60.0, "max_stale_seconds": None}),
])
def test_topbook_cli_preserves_preset_and_disabled_syntax(monkeypatch, tmp_path, overrides, expected):
    from types import SimpleNamespace
    from pmkt.cli import history

    calls = []

    class Client:
        def __init__(self, **kwargs):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass

    async def record(*args, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(topbook_rows=0, depth_rows=0, gap_rows=0, cycles=1, output_dir=tmp_path)

    monkeypatch.setattr(history, "AsyncClobClient", Client)
    monkeypatch.setattr(history, "AsyncKalshiClient", Client)
    monkeypatch.setattr(history.PmktConfig, "from_env", lambda: None)
    monkeypatch.setattr(history, "_read_table", lambda path: None)
    monkeypatch.setattr(history, "record_topbooks", record)
    result = CliRunner().invoke(app, ["record-topbooks", "--relations", str(tmp_path / "unused.csv"), *overrides])
    assert result.exit_code == 0, result.output
    actual = {key: value for key, value in calls[0].items() if key in {
        "duration_s", "interval_s", "max_stale_seconds"
    }}
    assert actual == expected
