from __future__ import annotations

import sys

from pmkt.streaming import (
    CliImportTimingSpec,
    FakeWebsocketReplayConfig,
    default_cli_import_timing_specs,
    measure_cli_import_timing,
    pr15_fake_websocket_replay_config,
    run_fake_websocket_load_replay,
)


class _CollectingSink:
    def __init__(self) -> None:
        self.rows: list[dict[str, object]] = []

    def write(self, event: dict[str, object]) -> None:
        self.rows.append(event)


def test_fake_websocket_load_replay_covers_pr15_scenarios() -> None:
    sink = _CollectingSink()
    report = run_fake_websocket_load_replay(
        pr15_fake_websocket_replay_config(),
        sink=sink,
    )

    assert report.event_count == 200
    assert report.accepted_event_count == 200
    assert report.sink_write_count == 200
    assert len(sink.rows) == 200
    assert report.slow_sink_count > 0
    assert report.reconnect_count > 0
    assert report.sequence_gap_count > 0
    assert report.stale_shard_count > 0
    assert report.max_sequence_gap == 2
    assert report.max_sink_lag_ms > 0
    assert set(report.scenario_flags) >= {
        "many_instruments",
        "slow_sinks",
        "reconnects",
        "sequence_gaps",
        "stale_shards",
    }
    assert report.summary["covers_required_pr15_scenarios"] is True


def test_pr15_fake_websocket_replay_config_names_required_scenario() -> None:
    config = pr15_fake_websocket_replay_config()

    assert config.instrument_count >= 50
    assert config.slow_sink_every > 0
    assert config.reconnect_every > 0
    assert config.sequence_gap_every > 0
    assert config.stale_shard_every > 0


def test_fake_websocket_load_replay_can_model_sink_backlog_drops() -> None:
    report = run_fake_websocket_load_replay(
        FakeWebsocketReplayConfig(
            instrument_count=5,
            events_per_instrument=5,
            event_interval_ms=1,
            slow_sink_every=1,
            slow_sink_ms=10,
            sink_backlog_limit=2,
        ),
    )

    assert report.dropped_event_count > 0
    assert report.accepted_event_count < report.event_count
    assert report.sink_write_count == 0
    assert "sink_backlog_drops" in report.scenario_flags


def test_default_import_timing_specs_cover_public_core_commands() -> None:
    specs = default_cli_import_timing_specs()
    commands = {spec.command for spec in specs}

    assert ("pmkt", "--help") in commands
    assert commands == {
        ("pmkt", "--help"),
        ("pmkt", "dataset", "--help"),
        ("pmkt", "stream-books", "--help"),
        ("pmkt", "reconstruct-book-tape", "--help"),
    }



def test_import_timing_runner_records_lightweight_command() -> None:
    (result,) = measure_cli_import_timing(
        (
            CliImportTimingSpec(
                label="python-help-smoke",
                command=(sys.executable, "-c", "print('ok')"),
                timeout_seconds=5,
            ),
        )
    )

    assert result.status == "passed"
    assert result.returncode == 0
    assert result.elapsed_ms >= 0
    assert result.stdout_bytes > 0
