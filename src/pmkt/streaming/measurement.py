from __future__ import annotations

import subprocess
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol


class ReplayEventSink(Protocol):
    def write(self, event: Mapping[str, Any]) -> None: ...


ReplaySink = ReplayEventSink | Callable[[Mapping[str, Any]], None]


@dataclass(frozen=True)
class FakeWebsocketReplayConfig:
    instrument_count: int = 100
    events_per_instrument: int = 20
    shard_count: int = 4
    event_interval_ms: float = 10.0
    slow_sink_every: int = 0
    slow_sink_ms: float = 0.0
    reconnect_every: int = 0
    sequence_gap_every: int = 0
    stale_shard_every: int = 0
    stale_after_ms: int = 5_000
    sink_backlog_limit: int = 0


@dataclass(frozen=True)
class FakeWebsocketReplayReport:
    scenario_id: str
    instrument_count: int
    shard_count: int
    event_count: int
    accepted_event_count: int
    sink_write_count: int
    slow_sink_count: int
    reconnect_count: int
    sequence_gap_count: int
    stale_shard_count: int
    max_sequence_gap: int
    max_sink_lag_ms: float
    dropped_event_count: int
    scenario_flags: tuple[str, ...]
    summary: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["scenario_flags"] = list(self.scenario_flags)
        return payload


@dataclass(frozen=True)
class CliImportTimingSpec:
    label: str
    command: tuple[str, ...]
    timeout_seconds: float = 20.0


@dataclass(frozen=True)
class CliImportTimingResult:
    label: str
    command: tuple[str, ...]
    elapsed_ms: float
    status: str
    returncode: int | None
    stdout_bytes: int
    stderr_bytes: int
    error: str = ""

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["command"] = list(self.command)
        return payload


def pr15_fake_websocket_replay_config() -> FakeWebsocketReplayConfig:
    return FakeWebsocketReplayConfig(
        instrument_count=50,
        events_per_instrument=4,
        slow_sink_every=5,
        slow_sink_ms=25,
        reconnect_every=17,
        sequence_gap_every=13,
        stale_shard_every=19,
    )


def run_fake_websocket_load_replay(
    config: FakeWebsocketReplayConfig | None = None,
    *,
    sink: ReplaySink | None = None,
    scenario_id: str = "fake_websocket_load_replay.v1",
    started_at_utc: str = "2026-07-03T00:00:00Z",
) -> FakeWebsocketReplayReport:
    """Run a deterministic stream replay without network I/O or sleeps."""
    active = config or FakeWebsocketReplayConfig()
    _validate_replay_config(active)
    started_at = _parse_utc(started_at_utc)
    event_count = accepted_event_count = sink_write_count = 0
    slow_sink_count = reconnect_count = sequence_gap_count = 0
    stale_shard_count = max_sequence_gap = dropped_event_count = 0
    sink_lag_ms = max_sink_lag_ms = 0.0

    for instrument_idx in range(active.instrument_count):
        instrument_id = f"instrument-{instrument_idx + 1:06d}"
        shard_id = f"shard-{instrument_idx % active.shard_count:02d}"
        sequence = 0
        for event_offset in range(active.events_per_instrument):
            event_count += 1
            sequence += 1
            sequence_gap = 0
            if _periodic(active.sequence_gap_every, event_count):
                sequence += 2
                sequence_gap = 2
                sequence_gap_count += 1
                max_sequence_gap = max(max_sequence_gap, sequence_gap)

            reconnect = _periodic(active.reconnect_every, event_count)
            stale_shard = _periodic(active.stale_shard_every, event_count)
            slow_sink = _periodic(active.slow_sink_every, event_count)
            reconnect_count += int(reconnect)
            stale_shard_count += int(stale_shard)
            slow_sink_count += int(slow_sink)
            sink_lag_ms = (
                sink_lag_ms + active.slow_sink_ms
                if slow_sink
                else max(0.0, sink_lag_ms - active.event_interval_ms)
            )
            max_sink_lag_ms = max(max_sink_lag_ms, sink_lag_ms)
            dropped = active.sink_backlog_limit > 0 and (
                _backlog_events(sink_lag_ms, active.event_interval_ms)
                > active.sink_backlog_limit
            )
            dropped_event_count += int(dropped)
            accepted_event_count += int(not dropped)

            event = {
                "scenario_id": scenario_id,
                "instrument_id": instrument_id,
                "shard_id": shard_id,
                "sequence": sequence,
                "sequence_gap": sequence_gap,
                "event_type": "book_delta",
                "connection_state": "reconnected" if reconnect else "connected",
                "received_at_utc": (
                    started_at
                    + timedelta(milliseconds=active.event_interval_ms * event_count)
                )
                .isoformat()
                .replace("+00:00", "Z"),
                "last_message_age_ms": (
                    active.stale_after_ms + active.event_interval_ms
                    if stale_shard
                    else active.event_interval_ms
                ),
                "sink_lag_ms": sink_lag_ms,
                "slow_sink": slow_sink,
                "quality_flags": _event_quality_flags(
                    reconnect=reconnect,
                    sequence_gap=sequence_gap,
                    stale_shard=stale_shard,
                    dropped=dropped,
                ),
                "event_offset": event_offset,
            }
            if not dropped and sink is not None:
                _write_replay_event(sink, event)
                sink_write_count += 1

    flags = _scenario_flags(
        instrument_count=active.instrument_count,
        slow_sink_count=slow_sink_count,
        reconnect_count=reconnect_count,
        sequence_gap_count=sequence_gap_count,
        stale_shard_count=stale_shard_count,
        dropped_event_count=dropped_event_count,
    )
    required = {
        "many_instruments",
        "slow_sinks",
        "reconnects",
        "sequence_gaps",
        "stale_shards",
    }
    return FakeWebsocketReplayReport(
        scenario_id=scenario_id,
        instrument_count=active.instrument_count,
        shard_count=active.shard_count,
        event_count=event_count,
        accepted_event_count=accepted_event_count,
        sink_write_count=sink_write_count,
        slow_sink_count=slow_sink_count,
        reconnect_count=reconnect_count,
        sequence_gap_count=sequence_gap_count,
        stale_shard_count=stale_shard_count,
        max_sequence_gap=max_sequence_gap,
        max_sink_lag_ms=max_sink_lag_ms,
        dropped_event_count=dropped_event_count,
        scenario_flags=tuple(sorted(flags)),
        summary={
            "measurement_scope": "offline_fake_websocket_replay",
            "network_io": False,
            "slept_for_slow_sinks": False,
            "covers_required_pr15_scenarios": required.issubset(flags),
            "required_scenarios": sorted(required),
        },
    )


def default_cli_import_timing_specs() -> tuple[CliImportTimingSpec, ...]:
    return (
        CliImportTimingSpec("pmkt-help", ("pmkt", "--help")),
        CliImportTimingSpec("pmkt-dataset-help", ("pmkt", "dataset", "--help")),
        CliImportTimingSpec(
            "pmkt-stream-books-help", ("pmkt", "stream-books", "--help")
        ),
        CliImportTimingSpec(
            "pmkt-reconstruct-book-tape-help",
            ("pmkt", "reconstruct-book-tape", "--help"),
        ),
    )


def measure_cli_import_timing(
    specs: Iterable[CliImportTimingSpec] | None = None,
    *,
    cwd: str | Path | None = None,
) -> tuple[CliImportTimingResult, ...]:
    results: list[CliImportTimingResult] = []
    for spec in tuple(specs or default_cli_import_timing_specs()):
        started = time.perf_counter()
        try:
            completed = subprocess.run(
                spec.command,
                cwd=str(cwd) if cwd is not None else None,
                capture_output=True,
                check=False,
                timeout=spec.timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            results.append(
                CliImportTimingResult(
                    label=spec.label,
                    command=spec.command,
                    elapsed_ms=(time.perf_counter() - started) * 1_000,
                    status="timeout",
                    returncode=None,
                    stdout_bytes=len(exc.stdout or b""),
                    stderr_bytes=len(exc.stderr or b""),
                    error=f"timed out after {spec.timeout_seconds:g}s",
                )
            )
            continue
        except OSError as exc:
            results.append(
                CliImportTimingResult(
                    label=spec.label,
                    command=spec.command,
                    elapsed_ms=(time.perf_counter() - started) * 1_000,
                    status="failed",
                    returncode=None,
                    stdout_bytes=0,
                    stderr_bytes=0,
                    error=str(exc),
                )
            )
            continue
        results.append(
            CliImportTimingResult(
                label=spec.label,
                command=spec.command,
                elapsed_ms=(time.perf_counter() - started) * 1_000,
                status="passed" if completed.returncode == 0 else "failed",
                returncode=completed.returncode,
                stdout_bytes=len(completed.stdout or b""),
                stderr_bytes=len(completed.stderr or b""),
            )
        )
    return tuple(results)


def _validate_replay_config(config: FakeWebsocketReplayConfig) -> None:
    for field in ("instrument_count", "events_per_instrument", "shard_count"):
        if getattr(config, field) <= 0:
            raise ValueError(f"{field} must be greater than 0")
    for field in (
        "event_interval_ms",
        "slow_sink_every",
        "slow_sink_ms",
        "reconnect_every",
        "sequence_gap_every",
        "stale_shard_every",
        "stale_after_ms",
        "sink_backlog_limit",
    ):
        if getattr(config, field) < 0:
            raise ValueError(f"{field} must be nonnegative")


def _periodic(every: int, index: int) -> bool:
    return every > 0 and index % every == 0


def _backlog_events(sink_lag_ms: float, event_interval_ms: float) -> int:
    return 0 if event_interval_ms <= 0 else int(sink_lag_ms // event_interval_ms)


def _write_replay_event(sink: ReplaySink, event: Mapping[str, Any]) -> None:
    if hasattr(sink, "write"):
        sink.write(event)
    else:
        sink(event)


def _event_quality_flags(
    *, reconnect: bool, sequence_gap: int, stale_shard: bool, dropped: bool
) -> tuple[str, ...]:
    flags: list[str] = []
    if reconnect:
        flags.append("reconnect")
    if sequence_gap:
        flags.append("sequence_gap")
    if stale_shard:
        flags.append("stale_shard")
    if dropped:
        flags.append("sink_backlog_drop")
    return tuple(flags)


def _scenario_flags(
    *,
    instrument_count: int,
    slow_sink_count: int,
    reconnect_count: int,
    sequence_gap_count: int,
    stale_shard_count: int,
    dropped_event_count: int,
) -> set[str]:
    flags: set[str] = set()
    if instrument_count >= 50:
        flags.add("many_instruments")
    if slow_sink_count:
        flags.add("slow_sinks")
    if reconnect_count:
        flags.add("reconnects")
    if sequence_gap_count:
        flags.add("sequence_gaps")
    if stale_shard_count:
        flags.add("stale_shards")
    if dropped_event_count:
        flags.add("sink_backlog_drops")
    return flags


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )


__all__ = [
    "CliImportTimingResult",
    "CliImportTimingSpec",
    "FakeWebsocketReplayConfig",
    "FakeWebsocketReplayReport",
    "ReplayEventSink",
    "ReplaySink",
    "default_cli_import_timing_specs",
    "measure_cli_import_timing",
    "pr15_fake_websocket_replay_config",
    "run_fake_websocket_load_replay",
]
