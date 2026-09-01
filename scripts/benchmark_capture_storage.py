from __future__ import annotations

import argparse
import csv
import gc
import hashlib
import json
import platform
import random
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

import pmkt.streaming.durability as durability_module
from pmkt.data.registry import (
    BOOK_TAPE_EVENT_SCHEMA_VERSION,
    BOOK_TAPE_LEVEL_SCHEMA_VERSION,
    arrow_schema,
    get_table_spec,
)
from pmkt.streaming.durability import (
    DurableCaptureCoordinator,
    normalize_capture_value,
)
from pmkt.streaming.durability_settings import CaptureDurabilitySettings
from pmkt.streaming.recovery import validate_commit_journal
from pmkt.streaming.recovery_contracts import CaptureCommitCause, RunStateV1
from pmkt.streaming.sqlite_durability import SQLiteCaptureCoordinator
from pmkt.streaming.storage_backends import (
    CaptureStorageBackend,
    CaptureStorageSettings,
)
from pmkt.streaming.tape import (
    CaptureCoordinate,
    NativeBookLevel,
    build_tape_batch,
    canonical_json_bytes,
    epoch_id,
)

RUN_ID = "capture-storage-benchmark"
SHARD_ID = "benchmark-shard"
STARTED_AT_UTC = "2026-08-25T10:00:00.000000Z"
ROLE_ORDER = ("tape_level", "tape_event")
ROLE_SCHEMA_VERSIONS = {
    "tape_level": BOOK_TAPE_LEVEL_SCHEMA_VERSION,
    "tape_event": BOOK_TAPE_EVENT_SCHEMA_VERSION,
}
ROLE_SCHEMAS = {
    role: arrow_schema(get_table_spec(version))
    for role, version in ROLE_SCHEMA_VERSIONS.items()
}


@dataclass(frozen=True)
class Workload:
    name: str
    instrument_count: int
    event_count: int
    events_per_group: int
    cause: CaptureCommitCause
    description: str


@dataclass(frozen=True)
class CaptureGroup:
    rows_by_role: Mapping[str, tuple[dict[str, Any], ...]]
    cause: CaptureCommitCause


WORKLOADS = (
    Workload(
        "steady_74",
        74,
        2_000,
        500,
        CaptureCommitCause.THRESHOLD_ROWS,
        "Steady traffic in four large durable groups.",
    ),
    Workload(
        "mixed_barriers_74",
        74,
        2_000,
        100,
        CaptureCommitCause.INVALIDATION,
        "Intermittent invalidation in twenty groups.",
    ),
    Workload(
        "barrier_storm_74",
        74,
        120,
        1,
        CaptureCommitCause.INVALIDATION,
        "Worst-case forced durability barrier for every event.",
    ),
    Workload(
        "snapshot_ramp_600",
        600,
        600,
        100,
        CaptureCommitCause.CHECKPOINT_STARTUP,
        "First-snapshot ramp across 600 instruments.",
    ),
    Workload(
        "steady_600",
        600,
        2_400,
        600,
        CaptureCommitCause.THRESHOLD_ROWS,
        "Large-universe steady traffic in four groups.",
    ),
)


def _percentile(values: Sequence[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _directory_metrics(root: Path) -> tuple[int, int]:
    files = [path for path in root.rglob("*") if path.is_file()]
    return len(files), sum(path.stat().st_size for path in files)


def _make_groups(workload: Workload) -> tuple[CaptureGroup, ...]:
    instruments = tuple(
        f"benchmark-token-{index:04d}"
        for index in range(workload.instrument_count)
    )
    base_time = datetime(2026, 8, 25, 10, 0, tzinfo=timezone.utc)
    pending: dict[str, list[dict[str, Any]]] = defaultdict(list)
    groups: list[CaptureGroup] = []
    for offset in range(workload.event_count):
        sequence = offset + 1
        instrument_index = offset % workload.instrument_count
        book_id = instruments[instrument_index]
        coordinate = CaptureCoordinate(
            collector_run_id=RUN_ID,
            shard_id=SHARD_ID,
            received_at_utc=(base_time + timedelta(microseconds=offset)).isoformat(
                timespec="microseconds"
            ),
            received_at_monotonic_ns=1_000_000_000 + offset,
            local_sequence=sequence,
            subsequence=1,
        )
        levels = (
            NativeBookLevel(
                source_side="bid",
                price="0.40",
                size_after_contracts=str(10 + offset % 7),
            ),
            NativeBookLevel(
                source_side="ask",
                price="0.60",
                size_after_contracts=str(5 + offset % 11),
            ),
        )
        batch = build_tape_batch(
            coordinate=coordinate,
            venue="polymarket",
            venue_market_id=f"market-{instrument_index:04d}",
            venue_book_id=book_id,
            event_kind="checkpoint",
            checkpoint_reason="periodic",
            epoch=epoch_id(
                coordinate,
                venue_book_id=book_id,
                epoch_generation=offset // workload.instrument_count,
            ),
            levels=levels,
            full_book_levels=levels,
            allowed_source_sides=("bid", "ask"),
            valid_state=True,
            reconstructible=True,
        )
        pending["tape_level"].extend(dict(row) for row in batch.levels)
        pending["tape_event"].append(dict(batch.event))
        if (
            sequence % workload.events_per_group == 0
            or sequence == workload.event_count
        ):
            groups.append(
                CaptureGroup(
                    rows_by_role={
                        role: tuple(pending.get(role, ())) for role in ROLE_ORDER
                    },
                    cause=workload.cause,
                )
            )
            pending.clear()
    return tuple(groups)


def _semantic_digest_from_groups(groups: Sequence[CaptureGroup]) -> str:
    digest = hashlib.sha256(b"pmkt-capture-storage-benchmark.v2\x00")
    for role in ROLE_ORDER:
        digest.update(role.encode("utf-8") + b"\x00")
        for group in groups:
            for row in group.rows_by_role.get(role, ()):
                encoded = canonical_json_bytes(normalize_capture_value(row))
                digest.update(len(encoded).to_bytes(8, "big", signed=False))
                digest.update(encoded)
    return digest.hexdigest()


def _semantic_digest_from_parquet(root: Path, state: RunStateV1) -> str:
    digest = hashlib.sha256(b"pmkt-capture-storage-benchmark.v2\x00")
    for role in ROLE_ORDER:
        digest.update(role.encode("utf-8") + b"\x00")
        dataset = root / state.expected_role_paths[role]
        for path in sorted(dataset.glob("part-*.parquet")):
            parquet = pq.ParquetFile(path)
            for batch in parquet.iter_batches(batch_size=10_000):
                for row in batch.to_pylist():
                    encoded = canonical_json_bytes(normalize_capture_value(row))
                    digest.update(len(encoded).to_bytes(8, "big", signed=False))
                    digest.update(encoded)
    return digest.hexdigest()


def _make_coordinator(
    root: Path,
    workload: Workload,
    backend: CaptureStorageBackend,
) -> DurableCaptureCoordinator:
    durability = CaptureDurabilitySettings.resolve(
        requested_segment_rows=50_000,
        requested_segment_seconds=300.0,
    )
    state = RunStateV1(
        run_id=RUN_ID,
        profile_name="book-tape",
        profile_version="1",
        expected_role_paths={role: f"datasets/{role}" for role in ROLE_ORDER},
        shard_plan={
            SHARD_ID: [
                f"benchmark-token-{index:04d}"
                for index in range(workload.instrument_count)
            ]
        },
        started_at_utc=STARTED_AT_UTC,
        capture_durability=durability.to_mapping(),
        capture_storage=CaptureStorageSettings.for_backend(backend).to_mapping(),
    )
    coordinator_type = (
        SQLiteCaptureCoordinator
        if backend is CaptureStorageBackend.SQLITE_WAL
        else DurableCaptureCoordinator
    )
    return coordinator_type(
        run_dir=root,
        run_state=state,
        role_schema_versions=ROLE_SCHEMA_VERSIONS,
        role_schemas=ROLE_SCHEMAS,
        segment_row_limit=durability.effective_segment_rows,
        commit_interval_seconds=durability.effective_segment_seconds,
        durability_settings=durability,
    )


def _run_once(
    root: Path,
    workload: Workload,
    groups: Sequence[CaptureGroup],
    backend: CaptureStorageBackend,
    *,
    trace_memory: bool,
) -> dict[str, Any]:
    coordinator = _make_coordinator(root, workload, backend)
    gc.collect()
    if trace_memory:
        tracemalloc.start()
    capture_wall_start = time.perf_counter_ns()
    capture_cpu_start = time.process_time_ns()
    commit_latencies_ms: list[float] = []
    for group in groups:
        for role in ROLE_ORDER:
            for row in group.rows_by_role.get(role, ()):
                coordinator.add(role, row)
        commit_start = time.perf_counter_ns()
        coordinator.commit(cause=group.cause, force=True)
        commit_latencies_ms.append(
            (time.perf_counter_ns() - commit_start) / 1_000_000.0
        )
    capture_cpu_ns = time.process_time_ns() - capture_cpu_start
    capture_wall_ns = time.perf_counter_ns() - capture_wall_start
    live_file_count, live_bytes = _directory_metrics(root)
    capture_peak_bytes = None
    if trace_memory:
        _, capture_peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.reset_peak()

    finalize_wall_start = time.perf_counter_ns()
    finalize_cpu_start = time.process_time_ns()
    coordinator.finalize_segments()
    finalize_cpu_ns = time.process_time_ns() - finalize_cpu_start
    finalize_wall_ns = time.perf_counter_ns() - finalize_wall_start
    storage_manifest = coordinator.storage_manifest()
    finalize_peak_bytes = None
    if trace_memory:
        _, finalize_peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    coordinator.mark_finalized()

    records = validate_commit_journal(root)
    semantic_digest = _semantic_digest_from_parquet(root, coordinator.state)
    final_file_count, final_bytes = _directory_metrics(root)
    parquet_file_count = len(list(root.rglob("part-*.parquet")))
    metrics = storage_manifest["metrics"]
    peak_bytes = (
        max(int(capture_peak_bytes or 0), int(finalize_peak_bytes or 0))
        if trace_memory
        else None
    )
    return {
        "backend": backend.value,
        "workload": workload.name,
        "event_count": workload.event_count,
        "group_count": len(groups),
        "journal_group_count": len(records),
        "capture_cpu_ns_per_event": capture_cpu_ns / workload.event_count,
        "capture_wall_ns_per_event": capture_wall_ns / workload.event_count,
        "total_cpu_ns_per_event": (
            capture_cpu_ns + finalize_cpu_ns
        )
        / workload.event_count,
        "total_wall_ns_per_event": (
            capture_wall_ns + finalize_wall_ns
        )
        / workload.event_count,
        "finalize_cpu_ms": finalize_cpu_ns / 1_000_000.0,
        "finalize_wall_ms": finalize_wall_ns / 1_000_000.0,
        "commit_latency_p50_ms": _percentile(commit_latencies_ms, 0.50),
        "commit_latency_p95_ms": _percentile(commit_latencies_ms, 0.95),
        "commit_latency_p99_ms": _percentile(commit_latencies_ms, 0.99),
        "commit_latency_max_ms": max(commit_latencies_ms, default=0.0),
        "live_file_count": live_file_count,
        "live_bytes": live_bytes,
        "final_file_count": final_file_count,
        "final_bytes": final_bytes,
        "parquet_file_count": parquet_file_count,
        "database_bytes": metrics["database_bytes"],
        "wal_peak_bytes": metrics["wal_peak_bytes"],
        "promotion_wall_ms": metrics["promotion"]["latency_ms"]["p50"],
        "peak_traced_bytes": peak_bytes,
        "capture_peak_traced_bytes": capture_peak_bytes,
        "finalize_peak_traced_bytes": finalize_peak_bytes,
        "semantic_digest": semantic_digest,
    }


def _parse_commit_probe_events(raw: str | None) -> tuple[int, ...]:
    if raw is None:
        return ()
    values = tuple(int(item.strip()) for item in raw.split(",") if item.strip())
    if not values or any(value <= 0 for value in values):
        raise ValueError("commit probe event counts must be positive integers")
    if len(set(values)) != len(values):
        raise ValueError("commit probe event counts must be unique")
    return values


def _run_commit_probe_once(
    root: Path,
    *,
    event_count: int,
    instrument_count: int,
    backend: CaptureStorageBackend,
) -> dict[str, Any]:
    workload = Workload(
        name=f"commit_probe_{event_count}_events",
        instrument_count=instrument_count,
        event_count=event_count,
        events_per_group=event_count,
        cause=CaptureCommitCause.THRESHOLD_ROWS,
        description="One synchronous row-threshold commit.",
    )
    group = _make_groups(workload)[0]
    expected_rows = sum(len(rows) for rows in group.rows_by_role.values())
    coordinator = _make_coordinator(root, workload, backend)
    stage_ns: defaultdict[str, int] = defaultdict(int)

    original_validate = coordinator._validate_group

    def timed_validate(*args: Any, **kwargs: Any) -> Any:
        started = time.perf_counter_ns()
        try:
            return original_validate(*args, **kwargs)
        finally:
            stage_ns["validation"] += time.perf_counter_ns() - started

    coordinator._validate_group = timed_validate

    if backend is CaptureStorageBackend.PARQUET_SEGMENTS:
        original_write = coordinator._write_role_segment
        original_publish = coordinator._publish_artifacts

        def timed_write(*args: Any, **kwargs: Any) -> Any:
            started = time.perf_counter_ns()
            try:
                return original_write(*args, **kwargs)
            finally:
                stage_ns["parquet_write"] += time.perf_counter_ns() - started

        def timed_publish(*args: Any, **kwargs: Any) -> Any:
            started = time.perf_counter_ns()
            try:
                return original_publish(*args, **kwargs)
            finally:
                stage_ns["publication"] += time.perf_counter_ns() - started

        coordinator._write_role_segment = timed_write
        coordinator._publish_artifacts = timed_publish

    original_readback = durability_module.read_committed_capture_rows

    def timed_readback(*args: Any, **kwargs: Any) -> Any:
        started = time.perf_counter_ns()
        try:
            return original_readback(*args, **kwargs)
        finally:
            stage_ns["parquet_readback"] += time.perf_counter_ns() - started

    gc.collect()
    add_wall_start = time.perf_counter_ns()
    add_cpu_start = time.process_time_ns()
    for role in ROLE_ORDER:
        for row in group.rows_by_role.get(role, ()):
            coordinator.add(role, row)
    add_cpu_ns = time.process_time_ns() - add_cpu_start
    add_wall_ns = time.perf_counter_ns() - add_wall_start

    durability_module.read_committed_capture_rows = timed_readback
    commit_wall_start = time.perf_counter_ns()
    commit_cpu_start = time.process_time_ns()
    try:
        record = coordinator.commit(cause=group.cause, force=True)
    except BaseException:
        close = getattr(coordinator, "close", None)
        if callable(close):
            close()
        raise
    finally:
        commit_cpu_ns = time.process_time_ns() - commit_cpu_start
        commit_wall_ns = time.perf_counter_ns() - commit_wall_start
        durability_module.read_committed_capture_rows = original_readback

    if sum(coordinator.row_counts.values()) != expected_rows:
        raise ValueError("commit probe row count mismatch")
    if backend is CaptureStorageBackend.PARQUET_SEGMENTS and record is None:
        raise ValueError("parquet commit probe did not publish a record")
    if backend is CaptureStorageBackend.SQLITE_WAL and record is not None:
        raise ValueError("sqlite commit probe unexpectedly published parquet")

    close = getattr(coordinator, "close", None)
    if callable(close):
        close()
    file_count, durable_bytes = _directory_metrics(root)
    attributed_ns = sum(stage_ns.values())
    return {
        "backend": backend.value,
        "event_count": event_count,
        "instrument_count": instrument_count,
        "row_count": expected_rows,
        "add_wall_ms": add_wall_ns / 1_000_000.0,
        "add_cpu_ms": add_cpu_ns / 1_000_000.0,
        "commit_wall_ms": commit_wall_ns / 1_000_000.0,
        "commit_cpu_ms": commit_cpu_ns / 1_000_000.0,
        "synchronous_event_loop_block_ms": commit_wall_ns / 1_000_000.0,
        "validation_ms": stage_ns["validation"] / 1_000_000.0,
        "parquet_write_ms": stage_ns["parquet_write"] / 1_000_000.0,
        "parquet_readback_ms": stage_ns["parquet_readback"] / 1_000_000.0,
        "publication_ms": stage_ns["publication"] / 1_000_000.0,
        "unattributed_commit_ms": max(0, commit_wall_ns - attributed_ns)
        / 1_000_000.0,
        "durable_file_count": file_count,
        "durable_bytes": durable_bytes,
    }


def _summarize_commit_probes(
    raw_runs: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    event_counts = sorted({int(row["event_count"]) for row in raw_runs})
    for event_count in event_counts:
        for backend in CaptureStorageBackend:
            selected = [
                row
                for row in raw_runs
                if int(row["event_count"]) == event_count
                and row["backend"] == backend.value
            ]
            if not selected:
                continue
            commit_values = [float(row["commit_wall_ms"]) for row in selected]
            summary: dict[str, Any] = {
                "backend": backend.value,
                "event_count": event_count,
                "row_count": int(selected[0]["row_count"]),
                "repeats": len(selected),
                "commit_wall_ms_p50": _percentile(commit_values, 0.50),
                "commit_wall_ms_p95": _percentile(commit_values, 0.95),
                "commit_wall_ms_max": max(commit_values),
            }
            for field in (
                "validation_ms",
                "parquet_write_ms",
                "parquet_readback_ms",
                "publication_ms",
                "unattributed_commit_ms",
            ):
                summary[f"{field}_p50"] = _percentile(
                    [float(row[field]) for row in selected], 0.50
                )
            summaries.append(summary)
    return summaries


def _run_commit_probes(
    *,
    output: Path,
    scratch: Path,
    event_counts: Sequence[int],
    instrument_count: int,
    warmups: int,
    repeats: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    raw_runs: list[dict[str, Any]] = []
    for event_count in event_counts:
        for iteration in range(warmups + repeats):
            order = (
                (
                    CaptureStorageBackend.PARQUET_SEGMENTS,
                    CaptureStorageBackend.SQLITE_WAL,
                )
                if iteration % 2 == 0
                else (
                    CaptureStorageBackend.SQLITE_WAL,
                    CaptureStorageBackend.PARQUET_SEGMENTS,
                )
            )
            for order_index, backend in enumerate(order):
                with tempfile.TemporaryDirectory(
                    dir=scratch,
                    prefix=f"commit-probe-{event_count}-{backend.value}-",
                ) as temporary:
                    result = _run_commit_probe_once(
                        Path(temporary),
                        event_count=event_count,
                        instrument_count=instrument_count,
                        backend=backend,
                    )
                if iteration >= warmups:
                    result["repeat"] = iteration - warmups + 1
                    result["paired_order"] = order_index + 1
                    raw_runs.append(result)
            print(
                f"commit_probe_{event_count}_events: {iteration + 1}/"
                f"{warmups + repeats}",
                flush=True,
            )
    summaries = _summarize_commit_probes(raw_runs)
    _write_csv(output / "commit_probe_raw.csv", raw_runs)
    _write_csv(output / "commit_probe_summary.csv", summaries)
    return raw_runs, summaries


def _bootstrap_median_ratio_ci(
    ratios: Sequence[float],
    *,
    iterations: int = 20_000,
) -> tuple[float, float]:
    generator = random.Random(20260825)
    estimates = [
        statistics.median(generator.choice(ratios) for _ in ratios)
        for _ in range(iterations)
    ]
    estimates.sort()
    return (
        estimates[int(0.025 * (len(estimates) - 1))],
        estimates[int(0.975 * (len(estimates) - 1))],
    )


def _summarize(raw_runs: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    ratio_metrics = (
        "capture_cpu_ns_per_event",
        "capture_wall_ns_per_event",
        "total_cpu_ns_per_event",
        "total_wall_ns_per_event",
        "commit_latency_p95_ms",
        "finalize_wall_ms",
        "live_file_count",
        "final_file_count",
        "final_bytes",
    )
    summaries: list[dict[str, Any]] = []
    for workload in WORKLOADS:
        selected = [row for row in raw_runs if row["workload"] == workload.name]
        parquet = sorted(
            (row for row in selected if row["backend"] == "parquet_segments"),
            key=lambda row: int(row["repeat"]),
        )
        sqlite = sorted(
            (row for row in selected if row["backend"] == "sqlite_wal_v1"),
            key=lambda row: int(row["repeat"]),
        )
        if not parquet or len(parquet) != len(sqlite):
            continue
        summary: dict[str, Any] = {
            **asdict(workload),
            "cause": workload.cause.value,
            "repeats": len(parquet),
            "semantic_digest_match": (
                len({str(row["semantic_digest"]) for row in parquet}) == 1
                and {str(row["semantic_digest"]) for row in parquet}
                == {str(row["semantic_digest"]) for row in sqlite}
            ),
            "parquet_median_parquet_file_count": statistics.median(
                float(row["parquet_file_count"]) for row in parquet
            ),
            "sqlite_median_parquet_file_count": statistics.median(
                float(row["parquet_file_count"]) for row in sqlite
            ),
        }
        for metric in ratio_metrics:
            ratios = [
                float(sqlite_row[metric]) / float(parquet_row[metric])
                for parquet_row, sqlite_row in zip(parquet, sqlite, strict=True)
                if float(parquet_row[metric]) != 0
            ]
            low, high = _bootstrap_median_ratio_ci(ratios)
            summary[f"sqlite_to_parquet_median_ratio_{metric}"] = (
                statistics.median(ratios)
            )
            summary[f"sqlite_to_parquet_ci95_low_{metric}"] = low
            summary[f"sqlite_to_parquet_ci95_high_{metric}"] = high
        summaries.append(summary)
    return summaries


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    fields = sorted({str(key) for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _environment() -> dict[str, Any]:
    return {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python": sys.version,
        "sqlite": __import__("sqlite3").sqlite_version,
        "git_commit": subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
        ).stdout.strip(),
        "git_dirty": bool(
            subprocess.run(
                ["git", "status", "--short"],
                check=False,
                capture_output=True,
                text=True,
            ).stdout.strip()
        ),
    }


def _selected_workloads(names: str | None) -> tuple[Workload, ...]:
    if not names:
        return WORKLOADS
    requested = {item.strip() for item in names.split(",") if item.strip()}
    selected = tuple(item for item in WORKLOADS if item.name in requested)
    missing = requested - {item.name for item in selected}
    if missing:
        raise ValueError("unknown workloads: " + ", ".join(sorted(missing)))
    return selected


def run(args: argparse.Namespace) -> dict[str, Any]:
    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    scratch = output / "scratch"
    scratch.mkdir(exist_ok=True)
    commit_probe_events = _parse_commit_probe_events(args.commit_probe_events)
    selected_workloads = (
        () if args.commit_probe_only else _selected_workloads(args.workloads)
    )
    if args.commit_probe_only and not commit_probe_events:
        raise ValueError("--commit-probe-only requires --commit-probe-events")
    raw_runs: list[dict[str, Any]] = []
    memory_runs: list[dict[str, Any]] = []
    for workload in selected_workloads:
        groups = _make_groups(workload)
        expected_digest = _semantic_digest_from_groups(groups)
        for iteration in range(args.warmups + args.repeats):
            order = (
                (
                    CaptureStorageBackend.PARQUET_SEGMENTS,
                    CaptureStorageBackend.SQLITE_WAL,
                )
                if iteration % 2 == 0
                else (
                    CaptureStorageBackend.SQLITE_WAL,
                    CaptureStorageBackend.PARQUET_SEGMENTS,
                )
            )
            for order_index, backend in enumerate(order):
                with tempfile.TemporaryDirectory(
                    dir=scratch,
                    prefix=f"{workload.name}-{backend.value}-",
                ) as temporary:
                    result = _run_once(
                        Path(temporary),
                        workload,
                        groups,
                        backend,
                        trace_memory=False,
                    )
                if result["semantic_digest"] != expected_digest:
                    raise ValueError(
                        f"{backend.value} changed {workload.name} semantics"
                    )
                if iteration >= args.warmups:
                    result["repeat"] = iteration - args.warmups + 1
                    result["paired_order"] = order_index + 1
                    raw_runs.append(result)
            print(
                f"{workload.name}: {iteration + 1}/"
                f"{args.warmups + args.repeats}",
                flush=True,
            )
        for backend in CaptureStorageBackend:
            with tempfile.TemporaryDirectory(
                dir=scratch,
                prefix=f"{workload.name}-{backend.value}-memory-",
            ) as temporary:
                result = _run_once(
                    Path(temporary),
                    workload,
                    groups,
                    backend,
                    trace_memory=True,
                )
            if result["semantic_digest"] != expected_digest:
                raise ValueError("memory pass changed semantic output")
            memory_runs.append(result)

    summaries = _summarize(raw_runs)
    commit_probe_runs: list[dict[str, Any]] = []
    commit_probe_summaries: list[dict[str, Any]] = []
    if commit_probe_events:
        commit_probe_runs, commit_probe_summaries = _run_commit_probes(
            output=output,
            scratch=scratch,
            event_counts=commit_probe_events,
            instrument_count=args.commit_probe_instruments,
            warmups=args.commit_probe_warmups,
            repeats=args.commit_probe_repeats,
        )
    payload = {
        "format": "pmkt.capture-storage-benchmark.v2",
        "environment": _environment(),
        "configuration": {
            "warmups": args.warmups,
            "repeats": args.repeats,
            "paired_order_alternates": True,
            "tracemalloc_separate_pass": True,
            "sqlite_journal_mode": "WAL",
            "sqlite_synchronous": "FULL",
            "workloads": [
                {**asdict(item), "cause": item.cause.value}
                for item in selected_workloads
            ],
            "commit_probe": {
                "event_counts": list(commit_probe_events),
                "rows_per_event": 3,
                "instrument_count": args.commit_probe_instruments,
                "warmups": args.commit_probe_warmups,
                "repeats": args.commit_probe_repeats,
                "finalization_included": False,
            },
        },
        "raw_runs": raw_runs,
        "memory_runs": memory_runs,
        "summary": summaries,
        "commit_probe_runs": commit_probe_runs,
        "commit_probe_summary": commit_probe_summaries,
    }
    (output / "raw_results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_csv(output / "raw_runs.csv", raw_runs)
    _write_csv(output / "summary.csv", summaries)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Paired benchmark of pmkt's actual Parquet-segment and SQLite/WAL "
            "capture coordinators."
        )
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--workloads")
    parser.add_argument(
        "--commit-probe-events",
        help=(
            "Comma-separated event counts for one-group synchronous commit probes; "
            "each deterministic event produces three tape rows."
        ),
    )
    parser.add_argument("--commit-probe-instruments", type=int, default=74)
    parser.add_argument("--commit-probe-warmups", type=int, default=1)
    parser.add_argument("--commit-probe-repeats", type=int, default=3)
    parser.add_argument("--commit-probe-only", action="store_true")
    args = parser.parse_args()
    if args.warmups < 0 or args.repeats < 1:
        parser.error("--warmups must be >= 0 and --repeats must be >= 1")
    if args.commit_probe_instruments < 1:
        parser.error("--commit-probe-instruments must be >= 1")
    if args.commit_probe_warmups < 0 or args.commit_probe_repeats < 1:
        parser.error(
            "--commit-probe-warmups must be >= 0 and "
            "--commit-probe-repeats must be >= 1"
        )
    payload = run(args)
    printed = (
        payload["commit_probe_summary"]
        if args.commit_probe_only
        else payload["summary"]
    )
    print(json.dumps(printed, indent=2, default=str))


if __name__ == "__main__":
    main()
