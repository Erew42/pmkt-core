from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import statistics
import tempfile
import time
import tracemalloc
from collections import deque
from pathlib import Path
from typing import Any, Iterable, Mapping

import pyarrow.dataset as ds

from pmkt.exchanges.kalshi.order_book_stream import (
    stream_kalshi_order_book_data,
)
from pmkt.exchanges.polymarket.order_book_stream import stream_order_book_data
from pmkt.streaming.supervisor import FeedShardHealth, LiveFeedSupervisor
from pmkt.streaming.feed_control import FeedControlScheduler
from pmkt.streaming.health_emission import (
    SlimHealthEmitter,
    feed_health_fingerprint,
)
from pmkt.streaming.profiles import select_storage_profile


class SyntheticClock:
    def __init__(self, *, step_ns: int = 1_000_000) -> None:
        self.now_ns = 1_000_000_000
        self.step_ns = step_ns

    def __call__(self) -> int:
        return self.now_ns

    def advance(self) -> None:
        self.now_ns += self.step_ns


class BenchmarkWebSocket:
    def __init__(self, messages: Iterable[str], clock: SyntheticClock) -> None:
        self.messages = deque(messages)
        self.clock = clock
        self.sent: list[str] = []
        self.closed = False
        self.processing_latencies_ns: list[int] = []
        self._last_request_ns: int | None = None

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def close(self) -> None:
        self.closed = True

    def __aiter__(self) -> "BenchmarkWebSocket":
        return self

    async def __anext__(self) -> str:
        requested_ns = time.perf_counter_ns()
        if self._last_request_ns is not None:
            self.processing_latencies_ns.append(requested_ns - self._last_request_ns)
        self._last_request_ns = requested_ns
        if not self.messages:
            raise StopAsyncIteration
        self.clock.advance()
        return self.messages.popleft()


class SyntheticReadAuth:
    """Test-only header provider; no credential or signing behavior."""

    def headers_for_get(self, path: str) -> dict[str, str]:
        del path
        return {"X-PMKT-BENCHMARK-READ": "1"}


def percentile(values: list[int], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def instrument_ids(venue: str, count: int) -> tuple[str, ...]:
    prefix = "token" if venue == "polymarket" else "KXBENCH"
    return tuple(f"{prefix}-{index:04d}" for index in range(count))


def selected_instrument(
    instruments: tuple[str, ...],
    *,
    index: int,
    message_count: int,
) -> str:
    if index >= int(message_count * 0.8):
        return instruments[0]
    return instruments[index % len(instruments)]


def websocket_messages(
    venue: str,
    instruments: tuple[str, ...],
    message_count: int,
) -> list[str]:
    rows: list[str] = []
    for index in range(message_count):
        instrument = selected_instrument(
            instruments,
            index=index,
            message_count=message_count,
        )
        if venue == "polymarket":
            payload: dict[str, Any] = {
                "event_type": "book",
                "asset_id": instrument,
                "market": f"market-{instrument}",
                "bids": [{"price": "0.40", "size": "10"}],
                "asks": [{"price": "0.60", "size": "5"}],
                "timestamp": str(1_700_000_000_000 + index),
                "hash": f"hash-{index}",
            }
        else:
            payload = {
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": index + 1,
                "msg": {
                    "market_ticker": instrument,
                    "market_id": f"market-{instrument}",
                    "yes_dollars_fp": [["0.40", "10.00"]],
                    "no_dollars_fp": [["0.60", "5.00"]],
                },
            }
        rows.append(json.dumps(payload, separators=(",", ":"), sort_keys=True))
    return rows


def semantic_digest(items: Iterable[Any]) -> str:
    payload = json.dumps(
        list(items),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def collapsed_health_digest(path: Path) -> str:
    if not path.exists():
        return semantic_digest(())
    fingerprints: list[str] = []
    for row in ds.dataset(path, format="parquet").to_table().to_pylist():
        fingerprint = feed_health_fingerprint(row)
        if not fingerprints or fingerprints[-1] != fingerprint:
            fingerprints.append(fingerprint)
    return semantic_digest(fingerprints)


def allocation_metrics(before: tracemalloc.Snapshot, event_count: int) -> dict[str, float]:
    positive_bytes = sum(
        max(0, stat.size_diff) for stat in tracemalloc.take_snapshot().compare_to(before, "lineno")
    )
    _, peak_bytes = tracemalloc.get_traced_memory()
    denominator = max(1, event_count)
    return {
        "net_allocated_bytes_per_event": positive_bytes / denominator,
        "peak_traced_bytes": float(peak_bytes),
    }


def benchmark_supervisor(
    *,
    venue: str,
    instrument_count: int,
    message_count: int,
    clock_step_ms: float,
) -> dict[str, Any]:
    instruments = instrument_ids(venue, instrument_count)
    shard = FeedShardHealth(
        venue=venue,
        shard_id=f"{venue}-0",
        subscribed_instruments=instruments,
    )
    supervisor = LiveFeedSupervisor([shard])
    emitter = SlimHealthEmitter(interval_seconds=10.0)
    clock = SyntheticClock(step_ns=int(clock_step_ms * 1_000_000))
    shard.mark_connected(now_monotonic_ns=clock())
    initial_rows = supervisor.feed_health_rows(
        now_monotonic_ns=clock(),
        observed_at_utc="2026-01-01T00:00:00+00:00",
        local_sequence=0,
        include_instrument_state=False,
    )
    emitter.observe(initial_rows, now_monotonic_ns=clock(), cause="connection")
    scheduler = FeedControlScheduler.from_thresholds(
        now_monotonic_ns=clock(),
        max_message_age_ms=supervisor.max_message_age_ms,
        max_valid_book_age_ms=supervisor.max_valid_book_age_ms,
    )
    latencies: list[int] = []
    semantic_events: list[Mapping[str, Any] | str] = []

    tracemalloc.start()
    before_allocations = tracemalloc.take_snapshot()
    wall_started = time.perf_counter_ns()
    cpu_started = time.process_time_ns()
    for index in range(message_count):
        event_started = time.perf_counter_ns()
        clock.advance()
        instrument = selected_instrument(
            instruments,
            index=index,
            message_count=message_count,
        )
        changed = shard.record_book(
            valid_state=True,
            now_monotonic_ns=clock(),
            instrument=instrument,
        )
        transition_keys = {(venue, shard.shard_id)} if changed else set()
        if index and index % 997 == 0:
            shard.record_sequence_gap(instrument=instrument)
            transition_keys.add((venue, shard.shard_id))
        elif index % 997 == 1 and "sequence_gap" in shard.quality_flags:
            shard.record_resync(
                now_monotonic_ns=clock(),
                instrument=instrument,
            )
            transition_keys.add((venue, shard.shard_id))

        periodic_keys: set[tuple[str, str]] = set()
        if scheduler.due(now_monotonic_ns=clock()):
            transition_keys.update(
                supervisor.invalidate_stale(
                    now_monotonic_ns=clock(),
                    venue=venue,
                )
            )
            periodic_keys.update(
                emitter.due_shard_keys(
                    supervisor.shard_keys(venue=venue),
                    now_monotonic_ns=clock(),
                )
            )
            scheduler.record_tick(
                now_monotonic_ns=clock(),
                stale_instrument_checks=(
                    supervisor.last_staleness_instruments_examined
                ),
            )
        selected_keys = transition_keys | periodic_keys
        if selected_keys:
            rows = supervisor.feed_health_rows(
                now_monotonic_ns=clock(),
                observed_at_utc="2026-01-01T00:00:00+00:00",
                local_sequence=index + 1,
                include_instrument_state=False,
                shard_keys=selected_keys,
            )
            emissions = emitter.observe(rows, now_monotonic_ns=clock())
            for emission in emissions:
                fingerprint = feed_health_fingerprint(emission.row)
                if not semantic_events or semantic_events[-1] != fingerprint:
                    semantic_events.append(fingerprint)
            scheduler.record_health_rows(
                transition_rows=len(transition_keys),
                periodic_rows=len(periodic_keys - transition_keys),
            )
            actions = supervisor.current_recovery_actions(
                venue=venue,
                shard_keys=None if periodic_keys else transition_keys,
            )
            scheduler.record_recovery(action_count=len(actions))
            semantic_events.extend(
                {
                    "action": action.action,
                    "venue": action.venue,
                    "shard_id": action.shard_id,
                    "reasons": action.reasons,
                }
                for action in actions
            )
        latencies.append(time.perf_counter_ns() - event_started)
    cpu_ns = time.process_time_ns() - cpu_started
    wall_ns = time.perf_counter_ns() - wall_started
    allocations = allocation_metrics(before_allocations, message_count)
    tracemalloc.stop()
    return {
        "layer": "supervisor",
        "venue": venue,
        "instrument_count": instrument_count,
        "message_count": message_count,
        "clock_step_ms": clock_step_ms,
        "cpu_ns_per_event": cpu_ns / max(1, message_count),
        "wall_ns_per_event": wall_ns / max(1, message_count),
        "latency_p50_ns": percentile(latencies, 0.50),
        "latency_p95_ns": percentile(latencies, 0.95),
        "latency_p99_ns": percentile(latencies, 0.99),
        "semantic_digest": semantic_digest(semantic_events),
        "feed_control_plane": scheduler.manifest_metrics(),
        "feed_health_emission": emitter.manifest_metrics(),
        **allocations,
    }


async def benchmark_collector(
    *,
    venue: str,
    instrument_count: int,
    message_count: int,
    clock_step_ms: float,
) -> dict[str, Any]:
    instruments = instrument_ids(venue, instrument_count)
    clock = SyntheticClock(step_ns=int(clock_step_ms * 1_000_000))
    websocket = BenchmarkWebSocket(
        websocket_messages(venue, instruments, message_count),
        clock,
    )

    tracemalloc.start()
    before_allocations = tracemalloc.take_snapshot()
    wall_started = time.perf_counter_ns()
    cpu_started = time.process_time_ns()
    with tempfile.TemporaryDirectory(prefix="pmkt-feed-control-") as temporary:
        root = Path(temporary)
        if venue == "polymarket":
            async def polymarket_connect(_: str) -> BenchmarkWebSocket:
                return websocket

            manifest = await stream_order_book_data(
                instruments,
                output_root=root,
                run_name="benchmark",
                duration_s=0,
                max_messages=message_count,
                capture_intent="smoke",
                heartbeat_interval=None,
                connect_factory=polymarket_connect,
                monotonic_ns=clock,
                storage_profile=select_storage_profile("mm-compact"),
            )
        else:
            async def kalshi_connect(
                _: str, __: dict[str, str]
            ) -> BenchmarkWebSocket:
                return websocket

            manifest = await stream_kalshi_order_book_data(
                instruments,
                output_root=root,
                run_name="benchmark",
                duration_s=0,
                max_messages=message_count,
                capture_intent="smoke",
                connect_factory=kalshi_connect,
                auth=SyntheticReadAuth(),
                monotonic_ns=clock,
                storage_profile=select_storage_profile("mm-compact"),
            )
        semantic_output = collapsed_health_digest(
            root / "benchmark" / "feed_health.parquet"
        )
        cpu_ns = time.process_time_ns() - cpu_started
        wall_ns = time.perf_counter_ns() - wall_started
        allocations = allocation_metrics(before_allocations, message_count)
    tracemalloc.stop()
    latencies = websocket.processing_latencies_ns
    return {
        "layer": "collector",
        "venue": venue,
        "instrument_count": instrument_count,
        "message_count": message_count,
        "clock_step_ms": clock_step_ms,
        "cpu_ns_per_event": cpu_ns / max(1, message_count),
        "wall_ns_per_event": wall_ns / max(1, message_count),
        "latency_p50_ns": percentile(latencies, 0.50),
        "latency_p95_ns": percentile(latencies, 0.95),
        "latency_p99_ns": percentile(latencies, 0.99),
        "semantic_digest": semantic_output,
        "feed_control_plane": manifest.get("feed_control_plane"),
        "feed_health_emission": manifest.get("feed_health_emission"),
        **allocations,
    }


def summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for row in rows:
        key = (str(row["layer"]), str(row["venue"]), int(row["instrument_count"]))
        grouped.setdefault(key, []).append(row)
    output: list[dict[str, Any]] = []
    metrics = (
        "cpu_ns_per_event",
        "wall_ns_per_event",
        "latency_p50_ns",
        "latency_p95_ns",
        "latency_p99_ns",
        "net_allocated_bytes_per_event",
        "peak_traced_bytes",
    )
    for (layer, venue, count), group in sorted(grouped.items()):
        output.append(
            {
                "layer": layer,
                "venue": venue,
                "instrument_count": count,
                "repeats": len(group),
                "semantic_digests": sorted(
                    {str(row["semantic_digest"]) for row in group}
                ),
                **{
                    f"median_{metric}": statistics.median(
                        float(row[metric]) for row in group
                    )
                    for metric in metrics
                },
            }
        )
    return output


async def run(args: argparse.Namespace) -> dict[str, Any]:
    layers = ("supervisor", "collector") if args.layer == "both" else (args.layer,)
    venues = ("polymarket", "kalshi") if args.venue == "both" else (args.venue,)
    counts = tuple(int(value) for value in args.instrument_counts.split(","))
    rows: list[dict[str, Any]] = []
    for layer in layers:
        for venue in venues:
            for count in counts:
                for repeat in range(args.warmups + args.repeats):
                    if layer == "supervisor":
                        result = benchmark_supervisor(
                            venue=venue,
                            instrument_count=count,
                            message_count=args.messages,
                            clock_step_ms=args.clock_step_ms,
                        )
                    else:
                        result = await benchmark_collector(
                            venue=venue,
                            instrument_count=count,
                            message_count=args.messages,
                            clock_step_ms=args.clock_step_ms,
                        )
                    if repeat >= args.warmups:
                        result["repeat"] = repeat - args.warmups + 1
                        rows.append(result)
    return {
        "policy": "feed-control-benchmark.v1",
        "warmups": args.warmups,
        "repeats": args.repeats,
        "clock_step_ms": args.clock_step_ms,
        "runs": rows,
        "summary": summary(rows),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark pmkt's incremental feed-health control plane."
    )
    parser.add_argument(
        "--layer",
        choices=("supervisor", "collector", "both"),
        default="both",
    )
    parser.add_argument(
        "--venue",
        choices=("polymarket", "kalshi", "both"),
        default="both",
    )
    parser.add_argument("--instrument-counts", default="1,74,600")
    parser.add_argument("--messages", type=int, default=2_000)
    parser.add_argument(
        "--clock-step-ms",
        type=float,
        default=1.0,
        help="Synthetic elapsed time per message; use >=20ms to exercise staleness.",
    )
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.clock_step_ms <= 0:
        raise SystemExit("--clock-step-ms must be positive")
    report = asyncio.run(run(args))
    encoded = json.dumps(report, indent=2, sort_keys=True)
    if args.output is None:
        print(encoded)
        return
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(encoded + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
