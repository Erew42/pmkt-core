"""Measure complete, synchronous workload calls. Setup belongs outside the timed callable."""

from __future__ import annotations

import gc
import math
import random
import time
import tracemalloc
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TypedDict


@dataclass(frozen=True)
class Case:
    name: str
    run: Callable[[], object]
    units_per_call: int = 1
    unit: str = "ops"

    def __post_init__(self) -> None:
        if not self.name or self.units_per_call <= 0 or not self.unit:
            raise ValueError(
                "cases require a name, a positive count, and a throughput unit"
            )


@dataclass(frozen=True)
class Options:
    warmup: int = 2
    iterations: int | None = 10
    seconds: float | None = None
    seed: int = 42
    alternate: bool = False
    disable_gc: bool = False
    memory: bool = False

    def __post_init__(self) -> None:
        if self.warmup < 0 or (self.iterations is None) == (self.seconds is None):
            raise ValueError(
                "choose either iterations or seconds, and a nonnegative warmup"
            )
        if self.iterations is not None and self.iterations < 1:
            raise ValueError("iterations must be positive")
        if self.seconds is not None and (
            not math.isfinite(self.seconds) or self.seconds <= 0
        ):
            raise ValueError("seconds must be finite and positive")


class Result(TypedDict):
    name: str
    samples: int
    seconds: dict[str, float]
    units_per_call: int
    unit: str
    throughput: float
    tracemalloc_peak_bytes: int | None


def summarize(
    name: str, durations: Sequence[float], units_per_call: int, unit: str
) -> Result:
    values = sorted(durations)
    if not values or any(not math.isfinite(v) or v <= 0 for v in values):
        raise ValueError(f"{name}: expected positive, finite sample durations")

    def percentile(fraction: float) -> float:
        index = (len(values) - 1) * fraction
        low = math.floor(index)
        return values[low] + (values[math.ceil(index)] - values[low]) * (index - low)

    median = percentile(0.5)
    return Result(
        name=name,
        samples=len(values),
        units_per_call=units_per_call,
        unit=unit,
        seconds={
            "median": median,
            "p95": percentile(0.95),
            "p99": percentile(0.99),
            "min": values[0],
            "max": values[-1],
        },
        throughput=units_per_call / median,
        tracemalloc_peak_bytes=None,
    )


def measure(cases: Sequence[Case], options: Options) -> list[Result]:
    if not cases or len({c.name for c in cases}) != len(cases):
        raise ValueError("select at least one case, each with a unique name")
    if options.memory and tracemalloc.is_tracing():
        raise ValueError(
            "memory measurement requires ownership of the tracemalloc session"
        )
    samples: list[list[float]] = [[] for _ in cases]
    elapsed = [0.0] * len(cases)
    active = list(range(len(cases)))
    was_enabled = gc.isenabled()
    rng_state = random.getstate()
    try:
        if options.disable_gc:
            gc.disable()
        round_no = 0
        while active:
            for i in reversed(active) if options.alternate and round_no % 2 else active:
                random.seed(options.seed + round_no)
                start = time.perf_counter_ns()
                cases[i].run()
                duration = (time.perf_counter_ns() - start) / 1e9
                if round_no >= options.warmup:
                    samples[i].append(duration)
                    elapsed[i] += duration
            round_no += 1
            active = [
                i
                for i in active
                if (
                    len(samples[i]) < options.iterations
                    if options.iterations is not None
                    else elapsed[i] < float(options.seconds or 0)
                )
            ]
        results = [
            summarize(c.name, s, c.units_per_call, c.unit)
            for c, s in zip(cases, samples)
        ]
        if options.memory:
            for case, result in zip(cases, results):
                random.seed(options.seed)
                tracemalloc.start()
                try:
                    case.run()
                    result["tracemalloc_peak_bytes"] = tracemalloc.get_traced_memory()[
                        1
                    ]
                finally:
                    tracemalloc.stop()
        return results
    finally:
        random.setstate(rng_state)
        (gc.enable if was_enabled else gc.disable)()
