"""Run this repository's Criterion target and translate its per-iteration samples."""

from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from .report import Report, make_report, utc_now
from .runner import Result, summarize


def read_results(root: Path) -> list[Result]:
    results: list[Result] = []
    for path in sorted(root.rglob("new/estimates.json")):
        meta = json.loads(path.with_name("benchmark.json").read_text())
        sample = json.loads(path.with_name("sample.json").read_text())
        durations = [
            t / n / 1e9 for t, n in zip(sample["times"], sample["iters"], strict=True)
        ]
        work = meta["throughput"]
        unit, count = ("frames", work["Elements"]) if work else ("ops", 1)
        result = summarize(meta["full_id"], durations, count, unit)
        # Criterion aggregates many invocations per sample. Its confidence bounds
        # estimate uncertainty in the median; they are not latency percentiles.
        median = json.loads(path.read_text())["median"]["point_estimate"] / 1e9
        if not math.isfinite(median) or median <= 0:
            raise ValueError(f"{path}: invalid median")
        result["seconds"]["median"] = median
        result["throughput"] = count / median
        results.append(result)
    if not results:
        raise ValueError("Criterion produced no matching measurements")
    return results


def _dataset(source_root: Path, data: Path | None) -> dict[str, Any]:
    fixture_root = source_root / "tests/fixtures/raw_capture"
    paths = []
    if data:
        chunks = [data] if data.is_file() else sorted(data.glob("chunk_*.jsonl.gz"))
        if not chunks:
            raise ValueError("expected a sealed capture chunk or session directory")
        for chunk in chunks:
            paths.extend(
                [
                    (chunk, "external"),
                    (chunk.with_name(chunk.name + ".manifest.json"), "external"),
                ]
            )
    else:
        paths = [
            (fixture_root / f"{venue}_00.jsonl.gz", "fixture")
            for venue in ("polymarket", "kalshi")
        ]
    files = []
    for path, role in paths:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1 << 20), b""):
                digest.update(block)
        files.append(
            {
                "name": path.name,
                "role": role,
                "sha256": digest.hexdigest(),
                "bytes": path.stat().st_size,
            }
        )
    return {"kind": "external" if data else "fixture", "files": files}


def run_criterion(
    source_root: Path,
    data: Path | None,
    case_filter: str,
    sample_size: int,
    warmup_seconds: float,
    measurement_seconds: float,
) -> Report:
    root = source_root.resolve()
    if not (root / "crates/pmkt-engine/benches/engine_bench.rs").is_file():
        raise ValueError("--source-root must point to a pmkt-core source checkout")
    if sample_size < 10 or not all(
        math.isfinite(t) and t > 0 for t in (warmup_seconds, measurement_seconds)
    ):
        raise ValueError(
            "Criterion needs at least 10 samples and positive finite timing budgets"
        )
    started = utc_now()
    dataset = _dataset(root, data)
    with tempfile.TemporaryDirectory(prefix="pmkt-criterion-") as temp:
        env = {**os.environ, "CRITERION_HOME": temp}
        env.pop("PMKT_BENCH_DATA_DIR", None)
        if data:
            env["PMKT_BENCH_DATA_DIR"] = str(data.resolve())
        cmd = [
            "cargo",
            "bench",
            "--locked",
            "-p",
            "pmkt-engine",
            "--bench",
            "engine_bench",
            "--",
            "--sample-size",
            str(sample_size),
            "--warm-up-time",
            str(warmup_seconds),
            "--measurement-time",
            str(measurement_seconds),
            "--noplot",
        ]
        if case_filter:
            cmd.append(case_filter)
        subprocess.run(cmd, cwd=root, env=env, stdout=sys.stderr, check=True)
        results = read_results(Path(temp))
    return make_report(
        suite="criterion",
        repo="pmkt-core",
        distribution="pmkt",
        package_file=root / "src/pmkt/__init__.py",
        dataset=dataset,
        options={
            "sample_size": sample_size,
            "warmup_seconds": warmup_seconds,
            "measurement_seconds": measurement_seconds,
            "filter": case_filter,
        },
        cases=results,
        started_at_utc=started,
    )
