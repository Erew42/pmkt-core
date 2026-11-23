from __future__ import annotations

import copy
import gc
import json
import random
import subprocess
import sys

import pytest

from pmkt.bench import Case, Options, compare, measure
from pmkt.bench.criterion import read_results
from pmkt.bench.runner import summarize


def test_percentiles_and_throughput():
    result = summarize("batch", [1, 3, 2], 600, "frames")
    assert result["seconds"] == pytest.approx(
        {"median": 2, "p95": 2.9, "p99": 2.98, "min": 1, "max": 3}
    )
    assert result["throughput"] == 300


def test_zero_durations_below_clock_resolution_are_valid():
    # Windows perf_counter_ns advances in 100 ns steps, so an empty call can measure 0.
    result = summarize("noop", [0, 0, 1e-7], 1, "ops")
    assert result["seconds"]["median"] == 0 and result["throughput"] is None
    with pytest.raises(ValueError):
        summarize("bad", [-1e-9], 1, "ops")


def test_alternating_rounds_and_rng_restore():
    calls = []

    def record(name):
        calls.append((name, random.random(), gc.isenabled()))

    state = random.getstate()
    before = gc.isenabled()
    results = measure(
        [Case(name, lambda name=name: record(name)) for name in "ab"],
        Options(warmup=1, iterations=2, alternate=True, disable_gc=True, memory=True),
    )
    assert "".join(row[0] for row in calls) == "abbaabab"
    assert calls[0][1] == calls[1][1] != calls[2][1]
    assert not any(row[2] for row in calls)
    assert gc.isenabled() == before and random.getstate() == state
    assert all(
        r["samples"] == 2 and r["tracemalloc_peak_bytes"] is not None for r in results
    )


def test_budget_and_invalid_options():
    [row] = measure(
        [Case("probe", lambda: None)], Options(warmup=0, iterations=None, seconds=1e-8)
    )
    assert row["samples"] >= 1
    with pytest.raises(ValueError):
        Options(iterations=1, seconds=1)
    with pytest.raises(ValueError):
        Options(iterations=None, seconds=float("nan"))


def test_installed_cli_and_comparison(tmp_path):
    base = tmp_path / "base.json"
    cmd = [sys.executable, "-m", "pmkt.bench"]
    subprocess.run(
        [
            *cmd,
            "run",
            "--warmup",
            "0",
            "--iterations",
            "1",
            "--case",
            "python.cold_process_import",
            "--output",
            str(base),
        ],
        cwd=tmp_path,
        check=True,
    )
    report = json.loads(base.read_text())
    assert (
        report["schema_version"] == "pmkt.bench.v1"
        and report["dataset"]["kind"] == "fixture"
    )
    assert "dirty" in report and "commit" in report["core_build_info"]
    candidate = copy.deepcopy(report)
    candidate["cases"][0]["seconds"]["p95"] *= 1.5
    # p95 is shown but does not gate.
    table, failed = compare(report, candidate, 15)
    assert not failed and "+50.00%" in table and "Warning" not in table
    candidate["cases"][0]["seconds"]["median"] *= 1.2
    path = tmp_path / "candidate.json"
    path.write_text(json.dumps(candidate))
    diff = subprocess.run(
        [
            *cmd,
            "compare",
            str(base),
            str(path),
            "--threshold",
            "15",
            "--fail-on-regression",
        ],
        capture_output=True,
        text=True,
    )
    assert diff.returncode == 1 and "+20.00%" in diff.stdout
    assert not compare(report, candidate, 25)[1]
    candidate["options"]["iterations"] = 20
    candidate["runtime"]["cpu"] = "other"
    table, _ = compare(report, candidate)
    assert "Warning: options.iterations differs: 1 vs 20" in table
    assert "Warning: runtime.cpu differs" in table
    zero = copy.deepcopy(report)
    zero["cases"][0]["seconds"]["median"] = 0.0
    zero["cases"][0]["throughput"] = None
    table, failed = compare(zero, report)
    assert not failed and "n/a" in table
    candidate["dataset"]["kind"] = "external"
    with pytest.raises(ValueError, match="dataset"):
        compare(report, candidate)


def test_criterion_translates_amortized_timings(tmp_path):
    batch = tmp_path / "probe/new"
    batch.mkdir(parents=True)
    (batch / "sample.json").write_text(
        json.dumps({"times": [100, 1000], "iters": [2, 10]})
    )
    (batch / "estimates.json").write_text(
        json.dumps({"median": {"point_estimate": 75}})
    )
    (batch / "benchmark.json").write_text(
        json.dumps({"full_id": "probe", "throughput": {"Elements": 4}})
    )
    [case] = read_results(tmp_path)
    assert case["seconds"] == pytest.approx(
        {"min": 50e-9, "max": 100e-9, "median": 75e-9, "p95": 97.5e-9, "p99": 99.5e-9}
    )
    assert case["throughput"] == pytest.approx(4 / 75e-9)


def test_windows_timer_resolution_noop_is_stable():
    # Regression: zero-length samples used to fail summarize intermittently.
    for _ in range(200):
        [row] = measure([Case("noop", lambda: None)], Options(warmup=0, iterations=10))
        assert row["seconds"]["median"] >= 0


def test_criterion_source_provenance_flags_mixed_builds():
    from pmkt.bench.criterion import source_provenance

    report = {
        "git_sha": "a" * 40,
        "dirty": False,
        "core_build_info": {"commit": "b" * 40},
    }
    assert source_provenance(report) == {
        "source_root_commit": "a" * 40,
        "source_root_dirty": False,
        "installed_core_commit": "b" * 40,
        "matches_installed_core": False,
    }
    report["core_build_info"]["commit"] = "a" * 40
    assert source_provenance(report)["matches_installed_core"]


def test_setup_runs_untimed_before_every_call():
    events = []
    case = Case(
        "reset",
        lambda: events.append("run"),
        setup=lambda: events.append("setup"),
    )
    measure([case], Options(warmup=1, iterations=2, memory=True))
    assert events == ["setup", "run"] * 4
