"""Compare matching workload definitions from two pmkt.bench.v1 reports."""

from __future__ import annotations

import math

from .report import SCHEMA_VERSION, Report
from .runner import Result


def compare(
    baseline: Report, candidate: Report, threshold: float = 5.0
) -> tuple[str, bool]:
    """Return a Markdown table and whether any latency or throughput regressed."""
    if not math.isfinite(threshold) or threshold < 0:
        raise ValueError("regression threshold must be finite and nonnegative")
    for report in (baseline, candidate):
        if report["schema_version"] != SCHEMA_VERSION:
            raise ValueError("unsupported report schema_version")
    for field in ("suite", "repo", "dataset"):
        if baseline[field] != candidate[field]:
            raise ValueError(f"cannot compare reports with different {field}")

    if baseline["options"].get("seed") != candidate["options"].get("seed"):
        raise ValueError("cannot compare workloads generated with different seeds")

    def index(report: Report) -> dict[str, Result]:
        entries = {r["name"]: r for r in report["cases"]}
        if not entries or len(entries) != len(report["cases"]):
            raise ValueError("empty or duplicate case names")
        return entries

    left, right = index(baseline), index(candidate)
    if left.keys() != right.keys():
        raise ValueError("case sets differ; select the same cases in both runs")
    lines = [
        "| Case | Median change | p95 change | Throughput change | Result |",
        "|---|---:|---:|---:|---|",
    ]
    failed = False
    for name, a in left.items():
        b = right[name]
        if (a["unit"], a["units_per_call"]) != (b["unit"], b["units_per_call"]):
            raise ValueError(f"{name}: throughput unit or work per call differs")

        def delta(before: float, after: float) -> float:
            if not all(math.isfinite(x) and x > 0 for x in (before, after)):
                raise ValueError(f"{name}: invalid measurement")
            return 100 * (after - before) / before

        median = delta(a["seconds"]["median"], b["seconds"]["median"])
        p95 = delta(a["seconds"]["p95"], b["seconds"]["p95"])
        rate = delta(a["throughput"], b["throughput"])
        regression = max(median, p95, -rate) > threshold
        failed |= regression
        lines.append(
            f"| {name} | {median:+.2f}% | {p95:+.2f}% | {rate:+.2f}% | "
            f"{'regression' if regression else 'ok'} |"
        )
    return "\n".join(lines), failed
