"""Compare matching workload definitions from two pmkt.bench.v1 reports."""

from __future__ import annotations

import math

from .report import SCHEMA_VERSION, Report
from .runner import Result

# Settings that change what a sample measures without changing the workloads.
RUNTIME_FIELDS = ("os", "cpu", "machine", "python", "rustc")


def setting_differences(baseline: Report, candidate: Report) -> list[str]:
    """Name run options and host fields that differ between two reports."""
    notes = []
    for section, left, right, keys in (
        (
            "options",
            baseline["options"],
            candidate["options"],
            sorted(baseline["options"].keys() | candidate["options"].keys()),
        ),
        ("runtime", baseline["runtime"], candidate["runtime"], RUNTIME_FIELDS),
    ):
        for key in keys:
            if left.get(key) != right.get(key):
                notes.append(
                    f"{section}.{key} differs: {left.get(key)!r} vs {right.get(key)!r}"
                )
    return notes


def compare(
    baseline: Report, candidate: Report, threshold: float = 5.0
) -> tuple[str, bool]:
    """Return a Markdown table and whether any median latency regressed.

    Only the median gates: p95 is noisy at typical sample counts and throughput
    is derived from the median. Both are still shown. Differing run settings are
    listed as warnings above the table.
    """
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
    lines = [f"Warning: {note}" for note in setting_differences(baseline, candidate)]
    if lines:
        lines.append("")
    lines += [
        "| Case | Median change | p95 change | Throughput change | Result |",
        "|---|---:|---:|---:|---|",
    ]
    failed = False
    for name, a in left.items():
        b = right[name]
        if (a["unit"], a["units_per_call"]) != (b["unit"], b["units_per_call"]):
            raise ValueError(f"{name}: throughput unit or work per call differs")

        def delta(before: float | None, after: float | None) -> float | None:
            if before is None or after is None:
                return None
            if not all(math.isfinite(x) and x >= 0 for x in (before, after)):
                raise ValueError(f"{name}: invalid measurement")
            # A zero baseline is below clock resolution; no relative change exists.
            return 100 * (after - before) / before if before > 0 else None

        def cell(change: float | None) -> str:
            return "n/a" if change is None else f"{change:+.2f}%"

        median = delta(a["seconds"]["median"], b["seconds"]["median"])
        p95 = delta(a["seconds"]["p95"], b["seconds"]["p95"])
        rate = delta(a["throughput"], b["throughput"])
        regression = median is not None and median > threshold
        failed |= regression
        lines.append(
            f"| {name} | {cell(median)} | {cell(p95)} | {cell(rate)} | "
            f"{'regression' if regression else 'ok'} |"
        )
    return "\n".join(lines), failed
