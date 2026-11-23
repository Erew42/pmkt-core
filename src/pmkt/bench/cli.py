"""Shared CLI for selecting cases, running rounds and comparing saved reports."""

from __future__ import annotations

import argparse
import fnmatch
import json
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any, ContextManager

import pmkt

from .compare import compare
from .report import Report, make_report, utc_now, write_report
from .runner import Case, Options, measure

Workloads = Callable[
    [Path | None, int, Callable[[str], bool]],
    ContextManager[tuple[dict[str, Any], list[Case]]],
]


def suite_main(
    *,
    prog: str,
    suite: str,
    repo: str,
    distribution: str,
    package_file: str,
    workloads: Workloads,
    argv: Sequence[str] | None = None,
    criterion: bool = False,
) -> int:
    parser = argparse.ArgumentParser(prog=prog)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="time the selected offline workloads")
    run.add_argument(
        "--case",
        action="append",
        default=[],
        help="case name or shell glob (repeatable)",
    )
    run.add_argument(
        "--data-dir", type=Path, help="external read-only dataset directory"
    )
    run.add_argument(
        "--output", type=Path, help="write a new JSON file (default: stdout)"
    )
    run.add_argument("--warmup", type=int, default=2, help="untimed rounds per case")
    budget = run.add_mutually_exclusive_group()
    budget.add_argument(
        "--iterations", type=int, help="measured rounds per case (default: 10)"
    )
    budget.add_argument(
        "--seconds", type=float, help="cumulative measured seconds per case"
    )
    run.add_argument("--seed", type=int, default=42)
    run.add_argument(
        "--alternate", action="store_true", help="reverse case order every other round"
    )
    run.add_argument("--disable-gc", action="store_true")
    run.add_argument(
        "--memory", action="store_true", help="separate tracemalloc run per case"
    )
    diff = commands.add_parser("compare", help="show changes between two JSON reports")
    diff.add_argument("baseline", type=Path)
    diff.add_argument("candidate", type=Path)
    diff.add_argument(
        "--threshold", type=float, default=5, help="regression threshold in percent"
    )
    diff.add_argument("--fail-on-regression", action="store_true")
    if criterion:
        rust = commands.add_parser(
            "criterion", help="run the Rust suite in a core source checkout"
        )
        rust.add_argument("--source-root", type=Path, default=Path.cwd())
        rust.add_argument(
            "--data-dir", type=Path, help="one sealed session directory or chunk"
        )
        rust.add_argument("--filter", default="", help="Criterion benchmark name regex")
        rust.add_argument("--sample-size", type=int, default=20)
        rust.add_argument("--warmup-seconds", type=float, default=1.0)
        rust.add_argument("--measurement-seconds", type=float, default=2.0)
        rust.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "compare":
            table, failed = compare(
                json.loads(args.baseline.read_text(encoding="utf-8")),
                json.loads(args.candidate.read_text(encoding="utf-8")),
                args.threshold,
            )
            print(table)
            return int(failed and args.fail_on_regression)
        report: Report
        if args.output and args.output.exists():
            raise ValueError(f"output already exists: {args.output}")
        if args.command == "criterion":
            from .criterion import run_criterion

            report = run_criterion(
                args.source_root,
                args.data_dir,
                args.filter,
                args.sample_size,
                args.warmup_seconds,
                args.measurement_seconds,
            )
        else:
            iterations = args.iterations
            if iterations is None and args.seconds is None:
                iterations = 10
            options = Options(
                args.warmup,
                iterations,
                args.seconds,
                args.seed,
                args.alternate,
                args.disable_gc,
                args.memory,
            )

            def selected(name: str) -> bool:
                return not args.case or any(
                    fnmatch.fnmatchcase(name, pat) for pat in args.case
                )

            started = utc_now()
            with workloads(args.data_dir, args.seed, selected) as (dataset, cases):
                cases = [c for c in cases if selected(c.name)]
                for pattern in args.case:
                    if not any(fnmatch.fnmatchcase(c.name, pattern) for c in cases):
                        raise ValueError(
                            f"pattern matches no available cases: {pattern}"
                        )
                report = make_report(
                    suite=suite,
                    repo=repo,
                    distribution=distribution,
                    package_file=package_file,
                    dataset=dataset,
                    options=asdict(options),
                    cases=measure(cases, options),
                    started_at_utc=started,
                )
        write_report(report, args.output)
        return 0
    except (
        ValueError,
        TypeError,
        KeyError,
        OSError,
        ImportError,
        subprocess.CalledProcessError,
    ) as exc:
        parser.exit(2, f"{prog}: {exc}\n")


def main(argv: Sequence[str] | None = None) -> int:
    from .workloads import workloads

    return suite_main(
        prog="pmkt-bench",
        suite="read-plane",
        repo="pmkt-core",
        distribution="pmkt",
        package_file=pmkt.__file__,
        workloads=workloads,
        argv=argv,
        criterion=True,
    )
