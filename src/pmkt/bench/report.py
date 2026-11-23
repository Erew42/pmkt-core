"""Versioned report shared by the public and private benchmark commands."""

from __future__ import annotations

import json
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypedDict

import pmkt
from pmkt.provenance import implementation_identity

from .runner import Result

SCHEMA_VERSION = "pmkt.bench.v1"


class Report(TypedDict):
    schema_version: str
    suite: str
    repo: str
    git_sha: str | None
    dirty: bool | None
    core_build_info: dict[str, Any]
    runtime: dict[str, Any]
    started_at_utc: str
    dataset: dict[str, Any]
    options: dict[str, Any]
    cases: list[Result]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_report(
    *,
    suite: str,
    repo: str,
    distribution: str,
    package_file: str | Path,
    dataset: dict[str, Any],
    options: dict[str, Any],
    cases: list[Result],
    started_at_utc: str,
) -> Report:
    identity = implementation_identity(package_file, distribution).require_consistent()
    core = implementation_identity(pmkt.__file__, "pmkt").require_consistent()
    build = Path(pmkt.__file__).with_name("_build_info.json")
    info = (
        json.loads(build.read_text(encoding="utf-8"))
        if build.exists()
        else {
            "distribution": core.distribution,
            "version": core.version,
            "commit": core.commit,
            "dirty": core.dirty,
            "evidence_source": core.evidence_source,
        }
    )
    try:
        rust = subprocess.check_output(
            ["rustc", "--version"], text=True, timeout=5
        ).strip()
    except (OSError, subprocess.SubprocessError):
        rust = None
    cpu = platform.processor() or platform.machine()
    cpuinfo = Path("/proc/cpuinfo")
    if sys.platform.startswith("linux") and cpuinfo.exists():
        cpu = next(
            (
                line.partition(":")[2].strip()
                for line in cpuinfo.read_text().splitlines()
                if line.startswith("model name")
            ),
            cpu,
        )
    return Report(
        schema_version=SCHEMA_VERSION,
        suite=suite,
        repo=repo,
        git_sha=identity.commit,
        dirty=identity.dirty,
        core_build_info=info,
        started_at_utc=started_at_utc,
        runtime={
            "python": platform.python_version(),
            "rustc": rust,
            "os": platform.platform(),
            "cpu": cpu,
            "machine": platform.machine(),
        },
        dataset=dataset,
        options=options,
        cases=cases,
    )


def write_report(report: Report, output: Path | None) -> None:
    payload = json.dumps(report, indent=2, allow_nan=False) + "\n"
    if output is None:
        sys.stdout.write(payload)
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        # A mistyped output path must never truncate a capture or a baseline.
        with output.open("x", encoding="utf-8") as handle:
            handle.write(payload)
