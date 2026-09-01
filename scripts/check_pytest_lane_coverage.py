from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path

TEST_PATH_RE = re.compile(r"tests/test_[A-Za-z0-9_./-]+\.py")


def _tracked_test_files(root: Path, tests_dir: str) -> set[str]:
    tests_glob = f"{tests_dir.rstrip('/')}/test_*.py"
    try:
        result = subprocess.run(
            ["git", "ls-files", tests_glob],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return {
            path.relative_to(root).as_posix()
            for path in (root / tests_dir).glob("test_*.py")
            if path.is_file()
        }
    tracked = {
        line.strip().replace("\\", "/")
        for line in result.stdout.splitlines()
        if line.strip()
    }
    if tracked:
        return tracked
    # A clean-history extraction has no tracked paths before its initial
    # commit. Validate the actual test tree during that bootstrap window.
    return {
        path.relative_to(root).as_posix()
        for path in (root / tests_dir).glob("test_*.py")
        if path.is_file()
    }


def assigned_pytest_lane_paths(workflow_path: Path) -> set[str]:
    lines = workflow_path.read_text(encoding="utf-8").splitlines()
    assigned: set[str] = set()
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped.startswith("paths:"):
            continue
        path_indent = len(line) - len(line.lstrip())
        for candidate in lines[index + 1 :]:
            if candidate.strip():
                indent = len(candidate) - len(candidate.lstrip())
                if indent <= path_indent:
                    break
            normalized = candidate.replace("\\", "/")
            assigned.update(match.group(0) for match in TEST_PATH_RE.finditer(normalized))
    return assigned


def check_pytest_lane_coverage(
    workflow_path: Path,
    tests_dir: Path,
    *,
    root: Path | None = None,
) -> tuple[list[str], list[str]]:
    root = root or Path.cwd()
    workflow_path = workflow_path if workflow_path.is_absolute() else root / workflow_path
    tests_dir_text = tests_dir.as_posix().strip("/") or "tests"

    tracked = _tracked_test_files(root, tests_dir_text)
    assigned = assigned_pytest_lane_paths(workflow_path)
    expected_prefix = f"{tests_dir_text}/test_"

    missing = sorted(tracked - assigned)
    stale = sorted(path for path in assigned - tracked if path.startswith(expected_prefix))
    return missing, stale


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check every tracked tests/test_*.py file is assigned to a pytest lane.",
    )
    parser.add_argument("workflow", type=Path)
    parser.add_argument("tests_dir", type=Path, nargs="?", default=Path("tests"))
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)

    missing, stale = check_pytest_lane_coverage(
        args.workflow,
        args.tests_dir,
        root=args.root,
    )
    if missing or stale:
        if missing:
            print("Missing pytest lane assignments:")
            for path in missing:
                print(f"  {path}")
        if stale:
            print("Stale pytest lane assignments:")
            for path in stale:
                print(f"  {path}")
        return 1

    print("Pytest lane coverage check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
