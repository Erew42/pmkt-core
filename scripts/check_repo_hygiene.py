from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

MAX_TRACKED_FILE_BYTES = 1_048_576

FORBIDDEN_PATH_PREFIXES = (
    ".venv/",
    "__pycache__/",
    ".pytest_cache/",
    ".mypy_cache/",
    ".ruff_cache/",
    ".cache/",
    ".tox/",
    ".nox/",
    "data/",
    "generated/",
    "output/",
    "runs/",
    "local_data/",
    "sample runs/",
    "artifacts/",
    "polymarket_workspace/",
    "docs/upstream_snapshots/",
)

FORBIDDEN_EXACT_PATHS = {
    ".coverage",
    "coverage.xml",
    "docs/plan1.md",
    "secrets.toml",
    ".streamlit/secrets.toml",
}

FORBIDDEN_DIR_NAMES = {
    "__pycache__",
    "build",
    "dist",
}

FORBIDDEN_DIR_SUFFIXES = (
    ".egg-info",
)

FORBIDDEN_SUFFIXES = (
    ".pyc",
    ".pyo",
    ".pyd",
    ".zip",
    ".parquet",
    ".duckdb",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".feather",
    ".arrow",
    ".pem",
    ".key",
    ".p8",
    ".p12",
    ".log",
    ":zone.identifier",
)

PRIVATE_KEY_RE = re.compile(
    rb"-----BEGIN (?:RSA |EC |OPENSSH |DSA |)?PRIVATE KEY-----"
)
SECRET_ENV_NAMES = {
    "PMKT_HOST_SECRET",
    "PMKT_POLYMARKET_API_KEY",
    "PMKT_POLYMARKET_API_SECRET",
    "PMKT_POLYMARKET_API_PASSPHRASE",
    "PMKT_POLYMARKET_L1_KEY",
    "PMKT_POLYMARKET_L2_KEY",
    "PMKT_POLYMARKET_PROXY_ADDRESS",
    "PMKT_POLYMARKET_PRIVATE_KEY_PATH",
    "PMKT_POLYMARKET_PRIVATE_KEY",
    "PMKT_POLYMARKET_FUNDER_ADDRESS",
    "PMKT_KALSHI_API_KEY_ID",
    "PMKT_KALSHI_PRIVATE_KEY_PATH",
    "PMKT_KALSHI_PRIVATE_KEY_PEM",
}
SECRET_ASSIGNMENT_RE = re.compile(
    r"^\s*(?:export\s+)?(PMKT_[A-Z0-9_]+)\s*=\s*(.+?)\s*$"
)


@dataclass(frozen=True)
class HygieneIssue:
    path: str
    reason: str


def normalize_repo_path(path: str) -> str:
    normalized = path.replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def tracked_files(root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return [normalize_repo_path(line) for line in result.stdout.splitlines() if line.strip()]


def _is_root_text_artifact(path: str) -> bool:
    if "/" in path:
        return False
    name = Path(path).name.lower()
    return (
        name == "out.txt"
        or name.endswith("_out.txt")
        or name.endswith("_output.txt")
    )


def _is_secret_file(path: str) -> bool:
    name = Path(path).name.lower()
    return (
        name == ".env"
        or name.startswith(".env.")
        or name.startswith("credentials") and name.endswith(".json")
        or name == "secrets.toml"
    )


def _is_placeholder_secret(value: str) -> bool:
    cleaned = value.strip().strip("'\"")
    lowered = cleaned.lower()
    if not cleaned:
        return True
    return (
        lowered in {"placeholder", "dummy", "example", "changeme", "redacted"}
        or lowered.startswith("your-")
        or lowered.startswith("<")
        or lowered.startswith("${")
        or "example" in lowered
        or "placeholder" in lowered
        or "redacted" in lowered
    )


def _path_policy_issues(path: str) -> list[HygieneIssue]:
    normalized = normalize_repo_path(path)
    lowered = normalized.lower()
    issues: list[HygieneIssue] = []

    for prefix in FORBIDDEN_PATH_PREFIXES:
        if lowered == prefix.rstrip("/") or lowered.startswith(prefix):
            issues.append(HygieneIssue(normalized, f"tracked generated/local path: {prefix}"))
            break

    path_parts = [part for part in lowered.split("/") if part]
    if any(part in FORBIDDEN_DIR_NAMES for part in path_parts):
        issues.append(HygieneIssue(normalized, "tracked build/cache directory path"))
    elif any(part.endswith(FORBIDDEN_DIR_SUFFIXES) for part in path_parts):
        issues.append(HygieneIssue(normalized, "tracked build metadata directory path"))

    if lowered in FORBIDDEN_EXACT_PATHS:
        issues.append(HygieneIssue(normalized, "tracked local planning or secret-adjacent file"))

    if lowered.endswith(FORBIDDEN_SUFFIXES):
        issues.append(HygieneIssue(normalized, "tracked archive, generated data, log, or secret file type"))

    if _is_root_text_artifact(lowered):
        issues.append(HygieneIssue(normalized, "tracked root one-off output text file"))

    if _is_secret_file(lowered):
        issues.append(HygieneIssue(normalized, "tracked secret-bearing filename"))

    return issues


def _content_policy_issues(root: Path, path: str) -> list[HygieneIssue]:
    normalized = normalize_repo_path(path)
    full_path = root / normalized
    issues: list[HygieneIssue] = []
    if not full_path.exists() or not full_path.is_file():
        return issues

    size = full_path.stat().st_size
    if size > MAX_TRACKED_FILE_BYTES:
        issues.append(
            HygieneIssue(
                normalized,
                f"tracked file exceeds {MAX_TRACKED_FILE_BYTES} bytes",
            )
        )

    sample = full_path.read_bytes()[:MAX_TRACKED_FILE_BYTES]
    if PRIVATE_KEY_RE.search(sample):
        issues.append(HygieneIssue(normalized, "tracked private key material"))

    try:
        text = sample.decode("utf-8")
    except UnicodeDecodeError:
        return issues

    for line in text.splitlines():
        match = SECRET_ASSIGNMENT_RE.match(line)
        if (
            match
            and match.group(1) in SECRET_ENV_NAMES
            and not _is_placeholder_secret(match.group(2))
        ):
            issues.append(
                HygieneIssue(
                    normalized,
                    f"tracked non-placeholder {match.group(1)} assignment",
                )
            )

    return issues


def check_repo_hygiene(root: Path, files: Iterable[str] | None = None) -> list[HygieneIssue]:
    repo_files = list(files) if files is not None else tracked_files(root)
    issues: list[HygieneIssue] = []
    for path in repo_files:
        normalized = normalize_repo_path(path)
        issues.extend(_path_policy_issues(normalized))
        issues.extend(_content_policy_issues(root, normalized))
    return issues


def format_issues(issues: Sequence[HygieneIssue]) -> str:
    lines = ["Repository hygiene check failed:"]
    for issue in issues:
        lines.append(f"- {issue.path}: {issue.reason}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fail if tracked files include local artifacts, generated data, or obvious secrets.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Repository root. Defaults to the current working directory.",
    )
    parser.add_argument(
        "--file-list",
        type=Path,
        help=(
            "Optional newline-delimited archive/source file list to check instead "
            "of reading tracked files from Git."
        ),
    )
    args = parser.parse_args(argv)

    files = None
    if args.file_list is not None:
        files = [
            normalize_repo_path(line)
            for line in args.file_list.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    issues = check_repo_hygiene(args.root, files)
    if issues:
        print(format_issues(issues), file=sys.stderr)
        return 1
    print("Repository hygiene check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
