from __future__ import annotations

from pathlib import Path

from scripts.check_pytest_lane_coverage import check_pytest_lane_coverage
from scripts.check_pytest_lane_coverage import main as lane_main
from scripts.check_repo_hygiene import (
    MAX_TRACKED_FILE_BYTES,
    check_repo_hygiene,
    main,
)


def _write(root: Path, path: str, content: bytes | str = "") -> None:
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, bytes):
        target.write_bytes(content)
    else:
        target.write_text(content, encoding="utf-8")


def _reasons(root: Path, files: list[str]) -> list[str]:
    return [issue.reason for issue in check_repo_hygiene(root, files)]


def test_repo_hygiene_rejects_local_planning_file(tmp_path: Path) -> None:
    _write(tmp_path, "docs/Plan1.md", "local credential-adjacent notes")

    reasons = _reasons(tmp_path, ["docs/Plan1.md"])

    assert "tracked local planning or secret-adjacent file" in reasons


def test_repo_hygiene_rejects_local_workspace_files(tmp_path: Path) -> None:
    _write(tmp_path, "Polymarket_workspace/reports/notes.md", "local-only")

    reasons = _reasons(tmp_path, ["Polymarket_workspace/reports/notes.md"])

    assert "tracked generated/local path: polymarket_workspace/" in reasons


def test_repo_hygiene_rejects_generated_data_archive_and_secret_names(tmp_path: Path) -> None:
    files = [
        "data/run/observations.parquet",
        "generated/report.json",
        "output/report.txt",
        "docs/upstream_snapshots/snapshot.json",
        "sample.zip",
        "sample.db",
        ".env",
        "credentials-dev.json",
    ]
    for path in files:
        _write(tmp_path, path, "{}")

    issues = check_repo_hygiene(tmp_path, files)
    issue_paths = {issue.path for issue in issues}

    assert "data/run/observations.parquet" in issue_paths
    assert "generated/report.json" in issue_paths
    assert "output/report.txt" in issue_paths
    assert "docs/upstream_snapshots/snapshot.json" in issue_paths
    assert "sample.zip" in issue_paths
    assert "sample.db" in issue_paths
    assert ".env" in issue_paths
    assert "credentials-dev.json" in issue_paths


def test_repo_hygiene_rejects_build_cache_and_coverage_files(tmp_path: Path) -> None:
    files = [
        "build/lib/pmkt.py",
        "dist/pmkt.whl",
        "src/pmkt.egg-info/PKG-INFO",
        "src/pmkt/__pycache__/models.cpython-310.pyc",
        ".coverage",
        "coverage.xml",
    ]
    for path in files:
        _write(tmp_path, path, "local artifact")

    issues = check_repo_hygiene(tmp_path, files)
    issue_paths = {issue.path for issue in issues}

    assert issue_paths == set(files)


def test_repo_hygiene_rejects_large_tracked_file(tmp_path: Path) -> None:
    _write(tmp_path, "src/pmkt/large_fixture.bin", b"x" * (MAX_TRACKED_FILE_BYTES + 1))

    reasons = _reasons(tmp_path, ["src/pmkt/large_fixture.bin"])

    assert f"tracked file exceeds {MAX_TRACKED_FILE_BYTES} bytes" in reasons


def test_repo_hygiene_rejects_private_key_and_real_secret_assignments(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "docs/config.md",
        "\n".join(
            [
                "-----BEGIN " + "PRIVATE KEY-----",
                "PMKT_KALSHI_API_KEY_ID=real-looking-value",
                "PMKT_POLYMARKET_API_SECRET=real-looking-polymarket-secret",
                "PMKT_POLYMARKET_PRIVATE_KEY_PATH=C:\\Users\\erik9\\.polymarket\\key.txt",
                "PMKT_POLYMARKET_FUNDER_ADDRESS=0x1234567890123456789012345678901234567890",
            ]
        ),
    )

    reasons = _reasons(tmp_path, ["docs/config.md"])

    assert "tracked private key material" in reasons
    assert "tracked non-placeholder PMKT_KALSHI_API_KEY_ID assignment" in reasons
    assert "tracked non-placeholder PMKT_POLYMARKET_API_SECRET assignment" in reasons
    assert "tracked non-placeholder PMKT_POLYMARKET_PRIVATE_KEY_PATH assignment" in reasons
    assert "tracked non-placeholder PMKT_POLYMARKET_FUNDER_ADDRESS assignment" in reasons


def test_repo_hygiene_allows_source_docs_tests_and_placeholder_secrets(tmp_path: Path) -> None:
    files = [
        "src/pmkt/example.py",
        "tests/test_example.py",
        "docs/example.md",
        "openapi/polymarket.min.json",
    ]
    for path in files:
        _write(
            tmp_path,
            path,
            "\n".join(
                [
                    "PMKT_KALSHI_API_KEY_ID=<your-key-id>",
                    "PMKT_POLYMARKET_API_SECRET=${PMKT_POLYMARKET_API_SECRET}",
                    "PMKT_CLOB_API_URL=https://example.test/clob",
                ]
            ),
        )

    issues = check_repo_hygiene(tmp_path, files)

    assert issues == []


def test_repo_hygiene_cli_accepts_explicit_archive_file_list(tmp_path: Path) -> None:
    _write(tmp_path, "src/pmkt/example.py", "print('ok')\n")
    file_list = tmp_path / "archive-files.txt"
    file_list.write_text("src/pmkt/example.py\n", encoding="utf-8")

    result = main(["--root", str(tmp_path), "--file-list", str(file_list)])

    assert result == 0


def test_repo_hygiene_cli_rejects_forbidden_archive_file_list_path(tmp_path: Path) -> None:
    _write(tmp_path, "src/pmkt.egg-info/PKG-INFO", "local artifact")
    file_list = tmp_path / "archive-files.txt"
    file_list.write_text("src/pmkt.egg-info/PKG-INFO\n", encoding="utf-8")

    result = main(["--root", str(tmp_path), "--file-list", str(file_list)])

    assert result == 1


def test_pytest_lane_coverage_passes_current_workflow() -> None:
    result = lane_main(
        [
            "--root",
            str(Path.cwd()),
            ".github/workflows/tests.yml",
            "tests",
        ],
    )

    assert result == 0


def test_workflow_has_required_pytest_lane_gate_job() -> None:
    workflow = Path(".github/workflows/tests.yml").read_text(encoding="utf-8")

    assert "pytest-lanes-gate:" in workflow
    assert "needs: pytest-lanes" in workflow
    assert "needs.pytest-lanes.result" in workflow


def test_pytest_lane_coverage_reports_unassigned_tests(tmp_path: Path) -> None:
    _write(
        tmp_path,
        ".github/workflows/tests.yml",
        "\n".join(
            [
                "jobs:",
                "  pytest-lanes:",
                "    strategy:",
                "      matrix:",
                "        lane:",
                "          - name: sample",
                "            paths: >-",
                "              tests/test_known.py",
            ]
        ),
    )
    _write(tmp_path, "tests/test_known.py")
    _write(tmp_path, "tests/test_missing.py")

    missing, stale = check_pytest_lane_coverage(
        Path(".github/workflows/tests.yml"),
        Path("tests"),
        root=tmp_path,
    )

    assert missing == ["tests/test_missing.py"]
    assert stale == []
