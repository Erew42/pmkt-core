from __future__ import annotations

import subprocess
import zipfile
from pathlib import Path

from scripts.build_source_archive import main, resolve_treeish


def _git(root: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=root, check=True, capture_output=True, text=True)


def _git_stdout(root: Path, *args: str, stdin: str | None = None) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        input=stdin,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _write(root: Path, path: str, content: str = "content\n") -> None:
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")


def _commit_fixture_repo(root: Path) -> None:
    _git(root, "init")
    _git(root, "config", "user.email", "test@example.invalid")
    _git(root, "config", "user.name", "Test User")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "fixture")


def test_source_archive_builder_writes_validated_git_archive(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _write(repo, "README.md", "# fixture\n")
    _write(repo, "src/pmkt/example.py", "VALUE = 1\n")
    _write(repo, "generated/local.txt", "ignored\n")
    _git(repo, "init")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Test User")
    _git(repo, "add", "README.md", "src/pmkt/example.py")
    _git(repo, "commit", "-m", "fixture")
    out = tmp_path / "pmkt-src.zip"

    result = main(["--root", str(repo), "--output", str(out)])

    assert result == 0
    with zipfile.ZipFile(out) as archive:
        names = set(archive.namelist())
    assert "README.md" in names
    assert "src/pmkt/example.py" in names
    assert "generated/local.txt" not in names
    assert not any(name.startswith(".git/") for name in names)


def test_source_archive_builder_rejects_forbidden_archived_path(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _write(repo, "src/pmkt/example.py", "VALUE = 1\n")
    _write(repo, "data/run.parquet", "not source\n")
    _commit_fixture_repo(repo)
    out = tmp_path / "pmkt-src.zip"

    result = main(["--root", str(repo), "--output", str(out)])

    assert result == 1
    assert not out.exists()


def test_source_archive_builder_rejects_tracked_symlink_cleanly(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _write(repo, "src/pmkt/example.py", "VALUE = 1\n")
    _git(repo, "init")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Test User")
    _git(repo, "add", "src/pmkt/example.py")
    _git(repo, "commit", "-m", "fixture")
    link_target_blob = _git_stdout(repo, "hash-object", "-w", "--stdin", stdin="src/pmkt/example.py")
    _git(
        repo,
        "update-index",
        "--add",
        "--cacheinfo",
        f"120000,{link_target_blob},linked-source",
    )
    _git(repo, "commit", "-m", "add symlink")
    out = tmp_path / "pmkt-src.zip"

    result = main(["--root", str(repo), "--output", str(out)])

    assert result == 1
    assert not out.exists()


def test_source_archive_builder_uses_resolved_tree_for_final_archive(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _write(repo, "README.md", "first\n")
    _commit_fixture_repo(repo)
    resolved_tree = resolve_treeish(repo, "HEAD")
    _write(repo, "README.md", "second\n")
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-m", "second")
    out = tmp_path / "pmkt-src.zip"

    result = main(["--root", str(repo), "--tree-ish", resolved_tree, "--output", str(out)])

    assert result == 0
    with zipfile.ZipFile(out) as archive:
        assert archive.read("README.md").decode("utf-8").replace("\r\n", "\n") == "first\n"


def test_source_archive_builder_dry_run_writes_file_list_without_archive(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _write(repo, "README.md", "# fixture\n")
    _write(repo, "tests/test_example.py", "def test_example():\n    assert True\n")
    _commit_fixture_repo(repo)
    out = tmp_path / "pmkt-src.zip"
    file_list = tmp_path / "archive-files.txt"

    result = main(
        [
            "--root",
            str(repo),
            "--output",
            str(out),
            "--dry-run",
            "--file-list-out",
            str(file_list),
        ]
    )

    assert result == 0
    assert not out.exists()
    assert file_list.read_text(encoding="utf-8").splitlines() == [
        "README.md",
        "tests/test_example.py",
    ]
