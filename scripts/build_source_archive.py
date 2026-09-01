from __future__ import annotations

import argparse
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.check_repo_hygiene import check_repo_hygiene, format_issues  # noqa: E402


def _run_git(root: Path, args: Sequence[str]) -> None:
    subprocess.run(["git", *args], cwd=root, check=True)


def _git_output(root: Path, args: Sequence[str]) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def resolve_treeish(root: Path, treeish: str) -> str:
    return _git_output(root, ["rev-parse", "--verify", f"{treeish}^{{tree}}"])


def _safe_extract_tar(path: Path, destination: Path) -> None:
    destination = destination.resolve()
    with tarfile.open(path, "r") as archive:
        for member in archive.getmembers():
            target = (destination / member.name).resolve()
            if destination != target and destination not in target.parents:
                raise RuntimeError(f"archive member escapes extraction root: {member.name}")
            if member.issym() or member.islnk():
                raise RuntimeError(f"archive member is a link: {member.name}")
        archive.extractall(destination)


def _extracted_files(root: Path) -> list[str]:
    return sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file()
    )


def _write_file_list(path: Path, files: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{file}\n" for file in files), encoding="utf-8")


def validate_tree_archive(root: Path, treeish: str) -> tuple[bool, list[str]]:
    with tempfile.TemporaryDirectory(prefix="pmkt-source-archive-") as temp_name:
        temp = Path(temp_name)
        probe_archive = temp / "source.tar"
        extracted = temp / "source"
        extracted.mkdir()

        _run_git(root, ["archive", "--format", "tar", "--output", str(probe_archive), treeish])
        try:
            _safe_extract_tar(probe_archive, extracted)
        except RuntimeError as exc:
            print(f"source archive validation failed: {exc}", file=sys.stderr)
            return False, []
        files = _extracted_files(extracted)
        issues = check_repo_hygiene(extracted, files)
        if issues:
            print(format_issues(issues), file=sys.stderr)
            return False, files
        return True, files


def build_source_archive(
    *,
    root: Path,
    output: Path,
    treeish: str = "HEAD",
    archive_format: str = "zip",
    dry_run: bool = False,
    force: bool = False,
    file_list_out: Path | None = None,
) -> int:
    root = root.resolve()
    output = output.resolve()
    try:
        resolved_tree = resolve_treeish(root, treeish)
    except subprocess.CalledProcessError:
        print(f"could not resolve git tree-ish: {treeish}", file=sys.stderr)
        return 1

    ok, files = validate_tree_archive(root, resolved_tree)
    if file_list_out is not None:
        _write_file_list(file_list_out, files)
    if not ok:
        return 1
    if dry_run:
        print(f"Source archive validation passed for {len(files)} files.")
        return 0
    if output.exists() and not force:
        print(
            f"source archive already exists: {output}; pass --force to replace it",
            file=sys.stderr,
        )
        return 1
    output.parent.mkdir(parents=True, exist_ok=True)
    _run_git(
        root,
        [
            "archive",
            "--format",
            archive_format,
            "--output",
            str(output),
            resolved_tree,
        ],
    )
    print(f"Wrote source archive: {output}")
    print(f"Validated {len(files)} archived files with repo hygiene policy.")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build a Git source archive after validating the exact archived "
            "tree with the repository hygiene policy."
        ),
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Repository root. Defaults to the current working directory.",
    )
    parser.add_argument(
        "--tree-ish",
        default="HEAD",
        help="Git tree-ish to archive. Defaults to HEAD.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Archive output path, for example ../pmkt-src.zip.",
    )
    parser.add_argument(
        "--format",
        choices=("zip", "tar"),
        default="zip",
        help="Archive format passed to git archive. Defaults to zip.",
    )
    parser.add_argument(
        "--file-list-out",
        type=Path,
        help="Optional path to write the validated archive file list.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the archive contents without writing the final archive.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replace an existing archive at --output.",
    )
    args = parser.parse_args(argv)
    return build_source_archive(
        root=args.root,
        output=args.output,
        treeish=args.tree_ish,
        archive_format=args.format,
        dry_run=args.dry_run,
        force=args.force,
        file_list_out=args.file_list_out,
    )


if __name__ == "__main__":
    raise SystemExit(main())
