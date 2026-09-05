"""Setuptools hooks: identity belongs to build output, never the source tree."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from setuptools.command.build_py import build_py
from setuptools.command.sdist import sdist


ROOT = Path(__file__).resolve().parent
PACKAGE = "pmkt"


def _git(*args: str) -> str | None:
    try:
        return subprocess.run(
            ["git", "-C", str(ROOT), *args], check=True, capture_output=True,
            text=True, timeout=5,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def build_identity(name: str, version: str) -> dict[str, Any]:
    embedded = ROOT / "src" / PACKAGE / "_build_info.json"
    top = _git("rev-parse", "--show-toplevel")
    if top is not None and Path(top).resolve() == ROOT:
        status = _git("status", "--porcelain", "--untracked-files=normal")
        identity = {
            "format": 1, "distribution": name, "version": version,
            "commit": _git("rev-parse", "HEAD"),
            "dirty": bool(status) if status is not None else None,
        }
        if embedded.exists():
            raise ValueError("source checkout must not contain generated build identity")
        return identity
    if embedded.is_file():
        identity = json.loads(embedded.read_text(encoding="utf-8"))
        if (identity.get("format"), identity.get("distribution"), identity.get("version")) != (1, name, version):
            raise ValueError("sdist build identity conflicts with project metadata")
        return dict(identity)
    return {"format": 1, "distribution": name, "version": version, "commit": None, "dirty": None}


def _write_identity(path: Path, identity: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(identity, indent=2, sort_keys=True) + "\n", encoding="utf-8")


class IdentityBuildPy(build_py):
    def run(self) -> None:
        identity = build_identity(self.distribution.get_name(), self.distribution.get_version())
        super().run()
        _write_identity(Path(self.build_lib) / PACKAGE / "_build_info.json", identity)

    def get_outputs(self, include_bytecode: int = 1) -> list[str]:
        outputs = super().get_outputs(include_bytecode)
        identity_path = str(Path(self.build_lib) / PACKAGE / "_build_info.json")
        return list(dict.fromkeys([*outputs, identity_path]))


class IdentitySdist(sdist):
    def make_release_tree(self, base_dir: str, files: list[str]) -> None:
        identity = build_identity(self.distribution.get_name(), self.distribution.get_version())
        super().make_release_tree(base_dir, files)
        _write_identity(Path(base_dir) / "src" / PACKAGE / "_build_info.json", identity)
