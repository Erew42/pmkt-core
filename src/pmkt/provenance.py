"""Observe implementation identity at the loaded package, independently of cwd.

This module uses only the standard library so diagnostics need no data extras.
Unknown identity is represented by null values; conflicts are never resolved by
choosing a preferred claim. Dependency requirements are not observations.
"""
from __future__ import annotations

import importlib.metadata
import json
import re
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import url2pathname


class ProvenanceError(ValueError):
    """Implementation identity is contradictory or unsuitable for a run."""


@dataclass(frozen=True)
class ImplementationIdentity:
    distribution: str
    package_path: str
    version: str | None = None
    commit: str | None = None
    dirty: bool | None = None
    evidence_source: str = "unknown"
    errors: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    def require_consistent(self) -> ImplementationIdentity:
        if self.errors:
            raise ProvenanceError("; ".join(self.errors))
        return self


def project_fields(root: Path) -> dict[str, str]:
    try:
        contents = (root / "pyproject.toml").read_text(encoding="utf-8")
    except OSError:
        return {}
    section = re.search(r"(?ms)^\[project\]\s*$(.*?)(?=^\[|\Z)", contents)
    if section is None:
        return {}
    return dict(re.findall(r'''(?m)^\s*(name|version)\s*=\s*["']([^"']+)["']''', section[1]))


def _git(root: Path, *args: str) -> str | None:
    try:
        return subprocess.run(
            ["git", "-C", str(root), *args], check=True, capture_output=True,
            text=True, timeout=5,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def source_identity(package_dir: Path, distribution_name: str) -> dict[str, Any] | None:
    root = package_dir.parent.parent
    if package_dir.parent.name != "src" or project_fields(root).get("name") != distribution_name:
        return None
    top = _git(root, "rev-parse", "--show-toplevel")
    if top is None or Path(top).resolve() != root.resolve():
        return None
    status = _git(root, "status", "--porcelain", "--untracked-files=normal")
    return {
        "version": project_fields(root).get("version"),
        "commit": _git(root, "rev-parse", "HEAD"),
        "dirty": bool(status) if status is not None else None,
        "evidence_source": "source-git",
    }


def _applicable_distribution(package_dir: Path, name: str) -> Any:
    try:
        distribution = importlib.metadata.distribution(name)
    except importlib.metadata.PackageNotFoundError:
        return None
    # A distribution elsewhere on sys.path cannot identify the loaded package.
    for entry in distribution.files or ():
        if str(entry).replace("\\", "/").endswith(f"{package_dir.name}/__init__.py"):
            if Path(str(distribution.locate_file(entry))).resolve().parent == package_dir:
                return distribution
    raw = distribution.read_text("direct_url.json")
    try:
        direct = json.loads(raw) if raw else {}
        url = urlparse(direct.get("url", ""))
        if direct.get("dir_info", {}).get("editable") and url.scheme == "file":
            root = Path(url2pathname(url.path)).resolve()
            if package_dir == root / "src" / package_dir.name:
                return distribution
    except (ValueError, TypeError, AttributeError):
        pass
    return None


def implementation_identity(package_file: str | Path, distribution_name: str) -> ImplementationIdentity:
    package_dir = Path(package_file).resolve().parent
    claims: list[dict[str, Any]] = []
    errors: list[str] = []
    source = source_identity(package_dir, distribution_name)
    if source is not None:
        claims.append(source)
    embedded = package_dir / "_build_info.json"
    if embedded.is_file():
        try:
            info = json.loads(embedded.read_text(encoding="utf-8"))
            if info.get("format") != 1 or info.get("distribution") != distribution_name:
                raise ValueError("wrong build identity format or distribution")
            claims.append({**info, "evidence_source": "embedded-build"})
        except (OSError, ValueError, AttributeError) as exc:
            errors.append(f"invalid embedded build identity: {exc}")
    distribution = _applicable_distribution(package_dir, distribution_name)
    if distribution is not None:
        claims.append({"version": distribution.version, "evidence_source": "distribution-metadata"})
        raw = distribution.read_text("direct_url.json")
        if raw:
            try:
                direct = json.loads(raw)
                vcs = direct.get("vcs_info", {})
                if vcs.get("vcs") == "git" and vcs.get("commit_id"):
                    claims.append({"commit": vcs["commit_id"], "evidence_source": "direct-url-git"})
            except (ValueError, AttributeError) as exc:
                errors.append(f"invalid direct URL metadata: {exc}")
    values: dict[str, Any] = {}
    for key in ("commit", "version", "dirty"):
        observations = [claim[key] for claim in claims if claim.get(key) is not None]
        if observations and any(value != observations[0] for value in observations):
            errors.append(f"conflicting {key} identities: {observations!r}")
        values[key] = observations[0] if observations else None
    commit = values["commit"]
    if commit is not None and (not isinstance(commit, str) or re.fullmatch(r"[0-9a-f]{40}", commit) is None):
        errors.append("commit identity must be a full Git SHA")
    if values["dirty"] is not None and not isinstance(values["dirty"], bool):
        errors.append("dirty identity must be boolean or null")
    return ImplementationIdentity(
        distribution=distribution_name, package_path=str(package_dir), **values,
        evidence_source="+".join(claim["evidence_source"] for claim in claims) or "unknown",
        errors=tuple(errors),
    )


def implementation_requirements(package_file: str | Path, distribution_name: str) -> tuple[str, ...]:
    """Return requirements only from metadata bound to the loaded package."""
    distribution = _applicable_distribution(Path(package_file).resolve().parent, distribution_name)
    return tuple(distribution.requires or ()) if distribution is not None else ()
