"""PEP 517 backend. Stamp observed source identity into maturin's archives."""
from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import subprocess
import sys
import tarfile
import zipfile
from email.parser import BytesParser
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import maturin
from maturin import (
    build_editable as build_editable,
    get_requires_for_build_editable as get_requires_for_build_editable,
    get_requires_for_build_sdist as get_requires_for_build_sdist,
    get_requires_for_build_wheel as get_requires_for_build_wheel,
    prepare_metadata_for_build_editable as prepare_metadata_for_build_editable,
    prepare_metadata_for_build_wheel as prepare_metadata_for_build_wheel,
)


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


def _identity_bytes(metadata: bytes) -> bytes:
    fields = BytesParser().parsebytes(metadata, headersonly=True)
    identity = build_identity(fields["Name"], fields["Version"])
    return (json.dumps(identity, indent=2, sort_keys=True) + "\n").encode()


def stamp_wheel(path: Path) -> None:
    identity_path = f"{PACKAGE}/_build_info.json"
    with NamedTemporaryFile(dir=path.parent, delete=False) as temp:
        staged = Path(temp.name)
    try:
        with zipfile.ZipFile(path) as src, zipfile.ZipFile(staged, "w") as dst:
            record = next(p for p in src.namelist() if p.endswith(".dist-info/RECORD"))
            data = _identity_bytes(src.read(record.removesuffix("RECORD") + "METADATA"))
            for info in src.infolist():
                if info.filename not in {identity_path, record}:
                    dst.writestr(info, src.read(info))
            dst.writestr(identity_path, data, compress_type=zipfile.ZIP_DEFLATED)
            digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
            rows = csv.reader(io.StringIO(src.read(record).decode()))
            updated = io.StringIO(newline="")
            writer = csv.writer(updated, lineterminator="\n")
            writer.writerows(row for row in rows if row[0] not in {identity_path, record})
            writer.writerows([(identity_path, f"sha256={digest}", len(data)), (record, "", "")])
            dst.writestr(record, updated.getvalue(), compress_type=zipfile.ZIP_DEFLATED)
        staged.replace(path)
    finally:
        staged.unlink(missing_ok=True)


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    filename = maturin.build_wheel(wheel_directory, config_settings, metadata_directory)
    stamp_wheel(Path(wheel_directory) / filename)
    return filename


def build_sdist(sdist_directory, config_settings=None):
    filename = maturin.build_sdist(sdist_directory, config_settings)
    path = Path(sdist_directory) / filename
    with NamedTemporaryFile(dir=path.parent, delete=False) as temp:
        staged = Path(temp.name)
    try:
        with tarfile.open(path, "r:gz") as src, tarfile.open(staged, "w:gz") as dst:
            root = filename.removesuffix(".tar.gz")
            metadata = src.extractfile(f"{root}/PKG-INFO")
            if metadata is None:
                raise ValueError("missing sdist metadata")
            data = _identity_bytes(metadata.read())
            identity = tarfile.TarInfo(f"{root}/src/{PACKAGE}/_build_info.json")
            identity.size = len(data)
            for member in src:
                if member.name != identity.name:
                    dst.addfile(member, src.extractfile(member) if member.isfile() else None)
            dst.addfile(identity, io.BytesIO(data))
        staged.replace(path)
    finally:
        staged.unlink(missing_ok=True)
    return filename


if __name__ == "__main__":
    # maturin-action builds platform wheels directly, outside PEP 517.
    stamp_wheel(Path(sys.argv[1]))
