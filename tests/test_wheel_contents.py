from __future__ import annotations

import subprocess
import sys
import json
import tarfile
import zipfile
from email.parser import Parser
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

FORBIDDEN_WHEEL_PREFIXES = (
    "pmkt_trading/",
    "pmkt/cross_platform/",
    "pmkt/execution/",
    "pmkt/matching/",
    "pmkt/opportunities/",
    "pmkt/strategies/",
    "pmkt/tracking/",
)

FORBIDDEN_WHEEL_FILES = {
    "pmkt/auth.py",
    "pmkt/polymarket_paper_canary.py",
    "pmkt/data/sports_corpus.py",
    "pmkt/exchanges/kalshi/auth.py",
    "pmkt/exchanges/polymarket/sdk.py",
}


def _build_wheel(output_dir: Path) -> Path:
    subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--wheel",
            "--no-isolation",
            "--outdir",
            str(output_dir),
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    wheels = list(output_dir.glob("*.whl"))
    assert len(wheels) == 1
    return wheels[0]


def test_built_wheel_contains_only_public_package(tmp_path: Path) -> None:
    wheel = _build_wheel(tmp_path)

    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        metadata = Parser().parsestr(archive.read(metadata_name).decode("utf-8"))

    forbidden = sorted(
        name
        for name in names
        if name in FORBIDDEN_WHEEL_FILES or name.startswith(FORBIDDEN_WHEEL_PREFIXES)
    )
    requirements = "\n".join(metadata.get_all("Requires-Dist", []))

    assert forbidden == []
    assert "pmkt/py.typed" in names
    assert "pmkt/text/taxonomy_data/token_aliases.json" in names
    assert metadata["Name"] == "pmkt"
    assert metadata["Version"] == "0.1.1"
    assert "cryptography" not in requirements.lower()
    assert "py-clob-client" not in requirements.lower()
    assert "py-builder-relayer-client" not in requirements.lower()


def test_sdist_rebuild_preserves_observed_identity_outside_git(tmp_path: Path) -> None:
    direct = _build_wheel(tmp_path / "direct")
    subprocess.run(
        [sys.executable, "-m", "build", "--sdist", "--no-isolation", "--outdir", str(tmp_path / "sdist")],
        cwd=ROOT, check=True, capture_output=True, text=True,
    )
    unpacked = tmp_path / "unpacked"
    with tarfile.open(next((tmp_path / "sdist").glob("*.tar.gz"))) as archive:
        archive.extractall(unpacked)
    source = next(unpacked.iterdir())
    # An unrelated enclosing repository must never supply the build commit.
    subprocess.run(["git", "init", str(unpacked)], check=True, capture_output=True)
    subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--no-isolation", "--outdir", str(tmp_path / "rebuilt")],
        cwd=source, check=True, capture_output=True, text=True,
    )
    rebuilt = next((tmp_path / "rebuilt").glob("*.whl"))
    with zipfile.ZipFile(direct) as archive:
        expected = json.loads(archive.read("pmkt/_build_info.json"))
    with zipfile.ZipFile(rebuilt) as archive:
        assert json.loads(archive.read("pmkt/_build_info.json")) == expected
    assert expected["commit"] and isinstance(expected["dirty"], bool)
    assert not (ROOT / "src/pmkt/_build_info.json").exists()
    installed = tmp_path / "installed"
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--no-deps", "--target", str(installed), str(rebuilt)],
        cwd=tmp_path, check=True, capture_output=True, text=True,
    )
    smoke = '''
import json, pathlib, sys
sys.path.insert(0, sys.argv[1])
import pmkt
from pmkt.data.manifests import build_run_manifest, write_manifest, validate_run_manifest
from pmkt.provenance import implementation_identity
identity = implementation_identity(pmkt.__file__, "pmkt").require_consistent()
assert identity.commit == sys.argv[2]
manifest = build_run_manifest(run_id="offline", run_dir=".", started_at_utc="2026-05-26T00:00:00Z", ended_at_utc="2026-05-26T00:01:00Z", status="success", command="offline-smoke", dataset_paths={}, schema_versions={}, row_counts={})
output = write_manifest("offline-manifest.json", manifest)
assert validate_run_manifest(output).ok
assert json.loads(output.read_text())["pmkt_core_commit"] == identity.commit
'''
    subprocess.run(
        [sys.executable, "-I", "-c", smoke, str(installed), expected["commit"]],
        cwd=tmp_path, check=True, capture_output=True, text=True,
    )
