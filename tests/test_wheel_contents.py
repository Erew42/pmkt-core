from __future__ import annotations

import subprocess
import sys
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
    assert metadata["Version"] == "0.1.0"
    assert "cryptography" not in requirements.lower()
    assert "py-clob-client" not in requirements.lower()
    assert "py-builder-relayer-client" not in requirements.lower()
