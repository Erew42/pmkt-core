from __future__ import annotations

import subprocess
import sys
import json
import os
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
    result = subprocess.run(
        [sys.executable, "-I", "-c", smoke, str(installed), expected["commit"]],
        cwd=tmp_path, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr


def test_installed_public_api_has_positive_and_negative_typing_evidence(
    tmp_path: Path,
) -> None:
    wheel = _build_wheel(tmp_path / "wheel")
    installed = tmp_path / "installed"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--target",
            str(installed),
            str(wheel),
        ],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )
    runtime = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "import pathlib, sys; sys.path.insert(0, sys.argv[1]); "
            "import pmkt.catalog; "
            "assert pathlib.Path(pmkt.catalog.__file__).is_relative_to(pathlib.Path(sys.argv[1]))",
            str(installed),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert runtime.returncode == 0, runtime.stderr

    positive = tmp_path / "catalog_positive.py"
    positive.write_text(
        """\
from pathlib import Path
from pmkt.catalog import CatalogQueryResult, CatalogSnapshot
from pmkt.config import PmktConfig, RequestPolicy
from pmkt.exchanges.kalshi import AsyncKalshiClient, KalshiInstrumentRef, KalshiMarketRef
from pmkt.exchanges.polymarket import AsyncClobClient, AsyncGammaClient, PolymarketInstrumentRef, PolymarketMarketRef
from pmkt.records import InstrumentRef, MarketRef

snapshot = CatalogSnapshot.open_latest_history(Path("data/markets"), path_base=Path("."))
result: CatalogQueryResult = snapshot.query(
    "SELECT ? AS n", parameters=(1,), max_result_rows=1, max_result_bytes=1024
)
table = result.to_arrow()
config = PmktConfig.from_values()
policy = RequestPolicy(max_attempts=2)
gamma = AsyncGammaClient(config=config, timeout_s=5.0, request_policy=policy)
clob = AsyncClobClient(config=config, timeout_s=5.0, request_policy=policy)
kalshi = AsyncKalshiClient(config=config, timeout_s=5.0, request_policy=policy)
poly_market = PolymarketMarketRef("market", condition_id="condition")
market: MarketRef = poly_market
instrument: InstrumentRef = PolymarketInstrumentRef("token", market=poly_market, outcome_index=0)
kalshi_instrument: InstrumentRef = KalshiInstrumentRef(KalshiMarketRef("ticker"), "yes")
""",
        encoding="utf-8",
    )
    negative = tmp_path / "catalog_negative.py"
    negative.write_text(
        """\
from pathlib import Path
from pmkt.catalog import CatalogSnapshot
from pmkt.config import PmktConfig
from pmkt.exchanges.kalshi import AsyncKalshiClient, KalshiInstrumentRef, KalshiMarketRef
from pmkt.exchanges.polymarket import AsyncClobClient, AsyncGammaClient, PolymarketMarketRef

snapshot = CatalogSnapshot.open_latest_history(Path("data/markets"), path_base=Path("."))
snapshot.query("SELECT 1", params=())
snapshot.query("SELECT ?", parameters=(object(),))
config = PmktConfig.from_values()
PolymarketMarketRef(ticker="wrong")
KalshiInstrumentRef(KalshiMarketRef("ticker"), "buy")
AsyncGammaClient(config=config, unsupported=True)
AsyncClobClient(config=config, timeout_s="slow")
AsyncKalshiClient(config=config, request_policy="bad")
""",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment["MYPYPATH"] = str(installed)
    command = [
        sys.executable,
        "-m",
        "mypy",
        "--strict",
        "--follow-imports=silent",
        "--no-incremental",
        "--show-error-codes",
    ]
    accepted = subprocess.run(
        [*command, str(positive)],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
    )
    rejected = subprocess.run(
        [*command, str(negative)],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
    )

    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    assert rejected.returncode != 0
    assert "[call-arg]" in rejected.stdout
    assert "[arg-type]" in rejected.stdout
    assert 'Unexpected keyword argument "ticker" for "PolymarketMarketRef"' in rejected.stdout
    assert 'Argument 2 to "KalshiInstrumentRef" has incompatible type' in rejected.stdout
    assert 'Unexpected keyword argument "unsupported" for "AsyncGammaClient"' in rejected.stdout
    assert 'Argument "timeout_s" to "AsyncClobClient" has incompatible type "str"' in rejected.stdout
    assert 'Argument "request_policy" to "AsyncKalshiClient" has incompatible type "str"' in rejected.stdout
