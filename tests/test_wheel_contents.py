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
    "pmkt/market_structure/",
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
    assert metadata["Version"] == "0.2.0"
    assert "cryptography" not in requirements.lower()
    assert "py-clob-client" not in requirements.lower()
    assert "py-builder-relayer-client" not in requirements.lower()
    assert "tzdata>=2026.3" in requirements


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
    runtime_script = r'''
import asyncio
from datetime import datetime, timedelta, timezone
import pathlib
import sys

sys.path.insert(0, sys.argv[1])

import httpx
from pmkt.exchanges.polymarket import AsyncClobClient, AsyncGammaClient, PolymarketFilter


def gamma_handler(request):
    assert request.url.path == "/markets/keyset"
    assert request.url.params.get_list("condition_ids") == ["condition"]
    assert request.url.params["closed"] == "false"
    return httpx.Response(
        200,
        json={
            "markets": [{
                "id": "market",
                "conditionId": "condition",
                "question": "Installed workflow?",
                "outcomes": '["Yes", "No"]',
                "clobTokenIds": '["yes-token", "no-token"]',
                "closed": False,
                "enableOrderBook": True,
            }],
            "next_cursor": "",
        },
    )


def clob_handler(request):
    if request.url.path == "/prices-history":
        assert request.url.params["market"] == "yes-token"
        assert request.url.params["fidelity"] == "60"
        return httpx.Response(
            200,
            json={"history": [{"t": 1767312000, "p": 0.45}]},
        )
    assert request.url.path == "/book"
    assert request.url.params["token_id"] == "yes-token"
    return httpx.Response(
        200,
        json={
            "asset_id": "yes-token",
            "market": "condition",
            "bids": [{"price": "0.4", "size": "3"}],
            "asks": [{"price": "0.6", "size": "2"}],
        },
    )


async def main():
    async with AsyncGammaClient(
        base_url="https://gamma.example",
        transport=httpx.MockTransport(gamma_handler),
    ) as gamma:
        result = await gamma.discover_markets(
            filters=PolymarketFilter(condition_ids=("condition",), closed=False),
            max_markets=2,
            max_pages=2,
            deadline_s=5.0,
        )
    market = result.items[0]
    instrument = market.instrument_for_label("Yes")
    async with AsyncClobClient(
        base_url="https://clob.example",
        transport=httpx.MockTransport(clob_handler),
    ) as clob:
        book = await clob.get_book(instrument, depth=1, deadline_s=5.0)
        history = await clob.get_price_history(
            instrument,
            start=datetime(2026, 1, 2, tzinfo=timezone.utc),
            end=datetime(2026, 1, 2, tzinfo=timezone.utc) + timedelta(days=1),
            sampling_minutes=60,
            max_points=10,
            deadline_s=5.0,
        )
    assert book.bids[0].quantity == 3.0
    assert history.points[0].price == 0.45
    assert history.coverage.source_completeness == "unknown"
    assert result.report.stop_reason == "source_exhausted"


asyncio.run(main())
assert pathlib.Path(sys.modules["pmkt"].__file__).is_relative_to(pathlib.Path(sys.argv[1]))
assert "pandas" not in sys.modules
assert "pyarrow" not in sys.modules
assert "duckdb" not in sys.modules
assert "pmkt.data" not in sys.modules
assert "pmkt.streaming" not in sys.modules
import pmkt.catalog
assert pathlib.Path(pmkt.catalog.__file__).is_relative_to(pathlib.Path(sys.argv[1]))
'''
    runtime = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            runtime_script,
            str(installed),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert runtime.returncode == 0, runtime.stderr

    kalshi_runtime_script = r'''
import asyncio
import pathlib
import sys

sys.path.insert(0, sys.argv[1])

import httpx
from pmkt.exchanges.kalshi import AsyncKalshiClient, KalshiFilter


def handler(request):
    market = {
        "ticker": "ticker",
        "title": "Installed Kalshi workflow?",
        "market_type": "binary",
        "status": "active",
    }
    if request.url.path.endswith("/orderbook"):
        assert not request.url.query
        return httpx.Response(200, json={
            "orderbook_fp": {
                "yes_dollars": [["0.4", "3"]],
                "no_dollars": [["0.35", "2"]],
            }
        })
    if request.url.path.endswith("/markets/ticker"):
        return httpx.Response(200, json={"market": market})
    assert request.url.params["tickers"] == "ticker"
    assert request.url.params["status"] == "open"
    assert request.url.params["mve_filter"] == "exclude"
    return httpx.Response(200, json={"markets": [market], "cursor": ""})


async def main():
    async with AsyncKalshiClient(
        base_url="https://kalshi.example",
        transport=httpx.MockTransport(handler),
    ) as kalshi:
        result = await kalshi.discover_markets(
            filters=KalshiFilter(
                tickers=("ticker",), status="open", mve_filter="exclude"
            ),
            max_markets=2,
            max_pages=2,
            deadline_s=5.0,
        )
        book = await kalshi.get_book(
            result.items[0].instruments[0], depth=1, deadline_s=5.0
        )
    assert book.asks[0].quantity == 2.0
    assert len(book.provenance.observations) == 2


asyncio.run(main())
assert pathlib.Path(sys.modules["pmkt"].__file__).is_relative_to(pathlib.Path(sys.argv[1]))
assert "pandas" not in sys.modules
assert "pyarrow" not in sys.modules
assert "duckdb" not in sys.modules
assert "pmkt.streaming" not in sys.modules
'''
    kalshi_runtime = subprocess.run(
        [sys.executable, "-I", "-c", kalshi_runtime_script, str(installed)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert kalshi_runtime.returncode == 0, kalshi_runtime.stderr

    example = tmp_path / "polymarket_history_example.py"
    example.write_bytes((ROOT / "scripts" / example.name).read_bytes())
    example_runtime_script = r'''
import pathlib
import runpy
import sys

sys.path.insert(0, sys.argv[1])
runpy.run_path(sys.argv[2], run_name="__main__")
assert pathlib.Path(sys.modules["pmkt"].__file__).is_relative_to(pathlib.Path(sys.argv[1]))
'''
    example_runtime = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            example_runtime_script,
            str(installed),
            str(example),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert example_runtime.returncode == 0, example_runtime.stderr
    assert json.loads(example_runtime.stdout) == {
        "accepted_rows": 2,
        "price_basis": "venue_defined",
        "prices": [0.35, 0.4],
        "raw_rows": 2,
        "source_completeness": "unknown",
        "token_id": "offline-token",
    }

    kalshi_example = tmp_path / "kalshi_candles_example.py"
    kalshi_example.write_bytes((ROOT / "scripts" / kalshi_example.name).read_bytes())
    kalshi_example_runtime = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            example_runtime_script,
            str(installed),
            str(kalshi_example),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert kalshi_example_runtime.returncode == 0, kalshi_example_runtime.stderr
    assert json.loads(kalshi_example_runtime.stdout) == {
        "accepted_rows": 1,
        "close": 0.4,
        "dataset": "historical",
        "source_completeness": "unknown",
        "ticker": "OFFLINE-CANDLE-MARKET",
        "volume_contracts": 12.5,
    }

    resolution_example = tmp_path / "resolution_example.py"
    resolution_example.write_bytes(
        (ROOT / "scripts" / resolution_example.name).read_bytes()
    )
    resolution_example_runtime = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            example_runtime_script,
            str(installed),
            str(resolution_example),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert resolution_example_runtime.returncode == 0, resolution_example_runtime.stderr
    assert json.loads(resolution_example_runtime.stdout) == {
        "canonical_source": "polygon_ctf",
        "market_key": "offline-market",
        "payouts": ["1", "0"],
        "resolver_version": "market_resolution_resolver.v3",
        "winner": "yes",
    }

    resolution_batch_example = tmp_path / "resolution_batch_example.py"
    resolution_batch_example.write_bytes(
        (ROOT / "scripts" / resolution_batch_example.name).read_bytes()
    )
    resolution_batch_example_runtime = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            example_runtime_script,
            str(installed),
            str(resolution_batch_example),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert resolution_batch_example_runtime.returncode == 0, (
        resolution_batch_example_runtime.stderr
    )
    assert json.loads(resolution_batch_example_runtime.stdout) == {
        "market_keys": ["offline-b", "offline-a", "offline-b"],
        "resolver_versions": [
            "market_resolution_resolver.v3",
            "market_resolution_resolver.v3",
            "market_resolution_resolver.v3",
        ],
        "winners": ["yes", "yes", "yes"],
    }

    positive = tmp_path / "catalog_positive.py"
    positive.write_text(
        """\
from datetime import datetime, timezone
from pathlib import Path
from pmkt.catalog import CatalogQueryResult, CatalogSnapshot
from pmkt.runtime import RequestPolicy
from pmkt.config import PmktConfig
from pmkt.exchanges.kalshi import AsyncKalshiClient, KalshiFilter, KalshiInstrumentRef, KalshiMarket, KalshiMarketRef
from pmkt.exchanges.polymarket import AsyncClobClient, AsyncGammaClient, PolymarketFilter, PolymarketInstrumentRef, PolymarketMarket, PolymarketMarketRef
from pmkt.records import BookSnapshot, CandleHistoryResult, DiscoveryResult, InstrumentRef, MarketRef, PriceHistoryResult
from pmkt.resolution import KalshiResolutionResolver, PolygonCtfClient, PolymarketResolutionResolver, ResolutionRecord

snapshot = CatalogSnapshot.open_latest_history(Path("data/markets"), path_base=Path("."))
result: CatalogQueryResult = snapshot.query(
    "SELECT ? AS n", parameters=(1,), max_result_rows=1, max_result_bytes=1024
)
table = result.to_arrow()
config = PmktConfig()
policy = RequestPolicy(max_attempts=2)
gamma = AsyncGammaClient(config=config, timeout_s=5.0, request_policy=policy)
clob = AsyncClobClient(config=config, timeout_s=5.0, request_policy=policy)
kalshi = AsyncKalshiClient(config=config, timeout_s=5.0, request_policy=policy)
ctf = PolygonCtfClient("https://rpc.example")
poly_resolver = PolymarketResolutionResolver(gamma_client=gamma, clob_client=clob, ctf_client=ctf)
kalshi_resolver = KalshiResolutionResolver(client=kalshi)
poly_market = PolymarketMarketRef("market", condition_id="condition")
poly_instrument = PolymarketInstrumentRef("token", market=poly_market, outcome_index=0)
market: MarketRef = poly_market
instrument: InstrumentRef = poly_instrument
kalshi_instrument: InstrumentRef = KalshiInstrumentRef(KalshiMarketRef("ticker"), "yes")

async def polymarket_workflow() -> None:
    resolution: ResolutionRecord = await poly_resolver.resolve(
        PolymarketMarketRef("market", condition_id="0xabc"),
        snapshot={"market_id": "market", "condition_id": "0xabc"},
        deadline_s=None,
    )
    resolutions: list[ResolutionRecord] = await poly_resolver.resolve_many(
        [PolymarketMarketRef("market", condition_id="0xabc")],
        concurrency=2,
        deadline_s=5.0,
    )
    discovery: DiscoveryResult[PolymarketMarket] = await gamma.discover_markets(
        filters=PolymarketFilter(condition_ids=("condition",), closed=False),
        max_markets=1,
        max_pages=2,
        deadline_s=5.0,
    )
    detail: PolymarketMarket = await gamma.get_market(
        market=PolymarketMarketRef("market"), deadline_s=5.0
    )
    selected = detail.instrument_for_label("Yes")
    book: BookSnapshot = await clob.get_book(selected, depth=5, deadline_s=5.0)
    history: PriceHistoryResult = await clob.get_price_history(
        poly_instrument,
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 2, tzinfo=timezone.utc),
        sampling_minutes=60,
        max_points=100,
        deadline_s=5.0,
        invalid_rows="report",
    )
    history.to_arrow()
    history.to_pandas()

async def kalshi_workflow() -> None:
    resolution: ResolutionRecord = await kalshi_resolver.resolve(
        KalshiMarketRef("ticker"), deadline_s=5.0
    )
    resolutions: list[ResolutionRecord] = await kalshi_resolver.resolve_many(
        [KalshiMarketRef("ticker")], concurrency=2, deadline_s=5.0
    )
    discovery: DiscoveryResult[KalshiMarket] = await kalshi.discover_markets(
        filters=KalshiFilter(
            tickers=("ticker",), status="open", mve_filter="exclude"
        ),
        max_markets=1,
        max_pages=2,
        deadline_s=5.0,
    )
    detail: KalshiMarket = await kalshi.get_market(
        market=KalshiMarketRef("ticker"), source="historical", deadline_s=5.0
    )
    book: BookSnapshot = await kalshi.get_book(
        discovery.items[0].instruments[0], depth=5, deadline_s=5.0
    )
    candles: CandleHistoryResult = await kalshi.get_candles(
        KalshiMarketRef("ticker"),
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 2, tzinfo=timezone.utc),
        period_minutes=60,
        source="historical",
        max_candles=100,
        deadline_s=5.0,
        invalid_rows="report",
    )
    candles.to_arrow()
    candles.to_pandas()
""",
        encoding="utf-8",
    )
    negative = tmp_path / "catalog_negative.py"
    negative.write_text(
        """\
from datetime import datetime, timezone
from pathlib import Path
from pmkt.catalog import CatalogSnapshot
from pmkt.config import PmktConfig
from pmkt.exchanges.kalshi import AsyncKalshiClient, KalshiInstrumentRef, KalshiMarketRef
from pmkt.exchanges.polymarket import AsyncClobClient, AsyncGammaClient, PolymarketInstrumentRef, PolymarketMarketRef
from pmkt.resolution import KalshiResolutionResolver, PolymarketResolutionResolver

snapshot = CatalogSnapshot.open_latest_history(Path("data/markets"), path_base=Path("."))
snapshot.query("SELECT 1", params=())
snapshot.query("SELECT ?", parameters=(object(),))
config = PmktConfig()
PolymarketMarketRef(ticker="wrong")
KalshiInstrumentRef(KalshiMarketRef("ticker"), "buy")
AsyncGammaClient(config=config, unsupported=True)
AsyncClobClient(config=config, timeout_s="slow")
AsyncKalshiClient(config=config, request_policy="bad")

async def invalid_polymarket_calls() -> None:
    gamma = AsyncGammaClient(config=config)
    clob = AsyncClobClient(config=config)
    await gamma.discover_markets(filters="bad")
    await gamma.get_market("market")
    await clob.get_book(PolymarketMarketRef("market"))
    await clob.get_book(PolymarketInstrumentRef("token"), depth="one")
    await clob.get_book(PolymarketInstrumentRef("token"), deadline_s=None)
    await clob.get_price_history(
        KalshiInstrumentRef(KalshiMarketRef("ticker"), "yes"),
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 2, tzinfo=timezone.utc),
        sampling_minutes=60,
    )
    await clob.get_price_history(
        PolymarketInstrumentRef("token"),
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    await clob.get_price_history(
        PolymarketInstrumentRef("token"),
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 2, tzinfo=timezone.utc),
        sampling_minutes=60,
        interval="1d",
    )

async def invalid_kalshi_calls() -> None:
    kalshi = AsyncKalshiClient(config=config)
    await kalshi.discover_markets()
    await kalshi.discover_markets(filters="bad")
    await kalshi.get_market("ticker")
    await kalshi.get_market(market=KalshiMarketRef("ticker"), source="archive")
    await kalshi.get_book(PolymarketInstrumentRef("token"))
    await kalshi.get_book(KalshiInstrumentRef(KalshiMarketRef("ticker"), "yes"), depth="one")
    await kalshi.get_candles(
        PolymarketMarketRef("market"),
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 2, tzinfo=timezone.utc),
        period_minutes=60,
    )
    await kalshi.get_candles(
        KalshiMarketRef("ticker"),
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    await kalshi.get_candles(
        KalshiMarketRef("ticker"),
        start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end=datetime(2026, 1, 2, tzinfo=timezone.utc),
        period_minutes=5,
        source="archive",
        deadline_s=None,
    )

async def invalid_resolution_calls() -> None:
    polymarket = PolymarketResolutionResolver()
    kalshi = KalshiResolutionResolver()
    await polymarket.resolve(KalshiMarketRef("ticker"))
    await kalshi.resolve(PolymarketMarketRef("market"))
    await polymarket.resolve(PolymarketMarketRef("market"), deadline_s="slow")
    await kalshi.resolve(KalshiMarketRef("ticker"), 5.0)
    await polymarket.resolve_many([KalshiMarketRef("ticker")])
    await kalshi.resolve_many([PolymarketMarketRef("market")])
    await polymarket.resolve_many(
        [PolymarketMarketRef("market")], deadline_s=None
    )
    await kalshi.resolve_many(
        [KalshiMarketRef("ticker")], parallelism=2
    )
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
    assert 'Argument "filters" to "discover_markets"' in rejected.stdout
    assert 'Too many positional arguments for "get_market"' in rejected.stdout
    assert 'Argument 1 to "get_book"' in rejected.stdout
    assert 'Argument "depth" to "get_book"' in rejected.stdout
    assert 'Argument "deadline_s" to "get_book"' in rejected.stdout
    assert 'Argument 1 to "get_price_history"' in rejected.stdout
    assert 'Missing named argument "sampling_minutes" for "get_price_history"' in rejected.stdout
    assert 'Unexpected keyword argument "interval" for "get_price_history"' in rejected.stdout
    assert 'Missing named argument "filters" for "discover_markets" of "AsyncKalshiClient"' in rejected.stdout
    assert 'Argument "filters" to "discover_markets" of "AsyncKalshiClient"' in rejected.stdout
    assert 'Too many positional arguments for "get_market" of "AsyncKalshiClient"' in rejected.stdout
    assert 'Argument "source" to "get_market" of "AsyncKalshiClient"' in rejected.stdout
    assert 'Argument 1 to "get_book" of "AsyncKalshiClient"' in rejected.stdout
    assert 'Argument "depth" to "get_book" of "AsyncKalshiClient"' in rejected.stdout
    assert 'Argument 1 to "get_candles" of "AsyncKalshiClient"' in rejected.stdout
    assert 'Missing named argument "period_minutes" for "get_candles"' in rejected.stdout
    assert 'Argument "period_minutes" to "get_candles"' in rejected.stdout
    assert 'Argument "source" to "get_candles"' in rejected.stdout
    assert 'Argument "deadline_s" to "get_candles"' in rejected.stdout
    assert (
        'Argument 1 to "resolve" of "PolymarketResolutionResolver" '
        'has incompatible type "KalshiMarketRef"'
        in rejected.stdout
    )
    assert (
        'Argument 1 to "resolve" of "KalshiResolutionResolver" '
        'has incompatible type "PolymarketMarketRef"'
        in rejected.stdout
    )
    assert (
        'Argument "deadline_s" to "resolve" of "PolymarketResolutionResolver" '
        'has incompatible type "str"'
        in rejected.stdout
    )
    assert (
        'Too many positional arguments for "resolve" of "KalshiResolutionResolver"'
        in rejected.stdout
    )
    assert (
        'List item 0 has incompatible type "KalshiMarketRef"; expected '
        '"PolymarketMarketRef"'
        in rejected.stdout
    )
    assert (
        'List item 0 has incompatible type "PolymarketMarketRef"; expected '
        '"KalshiMarketRef"'
        in rejected.stdout
    )
    assert 'Argument "deadline_s" to "resolve_many"' in rejected.stdout
    assert 'Unexpected keyword argument "parallelism" for "resolve_many"' in rejected.stdout
