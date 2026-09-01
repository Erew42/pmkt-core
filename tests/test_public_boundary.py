from __future__ import annotations

import ast
from pathlib import Path

import httpx
import pmkt.config as config_module
import pytest
from typer.testing import CliRunner

from pmkt.cli.app import app
from pmkt.exchanges.kalshi.client import KalshiHttpClient
from pmkt.exchanges.read_auth import ReadOnlyRequestError


ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = ROOT / "src" / "pmkt"

FORBIDDEN_MODULE_PREFIXES = (
    "pmkt_trading",
    "pmkt.auth",
    "pmkt.cross_platform",
    "pmkt.execution",
    "pmkt.matching",
    "pmkt.opportunities",
    "pmkt.polymarket_paper_canary",
    "pmkt.strategies",
    "pmkt.tracking",
    "pmkt.exchanges.kalshi.auth",
    "pmkt.exchanges.polymarket.sdk",
)

PRIVATE_COMMANDS = {
    "alerts",
    "backtest-report",
    "build-match-relations",
    "build-subscription-plan",
    "build-tracking-health",
    "build-tracking-matches",
    "canary-submit",
    "deployment",
    "drift-probe",
    "find-arbitrage-candidates",
    "kill-switch",
    "ledger",
    "live-ladder",
    "match",
    "match-markets",
    "monitor-kalshi-parlays",
    "polymarket-auth",
    "replay-paper-signals",
    "replay-passive-quotes",
    "run-polymarket-paper-canary",
    "runtime-backup",
    "scan-cross-venue-edges",
    "soak",
}


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    return imports


def test_core_source_has_no_private_imports() -> None:
    violations: list[str] = []
    for path in SOURCE_ROOT.rglob("*.py"):
        for imported in _imported_modules(path):
            if imported.startswith(FORBIDDEN_MODULE_PREFIXES):
                violations.append(f"{path.relative_to(ROOT)} -> {imported}")

    assert violations == []


def test_core_config_exposes_no_private_authority_fields() -> None:
    field_names = set(config_module.PmktConfig.model_fields)
    forbidden_fragments = {
        "account",
        "api_key",
        "credential",
        "deployment",
        "funder",
        "passphrase",
        "private_key",
        "proxy",
        "runtime_store",
        "secret",
        "signer",
        "subaccount",
        "user_ws",
    }

    violations = sorted(
        field
        for field in field_names
        if any(fragment in field.lower() for fragment in forbidden_fragments)
    )
    assert violations == []


def test_core_cli_excludes_private_commands() -> None:
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0, result.output
    help_text = result.output.lower()
    assert sorted(command for command in PRIVATE_COMMANDS if command in help_text) == []


def test_core_metadata_excludes_private_dependencies() -> None:
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8").lower()

    assert "cryptography" not in pyproject
    assert "py-clob-client" not in pyproject
    assert "py-builder-relayer-client" not in pyproject
    assert "openai-codex" not in pyproject


@pytest.mark.asyncio
async def test_kalshi_transport_rejects_writes_before_auth_or_network() -> None:
    auth_calls: list[str] = []
    network_calls: list[httpx.Request] = []

    class Provider:
        def headers_for_get(self, path: str) -> dict[str, str]:
            auth_calls.append(path)
            return {"X-Read-Auth": "test-only"}

    def handler(request: httpx.Request) -> httpx.Response:
        network_calls.append(request)
        return httpx.Response(200, json={})

    client = KalshiHttpClient(
        base_url="https://example.test/trade-api/v2",
        auth=Provider(),
        transport=httpx.MockTransport(handler),
    )
    try:
        for method in ("POST", "PUT", "PATCH", "DELETE"):
            with pytest.raises(ReadOnlyRequestError):
                await client.request_json(method, "/portfolio/orders")
    finally:
        await client.close()

    assert auth_calls == []
    assert network_calls == []


def test_core_source_does_not_read_private_credential_environment_variables() -> None:
    forbidden_names = {
        "PMKT_KALSHI_API_KEY_ID",
        "PMKT_KALSHI_PRIVATE_KEY_PATH",
        "PMKT_KALSHI_PRIVATE_KEY_PEM",
        "PMKT_POLYMARKET_API_KEY",
        "PMKT_POLYMARKET_API_SECRET",
        "PMKT_POLYMARKET_API_PASSPHRASE",
        "PMKT_POLYMARKET_PRIVATE_KEY",
        "PMKT_POLYMARKET_PRIVATE_KEY_PATH",
    }
    source = "\n".join(
        path.read_text(encoding="utf-8") for path in SOURCE_ROOT.rglob("*.py")
    )

    assert sorted(name for name in forbidden_names if name in source) == []
