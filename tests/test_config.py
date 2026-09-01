from __future__ import annotations

from pathlib import Path

import pmkt.config as config_module
from pmkt.config import KALSHI_ENDPOINTS, PmktConfig, get_config, resolve_default_env_files


def test_config_contains_only_public_endpoint_fields() -> None:
    assert set(PmktConfig.model_fields) == {
        "gamma_api_url",
        "clob_api_url",
        "polymarket_data_api_url",
        "clob_ws_url",
        "subgraph_api_url",
        "kalshi_env",
        "kalshi_api_url",
        "kalshi_ws_url",
    }


def test_kalshi_environment_resolves_public_endpoints() -> None:
    production = PmktConfig(_env_file=None)
    demo = PmktConfig(kalshi_env="demo", _env_file=None)

    assert production.resolved_kalshi_api_url == KALSHI_ENDPOINTS["prod"]["api"]
    assert production.resolved_kalshi_ws_url == KALSHI_ENDPOINTS["prod"]["ws"]
    assert demo.resolved_kalshi_api_url == KALSHI_ENDPOINTS["demo"]["api"]
    assert demo.resolved_kalshi_ws_url == KALSHI_ENDPOINTS["demo"]["ws"]


def test_explicit_endpoint_overrides_take_precedence() -> None:
    config = PmktConfig(
        kalshi_api_url="https://read.example/api",
        kalshi_ws_url="wss://read.example/ws",
        _env_file=None,
    )

    assert config.resolved_kalshi_api_url == "https://read.example/api"
    assert config.resolved_kalshi_ws_url == "wss://read.example/ws"


def test_default_env_files_resolve_from_source_root(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    nested = root / "a" / "b"
    nested.mkdir(parents=True)
    (root / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")

    assert resolve_default_env_files(cwd=nested) == (root / ".env", root / ".env.local")


def test_config_cache_refreshes(monkeypatch) -> None:
    monkeypatch.setattr(config_module, "_config", None)
    first = get_config()
    second = get_config()
    refreshed = get_config(refresh=True)

    assert first is second
    assert refreshed is not first
