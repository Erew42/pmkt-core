from __future__ import annotations

from pathlib import Path

from pmkt.config import KALSHI_ENDPOINTS, PmktConfig, resolve_default_env_files


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
    production = PmktConfig()
    demo = PmktConfig(kalshi_env="demo")

    assert production.resolved_kalshi_api_url == KALSHI_ENDPOINTS["prod"]["api"]
    assert production.resolved_kalshi_ws_url == KALSHI_ENDPOINTS["prod"]["ws"]
    assert demo.resolved_kalshi_api_url == KALSHI_ENDPOINTS["demo"]["api"]
    assert demo.resolved_kalshi_ws_url == KALSHI_ENDPOINTS["demo"]["ws"]


def test_explicit_endpoint_overrides_take_precedence() -> None:
    config = PmktConfig(
        kalshi_api_url="https://read.example/api",
        kalshi_ws_url="wss://read.example/ws",
            )

    assert config.resolved_kalshi_api_url == "https://read.example/api"
    assert config.resolved_kalshi_ws_url == "wss://read.example/ws"


def test_default_env_files_resolve_from_source_root(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    nested = root / "a" / "b"
    nested.mkdir(parents=True)
    (root / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")

    assert resolve_default_env_files(cwd=nested) == (root / ".env", root / ".env.local")




def test_explicit_configuration_never_loads_environment(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("PMKT_GAMMA_API_URL=https://dotenv.test")
    monkeypatch.setenv("PMKT_GAMMA_API_URL", "https://process.test")
    assert PmktConfig().gamma_api_url == "https://gamma-api.polymarket.com"
    assert PmktConfig.from_env().gamma_api_url == "https://process.test"
    assert PmktConfig.from_env(gamma_api_url="https://explicit.test").gamma_api_url == "https://explicit.test"


def test_environment_loader_preserves_dotenv_precedence(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("PMKT_GAMMA_API_URL", raising=False)
    monkeypatch.delenv("PMKT_ENV_FILE", raising=False)
    monkeypatch.delenv("PMKT_ENV_DIR", raising=False)
    (tmp_path / ".env").write_text("PMKT_GAMMA_API_URL=https://first.test")
    (tmp_path / ".env.local").write_text("PMKT_GAMMA_API_URL=https://second.test")
    assert PmktConfig.from_env().gamma_api_url == "https://second.test"
