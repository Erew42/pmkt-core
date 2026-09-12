from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal, TypeVar, cast

from pydantic_settings import BaseSettings, SettingsConfigDict

from pmkt._http import RequestPolicy


_ConfigT = TypeVar("_ConfigT", bound="PmktConfig")


KALSHI_ENDPOINTS = {
    "prod": {
        "api": "https://external-api.kalshi.com/trade-api/v2",
        "ws": "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
    },
    "demo": {
        "api": "https://external-api.demo.kalshi.co/trade-api/v2",
        "ws": "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2",
    },
}

_DEFAULT_ENV_FILENAMES = (".env", ".env.local")


def resolve_default_env_files(*, cwd: str | Path | None = None) -> tuple[Path, ...]:
    """Resolve optional read-side configuration files for the current checkout."""
    configured_file = _clean_env_path("PMKT_ENV_FILE")
    if configured_file is not None:
        return (configured_file,)

    configured_dir = _clean_env_path("PMKT_ENV_DIR")
    if configured_dir is not None:
        return _env_files_for_dir(configured_dir)

    current = (Path.cwd() if cwd is None else Path(cwd)).expanduser()
    return _env_files_for_dir(_find_source_root(current) or current)


class PmktConfig(BaseSettings):
    """Endpoint configuration for public, read-only market-data clients."""

    model_config = SettingsConfigDict(
        env_file=(".env", ".env.local"),
        env_file_encoding="utf-8",
        env_prefix="PMKT_",
        extra="ignore",
    )

    gamma_api_url: str = "https://gamma-api.polymarket.com"
    clob_api_url: str = "https://clob.polymarket.com"
    polymarket_data_api_url: str = "https://data-api.polymarket.com"
    clob_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    subgraph_api_url: str = (
        "https://api.thegraph.com/subgraphs/name/polymarket/matic-markets-7"
    )
    kalshi_env: Literal["prod", "demo"] = "prod"
    kalshi_api_url: str | None = None
    kalshi_ws_url: str | None = None

    def __init__(self, **values: Any) -> None:
        if "_env_file" not in values:
            values["_env_file"] = resolve_default_env_files()
        super().__init__(**values)

    @classmethod
    def from_values(cls: type[_ConfigT], **values: Any) -> _ConfigT:
        """Build independent settings from arguments and declared defaults only.

        The result is an instance of ``cls`` (including subclasses), so
        subclass fields are validated instead of being silently ignored.
        """

        return _init_only_variant(cls)(**values)

    @classmethod
    def from_env(cls, **values: Any) -> "PmktConfig":
        """Build settings through the legacy OS environment and dotenv sources."""

        return cls(**values)

    @property
    def resolved_kalshi_api_url(self) -> str:
        return self.kalshi_api_url or KALSHI_ENDPOINTS[self.kalshi_env]["api"]

    @property
    def resolved_kalshi_ws_url(self) -> str:
        return self.kalshi_ws_url or KALSHI_ENDPOINTS[self.kalshi_env]["ws"]


_INIT_ONLY_VARIANTS: dict[type[PmktConfig], type[PmktConfig]] = {}


def _init_only_variant(cls: type[_ConfigT]) -> type[_ConfigT]:
    """Return a cached subclass of ``cls`` whose only settings source is init."""

    cached = _INIT_ONLY_VARIANTS.get(cls)
    if cached is not None:
        return cast("type[_ConfigT]", cached)

    def __init__(self: PmktConfig, **values: Any) -> None:
        # Run the full constructor chain of ``cls`` with dotenv discovery
        # disabled; ``settings_customise_sources`` below drops the OS
        # environment and dotenv sources regardless of ``_env_file``.
        values["_env_file"] = None
        cls.__init__(self, **values)

    def settings_customise_sources(
        klass: type[BaseSettings],
        settings_cls: type[BaseSettings],
        init_settings: Any,
        env_settings: Any,
        dotenv_settings: Any,
        file_secret_settings: Any,
    ) -> tuple[Any, ...]:
        del klass, settings_cls, env_settings, dotenv_settings, file_secret_settings
        return (init_settings,)

    def __reduce__(self: PmktConfig) -> tuple[Any, ...]:
        # The variant class is created at runtime and cannot be found by name
        # when unpickling, so rebuild it from the public class instead. The
        # restore path sets pydantic state directly and never reads settings.
        return (_restore_init_only, (cls, self.__getstate__()))

    variant = type(
        f"_InitOnly{cls.__name__}",
        (cls,),
        {
            "__init__": __init__,
            "__module__": cls.__module__,
            "__qualname__": f"_InitOnly{cls.__qualname__}",
            "settings_customise_sources": classmethod(settings_customise_sources),
            "__reduce__": __reduce__,
        },
    )
    _INIT_ONLY_VARIANTS[cls] = variant
    return cast("type[_ConfigT]", variant)


def _restore_init_only(cls: type[_ConfigT], state: dict[Any, Any]) -> _ConfigT:
    """Unpickle an init-only config without consulting any settings source."""

    variant = _init_only_variant(cls)
    instance = variant.__new__(variant)
    instance.__setstate__(state)
    return instance


_config: PmktConfig | None = None


def get_config(*, refresh: bool = False) -> PmktConfig:
    global _config
    if refresh or _config is None:
        _config = PmktConfig()
    return _config


def _clean_env_path(name: str) -> Path | None:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return None
    return Path(value).expanduser()


def _env_files_for_dir(directory: Path) -> tuple[Path, ...]:
    return tuple(directory / filename for filename in _DEFAULT_ENV_FILENAMES)


def _find_source_root(cwd: Path) -> Path | None:
    for candidate in (cwd, *cwd.parents):
        pyproject = candidate / "pyproject.toml"
        if pyproject.is_file():
            return candidate
    return None


__all__ = [
    "KALSHI_ENDPOINTS",
    "PmktConfig",
    "RequestPolicy",
    "get_config",
    "resolve_default_env_files",
]
