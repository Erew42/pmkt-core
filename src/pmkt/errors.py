"""Public workflow errors and compatible existing error reexports."""

from __future__ import annotations

from pmkt.data.market_catalog.types import CatalogError as CatalogError


class OptionalDependencyError(ImportError):
    """A requested workflow needs an optional package extra."""


class ResultLimitExceededError(RuntimeError):
    """A bounded workflow result exceeded a caller-selected limit."""


__all__ = [
    "CatalogError",
    "OptionalDependencyError",
    "ResultLimitExceededError",
]
