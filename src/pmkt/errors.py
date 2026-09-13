"""Public workflow errors and compatible existing error reexports."""

from __future__ import annotations

from pmkt.data.market_catalog.types import CatalogError as CatalogError
from pmkt.exchanges.read_auth import (
    ReadAuthenticationRequiredError as ReadAuthenticationRequiredError,
)


class OptionalDependencyError(ImportError):
    """A requested workflow needs an optional package extra."""


class ResultLimitExceededError(RuntimeError):
    """A bounded workflow result exceeded a caller-selected limit."""


class OperationTimeoutError(TimeoutError):
    """A whole-operation monotonic expiry elapsed."""


__all__ = [
    "CatalogError",
    "OperationTimeoutError",
    "OptionalDependencyError",
    "ReadAuthenticationRequiredError",
    "ResultLimitExceededError",
]
