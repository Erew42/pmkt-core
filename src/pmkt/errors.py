"""Public workflow errors and compatible existing error reexports."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pmkt.exchanges.read_auth import (
    ReadAuthenticationRequiredError as ReadAuthenticationRequiredError,
)

if TYPE_CHECKING:
    from pmkt.data.market_catalog.types import CatalogError as CatalogError


class OptionalDependencyError(ImportError):
    """A requested workflow needs an optional package extra."""


class ResultLimitExceededError(RuntimeError):
    """A bounded workflow result exceeded a caller-selected limit."""


class OperationTimeoutError(TimeoutError):
    """A whole-operation monotonic expiry elapsed."""


class InvalidDataError(ValueError):
    """A remote response violates the workflow's structural contract."""


class MarketNotFoundError(LookupError):
    """A market or instrument was absent from one explicit lookup scope."""

    def __init__(self, *, venue: str, identifier: str, lookup_scope: str) -> None:
        self.venue = venue
        self.identifier = identifier
        self.lookup_scope = lookup_scope
        super().__init__(
            f"{venue} identifier {identifier!r} was not found in {lookup_scope}"
        )


def __getattr__(name: str) -> Any:
    if name == "CatalogError":
        from pmkt.data.market_catalog.types import CatalogError

        return CatalogError
    raise AttributeError(name)


__all__ = [
    "CatalogError",
    "InvalidDataError",
    "MarketNotFoundError",
    "OperationTimeoutError",
    "OptionalDependencyError",
    "ReadAuthenticationRequiredError",
    "ResultLimitExceededError",
]
