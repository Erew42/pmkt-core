from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol


class ReadOnlyRequestError(RuntimeError):
    """Raised when a read-only venue client is asked to perform a write."""


class ReadAuthenticationRequiredError(RuntimeError):
    """Raised when a venue requires injected authentication for a read."""


class ReadAuthHeaderProvider(Protocol):
    """Provide headers for an authenticated read without owning credentials.

    Implementations live outside the public data package.  The public client
    accepts only the resulting header provider and never loads or derives
    credential material itself.
    """

    def headers_for_get(self, path: str) -> Mapping[str, str]: ...


def headers_for_read(
    provider: ReadAuthHeaderProvider,
    method: str,
    path: str,
) -> dict[str, str]:
    normalized_method = method.upper()
    if normalized_method != "GET":
        raise ReadOnlyRequestError(
            f"authenticated venue access is read-only; blocked {normalized_method}"
        )
    return {
        str(name): str(value)
        for name, value in provider.headers_for_get(path).items()
    }


__all__ = [
    "ReadAuthHeaderProvider",
    "ReadAuthenticationRequiredError",
    "ReadOnlyRequestError",
    "headers_for_read",
]
