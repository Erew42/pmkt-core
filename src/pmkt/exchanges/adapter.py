from __future__ import annotations

from typing import Any, AsyncIterator, Protocol, runtime_checkable


@runtime_checkable
class VenueAdapter(Protocol):
    name: str

    def iter_markets(self, filters: dict[str, Any] | None = None) -> AsyncIterator[Any]:
        ...

    async def get_order_book(self, instrument_id: str) -> Any:
        ...

    def normalize_market(self, raw: Any) -> dict[str, Any]:
        ...

    def normalize_order_book(self, raw: Any) -> dict[str, Any] | list[dict[str, Any]]:
        ...


__all__ = ["VenueAdapter"]
