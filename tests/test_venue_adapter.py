from __future__ import annotations

from typing import Any, AsyncIterator

from pmkt.exchanges.adapter import VenueAdapter


class MinimalAdapter:
    name = "minimal"

    async def _markets(self) -> AsyncIterator[dict[str, str]]:
        yield {"id": "m1"}

    def iter_markets(self, filters: dict[str, Any] | None = None) -> AsyncIterator[Any]:
        return self._markets()

    async def get_order_book(self, instrument_id: str) -> dict[str, str]:
        return {"instrument_id": instrument_id}

    def normalize_market(self, raw: Any) -> dict[str, Any]:
        return dict(raw)

    def normalize_order_book(self, raw: Any) -> dict[str, Any]:
        return dict(raw)


def test_venue_adapter_runtime_protocol_smoke() -> None:
    adapter = MinimalAdapter()

    assert isinstance(adapter, VenueAdapter)
