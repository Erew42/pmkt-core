"""Read-only provider contracts used by resolution and custom data sources."""

from __future__ import annotations

from typing import Any, Protocol

from pmkt.runtime import OperationExpiry


class GammaResolutionProvider(Protocol):
    async def market(
        self, market_id: str | int, *, expiry: OperationExpiry | None = None
    ) -> dict[str, Any]: ...


class ClobResolutionProvider(Protocol):
    async def clob_market_info(
        self, condition_id: str, *, expiry: OperationExpiry | None = None
    ) -> dict[str, Any]: ...


class KalshiResolutionProvider(Protocol):
    async def market(
        self, ticker: str, *, expiry: OperationExpiry | None = None
    ) -> dict[str, Any]: ...

    async def historical_market(
        self, ticker: str, *, expiry: OperationExpiry | None = None
    ) -> dict[str, Any]: ...


class CtfResolutionProvider(Protocol):
    async def ensure_polygon(
        self, *, expiry: OperationExpiry | None = None
    ) -> None: ...

    async def payout_vector(
        self,
        condition_id: str,
        outcome_count: int,
        *,
        expiry: OperationExpiry | None = None,
    ) -> tuple[int, list[int]]: ...
