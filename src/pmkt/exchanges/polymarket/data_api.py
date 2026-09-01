from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

import httpx
from aiolimiter import AsyncLimiter

from pmkt._http import HttpClient
from pmkt.config import get_config


MAX_OPEN_INTEREST_MARKETS = 25
DEFAULT_DATA_API_MAX_RATE = 10
DEFAULT_DATA_API_PERIOD_SECONDS = 1


def normalize_condition_ids(condition_ids: Sequence[object]) -> tuple[str, ...]:
    requested: list[str] = []
    seen: set[str] = set()
    for value in condition_ids:
        token = str(value).strip()
        if not token or token in seen:
            continue
        seen.add(token)
        requested.append(token)
    if not requested:
        raise ValueError("At least one Polymarket condition ID is required.")
    if len(requested) > MAX_OPEN_INTEREST_MARKETS:
        raise ValueError(
            f"Polymarket /oi accepts at most {MAX_OPEN_INTEREST_MARKETS} markets per request."
        )
    return tuple(requested)


@dataclass(frozen=True)
class PolymarketOpenInterestBatch:
    requested_keys: tuple[str, ...]
    values: dict[str, Decimal]
    omitted_keys: tuple[str, ...]
    response_keys: tuple[str, ...]

    @property
    def value_coverage_complete(self) -> bool:
        return not self.omitted_keys

    @property
    def coverage_rate(self) -> float:
        return len(self.values) / len(self.requested_keys)


def normalize_polymarket_open_interest(
    requested_condition_ids: Sequence[object],
    payload: object,
) -> PolymarketOpenInterestBatch:
    requested = normalize_condition_ids(requested_condition_ids)
    if not isinstance(payload, list):
        raise TypeError("Polymarket /oi response must be a list.")

    requested_set = set(requested)
    values: dict[str, Decimal] = {}
    response_keys: list[str] = []
    for row in payload:
        if not isinstance(row, Mapping):
            raise TypeError("Polymarket /oi response rows must be objects.")
        market = str(row.get("market") or "").strip()
        if not market:
            raise ValueError("Polymarket /oi response row lacks market.")
        if market not in requested_set:
            raise ValueError(f"Unexpected Polymarket /oi market: {market}")
        if market in values:
            raise ValueError(f"Duplicate Polymarket /oi market: {market}")
        raw_value = row.get("value")
        if isinstance(raw_value, bool):
            raise ValueError(f"Invalid Polymarket /oi value for {market}: {raw_value}")
        try:
            value = Decimal(str(raw_value))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(
                f"Invalid Polymarket /oi value for {market}: {raw_value}"
            ) from exc
        if not value.is_finite() or value < 0:
            raise ValueError(f"Invalid Polymarket /oi value for {market}: {raw_value}")
        values[market] = value
        response_keys.append(market)

    omitted = tuple(key for key in requested if key not in values)
    return PolymarketOpenInterestBatch(
        requested_keys=requested,
        values=values,
        omitted_keys=omitted,
        response_keys=tuple(response_keys),
    )


class AsyncPolymarketDataClient:
    """Public Polymarket Data API client for activity endpoints."""

    def __init__(
        self,
        base_url: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        limiter: AsyncLimiter | None = None,
    ) -> None:
        self.base_url = base_url or get_config().polymarket_data_api_url
        self.limiter = limiter or AsyncLimiter(
            DEFAULT_DATA_API_MAX_RATE,
            DEFAULT_DATA_API_PERIOD_SECONDS,
        )
        self._http = HttpClient(
            base_url=self.base_url,
            transport=transport,
            limiter=self.limiter,
            timeout_s=30.0,
        )

    async def __aenter__(self) -> "AsyncPolymarketDataClient":
        await self._http.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        await self._http.close()

    async def open_interest_page(
        self, condition_ids: Sequence[object]
    ) -> list[dict[str, Any]]:
        requested = normalize_condition_ids(condition_ids)
        payload = await self._http.request_json(
            "GET",
            "/oi",
            params={"market": ",".join(requested)},
        )
        if not isinstance(payload, list) or any(
            not isinstance(row, dict) for row in payload
        ):
            raise TypeError("Polymarket /oi response must be list[dict].")
        return payload


__all__ = [
    "AsyncPolymarketDataClient",
    "DEFAULT_DATA_API_MAX_RATE",
    "DEFAULT_DATA_API_PERIOD_SECONDS",
    "MAX_OPEN_INTEREST_MARKETS",
    "PolymarketOpenInterestBatch",
    "normalize_condition_ids",
    "normalize_polymarket_open_interest",
]
