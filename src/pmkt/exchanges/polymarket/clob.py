from __future__ import annotations

from typing import Any, Literal, Sequence

import httpx
from aiolimiter import AsyncLimiter

from pmkt._http import HttpClient
from pmkt.config import get_config
from pmkt.models import OrderBook, PriceHistory
from pmkt.tokens import extract_token_ids


class AsyncClobClient:
    """CLOB API client (market data endpoints)."""

    def __init__(
        self,
        base_url: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        limiter: AsyncLimiter | None = None,
    ) -> None:
        self.base_url = base_url or get_config().clob_api_url
        self.transport = transport
        self.limiter = limiter or AsyncLimiter(10, 1)
        self._http = HttpClient(
            base_url=self.base_url, transport=self.transport, limiter=self.limiter
        )

    async def close(self) -> None:
        await self._http.close()

    def __enter__(self) -> "AsyncClobClient":
        raise RuntimeError("Use 'async with' for AsyncClobClient.")

    async def __aenter__(self) -> "AsyncClobClient":
        await self._http.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def book(self, token_id: str) -> OrderBook:
        data = await self._http.request_json("GET", "/book", params={"token_id": token_id})
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return OrderBook(**data)

    async def books(self, token_ids: Sequence[str]) -> list[OrderBook]:
        payload = [{"token_id": str(token_id)} for token_id in token_ids]
        data = await self._http.request_json("POST", "/books", json=payload)
        if not isinstance(data, list):
            raise TypeError(f"Expected list, got {type(data)}")
        return [OrderBook(**item) for item in data if isinstance(item, dict)]

    async def clob_market_info(self, condition_id: str) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET",
            f"/clob-markets/{condition_id}",
            params=None,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def price(self, token_id: str, side: Literal["BUY", "SELL"]) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET",
            "/price",
            params={"token_id": token_id, "side": side},
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def midpoint(self, token_id: str) -> dict[str, Any]:
        data = await self._http.request_json("GET", "/midpoint", params={"token_id": token_id})
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def fee_rate(self, token_id: str) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET",
            f"/fee-rate/{token_id}",
            params=None,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        if "base_fee" not in data:
            raise ValueError("Polymarket fee-rate response is missing base_fee")
        return data

    async def prices_history(
        self,
        market: str,
        interval: str | None = None,
        fidelity: int | None = None,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> PriceHistory:
        params: dict[str, Any] = {
            "market": market,
            "fidelity": fidelity,
            "startTs": start_ts,
            "endTs": end_ts,
        }
        if start_ts is None and end_ts is None:
            params["interval"] = interval
        data = await self._http.request_json("GET", "/prices-history", params=params)
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return PriceHistory(**data)

    async def batch_prices_history(
        self,
        markets: Sequence[str],
        interval: str | None = None,
        fidelity: int | None = None,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> dict[str, PriceHistory]:
        market_ids = [str(market).strip() for market in markets if str(market).strip()]
        if not market_ids:
            raise ValueError("markets must contain at least one market id")
        if len(market_ids) > 20:
            raise ValueError("batch-prices-history supports at most 20 markets per request")
        payload: dict[str, Any] = {
            "markets": market_ids,
            "fidelity": fidelity,
            "start_ts": start_ts,
            "end_ts": end_ts,
        }
        if start_ts is None and end_ts is None:
            payload["interval"] = interval
        payload = {key: value for key, value in payload.items() if value is not None}
        data = await self._http.request_json("POST", "/batch-prices-history", json=payload)
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        raw_history = data.get("history", data)
        if not isinstance(raw_history, dict):
            raise TypeError(f"Expected history dict, got {type(raw_history)}")
        histories: dict[str, PriceHistory] = {}
        for market, value in raw_history.items():
            if isinstance(value, dict):
                histories[str(market)] = PriceHistory(**value)
            elif isinstance(value, list):
                histories[str(market)] = PriceHistory(history=value)
        return histories

    async def event_prices_history(
        self,
        event_payload: dict[str, Any],
        interval: str,
        fidelity: int | None = None,
        skip_missing: bool = True,
    ) -> dict[str, PriceHistory]:
        token_ids = extract_token_ids(event_payload)
        if not token_ids:
            raise ValueError("No token ids found in event payload.")
        history: dict[str, PriceHistory] = {}
        for token_id in token_ids:
            params = {"market": token_id, "interval": interval, "fidelity": fidelity}
            response = await self._http._request("GET", "/prices-history", params=params)
            if response.status_code == 404:
                if skip_missing:
                    await response.aclose()
                    continue
                history[token_id] = PriceHistory(history=[])
                await response.aclose()
                continue
            if response.status_code >= 400:
                if skip_missing and "not found" in response.text.lower():
                    await response.aclose()
                    continue
                try:
                    response.raise_for_status()
                finally:
                    await response.aclose()
                continue
            data = response.json()
            await response.aclose()
            if not isinstance(data, dict):
                raise TypeError(f"Expected dict, got {type(data)}")
            history[token_id] = PriceHistory(**data)
        return history

ClobClient = AsyncClobClient
