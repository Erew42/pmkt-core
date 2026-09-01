from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, AsyncIterator, Iterable
from urllib.parse import urlparse

import httpx
from aiolimiter import AsyncLimiter

from pmkt._http import HttpClient, RequestPolicy
from pmkt.config import get_config
from pmkt.data.canonical import KALSHI_MARKET_SNAPSHOT_COLUMNS
from pmkt.data.normalize_kalshi import normalize_kalshi_market
from pmkt.data.prices import complement_probability as _price_complement
from pmkt.data.types import parse_float as _parse_float
from pmkt.exchanges.read_auth import (
    ReadAuthHeaderProvider,
    ReadOnlyRequestError,
    headers_for_read,
)

if TYPE_CHECKING:
    import pandas as pd


def _normalize_tickers(tickers: str | Iterable[str] | None) -> str | None:
    if tickers is None:
        return None
    if isinstance(tickers, str):
        return tickers
    cleaned = [str(ticker).strip() for ticker in tickers if str(ticker).strip()]
    return ",".join(dict.fromkeys(cleaned)) or None


def _signed_path(base_url: str, endpoint_path: str) -> str:
    base_path = urlparse(base_url).path.rstrip("/")
    endpoint = "/" + endpoint_path.lstrip("/")
    if base_path and (endpoint == base_path or endpoint.startswith(base_path + "/")):
        return endpoint
    return f"{base_path}{endpoint}" if base_path else endpoint


def _best_bid(levels: dict[float, float]) -> float | None:
    return max(levels) if levels else None


def _levels_to_map(levels: Any) -> dict[float, float]:
    parsed: dict[float, float] = {}
    if not isinstance(levels, list):
        return parsed
    for level in levels:
        if isinstance(level, dict):
            price = _parse_float(level.get("price") or level.get("price_dollars"))
            size = _parse_float(level.get("size") or level.get("count") or level.get("count_fp"))
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price = _parse_float(level[0])
            size = _parse_float(level[1])
        else:
            continue
        if price is None or size is None or size <= 0:
            continue
        parsed[price] = size
    return parsed


def normalize_kalshi_orderbook(
    payload: dict[str, Any],
    *,
    market_ticker: str | None = None,
    market_id: str | None = None,
) -> dict[str, Any]:
    """Normalize Kalshi bid-only YES/NO ladders into bid/ask probability fields."""
    raw_book = payload.get("orderbook_fp")
    book = raw_book if isinstance(raw_book, dict) else payload
    yes_levels = _levels_to_map(
        book.get("yes_dollars_fp")
        or book.get("yes_dollars")
        or book.get("yes")
        or []
    )
    no_levels = _levels_to_map(
        book.get("no_dollars_fp")
        or book.get("no_dollars")
        or book.get("no")
        or []
    )
    yes_bid = _best_bid(yes_levels)
    no_bid = _best_bid(no_levels)
    yes_ask = _price_complement(no_bid)
    no_ask = _price_complement(yes_bid)
    yes_bid_source = "direct" if yes_bid is not None else "missing"
    no_bid_source = "direct" if no_bid is not None else "missing"
    yes_ask_source = "complement_derived" if no_bid is not None else "missing"
    no_ask_source = "complement_derived" if yes_bid is not None else "missing"
    yes_bid_size = yes_levels.get(yes_bid) if yes_bid is not None else None
    no_bid_size = no_levels.get(no_bid) if no_bid is not None else None
    mid = (yes_bid + yes_ask) / 2.0 if yes_bid is not None and yes_ask is not None else None
    spread = yes_ask - yes_bid if yes_bid is not None and yes_ask is not None else None
    return {
        "exchange": "kalshi",
        "market_ticker": market_ticker,
        "market_id": market_id,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "yes_bid_source": yes_bid_source,
        "yes_ask_source": yes_ask_source,
        "no_bid_source": no_bid_source,
        "no_ask_source": no_ask_source,
        "yes_bid_size": yes_bid_size,
        "yes_ask_size": no_bid_size,
        "no_bid_size": no_bid_size,
        "no_ask_size": yes_bid_size,
        "mid": mid,
        "spread": spread,
        "depth": len(yes_levels) + len(no_levels),
        "yes_bid_depth": len(yes_levels),
        "no_bid_depth": len(no_levels),
        "yes_levels": sorted(
            ({"price": price, "size": size} for price, size in yes_levels.items()),
            key=lambda item: item["price"],
            reverse=True,
        ),
        "no_levels": sorted(
            ({"price": price, "size": size} for price, size in no_levels.items()),
            key=lambda item: item["price"],
            reverse=True,
        ),
    }


def kalshi_markets_dataframe(markets: list[dict[str, Any]]) -> pd.DataFrame:
    import pandas as pd

    rows = [normalize_kalshi_market(market) for market in markets if isinstance(market, dict)]
    df = pd.DataFrame(rows, columns=KALSHI_MARKET_SNAPSHOT_COLUMNS)
    if not df.empty and "market_key" in df.columns:
        df = df.sort_values("market_key").reset_index(drop=True)
    return df


def normalize_kalshi_event(event: dict[str, Any]) -> dict[str, Any]:
    ticker = event.get("event_ticker") or event.get("ticker")
    title = event.get("title") or event.get("name") or ticker
    status = event.get("status")
    category = (
        event.get("category")
        or event.get("series_category")
        or event.get("event_category")
    )
    return {
        "exchange": "kalshi",
        "event_ticker": str(ticker) if ticker is not None else None,
        "ticker": ticker,
        "title": title,
        "subtitle": event.get("subtitle") or event.get("sub_title"),
        "category": category,
        "series_ticker": event.get("series_ticker"),
        "status": status,
        "closed": status in {"closed", "settled"},
        "close_time": event.get("close_time") or event.get("expiration_time"),
        "open_time": event.get("open_time"),
        "updated_time": event.get("updated_time"),
        "raw_json": json.dumps(event, ensure_ascii=True, sort_keys=True, default=str),
    }


def kalshi_events_dataframe(events: list[dict[str, Any]]) -> pd.DataFrame:
    import pandas as pd

    rows = [normalize_kalshi_event(event) for event in events if isinstance(event, dict)]
    df = pd.DataFrame(rows)
    if not df.empty and "event_ticker" in df.columns:
        df = df.sort_values("event_ticker").reset_index(drop=True)
    return df


class KalshiHttpClient(HttpClient):
    def __init__(
        self,
        *,
        base_url: str,
        auth: ReadAuthHeaderProvider | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_s: float = 10.0,
        max_retries: int = 3,
        limiter: AsyncLimiter | None = None,
        request_policy: RequestPolicy | None = None,
    ) -> None:
        super().__init__(
            base_url=base_url,
            transport=transport,
            timeout_s=timeout_s,
            max_retries=max_retries,
            limiter=limiter,
            request_policy=request_policy,
        )
        self.header_provider = auth

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        if method.upper() != "GET":
            raise ReadOnlyRequestError(
                f"Kalshi market-data client is read-only; blocked {method.upper()}"
            )
        if self.header_provider is None:
            return await super()._request(
                method,
                path,
                params=params,
                json=json,
                headers=headers,
            )

        auth_headers = headers_for_read(
            self.header_provider,
            method,
            _signed_path(self.base_url, path),
        )
        request_headers = {**(headers or {}), **auth_headers}
        return await super()._request(
            method,
            path,
            params=params,
            json=json,
            headers=request_headers,
        )


class AsyncKalshiClient:
    """Kalshi REST market-data client."""

    def __init__(
        self,
        base_url: str | None = None,
        *,
        auth: ReadAuthHeaderProvider | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        limiter: AsyncLimiter | None = None,
        request_policy: RequestPolicy | None = None,
    ) -> None:
        self.base_url = base_url or get_config().resolved_kalshi_api_url
        self.header_provider = auth
        self.transport = transport
        self.limiter = limiter or AsyncLimiter(10, 1)
        self._http = KalshiHttpClient(
            base_url=self.base_url,
            auth=self.header_provider,
            transport=self.transport,
            limiter=self.limiter,
            request_policy=request_policy,
        )

    async def close(self) -> None:
        await self._http.close()

    def __enter__(self) -> "AsyncKalshiClient":
        raise RuntimeError("Use 'async with' for AsyncKalshiClient.")

    async def __aenter__(self) -> "AsyncKalshiClient":
        await self._http.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    @staticmethod
    def _validate_limit(limit: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise TypeError("limit must be an int")
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")

    async def markets_page(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        status: str | None = "open",
        event_ticker: str | None = None,
        series_ticker: str | None = None,
        tickers: str | Iterable[str] | None = None,
        mve_filter: str | None = None,
        min_close_ts: int | None = None,
        max_close_ts: int | None = None,
        min_created_ts: int | None = None,
        max_created_ts: int | None = None,
        min_updated_ts: int | None = None,
        max_updated_ts: int | None = None,
        min_settled_ts: int | None = None,
        max_settled_ts: int | None = None,
    ) -> dict[str, Any]:
        self._validate_limit(limit)
        params = {
            "limit": limit,
            "cursor": cursor,
            "status": status,
            "event_ticker": event_ticker,
            "series_ticker": series_ticker,
            "tickers": _normalize_tickers(tickers),
            "mve_filter": mve_filter,
            "min_close_ts": min_close_ts,
            "max_close_ts": max_close_ts,
            "min_created_ts": min_created_ts,
            "max_created_ts": max_created_ts,
            "min_updated_ts": min_updated_ts,
            "max_updated_ts": max_updated_ts,
            "min_settled_ts": min_settled_ts,
            "max_settled_ts": max_settled_ts,
        }
        data = await self._http.request_json("GET", "/markets", params=params)
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def market(self, ticker: str) -> dict[str, Any]:
        data = await self._http.request_json("GET", f"/markets/{ticker}", params=None)
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        market = data.get("market")
        if isinstance(market, dict):
            return market
        return data

    async def historical_market(self, ticker: str) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET",
            f"/historical/markets/{ticker}",
            params=None,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        market = data.get("market")
        if isinstance(market, dict):
            return market
        return data

    async def iter_markets(
        self,
        *,
        limit: int = 100,
        status: str | None = "open",
        max_pages: int | None = None,
        **params: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        cursor = params.pop("cursor", None)
        seen_cursors = {cursor} if cursor else set()
        pages = 0
        while True:
            if max_pages is not None and pages >= max_pages:
                break
            page = await self.markets_page(
                limit=limit,
                cursor=cursor,
                status=status,
                **params,
            )
            pages += 1
            markets = page.get("markets")
            if not isinstance(markets, list):
                raise TypeError("Expected response field 'markets' to be a list")
            for market in markets:
                if isinstance(market, dict):
                    yield market
            next_cursor = str(page.get("cursor") or "").strip() or None
            if next_cursor is None:
                break
            if next_cursor in seen_cursors:
                raise RuntimeError("Kalshi markets cursor repeated")
            seen_cursors.add(next_cursor)
            cursor = next_cursor

    async def events_page(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        status: str | None = None,
        series_ticker: str | None = None,
        with_nested_markets: bool | None = None,
    ) -> dict[str, Any]:
        self._validate_limit(limit)
        data = await self._http.request_json(
            "GET",
            "/events",
            params={
                "limit": limit,
                "cursor": cursor,
                "status": status,
                "series_ticker": series_ticker,
                "with_nested_markets": with_nested_markets,
            },
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def iter_events(
        self,
        *,
        limit: int = 100,
        status: str | None = None,
        max_pages: int | None = None,
        **params: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        cursor = params.pop("cursor", None)
        pages = 0
        while True:
            if max_pages is not None and pages >= max_pages:
                break
            page = await self.events_page(
                limit=limit,
                cursor=cursor,
                status=status,
                **params,
            )
            pages += 1
            events = page.get("events")
            if not isinstance(events, list) or not events:
                break
            for event in events:
                if isinstance(event, dict):
                    yield event
            cursor = page.get("cursor") or None
            if not cursor:
                break

    async def orderbook(self, ticker: str, *, depth: int | None = None) -> dict[str, Any]:
        params = {"depth": depth}
        data = await self._http.request_json(
            "GET",
            f"/markets/{ticker}/orderbook",
            params=params,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def normalized_orderbook(
        self,
        ticker: str,
        *,
        depth: int | None = None,
    ) -> dict[str, Any]:
        data = await self.orderbook(ticker, depth=depth)
        return normalize_kalshi_orderbook(data, market_ticker=ticker)

    async def market_candlesticks(
        self,
        series_ticker: str,
        ticker: str,
        *,
        start_ts: int,
        end_ts: int,
        period_interval: int,
        include_latest_before_start: bool | None = None,
    ) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET",
            f"/series/{series_ticker}/markets/{ticker}/candlesticks",
            params={
                "start_ts": start_ts,
                "end_ts": end_ts,
                "period_interval": period_interval,
                "include_latest_before_start": include_latest_before_start,
            },
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def batch_market_candlesticks(
        self,
        market_tickers: str | Iterable[str],
        *,
        start_ts: int,
        end_ts: int,
        period_interval: int,
        include_latest_before_start: bool | None = None,
    ) -> dict[str, Any]:
        tickers = _normalize_tickers(market_tickers)
        if not tickers:
            raise ValueError("market_tickers must contain at least one ticker")
        data = await self._http.request_json(
            "GET",
            "/markets/candlesticks",
            params={
                "market_tickers": tickers,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "period_interval": period_interval,
                "include_latest_before_start": include_latest_before_start,
            },
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def historical_market_candlesticks(
        self,
        ticker: str,
        *,
        start_ts: int,
        end_ts: int,
        period_interval: int,
    ) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET",
            f"/historical/markets/{ticker}/candlesticks",
            params={
                "start_ts": start_ts,
                "end_ts": end_ts,
                "period_interval": period_interval,
            },
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def historical_cutoff(self) -> dict[str, Any]:
        data = await self._http.request_json("GET", "/historical/cutoff")
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def series(
        self,
        series_ticker: str,
        *,
        include_volume: bool | None = None,
    ) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET",
            f"/series/{series_ticker}",
            params={"include_volume": include_volume},
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def series_list(
        self,
        *,
        category: str | None = None,
        tags: str | None = None,
        include_product_metadata: bool | None = None,
        include_volume: bool | None = None,
        min_updated_ts: int | None = None,
    ) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET",
            "/series",
            params={
                "category": category,
                "tags": tags,
                "include_product_metadata": include_product_metadata,
                "include_volume": include_volume,
                "min_updated_ts": min_updated_ts,
            },
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def trades(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        ticker: str | None = None,
        min_ts: int | None = None,
        max_ts: int | None = None,
    ) -> dict[str, Any]:
        self._validate_limit(limit)
        data = await self._http.request_json(
            "GET",
            "/markets/trades",
            params={
                "limit": limit,
                "cursor": cursor,
                "ticker": ticker,
                "min_ts": min_ts,
                "max_ts": max_ts,
            },
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def historical_trades(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        ticker: str | None = None,
        min_ts: int | None = None,
        max_ts: int | None = None,
        is_block_trade: bool | None = None,
    ) -> dict[str, Any]:
        self._validate_limit(limit)
        data = await self._http.request_json(
            "GET",
            "/historical/trades",
            params={
                "limit": limit,
                "cursor": cursor,
                "ticker": ticker,
                "min_ts": min_ts,
                "max_ts": max_ts,
                "is_block_trade": is_block_trade,
            },
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data


KalshiClient = AsyncKalshiClient


__all__ = [
    "AsyncKalshiClient",
    "KalshiClient",
    "kalshi_events_dataframe",
    "kalshi_markets_dataframe",
    "normalize_kalshi_event",
    "normalize_kalshi_market",
    "normalize_kalshi_orderbook",
]
