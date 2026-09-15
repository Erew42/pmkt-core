from __future__ import annotations

from datetime import datetime, timezone
import math
from typing import Any, Literal, Sequence
from urllib.parse import quote
from uuid import uuid4

import httpx
from pmkt.exchanges._requests import VenueRequests
from aiolimiter import AsyncLimiter

from pmkt.runtime import RequestPolicy
from pmkt._http import HttpClient
from pmkt.runtime import OperationExpiry
from pmkt.config import PmktConfig
from pmkt.errors import MarketNotFoundError
from pmkt.exchanges.polymarket._workflow import (
    clob_book_identities,
    clob_history_identities,
    normalize_clob_book,
    normalize_clob_price_history,
)
from pmkt.models import OrderBook, PriceHistory
from pmkt.records import (
    BookSnapshot,
    PolymarketInstrumentRef,
    PriceHistoryResult,
    RequestObservation,
)
from pmkt.tokens import extract_token_ids


class AsyncClobClient:
    """CLOB API client (market data endpoints)."""

    def __init__(
        self,
        base_url: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        limiter: AsyncLimiter | None = None,
        *,
        config: PmktConfig | None = None,
        timeout_s: float = 10.0,
        request_policy: RequestPolicy | None = None,
    ) -> None:
        self.base_url = (
            base_url
            if base_url is not None
            else config.clob_api_url
            if config is not None
            else PmktConfig().clob_api_url
        )
        self.transport = transport
        self.limiter = limiter or AsyncLimiter(10, 1)
        self._http = HttpClient(
            base_url=self.base_url,
            transport=self.transport,
            limiter=self.limiter,
            timeout_s=timeout_s,
            request_policy=request_policy,
            retryable_post_paths={"/books", "/batch-prices-history"},
        )
        self._requests = VenueRequests(self._http, venue="polymarket", service="clob")

    async def close(self) -> None:
        await self._http.close()

    def __enter__(self) -> "AsyncClobClient":
        raise RuntimeError("Use 'async with' for AsyncClobClient.")

    async def __aenter__(self) -> "AsyncClobClient":
        await self._http.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def book(
        self, token_id: str, *, expiry: OperationExpiry | None = None
    ) -> OrderBook:
        data = await self._http.request_json(
            "GET", "/book", params={"token_id": token_id}, expiry=expiry
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return OrderBook(**data)

    async def get_book(
        self,
        instrument: PolymarketInstrumentRef,
        *,
        depth: int | None = None,
        deadline_s: float = 30.0,
    ) -> BookSnapshot:
        """Fetch one strictly validated, normalized CLOB token book."""

        if not isinstance(instrument, PolymarketInstrumentRef):
            raise TypeError("instrument must be a PolymarketInstrumentRef")
        if depth is not None:
            if isinstance(depth, bool) or not isinstance(depth, int):
                raise TypeError("depth must be an int or None")
            if depth <= 0:
                raise ValueError("depth must be positive")
        expiry = OperationExpiry.bounded(deadline_s)
        observations: list[RequestObservation] = []
        try:
            payload, observation = await self._requests.request_json_observed(
                "GET",
                "/book",
                request_id=f"clob-book-{uuid4().hex}",
                endpoint_template="/book",
                effective_parameters={"token_id": instrument.token_id},
                params={"token_id": instrument.token_id},
                expiry=expiry,
                response_identities=lambda value: clob_book_identities(
                    value, instrument=instrument
                ),
                record_observation=observations.append,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise MarketNotFoundError(
                    venue="polymarket",
                    identifier=instrument.token_id,
                    lookup_scope="CLOB current token book",
                ) from exc
            raise
        expiry.checkpoint()
        result = normalize_clob_book(
            payload,
            instrument=instrument,
            depth=depth,
            observation=observation,
            expiry=expiry,
        )
        expiry.checkpoint()
        return result

    async def books(
        self, token_ids: Sequence[str], *, expiry: OperationExpiry | None = None
    ) -> list[OrderBook]:
        payload = [{"token_id": str(token_id)} for token_id in token_ids]
        data = await self._http.request_json(
            "POST", "/books", json=payload, expiry=expiry
        )
        if not isinstance(data, list):
            raise TypeError(f"Expected list, got {type(data)}")
        return [OrderBook(**item) for item in data if isinstance(item, dict)]

    async def clob_market_info(
        self, condition_id: str, *, expiry: OperationExpiry | None = None
    ) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET", f"/markets/{quote(condition_id, safe='')}", expiry=expiry
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def price(
        self,
        token_id: str,
        side: Literal["BUY", "SELL"],
        *,
        expiry: OperationExpiry | None = None,
    ) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET", "/price", params={"token_id": token_id, "side": side}, expiry=expiry
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def midpoint(
        self, token_id: str, *, expiry: OperationExpiry | None = None
    ) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET", "/midpoint", params={"token_id": token_id}, expiry=expiry
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def fee_rate(
        self, token_id: str, *, expiry: OperationExpiry | None = None
    ) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET", f"/fee-rate/{token_id}", params=None, expiry=expiry
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
        *,
        expiry: OperationExpiry | None = None,
    ) -> PriceHistory:
        data, _observation = await self._prices_history_payload(
            market=market,
            interval=interval,
            fidelity=fidelity,
            start_ts=start_ts,
            end_ts=end_ts,
            expiry=expiry,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return PriceHistory(**data)

    async def get_price_history(
        self,
        instrument: PolymarketInstrumentRef,
        *,
        start: datetime,
        end: datetime,
        sampling_minutes: int,
        max_points: int = 100_000,
        deadline_s: float = 60.0,
        invalid_rows: Literal["raise", "report"] = "raise",
    ) -> PriceHistoryResult:
        """Fetch explicit-window sampled CLOB prices for one token."""

        if not isinstance(instrument, PolymarketInstrumentRef):
            raise TypeError("instrument must be a PolymarketInstrumentRef")
        start_utc, end_utc = _utc_history_bounds(start, end)
        _require_positive_int(sampling_minutes, "sampling_minutes")
        _require_positive_int(max_points, "max_points")
        if invalid_rows not in ("raise", "report"):
            raise ValueError("invalid_rows must be 'raise' or 'report'")
        expiry = OperationExpiry.bounded(deadline_s)
        query_start_ts, query_end_ts = _clob_history_query_bounds(start_utc, end_utc)
        queried_start_utc = datetime.fromtimestamp(query_start_ts, tz=timezone.utc)
        queried_end_utc = datetime.fromtimestamp(query_end_ts, tz=timezone.utc)
        observations: list[RequestObservation] = []
        payload, observation = await self._prices_history_payload(
            market=instrument.token_id,
            interval=None,
            fidelity=sampling_minutes,
            start_ts=query_start_ts,
            end_ts=query_end_ts,
            instrument=instrument,
            expiry=expiry,
            observations=observations,
        )
        assert observation is not None
        expiry.checkpoint()
        result = normalize_clob_price_history(
            payload,
            instrument=instrument,
            requested_start_utc=start_utc,
            requested_end_utc=end_utc,
            queried_start_utc=queried_start_utc,
            queried_end_utc=queried_end_utc,
            sampling_minutes=sampling_minutes,
            max_points=max_points,
            invalid_rows=invalid_rows,
            observation=observation,
            expiry=expiry,
        )
        expiry.checkpoint()
        return result

    async def _prices_history_payload(
        self,
        *,
        market: str,
        interval: str | None,
        fidelity: int | None,
        start_ts: int | None,
        end_ts: int | None,
        instrument: PolymarketInstrumentRef | None = None,
        expiry: OperationExpiry | None = None,
        observations: list[RequestObservation] | None = None,
    ) -> tuple[object, RequestObservation | None]:
        params: dict[str, Any] = {
            "market": market,
            "fidelity": fidelity,
            "startTs": start_ts,
            "endTs": end_ts,
        }
        if start_ts is None and end_ts is None:
            params["interval"] = interval
        if observations is None:
            data = await self._http.request_json(
                "GET", "/prices-history", params=params, expiry=expiry
            )
            return data, None
        if instrument is None:
            raise RuntimeError("observed history fetch requires workflow context")
        data, observation = await self._requests.request_json_observed(
            "GET",
            "/prices-history",
            request_id=f"clob-history-{uuid4().hex}",
            endpoint_template="/prices-history",
            effective_parameters={
                "market": market,
                "fidelity": fidelity,
                "startTs": start_ts,
                "endTs": end_ts,
            },
            params=params,
            expiry=expiry,
            response_identities=lambda value: clob_history_identities(
                value, instrument=instrument
            ),
            record_observation=observations.append,
        )
        return data, observation

    async def batch_prices_history(
        self,
        markets: Sequence[str],
        interval: str | None = None,
        fidelity: int | None = None,
        start_ts: int | None = None,
        end_ts: int | None = None,
        *,
        expiry: OperationExpiry | None = None,
    ) -> dict[str, PriceHistory]:
        market_ids = [str(market).strip() for market in markets if str(market).strip()]
        if not market_ids:
            raise ValueError("markets must contain at least one market id")
        if len(market_ids) > 20:
            raise ValueError(
                "batch-prices-history supports at most 20 markets per request"
            )
        payload: dict[str, Any] = {
            "markets": market_ids,
            "fidelity": fidelity,
            "start_ts": start_ts,
            "end_ts": end_ts,
        }
        if start_ts is None and end_ts is None:
            payload["interval"] = interval
        payload = {key: value for key, value in payload.items() if value is not None}
        data = await self._http.request_json(
            "POST", "/batch-prices-history", json=payload, expiry=expiry
        )
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
        *,
        expiry: OperationExpiry | None = None,
    ) -> dict[str, PriceHistory]:
        token_ids = extract_token_ids(event_payload)
        if not token_ids:
            raise ValueError("No token ids found in event payload.")
        history: dict[str, PriceHistory] = {}
        for token_id in token_ids:
            params = {"market": token_id, "interval": interval, "fidelity": fidelity}
            response = await self._http._request(
                "GET", "/prices-history", params=params, expiry=expiry
            )
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
            try:
                if expiry is not None:
                    expiry.checkpoint()
                data = response.json()
                if expiry is not None:
                    expiry.checkpoint()
            finally:
                await response.aclose()
            if not isinstance(data, dict):
                raise TypeError(f"Expected dict, got {type(data)}")
            history[token_id] = PriceHistory(**data)
        if expiry is not None:
            expiry.checkpoint()
        return history


def _utc_history_bounds(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    normalized: list[datetime] = []
    for name, value in (("start", start), ("end", end)):
        if not isinstance(value, datetime):
            raise TypeError(f"{name} must be a datetime")
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must be timezone-aware")
        normalized.append(value.astimezone(timezone.utc))
    start_utc, end_utc = normalized
    if start_utc >= end_utc:
        raise ValueError("start must precede end after UTC normalization")
    return start_utc, end_utc


def _require_positive_int(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int")
    if value <= 0:
        raise ValueError(f"{name} must be positive")


def _clob_history_query_bounds(
    start_utc: datetime, end_utc: datetime
) -> tuple[int, int]:
    start_floor = math.floor(start_utc.timestamp())
    query_start = start_floor - 1 if start_utc.microsecond == 0 else start_floor
    end_floor = math.floor(end_utc.timestamp())
    query_end = end_floor if end_utc.microsecond == 0 else end_floor + 1
    return query_start, query_end
