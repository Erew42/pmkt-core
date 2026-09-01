from __future__ import annotations

from typing import Any, AsyncIterator, Sequence

import httpx
from aiolimiter import AsyncLimiter
from pydantic import TypeAdapter

from pmkt._http import HttpClient, RequestPolicy
from pmkt.models import Event, Market
from pmkt.pagination import normalize_polymarket_cursor, polymarket_cursor_stop_reason


from pmkt.config import get_config


class AsyncGammaClient:
    """Gamma API client (discovery endpoints)."""

    def __init__(
        self,
        base_url: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        limiter: AsyncLimiter | None = None,
        request_policy: RequestPolicy | None = None,
    ) -> None:
        self.base_url = base_url or get_config().gamma_api_url
        self.transport = transport
        # Default to 10 requests per second if not provided
        self.limiter = limiter or AsyncLimiter(10, 1)
        self._http = HttpClient(
            base_url=self.base_url,
            transport=self.transport,
            limiter=self.limiter,
            request_policy=request_policy,
        )

    async def close(self) -> None:
        await self._http.close()

    def __enter__(self) -> "AsyncGammaClient":
        raise RuntimeError("Use 'async with' for AsyncGammaClient.")

    async def __aenter__(self) -> "AsyncGammaClient":
        await self._http.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    def _validate_pagination(self, limit: int, offset: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise TypeError("limit must be an int")
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        if isinstance(offset, bool) or not isinstance(offset, int):
            raise TypeError("offset must be an int")
        if offset < 0:
            raise ValueError("offset must be >= 0")

    @staticmethod
    def _validate_keyset_limit(limit: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise TypeError("limit must be an int")
        if limit < 1 or limit > 100:
            raise ValueError("keyset limit must be between 1 and 100")

    async def market(self, market_id: str | int) -> dict[str, Any]:
        """Fetch one Gamma market by id, preserving raw fields for resolution joins."""
        data = await self._http.request_json(
            "GET", f"/markets/{market_id}", params=None
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def market_with_events(self, market_id: str | int) -> dict[str, Any]:
        """Fetch one market and attach its Gamma event metadata.

        Gamma's single-market endpoint omits ``events``. The filtered list
        endpoint includes it, but requires the correct closed-state filter.
        Resolve the state from the detail response, then require exactly one
        matching list row before returning a combined payload.
        """

        key = str(market_id)
        payload = await self.market(market_id)
        rows = await self._http.request_json(
            "GET",
            "/markets",
            params={"id": key, "closed": bool(payload.get("closed"))},
        )
        if (
            not isinstance(rows, list)
            or len(rows) != 1
            or not isinstance(rows[0], dict)
        ):
            raise ValueError(
                f"expected exactly one filtered Gamma market row for {key}, got {rows!r}"
            )
        returned_key = str(rows[0].get("id") or "")
        if returned_key != key:
            raise ValueError(
                f"filtered Gamma market key mismatch: requested {key}, got {returned_key}"
            )
        result = dict(payload)
        result["events"] = rows[0].get("events")
        return result

    async def markets_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        closed: bool | None = None,
        tag_id: str | None = None,
        related_tags: bool | None = None,
        exclude_tag_id: str | None = None,
    ) -> list[Market]:
        """Fetch one page from /markets using limit/offset pagination."""
        self._validate_pagination(limit, offset)
        data = await self.markets_raw_page(
            limit=limit,
            offset=offset,
            closed=closed,
            tag_id=tag_id,
            related_tags=related_tags,
            exclude_tag_id=exclude_tag_id,
        )
        return TypeAdapter(list[Market]).validate_python(data)

    async def markets_raw_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        closed: bool | None = None,
        tag_id: str | None = None,
        related_tags: bool | None = None,
        exclude_tag_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch one page while preserving decoded venue payload fields."""
        self._validate_pagination(limit, offset)
        data = await self._http.request_json(
            "GET",
            "/markets",
            params={
                "limit": limit,
                "offset": offset,
                "closed": closed,
                "tag_id": tag_id,
                "related_tags": related_tags,
                "exclude_tag_id": exclude_tag_id,
            },
        )
        if not isinstance(data, list) or any(not isinstance(row, dict) for row in data):
            raise TypeError(f"Expected list[dict], got {type(data)}")
        return data

    async def markets_keyset_page(
        self,
        *,
        limit: int = 100,
        after_cursor: str | None = None,
        closed: bool | None = None,
        tag_id: str | None = None,
        related_tags: bool | None = None,
        exclude_tag_id: str | None = None,
        condition_ids: Sequence[str] | None = None,
        order: str | None = None,
        ascending: bool | None = None,
    ) -> dict[str, Any]:
        """Fetch one page from /markets/keyset using cursor pagination."""
        self._validate_keyset_limit(limit)
        data = await self.markets_keyset_raw_page(
            limit=limit,
            after_cursor=after_cursor,
            closed=closed,
            tag_id=tag_id,
            related_tags=related_tags,
            exclude_tag_id=exclude_tag_id,
            condition_ids=condition_ids,
            order=order,
            ascending=ascending,
        )
        return {
            **data,
            "markets": TypeAdapter(list[Market]).validate_python(data["markets"]),
        }

    async def markets_keyset_raw_page(
        self,
        *,
        limit: int = 100,
        after_cursor: str | None = None,
        closed: bool | None = None,
        tag_id: str | None = None,
        related_tags: bool | None = None,
        exclude_tag_id: str | None = None,
        condition_ids: Sequence[str] | None = None,
        order: str | None = None,
        ascending: bool | None = None,
    ) -> dict[str, Any]:
        """Fetch a keyset page while preserving decoded venue payload fields."""
        self._validate_keyset_limit(limit)
        data = await self._http.request_json(
            "GET",
            "/markets/keyset",
            params={
                "limit": limit,
                "after_cursor": after_cursor,
                "closed": closed,
                "tag_id": tag_id,
                "related_tags": related_tags,
                "exclude_tag_id": exclude_tag_id,
                "condition_ids": list(condition_ids) if condition_ids else None,
                "order": order,
                "ascending": ascending,
            },
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        markets = data.get("markets")
        if not isinstance(markets, list) or any(
            not isinstance(row, dict) for row in markets
        ):
            raise TypeError("Expected keyset response field 'markets' to be list[dict]")
        return data

    async def events_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        closed: bool | None = None,
        tag_id: str | None = None,
        related_tags: bool | None = None,
        exclude_tag_id: str | None = None,
        order: str | None = None,
        ascending: bool | None = None,
    ) -> list[Event]:
        """Fetch one page from /events using limit/offset pagination."""
        self._validate_pagination(limit, offset)
        params = {
            "limit": limit,
            "offset": offset,
            "closed": closed,
            "tag_id": tag_id,
            "related_tags": related_tags,
            "exclude_tag_id": exclude_tag_id,
            "order": order,
            "ascending": ascending,
        }
        data = await self._http.request_json("GET", "/events", params=params)
        if not isinstance(data, list):
            raise TypeError(f"Expected list, got {type(data)}")
        return TypeAdapter(list[Event]).validate_python(data)

    async def iter_markets(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        closed: bool | None = None,
        tag_id: str | None = None,
        related_tags: bool | None = None,
        exclude_tag_id: str | None = None,
    ) -> AsyncIterator[Market]:
        """Iterate over /markets until pagination exhausts."""
        current_offset = offset
        while True:
            page = await self.markets_page(
                limit=limit,
                offset=current_offset,
                closed=closed,
                tag_id=tag_id,
                related_tags=related_tags,
                exclude_tag_id=exclude_tag_id,
            )
            if not page:
                break
            for market in page:
                yield market
            if len(page) < limit:
                break
            current_offset += limit

    async def iter_markets_keyset(
        self,
        *,
        limit: int = 100,
        after_cursor: str | None = None,
        closed: bool | None = None,
        tag_id: str | None = None,
        related_tags: bool | None = None,
        exclude_tag_id: str | None = None,
        condition_ids: Sequence[str] | None = None,
        order: str | None = None,
        ascending: bool | None = None,
    ) -> AsyncIterator[Market]:
        """Iterate over /markets/keyset until cursor exhaustion."""
        cursor = after_cursor
        while True:
            page = await self.markets_keyset_page(
                limit=limit,
                after_cursor=cursor,
                closed=closed,
                tag_id=tag_id,
                related_tags=related_tags,
                exclude_tag_id=exclude_tag_id,
                condition_ids=condition_ids,
                order=order,
                ascending=ascending,
            )
            markets = page["markets"]
            if not markets:
                break
            for market in markets:
                yield market
            next_cursor = normalize_polymarket_cursor(page.get("next_cursor"))
            if polymarket_cursor_stop_reason(next_cursor, previous_cursor=cursor):
                break
            cursor = next_cursor

    async def iter_events(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        closed: bool | None = None,
        tag_id: str | None = None,
        related_tags: bool | None = None,
        exclude_tag_id: str | None = None,
        order: str | None = None,
        ascending: bool | None = None,
    ) -> AsyncIterator[Event]:
        """Iterate over /events until pagination exhausts."""
        current_offset = offset
        while True:
            page = await self.events_page(
                limit=limit,
                offset=current_offset,
                closed=closed,
                tag_id=tag_id,
                related_tags=related_tags,
                exclude_tag_id=exclude_tag_id,
                order=order,
                ascending=ascending,
            )
            if not page:
                break
            for event in page:
                yield event
            if len(page) < limit:
                break
            current_offset += limit


GammaClient = AsyncGammaClient
