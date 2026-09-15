from __future__ import annotations

from typing import Any, AsyncIterator

import httpx
from pmkt.runtime import OperationExpiry
from aiolimiter import AsyncLimiter

from pmkt.runtime import RequestPolicy
from pmkt._http import HttpClient


from pmkt.config import PmktConfig


class AsyncSubgraphClient:
    """Subgraph API client for historical volume and liquidity endpoints."""

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
            else config.subgraph_api_url
            if config is not None
            else PmktConfig().subgraph_api_url
        )
        self.transport = transport
        # The Graph rate limits can be strict, default to 10 req/s to be safe
        self.limiter = limiter or AsyncLimiter(10, 1)
        self._http = HttpClient(
            base_url=self.base_url,
            transport=self.transport,
            limiter=self.limiter,
            timeout_s=timeout_s,
            request_policy=request_policy,
        )

    async def close(self) -> None:
        await self._http.close()

    def __enter__(self) -> "AsyncSubgraphClient":
        raise RuntimeError("Use 'async with' for AsyncSubgraphClient.")

    async def __aenter__(self) -> "AsyncSubgraphClient":
        await self._http.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def query(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
        *,
        expiry: OperationExpiry | None = None,
    ) -> dict[str, Any]:
        """Execute a raw GraphQL query."""
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        data = await self._http.request_json("POST", "", json=payload, expiry=expiry)

        if not isinstance(data, dict):
            raise TypeError(f"Expected dict from Subgraph, got {type(data)}")

        if "errors" in data:
            raise RuntimeError(f"Subgraph query failed: {data['errors']}")

        return data.get("data", {})

    async def iter_markets(
        self, limit: int = 100, *, expiry: OperationExpiry | None = None
    ) -> AsyncIterator[dict[str, Any]]:
        """Iterate all markets from Subgraph based on creation date."""
        query = """
        query GetMarkets($first: Int!, $skip: Int!) {
          markets(first: $first, skip: $skip, orderBy: creationTimestamp, orderDirection: desc) {
            id
            conditionId
            question
            category
            outcomes
            creationTimestamp
            volume
            volumeUSD
            liquidity
            liquidityUSD
          }
        }
        """
        skip = 0
        while True:
            data = await self.query(
                query, variables={"first": limit, "skip": skip}, expiry=expiry
            )
            markets = data.get("markets", [])
            if not markets:
                break
            for m in markets:
                yield m
            if len(markets) < limit:
                break
            skip += limit

    async def markets_by_condition_ids(
        self, condition_ids: list[str], *, expiry: OperationExpiry | None = None
    ) -> list[dict[str, Any]]:
        """Fetch volume and liquidity metadata for specific condition IDs."""
        query = """
        query GetMarketsByCondition($conditions: [String!]!) {
          markets(first: 1000, where: { conditionId_in: $conditions }) {
            id
            conditionId
            volume
            volumeUSD
            liquidity
            liquidityUSD
          }
        }
        """
        data = await self.query(
            query, variables={"conditions": condition_ids}, expiry=expiry
        )
        return data.get("markets", [])
