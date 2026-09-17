"""Resolve an ordered Polymarket batch with offline borrowed clients."""

from __future__ import annotations

import asyncio
import json

import httpx

from pmkt.exchanges.polymarket import AsyncGammaClient
from pmkt.records import PolymarketMarketRef
from pmkt.resolution import PolygonCtfClient, PolymarketResolutionResolver


_CONDITIONS = {"offline-a": "0xabc", "offline-b": "0xdef"}


def _gamma_handler(request: httpx.Request) -> httpx.Response:
    market_id = request.url.path.rsplit("/", 1)[-1]
    return httpx.Response(
        200,
        json={
            "id": market_id,
            "conditionId": _CONDITIONS[market_id],
            "outcomes": ["Yes", "No"],
        },
    )


def _rpc_handler(request: httpx.Request) -> httpx.Response:
    payload = json.loads(request.content)
    method = payload["method"]
    if method == "eth_chainId":
        result = "0x89"
    else:
        data = payload["params"][0]["data"]
        if data.startswith("0xdd34de67") or data.endswith("0" * 64):
            result = "0x1"
        else:
            result = "0x0"
    return httpx.Response(
        200,
        json={"jsonrpc": "2.0", "id": payload["id"], "result": result},
    )


async def main() -> None:
    markets = [
        PolymarketMarketRef("offline-b", condition_id=_CONDITIONS["offline-b"]),
        PolymarketMarketRef("offline-a", condition_id=_CONDITIONS["offline-a"]),
        PolymarketMarketRef("offline-b", condition_id=_CONDITIONS["offline-b"]),
    ]
    async with AsyncGammaClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(_gamma_handler),
    ) as gamma, PolygonCtfClient(
        "https://offline.invalid",
        transport=httpx.MockTransport(_rpc_handler),
    ) as ctf:
        records = await PolymarketResolutionResolver(
            gamma_client=gamma,
            ctf_client=ctf,
        ).resolve_many(markets, concurrency=2, deadline_s=5.0)
    print(
        json.dumps(
            {
                "market_keys": [record.market_key for record in records],
                "resolver_versions": [record.resolver_version for record in records],
                "winners": [record.winner for record in records],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
