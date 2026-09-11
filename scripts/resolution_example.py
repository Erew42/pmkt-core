"""Run one offline Polymarket resolution with explicit read-only CTF evidence."""

from __future__ import annotations

import asyncio
import json

import httpx

from pmkt.records import PolymarketMarketRef
from pmkt.resolution import PolygonCtfClient, PolymarketResolutionResolver


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
    market = PolymarketMarketRef("offline-market", condition_id="0xabc")
    async with PolygonCtfClient(
        "https://offline.invalid",
        transport=httpx.MockTransport(_rpc_handler),
    ) as ctf:
        resolver = PolymarketResolutionResolver(ctf_client=ctf)
        record = await resolver.resolve(
            market,
            snapshot={
                "market_id": market.market_id,
                "condition_id": market.condition_id,
                "outcomes": ["Yes", "No"],
            },
            deadline_s=5.0,
        )
    print(
        json.dumps(
            {
                "canonical_source": record.canonical_source,
                "market_key": record.market_key,
                "payouts": [payout.payout for payout in record.payouts],
                "resolver_version": record.resolver_version,
                "winner": record.winner,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
