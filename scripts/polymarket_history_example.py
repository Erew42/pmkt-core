"""Run an offline synthetic Polymarket sampled-price history workflow."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json

import httpx

from pmkt.exchanges.polymarket import AsyncClobClient, PolymarketInstrumentRef


TOKEN_ID = "offline-token"
START = datetime(2026, 1, 2, tzinfo=timezone.utc)


def _handler(request: httpx.Request) -> httpx.Response:
    assert request.url.path == "/prices-history"
    assert request.url.params["market"] == TOKEN_ID
    assert request.url.params["fidelity"] == "60"
    return httpx.Response(
        200,
        json={
            "market": TOKEN_ID,
            "history": [
                {"t": int(START.timestamp()), "p": 0.35},
                {"t": int((START + timedelta(hours=1)).timestamp()), "p": 0.4},
            ],
        },
    )


async def main() -> None:
    async with AsyncClobClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(_handler),
    ) as clob:
        history = await clob.get_price_history(
            PolymarketInstrumentRef(TOKEN_ID),
            start=START,
            end=START + timedelta(hours=2),
            sampling_minutes=60,
            max_points=10,
            deadline_s=5.0,
        )
    print(
        json.dumps(
            {
                "token_id": history.instrument.token_id,
                "prices": [point.price for point in history.points],
                "price_basis": history.price_basis,
                "raw_rows": history.coverage.raw_rows,
                "accepted_rows": history.coverage.accepted_rows,
                "source_completeness": history.coverage.source_completeness,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
