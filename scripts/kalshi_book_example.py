"""Run an offline synthetic Kalshi discovery-to-current-book workflow."""

from __future__ import annotations

import asyncio
import json

import httpx

from pmkt.exchanges.kalshi import (
    AsyncKalshiClient,
    KalshiFilter,
    KalshiInstrumentRef,
)


TICKER = "KXOFFLINE"


def _handler(request: httpx.Request) -> httpx.Response:
    market = {
        "ticker": TICKER,
        "title": "Will the offline example resolve Yes?",
        "market_type": "binary",
        "status": "active",
    }
    if request.url.path.endswith("/orderbook"):
        return httpx.Response(
            200,
            json={
                "orderbook_fp": {
                    "yes_dollars": [["0.40", "12"]],
                    "no_dollars": [["0.35", "5"]],
                }
            },
        )
    if request.url.path.endswith(f"/markets/{TICKER}"):
        return httpx.Response(200, json={"market": market})
    return httpx.Response(200, json={"markets": [market], "cursor": ""})


async def main() -> None:
    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(_handler),
    ) as kalshi:
        discovery = await kalshi.discover_markets(
            filters=KalshiFilter(
                tickers=(TICKER,), status="open", mve_filter="exclude"
            ),
            max_markets=1,
            max_pages=1,
            deadline_s=5.0,
        )
        instrument = discovery.items[0].instruments[0]
        book = await kalshi.get_book(instrument, depth=1, deadline_s=5.0)
    assert isinstance(book.instrument, KalshiInstrumentRef)
    print(
        json.dumps(
            {
                "ticker": book.instrument.market.ticker,
                "side": book.instrument.side,
                "bid": [book.bids[0].price, book.bids[0].quantity],
                "ask": [book.asks[0].price, book.asks[0].quantity],
                "quantity_unit": book.quantity_unit,
                "request_count": len(book.observations),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
