"""Run an offline synthetic Kalshi candle-history workflow."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json

import httpx

from pmkt.exchanges.kalshi import AsyncKalshiClient, KalshiMarketRef


TICKER = "OFFLINE-CANDLE-MARKET"
START = datetime(2026, 1, 2, tzinfo=timezone.utc)


def _handler(request: httpx.Request) -> httpx.Response:
    assert request.url.path == f"/historical/markets/{TICKER}/candlesticks"
    assert request.url.params["period_interval"] == "60"
    assert "include_latest_before_start" not in request.url.params
    return httpx.Response(
        200,
        json={
            "ticker": TICKER,
            "candlesticks": [
                {
                    "end_period_ts": int((START + timedelta(hours=1)).timestamp()),
                    "price": {
                        "open": "0.3500",
                        "high": "0.4200",
                        "low": "0.3400",
                        "close": "0.4000",
                        "mean": "0.3800",
                        "previous": "0.3300",
                    },
                    "yes_bid": {
                        "open": "0.3400",
                        "high": "0.3900",
                        "low": "0.3300",
                        "close": "0.3800",
                    },
                    "yes_ask": {
                        "open": "0.3600",
                        "high": "0.4300",
                        "low": "0.3500",
                        "close": "0.4100",
                    },
                    "volume": "12.50",
                    "open_interest": "25.00",
                }
            ],
        },
    )


async def main() -> None:
    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(_handler),
    ) as kalshi:
        history = await kalshi.get_candles(
            KalshiMarketRef(TICKER),
            start=START,
            end=START + timedelta(hours=2),
            period_minutes=60,
            source="historical",
            max_candles=10,
            deadline_s=5.0,
        )
    candle = history.candles[0]
    print(
        json.dumps(
            {
                "accepted_rows": history.coverage.accepted_rows,
                "close": candle.traded_price.close,
                "dataset": candle.dataset,
                "source_completeness": history.coverage.source_completeness,
                "ticker": history.market.ticker,
                "volume_contracts": candle.volume_contracts,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
