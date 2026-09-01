import os

import pytest

from pmkt.exchanges.kalshi.client import AsyncKalshiClient


RUN_KALSHI_INTEGRATION = os.getenv("PMKT_RUN_KALSHI_INTEGRATION") == "1"


@pytest.mark.asyncio
@pytest.mark.skipif(
    not RUN_KALSHI_INTEGRATION,
    reason="Set PMKT_RUN_KALSHI_INTEGRATION=1 to run live Kalshi REST smoke tests.",
)
async def test_public_kalshi_rest_discovery_fetches_open_market() -> None:
    async with AsyncKalshiClient() as client:
        page = await client.markets_page(limit=1, status="open")

    assert isinstance(page.get("markets"), list)
    assert len(page["markets"]) >= 1
