from decimal import Decimal

import httpx
import pytest

from pmkt.exchanges.polymarket.data_api import (
    AsyncPolymarketDataClient,
    normalize_polymarket_open_interest,
)


pytestmark = pytest.mark.asyncio


async def test_open_interest_page_preserves_raw_payload_and_comma_encoding() -> None:
    seen: dict[str, str | None] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["market"] = request.url.params.get("market")
        return httpx.Response(200, json=[{"market": "0xa", "value": 12.5}])

    async with AsyncPolymarketDataClient(
        base_url="https://example.com", transport=httpx.MockTransport(handler)
    ) as client:
        payload = await client.open_interest_page(["0xa", "0xb"])

    assert seen["market"] == "0xa,0xb"
    assert payload == [{"market": "0xa", "value": 12.5}]


async def test_open_interest_normalizer_retains_source_omissions() -> None:
    result = normalize_polymarket_open_interest(
        ["0xa", "0xb"], [{"market": "0xa", "value": "12.50"}]
    )
    assert result.values == {"0xa": Decimal("12.50")}
    assert result.omitted_keys == ("0xb",)
    assert result.coverage_rate == 0.5
    assert result.value_coverage_complete is False


@pytest.mark.parametrize(
    "payload,match",
    [
        ([{"market": "0xa", "value": 1}, {"market": "0xa", "value": 2}], "Duplicate"),
        ([{"market": "0xc", "value": 1}], "Unexpected"),
        ([{"market": "0xa", "value": -1}], "Invalid"),
        ([{"market": "0xa", "value": "nan"}], "Invalid"),
        ([{"market": "0xa", "value": True}], "Invalid"),
    ],
)
async def test_open_interest_normalizer_rejects_invalid_rows(payload, match) -> None:
    with pytest.raises(ValueError, match=match):
        normalize_polymarket_open_interest(["0xa"], payload)


async def test_open_interest_client_enforces_25_key_limit() -> None:
    client = AsyncPolymarketDataClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=[])),
    )
    with pytest.raises(ValueError, match="at most 25"):
        await client.open_interest_page([f"0x{index}" for index in range(26)])
    await client.close()
