from decimal import Decimal

import httpx
import pytest

from pmkt.exchanges.polymarket.data_api import (
    AsyncPolymarketDataClient,
    normalize_polymarket_open_interest,
)


pytestmark = pytest.mark.asyncio

CONDITION = "0x" + "a" * 64
WALLET_A = "0x" + "1" * 40
WALLET_B = "0x" + "2" * 40


def _position(wallet: str, *, status: str, token: str = "123") -> dict:
    return {
        "proxy_wallet": wallet,
        "condition_id": CONDITION,
        "token_id": token,
        "status": status,
        "current_size": 4 if status == "OPEN" else 0,
        "total_size": 9,
        "outcome": "Yes",
        "avg_price": 0.25,
        "current_value": 2,
        "realized_pnl": -1.5,
        "unrealized_pnl": 1,
    }


def _trade(wallet: str) -> dict:
    return {
        "proxy_wallet": wallet,
        "condition_id": CONDITION,
        "token_id": "123",
        "side": "BUY",
        "size": 1.25,
        "price": 0.123456789012345678,
        "timestamp": 1782752879,
        "transaction_hash": "0x" + "f" * 64,
        "outcome": "Yes",
    }


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


async def test_market_participants_collects_current_and_exited_market_positions() -> None:
    seen: list[tuple[str, str | None, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        seen.append((params["status"], params.get("cursor"), params.get("condition")))
        assert request.url.path == "/v2/positions"
        assert params["filter_amount"] == "0"
        if params["status"] == "OPEN":
            assert params["include_archived"] == "true"
        else:
            assert "include_archived" not in params
        assert params["filter_type"] == "TOKENS"
        if params["status"] == "OPEN" and params.get("cursor") is None:
            return httpx.Response(200, json={
                "data": [_position(WALLET_A, status="OPEN")],
                "pagination": {"has_more": True, "next_cursor": "next-open"},
            })
        if params["status"] == "OPEN":
            return httpx.Response(200, json={
                "data": [_position(WALLET_B, status="OPEN")],
                "pagination": {"has_more": False, "next_cursor": None},
            })
        return httpx.Response(200, json={
            "data": [_position(WALLET_A, status="CLOSED", token="456")],
            "pagination": {"has_more": False, "next_cursor": None},
        })

    async with AsyncPolymarketDataClient(
        base_url="https://data.example", transport=httpx.MockTransport(handler)
    ) as client:
        result = await client.market_participants(CONDITION, page_size=1)

    assert result.complete
    assert (result.current_pages, result.past_pages) == (2, 1)
    assert seen == [
        ("OPEN", None, CONDITION), ("OPEN", "next-open", CONDITION),
        ("CLOSED", None, CONDITION),
    ]
    assert [participant.wallet for participant in result.participants] == [WALLET_A, WALLET_B]
    assert len(result.participants[0].current_positions) == 1
    assert len(result.participants[0].past_positions) == 1
    assert result.participants[1].past_positions == ()


async def test_wallet_history_reads_full_trades_and_both_position_statuses() -> None:
    seen: list[tuple[str, str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        seen.append((request.url.path, params.get("status", ""), params.get("cursor")))
        assert params["user"] == WALLET_A
        if request.url.path == "/v2/trades":
            assert params["full_history"] == "true"
            assert params["taker_only"] == "false"
            return httpx.Response(200, content=(
                '{"data":[{"proxy_wallet":"' + WALLET_A + '","condition_id":"'
                + CONDITION + '","token_id":"123","side":"BUY","size":1.25,'
                '"price":0.123456789012345678,"timestamp":1782752879,'
                '"transaction_hash":"0x' + 'f' * 64 + '"}],'
                '"pagination":{"has_more":false,"next_cursor":null}}'
            ))
        status = params["status"]
        return httpx.Response(200, json={
            "data": [_position(WALLET_A, status=status)],
            "pagination": {"has_more": False, "next_cursor": None},
        })

    async with AsyncPolymarketDataClient(
        base_url="https://data.example", transport=httpx.MockTransport(handler)
    ) as client:
        result = await client.wallet_history(WALLET_A)

    assert result.complete
    assert len(result.trades) == len(result.current_positions) == len(result.past_positions) == 1
    assert result.trades[0].price == Decimal("0.123456789012345678")
    assert result.current_positions[0].current_size == Decimal("4")
    assert result.past_positions[0].realized_pnl == Decimal("-1.5")
    assert seen == [
        ("/v2/trades", "", None),
        ("/v2/positions", "OPEN", None),
        ("/v2/positions", "CLOSED", None),
    ]


async def test_market_participants_reports_page_cap_and_cursor() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["status"] == "OPEN":
            return httpx.Response(200, json={
                "data": [_position(WALLET_A, status="OPEN")],
                "pagination": {"has_more": True, "next_cursor": "resume-open"},
            })
        return httpx.Response(200, json={
            "data": [], "pagination": {"has_more": False, "next_cursor": None},
        })

    async with AsyncPolymarketDataClient(
        base_url="https://data.example", transport=httpx.MockTransport(handler)
    ) as client:
        result = await client.market_participants(CONDITION, max_pages_per_status=1)

    assert not result.complete
    assert result.current_next_cursor == "resume-open"
    assert result.past_next_cursor is None


async def test_wallet_history_rejects_wrong_wallet_in_source_row() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "data": [_trade(WALLET_B)],
            "pagination": {"has_more": False, "next_cursor": None},
        })

    async with AsyncPolymarketDataClient(
        base_url="https://data.example", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(ValueError, match="wallet differs"):
            await client.wallet_history(WALLET_A)
