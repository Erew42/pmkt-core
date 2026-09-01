import json

import httpx
import pytest

from pmkt.exchanges.polymarket.clob import AsyncClobClient


pytestmark = pytest.mark.asyncio


async def test_clob_paths_and_params() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        return httpx.Response(200, json={"hash": "abc", "market": "m1", "bids": [], "asks": []})

    transport = httpx.MockTransport(handler)
    async with AsyncClobClient(base_url="https://example.com/api", transport=transport) as client:
        await client.book("token-1")
        await client.price("token-2", "BUY")
        await client.midpoint("token-3")

    assert seen == [
        ("/api/book", {"token_id": "token-1"}),
        ("/api/price", {"token_id": "token-2", "side": "BUY"}),
        ("/api/midpoint", {"token_id": "token-3"}),
    ]


async def test_clob_books_posts_batch_body() -> None:
    seen: list[tuple[str, list[dict[str, str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, json.loads(request.content)))
        return httpx.Response(
            200,
            json=[
                {"hash": "h1", "market": "m1", "asset_id": "token-1", "bids": [], "asks": []},
                {"hash": "h2", "market": "m2", "asset_id": "token-2", "bids": [], "asks": []},
            ],
        )

    transport = httpx.MockTransport(handler)
    async with AsyncClobClient(base_url="https://example.com/api", transport=transport) as client:
        books = await client.books(["token-1", "token-2"])

    assert seen == [
        (
            "/api/books",
            [{"token_id": "token-1"}, {"token_id": "token-2"}],
        )
    ]
    assert [book.asset_id for book in books] == ["token-1", "token-2"]


async def test_clob_fee_rate_uses_token_path() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return httpx.Response(200, json={"base_fee": 30})

    transport = httpx.MockTransport(handler)
    async with AsyncClobClient(base_url="https://example.com/api", transport=transport) as client:
        payload = await client.fee_rate("token-1")

    assert seen == ["/api/fee-rate/token-1"]
    assert payload == {"base_fee": 30}


async def test_event_prices_history_skip_missing() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        market = request.url.params.get("market")
        if market == "missing":
            return httpx.Response(404, json={"detail": "Not found"})
        return httpx.Response(200, json={"market": market, "history": [{"t": 1234567890, "p": 0.5}]})

    transport = httpx.MockTransport(handler)
    async with AsyncClobClient(base_url="https://example.com/api", transport=transport) as client:
        event = {
            "markets": [
                {
                    "outcomes": [
                        {"token_id": "present"},
                        {"token_id": "missing"},
                    ]
                }
            ]
        }

        data = await client.event_prices_history(event, interval="1d", skip_missing=True)

    assert set(data.keys()) == {"present"}
    assert data["present"].history[0].t == 1234567890


async def test_prices_history_omits_interval_when_time_bounds_are_supplied() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        return httpx.Response(200, json={"history": [{"t": 1234567890, "p": 0.5}]})

    transport = httpx.MockTransport(handler)
    async with AsyncClobClient(base_url="https://example.com/api", transport=transport) as client:
        history = await client.prices_history(
            "token-1",
            interval="1d",
            fidelity=60,
            start_ts=100,
            end_ts=200,
        )

    assert seen == [
        (
            "/api/prices-history",
            {
                "market": "token-1",
                "fidelity": "60",
                "startTs": "100",
                "endTs": "200",
            },
        )
    ]
    assert history.history[0].p == 0.5


async def test_batch_prices_history_omits_interval_when_time_bounds_are_supplied() -> None:
    seen: list[tuple[str, dict[str, object]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, json.loads(request.content)))
        return httpx.Response(
            200,
            json={
                "history": {
                    "token-1": {"history": [{"t": 1234567890, "p": 0.5}]},
                    "token-2": {"history": [{"t": 1234567891, "p": 0.6}]},
                }
            },
        )

    transport = httpx.MockTransport(handler)
    async with AsyncClobClient(base_url="https://example.com/api", transport=transport) as client:
        history = await client.batch_prices_history(
            ["token-1", "token-2"],
            interval="1d",
            start_ts=100,
            end_ts=200,
        )

    assert seen == [
        (
            "/api/batch-prices-history",
            {
                "markets": ["token-1", "token-2"],
                "start_ts": 100,
                "end_ts": 200,
            },
        )
    ]
    assert set(history) == {"token-1", "token-2"}


async def test_batch_prices_history_accepts_live_history_lists() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "history": {
                    "token-1": [{"t": 1234567890, "p": 0.0}],
                    "token-2": [{"t": 1234567891, "p": 0.6}],
                }
            },
        )

    transport = httpx.MockTransport(handler)
    async with AsyncClobClient(base_url="https://example.com/api", transport=transport) as client:
        history = await client.batch_prices_history(["token-1", "token-2"], interval="1d")

    assert set(history) == {"token-1", "token-2"}
    assert history["token-1"].history[0].p == 0.0
    assert history["token-2"].history[0].t == 1234567891
