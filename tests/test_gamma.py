import httpx
import pytest

from pmkt._http import RequestPolicy
from pmkt.exchanges.polymarket.gamma import AsyncGammaClient


pytestmark = pytest.mark.asyncio


async def test_markets_page_joins_base_url_path() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["params"] = dict(request.url.params)
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    async with AsyncGammaClient(base_url="https://example.com/api", transport=transport) as client:
        data = await client.markets_page(limit=5, offset=10, closed=True, related_tags=False)

    assert data == []
    assert seen["path"] == "/api/markets"
    assert seen["params"] == {
        "limit": "5",
        "offset": "10",
        "closed": "true",
        "related_tags": "false",
    }


async def test_markets_page_rejects_invalid_pagination() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=[]))
    client = AsyncGammaClient(transport=transport)

    with pytest.raises(ValueError):
        await client.markets_page(limit=0)
    with pytest.raises(ValueError):
        await client.markets_page(offset=-1)
    with pytest.raises(TypeError):
        await client.markets_page(limit=1.5)
    with pytest.raises(TypeError):
        await client.markets_page(limit=True)


async def test_market_with_events_uses_detail_closed_state_and_validates_key() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        if request.url.path.endswith("/markets/42"):
            return httpx.Response(200, json={"id": "42", "closed": True})
        return httpx.Response(
            200,
            json=[{"id": "42", "events": [{"id": "event-1"}]}],
        )

    transport = httpx.MockTransport(handler)
    async with AsyncGammaClient(
        base_url="https://example.com/api", transport=transport
    ) as client:
        payload = await client.market_with_events("42")

    assert payload["events"] == [{"id": "event-1"}]
    assert seen == [
        ("/api/markets/42", {}),
        ("/api/markets", {"id": "42", "closed": "true"}),
    ]


async def test_markets_keyset_page_serializes_cursor_params() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["params"] = dict(request.url.params)
        return httpx.Response(
            200,
            json={
                "markets": [{"id": "1", "question": "Will it rain?"}],
                "next_cursor": "next-1",
            },
        )

    transport = httpx.MockTransport(handler)
    async with AsyncGammaClient(base_url="https://example.com/api", transport=transport) as client:
        data = await client.markets_keyset_page(
            limit=5,
            after_cursor="cursor-0",
            closed=False,
            related_tags=False,
            order="createdAt",
            ascending=False,
        )

    assert data["markets"][0].id == "1"
    assert data["next_cursor"] == "next-1"
    assert seen["path"] == "/api/markets/keyset"
    assert seen["params"] == {
        "limit": "5",
        "after_cursor": "cursor-0",
        "closed": "false",
        "related_tags": "false",
        "order": "createdAt",
        "ascending": "false",
    }


async def test_markets_keyset_serializes_repeated_condition_ids_and_caps_limit() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/markets":
            return httpx.Response(200, json=[])
        seen["condition_ids"] = request.url.params.get_list("condition_ids")
        return httpx.Response(200, json={"markets": [], "next_cursor": ""})

    transport = httpx.MockTransport(handler)
    async with AsyncGammaClient(transport=transport) as client:
        await client.markets_keyset_raw_page(
            limit=100,
            condition_ids=["0xaaa", "0xbbb"],
        )
        with pytest.raises(ValueError, match="between 1 and 100"):
            await client.markets_keyset_raw_page(limit=101)
        assert await client.markets_page(limit=101) == []

    assert seen["condition_ids"] == ["0xaaa", "0xbbb"]


async def test_iter_markets_keyset_stops_on_cursor_exhaustion() -> None:
    seen_cursors: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("after_cursor")
        seen_cursors.append(cursor)
        if cursor is None:
            return httpx.Response(
                200,
                json={
                    "markets": [{"id": "1", "question": "First market"}],
                    "next_cursor": "cursor-1",
                },
            )
        return httpx.Response(
            200,
            json={
                "markets": [{"id": "2", "question": "Second market"}],
                "next_cursor": "",
            },
        )

    transport = httpx.MockTransport(handler)
    async with AsyncGammaClient(base_url="https://example.com", transport=transport) as client:
        markets = [market async for market in client.iter_markets_keyset(limit=2, closed=False)]

    assert [market.id for market in markets] == ["1", "2"]
    assert seen_cursors == [None, "cursor-1"]


async def test_iter_markets_keyset_stops_on_terminal_lte_cursor() -> None:
    seen_cursors: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("after_cursor")
        seen_cursors.append(cursor)
        return httpx.Response(
            200,
            json={
                "markets": [{"id": "1", "question": "Terminal market"}],
                "next_cursor": "LTE=",
            },
        )

    transport = httpx.MockTransport(handler)
    async with AsyncGammaClient(base_url="https://example.com", transport=transport) as client:
        markets = [market async for market in client.iter_markets_keyset(limit=2)]

    assert [market.id for market in markets] == ["1"]
    assert seen_cursors == [None]


async def test_client_reuse_and_close() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=[]))
    client = AsyncGammaClient(transport=transport)

    await client.markets_page(limit=1)
    first = client._http._client
    await client.markets_page(limit=1)
    assert client._http._client is first

    await client.close()
    assert client._http._client is None


async def test_gamma_client_accepts_catalog_request_policy() -> None:
    policy = RequestPolicy(max_retries=20, backoff_base_s=5.0, backoff_max_s=60.0)
    client = AsyncGammaClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=[])),
        request_policy=policy,
    )

    assert client._http.request_policy is policy
    await client.close()


async def test_events_page_serializes_bool_params() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["params"] = dict(request.url.params)
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    async with AsyncGammaClient(base_url="https://example.com/api", transport=transport) as client:
        data = await client.events_page(
            limit=2, offset=0, closed=False, ascending=True, order="start_date"
        )

    assert data == []
    assert seen["path"] == "/api/events"
    assert seen["params"] == {
        "limit": "2",
        "offset": "0",
        "closed": "false",
        "ascending": "true",
        "order": "start_date",
    }
