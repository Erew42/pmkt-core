import asyncio
import hashlib
import json

import httpx
import pandas as pd
import pytest

from pmkt._http import RequestPolicy
from pmkt.data.canonical import KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION
from pmkt.data.normalize_kalshi import (
    kalshi_market_matches_query_status,
    kalshi_status_from_lifecycle_event,
    normalize_kalshi_market_status,
)
from pmkt.data.validation import validate_frame
from pmkt.exchanges.kalshi.client import (
    AsyncKalshiClient,
    normalize_kalshi_market,
    normalize_kalshi_orderbook,
)


class GateLimiter:
    def __init__(self) -> None:
        self.entered = 0
        self.all_entered = asyncio.Event()
        self.release = asyncio.Event()

    async def __aenter__(self):
        self.entered += 1
        if self.entered == 2:
            self.all_entered.set()
        await self.release.wait()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class PathHeaderAuth:
    def headers_for_get(self, path: str) -> dict[str, str]:
        return {"X-Auth-Path": f"GET {path}"}


@pytest.mark.asyncio
async def test_kalshi_markets_page_serializes_params() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        return httpx.Response(200, json={"markets": [], "cursor": ""})

    transport = httpx.MockTransport(handler)
    async with AsyncKalshiClient(
        base_url="https://example.com/trade-api/v2",
        transport=transport,
    ) as client:
        await client.markets_page(
            limit=50,
            cursor="abc",
            status="open",
            tickers=["KXONE", "KXTWO"],
            series_ticker="KXSERIES",
            min_created_ts=1_700_000_000,
            max_created_ts=1_700_003_600,
            min_updated_ts=1_700_000_100,
            max_updated_ts=1_700_003_700,
            min_settled_ts=1_700_000_200,
            max_settled_ts=1_700_003_800,
        )

    assert seen == [
        (
            "/trade-api/v2/markets",
            {
                "limit": "50",
                "cursor": "abc",
                "status": "open",
                "series_ticker": "KXSERIES",
                "tickers": "KXONE,KXTWO",
                "min_created_ts": "1700000000",
                "max_created_ts": "1700003600",
                "min_updated_ts": "1700000100",
                "max_updated_ts": "1700003700",
                "min_settled_ts": "1700000200",
                "max_settled_ts": "1700003800",
            },
        )
    ]


@pytest.mark.asyncio
async def test_kalshi_client_accepts_catalog_request_policy() -> None:
    policy = RequestPolicy(max_retries=20, backoff_base_s=5.0, backoff_max_s=60.0)
    client = AsyncKalshiClient(
        base_url="https://example.com/trade-api/v2",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"markets": [], "cursor": ""})
        ),
        request_policy=policy,
    )

    assert client._http.request_policy is policy
    await client.close()


@pytest.mark.asyncio
async def test_kalshi_markets_page_omits_status_when_none() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(dict(request.url.params))
        return httpx.Response(200, json={"markets": [], "cursor": ""})

    async with AsyncKalshiClient(
        base_url="https://example.com/trade-api/v2",
        transport=httpx.MockTransport(handler),
    ) as client:
        await client.markets_page(status=None, tickers=["KXONE", "KXTWO"])

    assert "status" not in seen
    assert seen["tickers"] == "KXONE,KXTWO"


@pytest.mark.asyncio
async def test_kalshi_iter_markets_follows_cursor_after_empty_page() -> None:
    cursors: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("cursor")
        cursors.append(cursor)
        if cursor is None:
            return httpx.Response(200, json={"markets": [], "cursor": "live"})
        return httpx.Response(
            200,
            json={"markets": [{"ticker": "KXFOUND"}], "cursor": ""},
        )

    async with AsyncKalshiClient(
        base_url="https://example.com/trade-api/v2",
        transport=httpx.MockTransport(handler),
    ) as client:
        rows = [row async for row in client.iter_markets(status=None)]

    assert cursors == [None, "live"]
    assert rows == [{"ticker": "KXFOUND"}]


@pytest.mark.asyncio
async def test_kalshi_iter_markets_stops_on_terminal_empty_page() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"markets": [], "cursor": ""})

    async with AsyncKalshiClient(
        base_url="https://example.com/trade-api/v2",
        transport=httpx.MockTransport(handler),
    ) as client:
        rows = [row async for row in client.iter_markets(status=None)]

    assert rows == []
    assert calls == 1


@pytest.mark.asyncio
async def test_kalshi_iter_markets_rejects_repeated_cursor() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"markets": [], "cursor": "same"})

    async with AsyncKalshiClient(
        base_url="https://example.com/trade-api/v2",
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(RuntimeError, match="cursor repeated"):
            _ = [row async for row in client.iter_markets(status=None)]


@pytest.mark.asyncio
async def test_kalshi_iter_markets_preserves_page_limit() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"markets": [], "cursor": str(calls)})

    async with AsyncKalshiClient(
        base_url="https://example.com/trade-api/v2",
        transport=httpx.MockTransport(handler),
    ) as client:
        rows = [row async for row in client.iter_markets(status=None, max_pages=2)]

    assert rows == []
    assert calls == 2


@pytest.mark.asyncio
async def test_kalshi_events_page_serializes_params() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        return httpx.Response(200, json={"events": [], "cursor": ""})

    transport = httpx.MockTransport(handler)
    async with AsyncKalshiClient(
        base_url="https://example.com/trade-api/v2",
        transport=transport,
    ) as client:
        await client.events_page(
            limit=25,
            cursor="abc",
            status="open",
            series_ticker="KXSERIES",
            with_nested_markets=True,
        )

    assert seen == [
        (
            "/trade-api/v2/events",
            {
                "limit": "25",
                "cursor": "abc",
                "status": "open",
                "series_ticker": "KXSERIES",
                "with_nested_markets": "true",
            },
        )
    ]


@pytest.mark.asyncio
async def test_kalshi_orderbook_path_and_depth() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        return httpx.Response(
            200,
            json={"orderbook_fp": {"yes_dollars": [], "no_dollars": []}},
        )

    transport = httpx.MockTransport(handler)
    async with AsyncKalshiClient(
        base_url="https://example.com/trade-api/v2",
        transport=transport,
    ) as client:
        await client.orderbook("KXTEST", depth=10)

    assert seen == [("/trade-api/v2/markets/KXTEST/orderbook", {"depth": "10"})]


@pytest.mark.asyncio
async def test_kalshi_candlestick_paths_and_params() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        return httpx.Response(200, json={"candlesticks": []})

    transport = httpx.MockTransport(handler)
    async with AsyncKalshiClient(
        base_url="https://example.com/trade-api/v2",
        transport=transport,
    ) as client:
        await client.market_candlesticks(
            "KXSERIES",
            "KXTEST",
            start_ts=100,
            end_ts=200,
            period_interval=60,
            include_latest_before_start=True,
        )
        await client.historical_market_candlesticks(
            "KXOLD",
            start_ts=100,
            end_ts=200,
            period_interval=1440,
        )

    assert seen == [
        (
            "/trade-api/v2/series/KXSERIES/markets/KXTEST/candlesticks",
            {
                "start_ts": "100",
                "end_ts": "200",
                "period_interval": "60",
                "include_latest_before_start": "true",
            },
        ),
        (
            "/trade-api/v2/historical/markets/KXOLD/candlesticks",
            {
                "start_ts": "100",
                "end_ts": "200",
                "period_interval": "1440",
            },
        ),
    ]


@pytest.mark.asyncio
async def test_kalshi_batch_candlesticks_and_historical_trades_params() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        return httpx.Response(200, json={"markets": [], "trades": [], "cursor": ""})

    transport = httpx.MockTransport(handler)
    async with AsyncKalshiClient(
        base_url="https://example.com/trade-api/v2",
        transport=transport,
    ) as client:
        await client.batch_market_candlesticks(
            ["KXONE", "KXTWO"],
            start_ts=100,
            end_ts=200,
            period_interval=1,
        )
        await client.historical_trades(
            ticker="KXONE",
            min_ts=100,
            max_ts=200,
            is_block_trade=False,
            limit=50,
        )

    assert seen == [
        (
            "/trade-api/v2/markets/candlesticks",
            {
                "market_tickers": "KXONE,KXTWO",
                "start_ts": "100",
                "end_ts": "200",
                "period_interval": "1",
            },
        ),
        (
            "/trade-api/v2/historical/trades",
            {
                "limit": "50",
                "ticker": "KXONE",
                "min_ts": "100",
                "max_ts": "200",
                "is_block_trade": "false",
            },
        ),
    ]


@pytest.mark.asyncio
async def test_kalshi_historical_market_unwraps_official_market_envelope() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/trade-api/v2/historical/markets/KXRAIN"
        return httpx.Response(
            200,
            json={
                "market": {
                    "ticker": "KXRAIN",
                    "status": "settled",
                    "settlement_value_dollars": "1",
                    "settlement_ts": "2026-01-01T00:00:00Z",
                    "is_provisional": False,
                }
            },
        )

    transport = httpx.MockTransport(handler)
    async with AsyncKalshiClient(
        base_url="https://example.com/trade-api/v2",
        transport=transport,
    ) as client:
        market = await client.historical_market("KXRAIN")

    assert market == {
        "ticker": "KXRAIN",
        "status": "settled",
        "settlement_value_dollars": "1",
        "settlement_ts": "2026-01-01T00:00:00Z",
        "is_provisional": False,
    }


@pytest.mark.asyncio
async def test_kalshi_auth_headers_are_per_request_under_concurrency() -> None:
    seen: list[tuple[str, str]] = []
    gate = GateLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, request.headers["X-Auth-Path"]))
        if request.url.path.endswith("/events"):
            return httpx.Response(200, json={"events": [], "cursor": ""})
        return httpx.Response(200, json={"markets": [], "cursor": ""})

    transport = httpx.MockTransport(handler)
    async with AsyncKalshiClient(
        base_url="https://example.com/trade-api/v2",
        auth=PathHeaderAuth(),
        transport=transport,
        limiter=gate,
    ) as client:
        markets_task = asyncio.create_task(client.markets_page(limit=1))
        events_task = asyncio.create_task(client.events_page(limit=1))
        await asyncio.wait_for(gate.all_entered.wait(), timeout=1)
        gate.release.set()
        await asyncio.gather(markets_task, events_task)

    assert sorted(seen) == [
        ("/trade-api/v2/events", "GET /trade-api/v2/events"),
        ("/trade-api/v2/markets", "GET /trade-api/v2/markets"),
    ]


def test_normalize_kalshi_orderbook_derives_bid_ask_equivalents() -> None:
    normalized = normalize_kalshi_orderbook(
        {
            "orderbook_fp": {
                "yes_dollars": [["0.4000", "10.00"], ["0.3000", "7.00"]],
                "no_dollars": [["0.3500", "5.00"]],
            }
        },
        market_ticker="KXTEST",
    )

    assert normalized["yes_bid"] == pytest.approx(0.4)
    assert normalized["yes_ask"] == pytest.approx(0.65)
    assert normalized["no_bid"] == pytest.approx(0.35)
    assert normalized["no_ask"] == pytest.approx(0.6)
    assert normalized["yes_bid_source"] == "direct"
    assert normalized["yes_ask_source"] == "complement_derived"
    assert normalized["no_bid_source"] == "direct"
    assert normalized["no_ask_source"] == "complement_derived"
    assert normalized["mid"] == pytest.approx(0.525)
    assert normalized["spread"] == pytest.approx(0.25)
    assert normalized["yes_bid_size"] == pytest.approx(10.0)
    assert normalized["yes_ask_size"] == pytest.approx(5.0)
    assert normalized["depth"] == 3
    assert normalized["yes_bid_depth"] == 2
    assert normalized["no_bid_depth"] == 1


def test_normalize_kalshi_orderbook_ignores_non_finite_price_levels() -> None:
    normalized = normalize_kalshi_orderbook(
        {
            "orderbook_fp": {
                "yes_dollars": [["inf", "10"], ["0.45", "nan"], ["0.40", "5"]],
                "no_dollars": [[float("-inf"), "5"], ["0.30", "4"]],
            }
        },
        market_ticker="KXTEST",
    )

    assert normalized["yes_bid"] == pytest.approx(0.4)
    assert normalized["yes_ask"] == pytest.approx(0.7)
    assert normalized["yes_bid_depth"] == 1
    assert normalized["no_bid_depth"] == 1


def test_normalize_kalshi_orderbook_marks_missing_derived_quote_sources() -> None:
    normalized = normalize_kalshi_orderbook(
        {"orderbook_fp": {"yes_dollars": [["0.4000", "10.00"]], "no_dollars": []}},
        market_ticker="KXTEST",
    )

    assert normalized["yes_bid_source"] == "direct"
    assert normalized["yes_ask"] is None
    assert normalized["yes_ask_source"] == "missing"
    assert normalized["no_bid_source"] == "missing"
    assert normalized["no_ask_source"] == "complement_derived"


def test_normalize_kalshi_market_ignores_non_finite_price_fields() -> None:
    raw_market = {
        "ticker": "KXTEST",
        "title": "Will event happen?",
        "yes_bid_dollars": "inf",
        "yes_ask_dollars": "0.60",
        "no_bid_dollars": "0.40",
        "volume_fp": "-inf",
        "liquidity_dollars": "100.5",
    }
    normalized = normalize_kalshi_market(raw_market)

    assert normalized["yes_bid"] is None
    assert normalized["no_bid"] == pytest.approx(0.40)
    assert normalized["mid"] is None
    assert normalized["volume"] is None
    assert normalized["liquidity"] == pytest.approx(100.5)
    assert normalized["schema_version"] == KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION
    assert normalized["raw_json"] == json.dumps(
        raw_market,
        ensure_ascii=True,
        sort_keys=True,
        default=str,
    )
    assert (
        normalized["raw_json_sha256"]
        == hashlib.sha256(normalized["raw_json"].encode("utf-8")).hexdigest()
    )
    assert validate_frame(
        pd.DataFrame([normalized]),
        KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
        strict=True,
    ).ok


@pytest.mark.parametrize(
    ("query_status", "response_status"),
    [
        ("unopened", "initialized"),
        ("open", "active"),
        ("paused", "inactive"),
        ("closed", "closed"),
        ("closed", "determined"),
        ("closed", "disputed"),
        ("closed", "amended"),
        ("settled", "finalized"),
    ],
)
def test_kalshi_query_status_uses_documented_response_vocabulary(
    query_status: str,
    response_status: str,
) -> None:
    assert kalshi_market_matches_query_status(response_status, query_status)
    assert not kalshi_market_matches_query_status("future_status", query_status)


def test_kalshi_market_status_aliases_and_terminal_flags_are_canonical() -> None:
    active = normalize_kalshi_market(
        {"ticker": "KXACTIVE", "title": "Active?", "status": "open"}
    )
    finalized = normalize_kalshi_market(
        {"ticker": "KXFINAL", "title": "Final?", "status": "finalized"}
    )
    disputed = normalize_kalshi_market(
        {"ticker": "KXDISPUTED", "title": "Disputed?", "status": "disputed"}
    )

    assert active["status"] == "active"
    assert active["closed"] is False
    assert finalized["closed"] is True
    assert finalized["is_provisional"] is False
    assert disputed["closed"] is True
    assert disputed["is_provisional"] is True
    assert normalize_kalshi_market_status(" settled ") == "finalized"
    assert kalshi_status_from_lifecycle_event("activated") == "active"
    assert kalshi_status_from_lifecycle_event("settled") == "finalized"
    assert kalshi_status_from_lifecycle_event("metadata_updated") is None


def test_normalize_kalshi_market_includes_binary_outcome_label_in_question() -> None:
    normalized = normalize_kalshi_market(
        {
            "ticker": "KXSPOTIFYARTISTD-26MAY18-DRA",
            "title": "Top USA Artist on Spotify on May 18, 2026?",
            "subtitle": "::",
            "yes_sub_title": "Drake",
            "custom_strike": {"Song/Artist/Album": "Drake"},
        }
    )

    assert (
        normalized["question"] == "Top USA Artist on Spotify on May 18, 2026? - Drake"
    )


def test_normalize_kalshi_market_uses_custom_strike_when_subtitle_is_empty() -> None:
    normalized = normalize_kalshi_market(
        {
            "ticker": "KXTEST",
            "title": "Will one of these artists be #1?",
            "subtitle": "::",
            "custom_strike": {"Song/Artist/Album": "Bad Bunny"},
        }
    )

    assert normalized["question"] == "Will one of these artists be #1? - Bad Bunny"
