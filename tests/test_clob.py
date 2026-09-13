import json

import httpx
import pytest

import pmkt.exchanges.polymarket.clob as clob_module
import pmkt.exchanges.polymarket._workflow as workflow_module
from pmkt._operation import OperationExpiry
from pmkt.errors import InvalidDataError, MarketNotFoundError, OperationTimeoutError
from pmkt.exchanges.polymarket.clob import AsyncClobClient
from pmkt.records import KalshiMarketRef, PolymarketInstrumentRef, PolymarketMarketRef


pytestmark = pytest.mark.asyncio


async def test_get_book_sorts_trims_and_preserves_identity_and_native_copy() -> None:
    payload = {
        "market": "condition",
        "asset_id": "token",
        "timestamp": "1789128107042",
        "bids": [
            {"price": "0.2", "size": "2"},
            {"price": "0.4", "size": "4"},
            {"price": "0.3", "size": "3"},
        ],
        "asks": [
            {"price": "0.8", "size": "8"},
            {"price": "0.6", "size": "6"},
            {"price": "0.7", "size": "7"},
        ],
    }
    instrument = PolymarketInstrumentRef(
        "token", market=PolymarketMarketRef("gamma-id", condition_id="condition")
    )
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    async with AsyncClobClient(transport=transport) as client:
        book = await client.get_book(instrument, depth=2)

    assert [level.price for level in book.bids] == [0.4, 0.3]
    assert [level.price for level in book.asks] == [0.6, 0.7]
    assert book.quantity_unit == "shares"
    assert book.native_bid_count == 3
    assert book.pre_trim_bid_count == 3
    assert book.returned_bid_count == 2
    assert book.observation.response_identities == (
        "token_id=token",
        "condition_id=condition",
    )
    payload["bids"] = []
    book.native_payload["asset_id"] = "changed"
    assert book.bids[0].quantity == 4.0
    assert book.instrument.token_id == "token"


async def test_get_book_empty_is_valid_payload_with_quality_state() -> None:
    payload = {"asset_id": "token", "bids": [], "asks": []}
    async with AsyncClobClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=payload)
        )
    ) as client:
        book = await client.get_book(PolymarketInstrumentRef("token"))
    assert book.bids == book.asks == ()
    assert book.valid_state is False
    assert book.quality_flags == ("empty_ask", "empty_bid")
    assert book.exchange_timestamp_utc is None


@pytest.mark.parametrize(
    "payload",
    [
        {"asset_id": "token", "asks": []},
        {"asset_id": "token", "bids": {}, "asks": []},
        {
            "asset_id": "token",
            "bids": [{"price": "nan", "size": "1"}],
            "asks": [],
        },
        {
            "asset_id": "token",
            "bids": [{"price": "1.1", "size": "1"}],
            "asks": [],
        },
        {
            "asset_id": "token",
            "bids": [{"price": "0.1", "size": "-1"}],
            "asks": [],
        },
        {"asset_id": "token", "bids": [["0.1", "1"]], "asks": []},
    ],
)
async def test_get_book_rejects_malformed_ladders(payload: object) -> None:
    async with AsyncClobClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=payload)
        )
    ) as client:
        with pytest.raises(InvalidDataError):
            await client.get_book(PolymarketInstrumentRef("token"))


async def test_get_book_checks_condition_not_gamma_market_id() -> None:
    matching = {"market": "condition", "bids": [], "asks": []}
    bare_gamma_parent = PolymarketInstrumentRef(
        "token", market=PolymarketMarketRef("not-a-condition")
    )
    async with AsyncClobClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=matching)
        )
    ) as client:
        await client.get_book(bare_gamma_parent)

    mismatched = {
        "asset_id": "other",
        "market": "other-condition",
        "bids": [],
        "asks": [],
    }
    enriched = PolymarketInstrumentRef(
        "token", market=PolymarketMarketRef("gamma", condition_id="condition")
    )
    async with AsyncClobClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=mismatched)
        )
    ) as client:
        with pytest.raises(InvalidDataError, match="token mismatch"):
            await client.get_book(enriched)

    condition_only_mismatch = {
        "asset_id": "token",
        "market": "other-condition",
        "bids": [],
        "asks": [],
    }
    async with AsyncClobClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=condition_only_mismatch)
        )
    ) as client:
        with pytest.raises(InvalidDataError, match="condition mismatch"):
            await client.get_book(enriched)


async def test_get_book_wrong_ref_depth_and_scoped_404() -> None:
    requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(404, json={"error": "not found"})

    client = AsyncClobClient(transport=httpx.MockTransport(handler))
    with pytest.raises(TypeError, match="PolymarketInstrumentRef"):
        await client.get_book(KalshiMarketRef("ticker"))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="depth"):
        await client.get_book(  # type: ignore[arg-type]
            PolymarketInstrumentRef("token"), depth=True
        )
    assert requests == 0
    with pytest.raises(MarketNotFoundError) as caught:
        await client.get_book(PolymarketInstrumentRef("missing"))
    assert caught.value.identifier == "missing"
    assert caught.value.lookup_scope == "CLOB current token book"
    await client.close()


@pytest.mark.parametrize("deadline", [None, True, 0, -1, float("nan")])
async def test_get_book_requires_bounded_deadline_before_io(deadline: object) -> None:
    requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, json={"bids": [], "asks": []})

    client = AsyncClobClient(transport=httpx.MockTransport(handler))
    expected = TypeError if deadline is None or isinstance(deadline, bool) else ValueError
    with pytest.raises(expected):
        await client.get_book(  # type: ignore[arg-type]
            PolymarketInstrumentRef("token"), deadline_s=deadline
        )
    assert requests == 0
    await client.close()


async def test_get_book_expiry_during_normalization_leaves_client_reusable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 0.0

    def clock() -> float:
        return now

    original_bounded = OperationExpiry.bounded
    monkeypatch.setattr(
        clob_module.OperationExpiry,
        "bounded",
        classmethod(lambda cls, timeout_s: original_bounded(timeout_s, clock=clock)),
    )
    original_finite_number = workflow_module._finite_number
    parsed_values = 0

    def finite_number_spy(value: object, label: str) -> float:
        nonlocal now, parsed_values
        parsed_values += 1
        result = original_finite_number(value, label)
        if parsed_values == 600:
            now = 2.0
        return result

    monkeypatch.setattr(workflow_module, "_finite_number", finite_number_spy)
    payload = {
        "hash": "hash",
        "asset_id": "token",
        "bids": [{"price": "0.4", "size": "1"}] * 5000,
        "asks": [{"price": "0.6", "size": "1"}] * 5000,
    }
    client = AsyncClobClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=payload)
        )
    )
    with pytest.raises(OperationTimeoutError, match="operation expired"):
        await client.get_book(PolymarketInstrumentRef("token"), deadline_s=2.0)
    assert 600 <= parsed_values < 20_000
    native = await client.book("token")
    assert native.asset_id == "token"
    await client.close()


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


@pytest.mark.parametrize(
    "requested,returned", [("0xABCDEF", "0xabcdef"), ("0xabcdef", "0xABCDEF")]
)
async def test_get_book_condition_identity_is_case_insensitive(requested, returned):
    instrument = PolymarketInstrumentRef(
        "token", market=PolymarketMarketRef("gamma-id", condition_id=requested)
    )
    payload = {"asset_id": "token", "market": returned, "bids": [], "asks": []}
    async with AsyncClobClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    ) as client:
        book = await client.get_book(instrument)
    assert book.instrument == instrument
    assert f"condition_id={returned}" in book.observation.response_identities
