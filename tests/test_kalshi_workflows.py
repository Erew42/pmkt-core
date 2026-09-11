from __future__ import annotations

import asyncio
import math

import httpx
import pytest

import pmkt.exchanges.kalshi._workflow as workflow_module
import pmkt.exchanges.kalshi.client as client_module
from pmkt._operation import OperationExpiry
from pmkt.errors import (
    InvalidDataError,
    MarketNotFoundError,
    OperationTimeoutError,
    UnsupportedCapabilityError,
)
from pmkt.exchanges.kalshi import (
    AsyncKalshiClient,
    KalshiFilter,
    KalshiInstrumentRef,
    KalshiMarketRef,
)
from pmkt.records import PolymarketInstrumentRef


pytestmark = pytest.mark.asyncio


def market_row(
    ticker: str = "KXTEST",
    *,
    title: str = "Will this happen?",
    market_type: object = "binary",
    status: object = "active",
    series_ticker: object = None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": ticker,
        "title": title,
        "market_type": market_type,
        "status": status,
        "event_ticker": "KXEVENT",
        "open_time": "2026-09-11T10:00:00Z",
        "close_time": "2026-09-11T11:00:00Z",
        "expected_expiration_time": "2026-09-11T12:00:00Z",
        "expiration_time": "2026-09-18T12:00:00Z",
        "created_time": "2026-09-11T09:00:00Z",
        "updated_time": "2026-09-11T10:01:00Z",
    }
    if series_ticker is not None:
        row["series_ticker"] = series_ticker
    return row


def market_envelope(
    ticker: str = "KXTEST",
    *,
    title: str = "Will this happen?",
    market_type: object = "binary",
    status: object = "active",
    series_ticker: object = None,
) -> dict[str, object]:
    return {
        "market": market_row(
            ticker,
            title=title,
            market_type=market_type,
            status=status,
            series_ticker=series_ticker,
        )
    }


async def test_filter_is_required_and_empty_tickers_do_no_io() -> None:
    requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, json={"markets": [], "cursor": ""})

    client = AsyncKalshiClient(transport=httpx.MockTransport(handler))
    with pytest.raises(TypeError, match="filters"):
        await client.discover_markets()  # type: ignore[call-arg]
    result = await client.discover_markets(filters=KalshiFilter(tickers=()))
    assert result.report.stop_reason == "empty_selection"
    assert result.report.traversal_complete
    assert requests == 0
    await client.close()


@pytest.mark.parametrize("status", ["unopened", "open", "paused", "closed", "settled"])
async def test_supported_status_values_and_wire_mapping(status: str) -> None:
    response_status = {
        "unopened": "initialized",
        "open": "active",
        "paused": "inactive",
        "closed": "determined",
        "settled": "finalized",
    }[status]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["status"] == status
        assert request.url.params["mve_filter"] == "exclude"
        return httpx.Response(
            200,
            json={"markets": [market_row(status=response_status)], "cursor": ""},
        )

    async with AsyncKalshiClient(transport=httpx.MockTransport(handler)) as client:
        result = await client.discover_markets(
            filters=KalshiFilter(status=status, mve_filter="exclude")  # type: ignore[arg-type]
        )
    assert [item.ref.ticker for item in result.items] == ["KXTEST"]


async def test_empty_filter_omits_status_and_mve_and_includes_finalized() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "status" not in request.url.params
        assert "mve_filter" not in request.url.params
        return httpx.Response(
            200,
            json={"markets": [market_row(status="finalized")], "cursor": ""},
        )

    async with AsyncKalshiClient(transport=httpx.MockTransport(handler)) as client:
        result = await client.discover_markets(filters=KalshiFilter())
    assert result.items[0].status == "finalized"
    assert result.report.source_scope.endswith("all_statuses_mve_included")
    assert dict(result.report.observations[0].effective_parameters) == {"limit": "1000"}


async def test_targeted_chunks_deduplicate_and_share_global_page_budget() -> None:
    tickers = tuple(f"T{index}" for index in range(21)) + ("T0",)
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        selected = request.url.params["tickers"]
        seen.append(selected)
        cursor = "more-2" if "cursor" in request.url.params else "more"
        return httpx.Response(200, json={"markets": [], "cursor": cursor})

    async with AsyncKalshiClient(transport=httpx.MockTransport(handler)) as client:
        result = await client.discover_markets(
            filters=KalshiFilter(tickers=tickers), max_pages=3
        )
    assert seen == [",".join(f"T{i}" for i in range(20)), "T20", seen[0]]
    assert result.report.requested_selector_count == 21
    assert result.report.queried_chunks == 2
    assert result.report.pages_fetched == 3
    assert result.report.stop_reason == "page_limit"


async def test_first_observation_wins_before_local_filter() -> None:
    rows = [
        market_row(title="does not match"),
        market_row(title="later target"),
    ]
    async with AsyncKalshiClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, json={"markets": rows, "cursor": ""}
            )
        )
    ) as client:
        result = await client.discover_markets(
            filters=KalshiFilter(question_contains="target")
        )
    assert result.items == ()
    assert result.report.rows_scanned == 2
    assert result.report.unique_markets_seen == 1
    assert result.report.duplicates_seen == 1


async def test_targeted_matching_rejects_unrelated_rows_and_caps_partial_page() -> None:
    rows = [market_row("UNRELATED"), market_row("T0"), market_row("T1")]
    async with AsyncKalshiClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, json={"markets": rows, "cursor": ""}
            )
        )
    ) as client:
        result = await client.discover_markets(
            filters=KalshiFilter(tickers=("T0", "T1")), max_markets=1
        )
    assert [item.ref.ticker for item in result.items] == ["T0"]
    assert result.report.stop_reason == "result_limit"
    assert result.report.rows_scanned == 2
    assert result.report.unique_markets_seen == 2
    assert not result.report.traversal_complete


async def test_unknown_and_unsupported_mapping_do_not_match_false_predicate() -> None:
    missing = market_row("MISSING")
    missing.pop("market_type")
    rows = [missing, market_row("SCALAR", market_type="scalar")]
    async with AsyncKalshiClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, json={"markets": rows, "cursor": ""}
            )
        )
    ) as client:
        result = await client.discover_markets(
            filters=KalshiFilter(has_instruments=False)
        )
    assert result.items == ()
    assert dict(result.report.unknown_filter_counts) == {"has_instruments": 2}


async def test_series_filter_does_not_fabricate_parent_enrichment() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["series_ticker"] == "SERIES"
        return httpx.Response(
            200, json={"markets": [market_row()], "cursor": ""}
        )

    async with AsyncKalshiClient(transport=httpx.MockTransport(handler)) as client:
        result = await client.discover_markets(
            filters=KalshiFilter(series_ticker="SERIES")
        )
    assert result.items[0].ref.series_ticker is None
    assert dict(result.report.unknown_filter_counts) == {"series_ticker": 1}
    assert "series_ticker" in result.report.applied_server_filters
    assert "series_ticker" not in result.report.applied_local_filters


async def test_returned_series_contradiction_is_excluded_and_diagnosed() -> None:
    async with AsyncKalshiClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={
                    "markets": [market_row(series_ticker="OTHER")],
                    "cursor": "",
                },
            )
        )
    ) as client:
        result = await client.discover_markets(
            filters=KalshiFilter(series_ticker="SERIES")
        )
    assert result.items == ()
    assert result.report.issues[0].code == "inconsistent_mapping"


@pytest.mark.parametrize(
    "payload",
    [
        {"markets": []},
        {"markets": "bad", "cursor": ""},
        {"markets": [], "cursor": 4},
        {"markets": [{"title": "missing ticker"}], "cursor": ""},
    ],
)
async def test_discovery_rejects_malformed_envelopes(payload: object) -> None:
    async with AsyncKalshiClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=payload)
        )
    ) as client:
        with pytest.raises(InvalidDataError):
            await client.discover_markets(filters=KalshiFilter())


async def test_discovery_rejects_repeated_cursor_before_result_limit() -> None:
    responses = iter(
        (
            {"markets": [], "cursor": "same"},
            {"markets": [market_row()], "cursor": "same"},
        )
    )
    async with AsyncKalshiClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=next(responses))
        )
    ) as client:
        with pytest.raises(InvalidDataError, match="repeated cursor"):
            await client.discover_markets(filters=KalshiFilter(), max_markets=1)


async def test_detail_source_is_explicit_encoded_and_has_no_fallback() -> None:
    urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        urls.append(str(request.url))
        return httpx.Response(404, json={"error": "missing"})

    client = AsyncKalshiClient(transport=httpx.MockTransport(handler))
    with pytest.raises(MarketNotFoundError) as live_error:
        await client.get_market(ticker="A/B?x=1", source="live")
    assert live_error.value.venue == "kalshi"
    assert live_error.value.identifier == "A/B?x=1"
    assert "historical archive was not checked" in live_error.value.lookup_scope
    assert len(urls) == 1
    assert urls[0].endswith("/markets/A%2FB%3Fx%3D1")
    urls.clear()
    with pytest.raises(MarketNotFoundError) as historical_error:
        await client.get_market(ticker="OLD", source="historical")
    assert "historical market detail" in historical_error.value.lookup_scope
    assert len(urls) == 1
    assert urls[0].endswith("/historical/markets/OLD")
    await client.close()


@pytest.mark.parametrize(
    ("type_fields", "mapping", "supported"),
    [
        ({}, "unknown", False),
        ({"market_type": "scalar"}, "unknown", False),
        ({"market_type": "binary"}, "mapped", True),
        ({"market_type": "binary", "type": "scalar"}, "inconsistent", False),
    ],
)
async def test_market_capability_mapping_states(
    type_fields: dict[str, object], mapping: str, supported: bool
) -> None:
    row = market_row()
    row.pop("market_type")
    row.update(type_fields)
    async with AsyncKalshiClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"market": row})
        )
    ) as client:
        market = await client.get_market(ticker="KXTEST")
    assert market.mapping_status == mapping
    assert market.book_supported is supported
    assert tuple(item.side for item in market.instruments) == (
        ("yes", "no") if supported else ()
    )


async def test_book_projects_both_outcomes_with_opposite_quantities_then_trims() -> None:
    calls: list[tuple[str, str]] = []
    book_payload = {
        "orderbook_fp": {
            "yes_dollars": [
                ["0.40000000003", "10"],
                ["0.30", "7"],
                ["0.40000000003", "12"],
                ["0", "5"],
                ["0.45", "0"],
            ],
            "no_dollars": [["0.35", "5"], ["0.20", "2"]],
        }
    }

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.url.query.decode()))
        if request.url.path.endswith("/orderbook"):
            return httpx.Response(200, json=book_payload)
        return httpx.Response(
            200, json=market_envelope(series_ticker="SERIES")
        )

    async with AsyncKalshiClient(transport=httpx.MockTransport(handler)) as client:
        yes = await client.get_book(
            KalshiInstrumentRef(KalshiMarketRef("KXTEST"), "yes"), depth=1
        )
        no = await client.get_book(
            KalshiInstrumentRef(KalshiMarketRef("KXTEST"), "no"), depth=1
    )
    assert isinstance(yes.instrument, KalshiInstrumentRef)
    assert yes.instrument.market.series_ticker == "SERIES"
    assert [(level.price, level.quantity) for level in yes.bids] == [
        (pytest.approx(0.40000000003), 12.0)
    ]
    assert [(level.price, level.quantity) for level in yes.asks] == [(0.65, 5.0)]
    assert [(level.price, level.quantity) for level in no.bids] == [(0.35, 5.0)]
    assert [(level.price, level.quantity) for level in no.asks] == [(0.6, 12.0)]
    assert yes.native_bid_count == 5
    assert yes.native_ask_count == 2
    assert yes.pre_trim_bid_count == 3
    assert yes.pre_trim_ask_count == 2
    assert yes.returned_bid_count == yes.returned_ask_count == 1
    assert yes.quantity_unit == "contracts"
    assert yes.bid_provenance == "direct"
    assert yes.ask_provenance == "complement_derived"
    assert len(yes.observations) == 2
    assert yes.observation == yes.observations[-1]
    assert all(query == "" for path, query in calls if path.endswith("/orderbook"))


async def test_book_preserves_valid_missing_and_empty_ladders() -> None:
    payload_values: list[dict[str, object]] = [
        market_envelope(),
        {"orderbook_fp": {}},
        market_envelope(),
        {"orderbook_fp": {"yes_dollars": [], "no_dollars": []}},
    ]
    payloads = iter(payload_values)
    async with AsyncKalshiClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=next(payloads))
        )
    ) as client:
        missing = await client.get_book(
            KalshiInstrumentRef(KalshiMarketRef("KXTEST"), "yes")
        )
        empty = await client.get_book(
            KalshiInstrumentRef(KalshiMarketRef("KXTEST"), "no")
        )
    for book in (missing, empty):
        assert book.bids == book.asks == ()
        assert not book.valid_state
        assert book.quality_flags == ("empty_ask", "empty_bid")
        assert book.bid_provenance == book.ask_provenance == "missing"


@pytest.mark.parametrize(
    "book",
    [
        {},
        {"orderbook_fp": None},
        {"orderbook_fp": {"yes_dollars": None}},
        {"orderbook_fp": {"yes_dollars": [[1]]}},
        {"orderbook_fp": {"yes_dollars": [["nan", "1"]]}},
        {"orderbook_fp": {"yes_dollars": [["0.2", "inf"]]}},
        {"orderbook_fp": {"yes_dollars": [["20", "1"]]}},
        {"orderbook_fp": {"yes_dollars": [["0.2", "-1"]]}},
        {"orderbook_fp": {"yes": [[1, 1]]}},
        {"orderbook_fp": {"yes_dollars_fp": [["0.2", "1"]]}},
        {
            "orderbook_fp": {
                "yes_dollars": [["0.2", "1"]],
                "yes_dollars_fp": [["nan", "bad"]],
            }
        },
    ],
)
async def test_book_rejects_malformed_or_unqualified_ladders(book: object) -> None:
    responses = iter((market_envelope(), book))
    async with AsyncKalshiClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=next(responses))
        )
    ) as client:
        with pytest.raises(InvalidDataError):
            await client.get_book(
                KalshiInstrumentRef(KalshiMarketRef("KXTEST"), "yes")
            )


@pytest.mark.parametrize("market_type", [None, "scalar"])
async def test_book_requires_same_operation_binary_capability(
    market_type: object,
) -> None:
    calls = 0
    row = market_row(market_type=market_type)
    if market_type is None:
        row.pop("market_type")

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"market": row})

    async with AsyncKalshiClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(UnsupportedCapabilityError):
            await client.get_book(
                KalshiInstrumentRef(KalshiMarketRef("KXTEST", "HINT"), "yes")
            )
    assert calls == 1


async def test_book_rejects_wrong_refs_before_io() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    client = AsyncKalshiClient(transport=httpx.MockTransport(handler))
    with pytest.raises(TypeError):
        await client.get_book(PolymarketInstrumentRef("token"))  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        await client.get_book(
            KalshiInstrumentRef(KalshiMarketRef("KXTEST"), "yes"), depth=True
        )
    assert calls == 0
    await client.close()


async def test_book_validates_series_across_capability_and_book_for_bare_ref() -> None:
    responses = iter(
        (
            market_envelope(series_ticker="SERIES-A"),
            {
                "ticker": "KXTEST",
                "series_ticker": "SERIES-B",
                "orderbook_fp": {},
            },
        )
    )
    async with AsyncKalshiClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=next(responses))
        )
    ) as client:
        with pytest.raises(InvalidDataError, match="series mismatch"):
            await client.get_book(
                KalshiInstrumentRef(KalshiMarketRef("KXTEST"), "yes")
            )


async def test_book_preserves_caller_series_when_capability_omits_it() -> None:
    responses = iter(
        (
            market_envelope(),
            {"series_ticker": "SERIES", "orderbook_fp": {}},
        )
    )
    async with AsyncKalshiClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=next(responses))
        )
    ) as client:
        book = await client.get_book(
            KalshiInstrumentRef(KalshiMarketRef("KXTEST", "SERIES"), "no")
        )
    assert isinstance(book.instrument, KalshiInstrumentRef)
    assert book.instrument.market.series_ticker == "SERIES"


@pytest.mark.parametrize(
    ("capability", "message"),
    [
        (market_envelope(ticker="OTHER"), "ticker mismatch"),
        (market_envelope(series_ticker="OTHER"), "series mismatch"),
    ],
)
async def test_book_rejects_capability_identity_contradictions_before_book(
    capability: dict[str, object], message: str
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=capability)

    async with AsyncKalshiClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(InvalidDataError, match=message):
            await client.get_book(
                KalshiInstrumentRef(KalshiMarketRef("KXTEST", "SERIES"), "yes")
            )
    assert calls == 1


async def test_capability_and_book_404_scopes_are_distinct() -> None:
    paths: list[str] = []

    def capability_missing(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(404, json={"error": "missing"})

    client = AsyncKalshiClient(transport=httpx.MockTransport(capability_missing))
    with pytest.raises(MarketNotFoundError) as capability_error:
        await client.get_book(
            KalshiInstrumentRef(KalshiMarketRef("KXTEST"), "yes")
        )
    assert "standard live market detail" in capability_error.value.lookup_scope
    assert len(paths) == 1 and not paths[0].endswith("/orderbook")
    await client.close()

    paths.clear()

    def book_missing(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path.endswith("/orderbook"):
            return httpx.Response(404, json={"error": "missing"})
        return httpx.Response(200, json=market_envelope())

    client = AsyncKalshiClient(transport=httpx.MockTransport(book_missing))
    with pytest.raises(MarketNotFoundError) as book_error:
        await client.get_book(
            KalshiInstrumentRef(KalshiMarketRef("KXTEST"), "yes")
        )
    assert book_error.value.lookup_scope == "Kalshi current order book"
    assert len(paths) == 2 and paths[-1].endswith("/orderbook")
    await client.close()


@pytest.mark.parametrize("deadline", [None, True, 0, -1, math.inf, math.nan])
async def test_workflow_deadlines_validate_before_io(deadline: object) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    client = AsyncKalshiClient(transport=httpx.MockTransport(handler))
    expected = TypeError if deadline is None or isinstance(deadline, bool) else ValueError
    with pytest.raises(expected):
        await client.discover_markets(
            filters=KalshiFilter(), deadline_s=deadline  # type: ignore[arg-type]
        )
    with pytest.raises(expected):
        await client.get_market(ticker="KXTEST", deadline_s=deadline)  # type: ignore[arg-type]
    with pytest.raises(expected):
        await client.get_book(
            KalshiInstrumentRef(KalshiMarketRef("KXTEST"), "yes"),
            deadline_s=deadline,  # type: ignore[arg-type]
        )
    assert calls == 0
    await client.close()


async def test_book_expiry_during_normalization_leaves_client_reusable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 0.0

    def clock() -> float:
        return now

    original_bounded = OperationExpiry.bounded
    monkeypatch.setattr(
        client_module.OperationExpiry,
        "bounded",
        classmethod(lambda cls, timeout_s: original_bounded(timeout_s, clock=clock)),
    )
    original_finite_number = workflow_module._finite_number
    parsed = 0

    def finite_number_spy(value: object, label: str) -> float:
        nonlocal now, parsed
        parsed += 1
        result = original_finite_number(value, label)
        if parsed == 600:
            now = 2.0
        return result

    monkeypatch.setattr(workflow_module, "_finite_number", finite_number_spy)
    huge_book = {
        "orderbook_fp": {
            "yes_dollars": [["0.4", "1"]] * 5000,
            "no_dollars": [["0.3", "1"]] * 5000,
        }
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/orderbook"):
            return httpx.Response(200, json=huge_book)
        return httpx.Response(200, json=market_envelope())

    client = AsyncKalshiClient(transport=httpx.MockTransport(handler))
    with pytest.raises(OperationTimeoutError, match="operation expired"):
        await client.get_book(
            KalshiInstrumentRef(KalshiMarketRef("KXTEST"), "yes"),
            deadline_s=1.0,
        )
    assert 600 <= parsed < 20_000
    assert (await client.market("KXTEST"))["ticker"] == "KXTEST"
    await client.close()


async def test_book_expiry_after_capability_normalization_precedes_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 0.0

    def clock() -> float:
        return now

    original_bounded = OperationExpiry.bounded
    monkeypatch.setattr(
        client_module.OperationExpiry,
        "bounded",
        classmethod(lambda cls, timeout_s: original_bounded(timeout_s, clock=clock)),
    )
    original_normalize = client_module.normalize_kalshi_workflow_market

    def expire_after_capability(*args: object, **kwargs: object):
        nonlocal now
        result = original_normalize(*args, **kwargs)  # type: ignore[arg-type]
        now = 2.0
        return result

    monkeypatch.setattr(
        client_module, "normalize_kalshi_workflow_market", expire_after_capability
    )
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        row = market_row(market_type="scalar")
        return httpx.Response(200, json={"market": row})

    client = AsyncKalshiClient(transport=httpx.MockTransport(handler))
    with pytest.raises(OperationTimeoutError, match="operation expired"):
        await client.get_book(
            KalshiInstrumentRef(KalshiMarketRef("KXTEST"), "yes"), deadline_s=1.0
        )
    assert calls == 1
    await client.close()


async def test_cancellation_during_book_transport_drains_and_reuses_client() -> None:
    entered = asyncio.Event()
    drained = asyncio.Event()
    release = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/orderbook"):
            entered.set()
            try:
                await release.wait()
            finally:
                await asyncio.sleep(0)
                drained.set()
            return httpx.Response(200, json={"orderbook_fp": {}})
        return httpx.Response(200, json=market_envelope())

    client = AsyncKalshiClient(transport=httpx.MockTransport(handler))
    await client.__aenter__()
    task = asyncio.create_task(
        client.get_book(KalshiInstrumentRef(KalshiMarketRef("KXTEST"), "yes"))
    )
    await asyncio.wait_for(entered.wait(), timeout=2.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert drained.is_set()
    assert (await client.market("KXTEST"))["ticker"] == "KXTEST"
    await client.close()


async def test_expiry_during_book_transport_drains_and_reuses_client() -> None:
    entered = asyncio.Event()
    drained = asyncio.Event()
    release = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/orderbook"):
            entered.set()
            try:
                await release.wait()
            finally:
                await asyncio.sleep(0)
                drained.set()
            return httpx.Response(200, json={"orderbook_fp": {}})
        return httpx.Response(200, json=market_envelope())

    client = AsyncKalshiClient(transport=httpx.MockTransport(handler))
    await client.__aenter__()
    task = asyncio.create_task(
        client.get_book(
            KalshiInstrumentRef(KalshiMarketRef("KXTEST"), "yes"),
            deadline_s=0.5,
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=2.0)
    with pytest.raises(OperationTimeoutError, match="operation expired"):
        await asyncio.wait_for(task, timeout=2.0)
    assert drained.is_set()
    assert (await client.market("KXTEST"))["ticker"] == "KXTEST"
    await client.close()
