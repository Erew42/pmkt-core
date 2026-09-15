from pmkt.records import PolymarketMarketRef
import httpx
import pytest

import pmkt.exchanges.polymarket.gamma as gamma_module
from pmkt.runtime import RequestPolicy
from pmkt.runtime import OperationExpiry
from pmkt.errors import InvalidDataError, MarketNotFoundError, OperationTimeoutError
from pmkt.exchanges.polymarket.gamma import AsyncGammaClient
from pmkt.records import PolymarketFilter


pytestmark = pytest.mark.asyncio


def market_row(
    market_id: str = "1",
    *,
    condition_id: str | None = "0xcondition",
    question: str | None = "Will it rain?",
    outcomes: object = '["Yes", "No"]',
    tokens: object = '["yes-token", "no-token"]',
    closed: bool | None = False,
) -> dict[str, object]:
    row: dict[str, object] = {
        "id": market_id,
        "outcomes": outcomes,
        "clobTokenIds": tokens,
    }
    if condition_id is not None:
        row["conditionId"] = condition_id
    if question is not None:
        row["question"] = question
    if closed is not None:
        row["closed"] = closed
    return row


async def test_get_market_strict_mapping_prices_and_defensive_native_copy() -> None:
    payload = market_row()
    payload["outcomePrices"] = '["0.4", "not-a-price"]'
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=payload))

    async with AsyncGammaClient(transport=transport) as client:
        market = await client.get_market(market=PolymarketMarketRef("1"))

    assert market.mapping_status == "mapped"
    assert market.outcome_prices is None
    assert [instrument.token_id for instrument in market.instruments] == [
        "yes-token",
        "no-token",
    ]
    assert market.instrument_for_label("Yes") == market.instruments[0]
    assert {issue.code for issue in market.issues} == {"invalid_outcome_prices"}
    assert market.observation.response_identities == (
        "market_id=1",
        "condition_id=0xcondition",
    )
    payload["question"] = "changed outside"
    market.native_payload["question"] = "changed attachment"
    assert market.question == "Will it rain?"


@pytest.mark.parametrize(
    ("outcomes", "tokens", "expected"),
    [
        ([], [], "empty"),
        (None, ["a"], "unknown"),
        (["Yes"], ["a", "b"], "inconsistent"),
        (["Yes", "No"], ["a", "a"], "inconsistent"),
        (None, ["a", "a"], "inconsistent"),
        ("Yes,No", '["a", "b"]', "inconsistent"),
        ({"Yes": 1}, ["a"], "inconsistent"),
        ([["Yes"]], ["a"], "inconsistent"),
        ([""], ["a"], "inconsistent"),
        (["Yes"], [1], "inconsistent"),
    ],
)
async def test_get_market_mapping_states(
    outcomes: object, tokens: object, expected: str
) -> None:
    payload = market_row(outcomes=outcomes, tokens=tokens)
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    async with AsyncGammaClient(transport=transport) as client:
        market = await client.get_market(market=PolymarketMarketRef("1"))
    assert market.mapping_status == expected
    if expected != "mapped":
        assert market.instruments == ()


async def test_mapping_aliases_are_semantic_and_duplicate_labels_are_ambiguous() -> None:
    payload = market_row(outcomes=["Yes", "Yes"], tokens='["a", "b"]')
    payload["clob_token_ids"] = ["a", "b"]
    payload["outcomePrices"] = '["0.2", "0.8"]'
    payload["outcome_prices"] = ["0.2", "0.8"]
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    async with AsyncGammaClient(transport=transport) as client:
        market = await client.get_market(market=PolymarketMarketRef("1"))
    assert market.mapping_status == "mapped"
    assert market.outcome_prices == (0.2, 0.8)
    with pytest.raises(ValueError, match="ambiguous"):
        market.instrument_for_label("Yes")
    with pytest.raises(KeyError):
        market.instrument_for_label("yes")


@pytest.mark.parametrize("capability", [(True, False), ("yes", None)])
async def test_bad_book_capability_evidence_is_not_promoted(
    capability: tuple[object, object | None],
) -> None:
    payload = market_row()
    payload["enableOrderBook"] = capability[0]
    if capability[1] is not None:
        payload["enable_order_book"] = capability[1]
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    async with AsyncGammaClient(transport=transport) as client:
        market = await client.get_market(market=PolymarketMarketRef("1"))
    assert market.mapping_status == "mapped"
    assert not market.book_supported
    assert {issue.code for issue in market.issues} == {"invalid_book_capability"}


async def test_get_market_encodes_id_and_scopes_identity_and_404_failures() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.raw_path.decode())
        if request.url.path.endswith("/missing"):
            return httpx.Response(404, json={"error": "not found"})
        return httpx.Response(200, json=market_row("other"))

    async with AsyncGammaClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(InvalidDataError, match="identity mismatch"):
            await client.get_market(market=PolymarketMarketRef("wanted/segment?x=1"))
        with pytest.raises(MarketNotFoundError) as caught:
            await client.get_market(market=PolymarketMarketRef("missing"))
    assert seen[0] == "/markets/wanted%2Fsegment%3Fx%3D1"
    assert caught.value.lookup_scope == "Gamma current market detail"


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
    policy = RequestPolicy(max_attempts=20, backoff_base_s=5.0, backoff_max_s=60.0)
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


async def test_discovery_alternates_lifecycle_and_continues_live_partition() -> None:
    seen: list[tuple[str | None, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("after_cursor")
        closed = request.url.params["closed"]
        seen.append((cursor, closed))
        if closed == "true":
            return httpx.Response(200, json={"markets": []})
        if cursor is None:
            return httpx.Response(
                200, json={"markets": [market_row("1")], "next_cursor": "c1"}
            )
        return httpx.Response(200, json={"markets": [market_row("2")]})

    async with AsyncGammaClient(transport=httpx.MockTransport(handler)) as client:
        result = await client.discover_markets(max_markets=10, max_pages=5)

    assert [market.ref.market_id for market in result.items] == ["1", "2"]
    assert seen == [(None, "false"), (None, "true"), ("c1", "false")]
    assert result.report.stop_reason == "source_exhausted"
    assert result.report.pages_fetched == 3
    assert result.report.traversal_complete


async def test_discovery_target_chunks_wire_filters_and_rejects_mismatches() -> None:
    condition_ids = tuple(f"c{i}" for i in range(21))
    seen: list[tuple[list[str], list[str], str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(
            (
                request.url.params.get_list("condition_ids"),
                request.url.params.get_list("tag_id"),
                request.url.params["closed"],
            )
        )
        return httpx.Response(
            200, json={"markets": [market_row(condition_id="wrong")]}
        )

    filters = PolymarketFilter(
        condition_ids=condition_ids, tag_id="123", related_tags=True
    )
    async with AsyncGammaClient(transport=httpx.MockTransport(handler)) as client:
        result = await client.discover_markets(filters=filters, max_pages=4)

    assert result.items == ()
    assert [len(entry[0]) for entry in seen] == [20, 20, 1, 1]
    assert [entry[2] for entry in seen] == ["false", "true", "false", "true"]
    assert all(entry[1] == ["123"] for entry in seen)
    assert result.report.requested_selector_count == 21
    assert result.report.queried_chunks == 2


async def test_discovery_preserves_opaque_whitespace_cursor() -> None:
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("after_cursor")
        seen.append(cursor)
        if cursor is None:
            return httpx.Response(
                200, json={"markets": [], "next_cursor": " c 1 "}
            )
        return httpx.Response(200, json={"markets": []})

    async with AsyncGammaClient(transport=httpx.MockTransport(handler)) as client:
        result = await client.discover_markets(
            filters=PolymarketFilter(closed=False), max_pages=2
        )
    assert seen == [None, " c 1 "]
    assert result.report.pages_fetched == 2


async def test_discovery_repeated_cursor_raises_at_result_limit_boundary() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("after_cursor") is None:
            return httpx.Response(
                200,
                json={
                    "markets": [market_row(question="does not match")],
                    "next_cursor": "cycle",
                },
            )
        return httpx.Response(
            200,
            json={
                "markets": [market_row("2", question="target")],
                "next_cursor": "cycle",
            },
        )

    async with AsyncGammaClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(InvalidDataError, match="repeated cursor"):
            await client.discover_markets(
                filters=PolymarketFilter(
                    closed=False, question_contains="target"
                ),
                max_markets=1,
            )


async def test_discovery_first_observed_duplicate_wins_before_filtering() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["closed"] == "false":
            return httpx.Response(
                200, json={"markets": [market_row(question="no")]}
            )
        return httpx.Response(
            200, json={"markets": [market_row(question="target")]}
        )

    async with AsyncGammaClient(transport=httpx.MockTransport(handler)) as client:
        result = await client.discover_markets(
            filters=PolymarketFilter(question_contains="target"), max_pages=2
        )
    assert result.items == ()
    assert result.report.rows_scanned == 2
    assert result.report.unique_markets_seen == 1
    assert result.report.duplicates_seen == 1


async def test_discovery_unknown_question_is_counted_not_fatal() -> None:
    payload = market_row(question=None)
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json={"markets": [payload]})
    )
    async with AsyncGammaClient(transport=transport) as client:
        result = await client.discover_markets(
            filters=PolymarketFilter(closed=False, question_contains="target")
        )
    assert result.items == ()
    assert dict(result.report.unknown_filter_counts) == {"question_contains": 1}


async def test_discovery_empty_selection_and_invalid_inputs_do_no_io() -> None:
    requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, json={"markets": []})

    client = AsyncGammaClient(transport=httpx.MockTransport(handler))
    result = await client.discover_markets(
        filters=PolymarketFilter(condition_ids=()), max_markets=1
    )
    assert result.report.stop_reason == "empty_selection"
    assert requests == 0
    with pytest.raises(TypeError):
        await client.discover_markets(max_pages=True)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        await client.discover_markets(filters=object())  # type: ignore[arg-type]
    assert requests == 0
    await client.close()


@pytest.mark.parametrize("deadline", [None, True, 0, -1, float("nan")])
async def test_new_gamma_workflows_require_bounded_deadlines_before_io(
    deadline: object,
) -> None:
    requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, json={"markets": []})

    client = AsyncGammaClient(transport=httpx.MockTransport(handler))
    expected = TypeError if deadline is None or isinstance(deadline, bool) else ValueError
    with pytest.raises(expected):
        await client.discover_markets(  # type: ignore[arg-type]
            filters=PolymarketFilter(condition_ids=()), deadline_s=deadline
        )
    with pytest.raises(expected):
        await client.get_market(market=PolymarketMarketRef("1"), deadline_s=deadline)  # type: ignore[arg-type]
    assert requests == 0
    await client.close()


async def test_discovery_result_limit_counts_only_inspected_terminal_rows() -> None:
    payload = {"markets": [market_row("1"), market_row("2"), market_row("3")]}
    async with AsyncGammaClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=payload)
        )
    ) as client:
        result = await client.discover_markets(
            filters=PolymarketFilter(closed=False), max_markets=2
        )
    assert [market.ref.market_id for market in result.items] == ["1", "2"]
    assert result.report.stop_reason == "result_limit"
    assert result.report.rows_scanned == 2
    assert result.report.unique_markets_seen == 2
    assert not result.report.traversal_complete


async def test_discovery_page_budget_is_global_across_chunks_and_partitions() -> None:
    condition_ids = tuple(f"c{i}" for i in range(21))
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(
            (request.url.params.get_list("condition_ids")[0], request.url.params["closed"])
        )
        return httpx.Response(200, json={"markets": [], "next_cursor": "more"})

    async with AsyncGammaClient(
        transport=httpx.MockTransport(handler)
    ) as client:
        result = await client.discover_markets(
            filters=PolymarketFilter(condition_ids=condition_ids), max_pages=3
        )
    assert seen == [("c0", "false"), ("c0", "true"), ("c20", "false")]
    assert result.report.stop_reason == "page_limit"
    assert result.report.pages_fetched == 3
    assert result.report.queried_chunks == 2
    assert not result.report.traversal_complete


async def test_mapping_predicates_do_not_fabricate_unknown_instruments() -> None:
    rows = [
        market_row("empty", outcomes=[], tokens=[]),
        market_row("unknown", outcomes=None, tokens=None),
        market_row("bad", outcomes=["Yes"], tokens=["a", "b"]),
    ]
    async with AsyncGammaClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"markets": rows})
        )
    ) as client:
        result = await client.discover_markets(
            filters=PolymarketFilter(closed=False, has_instruments=False)
        )
    assert [market.ref.market_id for market in result.items] == ["empty"]
    assert dict(result.report.unknown_filter_counts) == {"has_instruments": 2}


async def test_outcome_count_uses_labels_without_tokens_and_text_uses_casefold() -> None:
    row = market_row(question="Straße", outcomes=["Ja", "Nein"], tokens=None)
    async with AsyncGammaClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"markets": [row]})
        )
    ) as client:
        result = await client.discover_markets(
            filters=PolymarketFilter(
                closed=False, question_contains="STRASSE", outcome_count=2
            )
        )
    assert [market.ref.market_id for market in result.items] == ["1"]
    assert result.items[0].mapping_status == "unknown"


async def test_discovery_aggregates_more_than_twenty_traceable_issues() -> None:
    rows = [
        market_row(str(index), outcomes=["Yes"], tokens=["a", "b"])
        for index in range(25)
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"markets": rows if request.url.params["closed"] == "false" else []},
        )

    async with AsyncGammaClient(transport=httpx.MockTransport(handler)) as client:
        result = await client.discover_markets(max_markets=100)
    issue = next(
        issue for issue in result.report.issues if issue.code == "inconsistent_mapping"
    )
    assert issue.occurrence_count == 25
    assert len(issue.examples) == 20
    assert all("request_id=" in example and "market_id=" in example for example in issue.examples)


async def test_discovery_expiry_returns_no_partial_result_and_client_is_reusable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 0.0

    def clock() -> float:
        return now

    original_bounded = OperationExpiry.bounded
    monkeypatch.setattr(
        gamma_module.OperationExpiry,
        "bounded",
        classmethod(lambda cls, timeout_s: original_bounded(timeout_s, clock=clock)),
    )
    original_normalize = gamma_module.normalize_gamma_market

    def expire_during_normalization(*args, **kwargs):
        nonlocal now
        result = original_normalize(*args, **kwargs)
        now = 2.0
        return result

    monkeypatch.setattr(gamma_module, "normalize_gamma_market", expire_during_normalization)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/markets":
            return httpx.Response(200, json=[])
        return httpx.Response(200, json={"markets": [market_row()]})

    client = AsyncGammaClient(transport=httpx.MockTransport(handler))
    with pytest.raises(OperationTimeoutError, match="operation expired"):
        await client.discover_markets(
            filters=PolymarketFilter(closed=False), deadline_s=1.0
        )
    assert await client.markets_raw_page(limit=1) == []
    await client.close()


async def test_contradictory_identity_and_token_aliases_fail_or_degrade() -> None:
    identity_payload = market_row()
    identity_payload["market_id"] = "different"
    token_payload = market_row()
    token_payload["clob_token_ids"] = ["different", "tokens"]
    responses = iter((identity_payload, token_payload))
    async with AsyncGammaClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=next(responses))
        )
    ) as client:
        with pytest.raises(InvalidDataError, match="conflicting market ID"):
            await client.get_market(market=PolymarketMarketRef("1"))
        market = await client.get_market(market=PolymarketMarketRef("1"))
    assert market.mapping_status == "inconsistent"
    assert market.instruments == ()


@pytest.mark.parametrize(
    "requested,returned", [("0xABCDEF", "0xabcdef"), ("0xabcdef", "0xABCDEF")]
)
async def test_discovery_condition_ids_match_without_case_changes_to_evidence(
    requested, returned
):
    def handler(request):
        assert request.url.params.get_list("condition_ids") == [requested]
        return httpx.Response(
            200, json={"markets": [market_row(condition_id=returned)]}
        )

    async with AsyncGammaClient(transport=httpx.MockTransport(handler)) as client:
        result = await client.discover_markets(
            filters=PolymarketFilter(condition_ids=(requested,), closed=False)
        )
    assert len(result.items) == 1
    assert result.items[0].ref.condition_id == returned
    assert f"condition_id={returned}" in result.items[0].observation.response_identities
