from decimal import Decimal
import asyncio

import httpx
import pytest

from pmkt.errors import InvalidDataError
from pmkt.runtime import RequestPolicy
from pmkt._observations import classify_request_source, source_after_response
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
        "current_size": 0 if status == "CLOSED" else 4,
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


def _activity_trade(wallet: str) -> dict:
    return {**_trade(wallet), "type": "TRADE", "usdc_size": 1}


async def test_open_interest_page_preserves_raw_payload_and_comma_encoding() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        return httpx.Response(200, json={"data": [{"condition_id": "0xa", "value": 12.5}]})

    async with AsyncPolymarketDataClient(
        base_url="https://example.com", transport=httpx.MockTransport(handler)
    ) as client:
        payload = await client.open_interest_page(["0xa", "0xb"])

    assert seen == [("/v2/oi", {"condition": "0xa,0xb"})]
    assert payload == [{"condition_id": "0xa", "value": 12.5}]


async def test_open_interest_normalizer_retains_saved_v1_source_omissions() -> None:
    result = normalize_polymarket_open_interest(
        ["0xa", "0xb"], [{"market": "0xa", "value": "12.50"}]
    )
    assert result.values == {"0xa": Decimal("12.50")}
    assert result.omitted_keys == ("0xb",)
    assert result.coverage_rate == 0.5
    assert result.value_coverage_complete is False


@pytest.mark.parametrize("count", range(21, 26))
async def test_open_interest_normalizer_accepts_saved_v1_batches(count: int) -> None:
    ids = [f"0x{index}" for index in range(count)]
    rows = [{"market": key, "value": str(index)} for index, key in enumerate(ids[:-1])]
    result = normalize_polymarket_open_interest(ids + ids, rows)
    assert result.requested_keys == tuple(ids)
    assert result.values == {key: Decimal(index) for index, key in enumerate(ids[:-1])}
    assert result.omitted_keys == (ids[-1],)


@pytest.mark.parametrize("envelope", [False, True])
async def test_open_interest_v2_distinguishes_zero_from_omitted_conditions(envelope) -> None:
    rows = [{"condition_id": "0xa", "value": 0}, {"condition_id": "0xb", "value": "12.50"}]
    result = normalize_polymarket_open_interest(
        ["0xa", "0xb", "0xc"], {"data": rows} if envelope else rows
    )
    assert result.values == {"0xa": Decimal("0"), "0xb": Decimal("12.50")}
    assert result.omitted_keys == ("0xc",)
    assert result.response_keys == ("0xa", "0xb")


@pytest.mark.parametrize("payload", [[], {}, {"data": None}, {"data": {}}, {"data": [None]}])
async def test_open_interest_client_rejects_malformed_v2_envelopes(payload) -> None:
    async with AsyncPolymarketDataClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload)),
    ) as client:
        with pytest.raises(TypeError, match="/v2/oi"):
            await client.open_interest_page(["0xa"])


async def test_open_interest_empty_v2_data_preserves_all_omissions() -> None:
    async with AsyncPolymarketDataClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"data": []})),
    ) as client:
        rows = await client.open_interest_page(["0xa"])
    result = normalize_polymarket_open_interest(["0xa"], rows)
    assert result.values == {}
    assert result.omitted_keys == ("0xa",)


@pytest.mark.parametrize(
    "row,match",
    [
        ({"condition_id": "0xa", "market": "0xb", "value": 1}, "Conflicting"),
        ({"condition_id": "0xa", "value": None}, "Invalid"),
        ({"condition_id": "0xa", "value": True}, "Invalid"),
        ({"condition_id": "0xa", "value": "Infinity"}, "Invalid"),
        ({"condition_id": "0xa", "value": -1}, "Invalid"),
        ({"condition_id": "0xb", "value": 1}, "Unexpected"),
        ({"value": 1}, "lacks condition_id"),
    ],
)
async def test_open_interest_normalizer_rejects_invalid_v2_rows(row, match) -> None:
    with pytest.raises(ValueError, match=match):
        normalize_polymarket_open_interest(["0xa"], {"data": [row]})


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


async def test_open_interest_client_enforces_20_distinct_key_limit_before_io() -> None:
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"data": []})

    client = AsyncPolymarketDataClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(ValueError, match="at most 20"):
        await client.open_interest_page([f"0x{index}" for index in range(21)])
    assert requests == []
    ids = [f"0x{index}" for index in range(20)]
    await client.open_interest_page(ids + ids)
    assert requests[0].url.params["condition"] == ",".join(ids)
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
                "data": [_position(WALLET_A, status="REDEEMABLE")],
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
    redeemed = result.participants[0].current_positions[0]
    assert redeemed.status == "REDEEMABLE"
    assert redeemed.current_size == Decimal("4")
    observations = result.provenance.observations
    assert len(observations) == 3
    assert redeemed.request_id == observations[0].request_id
    assert dict(observations[0].effective_parameters)["status"] == "OPEN"
    assert dict(observations[1].effective_parameters)["cursor"] == "next-open"


async def test_wallet_history_reads_full_trades_and_both_position_statuses() -> None:
    seen: list[tuple[str, str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        seen.append((request.url.path, params.get("status", ""), params.get("cursor")))
        assert params["user"] == WALLET_A
        if request.url.path == "/v2/trades":
            assert params["start"] == "1"
            assert "full_history" not in params
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
    observations = result.provenance.observations
    assert len(observations) == 3
    assert [observation.endpoint_template for observation in observations] == [
        "/v2/trades", "/v2/positions", "/v2/positions",
    ]
    assert result.trades[0].request_id == observations[0].request_id
    assert result.current_positions[0].request_id == observations[1].request_id
    assert result.past_positions[0].request_id == observations[2].request_id
    params = dict(observations[0].effective_parameters)
    assert params["start"] == "1"
    assert params["taker_only"] == "false"
    assert "cursor" not in params
    assert result.provenance.raw_responses == ()
    for observation in observations:
        assert observation.outcome == "success"
        assert observation.status_code == 200
        assert observation.attempt_count == 1
        assert observation.origin == "https://data.example"
        assert observation.transport_origin == "caller_supplied"
        assert observation.data_scope == "unknown"
        assert observation.received_at_utc is not None
        assert result.started_at_utc <= observation.started_at_utc
        assert observation.started_at_utc <= observation.received_at_utc <= result.completed_at_utc


async def test_holders_page_preserves_groups_basis_cursor_and_observation() -> None:
    seen: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v2/holders"
        seen.append(dict(request.url.params))
        return httpx.Response(200, json={
            "data": [{"token_id": "123", "holders": [{
                "proxy_wallet": WALLET_A, "token_id": "123", "outcome_index": 0,
                "amount": 0.016591, "avg_price": 0.25, "entry_cost_usdc": 1,
            }]}, {"token_id": "456", "holders": []}],
            "pagination": {"has_more": True, "next_cursor": "next-holders"},
        })

    async with AsyncPolymarketDataClient(
        base_url="https://data.example", transport=httpx.MockTransport(handler)
    ) as client:
        result = await client.holders_page(
            condition_id=CONDITION, include_pnl=True, page_size=100,
        )

    assert result.balance_basis == "GROSS"
    assert result.next_cursor == "next-holders"
    assert [group.token_id for group in result.groups] == ["123", "456"]
    assert result.groups[0].holders[0].amount == Decimal("0.016591")
    assert result.groups[0].holders[0].request_id == result.observation.request_id
    assert seen == [{"condition": CONDITION, "include_pnl": "true",
                     "min_balance": "0", "limit": "100"}]


async def test_market_trades_page_is_maker_inclusive_and_condition_scoped() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v2/trades"
        assert dict(request.url.params) == {
            "condition": CONDITION, "taker_only": "false",
            "filter_type": "TOKENS", "filter_amount": "0.01", "limit": "10",
        }
        return httpx.Response(200, json={
            "data": [_trade(WALLET_A), _trade(WALLET_B)],
            "pagination": {"has_more": False, "next_cursor": None},
        })

    async with AsyncPolymarketDataClient(
        base_url="https://data.example", transport=httpx.MockTransport(handler)
    ) as client:
        result = await client.market_trades_page(condition_id=CONDITION, page_size=10)

    assert {trade.wallet for trade in result.trades} == {WALLET_A, WALLET_B}
    assert all(trade.request_id == result.observation.request_id for trade in result.trades)
    assert result.source_window == "fixed_three_years"
    assert result.minimum_size_shares == Decimal("0.01")


async def test_wallet_trade_preserves_short_source_condition_id() -> None:
    short_condition = "0x" + "a" * 62

    def handler(request: httpx.Request) -> httpx.Response:
        row = _trade(WALLET_A)
        row["condition_id"] = short_condition
        return httpx.Response(200, json={
            "data": [row], "pagination": {"has_more": False},
        })

    async with AsyncPolymarketDataClient(
        transport=httpx.MockTransport(handler)
    ) as client:
        wallet_trades, cursor = await client.trades_page(wallet=WALLET_A)
        with pytest.raises(InvalidDataError, match="condition differs"):
            await client.market_trades_page(condition_id=CONDITION)

    assert cursor is None
    assert wallet_trades[0].condition_id == short_condition


async def test_activity_page_requests_full_history_and_preserves_unknown_type() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v2/activity"
        assert dict(request.url.params) == {
            "user": WALLET_A, "condition": CONDITION, "start": "1",
            "exclude_deposits_withdrawals": "true", "limit": "20",
        }
        return httpx.Response(200, json={
            "data": [{
                "proxy_wallet": WALLET_A, "condition_id": CONDITION,
                "token_id": "123", "type": "NEW_EVENT", "side": "", "size": 2,
                "usdc_size": 0, "price": 0, "timestamp": 1782752879,
                "transaction_hash": "0x" + "f" * 64,
            }],
            "pagination": {"has_more": False, "next_cursor": None},
        })

    async with AsyncPolymarketDataClient(
        base_url="https://data.example", transport=httpx.MockTransport(handler)
    ) as client:
        result = await client.activity_page(
            wallet=WALLET_A, condition_id=CONDITION, page_size=20,
        )

    assert result.activities[0].event_type == "NEW_EVENT"
    assert result.activities[0].size == Decimal("2")
    assert result.activities[0].request_id == result.observation.request_id


@pytest.mark.parametrize("updates", [
    {"condition_id": ""}, {"token_id": ""}, {"side": ""},
    {"side": None}, {"side": "HOLD"}, {"size": 0},
    {"price": -0.1}, {"price": 1.1},
])
async def test_activity_trade_rejects_malformed_fill_fields(updates) -> None:
    row = _activity_trade(WALLET_A)
    row.update(updates)
    async with AsyncPolymarketDataClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={
            "data": [row], "pagination": {"has_more": False},
        }))
    ) as client:
        with pytest.raises(InvalidDataError):
            await client.activity_page(wallet=WALLET_A)


@pytest.mark.parametrize("feed", ["holders", "trades", "activity"])
async def test_participant_page_reads_resume_opaque_cursor(feed) -> None:
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("cursor")
        seen.append(cursor)
        assert request.url.path == f"/v2/{feed}"
        assert request.url.params["limit"] == "2"
        if cursor is None:
            return httpx.Response(200, json={
                "data": [], "pagination": {"has_more": True, "next_cursor": "opaque+token"},
            })
        assert cursor == "opaque+token"
        row = (
            {"token_id": "123", "holders": [{
                "proxy_wallet": WALLET_A, "token_id": "123",
                "outcome_index": 0, "amount": 1,
            }]}
            if feed == "holders" else
            _trade(WALLET_A) if feed == "trades" else _activity_trade(WALLET_A)
        )
        return httpx.Response(200, json={
            "data": [row], "pagination": {"has_more": False},
        })

    async with AsyncPolymarketDataClient(
        transport=httpx.MockTransport(handler)
    ) as client:
        if feed == "holders":
            first = await client.holders_page(condition_id=CONDITION, page_size=2)
            second = await client.holders_page(
                condition_id=CONDITION, page_size=2, cursor=first.next_cursor,
            )
            assert second.groups[0].holders[0].wallet == WALLET_A
        elif feed == "trades":
            first = await client.market_trades_page(condition_id=CONDITION, page_size=2)
            second = await client.market_trades_page(
                condition_id=CONDITION, page_size=2, cursor=first.next_cursor,
            )
            assert second.trades[0].wallet == WALLET_A
        else:
            first = await client.activity_page(wallet=WALLET_A, page_size=2)
            second = await client.activity_page(
                wallet=WALLET_A, page_size=2, cursor=first.next_cursor,
            )
            assert second.activities[0].wallet == WALLET_A

    assert seen == [None, "opaque+token"]
    assert first.next_cursor == "opaque+token"
    assert second.next_cursor is None
    assert dict(second.observation.effective_parameters)["cursor"] == "opaque+token"


async def test_activity_page_allows_empty_token_for_merge() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "data": [{
                "proxy_wallet": WALLET_A, "condition_id": CONDITION,
                "token_id": "", "type": "MERGE", "side": "", "size": 2,
                "usdc_size": 2, "price": 0, "timestamp": 1782752879,
                "transaction_hash": "0x" + "f" * 64,
            }],
            "pagination": {"has_more": False, "next_cursor": None},
        })

    async with AsyncPolymarketDataClient(
        transport=httpx.MockTransport(handler)
    ) as client:
        result = await client.activity_page(wallet=WALLET_A)

    assert result.activities[0].token_id == ""
    assert result.activities[0].event_type == "MERGE"


async def test_wallet_activity_keeps_cash_flow_rows_without_market() -> None:
    # Live wallet pages mix REWARD/YIELD/rebate rows with blank market IDs into
    # ordinary history; rejecting them made most wallet-wide pages unreadable.
    reward = {
        "proxy_wallet": WALLET_A, "condition_id": "", "token_id": "",
        "type": "REWARD", "side": "", "size": 30.29, "usdc_size": 30.29,
        "price": 0, "timestamp": 1782752879, "outcome_index": 999,
        "transaction_hash": "0x" + "e" * 64,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "data": [reward], "pagination": {"has_more": False, "next_cursor": None},
        })

    async with AsyncPolymarketDataClient(
        transport=httpx.MockTransport(handler)
    ) as client:
        result = await client.activity_page(wallet=WALLET_A)
        with pytest.raises(InvalidDataError):
            await client.activity_page(wallet=WALLET_A, condition_id=CONDITION)

    assert result.activities[0].event_type == "REWARD"
    assert result.activities[0].condition_id == ""
    assert result.activities[0].usdc_size == Decimal("30.29")


async def test_activity_page_preserves_optional_combo_flag() -> None:
    trade = {
        "proxy_wallet": WALLET_A, "condition_id": CONDITION,
        "token_id": "123", "type": "TRADE", "side": "BUY", "size": 2,
        "usdc_size": 1, "price": 0.5, "timestamp": 1782752879,
        "transaction_hash": "0x" + "f" * 64,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "data": [
                {**trade, "is_combo": True},
                {**trade, "is_combo": False},
                trade,
            ],
            "pagination": {"has_more": False, "next_cursor": None},
        })

    async with AsyncPolymarketDataClient(
        transport=httpx.MockTransport(handler)
    ) as client:
        page = await client.activity_page(wallet=WALLET_A)

    assert tuple(row.is_combo for row in page.activities) == (True, False, None)


async def test_activity_page_rejects_non_boolean_combo_flag() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "data": [{
                "proxy_wallet": WALLET_A, "condition_id": CONDITION,
                "token_id": "123", "type": "TRADE", "side": "BUY", "size": 2,
                "usdc_size": 1, "price": 0.5, "timestamp": 1782752879,
                "transaction_hash": "0x" + "f" * 64, "is_combo": "true",
            }],
            "pagination": {"has_more": False, "next_cursor": None},
        })

    async with AsyncPolymarketDataClient(
        transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(InvalidDataError, match="is_combo"):
            await client.activity_page(wallet=WALLET_A)


async def test_activity_page_accepts_combo_condition_id() -> None:
    combo_condition = (
        "0x0358aedb06b9a9095ab2b216cf8ae8961f0000000000000000000000000000"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["condition"] == combo_condition
        return httpx.Response(200, json={
            "data": [{
                "proxy_wallet": WALLET_A, "condition_id": combo_condition,
                "token_id": "123", "type": "TRADE", "side": "BUY", "size": 2,
                "usdc_size": 1, "price": 0.5, "timestamp": 1782752879,
                "transaction_hash": "0x" + "f" * 64, "is_combo": True,
            }],
            "pagination": {"has_more": False, "next_cursor": None},
        })

    async with AsyncPolymarketDataClient(
        transport=httpx.MockTransport(handler)
    ) as client:
        page = await client.activity_page(
            wallet=WALLET_A, condition_id=combo_condition,
        )

    assert len(page.activities) == 1
    assert page.activities[0].is_combo is True
    assert page.activities[0].condition_id == combo_condition


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
        with pytest.raises(InvalidDataError, match="wallet differs"):
            await client.wallet_history(WALLET_A)


@pytest.mark.parametrize("has_more", [True, False])
async def test_wallet_history_cap_retains_each_feed_and_can_resume(has_more) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        status = request.url.params.get("status", "trades")
        resumed = request.url.params.get("cursor") is not None
        row = _trade(WALLET_A) if status == "trades" else _position(WALLET_A, status=status)
        return httpx.Response(200, json={
            "data": [] if resumed else [row],
            "pagination": {
                "has_more": has_more and not resumed,
                "next_cursor": f"resume-{status}" if has_more and not resumed else None,
            },
        })

    async with AsyncPolymarketDataClient(
        base_url="https://data.example", transport=httpx.MockTransport(handler)
    ) as client:
        result = await client.wallet_history(WALLET_A, max_pages_per_feed=1)
        assert result.complete is not has_more
        assert (result.trade_pages, result.current_pages, result.past_pages) == (1, 1, 1)
        assert len(result.trades) == len(result.current_positions) == len(result.past_positions) == 1
        assert result.trades_next_cursor == ("resume-trades" if has_more else None)
        assert result.current_next_cursor == ("resume-OPEN" if has_more else None)
        assert result.past_next_cursor == ("resume-CLOSED" if has_more else None)
        if has_more:
            assert await client.trades_page(wallet=WALLET_A, cursor=result.trades_next_cursor) == ((), None)
            for status, cursor in (("OPEN", result.current_next_cursor), ("CLOSED", result.past_next_cursor)):
                assert await client.positions_page(wallet=WALLET_A, status=status, cursor=cursor) == ((), None)


@pytest.mark.parametrize("feed", ["positions", "trades"])
@pytest.mark.parametrize("payload", [
    [], {},
    {"data": [None], "pagination": {"has_more": False}},
    {"data": [], "pagination": []},
    {"data": [], "pagination": {"has_more": "false"}},
    {"data": [], "pagination": {"has_more": True}},
    {"data": [], "pagination": {"has_more": True, "next_cursor": 12}},
    {"data": [], "pagination": {"has_more": False, "next_cursor": "unexpected"}},
])
async def test_v2_malformed_envelopes_raise_invalid_data(feed, payload) -> None:
    async with AsyncPolymarketDataClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    ) as client:
        page = client.positions_page if feed == "positions" else client.trades_page
        with pytest.raises(InvalidDataError):
            await page(wallet=WALLET_A)


@pytest.mark.parametrize("content", [b'{"data":', b'\xff'])
async def test_v2_bad_json_raises_invalid_data_with_cause(content) -> None:
    async with AsyncPolymarketDataClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=content))
    ) as client:
        with pytest.raises(InvalidDataError, match="valid JSON") as caught:
            await client.trades_page(wallet=WALLET_A)
    assert caught.value.__cause__ is not None


@pytest.mark.parametrize("feed,updates", [
    ("positions", {"proxy_wallet": "bad"}),
    ("positions", {"condition_id": "bad"}),
    ("positions", {"proxy_wallet": WALLET_B}),
    ("positions", {"condition_id": "0x" + "b" * 64}),
    ("positions", {"status": None}),
    ("positions", {"current_size": -1}),
    ("positions", {"current_size": True}),
    ("positions", {"current_size": None}),
    ("positions", {"total_size": -1}),
    ("positions", {"avg_price": -0.1}),
    ("positions", {"avg_price": "nan"}),
    ("positions", {"outcome": []}),
    ("trades", {"proxy_wallet": "bad"}),
    ("trades", {"condition_id": "bad"}),
    ("trades", {"token_id": ""}),
    ("trades", {"size": "nonnumeric"}),
    ("trades", {"size": 0}),
    ("trades", {"price": 1.1}),
    ("trades", {"timestamp": True}),
    ("trades", {"timestamp": -1}),
])
async def test_v2_malformed_rows_raise_invalid_data(feed, updates) -> None:
    row = _position(WALLET_A, status="OPEN") if feed == "positions" else _trade(WALLET_A)
    row.update(updates)
    async with AsyncPolymarketDataClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={
            "data": [row], "pagination": {"has_more": False},
        }))
    ) as client:
        with pytest.raises(InvalidDataError):
            if feed == "positions":
                await client.positions_page(wallet=WALLET_A, condition_id=CONDITION)
            else:
                await client.trades_page(wallet=WALLET_A)


@pytest.mark.parametrize("workflow", ["market_participants", "wallet_history"])
async def test_repeated_cursor_raises_invalid_data(workflow) -> None:
    async with AsyncPolymarketDataClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={
            "data": [], "pagination": {"has_more": True, "next_cursor": "loop"},
        }))
    ) as client:
        method = getattr(client, workflow)
        with pytest.raises(InvalidDataError, match="repeated"):
            await method(CONDITION if workflow == "market_participants" else WALLET_A)


async def test_caller_validation_is_not_remote_invalid_data() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid arguments must fail before I/O")

    async with AsyncPolymarketDataClient(transport=httpx.MockTransport(handler)) as client:
        for kwargs in ({"wallet": "bad"}, {"wallet": WALLET_A, "page_size": 0}, {"wallet": WALLET_A, "status": "REDEEMABLE_LOST"}):
            with pytest.raises(ValueError) as caught:
                await client.positions_page(**kwargs)
            assert not isinstance(caught.value, InvalidDataError)


async def test_positions_preserves_source_avg_price_above_one() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        row = _position(WALLET_A, status="OPEN")
        row["avg_price"] = 1.0089
        return httpx.Response(200, json={
            "data": [row], "pagination": {"has_more": False},
        })

    async with AsyncPolymarketDataClient(
        transport=httpx.MockTransport(handler)
    ) as client:
        positions, cursor = await client.positions_page(wallet=WALLET_A)

    assert cursor is None
    assert positions[0].avg_price == Decimal("1.0089")


async def test_wallet_history_preserves_nontrade_position_and_empty_page_provenance() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        row = _position(WALLET_A, status="OPEN")
        row.update(avg_price=0, total_size=0)
        return httpx.Response(200, json={
            "data": [row] if request.url.params.get("status") == "OPEN" else [],
            "pagination": {"has_more": False},
        })

    async with AsyncPolymarketDataClient(transport=httpx.MockTransport(handler)) as client:
        result = await client.wallet_history(WALLET_A)
    assert result.complete and result.trades == ()
    position = result.current_positions[0]
    assert position.current_size == Decimal("4")
    assert position.avg_price == position.total_size == Decimal("0")
    assert len(result.provenance.observations) == 3


async def test_provenance_records_retries_and_stays_operation_local() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        call_number = calls
        await asyncio.sleep(0)
        if call_number == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={"data": [], "pagination": {"has_more": False}})

    async with AsyncPolymarketDataClient(
        base_url="https://name:password@data.example",
        transport=httpx.MockTransport(handler),
        request_policy=RequestPolicy(max_attempts=2, backoff_base_s=0),
    ) as client:
        first, second = await asyncio.gather(
            client.wallet_history(WALLET_A), client.wallet_history(WALLET_B),
        )
    assert calls == 7
    for result in (first, second):
        assert len(result.provenance.observations) == 3
        for observation in result.provenance.observations:
            assert observation.origin == "https://data.example"
            assert dict(observation.effective_parameters)["user"] == result.wallet
    first_ids = {observation.request_id for observation in first.provenance.observations}
    second_ids = {observation.request_id for observation in second.provenance.observations}
    assert not first_ids & second_ids
    assert sorted(o.attempt_count for r in (first, second) for o in r.provenance.observations) == [1, 1, 1, 1, 1, 2]


@pytest.mark.parametrize("transport_supplied,expected", [(False, "production"), (True, "unknown")])
async def test_data_api_source_classification(transport_supplied, expected) -> None:
    source = classify_request_source(
        "https://data-api.polymarket.com", venue="polymarket", service="data",
        transport_supplied=transport_supplied,
    )
    assert source.data_scope == expected
    assert source_after_response(source, "https://data-api.polymarket.com/v2/trades").data_scope == expected
    assert source_after_response(source, "https://data.example/v2/trades").data_scope == "unknown"


def _pnl_point(**updates) -> dict:
    point = {
        "timestamp": 1792507701, "source_block": 95868222,
        "realized_market_pnl": -52.561934, "realized_lp_pnl": 0.0,
        "realized_combo_pnl": 0.0, "realized_pnl": -52.561934,
        "unrealized_pnl": -0.002528, "fees": -0.713317, "fees_refunded": 0.69897,
        "fees_paid": -0.014347, "deposits": None,
        "volume": 5037.176366, "volume_usdc": 547.707853, "trade_count": 564,
    }
    point.update(updates)
    return point


def _json_transport(path: str, payload: object, seen: list[dict[str, str]] | None = None):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == path
        if seen is not None:
            seen.append(dict(request.url.params))
        return httpx.Response(200, json=payload)
    return httpx.MockTransport(handler)


async def test_user_pnl_keeps_block_label_and_null_atoms() -> None:
    seen: list[dict[str, str]] = []
    payload = {"data": {
        "proxy_wallet": WALLET_A, "interval": "all", "fidelity": "1d",
        "source_fidelity": "1d",
        "points": [_pnl_point(), _pnl_point(timestamp=1792507701, source_block=95868223)],
    }}
    async with AsyncPolymarketDataClient(transport=_json_transport("/v2/user-pnl", payload, seen)) as client:
        series = await client.user_pnl(WALLET_A)

    assert seen == [{"user": WALLET_A, "interval": "all", "fidelity": "1d"}]
    assert series is not None and series.source_fidelity == "1d"
    first = series.points[0]
    assert (first.timestamp_s, first.source_block, first.trade_count) == (1792507701, 95868222, 564)
    assert first.realized_pnl == Decimal("-52.561934")
    assert first.volume_shares == Decimal("5037.176366")
    assert first.deposits is None and first.wallet_income is None
    assert first.fees_paid == Decimal("-0.014347")
    # Duplicate labels stay in source order; selection rules belong to consumers.
    assert [point.source_block for point in series.points] == [95868222, 95868223]
    assert series.observation.request_id


async def test_user_pnl_null_data_is_unknown_wallet() -> None:
    async with AsyncPolymarketDataClient(
        transport=_json_transport("/v2/user-pnl", {"data": None})
    ) as client:
        assert await client.user_pnl(WALLET_A) is None


@pytest.mark.parametrize("data", [
    {"proxy_wallet": WALLET_B},
    {"interval": "1m"},
    {"fidelity": "1h"},
    {"points": None},
    {"points": [None]},
    {"points": [_pnl_point(realized_pnl=None)]},
    {"points": [_pnl_point(trade_count=-1)]},
    {"points": [_pnl_point(source_block=True)]},
    {"points": [_pnl_point(volume_usdc=-1)]},
    {"points": [_pnl_point(fees="nan")]},
    {"source_fidelity": ""},
])
async def test_user_pnl_rejects_contradictory_series(data) -> None:
    series = {"proxy_wallet": WALLET_A, "interval": "all", "fidelity": "1d",
              "source_fidelity": "1d", "points": [_pnl_point()], **data}
    async with AsyncPolymarketDataClient(
        transport=_json_transport("/v2/user-pnl", {"data": series})
    ) as client:
        with pytest.raises(InvalidDataError):
            await client.user_pnl(WALLET_A)


@pytest.mark.parametrize("payload", [[], {}, {"data": []}])
async def test_unpaginated_reads_reject_malformed_envelopes(payload) -> None:
    async with AsyncPolymarketDataClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    ) as client:
        for read in (client.user_pnl, client.user_stats, client.leaderboard_standing):
            with pytest.raises(InvalidDataError):
                await read(WALLET_A)


async def test_user_stats_separates_unknown_wallet_from_zero_profile() -> None:
    zeros = {"data": {"proxy_wallet": WALLET_A, "trades": 0, "biggest_win": 0,
                      "views": 0, "join_date": None, "all_time_pnl": None}}
    async with AsyncPolymarketDataClient(transport=_json_transport("/v2/user-stats", zeros)) as client:
        stats = await client.user_stats(WALLET_A)
    assert stats is not None
    assert (stats.markets_traded, stats.biggest_win_usdc, stats.join_date_s) == (0, Decimal("0"), None)
    assert stats.all_time_pnl is None

    async with AsyncPolymarketDataClient(
        transport=_json_transport("/v2/user-stats", {"data": None})
    ) as client:
        assert await client.user_stats(WALLET_A) is None


async def test_user_stats_parses_latest_pnl_point() -> None:
    seen: list[dict[str, str]] = []
    payload = {"data": {"proxy_wallet": WALLET_A, "trades": 158, "biggest_win": 12.5,
                        "views": 3, "join_date": 1700000000, "all_time_pnl": _pnl_point()}}
    async with AsyncPolymarketDataClient(transport=_json_transport("/v2/user-stats", payload, seen)) as client:
        stats = await client.user_stats(WALLET_A)
    assert seen == [{"user": WALLET_A}]
    assert stats is not None and stats.markets_traded == 158 and stats.join_date_s == 1700000000
    assert stats.all_time_pnl is not None and stats.all_time_pnl.source_block == 95868222


@pytest.mark.parametrize("updates", [
    {"proxy_wallet": WALLET_B}, {"trades": 1.5}, {"views": -1}, {"biggest_win": None},
    {"join_date": "2024"}, {"all_time_pnl": []},
])
async def test_user_stats_rejects_malformed_rows(updates) -> None:
    data = {"proxy_wallet": WALLET_A, "trades": 1, "biggest_win": 0, "views": 0, **updates}
    async with AsyncPolymarketDataClient(
        transport=_json_transport("/v2/user-stats", {"data": data})
    ) as client:
        with pytest.raises(InvalidDataError):
            await client.user_stats(WALLET_A)


async def test_leaderboard_page_keeps_ties_basis_and_cursor() -> None:
    seen: list[dict[str, str]] = []
    payload = {
        "data": [
            {"rank": 1, "user_id": WALLET_A, "pnl": 10.5, "volume": 100, "user_name": "a",
             "profile_image": "", "x_username": "", "verified": False},
            {"rank": 1, "user_id": WALLET_B, "pnl": 10.5, "volume": 50, "user_name": "b",
             "profile_image": "", "x_username": "", "verified": True},
        ],
        "pagination": {"limit": 2, "offset": 0, "has_more": True, "next_cursor": "board-2"},
    }
    async with AsyncPolymarketDataClient(transport=_json_transport("/v2/leaderboard", payload, seen)) as client:
        page = await client.leaderboard_page(page_size=2)

    assert seen == [{"time_period": "all", "category": "overall", "sort_by": "PNL", "limit": "2"}]
    assert [entry.rank for entry in page.entries] == [1, 1]
    assert page.entries[1].wallet == WALLET_B and page.entries[1].volume_shares == Decimal("50")
    assert page.entries[0].request_id == page.observation.request_id
    assert page.next_cursor == "board-2" and page.pnl_basis == "realized_lifetime"


async def test_leaderboard_finite_window_is_marked_basis() -> None:
    payload = {"data": [], "pagination": {"limit": 0, "offset": 0, "has_more": False}}
    async with AsyncPolymarketDataClient(transport=_json_transport("/v2/leaderboard", payload)) as client:
        page = await client.leaderboard_page(time_period="week", cursor="pinned")
    assert page.entries == () and page.pnl_basis == "marked_equity_change_net_of_flows"


@pytest.mark.parametrize("updates", [
    {"user_id": "not-a-wallet"}, {"rank": -1}, {"pnl": None}, {"volume": -1},
])
async def test_leaderboard_page_rejects_malformed_rows(updates) -> None:
    row = {"rank": 1, "user_id": WALLET_A, "pnl": 1, "volume": 1, **updates}
    payload = {"data": [row], "pagination": {"has_more": False}}
    async with AsyncPolymarketDataClient(transport=_json_transport("/v2/leaderboard", payload)) as client:
        with pytest.raises(InvalidDataError):
            await client.leaderboard_page()


@pytest.mark.parametrize("ranks,expected", [
    ({"rank_pnl": 7, "rank_volume": None}, (7, None)),
    ({"rank_pnl": 0, "rank_volume": 3}, (None, 3)),
    ({}, (None, None)),
])
async def test_leaderboard_standing_treats_null_and_zero_as_unranked(ranks, expected) -> None:
    seen: list[dict[str, str]] = []
    payload = {"data": {"user_id": WALLET_A, "pnl": -4.5, "volume": 12, "user_name": "",
                        "profile_image": "", "x_username": "", "verified": False, **ranks}}
    async with AsyncPolymarketDataClient(transport=_json_transport("/v2/leaderboard", payload, seen)) as client:
        standing = await client.leaderboard_standing(WALLET_A)
    assert seen == [{"user": WALLET_A, "time_period": "all", "category": "overall"}]
    assert standing is not None
    assert (standing.rank_pnl, standing.rank_volume) == expected
    assert standing.pnl_usdc == Decimal("-4.5")


async def test_leaderboard_standing_rejects_other_wallet_and_accepts_unknown() -> None:
    other = {"data": {"user_id": WALLET_B, "pnl": 0, "volume": 0}}
    async with AsyncPolymarketDataClient(transport=_json_transport("/v2/leaderboard", other)) as client:
        with pytest.raises(InvalidDataError):
            await client.leaderboard_standing(WALLET_A)
    async with AsyncPolymarketDataClient(
        transport=_json_transport("/v2/leaderboard", {"data": None})
    ) as client:
        assert await client.leaderboard_standing(WALLET_A) is None


async def test_pnl_and_board_arguments_fail_before_io() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid arguments must fail before I/O")

    async with AsyncPolymarketDataClient(transport=httpx.MockTransport(handler)) as client:
        calls = [
            client.user_pnl("bad"),
            client.user_pnl(WALLET_A, interval="1y"),
            client.user_pnl(WALLET_A, fidelity="1m"),
            client.user_stats("bad"),
            client.leaderboard_page(time_period="year"),
            client.leaderboard_page(sort_by="pnl"),
            client.leaderboard_page(category=""),
            client.leaderboard_page(page_size=1001),
            client.leaderboard_page(cursor=""),
            client.leaderboard_standing(WALLET_A, time_period="year"),
        ]
        for call in calls:
            with pytest.raises(ValueError) as caught:
                await call
            assert not isinstance(caught.value, InvalidDataError)
