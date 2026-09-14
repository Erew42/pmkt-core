from __future__ import annotations

from pmkt.records import KalshiMarketRef, PolymarketMarketRef

import asyncio
import json

import httpx
import pandas as pd
import pytest

from pmkt.runtime import RequestPolicy
from pmkt.runtime import OperationExpiry
from pmkt.data.canonical import (
    kalshi_market_snapshot_v2_row,
    market_resolution_row,
    polymarket_market_snapshot_v2_row,
)
from pmkt.data.normalize import extract_market_rows
from pmkt.data.validation import validate_frame
from pmkt.errors import OperationTimeoutError
from pmkt.exchanges.kalshi import AsyncKalshiClient
from pmkt.exchanges.kalshi.client import normalize_kalshi_market
from pmkt.exchanges.polymarket import AsyncGammaClient
from pmkt.exchanges.read_auth import ReadAuthenticationRequiredError
from pmkt.resolution import (
    EvmRpcError as ExportedEvmRpcError,
    KalshiResolutionResolver as ExportedKalshiResolutionResolver,
    PolygonCtfClient as ExportedPolygonCtfClient,
    PolymarketResolutionResolver as ExportedPolymarketResolutionResolver,
)
from pmkt.resolution.cache import resolve_market_resolution_cache
from pmkt.resolution.evm import EvmRpcError, PolygonCtfClient
from pmkt.resolution.kalshi import (
    KalshiResolutionResolver,
    kalshi_resolution_from_payload,
)
from pmkt.resolution.models import RESOLVER_VERSION, error_record
from pmkt.resolution.polymarket import PolymarketResolutionResolver, _clob_tokens


@pytest.mark.asyncio
async def test_rpc_request_timeout_is_capped_by_operation_expiry() -> None:
    timeouts = []

    def handler(request: httpx.Request) -> httpx.Response:
        timeouts.append(request.extensions["timeout"])
        return httpx.Response(200, json={"result": "0x89"})

    async with PolygonCtfClient(
        "https://rpc.test", timeout_s=20, transport=httpx.MockTransport(handler)
    ) as client:
        await client.ensure_polygon(expiry=OperationExpiry.after(2, clock=lambda: 100))
        await client.ensure_polygon()
    assert all(value == 2 for value in timeouts[0].values())
    assert all(value == 20 for value in timeouts[1].values())


def test_polymarket_normalizer_keeps_legacy_snapshot_v1_resolution_fields() -> None:
    [row] = extract_market_rows(
        [
            {
                "id": "pm-1",
                "question": "Will it rain?",
                "conditionId": "0xabc",
                "questionID": "q-1",
                "outcomes": '["Yes", "No"]',
                "outcomePrices": '["1", "0"]',
                "umaResolutionStatus": "resolved",
                "resolvedBy": "uma",
                "resolutionSource": "oracle",
            }
        ]
    )

    assert row["schema_version"] == "polymarket_market_snapshot.v1"
    assert row["market_id"] == "pm-1"
    assert row["condition_id"] == "0xabc"
    assert row["question_id"] == "q-1"
    assert row["outcome_labels_json"] == ["yes", "no"]
    assert row["outcome_prices_json"] == ["1", "0"]
    assert row["uma_resolution_status"] == "resolved"
    assert row["resolved_by"] == "uma"
    assert row["resolution_source"] == "oracle"


def test_polymarket_v2_snapshot_row_uses_explicit_trimmed_shape() -> None:
    row = polymarket_market_snapshot_v2_row(
        market_id="pm-1",
        question="Will it rain?",
        condition_id="0xabc",
        question_id="q-1",
        outcome_labels_json=["yes", "no"],
        outcome_prices_json=["1", "0"],
        uma_resolution_status="resolved",
        resolved_by="uma",
        resolution_source="oracle",
    )

    assert row["schema_version"] == "polymarket_market_snapshot.v2"
    assert row["market_id"] == "pm-1"
    assert "condition_id" not in row
    assert "uma_resolution_status" not in row
    assert validate_frame(pd.DataFrame([row]), "polymarket_market_snapshot.v2").ok


def test_kalshi_normalizer_keeps_legacy_snapshot_v1_settlement_fields() -> None:
    row = normalize_kalshi_market(
        {
            "ticker": "KXRAIN",
            "title": "Will it rain?",
            "status": "determined",
            "result": "yes",
            "settlement_value_dollars": "1",
            "settlement_ts": "2026-01-01T00:00:00Z",
            "expiration_value": "1",
            "rules_primary": "Primary rule",
            "rules_secondary": "Secondary rule",
        }
    )

    assert row["schema_version"] == "kalshi_market_snapshot.v1"
    assert row["market_key"] == "KXRAIN"
    assert row["result"] == "yes"
    assert row["settlement_value_dollars"] == "1"
    assert row["settlement_ts"] == "2026-01-01T00:00:00Z"
    assert row["expiration_value"] == "1"
    assert row["is_provisional"] is True
    assert row["rules_primary"] == "Primary rule"
    assert row["rules_secondary"] == "Secondary rule"


def test_kalshi_v2_snapshot_row_uses_explicit_trimmed_shape() -> None:
    row = kalshi_market_snapshot_v2_row(
        exchange="kalshi",
        market_key="KXRAIN",
        question="Will it rain?",
        status="determined",
        result="yes",
        settlement_value_dollars="1",
        settlement_ts="2026-01-01T00:00:00Z",
        expiration_value="1",
        is_provisional=True,
        rules_primary="Primary rule",
        rules_secondary="Secondary rule",
    )

    assert row["schema_version"] == "kalshi_market_snapshot.v2"
    assert row["market_key"] == "KXRAIN"
    assert "result" not in row
    assert "settlement_value_dollars" not in row
    assert validate_frame(pd.DataFrame([row]), "kalshi_market_snapshot.v2").ok


def test_kalshi_resolution_finalized_binary() -> None:
    record = kalshi_resolution_from_payload(
        {
            "market_key": "KXRAIN",
            "status": "finalized",
            "result": "no",
            "settlement_value_dollars": "0",
        }
    )

    assert record.resolution_state == "metadata_only"
    assert record.confidence == "metadata_only"
    assert record.canonical_source is None
    assert record.result == "no"
    assert record.winner == "no"
    assert record.payouts[1].payout == "1"


def test_kalshi_resolution_finalized_settlement_overrides_result_hint() -> None:
    record = kalshi_resolution_from_payload(
        {
            "market_key": "KXRAIN",
            "status": "finalized",
            "result": "yes",
            "settlement_value_dollars": "0",
        },
        source="kalshi_rest",
    )

    assert record.resolution_state == "final"
    assert record.confidence == "canonical"
    assert record.canonical_source == "kalshi_rest"
    assert record.result == "no"
    assert record.winner == "no"
    assert record.payouts[1].payout == "1"


@pytest.mark.parametrize("settlement_value", ["inf", "-inf", "nan", "not-a-number"])
def test_kalshi_resolution_finalized_rejects_non_finite_settlement(
    settlement_value: str,
) -> None:
    record = kalshi_resolution_from_payload(
        {
            "market_key": "KXRAIN",
            "status": "finalized",
            "result": "yes",
            "settlement_value_dollars": settlement_value,
        },
        source="kalshi_rest",
    )

    assert record.resolution_state == "closed_unresolved"
    assert record.confidence == "unavailable"
    assert record.canonical_source is None
    assert record.winner is None
    assert record.payouts == []


def test_kalshi_resolution_finalized_ignores_expiration_without_settlement() -> None:
    record = kalshi_resolution_from_payload(
        {
            "market_key": "KXRAIN",
            "status": "finalized",
            "expiration_value": "1",
        },
        source="kalshi_rest",
    )

    assert record.resolution_state == "closed_unresolved"
    assert record.confidence == "unavailable"
    assert record.canonical_source is None
    assert record.settlement_value_dollars is None
    assert record.winner is None
    assert record.payouts == []


@pytest.mark.parametrize(
    ("settlement_value", "expected_payout"),
    [
        ("0.4200", "0.42"),
        ("0.99999999999999999", "0.99999999999999999"),
        ("1.00000000000000001", "1.00000000000000001"),
    ],
)
def test_kalshi_resolution_finalized_preserves_exact_scalar_settlements(
    settlement_value: str,
    expected_payout: str,
) -> None:
    record = kalshi_resolution_from_payload(
        {
            "market_key": "KXSCALAR",
            "status": "finalized",
            "settlement_value_dollars": settlement_value,
        },
        source="kalshi_rest",
    )

    assert record.resolution_state == "final"
    assert record.confidence == "canonical"
    assert record.result_type == "scalar"
    assert record.winner is None
    assert record.result == expected_payout
    assert record.payouts[0].payout == expected_payout


@pytest.mark.parametrize("status", ["initialized", "inactive"])
def test_kalshi_resolution_maps_nonfinal_statuses(status: str) -> None:
    record = kalshi_resolution_from_payload(
        {
            "ticker": "KXRAIN",
            "status": status,
        }
    )

    assert record.resolution_state == "open"
    assert record.confidence == "unavailable"
    assert record.raw_status == status


@pytest.mark.asyncio
async def test_kalshi_resolver_prefers_live_rest_over_disagreeing_snapshot() -> None:
    class FakeKalshi:
        async def market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "finalized",
                "result": "no",
                "settlement_value_dollars": "0",
            }

        async def historical_market(self, ticker: str, *, expiry=None):
            request = httpx.Request(
                "GET", f"https://kalshi.test/historical/markets/{ticker}"
            )
            response = httpx.Response(404, request=request)
            raise httpx.HTTPStatusError("not found", request=request, response=response)

    record = await KalshiResolutionResolver(FakeKalshi()).resolve(
        KalshiMarketRef("KXRAIN"),
        snapshot={
            "ticker": "KXRAIN",
            "status": "finalized",
            "result": "yes",
            "settlement_value_dollars": "1",
        },
    )

    assert record.resolution_state == "final"
    assert record.confidence == "canonical"
    assert record.canonical_source == "kalshi_rest"
    assert record.winner == "no"
    assert {observation.source for observation in record.source_observations} == {
        "kalshi_snapshot",
        "kalshi_rest",
    }


@pytest.mark.asyncio
async def test_kalshi_resolver_uses_historical_fallback_after_404() -> None:
    class FakeKalshi:
        async def market(self, ticker: str, *, expiry=None):
            request = httpx.Request("GET", f"https://kalshi.test/markets/{ticker}")
            response = httpx.Response(404, request=request)
            raise httpx.HTTPStatusError("not found", request=request, response=response)

        async def historical_market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "finalized",
                "result": "yes",
                "settlement_value_dollars": "1",
            }

    record = await KalshiResolutionResolver(FakeKalshi()).resolve(KalshiMarketRef("KXRAIN"))

    assert record.resolution_state == "final"
    assert record.confidence == "canonical"
    assert record.canonical_source == "kalshi_historical_rest"
    assert record.winner == "yes"


@pytest.mark.asyncio
async def test_kalshi_resolver_accepts_official_historical_settlement_shape() -> None:
    class FakeKalshi:
        async def market(self, ticker: str, *, expiry=None):
            request = httpx.Request("GET", f"https://kalshi.test/markets/{ticker}")
            response = httpx.Response(404, request=request)
            raise httpx.HTTPStatusError("not found", request=request, response=response)

        async def historical_market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "settlement_value_dollars": "0.5",
                "settlement_ts": "2026-01-01T00:00:00Z",
            }

    record = await KalshiResolutionResolver(FakeKalshi()).resolve(KalshiMarketRef("KXSCALAR"))

    assert record.resolution_state == "final"
    assert record.confidence == "canonical"
    assert record.canonical_source == "kalshi_historical_rest"
    assert record.result_type == "scalar"
    assert record.result == "0.5"
    assert record.winner is None
    assert record.payouts[0].payout == "0.5"


@pytest.mark.asyncio
async def test_kalshi_resolver_preserves_snapshot_metadata_when_rest_unavailable() -> (
    None
):
    class FakeKalshi:
        async def market(self, ticker: str, *, expiry=None):
            return {"ticker": ticker}

        async def historical_market(self, ticker: str, *, expiry=None):
            return {"ticker": ticker}

    record = await KalshiResolutionResolver(FakeKalshi()).resolve(
        KalshiMarketRef("KXRAIN"),
        snapshot={
            "ticker": "KXRAIN",
            "status": "finalized",
            "settlement_value_dollars": "1",
            "settlement_ts": "2026-01-01T00:00:00Z",
        },
    )

    assert record.resolution_state == "metadata_only"
    assert record.confidence == "metadata_only"
    assert record.canonical_source is None
    assert record.winner == "yes"
    assert record.settlement_value_dollars == "1"
    assert {observation.source for observation in record.source_observations} == {
        "kalshi_snapshot",
        "kalshi_rest",
        "kalshi_historical_rest",
    }


@pytest.mark.asyncio
async def test_kalshi_resolver_preserves_live_error_over_snapshot_metadata() -> None:
    class FakeKalshi:
        async def market(self, ticker: str, *, expiry=None):
            request = httpx.Request("GET", f"https://kalshi.test/markets/{ticker}")
            response = httpx.Response(500, request=request)
            raise httpx.HTTPStatusError(
                "server error", request=request, response=response
            )

        async def historical_market(self, ticker: str, *, expiry=None):
            return {"ticker": ticker}

    record = await KalshiResolutionResolver(FakeKalshi()).resolve(
        KalshiMarketRef("KXRAIN"),
        snapshot={
            "ticker": "KXRAIN",
            "status": "finalized",
            "settlement_value_dollars": "1",
            "settlement_ts": "2026-01-01T00:00:00Z",
        },
    )

    assert record.resolution_state == "unavailable"
    assert record.confidence == "unavailable"
    assert record.error_type == "HTTPStatusError"
    assert record.winner is None
    rest_errors = [
        observation
        for observation in record.source_observations
        if observation.source == "kalshi_rest"
        and observation.error_type == "HTTPStatusError"
    ]
    assert len(rest_errors) == 1
    assert {observation.source for observation in record.source_observations} == {
        "kalshi_snapshot",
        "kalshi_rest",
        "kalshi_historical_rest",
    }


@pytest.mark.asyncio
async def test_kalshi_resolver_live_nonfinal_blocks_snapshot_finality() -> None:
    class FakeKalshi:
        async def market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "open",
            }

        async def historical_market(self, ticker: str, *, expiry=None):
            return {"ticker": ticker}

    record = await KalshiResolutionResolver(FakeKalshi()).resolve(
        KalshiMarketRef("KXRAIN"),
        snapshot={
            "ticker": "KXRAIN",
            "status": "finalized",
            "settlement_value_dollars": "1",
            "settlement_ts": "2026-01-01T00:00:00Z",
        },
    )

    assert record.resolution_state == "open"
    assert record.confidence == "unavailable"
    assert record.canonical_source is None
    assert record.winner is None
    assert record.raw_status == "open"
    assert {observation.source for observation in record.source_observations} == {
        "kalshi_snapshot",
        "kalshi_rest",
        "kalshi_historical_rest",
    }


@pytest.mark.asyncio
async def test_kalshi_resolver_historical_nonfinal_blocks_snapshot_finality_after_live_404() -> (
    None
):
    class FakeKalshi:
        async def market(self, ticker: str, *, expiry=None):
            request = httpx.Request("GET", f"https://kalshi.test/markets/{ticker}")
            response = httpx.Response(404, request=request)
            raise httpx.HTTPStatusError("not found", request=request, response=response)

        async def historical_market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "open",
            }

    record = await KalshiResolutionResolver(FakeKalshi()).resolve(
        KalshiMarketRef("KXRAIN"),
        snapshot={
            "ticker": "KXRAIN",
            "status": "finalized",
            "settlement_value_dollars": "1",
            "settlement_ts": "2026-01-01T00:00:00Z",
        },
    )

    assert record.resolution_state == "open"
    assert record.confidence == "unavailable"
    assert record.canonical_source is None
    assert record.winner is None
    assert record.raw_status == "open"
    assert {observation.source for observation in record.source_observations} == {
        "kalshi_snapshot",
        "kalshi_historical_rest",
    }


@pytest.mark.parametrize("status", ["initialized", "inactive"])
@pytest.mark.asyncio
async def test_kalshi_resolver_live_nonfinal_blocks_historical_final(
    status: str,
) -> None:
    class FakeKalshi:
        async def market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": status,
            }

        async def historical_market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "finalized",
                "result": "yes",
                "settlement_value_dollars": "1",
            }

    record = await KalshiResolutionResolver(FakeKalshi()).resolve(KalshiMarketRef("KXRAIN"))

    assert record.resolution_state == "open"
    assert record.confidence == "unavailable"
    assert record.canonical_source is None
    assert record.winner is None
    assert record.raw_status == status
    assert {observation.source for observation in record.source_observations} == {
        "kalshi_rest",
        "kalshi_historical_rest",
    }


@pytest.mark.asyncio
async def test_kalshi_resolver_marks_live_historical_conflict_inconsistent() -> None:
    class FakeKalshi:
        async def market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "finalized",
                "result": "yes",
                "settlement_value_dollars": "1",
            }

        async def historical_market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "finalized",
                "result": "no",
                "settlement_value_dollars": "0",
            }

    record = await KalshiResolutionResolver(FakeKalshi()).resolve(KalshiMarketRef("KXRAIN"))

    assert record.resolution_state == "inconsistent"
    assert record.confidence == "inconsistent"
    assert record.canonical_source is None
    assert record.winner is None
    assert {observation.source for observation in record.source_observations} == {
        "kalshi_rest",
        "kalshi_historical_rest",
    }


@pytest.mark.parametrize(
    ("live_settlement", "historical_settlement"),
    [
        ("0.42000000000000001", "0.42"),
        ("9007199254740992", "9007199254740993"),
    ],
)
@pytest.mark.asyncio
async def test_kalshi_resolver_marks_exact_scalar_conflicts_inconsistent(
    live_settlement: str,
    historical_settlement: str,
) -> None:
    class FakeKalshi:
        async def market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "finalized",
                "settlement_value_dollars": live_settlement,
            }

        async def historical_market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "finalized",
                "settlement_value_dollars": historical_settlement,
            }

    record = await KalshiResolutionResolver(FakeKalshi()).resolve(KalshiMarketRef("KXSCALAR"))

    assert record.resolution_state == "inconsistent"
    assert record.confidence == "inconsistent"
    assert record.canonical_source is None
    assert record.winner is None
    assert {observation.source for observation in record.source_observations} == {
        "kalshi_rest",
        "kalshi_historical_rest",
    }


@pytest.mark.asyncio
async def test_kalshi_resolver_keeps_equivalent_scalar_formats_consistent() -> None:
    class FakeKalshi:
        async def market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "finalized",
                "settlement_value_dollars": "0.420",
            }

        async def historical_market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "finalized",
                "settlement_value_dollars": "0.42",
            }

    record = await KalshiResolutionResolver(FakeKalshi()).resolve(KalshiMarketRef("KXSCALAR"))

    assert record.resolution_state == "final"
    assert record.confidence == "canonical"
    assert record.canonical_source == "kalshi_rest"
    assert record.result_type == "scalar"
    assert record.result == "0.42"
    assert record.payouts[0].payout == "0.42"


@pytest.mark.asyncio
async def test_kalshi_resolver_live_final_preserves_historical_error_observation() -> (
    None
):
    class FakeKalshi:
        async def market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "finalized",
                "result": "yes",
                "settlement_value_dollars": "1",
            }

        async def historical_market(self, ticker: str, *, expiry=None):
            request = httpx.Request(
                "GET", f"https://kalshi.test/historical/markets/{ticker}"
            )
            response = httpx.Response(500, request=request)
            raise httpx.HTTPStatusError(
                "server error", request=request, response=response
            )

    record = await KalshiResolutionResolver(FakeKalshi()).resolve(KalshiMarketRef("KXRAIN"))

    assert record.resolution_state == "final"
    assert record.confidence == "canonical"
    assert record.canonical_source == "kalshi_rest"
    assert record.winner == "yes"
    historical_errors = [
        observation
        for observation in record.source_observations
        if observation.source == "kalshi_historical_rest"
    ]
    assert len(historical_errors) == 1
    assert historical_errors[0].error_type == "HTTPStatusError"


@pytest.mark.asyncio
async def test_kalshi_resolver_records_historical_transport_error_observation() -> None:
    class FakeKalshi:
        async def market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "finalized",
                "settlement_value_dollars": "1",
            }

        async def historical_market(self, ticker: str, *, expiry=None):
            raise httpx.ConnectError(
                "historical transport failed",
                request=httpx.Request(
                    "GET", f"https://kalshi.test/historical/markets/{ticker}"
                ),
            )

    record = await KalshiResolutionResolver(FakeKalshi()).resolve(KalshiMarketRef("KXRAIN"))

    assert record.resolution_state == "final"
    assert record.confidence == "canonical"
    assert record.canonical_source == "kalshi_rest"
    historical_errors = [
        observation
        for observation in record.source_observations
        if observation.source == "kalshi_historical_rest"
    ]
    assert len(historical_errors) == 1
    assert historical_errors[0].error_type == "ConnectError"
    assert historical_errors[0].error_message == (
        "kalshi_historical_rest request failed (ConnectError)"
    )


@pytest.mark.asyncio
async def test_kalshi_resolver_snapshot_alone_is_noncanonical_fallback() -> None:
    record = await KalshiResolutionResolver().resolve(
        KalshiMarketRef("KXRAIN"),
        snapshot={
            "ticker": "KXRAIN",
            "status": "finalized",
            "result": "yes",
            "settlement_value_dollars": "1",
        },
    )

    assert record.resolution_state == "metadata_only"
    assert record.confidence == "metadata_only"
    assert record.canonical_source is None
    assert record.winner == "yes"
    assert record.source_observations[0].source == "kalshi_snapshot"


@pytest.mark.asyncio
async def test_polymarket_resolver_ctf_final_vector_is_canonical() -> None:
    class FakeCtf:
        async def ensure_polygon(self, *, expiry=None) -> None:
            return None

        async def payout_vector(self, condition_id: str, outcome_count: int, *, expiry=None):
            assert condition_id == "0xabc"
            assert outcome_count == 2
            return 1, [1, 0]

    resolver = PolymarketResolutionResolver(ctf_client=FakeCtf())
    record = await resolver.resolve(
        PolymarketMarketRef("pm-1"),
        snapshot={
            "market_id": "pm-1",
            "condition_id": "0xabc",
            "outcome_labels_json": ["yes", "no"],
        },
    )

    assert record.resolution_state == "final"
    assert record.confidence == "canonical"
    assert record.canonical_source == "polygon_ctf"
    assert record.winner == "yes"


@pytest.mark.asyncio
async def test_polymarket_resolver_ctf_final_retains_snapshot_label_mapping() -> None:
    class FakeCtf:
        async def ensure_polygon(self, *, expiry=None) -> None:
            return None

        async def payout_vector(self, condition_id: str, outcome_count: int, *, expiry=None):
            assert condition_id == "0xabc"
            assert outcome_count == 2
            return 1, [1, 0]

    record = await PolymarketResolutionResolver(ctf_client=FakeCtf()).resolve(
        PolymarketMarketRef("pm-1"),
        snapshot={
            "market_id": "pm-1",
            "condition_id": "0xabc",
            "outcome_labels_json": ["Yes", "No"],
        },
    )

    assert [observation.source for observation in record.source_observations] == [
        "polymarket_snapshot",
        "polygon_ctf",
    ]
    mapping = record.source_observations[0].evidence
    assert mapping is not None
    assert mapping["market_key"] == "pm-1"
    assert mapping["condition_id"] == "0xabc"
    assert mapping["outcome_labels"] == [
        {"outcome_index": 0, "outcome": "yes"},
        {"outcome_index": 1, "outcome": "no"},
    ]
    assert isinstance(mapping["payload_sha256"], str)


@pytest.mark.asyncio
async def test_polymarket_resolver_ctf_final_retains_gamma_label_mapping() -> None:
    class FakeGamma:
        async def market(self, market_key: str, *, expiry=None):
            assert market_key == "pm-1"
            return {
                "id": "pm-1",
                "conditionId": "0xabc",
                "outcomes": ["Over", "Under"],
            }

    class FakeCtf:
        async def ensure_polygon(self, *, expiry=None) -> None:
            return None

        async def payout_vector(self, condition_id: str, outcome_count: int, *, expiry=None):
            assert condition_id == "0xabc"
            assert outcome_count == 2
            return 1, [0, 1]

    record = await PolymarketResolutionResolver(
        gamma_client=FakeGamma(),
        ctf_client=FakeCtf(),
    ).resolve(PolymarketMarketRef("pm-1"), snapshot={})

    assert record.resolution_state == "final"
    assert record.winner == "under"
    assert [observation.source for observation in record.source_observations] == [
        "polymarket_gamma",
        "polymarket_gamma",
        "polygon_ctf",
    ]
    assert record.source_observations[0].evidence["status"] == "success"
    assert isinstance(record.source_observations[0].evidence["payload_sha256"], str)
    mapping = record.source_observations[1].evidence
    assert mapping is not None
    assert mapping["condition_id"] == "0xabc"
    assert mapping["outcome_labels"] == [
        {"outcome_index": 0, "outcome": "over"},
        {"outcome_index": 1, "outcome": "under"},
    ]
    assert isinstance(mapping["payload_sha256"], str)


@pytest.mark.parametrize(
    (
        "denominator",
        "numerators",
        "expected_winner",
        "expected_result_type",
        "expected_payouts",
    ),
    [
        (1, [1, 0], "yes", "binary", ["1", "0"]),
        (1, [0, 1], "no", "binary", ["0", "1"]),
        (2, [1, 1], None, "fractional", ["1/2", "1/2"]),
    ],
)
@pytest.mark.asyncio
async def test_polymarket_resolver_ctf_final_vectors_have_exact_payouts(
    denominator: int,
    numerators: list[int],
    expected_winner: str | None,
    expected_result_type: str,
    expected_payouts: list[str],
) -> None:
    class FakeCtf:
        async def ensure_polygon(self, *, expiry=None) -> None:
            return None

        async def payout_vector(self, condition_id: str, outcome_count: int, *, expiry=None):
            assert condition_id == "0xabc"
            assert outcome_count == 2
            return denominator, numerators

    record = await PolymarketResolutionResolver(ctf_client=FakeCtf()).resolve(
        PolymarketMarketRef("pm-1"),
        snapshot={
            "market_id": "pm-1",
            "condition_id": "0xabc",
            "outcome_labels_json": ["yes", "no"],
        },
    )

    assert record.resolution_state == "final"
    assert record.confidence == "canonical"
    assert record.canonical_source == "polygon_ctf"
    assert record.result_type == expected_result_type
    assert record.winner == expected_winner
    assert [payout.payout for payout in record.payouts] == expected_payouts


@pytest.mark.parametrize(
    ("denominator", "numerators", "expected_error"),
    [
        (1, [1, 1], "CTF payout numerators must sum to denominator"),
        (2, [2, 1], "CTF payout numerators must sum to denominator"),
        (2, [3, -1], "CTF payout vector contains a negative numerator"),
        (2, [], "CTF payout vector must include at least one numerator"),
    ],
)
@pytest.mark.asyncio
async def test_polymarket_resolver_rejects_invalid_ctf_payout_vectors(
    denominator: int,
    numerators: list[int],
    expected_error: str,
) -> None:
    class FakeCtf:
        async def ensure_polygon(self, *, expiry=None) -> None:
            return None

        async def payout_vector(self, condition_id: str, outcome_count: int, *, expiry=None):
            assert condition_id == "0xabc"
            assert outcome_count == 2
            return denominator, numerators

    record = await PolymarketResolutionResolver(ctf_client=FakeCtf()).resolve(
        PolymarketMarketRef("pm-1"),
        snapshot={
            "market_id": "pm-1",
            "condition_id": "0xabc",
            "outcome_labels_json": ["yes", "no"],
        },
    )

    assert record.resolution_state == "inconsistent"
    assert record.confidence == "inconsistent"
    assert record.canonical_source is None
    assert record.winner is None
    assert record.payouts == []
    assert record.error_type == "InvalidPayoutVector"
    assert record.error_message == expected_error


@pytest.mark.asyncio
async def test_polymarket_resolver_official_clob_tokens_are_non_authoritative() -> None:
    class FakeClob:
        async def clob_market_info(self, condition_id: str, *, expiry=None):
            return {
                "condition_id": condition_id,
                "question_id": "q-1",
                "t": [
                    {
                        "t": "217221323247049940788053017360663220218509556442267117653",
                        "o": "Yes",
                    },
                    {
                        "t": "483423353489462542133692381492404147668191676576509311866",
                        "o": "No",
                    },
                ],
            }

    resolver = PolymarketResolutionResolver(clob_client=FakeClob())
    record = await resolver.resolve(
        PolymarketMarketRef("pm-1"),
        snapshot={
            "market_id": "pm-1",
            "condition_id": "0xabc",
            "outcome_labels_json": ["yes", "no"],
        },
    )

    assert record.resolution_state == "open"
    assert record.confidence == "unavailable"
    assert record.canonical_source is None
    assert record.winner is None
    assert record.payouts == []
    assert [observation.source for observation in record.source_observations] == [
        "polymarket_clob",
    ]
    observation = record.source_observations[0]
    assert observation.confidence == "metadata_only"
    assert observation.evidence["status"] == "success"
    assert observation.evidence["condition_id"] == "0xabc"
    assert observation.evidence["tokens"] == [
        {
            "outcome_index": 0,
            "outcome": "yes",
            "token_id": "217221323247049940788053017360663220218509556442267117653",
        },
        {
            "outcome_index": 1,
            "outcome": "no",
            "token_id": "483423353489462542133692381492404147668191676576509311866",
        },
    ]


def test_polymarket_clob_tokens_parse_official_compressed_entries() -> None:
    assert _clob_tokens(
        {
            "t": [
                {
                    "t": "217221323247049940788053017360663220218509556442267117653",
                    "o": "Yes",
                },
                {
                    "t": "483423353489462542133692381492404147668191676576509311866",
                    "o": "No",
                },
            ],
        }
    ) == [
        {"t": "217221323247049940788053017360663220218509556442267117653", "o": "Yes"},
        {"t": "483423353489462542133692381492404147668191676576509311866", "o": "No"},
    ]


@pytest.mark.asyncio
async def test_polymarket_resolver_retains_gamma_failure_observation() -> None:
    class FakeGamma:
        async def market(self, market_key: str, *, expiry=None):
            raise httpx.ConnectError(
                "gamma unavailable",
                request=httpx.Request("GET", f"https://gamma.test/{market_key}"),
            )

    record = await PolymarketResolutionResolver(gamma_client=FakeGamma()).resolve(
        PolymarketMarketRef("pm-1"),
        snapshot={"market_id": "pm-1"},
    )

    assert record.resolution_state == "unavailable"
    assert [observation.source for observation in record.source_observations] == [
        "polymarket_gamma",
    ]
    observation = record.source_observations[0]
    assert observation.confidence == "unavailable"
    assert observation.error_type == "ConnectError"
    assert observation.evidence == {"market_key": "pm-1", "status": "failure"}


@pytest.mark.asyncio
async def test_polymarket_resolver_retains_clob_failure_observation() -> None:
    class FakeClob:
        async def clob_market_info(self, condition_id: str, *, expiry=None):
            raise httpx.ConnectError(
                "clob unavailable",
                request=httpx.Request("GET", f"https://clob.test/{condition_id}"),
            )

    record = await PolymarketResolutionResolver(clob_client=FakeClob()).resolve(
        PolymarketMarketRef("pm-1"),
        snapshot={
            "market_id": "pm-1",
            "condition_id": "0xabc",
            "outcome_labels_json": ["yes", "no"],
        },
    )

    assert record.resolution_state == "open"
    assert [observation.source for observation in record.source_observations] == [
        "polymarket_clob",
    ]
    observation = record.source_observations[0]
    assert observation.confidence == "unavailable"
    assert observation.error_type == "ConnectError"
    assert observation.evidence == {
        "market_key": "pm-1",
        "condition_id": "0xabc",
        "status": "failure",
    }


@pytest.mark.parametrize(
    "status",
    ["unresolved", "not_resolved", "pre_resolved_review"],
)
@pytest.mark.asyncio
async def test_polymarket_resolver_status_substrings_do_not_imply_finality(
    status: str,
) -> None:
    record = await PolymarketResolutionResolver().resolve(
        PolymarketMarketRef("pm-1"),
        snapshot={
            "market_id": "pm-1",
            "condition_id": "0xabc",
            "outcome_labels_json": ["yes", "no"],
            "uma_resolution_status": status,
        },
    )

    assert record.resolution_state == "open"
    assert record.confidence == "unavailable"
    assert record.canonical_source is None
    assert record.winner is None
    assert record.payouts == []


@pytest.mark.parametrize(
    "snapshot_update",
    [
        {"closed": True},
        {"resolvedBy": "uma"},
        {"resolved_by": "uma"},
    ],
)
@pytest.mark.asyncio
async def test_polymarket_resolver_non_authoritative_metadata_alone_is_not_final(
    snapshot_update: dict[str, object],
) -> None:
    snapshot = {
        "market_id": "pm-1",
        "condition_id": "0xabc",
        "outcome_labels_json": ["yes", "no"],
    }
    snapshot.update(snapshot_update)

    record = await PolymarketResolutionResolver().resolve(PolymarketMarketRef("pm-1"), snapshot=snapshot)

    assert record.resolution_state == "open"
    assert record.confidence == "unavailable"
    assert record.canonical_source is None
    assert record.winner is None
    assert record.payouts == []


@pytest.mark.asyncio
async def test_polymarket_resolver_near_certain_prices_are_non_authoritative() -> None:
    record = await PolymarketResolutionResolver().resolve(
        PolymarketMarketRef("pm-1"),
        snapshot={
            "market_id": "pm-1",
            "condition_id": "0xabc",
            "outcome_labels_json": ["yes", "no"],
            "outcome_prices_json": ["0.995", "0.005"],
        },
    )

    assert record.resolution_state == "metadata_only"
    assert record.confidence == "metadata_only"
    assert record.canonical_source is None
    assert record.winner is None
    assert record.payouts == []
    assert record.source_observations[0].source == "polymarket_diagnostics"


@pytest.mark.asyncio
async def test_polymarket_oversized_sourced_price_is_ignored_as_invalid_evidence() -> None:
    record = await PolymarketResolutionResolver().resolve(
        PolymarketMarketRef("pm-1"),
        snapshot={
            "market_id": "pm-1",
            "condition_id": "0xabc",
            "outcome_labels_json": ["yes", "no"],
            "outcome_prices_json": [10**400, 0],
        },
    )

    assert record.resolution_state == "open"
    assert record.canonical_source is None
    assert record.source_observations == []


@pytest.mark.asyncio
async def test_polymarket_resolver_official_gamma_fields_are_non_authoritative() -> (
    None
):
    class FakeGamma:
        async def market(self, market_id: str, *, expiry=None):
            return {
                "id": market_id,
                "question": "Will it rain?",
                "conditionId": "0xabc",
                "outcomes": ["Yes", "No"],
                "outcomePrices": ["1", "0"],
                "closed": True,
                "resolvedBy": "uma",
                "questionID": "q-1",
                "umaResolutionStatus": "resolved",
                "clobTokenIds": [
                    "217221323247049940788053017360663220218509556442267117653",
                    "483423353489462542133692381492404147668191676576509311866",
                ],
            }

    record = await PolymarketResolutionResolver(gamma_client=FakeGamma()).resolve(
        PolymarketMarketRef("pm-1"),
        snapshot={"market_id": "pm-1"},
    )

    assert record.resolution_state == "metadata_only"
    assert record.confidence == "metadata_only"
    assert record.canonical_source is None
    assert record.winner is None
    assert record.payouts == []
    assert [observation.source for observation in record.source_observations] == [
        "polymarket_gamma",
        "polymarket_gamma",
        "polymarket_diagnostics",
    ]
    assert record.source_observations[0].evidence["status"] == "success"
    assert record.source_observations[1].raw_status == "resolved"


@pytest.mark.asyncio
async def test_polymarket_resolver_winner_hint_keeps_snapshot_status_observation() -> (
    None
):
    record = await PolymarketResolutionResolver().resolve(
        PolymarketMarketRef("pm-1"),
        snapshot={
            "market_id": "pm-1",
            "condition_id": "0xabc",
            "outcome_labels_json": ["yes", "no"],
            "outcome_prices_json": ["1", "0"],
            "uma_resolution_status": "resolved",
        },
    )

    assert record.resolution_state == "metadata_only"
    assert record.confidence == "metadata_only"
    assert record.canonical_source is None
    assert record.winner is None
    assert record.payouts == []
    assert [observation.source for observation in record.source_observations] == [
        "polymarket_snapshot",
        "polymarket_diagnostics",
    ]
    assert record.source_observations[0].raw_status == "resolved"


@pytest.mark.asyncio
async def test_polymarket_resolver_metadata_status_is_diagnostic_not_canonical() -> (
    None
):
    record = await PolymarketResolutionResolver().resolve(
        PolymarketMarketRef("pm-1"),
        snapshot={
            "market_id": "pm-1",
            "condition_id": "0xabc",
            "outcome_labels_json": ["yes", "no"],
            "uma_resolution_status": "resolved",
        },
    )

    assert record.resolution_state == "metadata_only"
    assert record.confidence == "metadata_only"
    assert record.canonical_source is None
    assert record.winner is None
    assert record.payouts == []
    assert len(record.source_observations) == 1
    assert record.source_observations[0].source == "polymarket_snapshot"
    assert record.source_observations[0].raw_status == "resolved"


@pytest.mark.asyncio
async def test_polymarket_resolver_ctf_denominator_zero_is_nonfinal() -> None:
    class FakeCtf:
        async def ensure_polygon(self, *, expiry=None) -> None:
            return None

        async def payout_vector(self, condition_id: str, outcome_count: int, *, expiry=None):
            return 0, [0, 0]

    resolver = PolymarketResolutionResolver(ctf_client=FakeCtf())
    record = await resolver.resolve(
        PolymarketMarketRef("pm-1"),
        snapshot={
            "market_id": "pm-1",
            "condition_id": "0xabc",
            "outcome_labels_json": ["yes", "no"],
            "closed": True,
        },
    )

    assert record.resolution_state == "open"
    assert record.confidence == "metadata_only"
    assert record.canonical_source is None
    assert record.winner is None
    assert record.payouts == []
    assert len(record.source_observations) == 1
    assert record.source_observations[0].source == "polygon_ctf"
    assert record.source_observations[0].evidence == {"denominator": 0}


@pytest.mark.parametrize("status", ["active", "open", "unresolved"])
@pytest.mark.asyncio
async def test_polymarket_resolver_ctf_denominator_zero_ignores_lifecycle_status(
    status: str,
) -> None:
    class FakeCtf:
        async def ensure_polygon(self, *, expiry=None) -> None:
            return None

        async def payout_vector(self, condition_id: str, outcome_count: int, *, expiry=None):
            return 0, [0, 0]

    record = await PolymarketResolutionResolver(ctf_client=FakeCtf()).resolve(
        PolymarketMarketRef("pm-1"),
        snapshot={
            "market_id": "pm-1",
            "condition_id": "0xabc",
            "outcome_labels_json": ["yes", "no"],
            "status": status,
        },
    )

    assert record.resolution_state == "open"
    assert record.confidence == "metadata_only"
    assert record.canonical_source is None
    assert record.winner is None
    assert record.payouts == []
    assert [observation.source for observation in record.source_observations] == [
        "polygon_ctf",
    ]


@pytest.mark.parametrize(
    "status_update",
    [
        {"status": "resolved"},
        {"umaResolutionStatus": "resolved"},
        {"uma_resolution_status": "resolved"},
    ],
)
@pytest.mark.asyncio
async def test_polymarket_resolver_ctf_denominator_zero_status_is_diagnostic(
    status_update: dict[str, str],
) -> None:
    class FakeCtf:
        async def ensure_polygon(self, *, expiry=None) -> None:
            return None

        async def payout_vector(self, condition_id: str, outcome_count: int, *, expiry=None):
            return 0, [0, 0]

    snapshot = {
        "market_id": "pm-1",
        "condition_id": "0xabc",
        "outcome_labels_json": ["yes", "no"],
    }
    snapshot.update(status_update)

    record = await PolymarketResolutionResolver(ctf_client=FakeCtf()).resolve(
        PolymarketMarketRef("pm-1"),
        snapshot=snapshot,
    )

    assert record.resolution_state == "metadata_only"
    assert record.confidence == "metadata_only"
    assert record.canonical_source is None
    assert record.winner is None
    assert record.payouts == []
    assert [observation.source for observation in record.source_observations] == [
        "polygon_ctf",
        "polymarket_snapshot",
    ]
    assert record.source_observations[0].evidence == {"denominator": 0}
    assert record.source_observations[1].raw_status == "resolved"


@pytest.mark.asyncio
async def test_polygon_ctf_client_reads_chain_and_payout_vector() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        method = payload["method"]
        if method == "eth_chainId":
            result = "0x89"
        else:
            data = payload["params"][0]["data"]
            if data.startswith("0xdd34de67"):
                result = "0x1"
            elif data.endswith("0" * 63 + "0"):
                result = "0x1"
            else:
                result = "0x0"
        return httpx.Response(
            200, json={"jsonrpc": "2.0", "id": payload["id"], "result": result}
        )

    client = PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    )
    await client.ensure_polygon()
    denominator, numerators = await client.payout_vector("0xabc", 2)
    await client.close()

    assert denominator == 1
    assert numerators == [1, 0]


@pytest.mark.asyncio
async def test_polygon_ctf_client_rejects_wrong_chain() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        return httpx.Response(
            200, json={"jsonrpc": "2.0", "id": payload["id"], "result": "0x1"}
        )

    client = PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    )
    with pytest.raises(EvmRpcError):
        await client.ensure_polygon()
    await client.close()


def test_resolution_facade_exports_supported_single_resolution_types() -> None:
    assert ExportedPolymarketResolutionResolver is PolymarketResolutionResolver
    assert ExportedKalshiResolutionResolver is KalshiResolutionResolver
    assert ExportedPolygonCtfClient is PolygonCtfClient
    assert ExportedEvmRpcError is EvmRpcError
    assert RESOLVER_VERSION == "market_resolution_resolver.v3"


@pytest.mark.asyncio
async def test_single_resolvers_accept_typed_keyword_refs() -> None:
    polymarket = await PolymarketResolutionResolver().resolve(
        market_key=PolymarketMarketRef("pm-1", condition_id="0xabc"),
        snapshot={
            "market_id": "pm-1",
            "slug": "a-slug-is-not-the-market-id",
            "conditionId": "0xabc",
            "outcomes": ["Yes", "No"],
        },
    )
    kalshi = await KalshiResolutionResolver().resolve(
        market_key=KalshiMarketRef("KXRAIN", series_ticker="KXRAIN"),
        snapshot={
            "ticker": "KXRAIN",
            "series_ticker": "KXRAIN",
            "status": "finalized",
            "settlement_value_dollars": "1",
        },
    )
    legacy = await KalshiResolutionResolver().resolve(
        market_key=KalshiMarketRef("KXLEGACY"),
        snapshot={"ticker": "KXLEGACY", "status": "open"},
    )

    assert (polymarket.market_key, polymarket.input_identifier) == ("pm-1", "pm-1")
    assert polymarket.canonical_source is None
    assert (kalshi.market_key, kalshi.input_identifier) == ("KXRAIN", "KXRAIN")
    assert kalshi.resolution_state == "metadata_only"
    assert legacy.market_key == "KXLEGACY"


@pytest.mark.asyncio
async def test_typed_ref_validation_happens_before_io() -> None:
    calls = 0

    class FakeGamma:
        async def market(self, market_key: str, *, expiry=None):
            nonlocal calls
            calls += 1
            return {"id": market_key}

    resolver = PolymarketResolutionResolver(gamma_client=FakeGamma())
    with pytest.raises(TypeError, match="PolymarketMarketRef"):
        await resolver.resolve(KalshiMarketRef("KXWRONG"))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="does not match"):
        await resolver.resolve(
            PolymarketMarketRef("pm-1", condition_id="0xabc"),
            snapshot={"market_id": "pm-other", "condition_id": "0xabc"},
        )
    with pytest.raises(ValueError, match="contradictory condition"):
        await resolver.resolve(
            PolymarketMarketRef("pm-1"),
            snapshot={
                "market_id": "pm-1",
                "condition_id": "0xabc",
                "conditionId": "0xdef",
            },
        )
    with pytest.raises(ValueError, match="non-empty string"):
        await resolver.resolve(
            PolymarketMarketRef("pm-1"),
            snapshot={"market_id": 1},
        )
    class FakeCtf:
        async def ensure_polygon(self, *, expiry=None) -> None:
            raise AssertionError("invalid condition must fail before CTF I/O")

        async def payout_vector(self, condition_id: str, outcome_count: int, *, expiry=None):
            raise AssertionError("invalid condition must fail before CTF I/O")

    with pytest.raises(ValueError, match="condition_id is not hex"):
        await PolymarketResolutionResolver(
            gamma_client=FakeGamma(), ctf_client=FakeCtf()
        ).resolve(
            PolymarketMarketRef("pm-1", condition_id="not-hex")
        )
    kalshi_resolver = KalshiResolutionResolver(FakeGamma())
    with pytest.raises(TypeError, match="KalshiMarketRef"):
        await kalshi_resolver.resolve(  # type: ignore[arg-type]
            PolymarketMarketRef("pm-wrong")
        )
    with pytest.raises(ValueError, match="contradictory series"):
        await kalshi_resolver.resolve(
            KalshiMarketRef("KXRAIN"),
            snapshot={
                "ticker": "KXRAIN",
                "series_ticker": "SERIES-A",
                "seriesTicker": "SERIES-B",
            },
        )
    assert calls == 0


@pytest.mark.asyncio
async def test_returned_typed_identity_conflicts_are_source_evidence() -> None:
    class FakeGamma:
        async def market(self, market_key: str, *, expiry=None):
            return {
                "id": market_key,
                "market_id": "pm-other",
                "conditionId": "0xabc",
            }

    class FakeKalshi:
        async def market(self, ticker: str, *, expiry=None):
            return {"ticker": ticker, "market_key": "OTHER"}

        async def historical_market(self, ticker: str, *, expiry=None):
            return {"ticker": ticker, "series_ticker": "OTHER-SERIES"}

    class ContradictoryGamma:
        async def market(self, market_key: str, *, expiry=None):
            return {
                "id": market_key,
                "condition_id": "0xabc",
                "conditionId": "0xdef",
            }

    polymarket = await PolymarketResolutionResolver(
        gamma_client=FakeGamma()
    ).resolve(PolymarketMarketRef("pm-1"))
    contradictory_condition = await PolymarketResolutionResolver(
        gamma_client=ContradictoryGamma()
    ).resolve(PolymarketMarketRef("pm-1"))
    kalshi = await KalshiResolutionResolver(FakeKalshi()).resolve(
        KalshiMarketRef("KXRAIN", series_ticker="SERIES")
    )

    assert polymarket.resolution_state == "unavailable"
    assert polymarket.source_observations[0].error_type == (
        "InvalidResolutionEvidenceError"
    )
    assert contradictory_condition.source_observations[0].error_type == (
        "InvalidResolutionEvidenceError"
    )
    assert kalshi.resolution_state == "unavailable"
    assert {
        observation.error_type for observation in kalshi.source_observations
    } == {"InvalidResolutionEvidenceError"}


@pytest.mark.asyncio
async def test_programmer_errors_escape_single_resolvers() -> None:
    class BrokenGamma:
        async def market(self, market_key: str, *, expiry=None):
            raise RuntimeError("injected gamma bug")

    class BrokenKalshi:
        async def market(self, ticker: str, *, expiry=None):
            raise AttributeError("injected kalshi bug")

        async def historical_market(self, ticker: str, *, expiry=None):
            return {"ticker": ticker}

    class BrokenCtf:
        async def ensure_polygon(self, *, expiry=None) -> None:
            raise AssertionError("injected CTF bug")

        async def payout_vector(self, condition_id: str, outcome_count: int, *, expiry=None):
            return 1, [1, 0]

    with pytest.raises(RuntimeError, match="injected gamma bug"):
        await PolymarketResolutionResolver(gamma_client=BrokenGamma()).resolve(PolymarketMarketRef("pm-1"))
    with pytest.raises(AttributeError, match="injected kalshi bug"):
        await KalshiResolutionResolver(BrokenKalshi()).resolve(KalshiMarketRef("KXRAIN"))
    with pytest.raises(AssertionError, match="injected CTF bug"):
        await PolymarketResolutionResolver(ctf_client=BrokenCtf()).resolve(
            PolymarketMarketRef("pm-1"),
            snapshot={
                "market_id": "pm-1",
                "condition_id": "0xabc",
                "outcomes": ["Yes", "No"],
            },
        )


@pytest.mark.asyncio
async def test_kalshi_read_auth_signal_escapes_but_http_403_is_evidence() -> None:
    class MissingAuth:
        def headers_for_get(self, path: str) -> dict[str, str]:
            raise ReadAuthenticationRequiredError("auth marker secret")

    def should_not_run(request: httpx.Request) -> httpx.Response:
        raise AssertionError("network must not run after read-auth failure")

    auth_client = AsyncKalshiClient(
        base_url="https://kalshi.test",
        auth=MissingAuth(),
        transport=httpx.MockTransport(should_not_run),
    )
    try:
        with pytest.raises(ReadAuthenticationRequiredError):
            await KalshiResolutionResolver(auth_client).resolve(KalshiMarketRef("KXRAIN"))
    finally:
        await auth_client.close()

    def forbidden(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, content=b"REMOTE-BODY-MARKER")

    http_client = AsyncKalshiClient(
        base_url="https://user:password@kalshi.test/private-path?query=QUERY-MARKER",
        transport=httpx.MockTransport(forbidden),
        request_policy=RequestPolicy(max_attempts=1),
    )
    try:
        record = await KalshiResolutionResolver(http_client).resolve(KalshiMarketRef("KXRAIN"))
    finally:
        await http_client.close()

    serialized = json.dumps(record.to_row(), default=str)
    assert record.error_type == "HTTPStatusError"
    assert record.error_message == "kalshi_historical_rest returned HTTP 403"
    for marker in ("password", "private-path", "QUERY-MARKER", "REMOTE-BODY-MARKER"):
        assert marker not in serialized


@pytest.mark.asyncio
async def test_invalid_json_and_malformed_gamma_condition_are_source_evidence() -> None:
    def invalid_json(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not-json")

    client = AsyncGammaClient(
        base_url="https://gamma.test",
        transport=httpx.MockTransport(invalid_json),
        request_policy=RequestPolicy(max_attempts=1),
    )
    try:
        invalid = await PolymarketResolutionResolver(gamma_client=client).resolve(
            PolymarketMarketRef("pm-1")
        )
    finally:
        await client.close()

    class MalformedGamma:
        async def market(self, market_key: str, *, expiry=None):
            return {
                "id": market_key,
                "conditionId": "not-hex",
                "outcomes": ["Yes", "No"],
            }

    class UnusedCtf:
        async def ensure_polygon(self, *, expiry=None) -> None:
            raise AssertionError("malformed Gamma condition must block CTF I/O")

        async def payout_vector(self, condition_id: str, outcome_count: int, *, expiry=None):
            raise AssertionError("malformed Gamma condition must block CTF I/O")

    malformed = await PolymarketResolutionResolver(
        gamma_client=MalformedGamma(),
        ctf_client=UnusedCtf(),
    ).resolve(PolymarketMarketRef("pm-2"))

    assert invalid.source_observations[0].error_type == "JSONDecodeError"
    assert malformed.source_observations[0].error_type == (
        "InvalidResolutionEvidenceError"
    )


def test_resolution_error_record_sanitizes_external_exception_text() -> None:
    request = httpx.Request(
        "GET", "https://user:password@example.test/private?token=QUERY-MARKER"
    )
    response = httpx.Response(500, request=request, content=b"BODY-MARKER")
    error = httpx.HTTPStatusError(
        "remote message marker", request=request, response=response
    )

    record = error_record(
        platform="kalshi",
        market_key="KXRAIN",
        input_identifier="KXRAIN",
        error=error,
    )
    serialized = json.dumps(record.to_row(), default=str)

    assert record.error_message == "kalshi returned HTTP 500"
    for marker in (
        "password",
        "private",
        "QUERY-MARKER",
        "BODY-MARKER",
        "remote message marker",
    ):
        assert marker not in serialized


@pytest.mark.asyncio
async def test_persisted_rpc_error_does_not_include_rpc_url_or_remote_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"RPC-BODY-MARKER")

    client = PolygonCtfClient(
        "https://user:password@rpc.test/private-path?token=RPC-QUERY-MARKER",
        transport=httpx.MockTransport(handler),
    )
    try:
        record = await PolymarketResolutionResolver(ctf_client=client).resolve(
            PolymarketMarketRef("pm-1"),
            snapshot={
                "market_id": "pm-1",
                "condition_id": "0xabc",
                "outcomes": ["Yes", "No"],
            },
        )
    finally:
        await client.close()

    serialized = json.dumps(record.to_row(), default=str)
    assert record.canonical_source is None
    assert record.source_observations[0].error_message == (
        "polygon_ctf returned HTTP 500"
    )
    for marker in (
        "password",
        "private-path",
        "RPC-QUERY-MARKER",
        "RPC-BODY-MARKER",
    ):
        assert marker not in serialized


@pytest.mark.parametrize(
    ("payload_kind", "expected_type", "expected_message"),
    [
        ("rpc_error", "EvmRpcError", "polygon_ctf failed (EvmRpcError)"),
        ("invalid_encoding", "UnicodeDecodeError", "polygon_ctf returned invalid JSON"),
    ],
)
@pytest.mark.asyncio
async def test_persisted_rpc_decode_errors_are_sanitized(
    payload_kind: str,
    expected_type: str,
    expected_message: str,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if payload_kind == "invalid_encoding":
            return httpx.Response(200, content=bytes([255]))
        payload = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": payload["id"],
                "error": {
                    "message": "RPC-ERROR-BODY-MARKER",
                    "data": "RPC-HEADER-MARKER",
                },
            },
        )

    client = PolygonCtfClient(
        "https://user:password@rpc.test/private?token=RPC-QUERY-MARKER",
        transport=httpx.MockTransport(handler),
    )
    client._client.headers["Authorization"] = "RPC-AUTH-HEADER-MARKER"
    try:
        record = await PolymarketResolutionResolver(ctf_client=client).resolve(
            PolymarketMarketRef("pm-1"),
            snapshot={
                "market_id": "pm-1",
                "condition_id": "0xabc",
                "outcomes": ["Yes", "No"],
            },
        )
    finally:
        await client.close()

    [observation] = record.source_observations
    serialized = json.dumps(record.to_row(), default=str)
    assert observation.error_type == expected_type
    assert observation.error_message == expected_message
    for marker in (
        "password",
        "private",
        "RPC-QUERY-MARKER",
        "RPC-ERROR-BODY-MARKER",
        "RPC-HEADER-MARKER",
        "RPC-AUTH-HEADER-MARKER",
    ):
        assert marker not in serialized


@pytest.mark.asyncio
async def test_single_deadline_expires_while_waiting_for_limiter_without_request() -> None:
    entered = asyncio.Event()
    drained = asyncio.Event()
    block = False
    request_count = 0

    class ControlledLimiter:
        async def __aenter__(self) -> None:
            if not block:
                return
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0)
                drained.set()
                raise

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(200, json={"id": "pm-1"})

    client = AsyncGammaClient(
        base_url="https://gamma.test",
        limiter=ControlledLimiter(),  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        request_policy=RequestPolicy(max_attempts=1),
    )
    resolver = PolymarketResolutionResolver(gamma_client=client)
    try:
        await resolver.resolve(PolymarketMarketRef("pm-1"), deadline_s=1.0)  # warm the HTTP client
        block = True
        with pytest.raises(OperationTimeoutError):
            await resolver.resolve(PolymarketMarketRef("pm-1"), deadline_s=0.05)
        assert entered.is_set()
        assert drained.is_set()
        assert request_count == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_single_deadline_expires_in_retry_backoff_without_second_attempt() -> None:
    fail = False
    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        if fail:
            return httpx.Response(503, headers={"Retry-After": "1"})
        return httpx.Response(200, json={"id": "pm-1"})

    client = AsyncGammaClient(
        base_url="https://gamma.test",
        transport=httpx.MockTransport(handler),
        request_policy=RequestPolicy(max_attempts=2),
    )
    resolver = PolymarketResolutionResolver(gamma_client=client)
    try:
        await resolver.resolve(PolymarketMarketRef("pm-1"), deadline_s=1.0)
        fail = True
        with pytest.raises(OperationTimeoutError):
            await resolver.resolve(PolymarketMarketRef("pm-1"), deadline_s=0.05)
        await asyncio.sleep(0)
        assert request_count == 2
        fail = False
        record = await resolver.resolve(PolymarketMarketRef("pm-1"), deadline_s=1.0)
    finally:
        await client.close()

    assert record.market_key == "pm-1"


@pytest.mark.parametrize("failure", ["timeout", "caller_cancel"])
@pytest.mark.asyncio
async def test_transport_cleanup_completes_before_failure_and_client_reuses(
    failure: str,
) -> None:
    entered = asyncio.Event()
    drained = asyncio.Event()
    block = False

    async def handler(request: httpx.Request) -> httpx.Response:
        if block:
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0)
                drained.set()
                raise
        return httpx.Response(200, json={"id": "pm-1"})

    client = AsyncGammaClient(
        base_url="https://gamma.test",
        transport=httpx.MockTransport(handler),
        request_policy=RequestPolicy(max_attempts=1),
    )
    resolver = PolymarketResolutionResolver(gamma_client=client)
    try:
        await resolver.resolve(PolymarketMarketRef("pm-1"), deadline_s=1.0)
        block = True
        task = asyncio.create_task(
            resolver.resolve(
                PolymarketMarketRef("pm-1"),
                deadline_s=0.05 if failure == "timeout" else None,
            )
        )
        await asyncio.wait_for(entered.wait(), timeout=1.0)
        if failure == "caller_cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(OperationTimeoutError):
                await task
        assert drained.is_set()
        block = False
        record = await resolver.resolve(PolymarketMarketRef("pm-1"), deadline_s=1.0)
    finally:
        await client.close()

    assert record.market_key == "pm-1"


@pytest.mark.parametrize("stage", ["chain", "payout"])
@pytest.mark.parametrize("failure", ["timeout", "caller_cancel"])
@pytest.mark.asyncio
async def test_rpc_failure_drains_owned_await_and_client_is_reusable(
    stage: str,
    failure: str,
) -> None:
    entered = asyncio.Event()
    drained = asyncio.Event()
    block = True

    async def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        method = payload["method"]
        should_block = (stage == "chain" and method == "eth_chainId") or (
            stage == "payout" and method == "eth_call"
        )
        if block and should_block:
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0)
                drained.set()
                raise
        data = (
            payload.get("params", [{}])[0].get("data", "")
            if method == "eth_call"
            else ""
        )
        if method == "eth_chainId":
            result = "0x89"
        elif data.startswith("0xdd34de67") or data.endswith("0" * 64):
            result = "0x1"
        else:
            result = "0x0"
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": payload["id"], "result": result},
        )

    client = PolygonCtfClient(
        "https://rpc.test",
        transport=httpx.MockTransport(handler),
    )
    resolver = PolymarketResolutionResolver(ctf_client=client)
    snapshot = {
        "market_id": "pm-1",
        "condition_id": "0xabc",
        "outcomes": ["Yes", "No"],
    }
    try:
        task = asyncio.create_task(
            resolver.resolve(
                PolymarketMarketRef("pm-1"),
                snapshot=snapshot,
                deadline_s=0.05 if failure == "timeout" else None,
            )
        )
        await asyncio.wait_for(entered.wait(), timeout=1.0)
        if failure == "caller_cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(OperationTimeoutError):
                await task
        assert drained.is_set()
        assert resolver._ctf_chain_checked is (stage == "payout")
        block = False
        record = await resolver.resolve(PolymarketMarketRef("pm-1"), snapshot=snapshot, deadline_s=1.0)
    finally:
        await client.close()

    assert resolver._ctf_chain_checked is True
    assert record.canonical_source == "polygon_ctf"


@pytest.mark.asyncio
async def test_ctf_chain_success_is_cached_only_after_expiry_checkpoint() -> None:
    now = 0.0

    class Ctf:
        async def ensure_polygon(self, *, expiry=None) -> None:
            nonlocal now
            now = 2.0

        async def payout_vector(self, condition_id: str, outcome_count: int, *, expiry=None):
            raise AssertionError("expired chain validation must stop payout reads")

    resolver = PolymarketResolutionResolver(ctf_client=Ctf())
    expiry = OperationExpiry.after(1.0, clock=lambda: now)

    with pytest.raises(OperationTimeoutError):
        await resolver._resolve_with_expiry(
            PolymarketMarketRef("pm-1"),
            snapshot={
                "market_id": "pm-1",
                "condition_id": "0xabc",
                "outcomes": ["Yes", "No"],
            },
            expiry=expiry,
        )

    assert resolver._ctf_chain_checked is False


@pytest.mark.asyncio
async def test_late_normalization_checkpoint_uses_original_expiry() -> None:
    now = 0.0

    class SlowFloat:
        def __float__(self) -> float:
            nonlocal now
            now = 2.0
            return 0.5

    class Gamma:
        async def market(self, market_key: str, *, expiry=None):
            return {
                "id": market_key,
                "outcomes": ["Yes", "No"],
                "outcomePrices": [SlowFloat(), 0.5],
            }

    resolver = PolymarketResolutionResolver(gamma_client=Gamma())
    expiry = OperationExpiry.after(1.0, clock=lambda: now)

    with pytest.raises(OperationTimeoutError):
        await resolver._resolve_with_expiry(
            PolymarketMarketRef("pm-1"),
            snapshot=None,
            expiry=expiry,
        )


@pytest.mark.asyncio
async def test_provider_methods_receive_the_shared_resolution_expiry() -> None:
    gamma_called = False
    ctf_calls: list[str] = []

    class Gamma(AsyncGammaClient):
        async def market(self, market_id: str | int, *, expiry=None) -> dict[str, object]:
            nonlocal gamma_called
            gamma_called = True
            return {
                "id": str(market_id),
                "conditionId": "0xabc",
                "outcomes": ["Yes", "No"],
            }

    class Ctf(PolygonCtfClient):
        async def ensure_polygon(self, *, expiry=None) -> None:
            ctf_calls.append("chain")

        async def payout_vector(
            self, condition_id: str, outcome_count: int, *, expiry=None
        ) -> tuple[int, list[int]]:
            ctf_calls.append("payout")
            return 1, [1, 0]

    gamma = Gamma(base_url="https://gamma.test")
    ctf = Ctf("https://rpc.test")
    try:
        record = await PolymarketResolutionResolver(
            gamma_client=gamma, ctf_client=ctf
        ).resolve(PolymarketMarketRef("pm-1"), deadline_s=1.0)
    finally:
        await gamma.close()
        await ctf.close()

    assert gamma_called
    assert ctf_calls == ["chain", "payout"]
    assert record.canonical_source == "polygon_ctf"


@pytest.mark.asyncio
async def test_polygon_ctf_malformed_uint_result_is_evm_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": payload["id"], "result": "0xnope"},
        )

    client = PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    )
    try:
        with pytest.raises(EvmRpcError, match="malformed uint256"):
            await client.payout_denominator("0xabc")
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_resolution_cache_writes_parquet_and_summary(tmp_path) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    pd.DataFrame(
        [
            {
                "schema_version": "polymarket_market_snapshot.v1",
                "market_id": "pm-1",
                "question": "Will it rain?",
                "condition_id": "0xabc",
                "outcome_labels_json": ["yes", "no"],
            }
        ]
    ).to_parquet(polymarket_path, index=False)
    pd.DataFrame(
        [
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "market_key": "KXRAIN",
                "exchange": "kalshi",
                "question": "Will it rain?",
                "status": "finalized",
                "result": "yes",
                "settlement_value_dollars": "1",
            }
        ]
    ).to_parquet(kalshi_path, index=False)

    class FakeClob:
        async def clob_market_info(self, condition_id: str, *, expiry=None):
            return {
                "tokens": [
                    {"t": "token-yes", "o": "Yes"},
                    {"t": "token-no", "o": "No"},
                ]
            }

    class FakeGamma:
        async def market(self, market_id: str, *, expiry=None):
            return {}

    class FakeKalshi:
        async def market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "finalized",
                "result": "yes",
                "settlement_value_dollars": "1",
            }

        async def historical_market(self, ticker: str, *, expiry=None):
            return {}

    summary = await resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        gamma_client=FakeGamma(),
        clob_client=FakeClob(),
        kalshi_client=FakeKalshi(),
    )

    assert summary["row_count"] == 2
    assert (output_dir / "market_resolutions.parquet").exists()
    assert (output_dir / "summary.json").exists()
    written = pd.read_parquet(output_dir / "market_resolutions.parquet")
    assert set(written["platform"]) == {"polymarket", "kalshi"}


@pytest.mark.asyncio
async def test_resolution_cache_refresh_keeps_existing_canonical_on_weak_refresh(
    tmp_path,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    pd.DataFrame(columns=["schema_version", "market_id", "question"]).to_parquet(
        polymarket_path,
        index=False,
    )
    pd.DataFrame(
        [
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "market_key": "KXRAIN",
                "exchange": "kalshi",
                "question": "Will it rain?",
                "status": "active",
            }
        ]
    ).to_parquet(kalshi_path, index=False)
    pd.DataFrame(
        [
            market_resolution_row(
                platform="kalshi",
                market_key="KXRAIN",
                input_identifier="KXRAIN",
                resolution_state="final",
                result_type="binary",
                confidence="canonical",
                canonical_source="kalshi_rest",
                result="yes",
                winner="yes",
                observed_at_utc="2026-06-21T00:00:00+00:00",
                resolver_version=RESOLVER_VERSION,
            )
        ]
    ).to_parquet(output_dir / "market_resolutions.parquet", index=False)

    class WeakKalshi:
        async def market(self, ticker: str, *, expiry=None):
            return {"ticker": ticker, "status": "active"}

        async def historical_market(self, ticker: str, *, expiry=None):
            return {"ticker": ticker}

    await resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        kalshi_client=WeakKalshi(),
        refresh=True,
    )

    written = pd.read_parquet(output_dir / "market_resolutions.parquet")
    assert len(written) == 1
    [row] = written.to_dict("records")
    assert row["platform"] == "kalshi"
    assert row["market_key"] == "KXRAIN"
    assert row["resolution_state"] == "final"
    assert row["confidence"] == "canonical"
    assert row["winner"] == "yes"


@pytest.mark.asyncio
async def test_resolution_cache_refresh_records_fresh_authority_conflict(
    tmp_path,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    pd.DataFrame(columns=["schema_version", "market_id", "question"]).to_parquet(
        polymarket_path,
        index=False,
    )
    pd.DataFrame(
        [
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "market_key": "KXRAIN",
                "exchange": "kalshi",
                "question": "Will it rain?",
            }
        ]
    ).to_parquet(kalshi_path, index=False)
    pd.DataFrame(
        [
            market_resolution_row(
                platform="kalshi",
                market_key="KXRAIN",
                input_identifier="KXRAIN",
                resolution_state="final",
                result_type="binary",
                confidence="canonical",
                canonical_source="kalshi_rest",
                result="yes",
                winner="yes",
                observed_at_utc="2026-06-21T00:00:00+00:00",
                resolver_version=RESOLVER_VERSION,
            )
        ]
    ).to_parquet(output_dir / "market_resolutions.parquet", index=False)

    class ConflictingKalshi:
        async def market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "finalized",
                "settlement_value_dollars": "1",
            }

        async def historical_market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "finalized",
                "settlement_value_dollars": "0",
            }

    await resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        kalshi_client=ConflictingKalshi(),
        refresh=True,
    )

    written = pd.read_parquet(output_dir / "market_resolutions.parquet")
    assert len(written) == 1
    [row] = written.to_dict("records")
    assert row["resolution_state"] == "inconsistent"
    assert row["confidence"] == "inconsistent"
    assert row["error_type"] == "KalshiAuthorityConflict"


@pytest.mark.asyncio
async def test_resolution_cache_refresh_records_invalid_ctf_vector(tmp_path) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    pd.DataFrame(
        [
            {
                "schema_version": "polymarket_market_snapshot.v1",
                "market_id": "pm-1",
                "question": "Will it rain?",
                "condition_id": "0xabc",
                "outcome_labels_json": ["yes", "no"],
            }
        ]
    ).to_parquet(polymarket_path, index=False)
    pd.DataFrame(columns=["schema_version", "market_key", "question"]).to_parquet(
        kalshi_path,
        index=False,
    )
    pd.DataFrame(
        [
            market_resolution_row(
                platform="polymarket",
                market_key="pm-1",
                input_identifier="pm-1",
                resolution_state="final",
                result_type="binary",
                confidence="canonical",
                canonical_source="polygon_ctf",
                result="yes",
                winner="yes",
                observed_at_utc="2026-06-21T00:00:00+00:00",
                resolver_version=RESOLVER_VERSION,
            )
        ]
    ).to_parquet(output_dir / "market_resolutions.parquet", index=False)

    class InvalidCtf:
        async def ensure_polygon(self, *, expiry=None) -> None:
            return None

        async def payout_vector(self, condition_id: str, outcome_count: int, *, expiry=None):
            assert condition_id == "0xabc"
            assert outcome_count == 2
            return 1, [1, 1]

    class EmptyClob:
        async def clob_market_info(self, condition_id: str, *, expiry=None):
            return {}

    await resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        clob_client=EmptyClob(),
        ctf_client=InvalidCtf(),
        refresh=True,
    )

    written = pd.read_parquet(output_dir / "market_resolutions.parquet")
    assert len(written) == 1
    [row] = written.to_dict("records")
    assert row["platform"] == "polymarket"
    assert row["market_key"] == "pm-1"
    assert row["resolution_state"] == "inconsistent"
    assert row["confidence"] == "inconsistent"
    assert row["error_type"] == "InvalidPayoutVector"


@pytest.mark.asyncio
async def test_resolution_cache_retained_finals_are_universe_and_version_aware(
    tmp_path,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    matches_path = tmp_path / "matches.parquet"
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    pd.DataFrame(columns=["schema_version", "market_id", "question"]).to_parquet(
        polymarket_path,
        index=False,
    )
    pd.DataFrame(
        [
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "market_key": "KXRAIN",
                "exchange": "kalshi",
                "question": "Will it rain?",
            },
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "market_key": "KXOLD",
                "exchange": "kalshi",
                "question": "Old market?",
            },
        ]
    ).to_parquet(kalshi_path, index=False)
    pd.DataFrame([{"kalshi_market_key": "KXRAIN"}]).to_parquet(
        matches_path,
        index=False,
    )
    pd.DataFrame(
        [
            market_resolution_row(
                platform="kalshi",
                market_key="KXRAIN",
                input_identifier="KXRAIN",
                resolution_state="final",
                result_type="binary",
                confidence="canonical",
                canonical_source="kalshi_rest",
                result="no",
                winner="no",
                observed_at_utc="2026-06-20T00:00:00+00:00",
                resolver_version="market_resolution_resolver.v0",
            ),
            market_resolution_row(
                platform="kalshi",
                market_key="KXOLD",
                input_identifier="KXOLD",
                resolution_state="final",
                result_type="binary",
                confidence="canonical",
                canonical_source="kalshi_rest",
                result="yes",
                winner="yes",
                observed_at_utc="2026-06-20T00:00:00+00:00",
                resolver_version=RESOLVER_VERSION,
            ),
        ]
    ).to_parquet(output_dir / "market_resolutions.parquet", index=False)

    class FreshKalshi:
        async def market(self, ticker: str, *, expiry=None):
            return {
                "ticker": ticker,
                "status": "finalized",
                "settlement_value_dollars": "1",
            }

        async def historical_market(self, ticker: str, *, expiry=None):
            return {}

    await resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        matches_path=matches_path,
        output_dir=output_dir,
        kalshi_client=FreshKalshi(),
    )

    written = pd.read_parquet(output_dir / "market_resolutions.parquet")
    assert written["market_key"].tolist() == ["KXRAIN"]
    [row] = written.to_dict("records")
    assert row["resolver_version"] == RESOLVER_VERSION
    assert row["resolution_state"] == "final"
    assert row["winner"] == "yes"


@pytest.mark.asyncio
async def test_resolution_rejects_untyped_identifiers() -> None:
    with pytest.raises(TypeError, match="PolymarketMarketRef"):
        await PolymarketResolutionResolver().resolve("pm-1")
    with pytest.raises(TypeError, match="KalshiMarketRef"):
        await KalshiResolutionResolver().resolve("KX-ONE")
