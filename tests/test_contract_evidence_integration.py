from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest

from pmkt.data.contract_evidence import contract_evidence_dataframe
from pmkt.data.registry import CONTRACT_EVIDENCE_SCHEMA_VERSION
from pmkt.data.validation import validate_frame
from pmkt.exchanges.kalshi.client import AsyncKalshiClient
from pmkt.exchanges.polymarket.gamma import AsyncGammaClient

RUN_INTEGRATION = os.getenv("PMKT_RUN_CONTRACT_EVIDENCE_INTEGRATION") == "1"


@pytest.mark.asyncio
@pytest.mark.skipif(
    not RUN_INTEGRATION,
    reason=(
        "Set PMKT_RUN_CONTRACT_EVIDENCE_INTEGRATION=1 to run bounded public "
        "contract-evidence API smoke tests."
    ),
)
async def test_public_venue_contract_evidence_smoke() -> None:
    async with AsyncGammaClient() as polymarket:
        polymarket_rows = await polymarket.markets_raw_page(
            limit=1, offset=0, closed=False
        )
        polymarket_observed_at = datetime.now(timezone.utc).isoformat()
    assert polymarket_rows
    polymarket_evidence = contract_evidence_dataframe(
        polymarket_rows,
        venue="polymarket",
        source_endpoint="gamma:/markets",
        payload_scope="integration_list",
        observed_at_utc=polymarket_observed_at,
    )
    assert validate_frame(
        polymarket_evidence, CONTRACT_EVIDENCE_SCHEMA_VERSION, strict=True
    ).ok

    async with AsyncKalshiClient() as kalshi:
        kalshi_page = await kalshi.markets_page(limit=1, status="open")
        kalshi_observed_at = datetime.now(timezone.utc).isoformat()
    kalshi_rows = kalshi_page.get("markets")
    assert isinstance(kalshi_rows, list) and kalshi_rows
    kalshi_evidence = contract_evidence_dataframe(
        kalshi_rows,
        venue="kalshi",
        source_endpoint="kalshi:/markets",
        payload_scope="integration_list",
        observed_at_utc=kalshi_observed_at,
    )
    assert validate_frame(
        kalshi_evidence, CONTRACT_EVIDENCE_SCHEMA_VERSION, strict=True
    ).ok
