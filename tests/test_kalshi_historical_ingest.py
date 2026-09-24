from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from pmkt.cli import ingest
from pmkt.data.contract_evidence_manifest import verify_contract_evidence_manifest


def _market(ticker: str) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "event_ticker": "KXOLDER-26JAN01",
        "status": "settled",
        "result": "yes",
        "settlement_ts": "2026-01-02T00:00:00Z",
        "close_time": "2026-01-01T00:00:00Z",
        "rules_primary": "Resolves yes when the event occurs.",
    }


class FakeKalshi:
    def __init__(self, pages: dict[str | None, dict[str, Any]]) -> None:
        self.pages = pages
        self.cursors: list[str | None] = []
        self.filters: list[tuple[str | None, str | None, str | None]] = []

    async def __aenter__(self) -> FakeKalshi:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def historical_markets_page(
        self,
        *,
        cursor: str | None,
        event_ticker: str | None,
        series_ticker: str | None,
        tickers: str | None,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        self.cursors.append(cursor)
        self.filters.append((event_ticker, series_ticker, tickers))
        return self.pages[cursor]


def _run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake: FakeKalshi,
    *,
    complete: bool,
    evidence: bool = False,
    series_ticker: str | None = "KXOLDER",
) -> tuple[Path, Path]:
    monkeypatch.setattr(ingest, "AsyncKalshiClient", lambda **_kwargs: fake)
    out = tmp_path / "markets.parquet"
    manifest = tmp_path / "collection.json"
    asyncio.run(
        ingest._ingest_kalshi_historical_markets_async(
            out,
            limit=2,
            max_pages=1,
            complete=complete,
            event_ticker=None,
            series_ticker=series_ticker,
            tickers=None,
            manifest_out=manifest,
            contract_evidence_out=(tmp_path / "evidence.parquet") if evidence else None,
        )
    )
    return out, manifest


def test_historical_ingest_bounded_records_cursor_and_archive_fields(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = FakeKalshi({None: {"markets": [_market("KXOLD-ONE")], "cursor": "next"}})

    out, manifest_path = _run(monkeypatch, tmp_path, fake, complete=False)

    assert fake.cursors == [None]
    assert fake.filters == [(None, "KXOLDER", None)]
    row = pd.read_parquet(out).iloc[0]
    assert row["status"] == "finalized"
    assert row["result"] == "yes"
    assert row["settlement_ts"] == "2026-01-02T00:00:00Z"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["endpoint"] == "/historical/markets"
    assert manifest["filters"]["series_ticker"] == "KXOLDER"
    assert manifest["final_cursor"] == "next"
    assert manifest["collection_complete"] is False
    assert manifest["unique_market_count"] == 1


def test_historical_ingest_complete_follows_empty_page_and_verifies_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = FakeKalshi(
        {
            None: {"markets": [_market("KXOLD-ONE")], "cursor": "next"},
            "next": {"markets": [], "cursor": "last"},
            "last": {"markets": [_market("KXOLD-TWO")], "cursor": ""},
        }
    )

    out, manifest_path = _run(
        monkeypatch, tmp_path, fake, complete=True, evidence=True, series_ticker=None
    )

    assert fake.cursors == [None, "next", "last"]
    assert fake.filters == [(None, None, None)] * 3
    assert len(pd.read_parquet(out)) == 2
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["collection_complete"] is True
    assert manifest["stop_reason"] == "cursor_exhausted"
    assert manifest["page_count"] == 3
    evidence_path = Path(manifest["dataset_paths"]["contract_evidence"])
    evidence_manifest = Path(manifest["dataset_paths"]["contract_evidence_manifest"])
    verified = verify_contract_evidence_manifest(
        pd.read_parquet(evidence_path),
        artifact_path=evidence_path,
        manifest_path=evidence_manifest,
        expected_venue="kalshi",
        expected_source_endpoint="kalshi:/historical/markets",
        expected_payload_scope="cursor_list",
        expected_observation_time_source="capture_clock",
        expected_source_payload_kind="venue_api_response",
    )
    assert verified["authoritative_complete"] is True


def test_historical_ingest_rejects_duplicate_tickers_before_publication(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = FakeKalshi(
        {
            None: {"markets": [_market("KXOLD-ONE")], "cursor": "next"},
            "next": {"markets": [_market("KXOLD-ONE")], "cursor": ""},
        }
    )
    monkeypatch.setattr(ingest, "AsyncKalshiClient", lambda **_kwargs: fake)
    out = tmp_path / "markets.parquet"

    with pytest.raises(ValueError, match="duplicate Kalshi historical market key"):
        asyncio.run(
            ingest._ingest_kalshi_historical_markets_async(
                out,
                limit=2,
                max_pages=1,
                complete=True,
                event_ticker=None,
                series_ticker=None,
                tickers=None,
                manifest_out=tmp_path / "collection.json",
                contract_evidence_out=None,
            )
        )
    assert not out.exists()
    assert not (tmp_path / "collection.json").exists()


def test_historical_ingest_rejects_mixed_filters_before_network(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        asyncio.run(
            ingest._ingest_kalshi_historical_markets_async(
                tmp_path / "markets.parquet",
                limit=2,
                max_pages=1,
                complete=False,
                event_ticker="KXEVENT",
                series_ticker="KXSERIES",
                tickers=None,
                manifest_out=None,
                contract_evidence_out=None,
            )
        )


def test_historical_ingest_rejects_repeated_cursor_without_publishing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = FakeKalshi(
        {
            None: {"markets": [_market("KXOLD-ONE")], "cursor": "same"},
            "same": {"markets": [], "cursor": "same"},
        }
    )
    monkeypatch.setattr(ingest, "AsyncKalshiClient", lambda **_kwargs: fake)
    out = tmp_path / "markets.parquet"

    with pytest.raises(RuntimeError, match="cursor repeated"):
        asyncio.run(
            ingest._ingest_kalshi_historical_markets_async(
                out,
                limit=2,
                max_pages=1,
                complete=True,
                event_ticker=None,
                series_ticker=None,
                tickers=None,
                manifest_out=tmp_path / "collection.json",
                contract_evidence_out=None,
            )
        )
    assert not out.exists()
