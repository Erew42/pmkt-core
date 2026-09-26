from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import pytest
from typer.testing import CliRunner

from pmkt.cli import ingest
from pmkt.cli.app import app
from pmkt.data.contract_evidence_manifest import file_sha256, verify_contract_evidence_manifest


def _market(ticker: str) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "event_ticker": "KXOLDER-26JAN01",
        "status": "finalized",
        "result": "yes",
        "settlement_ts": "2026-01-02T00:00:00Z",
        "close_time": "2026-01-01T00:00:00Z",
        "rules_primary": "Resolves yes when the event occurs.",
    }


class FakeKalshi:
    def __init__(
        self,
        pages: dict[str | None, dict[str, Any]],
        *,
        cutoffs: list[str] | None = None,
        on_page: Callable[[str | None], None] | None = None,
    ) -> None:
        self.pages = pages
        self.cutoffs = cutoffs or ["2026-07-25T00:00:00Z"]
        self.cutoff_calls = 0
        self.on_page = on_page
        self.cursors: list[str | None] = []
        self.limits: list[int] = []
        self.filters: list[tuple[str | None, str | None, str | None, str | None]] = []

    async def __aenter__(self) -> FakeKalshi:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def historical_markets_page(
        self,
        *,
        limit: int,
        cursor: str | None,
        event_ticker: str | None,
        series_ticker: str | None,
        tickers: str | None,
        mve_filter: str | None,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        self.cursors.append(cursor)
        self.limits.append(limit)
        self.filters.append((event_ticker, series_ticker, tickers, mve_filter))
        if self.on_page is not None:
            self.on_page(cursor)
        return self.pages[cursor]

    async def historical_cutoff(self) -> dict[str, str]:
        value = self.cutoffs[min(self.cutoff_calls, len(self.cutoffs) - 1)]
        self.cutoff_calls += 1
        return {"market_settled_ts": value}


def _run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake: FakeKalshi,
    *,
    complete: bool,
    evidence: bool = False,
    series_ticker: str | None = "KXOLDER",
    mve_filter: str | None = None,
    resume_from: Path | None = None,
    max_pages: int = 1,
    name: str = "collection",
) -> tuple[Path, Path]:
    monkeypatch.setattr(ingest, "AsyncKalshiClient", lambda **_kwargs: fake)
    out = tmp_path / f"{name}.parquet"
    manifest = tmp_path / f"{name}.json"
    asyncio.run(
        ingest._ingest_kalshi_historical_markets_async(
            out,
            limit=2,
            max_pages=max_pages,
            complete=complete,
            event_ticker=None,
            series_ticker=series_ticker,
            tickers=None,
            manifest_out=manifest,
            contract_evidence_out=(tmp_path / f"{name}-evidence.parquet") if evidence else None,
            mve_filter=mve_filter,
            resume_from=resume_from,
        )
    )
    return out, manifest


def test_historical_ingest_bounded_records_cursor_and_archive_fields(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = FakeKalshi({None: {"markets": [_market("KXOLD-ONE")], "cursor": "next"}})

    out, manifest_path = _run(monkeypatch, tmp_path, fake, complete=False)

    assert fake.cursors == [None]
    assert fake.filters == [(None, "KXOLDER", None, None)]
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
    assert fake.filters == [(None, None, None, None)] * 3
    assert len(pd.read_parquet(out)) == 2
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["collection_complete"] is True
    assert manifest["stop_reason"] == "cursor_exhausted"
    assert manifest["page_count"] == 3
    assert manifest["historical_cutoff"]["as_of_market_settled_ts"] == "2026-07-25T00:00:00Z"
    assert manifest["output_sha256"] == file_sha256(out)
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
    source = json.loads(
        (evidence_path.parent / "source_collection_manifest.json").read_text(encoding="utf-8")
    )
    assert source["request_parameters"]["series_ticker"] is None
    assert source["request_parameters"]["historical_cutoff"] == manifest["historical_cutoff"]


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
                manifest_out=tmp_path / "collection.json",
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


def test_historical_ingest_pins_cutoff_and_filters_snapshot_and_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    old = _market("KXOLD")
    old["settlement_ts"] = "2026-07-24T23:45:26.01563Z"
    new = _market("KXNEW")
    new["settlement_ts"] = "2026-07-25T12:00:00Z"
    fake = FakeKalshi(
        {None: {"markets": [old, new], "cursor": ""}},
        cutoffs=["2026-07-25T00:00:00Z", "2026-07-26T00:00:00Z"],
    )

    out, manifest_path = _run(
        monkeypatch, tmp_path, fake, complete=True, evidence=True, series_ticker="KXOLDER"
    )

    assert pd.read_parquet(out)["market_key"].tolist() == ["KXOLD"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["rows_dropped_settled_at_or_after_as_of"] == 1
    assert manifest["historical_cutoff"] == {
        "as_of_market_settled_ts": "2026-07-25T00:00:00Z",
        "observed_start": "2026-07-25T00:00:00Z",
        "observed_end": "2026-07-26T00:00:00Z",
    }
    evidence_path = Path(manifest["dataset_paths"]["contract_evidence"])
    assert len(pd.read_parquet(evidence_path)) == 1
    source = json.loads(
        (evidence_path.parent / "source_collection_manifest.json").read_text(encoding="utf-8")
    )
    assert source["request_parameters"]["series_ticker"] == "KXOLDER"
    assert source["request_parameters"]["historical_cutoff"] == manifest["historical_cutoff"]
    verify_contract_evidence_manifest(
        pd.read_parquet(evidence_path),
        artifact_path=evidence_path,
        manifest_path=Path(manifest["dataset_paths"]["contract_evidence_manifest"]),
        expected_venue="kalshi",
        expected_source_endpoint="kalshi:/historical/markets",
        expected_payload_scope="cursor_list",
        expected_observation_time_source="capture_clock",
        expected_source_payload_kind="venue_api_response",
    )


@pytest.mark.parametrize("problem", ["backwards", "missing_settlement"])
def test_historical_ingest_rejects_unclassifiable_cutoff_or_market(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, problem: str
) -> None:
    market = _market("KXOLD")
    if problem == "missing_settlement":
        market.pop("settlement_ts")
    cutoffs = ["2026-07-25T00:00:00Z"]
    if problem == "backwards":
        cutoffs.append("2026-07-24T00:00:00Z")
    fake = FakeKalshi({None: {"markets": [market], "cursor": ""}}, cutoffs=cutoffs)

    with pytest.raises((RuntimeError, ValueError)):
        _run(monkeypatch, tmp_path, fake, complete=True, evidence=True)

    assert not (tmp_path / "collection.parquet").exists()
    assert not (tmp_path / "collection.json").exists()
    assert not (tmp_path / "collection-evidence.parquet.bundle").exists()


def test_historical_ingest_excludes_mve_and_records_filter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = FakeKalshi({None: {"markets": [_market("KXOLD")], "cursor": ""}})
    _, manifest_path = _run(
        monkeypatch, tmp_path, fake, complete=True, series_ticker=None, mve_filter="exclude"
    )
    assert fake.filters == [(None, None, None, "exclude")]
    assert json.loads(manifest_path.read_text(encoding="utf-8"))["filters"]["mve_filter"] == "exclude"


def test_historical_ingest_bounded_two_pages_keeps_continuation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = FakeKalshi(
        {
            None: {"markets": [_market("KXONE")], "cursor": "page-1"},
            "page-1": {"markets": [_market("KXTWO")], "cursor": "page-2"},
        }
    )
    _, manifest_path = _run(
        monkeypatch, tmp_path, fake, complete=False, max_pages=2, series_ticker=None
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["page_count"] == 2
    assert manifest["final_cursor"] == "page-2"
    assert manifest["stop_reason"] == "max_pages_reached"
    assert manifest["collection_complete"] is False


@pytest.mark.parametrize("selector", ["event_ticker", "series_ticker", "tickers"])
def test_historical_ingest_rejects_mve_with_other_selector(
    tmp_path: Path, selector: str
) -> None:
    kwargs = {"event_ticker": None, "series_ticker": None, "tickers": None}
    kwargs[selector] = "KXONE"
    with pytest.raises(ValueError, match="mutually exclusive"):
        asyncio.run(
            ingest._ingest_kalshi_historical_markets_async(
                tmp_path / "collection.parquet",
                limit=2,
                max_pages=1,
                complete=False,
                manifest_out=tmp_path / "collection.json",
                contract_evidence_out=None,
                mve_filter="exclude",
                **kwargs,
            )
        )


def test_historical_ingest_keeps_foreign_output_created_mid_scan(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    foreign = tmp_path / "collection.parquet"
    fake = FakeKalshi(
        {None: {"markets": [_market("KXOLD")], "cursor": ""}},
        on_page=lambda _cursor: foreign.write_bytes(b"other publisher"),
    )
    with pytest.raises(FileExistsError):
        _run(monkeypatch, tmp_path, fake, complete=True)
    assert foreign.read_bytes() == b"other publisher"
    assert not (tmp_path / "collection.json").exists()


@pytest.mark.parametrize("foreign_kind", ["manifest", "evidence"])
def test_historical_ingest_keeps_foreign_manifest_or_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, foreign_kind: str
) -> None:
    foreign_manifest = tmp_path / "collection.json"
    foreign_bundle = tmp_path / "collection-evidence.parquet.bundle"

    def publish_foreign(_cursor: str | None) -> None:
        if foreign_kind == "manifest":
            foreign_manifest.write_text("other publisher", encoding="utf-8")
        else:
            foreign_bundle.mkdir()
            (foreign_bundle / "sentinel").write_text("other publisher", encoding="utf-8")

    fake = FakeKalshi(
        {None: {"markets": [_market("KXOLD")], "cursor": ""}},
        on_page=publish_foreign,
    )
    with pytest.raises(OSError):
        _run(monkeypatch, tmp_path, fake, complete=True, evidence=True)
    assert not (tmp_path / "collection.parquet").exists()
    if foreign_kind == "manifest":
        assert foreign_manifest.read_text(encoding="utf-8") == "other publisher"
        assert not foreign_bundle.exists()
    else:
        assert (foreign_bundle / "sentinel").read_text(encoding="utf-8") == "other publisher"
        assert not foreign_manifest.exists()


def test_historical_ingest_resume_chain_keeps_scope_and_as_of(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pages = {
        None: {"markets": [_market("KXONE")], "cursor": "page-1"},
        "page-1": {"markets": [_market("KXTWO")], "cursor": "page-2"},
        "page-2": {"markets": [_market("KXTHREE")], "cursor": ""},
    }
    pages["page-1"]["markets"][0]["settlement_ts"] = "2026-07-25T12:00:00Z"
    manifests: list[Path] = []
    outputs: list[Path] = []
    for index in range(3):
        fake = FakeKalshi(
            pages,
            cutoffs=[f"2026-07-{25 + index}T00:00:00Z"],
        )
        out, manifest_path = _run(
            monkeypatch,
            tmp_path,
            fake,
            complete=False,
            evidence=index == 2,
            series_ticker=None,
            mve_filter="exclude" if index == 0 else None,
            resume_from=manifests[-1] if manifests else None,
            name=f"seg{index}",
        )
        outputs.append(out)
        manifests.append(manifest_path)
        assert fake.cursors == [[None], ["page-1"], ["page-2"]][index]
        assert fake.filters == [(None, None, None, "exclude")]
    records = [json.loads(path.read_text(encoding="utf-8")) for path in manifests]
    assert [record["segment_index"] for record in records] == [0, 1, 2]
    assert [record["start_cursor"] for record in records] == [None, "page-1", "page-2"]
    assert [record["final_cursor"] for record in records] == ["page-1", "page-2", None]
    assert [record["previous_manifest_sha256"] for record in records] == [
        None,
        file_sha256(manifests[0]),
        file_sha256(manifests[1]),
    ]
    assert len({record["chain_id"] for record in records}) == 1
    assert all(record["filters"]["mve_filter"] == "exclude" for record in records)
    assert all(
        record["historical_cutoff"]["as_of_market_settled_ts"] == "2026-07-25T00:00:00Z"
        for record in records
    )
    assert [record["collection_complete"] for record in records] == [False] * 3
    assert records[-1]["stop_reason"] == "cursor_exhausted"
    assert [len(pd.read_parquet(path)) for path in outputs] == [1, 0, 1]
    assert records[1]["rows_dropped_settled_at_or_after_as_of"] == 1
    evidence_path = Path(records[2]["dataset_paths"]["contract_evidence"])
    source = json.loads(
        (evidence_path.parent / "source_collection_manifest.json").read_text(encoding="utf-8")
    )
    assert source["request_parameters"]["chain_id"] == records[2]["chain_id"]
    assert source["request_parameters"]["previous_manifest_sha256"] == file_sha256(manifests[1])
    verified = verify_contract_evidence_manifest(
        pd.read_parquet(evidence_path),
        artifact_path=evidence_path,
        manifest_path=Path(records[2]["dataset_paths"]["contract_evidence_manifest"]),
        expected_venue="kalshi",
        expected_source_endpoint="kalshi:/historical/markets",
        expected_payload_scope="cursor_list",
        expected_observation_time_source="capture_clock",
        expected_source_payload_kind="venue_api_response",
    )
    assert verified["authoritative_complete"] is False


def test_historical_ingest_failed_segment_can_be_retried(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    first = FakeKalshi({None: {"markets": [_market("KXONE")], "cursor": "next"}})
    _, previous = _run(
        monkeypatch, tmp_path, first, complete=False, name="seg0", series_ticker=None
    )
    bad = _market("KXTWO")
    bad.pop("settlement_ts")
    failed = FakeKalshi({"next": {"markets": [bad], "cursor": ""}})
    with pytest.raises(ValueError, match="settlement_ts"):
        _run(
            monkeypatch, tmp_path, failed, complete=False, name="seg1",
            series_ticker=None, resume_from=previous,
        )
    assert not (tmp_path / "seg1.parquet").exists()
    assert not (tmp_path / "seg1.json").exists()
    retry = FakeKalshi({"next": {"markets": [_market("KXTWO")], "cursor": ""}})
    _, manifest_path = _run(
        monkeypatch, tmp_path, retry, complete=False, name="seg1",
        series_ticker=None, resume_from=previous,
    )
    assert json.loads(manifest_path.read_text(encoding="utf-8"))["previous_manifest_sha256"] == file_sha256(previous)


def test_historical_ingest_rejects_resume_with_filter_or_exhausted_cursor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = FakeKalshi({None: {"markets": [_market("KXONE")], "cursor": ""}})
    _, previous = _run(
        monkeypatch, tmp_path, fake, complete=True, name="seg0", series_ticker=None
    )
    with pytest.raises(ValueError, match="cannot be combined"):
        _run(
            monkeypatch, tmp_path, fake, complete=False, name="seg1",
            series_ticker="KXOLDER", resume_from=previous,
        )
    with pytest.raises(ValueError, match="no continuation cursor"):
        _run(
            monkeypatch, tmp_path, fake, complete=False, name="seg1",
            series_ticker=None, resume_from=previous,
        )
    assert fake.cursors == [None]


def test_historical_ingest_cli_requires_manifest_and_defaults_to_1000(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    missing = CliRunner().invoke(
        app, ["ingest-kalshi-historical-markets", "--out", str(tmp_path / "out.parquet")]
    )
    assert missing.exit_code != 0
    fake = FakeKalshi({None: {"markets": [_market("KXONE")], "cursor": ""}})
    monkeypatch.setattr(ingest, "AsyncKalshiClient", lambda **_kwargs: fake)
    result = CliRunner().invoke(
        app,
        [
            "ingest-kalshi-historical-markets",
            "--out", str(tmp_path / "out.parquet"),
            "--manifest-out", str(tmp_path / "out.json"),
            "--mve-filter", "exclude",
        ],
    )
    assert result.exit_code == 0, result.output
    assert fake.limits == [1000]
    assert fake.filters == [(None, None, None, "exclude")]


def test_historical_ingest_cli_strips_comma_ticker_whitespace(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = FakeKalshi({None: {"markets": [_market("KXONE")], "cursor": ""}})
    monkeypatch.setattr(ingest, "AsyncKalshiClient", lambda **_kwargs: fake)
    result = CliRunner().invoke(
        app,
        [
            "ingest-kalshi-historical-markets",
            "--out", str(tmp_path / "out.parquet"),
            "--manifest-out", str(tmp_path / "out.json"),
            "--tickers", "KXONE, KXTWO",
        ],
    )
    assert result.exit_code == 0, result.output
    assert fake.filters == [(None, None, "KXONE,KXTWO", None)]
