from pathlib import Path

import pandas as pd
import pytest

from pmkt.data.recorder import record_topbooks
from pmkt.data.validation import validate_frame
from pmkt.data.canonical import match_relation_row


pytestmark = pytest.mark.asyncio


def _read_segmented_parquet(path: Path) -> pd.DataFrame:
    files = sorted(path.rglob("*.parquet"))
    assert files
    return pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)


async def test_record_topbooks_writes_partitioned_topbooks_and_gap_sidecar(tmp_path) -> None:
    class FakePolymarket:
        async def book(self, token_id):
            assert token_id == "pm-token"
            return {
                "asset_id": token_id,
                "market": "pm-market",
                "hash": "pm-hash",
                "bids": [{"price": "0.39", "size": "10"}],
                "asks": [{"price": "0.40", "size": "5"}],
            }

    class FakeKalshi:
        async def orderbook(self, ticker, *, depth=None):
            assert ticker == "KXTEST"
            assert depth == 5
            return {
                "orderbook_fp": {
                    "yes_dollars": [["0.45", "7"]],
                    "no_dollars": [["0.54", "8"]],
                }
            }

    relations = pd.DataFrame(
        [
            match_relation_row(
                match_id="pm-market|pm-token|KXTEST|KXTEST:YES",
                polymarket_market_key="pm-market",
                polymarket_instrument_key="pm-token",
                polymarket_token_id="pm-token",
                kalshi_market_key="KXTEST",
                kalshi_instrument_key="KXTEST:YES",
                relation_label="exact_equivalent",
                is_trade_equivalent=True,
                is_tracking_useful=True,
                review_status="approved",
                evidence_json={"source": "test", "semantic_match_status": "compatible"},
            )
        ]
    )

    result = await record_topbooks(
        relations,
        polymarket_client=FakePolymarket(),
        kalshi_client=FakeKalshi(),
        out_dir=tmp_path,
        run_id="rec-test",
        max_cycles=1,
        interval_s=1,
        depth=5,
    )

    assert result.topbook_rows == 3
    assert result.depth_rows == 4
    assert result.gap_rows == 0
    topbooks = _read_segmented_parquet(tmp_path / "topbooks")
    assert validate_frame(topbooks, "topbook.v1", strict=True).ok
    assert set(topbooks["exchange"]) == {"polymarket", "kalshi"}
    assert set(topbooks["instrument_id"]) == {"pm-token", "KXTEST:YES", "KXTEST:NO"}
    depths = _read_segmented_parquet(tmp_path / "depth")
    assert validate_frame(depths, "depth.v1", strict=True).ok
    assert set(depths["instrument_id"]) == {"pm-token", "KXTEST:YES", "KXTEST:NO"}
    gaps = _read_segmented_parquet(tmp_path / "topbook_capture_gaps")
    assert validate_frame(gaps, "topbook_capture_gap.v1", strict=True).ok
    assert gaps.empty


async def test_record_topbooks_flags_stale_exchange_timestamp(tmp_path) -> None:
    class FakePolymarket:
        async def book(self, token_id):
            return {
                "asset_id": token_id,
                "market": "pm-market",
                "hash": "pm-old",
                "timestamp": "1700000000",
                "bids": [{"price": "0.39", "size": "10"}],
                "asks": [{"price": "0.40", "size": "5"}],
            }

    class FakeKalshi:
        async def orderbook(self, ticker, *, depth=None):
            return {
                "orderbook_fp": {
                    "yes_dollars": [["0.45", "7"]],
                    "no_dollars": [["0.54", "8"]],
                }
            }

    relations = pd.DataFrame(
        [
            match_relation_row(
                match_id="stale-match",
                polymarket_market_key="pm-market",
                polymarket_instrument_key="pm-token",
                polymarket_token_id="pm-token",
                kalshi_market_key="KXTEST",
                kalshi_instrument_key="KXTEST:YES",
                relation_label="exact_equivalent",
                is_trade_equivalent=True,
                is_tracking_useful=True,
                review_status="approved",
                evidence_json={"source": "test", "semantic_match_status": "compatible"},
            )
        ]
    )

    result = await record_topbooks(
        relations,
        polymarket_client=FakePolymarket(),
        kalshi_client=FakeKalshi(),
        out_dir=tmp_path,
        run_id="stale-test",
        max_cycles=1,
        interval_s=1,
        max_stale_seconds=1,
        utc_now=lambda: "2026-06-17T12:00:00+00:00",
    )

    assert result.gap_rows == 1
    gaps = _read_segmented_parquet(tmp_path / "topbook_capture_gaps")
    assert validate_frame(gaps, "topbook_capture_gap.v1", strict=True).ok
    assert gaps.loc[0, "gap_type"] == "stale_book"
    assert gaps.loc[0, "instrument_id"] == "pm-token"


async def test_record_topbooks_kalshi_fetch_error_uses_relation_instrument_key(tmp_path) -> None:
    class FakePolymarket:
        async def book(self, token_id):
            return {
                "asset_id": token_id,
                "market": "pm-market",
                "hash": "pm-hash",
                "bids": [{"price": "0.39", "size": "10"}],
                "asks": [{"price": "0.40", "size": "5"}],
            }

    class FakeKalshi:
        async def orderbook(self, ticker, *, depth=None):
            raise RuntimeError("kalshi unavailable")

    relations = pd.DataFrame(
        [
            match_relation_row(
                match_id="fetch-error-match",
                polymarket_market_key="pm-market",
                polymarket_instrument_key="pm-token",
                polymarket_token_id="pm-token",
                kalshi_market_key="KXTEST",
                kalshi_instrument_key="KXTEST:YES",
                relation_label="exact_equivalent",
                is_trade_equivalent=True,
                is_tracking_useful=True,
                review_status="approved",
                evidence_json={"source": "test", "semantic_match_status": "compatible"},
            )
        ]
    )

    result = await record_topbooks(
        relations,
        polymarket_client=FakePolymarket(),
        kalshi_client=FakeKalshi(),
        out_dir=tmp_path,
        run_id="fetch-error-test",
        max_cycles=1,
        interval_s=1,
    )

    assert result.gap_rows == 2
    gaps = _read_segmented_parquet(tmp_path / "topbook_capture_gaps")
    assert validate_frame(gaps, "topbook_capture_gap.v1", strict=True).ok
    fetch_errors = gaps[gaps["gap_type"] == "fetch_error"]
    assert fetch_errors.shape[0] == 1
    assert fetch_errors.iloc[0]["instrument_id"] == "KXTEST:YES"
    assert fetch_errors.iloc[0]["match_id"] == "fetch-error-match"
