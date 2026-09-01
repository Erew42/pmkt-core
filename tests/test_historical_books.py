import pandas as pd
import pytest

from pmkt.data.historical_books import (
    backfill_venue_history,
    venue_history_capability_matrix,
    write_historical_backfill_result,
)
from pmkt.data.validation import validate_frame


@pytest.mark.asyncio
async def test_polymarket_historical_backfill_preserves_price_context_and_book_gaps(tmp_path) -> None:
    class FakePolymarket:
        async def prices_history(self, market, **params):
            assert market == "token-1"
            assert params["start_ts"] == 1_700_000_000
            assert params["end_ts"] == 1_700_086_400
            return {"history": [{"t": 1_700_000_000, "p": 0}, {"t": 1_700_000_100, "p": 0.42}]}

    result = await backfill_venue_history(
        "polymarket",
        ["token-1"],
        start_ts=1_700_000_000,
        end_ts=1_700_086_400,
        interval="1d",
        polymarket_client=FakePolymarket(),
        run_id="hist-test",
    )

    assert validate_frame(result.historical_prices, "historical_price.v1", strict=True).ok
    assert validate_frame(result.gaps, "historical_backfill_gap.v1", strict=True).ok
    assert result.historical_prices.loc[0, "exchange"] == "polymarket"
    assert result.historical_prices["close_dollars"].tolist() == [0.0, 0.42]
    # No fidelity supplied -> interval is derived from the sample spacing (100s here).
    assert result.historical_prices["interval_seconds"].tolist() == [100, 100]
    assert set(result.historical_prices["evidence_role"]) == {"price_context"}
    assert set(result.gaps["capability"]) == {"historical_topbook", "historical_depth"}
    assert set(result.gaps["status"]) == {"unsupported"}

    paths = write_historical_backfill_result(result, tmp_path)
    assert pd.read_parquet(paths["historical_prices"]).shape[0] == 2
    assert pd.read_parquet(paths["trades"]).empty
    assert pd.read_parquet(paths["historical_backfill_gaps"]).shape[0] == 2


@pytest.mark.asyncio
async def test_kalshi_historical_backfill_normalizes_candlestick_price_types() -> None:
    class FakeKalshi:
        async def historical_trades(self, **params):
            assert params["ticker"] == "KXTEST"
            return {
                "trades": [
                    {
                        "trade_id": "trade-1",
                        "ticker": "KXTEST",
                        "created_ts": 1_700_000_001,
                        "yes_price_dollars": "0.42",
                        "count": "3",
                        "taker_side": "yes",
                    }
                ],
                "cursor": "",
            }

        async def historical_market_candlesticks(self, ticker, **params):
            assert ticker == "KXTEST"
            assert params["period_interval"] == 60
            return {
                "candlesticks": [
                    {
                        "end_period_ts": 1_700_000_000,
                        "yes_bid": {
                            "open": "0.40",
                            "low": "0.39",
                            "high": "0.42",
                            "close": "0.41",
                        },
                        "yes_ask": {
                            "open": "0.43",
                            "low": "0.42",
                            "high": "0.45",
                            "close": "0.44",
                        },
                        "price": {
                            "open": "0.41",
                            "low": "0.40",
                            "high": "0.43",
                            "close": "0.42",
                            "mean": "0.415",
                            "previous": "0.405",
                        },
                        "volume": "10",
                        "open_interest": "20",
                    }
                ]
            }

    result = await backfill_venue_history(
        "kalshi",
        ["KXTEST"],
        start_ts=1_699_996_400,
        end_ts=1_700_000_000,
        period_interval=60,
        use_historical=True,
        kalshi_client=FakeKalshi(),
        run_id="kalshi-hist-test",
    )

    assert validate_frame(result.historical_prices, "historical_price.v1", strict=True).ok
    assert set(result.historical_prices["price_type"]) == {"yes_bid", "yes_ask", "price"}
    price = result.historical_prices[result.historical_prices["price_type"] == "price"].iloc[0]
    assert price["mean_dollars"] == pytest.approx(0.415)
    assert price["previous_dollars"] == pytest.approx(0.405)
    assert price["volume_contracts"] == pytest.approx(10)
    assert validate_frame(result.trades, "trade.v1", strict=True).ok
    assert result.trades.loc[0, "venue_trade_id"] == "trade-1"
    assert result.trades.loc[0, "notional_dollars"] == pytest.approx(1.26)


@pytest.mark.asyncio
async def test_kalshi_historical_backfill_skips_empty_price_ohlc_maps() -> None:
    class FakeKalshi:
        async def historical_market_candlesticks(self, ticker, **params):
            assert ticker == "KXTEST"
            return {
                "candlesticks": [
                    {
                        "end_period_ts": 1_700_000_000,
                        "yes_bid": {"close_dollars": "0.41"},
                        "yes_ask": {"close_dollars": "0.44"},
                        "price": {},
                    }
                ]
            }

    result = await backfill_venue_history(
        "kalshi",
        ["KXTEST"],
        start_ts=1_699_996_400,
        end_ts=1_700_000_000,
        period_interval=60,
        use_historical=True,
        include_trades=False,
        kalshi_client=FakeKalshi(),
        run_id="kalshi-empty-price-test",
    )

    assert validate_frame(result.historical_prices, "historical_price.v1", strict=True).ok
    assert result.historical_prices["price_type"].tolist() == ["yes_bid", "yes_ask"]
    assert "price" not in set(result.historical_prices["price_type"])
    assert "empty_response" not in set(result.gaps["status"])


@pytest.mark.asyncio
async def test_kalshi_historical_backfill_emits_gap_for_unparseable_candle() -> None:
    class FakeKalshi:
        async def historical_market_candlesticks(self, ticker, **params):
            assert ticker == "KXTEST"
            return {
                "candlesticks": [
                    {
                        "end_period_ts": 1_700_000_000,
                        "yes_bid": {"open_cents": 41, "close_cents": 42},
                        "yes_ask": {"open_cents": 43, "close_cents": 44},
                    }
                ]
            }

    result = await backfill_venue_history(
        "kalshi",
        ["KXTEST"],
        start_ts=1_699_996_400,
        end_ts=1_700_000_000,
        period_interval=60,
        use_historical=True,
        include_trades=False,
        kalshi_client=FakeKalshi(),
        run_id="kalshi-unparseable-candle-test",
    )

    assert validate_frame(result.historical_prices, "historical_price.v1", strict=True).ok
    assert result.historical_prices.empty
    assert validate_frame(result.gaps, "historical_backfill_gap.v1", strict=True).ok
    parse_gaps = result.gaps[result.gaps["status"] == "empty_response"]
    assert parse_gaps.shape[0] == 1
    assert parse_gaps.iloc[0]["instrument_id"] == "KXTEST:YES"
    assert parse_gaps.iloc[0]["capability"] == "historical_market_candlesticks"
    assert "no_parseable_ohlc" in parse_gaps.iloc[0]["reason"]


@pytest.mark.asyncio
async def test_kalshi_historical_backfill_rejects_out_of_range_bare_prices() -> None:
    class FakeKalshi:
        async def historical_market_candlesticks(self, ticker, **params):
            assert ticker == "KXTEST"
            return {
                "candlesticks": [
                    {
                        "end_period_ts": 1_700_000_000,
                        "yes_bid": {"open": 41, "close": 42},
                        "yes_ask": {"open": 43, "close": 44},
                    }
                ]
            }

    result = await backfill_venue_history(
        "kalshi",
        ["KXTEST"],
        start_ts=1_699_996_400,
        end_ts=1_700_000_000,
        period_interval=60,
        use_historical=True,
        include_trades=False,
        kalshi_client=FakeKalshi(),
        run_id="kalshi-cents-guard-test",
    )

    # Bare integer "cents" must not be read as dollars; the row is dropped and a gap emitted.
    assert result.historical_prices.empty
    parse_gaps = result.gaps[result.gaps["status"] == "empty_response"]
    assert parse_gaps.shape[0] == 1
    assert "no_parseable_ohlc" in parse_gaps.iloc[0]["reason"]


def test_venue_history_capability_matrix_documents_unsupported_orderbook_backfill() -> None:
    matrix = venue_history_capability_matrix(checked_at_utc="2026-06-17T00:00:00+00:00")

    assert validate_frame(matrix, "venue_history_capability.v1", strict=True).ok
    unsupported = matrix[matrix["capability"].isin(["historical_topbook", "historical_depth"])]
    assert set(unsupported["supported"]) == {False}
    assert set(unsupported["evidence_role"]) == {"executable_book"}
    price_context = matrix[
        matrix["capability"].isin(
            ["price_history", "batch_price_history", "market_candlesticks"]
        )
    ]
    assert set(price_context["evidence_role"]) == {"price_context"}
