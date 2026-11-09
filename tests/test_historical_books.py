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


@pytest.mark.parametrize("economics", [
    {"yes_price_dollars": "0.42", "count_fp": "1.75"},
    {"yes_price_dollars_fp": "0.42", "count_fp": "1.75"},
    {"yes_price": 1, "count": 1, "count_fp": "1.75"},
    {"yes_price": 42, "count": 2},
    {"yes_price_dollars": "0.42", "yes_price": 42, "count": 1, "count_fp": "1.75"},
])
def test_historical_and_recording_trade_economics_agree(economics):
    from pmkt.data.historical_books import _kalshi_trade_rows
    from pmkt.streaming.trades import recording_trade

    raw = {"trade_id": "t", "market_ticker": "KXTEST", **economics}
    historical = _kalshi_trade_rows({"trades": [raw]}, collector_run_id="r", default_ticker="KXTEST")[0]
    streaming = recording_trade("kalshi", {"type": "trade", "msg": raw})
    assert historical["price_dollars"] == streaming["price"]
    assert historical["size_contracts"] == streaming["quantity"]
    assert historical["notional_dollars"] == pytest.approx(streaming["price"] * streaming["quantity"])
    assert historical["raw_json"] == raw


@pytest.mark.parametrize("economics", [
    {"price": 0.42, "count": 1},
    {"yes_price_dollars": "0.42", "yes_price": 1, "count": 1},
])
def test_historical_and_recording_reject_ambiguous_trade_prices(economics):
    from pmkt.data.historical_books import _kalshi_trade_rows
    from pmkt.streaming.trades import ObservationValidationError, recording_trade

    raw = {"trade_id": "t", "market_ticker": "KXTEST", **economics}
    with pytest.raises(ObservationValidationError):
        _kalshi_trade_rows({"trades": [raw]}, collector_run_id="r", default_ticker="KXTEST")
    with pytest.raises(ObservationValidationError):
        recording_trade("kalshi", {"type": "trade", "msg": raw})


@pytest.mark.asyncio
@pytest.mark.parametrize("cursors,cap,expected", [
    (["next"], 1, "page_limit"),
    (["same", "same"], 5, "repeated_cursor"),
    (["a", "b", "a"], 5, "repeated_cursor"),
    ([None], 1, "cursor_exhausted"),
    (["a", None], 2, "cursor_exhausted"),
    (["a", "error"], 3, "fetch_error"),
])
async def test_historical_trade_pagination_completeness(cursors, cap, expected, tmp_path):
    import json

    class FakeKalshi:
        def __init__(self):
            self.calls = {}

        async def historical_market_candlesticks(self, ticker, **params):
            return {"candlesticks": []}

        async def historical_trades(self, ticker, **params):
            index = self.calls.get(ticker, 0)
            self.calls[ticker] = index + 1
            cursor = cursors[index] if ticker == "A" else None
            if cursor == "error":
                raise RuntimeError("fake failure")
            # Empty pages with cursors must still be treated as incomplete.
            return {"trades": ([{"trade_id": f"{ticker}-{index}", "yes_price": 1,
                                 "count_fp": "1.75", "created_ts": 1}]
                               if index == 0 else []), "cursor": cursor}

    fake = FakeKalshi()
    result = await backfill_venue_history(
        "kalshi", ["A", "B"], start_ts=1, end_ts=2,
        use_historical=True, kalshi_client=fake, max_trade_pages=cap,
    )
    a, b = result.summary["trade_partitions"]
    assert a["stop_reason"] == expected
    assert a["complete"] == (expected == "cursor_exhausted")
    assert b["complete"] is True
    assert a["row_count"] == 1
    assert b["row_count"] == 1
    assert a["page_count"] == len(cursors) - (expected == "fetch_error")
    assert fake.calls["A"] == len(cursors)
    paths = write_historical_backfill_result(result, tmp_path)
    summary = json.loads(paths["summary"].read_text())
    assert summary["trade_partitions"] == result.summary["trade_partitions"]
    gaps = pd.read_parquet(paths["historical_backfill_gaps"])
    trade_gaps = gaps[gaps["capability"] == "historical_trades"]
    if expected == "cursor_exhausted":
        assert trade_gaps.empty
    else:
        assert len(trade_gaps) == 1
        assert json.loads(trade_gaps.iloc[0]["raw_json"])["stop_reason"] == expected
        assert trade_gaps.iloc[0]["start_ts_utc"] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("max_pages,limit", [(0, 1), (1, 0), (-1, 1)])
async def test_historical_trade_pagination_rejects_nonpositive_bounds(max_pages, limit):
    from pmkt.data.historical_books import _backfill_kalshi_trades
    with pytest.raises(ValueError, match="positive"):
        await _backfill_kalshi_trades(
            None, ["A"], run_id="r", start_ts=1, end_ts=2,
            max_pages=max_pages, limit=limit,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_page", [False, True])
@pytest.mark.parametrize("malformed", [
    {}, {"trades": None}, {"trades": "bad"}, {"trades": [None]},
    {"trades": {"error": "bad"}},
    {"trades": [{"trade_id": "partial", "yes_price": 42, "count": 1}, None]},
    {"trades": [{}]},
    {"trades": [{"trade_id": "partial", "yes_price": 42, "count": 1}, {}]},
])
async def test_malformed_trade_page_stays_incomplete_and_retains_prior_rows(
    tmp_path, malformed, prior_page
):
    import json

    trade = {"trade_id": "kept", "yes_price": 42, "count_fp": "1.75", "created_ts": 1}

    class FakeKalshi:
        async def historical_market_candlesticks(self, ticker, **params):
            return {"candlesticks": []}

        async def historical_trades(self, **params):
            if prior_page and params["cursor"] is None:
                return {"trades": [trade], "cursor": "next"}
            return malformed

    result = await backfill_venue_history(
        "kalshi", ["A"], start_ts=1, end_ts=2, use_historical=True,
        kalshi_client=FakeKalshi(), max_trade_pages=3,
    )
    partition = result.summary["trade_partitions"][0]
    assert partition["complete"] is False
    assert partition["stop_reason"] == "fetch_error"
    assert partition["page_count"] == int(prior_page)
    assert partition["row_count"] == int(prior_page)
    assert partition["continuation_cursor"] == ("next" if prior_page else None)
    paths = write_historical_backfill_result(result, tmp_path)
    assert json.loads(paths["summary"].read_text())["trade_partitions"] == [partition]
    gaps = pd.read_parquet(paths["historical_backfill_gaps"])
    trade_gaps = gaps[gaps["capability"] == "historical_trades"]
    assert len(trade_gaps) == 1
    assert trade_gaps.iloc[0]["status"] == "fetch_error"
    assert json.loads(trade_gaps.iloc[0]["raw_json"])["complete"] is False
    trades = pd.read_parquet(paths["trades"])
    assert len(trades) == int(prior_page)
    if prior_page:
        assert json.loads(trades.iloc[0]["raw_json"]) == trade


@pytest.mark.asyncio
@pytest.mark.parametrize("pages,kept_ids,stop_reason", [
    ([(["one", "one"], None)], [], "fetch_error"),
    ([(["one"], "a"), (["one"], "a")], ["one"], "fetch_error"),
    ([(["one"], "a"), (["two", "one"], "b")], ["one"], "fetch_error"),
    ([(["one"], "a"), (["two"], "a")], ["one", "two"], "repeated_cursor"),
    ([([], None)], [], "cursor_exhausted"),
    ([(["one"], "a"), ([], None)], ["one"], "cursor_exhausted"),
])
async def test_historical_trade_page_acceptance_preserves_publishable_prefix(
    tmp_path, pages, kept_ids, stop_reason
):
    import json

    def trade(trade_id):
        return {"trade_id": trade_id, "yes_price": 42, "count": 2,
                "count_fp": "1.75", "created_ts": 11}

    class FakeKalshi:
        calls = 0

        async def historical_market_candlesticks(self, ticker, **params):
            return {"candlesticks": []}

        async def historical_trades(self, **params):
            assert params["min_ts"] == 10 and params["max_ts"] == 20
            assert params["cursor"] == (None if self.calls == 0 else "a")
            ids, cursor = pages[self.calls]
            self.calls += 1
            return {"trades": [trade(trade_id) for trade_id in ids], "cursor": cursor}

    fake = FakeKalshi()
    result = await backfill_venue_history(
        "kalshi", ["A"], start_ts=10, end_ts=20, use_historical=True,
        kalshi_client=fake, max_trade_pages=5,
    )
    partition = result.summary["trade_partitions"][0]
    assert fake.calls == len(pages)
    assert partition["stop_reason"] == stop_reason
    assert partition["complete"] == (stop_reason == "cursor_exhausted")
    assert partition["row_count"] == len(kept_ids)
    assert partition["page_count"] == len(pages) - (stop_reason == "fetch_error")
    expected_cursor = ("a" if len(pages) > 1 else None)
    if stop_reason != "fetch_error":
        expected_cursor = pages[-1][1]
    assert partition["continuation_cursor"] == expected_cursor

    paths = write_historical_backfill_result(result, tmp_path)
    assert len(paths) == 5 and all(path.is_file() for path in paths.values())
    assert json.loads(paths["summary"].read_text())["trade_partitions"] == [partition]
    trades = pd.read_parquet(paths["trades"])
    assert trades["venue_trade_id"].tolist() == kept_ids
    assert not trades.duplicated(["venue", "venue_trade_id"]).any()
    for row in trades.to_dict("records"):
        assert json.loads(row["raw_json"]) == trade(row["venue_trade_id"])
        assert row["price_dollars"] == pytest.approx(0.42)
        assert row["size_contracts"] == pytest.approx(1.75)
        assert row["notional_dollars"] == pytest.approx(0.735)
    gaps = pd.read_parquet(paths["historical_backfill_gaps"])
    trade_gaps = gaps[gaps["capability"] == "historical_trades"]
    if stop_reason == "cursor_exhausted":
        assert trade_gaps.empty
    else:
        gap = trade_gaps.iloc[0]
        assert len(trade_gaps) == 1
        assert gap["status"] == ("fetch_error" if stop_reason == "fetch_error" else "truncated")
        assert json.loads(gap["raw_json"]) == partition
        assert pd.Timestamp(gap["start_ts_utc"]).timestamp() == 10
        assert pd.Timestamp(gap["end_ts_utc"]).timestamp() == 20
        if stop_reason == "fetch_error":
            assert "duplicate" in gap["reason"]


@pytest.mark.asyncio
@pytest.mark.parametrize("duplicate", [False, True])
async def test_historical_trade_keys_are_unique_across_ticker_partitions(tmp_path, duplicate):
    class FakeKalshi:
        async def historical_market_candlesticks(self, ticker, **params):
            return {"candlesticks": []}

        async def historical_trades(self, ticker, **params):
            return {"trades": [{"trade_id": "A" if duplicate else ticker,
                                "ticker": ticker, "yes_price": 42, "count": 1}]}

    result = await backfill_venue_history(
        "kalshi", ["A", "B"], start_ts=10, end_ts=20,
        use_historical=True, kalshi_client=FakeKalshi(),
    )
    a, b = result.summary["trade_partitions"]
    assert a["complete"] is True
    assert b["complete"] is (not duplicate)
    assert b["stop_reason"] == ("fetch_error" if duplicate else "cursor_exhausted")
    assert b["page_count"] == b["row_count"] == (0 if duplicate else 1)
    paths = write_historical_backfill_result(result, tmp_path)
    trades = pd.read_parquet(paths["trades"])
    assert trades["venue_trade_id"].tolist() == (["A"] if duplicate else ["A", "B"])


class _FailIfCalled:
    def __getattr__(self, name):
        raise AssertionError(f"client.{name} must not be used after invalid interval input")


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [0, -1])
async def test_backfill_rejects_nonpositive_polymarket_fidelity_before_fetch(value):
    with pytest.raises(ValueError, match="fidelity must be a positive"):
        await backfill_venue_history(
            "polymarket", ["token"], fidelity=value, polymarket_client=_FailIfCalled())


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [0, -1])
async def test_backfill_rejects_nonpositive_kalshi_period_interval_before_fetch(value):
    with pytest.raises(ValueError, match="period_interval must be a positive"):
        await backfill_venue_history(
            "kalshi", ["KXTEST"], start_ts=0, end_ts=60, period_interval=value,
            kalshi_client=_FailIfCalled())
