from datetime import datetime, timezone

import pandas as pd
import pytest

from pmkt.data.historical_books import (
    backfill_venue_history,
    venue_history_capability_matrix,
    write_historical_backfill_result,
)
from pmkt.data.validation import validate_frame


@pytest.mark.asyncio
@pytest.mark.parametrize("cutoff", [100, 100.5])
@pytest.mark.parametrize("comparison", ["inclusive", "exclusive", "bucket"])
async def test_kalshi_trade_cutoff_preserves_fractional_timestamps(cutoff, comparison):
    timestamps = [98, 99, 99.5, 100, 100.25, 100.5, 101, 106]
    calls = []

    class FakeKalshi:
        async def historical_cutoff(self):
            return {"trades_created_ts": datetime.fromtimestamp(cutoff, timezone.utc).isoformat()}

        async def batch_market_candlesticks(self, *args, **kwargs):
            return {"markets": []}

        def page(self, historical, params):
            calls.append((historical, params))
            trades = []
            for ts in timestamps:
                if (ts < cutoff) != historical:
                    continue
                filtered_ts = int(ts) if comparison == "bucket" else ts
                if comparison == "exclusive":
                    included = params["min_ts"] < filtered_ts < params["max_ts"]
                else:
                    included = params["min_ts"] <= filtered_ts <= params["max_ts"]
                if included:
                    trades.append({"trade_id": str(ts), "ticker": "A", "yes_price": 42,
                                   "count": 1, "created_time":
                                   datetime.fromtimestamp(ts, timezone.utc).isoformat()})
            return {"trades": trades, "cursor": ""}

        async def historical_trades(self, **params):
            return self.page(True, params)

        async def trades(self, **params):
            return self.page(False, params)

    result = await backfill_venue_history(
        "kalshi", ["A"], start_ts=90, end_ts=110, kalshi_client=FakeKalshi(),
    )
    assert set(result.trades["venue_trade_id"]) == {str(ts) for ts in timestamps}
    assert result.summary["trade_partitions"][0]["complete"] is True
    assert result.gaps[result.gaps["capability"] == "historical_trades"].empty
    assert len(calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", [False, True])
async def test_kalshi_trade_cutoff_deduplicates_identical_cross_source_rows(conflict, monkeypatch):
    import itertools
    from pmkt.data import historical_books

    receipts = itertools.count()
    monkeypatch.setattr(historical_books, "utc_now_iso",
                        lambda: f"2026-01-01T00:00:00.{next(receipts):06d}Z")
    duplicate = {"trade_id": "shared", "ticker": "A", "yes_price": 42,
                 "count": 1, "created_ts": 100}

    class FakeKalshi:
        async def historical_cutoff(self):
            return {"trades_created_ts": "1970-01-01T00:01:40Z"}

        async def batch_market_candlesticks(self, *args, **kwargs):
            return {"markets": []}

        async def historical_trades(self, **params):
            return {"trades": [duplicate], "cursor": ""}

        async def trades(self, **params):
            return {"trades": [{**duplicate, "trade_id": "new", "created_ts": 106},
                               {**duplicate, "count": 2 if conflict else 1}], "cursor": ""}

    result = await backfill_venue_history(
        "kalshi", ["A"], start_ts=90, end_ts=110, kalshi_client=FakeKalshi(),
        checked_at_utc="2026-01-01T00:00:00Z",
    )
    [partition] = result.summary["trade_partitions"]
    assert partition["complete"] is (not conflict)
    assert result.trades["venue_trade_id"].tolist() == (["shared"] if conflict else ["shared", "new"])
    assert partition["row_count"] == len(result.trades)
    assert validate_frame(result.trades, "trade.v1", strict=True).ok
    trade_gaps = result.gaps[result.gaps["capability"] == "historical_trades"]
    assert trade_gaps.empty is (not conflict)


@pytest.mark.asyncio
@pytest.mark.parametrize("final_cutoff", ["1970-01-01T00:01:45Z", None, "error"])
async def test_kalshi_trade_cutoff_recheck_prevents_false_completeness(final_cutoff, tmp_path):
    class FakeKalshi:
        cutoff_calls = 0
        migrated = False

        async def historical_cutoff(self):
            self.cutoff_calls += 1
            if self.cutoff_calls == 1:
                return {"trades_created_ts": "1970-01-01T00:01:40Z"}
            if final_cutoff == "error":
                raise RuntimeError("cutoff unavailable")
            return {"trades_created_ts": final_cutoff}

        async def batch_market_candlesticks(self, *args, **kwargs):
            return {"markets": []}

        async def historical_trades(self, **params):
            self.migrated = True
            return {"trades": [{"trade_id": "old", "yes_price": 42,
                                "count": 1, "created_ts": 98}], "cursor": ""}

        async def trades(self, **params):
            # The trade at 102 moved to the archive after its request finished.
            assert self.migrated
            return {"trades": [{"trade_id": "recent", "yes_price": 42,
                                "count": 1, "created_ts": 106}], "cursor": ""}

    fake = FakeKalshi()
    result = await backfill_venue_history(
        "kalshi", ["A"], start_ts=90, end_ts=110, kalshi_client=fake,
    )
    [partition] = result.summary["trade_partitions"]
    assert partition["complete"] is False
    assert fake.cutoff_calls == 2
    assert partition["stop_reason"] == ("cutoff_advanced" if final_cutoff not in {None, "error"}
                                       else "fetch_error")
    assert partition["exhausted_endpoints"] == ["/historical/trades", "/markets/trades"]
    if final_cutoff not in {None, "error"}:
        assert partition["trades_created_ts_after"] == "1970-01-01T00:01:45+00:00"
    paths = write_historical_backfill_result(result, tmp_path)
    trade_gaps = pd.read_parquet(paths["historical_backfill_gaps"])
    trade_gaps = trade_gaps[trade_gaps["capability"] == "historical_trades"]
    assert len(trade_gaps) == 1
    assert trade_gaps.iloc[0]["endpoint"] == "/historical/cutoff"
    assert set(pd.read_parquet(paths["trades"])["venue_trade_id"]) == {"old", "recent"}


@pytest.mark.asyncio
@pytest.mark.parametrize("start,end,expected_sources", [
    (90, 99, ["historical"]),
    (100, 110, ["live"]),
    (90, 110, ["historical", "live"]),
    (90, 100, ["historical", "live"]),
])
async def test_kalshi_trades_route_at_cutoff_and_recheck_each_partition(start, end, expected_sources):
    calls = []

    class FakeKalshi:
        cutoff_calls = 0

        async def historical_cutoff(self):
            self.cutoff_calls += 1
            return {"trades_created_ts": "1970-01-01T00:01:40Z"}

        async def batch_market_candlesticks(self, *args, **kwargs):
            return {"markets": []}

        def page(self, source, params):
            calls.append((source, params))
            return {"trades": [{"trade_id": f"{params['ticker']}-{source}",
                                "ticker": params["ticker"], "yes_price": 42, "count": 1,
                                "created_ts": 99 if source == "historical" else 100}],
                    "cursor": ""}

        async def historical_trades(self, **params):
            return self.page("historical", params)

        async def trades(self, **params):
            return self.page("live", params)

    fake = FakeKalshi()
    result = await backfill_venue_history(
        "kalshi", ["A", "B"], start_ts=start, end_ts=end, kalshi_client=fake,
    )
    assert fake.cutoff_calls == 3
    assert [source for source, _ in calls] == expected_sources * 2
    for source, params in calls:
        assert params["min_ts"] == (start if source == "historical" else max(start, 99))
        assert params["max_ts"] == (min(end, 101) if source == "historical" else end)
    assert len(result.trades) == len(expected_sources) * 2
    assert all(part["complete"] for part in result.summary["trade_partitions"])


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", [
    "exhausted", "page_limit", "boundary_cap", "error", "cutoff_error", "duplicate",
])
async def test_kalshi_live_trade_pages_control_completeness(ending):
    class FakeKalshi:
        async def historical_cutoff(self):
            if ending == "cutoff_error":
                return {}
            return {"trades_created_ts": "1970-01-01T00:01:40Z"}

        async def batch_market_candlesticks(self, *args, **kwargs):
            return {"markets": []}

        async def historical_trades(self, **params):
            return {"trades": [{"trade_id": "old", "yes_price": 42, "count": 1,
                                "created_ts": 99}], "cursor": ""}

        async def trades(self, **params):
            if ending == "error":
                raise RuntimeError("live failed")
            if params["cursor"] is None:
                return {"trades": [], "cursor": "next"}
            return {"trades": [{"trade_id": "old" if ending == "duplicate" else "new",
                                "yes_price": 42, "count": 1,
                                "created_ts": 100}], "cursor": ""}

    result = await backfill_venue_history(
        "kalshi", ["A"], start_ts=90, end_ts=110, kalshi_client=FakeKalshi(),
        max_trade_pages={"page_limit": 2, "boundary_cap": 1}.get(ending, 3),
    )
    [partition] = result.summary["trade_partitions"]
    assert partition["complete"] == (ending == "exhausted")
    assert len(result.trades) == {"exhausted": 2, "page_limit": 1, "error": 1,
                                  "cutoff_error": 0, "boundary_cap": 1, "duplicate": 1}[ending]
    trade_gaps = result.gaps[result.gaps["capability"] == "historical_trades"]
    assert trade_gaps.empty == (ending == "exhausted")
    if ending != "exhausted":
        assert trade_gaps.iloc[0]["endpoint"] == (
            "/historical/cutoff" if ending == "cutoff_error" else "/markets/trades"
        )


class _HistoricalKalshi:
    async def historical_cutoff(self):
        return {"trades_created_ts": "2100-01-01T00:00:00Z"}


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
    class FakeKalshi(_HistoricalKalshi):
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
    class FakeKalshi(_HistoricalKalshi):
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
    class FakeKalshi(_HistoricalKalshi):
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
    class FakeKalshi(_HistoricalKalshi):
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
@pytest.mark.parametrize("source", ["historical", "live"])
@pytest.mark.parametrize("cursors,cap,expected", [
    (["next"], 1, "page_limit"),
    (["same", "same"], 5, "repeated_cursor"),
    (["a", "b", "a"], 5, "repeated_cursor"),
    ([None], 1, "cursor_exhausted"),
    (["a", None], 2, "cursor_exhausted"),
    (["a", "error"], 3, "fetch_error"),
])
async def test_historical_trade_pagination_completeness(cursors, cap, expected, tmp_path, source):
    import json

    class FakeKalshi(_HistoricalKalshi):
        def __init__(self):
            self.calls = {}

        async def historical_cutoff(self):
            if source == "live":
                return {"trades_created_ts": "1970-01-01T00:00:00Z"}
            return await super().historical_cutoff()

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

        trades = historical_trades

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

    class FakeKalshi(_HistoricalKalshi):
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

    class FakeKalshi(_HistoricalKalshi):
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
    class FakeKalshi(_HistoricalKalshi):
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


@pytest.mark.asyncio
async def test_kalshi_trade_cutoff_keeps_sub_microsecond_precision():
    # 100 s plus 100 ns: float seconds would round the cutoff onto the start
    # second and skip historical storage for a trade at exactly 100 s.
    calls = []

    class FakeKalshi:
        async def historical_cutoff(self):
            return {"trades_created_ts": "1970-01-01T00:01:40.000000100Z"}

        async def batch_market_candlesticks(self, *args, **kwargs):
            return {"markets": []}

        def page(self, historical, params):
            calls.append((historical, params))
            ts = 100 if historical else 101
            included = params["min_ts"] <= ts <= params["max_ts"]
            trades = [{"trade_id": str(ts), "ticker": "A", "yes_price": 42, "count": 1,
                       "created_time": datetime.fromtimestamp(ts, timezone.utc).isoformat()}]
            return {"trades": trades if included else [], "cursor": ""}

        async def historical_trades(self, **params):
            return self.page(True, params)

        async def trades(self, **params):
            return self.page(False, params)

    result = await backfill_venue_history(
        "kalshi", ["A"], start_ts=100, end_ts=110, kalshi_client=FakeKalshi(),
    )
    assert set(result.trades["venue_trade_id"]) == {"100", "101"}
    assert [historical for historical, _ in calls] == [True, False]
    assert result.summary["trade_partitions"][0]["complete"] is True
