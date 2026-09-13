from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import math
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pytest

from pmkt._operation import OperationExpiry
from pmkt.errors import (
    InvalidDataError,
    MarketNotFoundError,
    OperationTimeoutError,
    ResultLimitExceededError,
)
from pmkt.exchanges.kalshi import AsyncKalshiClient, KalshiMarketRef
from pmkt.exchanges.kalshi._history import (
    CandlePayload,
    normalize_kalshi_candle_history,
    parse_kalshi_settlement_timestamp,
)
from pmkt.exchanges.kalshi.client import _kalshi_candle_query_windows
from pmkt.records import (
    CandleOHLC,
    HistoryQueryWindow,
    KalshiCandle,
    RequestObservation,
)


BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)
NOW = BASE + timedelta(days=500)
TICKER = "KX TEST/ONE"
EVENT = "KX EVENT/ONE"
SERIES = "KX SERIES/ONE"


def _historical_row(end: datetime, *, close: str | None = "0.4000") -> dict[str, Any]:
    return {
        "end_period_ts": int(end.timestamp()),
        "open_interest": "12.50",
        "price": {
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "mean": close,
            "previous": "0.3500",
        },
        "volume": "2.25",
        "yes_ask": {
            "open": "0.4500",
            "high": "0.4600",
            "low": "0.4400",
            "close": "0.4500",
        },
        "yes_bid": {
            "open": "0.3500",
            "high": "0.3600",
            "low": "0.3400",
            "close": "0.3500",
        },
    }


def _live_row(end: datetime, *, empty_price: bool = False) -> dict[str, Any]:
    price: dict[str, object] = {}
    if not empty_price:
        price = {
            "open_dollars": "0.4000",
            "high_dollars": "0.4200",
            "low_dollars": "0.3900",
            "close_dollars": "0.4100",
            "mean_dollars": "0.4050",
            "previous_dollars": "0.3800",
        }
    return {
        "end_period_ts": int(end.timestamp()),
        "open_interest_fp": "12.50",
        "price": price,
        "volume_fp": "2.25",
        "yes_ask": {
            "open_dollars": "0.4500",
            "high_dollars": "0.4600",
            "low_dollars": "0.4400",
            "close_dollars": "0.4500",
        },
        "yes_bid": {
            "open_dollars": "0.3500",
            "high_dollars": "0.3600",
            "low_dollars": "0.3400",
            "close_dollars": "0.3500",
        },
    }


def _market(*, settlement: str | None = None) -> dict[str, object]:
    return {
        "market": {
            "ticker": TICKER,
            "event_ticker": EVENT,
            "market_type": "binary",
            "status": "finalized" if settlement else "active",
            "settlement_ts": settlement,
        }
    }


def _event() -> dict[str, object]:
    return {"event": {"event_ticker": EVENT, "series_ticker": SERIES}}


def _cutoff() -> dict[str, str]:
    return {
        "market_settled_ts": (BASE + timedelta(days=5)).isoformat(),
        "trades_created_ts": (BASE - timedelta(days=100)).isoformat(),
    }


def _observation(request_id: str = "candle-test") -> RequestObservation:
    return RequestObservation(
        request_id=request_id,
        venue="kalshi",
        data_scope="synthetic",
        transport_origin="caller_supplied",
        origin="https://offline.invalid",
        endpoint_template="/historical/markets/{ticker}/candlesticks",
        effective_parameters=(),
        started_at_utc=BASE,
        received_at_utc=BASE,
        attempt_count=1,
        outcome="success",
        status_code=200,
    )


@pytest.mark.parametrize("digits", ["1", "12", "123", "1234", "12345", "123456"])
def test_routing_timestamp_fraction_precision_is_python_version_independent(
    digits: str,
) -> None:
    parsed = parse_kalshi_settlement_timestamp(
        f"2026-01-01T00:00:00.{digits}Z"
    )
    assert parsed is not None
    assert parsed.microsecond == int(digits.ljust(6, "0"))


def test_routing_timestamp_rejects_submicrosecond_precision() -> None:
    with pytest.raises(InvalidDataError, match="microsecond precision"):
        parse_kalshi_settlement_timestamp("2026-01-01T00:00:00.1234567Z")


async def _explicit_history(
    payload: object,
    *,
    start: datetime = BASE,
    end: datetime = BASE + timedelta(hours=3),
    period_minutes: int = 60,
    invalid_rows: str = "raise",
    max_candles: int = 100,
) -> Any:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.startswith("/historical/markets/")
        return httpx.Response(200, json=payload)

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
    ) as client:
        return await client.get_candles(
            KalshiMarketRef(TICKER),
            start=start,
            end=end,
            period_minutes=period_minutes,  # type: ignore[arg-type]
            source="historical",
            invalid_rows=invalid_rows,  # type: ignore[arg-type]
            max_candles=max_candles,
            deadline_s=2.0,
        )


async def _explicit_live(
    payload: object,
    *,
    invalid_rows: str = "raise",
) -> Any:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/markets/"):
            return httpx.Response(200, json=_market())
        if request.url.path.startswith("/events/"):
            return httpx.Response(200, json=_event())
        return httpx.Response(200, json=payload)

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
    ) as client:
        return await client.get_candles(
            KalshiMarketRef(TICKER),
            start=BASE,
            end=BASE + timedelta(hours=3),
            period_minutes=60,
            source="live",
            invalid_rows=invalid_rows,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_explicit_historical_decodes_units_without_series_or_fallback() -> None:
    requests: list[httpx.Request] = []
    payload = {
        "ticker": TICKER,
        "candlesticks": [_historical_row(BASE + timedelta(hours=1))],
        "unknown_native": {"kept": True},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=payload)

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
    ) as client:
        result = await client.get_candles(
            KalshiMarketRef(TICKER, series_ticker="contradictory-unused-hint"),
            start=BASE,
            end=BASE + timedelta(hours=2),
            period_minutes=60,
            source="historical",
        )

    assert len(requests) == 1
    assert requests[0].url.raw_path.startswith(
        b"/historical/markets/KX%20TEST%2FONE/candlesticks"
    )
    assert "include_latest_before_start" not in requests[0].url.params
    candle = result.candles[0]
    assert candle.traded_price.close == 0.4
    assert candle.traded_price_mean == 0.4
    assert candle.traded_price_previous == 0.35
    assert candle.volume_contracts == 2.25
    assert candle.open_interest_contracts == 12.5
    assert result.native_payloads[0]["unknown_native"] == {"kept": True}
    assert result.coverage.datasets == ("historical",)


@pytest.mark.asyncio
async def test_explicit_live_resolves_verified_event_series_and_disables_projection() -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path.startswith("/markets/"):
            return httpx.Response(200, json=_market())
        if request.url.path.startswith("/events/"):
            return httpx.Response(200, json=_event())
        if request.url.path.startswith("/series/"):
            assert request.url.params["include_latest_before_start"] == "false"
            return httpx.Response(
                200,
                json={
                    "ticker": TICKER,
                    "candlesticks": [_live_row(BASE + timedelta(hours=1))],
                },
            )
        raise AssertionError(request.url)

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
    ) as client:
        result = await client.get_candles(
            KalshiMarketRef(TICKER, series_ticker=SERIES),
            start=BASE,
            end=BASE + timedelta(hours=2),
            period_minutes=60,
            source="live",
        )

    assert paths == [
        "/markets/KX TEST/ONE",
        "/events/KX EVENT/ONE",
        "/series/KX SERIES/ONE/markets/KX TEST/ONE/candlesticks",
    ]
    assert result.candles[0].dataset == "live"
    assert result.candles[0].traded_price_mean == 0.405
    assert "live_synthetic_projection_disabled" in result.quality_flags


@pytest.mark.asyncio
async def test_live_rejects_contradictory_event_series_before_candles() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if request.url.path.startswith("/markets/"):
            return httpx.Response(200, json=_market())
        return httpx.Response(200, json=_event())

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
    ) as client:
        with pytest.raises(InvalidDataError, match="event series mismatch"):
            await client.get_candles(
                KalshiMarketRef(TICKER, series_ticker="WRONG"),
                start=BASE,
                end=BASE + timedelta(hours=2),
                period_minutes=60,
                source="live",
            )
    assert calls == 2


@pytest.mark.asyncio
async def test_live_rejects_event_alias_and_candle_series_contradictions() -> None:
    event_alias_conflict = _event()
    assert isinstance(event_alias_conflict["event"], dict)
    event_alias_conflict["event"]["ticker"] = "OTHER-EVENT"

    def alias_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/markets/"):
            return httpx.Response(200, json=_market())
        return httpx.Response(200, json=event_alias_conflict)

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(alias_handler),
        _utc_now=lambda: NOW,
    ) as client:
        with pytest.raises(InvalidDataError, match="conflicting ticker aliases"):
            await client.get_candles(
                KalshiMarketRef(TICKER),
                start=BASE,
                end=BASE + timedelta(hours=2),
                period_minutes=60,
                source="live",
            )

    def candle_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/markets/"):
            return httpx.Response(200, json=_market())
        if request.url.path.startswith("/events/"):
            return httpx.Response(200, json=_event())
        return httpx.Response(
            200,
            json={
                "ticker": TICKER,
                "series_ticker": "OTHER-SERIES",
                "candlesticks": [],
            },
        )

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(candle_handler),
        _utc_now=lambda: NOW,
    ) as client:
        with pytest.raises(InvalidDataError, match="candle series mismatch"):
            await client.get_candles(
                KalshiMarketRef(TICKER),
                start=BASE,
                end=BASE + timedelta(hours=2),
                period_minutes=60,
                source="live",
            )


@pytest.mark.asyncio
async def test_auto_routes_on_market_settlement_cutoff_not_requested_or_trade_cutoff() -> None:
    paths: list[str] = []
    settlement = (BASE + timedelta(days=4)).isoformat()

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path == "/historical/cutoff":
            return httpx.Response(200, json=_cutoff())
        if request.url.path.startswith("/markets/"):
            return httpx.Response(200, json=_market(settlement=settlement))
        return httpx.Response(
            200,
            json={
                "ticker": TICKER,
                "candlesticks": [_historical_row(BASE + timedelta(hours=1))],
            },
        )

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
    ) as client:
        result = await client.get_candles(
            KalshiMarketRef(TICKER),
            start=BASE + timedelta(days=100),
            end=BASE + timedelta(days=100, hours=2),
            period_minutes=60,
        )
    assert paths[-1].startswith("/historical/markets/")
    assert not any(path.startswith("/events/") for path in paths)
    assert result.historical_cutoff_utc == BASE + timedelta(days=5)
    assert result.coverage.raw_rows == 1
    assert result.coverage.outside_window_rows == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("settlement_delta", "expected_dataset"),
    [(-1, "historical"), (0, "live"), (1, "live")],
)
async def test_auto_cutoff_boundary_is_strictly_before(
    settlement_delta: int, expected_dataset: str
) -> None:
    cutoff = BASE + timedelta(days=5)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/historical/cutoff":
            return httpx.Response(
                200, json={"market_settled_ts": cutoff.isoformat()}
            )
        if request.url.path.startswith("/markets/"):
            return httpx.Response(
                200,
                json=_market(
                    settlement=(cutoff + timedelta(seconds=settlement_delta)).isoformat()
                ),
            )
        if request.url.path.startswith("/events/"):
            return httpx.Response(200, json=_event())
        row = (
            _historical_row(BASE + timedelta(hours=1))
            if expected_dataset == "historical"
            else _live_row(BASE + timedelta(hours=1))
        )
        return httpx.Response(200, json={"ticker": TICKER, "candlesticks": [row]})

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
    ) as client:
        result = await client.get_candles(
            KalshiMarketRef(TICKER),
            start=BASE,
            end=BASE + timedelta(hours=2),
            period_minutes=60,
        )
    assert result.candles[0].dataset == expected_dataset
    assert result.routing_market is not None
    assert result.routing_market.settlement_ts is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_value", [True, 123, "2026-01-01T00:00:00.1234567Z"])
async def test_auto_rejects_malformed_native_settlement_evidence(
    bad_value: object,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/historical/cutoff":
            return httpx.Response(200, json=_cutoff())
        payload = _market()
        assert isinstance(payload["market"], dict)
        payload["market"]["settlement_ts"] = bad_value
        return httpx.Response(200, json=payload)

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
    ) as client:
        with pytest.raises(InvalidDataError, match="settlement_ts"):
            await client.get_candles(
                KalshiMarketRef(TICKER),
                start=BASE,
                end=BASE + timedelta(hours=2),
                period_minutes=60,
            )


@pytest.mark.asyncio
async def test_auto_live_404_uses_bounded_historical_existence_lookup() -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path == "/historical/cutoff":
            return httpx.Response(200, json=_cutoff())
        if request.url.raw_path == b"/markets/KX%20TEST%2FONE":
            return httpx.Response(404)
        if request.url.raw_path == b"/historical/markets/KX%20TEST%2FONE":
            return httpx.Response(200, json=_market(settlement=BASE.isoformat()))
        return httpx.Response(200, json={"ticker": TICKER, "candlesticks": []})

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
    ) as client:
        result = await client.get_candles(
            KalshiMarketRef(TICKER),
            start=BASE,
            end=BASE + timedelta(hours=2),
            period_minutes=60,
        )
    assert len(paths) == 4
    assert paths[-1].endswith("/candlesticks")
    assert "live_metadata_absent_archive_verified" in result.quality_flags


@pytest.mark.asyncio
async def test_auto_archive_metadata_checks_supplied_series_evidence() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/historical/cutoff":
            return httpx.Response(200, json=_cutoff())
        if request.url.raw_path == b"/markets/KX%20TEST%2FONE":
            return httpx.Response(404)
        historical = _market(settlement=BASE.isoformat())
        assert isinstance(historical["market"], dict)
        historical["market"]["series_ticker"] = "OTHER-SERIES"
        return httpx.Response(200, json=historical)

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
    ) as client:
        with pytest.raises(InvalidDataError, match="detail series mismatch"):
            await client.get_candles(
                KalshiMarketRef(TICKER, series_ticker=SERIES),
                start=BASE,
                end=BASE + timedelta(hours=2),
                period_minutes=60,
            )


@pytest.mark.asyncio
async def test_auto_candle_404_falls_back_once_but_empty_success_does_not() -> None:
    async def run(live_status: int) -> list[str]:
        paths: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            paths.append(request.url.path)
            if request.url.path == "/historical/cutoff":
                return httpx.Response(200, json=_cutoff())
            if request.url.path.startswith("/markets/"):
                return httpx.Response(200, json=_market())
            if request.url.path.startswith("/events/"):
                return httpx.Response(200, json=_event())
            if request.url.path.startswith("/series/"):
                return httpx.Response(
                    live_status,
                    json={"ticker": TICKER, "candlesticks": []}
                    if live_status == 200
                    else None,
                )
            return httpx.Response(200, json={"ticker": TICKER, "candlesticks": []})

        async with AsyncKalshiClient(
            base_url="https://offline.invalid",
            transport=httpx.MockTransport(handler),
            _utc_now=lambda: NOW,
        ) as client:
            result = await client.get_candles(
                KalshiMarketRef(TICKER),
                start=BASE,
                end=BASE + timedelta(hours=2),
                period_minutes=60,
            )
        if live_status == 404:
            assert "qualified_404_migration_fallback" in result.quality_flags
        return paths

    empty_paths = await run(200)
    fallback_paths = await run(404)
    assert len(empty_paths) == 4
    assert len(fallback_paths) == 5
    assert fallback_paths[-1].startswith("/historical/markets/")


@pytest.mark.asyncio
async def test_migration_overlap_conflict_issues_name_each_originating_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "pmkt.exchanges.kalshi.client.KALSHI_CANDLE_QUERY_PERIODS_PER_REQUEST", 1
    )
    live_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal live_calls
        if request.url.path == "/historical/cutoff":
            return httpx.Response(200, json=_cutoff())
        if request.url.path.startswith("/markets/"):
            return httpx.Response(200, json=_market())
        if request.url.path.startswith("/events/"):
            return httpx.Response(200, json=_event())
        if request.url.path.startswith("/series/"):
            live_calls += 1
            if live_calls == 2:
                return httpx.Response(404)
            return httpx.Response(
                200,
                json={
                    "ticker": TICKER,
                    "candlesticks": [_live_row(BASE + timedelta(hours=1))],
                },
            )
        return httpx.Response(
            200,
            json={
                "ticker": TICKER,
                "candlesticks": [_historical_row(BASE + timedelta(hours=1))],
            },
        )

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
    ) as client:
        result = await client.get_candles(
            KalshiMarketRef(TICKER),
            start=BASE,
            end=BASE + timedelta(hours=2),
            period_minutes=60,
            invalid_rows="report",
        )
    assert result.candles == ()
    assert result.coverage.raw_rows == 3
    assert result.coverage.conflicting_rows == 3
    assert len(result.issues) == 3
    assert sum(issue.occurrence_count for issue in result.issues) == 3
    observation_ids = {observation.request_id for observation in result.observations}
    assert {issue.request_id for issue in result.issues} <= observation_ids


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403, 500])
async def test_auto_never_falls_back_on_non_404_http_errors(status: int) -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path == "/historical/cutoff":
            return httpx.Response(200, json=_cutoff())
        if request.url.path.startswith("/markets/"):
            return httpx.Response(200, json=_market())
        if request.url.path.startswith("/events/"):
            return httpx.Response(200, json=_event())
        return httpx.Response(status)

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
        request_policy=None,
    ) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_candles(
                KalshiMarketRef(TICKER),
                start=BASE,
                end=BASE + timedelta(hours=2),
                period_minutes=60,
            )
    assert not any(path.startswith("/historical/markets/") for path in paths)


@pytest.mark.asyncio
async def test_auto_timeout_never_falls_back_and_drains_request() -> None:
    paths: list[str] = []
    entered = asyncio.Event()
    drained = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path == "/historical/cutoff":
            return httpx.Response(200, json=_cutoff())
        if request.url.path.startswith("/markets/"):
            return httpx.Response(200, json=_market())
        if request.url.path.startswith("/events/"):
            return httpx.Response(200, json=_event())
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            drained.set()

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
    ) as client:
        await client.market(TICKER)
        paths.clear()
        task = asyncio.create_task(
            client.get_candles(
                KalshiMarketRef(TICKER),
                start=BASE,
                end=BASE + timedelta(hours=2),
                period_minutes=60,
                deadline_s=0.1,
            )
        )
        await asyncio.wait_for(entered.wait(), timeout=2.0)
        with pytest.raises(OperationTimeoutError):
            await asyncio.wait_for(task, timeout=2.0)
    assert drained.is_set()
    assert not any(path.startswith("/historical/markets/") for path in paths)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_rows", ["raise", "report"])
async def test_unknown_layout_and_identity_mismatch_are_always_fatal(
    invalid_rows: str,
) -> None:
    bad_layout = _historical_row(BASE + timedelta(hours=1))
    bad_layout["volume_fp"] = bad_layout.pop("volume")
    with pytest.raises(InvalidDataError):
        await _explicit_history(
            {"ticker": TICKER, "candlesticks": [bad_layout]},
            invalid_rows=invalid_rows,
        )
    with pytest.raises(InvalidDataError, match="ticker mismatch"):
        await _explicit_history(
            {"ticker": "OTHER", "candlesticks": []}, invalid_rows=invalid_rows
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_rows", ["raise", "report"])
@pytest.mark.parametrize("payload", [[], {}, {"candlesticks": {}}])
async def test_candle_envelope_errors_are_always_fatal(
    payload: object, invalid_rows: str
) -> None:
    with pytest.raises(InvalidDataError):
        await _explicit_history(payload, invalid_rows=invalid_rows)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_rows", ["raise", "report"])
async def test_live_and_archive_component_layouts_never_cross_decode(
    invalid_rows: str,
) -> None:
    historical = _historical_row(BASE + timedelta(hours=1))
    live = _live_row(BASE + timedelta(hours=1))
    historical["end_period_ts"] = "also-invalid"
    live["end_period_ts"] = "also-invalid"
    with pytest.raises(InvalidDataError):
        await _explicit_live(
            {"ticker": TICKER, "candlesticks": [historical]},
            invalid_rows=invalid_rows,
        )
    with pytest.raises(InvalidDataError):
        await _explicit_history(
            {"ticker": TICKER, "candlesticks": [live]},
            invalid_rows=invalid_rows,
        )


@pytest.mark.asyncio
async def test_report_mode_layout_preflight_precedes_scalar_decoding() -> None:
    live = _live_row(BASE + timedelta(hours=1))
    live["price"]["open_dollars"] = "bad"
    live["yes_bid"] = _historical_row(BASE + timedelta(hours=1))["yes_bid"]
    with pytest.raises(InvalidDataError, match="missing required open_dollars"):
        await _explicit_live(
            {"ticker": TICKER, "candlesticks": [live]}, invalid_rows="report"
        )

    historical = _historical_row(BASE + timedelta(hours=1))
    historical["price"]["open"] = "bad"
    historical["yes_bid"] = _live_row(BASE + timedelta(hours=1))["yes_bid"]
    with pytest.raises(InvalidDataError, match="missing required open"):
        await _explicit_history(
            {"ticker": TICKER, "candlesticks": [historical]},
            invalid_rows="report",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate",
    [
        lambda row: row.update(end_period_ts=True),
        lambda row: row.update(end_period_ts=1.5),
        lambda row: row.update(end_period_ts="bad"),
        lambda row: row.update(end_period_ts=10**100),
        lambda row: row["price"].update(close=True),
        lambda row: row["price"].update(close="nan"),
        lambda row: row["price"].update(close="inf"),
        lambda row: row["price"].update(close="1.1000"),
        lambda row: row.update(volume=True),
        lambda row: row.update(volume="-0.1"),
    ],
)
async def test_report_mode_scalar_error_matrix_preserves_adjacent_valids(
    mutate: Any,
) -> None:
    invalid = _historical_row(BASE + timedelta(hours=2))
    mutate(invalid)
    result = await _explicit_history(
        {
            "ticker": TICKER,
            "candlesticks": [
                _historical_row(BASE + timedelta(hours=1), close="0.3000"),
                invalid,
                _historical_row(BASE + timedelta(hours=3), close="0.5000"),
            ],
        },
        end=BASE + timedelta(hours=4),
        invalid_rows="report",
    )
    assert [candle.traded_price.close for candle in result.candles] == [0.3, 0.5]
    assert result.coverage.rejected_rows == 1


@pytest.mark.asyncio
async def test_report_mode_preserves_valid_invalid_valid_and_bounds_examples() -> None:
    invalid_timestamp = _historical_row(BASE + timedelta(hours=1))
    invalid_timestamp["end_period_ts"] = "bad"
    rows: list[object] = [
        _historical_row(BASE + timedelta(hours=1), close="0.3000"),
        *[dict(invalid_timestamp) for _ in range(25)],
        _historical_row(BASE + timedelta(hours=2), close="0.5000"),
    ]
    result = await _explicit_history(
        {"ticker": TICKER, "candlesticks": rows}, invalid_rows="report"
    )
    assert [candle.traded_price.close for candle in result.candles] == [0.3, 0.5]
    assert result.coverage.raw_rows == 27
    assert result.coverage.rejected_rows == 25
    assert result.issues[0].occurrence_count == 25
    assert len(result.issues[0].examples) == 20


@pytest.mark.asyncio
async def test_reconciliation_precedes_containment_and_cap() -> None:
    outside_a = _historical_row(BASE, close="0.2000")
    outside_b = _historical_row(BASE, close="0.3000")
    inside = _historical_row(BASE + timedelta(hours=2), close="0.4000")
    payload = {
        "ticker": TICKER,
        "candlesticks": [outside_a, outside_a, outside_b, inside, outside_a],
    }
    with pytest.raises(InvalidDataError, match="conflicting values"):
        await _explicit_history(payload, start=BASE + timedelta(hours=1))
    result = await _explicit_history(
        payload,
        start=BASE + timedelta(hours=1),
        invalid_rows="report",
        max_candles=1,
    )
    assert len(result.candles) == 1
    assert result.coverage.rejected_rows == 4
    assert result.coverage.conflicting_rows == 4
    assert result.coverage.duplicate_rows == 0
    assert result.coverage.outside_window_rows == 0


@pytest.mark.asyncio
async def test_quote_only_candle_is_legitimate_and_boundaries_are_fully_contained() -> None:
    rows = [
        _historical_row(BASE + timedelta(hours=1), close=None),
        _historical_row(BASE + timedelta(hours=2), close=None),
    ]
    result = await _explicit_history(
        {"ticker": TICKER, "candlesticks": rows},
        start=BASE + timedelta(minutes=30),
        end=BASE + timedelta(hours=2),
    )
    assert len(result.candles) == 1
    assert result.candles[0].period_start_utc == BASE + timedelta(hours=1)
    assert result.candles[0].traded_price.close is None
    assert result.candles[0].yes_ask.close == 0.45
    assert "no_traded_price_ohlc" in result.candles[0].quality_flags
    assert result.coverage.outside_window_rows == 1
    assert result.coverage.synthetic_rows == 0


@pytest.mark.asyncio
async def test_empty_conversions_keep_typed_utc_columns_and_metadata() -> None:
    result = await _explicit_history({"ticker": TICKER, "candlesticks": []})
    table = result.to_arrow()
    frame = result.to_pandas()
    assert str(table.schema.field("period_start_utc").type) == "timestamp[us, tz=UTC]"
    assert str(table.schema.field("period_end_utc").type) == "timestamp[us, tz=UTC]"
    assert table.schema.metadata[b"market_ticker"] == TICKER.encode()
    assert str(frame.dtypes["period_start_utc"]) == "datetime64[ns, UTC]"
    assert str(frame.dtypes["period_end_utc"]) == "datetime64[ns, UTC]"
    assert frame.attrs["interpretation_id"] == result.interpretation_id


def test_candle_record_period_rejects_bool_and_float() -> None:
    values: dict[str, object] = {
        "market": KalshiMarketRef(TICKER),
        "period_start_utc": BASE,
        "period_end_utc": BASE + timedelta(minutes=1),
        "native_end_timestamp": int((BASE + timedelta(minutes=1)).timestamp()),
        "period_minutes": 1,
        "dataset": "historical",
        "traded_price": CandleOHLC(None, None, None, None),
        "traded_price_mean": None,
        "traded_price_previous": None,
        "yes_bid": CandleOHLC(None, None, None, None),
        "yes_ask": CandleOHLC(None, None, None, None),
        "volume_contracts": None,
        "open_interest_contracts": None,
        "quality_flags": (),
        "native_payload": {},
    }
    for invalid in (True, 1.0):
        with pytest.raises(TypeError):
            KalshiCandle(**{**values, "period_minutes": invalid})  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_running_period_uses_one_frozen_utc_now() -> None:
    clock_calls = 0

    def clock() -> datetime:
        nonlocal clock_calls
        clock_calls += 1
        return BASE + timedelta(hours=1, minutes=30)

    payload = {
        "ticker": TICKER,
        "candlesticks": [
            _historical_row(BASE + timedelta(hours=1)),
            _historical_row(BASE + timedelta(hours=2)),
        ],
    }

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=clock,
    ) as client:
        result = await client.get_candles(
            KalshiMarketRef(TICKER),
            start=BASE,
            end=BASE + timedelta(hours=3),
            period_minutes=60,
            source="historical",
        )
    assert clock_calls == 1
    assert len(result.candles) == 1
    assert result.coverage.running_rows == 1


@pytest.mark.asyncio
async def test_utc_first_fold_ordering_and_subsecond_containment() -> None:
    berlin = ZoneInfo("Europe/Berlin")
    earlier_utc_later_wall = datetime(2024, 10, 27, 2, 45, tzinfo=berlin, fold=0)
    later_utc_earlier_wall = datetime(2024, 10, 27, 2, 30, tzinfo=berlin, fold=1)
    empty = await _explicit_history(
        {"ticker": TICKER, "candlesticks": []},
        start=earlier_utc_later_wall,
        end=later_utc_earlier_wall,
    )
    assert empty.requested_start_utc < empty.requested_end_utc
    with pytest.raises(ValueError, match="UTC normalization"):
        await _explicit_history(
            {"ticker": TICKER, "candlesticks": []},
            start=later_utc_earlier_wall,
            end=earlier_utc_later_wall,
        )

    result = await _explicit_history(
        {
            "ticker": TICKER,
            "candlesticks": [
                _historical_row(BASE + timedelta(hours=1)),
                _historical_row(BASE + timedelta(hours=2)),
            ],
        },
        start=BASE + timedelta(microseconds=500_000),
        end=BASE + timedelta(hours=2, microseconds=500_000),
    )
    assert [candle.period_end_utc for candle in result.candles] == [
        BASE + timedelta(hours=2)
    ]
    query = result.coverage.queried_windows[0]
    assert query.start_utc == BASE
    assert query.end_utc == BASE + timedelta(hours=2, seconds=1)


@pytest.mark.asyncio
async def test_daily_fixed_elapsed_intervals_validate_both_dst_transitions() -> None:
    ny = ZoneInfo("America/New_York")
    local_starts = [
        datetime(2026, 3, 8, tzinfo=ny),
        datetime(2026, 3, 9, tzinfo=ny),
        datetime(2025, 11, 2, tzinfo=ny),
        datetime(2025, 11, 3, tzinfo=ny),
    ]
    rows = [
        _historical_row(start.astimezone(timezone.utc) + timedelta(days=1))
        for start in local_starts
    ]
    start = min(item.astimezone(timezone.utc) for item in local_starts)
    end = max(item.astimezone(timezone.utc) + timedelta(days=1) for item in local_starts)
    result = await _explicit_history(
        {"ticker": TICKER, "candlesticks": rows},
        start=start,
        end=end,
        period_minutes=1440,
    )
    assert len(result.candles) == 4
    assert all(
        candle.period_end_utc - candle.period_start_utc == timedelta(days=1)
        for candle in result.candles
    )
    end_hours = [candle.period_end_utc.astimezone(ny).hour for candle in result.candles]
    assert 1 in end_hours
    assert 23 in end_hours


@pytest.mark.asyncio
async def test_daily_saved_dst_labels_and_invalid_alignment() -> None:
    valid_labels = [1773032400, 1773115200, 1762142400, 1762232400]
    valid_rows = [
        {**_historical_row(BASE + timedelta(days=1)), "end_period_ts": label}
        for label in valid_labels
    ]
    start = datetime.fromtimestamp(min(valid_labels) - 86_400, tz=timezone.utc)
    end = datetime.fromtimestamp(max(valid_labels), tz=timezone.utc)
    result = await _explicit_history(
        {"ticker": TICKER, "candlesticks": valid_rows},
        start=start,
        end=end,
        period_minutes=1440,
    )
    assert [candle.native_end_timestamp for candle in result.candles] == sorted(
        valid_labels
    )

    invalid = {**valid_rows[0], "end_period_ts": valid_labels[0] + 3600}
    with pytest.raises(InvalidDataError, match="New_York midnight"):
        await _explicit_history(
            {"ticker": TICKER, "candlesticks": [invalid]},
            start=start,
            end=end,
            period_minutes=1440,
        )


@pytest.mark.asyncio
async def test_varied_internal_chunk_sizes_return_same_candles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        _historical_row(BASE + timedelta(hours=hour), close=f"0.{hour}000")
        for hour in range(1, 6)
    ]

    async def run(chunk_size: int) -> tuple[tuple[int, float | None], ...]:
        monkeypatch.setattr(
            "pmkt.exchanges.kalshi.client.KALSHI_CANDLE_QUERY_PERIODS_PER_REQUEST",
            chunk_size,
        )

        def handler(request: httpx.Request) -> httpx.Response:
            start_ts = int(request.url.params["start_ts"])
            end_ts = int(request.url.params["end_ts"])
            selected = [
                row
                for row in rows
                if start_ts <= int(row["end_period_ts"]) <= end_ts
            ]
            return httpx.Response(
                200, json={"ticker": TICKER, "candlesticks": selected}
            )

        async with AsyncKalshiClient(
            base_url="https://offline.invalid",
            transport=httpx.MockTransport(handler),
            _utc_now=lambda: NOW,
        ) as client:
            result = await client.get_candles(
                KalshiMarketRef(TICKER),
                start=BASE + timedelta(minutes=30),
                end=BASE + timedelta(hours=5, minutes=45),
                period_minutes=60,
                source="historical",
            )
        return tuple(
            (candle.native_end_timestamp, candle.traded_price.close)
            for candle in result.candles
        )

    expected = tuple(
        (int((BASE + timedelta(hours=hour)).timestamp()), float(f"0.{hour}000"))
        for hour in range(2, 6)
    )
    assert await run(1) == await run(2) == await run(100) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ({"market": "bad"}, TypeError),
        ({"start": BASE.replace(tzinfo=None)}, ValueError),
        ({"end": BASE.replace(tzinfo=None)}, ValueError),
        ({"period_minutes": True}, TypeError),
        ({"period_minutes": 5}, ValueError),
        ({"max_candles": True}, TypeError),
        ({"max_candles": 0}, ValueError),
        ({"deadline_s": None}, TypeError),
        ({"deadline_s": math.inf}, ValueError),
        ({"source": "archive"}, ValueError),
        ({"invalid_rows": "ignore"}, ValueError),
    ],
)
async def test_invalid_inputs_fail_before_io(
    changes: dict[str, object], error: type[Exception]
) -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"ticker": TICKER, "candlesticks": []})

    kwargs: dict[str, object] = {
        "market": KalshiMarketRef(TICKER),
        "start": BASE,
        "end": BASE + timedelta(hours=1),
        "period_minutes": 60,
        "source": "historical",
    }
    kwargs.update(changes)
    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
    ) as client:
        with pytest.raises(error):
            await client.get_candles(**kwargs)  # type: ignore[arg-type]
    assert calls == 0


@pytest.mark.asyncio
async def test_output_cap_raises_without_truncation() -> None:
    with pytest.raises(ResultLimitExceededError, match="max_candles=1"):
        await _explicit_history(
            {
                "ticker": TICKER,
                "candlesticks": [
                    _historical_row(BASE + timedelta(hours=1)),
                    _historical_row(BASE + timedelta(hours=2)),
                ],
            },
            max_candles=1,
        )


@pytest.mark.asyncio
async def test_native_methods_keep_return_and_parameter_defaults() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"candlesticks": []})

    async with AsyncKalshiClient(
        base_url="https://offline.invalid", transport=httpx.MockTransport(handler)
    ) as client:
        native = await client.market_candlesticks(
            "SERIES",
            "TICKER",
            start_ts=1,
            end_ts=2,
            period_interval=1,
        )
        historical = await client.historical_market_candlesticks(
            "TICKER", start_ts=1, end_ts=2, period_interval=1
        )
    assert native == historical == {"candlesticks": []}
    assert "include_latest_before_start" not in requests[0].url.params
    assert "include_latest_before_start" not in requests[1].url.params


@pytest.mark.asyncio
@pytest.mark.parametrize("termination", ["timeout", "cancel"])
async def test_termination_drains_transport_and_client_is_reusable(
    termination: str,
) -> None:
    calls = 0
    entered = asyncio.Event()
    drained = asyncio.Event()
    release = asyncio.Event()

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 2:
            entered.set()
            try:
                await release.wait()
            finally:
                await asyncio.sleep(0)
                drained.set()
        return httpx.Response(200, json={"ticker": TICKER, "candlesticks": []})

    client = AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
    )
    await client.__aenter__()
    try:
        await client.get_candles(
            KalshiMarketRef(TICKER),
            start=BASE,
            end=BASE + timedelta(hours=1),
            period_minutes=60,
            source="historical",
        )
        task = asyncio.create_task(
            client.get_candles(
                KalshiMarketRef(TICKER),
                start=BASE,
                end=BASE + timedelta(hours=1),
                period_minutes=60,
                source="historical",
                deadline_s=0.1 if termination == "timeout" else 5.0,
            )
        )
        await asyncio.wait_for(entered.wait(), timeout=2.0)
        if termination == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(OperationTimeoutError):
                await asyncio.wait_for(task, timeout=2.0)
        assert drained.is_set()
        reused = await client.get_candles(
            KalshiMarketRef(TICKER),
            start=BASE,
            end=BASE + timedelta(hours=1),
            period_minutes=60,
            source="historical",
        )
    finally:
        await client.close()
    assert reused.candles == ()
    assert calls == 3


def test_normalization_and_window_planning_checkpoint_expiry() -> None:
    clock_calls = 0

    def clock() -> float:
        nonlocal clock_calls
        clock_calls += 1
        return 0.0 if clock_calls == 1 else 2.0

    observation = _observation()
    expiry = OperationExpiry(deadline_monotonic=1.0, _clock=clock)
    with pytest.raises(OperationTimeoutError):
        normalize_kalshi_candle_history(
            (
                CandlePayload(
                    {
                        "ticker": TICKER,
                        "candlesticks": [
                            _historical_row(BASE + timedelta(hours=1))
                        ]
                        * 1000,
                    },
                    "historical",
                    observation,
                ),
            ),
            market=KalshiMarketRef(TICKER),
            requested_start_utc=BASE,
            requested_end_utc=BASE + timedelta(hours=2),
            period_minutes=60,
            requested_source="historical",
            completed_through_utc=NOW,
            historical_cutoff_utc=None,
            queried_windows=(
                HistoryQueryWindow(
                    BASE,
                    BASE + timedelta(hours=2),
                    "historical",
                    "/historical/markets/{ticker}/candlesticks",
                ),
            ),
            observations=(observation,),
            max_candles=10,
            invalid_rows="report",
            routing_flags=(),
            routing_market=None,
            expiry=expiry,
        )

    clock_calls = 0
    expiry = OperationExpiry(deadline_monotonic=1.0, _clock=clock)
    with pytest.raises(OperationTimeoutError):
        _kalshi_candle_query_windows(
            BASE,
            BASE + timedelta(days=100),
            period_minutes=1,
            periods_per_request=1,
            expiry=expiry,
        )


@pytest.mark.asyncio
async def test_get_candles_checkpoints_after_result_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = {"expired": False}
    expiry = OperationExpiry(
        deadline_monotonic=1.0,
        _clock=lambda: 2.0 if state["expired"] else 0.0,
    )

    class FakeExpiry:
        @classmethod
        def bounded(cls, _timeout_s: float) -> OperationExpiry:
            return expiry

    original = normalize_kalshi_candle_history

    def wrapped(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        state["expired"] = True
        return result

    monkeypatch.setattr("pmkt.exchanges.kalshi.client.OperationExpiry", FakeExpiry)
    monkeypatch.setattr(
        "pmkt.exchanges.kalshi.client.normalize_kalshi_candle_history", wrapped
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ticker": TICKER, "candlesticks": []})

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
    ) as client:
        with pytest.raises(OperationTimeoutError):
            await client.get_candles(
                KalshiMarketRef(TICKER),
                start=BASE,
                end=BASE + timedelta(hours=1),
                period_minutes=60,
                source="historical",
            )


@pytest.mark.asyncio
async def test_explicit_source_404_never_switches() -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(404)

    async with AsyncKalshiClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
        _utc_now=lambda: NOW,
    ) as client:
        with pytest.raises(MarketNotFoundError):
            await client.get_candles(
                KalshiMarketRef(TICKER),
                start=BASE,
                end=BASE + timedelta(hours=1),
                period_minutes=60,
                source="historical",
            )
    assert len(paths) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_rows", ["raise", "report"])
@pytest.mark.parametrize(
    "price,expected,flag",
    [
        (
            {"previous_dollars": "0.4000"},
            CandleOHLC(None, None, None, None),
            "no_traded_price_ohlc",
        ),
        (
            {"previous_dollars": "0.4000", "mean_dollars": "0.4200"},
            CandleOHLC(None, None, None, None),
            "no_traded_price_ohlc",
        ),
        (
            {"close_dollars": "0.4100"},
            CandleOHLC(None, None, None, 0.41),
            "partial_traded_price_ohlc",
        ),
    ],
)
async def test_live_sparse_trade_prices_preserve_quotes_and_missing_evidence(
    invalid_rows, price, expected, flag
):
    # Synthetic regression for the previous-only live shape observed on 2026-09-12.
    row = _live_row(BASE + timedelta(hours=1))
    row["price"] = price
    row["volume_fp"] = "0.00"
    result = await _explicit_live(
        {"ticker": TICKER, "candlesticks": [row]}, invalid_rows=invalid_rows
    )
    (candle,) = result.candles
    assert candle.traded_price == expected
    assert candle.traded_price_previous == (
        0.4 if "previous_dollars" in price else None
    )
    assert candle.traded_price_mean == (0.42 if "mean_dollars" in price else None)
    assert candle.quality_flags == (flag,)
    assert candle.yes_bid.close == 0.35
    assert candle.yes_ask.close == 0.45
    assert candle.volume_contracts == 0
    assert candle.native_payload["price"] == price
    assert result.coverage.rejected_rows == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("component", ["yes_bid", "yes_ask"])
async def test_sparse_price_does_not_relax_quote_layout(component):
    row = _live_row(BASE + timedelta(hours=1))
    row["price"] = {"previous_dollars": "0.4"}
    row[component].pop("open_dollars")
    with pytest.raises(InvalidDataError, match="missing required open_dollars"):
        await _explicit_live(
            {"ticker": TICKER, "candlesticks": [row]}, invalid_rows="report"
        )


@pytest.mark.asyncio
async def test_sparse_price_invalid_value_still_obeys_row_policy():
    bad = _live_row(BASE + timedelta(hours=1))
    bad["price"] = {"previous_dollars": "1.5"}
    good = _live_row(BASE + timedelta(hours=2))
    payload = {"ticker": TICKER, "candlesticks": [bad, good]}
    with pytest.raises(InvalidDataError, match="previous_dollars"):
        await _explicit_live(payload)
    result = await _explicit_live(payload, invalid_rows="report")
    assert len(result.candles) == 1
    assert result.coverage.rejected_rows == 1
