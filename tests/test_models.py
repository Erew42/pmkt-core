import pytest

from pmkt.models import Event, Market


def test_market_preserves_dashboard_metrics_and_extra_fields() -> None:
    market = Market(
        id="market-1",
        question="Will this test pass?",
        marketCap=1234.5,
        volume24hr=98.7,
        liquidityNum=456.0,
        bestBid=0.42,
        bestAsk=0.47,
        lastTradePrice=0.44,
        unexpectedLiveMetric=12,
    )

    data = market.model_dump()

    assert data["market_cap"] == pytest.approx(1234.5)
    assert data["volume_24hr"] == pytest.approx(98.7)
    assert data["liquidity_num"] == pytest.approx(456.0)
    assert data["best_bid"] == pytest.approx(0.42)
    assert data["best_ask"] == pytest.approx(0.47)
    assert data["last_trade_price"] == pytest.approx(0.44)
    assert data["unexpectedLiveMetric"] == 12


def test_models_normalize_numeric_api_ids_to_strings() -> None:
    event = Event(
        id=123,
        title="Will numeric IDs parse?",
        markets=[
            {
                "id": 456,
                "question": "Nested market?",
                "conditionId": 789,
            }
        ],
    )

    assert event.id == "123"
    assert event.markets[0].id == "456"
    assert event.markets[0].condition_id == "789"


def test_market_declares_gamma_timing_fields() -> None:
    market = Market(
        id="market-1",
        question="Timing field market?",
        startDate="2026-01-01T00:00:00Z",
        startDateIso="2026-01-01",
        endDate="2026-02-01T00:00:00Z",
        endDateIso="2026-02-01",
        acceptingOrdersTimestamp="2026-01-01T00:05:00Z",
        eventStartTime="2026-01-15T00:00:00Z",
        gameStartTime="2026-01-15T00:00:00Z",
    )

    data = market.model_dump()
    alias_data = market.model_dump(by_alias=True)

    assert data["start_date"] == "2026-01-01T00:00:00Z"
    assert data["start_date_iso"] == "2026-01-01"
    assert data["end_date"] == "2026-02-01T00:00:00Z"
    assert data["end_date_iso"] == "2026-02-01"
    assert data["accepting_orders_timestamp"] == "2026-01-01T00:05:00Z"
    assert data["event_start_time"] == "2026-01-15T00:00:00Z"
    assert data["game_start_time"] == "2026-01-15T00:00:00Z"
    assert alias_data["startDate"] == "2026-01-01T00:00:00Z"
    assert alias_data["acceptingOrdersTimestamp"] == "2026-01-01T00:05:00Z"


def test_event_declares_gamma_start_time_fields() -> None:
    event = Event(
        id="event-1",
        title="Timing event?",
        startDate="2026-01-01T00:00:00Z",
        startTime="2026-01-15T00:00:00Z",
        eventDate="2026-01-15T00:00:00Z",
        createdAt="2025-12-31T00:00:00Z",
    )

    data = event.model_dump()

    assert data["start_date"] == "2026-01-01T00:00:00Z"
    assert data["start_time"] == "2026-01-15T00:00:00Z"
    assert data["event_date"] == "2026-01-15T00:00:00Z"
    assert data["created_at"] == "2025-12-31T00:00:00Z"
