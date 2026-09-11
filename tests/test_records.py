from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from pmkt.exchanges.kalshi import KalshiInstrumentRef, KalshiMarketRef
from pmkt.exchanges.polymarket import (
    PolymarketInstrumentRef,
    PolymarketMarketRef,
)
from pmkt.records import InstrumentRef, MarketRef, PolymarketFilter


def test_polymarket_filter_validates_qualified_surface() -> None:
    value = PolymarketFilter(
        condition_ids=("0xa", "0xb"),
        closed=False,
        tag_id="123",
        related_tags=True,
        question_contains="Inflation",
        outcome_count=2,
        has_instruments=True,
    )
    assert value.condition_ids == ("0xa", "0xb")

    with pytest.raises(TypeError, match="tuple"):
        PolymarketFilter(condition_ids=["0xa"])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="condition_id"):
        PolymarketFilter(condition_ids=(" ",))
    with pytest.raises(ValueError, match="decimal"):
        PolymarketFilter(tag_id="-1")
    with pytest.raises(ValueError, match="requires tag_id"):
        PolymarketFilter(related_tags=False)
    with pytest.raises(TypeError, match="outcome_count"):
        PolymarketFilter(outcome_count=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="positive"):
        PolymarketFilter(outcome_count=0)


@pytest.mark.parametrize(
    ("factory", "value"),
    [
        (PolymarketMarketRef, ""),
        (PolymarketMarketRef, "  "),
        (PolymarketInstrumentRef, ""),
        (KalshiMarketRef, ""),
    ],
)
def test_reference_identifiers_must_be_nonempty_strings(factory, value) -> None:
    with pytest.raises(ValueError):
        factory(value)
    with pytest.raises(TypeError):
        factory(7)


def test_reference_enrichment_does_not_change_identity_or_hash() -> None:
    bare_market = PolymarketMarketRef("42")
    enriched_market = PolymarketMarketRef("42", condition_id="0xcondition")
    bare_token = PolymarketInstrumentRef("token")
    enriched_token = PolymarketInstrumentRef(
        "token", market=enriched_market, outcome_index=1
    )
    bare_kalshi = KalshiMarketRef("SERIES-MARKET")
    enriched_kalshi = KalshiMarketRef("SERIES-MARKET", series_ticker="SERIES")

    assert bare_market == enriched_market
    assert hash(bare_market) == hash(enriched_market)
    assert bare_token == enriched_token
    assert hash(bare_token) == hash(enriched_token)
    assert bare_kalshi == enriched_kalshi
    assert hash(bare_kalshi) == hash(enriched_kalshi)
    assert bare_market.identity_key() == ("polymarket", "42")
    assert bare_token.identity_key() == ("polymarket", "token")
    assert bare_kalshi.identity_key() == ("kalshi", "SERIES-MARKET")


def test_kalshi_instrument_identity_uses_market_ticker_and_side() -> None:
    bare = KalshiInstrumentRef(KalshiMarketRef("TICKER"), "yes")
    enriched = KalshiInstrumentRef(
        KalshiMarketRef("TICKER", series_ticker="SERIES"), "yes"
    )

    assert bare == enriched
    assert hash(bare) == hash(enriched)
    assert bare.identity_key() == ("kalshi", "TICKER", "yes")


def test_reference_runtime_validation_rejects_wrong_classes_sides_and_indexes() -> None:
    polymarket = PolymarketMarketRef("market")
    kalshi = KalshiMarketRef("ticker")

    with pytest.raises(TypeError, match="PolymarketMarketRef"):
        PolymarketInstrumentRef("token", market=kalshi)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="KalshiMarketRef"):
        KalshiInstrumentRef(polymarket, "yes")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="side"):
        KalshiInstrumentRef(kalshi, "buy")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="outcome_index"):
        PolymarketInstrumentRef("token", outcome_index=True)
    with pytest.raises(TypeError, match="outcome_index"):
        PolymarketInstrumentRef("token", outcome_index=1.0)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="nonnegative"):
        PolymarketInstrumentRef("token", outcome_index=-1)
    with pytest.raises(ValueError, match="condition_id"):
        PolymarketMarketRef("market", condition_id=" ")
    with pytest.raises(ValueError, match="series_ticker"):
        KalshiMarketRef("ticker", series_ticker=" ")


def test_references_are_frozen_and_union_aliases_cover_delivered_types() -> None:
    market: MarketRef = PolymarketMarketRef("market")
    instrument: InstrumentRef = KalshiInstrumentRef(KalshiMarketRef("ticker"), "no")

    with pytest.raises(FrozenInstanceError):
        market.market_id = "changed"  # type: ignore[misc,union-attr]
    assert instrument.identity_key() == ("kalshi", "ticker", "no")


def test_venue_facades_reexport_the_canonical_record_classes() -> None:
    from pmkt.records import (
        KalshiInstrumentRef as CanonicalKalshiInstrumentRef,
        KalshiMarketRef as CanonicalKalshiMarketRef,
        PolymarketInstrumentRef as CanonicalPolymarketInstrumentRef,
        PolymarketMarketRef as CanonicalPolymarketMarketRef,
    )

    assert KalshiInstrumentRef is CanonicalKalshiInstrumentRef
    assert KalshiMarketRef is CanonicalKalshiMarketRef
    assert PolymarketInstrumentRef is CanonicalPolymarketInstrumentRef
    assert PolymarketMarketRef is CanonicalPolymarketMarketRef
