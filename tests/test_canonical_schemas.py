from __future__ import annotations

import pmkt.data as data_module
import pmkt.data.canonical as canonical_module
import pmkt.data.registry as registry_module
import pmkt.data.schemas as schemas_module
import pmkt.data.validation as validation_module
from pmkt.data.canonical import (
    ARBITRAGE_CANDIDATE_COLUMNS,
    ARBITRAGE_CANDIDATE_SCHEMA_VERSION,
    BASKET_ORDER_INTENT_COLUMNS,
    BASKET_ORDER_INTENT_SCHEMA_VERSION,
    BASKET_PAPER_FILL_COLUMNS,
    BASKET_PAPER_FILL_SCHEMA_VERSION,
    BASKET_PAPER_POSITION_COLUMNS,
    BASKET_PAPER_POSITION_SCHEMA_VERSION,
    CANARY_CANDIDATE_COLUMNS,
    CANARY_CANDIDATE_SCHEMA_VERSION,
    CANARY_REJECTION_COLUMNS,
    CANARY_REJECTION_SCHEMA_VERSION,
    EVENT_COLUMNS,
    EVENT_SCHEMA_VERSION,
    EXECUTION_SIZING_PLAN_COLUMNS,
    EXECUTION_SIZING_PLAN_SCHEMA_VERSION,
    FEED_HEALTH_COLUMNS,
    FEED_HEALTH_SCHEMA_VERSION,
    INSTRUMENT_COLUMNS,
    INSTRUMENT_SCHEMA_VERSION,
    KALSHI_MARKET_SNAPSHOT_COLUMNS,
    KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    MARKET_COLUMNS,
    MARKET_SCHEMA_VERSION,
    MATCH_COLUMNS,
    MATCH_RELATION_COLUMNS,
    MATCH_RELATION_SCHEMA_VERSION,
    MATCH_SCHEMA_VERSION,
    ORDER_INTENT_COLUMNS,
    ORDER_INTENT_SCHEMA_VERSION,
    ORDER_STATE_COLUMNS,
    ORDER_STATE_SCHEMA_VERSION,
    PAPER_FILL_COLUMNS,
    PAPER_FILL_SCHEMA_VERSION,
    PAPER_POSITION_COLUMNS,
    PAPER_POSITION_SCHEMA_VERSION,
    POLYMARKET_MARKET_SNAPSHOT_COLUMNS,
    POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
    RUN_MANIFEST_COLUMNS,
    RUN_MANIFEST_SCHEMA_VERSION,
    SCHEMA_SPECS,
    SCAN_CYCLE_COLUMNS,
    SCAN_CYCLE_SCHEMA_VERSION,
    SIGNAL_COLUMNS,
    SIGNAL_SCHEMA_VERSION,
    TRACKING_MATCH_COLUMNS,
    TRACKING_MATCH_SCHEMA_VERSION,
    TRACKING_HEALTH_COLUMNS,
    TRACKING_HEALTH_SCHEMA_VERSION,
    TRADE_COLUMNS,
    TRADE_SCHEMA_VERSION,
    arbitrage_candidate_row,
    basket_order_intent_row,
    basket_paper_fill_row,
    basket_paper_position_row,
    canary_candidate_row,
    canary_rejection_row,
    event_row,
    execution_sizing_plan_row,
    feed_health_row,
    instrument_row,
    kalshi_market_snapshot_row,
    market_match_row,
    match_relation_row,
    market_row,
    order_intent_row,
    order_state_row,
    paper_fill_row,
    paper_position_row,
    polymarket_market_snapshot_row,
    run_manifest_row,
    scan_cycle_row,
    signal_row,
    tracking_match_row,
    tracking_health_row,
    trade_row,
)


def test_schema_constants_are_registry_owned() -> None:
    assert canonical_module.EVENT_COLUMNS is registry_module.EVENT_COLUMNS
    assert canonical_module.MATCH_COLUMNS is registry_module.MATCH_COLUMNS
    assert canonical_module.RUN_MANIFEST_COLUMNS is registry_module.RUN_MANIFEST_COLUMNS
    assert schemas_module.TOPBOOK_COLUMNS is registry_module.TOPBOOK_COLUMNS
    assert schemas_module.DEPTH_COLUMNS is registry_module.DEPTH_COLUMNS
    assert data_module.MARKET_COLUMNS is registry_module.MARKET_COLUMNS
    assert data_module.TOPBOOK_COLUMNS is registry_module.TOPBOOK_COLUMNS


def test_data_package_lazy_attrs_resolve_canonical_registry_module() -> None:
    data_module.__dict__.pop("MARKET_COLUMNS", None)
    data_module.__dict__.pop("TOPBOOK_COLUMNS", None)

    assert data_module.MARKET_COLUMNS is registry_module.MARKET_COLUMNS
    assert data_module.TOPBOOK_COLUMNS is registry_module.TOPBOOK_COLUMNS


def test_data_package_exposes_strict_converter_without_replacing_cleaner() -> None:
    assert data_module.convert_frame_strict is validation_module.convert_frame_strict
    assert data_module.coerce_frame is validation_module.coerce_frame


def test_canonical_schema_specs_are_complete() -> None:
    from pmkt.data.registry import get_table_spec, list_table_specs

    registry_specs = {spec.name: spec for spec in list_table_specs()}

    assert set(SCHEMA_SPECS) == set(registry_specs)
    for spec in SCHEMA_SPECS.values():
        assert spec.columns[0] == "schema_version"
        assert spec.description
        table = get_table_spec(spec.version)
        assert spec.columns == table.columns
        assert spec.description == table.description
    assert {spec.version for spec in SCHEMA_SPECS.values()} == {
        spec.version for spec in registry_specs.values()
    }


def test_event_row_drops_unknown_fields_and_preserves_order() -> None:
    row = event_row(venue="polymarket", venue_event_id="event-1", ignored="x")

    assert list(row) == EVENT_COLUMNS
    assert row["schema_version"] == EVENT_SCHEMA_VERSION
    assert row["venue"] == "polymarket"
    assert "ignored" not in row


def test_market_and_instrument_rows_use_distinct_contract_boundaries() -> None:
    market = market_row(
        venue="kalshi",
        venue_market_id="KXTEST-YES",
        question="Will the event happen?",
        rules="Resolves yes if the event happens.",
    )
    instrument = instrument_row(
        venue="kalshi",
        venue_market_id="KXTEST-YES",
        instrument_id="KXTEST-YES:YES",
        outcome="YES",
    )

    assert list(market) == MARKET_COLUMNS
    assert list(instrument) == INSTRUMENT_COLUMNS
    assert market["schema_version"] == MARKET_SCHEMA_VERSION
    assert instrument["schema_version"] == INSTRUMENT_SCHEMA_VERSION
    assert market["question"] == "Will the event happen?"
    assert instrument["outcome"] == "YES"


def test_source_market_snapshot_rows_have_stable_source_boundaries() -> None:
    polymarket = polymarket_market_snapshot_row(
        market_id="pm-1",
        question="Will it rain?",
        open_time="2026-01-01T00:00:00Z",
        start_time="2026-01-01T00:00:00Z",
        token_ids=["token-yes", "token-no"],
        raw_json="{}",
        raw_json_sha256="0" * 64,
    )
    kalshi = kalshi_market_snapshot_row(
        exchange="kalshi",
        market_key="KXRAIN",
        instrument_key="KXRAIN:YES",
        question="Will it rain?",
        raw_json="{}",
        raw_json_sha256="1" * 64,
    )

    assert list(polymarket) == POLYMARKET_MARKET_SNAPSHOT_COLUMNS
    assert list(kalshi) == KALSHI_MARKET_SNAPSHOT_COLUMNS
    assert polymarket["schema_version"] == POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION
    assert kalshi["schema_version"] == KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION
    assert polymarket["market_id"] == "pm-1"
    assert polymarket["open_time"] == "2026-01-01T00:00:00Z"
    assert polymarket["start_time"] == "2026-01-01T00:00:00Z"
    assert kalshi["market_key"] == "KXRAIN"


def test_research_artifact_rows_have_stable_versions() -> None:
    trade = trade_row(venue="polymarket", instrument_id="token-1", price_dollars=0.51)
    match = market_match_row(
        polymarket_instrument_id="token-1",
        kalshi_instrument_id="KXTEST:YES",
        match_status="candidate",
    )
    relation = match_relation_row(
        match_id="match-1",
        polymarket_market_key="pm-1",
        polymarket_token_id="token-1",
        kalshi_market_key="KXTEST",
        relation_label="same_context_only",
        is_trade_equivalent=False,
    )
    candidate = arbitrage_candidate_row(
        polymarket_instrument_id="token-1",
        kalshi_instrument_id="KXTEST:YES",
        net_edge_dollars=0.02,
    )
    signal = signal_row(
        signal_id="signal-1",
        match_id="match-1",
        relation_label="exact_equivalent",
        execution_allowed=False,
    )
    sizing_plan = execution_sizing_plan_row(
        plan_id="plan-1",
        plan_fingerprint="fingerprint-1",
        match_id="match-1",
        signal_id="signal-1",
        side_plan="buy_polymarket_sell_kalshi",
        sizing_source="depth_adjusted",
        execution_allowed=True,
        risk_flags="",
        executable_size=1.0,
        cash_notional_dollars=0.4,
        max_loss_dollars=0.4,
        unhedged_first_leg_max_loss_dollars=0.4,
        gross_leg_notional_dollars=0.4,
        depth_levels_consumed=1,
        intended_orders_json=[],
        created_at_utc="2026-06-05T12:00:00+00:00",
    )
    tracking_health = tracking_health_row(
        match_id="match-1",
        observed_at_utc="2026-05-31T00:00:00+00:00",
        relation_label="same_context_only",
        tracking_ready=True,
        health_status="ready",
        health_flags="",
    )
    feed_health = feed_health_row(
        observed_at_utc="2026-05-31T00:00:00+00:00",
        local_sequence=1,
        venue="polymarket",
        shard_id="polymarket-0",
        connection_state="connected",
        instrument_count=1,
        quality_flags="",
    )
    tracking_match = tracking_match_row(
        tracking_pair_id="pm:pm-1|kalshi:KXTEST",
        match_tier="track_event_related",
        relation_type="same_event_different_outcome",
        polymarket_market_key="pm-1",
        kalshi_market_key="KXTEST",
    )
    intent = order_intent_row(
        order_intent_id="intent-1",
        signal_id="signal-1",
        venue="polymarket",
        instrument_id="token-1",
    )
    state = order_state_row(
        client_order_id="client-1",
        venue="polymarket",
        instrument_id="token-1",
        status="filled",
    )
    fill = paper_fill_row(
        paper_fill_id="fill-1",
        order_intent_id="intent-1",
        signal_id="signal-1",
        client_order_id="client-1",
    )
    position = paper_position_row(
        paper_position_id="position-1",
        signal_id="signal-1",
        match_id="match-1",
    )
    manifest = run_manifest_row(run_id="run-1")
    scan_cycle = scan_cycle_row(run_id="run-1", cycle_id="cycle-1", cycle_index=1)
    canary_candidate = canary_candidate_row(
        candidate_id="candidate-1",
        run_id="run-1",
        cycle_id="cycle-1",
        strategy_version="strategy",
        detector_name="detector",
        observed_at_utc="2026-06-02T00:00:00+00:00",
        formula_type="exhaustive_buy_all_yes",
        decision="reject",
        execution_allowed=False,
    )
    canary_rejection = canary_rejection_row(
        rejection_id="rejection-1",
        run_id="run-1",
        cycle_id="cycle-1",
        strategy_version="strategy",
        detector_name="detector",
        decision_reason="blocked",
    )
    basket_intent = basket_order_intent_row(
        basket_order_intent_id="basket-intent-1",
        candidate_id="candidate-1",
        run_id="run-1",
        cycle_id="cycle-1",
        venue="polymarket",
        instrument_id="token-1",
        action="buy",
        book_side="ask",
        limit_price=0.4,
        size_contracts=5.0,
        client_order_id="client-1",
        risk_check_status="passed",
        created_at_utc="2026-06-02T00:00:00+00:00",
        mode="paper",
    )
    basket_fill = basket_paper_fill_row(
        basket_paper_fill_id="basket-fill-1",
        basket_order_intent_id="basket-intent-1",
        candidate_id="candidate-1",
        client_order_id="client-1",
        venue="polymarket",
        instrument_id="token-1",
        action="buy",
        book_side="ask",
        fill_price_dollars=0.4,
        size_contracts=5.0,
        notional_dollars=2.0,
        fees_dollars=0.0,
        filled_at_utc="2026-06-02T00:00:00+00:00",
        simulator_version="sim",
        fill_type="full",
    )
    basket_position = basket_paper_position_row(
        basket_paper_position_id="basket-position-1",
        candidate_id="candidate-1",
        run_id="run-1",
        cycle_id="cycle-1",
        opened_at_utc="2026-06-02T00:00:00+00:00",
        as_of_utc="2026-06-02T00:00:00+00:00",
        status="open",
        filled_size_contracts=5.0,
        net_pnl_dollars=0.1,
    )

    assert list(trade) == TRADE_COLUMNS
    assert list(match) == MATCH_COLUMNS
    assert list(tracking_match) == TRACKING_MATCH_COLUMNS
    assert list(relation) == MATCH_RELATION_COLUMNS
    assert list(feed_health) == FEED_HEALTH_COLUMNS
    assert list(tracking_health) == TRACKING_HEALTH_COLUMNS
    assert list(signal) == SIGNAL_COLUMNS
    assert list(sizing_plan) == EXECUTION_SIZING_PLAN_COLUMNS
    assert list(intent) == ORDER_INTENT_COLUMNS
    assert list(state) == ORDER_STATE_COLUMNS
    assert list(fill) == PAPER_FILL_COLUMNS
    assert list(position) == PAPER_POSITION_COLUMNS
    assert list(candidate) == ARBITRAGE_CANDIDATE_COLUMNS
    assert list(manifest) == RUN_MANIFEST_COLUMNS
    assert list(scan_cycle) == SCAN_CYCLE_COLUMNS
    assert list(canary_candidate) == CANARY_CANDIDATE_COLUMNS
    assert list(canary_rejection) == CANARY_REJECTION_COLUMNS
    assert list(basket_intent) == BASKET_ORDER_INTENT_COLUMNS
    assert list(basket_fill) == BASKET_PAPER_FILL_COLUMNS
    assert list(basket_position) == BASKET_PAPER_POSITION_COLUMNS
    assert trade["schema_version"] == TRADE_SCHEMA_VERSION
    assert match["schema_version"] == MATCH_SCHEMA_VERSION
    assert tracking_match["schema_version"] == TRACKING_MATCH_SCHEMA_VERSION
    assert relation["schema_version"] == MATCH_RELATION_SCHEMA_VERSION
    assert feed_health["schema_version"] == FEED_HEALTH_SCHEMA_VERSION
    assert tracking_health["schema_version"] == TRACKING_HEALTH_SCHEMA_VERSION
    assert signal["schema_version"] == SIGNAL_SCHEMA_VERSION
    assert sizing_plan["schema_version"] == EXECUTION_SIZING_PLAN_SCHEMA_VERSION
    assert intent["schema_version"] == ORDER_INTENT_SCHEMA_VERSION
    assert state["schema_version"] == ORDER_STATE_SCHEMA_VERSION
    assert fill["schema_version"] == PAPER_FILL_SCHEMA_VERSION
    assert position["schema_version"] == PAPER_POSITION_SCHEMA_VERSION
    assert candidate["schema_version"] == ARBITRAGE_CANDIDATE_SCHEMA_VERSION
    assert manifest["schema_version"] == RUN_MANIFEST_SCHEMA_VERSION
    assert scan_cycle["schema_version"] == SCAN_CYCLE_SCHEMA_VERSION
    assert canary_candidate["schema_version"] == CANARY_CANDIDATE_SCHEMA_VERSION
    assert canary_rejection["schema_version"] == CANARY_REJECTION_SCHEMA_VERSION
    assert basket_intent["schema_version"] == BASKET_ORDER_INTENT_SCHEMA_VERSION
    assert basket_fill["schema_version"] == BASKET_PAPER_FILL_SCHEMA_VERSION
    assert basket_position["schema_version"] == BASKET_PAPER_POSITION_SCHEMA_VERSION
    assert match["polymarket_instrument_key"] == "token-1"
    assert match["kalshi_instrument_key"] == "KXTEST:YES"
    assert match["review_status"] == "candidate"
