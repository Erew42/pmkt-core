from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from pmkt.data.registry import (
    CANONICAL_SCHEMA_VERSION,
    EVENT_SCHEMA_VERSION,
    MARKET_SCHEMA_VERSION,
    POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
    KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION_V2,
    KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION_V2,
    CONTRACT_EVIDENCE_SCHEMA_VERSION,
    MARKET_RESOLUTION_SCHEMA_VERSION,
    INSTRUMENT_SCHEMA_VERSION,
    TRADE_SCHEMA_VERSION,
    BOOK_TAPE_EVENT_SCHEMA_VERSION,
    BOOK_TAPE_LEVEL_SCHEMA_VERSION,
    BOOK_TAPE_CONTROL_SCHEMA_VERSION,
    STREAM_LIFECYCLE_SCHEMA_VERSION,
    MATCH_SCHEMA_VERSION_V1,
    MATCH_SCHEMA_VERSION,
    CO_RESOLUTION_OBSERVATION_SCHEMA_VERSION,
    CO_RESOLUTION_SCORE_SCHEMA_VERSION,
    TRACKING_MATCH_SCHEMA_VERSION,
    MATCH_RELATION_SCHEMA_VERSION,
    FEED_HEALTH_SCHEMA_VERSION,
    TRACKING_HEALTH_SCHEMA_VERSION,
    SIGNAL_SCHEMA_VERSION,
    EXECUTION_SIZING_PLAN_SCHEMA_VERSION,
    MAKER_QUOTE_PLAN_SCHEMA_VERSION,
    ORDER_INTENT_SCHEMA_VERSION,
    ORDER_STATE_SCHEMA_VERSION,
    PAPER_FILL_SCHEMA_VERSION,
    PAPER_POSITION_SCHEMA_VERSION,
    PASSIVE_QUOTE_EVALUATION_SCHEMA_VERSION,
    PASSIVE_FILL_SCHEMA_VERSION,
    PASSIVE_MARKOUT_SCHEMA_VERSION,
    ARBITRAGE_CANDIDATE_SCHEMA_VERSION,
    RUN_MANIFEST_SCHEMA_VERSION,
    SCAN_CYCLE_SCHEMA_VERSION,
    CANARY_CANDIDATE_SCHEMA_VERSION,
    CANARY_REJECTION_SCHEMA_VERSION,
    SOAK_RUN_PLAN_SCHEMA_VERSION,
    SOAK_RUN_REPORT_SCHEMA_VERSION,
    BASKET_ORDER_INTENT_SCHEMA_VERSION,
    BASKET_PAPER_FILL_SCHEMA_VERSION,
    BASKET_PAPER_POSITION_SCHEMA_VERSION,
    HISTORICAL_PRICE_SCHEMA_VERSION,
    VENUE_HISTORY_CAPABILITY_SCHEMA_VERSION,
    HISTORICAL_BACKFILL_GAP_SCHEMA_VERSION,
    TOPBOOK_CAPTURE_GAP_SCHEMA_VERSION,
    CONVERGENCE_OBSERVATION_SCHEMA_VERSION,
    CONVERGENCE_SUMMARY_SCHEMA_VERSION,
    BACKTEST_REPORT_SCHEMA_VERSION,
    VENUES,
    EVENT_COLUMNS,
    MARKET_COLUMNS,
    POLYMARKET_MARKET_SNAPSHOT_COLUMNS,
    KALSHI_MARKET_SNAPSHOT_COLUMNS,
    POLYMARKET_MARKET_SNAPSHOT_COLUMNS_V2,
    KALSHI_MARKET_SNAPSHOT_COLUMNS_V2,
    CONTRACT_EVIDENCE_COLUMNS,
    MARKET_RESOLUTION_COLUMNS,
    INSTRUMENT_COLUMNS,
    TRADE_COLUMNS,
    MATCH_COLUMNS_V1,
    MATCH_COLUMNS,
    CO_RESOLUTION_OBSERVATION_COLUMNS,
    CO_RESOLUTION_SCORE_COLUMNS,
    TRACKING_MATCH_COLUMNS,
    MATCH_RELATION_COLUMNS,
    TRACKING_HEALTH_COLUMNS,
    FEED_HEALTH_COLUMNS,
    SIGNAL_COLUMNS,
    EXECUTION_SIZING_PLAN_COLUMNS,
    MAKER_QUOTE_PLAN_COLUMNS,
    ORDER_INTENT_COLUMNS,
    ORDER_STATE_COLUMNS,
    PAPER_FILL_COLUMNS,
    PAPER_POSITION_COLUMNS,
    PASSIVE_QUOTE_EVALUATION_COLUMNS,
    PASSIVE_FILL_COLUMNS,
    PASSIVE_MARKOUT_COLUMNS,
    ARBITRAGE_CANDIDATE_COLUMNS,
    RUN_MANIFEST_COLUMNS,
    SCAN_CYCLE_COLUMNS,
    CANARY_CANDIDATE_COLUMNS,
    CANARY_REJECTION_COLUMNS,
    SOAK_RUN_PLAN_COLUMNS,
    SOAK_RUN_REPORT_COLUMNS,
    BASKET_ORDER_INTENT_COLUMNS,
    BASKET_PAPER_FILL_COLUMNS,
    BASKET_PAPER_POSITION_COLUMNS,
    HISTORICAL_PRICE_COLUMNS,
    VENUE_HISTORY_CAPABILITY_COLUMNS,
    HISTORICAL_BACKFILL_GAP_COLUMNS,
    TOPBOOK_CAPTURE_GAP_COLUMNS,
    CONVERGENCE_OBSERVATION_COLUMNS,
    CONVERGENCE_SUMMARY_COLUMNS,
    BACKTEST_REPORT_COLUMNS,
)


def canonical_fixed_decimal(value: Any) -> str:
    """Return the unique non-exponent fixed-decimal spelling for a number."""
    if isinstance(value, bool):
        raise ValueError("boolean is not a decimal value")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"invalid decimal value: {value!r}") from None
    if not parsed.is_finite():
        raise ValueError("decimal value must be finite")
    rendered = format(parsed, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    if rendered in {"", "-0"}:
        return "0"
    return rendered


@dataclass(frozen=True)
class SchemaSpec:
    name: str
    version: str
    columns: tuple[str, ...]
    description: str


class _RegistryBackedSchemaSpecs(Mapping[str, SchemaSpec]):
    """Legacy schema metadata derived from the richer TableSpec registry."""

    def __init__(self) -> None:
        self._cache: dict[str, SchemaSpec] | None = None

    def _materialized(self) -> dict[str, SchemaSpec]:
        if self._cache is None:
            from pmkt.data.registry import list_table_specs

            self._cache = {
                table.name: SchemaSpec(
                    name=table.name,
                    version=table.version,
                    columns=table.columns,
                    description=table.description,
                )
                for table in list_table_specs()
            }
        return self._cache

    def __getitem__(self, key: str) -> SchemaSpec:
        return self._materialized()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._materialized())

    def __len__(self) -> int:
        return len(self._materialized())


SCHEMA_SPECS: Mapping[str, SchemaSpec] = _RegistryBackedSchemaSpecs()


def canonical_row(schema_version: str, **values: Any) -> dict[str, Any]:
    """Return a stable canonical row with unknown keys dropped.

    These row builders are intentionally light-weight. Exchange-specific modules
    should still perform API parsing and unit conversion before calling them.
    """
    from pmkt.data.registry import get_table_spec

    columns = get_table_spec(schema_version).columns
    row: dict[str, Any] = {column: None for column in columns}
    row["schema_version"] = schema_version
    row.update({key: value for key, value in values.items() if key in row})
    return row


def event_row(**values: Any) -> dict[str, Any]:
    return canonical_row(EVENT_SCHEMA_VERSION, **values)


def market_row(**values: Any) -> dict[str, Any]:
    return canonical_row(MARKET_SCHEMA_VERSION, **values)


def polymarket_market_snapshot_row(**values: Any) -> dict[str, Any]:
    return canonical_row(POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION, **values)


def kalshi_market_snapshot_row(**values: Any) -> dict[str, Any]:
    return canonical_row(KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION, **values)


def polymarket_market_snapshot_v2_row(**values: Any) -> dict[str, Any]:
    return canonical_row(POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION_V2, **values)


def kalshi_market_snapshot_v2_row(**values: Any) -> dict[str, Any]:
    return canonical_row(KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION_V2, **values)


def contract_evidence_row(**values: Any) -> dict[str, Any]:
    return canonical_row(CONTRACT_EVIDENCE_SCHEMA_VERSION, **values)


def market_resolution_row(**values: Any) -> dict[str, Any]:
    return canonical_row(MARKET_RESOLUTION_SCHEMA_VERSION, **values)


def instrument_row(**values: Any) -> dict[str, Any]:
    return canonical_row(INSTRUMENT_SCHEMA_VERSION, **values)


def trade_row(**values: Any) -> dict[str, Any]:
    return canonical_row(TRADE_SCHEMA_VERSION, **values)


def book_tape_event_row(**values: Any) -> dict[str, Any]:
    return canonical_row(BOOK_TAPE_EVENT_SCHEMA_VERSION, **values)


def book_tape_level_row(**values: Any) -> dict[str, Any]:
    return canonical_row(BOOK_TAPE_LEVEL_SCHEMA_VERSION, **values)


def book_tape_control_row(**values: Any) -> dict[str, Any]:
    return canonical_row(BOOK_TAPE_CONTROL_SCHEMA_VERSION, **values)


def stream_lifecycle_row(**values: Any) -> dict[str, Any]:
    return canonical_row(STREAM_LIFECYCLE_SCHEMA_VERSION, **values)


_MATCH_V1_TO_V2_ALIASES = {
    "polymarket_venue_market_id": "polymarket_market_key",
    "polymarket_instrument_id": "polymarket_instrument_key",
    "kalshi_venue_market_id": "kalshi_market_key",
    "kalshi_instrument_id": "kalshi_instrument_key",
    "match_status": "review_status",
}


def market_match_row(**values: Any) -> dict[str, Any]:
    normalized = dict(values)
    for legacy_key, current_key in _MATCH_V1_TO_V2_ALIASES.items():
        if legacy_key in normalized and current_key not in normalized:
            normalized[current_key] = normalized[legacy_key]
    return canonical_row(MATCH_SCHEMA_VERSION, **normalized)


def co_resolution_observation_row(**values: Any) -> dict[str, Any]:
    return canonical_row(CO_RESOLUTION_OBSERVATION_SCHEMA_VERSION, **values)


def co_resolution_score_row(**values: Any) -> dict[str, Any]:
    return canonical_row(CO_RESOLUTION_SCORE_SCHEMA_VERSION, **values)


def tracking_match_row(**values: Any) -> dict[str, Any]:
    return canonical_row(TRACKING_MATCH_SCHEMA_VERSION, **values)


def match_relation_row(**values: Any) -> dict[str, Any]:
    return canonical_row(MATCH_RELATION_SCHEMA_VERSION, **values)


def feed_health_row(**values: Any) -> dict[str, Any]:
    return canonical_row(FEED_HEALTH_SCHEMA_VERSION, **values)


def tracking_health_row(**values: Any) -> dict[str, Any]:
    return canonical_row(TRACKING_HEALTH_SCHEMA_VERSION, **values)


def signal_row(**values: Any) -> dict[str, Any]:
    return canonical_row(SIGNAL_SCHEMA_VERSION, **values)


def execution_sizing_plan_row(**values: Any) -> dict[str, Any]:
    return canonical_row(EXECUTION_SIZING_PLAN_SCHEMA_VERSION, **values)


def maker_quote_plan_row(**values: Any) -> dict[str, Any]:
    return canonical_row(MAKER_QUOTE_PLAN_SCHEMA_VERSION, **values)


def order_intent_row(**values: Any) -> dict[str, Any]:
    return canonical_row(ORDER_INTENT_SCHEMA_VERSION, **values)


def order_state_row(**values: Any) -> dict[str, Any]:
    return canonical_row(ORDER_STATE_SCHEMA_VERSION, **values)


def paper_fill_row(**values: Any) -> dict[str, Any]:
    return canonical_row(PAPER_FILL_SCHEMA_VERSION, **values)


def paper_position_row(**values: Any) -> dict[str, Any]:
    return canonical_row(PAPER_POSITION_SCHEMA_VERSION, **values)


def passive_quote_evaluation_row(**values: Any) -> dict[str, Any]:
    return canonical_row(PASSIVE_QUOTE_EVALUATION_SCHEMA_VERSION, **values)


def passive_fill_row(**values: Any) -> dict[str, Any]:
    return canonical_row(PASSIVE_FILL_SCHEMA_VERSION, **values)


def passive_markout_row(**values: Any) -> dict[str, Any]:
    return canonical_row(PASSIVE_MARKOUT_SCHEMA_VERSION, **values)


def arbitrage_candidate_row(**values: Any) -> dict[str, Any]:
    return canonical_row(ARBITRAGE_CANDIDATE_SCHEMA_VERSION, **values)


def run_manifest_row(**values: Any) -> dict[str, Any]:
    if values.get("started_at_utc") is None:
        values["started_at_utc"] = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")
    return canonical_row(RUN_MANIFEST_SCHEMA_VERSION, **values)


def scan_cycle_row(**values: Any) -> dict[str, Any]:
    return canonical_row(SCAN_CYCLE_SCHEMA_VERSION, **values)


def canary_candidate_row(**values: Any) -> dict[str, Any]:
    return canonical_row(CANARY_CANDIDATE_SCHEMA_VERSION, **values)


def canary_rejection_row(**values: Any) -> dict[str, Any]:
    return canonical_row(CANARY_REJECTION_SCHEMA_VERSION, **values)


def soak_run_plan_row(**values: Any) -> dict[str, Any]:
    return canonical_row(SOAK_RUN_PLAN_SCHEMA_VERSION, **values)


def soak_run_report_row(**values: Any) -> dict[str, Any]:
    return canonical_row(SOAK_RUN_REPORT_SCHEMA_VERSION, **values)


def basket_order_intent_row(**values: Any) -> dict[str, Any]:
    return canonical_row(BASKET_ORDER_INTENT_SCHEMA_VERSION, **values)


def basket_paper_fill_row(**values: Any) -> dict[str, Any]:
    return canonical_row(BASKET_PAPER_FILL_SCHEMA_VERSION, **values)


def basket_paper_position_row(**values: Any) -> dict[str, Any]:
    return canonical_row(BASKET_PAPER_POSITION_SCHEMA_VERSION, **values)


def historical_price_row(**values: Any) -> dict[str, Any]:
    return canonical_row(HISTORICAL_PRICE_SCHEMA_VERSION, **values)


def venue_history_capability_row(**values: Any) -> dict[str, Any]:
    return canonical_row(VENUE_HISTORY_CAPABILITY_SCHEMA_VERSION, **values)


def historical_backfill_gap_row(**values: Any) -> dict[str, Any]:
    return canonical_row(HISTORICAL_BACKFILL_GAP_SCHEMA_VERSION, **values)


def topbook_capture_gap_row(**values: Any) -> dict[str, Any]:
    return canonical_row(TOPBOOK_CAPTURE_GAP_SCHEMA_VERSION, **values)


def convergence_observation_row(**values: Any) -> dict[str, Any]:
    return canonical_row(CONVERGENCE_OBSERVATION_SCHEMA_VERSION, **values)


def convergence_summary_row(**values: Any) -> dict[str, Any]:
    return canonical_row(CONVERGENCE_SUMMARY_SCHEMA_VERSION, **values)


def backtest_report_row(**values: Any) -> dict[str, Any]:
    return canonical_row(BACKTEST_REPORT_SCHEMA_VERSION, **values)


__all__ = [
    "ARBITRAGE_CANDIDATE_COLUMNS",
    "ARBITRAGE_CANDIDATE_SCHEMA_VERSION",
    "BACKTEST_REPORT_COLUMNS",
    "BACKTEST_REPORT_SCHEMA_VERSION",
    "BASKET_ORDER_INTENT_COLUMNS",
    "BASKET_ORDER_INTENT_SCHEMA_VERSION",
    "BASKET_PAPER_FILL_COLUMNS",
    "BASKET_PAPER_FILL_SCHEMA_VERSION",
    "BASKET_PAPER_POSITION_COLUMNS",
    "BASKET_PAPER_POSITION_SCHEMA_VERSION",
    "CONVERGENCE_OBSERVATION_COLUMNS",
    "CONVERGENCE_OBSERVATION_SCHEMA_VERSION",
    "CONVERGENCE_SUMMARY_COLUMNS",
    "CONVERGENCE_SUMMARY_SCHEMA_VERSION",
    "CANONICAL_SCHEMA_VERSION",
    "CANARY_CANDIDATE_COLUMNS",
    "CANARY_CANDIDATE_SCHEMA_VERSION",
    "CANARY_REJECTION_COLUMNS",
    "CANARY_REJECTION_SCHEMA_VERSION",
    "CO_RESOLUTION_OBSERVATION_COLUMNS",
    "CO_RESOLUTION_OBSERVATION_SCHEMA_VERSION",
    "CO_RESOLUTION_SCORE_COLUMNS",
    "CO_RESOLUTION_SCORE_SCHEMA_VERSION",
    "EVENT_COLUMNS",
    "EVENT_SCHEMA_VERSION",
    "EXECUTION_SIZING_PLAN_COLUMNS",
    "EXECUTION_SIZING_PLAN_SCHEMA_VERSION",
    "MAKER_QUOTE_PLAN_COLUMNS",
    "MAKER_QUOTE_PLAN_SCHEMA_VERSION",
    "FEED_HEALTH_COLUMNS",
    "FEED_HEALTH_SCHEMA_VERSION",
    "HISTORICAL_BACKFILL_GAP_COLUMNS",
    "HISTORICAL_BACKFILL_GAP_SCHEMA_VERSION",
    "HISTORICAL_PRICE_COLUMNS",
    "HISTORICAL_PRICE_SCHEMA_VERSION",
    "INSTRUMENT_COLUMNS",
    "INSTRUMENT_SCHEMA_VERSION",
    "KALSHI_MARKET_SNAPSHOT_COLUMNS",
    "KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION",
    "KALSHI_MARKET_SNAPSHOT_COLUMNS_V2",
    "KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION_V2",
    "CONTRACT_EVIDENCE_COLUMNS",
    "CONTRACT_EVIDENCE_SCHEMA_VERSION",
    "MARKET_COLUMNS",
    "MARKET_RESOLUTION_COLUMNS",
    "MARKET_RESOLUTION_SCHEMA_VERSION",
    "MARKET_SCHEMA_VERSION",
    "MATCH_COLUMNS",
    "MATCH_COLUMNS_V1",
    "MATCH_RELATION_COLUMNS",
    "MATCH_RELATION_SCHEMA_VERSION",
    "MATCH_SCHEMA_VERSION",
    "MATCH_SCHEMA_VERSION_V1",
    "ORDER_INTENT_COLUMNS",
    "ORDER_INTENT_SCHEMA_VERSION",
    "ORDER_STATE_COLUMNS",
    "ORDER_STATE_SCHEMA_VERSION",
    "PAPER_FILL_COLUMNS",
    "PAPER_FILL_SCHEMA_VERSION",
    "PASSIVE_FILL_COLUMNS",
    "PASSIVE_FILL_SCHEMA_VERSION",
    "PASSIVE_MARKOUT_COLUMNS",
    "PASSIVE_MARKOUT_SCHEMA_VERSION",
    "PASSIVE_QUOTE_EVALUATION_COLUMNS",
    "PASSIVE_QUOTE_EVALUATION_SCHEMA_VERSION",
    "PAPER_POSITION_COLUMNS",
    "PAPER_POSITION_SCHEMA_VERSION",
    "POLYMARKET_MARKET_SNAPSHOT_COLUMNS",
    "POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION",
    "POLYMARKET_MARKET_SNAPSHOT_COLUMNS_V2",
    "POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION_V2",
    "RUN_MANIFEST_COLUMNS",
    "RUN_MANIFEST_SCHEMA_VERSION",
    "SCHEMA_SPECS",
    "SCAN_CYCLE_COLUMNS",
    "SCAN_CYCLE_SCHEMA_VERSION",
    "SOAK_RUN_PLAN_COLUMNS",
    "SOAK_RUN_REPORT_COLUMNS",
    "SOAK_RUN_PLAN_SCHEMA_VERSION",
    "SOAK_RUN_REPORT_SCHEMA_VERSION",
    "SIGNAL_COLUMNS",
    "SIGNAL_SCHEMA_VERSION",
    "TRACKING_MATCH_COLUMNS",
    "TRACKING_MATCH_SCHEMA_VERSION",
    "TRACKING_HEALTH_COLUMNS",
    "TRACKING_HEALTH_SCHEMA_VERSION",
    "TOPBOOK_CAPTURE_GAP_COLUMNS",
    "TOPBOOK_CAPTURE_GAP_SCHEMA_VERSION",
    "TRADE_COLUMNS",
    "TRADE_SCHEMA_VERSION",
    "BOOK_TAPE_EVENT_SCHEMA_VERSION",
    "BOOK_TAPE_LEVEL_SCHEMA_VERSION",
    "BOOK_TAPE_CONTROL_SCHEMA_VERSION",
    "STREAM_LIFECYCLE_SCHEMA_VERSION",
    "VENUE_HISTORY_CAPABILITY_COLUMNS",
    "VENUE_HISTORY_CAPABILITY_SCHEMA_VERSION",
    "VENUES",
    "SchemaSpec",
    "arbitrage_candidate_row",
    "basket_order_intent_row",
    "basket_paper_fill_row",
    "basket_paper_position_row",
    "backtest_report_row",
    "book_tape_control_row",
    "book_tape_event_row",
    "book_tape_level_row",
    "canary_candidate_row",
    "canary_rejection_row",
    "canonical_fixed_decimal",
    "canonical_row",
    "co_resolution_observation_row",
    "co_resolution_score_row",
    "convergence_observation_row",
    "convergence_summary_row",
    "event_row",
    "execution_sizing_plan_row",
    "feed_health_row",
    "historical_backfill_gap_row",
    "historical_price_row",
    "instrument_row",
    "kalshi_market_snapshot_row",
    "kalshi_market_snapshot_v2_row",
    "contract_evidence_row",
    "maker_quote_plan_row",
    "market_match_row",
    "market_resolution_row",
    "match_relation_row",
    "market_row",
    "order_intent_row",
    "order_state_row",
    "paper_fill_row",
    "passive_fill_row",
    "passive_markout_row",
    "passive_quote_evaluation_row",
    "paper_position_row",
    "polymarket_market_snapshot_row",
    "polymarket_market_snapshot_v2_row",
    "run_manifest_row",
    "scan_cycle_row",
    "soak_run_plan_row",
    "soak_run_report_row",
    "signal_row",
    "stream_lifecycle_row",
    "topbook_capture_gap_row",
    "tracking_match_row",
    "tracking_health_row",
    "trade_row",
    "venue_history_capability_row",
]
