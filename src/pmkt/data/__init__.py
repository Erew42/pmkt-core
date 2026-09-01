from __future__ import annotations

from importlib import import_module
from typing import Any

_LAZY_MODULES: dict[str, str] = {
    "canonical": "pmkt.data.canonical",
    "contract_evidence": "pmkt.data.contract_evidence",
    "contract_evidence_manifest": "pmkt.data.contract_evidence_manifest",
    "features": "pmkt.data.features",
    "io": "pmkt.data.io",
    "manifests": "pmkt.data.manifests",
    "market_data": "pmkt.data.market_data",
    "normalize": "pmkt.data.normalize",
    "normalize_books": "pmkt.data.normalize_books",
    "registry": "pmkt.data.registry",
    "schemas": "pmkt.data.schemas",
    "storage": "pmkt.data.storage",
    "validation": "pmkt.data.validation",
}

_LAZY_ATTRS: dict[str, tuple[str, str]] = {
    "CONTRACT_EVIDENCE_COLUMNS": ("pmkt.data.registry", "CONTRACT_EVIDENCE_COLUMNS"),
    "CONTRACT_EVIDENCE_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "CONTRACT_EVIDENCE_SCHEMA_VERSION",
    ),
    "ARBITRAGE_CANDIDATE_COLUMNS": (
        "pmkt.data.registry",
        "ARBITRAGE_CANDIDATE_COLUMNS",
    ),
    "ARBITRAGE_CANDIDATE_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "ARBITRAGE_CANDIDATE_SCHEMA_VERSION",
    ),
    "BASKET_ORDER_INTENT_COLUMNS": (
        "pmkt.data.registry",
        "BASKET_ORDER_INTENT_COLUMNS",
    ),
    "BASKET_ORDER_INTENT_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "BASKET_ORDER_INTENT_SCHEMA_VERSION",
    ),
    "BASKET_PAPER_FILL_COLUMNS": ("pmkt.data.registry", "BASKET_PAPER_FILL_COLUMNS"),
    "BASKET_PAPER_FILL_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "BASKET_PAPER_FILL_SCHEMA_VERSION",
    ),
    "BASKET_PAPER_POSITION_COLUMNS": (
        "pmkt.data.registry",
        "BASKET_PAPER_POSITION_COLUMNS",
    ),
    "BASKET_PAPER_POSITION_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "BASKET_PAPER_POSITION_SCHEMA_VERSION",
    ),
    "CANARY_CANDIDATE_COLUMNS": ("pmkt.data.registry", "CANARY_CANDIDATE_COLUMNS"),
    "CANARY_CANDIDATE_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "CANARY_CANDIDATE_SCHEMA_VERSION",
    ),
    "CANARY_REJECTION_COLUMNS": ("pmkt.data.registry", "CANARY_REJECTION_COLUMNS"),
    "CANARY_REJECTION_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "CANARY_REJECTION_SCHEMA_VERSION",
    ),
    "CsvSink": ("pmkt.data.io", "CsvSink"),
    "DEFAULT_BOOK_BATCH_SIZE": ("pmkt.data.market_data", "DEFAULT_BOOK_BATCH_SIZE"),
    "DEFAULT_EVENT_SLUG": ("pmkt.data.market_data", "DEFAULT_EVENT_SLUG"),
    "DEPTH_COLUMNS": ("pmkt.data.registry", "DEPTH_COLUMNS"),
    "DEPTH_SCHEMA_VERSION": ("pmkt.data.registry", "DEPTH_SCHEMA_VERSION"),
    "EVENT_COLUMNS": ("pmkt.data.registry", "EVENT_COLUMNS"),
    "EVENT_SCHEMA_VERSION": ("pmkt.data.registry", "EVENT_SCHEMA_VERSION"),
    "FEED_HEALTH_COLUMNS": ("pmkt.data.registry", "FEED_HEALTH_COLUMNS"),
    "FEED_HEALTH_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "FEED_HEALTH_SCHEMA_VERSION",
    ),
    "FieldSpec": ("pmkt.data.registry", "FieldSpec"),
    "INSTRUMENT_COLUMNS": ("pmkt.data.registry", "INSTRUMENT_COLUMNS"),
    "INSTRUMENT_SCHEMA_VERSION": ("pmkt.data.registry", "INSTRUMENT_SCHEMA_VERSION"),
    "JsonlSink": ("pmkt.data.io", "JsonlSink"),
    "KALSHI_MARKET_SNAPSHOT_COLUMNS": (
        "pmkt.data.registry",
        "KALSHI_MARKET_SNAPSHOT_COLUMNS",
    ),
    "KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION",
    ),
    "KALSHI_MARKET_SNAPSHOT_COLUMNS_V2": (
        "pmkt.data.registry",
        "KALSHI_MARKET_SNAPSHOT_COLUMNS_V2",
    ),
    "KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION_V2": (
        "pmkt.data.registry",
        "KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION_V2",
    ),
    "MARKET_COLUMNS": ("pmkt.data.registry", "MARKET_COLUMNS"),
    "MARKET_RESOLUTION_COLUMNS": ("pmkt.data.registry", "MARKET_RESOLUTION_COLUMNS"),
    "MARKET_RESOLUTION_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "MARKET_RESOLUTION_SCHEMA_VERSION",
    ),
    "MARKET_SCHEMA_VERSION": ("pmkt.data.registry", "MARKET_SCHEMA_VERSION"),
    "ManifestDatasetValidation": (
        "pmkt.data.manifests",
        "ManifestDatasetValidation",
    ),
    "ManifestValidationReport": ("pmkt.data.manifests", "ManifestValidationReport"),
    "MATCH_COLUMNS": ("pmkt.data.registry", "MATCH_COLUMNS"),
    "MATCH_COLUMNS_V1": ("pmkt.data.registry", "MATCH_COLUMNS_V1"),
    "MATCH_RELATION_COLUMNS": ("pmkt.data.registry", "MATCH_RELATION_COLUMNS"),
    "MATCH_RELATION_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "MATCH_RELATION_SCHEMA_VERSION",
    ),
    "MATCH_SCHEMA_VERSION": ("pmkt.data.registry", "MATCH_SCHEMA_VERSION"),
    "MATCH_SCHEMA_VERSION_V1": ("pmkt.data.registry", "MATCH_SCHEMA_VERSION_V1"),
    "MAKER_QUOTE_PLAN_COLUMNS": ("pmkt.data.registry", "MAKER_QUOTE_PLAN_COLUMNS"),
    "MAKER_QUOTE_PLAN_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "MAKER_QUOTE_PLAN_SCHEMA_VERSION",
    ),
    "TRACKING_MATCH_COLUMNS": ("pmkt.data.registry", "TRACKING_MATCH_COLUMNS"),
    "TRACKING_MATCH_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "TRACKING_MATCH_SCHEMA_VERSION",
    ),
    "ORDER_INTENT_COLUMNS": ("pmkt.data.registry", "ORDER_INTENT_COLUMNS"),
    "ORDER_INTENT_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "ORDER_INTENT_SCHEMA_VERSION",
    ),
    "ORDER_STATE_COLUMNS": ("pmkt.data.registry", "ORDER_STATE_COLUMNS"),
    "ORDER_STATE_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "ORDER_STATE_SCHEMA_VERSION",
    ),
    "PAPER_FILL_COLUMNS": ("pmkt.data.registry", "PAPER_FILL_COLUMNS"),
    "PAPER_FILL_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "PAPER_FILL_SCHEMA_VERSION",
    ),
    "PASSIVE_QUOTE_EVALUATION_COLUMNS": (
        "pmkt.data.registry",
        "PASSIVE_QUOTE_EVALUATION_COLUMNS",
    ),
    "PASSIVE_QUOTE_EVALUATION_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "PASSIVE_QUOTE_EVALUATION_SCHEMA_VERSION",
    ),
    "PASSIVE_FILL_COLUMNS": ("pmkt.data.registry", "PASSIVE_FILL_COLUMNS"),
    "PASSIVE_FILL_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "PASSIVE_FILL_SCHEMA_VERSION",
    ),
    "PASSIVE_MARKOUT_COLUMNS": ("pmkt.data.registry", "PASSIVE_MARKOUT_COLUMNS"),
    "PASSIVE_MARKOUT_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "PASSIVE_MARKOUT_SCHEMA_VERSION",
    ),
    "PAPER_POSITION_COLUMNS": ("pmkt.data.registry", "PAPER_POSITION_COLUMNS"),
    "PAPER_POSITION_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "PAPER_POSITION_SCHEMA_VERSION",
    ),
    "ParquetSink": ("pmkt.data.io", "ParquetSink"),
    "POLYMARKET_MARKET_SNAPSHOT_COLUMNS": (
        "pmkt.data.registry",
        "POLYMARKET_MARKET_SNAPSHOT_COLUMNS",
    ),
    "POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION",
    ),
    "POLYMARKET_MARKET_SNAPSHOT_COLUMNS_V2": (
        "pmkt.data.registry",
        "POLYMARKET_MARKET_SNAPSHOT_COLUMNS_V2",
    ),
    "POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION_V2": (
        "pmkt.data.registry",
        "POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION_V2",
    ),
    "RUN_MANIFEST_COLUMNS": ("pmkt.data.registry", "RUN_MANIFEST_COLUMNS"),
    "RUN_MANIFEST_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "RUN_MANIFEST_SCHEMA_VERSION",
    ),
    "SCHEMA_SPECS": ("pmkt.data.canonical", "SCHEMA_SPECS"),
    "SCAN_CYCLE_COLUMNS": ("pmkt.data.registry", "SCAN_CYCLE_COLUMNS"),
    "SCAN_CYCLE_SCHEMA_VERSION": ("pmkt.data.registry", "SCAN_CYCLE_SCHEMA_VERSION"),
    "SIGNAL_COLUMNS": ("pmkt.data.registry", "SIGNAL_COLUMNS"),
    "SIGNAL_SCHEMA_VERSION": ("pmkt.data.registry", "SIGNAL_SCHEMA_VERSION"),
    "Sink": ("pmkt.data.io", "Sink"),
    "SchemaValidationReport": ("pmkt.data.validation", "SchemaValidationReport"),
    "TOPBOOK_COLUMNS": ("pmkt.data.registry", "TOPBOOK_COLUMNS"),
    "TOPBOOK_SCHEMA_VERSION": ("pmkt.data.registry", "TOPBOOK_SCHEMA_VERSION"),
    "TableSpec": ("pmkt.data.registry", "TableSpec"),
    "TRACKING_HEALTH_COLUMNS": ("pmkt.data.registry", "TRACKING_HEALTH_COLUMNS"),
    "TRACKING_HEALTH_SCHEMA_VERSION": (
        "pmkt.data.registry",
        "TRACKING_HEALTH_SCHEMA_VERSION",
    ),
    "TRADE_COLUMNS": ("pmkt.data.registry", "TRADE_COLUMNS"),
    "TRADE_SCHEMA_VERSION": ("pmkt.data.registry", "TRADE_SCHEMA_VERSION"),
    "arbitrage_candidate_row": ("pmkt.data.canonical", "arbitrage_candidate_row"),
    "arrow_schema": ("pmkt.data.registry", "arrow_schema"),
    "basket_order_intent_row": ("pmkt.data.canonical", "basket_order_intent_row"),
    "basket_paper_fill_row": ("pmkt.data.canonical", "basket_paper_fill_row"),
    "basket_paper_position_row": ("pmkt.data.canonical", "basket_paper_position_row"),
    "batched": ("pmkt.data.market_data", "batched"),
    "canary_candidate_row": ("pmkt.data.canonical", "canary_candidate_row"),
    "canary_rejection_row": ("pmkt.data.canonical", "canary_rejection_row"),
    "collect_order_book_summaries_parquet": (
        "pmkt.data.market_data",
        "collect_order_book_summaries_parquet",
    ),
    "compute_features": ("pmkt.data.features", "compute_features"),
    "contract_evidence_dataframe": (
        "pmkt.data.contract_evidence",
        "contract_evidence_dataframe",
    ),
    "contract_evidence_row": ("pmkt.data.canonical", "contract_evidence_row"),
    "coerce_frame": ("pmkt.data.validation", "coerce_frame"),
    "coerce_snapshot_frame": ("pmkt.data.validation", "coerce_snapshot_frame"),
    "convert_frame_strict": ("pmkt.data.validation", "convert_frame_strict"),
    "depth_row": ("pmkt.data.schemas", "depth_row"),
    "event_row": ("pmkt.data.canonical", "event_row"),
    "feed_health_row": ("pmkt.data.canonical", "feed_health_row"),
    "fetch_trade_history": ("pmkt.data.market_data", "fetch_trade_history"),
    "find_event_by_slug": ("pmkt.data.market_data", "find_event_by_slug"),
    "get_table_spec": ("pmkt.data.registry", "get_table_spec"),
    "infer_and_validate_frame": ("pmkt.data.validation", "infer_and_validate_frame"),
    "instrument_row": ("pmkt.data.canonical", "instrument_row"),
    "iter_order_book_metrics": ("pmkt.data.market_data", "iter_order_book_metrics"),
    "join_market_metadata": ("pmkt.data.features", "join_market_metadata"),
    "kalshi_market_snapshot_row": (
        "pmkt.data.canonical",
        "kalshi_market_snapshot_row",
    ),
    "kalshi_market_snapshot_v2_row": (
        "pmkt.data.canonical",
        "kalshi_market_snapshot_v2_row",
    ),
    "kalshi_orderbook_to_topbook": (
        "pmkt.data.normalize_books",
        "kalshi_orderbook_to_topbook",
    ),
    "kalshi_ws_snapshot_to_topbook": (
        "pmkt.data.normalize_books",
        "kalshi_ws_snapshot_to_topbook",
    ),
    "list_table_specs": ("pmkt.data.registry", "list_table_specs"),
    "logit": ("pmkt.data.features", "logit"),
    "market_match_row": ("pmkt.data.canonical", "market_match_row"),
    "market_resolution_row": ("pmkt.data.canonical", "market_resolution_row"),
    "match_relation_row": ("pmkt.data.canonical", "match_relation_row"),
    "market_row": ("pmkt.data.canonical", "market_row"),
    "maker_quote_plan_row": ("pmkt.data.canonical", "maker_quote_plan_row"),
    "order_intent_row": ("pmkt.data.canonical", "order_intent_row"),
    "order_state_row": ("pmkt.data.canonical", "order_state_row"),
    "paper_fill_row": ("pmkt.data.canonical", "paper_fill_row"),
    "passive_fill_row": ("pmkt.data.canonical", "passive_fill_row"),
    "passive_markout_row": ("pmkt.data.canonical", "passive_markout_row"),
    "passive_quote_evaluation_row": (
        "pmkt.data.canonical",
        "passive_quote_evaluation_row",
    ),
    "order_book_summary_dataframe": (
        "pmkt.data.market_data",
        "order_book_summary_dataframe",
    ),
    "polymarket_book_to_topbook": (
        "pmkt.data.normalize_books",
        "polymarket_book_to_topbook",
    ),
    "polymarket_market_snapshot_row": (
        "pmkt.data.canonical",
        "polymarket_market_snapshot_row",
    ),
    "polymarket_market_snapshot_v2_row": (
        "pmkt.data.canonical",
        "polymarket_market_snapshot_v2_row",
    ),
    "polymarket_ws_snapshot_to_topbook": (
        "pmkt.data.normalize_books",
        "polymarket_ws_snapshot_to_topbook",
    ),
    "quality_flag_counts": ("pmkt.data.validation", "quality_flag_counts"),
    "paper_position_row": ("pmkt.data.canonical", "paper_position_row"),
    "run_manifest_row": ("pmkt.data.canonical", "run_manifest_row"),
    "scan_cycle_row": ("pmkt.data.canonical", "scan_cycle_row"),
    "signal_row": ("pmkt.data.canonical", "signal_row"),
    "stable_evidence_projection_hash": (
        "pmkt.data.contract_evidence",
        "stable_evidence_projection_hash",
    ),
    "stable_source_row_hash": (
        "pmkt.data.contract_evidence",
        "stable_source_row_hash",
    ),
    "topbook_row": ("pmkt.data.schemas", "topbook_row"),
    "tracking_match_row": ("pmkt.data.canonical", "tracking_match_row"),
    "tracking_health_row": ("pmkt.data.canonical", "tracking_health_row"),
    "trade_history_dataframe": ("pmkt.data.market_data", "trade_history_dataframe"),
    "trade_row": ("pmkt.data.canonical", "trade_row"),
    "validate_frame": ("pmkt.data.validation", "validate_frame"),
    "validate_run_manifest": ("pmkt.data.manifests", "validate_run_manifest"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY_MODULES:
        module = import_module(_LAZY_MODULES[name])
        globals()[name] = module
        return module
    if name in _LAZY_ATTRS:
        module_name, attr_name = _LAZY_ATTRS[name]
        value = getattr(import_module(module_name), attr_name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_LAZY_MODULES) | set(_LAZY_ATTRS))


__all__ = list(_LAZY_ATTRS)
