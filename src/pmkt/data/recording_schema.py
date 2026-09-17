"""Physical contracts for recording.v1 (independent of historical tape schemas)."""

from __future__ import annotations

from typing import Any


# name, physical type, nullable. Times are UTC ISO-8601 strings; prices are
# dollars per contract and quantities are contracts. JSON fields are strings.
COMMON = (
    ("schema_version", "string", False),
    ("run_id", "string", False),
    ("record_sequence", "int64", False),
    ("venue", "string", False),
    ("instrument_id", "string", True),
    ("venue_market_id", "string", True),
    ("connection_generation", "int64", False),
    ("source_sequence", "int64", True),
    ("received_at_utc", "string", True),
    ("received_monotonic_ns", "int64", True),
    ("venue_time_utc", "string", True),
    ("observed_at_utc", "string", False),
)
BOOK = (
    ("initialized", "bool", False),
    ("integrity_valid", "bool", False),
    ("quote_valid", "bool", False),
    ("tick_size", "float64", True),
    ("minimum_order_size", "float64", True),
    ("quality_flags_json", "string", False),
)
RECORDING_FIELDS = {
    "topbook": COMMON
    + BOOK
    + (
        ("bid_price", "float64", True),
        ("bid_quantity", "float64", True),
        ("ask_price", "float64", True),
        ("ask_quantity", "float64", True),
    ),
    "book_snapshots": COMMON
    + BOOK
    + (
        ("snapshot_id", "int64", False),
        ("causes_json", "string", False),
        ("bid_level_count", "int64", False),
        ("ask_level_count", "int64", False),
    ),
    "book_levels": (
        ("schema_version", "string", False),
        ("run_id", "string", False),
        ("snapshot_id", "int64", False),
        ("side", "string", False),
        ("price", "float64", False),
        ("quantity", "float64", False),
    ),
    "trades": COMMON
    + (
        ("venue_trade_id", "string", True),
        ("price", "float64", False),
        ("quantity", "float64", True),
        ("reported_side", "string", True),
    ),
    "events": COMMON
    + (
        ("kind", "string", False),
        ("details_json", "string", False),
    ),
}
RECORDING_KEYS: dict[str, tuple[str, ...]] = {
    name: ("run_id", "record_sequence") for name in RECORDING_FIELDS
}
RECORDING_KEYS["book_snapshots"] = ("run_id", "snapshot_id")
RECORDING_KEYS["book_levels"] = ("run_id", "snapshot_id", "side", "price")


def recording_specs(field_type: Any, table_type: Any) -> dict[str, Any]:
    """Called by the registry without a circular import or optional dependencies."""
    return {
        f"recording_{name}.v1": table_type(
            name=f"recording_{name}",
            version=f"recording_{name}.v1",
            fields=tuple(field_type(*field) for field in fields),
            primary_key=RECORDING_KEYS[name],
            description=f"recording.v1 {name}; see docs/stream_recording_contract.md",
        )
        for name, fields in RECORDING_FIELDS.items()
    }
