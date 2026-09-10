from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from pmkt.data.canonical import canonical_fixed_decimal
from pmkt.data.registry import (
    TOPBOOK_COLUMNS as TOPBOOK_COLUMNS,
    DEPTH_COLUMNS as DEPTH_COLUMNS,
    DEPTH_SCHEMA_VERSION,
    TOPBOOK_SCHEMA_VERSION,
    get_table_spec,
)


_TOPBOOK_FIELD_DTYPES = {
    field.name: field.dtype for field in get_table_spec(TOPBOOK_SCHEMA_VERSION).fields
}


def topbook_row(**values: Any) -> dict[str, Any]:
    version = values.pop("schema_version", TOPBOOK_SCHEMA_VERSION)
    if version not in {TOPBOOK_SCHEMA_VERSION, "topbook.v2"}:
        raise ValueError(f"unsupported topbook schema {version!r}")
    row: dict[str, Any] = {column: None for column in get_table_spec(version).columns}
    row["schema_version"] = version
    row["collector_run_id"] = ""
    row["valid_state"] = False
    row["quality_flags"] = []
    row.update({key: value for key, value in values.items() if key in row})
    if row.get("best_bid_source") is None:
        row["best_bid_source"] = "direct" if row.get("best_bid_dollars") is not None else "missing"
    if row.get("best_ask_source") is None:
        row["best_ask_source"] = "direct" if row.get("best_ask_dollars") is not None else "missing"
    return row


def topbook_evidence_id(row: Mapping[str, Any]) -> str:
    """Return the physical-schema-stable identity of a topbook.v1 row.

    Evidence controls are persisted separately from their topbook parents.  Hash
    values therefore have to survive the registered Arrow conversion boundary,
    in particular Decimal-to-float64 conversion, rather than identify an
    arbitrary producer-side Python representation.
    """
    spec = get_table_spec(str(row.get("schema_version") or TOPBOOK_SCHEMA_VERSION))
    projection = {
        field.name: _canonical_topbook_field_value(
            row.get(field.name), dtype=field.dtype
        )
        for field in spec.fields
    }
    payload = json.dumps(
        projection,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _canonical_topbook_field_value(value: Any, *, dtype: str) -> Any:
    if (
        value is None
        or type(value).__name__ == "NAType"
        or isinstance(value, float)
        and math.isnan(value)
    ):
        return None
    if dtype == "float64":
        if isinstance(value, bool):
            raise ValueError("topbook float evidence does not support booleans")
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"invalid topbook float evidence: {value!r}") from None
        if not math.isfinite(parsed):
            raise ValueError("topbook evidence does not support non-finite floats")
        return canonical_fixed_decimal(parsed)
    if dtype in {"int32", "int64"}:
        if isinstance(value, bool):
            raise ValueError("topbook integer evidence does not support booleans")
        try:
            return int(value)
        except (TypeError, ValueError, OverflowError):
            raise ValueError(f"invalid topbook integer evidence: {value!r}") from None
    if dtype == "bool":
        if not isinstance(value, bool):
            raise ValueError(f"invalid topbook boolean evidence: {value!r}")
        return value
    if dtype in {"string", "large_string"}:
        return str(value)
    return _canonical_evidence_value(value)


def _canonical_evidence_value(value: Any) -> Any:
    if value is None or type(value).__name__ == "NAType":
        return None
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_evidence_value(value[key])
            for key in sorted(value)
        }
    if isinstance(value, (list, tuple)):
        return [_canonical_evidence_value(item) for item in value]
    if isinstance(value, Decimal):
        return canonical_fixed_decimal(value)
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if not math.isfinite(value):
            raise ValueError("topbook evidence does not support non-finite floats")
        return canonical_fixed_decimal(value)
    if not isinstance(value, (str, bytes, bool, int)) and hasattr(value, "tolist"):
        return _canonical_evidence_value(value.tolist())
    if not isinstance(value, (str, bytes, bool, int)) and hasattr(value, "item"):
        return _canonical_evidence_value(value.item())
    return value


def depth_row(**values: Any) -> dict[str, Any]:
    version = values.pop("schema_version", DEPTH_SCHEMA_VERSION)
    if version not in {DEPTH_SCHEMA_VERSION, "depth.v2"}:
        raise ValueError(f"unsupported depth schema {version!r}")
    row: dict[str, Any] = {column: None for column in get_table_spec(version).columns}
    row["schema_version"] = version
    row["collector_run_id"] = ""
    row["valid_state"] = False
    row["quality_flags"] = []
    row.update({key: value for key, value in values.items() if key in row})
    return row
