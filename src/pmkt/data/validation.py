from __future__ import annotations

import json
import math
import re
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from collections.abc import Callable
from typing import Any, Iterable

import pandas as pd

from pmkt.data.canonical import canonical_fixed_decimal
from pmkt.data.schemas import topbook_evidence_id
from pmkt.data.types import parse_int
from pmkt.data.registry import TableSpec, get_table_spec, infer_table_spec
from pmkt.data.registry import (
    CONTRACT_EVIDENCE_SCHEMA_VERSION,
    CO_RESOLUTION_OBSERVATION_SCHEMA_VERSION,
    CO_RESOLUTION_SCORE_SCHEMA_VERSION,
    BOOK_TAPE_CONTROL_SCHEMA_VERSION,
    BOOK_TAPE_EVENT_SCHEMA_VERSION,
    BOOK_TAPE_LEVEL_SCHEMA_VERSION,
    KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
    KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION_V2,
    MARKET_TAXONOMY_EVIDENCE_SCHEMA_VERSION,
    MARKET_RESOLUTION_SCHEMA_VERSION,
    POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
    POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION_V2,
    STREAM_LIFECYCLE_SCHEMA_VERSION,
    TOPBOOK_SCHEMA_VERSION,
    TRADE_SCHEMA_VERSION,
)

_PRICE_TOLERANCE = 1e-6
_UTC_TIMESTAMP_RE = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"
    r"T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?"
    r"(?P<offset>Z|\+00(?::?00)?)$"
)
_UNIX_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)
_BOOK_RECOVERY_EVIDENCE_ROLES = frozenset(
    {"tape_event", "topbook_main", "topbook_checkpoint"}
)
_INTEGER_BOUNDS = {
    "int32": (-(1 << 31), (1 << 31) - 1),
    "int64": (-(1 << 63), (1 << 63) - 1),
}

_SNAPSHOT_SCHEMA_COMPATIBILITY = {
    POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION: frozenset(
        {POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION}
    ),
    KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION: frozenset(
        {KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION}
    ),
    POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION_V2: frozenset(
        {
            POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION,
            POLYMARKET_MARKET_SNAPSHOT_SCHEMA_VERSION_V2,
        }
    ),
    KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION_V2: frozenset(
        {
            KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION,
            KALSHI_MARKET_SNAPSHOT_SCHEMA_VERSION_V2,
        }
    ),
}


@dataclass(frozen=True)
class SchemaValidationReport:
    schema: str
    row_count: int
    strict: bool = False
    missing_columns: tuple[str, ...] = ()
    extra_columns: tuple[str, ...] = ()
    nullability_errors: tuple[str, ...] = ()
    dtype_errors: tuple[str, ...] = ()
    invariant_errors: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not (
            self.missing_columns
            or (self.strict and self.extra_columns)
            or self.nullability_errors
            or self.dtype_errors
            or self.invariant_errors
        )

    @property
    def errors(self) -> tuple[str, ...]:
        messages: list[str] = []
        if self.missing_columns:
            messages.append(f"missing columns: {', '.join(self.missing_columns)}")
        if self.strict and self.extra_columns:
            messages.append(f"extra columns: {', '.join(self.extra_columns)}")
        messages.extend(self.nullability_errors)
        messages.extend(self.dtype_errors)
        messages.extend(self.invariant_errors)
        return tuple(messages)


@dataclass(frozen=True)
class BookTapeBundleValidationReport:
    errors: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.errors


def validate_frame(
    df: pd.DataFrame,
    spec: TableSpec | str,
    *,
    strict: bool = False,
) -> SchemaValidationReport:
    table = get_table_spec(spec) if isinstance(spec, str) else spec
    expected = set(table.columns)
    actual = set(df.columns)
    missing = tuple(column for column in table.columns if column not in actual)
    extra = tuple(sorted(actual - expected))
    nullability_errors = _nullability_errors(df, table)
    dtype_errors = _dtype_errors(df, table)
    invariant_errors = [
        *_schema_version_errors(df, table),
        *_primary_key_errors(df, table, strict=strict),
        *_invariant_errors(df, table),
    ]
    return SchemaValidationReport(
        schema=table.version,
        row_count=len(df),
        strict=strict,
        missing_columns=missing,
        extra_columns=extra,
        nullability_errors=tuple(nullability_errors),
        dtype_errors=tuple(dtype_errors),
        invariant_errors=tuple(invariant_errors),
    )


def _primary_key_errors(
    df: pd.DataFrame,
    table: TableSpec,
    *,
    strict: bool,
) -> list[str]:
    if not strict or not table.primary_key:
        return []
    key_columns = list(table.primary_key)
    if any(column not in df.columns for column in key_columns):
        return []
    duplicates = df.duplicated(subset=key_columns, keep=False)
    if not bool(duplicates.any()):
        return []
    duplicate_count = int(df.loc[duplicates, key_columns].drop_duplicates().shape[0])
    joined = ", ".join(key_columns)
    return [f"primary key ({joined}): {duplicate_count} duplicate values"]


def coerce_frame(df: pd.DataFrame, spec: TableSpec | str) -> pd.DataFrame:
    table = get_table_spec(spec) if isinstance(spec, str) else spec
    missing_required = [
        field.name
        for field in table.fields
        if not field.nullable
        and field.name not in df.columns
        and field.name != "schema_version"
    ]
    if missing_required:
        raise ValueError(f"missing required columns: {', '.join(missing_required)}")
    coerced = df.copy()
    for field in table.fields:
        if field.name not in coerced.columns:
            coerced[field.name] = pd.NA
        if field.name == "schema_version":
            coerced[field.name] = table.version
        coerced[field.name] = _coerce_series(coerced[field.name], field.dtype)
    return coerced.loc[:, list(table.columns)]


def convert_frame_strict(df: pd.DataFrame, spec: TableSpec | str) -> pd.DataFrame:
    """Convert a schema-compatible frame without cleaning away evidence.

    Unlike :func:`coerce_frame`, this boundary rejects extra columns, wrong
    versions, duplicate keys, malformed values, and invariant violations before
    conversion. It may add the selected schema version and absent nullable
    columns, but it never projects away a source field or turns an invalid value
    into nullable missing data.
    """

    table = get_table_spec(spec) if isinstance(spec, str) else spec
    prepared = df.copy()
    if "schema_version" in table.columns and "schema_version" not in prepared.columns:
        prepared["schema_version"] = table.version
    for field in table.fields:
        if field.nullable and field.name not in prepared.columns:
            prepared[field.name] = pd.NA

    strict_json_errors = _strict_json_dtype_errors(prepared, table)
    if strict_json_errors:
        raise ValueError(
            f"cannot strictly convert {table.version}: "
            + "; ".join(strict_json_errors)
        )

    source_report = validate_frame(prepared, table, strict=True)
    if not source_report.ok:
        raise ValueError(
            f"cannot strictly convert {table.version}: "
            + "; ".join(source_report.errors)
        )

    converted = prepared.copy()
    for field in table.fields:
        converted[field.name] = _coerce_series(converted[field.name], field.dtype)
    converted = converted.loc[:, list(table.columns)]

    output_report = validate_frame(converted, table, strict=True)
    if not output_report.ok:
        raise ValueError(
            f"strict conversion produced invalid {table.version}: "
            + "; ".join(output_report.errors)
        )
    return converted


def coerce_snapshot_frame(df: pd.DataFrame, schema_version: str) -> pd.DataFrame:
    """Coerce a source snapshot frame to a selected compatible snapshot schema."""
    if schema_version not in _SNAPSHOT_SCHEMA_COMPATIBILITY:
        known = ", ".join(sorted(_SNAPSHOT_SCHEMA_COMPATIBILITY))
        raise ValueError(
            f"unsupported snapshot schema {schema_version!r}; known schemas: {known}"
        )
    if "schema_version" in df.columns and not df.empty:
        versions = set(df["schema_version"].dropna().astype(str))
        unsupported = versions - _SNAPSHOT_SCHEMA_COMPATIBILITY[schema_version]
        if unsupported:
            bad = ", ".join(sorted(unsupported))
            raise ValueError(
                f"incompatible source snapshot schema_version values: {bad}"
            )
    return coerce_frame(df, schema_version)


def infer_and_validate_frame(
    df: pd.DataFrame,
    schema_version: str | None = None,
    *,
    strict: bool = False,
) -> SchemaValidationReport:
    if schema_version is None and "schema_version" in df.columns and not df.empty:
        versions = sorted(set(df["schema_version"].dropna().astype(str)))
        if len(versions) == 1:
            schema_version = versions[0]
    return validate_frame(
        df,
        infer_table_spec(df.columns, schema_version=schema_version),
        strict=strict,
    )


def quality_flag_counts(df: pd.DataFrame) -> dict[str, int]:
    if "quality_flags" not in df.columns:
        return {}
    counter: Counter[str] = Counter()
    for value in df["quality_flags"].tolist():
        counter.update(_flags(value))
    return dict(sorted(counter.items()))


def _nullability_errors(df: pd.DataFrame, spec: TableSpec) -> list[str]:
    errors: list[str] = []
    for field in spec.fields:
        if field.name not in df.columns or field.nullable:
            continue
        null_count = int(df[field.name].isna().sum())
        if null_count:
            errors.append(
                f"{field.name}: {null_count} null values in non-nullable field"
            )
    return errors


def _dtype_errors(df: pd.DataFrame, spec: TableSpec) -> list[str]:
    errors: list[str] = []
    for field in spec.fields:
        if field.name not in df.columns:
            continue
        series = df[field.name]
        bad_count = _bad_dtype_count(series, field.dtype)
        if bad_count:
            errors.append(
                f"{field.name}: {bad_count} values incompatible with {field.dtype}"
            )
        if field.allowed_values:
            values = set(series.dropna().astype(str))
            bad_values = sorted(values - set(field.allowed_values))
            if bad_values:
                errors.append(f"{field.name}: unsupported values {bad_values}")
    return errors


def _bad_dtype_count(series: pd.Series, dtype: str) -> int:
    non_null = series.dropna()
    if non_null.empty:
        return 0
    if dtype == "float64":
        return sum(
            1 for value in non_null.tolist() if _parse_strict_float(value) is None
        )
    if dtype in {"int32", "int64"}:
        return sum(
            1
            for value in non_null.tolist()
            if not _strict_int_compatible(value, dtype)
        )
    if dtype == "bool":
        return sum(1 for value in non_null.tolist() if _parse_bool(value) is None)
    if dtype == "json":
        return sum(1 for value in non_null.tolist() if not _json_compatible(value))
    if dtype == "list[string]":
        return sum(
            1 for value in non_null.tolist() if _parse_string_list(value) is None
        )
    if dtype in {"large_string", "string"}:
        return sum(1 for value in non_null.tolist() if not _string_compatible(value))
    return 0


def _parse_strict_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _bounded_int(value: Any, dtype: str) -> int | None:
    parsed = parse_int(value)
    lower, upper = _INTEGER_BOUNDS[dtype]
    if parsed is None or parsed < lower or parsed > upper:
        return None
    return parsed


def _strict_int_compatible(value: Any, dtype: str) -> bool:
    return _bounded_int(value, dtype) is not None


def _coerce_series(series: pd.Series, dtype: str) -> pd.Series:
    if dtype == "float64":
        return pd.to_numeric(series, errors="coerce")
    if dtype in {"int32", "int64"}:
        pandas_dtype = "Int32" if dtype == "int32" else "Int64"
        values = [_bounded_int(value, dtype) for value in series.tolist()]
        return pd.Series(
            pd.array(values, dtype=pandas_dtype),
            index=series.index,
            name=series.name,
        )
    if dtype == "bool":
        return series.map(_parse_bool).astype("boolean")
    if dtype == "json":
        return series.map(_json_string)
    if dtype == "list[string]":
        return series.map(lambda value: _parse_string_list(value) or [])
    return series.astype("string")


def _parsed_bool_mask(series: pd.Series) -> pd.Series:
    return series.map(_parse_bool).astype("boolean").fillna(False).astype(bool)


def _bool_mask(series: pd.Series, predicate: Callable[[Any], bool]) -> pd.Series:
    """Map a predicate to a mask that stays boolean on an empty frame.

    ``Series.map`` preserves the source dtype when there are no rows, so a
    string-dtype column yields a string mask and the boolean reductions below
    raise ``TypeError`` instead of reporting invariant errors.
    """
    return series.map(predicate).astype(bool)


FLAG_TOKEN_COLUMNS = frozenset({"quality_flags", "data_quality_flags"})
_FLAG_TOKEN_MAX_LEN = 64


def _is_missing_scalar(value: Any) -> bool:
    if value is None or not pd.api.types.is_scalar(value):
        return value is None
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _flag_token_errors(df: pd.DataFrame, spec: TableSpec) -> list[str]:
    """Validate flag columns contain whole, canonical tokens.

    Container-shape validation alone accepted a list of single characters, which
    is exactly what a ";"-joined string became when handed to a
    ``list<string>`` Arrow field.  Scoped to flag columns only: prose list
    columns such as ``data_quality_caveats`` and ``assumption_caveats``
    legitimately contain spaces and punctuation and must not be checked here.
    """
    errors: list[str] = []
    for column in FLAG_TOKEN_COLUMNS & set(spec.columns) & set(df.columns):
        bad_shape = 0
        bad_token = 0
        for value in df[column].tolist():
            if _is_missing_scalar(value):
                continue
            if isinstance(value, (str, bytes)):
                bad_shape += 1
                continue
            try:
                tokens = list(value)
            except TypeError:
                bad_shape += 1
                continue
            for token in tokens:
                text = str(token)
                if (
                    not text
                    or text.strip() != text
                    or " " in text
                    or ";" in text
                    or "," in text
                    or len(text) < 2
                    or len(text) > _FLAG_TOKEN_MAX_LEN
                ):
                    bad_token += 1
                    break
        if bad_shape:
            errors.append(
                f"{column}: {bad_shape} values are not a list of tokens "
                "(a delimited string is not a flag list)"
            )
        if bad_token:
            errors.append(
                f"{column}: {bad_token} values contain malformed flag tokens "
                "(empty, single-character, whitespace, or embedded separators)"
            )
    return errors


def _invariant_errors(df: pd.DataFrame, spec: TableSpec) -> list[str]:
    errors: list[str] = []
    errors.extend(_price_invariant_errors(df, spec))
    errors.extend(_flag_token_errors(df, spec))
    if spec.version == MARKET_RESOLUTION_SCHEMA_VERSION:
        errors.extend(_market_resolution_invariant_errors(df))
    if spec.version == CONTRACT_EVIDENCE_SCHEMA_VERSION:
        errors.extend(_contract_evidence_invariant_errors(df))
    if spec.version == MARKET_TAXONOMY_EVIDENCE_SCHEMA_VERSION:
        errors.extend(_market_taxonomy_evidence_invariant_errors(df))
    if spec.version == "topbook.v1":
        errors.extend(_topbook_invariant_errors(df))
        errors.extend(_canonical_kalshi_topbook_errors(df))
    if spec.version == "depth.v1":
        errors.extend(_depth_invariant_errors(df))
    if spec.version == "match_relation.v1":
        errors.extend(_match_relation_invariant_errors(df))
    if spec.version == "tracking_match.v1":
        errors.extend(_tracking_match_invariant_errors(df))
    if spec.version == "tracking_health.v1":
        errors.extend(_tracking_health_invariant_errors(df))
    if spec.version == "signal.v1":
        errors.extend(_signal_invariant_errors(df))
    if spec.version == "maker_quote_plan.v1":
        errors.extend(_maker_quote_plan_invariant_errors(df))
    if spec.version == "order_intent.v1":
        errors.extend(_order_intent_invariant_errors(df))
    if spec.version == "order_state.v1":
        errors.extend(_order_state_invariant_errors(df))
    if spec.version == "paper_fill.v1":
        errors.extend(_paper_fill_invariant_errors(df))
    if spec.version == "paper_position.v1":
        errors.extend(_paper_position_invariant_errors(df))
    if spec.version == "historical_price.v1":
        errors.extend(_historical_price_invariant_errors(df))
    if spec.version == "convergence_observation.v1":
        errors.extend(_convergence_observation_invariant_errors(df))
    if spec.version == "convergence_summary.v1":
        errors.extend(_convergence_summary_invariant_errors(df))
    if spec.version == "backtest_report.v1":
        errors.extend(_backtest_report_invariant_errors(df))
    if spec.version == CO_RESOLUTION_OBSERVATION_SCHEMA_VERSION:
        errors.extend(_co_resolution_observation_invariant_errors(df))
    if spec.version == CO_RESOLUTION_SCORE_SCHEMA_VERSION:
        errors.extend(_co_resolution_score_invariant_errors(df))
    if spec.version == BOOK_TAPE_EVENT_SCHEMA_VERSION and _has_columns(
        df,
        "event_kind",
        "epoch_id",
        "checkpoint_reason",
        "side_counts_json",
        "reconstructible",
    ):
        errors.extend(_book_tape_event_invariant_errors(df))
    if spec.version == BOOK_TAPE_LEVEL_SCHEMA_VERSION and _has_columns(
        df,
        "venue",
        "source_side",
        "price_key",
        "price_dollars",
    ):
        errors.extend(_book_tape_level_invariant_errors(df))
    if spec.version == BOOK_TAPE_CONTROL_SCHEMA_VERSION and _has_columns(
        df,
        "control_type",
        "valid_after",
        "evidence_role",
        "evidence_id",
    ):
        errors.extend(_book_tape_control_invariant_errors(df))
    if spec.version == STREAM_LIFECYCLE_SCHEMA_VERSION and _has_columns(
        df,
        "venue",
        "event_type",
    ):
        errors.extend(_stream_lifecycle_invariant_errors(df))
    if spec.version == TRADE_SCHEMA_VERSION and _has_columns(
        df,
        "collector_run_id",
        "received_at_monotonic_ns",
        "local_sequence",
        "subsequence",
    ):
        errors.extend(_trade_invariant_errors(df))
    if {"yes_bid", "yes_ask", "no_bid", "no_ask"}.intersection(df.columns):
        errors.extend(_kalshi_complement_errors(df))
    return errors


def _has_columns(df: pd.DataFrame, *columns: str) -> bool:
    """Return whether a schema-specific vectorized hook can run safely."""
    return all(column in df.columns for column in columns)


_LOWER_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FIXED_DECIMAL_RE = re.compile(r"^(?:0|1)(?:\.\d+)?$")
_POLYMARKET_TAPE_SIDES = {"bid", "ask"}
_KALSHI_TAPE_SIDES = {"yes", "no"}
_POLYMARKET_LIFECYCLE_EVENTS = {
    "new_market",
    "market_resolved",
    "tick_size_change",
}
_KALSHI_LIFECYCLE_EVENTS = {
    "created",
    "activated",
    "deactivated",
    "close_date_updated",
    "determined",
    "settled",
    "metadata_updated",
}


def _trade_invariant_errors(df: pd.DataFrame) -> list[str]:
    """Require capture coordinates to be absent together or complete together."""
    errors = _nonnegative_errors(
        df, ("received_at_monotonic_ns", "local_sequence", "subsequence")
    )
    errors.extend(_explicit_utc_errors(df, ("trade_ts_utc", "received_at_utc")))
    coordinate_columns = (
        "collector_run_id",
        "received_at_monotonic_ns",
        "local_sequence",
        "subsequence",
    )
    present = pd.DataFrame(
        {
            column: (
                _bool_mask(df[column], _present_text)
                if column == "collector_run_id"
                else df[column].notna()
            )
            for column in coordinate_columns
        },
        index=df.index,
    )
    incomplete = (present.any(axis=1) & ~present.all(axis=1)).to_numpy()
    values = present.to_numpy(dtype=bool)
    for position in incomplete.nonzero()[0]:
        missing = ", ".join(
            column
            for column, is_present in zip(
                coordinate_columns,
                values[position],
                strict=True,
            )
            if not is_present
        )
        errors.append(
            "trade capture coordinate: "
            f"row {df.index[position]} is incomplete; missing {missing}"
        )
    return errors

def _book_tape_event_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors = _nonnegative_errors(
        df,
        (
            "received_at_monotonic_ns",
            "local_sequence",
            "subsequence",
            "expected_level_row_count",
        ),
    )
    errors.extend(_explicit_utc_errors(df, ("received_at_utc", "exchange_at_utc")))
    errors.extend(
        _sha256_errors(
            df,
            (
                "event_id",
                "epoch_id",
                "post_book_hash",
                "raw_event_hash",
                "event_payload_hash",
            ),
        )
    )
    errors.extend(_canonical_json_errors(df, "quality_flags_json", list, required=True))
    kind = df["event_kind"].fillna("").astype(str)
    checkpoint = kind.eq("checkpoint")
    delta = kind.eq("delta")
    epoch_present = _bool_mask(df["epoch_id"], _present_text)
    reason_present = _bool_mask(df["checkpoint_reason"], _present_text)
    side_counts_present = _bool_mask(df["side_counts_json"], _present_text)
    reconstructible = _bool_mask(
        df["reconstructible"], lambda value: _parse_bool(value) is True
    )
    checkpoint_missing_epoch = checkpoint & ~epoch_present
    checkpoint_missing_reason = checkpoint & ~reason_present
    checkpoint_missing_sides = checkpoint & ~side_counts_present
    delta_with_reason = delta & reason_present
    delta_with_sides = delta & side_counts_present
    reconstructible_missing_epoch = reconstructible & ~epoch_present
    side_errors_by_position: dict[int, list[str]] = {}
    for position in (checkpoint & side_counts_present).to_numpy().nonzero()[0]:
        row_errors = _checkpoint_side_count_errors(df.iloc[position].to_dict())
        if row_errors:
            side_errors_by_position[int(position)] = row_errors
    failing = (
        checkpoint_missing_epoch
        | checkpoint_missing_reason
        | checkpoint_missing_sides
        | delta_with_reason
        | delta_with_sides
        | reconstructible_missing_epoch
    ).to_numpy()
    failing_positions = set(int(position) for position in failing.nonzero()[0])
    failing_positions.update(side_errors_by_position)
    for position in sorted(failing_positions):
        if bool(checkpoint.iloc[position]):
            if bool(checkpoint_missing_epoch.iloc[position]):
                errors.append("epoch_id: checkpoint requires an epoch id")
            if bool(checkpoint_missing_reason.iloc[position]):
                errors.append("checkpoint_reason: checkpoint requires a reason")
            if bool(checkpoint_missing_sides.iloc[position]):
                errors.append(
                    "side_counts_json: checkpoint requires explicit side counts"
                )
            else:
                errors.extend(side_errors_by_position.get(position, ()))
        elif bool(delta.iloc[position]):
            if bool(delta_with_reason.iloc[position]):
                errors.append(
                    "checkpoint_reason: delta must not declare a checkpoint reason"
                )
            if bool(delta_with_sides.iloc[position]):
                errors.append(
                    "side_counts_json: delta must not declare checkpoint side counts"
                )
        if bool(reconstructible_missing_epoch.iloc[position]):
            errors.append("epoch_id: reconstructible event requires an open epoch")
    return errors

def _checkpoint_side_count_errors(row: Mapping[str, Any]) -> list[str]:
    value = _decoded_json(row.get("side_counts_json"))
    if not isinstance(value, Mapping):
        return ["side_counts_json: checkpoint value must be a JSON object"]
    venue = str(row.get("venue") or "")
    expected_sides = (
        _POLYMARKET_TAPE_SIDES if venue == "polymarket" else _KALSHI_TAPE_SIDES
    )
    keys = {str(key) for key in value}
    errors: list[str] = []
    if keys != expected_sides:
        errors.append(
            "side_counts_json: "
            f"{venue} checkpoint sides must equal {sorted(expected_sides)}"
        )
        return errors
    counts: list[int] = []
    for side in sorted(expected_sides):
        raw = value.get(side)
        parsed = _parse_strict_float(raw)
        if (
            isinstance(raw, bool)
            or parsed is None
            or not parsed.is_integer()
            or parsed < 0
        ):
            errors.append(
                f"side_counts_json: side {side} count must be a nonnegative integer"
            )
            continue
        counts.append(int(parsed))
    expected_count = row.get("expected_level_row_count")
    parsed_expected = _parse_strict_float(expected_count)
    if (
        len(counts) == len(expected_sides)
        and parsed_expected is not None
        and parsed_expected.is_integer()
    ):
        if sum(counts) != int(parsed_expected):
            errors.append(
                "side_counts_json: side counts do not equal expected level row count"
            )
    return errors


def _book_tape_level_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors = _nonnegative_errors(
        df,
        ("price_dollars", "size_after_contracts", "level_ordinal"),
    )
    errors.extend(_sha256_errors(df, ("event_id", "epoch_id")))
    venues = df["venue"].fillna("").astype(str)
    sides = df["source_side"].fillna("").astype(str)
    side_valid = (venues.eq("polymarket") & sides.isin(_POLYMARKET_TAPE_SIDES)) | (
        ~venues.eq("polymarket") & sides.isin(_KALSHI_TAPE_SIDES)
    )
    keys = df["price_key"].fillna("").astype(str)
    canonical_key = keys.str.fullmatch(_FIXED_DECIMAL_RE).fillna(False)
    parsed_prices = df["price_dollars"].map(_parse_strict_float)
    expected_keys = parsed_prices.map(
        lambda value: canonical_fixed_decimal(value) if value is not None else None
    )
    disagreement = canonical_key & parsed_prices.notna() & keys.ne(expected_keys)
    failing = (~side_valid | ~canonical_key | disagreement).to_numpy()
    for position in failing.nonzero()[0]:
        if not bool(side_valid.iloc[position]):
            errors.append(
                f"source_side: {venues.iloc[position]} "
                f"does not allow {sides.iloc[position]!r}"
            )
        if not bool(canonical_key.iloc[position]):
            errors.append("price_key: value is not a canonical fixed decimal in [0, 1]")
            continue
        if bool(disagreement.iloc[position]):
            errors.append("price_key: value disagrees with price_dollars")
    return errors

def _book_tape_control_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors = _nonnegative_errors(
        df, ("received_at_monotonic_ns", "local_sequence", "subsequence")
    )
    errors.extend(_explicit_utc_errors(df, ("received_at_utc", "exchange_at_utc")))
    errors.extend(_sha256_errors(df, ("control_id", "epoch_id", "evidence_id")))
    errors.extend(_canonical_json_errors(df, "quality_flags_json", list, required=True))
    control_type = df["control_type"].fillna("").astype(str)
    valid_after = _bool_mask(
        df["valid_after"], lambda value: _parse_bool(value) is True
    )
    recovered = control_type.eq("book_recovered")
    role_present = _bool_mask(df["evidence_role"], _present_text)
    id_present = _bool_mask(df["evidence_id"], _present_text)
    role_valid = df["evidence_role"].fillna("").astype(str).isin(
        _BOOK_RECOVERY_EVIDENCE_ROLES
    )
    recovered_invalid = recovered & ~valid_after
    recovered_missing_reference = recovered & ~(role_present & id_present)
    recovered_invalid_role = recovered & role_present & id_present & ~role_valid
    invalidated_valid = control_type.eq("book_invalidated") & valid_after
    ended_valid = control_type.eq("stream_ended") & valid_after
    failing = (
        recovered_invalid
        | recovered_missing_reference
        | recovered_invalid_role
        | invalidated_valid
        | ended_valid
    ).to_numpy()
    for position in failing.nonzero()[0]:
        if bool(recovered.iloc[position]):
            if bool(recovered_invalid.iloc[position]):
                errors.append("valid_after: book_recovered must open valid state")
            if bool(recovered_missing_reference.iloc[position]):
                errors.append("evidence reference: book_recovered requires role and id")
            elif bool(recovered_invalid_role.iloc[position]):
                errors.append(
                    "evidence_role: book_recovered must reference tape_event, "
                    "topbook_main, or topbook_checkpoint"
                )
        elif bool(invalidated_valid.iloc[position]):
            errors.append("valid_after: book_invalidated must close valid state")
        elif bool(ended_valid.iloc[position]):
            errors.append("valid_after: stream_ended must close valid state")
    return errors

def _stream_lifecycle_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors = _nonnegative_errors(
        df,
        (
            "received_at_monotonic_ns",
            "local_sequence",
            "subsequence",
            "previous_tick_size_dollars",
            "new_tick_size_dollars",
        ),
    )
    errors.extend(
        _explicit_utc_errors(
            df, ("received_at_utc", "exchange_at_utc", "market_close_at_utc")
        )
    )
    errors.extend(_sha256_errors(df, ("lifecycle_event_id", "raw_event_hash")))
    errors.extend(_canonical_json_errors(df, "quality_flags_json", list, required=True))
    venues = df["venue"].fillna("").astype(str)
    event_types = df["event_type"].fillna("").astype(str)
    valid = (
        venues.eq("polymarket") & event_types.isin(_POLYMARKET_LIFECYCLE_EVENTS)
    ) | (~venues.eq("polymarket") & event_types.isin(_KALSHI_LIFECYCLE_EVENTS))
    for position in (~valid).to_numpy().nonzero()[0]:
        errors.append(
            f"event_type: {venues.iloc[position]} "
            f"does not allow {event_types.iloc[position]!r}"
        )
    return errors

def _explicit_utc_errors(df: pd.DataFrame, columns: Iterable[str]) -> list[str]:
    errors: list[str] = []
    for column in columns:
        if column not in df.columns:
            continue
        invalid = sum(
            1
            for value in df[column].tolist()
            if _present_text(value) and not _is_explicit_utc_timestamp(value)
        )
        if invalid:
            errors.append(f"{column}: {invalid} values are not explicit UTC timestamps")
    return errors


def _sha256_errors(df: pd.DataFrame, columns: Iterable[str]) -> list[str]:
    errors: list[str] = []
    for column in columns:
        if column not in df.columns:
            continue
        invalid = sum(
            1
            for value in df[column].tolist()
            if _present_text(value)
            and _LOWER_SHA256_RE.fullmatch(str(value).strip()) is None
        )
        if invalid:
            errors.append(f"{column}: {invalid} values are not lowercase sha256")
    return errors


def _canonical_json_errors(
    df: pd.DataFrame,
    column: str,
    expected_type: type,
    *,
    required: bool,
) -> list[str]:
    if column not in df.columns:
        return []
    invalid = 0
    for value in df[column].tolist():
        if not _present_text(value):
            invalid += int(required)
            continue
        decoded = _decoded_json(value)
        if not isinstance(decoded, expected_type):
            invalid += 1
            continue
        if (
            isinstance(value, str)
            and json.dumps(
                decoded, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            )
            != value
        ):
            invalid += 1
    return [f"{column}: {invalid} values are not canonical JSON"] if invalid else []


def _json_shape_errors(
    df: pd.DataFrame,
    column: str,
    expected_type: type,
    *,
    required: bool,
) -> list[str]:
    if column not in df.columns:
        return []
    invalid = 0
    for value in df[column].tolist():
        if not _present_text(value):
            invalid += int(required)
            continue
        if isinstance(value, str):
            try:
                decoded = json.loads(
                    value,
                    parse_constant=_reject_nonstandard_json_constant,
                )
            except (json.JSONDecodeError, ValueError):
                invalid += 1
                continue
        else:
            decoded = value
        if decoded is None and not required:
            # Retained taxonomy artifacts encode an absent optional object as
            # the exact JSON scalar ``null`` rather than a Parquet null.
            continue
        if not isinstance(decoded, expected_type):
            invalid += 1
    expected_name = "array" if expected_type is list else "object"
    return (
        [f"{column}: {invalid} values are not valid JSON {expected_name}s"]
        if invalid
        else []
    )


def _market_taxonomy_evidence_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors = _explicit_utc_errors(df, ("requested_at_utc", "observed_at_utc"))
    errors.extend(
        _sha256_errors(
            df,
            ("source_payload_sha256", "snapshot_raw_json_sha256"),
        )
    )
    errors.extend(_json_shape_errors(df, "native_tags_json", list, required=True))
    errors.extend(_json_shape_errors(df, "native_series_json", list, required=True))
    errors.extend(
        _json_shape_errors(df, "structured_sport_json", dict, required=False)
    )
    errors.extend(_json_shape_errors(df, "issues_json", list, required=True))
    return errors


def _decoded_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return value


def validate_book_tape_bundle(
    events: pd.DataFrame,
    levels: pd.DataFrame,
    controls: pd.DataFrame | None = None,
    *,
    expected_encoding_version: str | None = None,
) -> BookTapeBundleValidationReport:
    """Validate strict tape schemas plus event/level/control relationships."""
    errors: list[str] = []
    for frame, version in (
        (events, BOOK_TAPE_EVENT_SCHEMA_VERSION),
        (levels, BOOK_TAPE_LEVEL_SCHEMA_VERSION),
    ):
        report = validate_frame(frame, version, strict=True)
        errors.extend(f"{version}: {error}" for error in report.errors)
    if controls is not None:
        report = validate_frame(controls, BOOK_TAPE_CONTROL_SCHEMA_VERSION, strict=True)
        errors.extend(
            f"{BOOK_TAPE_CONTROL_SCHEMA_VERSION}: {error}" for error in report.errors
        )
    if errors:
        return BookTapeBundleValidationReport(tuple(errors))

    for frame, version in (
        (events, BOOK_TAPE_EVENT_SCHEMA_VERSION),
        (levels, BOOK_TAPE_LEVEL_SCHEMA_VERSION),
    ):
        for column in ("collector_run_id", "event_id"):
            incompatible = sum(
                not isinstance(value, str) for value in frame[column].tolist()
            )
            if incompatible:
                errors.append(
                    f"{version}: {column}: {incompatible} values must be strings"
                )
    if errors:
        return BookTapeBundleValidationReport(tuple(errors))

    if expected_encoding_version is not None:
        if not _present_text(expected_encoding_version):
            errors.append(
                "book_tape_event.v1: expected encoding_version must be non-empty"
            )
        else:
            actual_versions = sorted(
                {str(value) for value in events["encoding_version"].tolist()}
            )
            if any(value != expected_encoding_version for value in actual_versions):
                errors.append(
                    "book_tape_event.v1: encoding_version must equal "
                    f"{expected_encoding_version!r}; found {actual_versions!r}"
                )

    key_columns = ["collector_run_id", "event_id"]
    parent_columns = ["venue", "venue_book_id", "epoch_id"]
    parent_lookup = events.loc[
        :,
        [
            *key_columns,
            *parent_columns,
            "expected_level_row_count",
            "event_kind",
            "side_counts_json",
        ],
    ].rename(
        columns={
            column: f"_parent_{column}"
            for column in (
                *parent_columns,
                "expected_level_row_count",
                "event_kind",
                "side_counts_json",
            )
        }
    )
    merged = levels.merge(
        parent_lookup,
        on=key_columns,
        how="left",
        sort=False,
        validate="many_to_one",
        indicator=True,
    )
    orphan = merged["_merge"].eq("left_only")
    for key in merged.loc[orphan, key_columns].itertuples(index=False, name=None):
        normalized = tuple(str(value) for value in key)
        errors.append(f"book_tape_level.v1: orphan event foreign key {normalized}")
    matched = ~orphan
    for column in parent_columns:
        child = merged[column].map(_evidence_value)
        parent = merged[f"_parent_{column}"].map(_evidence_value)
        mismatch = matched & ~(child.eq(parent) | (child.isna() & parent.isna()))
        for key in merged.loc[mismatch, key_columns].itertuples(
            index=False,
            name=None,
        ):
            normalized = tuple(str(value) for value in key)
            errors.append(
                f"book_tape_level.v1: {column} disagrees with parent {normalized}"
            )

    actual_counts = (
        levels.groupby(key_columns, sort=False, dropna=False)
        .size()
        .rename("_actual_level_count")
        .reset_index()
    )
    event_counts = events.merge(
        actual_counts,
        on=key_columns,
        how="left",
        sort=False,
        validate="one_to_one",
    )
    event_counts["_actual_level_count"] = (
        event_counts["_actual_level_count"].fillna(0).astype("int64")
    )
    expected_counts = (
        pd.to_numeric(
            event_counts["expected_level_row_count"],
            errors="coerce",
        )
        .fillna(-1)
        .astype("int64")
    )
    count_mismatch = event_counts["_actual_level_count"].ne(expected_counts)
    for row in event_counts.loc[
        count_mismatch,
        [*key_columns, "expected_level_row_count", "_actual_level_count"],
    ].itertuples(index=False, name=None):
        run_id, event_id, expected, actual = row
        key = (str(run_id), str(event_id))
        errors.append(
            f"book_tape_event.v1: {key} expected {int(expected)} levels, "
            f"found {int(actual)}"
        )

    checkpoints = events[events["event_kind"].fillna("").astype(str).eq("checkpoint")]
    checkpoint_records = checkpoints.to_dict("records")
    checkpoint_keys = {
        (event["collector_run_id"], event["event_id"])
        for event in checkpoint_records
    }
    actual_sides: dict[tuple[str, str], dict[str, int]] = {}
    if checkpoint_keys:
        for run_id, event_id, side in levels[
            [*key_columns, "source_side"]
        ].itertuples(index=False, name=None):
            key = (run_id, event_id)
            if key not in checkpoint_keys:
                continue
            side_text = str(side)
            counts = actual_sides.setdefault(key, {})
            counts[side_text] = counts.get(side_text, 0) + 1
    for event in checkpoint_records:
        key = (event["collector_run_id"], event["event_id"])
        declared_counts = _decoded_json(event.get("side_counts_json"))
        declared = (
            {str(side): int(count) for side, count in declared_counts.items()}
            if isinstance(declared_counts, Mapping)
            else None
        )
        actual = actual_sides.get(key, {})
        if declared is not None and declared != {
            side: actual.get(side, 0) for side in declared
        }:
            errors.append(
                f"book_tape_event.v1: {key} side counts disagree with level rows"
            )
    if controls is not None:
        errors.extend(_book_control_evidence_errors(controls, tape_events=events))
    return BookTapeBundleValidationReport(tuple(errors))


def validate_book_control_evidence(
    controls: pd.DataFrame,
    *,
    tape_events: pd.DataFrame | None = None,
    topbook_main: pd.DataFrame | None = None,
    topbook_checkpoint: pd.DataFrame | None = None,
) -> BookTapeBundleValidationReport:
    """Validate recovery controls against their exact physical evidence roles."""
    errors: list[str] = []
    for frame, version in (
        (controls, BOOK_TAPE_CONTROL_SCHEMA_VERSION),
        (tape_events, BOOK_TAPE_EVENT_SCHEMA_VERSION),
        (topbook_main, TOPBOOK_SCHEMA_VERSION),
        (topbook_checkpoint, TOPBOOK_SCHEMA_VERSION),
    ):
        if frame is None:
            continue
        report = validate_frame(frame, version, strict=True)
        errors.extend(f"{version}: {error}" for error in report.errors)
    if not errors:
        errors.extend(
            _book_control_evidence_errors(
                controls,
                tape_events=tape_events,
                topbook_main=topbook_main,
                topbook_checkpoint=topbook_checkpoint,
            )
        )
    return BookTapeBundleValidationReport(tuple(errors))


def _book_control_evidence_errors(
    controls: pd.DataFrame,
    *,
    tape_events: pd.DataFrame | None = None,
    topbook_main: pd.DataFrame | None = None,
    topbook_checkpoint: pd.DataFrame | None = None,
) -> list[str]:
    errors: list[str] = []
    evidence_frames = {
        "tape_event": tape_events,
        "topbook_main": topbook_main,
        "topbook_checkpoint": topbook_checkpoint,
    }
    evidence_by_role: dict[str, dict[tuple[str, str], pd.Series]] = {}
    for role, frame in evidence_frames.items():
        if frame is None:
            continue
        index: dict[tuple[str, str], pd.Series] = {}
        for _, row in frame.iterrows():
            evidence_id = (
                _evidence_text(row.get("event_id"))
                if role == "tape_event"
                else topbook_evidence_id(row)
            )
            key = (_evidence_text(row.get("collector_run_id")), evidence_id)
            if key in index:
                errors.append(f"{role}: duplicate canonical evidence identity {key}")
            else:
                index[key] = row
        evidence_by_role[role] = index

    for _, control in controls.iterrows():
        role = _evidence_text(control.get("evidence_role"))
        if not role:
            continue
        key = (
            _evidence_text(control.get("collector_run_id")),
            _evidence_text(control.get("evidence_id")),
        )
        parent = evidence_by_role.get(role, {}).get(key)
        if parent is None:
            errors.append(f"book_tape_control.v1: unresolved {role} evidence {key}")
            continue
        field_pairs: tuple[tuple[str, str], ...]
        if role == "tape_event":
            field_pairs = (
                ("venue", "venue"),
                ("venue_market_id", "venue_market_id"),
                ("venue_book_id", "venue_book_id"),
                ("epoch_id", "epoch_id"),
                ("received_at_utc", "received_at_utc"),
                ("received_at_monotonic_ns", "received_at_monotonic_ns"),
                ("exchange_at_utc", "exchange_at_utc"),
                ("local_sequence", "local_sequence"),
                ("venue_sequence", "venue_sequence"),
            )
        else:
            field_pairs = (
                ("venue", "exchange"),
                ("venue_market_id", "venue_market_id"),
                ("venue_book_id", "instrument_id"),
                ("received_at_utc", "received_at_utc"),
                ("received_at_monotonic_ns", "received_at_monotonic_ns"),
                ("exchange_at_utc", "exchange_ts_utc"),
                ("local_sequence", "local_sequence"),
                ("venue_sequence", "venue_sequence"),
            )
        for control_field, evidence_field in field_pairs:
            control_value = _evidence_value(control.get(control_field))
            evidence_value = _evidence_value(parent.get(evidence_field))
            if control_field in {"received_at_utc", "exchange_at_utc"}:
                try:
                    timestamps_equal = _causal_utc_timestamps_equal(
                        control_value,
                        evidence_value,
                        optional=control_field == "exchange_at_utc",
                    )
                except ValueError:
                    errors.append(
                        "book_tape_control.v1: "
                        f"{control_field} or {role} {evidence_field} is not a "
                        f"valid explicit UTC timestamp for evidence {key}"
                    )
                    continue
                if not timestamps_equal:
                    errors.append(
                        "book_tape_control.v1: "
                        f"{control_field} disagrees with {role} evidence {key}"
                    )
                continue
            if control_field == "venue_sequence":
                control_value = None if control_value is None else str(control_value)
                evidence_value = None if evidence_value is None else str(evidence_value)
            if control_value != evidence_value:
                errors.append(
                    "book_tape_control.v1: "
                    f"{control_field} disagrees with {role} evidence {key}"
                )
        if role == "tape_event":
            expected_subsequence = int(parent.get("subsequence") or 0) + 1
            if int(control.get("subsequence") or 0) != expected_subsequence:
                errors.append(
                    "book_tape_control.v1: subsequence disagrees with "
                    f"{role} evidence {key}"
                )
        elif _evidence_value(control.get("epoch_id")) is not None:
            errors.append(
                "book_tape_control.v1: epoch_id must be absent for topbook evidence"
            )
        if str(control.get("control_type") or "") != "book_recovered":
            continue
        if role == "tape_event":
            if str(parent.get("event_kind") or "") != "checkpoint":
                errors.append(
                    "book_tape_control.v1: recovery evidence must be a checkpoint"
                )
            if _parse_bool(parent.get("reconstructible")) is not True:
                errors.append(
                    "book_tape_control.v1: recovery evidence must be reconstructible"
                )
        if _parse_bool(parent.get("valid_state")) is not True:
            errors.append(
                "book_tape_control.v1: recovery evidence must have valid state"
            )
    return errors


def _evidence_text(value: Any) -> str:
    normalized = _evidence_value(value)
    return "" if normalized is None else str(normalized).strip()


def _causal_utc_timestamps_equal(
    left: Any,
    right: Any,
    *,
    optional: bool,
) -> bool:
    left_missing = left is None
    right_missing = right is None
    if left_missing or right_missing:
        if not optional:
            raise ValueError("required UTC timestamp is absent")
        return left_missing and right_missing
    return _utc_epoch_nanoseconds(left) == _utc_epoch_nanoseconds(right)


def _utc_epoch_nanoseconds(value: Any) -> int:
    """Parse an explicit UTC timestamp without losing sub-microsecond precision."""
    text = str(value).strip()
    match = _UTC_TIMESTAMP_RE.fullmatch(text)
    if match is None:
        raise ValueError("invalid explicit UTC timestamp")
    parts = {
        name: int(match.group(name))
        for name in ("year", "month", "day", "hour", "minute", "second")
    }
    try:
        parsed = datetime(**parts, tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError("invalid explicit UTC timestamp") from exc
    delta = parsed - _UNIX_EPOCH_UTC
    whole_seconds = delta.days * 86_400 + delta.seconds
    fraction = (match.group("fraction") or "").ljust(9, "0")
    return whole_seconds * 1_000_000_000 + int(fraction or "0")


def _evidence_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    if not isinstance(value, (str, bytes, bool, int, float)) and hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _contract_evidence_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    hash_columns = (
        "evidence_id",
        "source_row_hash",
        "raw_payload_hash",
        "evidence_projection_hash",
    )
    hexadecimal = set("0123456789abcdef")
    for column in hash_columns:
        if column not in df.columns:
            continue
        invalid = 0
        for value in df[column].tolist():
            if value is None or (isinstance(value, float) and math.isnan(value)):
                if column == "raw_payload_hash":
                    continue
                invalid += 1
                continue
            text = str(value).strip()
            if len(text) != 64 or any(char not in hexadecimal for char in text):
                invalid += 1
        if invalid:
            errors.append(f"{column}: {invalid} values are not lowercase sha256")

    for column in ("observed_at_utc", "derived_at_utc", "close_time"):
        if column not in df.columns:
            continue
        invalid = sum(
            1
            for value in df[column].tolist()
            if _present_text(value) and not _is_explicit_utc_timestamp(value)
        )
        if invalid:
            errors.append(f"{column}: {invalid} values are not explicit UTC timestamps")

    identity_bad = 0
    rules_bad = 0
    mapping_bad = 0
    reasons_bad = 0
    for _, row in df.iterrows():
        identity_complete = _parse_bool(row.get("identity_complete")) is True
        identity_valid = bool(
            _present_text(row.get("venue"))
            and _present_text(row.get("market_key"))
            and _present_text(row.get("venue_event_key"))
        )
        if identity_complete != identity_valid:
            identity_bad += 1

        rules_complete = _parse_bool(row.get("rules_complete")) is True
        rules_valid = _present_text(row.get("rules_text"))
        if rules_complete != rules_valid:
            rules_bad += 1

        mappings = _json_or_list(row.get("instrument_mapping_json"))
        mapping_keys: list[tuple[str, str]] = []
        for item in mappings:
            if not isinstance(item, Mapping):
                continue
            instrument = str(item.get("instrument_key") or "").strip()
            outcome = str(item.get("outcome") or "").strip()
            if instrument and outcome:
                mapping_keys.append((instrument, outcome))
        mapping_valid = bool(
            mappings
            and len(mapping_keys) == len(mappings)
            and len({key for key, _ in mapping_keys}) == len(mapping_keys)
            and len({outcome.casefold() for _, outcome in mapping_keys})
            == len(mapping_keys)
        )
        mapping_complete = _parse_bool(row.get("instrument_mapping_complete")) is True
        if mapping_complete != mapping_valid:
            mapping_bad += 1

        reasons = {
            str(value).strip()
            for value in _json_or_list(row.get("completeness_reasons_json"))
            if str(value).strip()
        }
        expected = {
            reason
            for complete, reason in (
                (identity_complete, "identity_incomplete"),
                (rules_complete, "rules_incomplete"),
                (mapping_complete, "instrument_mapping_incomplete"),
            )
            if not complete
        }
        controlled_reasons = {
            "identity_incomplete",
            "rules_incomplete",
            "instrument_mapping_incomplete",
        }
        event_reason_expected = not _present_text(row.get("venue_event_key"))
        if reasons.intersection(controlled_reasons) != expected:
            reasons_bad += 1
        if ("venue_event_identity_incomplete" in reasons) != event_reason_expected:
            reasons_bad += 1

    if identity_bad:
        errors.append(
            "identity_complete: "
            f"{identity_bad} rows disagree with venue/market/event identity"
        )
    if rules_bad:
        errors.append(
            f"rules_complete: {rules_bad} rows disagree with populated rules_text"
        )
    if mapping_bad:
        errors.append(
            "instrument_mapping_complete: "
            f"{mapping_bad} rows disagree with unique instrument/outcome mappings"
        )
    if reasons_bad:
        errors.append(
            "completeness_reasons_json: "
            f"{reasons_bad} rows disagree with completeness flags"
        )
    return errors


def _is_explicit_utc_timestamp(value: Any) -> bool:
    try:
        _utc_epoch_nanoseconds(value)
    except ValueError:
        pass
    else:
        return True
    text = str(value).strip()
    if not text:
        return False
    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() == timezone.utc.utcoffset(
        None
    )


def _present_text(value: Any) -> bool:
    if value is None:
        return False
    try:
        if bool(pd.isna(value)):
            return False
    except (TypeError, ValueError):
        pass
    return bool(str(value).strip())


def _schema_version_errors(df: pd.DataFrame, spec: TableSpec) -> list[str]:
    if "schema_version" not in df.columns:
        return []
    values = df["schema_version"].dropna().astype(str)
    if values.empty:
        return []
    bad = values != spec.version
    if bool(bad.any()):
        return [f"schema_version: {int(bad.sum())} values do not equal {spec.version}"]
    return []


def _price_invariant_errors(df: pd.DataFrame, spec: TableSpec) -> list[str]:
    errors: list[str] = []
    for column in spec.columns:
        if column not in df.columns:
            continue
        lower = column.lower()
        is_price_column = (
            lower
            in {
                "price",
                "bid",
                "ask",
                "mid",
                "price_gap",
                "yes_bid",
                "yes_ask",
                "no_bid",
                "no_ask",
                "best_bid",
                "best_ask",
            }
            or lower.endswith("_bid")
            or lower.endswith("_ask")
            or lower.endswith("_mid")
            or lower.endswith("_price")
            or "price_dollars" in lower
            or "bid_dollars" in lower
            or "ask_dollars" in lower
            or "mid_dollars" in lower
        )
        if not is_price_column:
            continue
        values = pd.to_numeric(df[column], errors="coerce")
        present = values.notna()
        bad = present & ~values.between(0, 1)
        if bool(bad.any()):
            errors.append(f"{column}: {int(bad.sum())} prices outside [0, 1]")
    return errors


def _topbook_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    bid = _numeric_column(df, "best_bid_dollars")
    ask = _numeric_column(df, "best_ask_dollars")
    spread = _numeric_column(df, "spread_dollars")
    severe = _bool_mask(_quality_flag_series(df), _has_severe_flag)
    if bid is not None and ask is not None:
        computed = ask - bid
        if spread is not None:
            mismatch = (
                computed.notna()
                & spread.notna()
                & ((computed - spread).abs() > _PRICE_TOLERANCE)
            )
            if bool(mismatch.any()):
                errors.append(
                    f"spread_dollars: {int(mismatch.sum())} rows do not equal ask-bid"
                )
        negative = computed.notna() & (computed < 0) & ~severe
        if bool(negative.any()):
            errors.append(
                "spread_dollars: "
                f"{int(negative.sum())} negative spreads without crossed/negative-spread flag"
            )
    errors.extend(
        _nonnegative_errors(
            df, ("bid_size_contracts", "ask_size_contracts", "quote_age_ms")
        )
    )
    if "valid_state" in df.columns:
        valid = _parsed_bool_mask(df["valid_state"])
        invalid = severe & valid
        if bool(invalid.any()):
            errors.append(
                f"valid_state: {int(invalid.sum())} severe-flag rows marked valid"
            )
    return errors


def _depth_invariant_errors(df: pd.DataFrame) -> list[str]:
    return _nonnegative_errors(
        df,
        ("size_contracts", "cumulative_size_contracts", "level_index"),
    )


def _market_resolution_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    key_columns = ("platform", "market_key")
    if not set(key_columns).issubset(df.columns):
        return errors
    for column in key_columns:
        values = df[column].dropna().astype(str).str.strip()
        empty = values == ""
        if bool(empty.any()):
            errors.append(f"{column}: {int(empty.sum())} empty values")
    keyed = df.loc[
        df[list(key_columns)].notna().all(axis=1),
        list(key_columns),
    ].astype(str)
    keyed = keyed.apply(lambda col: col.str.strip())
    keyed = keyed[(keyed["platform"] != "") & (keyed["market_key"] != "")]
    duplicates = keyed.duplicated(subset=list(key_columns), keep=False)
    if bool(duplicates.any()):
        duplicate_count = int(keyed.loc[duplicates].drop_duplicates().shape[0])
        errors.append(
            "platform/market_key: "
            f"{duplicate_count} duplicate market resolution key values"
        )
    return errors


def _co_resolution_observation_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    key_columns = (
        "observation_run_id",
        "polymarket_market_key",
        "polymarket_instrument_key",
        "kalshi_market_key",
        "kalshi_instrument_key",
    )
    if set(key_columns).issubset(df.columns):
        keyed = df.loc[:, list(key_columns)].fillna("").astype(str)
        keyed = keyed.apply(lambda col: col.str.strip())
        duplicates = keyed.duplicated(subset=list(key_columns), keep=False)
        if bool(duplicates.any()):
            duplicate_count = int(keyed.loc[duplicates].drop_duplicates().shape[0])
            errors.append(
                "co_resolution_observation primary key: "
                f"{duplicate_count} duplicate values"
            )
    for column in ("observation_run_id", "polymarket_market_key", "kalshi_market_key"):
        if column in df.columns:
            empty = df[column].fillna("").astype(str).str.strip() == ""
            if bool(empty.any()):
                errors.append(f"{column}: {int(empty.sum())} empty values")
    if "binary_label_grain" in df.columns:
        grains = df["binary_label_grain"].fillna("").astype(str).str.strip()
        bad = ~grains.isin({"instrument", "market_fallback"})
        if bool(bad.any()):
            errors.append(f"binary_label_grain: {int(bad.sum())} unsupported values")

    included = _nullable_bool_column(df, "included_in_fit").fillna(False)
    known_binary = _nullable_bool_column(df, "known_binary_pair").fillna(False)
    both_terminal = _nullable_bool_column(df, "both_terminal_known").fillna(False)
    same_binary = _nullable_bool_column(df, "same_binary_outcome")
    inverse_binary = _nullable_bool_column(df, "inverse_binary_outcome")
    same_terminal = _nullable_bool_column(df, "same_terminal_outcome")

    if "binary_label_grain" in df.columns:
        instrument_grain = (
            df["binary_label_grain"].fillna("").astype(str).str.strip() == "instrument"
        )
        bad_included_grain = included & ~instrument_grain
        if bool(bad_included_grain.any()):
            errors.append(
                "included_in_fit: "
                f"{int(bad_included_grain.sum())} rows are not instrument-grain"
            )
        bad_known_grain = known_binary & ~instrument_grain
        if bool(bad_known_grain.any()):
            errors.append(
                "known_binary_pair: "
                f"{int(bad_known_grain.sum())} rows are not instrument-grain"
            )
    bad_included_known = included & ~known_binary
    if bool(bad_included_known.any()):
        errors.append(
            "included_in_fit: "
            f"{int(bad_included_known.sum())} rows are not known binary pairs"
        )
    for column in ("polymarket_binary_yes", "kalshi_binary_yes"):
        if column not in df.columns:
            continue
        missing = pd.to_numeric(df[column], errors="coerce").isna()
        bad = included & missing
        if bool(bad.any()):
            errors.append(f"{column}: {int(bad.sum())} included rows are null")
        known_missing = known_binary & missing
        if bool(known_missing.any()):
            errors.append(
                f"{column}: {int(known_missing.sum())} known binary rows are null"
            )
        values = pd.to_numeric(df[column], errors="coerce")
        present = df[column].notna()
        unsupported = present & ~values.isin([0, 1])
        if bool(unsupported.any()):
            errors.append(f"{column}: {int(unsupported.sum())} values outside {{0, 1}}")
    if "exclusion_reason" in df.columns:
        reasons = df["exclusion_reason"].fillna("").astype(str).str.strip()
        included_with_reason = included & (reasons != "")
        excluded_without_reason = ~included & (reasons == "")
        if bool(included_with_reason.any()):
            errors.append(
                "exclusion_reason: "
                f"{int(included_with_reason.sum())} included rows have exclusions"
            )
        if bool(excluded_without_reason.any()):
            errors.append(
                "exclusion_reason: "
                f"{int(excluded_without_reason.sum())} excluded rows lack a reason"
            )

    if bool((~known_binary & same_binary.notna()).any()):
        errors.append(
            "same_binary_outcome: "
            f"{int((~known_binary & same_binary.notna()).sum())} non-binary rows are populated"
        )
    if bool((~known_binary & inverse_binary.notna()).any()):
        errors.append(
            "inverse_binary_outcome: "
            f"{int((~known_binary & inverse_binary.notna()).sum())} non-binary rows are populated"
        )
    if bool((known_binary & (same_binary.isna() | inverse_binary.isna())).any()):
        errors.append(
            "same_binary_outcome/inverse_binary_outcome: "
            f"{int((known_binary & (same_binary.isna() | inverse_binary.isna())).sum())} known binary rows are null"
        )
    complementary = known_binary & same_binary.notna() & inverse_binary.notna()
    bad_complement = complementary & (same_binary == inverse_binary)
    if bool(bad_complement.any()):
        errors.append(
            "same_binary_outcome/inverse_binary_outcome: "
            f"{int(bad_complement.sum())} known binary rows are not complementary"
        )
    if bool((~both_terminal & same_terminal.notna()).any()):
        errors.append(
            "same_terminal_outcome: "
            f"{int((~both_terminal & same_terminal.notna()).sum())} unknown terminal rows are populated"
        )

    probability_columns = (
        "polymarket_marginal_probability",
        "kalshi_marginal_probability",
        "independent_same_probability",
        "independent_inverse_probability",
    )
    for column in probability_columns:
        if column not in df.columns:
            continue
        values = pd.to_numeric(df[column], errors="coerce")
        present = df[column].notna()
        bad = present & ~values.between(0, 1)
        if bool(bad.any()):
            errors.append(f"{column}: {int(bad.sum())} probabilities outside [0, 1]")
    return errors


def _co_resolution_score_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    key_columns = (
        "experiment_id",
        "scorer_run_id",
        "polymarket_market_key",
        "polymarket_instrument_key",
        "kalshi_market_key",
        "kalshi_instrument_key",
    )
    if set(key_columns).issubset(df.columns):
        keyed = df.loc[:, list(key_columns)].fillna("").astype(str)
        keyed = keyed.apply(lambda col: col.str.strip())
        duplicates = keyed.duplicated(subset=list(key_columns), keep=False)
        if bool(duplicates.any()):
            duplicate_count = int(keyed.loc[duplicates].drop_duplicates().shape[0])
            errors.append(
                f"co_resolution_score primary key: {duplicate_count} duplicate values"
            )
    for column in (
        "experiment_id",
        "manifest_hash",
        "model_family",
        "model_spec_id",
        "feature_set_id",
        "label_policy_id",
        "scorer_run_id",
        "polymarket_market_key",
        "polymarket_instrument_key",
        "kalshi_market_key",
        "kalshi_instrument_key",
    ):
        if column in df.columns:
            empty = df[column].fillna("").astype(str).str.strip() == ""
            if bool(empty.any()):
                errors.append(f"{column}: {int(empty.sum())} empty values")

    probability_columns = (
        "co_resolution_probability",
        "co_resolution_lower",
        "co_resolution_upper",
        "inverse_resolution_probability",
        "inverse_resolution_lower",
        "inverse_resolution_upper",
        "independent_same_probability",
        "independent_inverse_probability",
    )
    for column in probability_columns:
        if column not in df.columns:
            continue
        values = pd.to_numeric(df[column], errors="coerce")
        present = df[column].notna()
        bad = present & ~values.between(0, 1)
        if bool(bad.any()):
            errors.append(f"{column}: {int(bad.sum())} probabilities outside [0, 1]")
    errors.extend(
        _ordered_interval_errors(
            df,
            probability="co_resolution_probability",
            lower="co_resolution_lower",
            upper="co_resolution_upper",
        )
    )
    errors.extend(
        _ordered_interval_errors(
            df,
            probability="inverse_resolution_probability",
            lower="inverse_resolution_lower",
            upper="inverse_resolution_upper",
        )
    )

    required_flag = "binary_complement_incoherent"
    if {
        "score_semantics",
        "complement_residual",
        "data_quality_flags",
        "risk_flags",
    }.issubset(df.columns):
        semantics = df["score_semantics"].fillna("").astype(str).str.strip()
        residuals = pd.to_numeric(df["complement_residual"], errors="coerce")
        incoherent = (
            (semantics == "binary_same_inverse_complement")
            & residuals.notna()
            & (residuals.abs() > 1e-6)
        )
        missing_flag = []
        for idx in df.index[incoherent].tolist():
            data_flags = set(_flags(df.at[idx, "data_quality_flags"]))
            risk_flags = set(_flags(df.at[idx, "risk_flags"]))
            missing_flag.append(
                required_flag not in data_flags or required_flag not in risk_flags
            )
        if any(missing_flag):
            errors.append(
                "complement_residual: "
                f"{sum(missing_flag)} incoherent rows missing binary_complement_incoherent flag"
            )
    return errors


def _ordered_interval_errors(
    df: pd.DataFrame,
    *,
    probability: str,
    lower: str,
    upper: str,
) -> list[str]:
    if not {probability, lower, upper}.issubset(df.columns):
        return []
    prob = pd.to_numeric(df[probability], errors="coerce")
    low = pd.to_numeric(df[lower], errors="coerce")
    high = pd.to_numeric(df[upper], errors="coerce")
    present = prob.notna() & low.notna() & high.notna()
    bad = present & ((low > prob) | (prob > high))
    if bool(bad.any()):
        return [
            f"{lower}/{probability}/{upper}: {int(bad.sum())} intervals are misordered"
        ]
    return []


def _kalshi_complement_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    yes_bid = _numeric_column(df, "yes_bid")
    yes_ask = _numeric_column(df, "yes_ask")
    no_bid = _numeric_column(df, "no_bid")
    no_ask = _numeric_column(df, "no_ask")
    severe = _bool_mask(_quality_flag_series(df), _has_severe_flag)
    if yes_ask is not None and no_bid is not None:
        mismatch = (
            yes_ask.notna()
            & no_bid.notna()
            & ((yes_ask - (1 - no_bid)).abs() > _PRICE_TOLERANCE)
        )
        if bool(mismatch.any()):
            errors.append(
                f"yes_ask: {int(mismatch.sum())} rows do not equal 1 - no_bid"
            )
    if no_ask is not None and yes_bid is not None:
        mismatch = (
            no_ask.notna()
            & yes_bid.notna()
            & ((no_ask - (1 - yes_bid)).abs() > _PRICE_TOLERANCE)
        )
        if bool(mismatch.any()):
            errors.append(
                f"no_ask: {int(mismatch.sum())} rows do not equal 1 - yes_bid"
            )
    if yes_bid is not None and no_bid is not None:
        crossed = (
            yes_bid.notna()
            & no_bid.notna()
            & ((yes_bid + no_bid) > 1 + _PRICE_TOLERANCE)
            & ~severe
        )
        if bool(crossed.any()):
            errors.append(
                f"yes_bid/no_bid: {int(crossed.sum())} crossed rows without quality flag"
            )
    return errors


def _match_relation_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    if "match_id" in df.columns:
        match_ids = df["match_id"].dropna().astype(str).str.strip()
        empty = match_ids == ""
        if bool(empty.any()):
            errors.append(f"match_id: {int(empty.sum())} empty values")
        duplicates = match_ids[match_ids.duplicated()]
        if not duplicates.empty:
            errors.append(f"match_id: {duplicates.nunique()} duplicate values")
    id_columns = {
        "match_id",
        "polymarket_market_key",
        "polymarket_instrument_key",
        "kalshi_market_key",
        "kalshi_instrument_key",
    }
    if id_columns.issubset(df.columns):
        expected_ids = (
            df.apply(_expected_match_relation_id, axis=1)
            .fillna("")
            .astype(str)
            .str.strip()
        )
        actual_ids = df["match_id"].fillna("").astype(str).str.strip()
        has_expected = expected_ids.ne("")
        mismatched_ids = has_expected & actual_ids.ne(expected_ids)
        if bool(mismatched_ids.any()):
            errors.append(
                "match_id: "
                f"{int(mismatched_ids.sum())} rows do not match venue instrument keys"
            )
    for column in (
        "polymarket_market_key",
        "polymarket_instrument_key",
        "polymarket_token_id",
        "kalshi_market_key",
        "kalshi_instrument_key",
        "relation_label",
    ):
        if column in df.columns:
            values = df[column].dropna().astype(str).str.strip()
            empty = values == ""
            if bool(empty.any()):
                errors.append(f"{column}: {int(empty.sum())} empty values")
    if "relation_label" not in df.columns:
        return errors
    labels = df["relation_label"].fillna("").astype(str).str.strip().str.lower()
    allowed = {
        "exact_equivalent",
        "inverse_equivalent",
        "same_event_different_outcome",
        "same_event_different_cutoff",
        "same_event_different_settlement_scope",
        "same_event_different_resolution_source",
        "same_context_only",
        "parlay_or_composite",
        "ambiguous_requires_review",
        "unrelated",
    }
    bad_labels = sorted(set(labels) - allowed)
    if bad_labels:
        errors.append(f"relation_label: unsupported values {bad_labels}")
    if "confidence_score" in df.columns:
        confidence = pd.to_numeric(df["confidence_score"], errors="coerce")
        present = df["confidence_score"].notna()
        bad = present & ~confidence.between(0, 1)
        if bool(bad.any()):
            errors.append(f"confidence_score: {int(bad.sum())} values outside [0, 1]")
    if "polymarket_token_id" in df.columns:
        token_values = df["polymarket_token_id"].dropna().astype(str)
        stripped = token_values.str.strip()
        serialized = (
            stripped.str.startswith("[")
            | stripped.str.startswith("(")
            | token_values.str.contains(",", regex=False)
        )
        if bool(serialized.any()):
            errors.append(
                "polymarket_token_id: "
                f"{int(serialized.sum())} rows contain serialized/list-like token ids"
            )
        long_decimal = token_values.map(
            lambda value: value.isdigit() and len(value) > 90
        )
        if bool(long_decimal.any()):
            errors.append(
                "polymarket_token_id: "
                f"{int(long_decimal.sum())} rows look longer than single CLOB token ids"
            )
    if "evidence_json" in df.columns:
        bad_evidence = 0
        for value in df["evidence_json"].tolist():
            if value is None:
                bad_evidence += 1
                continue
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                except json.JSONDecodeError:
                    bad_evidence += 1
                    continue
                if not isinstance(parsed, dict):
                    bad_evidence += 1
            elif not isinstance(value, Mapping):
                bad_evidence += 1
        if bad_evidence:
            errors.append(f"evidence_json: {bad_evidence} rows are not JSON objects")
    return errors


def _expected_match_relation_id(row: pd.Series) -> str:
    pm_market = _match_relation_id_part(row.get("polymarket_market_key"))
    pm_instrument = _match_relation_id_part(row.get("polymarket_instrument_key"))
    kx_market = _match_relation_id_part(row.get("kalshi_market_key"))
    kx_instrument = _match_relation_id_part(row.get("kalshi_instrument_key"))
    if pm_market and pm_instrument and kx_market and kx_instrument:
        return f"pm:{pm_market}:{pm_instrument}|kalshi:{kx_market}:{kx_instrument}"
    return ""


def _match_relation_id_part(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text:
        return None
    return text.replace("|", "%7C").replace(" ", "_")


def _tracking_match_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    for column in (
        "tracking_pair_id",
        "match_tier",
        "relation_type",
        "polymarket_market_key",
        "kalshi_market_key",
        "polymarket_question",
        "kalshi_question",
    ):
        if column in df.columns:
            values = df[column].dropna().astype(str).str.strip()
            empty = values == ""
            if bool(empty.any()):
                errors.append(f"{column}: {int(empty.sum())} empty values")
    if "tracking_pair_id" in df.columns:
        pair_ids = df["tracking_pair_id"].dropna().astype(str).str.strip()
        duplicates = pair_ids[pair_ids.duplicated()]
        if not duplicates.empty:
            errors.append(f"tracking_pair_id: {duplicates.nunique()} duplicate values")
    if {"tracking_pair_id", "polymarket_market_key", "kalshi_market_key"}.issubset(
        df.columns
    ):
        expected = (
            "pm:"
            + df["polymarket_market_key"].fillna("").astype(str).str.strip()
            + "|kalshi:"
            + df["kalshi_market_key"].fillna("").astype(str).str.strip()
        )
        actual = df["tracking_pair_id"].fillna("").astype(str).str.strip()
        has_keys = df["polymarket_market_key"].fillna("").astype(str).str.strip().ne(
            ""
        ) & df["kalshi_market_key"].fillna("").astype(str).str.strip().ne("")
        mismatched = has_keys & actual.ne(expected)
        if bool(mismatched.any()):
            errors.append(
                "tracking_pair_id: "
                f"{int(mismatched.sum())} rows do not match venue market keys"
            )
    for column in ("confidence_score", "title_similarity"):
        if column in df.columns:
            score = pd.to_numeric(df[column], errors="coerce")
            present = df[column].notna()
            bad = present & ~score.between(0, 1)
            if bool(bad.any()):
                errors.append(f"{column}: {int(bad.sum())} values outside [0, 1]")
    return errors


def _tracking_health_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    for column in (
        "match_id",
        "observed_at_utc",
        "relation_label",
        "health_status",
    ):
        if column in df.columns:
            values = df[column].dropna().astype(str).str.strip()
            empty = values == ""
            if bool(empty.any()):
                errors.append(f"{column}: {int(empty.sum())} empty values")
    errors.extend(
        _nonnegative_errors(
            df,
            ("polymarket_quote_age_ms", "kalshi_quote_age_ms", "max_quote_age_ms"),
        )
    )
    if "health_status" in df.columns:
        statuses = df["health_status"].fillna("").astype(str).str.strip().str.lower()
        allowed = {"ready", "missing", "invalid", "stale", "skipped", "blocked"}
        bad = ~statuses.isin(allowed)
        if bool(bad.any()):
            errors.append(f"health_status: {int(bad.sum())} unsupported values")
    return errors


def _canonical_kalshi_topbook_errors(df: pd.DataFrame) -> list[str]:
    required = {
        "exchange",
        "outcome",
        "best_bid_dollars",
        "best_ask_dollars",
        "quality_flags",
    }
    if not required.issubset(df.columns):
        return []
    kalshi = df[df["exchange"].astype(str).str.lower() == "kalshi"].copy()
    if kalshi.empty:
        return []
    group_columns = [
        column
        for column in (
            "collector_run_id",
            "venue_market_id",
            "received_at_utc",
            "local_sequence",
            "venue_sequence",
        )
        if column in kalshi.columns
    ]
    if not group_columns:
        return [
            "kalshi topbook complements: skipped because grouping columns are missing"
        ]

    yes_ask_mismatches = 0
    no_ask_mismatches = 0
    crossed_without_flag = 0
    for _, group in kalshi.groupby(group_columns, dropna=False):
        yes_rows = group[group["outcome"].astype(str).str.upper() == "YES"]
        no_rows = group[group["outcome"].astype(str).str.upper() == "NO"]
        if yes_rows.empty or no_rows.empty:
            continue
        yes = yes_rows.iloc[0]
        no = no_rows.iloc[0]
        yes_bid = _parse_floatish(yes.get("best_bid_dollars"))
        yes_ask = _parse_floatish(yes.get("best_ask_dollars"))
        no_bid = _parse_floatish(no.get("best_bid_dollars"))
        no_ask = _parse_floatish(no.get("best_ask_dollars"))
        severe = _has_severe_flag(yes.get("quality_flags")) or _has_severe_flag(
            no.get("quality_flags")
        )
        if (
            yes_ask is not None
            and no_bid is not None
            and abs(yes_ask - (1 - no_bid)) > _PRICE_TOLERANCE
        ):
            yes_ask_mismatches += 1
        if (
            no_ask is not None
            and yes_bid is not None
            and abs(no_ask - (1 - yes_bid)) > _PRICE_TOLERANCE
        ):
            no_ask_mismatches += 1
        if (
            yes_bid is not None
            and no_bid is not None
            and yes_bid + no_bid > 1 + _PRICE_TOLERANCE
            and not severe
        ):
            crossed_without_flag += 1

    errors: list[str] = []
    if yes_ask_mismatches:
        errors.append(
            "kalshi topbook complements: "
            f"{yes_ask_mismatches} YES asks do not equal 1 - NO bid"
        )
    if no_ask_mismatches:
        errors.append(
            "kalshi topbook complements: "
            f"{no_ask_mismatches} NO asks do not equal 1 - YES bid"
        )
    if crossed_without_flag:
        errors.append(
            "kalshi topbook complements: "
            f"{crossed_without_flag} crossed YES/NO bid pairs without quality flag"
        )
    return errors


def _signal_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    if "signal_id" in df.columns:
        signal_ids = df["signal_id"].dropna().astype(str).str.strip()
        empty = signal_ids == ""
        if bool(empty.any()):
            errors.append(f"signal_id: {int(empty.sum())} empty values")
        duplicates = signal_ids[signal_ids.duplicated()]
        if not duplicates.empty:
            errors.append(f"signal_id: {duplicates.nunique()} duplicate values")
    for column in (
        "match_id",
        "strategy_version",
        "observed_at_utc",
        "relation_label",
        "decision",
    ):
        if column in df.columns:
            values = df[column].dropna().astype(str).str.strip()
            empty = values == ""
            if bool(empty.any()):
                errors.append(f"{column}: {int(empty.sum())} empty values")
    errors.extend(
        _nonnegative_errors(
            df,
            ("fee_estimate", "slippage_estimate", "executable_size", "quote_age_ms"),
        )
    )
    if {"gross_edge", "fee_estimate", "slippage_estimate", "net_edge"}.issubset(
        df.columns
    ):
        gross = pd.to_numeric(df["gross_edge"], errors="coerce")
        fees = pd.to_numeric(df["fee_estimate"], errors="coerce").fillna(0.0)
        slippage = pd.to_numeric(df["slippage_estimate"], errors="coerce").fillna(0.0)
        net = pd.to_numeric(df["net_edge"], errors="coerce")
        mismatch = (
            gross.notna()
            & net.notna()
            & ((gross - fees - slippage - net).abs() > _PRICE_TOLERANCE)
        )
        if bool(mismatch.any()):
            errors.append(
                f"net_edge: {int(mismatch.sum())} rows do not equal gross-fees-slippage"
            )
    return errors


def _order_intent_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    for column in ("order_intent_id", "signal_id", "client_order_id"):
        if column in df.columns:
            values = df[column].dropna().astype(str).str.strip()
            empty = values == ""
            if bool(empty.any()):
                errors.append(f"{column}: {int(empty.sum())} empty values")
            if column in {"order_intent_id", "client_order_id"}:
                duplicates = values[values.duplicated()]
                if not duplicates.empty:
                    errors.append(f"{column}: {duplicates.nunique()} duplicate values")
    errors.extend(_nonnegative_errors(df, ("limit_price", "size_contracts")))
    if "action" in df.columns:
        actions = df["action"].fillna("").astype(str).str.strip().str.lower()
        bad = ~actions.isin({"buy", "sell"})
        if bool(bad.any()):
            errors.append(f"action: {int(bad.sum())} unsupported values")
    if "book_side" in df.columns:
        sides = df["book_side"].fillna("").astype(str).str.strip().str.lower()
        bad = ~sides.isin({"bid", "ask"})
        if bool(bad.any()):
            errors.append(f"book_side: {int(bad.sum())} unsupported values")
    if "client_order_id" in df.columns:
        payload_columns = [
            column
            for column in (
                "signal_id",
                "venue",
                "instrument_id",
                "outcome_side",
                "action",
                "book_side",
                "limit_price",
                "size_contracts",
                "order_type",
                "post_only",
                "reduce_only",
                "expires_at_utc",
                "mode",
            )
            if column in df.columns
        ]
        conflicts = 0
        for _, group in df.groupby("client_order_id", dropna=False):
            if len(group) <= 1:
                continue
            if group[payload_columns].astype(str).drop_duplicates().shape[0] > 1:
                conflicts += 1
        if conflicts:
            errors.append(
                f"client_order_id: {conflicts} duplicate ids have conflicting payloads"
            )
    return errors


def _maker_quote_plan_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []

    def require_nonempty(
        columns: Iterable[str], *, mask: pd.Series | None = None
    ) -> None:
        frame = df if mask is None else df.loc[mask]
        for column in columns:
            if column in frame.columns:
                values = frame[column].dropna().astype(str).str.strip()
                empty = values == ""
                if bool(empty.any()):
                    errors.append(f"{column}: {int(empty.sum())} empty values")

    require_nonempty(
        (
            "plan_id",
            "plan_fingerprint",
            "match_id",
        )
    )
    errors.extend(
        _nonnegative_errors(
            df,
            (
                "quote_size_contracts",
                "hedge_size_contracts",
                "maker_fee_dollars",
                "hedge_taker_fee_dollars",
                "slippage_allowance",
                "depth_levels_consumed",
                "quote_ttl_ms",
                "min_quote_size_contracts",
                "requote_threshold",
            ),
        )
    )
    if "quote_action" in df.columns:
        actions = df["quote_action"].fillna("").astype(str).str.strip().str.lower()
        bad = ~actions.isin({"buy", "sell"})
        if bool(bad.any()):
            errors.append(f"quote_action: {int(bad.sum())} unsupported values")
    if "hedge_action" in df.columns:
        actions = df["hedge_action"].fillna("").astype(str).str.strip().str.lower()
        bad = ~actions.isin({"buy", "sell"})
        if bool(bad.any()):
            errors.append(f"hedge_action: {int(bad.sum())} unsupported values")
    for column in ("quote_book_side", "hedge_book_side"):
        if column in df.columns:
            sides = df[column].fillna("").astype(str).str.strip().str.lower()
            bad = ~sides.isin({"bid", "ask"})
            if bool(bad.any()):
                errors.append(f"{column}: {int(bad.sum())} unsupported values")
    return errors


def _order_state_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    errors.extend(
        _nonnegative_errors(
            df,
            (
                "filled_size_contracts",
                "remaining_size_contracts",
                "average_fill_price",
                "fees_dollars",
            ),
        )
    )
    if "status" in df.columns:
        statuses = df["status"].fillna("").astype(str).str.strip().str.lower()
        allowed = {"filled", "partially_filled", "cancelled", "rejected", "open"}
        bad = ~statuses.isin(allowed)
        if bool(bad.any()):
            errors.append(f"status: {int(bad.sum())} unsupported values")
    if {"filled_size_contracts", "average_fill_price"}.issubset(df.columns):
        filled = pd.to_numeric(df["filled_size_contracts"], errors="coerce")
        avg = pd.to_numeric(df["average_fill_price"], errors="coerce")
        bad = (filled > 0) & avg.isna()
        if bool(bad.any()):
            errors.append(
                "average_fill_price: "
                f"{int(bad.sum())} filled rows have no average price"
            )
    return errors


def _paper_fill_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    if "paper_fill_id" in df.columns:
        fill_ids = df["paper_fill_id"].dropna().astype(str).str.strip()
        empty = fill_ids == ""
        if bool(empty.any()):
            errors.append(f"paper_fill_id: {int(empty.sum())} empty values")
        duplicates = fill_ids[fill_ids.duplicated()]
        if not duplicates.empty:
            errors.append(f"paper_fill_id: {duplicates.nunique()} duplicate values")
    errors.extend(
        _nonnegative_errors(
            df,
            (
                "fill_price_dollars",
                "size_contracts",
                "notional_dollars",
                "fees_dollars",
                "latency_ms",
            ),
        )
    )
    if {"fill_price_dollars", "size_contracts", "notional_dollars"}.issubset(
        df.columns
    ):
        price = pd.to_numeric(df["fill_price_dollars"], errors="coerce")
        size = pd.to_numeric(df["size_contracts"], errors="coerce")
        notional = pd.to_numeric(df["notional_dollars"], errors="coerce")
        mismatch = (
            price.notna()
            & size.notna()
            & notional.notna()
            & ((price * size - notional).abs() > _PRICE_TOLERANCE)
        )
        if bool(mismatch.any()):
            errors.append(
                "notional_dollars: "
                f"{int(mismatch.sum())} rows do not equal fill_price*size"
            )
    if "action" in df.columns:
        actions = df["action"].fillna("").astype(str).str.strip().str.lower()
        bad = ~actions.isin({"buy", "sell"})
        if bool(bad.any()):
            errors.append(f"action: {int(bad.sum())} unsupported values")
    if "book_side" in df.columns:
        sides = df["book_side"].fillna("").astype(str).str.strip().str.lower()
        bad = ~sides.isin({"bid", "ask"})
        if bool(bad.any()):
            errors.append(f"book_side: {int(bad.sum())} unsupported values")
    return errors


def _paper_position_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    errors.extend(
        _nonnegative_errors(
            df,
            (
                "filled_size_contracts",
                "unmatched_leg_size_contracts",
                "buy_notional_dollars",
                "sell_notional_dollars",
                "fees_dollars",
            ),
        )
    )
    required = {
        "buy_notional_dollars",
        "sell_notional_dollars",
        "fees_dollars",
        "gross_pnl_dollars",
        "net_pnl_dollars",
    }
    if required.issubset(df.columns):
        buy = pd.to_numeric(df["buy_notional_dollars"], errors="coerce").fillna(0.0)
        sell = pd.to_numeric(df["sell_notional_dollars"], errors="coerce").fillna(0.0)
        fees = pd.to_numeric(df["fees_dollars"], errors="coerce").fillna(0.0)
        gross = pd.to_numeric(df["gross_pnl_dollars"], errors="coerce")
        net = pd.to_numeric(df["net_pnl_dollars"], errors="coerce")
        gross_mismatch = gross.notna() & ((sell - buy - gross).abs() > _PRICE_TOLERANCE)
        net_mismatch = net.notna() & ((gross - fees - net).abs() > _PRICE_TOLERANCE)
        if bool(gross_mismatch.any()):
            errors.append(
                "gross_pnl_dollars: "
                f"{int(gross_mismatch.sum())} rows do not equal sell-buy notional"
            )
        if bool(net_mismatch.any()):
            errors.append(
                "net_pnl_dollars: "
                f"{int(net_mismatch.sum())} rows do not equal gross-fees"
            )
        if (
            "realized_pnl_dollars" in df.columns
            and "unrealized_pnl_dollars" in df.columns
        ):
            realized = pd.to_numeric(df["realized_pnl_dollars"], errors="coerce")
            unrealized = pd.to_numeric(df["unrealized_pnl_dollars"], errors="coerce")
            total = realized.fillna(0.0) + unrealized.fillna(0.0)
            pnl_mismatch = net.notna() & ((total - net).abs() > _PRICE_TOLERANCE)
            if bool(pnl_mismatch.any()):
                errors.append(
                    "net_pnl_dollars: "
                    f"{int(pnl_mismatch.sum())} rows do not equal realized+unrealized PnL"
                )
    return errors


def _historical_price_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    price_columns = (
        "open_dollars",
        "high_dollars",
        "low_dollars",
        "close_dollars",
        "mean_dollars",
        "min_dollars",
        "max_dollars",
        "previous_dollars",
    )
    for column in price_columns:
        if column not in df.columns:
            continue
        values = pd.to_numeric(df[column], errors="coerce")
        present = values.notna()
        bad = present & ~values.between(0, 1)
        if bool(bad.any()):
            errors.append(f"{column}: {int(bad.sum())} prices outside [0, 1]")
    errors.extend(
        _nonnegative_errors(
            df,
            ("interval_seconds", "volume_contracts", "open_interest_contracts"),
        )
    )
    return errors


def _convergence_observation_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    errors.extend(
        _nonnegative_errors(
            df,
            ("horizon_seconds", "elapsed_seconds", "time_to_convergence_seconds"),
        )
    )
    if {"initial_net_edge", "evaluation_net_edge", "edge_change"}.issubset(df.columns):
        initial = pd.to_numeric(df["initial_net_edge"], errors="coerce")
        final = pd.to_numeric(df["evaluation_net_edge"], errors="coerce")
        change = pd.to_numeric(df["edge_change"], errors="coerce")
        mismatch = (
            initial.notna()
            & final.notna()
            & change.notna()
            & ((final - initial - change).abs() > _PRICE_TOLERANCE)
        )
        if bool(mismatch.any()):
            errors.append(
                f"edge_change: {int(mismatch.sum())} rows do not equal final-initial net edge"
            )
    return errors


def _convergence_summary_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    errors.extend(
        _nonnegative_errors(
            df,
            (
                "observation_count",
                "decision_count",
                "converged_count",
                "persisted_count",
                "widened_count",
                "gap_count",
                "median_time_to_convergence_seconds",
            ),
        )
    )
    for column in ("convergence_rate", "survivorship_rate"):
        if column not in df.columns:
            continue
        values = pd.to_numeric(df[column], errors="coerce")
        present = values.notna()
        bad = present & ~values.between(0, 1)
        if bool(bad.any()):
            errors.append(f"{column}: {int(bad.sum())} values outside [0, 1]")
    return errors


def _backtest_report_invariant_errors(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    core_metric_columns = (
        "decision_count",
        "attempted_count",
        "fill_count",
        "position_count",
        "filled_size_contracts",
        "capital_at_risk_dollars",
        "max_concurrent_notional_dollars",
        "theoretical_edge_dollars",
        "fee_drag_dollars",
        "fill_rate",
        "markout_window_count",
    )
    for column in core_metric_columns:
        if column in df.columns:
            missing = df[column].isna()
            if bool(missing.any()):
                errors.append(
                    f"{column}: {int(missing.sum())} missing report metric values"
                )
    for column in (
        "fill_model_params_json",
        "markout_decay_json",
        "source_artifacts_json",
    ):
        if column in df.columns:
            missing = df[column].isna()
            if bool(missing.any()):
                errors.append(
                    f"{column}: {int(missing.sum())} missing report metadata values"
                )
    for column in ("data_quality_caveats", "assumption_caveats"):
        if column not in df.columns:
            continue
        missing = df[column].isna()
        if bool(missing.any()):
            errors.append(f"{column}: {int(missing.sum())} missing caveat lists")
    allowed_values = {
        "strategy_family": {"taker", "maker"},
        "source_kind": {"taker_batch", "taker_two_leg", "maker_passive"},
        "fill_model": {"optimistic", "topbook_cross", "pessimistic_queue"},
    }
    for column, allowed in allowed_values.items():
        if column not in df.columns:
            continue
        values = df[column].dropna().astype(str).str.strip().str.lower()
        bad = ~values.isin(allowed)
        if bool(bad.any()):
            errors.append(f"{column}: unsupported values {sorted(set(values[bad]))}")
    errors.extend(
        _nonnegative_errors(
            df,
            (
                "attempted_count",
                "capital_at_risk_dollars",
                "decision_count",
                "fee_drag_dollars",
                "fill_count",
                "filled_size_contracts",
                "markout_window_count",
                "max_concurrent_notional_dollars",
                "position_count",
            ),
        )
    )
    for column in ("fill_rate", "hit_rate"):
        if column not in df.columns:
            continue
        values = pd.to_numeric(df[column], errors="coerce")
        present = values.notna()
        bad = present & ~values.between(0, 1)
        if bool(bad.any()):
            errors.append(f"{column}: {int(bad.sum())} values outside [0, 1]")
    if {"fill_count", "attempted_count"}.issubset(df.columns):
        fill_count = pd.to_numeric(df["fill_count"], errors="coerce")
        attempted_count = pd.to_numeric(df["attempted_count"], errors="coerce")
        bad = (
            fill_count.notna()
            & attempted_count.notna()
            & (fill_count > attempted_count)
        )
        if bool(bad.any()):
            errors.append(f"fill_count: {int(bad.sum())} rows exceed attempted_count")
    bad_fill_rate = _ratio_mismatch_count(
        df,
        "fill_count",
        "attempted_count",
        "fill_rate",
        zero_zero_is_zero=True,
    )
    if bad_fill_rate:
        errors.append(
            f"fill_rate: {bad_fill_rate} values do not match fill_count / attempted_count"
        )
    if {"fill_count", "filled_size_contracts"}.issubset(df.columns):
        fill_count = pd.to_numeric(df["fill_count"], errors="coerce")
        filled_size = pd.to_numeric(df["filled_size_contracts"], errors="coerce")
        bad = (
            fill_count.notna()
            & filled_size.notna()
            & (fill_count == 0)
            & (filled_size != 0)
        )
        if bool(bad.any()):
            errors.append(
                f"filled_size_contracts: {int(bad.sum())} zero-fill rows have nonzero size"
            )
    if {"capital_at_risk_dollars", "max_concurrent_notional_dollars"}.issubset(
        df.columns
    ):
        capital = pd.to_numeric(df["capital_at_risk_dollars"], errors="coerce")
        max_concurrent = pd.to_numeric(
            df["max_concurrent_notional_dollars"], errors="coerce"
        )
        bad = capital.notna() & max_concurrent.notna() & (max_concurrent > capital)
        if bool(bad.any()):
            errors.append(
                "max_concurrent_notional_dollars: "
                f"{int(bad.sum())} rows exceed capital_at_risk_dollars"
            )
    if {
        "capital_at_risk_dollars",
        "estimated_pnl_lower_dollars",
        "estimated_pnl_upper_dollars",
        "return_on_capital_lower",
        "return_on_capital_upper",
        "fee_drag_ratio",
    }.issubset(df.columns):
        capital = pd.to_numeric(df["capital_at_risk_dollars"], errors="coerce")
        capital_present = capital.notna() & (capital > 0)
        pnl_lower = pd.to_numeric(df["estimated_pnl_lower_dollars"], errors="coerce")
        pnl_upper = pd.to_numeric(df["estimated_pnl_upper_dollars"], errors="coerce")
        missing_lower = (
            capital_present & pnl_lower.notna() & df["return_on_capital_lower"].isna()
        )
        if bool(missing_lower.any()):
            errors.append(
                "return_on_capital_lower: "
                f"{int(missing_lower.sum())} missing values with computable capital return"
            )
        missing_upper = (
            capital_present & pnl_upper.notna() & df["return_on_capital_upper"].isna()
        )
        if bool(missing_upper.any()):
            errors.append(
                "return_on_capital_upper: "
                f"{int(missing_upper.sum())} missing values with computable capital return"
            )
        for column in ("fee_drag_ratio",):
            missing = capital_present & df[column].isna()
            if bool(missing.any()):
                errors.append(
                    f"{column}: {int(missing.sum())} missing values with capital at risk"
                )
        for ratio_column, numerator_column in (
            ("return_on_capital_lower", "estimated_pnl_lower_dollars"),
            ("return_on_capital_upper", "estimated_pnl_upper_dollars"),
            ("fee_drag_ratio", "fee_drag_dollars"),
        ):
            mismatches = _ratio_mismatch_count(
                df,
                numerator_column,
                "capital_at_risk_dollars",
                ratio_column,
            )
            if mismatches:
                errors.append(
                    f"{ratio_column}: {mismatches} values do not match "
                    f"{numerator_column} / capital_at_risk_dollars"
                )
    if {
        "theoretical_edge_dollars",
        "estimated_pnl_upper_dollars",
        "edge_capture_ratio",
    }.issubset(df.columns):
        theoretical = pd.to_numeric(df["theoretical_edge_dollars"], errors="coerce")
        pnl_upper = pd.to_numeric(df["estimated_pnl_upper_dollars"], errors="coerce")
        edge_present = theoretical.notna() & (theoretical != 0) & pnl_upper.notna()
        missing = edge_present & df["edge_capture_ratio"].isna()
        if bool(missing.any()):
            errors.append(
                f"edge_capture_ratio: {int(missing.sum())} missing values with theoretical edge"
            )
        family = (
            df["strategy_family"].fillna("").astype(str).str.strip().str.lower()
            if "strategy_family" in df.columns
            else pd.Series(["maker"] * len(df), index=df.index)
        )
        mismatches = _ratio_mismatch_count(
            df[family == "maker"],
            "estimated_pnl_upper_dollars",
            "theoretical_edge_dollars",
            "edge_capture_ratio",
        )
        if mismatches:
            errors.append(
                "edge_capture_ratio: "
                f"{mismatches} values do not match estimated_pnl_upper_dollars / "
                "theoretical_edge_dollars"
            )
    errors.extend(
        _lower_upper_errors(
            df,
            "estimated_pnl_lower_dollars",
            "estimated_pnl_upper_dollars",
        )
    )
    errors.extend(
        _lower_upper_errors(
            df,
            "markout_pnl_lower_dollars",
            "markout_pnl_upper_dollars",
        )
    )
    return errors


def _lower_upper_errors(
    df: pd.DataFrame, lower_column: str, upper_column: str
) -> list[str]:
    if {lower_column, upper_column}.issubset(df.columns):
        lower = pd.to_numeric(df[lower_column], errors="coerce")
        upper = pd.to_numeric(df[upper_column], errors="coerce")
        partial = lower.notna() != upper.notna()
        if bool(partial.any()):
            return [
                f"{lower_column}/{upper_column}: {int(partial.sum())} one-sided bands"
            ]
        bad = lower.notna() & upper.notna() & (lower > upper)
        if bool(bad.any()):
            return [f"{lower_column}/{upper_column}: {int(bad.sum())} inverted bands"]
    return []


def _ratio_mismatch_count(
    df: pd.DataFrame,
    numerator_column: str,
    denominator_column: str,
    ratio_column: str,
    *,
    zero_zero_is_zero: bool = False,
) -> int:
    if {numerator_column, denominator_column, ratio_column}.difference(df.columns):
        return 0
    numerator = pd.to_numeric(df[numerator_column], errors="coerce")
    denominator = pd.to_numeric(df[denominator_column], errors="coerce")
    ratio = pd.to_numeric(df[ratio_column], errors="coerce")
    mismatches = 0
    for numerator_value, denominator_value, ratio_value in zip(
        numerator,
        denominator,
        ratio,
        strict=True,
    ):
        if (
            pd.isna(numerator_value)
            or pd.isna(denominator_value)
            or pd.isna(ratio_value)
        ):
            continue
        denominator_float = float(denominator_value)
        numerator_float = float(numerator_value)
        if denominator_float == 0.0:
            if zero_zero_is_zero and numerator_float == 0.0:
                expected = 0.0
            else:
                mismatches += 1
                continue
        else:
            expected = numerator_float / denominator_float
        if not math.isclose(float(ratio_value), expected, rel_tol=1e-9, abs_tol=1e-9):
            mismatches += 1
    return mismatches


def _nonnegative_errors(df: pd.DataFrame, columns: Iterable[str]) -> list[str]:
    errors: list[str] = []
    for column in columns:
        if column not in df.columns:
            continue
        values = pd.to_numeric(df[column], errors="coerce")
        bad = values.notna() & (values < 0)
        if bool(bad.any()):
            errors.append(f"{column}: {int(bad.sum())} negative values")
    return errors


def _numeric_column(df: pd.DataFrame, column: str) -> pd.Series | None:
    if column not in df.columns:
        return None
    return pd.to_numeric(df[column], errors="coerce")


def _quality_flag_series(df: pd.DataFrame) -> pd.Series:
    if "quality_flags" in df.columns:
        return df["quality_flags"]
    return pd.Series([[] for _ in range(len(df))], index=df.index)


def _parse_floatish(value: Any) -> float | None:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)


def _flags(value: Any) -> tuple[str, ...]:
    parsed = _parse_string_list(value)
    return tuple(parsed or ())


def _json_or_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return _parse_string_list(text) or []
        return parsed if isinstance(parsed, list) else []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    if hasattr(value, "tolist"):
        converted = value.tolist()
        return list(converted) if isinstance(converted, list) else []
    return []


def _has_severe_flag(value: Any) -> bool:
    flags = set(_flags(value))
    return bool(
        flags
        & {
            "crossed_book",
            "negative_spread",
            "seq_gap",
            "reconnect",
            "no_initial_snapshot",
        }
    )


def _parse_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def _nullable_bool_column(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([None for _ in range(len(df))], index=df.index)
    return df[column].map(_parse_bool)


def _parse_string_list(value: Any) -> list[str] | None:
    if value is None:
        return []
    if not isinstance(value, (str, bytes)) and hasattr(value, "tolist"):
        return _parse_string_list(value.tolist())
    if isinstance(value, float) and math.isnan(value):
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                decoded = json.loads(text)
            except json.JSONDecodeError:
                decoded = None
            if isinstance(decoded, list):
                return [str(item) for item in decoded if str(item).strip()]
        separator = ";" if ";" in text else ","
        return [part.strip() for part in text.split(separator) if part.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item).strip()]
    return None


def _string_compatible(value: Any) -> bool:
    if isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, (bytes, bytearray, Mapping, list, tuple, set)):
        return False
    return bool(pd.api.types.is_scalar(value))


def _json_compatible(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return True
    try:
        json.dumps(_json_safe(value))
    except (TypeError, ValueError):
        return False
    return True


def _strict_json_dtype_errors(df: pd.DataFrame, table: TableSpec) -> list[str]:
    errors: list[str] = []
    for field in table.fields:
        if field.dtype != "json" or field.name not in df.columns:
            continue
        invalid = sum(
            1
            for value in df[field.name].tolist()
            if not _strict_json_compatible(value)
        )
        if invalid:
            errors.append(
                f"{field.name}: {invalid} values incompatible with strict json"
            )
    return errors


def _strict_json_compatible(value: Any) -> bool:
    if value is None or _is_missing_scalar(value):
        return True
    if isinstance(value, str):
        try:
            json.loads(value, parse_constant=_reject_nonstandard_json_constant)
        except (json.JSONDecodeError, ValueError):
            return False
        return True
    return _strict_json_native_value(value)


def _strict_json_native_value(value: Any) -> bool:
    if value is None or isinstance(value, (str, bool, int)):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, Mapping):
        return all(
            isinstance(key, str) and _strict_json_native_value(item)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return all(_strict_json_native_value(item) for item in value)
    if not isinstance(value, (str, bytes)) and hasattr(value, "tolist"):
        try:
            return _strict_json_native_value(value.tolist())
        except (TypeError, ValueError):
            return False
    if not isinstance(value, (str, bytes)) and hasattr(value, "item"):
        try:
            return _strict_json_native_value(value.item())
        except (TypeError, ValueError):
            return False
    return False


def _reject_nonstandard_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant {value}")


def _json_string(value: Any) -> str | None:
    if value is None:
        return None
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        missing = False
    if isinstance(missing, bool) and missing:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(_json_safe(value), sort_keys=True)


def _json_safe(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return [_json_safe(item) for item in sorted(value, key=lambda item: str(item))]
    if not isinstance(value, (str, bytes)) and hasattr(value, "tolist"):
        return _json_safe(value.tolist())
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


__all__ = [
    "SchemaValidationReport",
    "coerce_frame",
    "coerce_snapshot_frame",
    "convert_frame_strict",
    "infer_and_validate_frame",
    "quality_flag_counts",
    "validate_frame",
]
