from __future__ import annotations

import asyncio
import json
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd

from pmkt.data.canonical import market_resolution_row
from pmkt.data.registry import MARKET_RESOLUTION_COLUMNS, MARKET_RESOLUTION_SCHEMA_VERSION
from pmkt.data.storage.parquet import write_parquet
from pmkt.exchanges.kalshi.client import AsyncKalshiClient
from pmkt.exchanges.polymarket.clob import AsyncClobClient
from pmkt.exchanges.polymarket.gamma import AsyncGammaClient
from pmkt.resolution.evm import PolygonCtfClient
from pmkt.resolution.kalshi import KalshiResolutionResolver
from pmkt.resolution.models import (
    CONFIDENCE_CANONICAL,
    CONFIDENCE_INCONSISTENT,
    CONFIDENCE_METADATA_ONLY,
    CONFIDENCE_PROVISIONAL,
    CONFIDENCE_UNAVAILABLE,
    RESOLVER_VERSION,
    RESULT_TYPE_SCALAR,
    RESULT_TYPE_UNKNOWN,
    STATE_CLOSED_UNRESOLVED,
    STATE_DISPUTED,
    STATE_FINAL,
    STATE_INCONSISTENT,
    STATE_METADATA_ONLY,
    STATE_OPEN,
    STATE_ORACLE_RESOLVED_CHAIN_PENDING,
    STATE_PLATFORM_REPORTED_CHAIN_PENDING,
    STATE_PROVISIONAL,
    STATE_UNAVAILABLE,
    error_record,
)
from pmkt.resolution.polymarket import PolymarketResolutionResolver

POLYMARKET_RESOLUTION_COLUMNS = [
    "market_id",
    "market_key",
    "slug",
    "question",
    "closed",
    "condition_id",
    "question_id",
    "outcome_labels_json",
    "outcome_prices_json",
    "uma_resolution_status",
    "resolved_by",
    "resolution_source",
    "close_time",
    "updated_time",
]

KALSHI_RESOLUTION_COLUMNS = [
    "market_key",
    "ticker",
    "question",
    "status",
    "closed",
    "result",
    "settlement_value_dollars",
    "settlement_ts",
    "expiration_value",
    "is_provisional",
    "rules_primary",
    "rules_secondary",
    "close_time",
    "updated_time",
]

_SNAPSHOT_FRESHNESS_TIMESTAMP_COLUMNS = (
    "observed_at_utc",
    "observed_at",
    "updated_time",
    "updated_at",
    "last_updated",
    "settlement_ts",
)

_WEAK_FINALITY_STATES = {
    STATE_CLOSED_UNRESOLVED,
    STATE_METADATA_ONLY,
    STATE_OPEN,
    STATE_ORACLE_RESOLVED_CHAIN_PENDING,
    STATE_PLATFORM_REPORTED_CHAIN_PENDING,
    STATE_PROVISIONAL,
    STATE_UNAVAILABLE,
}

_WEAK_FINALITY_CONFIDENCES = {
    CONFIDENCE_METADATA_ONLY,
    CONFIDENCE_PROVISIONAL,
    CONFIDENCE_UNAVAILABLE,
}

_LEGACY_UNSAFE_RESOLVER_VERSIONS = frozenset({"market_resolution_resolver.v1"})
_MAX_CARRIED_SOURCE_OBSERVATIONS = 32


def _available_columns(path: Path) -> set[str]:
    import pyarrow.dataset as ds

    return set(ds.dataset(path, format="parquet").schema.names)


def _read_existing_columns(path: Path, columns: list[str]) -> pd.DataFrame:
    available = _available_columns(path)
    selected = [column for column in columns if column in available]
    if not selected:
        return pd.DataFrame()
    return pd.read_parquet(path, columns=selected)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    return bool(missing) if isinstance(missing, bool) else False


def _json_safe_for_sort(value: Any) -> Any:
    if _is_missing(value):
        return None
    if isinstance(value, dict):
        return {str(key): _json_safe_for_sort(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_json_safe_for_sort(item) for item in value]
    if isinstance(value, set):
        return sorted((_json_safe_for_sort(item) for item in value), key=str)
    if not isinstance(value, (str, bytes)) and hasattr(value, "tolist"):
        return _json_safe_for_sort(value.tolist())
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _stable_json(row: dict[str, Any]) -> str:
    cleaned = {str(key): _json_safe_for_sort(value) for key, value in sorted(row.items())}
    return json.dumps(cleaned, sort_keys=True, default=str)


def _stable_snapshot_sort_json(row: dict[str, Any]) -> str:
    return _stable_json({key: value for key, value in row.items() if key != "close_time"})


def _parsed_timestamp_ns(value: Any) -> int:
    if _is_missing(value):
        return -1
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return -1
    return int(parsed.value)


def _latest_row_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    freshness_timestamps = tuple(
        _parsed_timestamp_ns(row.get(column)) for column in _SNAPSHOT_FRESHNESS_TIMESTAMP_COLUMNS
    )
    return (
        max(freshness_timestamps, default=-1),
        freshness_timestamps,
        _stable_snapshot_sort_json(row),
    )


def _unique_market_rows(df: pd.DataFrame, key_column: str) -> list[dict[str, Any]]:
    if df.empty or key_column not in df.columns:
        return []
    latest_by_key: dict[str, dict[str, Any]] = {}
    for row in df.dropna(subset=[key_column]).to_dict("records"):
        key = str(row.get(key_column))
        current = latest_by_key.get(key)
        if current is None or _latest_row_sort_key(row) > _latest_row_sort_key(current):
            latest_by_key[key] = row
    return [latest_by_key[key] for key in sorted(latest_by_key)]


def _matches_universe(
    matches_path: Path | None,
) -> tuple[set[str] | None, set[str] | None]:
    if matches_path is None:
        return None, None
    match_df = pd.read_parquet(matches_path)
    polymarket_column = next(
        (
            column
            for column in (
                "polymarket_market_key",
                "polymarket_venue_market_id",
                "market_id",
            )
            if column in match_df.columns
        ),
        None,
    )
    kalshi_column = next(
        (
            column
            for column in ("kalshi_market_key", "kalshi_venue_market_id", "ticker")
            if column in match_df.columns
        ),
        None,
    )
    polymarket_keys = (
        {str(value) for value in match_df[polymarket_column].dropna()}
        if polymarket_column
        else set()
    )
    kalshi_keys = (
        {str(value).split(":")[0] for value in match_df[kalshi_column].dropna()}
        if kalshi_column
        else set()
    )
    return polymarket_keys, kalshi_keys


def _filter_rows(
    rows: list[dict[str, Any]], key_column: str, keys: set[str] | None
) -> list[dict[str, Any]]:
    if keys is None:
        return rows
    return [row for row in rows if str(row.get(key_column)) in keys]


def _pair(platform: str, market_key: Any) -> tuple[str, str]:
    return platform, str(market_key)


def _row_pair(row: dict[str, Any]) -> tuple[str, str]:
    return _pair(str(row.get("platform")), row.get("market_key"))


def _requested_pairs(
    polymarket_rows: list[dict[str, Any]],
    kalshi_rows: list[dict[str, Any]],
) -> set[tuple[str, str]]:
    pairs = {_pair("polymarket", row.get("market_id")) for row in polymarket_rows}
    pairs.update(_pair("kalshi", row.get("market_key")) for row in kalshi_rows)
    return pairs


def _existing_current_rows(
    output_path: Path,
    requested_pairs: set[tuple[str, str]],
) -> dict[tuple[str, str], dict[str, Any]]:
    if not output_path.exists() or not requested_pairs:
        return {}
    existing = pd.read_parquet(output_path)
    if existing.empty:
        return {}
    if "resolver_version" not in existing.columns:
        return {}
    current_version = existing["resolver_version"] == RESOLVER_VERSION
    canonical_final = (existing["resolution_state"] == STATE_FINAL) & (
        existing["confidence"] == CONFIDENCE_CANONICAL
    )
    cache_conflict = existing["resolution_state"] == STATE_INCONSISTENT
    if "error_type" in existing.columns:
        cache_conflict &= existing["error_type"] == "MarketResolutionCacheConflict"
    rows = [
        row
        for row in existing[current_version & (canonical_final | cache_conflict)].to_dict("records")
        if _row_pair(row) in requested_pairs
    ]
    latest_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        pair = _row_pair(row)
        current = latest_by_pair.get(pair)
        if current is None or _latest_row_sort_key(row) > _latest_row_sort_key(current):
            latest_by_pair[pair] = row
    return latest_by_pair


def _existing_legacy_unsafe_rows(
    output_path: Path,
    requested_pairs: set[tuple[str, str]],
) -> dict[tuple[str, str], dict[str, Any]]:
    if not output_path.exists() or not requested_pairs:
        return {}
    existing = pd.read_parquet(output_path)
    if existing.empty or "resolver_version" not in existing.columns:
        return {}
    legacy_version = existing["resolver_version"].isin(_LEGACY_UNSAFE_RESOLVER_VERSIONS)
    canonical_final = (existing["resolution_state"] == STATE_FINAL) & (
        existing["confidence"] == CONFIDENCE_CANONICAL
    )
    rows = [
        row
        for row in existing[legacy_version & canonical_final].to_dict("records")
        if _row_pair(row) in requested_pairs
    ]
    latest_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        pair = _row_pair(row)
        current = latest_by_pair.get(pair)
        if current is None or _latest_row_sort_key(row) > _latest_row_sort_key(current):
            latest_by_pair[pair] = row
    return latest_by_pair


def _decode_observations(value: Any) -> list[dict[str, Any]]:
    if _is_missing(value):
        return []
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return []
        value = decoded
    if not isinstance(value, (str, bytes)) and hasattr(value, "tolist"):
        value = value.tolist()
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def _bounded_observations(
    *observation_groups: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    seen: set[str] = set()
    for group in observation_groups:
        for observation in group:
            key = _stable_json(observation)
            if key in seen:
                continue
            seen.add(key)
            observations.append(observation)
    return observations[-_MAX_CARRIED_SOURCE_OBSERVATIONS:]


def _is_weaker_than_current_final(row: dict[str, Any]) -> bool:
    state = str(row.get("resolution_state"))
    confidence = str(row.get("confidence"))
    if state in {STATE_DISPUTED, STATE_INCONSISTENT}:
        return False
    return (
        state != STATE_FINAL
        or confidence != CONFIDENCE_CANONICAL
        or confidence in _WEAK_FINALITY_CONFIDENCES
        or state in _WEAK_FINALITY_STATES
    )


def _is_canonical_final(row: dict[str, Any]) -> bool:
    return (
        str(row.get("resolution_state")) == STATE_FINAL
        and str(row.get("confidence")) == CONFIDENCE_CANONICAL
    )


def _field_text(row: dict[str, Any], field: str) -> str | None:
    value = row.get(field)
    if _is_missing(value):
        return None
    text = str(value).strip()
    return text or None


def _decimal_key(value: str | None) -> Decimal | None:
    if value is None:
        return None
    try:
        decimal = Decimal(value)
    except InvalidOperation:
        return None
    return decimal if decimal.is_finite() else None


def _binary_terminal_key(value: str | None) -> tuple[str, str] | None:
    if value is None:
        return None
    normalized = value.lower()
    if normalized in {"yes", "no", "refund"}:
        return ("terminal", normalized)
    return None


def _terminal_key(row: dict[str, Any]) -> tuple[str, Any] | None:
    result_type = str(row.get("result_type") or "")
    if result_type == RESULT_TYPE_SCALAR:
        settlement = _field_text(row, "settlement_value_dollars")
        if settlement:
            settlement_decimal = _decimal_key(settlement)
            return ("settlement", settlement_decimal if settlement_decimal is not None else settlement)
        result = _field_text(row, "result")
        if result:
            result_decimal = _decimal_key(result)
            return ("result", result_decimal if result_decimal is not None else result.lower())
    for field in ("winner", "result"):
        terminal = _binary_terminal_key(_field_text(row, field))
        if terminal is not None:
            return terminal
    payouts = _decode_observations(row.get("payouts_json"))
    if payouts:
        return ("payouts", _stable_json({"payouts": payouts}))
    settlement = _field_text(row, "settlement_value_dollars")
    if settlement:
        settlement_decimal = _decimal_key(settlement)
        return ("settlement", settlement_decimal if settlement_decimal is not None else settlement)
    result = _field_text(row, "result")
    if result:
        result_decimal = _decimal_key(result)
        return ("result", result_decimal if result_decimal is not None else result.lower())
    return None


def _canonical_final_conflict(
    existing_row: dict[str, Any],
    refreshed_row: dict[str, Any],
) -> bool:
    if not (_is_canonical_final(existing_row) and _is_canonical_final(refreshed_row)):
        return False
    existing_key = _terminal_key(existing_row)
    refreshed_key = _terminal_key(refreshed_row)
    return existing_key is not None and refreshed_key is not None and existing_key != refreshed_key


def _is_cache_conflict(row: dict[str, Any]) -> bool:
    return (
        str(row.get("resolution_state")) == STATE_INCONSISTENT
        and str(row.get("error_type")) == "MarketResolutionCacheConflict"
    )


def _inconsistent_cache_conflict(
    existing_row: dict[str, Any],
    refreshed_row: dict[str, Any],
) -> dict[str, Any]:
    platform = str(refreshed_row.get("platform") or existing_row.get("platform"))
    market_key = str(refreshed_row.get("market_key") or existing_row.get("market_key"))
    observations = [
        *_decode_observations(existing_row.get("source_observations_json")),
        *_decode_observations(refreshed_row.get("source_observations_json")),
    ]
    return market_resolution_row(
        platform=platform,
        market_key=market_key,
        input_identifier=refreshed_row.get("input_identifier")
        or existing_row.get("input_identifier")
        or market_key,
        resolution_state=STATE_INCONSISTENT,
        result_type=RESULT_TYPE_UNKNOWN,
        confidence=CONFIDENCE_INCONSISTENT,
        source_observations_json=observations,
        observed_at_utc=refreshed_row.get("observed_at_utc") or existing_row.get("observed_at_utc"),
        resolver_version=RESOLVER_VERSION,
        error_type="MarketResolutionCacheConflict",
        error_message=(
            f"Existing canonical result {_terminal_key(existing_row)!r} conflicts with "
            f"refreshed canonical result {_terminal_key(refreshed_row)!r}"
        ),
    )


def _carry_forward_final(
    existing_row: dict[str, Any],
    attempted_row: dict[str, Any],
) -> dict[str, Any]:
    carried = dict(existing_row)
    attempted_observations = _decode_observations(attempted_row.get("source_observations_json"))
    if attempted_observations:
        carried["source_observations_json"] = _bounded_observations(
            _decode_observations(carried.get("source_observations_json")),
            attempted_observations,
        )
    return carried


def _sort_resolution_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: (*_row_pair(row), _latest_row_sort_key(row)))


def _summary(rows: list[dict[str, Any]], *, output_path: Path) -> dict[str, Any]:
    by_platform = Counter(str(row.get("platform")) for row in rows)
    by_state = Counter(str(row.get("resolution_state")) for row in rows)
    by_confidence = Counter(str(row.get("confidence")) for row in rows)
    return {
        "schema_version": MARKET_RESOLUTION_SCHEMA_VERSION,
        "row_count": len(rows),
        "output_path": str(output_path),
        "by_platform": dict(sorted(by_platform.items())),
        "by_resolution_state": dict(sorted(by_state.items())),
        "by_confidence": dict(sorted(by_confidence.items())),
    }


async def resolve_market_resolution_cache(
    *,
    polymarket_markets_path: Path,
    kalshi_markets_path: Path,
    output_dir: Path,
    matches_path: Path | None = None,
    polygon_rpc_url: str | None = None,
    concurrency: int = 8,
    refresh: bool = False,
    max_markets: int | None = None,
    gamma_client: Any | None = None,
    clob_client: Any | None = None,
    kalshi_client: Any | None = None,
    ctf_client: PolygonCtfClient | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "market_resolutions.parquet"
    summary_path = output_dir / "summary.json"

    polymarket_keys, kalshi_keys = _matches_universe(matches_path)

    polymarket_df = _read_existing_columns(
        polymarket_markets_path,
        POLYMARKET_RESOLUTION_COLUMNS,
    )
    kalshi_df = _read_existing_columns(kalshi_markets_path, KALSHI_RESOLUTION_COLUMNS)

    polymarket_rows = _filter_rows(
        _unique_market_rows(polymarket_df, "market_id"),
        "market_id",
        polymarket_keys,
    )
    kalshi_rows = _filter_rows(
        _unique_market_rows(kalshi_df, "market_key"),
        "market_key",
        kalshi_keys,
    )
    if max_markets is not None:
        polymarket_rows = polymarket_rows[:max_markets]
        kalshi_rows = kalshi_rows[:max_markets]

    requested_pairs = _requested_pairs(polymarket_rows, kalshi_rows)
    existing_current_rows = _existing_current_rows(output_path, requested_pairs)
    existing_legacy_rows = _existing_legacy_unsafe_rows(output_path, requested_pairs)
    existing_merge_rows = {**existing_legacy_rows, **existing_current_rows}
    skip_keys = set() if refresh else set(existing_current_rows)

    owns_gamma = gamma_client is None
    owns_clob = clob_client is None
    owns_kalshi = kalshi_client is None
    owns_ctf = ctf_client is None and polygon_rpc_url is not None
    gamma_client = gamma_client or AsyncGammaClient()
    clob_client = clob_client or AsyncClobClient()
    kalshi_client = kalshi_client or AsyncKalshiClient()
    ctf_client = ctf_client or (
        PolygonCtfClient(polygon_rpc_url) if polygon_rpc_url else None
    )

    polymarket_resolver = PolymarketResolutionResolver(
        gamma_client=gamma_client,
        clob_client=clob_client,
        ctf_client=ctf_client,
    )
    kalshi_resolver = KalshiResolutionResolver(kalshi_client)
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def resolve_one(platform: str, row: dict[str, Any]) -> dict[str, Any]:
        async with semaphore:
            if platform == "polymarket":
                key = str(row["market_id"])
                try:
                    record = await polymarket_resolver.resolve(key, snapshot=row)
                except Exception as exc:
                    record = error_record(
                        platform="polymarket",
                        market_key=key,
                        input_identifier=key,
                        error=exc,
                    )
                return record.to_row()
            key = str(row["market_key"])
            try:
                record = await kalshi_resolver.resolve(key, snapshot=row)
            except Exception as exc:
                record = error_record(
                    platform="kalshi",
                    market_key=key,
                    input_identifier=key,
                    error=exc,
                )
            return record.to_row()

    tasks = []
    for row in polymarket_rows:
        key = ("polymarket", str(row.get("market_id")))
        if key not in skip_keys:
            tasks.append(resolve_one("polymarket", row))
    for row in kalshi_rows:
        key = ("kalshi", str(row.get("market_key")))
        if key not in skip_keys:
            tasks.append(resolve_one("kalshi", row))

    try:
        resolved = await asyncio.gather(*tasks)
    finally:
        if owns_ctf and ctf_client is not None:
            await ctf_client.close()
        if owns_gamma:
            await gamma_client.close()
        if owns_clob:
            await clob_client.close()
        if owns_kalshi:
            await kalshi_client.close()

    rows_by_pair = {
        pair: dict(row)
        for pair, row in existing_current_rows.items()
        if pair in requested_pairs and pair in skip_keys
    }
    for row in resolved:
        pair = _row_pair(row)
        existing_row = existing_merge_rows.get(pair)
        if existing_row is not None:
            if pair in existing_legacy_rows:
                if _canonical_final_conflict(existing_row, row):
                    rows_by_pair[pair] = _inconsistent_cache_conflict(existing_row, row)
                else:
                    rows_by_pair[pair] = row
            elif _is_cache_conflict(existing_row) and str(row.get("resolution_state")) not in {
                STATE_DISPUTED,
                STATE_INCONSISTENT,
            }:
                rows_by_pair[pair] = _carry_forward_final(existing_row, row)
            elif _canonical_final_conflict(existing_row, row):
                rows_by_pair[pair] = _inconsistent_cache_conflict(existing_row, row)
            elif _is_weaker_than_current_final(row):
                rows_by_pair[pair] = _carry_forward_final(existing_row, row)
            else:
                rows_by_pair[pair] = row
        else:
            rows_by_pair[pair] = row
    rows = _sort_resolution_rows(list(rows_by_pair.values()))
    frame = pd.DataFrame(rows, columns=MARKET_RESOLUTION_COLUMNS)
    write_parquet(
        frame,
        output_path,
        schema=MARKET_RESOLUTION_SCHEMA_VERSION,
        coerce=True,
        validate=True,
        strict=True,
    )
    summary = _summary(rows, output_path=output_path)
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8"
    )
    return {**summary, "summary_path": str(summary_path)}


__all__ = [
    "resolve_market_resolution_cache",
]
