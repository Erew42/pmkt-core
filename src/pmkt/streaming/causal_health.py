from __future__ import annotations

import json
from collections import defaultdict
from typing import Any, Iterable, Mapping

from pmkt.data.time import parse_utc_timestamp
from pmkt.data.types import parse_bool

CAUSAL_HEALTH_SELECTION_VERSION = "causal-health-selection.v1"
MISSING_DETAIL_FLAG = "missing_causal_instrument_detail"
MALFORMED_DETAIL_FLAG = "malformed_causal_instrument_detail"
STALE_DETAIL_FLAG = "stale_causal_instrument_detail"
INCOMPLETE_DETAIL_FLAG = "incomplete_causal_instrument_detail"
CAUSAL_DETAIL_FAILURE_FLAGS = frozenset(
    {
        MISSING_DETAIL_FLAG,
        MALFORMED_DETAIL_FLAG,
        STALE_DETAIL_FLAG,
        INCOMPLETE_DETAIL_FLAG,
    }
)


def select_causal_health_by_shard(
    rows: Iterable[Mapping[str, Any]],
    *,
    venue: str | None = None,
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[tuple[tuple[Any, ...], Mapping[str, Any]]]] = defaultdict(
        list
    )
    for input_order, row in enumerate(rows):
        if venue is not None and str(row.get("venue") or "") != venue:
            continue
        shard_id = str(row.get("shard_id") or "__default__")
        grouped[shard_id].append((_causal_key(row, input_order), row))
    selected: dict[str, dict[str, Any]] = {}
    for shard_id, candidates in grouped.items():
        ordered = [row for _, row in sorted(candidates, key=lambda item: item[0])]
        latest = dict(ordered[-1])
        detail, failure_flag = _latest_detail(ordered)
        if detail is not None:
            latest["instrument_state_json"] = detail
        else:
            latest["instrument_state_json"] = ""
            _append_quality_flag(latest, failure_flag or MISSING_DETAIL_FLAG)
            latest["invalid_instrument_count"] = max(
                1, _integer(latest.get("invalid_instrument_count"))
            )
            latest["valid_instrument_count"] = 0
        selected[shard_id] = latest
    return selected


def _latest_detail(
    ordered: list[Mapping[str, Any]],
) -> tuple[str | list[Mapping[str, Any]] | None, str | None]:
    for row in reversed(ordered):
        raw = row.get("instrument_state_json")
        if raw is None or raw == "" or raw == []:
            continue
        if isinstance(raw, list):
            decoded = raw
        elif isinstance(raw, str):
            try:
                decoded = json.loads(raw)
            except json.JSONDecodeError:
                return None, MALFORMED_DETAIL_FLAG
        else:
            return None, MALFORMED_DETAIL_FLAG
        if not isinstance(decoded, list) or not all(
            isinstance(item, Mapping) for item in decoded
        ):
            return None, MALFORMED_DETAIL_FLAG
        if not _detail_is_complete(decoded, row=row):
            return None, INCOMPLETE_DETAIL_FLAG
        return raw, None
    return None, None


def _detail_is_complete(
    decoded: list[Mapping[str, Any]], *, row: Mapping[str, Any]
) -> bool:
    states: list[tuple[bool, bool]] = []
    seen: set[str] = set()
    for item in decoded:
        instrument = str(item.get("instrument") or "").strip()
        valid = parse_bool(item.get("valid_state"))
        if not instrument or instrument in seen or valid is None:
            return False
        seen.add(instrument)
        stale = any(
            flag.startswith("stale_")
            for flag in _quality_flags(item.get("quality_flags"))
        )
        states.append((valid, stale))

    instrument_count = _optional_nonnegative_integer(row, "instrument_count")
    missing_count = _optional_nonnegative_integer(row, "missing_instrument_count")
    if instrument_count is not None:
        if missing_count is None or len(states) + missing_count != instrument_count:
            return False
    derived = {
        "valid_instrument_count": sum(valid and not stale for valid, stale in states),
        "invalid_instrument_count": sum(not valid for valid, _ in states),
        "stale_instrument_count": sum(stale for _, stale in states),
    }
    for field, expected in derived.items():
        declared = _optional_nonnegative_integer(row, field)
        if declared is not None and declared != expected:
            return False
    return True


def _quality_flags(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return tuple(flag for flag in value.replace(",", ";").split(";") if flag)
    if isinstance(value, Iterable) and not isinstance(value, Mapping):
        return tuple(str(flag) for flag in value if str(flag))
    return ()


def _optional_nonnegative_integer(row: Mapping[str, Any], key: str) -> int | None:
    if key not in row or row[key] is None:
        return None
    value = row[key]
    if isinstance(value, bool):
        return -1
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return -1
    return parsed if parsed >= 0 else -1


def _causal_key(row: Mapping[str, Any], input_order: int) -> tuple[Any, ...]:
    parsed = parse_utc_timestamp(row.get("observed_at_utc"))
    timestamp = parsed.timestamp() if parsed is not None else float("-inf")
    return (timestamp, _integer(row.get("local_sequence"), default=-1), input_order)


def _append_quality_flag(row: dict[str, Any], flag: str) -> None:
    raw = row.get("quality_flags")
    if isinstance(raw, str):
        flags = [item for item in raw.replace(",", ";").split(";") if item]
    elif isinstance(raw, Iterable):
        flags = [str(item) for item in raw if str(item)]
    else:
        flags = []
    row["quality_flags"] = sorted(set([*flags, flag]))


def _integer(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


__all__ = [
    "CAUSAL_DETAIL_FAILURE_FLAGS",
    "CAUSAL_HEALTH_SELECTION_VERSION",
    "INCOMPLETE_DETAIL_FLAG",
    "MALFORMED_DETAIL_FLAG",
    "MISSING_DETAIL_FLAG",
    "STALE_DETAIL_FLAG",
    "select_causal_health_by_shard",
]
