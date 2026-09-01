"""
Time policy:
- time_value is derived strictly from question text (by/on/until/between) and stored as ISO date strings.
- close_ts is metadata only; it does not shift time_value (no implicit +1 day).
- when a date omits a year, close_time's year is used and a warning is emitted.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from pmkt.market_structure.grouping import (
    Interval,
    NUMBER_PATTERN,
    canonical_signature,
    interval_order_key,
    parse_interval,
)
from pmkt.data.normalize import extract_close_time, extract_event_id, extract_event_slug

DATE_PATTERN = (
    r"(?:\d{4}[-/]\d{1,2}[-/]\d{1,2}"
    r"|\d{1,2}/\d{1,2}/\d{2,4}"
    r"|(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?"
    r"|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"\s+\d{1,2}(?:st|nd|rd|th)?(?:,?\s*\d{4})?"
    r"|\d{1,2}(?:st|nd|rd|th)?\s+"
    r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?"
    r"|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"(?:,?\s*\d{4})?)"
)

MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


@dataclass(frozen=True)
class TimeAnnotation:
    time_kind: str
    time_value: str | dict[str, str] | None
    time_qualifier: str | None
    cleaned_question: str
    warnings: list[str]


def _stable_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, default=str, ensure_ascii=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _parse_timestamp(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric_ts = float(value)
        if numeric_ts > 1e12:
            numeric_ts /= 1000.0
        return numeric_ts
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        parsed_ts: float | None
        try:
            parsed_ts = float(raw)
        except ValueError:
            parsed_ts = None
        if parsed_ts is not None:
            if parsed_ts > 1e12:
                parsed_ts /= 1000.0
            return parsed_ts
        try:
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    return None


def _year_hint(close_time: Any | None) -> int | None:
    ts = _parse_timestamp(close_time)
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).year


def _parse_numeric_date(match: re.Match[str]) -> date | None:
    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _parse_slash_date(match: re.Match[str]) -> date | None:
    left = int(match.group(1))
    right = int(match.group(2))
    year = int(match.group(3))
    if year < 100:
        year += 2000
    if left > 12:
        day, month = left, right
    else:
        month, day = left, right
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _parse_month_name_date(parts: dict[str, str], *, year_hint: int | None) -> tuple[date | None, str | None]:
    warnings: list[str] = []
    month_name = parts.get("month")
    day_str = parts.get("day")
    year_str = parts.get("year")
    if not month_name or not day_str:
        return None, "date_parse_failed"
    month = MONTHS.get(month_name.lower())
    if month is None:
        return None, "date_parse_failed"
    day = int(day_str)
    year = None
    if year_str:
        year = int(year_str)
    elif year_hint:
        year = year_hint
        warnings.append("year_inferred_from_close_time")
    else:
        return None, "year_missing"
    try:
        parsed = date(year, month, day)
    except ValueError:
        return None, "date_parse_failed"
    warning = warnings[0] if warnings else None
    return parsed, warning


def _parse_date_string(raw: str, *, year_hint: int | None) -> tuple[str | None, str | None]:
    text = raw.strip()
    if not text:
        return None, "date_parse_failed"
    numeric = re.search(r"\b(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b", text)
    if numeric:
        parsed = _parse_numeric_date(numeric)
        if parsed:
            return parsed.isoformat(), None
        return None, "date_parse_failed"
    slash = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b", text)
    if slash:
        parsed = _parse_slash_date(slash)
        if parsed:
            return parsed.isoformat(), None
        return None, "date_parse_failed"
    month_first = re.search(
        r"\b(?P<month>[a-z]{3,9})\s+(?P<day>\d{1,2})(?:st|nd|rd|th)?(?:,?\s*(?P<year>\d{4}))?\b",
        text,
        re.IGNORECASE,
    )
    if month_first:
        parsed, warning = _parse_month_name_date(month_first.groupdict(), year_hint=year_hint)
        if parsed:
            return parsed.isoformat(), warning
        return None, warning
    day_first = re.search(
        r"\b(?P<day>\d{1,2})(?:st|nd|rd|th)?\s+(?P<month>[a-z]{3,9})(?:,?\s*(?P<year>\d{4}))?\b",
        text,
        re.IGNORECASE,
    )
    if day_first:
        parsed, warning = _parse_month_name_date(day_first.groupdict(), year_hint=year_hint)
        if parsed:
            return parsed.isoformat(), warning
        return None, warning
    return None, "date_parse_failed"


def parse_time_annotation(question: str, close_time: Any | None = None) -> TimeAnnotation:
    if not question:
        return TimeAnnotation(
            time_kind="none",
            time_value=None,
            time_qualifier=None,
            cleaned_question=question,
            warnings=[],
        )
    year_hint = _year_hint(close_time)
    warnings: list[str] = []
    cleaned_question = question

    window_patterns = [
        re.compile(
            rf"\b(?P<qualifier>between)\s+(?P<start>{DATE_PATTERN})\s+(?:and|to)\s+(?P<end>{DATE_PATTERN})\b",
            re.IGNORECASE,
        ),
        re.compile(
            rf"\b(?P<qualifier>from)\s+(?P<start>{DATE_PATTERN})\s+(?:to|through|until)\s+(?P<end>{DATE_PATTERN})\b",
            re.IGNORECASE,
        ),
    ]
    for pattern in window_patterns:
        match = pattern.search(question)
        if match:
            start_raw = match.group("start")
            end_raw = match.group("end")
            start_iso, start_warn = _parse_date_string(start_raw, year_hint=year_hint)
            end_iso, end_warn = _parse_date_string(end_raw, year_hint=year_hint)
            if start_warn:
                warnings.append(start_warn)
            if end_warn:
                warnings.append(end_warn)
            cleaned_question = _remove_span(question, match.span())
            if start_iso and end_iso:
                return TimeAnnotation(
                    time_kind="window",
                    time_value={"start": start_iso, "end": end_iso},
                    time_qualifier=match.group("qualifier").lower(),
                    cleaned_question=cleaned_question,
                    warnings=warnings,
                )
            warnings.append("date_parse_failed")
            return TimeAnnotation(
                time_kind="unknown",
                time_value=None,
                time_qualifier=match.group("qualifier").lower(),
                cleaned_question=cleaned_question,
                warnings=_dedupe(warnings),
            )

    deadline_pattern = re.compile(
        rf"\b(?P<qualifier>by|until|before|prior to|through)\s+(?P<date>{DATE_PATTERN})\b",
        re.IGNORECASE,
    )
    match = deadline_pattern.search(question)
    if match:
        date_raw = match.group("date")
        iso_value, warning = _parse_date_string(date_raw, year_hint=year_hint)
        if warning:
            warnings.append(warning)
        cleaned_question = _remove_span(question, match.span())
        if iso_value:
            return TimeAnnotation(
                time_kind="deadline",
                time_value=iso_value,
                time_qualifier=match.group("qualifier").lower(),
                cleaned_question=cleaned_question,
                warnings=_dedupe(warnings),
            )
        warnings.append("date_parse_failed")
        return TimeAnnotation(
            time_kind="unknown",
            time_value=None,
            time_qualifier=match.group("qualifier").lower(),
            cleaned_question=cleaned_question,
            warnings=_dedupe(warnings),
        )

    point_pattern = re.compile(
        rf"\b(?P<qualifier>on|at|as of)\s+(?P<date>{DATE_PATTERN})\b",
        re.IGNORECASE,
    )
    match = point_pattern.search(question)
    if match:
        date_raw = match.group("date")
        iso_value, warning = _parse_date_string(date_raw, year_hint=year_hint)
        if warning:
            warnings.append(warning)
        cleaned_question = _remove_span(question, match.span())
        if iso_value:
            return TimeAnnotation(
                time_kind="point",
                time_value=iso_value,
                time_qualifier=match.group("qualifier").lower(),
                cleaned_question=cleaned_question,
                warnings=_dedupe(warnings),
            )
        warnings.append("date_parse_failed")
        return TimeAnnotation(
            time_kind="unknown",
            time_value=None,
            time_qualifier=match.group("qualifier").lower(),
            cleaned_question=cleaned_question,
            warnings=_dedupe(warnings),
        )

    return TimeAnnotation(
        time_kind="none",
        time_value=None,
        time_qualifier=None,
        cleaned_question=question,
        warnings=[],
    )


def _remove_span(text: str, span: tuple[int, int]) -> str:
    start, end = span
    if start >= end:
        return text
    cleaned = f"{text[:start]} {text[end:]}"
    return re.sub(r"\s+", " ", cleaned).strip()


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output




def _extract_question(market: dict[str, Any]) -> str:
    question = market.get("question") or market.get("title") or market.get("slug") or ""
    return str(question) if question is not None else ""


def _extract_token_ids(market: dict[str, Any]) -> list[str]:
    token_ids = market.get("token_ids")
    if token_ids is None:
        token_ids = market.get("token_id")
    if token_ids is None:
        return []
    if isinstance(token_ids, dict):
        return []
    if isinstance(token_ids, (list, tuple, set)):
        return [str(token) for token in token_ids if token is not None]
    if hasattr(token_ids, "__iter__") and not isinstance(token_ids, (str, bytes)):
        return [str(token) for token in list(token_ids) if token is not None]
    return [str(token_ids)]


def _family_signature(question: str) -> str:
    return canonical_signature(_normalize_range_tokens(question))


def _structure_signature(question: str) -> str:
    return canonical_signature(_normalize_range_tokens(question))


def _normalize_range_tokens(question: str) -> str:
    return re.sub(
        rf"({NUMBER_PATTERN})\s*-\s*({NUMBER_PATTERN})",
        r"\1 to \2",
        question,
        flags=re.IGNORECASE,
    )


def _format_number(value: float | None, *, compact: bool = True) -> str:
    if value is None:
        return "n/a"
    number = float(value)
    abs_value = abs(number)
    if compact and abs_value >= 1_000_000_000:
        return _trim_trailing_zeros(f"{number / 1_000_000_000:.3g}") + "b"
    if compact and abs_value >= 1_000_000:
        return _trim_trailing_zeros(f"{number / 1_000_000:.3g}") + "m"
    if float(number).is_integer():
        return f"{number:,.0f}"
    return f"{number:,.4g}"


def _interval_label(interval: Interval, unit: str | None) -> str:
    if interval.kind in {"lt", "lte"} and interval.high is not None:
        op = "<=" if interval.kind == "lte" else "<"
        return f"{op} {_format_number_with_unit(interval.high, unit)}"
    if interval.kind in {"gt", "gte"} and interval.low is not None:
        op = ">=" if interval.kind == "gte" else ">"
        return f"{op} {_format_number_with_unit(interval.low, unit)}"
    if interval.kind == "eq" and interval.low is not None:
        return f"= {_format_number_with_unit(interval.low, unit)}"
    if interval.low is not None or interval.high is not None:
        return f"{_format_number_with_unit(interval.low, unit)} - {_format_number_with_unit(interval.high, unit)}"
    return "n/a"


def _infer_unit(question: str) -> str:
    lowered = question.lower()
    if "%" in question or "percent" in lowered or "percentage" in lowered:
        return "percent"
    if "bps" in lowered or "basis point" in lowered:
        return "percent"
    if "$" in question or "usd" in lowered or "dollar" in lowered:
        return "usd"
    return "count"


def _interval_kind_consistent(kind: str, low: float | None, high: float | None) -> bool:
    if kind in {"lt", "lte"}:
        return low is None and high is not None
    if kind in {"gt", "gte"}:
        return low is not None and high is None
    if kind == "range":
        return low is not None and high is not None
    if kind == "eq":
        return low is not None and high is not None and low == high
    return True


def _infer_kind_from_bounds(low: float | None, high: float | None) -> str:
    if low is None and high is None:
        return "unknown"
    if low is None:
        return "lt"
    if high is None:
        return "gt"
    if low == high:
        return "eq"
    return "range"


def _interval_payload(interval: Interval, unit: str | None, warnings: list[str]) -> dict[str, Any]:
    low = interval.low
    high = interval.high
    kind = interval.kind
    low_inclusive = interval.low_inclusive if low is not None else None
    high_inclusive = interval.high_inclusive if high is not None else None

    if low is not None and high is not None and low > high:
        warnings.append("interval_bounds_inverted")
        low, high = high, low
        low_inclusive, high_inclusive = high_inclusive, low_inclusive

    if not _interval_kind_consistent(kind, low, high):
        warnings.append("interval_kind_bounds_mismatch")
        kind = _infer_kind_from_bounds(low, high)
    if kind == "eq":
        if low is not None:
            low_inclusive = True
        if high is not None:
            high_inclusive = True

    label_interval = Interval(
        low=low,
        high=high,
        low_inclusive=bool(low_inclusive),
        high_inclusive=bool(high_inclusive),
        kind=kind,
    )

    return {
        "low": low,
        "high": high,
        "low_inclusive": low_inclusive,
        "high_inclusive": high_inclusive,
        "kind": kind,
        "label": _interval_label(label_interval, unit),
        "unit": unit,
    }


def _format_number_with_unit(value: float | None, unit: str | None) -> str:
    if value is None:
        return "n/a"
    if unit == "percent":
        return f"{_format_number(value, compact=False)}%"
    if unit == "usd":
        return f"${_format_number(value, compact=True)}"
    return _format_number(value, compact=True)


def _trim_trailing_zeros(value: str) -> str:
    if "." not in value:
        return value
    value = value.rstrip("0").rstrip(".")
    return value


def _intervals_disjoint_with_tails(intervals: list[Interval]) -> bool:
    if len(intervals) < 2:
        return False
    intervals = sorted(intervals, key=interval_order_key)
    seen_low_tail = False
    seen_high_tail = False
    prev_high = None
    prev_high_inclusive = False
    for index, interval in enumerate(intervals):
        if interval.low is None and interval.high is not None:
            if seen_low_tail or index != 0:
                return False
            seen_low_tail = True
            prev_high = interval.high
            prev_high_inclusive = interval.high_inclusive
            continue
        if interval.high is None and interval.low is not None:
            if seen_high_tail:
                return False
            if _bounds_overlap(prev_high, prev_high_inclusive, interval.low, interval.low_inclusive):
                return False
            seen_high_tail = True
            prev_high = interval.high
            prev_high_inclusive = interval.high_inclusive
            continue
        if interval.low is None or interval.high is None:
            return False
        if _bounds_overlap(prev_high, prev_high_inclusive, interval.low, interval.low_inclusive):
            return False
        prev_high = interval.high
        prev_high_inclusive = interval.high_inclusive
    return True


def _interval_overlap_warning(intervals: list[Interval]) -> bool:
    if len(intervals) < 2:
        return False
    intervals = sorted(intervals, key=interval_order_key)
    prev_high = None
    prev_high_inclusive = False
    for interval in intervals:
        if interval.low is None and interval.high is not None:
            prev_high = interval.high
            prev_high_inclusive = interval.high_inclusive
            continue
        if interval.low is None:
            return True
        if _bounds_overlap(prev_high, prev_high_inclusive, interval.low, interval.low_inclusive):
            return True
        prev_high = interval.high
        prev_high_inclusive = interval.high_inclusive
    return False


def _bounds_overlap(
    prev_high: float | None,
    prev_high_inclusive: bool,
    current_low: float | None,
    current_low_inclusive: bool,
) -> bool:
    if prev_high is None or current_low is None:
        return False
    if current_low < prev_high:
        return True
    return False


def _is_threshold_ladder(intervals: list[Interval]) -> bool:
    if len(intervals) < 2:
        return False
    if all(interval.high is not None and interval.low is None for interval in intervals):
        highs = [interval.high for interval in intervals if interval.high is not None]
        return len(set(highs)) == len(highs)
    if all(interval.low is not None and interval.high is None for interval in intervals):
        lows = [interval.low for interval in intervals if interval.low is not None]
        return len(set(lows)) == len(lows)
    return False


def _structure_confidence(structure_type: str, warnings: list[str]) -> float:
    base = {
        "range_ladder": 0.9,
        "threshold_ladder": 0.8,
        "multi_outcome": 0.7,
        "binary_single": 0.6,
        "misc": 0.4,
    }.get(structure_type, 0.4)
    penalty = 0.05 * len(warnings)
    return max(0.0, min(1.0, base - penalty))


def discover_structures(
    markets: Any,
    *,
    features: Any | None = None,
    markets_path: str | None = None,
    features_path: str | None = None,
    max_families: int | None = None,
    max_members: int | None = None,
) -> dict[str, Any]:
    if hasattr(markets, "to_dict"):
        markets = markets.to_dict(orient="records")
    if not isinstance(markets, list):
        raise ValueError("markets must be a list of dicts or a DataFrame")

    features_map = _latest_features_map(features) if features is not None else {}

    prepared: list[dict[str, Any]] = []
    for market in markets:
        if not isinstance(market, dict):
            continue
        question = _extract_question(market)
        close_time = extract_close_time(market)
        annotation = parse_time_annotation(question, close_time=close_time)
        cleaned_question = annotation.cleaned_question or question
        signature = _family_signature(cleaned_question)
        event_id = extract_event_id(market)
        event_slug = extract_event_slug(market)
        family_key = event_id or event_slug or signature
        prepared.append(
            {
                "market": market,
                "question": question,
                "event_id": event_id,
                "event_slug": event_slug,
                "family_key": family_key,
                "signature": signature,
                "time_kind": annotation.time_kind,
                "time_value": annotation.time_value,
                "time_qualifier": annotation.time_qualifier,
                "time_warnings": annotation.warnings,
                "cleaned_question": cleaned_question,
            }
        )

    grouped_families: dict[str, list[dict[str, Any]]] = {}
    for item in prepared:
        grouped_families.setdefault(str(item["family_key"]), []).append(item)

    family_keys_sorted = sorted(grouped_families.keys())
    if max_families is not None:
        family_keys_sorted = family_keys_sorted[: max_families]

    families: list[dict[str, Any]] = []
    for family_key in family_keys_sorted:
        items = grouped_families[family_key]
        items = sorted(items, key=lambda entry: str(entry["market"].get("market_id") or entry["question"]))
        if max_members is not None:
            items = items[: max_members]

        event_id = None
        event_slug = None
        for entry in items:
            if entry["event_id"]:
                event_id = entry["event_id"]
                break
            if entry.get("event_slug"):
                event_slug = entry["event_slug"]
        if event_slug is None:
            for entry in items:
                if entry.get("event_slug"):
                    event_slug = entry["event_slug"]
                    break

        base_question = ""
        for entry in items:
            if entry["question"]:
                base_question = entry["question"]
                break

        family_id = _stable_hash(("family", family_key))

        slices: dict[tuple[str, str, str | None], list[dict[str, Any]]] = {}
        slice_meta: dict[tuple[str, str, str | None], dict[str, Any]] = {}
        for entry in items:
            time_value_key = json.dumps(entry["time_value"], sort_keys=True, default=str, ensure_ascii=True)
            slice_key = (entry["time_kind"], time_value_key, entry["time_qualifier"])
            slices.setdefault(slice_key, []).append(entry)
            slice_meta[slice_key] = {
                "time_kind": entry["time_kind"],
                "time_value": entry["time_value"],
                "time_qualifier": entry["time_qualifier"],
            }

        time_slices: list[dict[str, Any]] = []
        for slice_key in sorted(slices.keys(), key=lambda key: (key[0], key[1] or "")):
            slice_items = slices[slice_key]
            meta = slice_meta[slice_key]
            structure_groups: dict[str, list[dict[str, Any]]] = {}
            for entry in slice_items:
                structure_key = _structure_signature(entry["cleaned_question"])
                structure_groups.setdefault(structure_key, []).append(entry)

            structures: list[dict[str, Any]] = []
            for structure_key in sorted(structure_groups.keys()):
                group_items = structure_groups[structure_key]
                intervals: list[Interval] = []
                members: list[dict[str, Any]] = []
                warnings: list[str] = []
                has_multi_outcome = False

                for entry in group_items:
                    market = entry["market"]
                    question = entry["question"]
                    token_ids = _extract_token_ids(market)
                    if len(token_ids) > 2:
                        has_multi_outcome = True
                    interval = parse_interval(question)
                    interval_payload = None
                    unit = _infer_unit(base_question or question or "")
                    if interval:
                        intervals.append(interval)
                        interval_payload = _interval_payload(interval, unit, warnings)
                    metadata: dict[str, Any] = {}
                    for key in ("volume", "liquidity"):
                        raw_value = market.get(key)
                        try:
                            metadata[key] = float(raw_value)
                        except (TypeError, ValueError):
                            continue
                    close_ts = _parse_timestamp(extract_close_time(market))
                    if close_ts is not None:
                        metadata["close_ts"] = close_ts
                    token_id = token_ids[0] if len(token_ids) == 1 else None
                    member = {
                        "market_id": market.get("market_id") or market.get("id"),
                        "slug": market.get("slug"),
                        "question": question,
                        "interval": interval_payload,
                        "time_annotation": None,
                    }
                    if token_id:
                        member["token_id"] = token_id
                    else:
                        member["token_ids"] = token_ids
                    if metadata:
                        member["metadata"] = metadata
                    if entry["time_warnings"]:
                        warnings.extend(entry["time_warnings"])
                    if token_id and token_id in features_map:
                        member.setdefault("metadata", {})
                        member["metadata"].update(features_map[token_id])
                    members.append(member)

                structure_type = "misc"
                if has_multi_outcome:
                    structure_type = "multi_outcome"
                elif intervals:
                    if len(intervals) == 1:
                        structure_type = "binary_single"
                    elif _intervals_disjoint_with_tails(intervals):
                        structure_type = "range_ladder"
                        if _interval_overlap_warning(intervals):
                            warnings.append("interval_overlap_detected")
                    elif _is_threshold_ladder(intervals):
                        structure_type = "threshold_ladder"
                    else:
                        structure_type = "misc"
                        if _interval_overlap_warning(intervals):
                            warnings.append("interval_overlap_detected")
                else:
                    structure_type = "binary_single"

                if structure_type == "threshold_ladder" and not _is_threshold_ladder(intervals):
                    warnings.append("non_monotone_thresholds")

                if structure_type in {"range_ladder", "threshold_ladder"}:
                    members = sorted(
                        members,
                        key=lambda member: interval_order_key(parse_interval(member.get("question") or "") or Interval(None, None, False, False, "unknown")),
                    )
                else:
                    members = sorted(
                        members,
                        key=lambda member: str(member.get("market_id") or member.get("question") or ""),
                    )

                if max_members is not None and len(members) > max_members:
                    members = members[: max_members]
                    warnings.append("member_limit_applied")

                unit = _infer_unit(base_question or "")
                axis_spec: dict[str, list[dict[str, Any]]] = {"axes": []}
                if structure_type == "range_ladder":
                    axis_spec["axes"].append(
                        {"name": "range", "kind": "numeric_interval", "unit": unit}
                    )
                elif structure_type == "threshold_ladder":
                    axis_spec["axes"].append(
                        {"name": "threshold", "kind": "numeric_threshold", "unit": unit}
                    )
                elif structure_type == "multi_outcome":
                    axis_spec["axes"].append({"name": "outcome", "kind": "categorical", "unit": None})
                elif structure_type == "binary_single":
                    axis_spec["axes"].append({"name": "binary", "kind": "binary", "unit": None})
                else:
                    axis_spec["axes"].append({"name": "unknown", "kind": "unknown", "unit": None})

                structure_id = _stable_hash(
                    (
                        family_id,
                        meta["time_kind"],
                        meta["time_value"],
                        structure_type,
                        structure_key,
                    )
                )
                warnings = _dedupe(warnings)
                structures.append(
                    {
                        "structure_id": structure_id,
                        "structure_type": structure_type,
                        "axis_spec": axis_spec,
                        "members": members,
                        "confidence": _structure_confidence(structure_type, warnings),
                        "warnings": warnings,
                    }
                )

            slice_id = _stable_hash((family_id, meta["time_kind"], meta["time_value"]))
            time_slices.append(
                {
                    "slice_id": slice_id,
                    "time_kind": meta["time_kind"],
                    "time_value": meta["time_value"],
                    "time_qualifier": meta["time_qualifier"],
                    "member_count": len(slice_items),
                    "structures": structures,
                }
            )

        families.append(
            {
                "family_id": family_id,
                "family_key": family_key,
                "event_id": event_id,
                "event_slug": event_slug,
                "base_question": base_question,
                "member_count": len(items),
                "time_slices": time_slices,
            }
        )

    generated_at = datetime.now(tz=timezone.utc).isoformat()
    artifact: dict[str, Any] = {
        "spec_version": 1,
        "generated_at": generated_at,
        "input_artifacts": {
            "markets_parquet_path": markets_path,
            "markets_row_count": len(markets),
        },
        "structure_types": {
            "range_ladder": "Disjoint numeric intervals forming a distribution.",
            "threshold_ladder": "Monotone numeric thresholds (greater/less than).",
            "multi_outcome": "Single market with multiple categorical outcomes.",
            "binary_single": "Single binary market without ladder structure.",
            "misc": "Unclassified or mixed structure.",
        },
        "families": families,
    }
    if features_path:
        artifact["input_artifacts"]["features_parquet_path"] = features_path
    if features is not None:
        try:
            artifact["input_artifacts"]["features_row_count"] = len(features)
        except TypeError:
            pass
    return artifact


def _latest_features_map(features_df: Any) -> dict[str, dict[str, Any]]:
    if features_df is None or not hasattr(features_df, "columns"):
        return {}
    if "token_id" not in features_df.columns:
        return {}
    import pandas as pd

    df = features_df.copy()
    if "bar_start" in df.columns:
        df["bar_start"] = pd.to_datetime(df["bar_start"], utc=True, errors="coerce")
        df = df.sort_values("bar_start")
        df = df.groupby("token_id", as_index=False).tail(1)
    df = df.dropna(subset=["token_id"])
    df["token_id"] = df["token_id"].astype(str)
    metrics = {}
    for _, row in df.iterrows():
        token_id = str(row["token_id"])
        payload = {}
        for key in ("mid_last", "spread_bps_mean", "rv"):
            if key in row and row[key] is not None:
                try:
                    payload[key] = float(row[key])
                except (TypeError, ValueError):
                    continue
        if payload:
            metrics[token_id] = payload
    return metrics


def structures_to_group_records(artifact: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    groups: list[dict[str, Any]] = []
    members: list[dict[str, Any]] = []

    for family in artifact.get("families", []) or []:
        family_id = family.get("family_id")
        base_question = family.get("base_question")
        event_id = family.get("event_id")
        event_slug = family.get("event_slug")
        family_key = family.get("family_key")
        for slice_item in family.get("time_slices", []) or []:
            slice_id = slice_item.get("slice_id")
            time_kind = slice_item.get("time_kind")
            time_value = slice_item.get("time_value")
            time_value_label = _stringify_time_value(time_value)
            time_qualifier = slice_item.get("time_qualifier")
            for structure in slice_item.get("structures", []) or []:
                structure_id = structure.get("structure_id")
                structure_type = structure.get("structure_type")
                warnings = structure.get("warnings") or []
                confidence = structure.get("confidence")
                raw_members = structure.get("members") or []
                flat_members = [_flatten_member(member) for member in raw_members if isinstance(member, dict)]
                member_count = len(flat_members)

                volume_sum = 0.0
                liquidity_sum = 0.0
                for member in raw_members:
                    if not isinstance(member, dict):
                        continue
                    metadata = member.get("metadata") or {}
                    for key, total_ref in (("volume", "volume"), ("liquidity", "liquidity")):
                        raw_value = metadata.get(key)
                        if raw_value is None:
                            continue
                        try:
                            parsed = float(raw_value)
                        except (TypeError, ValueError):
                            continue
                        if total_ref == "volume":
                            volume_sum += parsed
                        else:
                            liquidity_sum += parsed

                group_record = {
                    "group_id": structure_id,
                    "group_type": structure_type,
                    "family_id": family_id,
                    "slice_id": slice_id,
                    "structure_id": structure_id,
                    "structure_type": structure_type,
                    "family_key": family_key,
                    "event_id": event_id,
                    "event_slug": event_slug,
                    "time_kind": time_kind,
                    "time_value": time_value_label,
                    "time_qualifier": time_qualifier,
                    "base_question": base_question,
                    "member_count": member_count,
                    "members": flat_members,
                    "warnings": warnings,
                    "confidence": confidence,
                }
                if volume_sum > 0:
                    group_record["volume_sum"] = volume_sum
                if liquidity_sum > 0:
                    group_record["liquidity_sum"] = liquidity_sum

                groups.append(group_record)

                for member in flat_members:
                    token_value = member.get("token_id")
                    token_list: list[str | None] = []
                    if isinstance(token_value, list):
                        token_list = [str(token) for token in token_value if token is not None]
                    elif token_value is not None:
                        token_list = [str(token_value)]
                    if not token_list:
                        token_list = [None]
                    for token_id in token_list:
                        members.append(
                            {
                                "family_id": family_id,
                                "slice_id": slice_id,
                                "structure_id": structure_id,
                                "group_id": structure_id,
                                "group_type": structure_type,
                                "time_kind": time_kind,
                                "time_value": time_value_label,
                                "time_qualifier": time_qualifier,
                                "base_question": base_question,
                                "market_id": member.get("market_id"),
                                "slug": member.get("slug"),
                                "question": member.get("question"),
                                "token_id": token_id,
                                "interval_low": member.get("interval_low"),
                                "interval_high": member.get("interval_high"),
                                "interval_kind": member.get("interval_kind"),
                                "interval_low_inclusive": member.get("interval_low_inclusive"),
                                "interval_high_inclusive": member.get("interval_high_inclusive"),
                                "interval_label": member.get("interval_label"),
                                "interval_unit": member.get("interval_unit"),
                            }
                        )
    return groups, members


def _flatten_member(member: dict[str, Any]) -> dict[str, Any]:
    interval = member.get("interval") or {}
    interval_low = interval.get("low") if isinstance(interval, dict) else None
    interval_high = interval.get("high") if isinstance(interval, dict) else None
    interval_low_inclusive = interval.get("low_inclusive") if isinstance(interval, dict) else None
    interval_high_inclusive = interval.get("high_inclusive") if isinstance(interval, dict) else None
    interval_kind = interval.get("kind") if isinstance(interval, dict) else None
    interval_label = interval.get("label") if isinstance(interval, dict) else None
    interval_unit = interval.get("unit") if isinstance(interval, dict) else None

    flat = {
        "market_id": member.get("market_id"),
        "slug": member.get("slug"),
        "question": member.get("question"),
        "token_id": member.get("token_id") if "token_id" in member else member.get("token_ids"),
        "interval_low": interval_low,
        "interval_high": interval_high,
        "interval_kind": interval_kind,
        "interval_low_inclusive": interval_low_inclusive,
        "interval_high_inclusive": interval_high_inclusive,
        "interval_label": interval_label,
        "interval_unit": interval_unit,
    }
    metadata = member.get("metadata")
    if isinstance(metadata, dict):
        for key in ("volume", "liquidity", "close_ts", "mid_last", "spread_bps_mean", "rv"):
            if key in metadata:
                flat[key] = metadata.get(key)
    return flat


def _stringify_time_value(time_value: Any) -> str | None:
    if time_value is None:
        return None
    if isinstance(time_value, str):
        return time_value
    try:
        return json.dumps(time_value, sort_keys=True, ensure_ascii=True, default=str)
    except TypeError:
        return str(time_value)
