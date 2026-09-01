from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

NUMBER_PATTERN = r"\d[\d,]*(?:\.\d+)?(?:\s*(?:k|m|b)\b)?"


@dataclass(frozen=True)
class Interval:
    low: float | None
    high: float | None
    low_inclusive: bool
    high_inclusive: bool
    kind: str


def _parse_number(raw: str | None) -> float | None:
    if raw is None:
        return None
    cleaned = raw.strip().lower().replace(",", "").replace(" ", "")
    if not cleaned:
        return None
    match = re.fullmatch(r"(-?\d+(?:\.\d+)?)([kmb])?", cleaned)
    if not match:
        return None
    value = float(match.group(1))
    suffix = match.group(2)
    if suffix == "k":
        return value * 1_000.0
    if suffix == "m":
        return value * 1_000_000.0
    if suffix == "b":
        return value * 1_000_000_000.0
    return value


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


def parse_interval(question: str) -> Interval | None:
    if not question:
        return None
    text = question.lower()
    text = (
        text.replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2264", "<=")
        .replace("\u2265", ">=")
    )
    text = _normalize_number_words(text)
    text = text.replace("$", "").replace("%", "")
    text = re.sub(rf"({NUMBER_PATTERN})\s*-\s*({NUMBER_PATTERN})", r"\1 to \2", text)

    between = re.search(
        rf"\bbetween\s+({NUMBER_PATTERN})\s+(?:and|to)\s+({NUMBER_PATTERN})",
        text,
    )
    if between:
        low = _parse_number(between.group(1))
        high = _parse_number(between.group(2))
        if low is not None and high is not None:
            return Interval(low=low, high=high, low_inclusive=True, high_inclusive=True, kind="range")

    from_to = re.search(
        rf"\bfrom\s+({NUMBER_PATTERN})\s+(?:to|through|until)\s+({NUMBER_PATTERN})",
        text,
    )
    if from_to:
        low = _parse_number(from_to.group(1))
        high = _parse_number(from_to.group(2))
        if low is not None and high is not None:
            return Interval(low=low, high=high, low_inclusive=True, high_inclusive=True, kind="range")

    hyphen = re.search(rf"({NUMBER_PATTERN})\s*-\s*({NUMBER_PATTERN})", text)
    if hyphen:
        low = _parse_number(hyphen.group(1))
        high = _parse_number(hyphen.group(2))
        if low is not None and high is not None:
            return Interval(low=low, high=high, low_inclusive=True, high_inclusive=True, kind="range")

    to_range = re.search(
        rf"({NUMBER_PATTERN})\s+(?:to|through|until)\s+({NUMBER_PATTERN})",
        text,
    )
    if to_range:
        low = _parse_number(to_range.group(1))
        high = _parse_number(to_range.group(2))
        if low is not None and high is not None:
            return Interval(low=low, high=high, low_inclusive=True, high_inclusive=True, kind="range")

    lte = re.search(
        rf"(?:<=|at most|no more than|less than or equal to)\s*({NUMBER_PATTERN})",
        text,
    )
    if lte:
        high = _parse_number(lte.group(1))
        if high is not None:
            return Interval(low=None, high=high, low_inclusive=False, high_inclusive=True, kind="lte")

    lt = re.search(
        rf"(?:<|less than|under|below)\s*({NUMBER_PATTERN})",
        text,
    )
    if lt:
        high = _parse_number(lt.group(1))
        if high is not None:
            return Interval(low=None, high=high, low_inclusive=False, high_inclusive=False, kind="lt")

    gte = re.search(
        rf"(?:>=|at least|no less than|greater than or equal to)\s*({NUMBER_PATTERN})",
        text,
    )
    if gte:
        low = _parse_number(gte.group(1))
        if low is not None:
            return Interval(low=low, high=None, low_inclusive=True, high_inclusive=False, kind="gte")

    gte_trailing = re.search(
        rf"({NUMBER_PATTERN})\s*(?:\+|\bor more\b|\bor greater\b|\bor higher\b)",
        text,
    )
    if gte_trailing:
        low = _parse_number(gte_trailing.group(1))
        if low is not None:
            return Interval(low=low, high=None, low_inclusive=True, high_inclusive=False, kind="gte")

    gt = re.search(
        rf"(?:>|more than|greater than|over|above)\s*({NUMBER_PATTERN})",
        text,
    )
    if gt:
        low = _parse_number(gt.group(1))
        if low is not None:
            return Interval(low=low, high=None, low_inclusive=False, high_inclusive=False, kind="gt")

    lte_trailing = re.search(
        rf"({NUMBER_PATTERN})\s*(?:\bor less\b|\bor fewer\b|\bor lower\b)",
        text,
    )
    if lte_trailing:
        high = _parse_number(lte_trailing.group(1))
        if high is not None:
            return Interval(low=None, high=high, low_inclusive=False, high_inclusive=True, kind="lte")

    eq = re.search(
        rf"(?:=|equals|equal to|exactly)\s*({NUMBER_PATTERN})",
        text,
    )
    if eq:
        value = _parse_number(eq.group(1))
        if value is not None:
            return Interval(low=value, high=value, low_inclusive=True, high_inclusive=True, kind="eq")

    # NLP Fallback
    from pmkt.market_structure.nlp import extract_bounds_heuristic
    fallback_res = extract_bounds_heuristic(question)
    if fallback_res:
        kind = fallback_res["kind"]
        return Interval(
            low=fallback_res.get("low"),
            high=fallback_res.get("high"),
            low_inclusive=True, # Heuristic is simplified, assume inclusive for now
            high_inclusive=True,
            kind=kind
        )

    return None


def canonical_signature(question: str) -> str:
    if not question:
        return ""
    text = question.lower()
    text = (
        text.replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2264", "<=")
        .replace("\u2265", ">=")
    )
    text = _normalize_number_words(text)
    text = text.replace("$", "").replace("%", "")
    text = re.sub(rf"({NUMBER_PATTERN})\s*-\s*({NUMBER_PATTERN})", r"\1 to \2", text)

    text = re.sub(r"\bless than or equal to\b", " lte ", text)
    text = re.sub(r"\bat most\b", " lte ", text)
    text = re.sub(r"\bno more than\b", " lte ", text)
    text = re.sub(r"\bless than\b", " lt ", text)
    text = re.sub(r"\bunder\b", " lt ", text)
    text = re.sub(r"\bbelow\b", " lt ", text)
    text = re.sub(r"\bgreater than or equal to\b", " gte ", text)
    text = re.sub(r"\bat least\b", " gte ", text)
    text = re.sub(r"\bno less than\b", " gte ", text)
    text = re.sub(r"\bgreater than\b", " gt ", text)
    text = re.sub(r"\bmore than\b", " gt ", text)
    text = re.sub(r"\bover\b", " gt ", text)
    text = re.sub(r"\babove\b", " gt ", text)
    text = re.sub(r"<=", " lte ", text)
    text = re.sub(r">=", " gte ", text)
    text = re.sub(r"<", " lt ", text)
    text = re.sub(r">", " gt ", text)

    text = re.sub(NUMBER_PATTERN, " num ", text)
    text = text.replace("-", " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)

    text = re.sub(r"\bbetween\b\s+num\s+(?:and|to)\s+num\b", " range num ", text)
    text = re.sub(r"\bnum\b\s+(?:to|and)\s+num\b", " range num ", text)
    text = re.sub(r"\b(lt|lte|gt|gte)\b\s+num\b", " range num ", text)
    text = re.sub(r"\brange\b\s+num(?:\s+num)?\b", " range num ", text)

    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\bnum\b", "NUM", text)
    return text


def _extract_event_id(market: dict[str, Any]) -> str | None:
    for key in ("event_id", "eventId", "eventID"):
        value = market.get(key)
        if value is not None:
            return str(value)
    event = market.get("event")
    if isinstance(event, dict):
        for key in ("id", "event_id", "eventId"):
            value = event.get(key)
            if value is not None:
                return str(value)
    events = market.get("events")
    if isinstance(events, list) and events:
        first = events[0]
        if isinstance(first, dict):
            for key in ("id", "event_id", "eventId"):
                value = first.get(key)
                if value is not None:
                    return str(value)
    return None


def _extract_close_ts(market: dict[str, Any]) -> float | None:
    keys = (
        "close_ts",
        "close_time",
        "closeTime",
        "closeDate",
        "close_date",
        "endDate",
        "endDateIso",
        "end_date",
        "endTime",
        "end_time",
        "resolutionTime",
        "resolution_time",
        "resolveTime",
        "resolve_time",
        "expiresAt",
        "expires_at",
        "closedTime",
    )
    for key in keys:
        if key in market:
            parsed = _parse_timestamp(market.get(key))
            if parsed is not None:
                return parsed
    event = market.get("event")
    if isinstance(event, dict):
        for key in keys:
            if key in event:
                parsed = _parse_timestamp(event.get(key))
                if parsed is not None:
                    return parsed
    events = market.get("events")
    if isinstance(events, list) and events:
        first = events[0]
        if isinstance(first, dict):
            for key in keys:
                if key in first:
                    parsed = _parse_timestamp(first.get(key))
                    if parsed is not None:
                        return parsed
    return None


def _normalize_number_words(text: str) -> str:
    text = re.sub(
        r"\b(\d[\d,]*(?:\.\d+)?)\s*(million|millions)\b",
        r"\1m",
        text,
    )
    text = re.sub(
        r"\b(\d[\d,]*(?:\.\d+)?)\s*(billion|billions)\b",
        r"\1b",
        text,
    )
    return text


def _close_bucket(close_ts: float | None) -> str | None:
    if close_ts is None:
        return None
    dt = datetime.fromtimestamp(close_ts, tz=timezone.utc)
    return dt.date().isoformat()


def group_key(market: dict[str, Any]) -> tuple[Any, ...]:
    event_id = _extract_event_id(market)
    if event_id:
        return ("event", event_id)
    question = market.get("question") or market.get("title") or market.get("slug") or ""
    signature = canonical_signature(str(question))
    close_ts = _extract_close_ts(market)
    close_bucket = _close_bucket(close_ts)
    return ("signature", signature, close_bucket)


def _stable_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, default=str, ensure_ascii=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _member_token_id(token_ids: Any) -> Any:
    if isinstance(token_ids, list):
        if len(token_ids) == 1:
            return token_ids[0]
        return token_ids
    return token_ids


def _interval_from_member(member: dict[str, Any]) -> Interval | None:
    if "interval_kind" in member:
        kind = member.get("interval_kind")
        if kind and kind != "unknown":
            return Interval(
                low=member.get("interval_low"),
                high=member.get("interval_high"),
                low_inclusive=bool(member.get("interval_low_inclusive")),
                high_inclusive=bool(member.get("interval_high_inclusive")),
                kind=str(kind),
            )
    question = member.get("question") or member.get("title")
    if isinstance(question, str):
        return parse_interval(question)
    return None


def interval_order_key(interval: Interval) -> tuple[int, float]:
    if interval.kind in {"lt", "lte"} or (interval.low is None and interval.high is not None):
        return (0, interval.high if interval.high is not None else float("inf"))
    if interval.low is not None and interval.high is not None:
        return (1, interval.low)
    if interval.kind in {"gt", "gte"} or (interval.low is not None and interval.high is None):
        return (2, interval.low if interval.low is not None else float("inf"))
    return (3, float("inf"))


def order_members_for_ladder(members: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def key(member: dict[str, Any]) -> tuple[int, float]:
        interval = _interval_from_member(member)
        if interval is None:
            return (3, float("inf"))
        return interval_order_key(interval)

    return sorted(members, key=key)


def intervals_disjoint_and_ordered(members: Iterable[Any]) -> bool:
    intervals: list[Interval] = []
    for member in members:
        interval = member if isinstance(member, Interval) else _interval_from_member(member)
        if interval is None:
            return False
        if interval.kind == "unknown":
            return False
        intervals.append(interval)
    if len(intervals) < 2:
        return False
    intervals = sorted(intervals, key=interval_order_key)

    prev = intervals[0]
    for current in intervals[1:]:
        if prev.high is None:
            return False
        if current.low is None:
            return False
        if current.low > prev.high:
            prev = current
            continue
        if current.low == prev.high:
            prev = current
            continue
        return False
    return True


def coverage_looks_complete(members: Iterable[Any]) -> bool:
    intervals: list[Interval] = []
    for member in members:
        interval = member if isinstance(member, Interval) else _interval_from_member(member)
        if interval is None or interval.kind == "unknown":
            return False
        intervals.append(interval)
    if not intervals:
        return False
    has_low_tail = any(interval.low is None and interval.high is not None for interval in intervals)
    has_high_tail = any(interval.high is None and interval.low is not None for interval in intervals)
    return has_low_tail and has_high_tail


def sum_prob_sanity(members: Iterable[dict[str, Any]], p_col: str = "mid_last") -> str:
    values: list[float] = []
    for member in members:
        if not isinstance(member, dict):
            continue
        raw = member.get(p_col)
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if value >= 0:
            values.append(value)
    if not values:
        return "unknown"
    total = sum(values)
    if total < 0.85 or total > 1.15:
        return "warn"
    return "ok"


def _is_threshold_ladder(intervals: list[Interval]) -> bool:
    if len(intervals) < 2:
        return False
    if all(interval.high is not None and interval.low is None for interval in intervals):
        highs = sorted(interval.high for interval in intervals if interval.high is not None)
        return len(set(highs)) == len(highs)
    if all(interval.low is not None and interval.high is None for interval in intervals):
        lows = sorted(interval.low for interval in intervals if interval.low is not None)
        return len(set(lows)) == len(lows)
    return False


def _is_range_ladder(intervals: list[Interval]) -> bool:
    if len(intervals) < 2:
        return False
    if not any(interval.low is not None and interval.high is not None for interval in intervals):
        return False
    return intervals_disjoint_and_ordered(intervals)


def infer_group_type(group_markets: Iterable[dict[str, Any]]) -> str:
    intervals: list[Interval] = []
    has_multi_outcome = False
    has_event = False

    for market in group_markets:
        if not isinstance(market, dict):
            continue
        if _extract_event_id(market):
            has_event = True
        token_ids = market.get("token_ids") or market.get("token_id")
        if isinstance(token_ids, list) and len(token_ids) > 2:
            has_multi_outcome = True
        interval = None
        if "interval_kind" in market:
            interval = _interval_from_member(market)
        if interval is None:
            question = market.get("question") or market.get("title")
            if isinstance(question, str):
                interval = parse_interval(question)
        if interval is not None and interval.kind != "unknown":
            intervals.append(interval)

    if intervals and _is_range_ladder(intervals):
        return "range_ladder"
    if intervals and _is_threshold_ladder(intervals):
        return "threshold_ladder"
    if has_multi_outcome:
        return "multi_outcome"
    if has_event:
        return "event"
    return "misc"


def build_market_groups(markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for market in markets:
        if not isinstance(market, dict):
            continue
        key = group_key(market)
        grouped.setdefault(key, []).append(market)

    records: list[dict[str, Any]] = []
    for key, group_markets in grouped.items():
        event_id = None
        if key and key[0] == "event":
            event_id = key[1]
        if event_id is None:
            for market in group_markets:
                event_id = _extract_event_id(market)
                if event_id:
                    break

        close_values = [
            ts for market in group_markets if (ts := _extract_close_ts(market)) is not None
        ]
        close_ts = min(close_values) if close_values else None

        questions = [
            str(question)
            for market in group_markets
            if (question := (market.get("question") or market.get("title"))) is not None
        ]
        base_question = canonical_signature(questions[0]) if questions else ""

        members: list[dict[str, Any]] = []
        volume_total = 0.0
        liquidity_total = 0.0
        for market in group_markets:
            question = market.get("question") or market.get("title") or ""
            interval = parse_interval(str(question)) if question else None
            token_ids = market.get("token_ids") or market.get("token_id")
            member = {
                "market_id": market.get("market_id") or market.get("id"),
                "slug": market.get("slug"),
                "question": question,
                "token_id": _member_token_id(token_ids),
                "interval_low": interval.low if interval else None,
                "interval_high": interval.high if interval else None,
                "interval_kind": interval.kind if interval else None,
                "interval_low_inclusive": interval.low_inclusive if interval else None,
                "interval_high_inclusive": interval.high_inclusive if interval else None,
            }
            members.append(member)

            for metric_key in ("volume", "liquidity"):
                raw_value = market.get(metric_key)
                if raw_value is None:
                    continue
                try:
                    parsed = float(raw_value)
                except (TypeError, ValueError):
                    continue
                if metric_key == "volume":
                    volume_total += parsed
                else:
                    liquidity_total += parsed

        record = {
            "group_id": _stable_hash(key),
            "group_type": infer_group_type(group_markets),
            "event_id": event_id,
            "base_question": base_question,
            "close_ts": close_ts,
            "member_count": len(members),
            "members": members,
        }
        if volume_total > 0:
            record["volume_sum"] = volume_total
        if liquidity_total > 0:
            record["liquidity_sum"] = liquidity_total
        records.append(record)

    return records
