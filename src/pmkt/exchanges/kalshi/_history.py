"""Strict private decoding for the normalized Kalshi candle workflow."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
import re
from typing import Literal, Sequence
from zoneinfo import ZoneInfo

from pmkt import __version__
from pmkt._operation import OperationExpiry
from pmkt.errors import InvalidDataError, ResultLimitExceededError
from pmkt.records import (
    CandleHistoryResult,
    CandleOHLC,
    DataIssue,
    HistoryCoverage,
    HistoryQueryWindow,
    KalshiCandle,
    KalshiMarket,
    KalshiMarketRef,
    RequestObservation,
)


KALSHI_CANDLE_INTERPRETATION_ID = "kalshi_market_candles.v1"
KALSHI_DAILY_NOMINAL_INTERPRETATION = (
    "fixed 1440-minute interval whose inferred start is America/New_York midnight"
)
_NEW_YORK = ZoneInfo("America/New_York")
_MISSING = object()
_OHLC = ("open", "high", "low", "close")
_EXPLICIT_OFFSET_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}"
    r"(?P<fraction>\.\d+)?(?P<offset>Z|z|[+-]\d{2}(?::?\d{2})?)$"
)


class UnsupportedCandleLayoutError(InvalidDataError):
    """The selected dataset cannot decode the supplied candle component layout."""


@dataclass(frozen=True)
class CandlePayload:
    payload: object
    dataset: Literal["live", "historical"]
    observation: RequestObservation


@dataclass(frozen=True)
class _Candidate:
    candle: KalshiCandle
    signature: tuple[object, ...]
    occurrence_count: int
    conflicting: bool
    examples: tuple[str, ...]
    request_counts: tuple[tuple[str, int], ...]


def kalshi_candle_identities(
    payload: object, *, market: KalshiMarketRef
) -> tuple[str, ...]:
    rows, supplied_ticker, supplied_series = decode_kalshi_candle_envelope(payload)
    del rows
    if supplied_ticker is not None and supplied_ticker != market.ticker:
        raise InvalidDataError(
            "Kalshi candle ticker mismatch: "
            f"requested {market.ticker!r}, received {supplied_ticker!r}"
        )
    if (
        supplied_series is not None
        and market.series_ticker is not None
        and supplied_series != market.series_ticker
    ):
        raise InvalidDataError(
            "Kalshi candle series mismatch: "
            f"expected {market.series_ticker!r}, received {supplied_series!r}"
        )
    identities: list[str] = []
    if supplied_ticker is not None:
        identities.append(f"ticker={supplied_ticker}")
    if supplied_series is not None:
        identities.append(f"series_ticker={supplied_series}")
    return tuple(identities)


def decode_kalshi_candle_envelope(
    payload: object,
) -> tuple[list[object], str | None, str | None]:
    if not isinstance(payload, dict):
        raise InvalidDataError("Kalshi candle response must be an object")
    if "candlesticks" not in payload or not isinstance(payload["candlesticks"], list):
        raise InvalidDataError("Kalshi candle field 'candlesticks' must be an array")
    supplied_ticker: str | None = None
    if "ticker" in payload and payload["ticker"] is not None:
        value = payload["ticker"]
        if not isinstance(value, str) or not value.strip():
            raise InvalidDataError("Kalshi candle ticker must be a nonempty string")
        supplied_ticker = value
    supplied_series: str | None = None
    if "series_ticker" in payload and payload["series_ticker"] is not None:
        value = payload["series_ticker"]
        if not isinstance(value, str) or not value.strip():
            raise InvalidDataError(
                "Kalshi candle series_ticker must be a nonempty string"
            )
        supplied_series = value
    return payload["candlesticks"], supplied_ticker, supplied_series


def kalshi_event_identities(
    payload: object,
    *,
    event_ticker: str,
    expected_series_ticker: str | None,
) -> tuple[str, ...]:
    series_ticker = decode_kalshi_event_series(
        payload,
        event_ticker=event_ticker,
        expected_series_ticker=expected_series_ticker,
    )
    return (f"event_ticker={event_ticker}", f"series_ticker={series_ticker}")


def decode_kalshi_event_series(
    payload: object,
    *,
    event_ticker: str,
    expected_series_ticker: str | None,
) -> str:
    if not isinstance(payload, dict):
        raise InvalidDataError("Kalshi event response must be an object")
    row = payload.get("event", payload)
    if not isinstance(row, dict):
        raise InvalidDataError("Kalshi event field 'event' must be an object")
    supplied_event = row.get("event_ticker", row.get("ticker", _MISSING))
    if (
        "event_ticker" in row
        and "ticker" in row
        and row["event_ticker"] != row["ticker"]
    ):
        raise InvalidDataError("Kalshi event response has conflicting ticker aliases")
    if not isinstance(supplied_event, str) or not supplied_event.strip():
        raise InvalidDataError("Kalshi event response is missing a valid event ticker")
    if supplied_event != event_ticker:
        raise InvalidDataError(
            "Kalshi event ticker mismatch: "
            f"requested {event_ticker!r}, received {supplied_event!r}"
        )
    series = row.get("series_ticker", _MISSING)
    if not isinstance(series, str) or not series.strip():
        raise InvalidDataError("Kalshi event response is missing a valid series ticker")
    if expected_series_ticker is not None and series != expected_series_ticker:
        raise InvalidDataError(
            "Kalshi event series mismatch: "
            f"expected {expected_series_ticker!r}, received {series!r}"
        )
    return series


def kalshi_cutoff_identities(payload: object) -> tuple[str, ...]:
    cutoff = decode_kalshi_historical_cutoff(payload)
    return (f"market_settled_ts={cutoff.isoformat()}",)


def decode_kalshi_historical_cutoff(payload: object) -> datetime:
    if not isinstance(payload, dict):
        raise InvalidDataError("Kalshi historical cutoff response must be an object")
    if "market_settled_ts" not in payload:
        raise InvalidDataError("Kalshi historical cutoff is missing market_settled_ts")
    return _parse_routing_timestamp(
        payload["market_settled_ts"], "historical cutoff market_settled_ts"
    )


def parse_kalshi_settlement_timestamp(value: object) -> datetime | None:
    if value is None:
        return None
    return _parse_routing_timestamp(value, "market settlement_ts")


def normalize_kalshi_candle_history(
    candle_payloads: Sequence[CandlePayload],
    *,
    market: KalshiMarketRef,
    requested_start_utc: datetime,
    requested_end_utc: datetime,
    period_minutes: Literal[1, 60, 1440],
    requested_source: Literal["auto", "live", "historical"],
    completed_through_utc: datetime,
    historical_cutoff_utc: datetime | None,
    queried_windows: Sequence[HistoryQueryWindow],
    observations: Sequence[RequestObservation],
    max_candles: int,
    invalid_rows: Literal["raise", "report"],
    routing_flags: Sequence[str],
    routing_market: KalshiMarket | None,
    expiry: OperationExpiry,
) -> CandleHistoryResult:
    """Reconcile all valid rows before original-window and completion filtering."""

    if not candle_payloads or not observations:
        raise RuntimeError("candle normalization requires request evidence")
    candidates: dict[tuple[int, int], _Candidate] = {}
    invalid_groups: dict[str, tuple[int, list[str]]] = {}
    raw_rows = 0
    native_payloads: list[dict[str, object]] = []

    for payload_index, source_payload in enumerate(candle_payloads):
        rows, _, _ = decode_kalshi_candle_envelope(source_payload.payload)
        native_payload = deepcopy(source_payload.payload)
        assert isinstance(native_payload, dict)
        native_payloads.append(native_payload)
        for row_index, row in enumerate(rows):
            if raw_rows % 256 == 0:
                expiry.checkpoint()
            raw_rows += 1
            try:
                candle = _parse_candle(
                    row,
                    market=market,
                    period_minutes=period_minutes,
                    dataset=source_payload.dataset,
                )
            except UnsupportedCandleLayoutError:
                raise
            except InvalidDataError as exc:
                if invalid_rows == "raise":
                    raise
                request_id = source_payload.observation.request_id
                count, issue_examples = invalid_groups.setdefault(request_id, (0, []))
                if len(issue_examples) < 20:
                    issue_examples.append(
                        f"payload {payload_index} row {row_index}: {exc}"[:512]
                    )
                invalid_groups[request_id] = (count + 1, issue_examples)
                continue
            key = (int(candle.period_start_utc.timestamp()), candle.native_end_timestamp)
            signature = _candle_signature(candle)
            previous = candidates.get(key)
            if previous is None:
                candidates[key] = _Candidate(
                    candle=candle,
                    signature=signature,
                    occurrence_count=1,
                    conflicting=False,
                    examples=(source_payload.dataset,),
                    request_counts=((source_payload.observation.request_id, 1),),
                )
                continue
            candidate_examples = previous.examples
            label = source_payload.dataset
            if label not in candidate_examples and len(candidate_examples) < 5:
                candidate_examples = (*candidate_examples, label)
            request_counts = dict(previous.request_counts)
            request_id = source_payload.observation.request_id
            request_counts[request_id] = request_counts.get(request_id, 0) + 1
            candidates[key] = _Candidate(
                candle=previous.candle,
                signature=previous.signature,
                occurrence_count=previous.occurrence_count + 1,
                conflicting=previous.conflicting or signature != previous.signature,
                examples=candidate_examples,
                request_counts=tuple(request_counts.items()),
            )

    candles: list[KalshiCandle] = []
    duplicate_rows = 0
    conflicting_rows = 0
    outside_window_rows = 0
    running_rows = 0
    conflict_groups: dict[str, tuple[int, list[str]]] = {}
    for offset, key in enumerate(sorted(candidates)):
        if offset % 256 == 0:
            expiry.checkpoint()
        candidate = candidates[key]
        if candidate.conflicting:
            if invalid_rows == "raise":
                raise InvalidDataError(
                    "Kalshi candle history contains conflicting values at nominal "
                    f"interval {key[0]}-{key[1]}"
                )
            conflicting_rows += candidate.occurrence_count
            for request_id, occurrence_count in candidate.request_counts:
                count, examples = conflict_groups.setdefault(request_id, (0, []))
                if len(examples) < 20:
                    examples.append(
                        (
                            f"interval {key[0]}-{key[1]} across "
                            f"{', '.join(candidate.examples)}"
                        )[:512]
                    )
                conflict_groups[request_id] = (count + occurrence_count, examples)
            continue
        candle = candidate.candle
        if not (
            candle.period_start_utc >= requested_start_utc
            and candle.period_end_utc <= requested_end_utc
        ):
            outside_window_rows += candidate.occurrence_count
            continue
        if candle.period_end_utc > completed_through_utc:
            running_rows += candidate.occurrence_count
            continue
        duplicate_rows += candidate.occurrence_count - 1
        candles.append(candle)

    expiry.checkpoint()
    if len(candles) > max_candles:
        raise ResultLimitExceededError(
            f"Kalshi candle history exceeded max_candles={max_candles}"
        )

    issues: list[DataIssue] = []
    invalid_count = sum(count for count, _ in invalid_groups.values())
    for request_id, (count, examples) in invalid_groups.items():
        issues.append(
            DataIssue(
                code="invalid_row",
                severity="warning",
                request_id=request_id,
                row_locator="candlesticks",
                occurrence_count=count,
                examples=tuple(examples),
            )
        )
    for request_id, (count, examples) in conflict_groups.items():
        issues.append(
            DataIssue(
                code="conflicting_duplicate",
                severity="warning",
                request_id=request_id,
                row_locator="candlesticks",
                field_locator="period/value",
                occurrence_count=count,
                examples=tuple(examples),
            )
        )

    quality_flags = set(routing_flags)
    if outside_window_rows:
        quality_flags.add("excluded_outside_requested_window")
    if running_rows:
        quality_flags.add("excluded_running_period")
    if any(item.dataset == "live" for item in candle_payloads):
        quality_flags.add("live_synthetic_projection_disabled")
    if period_minutes == 1440:
        quality_flags.add("daily_fixed_1440m_start_ny_midnight")
    observed_start = candles[0].period_start_utc if candles else None
    observed_end = candles[-1].period_end_utc if candles else None
    datasets = tuple(dict.fromkeys(item.dataset for item in candle_payloads))
    coverage = HistoryCoverage(
        requested_start_utc=requested_start_utc,
        requested_end_utc=requested_end_utc,
        queried_windows=tuple(queried_windows),
        datasets=datasets,
        observed_start_utc=observed_start,
        observed_end_utc=observed_end,
        requests_complete=True,
        source_completeness="unknown",
        raw_rows=raw_rows,
        accepted_rows=len(candles),
        rejected_rows=invalid_count + conflicting_rows,
        duplicate_rows=duplicate_rows,
        conflicting_rows=conflicting_rows,
        outside_window_rows=outside_window_rows,
        running_rows=running_rows,
        synthetic_rows=0,
    )
    expiry.checkpoint()
    return CandleHistoryResult(
        market=market,
        candles=tuple(candles),
        requested_start_utc=requested_start_utc,
        requested_end_utc=requested_end_utc,
        period_minutes=period_minutes,
        source=requested_source,
        completed_through_utc=completed_through_utc,
        historical_cutoff_utc=historical_cutoff_utc,
        observation=observations[-1],
        observations=tuple(observations),
        issues=tuple(issues),
        interpretation_id=KALSHI_CANDLE_INTERPRETATION_ID,
        package_version=__version__,
        coverage=coverage,
        quality_flags=tuple(sorted(quality_flags)),
        routing_market=routing_market,
        native_payloads=tuple(native_payloads),
    )


def _parse_candle(
    row: object,
    *,
    market: KalshiMarketRef,
    period_minutes: Literal[1, 60, 1440],
    dataset: Literal["live", "historical"],
) -> KalshiCandle:
    if not isinstance(row, dict):
        raise InvalidDataError("Kalshi candle row must be an object")
    _preflight_candle_layout(row, dataset=dataset)
    if dataset == "live":
        traded, mean, previous = _live_price(row.get("price", _MISSING))
        yes_bid = _ohlc(row.get("yes_bid", _MISSING), suffix="_dollars")
        yes_ask = _ohlc(row.get("yes_ask", _MISSING), suffix="_dollars")
        volume = _optional_contracts(row.get("volume_fp", _MISSING), "volume_fp")
        open_interest = _optional_contracts(
            row.get("open_interest_fp", _MISSING), "open_interest_fp"
        )
    else:
        traded, mean, previous = _historical_price(row.get("price", _MISSING))
        yes_bid = _ohlc(row.get("yes_bid", _MISSING), suffix="")
        yes_ask = _ohlc(row.get("yes_ask", _MISSING), suffix="")
        volume = _optional_contracts(row.get("volume", _MISSING), "volume")
        open_interest = _optional_contracts(
            row.get("open_interest", _MISSING), "open_interest"
        )
    end_timestamp = row.get("end_period_ts", _MISSING)
    if isinstance(end_timestamp, bool) or not isinstance(end_timestamp, int):
        raise InvalidDataError("Kalshi candle end_period_ts must be an integer")
    seconds = period_minutes * 60
    try:
        period_end = datetime.fromtimestamp(end_timestamp, tz=timezone.utc)
        period_start = period_end - timedelta(seconds=seconds)
    except (OverflowError, OSError, ValueError) as exc:
        raise InvalidDataError(
            "Kalshi candle end_period_ts is outside the supported range"
        ) from exc
    _validate_alignment(period_end, period_start, period_minutes)

    flags: set[str] = set()
    traded_values = (traded.open, traded.high, traded.low, traded.close)
    if all(value is None for value in traded_values):
        flags.add("no_traded_price_ohlc")
    elif any(value is None for value in traded_values):
        flags.add("partial_traded_price_ohlc")
    native = deepcopy(row)
    assert isinstance(native, dict)
    return KalshiCandle(
        market=market,
        period_start_utc=period_start,
        period_end_utc=period_end,
        native_end_timestamp=end_timestamp,
        period_minutes=period_minutes,
        dataset=dataset,
        traded_price=traded,
        traded_price_mean=mean,
        traded_price_previous=previous,
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        volume_contracts=volume,
        open_interest_contracts=open_interest,
        quality_flags=tuple(sorted(flags)),
        native_payload=native,
    )


def _live_price(value: object) -> tuple[CandleOHLC, float | None, float | None]:
    if not isinstance(value, dict):
        raise UnsupportedCandleLayoutError(
            "Kalshi live candle price must be an object"
        )
    if not value:
        return CandleOHLC(None, None, None, None), None, None
    ohlc = _ohlc(value, suffix="_dollars")
    mean = _optional_probability(value.get("mean_dollars", None), "mean_dollars")
    previous = _optional_probability(
        value.get("previous_dollars", None), "previous_dollars"
    )
    return ohlc, mean, previous


def _historical_price(
    value: object,
) -> tuple[CandleOHLC, float | None, float | None]:
    if not isinstance(value, dict):
        raise UnsupportedCandleLayoutError(
            "Kalshi historical candle price must be an object"
        )
    ohlc = _ohlc(value, suffix="")
    if "mean" not in value or "previous" not in value:
        raise UnsupportedCandleLayoutError(
            "Kalshi historical candle price must supply mean and previous"
        )
    return (
        ohlc,
        _optional_probability(value["mean"], "mean"),
        _optional_probability(value["previous"], "previous"),
    )


def _ohlc(value: object, *, suffix: str) -> CandleOHLC:
    _require_ohlc_layout(value, suffix=suffix)
    assert isinstance(value, dict)
    names = tuple(f"{name}{suffix}" for name in _OHLC)
    parsed = [_optional_probability(value[name], name) for name in names]
    return CandleOHLC(parsed[0], parsed[1], parsed[2], parsed[3])


def _preflight_candle_layout(
    row: dict[object, object], *, dataset: Literal["live", "historical"]
) -> None:
    price = row.get("price", _MISSING)
    if not isinstance(price, dict):
        raise UnsupportedCandleLayoutError(
            f"Kalshi {dataset} candle price must be an object"
        )
    suffix = "_dollars" if dataset == "live" else ""
    if price or dataset == "historical":
        _require_ohlc_layout(price, suffix=suffix)
    if dataset == "historical" and (
        "mean" not in price or "previous" not in price
    ):
        raise UnsupportedCandleLayoutError(
            "Kalshi historical candle price must supply mean and previous"
        )
    _require_ohlc_layout(row.get("yes_bid", _MISSING), suffix=suffix)
    _require_ohlc_layout(row.get("yes_ask", _MISSING), suffix=suffix)
    quantity_fields = (
        ("volume_fp", "open_interest_fp")
        if dataset == "live"
        else ("volume", "open_interest")
    )
    missing = [field for field in quantity_fields if field not in row]
    if missing:
        raise UnsupportedCandleLayoutError(
            "Kalshi candle is missing required " + ", ".join(missing)
        )


def _require_ohlc_layout(value: object, *, suffix: str) -> None:
    if not isinstance(value, dict):
        raise UnsupportedCandleLayoutError(
            "Kalshi candle OHLC component must be an object"
        )
    names = tuple(f"{name}{suffix}" for name in _OHLC)
    if any(name not in value for name in names):
        raise UnsupportedCandleLayoutError(
            "Kalshi candle OHLC component is missing required "
            + ", ".join(name for name in names if name not in value)
        )


def _optional_probability(value: object, field: str) -> float | None:
    if value is None:
        return None
    number = _finite_number(value, field)
    if not 0 <= number <= 1:
        raise InvalidDataError(f"Kalshi candle {field} must be between 0 and 1")
    return number


def _optional_contracts(value: object, field: str) -> float | None:
    if value is _MISSING:
        raise UnsupportedCandleLayoutError(
            f"Kalshi candle is missing required {field}"
        )
    if value is None:
        return None
    number = _finite_number(value, field)
    if number < 0:
        raise InvalidDataError(f"Kalshi candle {field} must be nonnegative")
    return number


def _finite_number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise InvalidDataError(f"Kalshi candle {field} must be numeric")
    try:
        number = float(value)
    except (OverflowError, TypeError, ValueError) as exc:
        raise InvalidDataError(f"Kalshi candle {field} must be numeric") from exc
    if not math.isfinite(number):
        raise InvalidDataError(f"Kalshi candle {field} must be finite")
    return number


def _validate_alignment(
    period_end: datetime, period_start: datetime, period_minutes: int
) -> None:
    if period_minutes in (1, 60):
        if int(period_end.timestamp()) % (period_minutes * 60) != 0:
            raise InvalidDataError(
                f"Kalshi {period_minutes}-minute candle end is not grid-aligned"
            )
        return
    local_start = period_start.astimezone(_NEW_YORK)
    if (
        local_start.hour,
        local_start.minute,
        local_start.second,
        local_start.microsecond,
    ) != (0, 0, 0, 0):
        raise InvalidDataError(
            "Kalshi 1440-minute candle inferred start is not "
            "America/New_York midnight"
        )


def _candle_signature(candle: KalshiCandle) -> tuple[object, ...]:
    return (
        candle.traded_price,
        candle.traded_price_mean,
        candle.traded_price_previous,
        candle.yes_bid,
        candle.yes_ask,
        candle.volume_contracts,
        candle.open_interest_contracts,
        candle.quality_flags,
    )


def _parse_routing_timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise InvalidDataError(f"Kalshi {label} is invalid")
    text = value.strip()
    match = _EXPLICIT_OFFSET_RE.fullmatch(text)
    if match is None:
        raise InvalidDataError(f"Kalshi {label} must have an explicit UTC offset")
    fraction = match.group("fraction")
    if fraction is not None and len(fraction) - 1 > 6:
        raise InvalidDataError(
            f"Kalshi {label} exceeds supported microsecond precision"
        )
    if fraction is not None and len(fraction) - 1 < 6:
        start, end = match.span("fraction")
        text = text[:start] + "." + fraction[1:].ljust(6, "0") + text[end:]
    offset = match.group("offset")
    if offset in ("Z", "z"):
        text = text[:-1] + "+00:00"
    elif len(offset) == 3:
        text = text[: -len(offset)] + offset + ":00"
    elif len(offset) == 5 and ":" not in offset:
        text = text[: -len(offset)] + offset[:3] + ":" + offset[3:]
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise InvalidDataError(f"Kalshi {label} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise InvalidDataError(f"Kalshi {label} must have an explicit UTC offset")
    return parsed.astimezone(timezone.utc)


__all__ = [
    "CandlePayload",
    "KALSHI_CANDLE_INTERPRETATION_ID",
    "KALSHI_DAILY_NOMINAL_INTERPRETATION",
    "decode_kalshi_candle_envelope",
    "decode_kalshi_event_series",
    "decode_kalshi_historical_cutoff",
    "kalshi_candle_identities",
    "kalshi_cutoff_identities",
    "kalshi_event_identities",
    "normalize_kalshi_candle_history",
    "parse_kalshi_settlement_timestamp",
]
