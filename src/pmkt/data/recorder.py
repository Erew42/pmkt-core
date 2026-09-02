from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from pmkt.data.books import parse_levels
from pmkt.data.canonical import topbook_capture_gap_row
from pmkt.data.io import append_parquet_segment
from pmkt.data.normalize_books import kalshi_orderbook_to_topbook, polymarket_book_to_topbook
from pmkt.data.registry import arrow_schema, get_table_spec
from pmkt.data.schemas import depth_row
from pmkt.data.time import parse_utc_timestamp
from pmkt.data.time import utc_now_iso
from pmkt.data.types import parse_float
from pmkt.data.validation import coerce_frame, validate_frame


@dataclass(frozen=True)
class TopbookRecorderResult:
    run_id: str
    output_dir: Path
    topbook_rows: int
    depth_rows: int
    gap_rows: int
    cycles: int
    summary: dict[str, Any]


async def record_topbooks(
    relations: pd.DataFrame,
    *,
    polymarket_client: Any,
    kalshi_client: Any,
    out_dir: str | Path,
    run_id: str | None = None,
    interval_s: float = 1.0,
    duration_s: float | None = None,
    max_cycles: int | None = None,
    depth: int | None = None,
    max_stale_seconds: float | None = 30.0,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    utc_now: Callable[[], str] = utc_now_iso,
) -> TopbookRecorderResult:
    if interval_s <= 0:
        raise ValueError("interval_s must be > 0")
    if max_cycles is None and duration_s is None:
        max_cycles = 1
    if max_cycles is not None and max_cycles <= 0:
        raise ValueError("max_cycles must be > 0 when provided")
    if duration_s is not None and duration_s <= 0:
        raise ValueError("duration_s must be > 0 when provided")
    if depth is not None and depth <= 0:
        raise ValueError("depth must be > 0 when provided")
    if max_stale_seconds is not None and max_stale_seconds < 0:
        raise ValueError("max_stale_seconds must be >= 0 when provided")

    pairs = _relation_pairs(relations)
    run_id = run_id or f"topbook-{uuid.uuid4().hex[:12]}"
    output = Path(out_dir)
    output.mkdir(parents=True, exist_ok=True)
    start_mono = time.monotonic()
    next_tick = start_mono
    cycle_index = 0
    topbook_rows = 0
    depth_rows = 0
    gap_rows = 0
    local_sequence = 0

    while True:
        if max_cycles is not None and cycle_index >= max_cycles:
            break
        if duration_s is not None and time.monotonic() - start_mono >= duration_s:
            break
        wait_for = next_tick - time.monotonic()
        if wait_for > 0:
            await sleep(wait_for)

        cycle_id = f"{run_id}-{cycle_index:06d}"
        observed_at_utc = utc_now()
        observed_at_mono = time.monotonic_ns()
        cycle_topbooks: list[dict[str, Any]] = []
        cycle_gaps: list[dict[str, Any]] = []

        pm_books, pm_depths, pm_gaps = await _fetch_polymarket_topbooks(
            polymarket_client,
            pairs,
            run_id=run_id,
            cycle_id=cycle_id,
            cycle_index=cycle_index,
            observed_at_utc=observed_at_utc,
            observed_at_mono=observed_at_mono,
            local_sequence_start=local_sequence,
            depth=depth,
        )
        cycle_topbooks.extend(pm_books)
        cycle_depths: list[dict[str, Any]] = list(pm_depths)
        cycle_gaps.extend(pm_gaps)
        local_sequence += len(pm_books)

        kalshi_books, kalshi_depths, kalshi_gaps = await _fetch_kalshi_topbooks(
            kalshi_client,
            pairs,
            run_id=run_id,
            cycle_id=cycle_id,
            cycle_index=cycle_index,
            observed_at_utc=observed_at_utc,
            observed_at_mono=observed_at_mono,
            local_sequence_start=local_sequence,
            depth=depth,
        )
        cycle_topbooks.extend(kalshi_books)
        cycle_depths.extend(kalshi_depths)
        cycle_gaps.extend(kalshi_gaps)
        local_sequence += len(kalshi_books)
        cycle_gaps.extend(
            _coverage_gap_rows(
                pairs,
                cycle_topbooks,
                run_id=run_id,
                cycle_id=cycle_id,
                cycle_index=cycle_index,
                observed_at_utc=observed_at_utc,
            )
        )
        cycle_gaps.extend(
            _stale_gap_rows(
                cycle_topbooks,
                run_id=run_id,
                cycle_id=cycle_id,
                cycle_index=cycle_index,
                observed_at_utc=observed_at_utc,
                max_stale_seconds=max_stale_seconds,
            )
        )

        topbook_rows += _write_topbook_segments(cycle_topbooks, output)
        depth_rows += _write_depth_segments(cycle_depths, output)
        gap_rows += _write_gap_segments(cycle_gaps, output)
        cycle_index += 1
        next_tick += interval_s

    if gap_rows == 0:
        _write_empty_gap_dataset(output)

    summary = {
        "run_id": run_id,
        "output_dir": str(output),
        "relation_rows": len(relations),
        "tracked_pairs": len(pairs),
        "cycles": cycle_index,
        "topbook_rows": topbook_rows,
        "depth_rows": depth_rows,
        "gap_rows": gap_rows,
    }
    return TopbookRecorderResult(
        run_id=run_id,
        output_dir=output,
        topbook_rows=topbook_rows,
        depth_rows=depth_rows,
        gap_rows=gap_rows,
        cycles=cycle_index,
        summary=summary,
    )


async def _fetch_polymarket_topbooks(
    client: Any,
    pairs: Sequence[Mapping[str, str]],
    *,
    run_id: str,
    cycle_id: str,
    cycle_index: int,
    observed_at_utc: str,
    observed_at_mono: int,
    local_sequence_start: int,
    depth: int | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    depth_rows: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    tokens = sorted({pair["polymarket_token_id"] for pair in pairs if pair["polymarket_token_id"]})
    sequence = local_sequence_start
    for token_id in tokens:
        started = time.monotonic()
        try:
            book = await client.book(token_id)
        except Exception as exc:
            gaps.append(
                _capture_gap_row(
                    run_id=run_id,
                    cycle_id=cycle_id,
                    cycle_index=cycle_index,
                    observed_at_utc=observed_at_utc,
                    exchange="polymarket",
                    instrument_id=token_id,
                    gap_type="fetch_error",
                    status="error",
                    reason=str(exc),
                    source_endpoint="/book",
                    fetch_latency_ms=(time.monotonic() - started) * 1000.0,
                )
            )
            continue
        sequence += 1
        topbook = polymarket_book_to_topbook(
            book,
            token_id=token_id,
            collector_run_id=run_id,
            source="rest_poll",
            received_at_utc=observed_at_utc,
            received_at_monotonic_ns=observed_at_mono,
            local_sequence=sequence,
            raw_event_ref="/book",
        )
        rows.append(topbook)
        if depth is not None:
            depth_rows.extend(
                _polymarket_depth_rows(
                    book,
                    token_id=token_id,
                    collector_run_id=run_id,
                    received_at_utc=observed_at_utc,
                    local_sequence=sequence,
                    max_levels=depth,
                    topbook=topbook,
                )
            )
    return rows, depth_rows, gaps


async def _fetch_kalshi_topbooks(
    client: Any,
    pairs: Sequence[Mapping[str, str]],
    *,
    run_id: str,
    cycle_id: str,
    cycle_index: int,
    observed_at_utc: str,
    observed_at_mono: int,
    local_sequence_start: int,
    depth: int | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    depth_rows: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    tickers = sorted({pair["kalshi_market_key"] for pair in pairs if pair["kalshi_market_key"]})
    sequence = local_sequence_start
    for ticker in tickers:
        started = time.monotonic()
        try:
            orderbook = await client.orderbook(ticker, depth=depth)
        except Exception as exc:
            latency_ms = (time.monotonic() - started) * 1000.0
            relation_pairs = [
                pair for pair in pairs if pair["kalshi_market_key"] == ticker
            ] or [{"kalshi_instrument_key": ticker, "match_id": ""}]
            for pair in relation_pairs:
                gaps.append(
                    _capture_gap_row(
                        run_id=run_id,
                        cycle_id=cycle_id,
                        cycle_index=cycle_index,
                        observed_at_utc=observed_at_utc,
                        exchange="kalshi",
                        venue_market_id=ticker,
                        instrument_id=pair["kalshi_instrument_key"],
                        match_id=pair["match_id"],
                        gap_type="fetch_error",
                        status="error",
                        reason=str(exc),
                        source_endpoint="/markets/{ticker}/orderbook",
                        fetch_latency_ms=latency_ms,
                    )
                )
            continue
        sequence += 1
        topbooks = kalshi_orderbook_to_topbook(
            orderbook,
            market_ticker=ticker,
            collector_run_id=run_id,
            source="rest_poll",
            received_at_utc=observed_at_utc,
            received_at_monotonic_ns=observed_at_mono,
            local_sequence=sequence,
            raw_event_ref="/markets/{ticker}/orderbook",
        )
        rows.extend(topbooks)
        if depth is not None:
            depth_rows.extend(
                _kalshi_depth_rows(
                    orderbook,
                    market_ticker=ticker,
                    collector_run_id=run_id,
                    received_at_utc=observed_at_utc,
                    local_sequence=sequence,
                    max_levels=depth,
                    topbooks=topbooks,
                )
            )
    return rows, depth_rows, gaps


def _relation_pairs(relations: pd.DataFrame) -> list[dict[str, str]]:
    if relations.empty:
        return []
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for _, row in relations.iterrows():
        polymarket_token = _text(row.get("polymarket_token_id")) or _text(
            row.get("polymarket_instrument_key")
        )
        kalshi_instrument = _text(row.get("kalshi_instrument_key"))
        kalshi_market = _text(row.get("kalshi_market_key")) or kalshi_instrument.split(
            ":", 1
        )[0]
        match_id = _text(row.get("match_id")) or "|".join(
            [polymarket_token, kalshi_instrument or kalshi_market]
        )
        key = (match_id, polymarket_token, kalshi_instrument or kalshi_market)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "match_id": match_id,
                "polymarket_token_id": polymarket_token,
                "kalshi_market_key": kalshi_market,
                "kalshi_instrument_key": kalshi_instrument or f"{kalshi_market}:YES",
            }
        )
    return rows


def _coverage_gap_rows(
    pairs: Sequence[Mapping[str, str]],
    topbooks: Sequence[Mapping[str, Any]],
    *,
    run_id: str,
    cycle_id: str,
    cycle_index: int,
    observed_at_utc: str,
) -> list[dict[str, Any]]:
    available = {
        (_text(row.get("exchange")), _text(row.get("instrument_id")))
        for row in topbooks
        if _text(row.get("exchange")) and _text(row.get("instrument_id"))
    }
    rows: list[dict[str, Any]] = []
    for pair in pairs:
        pm_key = pair["polymarket_token_id"]
        kalshi_key = pair["kalshi_instrument_key"]
        if pm_key and ("polymarket", pm_key) not in available:
            rows.append(
                _capture_gap_row(
                    run_id=run_id,
                    cycle_id=cycle_id,
                    cycle_index=cycle_index,
                    observed_at_utc=observed_at_utc,
                    exchange="polymarket",
                    instrument_id=pm_key,
                    match_id=pair["match_id"],
                    gap_type="missing_relation_book",
                    status="missing",
                    reason="no_topbook_row_for_relation_in_cycle",
                    source_endpoint="/book",
                )
            )
        if kalshi_key and ("kalshi", kalshi_key) not in available:
            rows.append(
                _capture_gap_row(
                    run_id=run_id,
                    cycle_id=cycle_id,
                    cycle_index=cycle_index,
                    observed_at_utc=observed_at_utc,
                    exchange="kalshi",
                    venue_market_id=pair["kalshi_market_key"],
                    instrument_id=kalshi_key,
                    match_id=pair["match_id"],
                    gap_type="missing_relation_book",
                    status="missing",
                    reason="no_topbook_row_for_relation_in_cycle",
                    source_endpoint="/markets/{ticker}/orderbook",
                )
            )
    return rows


def _stale_gap_rows(
    topbooks: Sequence[Mapping[str, Any]],
    *,
    run_id: str,
    cycle_id: str,
    cycle_index: int,
    observed_at_utc: str,
    max_stale_seconds: float | None,
) -> list[dict[str, Any]]:
    if max_stale_seconds is None:
        return []
    observed = parse_utc_timestamp(observed_at_utc)
    if observed is None:
        return []
    rows: list[dict[str, Any]] = []
    for topbook in topbooks:
        source_ts = parse_utc_timestamp(topbook.get("exchange_ts_utc")) or parse_utc_timestamp(
            topbook.get("received_at_utc")
        )
        if source_ts is None:
            continue
        age_seconds = (observed - source_ts).total_seconds()
        if age_seconds <= max_stale_seconds:
            continue
        rows.append(
            _capture_gap_row(
                run_id=run_id,
                cycle_id=cycle_id,
                cycle_index=cycle_index,
                observed_at_utc=observed_at_utc,
                exchange=_text(topbook.get("exchange")),
                venue_market_id=_text(topbook.get("venue_market_id")),
                instrument_id=_text(topbook.get("instrument_id")),
                gap_type="stale_book",
                status="stale",
                reason=f"book_timestamp_age_seconds={age_seconds:.3f}",
                source_endpoint=_text(topbook.get("raw_event_ref")) or "unknown",
            )
        )
    return rows


def _polymarket_depth_rows(
    book: Any,
    *,
    token_id: str,
    collector_run_id: str,
    received_at_utc: str,
    local_sequence: int,
    max_levels: int,
    topbook: Mapping[str, Any],
) -> list[dict[str, Any]]:
    payload = _as_dict(book)
    rows: list[dict[str, Any]] = []
    for side, levels in (
        ("bid", sorted(parse_levels(payload.get("bids")), key=lambda level: level.price, reverse=True)),
        ("ask", sorted(parse_levels(payload.get("asks")), key=lambda level: level.price)),
    ):
        cumulative = 0.0
        for index, level in enumerate(levels[:max_levels]):
            size = parse_float(level.size) or 0.0
            cumulative += size
            rows.append(
                depth_row(
                    collector_run_id=collector_run_id,
                    exchange="polymarket",
                    venue_market_id=payload.get("market") or token_id or payload.get("asset_id"),
                    instrument_id=token_id or payload.get("asset_id"),
                    source="rest_poll",
                    received_at_utc=received_at_utc,
                    exchange_ts_utc=topbook.get("exchange_ts_utc"),
                    local_sequence=local_sequence,
                    book_hash=payload.get("hash"),
                    side=side,
                    level_index=index,
                    price_dollars=level.price,
                    size_contracts=size,
                    cumulative_size_contracts=cumulative,
                    is_delta=False,
                    valid_state=topbook.get("valid_state"),
                    quality_flags=topbook.get("quality_flags"),
                )
            )
    return rows


def _kalshi_depth_rows(
    orderbook: Any,
    *,
    market_ticker: str,
    collector_run_id: str,
    received_at_utc: str,
    local_sequence: int,
    max_levels: int,
    topbooks: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    payload = _as_dict(orderbook)
    book_raw = payload.get("orderbook_fp")
    book = book_raw if isinstance(book_raw, dict) else payload
    by_outcome = {_text(row.get("outcome")).upper(): row for row in topbooks}
    rows: list[dict[str, Any]] = []
    for outcome, side, levels in (
        (
            "YES",
            "yes",
            sorted(
                parse_levels(
                    book.get("yes_dollars_fp")
                    or book.get("yes_dollars")
                    or book.get("yes")
                    or []
                ),
                key=lambda level: level.price,
                reverse=True,
            ),
        ),
        (
            "NO",
            "no",
            sorted(
                parse_levels(
                    book.get("no_dollars_fp")
                    or book.get("no_dollars")
                    or book.get("no")
                    or []
                ),
                key=lambda level: level.price,
                reverse=True,
            ),
        ),
    ):
        topbook = by_outcome.get(outcome, {})
        cumulative = 0.0
        for index, level in enumerate(levels[:max_levels]):
            size = parse_float(level.size) or 0.0
            cumulative += size
            rows.append(
                depth_row(
                    collector_run_id=collector_run_id,
                    exchange="kalshi",
                    venue_market_id=market_ticker,
                    instrument_id=f"{market_ticker}:{outcome}",
                    outcome=outcome,
                    source="rest_poll",
                    received_at_utc=received_at_utc,
                    exchange_ts_utc=topbook.get("exchange_ts_utc"),
                    local_sequence=local_sequence,
                    side=side,
                    level_index=index,
                    price_dollars=level.price,
                    size_contracts=size,
                    cumulative_size_contracts=cumulative,
                    is_delta=False,
                    valid_state=topbook.get("valid_state"),
                    quality_flags=topbook.get("quality_flags"),
                )
            )
    return rows


def write_topbook_segments(
    topbooks: pd.DataFrame | Sequence[dict[str, Any]],
    out_dir: str | Path,
) -> int:
    """Append strict topbook.v1 rows using the CR-10.1 partition layout."""
    rows = (
        topbooks.to_dict("records")
        if isinstance(topbooks, pd.DataFrame)
        else list(topbooks)
    )
    return _write_topbook_segments(rows, Path(out_dir))


def _write_topbook_segments(rows: Sequence[dict[str, Any]], out_dir: Path) -> int:
    if not rows:
        return 0
    written = 0
    topbook_spec = get_table_spec("topbook.v1")
    schema = arrow_schema(topbook_spec)
    frame = _strict_frame(rows, "topbook.v1")
    for (exchange, date), group in frame.groupby(
        ["exchange", frame["received_at_utc"].astype(str).str.slice(0, 10)],
        dropna=False,
    ):
        path = out_dir / "topbooks" / f"exchange={exchange}" / f"date={date}"
        records = _records_for_arrow(group)
        append_parquet_segment(path, schema, records)
        written += len(records)
    return written


def _write_depth_segments(rows: Sequence[dict[str, Any]], out_dir: Path) -> int:
    if not rows:
        return 0
    depth_spec = get_table_spec("depth.v1")
    schema = arrow_schema(depth_spec)
    frame = _strict_frame(rows, "depth.v1")
    written = 0
    for (exchange, date), group in frame.groupby(
        ["exchange", frame["received_at_utc"].astype(str).str.slice(0, 10)],
        dropna=False,
    ):
        path = out_dir / "depth" / f"exchange={exchange}" / f"date={date}"
        records = _records_for_arrow(group)
        append_parquet_segment(path, schema, records)
        written += len(records)
    return written


def _write_gap_segments(rows: Sequence[dict[str, Any]], out_dir: Path) -> int:
    if not rows:
        return 0
    gap_spec = get_table_spec("topbook_capture_gap.v1")
    schema = arrow_schema(gap_spec)
    frame = _strict_frame(rows, "topbook_capture_gap.v1")
    for date, group in frame.groupby(frame["observed_at_utc"].astype(str).str.slice(0, 10), dropna=False):
        path = out_dir / "topbook_capture_gaps" / f"date={date}"
        append_parquet_segment(path, schema, _records_for_arrow(group))
    return len(frame)


def _write_empty_gap_dataset(out_dir: Path) -> None:
    gap_spec = get_table_spec("topbook_capture_gap.v1")
    append_parquet_segment(
        out_dir / "topbook_capture_gaps",
        arrow_schema(gap_spec),
        [],
    )


def _strict_frame(rows: Sequence[dict[str, Any]], schema: str) -> pd.DataFrame:
    frame = coerce_frame(pd.DataFrame(rows), schema)
    report = validate_frame(frame, schema, strict=True)
    if not report.ok:
        raise ValueError("; ".join(report.errors))
    return frame


def _records_for_arrow(frame: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in frame.to_dict("records"):
        record: dict[str, Any] = {}
        for key, value in item.items():
            if _is_missing(value):
                record[key] = None
            else:
                record[key] = value
        records.append(record)
    return records


def _is_missing(value: Any) -> bool:
    if isinstance(value, (list, tuple, dict)):
        return False
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _capture_gap_row(
    *,
    run_id: str,
    cycle_id: str,
    cycle_index: int,
    observed_at_utc: str,
    exchange: str,
    gap_type: str,
    status: str,
    reason: str,
    source_endpoint: str,
    venue_market_id: str | None = None,
    instrument_id: str | None = None,
    match_id: str | None = None,
    fetch_latency_ms: float | None = None,
) -> dict[str, Any]:
    return topbook_capture_gap_row(
        run_id=run_id,
        cycle_id=cycle_id,
        cycle_index=cycle_index,
        observed_at_utc=observed_at_utc,
        exchange=exchange,
        venue_market_id=venue_market_id or _venue_market_from_instrument(exchange, instrument_id),
        instrument_id=instrument_id,
        match_id=match_id,
        gap_type=gap_type,
        status=status,
        reason=reason,
        source_endpoint=source_endpoint,
        fetch_latency_ms=fetch_latency_ms,
    )


def _venue_market_from_instrument(exchange: str, instrument_id: str | None) -> str | None:
    if instrument_id is None:
        return None
    if exchange == "kalshi":
        return instrument_id.split(":", 1)[0]
    return instrument_id


def _as_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        value = value.model_dump()
    elif hasattr(value, "dict"):
        value = value.dict()
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


__all__ = [
    "TopbookRecorderResult",
    "record_topbooks",
    "write_topbook_segments",
]
