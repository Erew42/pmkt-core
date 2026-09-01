"""Venue catalog collection without execution authority."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Literal

from pmkt.data.canonical import (
    KALSHI_MARKET_SNAPSHOT_COLUMNS,
)
from pmkt.data.normalize_kalshi import (
    normalize_kalshi_market,
)
from pmkt.pagination import normalize_polymarket_cursor

from .fs import (
    _payload_hash,
    _raw_key,
    iso_utc,
    row_timestamp,
    utc_now,
)
from .types import (
    CatalogError,
    CollectionResult,
    FilterAgreementError,
)


_PM_CREATED_KEYS = ("createdAt", "created_at", "created")


_PM_UPDATED_KEYS = ("updatedAt", "updated_at", "updated")


_KX_CREATED_KEYS = ("created_time", "created_ts", "created_at")


_KX_UPDATED_KEYS = ("updated_time", "updated_ts", "updated_at")


def _dedupe_rows(
    rows: Iterable[Mapping[str, Any]], *, venue: str, timestamp_keys: Sequence[str]
) -> list[dict[str, Any]]:
    minimum = datetime.min.replace(tzinfo=timezone.utc)
    selected: dict[str, tuple[datetime, str, dict[str, Any]]] = {}
    for source in rows:
        key = _raw_key(source, venue)
        if not key:
            raise CatalogError(f"{venue} market row has no durable key")
        row = dict(source)
        candidate = (
            row_timestamp(row, timestamp_keys) or minimum,
            _payload_hash(row),
            row,
        )
        previous = selected.get(key)
        if previous is None or candidate[:2] > previous[:2]:
            selected[key] = candidate
    return [selected[key][2] for key in sorted(selected)]


def kalshi_snapshot_dataframe(markets: Sequence[Mapping[str, Any]]) -> Any:
    """Normalize Kalshi markets without crossing the data/exchange boundary."""
    import pandas as pd

    rows = [normalize_kalshi_market(dict(market)) for market in markets]
    frame = pd.DataFrame(rows, columns=KALSHI_MARKET_SNAPSHOT_COLUMNS)
    if not frame.empty:
        frame = frame.sort_values("market_key").reset_index(drop=True)
    return frame


async def _target_polymarket_rows(
    client: Any, rows: Sequence[Mapping[str, Any]]
) -> dict[str, dict[str, Any]]:
    resolved: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = _raw_key(row, "polymarket")
        if not key:
            continue
        target = await client.market(key)
        if not isinstance(target, dict):
            raise CatalogError(f"Polymarket target read for {key} was not an object")
        resolved[key] = target
    return resolved


async def collect_polymarket_discovery(
    client: Any,
    *,
    cutoff: datetime,
    max_pages: int = 10_000,
    retry_delay_seconds: float = 0.0,
) -> CollectionResult:
    """Collect creation-ordered open and closed lanes, failing closed on gaps."""
    all_rows: list[dict[str, Any]] = []
    lane_details: dict[str, Any] = {}
    all_created: list[datetime] = []
    for closed in (False, True):
        lane = "closed" if closed else "open"
        cursor: str | None = None
        seen_cursors: set[str] = set()
        previous_oldest: datetime | None = None
        consecutive_old_pages = 0
        page_count = 0
        raw_count = 0
        retry_requests = 0
        target_reads = 0
        stop_reason = "not_started"
        while True:
            if page_count >= max_pages:
                raise CatalogError(f"Polymarket {lane} lane exceeded {max_pages} pages")
            page: dict[str, Any] | None = None
            page_rows: list[dict[str, Any]] = []
            stamps: list[datetime | None] = []
            for attempt in range(4):
                page = await client.markets_keyset_raw_page(
                    limit=100,
                    after_cursor=cursor,
                    closed=closed,
                    order="createdAt",
                    ascending=False,
                )
                payload = page.get("markets")
                if not isinstance(payload, list) or any(
                    not isinstance(row, dict) for row in payload
                ):
                    raise CatalogError("Polymarket keyset response has invalid markets")
                page_rows = [dict(row) for row in payload]
                stamps = [row_timestamp(row, _PM_CREATED_KEYS) for row in page_rows]
                if all(stamp is not None for stamp in stamps):
                    break
                if attempt < 3:
                    retry_requests += 1
                    if retry_delay_seconds:
                        await asyncio.sleep(retry_delay_seconds)
            missing = [
                row
                for row, stamp in zip(page_rows, stamps, strict=True)
                if stamp is None
            ]
            if missing:
                targeted = await _target_polymarket_rows(client, missing)
                target_reads += len(targeted)
                page_rows = [
                    targeted.get(_raw_key(row, "polymarket"), row) for row in page_rows
                ]
                stamps = [row_timestamp(row, _PM_CREATED_KEYS) for row in page_rows]
            if any(stamp is None for stamp in stamps):
                keys = [_raw_key(row, "polymarket") for row in page_rows]
                raise CatalogError(
                    f"Polymarket {lane} page still has unresolved createdAt: {keys}"
                )
            assert page is not None
            page_count += 1
            raw_count += len(page_rows)
            typed_stamps = [stamp for stamp in stamps if stamp is not None]
            if typed_stamps:
                if typed_stamps != sorted(typed_stamps, reverse=True):
                    raise CatalogError("Polymarket createdAt order is not descending")
                newest = typed_stamps[0]
                oldest = typed_stamps[-1]
                if previous_oldest is not None and newest > previous_oldest:
                    raise CatalogError(
                        "Polymarket createdAt order regressed across page boundary"
                    )
                previous_oldest = oldest
                all_created.extend(typed_stamps)
                if all(stamp < cutoff for stamp in typed_stamps):
                    consecutive_old_pages += 1
                else:
                    consecutive_old_pages = 0
                all_rows.extend(
                    row
                    for row, stamp in zip(page_rows, typed_stamps, strict=True)
                    if stamp >= cutoff
                )
                if consecutive_old_pages >= 2:
                    stop_reason = "two_complete_pages_before_cutoff"
                    break
            next_cursor = normalize_polymarket_cursor(page.get("next_cursor"))
            if not next_cursor:
                stop_reason = "cursor_exhausted"
                break
            if next_cursor == cursor or next_cursor in seen_cursors:
                raise CatalogError("Polymarket keyset cursor repeated")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        lane_details[lane] = {
            "closed": closed,
            "pages": page_count,
            "raw_rows": raw_count,
            "timestamp_retry_requests": retry_requests,
            "target_reads": target_reads,
            "stop_reason": stop_reason,
            "terminal_cursor": cursor,
            "complete": stop_reason
            in {"cursor_exhausted", "two_complete_pages_before_cutoff"},
        }

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in all_rows:
        grouped.setdefault(_raw_key(row, "polymarket"), []).append(row)
    reconciled: list[dict[str, Any]] = []
    targeted_conflicts = 0
    for key, candidates in sorted(grouped.items()):
        by_hash = {_payload_hash(row): row for row in candidates}
        if len(by_hash) == 1:
            reconciled.append(next(iter(by_hash.values())))
            continue
        ranked = [
            (row_timestamp(row, _PM_UPDATED_KEYS), _payload_hash(row), row)
            for row in by_hash.values()
        ]
        if all(stamp is not None for stamp, _digest, _row in ranked):
            ranked.sort(
                key=lambda value: (
                    value[0] or datetime.min.replace(tzinfo=timezone.utc),
                    value[1],
                )
            )
            if ranked[-1][0] != ranked[-2][0]:
                reconciled.append(ranked[-1][2])
                continue
        target = await client.market(key)
        targeted_conflicts += 1
        if (
            not isinstance(target, dict)
            or _raw_key(target, "polymarket") != key
            or row_timestamp(target, _PM_UPDATED_KEYS) is None
        ):
            raise CatalogError(f"Polymarket duplicate conflict for {key} is unresolved")
        reconciled.append(target)
    high = max(all_created, default=cutoff)
    return CollectionResult(
        rows=reconciled,
        high_watermark=max(cutoff, high),
        details={
            "endpoint": "/markets/keyset",
            "request": {"limit": 100, "order": "createdAt", "ascending": False},
            "lanes": lane_details,
            "raw_rows": sum(item["raw_rows"] for item in lane_details.values()),
            "targeted_conflict_reads": targeted_conflicts,
            "complete": all(item["complete"] for item in lane_details.values()),
        },
    )


async def _collect_kalshi_pages(
    client: Any,
    *,
    max_pages: int,
    **params: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cursor: str | None = None
    seen_cursors: set[str] = set()
    rows: list[dict[str, Any]] = []
    pages = 0
    empty_live_pages = 0
    while True:
        if pages >= max_pages:
            raise CatalogError(f"Kalshi collection exceeded {max_pages} pages")
        page = await client.markets_page(limit=1000, cursor=cursor, **params)
        pages += 1
        payload = page.get("markets")
        if not isinstance(payload, list) or any(
            not isinstance(row, dict) for row in payload
        ):
            raise CatalogError("Kalshi response has invalid markets")
        page_rows = [dict(row) for row in payload]
        rows.extend(page_rows)
        next_cursor = str(page.get("cursor") or "").strip() or None
        if next_cursor is None:
            return rows, {
                "pages": pages,
                "raw_rows": len(rows),
                "empty_live_cursor_pages": empty_live_pages,
                "terminal_cursor": None,
                "stop_reason": "cursor_exhausted",
                "complete": True,
            }
        if next_cursor == cursor or next_cursor in seen_cursors:
            raise CatalogError("Kalshi markets cursor repeated")
        if not page_rows:
            empty_live_pages += 1
        seen_cursors.add(next_cursor)
        cursor = next_cursor


async def collect_kalshi_discovery(
    client: Any,
    *,
    cutoff: datetime,
    native_family: Literal["kalshi_conventional", "kalshi_mve"],
    max_pages: int = 10_000,
) -> CollectionResult:
    mve_filter = "only" if native_family == "kalshi_mve" else "exclude"
    rows, details = await _collect_kalshi_pages(
        client,
        max_pages=max_pages,
        status=None,
        mve_filter=mve_filter,
        min_created_ts=int(cutoff.timestamp()),
    )
    created = [row_timestamp(row, _KX_CREATED_KEYS) for row in rows]
    if any(stamp is None for stamp in created):
        missing = [
            _raw_key(row, "kalshi")
            for row, stamp in zip(rows, created, strict=True)
            if stamp is None
        ]
        raise CatalogError(f"Kalshi creation timestamps are unresolved: {missing[:20]}")
    typed = [stamp for stamp in created if stamp is not None]
    deduped = _dedupe_rows(
        rows,
        venue="kalshi",
        timestamp_keys=(*_KX_UPDATED_KEYS, *_KX_CREATED_KEYS),
    )
    details.update(
        {
            "endpoint": "/markets",
            "request": {
                "limit": 1000,
                "status": None,
                "mve_filter": mve_filter,
                "min_created_ts": int(cutoff.timestamp()),
            },
            "native_family": native_family,
        }
    )
    return CollectionResult(
        rows=deduped,
        high_watermark=max([cutoff, *typed]),
        details=details,
    )


async def verify_kalshi_filter_agreement(
    client: Any,
    *,
    cutoff: datetime,
    window_end: datetime | None = None,
    max_pages: int = 10_000,
) -> dict[str, Any]:
    """Verify that Kalshi's MVE filters partition one exhausted recent window."""
    fixed_end = (window_end or utc_now()).astimezone(timezone.utc)
    if fixed_end < cutoff:
        raise ValueError("Kalshi filter-agreement window end precedes its cutoff")
    collected: dict[str, set[str]] = {}
    lane_details: dict[str, Any] = {}
    for name, mve_filter in (("only", "only"), ("exclude", "exclude"), ("all", None)):
        rows, details = await _collect_kalshi_pages(
            client,
            max_pages=max_pages,
            status=None,
            mve_filter=mve_filter,
            min_created_ts=int(cutoff.timestamp()),
            max_created_ts=int(fixed_end.timestamp()),
        )
        collected[name] = {_raw_key(row, "kalshi") for row in rows}
        lane_details[name] = details
    intersection = sorted(collected["only"] & collected["exclude"])
    missing = sorted(collected["all"] - (collected["only"] | collected["exclude"]))
    extra = sorted((collected["only"] | collected["exclude"]) - collected["all"])
    report = {
        "cutoff_utc": iso_utc(cutoff),
        "window_end_utc": iso_utc(fixed_end),
        "only_count": len(collected["only"]),
        "exclude_count": len(collected["exclude"]),
        "unfiltered_count": len(collected["all"]),
        "intersection": intersection,
        "missing": missing,
        "extra": extra,
        "conflicting_keys": sorted(set(intersection + missing + extra)),
        "lanes": lane_details,
        "complete": not (intersection or missing or extra),
    }
    if not report["complete"]:
        raise FilterAgreementError(report)
    return report


async def collect_polymarket_current(
    client: Any, *, max_pages: int = 10_000
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cursor: str | None = None
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    pages = 0
    while True:
        if pages >= max_pages:
            raise CatalogError(f"Polymarket current census exceeded {max_pages} pages")
        page = await client.markets_keyset_raw_page(
            limit=100,
            after_cursor=cursor,
            closed=False,
        )
        pages += 1
        payload = page.get("markets")
        if not isinstance(payload, list) or any(
            not isinstance(row, dict) for row in payload
        ):
            raise CatalogError("Polymarket current response has invalid markets")
        rows.extend(dict(row) for row in payload)
        next_cursor = normalize_polymarket_cursor(page.get("next_cursor"))
        if not next_cursor:
            break
        if next_cursor == cursor or next_cursor in seen:
            raise CatalogError("Polymarket current cursor repeated")
        seen.add(next_cursor)
        cursor = next_cursor
    deduped = _dedupe_rows(
        rows,
        venue="polymarket",
        timestamp_keys=(*_PM_UPDATED_KEYS, *_PM_CREATED_KEYS),
    )
    return deduped, {
        "endpoint": "/markets/keyset",
        "closed": False,
        "pages": pages,
        "raw_rows": len(rows),
        "unique_rows": len(deduped),
        "terminal_cursor": cursor,
        "stop_reason": "cursor_exhausted",
        "complete": True,
    }


async def collect_kalshi_current_family(
    client: Any,
    *,
    native_family: Literal["kalshi_conventional", "kalshi_mve"],
    max_pages: int = 10_000,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    mve_filter = "only" if native_family == "kalshi_mve" else "exclude"
    outputs: dict[str, list[dict[str, Any]]] = {}
    details: dict[str, Any] = {}
    for status in ("open", "unopened", "paused"):
        rows, lane = await _collect_kalshi_pages(
            client,
            max_pages=max_pages,
            status=status,
            mve_filter=mve_filter,
        )
        outputs[status] = _dedupe_rows(
            rows,
            venue="kalshi",
            timestamp_keys=(*_KX_UPDATED_KEYS, *_KX_CREATED_KEYS),
        )
        lane["query_status"] = status
        lane["mve_filter"] = mve_filter
        details[status] = lane
    return outputs, {
        "native_family": native_family,
        "lanes": details,
        "complete": all(lane["complete"] for lane in details.values()),
    }
