from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable, Protocol, Sequence

import httpx

from pmkt.data.types import parse_float as _shared_parse_float
from pmkt.data.normalize_books import polymarket_book_to_topbook
from pmkt.data.schemas import TOPBOOK_COLUMNS
from pmkt.data.storage.parquet import write_parquet
from pmkt.data.io import ParquetSink, Sink

DEFAULT_EVENT_SLUG = "us-next-strikes-iran-on"
DEFAULT_BOOK_BATCH_SIZE = 100

logger = logging.getLogger(__name__)


class PolymarketClobProtocol(Protocol):
    async def prices_history(
        self,
        token_id: str,
        *,
        interval: str,
        fidelity: int | None = None,
    ) -> Any: ...

    async def book(self, token_id: str) -> Any: ...

    async def books(self, token_ids: Sequence[str]) -> list[Any]: ...


class GammaClientProtocol(Protocol):
    async def events_page(self, *, limit: int, offset: int) -> list[Any]: ...

    async def markets_page(self, *, limit: int, offset: int) -> list[Any]: ...


def _parse_float(value: Any) -> float | None:
    return _shared_parse_float(value)


def _normalize_token_ids(token_ids: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    for token_id in token_ids:
        token_text = str(token_id).strip()
        if token_text and token_text not in normalized:
            normalized.append(token_text)
    if not normalized:
        raise ValueError("token_ids must contain at least one non-empty token id")
    return normalized


def batched(items: Sequence[str], n: int):
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _iso_from_seconds(value: float) -> str:
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()


def _book_to_dict(book: Any) -> Any:
    if isinstance(book, Exception):
        return book
    if hasattr(book, "model_dump"):
        return book.model_dump()
    if hasattr(book, "dict"):
        return book.dict()
    if isinstance(book, dict):
        return dict(book)
    return book


def _with_request_metadata(book: Any, metadata: dict[str, Any]) -> Any:
    payload = _book_to_dict(book)
    if isinstance(payload, dict):
        payload["_request_metadata"] = metadata
    return payload


def _parse_history_item(item: Any) -> tuple[float, float, float | None] | None:
    if isinstance(item, dict):
        ts = item.get("t") or item.get("timestamp") or item.get("time")
        price = item.get("p") or item.get("price") or item.get("value")
        size = (
            item.get("s")
            or item.get("size")
            or item.get("amount")
            or item.get("volume")
        )
    elif isinstance(item, (list, tuple)) and len(item) >= 2:
        ts = item[0]
        price = item[1]
        size = item[2] if len(item) > 2 else None
    else:
        return None

    ts_parsed = _parse_float(ts)
    price_parsed = _parse_float(price)
    size_parsed = _parse_float(size)
    if ts_parsed is None or price_parsed is None:
        return None
    return ts_parsed, price_parsed, size_parsed


def _extract_history(payload: Any) -> list[Any]:
    if hasattr(payload, "model_dump"):
        payload = payload.model_dump()
    elif hasattr(payload, "dict"):
        payload = payload.dict()

    if not isinstance(payload, dict):
        return []
    history = payload.get("history", [])
    if not isinstance(history, list):
        return []
    return history


async def fetch_trade_history(
    clob: PolymarketClobProtocol,
    token_id: str,
    *,
    interval: str,
    fidelity: int | None = None,
) -> list[dict[str, Any]]:
    payload = await clob.prices_history(token_id, interval=interval, fidelity=fidelity)
    history = _extract_history(payload)
    rows: list[dict[str, Any]] = []
    for item in history:
        parsed = _parse_history_item(item)
        if parsed is None:
            continue
        ts_parsed, price_parsed, size_parsed = parsed
        rows.append(
            {
                "timestamp": ts_parsed,
                "price": price_parsed,
                "size": size_parsed,
            }
        )
    return rows


def trade_history_dataframe(rows: Sequence[dict[str, Any]]):
    import pandas as pd

    df = pd.DataFrame(rows)
    if not df.empty and "timestamp" in df.columns:
        df = df.sort_values("timestamp")
    return df


def order_book_summary_dataframe(summary_csv: str | Path):
    import pandas as pd

    df = pd.read_csv(summary_csv)
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], unit="s", errors="coerce")
    return df


def _matches_slug(payload: dict[str, Any], slug: str) -> bool:
    for key in ("slug", "event_slug", "eventSlug", "market_slug", "marketSlug"):
        value = payload.get(key)
        if isinstance(value, str) and value == slug:
            return True
    return False


async def _scan_gamma_pages(
    fetch_page: Callable[..., Awaitable[list[Any]]],
    slug: str,
    *,
    limit: int,
    max_pages: int,
) -> dict[str, Any] | None:
    offset = 0
    for _ in range(max_pages):
        page = await fetch_page(limit=limit, offset=offset)
        if not page:
            break
        for item in page:
            data = item.model_dump() if hasattr(item, "model_dump") else (item.dict() if hasattr(item, "dict") else item)
            if isinstance(data, dict) and _matches_slug(data, slug):
                return data
        if len(page) < limit:
            break
        offset += limit
    return None


async def find_event_by_slug(
    gamma: GammaClientProtocol,
    slug: str,
    *,
    limit: int = 100,
    max_pages: int = 10,
) -> dict[str, Any]:
    if not slug:
        raise ValueError("slug is required")
    event = await _scan_gamma_pages(
        gamma.events_page, slug, limit=limit, max_pages=max_pages
    )
    if event is not None:
        return event
    market = await _scan_gamma_pages(
        gamma.markets_page, slug, limit=limit, max_pages=max_pages
    )
    if market is not None:
        return market
    raise ValueError(f"No event or market found for slug '{slug}'.")


def _best_price(levels: Any, *, prefer_max: bool) -> float | None:
    if not isinstance(levels, list):
        return None
    prices: list[float] = []
    for level in levels:
        if isinstance(level, dict):
            price = level.get("price")
        elif isinstance(level, (list, tuple)) and level:
            price = level[0]
        else:
            continue
        parsed = _parse_float(price)
        if parsed is not None:
            prices.append(parsed)
    if not prices:
        return None
    return max(prices) if prefer_max else min(prices)


def _best_bid_ask(book: dict[str, Any]) -> tuple[float | None, float | None]:
    return (
        _best_price(book.get("bids"), prefer_max=True),
        _best_price(book.get("asks"), prefer_max=False),
    )


# Import removed because metrics generation is decoupled from storage

async def fetch_order_books(
    clob: PolymarketClobProtocol,
    token_ids: Sequence[str],
    *,
    poll_interval_s: float,
    max_snapshots: int | None = None,
    duration_s: float | None = None,
    batch_size: int | None = DEFAULT_BOOK_BATCH_SIZE,
) -> AsyncIterator[tuple[float, str, dict[str, Any] | Exception]]:
    """
    Generator that yields (timestamp, token_id, book_or_exception).
    """
    token_id_list = _normalize_token_ids(token_ids)
    if poll_interval_s <= 0:
        raise ValueError("poll_interval_s must be > 0")
    if batch_size is not None and batch_size <= 0:
        raise ValueError("batch_size must be > 0 when provided")
    if max_snapshots is None and duration_s is None:
        raise ValueError("max_snapshots or duration_s is required")

    start = time.monotonic()
    next_tick = start
    snapshot_count = 0

    while True:
        if max_snapshots is not None and snapshot_count >= max_snapshots:
            break
        if duration_s is not None and time.monotonic() - start >= duration_s:
            break

        try:
            now = time.monotonic()
            sleep_for = next_tick - now
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)

            if batch_size is not None and batch_size > 1 and hasattr(clob, "books"):
                for chunk in batched(token_id_list, batch_size):
                    started_at = time.time()
                    started_mono = time.monotonic()
                    try:
                        batch_books = await clob.books(chunk)
                        completed_at = time.time()
                        metadata = {
                            "request_started_at_utc": _iso_from_seconds(started_at),
                            "request_completed_at_utc": _iso_from_seconds(completed_at),
                            "request_latency_ms": (time.monotonic() - started_mono) * 1000.0,
                            "source_endpoint": "/books",
                            "http_status": None,
                            "request_batch_size": len(chunk),
                        }
                        books_by_asset: dict[str, dict[str, Any]] = {}
                        for batch_book in batch_books:
                            payload = _book_to_dict(batch_book)
                            if isinstance(payload, dict):
                                asset_id = payload.get("asset_id")
                                if asset_id is not None:
                                    books_by_asset[str(asset_id)] = payload
                        if books_by_asset:
                            for token_id in chunk:
                                matched_book = books_by_asset.get(token_id)
                                if matched_book is None:
                                    yield completed_at, token_id, RuntimeError(
                                        f"Batch /books response did not include token_id={token_id}"
                                    )
                                else:
                                    yield completed_at, token_id, _with_request_metadata(matched_book, metadata)
                            continue

                        if len(batch_books) == len(chunk):
                            for token_id, book in zip(chunk, batch_books):
                                yield completed_at, token_id, _with_request_metadata(book, metadata)
                            continue

                        for token_id in chunk:
                            yield completed_at, token_id, RuntimeError(
                                f"Batch /books response did not include token_id={token_id}"
                            )
                    except Exception as exc:
                        completed_at = time.time()
                        for token_id in chunk:
                            yield completed_at, token_id, exc
            else:
                async def fetch_one(token_id: str) -> tuple[str, Any]:
                    started_at = time.time()
                    started_mono = time.monotonic()
                    try:
                        book = await clob.book(token_id)
                        completed_at = time.time()
                        metadata = {
                            "request_started_at_utc": _iso_from_seconds(started_at),
                            "request_completed_at_utc": _iso_from_seconds(completed_at),
                            "request_latency_ms": (time.monotonic() - started_mono) * 1000.0,
                            "source_endpoint": "/book",
                            "http_status": None,
                            "request_batch_size": 1,
                        }
                        return token_id, _with_request_metadata(book, metadata)
                    except Exception as exc:
                        return token_id, exc

                books = await asyncio.gather(*(fetch_one(token_id) for token_id in token_id_list))

                for token_id, book in books:
                    yield time.time(), token_id, book

            snapshot_count += 1
            next_tick += poll_interval_s

        except (httpx.RequestError, httpx.HTTPStatusError):
            logger.exception("Order book snapshot poll failed")
            await asyncio.sleep(1.0)
            next_tick = time.monotonic() + poll_interval_s


async def run_pipeline(
    source: AsyncIterator[tuple[float, str, Any]],
    sinks: Sequence[Sink],
    processor: Callable[[float, str, Any], Awaitable[Any] | Any | None] | None = None
) -> None:
    """
    Pulls data from source and writes to sinks.
    Optional processor can transform data before writing.
    """
    opened_sinks: list[Sink] = []
    try:
        for sink in sinks:
            await sink.__aenter__()
            opened_sinks.append(sink)

        async for timestamp, token_id, item in source:
            data_to_write = item
            if processor is not None:
                data_to_write = processor(timestamp, token_id, item)
                if inspect.isawaitable(data_to_write):
                    data_to_write = await data_to_write

            if data_to_write is None:
                continue

            for sink in sinks:
                await sink.write(data_to_write)
    finally:
        for sink in reversed(opened_sinks):
            await sink.close()





async def iter_order_book_metrics(
    clob: PolymarketClobProtocol,
    token_ids: Sequence[str],
    *,
    poll_interval_s: float,
    max_snapshots: int | None = None,
    duration_s: float | None = None,
    batch_size: int | None = DEFAULT_BOOK_BATCH_SIZE,
    collector_run_id: str = "",
) -> AsyncIterator[dict[str, Any]]:
    """
    Generator that fetches order books and yields a dictionary of calculated metrics.
    Decoupled from persistence: pass this generator to a storage pipeline.
    """
    source = fetch_order_books(
        clob, token_ids,
        poll_interval_s=poll_interval_s,
        max_snapshots=max_snapshots,
        duration_s=duration_s,
        batch_size=batch_size,
    )

    local_sequence = 0
    async for timestamp, token_id, book in source:
        if isinstance(book, Exception):
            logger.warning("Order book fetch failed for token_id=%s: %s", token_id, book)
            continue

        local_sequence += 1
        request_metadata = {}
        if isinstance(book, dict) and isinstance(book.get("_request_metadata"), dict):
            request_metadata = book["_request_metadata"]
        best_bid, best_ask = _best_bid_ask(book)
        mid = None
        spread = None
        spread_bps = None
        if best_bid is not None and best_ask is not None:
            mid = (best_bid + best_ask) / 2.0
            spread = best_ask - best_bid
            if mid > 0:
                spread_bps = spread / mid * 1e4
        topbook = polymarket_book_to_topbook(
            book,
            token_id=token_id,
            collector_run_id=collector_run_id,
            source="rest_poll",
            received_at_utc=_iso_from_seconds(timestamp),
            local_sequence=local_sequence,
            raw_event_ref=request_metadata.get("source_endpoint"),
        )

        yield {
            "ts": timestamp,
            "token_id": token_id,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid": mid,
            "spread": spread,
            "spread_bps": spread_bps,
            "book": book,
            "request_started_at_utc": request_metadata.get("request_started_at_utc"),
            "request_completed_at_utc": request_metadata.get("request_completed_at_utc"),
            "request_latency_ms": request_metadata.get("request_latency_ms"),
            "source_endpoint": request_metadata.get("source_endpoint"),
            "request_batch_size": request_metadata.get("request_batch_size"),
            "valid_state": topbook["valid_state"],
            "quality_flags": list(topbook["quality_flags"]),
            "topbook": topbook,
        }


async def collect_order_book_summaries_parquet(
    clob: PolymarketClobProtocol,
    token_ids: Sequence[str],
    *,
    output_path: str | Path,
    poll_interval_s: float,
    max_snapshots: int | None = None,
    duration_s: float | None = None,
    also_jsonl_dir: str | Path | None = None,
    flush_interval: int = 1000,
    batch_size: int | None = DEFAULT_BOOK_BATCH_SIZE,
) -> Path:
    """Collect order-book metrics into parquet, with optional raw JSONL snapshots."""
    if flush_interval <= 0:
        raise ValueError("flush_interval must be > 0")
    token_id_list = _normalize_token_ids(token_ids)
    output = Path(output_path)
    columns = [
        "ts",
        "token_id",
        "best_bid",
        "best_ask",
        "mid",
        "spread",
        "spread_bps",
        "request_started_at_utc",
        "request_completed_at_utc",
        "request_latency_ms",
        "source_endpoint",
        "request_batch_size",
        "valid_state",
        "quality_flags",
    ]
    parquet_sink = ParquetSink(output, columns=columns, flush_interval=flush_interval)
    jsonl_dir = Path(also_jsonl_dir) if also_jsonl_dir is not None else None
    if jsonl_dir is not None:
        jsonl_dir.mkdir(parents=True, exist_ok=True)

    async def source() -> AsyncIterator[tuple[float, str, Any]]:
        async for metric in iter_order_book_metrics(
            clob,
            token_id_list,
            poll_interval_s=poll_interval_s,
            max_snapshots=max_snapshots,
            duration_s=duration_s,
            batch_size=batch_size,
        ):
            if jsonl_dir is not None:
                jsonl_path = jsonl_dir / f"{metric['token_id']}.jsonl"
                with jsonl_path.open("a", encoding="utf-8") as handle:
                    handle.write(
                        json.dumps(
                            {
                                "ts": metric["ts"],
                                "token_id": metric["token_id"],
                                "book": metric["book"],
                            }
                        )
                        + "\n"
                    )
            yield float(metric["ts"]), str(metric["token_id"]), {
                column: metric.get(column) for column in columns
            }

    await run_pipeline(source(), [parquet_sink])
    return output


async def collect_order_book_topbooks_parquet(
    clob: PolymarketClobProtocol,
    token_ids: Sequence[str],
    *,
    output_path: str | Path,
    poll_interval_s: float,
    max_snapshots: int | None = None,
    duration_s: float | None = None,
    batch_size: int | None = DEFAULT_BOOK_BATCH_SIZE,
    collector_run_id: str = "",
    allow_missing_tokens: bool = False,
) -> Path:
    """Collect Polymarket REST order books as strict canonical topbook.v1 rows."""
    import pandas as pd

    token_id_list = _normalize_token_ids(token_ids)
    rows: list[dict[str, Any]] = []
    async for metric in iter_order_book_metrics(
        clob,
        token_id_list,
        poll_interval_s=poll_interval_s,
        max_snapshots=max_snapshots,
        duration_s=duration_s,
        batch_size=batch_size,
        collector_run_id=collector_run_id,
    ):
        topbook = metric.get("topbook")
        if isinstance(topbook, dict):
            rows.append(topbook)
    if not rows:
        raise RuntimeError("canonical Polymarket topbook collection produced no rows")
    observed_tokens = {str(row.get("instrument_id")) for row in rows if row.get("instrument_id")}
    missing_tokens = [token_id for token_id in token_id_list if token_id not in observed_tokens]
    if missing_tokens and not allow_missing_tokens:
        raise RuntimeError(
            "canonical Polymarket topbook collection missing token ids: "
            + ", ".join(missing_tokens[:10])
        )
    df = pd.DataFrame(rows, columns=TOPBOOK_COLUMNS)
    return write_parquet(
        df,
        Path(output_path),
        schema="topbook.v1",
        coerce=True,
        strict=True,
    )
