import json

import httpx
import pandas as pd
import pytest

from pmkt.data.market_data import (
    collect_order_book_summaries_parquet,
    collect_order_book_topbooks_parquet,
    fetch_order_books,
    fetch_trade_history,
    find_event_by_slug,
    iter_order_book_metrics,
    run_pipeline,
)
from pmkt.exchanges.polymarket.gamma import AsyncGammaClient
from pmkt.models import PriceHistory, PriceHistoryPoint


pytestmark = pytest.mark.asyncio


async def test_fetch_trade_history_parses_history() -> None:
    class FakeClob:
        async def prices_history(self, token_id: str, interval: str, fidelity: int | None = None):
            return {
                "history": [
                    {"t": 1, "p": "0.45", "s": "10"},
                    [2, 0.5, 3],
                    {"timestamp": "3", "price": 0.55},
                    {"time": 4, "value": "0.6", "size": "2"},
                    "bad",
                ]
            }

    data = await fetch_trade_history(FakeClob(), "token", interval="1h")

    assert data == [
        {"timestamp": 1.0, "price": 0.45, "size": 10.0},
        {"timestamp": 2.0, "price": 0.5, "size": 3.0},
        {"timestamp": 3.0, "price": 0.55, "size": None},
        {"timestamp": 4.0, "price": 0.6, "size": 2.0},
    ]


async def test_fetch_trade_history_parses_price_history_model() -> None:
    class FakeClob:
        async def prices_history(self, token_id: str, interval: str, fidelity: int | None = None):
            return PriceHistory(
                history=[
                    PriceHistoryPoint(t=1234567890, p=0.42),
                    PriceHistoryPoint(t=1234567891, p=0.43),
                ]
            )

    data = await fetch_trade_history(FakeClob(), "token", interval="1h")

    assert data == [
        {"timestamp": 1234567890.0, "price": 0.42, "size": None},
        {"timestamp": 1234567891.0, "price": 0.43, "size": None},
    ]


async def test_find_event_by_slug_iterates_pages() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params.get("offset", "0"))
        if offset == 0:
            return httpx.Response(200, json=[{"id": "event-0", "title": "Nope", "slug": "nope"}])
        if offset == 1:
            return httpx.Response(200, json=[{"id": "event-1", "title": "Target", "slug": "target"}])
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    client = AsyncGammaClient(base_url="https://example.com/api", transport=transport)

    event = await find_event_by_slug(client, "target", limit=1, max_pages=3)

    assert event["id"] == "event-1"
    await client.close()


async def test_iter_order_book_metrics() -> None:
    class FakeClob:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def book(self, token_id: str):
            self.calls.append(token_id)
            return {
                "bids": [{"price": 0.4, "size": 10}],
                "asks": [{"price": 0.6, "size": 5}],
            }

    clob = FakeClob()

    results = []
    async for metric in iter_order_book_metrics(
        clob,
        ["token-1"],
        poll_interval_s=0.01,
        max_snapshots=1,
    ):
        results.append(metric)

    assert len(results) == 1
    assert clob.calls == ["token-1"]

    metric = results[0]
    assert metric["token_id"] == "token-1"
    assert metric["best_bid"] == pytest.approx(0.4)
    assert metric["best_ask"] == pytest.approx(0.6)
    assert metric["spread"] == pytest.approx(0.2)
    assert metric["mid"] == pytest.approx(0.5)
    assert metric["valid_state"] is True
    assert metric["quality_flags"] == []
    assert metric["topbook"]["schema_version"] == "topbook.v1"


async def test_fetch_order_books_uses_batch_books_and_deduplicates_tokens() -> None:
    class FakeClob:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        async def books(self, token_ids: list[str]):
            self.calls.append(list(token_ids))
            return [
                {
                    "hash": f"h-{token_id}",
                    "asset_id": token_id,
                    "bids": [{"price": "0.4", "size": "10"}],
                    "asks": [{"price": "0.6", "size": "5"}],
                }
                for token_id in reversed(token_ids)
            ]

    clob = FakeClob()
    rows = []
    async for row in fetch_order_books(
        clob,
        ["token-1", "token-1", "token-2"],
        poll_interval_s=0.01,
        max_snapshots=1,
        batch_size=50,
    ):
        rows.append(row)

    assert clob.calls == [["token-1", "token-2"]]
    assert [token_id for _, token_id, _ in rows] == ["token-1", "token-2"]
    assert [book["asset_id"] for _, _, book in rows] == ["token-1", "token-2"]
    assert [book["hash"] for _, _, book in rows] == ["h-token-1", "h-token-2"]
    assert all(book["_request_metadata"]["source_endpoint"] == "/books" for _, _, book in rows)


async def test_collect_order_book_topbooks_can_allow_missing_tokens(tmp_path) -> None:
    class PartialClob:
        async def books(self, _token_ids: list[str]) -> list[dict[str, object]]:
            return [
                {
                    "asset_id": "token-1",
                    "market": "market-token-1",
                    "hash": "hash-token-1",
                    "bids": [{"price": "0.4", "size": "10"}],
                    "asks": [{"price": "0.6", "size": "5"}],
                }
            ]

    output = tmp_path / "topbooks.parquet"

    await collect_order_book_topbooks_parquet(
        PartialClob(),
        ["token-1", "token-2"],
        output_path=output,
        poll_interval_s=0.01,
        max_snapshots=1,
        allow_missing_tokens=True,
    )

    df = pd.read_parquet(output)
    assert df["instrument_id"].tolist() == ["token-1"]


async def test_run_pipeline_opens_writes_skips_and_closes_sinks() -> None:
    class FakeSink:
        def __init__(self) -> None:
            self.opened = False
            self.closed = False
            self.writes: list[dict[str, object]] = []

        async def __aenter__(self):
            self.opened = True
            return self

        async def write(self, item):
            assert self.opened
            self.writes.append(item)

        async def close(self) -> None:
            self.closed = True

    async def source():
        yield 1.0, "token-1", {"value": 1}
        yield 2.0, "skip", {"value": 2}

    def processor(timestamp, token_id, item):
        if token_id == "skip":
            return None
        return {"ts": timestamp, "token_id": token_id, **item}

    first = FakeSink()
    second = FakeSink()

    await run_pipeline(source(), [first, second], processor=processor)

    expected = [{"ts": 1.0, "token_id": "token-1", "value": 1}]
    assert first.writes == expected
    assert second.writes == expected
    assert first.closed
    assert second.closed


async def test_run_pipeline_awaits_sync_processor_returning_awaitable() -> None:
    class FakeSink:
        def __init__(self) -> None:
            self.closed = False
            self.writes: list[dict[str, object]] = []

        async def __aenter__(self):
            return self

        async def write(self, item):
            self.writes.append(item)

        async def close(self) -> None:
            self.closed = True

    async def source():
        yield 1.0, "token-1", {"value": 1}

    def processor(timestamp, token_id, item):
        async def inner():
            return {"ts": timestamp, "token_id": token_id, **item}

        return inner()

    sink = FakeSink()

    await run_pipeline(source(), [sink], processor=processor)

    assert sink.writes == [{"ts": 1.0, "token_id": "token-1", "value": 1}]
    assert sink.closed


async def test_run_pipeline_closes_sinks_on_write_error() -> None:
    class FailingSink:
        def __init__(self) -> None:
            self.closed = False

        async def __aenter__(self):
            return self

        async def write(self, item):
            raise RuntimeError("write failed")

        async def close(self) -> None:
            self.closed = True

    async def source():
        yield 1.0, "token-1", {"value": 1}

    sink = FailingSink()

    with pytest.raises(RuntimeError, match="write failed"):
        await run_pipeline(source(), [sink])

    assert sink.closed


async def test_collect_order_book_summaries_parquet_writes_output(tmp_path) -> None:
    class FakeClob:
        async def book(self, token_id: str):
            return {
                "bids": [{"price": 0.4, "size": 10}],
                "asks": [{"price": 0.6, "size": 5}],
            }

    output_path = tmp_path / "summaries.parquet"
    jsonl_dir = tmp_path / "raw"

    result = await collect_order_book_summaries_parquet(
        FakeClob(),
        ["token-1"],
        output_path=output_path,
        poll_interval_s=0.01,
        max_snapshots=1,
        also_jsonl_dir=jsonl_dir,
    )

    assert result == output_path
    assert output_path.exists()

    import pandas as pd

    df = pd.read_parquet(output_path)
    assert df["token_id"].tolist() == ["token-1"]
    assert df["best_bid"].iloc[0] == pytest.approx(0.4)
    assert df["best_ask"].iloc[0] == pytest.approx(0.6)
    assert df["mid"].iloc[0] == pytest.approx(0.5)

    raw_line = (jsonl_dir / "token-1.jsonl").read_text(encoding="utf-8").strip()
    raw = json.loads(raw_line)
    assert raw["token_id"] == "token-1"
    assert raw["book"]["bids"][0]["price"] == 0.4


async def test_collect_order_book_summaries_rejects_empty_token_ids(tmp_path) -> None:
    class FakeClob:
        async def book(self, token_id: str):
            raise AssertionError("book should not be called")

    output_path = tmp_path / "summaries.parquet"

    with pytest.raises(ValueError, match="non-empty token id"):
        await collect_order_book_summaries_parquet(
            FakeClob(),
            [" ", ""],
            output_path=output_path,
            poll_interval_s=0.01,
            max_snapshots=1,
        )

    assert not output_path.exists()
