from __future__ import annotations

from pmkt.records import PolymarketMarketRef

import asyncio
from typing import Any

import httpx
import pytest

import pmkt.resolution._batch as batch_module
from pmkt.runtime import RequestPolicy
from pmkt.runtime import OperationExpiry
from pmkt.errors import OperationTimeoutError, ReadAuthenticationRequiredError
from pmkt.exchanges.polymarket import AsyncGammaClient
from pmkt.records import KalshiMarketRef
from pmkt.resolution import KalshiResolutionResolver, PolymarketResolutionResolver
from pmkt.resolution.models import ResolutionRecord, SourceObservation


def _record(market_key: str, invocation: int = 0) -> ResolutionRecord:
    return ResolutionRecord(
        platform="polymarket",
        market_key=market_key,
        input_identifier=market_key,
        resolution_state="unavailable",
        confidence="unavailable",
        source_observations=[
            SourceObservation(
                source="test",
                confidence="unavailable",
                evidence={"invocation": invocation},
            )
        ],
    )


@pytest.mark.asyncio
async def test_batch_validates_options_and_all_inputs_before_io() -> None:
    calls = 0

    class Gamma:
        async def market(self, market_id: str, *, expiry=None) -> dict[str, Any]:
            nonlocal calls
            calls += 1
            return {"id": market_id}

    polymarket = PolymarketResolutionResolver(gamma_client=Gamma())
    kalshi = KalshiResolutionResolver()

    assert await polymarket.resolve_many([]) == []
    assert await kalshi.resolve_many([]) == []
    for invalid in ("", b"", bytearray()):
        with pytest.raises(TypeError, match="sequence"):
            await polymarket.resolve_many(invalid)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="sequence"):
            await kalshi.resolve_many(invalid)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="PolymarketMarketRef"):
        await polymarket.resolve_many(  # type: ignore[list-item]
            [PolymarketMarketRef("pm-1"), KalshiMarketRef("KX-WRONG")]
        )
    with pytest.raises(TypeError, match="KalshiMarketRef"):
        await kalshi.resolve_many(  # type: ignore[list-item]
            [KalshiMarketRef("KX-1"), PolymarketMarketRef("pm-wrong")]
        )
    for concurrency, error in ((True, TypeError), (1.5, TypeError), (0, ValueError)):
        with pytest.raises(error):
            await polymarket.resolve_many(  # type: ignore[arg-type]
                [PolymarketMarketRef("pm-1")], concurrency=concurrency
            )
    for deadline, error in (
        (None, TypeError),
        (True, TypeError),
        (0.0, ValueError),
        (float("inf"), ValueError),
    ):
        with pytest.raises(error):
            await polymarket.resolve_many(  # type: ignore[arg-type]
                [], deadline_s=deadline
            )
    assert calls == 0


@pytest.mark.asyncio
async def test_batch_late_malformed_ctf_enrichment_fails_before_io() -> None:
    calls = 0

    class Gamma:
        async def market(self, market_id: str, *, expiry=None) -> dict[str, Any]:
            nonlocal calls
            calls += 1
            return {"id": market_id}

    class Ctf:
        async def ensure_polygon(self, *, expiry=None) -> None:
            raise AssertionError("CTF I/O must not start")

        async def payout_vector(
            self, condition_id: str, outcome_count: int
        ) -> tuple[int, list[int]]:
            raise AssertionError("CTF I/O must not start")

    resolver = PolymarketResolutionResolver(gamma_client=Gamma(), ctf_client=Ctf())
    with pytest.raises(ValueError, match="condition_id is not hex"):
        await resolver.resolve_many(
            [
                PolymarketMarketRef("pm-1", condition_id="0xabc"),
                PolymarketMarketRef("pm-2", condition_id="not-hex"),
            ]
        )
    assert calls == 0


@pytest.mark.asyncio
async def test_batch_uses_fixed_live_workers_and_preserves_order_and_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = PolymarketResolutionResolver()
    release = asyncio.Event()
    all_parked = asyncio.Event()
    all_but_first_completed = asyncio.Event()
    first_release = asyncio.Event()
    active = 0
    peak = 0
    invocations = 0
    completion_order: list[str] = []

    async def resolve_prepared(
        prepared: Any, *, expiry: OperationExpiry
    ) -> ResolutionRecord:
        nonlocal active, peak, invocations
        invocations += 1
        invocation = invocations
        active += 1
        peak = max(peak, active)
        if active == 3:
            all_parked.set()
        try:
            await release.wait()
        finally:
            active -= 1
        if invocation == 1:
            await first_release.wait()
        expiry.checkpoint()
        completion_order.append(prepared.market_key)
        if len(completion_order) == len(markets) - 1:
            all_but_first_completed.set()
        return _record(prepared.market_key, invocation)

    monkeypatch.setattr(resolver, "_resolve_prepared_with_expiry", resolve_prepared)
    markets = [
        PolymarketMarketRef("duplicate" if index in (0, 49) else f"pm-{index}")
        for index in range(50)
    ]
    task = asyncio.create_task(
        resolver.resolve_many(markets, concurrency=3, deadline_s=5.0)
    )
    await asyncio.wait_for(all_parked.wait(), timeout=1.0)
    live_workers = [
        candidate
        for candidate in asyncio.all_tasks()
        if candidate.get_name().startswith("pmkt-resolution-worker-")
        and not candidate.done()
    ]
    assert len(live_workers) == 3
    assert invocations == 3
    release.set()
    await asyncio.wait_for(all_but_first_completed.wait(), timeout=1.0)
    assert not task.done()
    first_release.set()
    records = await task

    assert peak == 3
    assert completion_order != [market.market_id for market in markets]
    assert [record.market_key for record in records] == [
        market.market_id for market in markets
    ]
    assert invocations == len(markets)
    assert records[0] is not records[-1]
    assert records[0].source_observations is not records[-1].source_observations
    assert records[0].source_observations[0].evidence != (
        records[-1].source_observations[0].evidence
    )


@pytest.mark.asyncio
async def test_batch_drains_workers_when_later_worker_allocation_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[asyncio.Task[None]] = []
    original_create_task = asyncio.create_task

    def controlled_create_task(coro: Any, *, name: str | None = None) -> Any:
        if created:
            raise RuntimeError("injected allocation failure")
        task = original_create_task(coro, name=name)
        created.append(task)
        return task

    monkeypatch.setattr(batch_module.asyncio, "create_task", controlled_create_task)
    with pytest.raises(RuntimeError, match="allocation failure"):
        await PolymarketResolutionResolver().resolve_many(
            [PolymarketMarketRef("pm-1"), PolymarketMarketRef("pm-2")],
            concurrency=2,
        )
    assert len(created) == 1
    assert created[0].done()
    assert created[0].cancelled()


@pytest.mark.asyncio
async def test_batch_expected_source_failures_keep_order_and_cardinality() -> None:
    class Gamma:
        async def market(self, market_id: str, *, expiry=None) -> dict[str, Any]:
            request = httpx.Request("GET", f"https://gamma.test/{market_id}")
            if market_id == "transport":
                raise httpx.ConnectError("offline", request=request)
            if market_id == "forbidden":
                response = httpx.Response(403, request=request)
                raise httpx.HTTPStatusError(
                    "forbidden", request=request, response=response
                )
            return {"id": market_id, "closed": False}

    records = await PolymarketResolutionResolver(gamma_client=Gamma()).resolve_many(
        [
            PolymarketMarketRef("ok"),
            PolymarketMarketRef("transport"),
            PolymarketMarketRef("forbidden"),
        ],
        concurrency=2,
    )

    assert [record.market_key for record in records] == [
        "ok",
        "transport",
        "forbidden",
    ]
    assert len(records) == 3
    assert records[1].source_observations[0].error_type == "ConnectError"
    assert records[2].source_observations[0].error_type == "HTTPStatusError"


@pytest.mark.parametrize(
    "failure",
    [ReadAuthenticationRequiredError("read auth required"), RuntimeError("worker bug")],
)
@pytest.mark.asyncio
async def test_batch_global_or_programmer_failure_drains_sibling_and_client_reuses(
    failure: Exception,
) -> None:
    slow_entered = asyncio.Event()
    slow_drained = asyncio.Event()
    failing = True

    class Client:
        async def market(self, ticker: str, *, expiry=None) -> dict[str, Any]:
            if not failing:
                return {"ticker": ticker, "status": "finalized", "result": "yes"}
            if ticker == "slow":
                slow_entered.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    await asyncio.sleep(0)
                    slow_drained.set()
                    raise
            await slow_entered.wait()
            raise failure

        async def historical_market(self, ticker: str, *, expiry=None) -> dict[str, Any]:
            return {"ticker": ticker, "status": "finalized", "result": "yes"}

    resolver = KalshiResolutionResolver(Client())
    with pytest.raises(type(failure), match=str(failure)):
        await resolver.resolve_many(
            [KalshiMarketRef("slow"), KalshiMarketRef("boom")], concurrency=2
        )
    assert slow_drained.is_set()
    failing = False
    [record] = await resolver.resolve_many([KalshiMarketRef("reused")])
    assert record.market_key == "reused"


@pytest.mark.asyncio
async def test_batch_uses_one_expiry_across_sequential_items(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 0.0
    expiry_ids: set[int] = set()
    calls = 0
    resolver = PolymarketResolutionResolver()

    def bounded(timeout_s: float) -> OperationExpiry:
        return OperationExpiry.after(timeout_s, clock=lambda: now)

    async def resolve_prepared(
        prepared: Any, *, expiry: OperationExpiry
    ) -> ResolutionRecord:
        nonlocal now, calls
        calls += 1
        expiry_ids.add(id(expiry))
        now += 0.6
        return _record(prepared.market_key, calls)

    monkeypatch.setattr(batch_module.OperationExpiry, "bounded", bounded)
    monkeypatch.setattr(resolver, "_resolve_prepared_with_expiry", resolve_prepared)
    with pytest.raises(OperationTimeoutError):
        await resolver.resolve_many(
            [PolymarketMarketRef("pm-1"), PolymarketMarketRef("pm-2")],
            concurrency=1,
            deadline_s=1.0,
        )
    assert calls == 2
    assert len(expiry_ids) == 1


@pytest.mark.asyncio
async def test_concurrent_batches_have_independent_cleanup_and_expiry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = PolymarketResolutionResolver()
    cancel_entered = asyncio.Event()
    cancel_drained = asyncio.Event()
    keep_entered = asyncio.Event()
    keep_release = asyncio.Event()

    async def resolve_prepared(
        prepared: Any, *, expiry: OperationExpiry
    ) -> ResolutionRecord:
        if prepared.market_key == "cancel":
            cancel_entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0)
                cancel_drained.set()
                raise
        if prepared.market_key == "keep":
            keep_entered.set()
            await keep_release.wait()
        expiry.checkpoint()
        return _record(prepared.market_key)

    monkeypatch.setattr(resolver, "_resolve_prepared_with_expiry", resolve_prepared)
    cancelled = asyncio.create_task(
        resolver.resolve_many([PolymarketMarketRef("cancel")])
    )
    kept = asyncio.create_task(resolver.resolve_many([PolymarketMarketRef("keep")]))
    await asyncio.wait_for(
        asyncio.gather(cancel_entered.wait(), keep_entered.wait()), timeout=1.0
    )
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    assert cancel_drained.is_set()
    assert not kept.done()
    keep_release.set()
    [kept_record] = await kept
    [reused_record] = await resolver.resolve_many([PolymarketMarketRef("reused")])
    assert kept_record.market_key == "keep"
    assert reused_record.market_key == "reused"


@pytest.mark.asyncio
async def test_batch_timeout_while_limiter_blocked_drains_without_request() -> None:
    entered = asyncio.Event()
    drained = asyncio.Event()
    blocked = False
    active = 0
    request_count = 0

    class Limiter:
        async def __aenter__(self) -> None:
            nonlocal active
            if not blocked:
                return
            active += 1
            if active == 2:
                entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0)
                active -= 1
                if active == 0:
                    drained.set()
                raise

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(200, json={"id": request.url.path.rsplit("/", 1)[-1]})

    client = AsyncGammaClient(
        base_url="https://gamma.test",
        limiter=Limiter(),  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
        request_policy=RequestPolicy(max_attempts=1),
    )
    resolver = PolymarketResolutionResolver(gamma_client=client)
    try:
        await resolver.resolve(PolymarketMarketRef("warm"), deadline_s=1.0)
        blocked = True
        task = asyncio.create_task(
            resolver.resolve_many(
                [PolymarketMarketRef("pm-1"), PolymarketMarketRef("pm-2")],
                concurrency=2,
                deadline_s=0.05,
            )
        )
        await asyncio.wait_for(entered.wait(), timeout=1.0)
        with pytest.raises(OperationTimeoutError):
            await task
        assert drained.is_set()
        assert request_count == 1
        blocked = False
        records = await resolver.resolve_many([PolymarketMarketRef("reused")])
    finally:
        await client.close()
    assert records[0].market_key == "reused"


@pytest.mark.parametrize("failure", ["timeout", "caller_cancel"])
@pytest.mark.asyncio
async def test_batch_transport_failure_drains_before_raise_and_client_reuses(
    failure: str,
) -> None:
    all_entered = asyncio.Event()
    all_drained = asyncio.Event()
    blocked = False
    active = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active
        if blocked:
            active += 1
            if active == 2:
                all_entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0)
                active -= 1
                if active == 0:
                    all_drained.set()
                raise
        return httpx.Response(200, json={"id": request.url.path.rsplit("/", 1)[-1]})

    client = AsyncGammaClient(
        base_url="https://gamma.test",
        transport=httpx.MockTransport(handler),
        request_policy=RequestPolicy(max_attempts=1),
    )
    resolver = PolymarketResolutionResolver(gamma_client=client)
    try:
        await resolver.resolve(PolymarketMarketRef("warm"), deadline_s=1.0)
        blocked = True
        task = asyncio.create_task(
            resolver.resolve_many(
                [PolymarketMarketRef("pm-1"), PolymarketMarketRef("pm-2")],
                concurrency=2,
                deadline_s=0.05 if failure == "timeout" else 5.0,
            )
        )
        await asyncio.wait_for(all_entered.wait(), timeout=1.0)
        if failure == "caller_cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(OperationTimeoutError):
                await task
        assert all_drained.is_set()
        blocked = False
        [record] = await resolver.resolve_many([PolymarketMarketRef("reused")])
    finally:
        await client.close()
    assert record.market_key == "reused"


@pytest.mark.asyncio
async def test_batch_timeout_in_retry_backoff_starts_no_late_retry() -> None:
    failing = False
    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        if failing:
            return httpx.Response(503, headers={"Retry-After": "1"})
        return httpx.Response(200, json={"id": request.url.path.rsplit("/", 1)[-1]})

    client = AsyncGammaClient(
        base_url="https://gamma.test",
        transport=httpx.MockTransport(handler),
        request_policy=RequestPolicy(max_attempts=2),
    )
    resolver = PolymarketResolutionResolver(gamma_client=client)
    try:
        await resolver.resolve(PolymarketMarketRef("warm"), deadline_s=1.0)
        failing = True
        with pytest.raises(OperationTimeoutError):
            await resolver.resolve_many(
                [PolymarketMarketRef("pm-1"), PolymarketMarketRef("pm-2")],
                concurrency=2,
                deadline_s=0.05,
            )
        await asyncio.sleep(0)
        assert request_count == 3
        failing = False
        [record] = await resolver.resolve_many([PolymarketMarketRef("reused")])
    finally:
        await client.close()
    assert record.market_key == "reused"
