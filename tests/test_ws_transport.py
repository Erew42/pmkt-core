from __future__ import annotations

import asyncio
import socket
from collections.abc import Callable

import pytest

from pmkt.exchanges.ws_transport import (
    WebSocketDeadlineExceeded,
    WebSocketRetryBudget,
    is_transport_teardown_race,
)
from pmkt.exchanges.kalshi.ws import AsyncKalshiWebSocketClient
from pmkt.exchanges.polymarket.ws import AsyncMarketWebSocketClient


class RetrySocket:
    def __init__(self, failure: str | None = None) -> None:
        self.failure = failure
        self.closed = False
        self.sent: list[str] = []

    async def send(self, payload: str) -> None:
        self.sent.append(payload)
        if self.failure == "subscription":
            raise OSError("subscription failed")

    async def close(self) -> None:
        self.closed = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.failure == "receive":
            raise OSError("receive failed")
        if self.failure == "clean":
            raise StopAsyncIteration
        return '{"type":"ticker","event_type":"book"}'


class FakeReadAuth:
    def headers_for_get(self, path):
        return {}


@pytest.fixture(params=["polymarket", "kalshi"])
def retry_client(request):
    def create(factory, **kwargs):
        if request.param == "polymarket":
            return AsyncMarketWebSocketClient(
                ["123"], connect_factory=factory, heartbeat_interval=None, **kwargs,
            )
        return AsyncKalshiWebSocketClient(
            ["TEST"], connect_factory=factory, auth=FakeReadAuth(), **kwargs,
        )
    return create


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", ["startup", "receive", "clean", "subscription"])
@pytest.mark.parametrize("succeeds", [False, True], ids=["exhaustion", "recovered"])
async def test_repeated_connection_failures_use_the_remaining_budget(
    retry_client, origin, succeeds,
) -> None:
    first = RetrySocket(origin)
    recovered = RetrySocket()
    calls = 0
    delays = []
    error = socket.gaierror(11001, "injected DNS failure")

    async def factory(*args):
        nonlocal calls
        calls += 1
        if calls == 1 and origin != "startup":
            return first
        if calls == 4 and succeeds:
            return recovered
        raise error

    async def sleep(delay):
        delays.append(delay)

    client = retry_client(factory, sleep=sleep)
    try:
        iterator = client.iter_messages(max_reconnects=3, reconnect_backoff=0.25)
        if succeeds:
            assert await anext(iterator)
            assert recovered.sent
        else:
            with pytest.raises(socket.gaierror) as caught:
                await anext(iterator)
            assert caught.value is error
        assert calls == 4
        assert delays == [0.25, 0.5, 0.75]
        if origin != "startup":
            assert first.closed
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_context_entry_failure_closes_unsubscribed_socket(retry_client):
    raw = RetrySocket("subscription")

    async def factory(*args):
        return raw

    client = retry_client(factory)
    with pytest.raises(OSError, match="subscription failed"):
        async with client:
            pytest.fail("failed subscription entered context")
    assert raw.closed
    assert not client.is_connected


@pytest.mark.asyncio
async def test_context_and_requested_recovery_share_iterator_budget(retry_client):
    calls = 0
    delays = []

    async def factory(*args):
        nonlocal calls
        calls += 1
        if calls in (1, 3, 5):
            raise socket.gaierror(11001, "DNS failed")
        return RetrySocket("receive" if calls == 4 else None)

    async def sleep(delay):
        delays.append(delay)

    budget = WebSocketRetryBudget(3, sleep=sleep)
    async with retry_client(factory, retry_budget=budget) as client:
        assert budget.used == 1
        await budget.run(client.reconnect, retry_first=True, immediate_first=True)
        assert budget.used == 3
        with pytest.raises(OSError, match="receive failed"):
            await anext(client.iter_messages())
        assert budget.used == 3
    assert calls == 4
    assert delays == [0.5, 1.5]


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["connect", "backoff", "subscription"])
async def test_cancellation_does_not_retry_or_leak_socket(retry_client, boundary):
    entered = asyncio.Event()
    raw = RetrySocket()
    calls = 0

    async def block(*args):
        entered.set()
        await asyncio.Future()

    if boundary == "subscription":
        raw.send = block

    async def factory(*args):
        nonlocal calls
        calls += 1
        if boundary == "connect":
            await block()
        if boundary == "backoff":
            raise OSError("connect failed")
        return raw

    client = retry_client(factory, sleep=block)
    task = asyncio.create_task(anext(client.iter_messages()))
    await asyncio.wait_for(entered.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert calls == 1
    assert not client.is_connected
    if boundary == "subscription":
        assert raw.closed


@pytest.mark.asyncio
async def test_reconnect_false_disables_shared_budget(retry_client):
    calls = 0

    async def factory(*args):
        nonlocal calls
        calls += 1
        raise OSError("connect failed")

    budget = WebSocketRetryBudget(3)
    client = retry_client(factory, retry_budget=budget)
    with pytest.raises(OSError, match="connect failed"):
        await anext(client.iter_messages(reconnect=False))
    assert calls == 1
    assert budget.used == 0


@pytest.mark.asyncio
async def test_retry_deadline_prevents_attempt_after_backoff():
    now = 0.0
    calls = 0

    async def fail():
        nonlocal calls
        calls += 1
        raise OSError("connect failed")

    async def sleep(delay):
        nonlocal now
        now = 2.0

    budget = WebSocketRetryBudget(3, deadline=1.0, clock=lambda: now, sleep=sleep)
    with pytest.raises(WebSocketDeadlineExceeded):
        await budget.run(fail)
    assert calls == 1


@pytest.mark.asyncio
async def test_deadline_is_rechecked_when_connection_task_starts():
    now = 0.0
    calls = 0

    def expire():
        nonlocal now
        now = 2.0

    async def connect():
        nonlocal calls
        calls += 1

    budget = WebSocketRetryBudget(deadline=1.0, clock=lambda: now)
    asyncio.get_running_loop().call_soon(expire)
    with pytest.raises(WebSocketDeadlineExceeded):
        await budget.run(connect)
    assert calls == 0


@pytest.mark.asyncio
async def test_retry_deadline_cancels_blocked_connection():
    cancelled = asyncio.Event()

    async def block():
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    budget = WebSocketRetryBudget(3, deadline=asyncio.get_running_loop().time() + 0.05)
    with pytest.raises(WebSocketDeadlineExceeded):
        await budget.run(block)
    assert cancelled.is_set()
    assert budget.used == 0


def _transport_error(attribute: str) -> AttributeError:
    namespace: dict[str, object] = {"__name__": "asyncio.sslproto"}
    source = (
        "def raise_transport_error():\n"
        "    transport = None\n"
        f"    transport.{attribute}()\n"
    )
    exec(compile(source, "sslproto.py", "exec"), namespace)
    raiser = namespace["raise_transport_error"]
    assert isinstance(raiser, Callable)
    try:
        raiser()
    except AttributeError as exc:
        return exc
    raise AssertionError("synthetic transport error did not raise")


@pytest.mark.parametrize("attribute", ["pause_reading", "resume_reading"])
def test_transport_teardown_race_requires_shape_and_transport_origin(
    attribute: str,
) -> None:
    assert is_transport_teardown_race(_transport_error(attribute))


def test_transport_teardown_race_rejects_matching_application_error() -> None:
    try:
        transport = None
        transport.pause_reading()  # type: ignore[union-attr]
    except AttributeError as exc:
        assert not is_transport_teardown_race(exc)
    else:
        raise AssertionError("application error did not raise")


def test_transport_teardown_race_rejects_exception_without_traceback() -> None:
    exc = AttributeError("'NoneType' object has no attribute 'resume_reading'")

    assert not is_transport_teardown_race(exc)


def test_transport_teardown_race_rejects_unrelated_attribute_error() -> None:
    assert not is_transport_teardown_race(AttributeError("application bug"))


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_first", [False, True])
async def test_cancellation_wins_concurrent_bounded_operation_completion(retry_first):
    parent = None
    calls = 0

    async def complete_and_cancel():
        nonlocal calls
        calls += 1
        assert parent is not None
        parent.cancel()

    budget = WebSocketRetryBudget(
        deadline=asyncio.get_running_loop().time() + 10,
        sleep=lambda _: complete_and_cancel(),
    )
    parent = asyncio.create_task(budget.run(complete_and_cancel, retry_first=retry_first))
    with pytest.raises(asyncio.CancelledError):
        await parent
    assert calls == 1
    assert budget.used == int(retry_first)
    assert budget.last_error is None


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["polymarket", "kalshi"])
@pytest.mark.parametrize("boundary", ["connect", "receive", "reconnect", "backoff", "close"])
async def test_capture_cancellation_persists_v3_manifest_at_lifecycle_boundaries(
    monkeypatch, tmp_path, venue, boundary,
):
    import importlib
    import json
    from pmkt.data.manifests import validate_run_manifest
    from pmkt.streaming.profiles import select_storage_profile

    module = importlib.import_module(f"pmkt.exchanges.{venue}.order_book_stream")
    capture = (module.stream_order_book_data if venue == "polymarket"
               else module.stream_kalshi_order_book_data)
    entered = asyncio.Event()
    connections = 0

    async def block():
        entered.set()
        await asyncio.Future()

    class ControlledSocket(RetrySocket):
        async def __anext__(self):
            if boundary == "receive":
                await block()
            if boundary in {"reconnect", "backoff"}:
                raise OSError("injected disconnect")
            return '{"type":"ticker","event_type":"last_trade_price","asset_id":"a"}'

        async def close(self):
            self.closed = True
            if boundary == "close":
                await block()

    raw = ControlledSocket()

    async def factory(*args):
        nonlocal connections
        connections += 1
        if boundary == "connect" or (boundary == "reconnect" and connections == 2):
            await block()
        return raw

    async def backoff(_):
        if boundary == "backoff":
            await block()

    def budget(*args, **kwargs):
        return WebSocketRetryBudget(*args, **kwargs, sleep=backoff)

    monkeypatch.setattr(module, "WebSocketRetryBudget", budget)
    kwargs = {"heartbeat_interval": None} if venue == "polymarket" else {"auth": FakeReadAuth()}
    task = asyncio.create_task(capture(
        ["a"], output_root=tmp_path, run_name="cancelled", duration_s=60,
        max_messages=1, connect_factory=factory,
        storage_profile=select_storage_profile("full", profile_version="3"), **kwargs,
    ))
    try:
        await asyncio.wait_for(entered.wait(), timeout=3)
        previous_connections = connections
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=1)
        assert task in done, "cancellation must finish within one second"
        with pytest.raises(asyncio.CancelledError):
            await task
        assert connections == previous_connections
        manifest_path = tmp_path / "cancelled" / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        assert manifest["capture_completeness"]["terminal_reason"] == "cancelled"
        validation = validate_run_manifest(manifest_path)
        assert validation.ok, validation.all_errors
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_outer_cancellation_during_deadline_cleanup_is_not_suppressed():
    cleaning_up = asyncio.Event()

    async def operation():
        try:
            await asyncio.Future()
        finally:
            cleaning_up.set()
            await asyncio.Future()

    budget = WebSocketRetryBudget(deadline=asyncio.get_running_loop().time() + 0.02)
    task = asyncio.create_task(budget.run(operation))
    await asyncio.wait_for(cleaning_up.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert budget.used == 0


@pytest.mark.asyncio
async def test_retry_diagnostic_precedes_invalidation_and_error_clear():
    from websockets.exceptions import ConnectionClosedError
    from websockets.frames import Close
    observed = []
    budget = WebSocketRetryBudget(
        backoff=0,
        on_retry=lambda event: observed.append(event),
        on_reconnect=lambda: observed.append("invalidated"),
    )
    budget.last_error = ConnectionClosedError(Close(1011, "server restart"), None)
    budget.retry_context = {"origin": "transport", "reason": "receive_failure"}
    async def connected():
        assert observed[0]["received_close_code"] == 1011
        assert observed[0]["received_close_reason"] == "server restart"
        assert observed[1] == "invalidated"
    await budget.run(connected, retry_first=True)
    assert observed[0]["exception_type"] == "ConnectionClosedError"
    assert observed[0]["replacement_attempt"] == 1
    assert budget.last_error is None
    assert budget.retry_context == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["receive", "clean"])
async def test_client_retry_records_actual_cause(retry_client, failure):
    records = []
    sockets = iter([RetrySocket(failure), RetrySocket()])
    budget = WebSocketRetryBudget(backoff=0, on_retry=records.append)
    client = retry_client(lambda *args: next(sockets), retry_budget=budget)
    async with client:
        iterator = client.iter_messages()
        await iterator.__anext__()
        await iterator.aclose()
    assert len(records) == 1
    assert records[0]["origin"] == "transport"
    assert records[0]["reason"] == ("receive_failure" if failure == "receive" else "clean_close")
    assert records[0]["exception_type"] == ("OSError" if failure == "receive" else None)
