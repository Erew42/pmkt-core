"""Private fixed-worker orchestration for ordered resolution batches."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar, cast

from pmkt._operation import OperationExpiry, cancel_and_drain


_InputT = TypeVar("_InputT")
_PreparedT = TypeVar("_PreparedT")
_ResultT = TypeVar("_ResultT")
_MISSING = object()


async def resolve_ordered_batch(
    items: Sequence[_InputT],
    *,
    concurrency: int,
    deadline_s: float,
    prepare: Callable[[_InputT], _PreparedT],
    resolve_one: Callable[[_PreparedT, OperationExpiry], Awaitable[_ResultT]],
) -> list[_ResultT]:
    """Prepare all inputs, then resolve them with a fixed number of workers."""

    if isinstance(items, (str, bytes, bytearray)) or not isinstance(items, Sequence):
        raise TypeError("markets must be a sequence")
    if isinstance(concurrency, bool) or not isinstance(concurrency, int):
        raise TypeError("concurrency must be an int")
    if concurrency <= 0:
        raise ValueError("concurrency must be positive")
    expiry = OperationExpiry.bounded(deadline_s)

    prepared: list[_PreparedT] = []
    for item in items:
        prepared.append(prepare(item))
        expiry.checkpoint()
    if not prepared:
        expiry.checkpoint()
        return []

    results: list[_ResultT | object] = [_MISSING] * len(prepared)
    next_index = 0

    async def worker() -> None:
        nonlocal next_index
        while True:
            expiry.checkpoint()
            if next_index >= len(prepared):
                return
            index = next_index
            next_index += 1
            results[index] = await resolve_one(prepared[index], expiry)
            expiry.checkpoint()

    workers: list[asyncio.Task[None]] = []
    try:
        for worker_index in range(min(concurrency, len(prepared))):
            worker_coro = worker()
            try:
                task = asyncio.create_task(
                    worker_coro,
                    name=f"pmkt-resolution-worker-{worker_index}",
                )
            except BaseException:
                worker_coro.close()
                raise
            workers.append(task)
        await asyncio.gather(*workers)
    except BaseException:
        await cancel_and_drain(workers)
        raise

    expiry.checkpoint()
    if any(result is _MISSING for result in results):
        raise RuntimeError("resolution batch completed with a missing result")
    return cast(list[_ResultT], results)


__all__: list[str] = []
