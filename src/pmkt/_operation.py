"""Small operation-local expiry primitives for nested async workflows."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
import math
import time
from typing import Any, TypeVar

from pmkt.errors import OperationTimeoutError


_T = TypeVar("_T")


@dataclass(frozen=True)
class OperationExpiry:
    """A monotonic expiry passed explicitly through one operation's call tree."""

    deadline_monotonic: float | None
    _clock: Callable[[], float] = field(default=time.monotonic, repr=False, compare=False)

    @classmethod
    def after(
        cls,
        timeout_s: float | None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> "OperationExpiry":
        if timeout_s is None:
            return cls(None, clock)
        if isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)):
            raise TypeError("timeout_s must be a number or None")
        timeout = float(timeout_s)
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout_s must be finite and positive")
        return cls(clock() + timeout, clock)

    def remaining_s(self) -> float | None:
        if self.deadline_monotonic is None:
            return None
        return max(0.0, self.deadline_monotonic - self._clock())

    def checkpoint(self) -> None:
        remaining = self.remaining_s()
        if remaining is not None and remaining <= 0:
            raise OperationTimeoutError("operation expired")

    def capped_timeout(self, timeout_s: float) -> float:
        if isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)):
            raise TypeError("timeout_s must be a number")
        timeout = float(timeout_s)
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout_s must be finite and positive")
        remaining = self.remaining_s()
        if remaining is None:
            return timeout
        if remaining <= 0:
            raise OperationTimeoutError("operation expired")
        return min(timeout, remaining)

    async def run(self, factory: Callable[[], Awaitable[_T]]) -> _T:
        """Await owned work within the remaining budget and always drain it."""

        self.checkpoint()
        remaining = self.remaining_s()
        if remaining is None:
            return await factory()

        task = asyncio.ensure_future(factory())
        try:
            result = await asyncio.wait_for(asyncio.shield(task), timeout=remaining)
        except asyncio.TimeoutError as exc:
            if task.done():
                result = task.result()
            else:
                await cancel_and_drain((task,))
                raise OperationTimeoutError("operation expired") from exc
        except BaseException:
            await cancel_and_drain((task,))
            raise
        self.checkpoint()
        return result

    async def sleep(self, delay_s: float) -> None:
        if delay_s <= 0:
            self.checkpoint()
            return
        await self.run(lambda: asyncio.sleep(delay_s))


async def cancel_and_drain(tasks: Iterable[asyncio.Task[Any]]) -> None:
    owned = tuple(tasks)
    for task in owned:
        if not task.done():
            task.cancel()
    if owned:
        await asyncio.gather(*owned, return_exceptions=True)


__all__ = ["OperationExpiry", "cancel_and_drain"]
