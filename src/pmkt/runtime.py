"""Small operation-local expiry primitives for nested async workflows."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
import math
import time
from typing import Any, TypeVar, Mapping
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx

from pmkt.errors import OperationTimeoutError


_T = TypeVar("_T")


@dataclass(frozen=True)
class OperationExpiry:
    """A monotonic expiry passed explicitly through one operation's call tree."""

    deadline_monotonic: float | None
    _clock: Callable[[], float] = field(
        default=time.monotonic, repr=False, compare=False
    )

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

    @classmethod
    def bounded(
        cls,
        timeout_s: float,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> "OperationExpiry":
        """Create an expiry for a workflow that may never be unbounded."""

        if timeout_s is None:
            raise TypeError("timeout_s must be a number")
        return cls.after(timeout_s, clock=clock)

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


def parse_retry_after(
    value: str | None, *, now: datetime | None = None
) -> float | None:
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        reference = now or datetime.now(timezone.utc)
        return max(0.0, (parsed - reference).total_seconds())
    return seconds if seconds >= 0 else None


@dataclass(frozen=True)
class RequestPolicy:
    """Retry budget; max_attempts includes the initial request."""

    max_attempts: int = 3
    retry_status_codes: frozenset[int] = frozenset({429, 500, 502, 503, 504})
    retry_non_idempotent_methods: bool = False
    idempotent_methods: frozenset[str] = frozenset(
        {"GET", "HEAD", "OPTIONS", "TRACE", "PUT", "DELETE"}
    )
    idempotency_headers: tuple[str, ...] = ("Idempotency-Key", "X-Idempotency-Key")
    backoff_base_s: float = 1.0
    backoff_max_s: float = 10.0
    max_retry_after_s: float | None = 60.0
    respect_retry_after: bool = True

    def __post_init__(self) -> None:
        if isinstance(self.max_attempts, bool) or not isinstance(
            self.max_attempts, int
        ):
            raise TypeError("max_attempts must be an int")
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be positive")

    def attempts_for(
        self,
        method: str,
        headers: Mapping[str, str] | None = None,
    ) -> int:
        attempts = self.max_attempts
        if self.retry_non_idempotent_methods:
            return attempts
        if method.upper() in self.idempotent_methods:
            return attempts
        if self.has_idempotency_marker(headers):
            return attempts
        return 1

    def has_idempotency_marker(self, headers: Mapping[str, str] | None) -> bool:
        header_names = {name.lower() for name in (headers or {})}
        return any(name.lower() in header_names for name in self.idempotency_headers)

    def should_retry_response(self, response: httpx.Response) -> bool:
        return response.status_code in self.retry_status_codes

    def should_retry_exception(self, exc: Exception) -> bool:
        return isinstance(exc, httpx.RequestError)

    def delay_for(
        self, *, attempt: int, response: httpx.Response | None = None
    ) -> float:
        if response is not None and self.respect_retry_after:
            retry_after = parse_retry_after(response.headers.get("Retry-After"))
            if retry_after is not None:
                if self.max_retry_after_s is None:
                    return retry_after
                return min(retry_after, self.max_retry_after_s)
        return min(self.backoff_base_s * (2 ** (attempt - 1)), self.backoff_max_s)


__all__ = ["OperationExpiry", "RequestPolicy"]
