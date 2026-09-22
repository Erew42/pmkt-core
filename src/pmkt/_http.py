from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import math
from typing import Any, Collection, Mapping

import httpx
from aiolimiter import AsyncLimiter

from pmkt.runtime import OperationExpiry, RequestPolicy


def format_url(base_url: httpx.URL, path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return str(base_url.join(path.lstrip("/")))


def _normalize_params(params: dict[str, Any] | None) -> dict[str, Any] | None:
    if not params:
        return None
    normalized: dict[str, Any] = {}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, bool):
            normalized[key] = "true" if value else "false"
        else:
            normalized[key] = value
    return normalized


def _positive_timeout(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("timeout_s must be a number")
    timeout = float(value)
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout_s must be finite and positive")
    return timeout


@dataclass
class _RequestTrace:
    attempt_count: int = 0
    started_at_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    received_at_utc: datetime | None = None
    status_code: int | None = None
    response_url: str | None = None


class HttpClient:
    """Robust internal HTTP client handling retries and rate limits."""

    def __init__(
        self,
        base_url: str,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_s: float = 10.0,
        headers: dict[str, str] | None = None,
        limiter: AsyncLimiter | None = None,
        request_policy: RequestPolicy | None = None,
        *,
        max_attempts: int = 3,
        retryable_post_paths: Collection[str] = (),
        follow_redirects: bool = False,
    ) -> None:

        self.base_url = base_url
        self.request_policy = request_policy or RequestPolicy(
            max_attempts=max_attempts,
        )
        self.timeout_s = _positive_timeout(timeout_s)
        self._headers = headers or {"Accept": "application/json"}
        self.limiter = limiter
        self.retryable_post_paths = frozenset(
            "/" + path.lstrip("/") for path in retryable_post_paths
        )
        self._follow_redirects = follow_redirects
        self._transport = transport
        self._client: httpx.AsyncClient | None = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url.rstrip("/"),
                timeout=httpx.Timeout(self.timeout_s),
                headers=self._headers,
                transport=self._transport,
                follow_redirects=self._follow_redirects,
            )
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def __enter__(self) -> "HttpClient":
        raise RuntimeError("Use 'async with' for HttpClient.")

    async def __aenter__(self) -> "HttpClient":
        self._get_client()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
        *,
        expiry: OperationExpiry | None = None,
        trace: _RequestTrace | None = None,
    ) -> httpx.Response:
        request_path = path
        if not (path.startswith("http://") or path.startswith("https://")):
            request_path = path.lstrip("/")
        normalized_params = _normalize_params(params)
        client = self._get_client()
        attempts = self._attempts_for(method, request_path, headers)

        for attempt in range(1, attempts + 1):
            if expiry is not None:
                expiry.checkpoint()
            try:
                response = await self._send_request(
                    client,
                    method,
                    request_path,
                    params=normalized_params,
                    json=json,
                    headers=headers,
                    expiry=expiry,
                    trace=trace,
                )
            except httpx.RequestError as exc:
                if (
                    attempt == attempts
                    or not self.request_policy.should_retry_exception(exc)
                ):
                    raise
                await self._sleep(
                    self.request_policy.delay_for(attempt=attempt), expiry=expiry
                )
                continue

            if self.request_policy.should_retry_response(response):
                if attempt == attempts:
                    return response
                delay = self.request_policy.delay_for(
                    attempt=attempt, response=response
                )
                await response.aclose()
                await self._sleep(delay, expiry=expiry)
                continue
            return response

        raise RuntimeError("request failed unexpectedly")

    def _attempts_for(
        self,
        method: str,
        request_path: str,
        headers: Mapping[str, str] | None,
    ) -> int:
        normalized_path = "/" + request_path.split("?", 1)[0].lstrip("/")
        if method.upper() == "POST" and normalized_path in self.retryable_post_paths:
            return self.request_policy.max_attempts
        return self.request_policy.attempts_for(method, headers)

    async def _sleep(
        self,
        delay_s: float,
        *,
        expiry: OperationExpiry | None,
    ) -> None:
        if expiry is None:
            await asyncio.sleep(delay_s)
        else:
            await expiry.sleep(delay_s)

    async def _send_request(
        self,
        client: httpx.AsyncClient,
        method: str,
        request_path: str,
        *,
        params: dict[str, Any] | None,
        json: Any | None,
        headers: dict[str, str] | None,
        expiry: OperationExpiry | None = None,
        trace: _RequestTrace | None = None,
    ) -> httpx.Response:
        async def send() -> httpx.Response:
            timeout = (
                expiry.capped_timeout(self.timeout_s) if expiry is not None else None
            )
            kwargs: dict[str, Any] = {
                "params": params,
                "json": json,
                "headers": headers,
            }
            if timeout is not None:
                kwargs["timeout"] = timeout
            if trace is not None:
                trace.attempt_count += 1
            response = await client.request(method, request_path, **kwargs)
            if trace is not None:
                trace.received_at_utc = datetime.now(timezone.utc)
                trace.status_code = response.status_code
                trace.response_url = str(response.url)
            return response

        async def acquire_and_send() -> httpx.Response:
            if self.limiter:
                async with self.limiter:
                    return await send()
            return await send()

        if expiry is not None:
            return await expiry.run(acquire_and_send)
        return await acquire_and_send()

    async def request_response(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        expiry: OperationExpiry | None = None,
    ) -> httpx.Response:
        """Return a fully read, closed response for internal API diagnostics."""
        response = await self._request(method, path, params=params, expiry=expiry)
        try:
            if expiry is None:
                await response.aread()
            else:
                await expiry.run(response.aread)
            return response
        finally:
            await response.aclose()

    async def request_json(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
        *,
        expiry: OperationExpiry | None = None,
    ) -> Any:
        response = await self._request(
            method,
            path,
            params=params,
            json=json,
            headers=headers,
            expiry=expiry,
        )
        return await self._decode_response(response, expiry=expiry)

    async def _decode_response(
        self,
        response: httpx.Response,
        *,
        expiry: OperationExpiry | None,
    ) -> Any:
        try:
            response.raise_for_status()
            if expiry is not None:
                expiry.checkpoint()
            data = response.json()
            if expiry is not None:
                expiry.checkpoint()
            return data
        finally:
            await response.aclose()
