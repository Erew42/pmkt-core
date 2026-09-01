from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import time
from typing import Any, Mapping

import httpx
from aiolimiter import AsyncLimiter


def parse_retry_after(value: str | None, *, now: datetime | None = None) -> float | None:
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
    max_retries: int = 3
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

    def attempts_for(
        self,
        method: str,
        headers: Mapping[str, str] | None = None,
    ) -> int:
        attempts = max(1, self.max_retries)
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

    def delay_for(self, *, attempt: int, response: httpx.Response | None = None) -> float:
        if response is not None and self.respect_retry_after:
            retry_after = parse_retry_after(response.headers.get("Retry-After"))
            if retry_after is not None:
                if self.max_retry_after_s is None:
                    return retry_after
                return min(retry_after, self.max_retry_after_s)
        return min(self.backoff_base_s * (2 ** (attempt - 1)), self.backoff_max_s)


def request_with_retry(
    client: httpx.Client,
    method: str,
    path: str,
    params: dict[str, Any] | None = None,
    max_retries: int = 3,
    headers: dict[str, str] | None = None,
    request_policy: RequestPolicy | None = None,
) -> httpx.Response:
    policy = request_policy or RequestPolicy(max_retries=max_retries)
    attempts = policy.attempts_for(method, headers)
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = client.request(
                method,
                path,
                params=_normalize_params(params),
                headers=headers,
            )
        except httpx.RequestError as exc:
            last_error = exc
            if attempt == attempts or not policy.should_retry_exception(exc):
                raise
            time.sleep(policy.delay_for(attempt=attempt))
            continue

        if policy.should_retry_response(response):
            if attempt == attempts:
                return response
            backoff = policy.delay_for(attempt=attempt, response=response)
            response.close()
            time.sleep(backoff)
            continue

        return response

    if last_error:
        raise last_error
    raise RuntimeError("request failed unexpectedly")


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


class HttpClient:
    """Robust internal HTTP Client handling retries and rate limits."""

    def __init__(
        self,
        base_url: str,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_s: float = 10.0,
        headers: dict[str, str] | None = None,
        max_retries: int = 3,
        limiter: AsyncLimiter | None = None,
        request_policy: RequestPolicy | None = None,
    ) -> None:
        self.base_url = base_url
        self.request_policy = request_policy or RequestPolicy(max_retries=max_retries)
        self.max_retries = self.request_policy.max_retries
        self.timeout_s = timeout_s
        self._headers = headers or {"Accept": "application/json"}
        self.limiter = limiter

        self._transport = transport
        self._client: httpx.AsyncClient | None = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url.rstrip("/"),
                timeout=httpx.Timeout(self.timeout_s),
                headers=self._headers,
                transport=self._transport,
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
    ) -> httpx.Response:
        request_path = path
        if not (path.startswith("http://") or path.startswith("https://")):
            request_path = path.lstrip("/")
        normalized_params = _normalize_params(params)
        client = self._get_client()
        attempts = self.request_policy.attempts_for(method, headers)

        for attempt in range(1, attempts + 1):
            try:
                response = await self._send_request(
                    client,
                    method,
                    request_path,
                    params=normalized_params,
                    json=json,
                    headers=headers,
                )
            except httpx.RequestError as exc:
                if attempt == attempts or not self.request_policy.should_retry_exception(exc):
                    raise
                await asyncio.sleep(self.request_policy.delay_for(attempt=attempt))
                continue

            if self.request_policy.should_retry_response(response):
                if attempt == attempts:
                    return response
                delay = self.request_policy.delay_for(attempt=attempt, response=response)
                await response.aclose()
                await asyncio.sleep(delay)
                continue
            return response

        raise RuntimeError("request failed unexpectedly")

    async def _send_request(
        self,
        client: httpx.AsyncClient,
        method: str,
        request_path: str,
        *,
        params: dict[str, Any] | None,
        json: Any | None,
        headers: dict[str, str] | None,
    ) -> httpx.Response:
        if self.limiter:
            async with self.limiter:
                return await client.request(
                    method,
                    request_path,
                    params=params,
                    json=json,
                    headers=headers,
                )
        return await client.request(
            method,
            request_path,
            params=params,
            json=json,
            headers=headers,
        )

    async def request_json(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        response = await self._request(
            method,
            path,
            params=params,
            json=json,
            headers=headers,
        )
        try:
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError:
            raise
        finally:
            await response.aclose()
