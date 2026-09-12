from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from json import JSONDecodeError
import math
import time
from typing import Any, Callable, Collection, Mapping, Sequence

import httpx
from aiolimiter import AsyncLimiter

from pmkt._observations import (
    classify_request_source,
    sanitize_effective_parameters,
    sanitize_endpoint_template,
    source_after_response,
)
from pmkt._operation import OperationExpiry
from pmkt.errors import InvalidDataError, OperationTimeoutError
from pmkt.records import RequestObservation, RequestOutcome


class _Omitted:
    pass


_OMITTED = _Omitted()


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


@dataclass(frozen=True, init=False)
class RequestPolicy:
    max_retries: int
    retry_status_codes: frozenset[int]
    retry_non_idempotent_methods: bool
    idempotent_methods: frozenset[str]
    idempotency_headers: tuple[str, ...]
    backoff_base_s: float
    backoff_max_s: float
    max_retry_after_s: float | None
    respect_retry_after: bool

    def __init__(
        self,
        max_retries: int | _Omitted = _OMITTED,
        retry_status_codes: frozenset[int] = frozenset({429, 500, 502, 503, 504}),
        retry_non_idempotent_methods: bool = False,
        idempotent_methods: frozenset[str] = frozenset(
            {"GET", "HEAD", "OPTIONS", "TRACE", "PUT", "DELETE"}
        ),
        idempotency_headers: tuple[str, ...] = (
            "Idempotency-Key",
            "X-Idempotency-Key",
        ),
        backoff_base_s: float = 1.0,
        backoff_max_s: float = 10.0,
        max_retry_after_s: float | None = 60.0,
        respect_retry_after: bool = True,
        *,
        max_attempts: int | _Omitted = _OMITTED,
    ) -> None:
        if not isinstance(max_retries, _Omitted) and not isinstance(
            max_attempts, _Omitted
        ):
            raise ValueError("max_retries and max_attempts cannot both be supplied")
        attempts = (
            max_attempts
            if not isinstance(max_attempts, _Omitted)
            else max_retries
            if not isinstance(max_retries, _Omitted)
            else 3
        )
        if isinstance(attempts, bool) or not isinstance(attempts, int):
            raise TypeError("max_attempts must be an int")
        object.__setattr__(self, "max_retries", attempts)
        object.__setattr__(self, "retry_status_codes", retry_status_codes)
        object.__setattr__(
            self, "retry_non_idempotent_methods", retry_non_idempotent_methods
        )
        object.__setattr__(self, "idempotent_methods", idempotent_methods)
        object.__setattr__(self, "idempotency_headers", idempotency_headers)
        object.__setattr__(self, "backoff_base_s", backoff_base_s)
        object.__setattr__(self, "backoff_max_s", backoff_max_s)
        object.__setattr__(self, "max_retry_after_s", max_retry_after_s)
        object.__setattr__(self, "respect_retry_after", respect_retry_after)

    @property
    def max_attempts(self) -> int:
        return self.max_retries

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
    max_retries: int | _Omitted = _OMITTED,
    headers: dict[str, str] | None = None,
    request_policy: RequestPolicy | None = None,
    *,
    max_attempts: int | _Omitted = _OMITTED,
) -> httpx.Response:
    _reject_attempt_alias_conflict(max_retries, max_attempts)
    policy = request_policy or RequestPolicy(
        max_retries=max_retries,
        max_attempts=max_attempts,
    )
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


def _reject_attempt_alias_conflict(
    max_retries: int | _Omitted,
    max_attempts: int | _Omitted,
) -> None:
    if not isinstance(max_retries, _Omitted) and not isinstance(
        max_attempts, _Omitted
    ):
        raise ValueError("max_retries and max_attempts cannot both be supplied")


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


class HttpClient:
    """Robust internal HTTP client handling retries and rate limits."""

    def __init__(
        self,
        base_url: str,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_s: float = 10.0,
        headers: dict[str, str] | None = None,
        max_retries: int | _Omitted = _OMITTED,
        limiter: AsyncLimiter | None = None,
        request_policy: RequestPolicy | None = None,
        *,
        max_attempts: int | _Omitted = _OMITTED,
        retryable_post_paths: Collection[str] = (),
        source_venue: str = "unknown",
        source_service: str = "unknown",
    ) -> None:
        _reject_attempt_alias_conflict(max_retries, max_attempts)
        self.base_url = base_url
        self.request_policy = request_policy or RequestPolicy(
            max_retries=max_retries,
            max_attempts=max_attempts,
        )
        self.max_retries = self.request_policy.max_retries
        self.timeout_s = _positive_timeout(timeout_s)
        self._headers = headers or {"Accept": "application/json"}
        self.limiter = limiter
        self.retryable_post_paths = frozenset(
            "/" + path.lstrip("/") for path in retryable_post_paths
        )
        self._transport = transport
        self._client: httpx.AsyncClient | None = None
        self.source = classify_request_source(
            base_url,
            venue=source_venue,
            service=source_service,
            transport_supplied=transport is not None,
        )

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
                if attempt == attempts or not self.request_policy.should_retry_exception(exc):
                    raise
                await self._sleep(
                    self.request_policy.delay_for(attempt=attempt), expiry=expiry
                )
                continue

            if self.request_policy.should_retry_response(response):
                if attempt == attempts:
                    return response
                delay = self.request_policy.delay_for(attempt=attempt, response=response)
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
            return max(1, self.request_policy.max_attempts)
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
            return await client.request(method, request_path, **kwargs)

        async def acquire_and_send() -> httpx.Response:
            if self.limiter:
                async with self.limiter:
                    return await send()
            return await send()

        if expiry is not None:
            return await expiry.run(acquire_and_send)
        return await acquire_and_send()

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
        # Forward the expiry only when one was supplied. Subclasses that override
        # ``_request`` with the pre-workflow signature (no ``expiry``/``trace``
        # keywords) must keep serving the retained native methods, which never
        # pass an expiry.
        if expiry is None:
            response = await self._request(
                method,
                path,
                params=params,
                json=json,
                headers=headers,
            )
        else:
            response = await self._request(
                method,
                path,
                params=params,
                json=json,
                headers=headers,
                expiry=expiry,
            )
        return await self._decode_response(response, expiry=expiry)

    async def request_json_observed(
        self,
        method: str,
        path: str,
        *,
        request_id: str,
        endpoint_template: str,
        parameter_allowlist: Collection[str],
        effective_parameters: Mapping[str, object] | None = None,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
        expiry: OperationExpiry | None = None,
        response_identities: Callable[[Any], Sequence[str]] | None = None,
        record_observation: Callable[[RequestObservation], None] | None = None,
    ) -> tuple[Any, RequestObservation]:
        """Fetch decoded JSON and return sanitized operation-local provenance."""

        template = sanitize_endpoint_template(endpoint_template)
        if not isinstance(request_id, str):
            raise TypeError("request_id must be a string")
        if not request_id.strip():
            raise ValueError("request_id must not be empty")
        observed_parameters = sanitize_effective_parameters(
            effective_parameters,
            allowlist=parameter_allowlist,
        )
        trace = _RequestTrace()
        started = datetime.now(timezone.utc)
        received: datetime | None = None
        status_code: int | None = None
        source = self.source
        identities: tuple[str, ...] = ()
        outcome: RequestOutcome = "error"
        observation: RequestObservation
        response: httpx.Response | None = None
        try:
            response = await self._request(
                method,
                path,
                params=params,
                json=json,
                headers=headers,
                expiry=expiry,
                trace=trace,
            )
            received = datetime.now(timezone.utc)
            status_code = response.status_code
            source = source_after_response(source, str(response.url))
            data = await self._decode_response(response, expiry=expiry)
            response = None
            if response_identities is not None:
                if expiry is not None:
                    expiry.checkpoint()
                identities = tuple(response_identities(data))
                if expiry is not None:
                    expiry.checkpoint()
            outcome = "success"
        except OperationTimeoutError:
            outcome = "timeout"
            raise
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except httpx.HTTPStatusError:
            outcome = "http_error"
            raise
        except httpx.RequestError:
            outcome = "transport_error"
            raise
        except JSONDecodeError:
            outcome = "invalid_response"
            raise
        except InvalidDataError:
            outcome = "invalid_response"
            raise
        finally:
            if response is not None:
                await response.aclose()
            observation = RequestObservation(
                request_id=request_id,
                venue=source.venue,
                data_scope=source.data_scope,
                transport_origin=source.transport_origin,
                origin=source.origin,
                endpoint_template=template,
                effective_parameters=observed_parameters,
                started_at_utc=started,
                received_at_utc=received,
                attempt_count=trace.attempt_count,
                outcome=outcome,
                status_code=status_code,
                response_identities=identities,
            )
            if record_observation is not None:
                record_observation(observation)
        return data, observation

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
