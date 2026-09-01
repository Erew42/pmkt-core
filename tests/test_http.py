import json

import httpx
import pytest

import pmkt._http as http_module
from pmkt._http import HttpClient, RequestPolicy, format_url, request_with_retry


pytestmark = pytest.mark.asyncio


class _TrackingStream(httpx.SyncByteStream):
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.closed = False

    def __iter__(self):
        yield self.payload

    def close(self) -> None:
        self.closed = True


class _FakeSyncClient:
    def __init__(self, responses: list[httpx.Response]) -> None:
        self.responses = list(responses)
        self.requests: list[dict[str, object]] = []

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        self.requests.append(
            {
                "method": method,
                "path": path,
                "params": params,
                "headers": headers,
            }
        )
        return self.responses.pop(0)


async def test_retryable_response_is_closed_before_error() -> None:
    responses: list[httpx.Response] = []

    def handler(request: httpx.Request) -> httpx.Response:
        response = httpx.Response(500, request=request, json={"error": "temporary"})
        responses.append(response)
        return response

    client = HttpClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
        max_retries=1,
    )

    with pytest.raises(httpx.HTTPStatusError):
        await client.request_json("GET", "/markets")

    assert len(responses) == 1
    assert responses[0].is_closed
    await client.close()


async def test_retryable_response_is_closed_before_success(monkeypatch) -> None:
    async def no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(http_module.asyncio, "sleep", no_sleep)
    responses: list[httpx.Response] = []

    def handler(request: httpx.Request) -> httpx.Response:
        status_code = 500 if not responses else 200
        response = httpx.Response(
            status_code,
            request=request,
            json={"attempt": len(responses) + 1},
        )
        responses.append(response)
        return response

    client = HttpClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
        max_retries=2,
    )

    assert await client.request_json("GET", "/markets") == {"attempt": 2}
    assert [response.is_closed for response in responses] == [True, True]
    await client.close()


async def test_async_request_honors_retry_after(monkeypatch) -> None:
    sleeps: list[float] = []

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(http_module.asyncio, "sleep", record_sleep)
    responses: list[httpx.Response] = []

    def handler(request: httpx.Request) -> httpx.Response:
        status_code = 429 if not responses else 200
        headers = {"Retry-After": "0.25"} if status_code == 429 else {}
        response = httpx.Response(
            status_code,
            request=request,
            headers=headers,
            json={"attempt": len(responses) + 1},
        )
        responses.append(response)
        return response

    client = HttpClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
        max_retries=2,
    )

    assert await client.request_json("GET", "/markets") == {"attempt": 2}
    assert sleeps == [0.25]
    assert [response.is_closed for response in responses] == [True, True]
    await client.close()


async def test_request_policy_caps_large_retry_after() -> None:
    response = httpx.Response(429, headers={"Retry-After": "3600"})

    assert (
        RequestPolicy(max_retry_after_s=1.5).delay_for(attempt=1, response=response)
        == 1.5
    )
    assert (
        RequestPolicy(max_retry_after_s=None).delay_for(attempt=1, response=response)
        == 3600
    )


async def test_request_policy_retries_get_but_not_plain_post_by_default() -> None:
    policy = RequestPolicy(max_retries=3)

    assert policy.attempts_for("GET") == 3
    assert policy.attempts_for("POST") == 1
    assert policy.attempts_for("POST", {"Idempotency-Key": "order-1"}) == 3


async def test_async_request_does_not_retry_501(monkeypatch) -> None:
    async def no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(http_module.asyncio, "sleep", no_sleep)
    seen: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(1)
        return httpx.Response(501, request=request, json={"error": "not implemented"})

    client = HttpClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
        max_retries=3,
    )

    with pytest.raises(httpx.HTTPStatusError):
        await client.request_json("GET", "/markets")

    assert len(seen) == 1
    await client.close()


async def test_async_request_does_not_retry_plain_post_by_default(monkeypatch) -> None:
    monkeypatch.setattr(http_module.asyncio, "sleep", lambda _: None)
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.method)
        return httpx.Response(500, request=request, json={"error": "temporary"})

    client = HttpClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
        max_retries=3,
    )

    with pytest.raises(httpx.HTTPStatusError):
        await client.request_json("POST", "/orders", json={"client_order_id": "order-1"})

    assert seen == ["POST"]
    await client.close()


async def test_async_request_retries_post_with_idempotency_header(monkeypatch) -> None:
    async def no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(http_module.asyncio, "sleep", no_sleep)
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["Idempotency-Key"])
        status_code = 500 if len(seen) == 1 else 200
        return httpx.Response(status_code, request=request, json={"attempt": len(seen)})

    client = HttpClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
        max_retries=3,
    )

    assert await client.request_json(
        "POST",
        "/orders",
        json={"client_order_id": "order-1"},
        headers={"Idempotency-Key": "order-1"},
    ) == {"attempt": 2}

    assert seen == ["order-1", "order-1"]
    await client.close()


async def test_async_request_retries_timeout(monkeypatch) -> None:
    async def no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(http_module.asyncio, "sleep", no_sleep)
    seen: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(1)
        if len(seen) == 1:
            raise httpx.ReadTimeout("temporary timeout", request=request)
        return httpx.Response(200, request=request, json={"ok": True})

    client = HttpClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
        max_retries=2,
    )

    assert await client.request_json("GET", "/markets") == {"ok": True}
    assert len(seen) == 2
    await client.close()


async def test_catalog_sized_policy_waits_and_retries_same_page(monkeypatch) -> None:
    sleeps: list[float] = []

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(http_module.asyncio, "sleep", record_sleep)
    seen_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_urls.append(str(request.url))
        if len(seen_urls) < 5:
            raise httpx.ConnectError("temporary DNS outage", request=request)
        return httpx.Response(200, request=request, json={"cursor": "next"})

    client = HttpClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
        request_policy=RequestPolicy(
            max_retries=20,
            backoff_base_s=5.0,
            backoff_max_s=60.0,
        ),
    )

    assert await client.request_json(
        "GET", "/markets", params={"cursor": "same-page"}
    ) == {"cursor": "next"}
    assert sleeps == [5.0, 10.0, 20.0, 40.0]
    assert seen_urls == [
        "https://example.com/markets?cursor=same-page",
    ] * 5
    await client.close()


async def test_request_params_normalize_bool_and_omit_none() -> None:
    seen_params: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_params.append(dict(request.url.params))
        return httpx.Response(200, request=request, json={"ok": True})

    client = HttpClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
    )

    await client.request_json(
        "GET",
        "/markets",
        params={"active": True, "closed": False, "cursor": None, "limit": 100},
    )

    assert seen_params == [{"active": "true", "closed": "false", "limit": "100"}]
    await client.close()


async def test_response_is_closed_when_json_decode_fails() -> None:
    responses: list[httpx.Response] = []

    def handler(request: httpx.Request) -> httpx.Response:
        response = httpx.Response(200, request=request, text="{bad json")
        responses.append(response)
        return response

    client = HttpClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(json.JSONDecodeError):
        await client.request_json("GET", "/markets")

    assert len(responses) == 1
    assert responses[0].is_closed
    await client.close()


async def test_sync_request_with_retry_retries_retryable_status(monkeypatch) -> None:
    monkeypatch.setattr(http_module.time, "sleep", lambda _: None)
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        status_code = 500 if len(seen) == 1 else 200
        return httpx.Response(
            status_code,
            request=request,
            json={"attempt": len(seen)},
        )

    with httpx.Client(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
    ) as client:
        response = request_with_retry(
            client,
            "GET",
            "/markets",
            params={"closed": False, "cursor": None},
            max_retries=2,
        )

    assert response.status_code == 200
    assert response.json() == {"attempt": 2}
    assert seen == [
        "https://example.com/markets?closed=false",
        "https://example.com/markets?closed=false",
    ]


async def test_sync_request_with_retry_honors_retry_after(monkeypatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr(http_module.time, "sleep", sleeps.append)
    responses = iter(
        [
            httpx.Response(429, headers={"Retry-After": "0.25"}),
            httpx.Response(200, json={"ok": True}),
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        response = next(responses)
        response.request = request
        return response

    with httpx.Client(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
    ) as client:
        response = request_with_retry(client, "GET", "/markets", max_retries=2)

    assert response.status_code == 200
    assert sleeps == [0.25]


async def test_sync_request_with_retry_does_not_retry_501(monkeypatch) -> None:
    monkeypatch.setattr(http_module.time, "sleep", lambda _: None)
    seen: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(1)
        return httpx.Response(
            501,
            request=request,
            json={"error": "not implemented"},
        )

    with httpx.Client(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
    ) as client:
        response = request_with_retry(client, "GET", "/markets", max_retries=3)

    assert response.status_code == 501
    assert len(seen) == 1


async def test_sync_request_with_retry_closes_intermediate_retryable_response(
    monkeypatch,
) -> None:
    monkeypatch.setattr(http_module.time, "sleep", lambda _: None)
    retryable = httpx.Response(
        500,
        stream=_TrackingStream(b'{"attempt":1}'),
    )
    success = httpx.Response(
        200,
        stream=_TrackingStream(b'{"attempt":2}'),
    )
    client = _FakeSyncClient([retryable, success])

    response = request_with_retry(client, "GET", "/markets", max_retries=2)  # type: ignore[arg-type]

    assert response is success
    assert retryable.is_closed is True
    assert success.is_closed is False


async def test_sync_request_with_retry_returns_final_retryable_response_open(monkeypatch) -> None:
    monkeypatch.setattr(http_module.time, "sleep", lambda _: None)
    retryable = httpx.Response(
        500,
        stream=_TrackingStream(b'{"error":"temporary"}'),
    )
    client = _FakeSyncClient([retryable])

    response = request_with_retry(client, "GET", "/markets", max_retries=1)  # type: ignore[arg-type]

    assert response is retryable
    assert response.status_code == 500
    assert retryable.is_closed is False
    assert response.read() == b'{"error":"temporary"}'
    assert retryable.is_closed is True


async def test_format_url_preserves_absolute_and_joins_relative() -> None:
    base_url = httpx.URL("https://example.com/api/")

    assert format_url(base_url, "https://other.test/path") == "https://other.test/path"
    assert format_url(base_url, "/markets") == "https://example.com/api/markets"
