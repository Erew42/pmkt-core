import json

import httpx
import pytest

import pmkt._http as http_module
from pmkt.runtime import RequestPolicy
from pmkt._http import HttpClient, format_url


pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("status", [200, 500])
async def test_diagnostic_response_is_readable_after_transport_cleanup(status) -> None:
    async with HttpClient(
        "https://example.test",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(status, json={"ok": True})
        ),
        request_policy=RequestPolicy(max_attempts=1),
    ) as client:
        response = await client.request_response("GET", "/markets")
        assert response.is_closed
        assert response.status_code == status
        assert response.json() == {"ok": True}


async def test_retryable_response_is_closed_before_error() -> None:
    responses: list[httpx.Response] = []

    def handler(request: httpx.Request) -> httpx.Response:
        response = httpx.Response(500, request=request, json={"error": "temporary"})
        responses.append(response)
        return response

    client = HttpClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
        max_attempts=1,
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
        max_attempts=2,
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
        max_attempts=2,
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
    policy = RequestPolicy(max_attempts=3)

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
        max_attempts=3,
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
        max_attempts=3,
    )

    with pytest.raises(httpx.HTTPStatusError):
        await client.request_json(
            "POST", "/orders", json={"client_order_id": "order-1"}
        )

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
        max_attempts=3,
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
        max_attempts=2,
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
            max_attempts=20,
            backoff_base_s=5.0,
            backoff_max_s=60.0,
        ),
    )

    assert await client.request_json(
        "GET", "/markets", params={"cursor": "same-page"}
    ) == {"cursor": "next"}
    assert sleeps == [5.0, 10.0, 20.0, 40.0]
    assert (
        seen_urls
        == [
            "https://example.com/markets?cursor=same-page",
        ]
        * 5
    )
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


async def test_format_url_preserves_absolute_and_joins_relative() -> None:
    base_url = httpx.URL("https://example.com/api/")

    assert format_url(base_url, "https://other.test/path") == "https://other.test/path"
    assert format_url(base_url, "/markets") == "https://example.com/api/markets"
