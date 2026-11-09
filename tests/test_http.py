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



def _default_test_client(name, **kwargs):
    from pmkt.exchanges.kalshi.client import AsyncKalshiClient, KalshiHttpClient
    from pmkt.exchanges.polymarket.gamma import AsyncGammaClient
    from pmkt.exchanges.polymarket.clob import AsyncClobClient
    from pmkt.exchanges.polymarket.data_api import AsyncPolymarketDataClient

    factory = {
        "http": HttpClient, "kalshi-http": KalshiHttpClient, "kalshi": AsyncKalshiClient,
        "gamma": AsyncGammaClient, "clob": AsyncClobClient, "data": AsyncPolymarketDataClient,
    }[name]
    return factory(base_url="https://example.test", **kwargs)


@pytest.mark.parametrize("name", ["http", "kalshi-http", "kalshi", "gamma", "clob", "data"])
@pytest.mark.parametrize("timeout,attempts", [(None, None), (2.5, 1)])
async def test_public_clients_effective_timeout_and_total_attempts(name, timeout, attempts):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(503, json={"error": "offline"})

    policy_options = {"backoff_base_s": 0.0}
    if attempts is not None:
        policy_options["max_attempts"] = attempts
    options = {} if timeout is None else {"timeout_s": timeout}
    client = _default_test_client(name, transport=httpx.MockTransport(handler),
                                  request_policy=RequestPolicy(**policy_options), **options)
    http = client if isinstance(client, HttpClient) else client._http
    try:
        response = await http.request_response("GET", "/read")
        assert response.status_code == 503
        assert len(requests) == (3 if attempts is None else attempts)
        expected = timeout if timeout is not None else (30.0 if name == "data" else 10.0)
        assert all(request.extensions["timeout"]["read"] == expected for request in requests)
    finally:
        await client.close()


@pytest.mark.parametrize("name", ["kalshi-http", "kalshi", "gamma", "clob", "data"])
async def test_public_client_wrappers_do_not_supply_common_defaults(monkeypatch, name):
    calls = []
    original = HttpClient.__init__

    def init(self, *args, **kwargs):
        calls.append({key: value for key, value in kwargs.items() if key in {"timeout_s", "max_attempts"}})
        original(self, *args, **kwargs)

    monkeypatch.setattr(HttpClient, "__init__", init)
    client = _default_test_client(name)
    try:
        assert calls == ([{"timeout_s": 30.0}] if name == "data" else [{}])
    finally:
        await client.close()


@pytest.mark.parametrize("name", ["http", "kalshi-http", "kalshi", "gamma", "clob", "data"])
@pytest.mark.parametrize("timeout,error", [(0, ValueError), (-1, ValueError), (False, TypeError), (float("inf"), ValueError)])
async def test_public_clients_preserve_invalid_explicit_timeouts(name, timeout, error):
    with pytest.raises(error, match="timeout_s"):
        _default_test_client(name, timeout_s=timeout)


@pytest.mark.parametrize("name", ["kalshi", "gamma", "clob", "data"])
async def test_service_rate_presets_and_explicit_limiter(name):
    from aiolimiter import AsyncLimiter
    client = _default_test_client(name)
    override = AsyncLimiter(2, 3)
    explicit = _default_test_client(name, limiter=override)
    try:
        assert (client.limiter.max_rate, client.limiter.time_period) == (10, 1)
        assert explicit.limiter is override
        assert explicit._http.limiter is override
    finally:
        await client.close()
        await explicit.close()


@pytest.mark.parametrize("name", ["http", "kalshi-http", "kalshi", "gamma", "clob", "data"])
async def test_public_clients_acquire_falsey_limiter(name):
    from aiolimiter import AsyncLimiter

    acquisitions = []

    class FalseyLimiter(AsyncLimiter):
        def __bool__(self):
            return False

        async def __aenter__(self):
            acquisitions.append(True)
            return await super().__aenter__()

    limiter = FalseyLimiter(1)
    client = _default_test_client(
        name, limiter=limiter,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"ok": True})),
    )
    http = client if isinstance(client, HttpClient) else client._http
    try:
        assert http.limiter is limiter
        assert await http.request_json("GET", "/read") == {"ok": True}
        assert acquisitions == [True]
    finally:
        await client.close()


@pytest.mark.parametrize("name", ["http", "kalshi-http", "kalshi", "gamma", "clob", "data"])
async def test_public_clients_default_request_makes_three_attempts(monkeypatch, name):
    async def no_sleep(_delay):
        pass

    monkeypatch.setattr(http_module.asyncio, "sleep", no_sleep)
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(503, json={"error": "offline"})

    client = _default_test_client(name, transport=httpx.MockTransport(handler))
    http = client if isinstance(client, HttpClient) else client._http
    try:
        response = await http.request_response("GET", "/read")
        assert response.status_code == 503
        assert len(requests) == 3
    finally:
        await client.close()


async def test_kalshi_http_client_forwards_max_attempts_without_request_policy(monkeypatch):
    from pmkt.exchanges.kalshi.client import KalshiHttpClient

    async def no_sleep(_delay):
        pass

    monkeypatch.setattr(http_module.asyncio, "sleep", no_sleep)
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(503, json={"error": "offline"})

    client = KalshiHttpClient(base_url="https://example.test",
                              transport=httpx.MockTransport(handler), max_attempts=1)
    try:
        response = await client.request_response("GET", "/read")
        assert response.status_code == 503
        assert len(requests) == 1
    finally:
        await client.close()
