from __future__ import annotations

import asyncio
import json
from pathlib import Path
import httpx
import pytest

import pmkt.config as config_module
import pmkt.exchanges.kalshi.client as kalshi_module
import pmkt.exchanges.polymarket.clob as clob_module
import pmkt.exchanges.polymarket.data_api as data_api_module
import pmkt.exchanges.polymarket.gamma as gamma_module
import pmkt.exchanges.polymarket.subgraph as subgraph_module
from pmkt._http import HttpClient, RequestPolicy
from pmkt._observations import (
    classify_request_source,
    sanitize_effective_parameters,
    sanitize_endpoint_template,
    source_after_response,
)
from pmkt._operation import OperationExpiry
from pmkt.config import PmktConfig
from pmkt.config import RequestPolicy as ConfigRequestPolicy
from pmkt.errors import (
    InvalidDataError,
    OperationTimeoutError,
    ReadAuthenticationRequiredError,
)
from pmkt.exchanges.kalshi import AsyncKalshiClient
from pmkt.exchanges.polymarket import AsyncClobClient, AsyncGammaClient
from pmkt.exchanges.polymarket.data_api import AsyncPolymarketDataClient
from pmkt.exchanges.polymarket.subgraph import AsyncSubgraphClient
from pmkt.exchanges.read_auth import (
    ReadAuthenticationRequiredError as CanonicalReadAuthenticationRequiredError,
)


def test_from_values_bypasses_os_dotenv_discovery_and_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("PMKT_GAMMA_API_URL=https://dotenv.invalid\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PMKT_GAMMA_API_URL", "https://environment.invalid")
    cached = PmktConfig.from_values(gamma_api_url="https://cached.test")
    monkeypatch.setattr(config_module, "_config", cached)

    def fail_discovery(*args: object, **kwargs: object) -> tuple[Path, ...]:
        raise AssertionError("from_values discovered environment files")

    monkeypatch.setattr(config_module, "resolve_default_env_files", fail_discovery)
    defaulted = PmktConfig.from_values()
    explicit = PmktConfig.from_values(gamma_api_url="https://explicit.test")

    assert defaulted.gamma_api_url == "https://gamma-api.polymarket.com"
    assert explicit.gamma_api_url == "https://explicit.test"
    assert config_module._config is cached


def test_from_env_and_legacy_constructor_preserve_environment_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PMKT_GAMMA_API_URL", "https://environment.test")

    assert PmktConfig.from_env(_env_file=None).gamma_api_url == "https://environment.test"
    assert PmktConfig(_env_file=None).gamma_api_url == "https://environment.test"


@pytest.mark.asyncio
async def test_supplied_config_clients_do_not_call_global_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_global() -> PmktConfig:
        raise AssertionError("supplied config called get_config")

    for module in (
        gamma_module,
        clob_module,
        data_api_module,
        subgraph_module,
        kalshi_module,
    ):
        monkeypatch.setattr(module, "get_config", fail_global)

    config = PmktConfig.from_values(
        gamma_api_url="https://gamma.test",
        clob_api_url="https://clob.test",
        polymarket_data_api_url="https://data.test",
        subgraph_api_url="https://subgraph.test",
        kalshi_api_url="https://kalshi.test/trade-api/v2",
    )
    second_config = PmktConfig.from_values(gamma_api_url="https://gamma-two.test")
    clients = (
        AsyncGammaClient(config=config),
        AsyncGammaClient(config=second_config),
        AsyncClobClient(config=config),
        AsyncPolymarketDataClient(config=config),
        AsyncSubgraphClient(config=config),
        AsyncKalshiClient(config=config),
    )
    try:
        assert [client.base_url for client in clients] == [
            "https://gamma.test",
            "https://gamma-two.test",
            "https://clob.test",
            "https://data.test",
            "https://subgraph.test",
            "https://kalshi.test/trade-api/v2",
        ]
    finally:
        await asyncio.gather(*(client.close() for client in clients))


@pytest.mark.asyncio
async def test_explicit_endpoint_precedes_supplied_config_and_positionals_remain_valid() -> None:
    config = PmktConfig.from_values(gamma_api_url="https://configured.test")
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, request=request, json={})
    )
    limiter = _PassLimiter()
    policy = RequestPolicy(max_attempts=1)
    gamma = AsyncGammaClient("https://explicit.test", transport, limiter, policy, config=config)
    clob = AsyncClobClient("https://explicit.test", transport, limiter)
    data = AsyncPolymarketDataClient("https://explicit.test", transport, limiter)
    subgraph = AsyncSubgraphClient("https://explicit.test", transport, limiter)
    kalshi = AsyncKalshiClient("https://explicit.test", transport=transport, limiter=limiter)
    try:
        assert gamma.base_url == "https://explicit.test"
        assert all(
            client.base_url == "https://explicit.test"
            for client in (clob, data, subgraph, kalshi)
        )
    finally:
        await asyncio.gather(
            gamma.close(), clob.close(), data.close(), subgraph.close(), kalshi.close()
        )


def test_request_policy_attempt_alias_preserves_total_attempt_semantics() -> None:
    legacy = RequestPolicy(2)
    renamed = RequestPolicy(max_attempts=2)

    assert legacy.max_retries == legacy.max_attempts == 2
    assert renamed.max_retries == renamed.max_attempts == 2
    assert legacy.attempts_for("GET") == renamed.attempts_for("GET") == 2
    with pytest.raises(ValueError, match="cannot both"):
        RequestPolicy(max_retries=2, max_attempts=2)
    assert ConfigRequestPolicy is RequestPolicy
    assert ReadAuthenticationRequiredError is CanonicalReadAuthenticationRequiredError


@pytest.mark.parametrize(
    "client_type",
    [
        AsyncGammaClient,
        AsyncClobClient,
        AsyncPolymarketDataClient,
        AsyncSubgraphClient,
        AsyncKalshiClient,
    ],
)
@pytest.mark.parametrize(
    "timeout_s",
    [True, "slow", 0, -1, float("inf"), float("nan")],
)
def test_rest_client_timeout_must_be_finite_positive_numeric(
    client_type: type, timeout_s: object
) -> None:
    error = TypeError if isinstance(timeout_s, (bool, str)) else ValueError
    with pytest.raises(error, match="timeout_s"):
        client_type(
            config=PmktConfig.from_values(),
            timeout_s=timeout_s,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_clob_retries_only_allowlisted_read_posts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", no_sleep)
    counts: dict[str, int] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        counts[path] = counts.get(path, 0) + 1
        status = 500 if counts[path] == 1 else 200
        payload: object = [] if path == "/books" else {"history": {}}
        if path == "/arbitrary":
            payload = {"error": "temporary"}
        return httpx.Response(status, request=request, json=payload)

    client = AsyncClobClient(
        base_url="https://clob.test",
        transport=httpx.MockTransport(handler),
        request_policy=RequestPolicy(max_attempts=2, backoff_base_s=0),
    )
    try:
        assert await client.books(["token"]) == []
        assert await client.batch_prices_history(["token"]) == {}
        with pytest.raises(httpx.HTTPStatusError):
            await client._http.request_json("POST", "/arbitrary", json={})
    finally:
        await client.close()

    assert counts == {"/books": 2, "/batch-prices-history": 2, "/arbitrary": 1}


@pytest.mark.asyncio
async def test_preexpired_operation_does_not_create_or_dispatch_work() -> None:
    created = 0

    def clock() -> float:
        return 2.0

    expiry = OperationExpiry(deadline_monotonic=1.0, _clock=clock)

    def factory():
        nonlocal created
        created += 1

        async def work() -> None:
            raise AssertionError("expired operation ran work")

        return work()

    with pytest.raises(OperationTimeoutError):
        await expiry.run(factory)
    assert created == 0


@pytest.mark.asyncio
async def test_inner_timeout_error_is_not_reclassified() -> None:
    async def work() -> None:
        raise TimeoutError("worker failure")

    with pytest.raises(TimeoutError) as caught:
        await OperationExpiry.after(1).run(work)

    assert type(caught.value) is TimeoutError
    assert str(caught.value) == "worker failure"


@pytest.mark.asyncio
async def test_limiter_expiry_drains_with_zero_dispatched_attempts() -> None:
    limiter = _BlockingLimiter()
    observations = []
    client = HttpClient(
        "https://clob.polymarket.com",
        limiter=limiter,  # type: ignore[arg-type]
        source_venue="polymarket",
        source_service="clob",
    )
    client._get_client()
    expiry = OperationExpiry.after(0.05)
    task = asyncio.create_task(
        client.request_json_observed(
            "GET",
            "/book",
            request_id="limited",
            endpoint_template="/book",
            parameter_allowlist={"token_id"},
            effective_parameters={"token_id": "token"},
            params={"token_id": "token"},
            expiry=expiry,
            record_observation=observations.append,
        )
    )
    await asyncio.wait_for(limiter.entered.wait(), timeout=1)
    with pytest.raises(OperationTimeoutError):
        await asyncio.wait_for(task, timeout=1)

    assert limiter.cancelled.is_set()
    assert len(observations) == 1
    assert observations[0].attempt_count == 0
    assert observations[0].outcome == "timeout"
    await client.close()


@pytest.mark.asyncio
async def test_transport_expiry_drains_and_borrowed_client_is_reusable() -> None:
    transport = _FirstRequestBlocksTransport()
    client = HttpClient("https://example.test", transport=transport)
    client._get_client()
    first = asyncio.create_task(
        client.request_json("GET", "/slow", expiry=OperationExpiry.after(0.05))
    )
    await asyncio.wait_for(transport.entered.wait(), timeout=1)
    with pytest.raises(OperationTimeoutError):
        await asyncio.wait_for(first, timeout=1)

    assert transport.cancelled.is_set()
    assert await client.request_json("GET", "/reused") == {"ok": True}
    await client.close()


@pytest.mark.asyncio
async def test_retry_after_expiry_does_not_dispatch_post_expiry_request() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            503,
            request=request,
            headers={"Retry-After": "3600"},
            json={"error": "temporary"},
        )

    client = HttpClient(
        "https://example.test",
        transport=httpx.MockTransport(handler),
        request_policy=RequestPolicy(max_attempts=2, max_retry_after_s=None),
    )
    client._get_client()
    try:
        with pytest.raises(OperationTimeoutError):
            await asyncio.wait_for(
                client.request_json(
                    "GET", "/markets", expiry=OperationExpiry.after(0.05)
                ),
                timeout=1,
            )
    finally:
        await client.close()

    assert calls == 1


@pytest.mark.asyncio
async def test_concurrent_calls_use_independent_expiries_on_one_client() -> None:
    release_slow = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/slow":
            await release_slow.wait()
        return httpx.Response(200, request=request, json={"path": request.url.path})

    client = HttpClient("https://example.test", transport=httpx.MockTransport(handler))
    client._get_client()
    slow = asyncio.create_task(
        client.request_json("GET", "/slow", expiry=OperationExpiry.after(0.05))
    )
    fast = asyncio.create_task(
        client.request_json("GET", "/fast", expiry=OperationExpiry.after(1.0))
    )
    try:
        assert await asyncio.wait_for(fast, timeout=1) == {"path": "/fast"}
        with pytest.raises(OperationTimeoutError):
            await asyncio.wait_for(slow, timeout=1)
    finally:
        release_slow.set()
        await client.close()


@pytest.mark.asyncio
async def test_caller_cancellation_drains_owned_transport_work() -> None:
    transport = _FirstRequestBlocksTransport(always_block=True)
    client = HttpClient("https://example.test", transport=transport)
    client._get_client()
    task = asyncio.create_task(
        client.request_json("GET", "/slow", expiry=OperationExpiry.after(60))
    )
    await asyncio.wait_for(transport.entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)

    assert transport.cancelled.is_set()
    transport.always_block = False
    assert await client.request_json("GET", "/reused") == {"ok": True}
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("expiry_point", ["decode", "identity"])
async def test_expiry_checkpoint_prevents_late_success(expiry_point: str) -> None:
    now = 0.0

    def clock() -> float:
        return now

    def handler(request: httpx.Request) -> httpx.Response:
        response = httpx.Response(200, request=request, json={"id": "market-1"})
        if expiry_point == "decode":
            original_json = response.json

            def decode() -> object:
                nonlocal now
                value = original_json()
                now = 2.0
                return value

            response.json = decode  # type: ignore[method-assign]
        return response

    def identities(payload: object) -> list[str]:
        nonlocal now
        assert isinstance(payload, dict)
        if expiry_point == "identity":
            now = 2.0
        return [str(payload["id"])]

    client = HttpClient(
        "https://example.test", transport=httpx.MockTransport(handler)
    )
    client._get_client()
    try:
        with pytest.raises(OperationTimeoutError):
            await client.request_json_observed(
                "GET",
                "/market",
                request_id="late-result",
                endpoint_template="/market",
                parameter_allowlist=(),
                expiry=OperationExpiry(deadline_monotonic=1.0, _clock=clock),
                response_identities=identities,
            )
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_observed_request_is_sanitized_and_injected_transport_is_unknown() -> None:
    seen_timeouts: list[dict[str, float]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_timeouts.append(request.extensions["timeout"])
        return httpx.Response(200, request=request, json={"id": "market-1"})

    client = HttpClient(
        "https://user:password@clob.polymarket.com",
        transport=httpx.MockTransport(handler),
        timeout_s=10,
        source_venue="polymarket",
        source_service="clob",
    )
    try:
        data, observation = await client.request_json_observed(
            "GET",
            "/book?token_id=secret-in-url",
            request_id="request-1",
            endpoint_template="/book",
            parameter_allowlist={"token_id"},
            effective_parameters={"token_id": "public-token"},
            headers={"Authorization": "secret"},
            expiry=OperationExpiry.after(1),
            response_identities=lambda payload: [payload["id"]],
        )
    finally:
        await client.close()

    assert data == {"id": "market-1"}
    assert observation.data_scope == "unknown"
    assert observation.transport_origin == "caller_supplied"
    assert observation.origin == "https://clob.polymarket.com"
    assert observation.endpoint_template == "/book"
    assert observation.effective_parameters == (("token_id", "public-token"),)
    assert observation.response_identities == ("market-1",)
    assert observation.attempt_count == 1
    assert observation.status_code == 200
    assert "secret" not in repr(observation)
    assert seen_timeouts and max(seen_timeouts[0].values()) < 10


@pytest.mark.asyncio
async def test_unexpected_observation_error_propagates_without_remote_reclassification() -> None:
    observations = []
    client = HttpClient(
        "https://example.test",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, request=request, json={"ok": True})
        ),
    )

    def programmer_bug(_payload: object) -> list[str]:
        raise RuntimeError("bug in workflow mapper")

    try:
        with pytest.raises(RuntimeError, match="workflow mapper"):
            await client.request_json_observed(
                "GET",
                "/markets",
                request_id="bug",
                endpoint_template="/markets",
                parameter_allowlist=(),
                response_identities=programmer_bug,
                record_observation=observations.append,
            )
    finally:
        await client.close()

    assert len(observations) == 1
    assert observations[0].outcome == "error"


@pytest.mark.asyncio
async def test_invalid_data_from_identity_extractor_is_classified_as_invalid_response() -> None:
    observations = []
    client = HttpClient(
        "https://example.test",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, request=request, json={"id": "wrong"})
        ),
    )

    def invalid_identity(_payload: object) -> list[str]:
        raise InvalidDataError("response identity mismatch")

    try:
        with pytest.raises(InvalidDataError, match="identity mismatch"):
            await client.request_json_observed(
                "GET",
                "/markets/expected",
                request_id="invalid-data",
                endpoint_template="/markets/{market_id}",
                parameter_allowlist=(),
                response_identities=invalid_identity,
                record_observation=observations.append,
            )
    finally:
        await client.close()

    assert len(observations) == 1
    assert observations[0].outcome == "invalid_response"


def test_observation_templates_parameters_and_sources_are_conservative() -> None:
    production = classify_request_source(
        "https://gamma-api.polymarket.com",
        venue="polymarket",
        service="gamma",
        transport_supplied=False,
    )
    custom = classify_request_source(
        "https://gamma-api.polymarket.com/custom",
        venue="polymarket",
        service="gamma",
        transport_supplied=False,
    )
    kalshi = classify_request_source(
        "https://external-api.kalshi.com/trade-api/v2",
        venue="kalshi",
        service="kalshi",
        transport_supplied=False,
    )

    assert production.data_scope == "production"
    assert source_after_response(production, "https://gamma-api.polymarket.com/markets?x=1").data_scope == "production"
    assert custom.data_scope == "unknown"
    assert source_after_response(custom, "https://gamma-api.polymarket.com/custom/markets").data_scope == "unknown"
    assert source_after_response(kalshi, "https://external-api.kalshi.com/portfolio").data_scope == "unknown"
    assert source_after_response(kalshi, "https://user:pw@external-api.kalshi.com/trade-api/v2/markets").data_scope == "unknown"
    assert source_after_response(production, "https://elsewhere.test/markets").data_scope == "unknown"
    assert classify_request_source(
        "http://clob.polymarket.com",
        venue="polymarket",
        service="clob",
        transport_supplied=False,
    ).data_scope == "unknown"
    assert classify_request_source(
        "https://clob.polymarket.com:8443",
        venue="polymarket",
        service="clob",
        transport_supplied=False,
    ).data_scope == "unknown"
    assert classify_request_source(
        "https://[2001:db8::1]:8443/api",
        venue="unknown",
        service="unknown",
        transport_supplied=False,
    ).origin == "https://[2001:db8::1]:8443"

    for unsafe in (
        "https://example.test/markets",
        "/markets?api_key=secret",
        "/markets#fragment",
    ):
        with pytest.raises(ValueError):
            sanitize_endpoint_template(unsafe)
    with pytest.raises(ValueError, match="not allowlisted"):
        sanitize_effective_parameters({"api_key": "secret"}, allowlist={"limit"})


def test_long_allowlisted_selector_sequence_is_preserved_unambiguously() -> None:
    selectors = [f"0x{index:064x}" for index in range(20)]

    parameters = sanitize_effective_parameters(
        {"condition_ids": selectors}, allowlist={"condition_ids"}
    )

    assert len(parameters[0][1]) > 1300
    assert json.loads(parameters[0][1]) == selectors


class _PassLimiter:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _BlockingLimiter:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def __aenter__(self) -> None:
        self.entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.cancelled.set()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FirstRequestBlocksTransport(httpx.AsyncBaseTransport):
    def __init__(self, *, always_block: bool = False) -> None:
        self.always_block = always_block
        self.calls = 0
        self.entered = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        if self.always_block or self.calls == 1:
            self.entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0)
                self.cancelled.set()
        return httpx.Response(200, request=request, json={"ok": True})
