from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import math
from zoneinfo import ZoneInfo

import httpx
from pydantic import ValidationError
import pytest

from pmkt.runtime import OperationExpiry
from pmkt.errors import InvalidDataError, OperationTimeoutError, ResultLimitExceededError
from pmkt.exchanges.polymarket import AsyncClobClient
from pmkt.exchanges.polymarket._workflow import normalize_clob_price_history
from pmkt.models import PriceHistory
from pmkt.records import (
    PolymarketInstrumentRef,
    PolymarketMarketRef,
    PriceHistoryResult,
    RequestObservation,
)


UTC = timezone.utc
BASE = datetime(2026, 1, 2, tzinfo=UTC)


async def _history(
    payload: object,
    *,
    start: datetime = BASE,
    end: datetime = BASE + timedelta(seconds=10),
    instrument: PolymarketInstrumentRef | None = None,
    max_points: int = 100_000,
    invalid_rows: str = "raise",
    raw_json: bool = False,
) -> PriceHistoryResult:
    def handler(_request: httpx.Request) -> httpx.Response:
        if raw_json:
            return httpx.Response(
                200,
                content=json.dumps(payload).encode("utf-8"),
                headers={"content-type": "application/json"},
            )
        return httpx.Response(200, json=payload)

    async with AsyncClobClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
    ) as client:
        return await client.get_price_history(
            instrument or PolymarketInstrumentRef("token"),
            start=start,
            end=end,
            sampling_minutes=1,
            max_points=max_points,
            invalid_rows=invalid_rows,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_price_history_accepts_zero_and_one_and_retains_provenance() -> None:
    first = int(BASE.timestamp())
    result = await _history(
        {"history": [{"t": first + 1, "p": 0}, {"t": first + 2, "p": 1}]}
    )

    assert [point.price for point in result.points] == [0.0, 1.0]
    assert result.price_basis == "venue_defined"
    assert result.dataset == "clob_sampled_prices"
    assert result.coverage.raw_rows == 2
    assert result.coverage.accepted_rows == 2
    assert result.coverage.source_completeness == "unknown"
    assert result.coverage.requests_complete is True
    assert result.provenance.observations[-1].endpoint_template == "/prices-history"
    assert result.provenance.observations[-1].response_identities == ()
    assert result.provenance.raw_responses[0].payload["history"] == [
        {"t": first + 1, "p": 0},
        {"t": first + 2, "p": 1},
    ]


@pytest.mark.asyncio
async def test_price_history_explicit_query_widens_only_integer_strict_bounds() -> None:
    requests: list[httpx.Request] = []
    start = BASE + timedelta(microseconds=250_000)
    end = BASE + timedelta(seconds=3, microseconds=250_000)
    base_s = int(BASE.timestamp())

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "history": [
                    {"t": base_s, "p": 0.1},
                    {"t": base_s + 1, "p": 0.2},
                    {"t": base_s + 3, "p": 0.3},
                    {"t": base_s + 4, "p": 0.4},
                ]
            },
        )

    async with AsyncClobClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
    ) as client:
        result = await client.get_price_history(
            PolymarketInstrumentRef("token"),
            start=start,
            end=end,
            sampling_minutes=7,
        )

    assert len(requests) == 1
    assert dict(requests[0].url.params) == {
        "market": "token",
        "fidelity": "7",
        "startTs": str(base_s),
        "endTs": str(base_s + 4),
    }
    assert [point.timestamp_utc for point in result.points] == [
        BASE + timedelta(seconds=1),
        BASE + timedelta(seconds=3),
    ]
    assert result.coverage.outside_window_rows == 2
    assert result.coverage.queried_windows[0].start_utc == BASE
    assert result.coverage.queried_windows[-1].end_utc == BASE + timedelta(seconds=4)


@pytest.mark.asyncio
async def test_price_history_exact_bounds_are_start_inclusive_end_exclusive() -> None:
    base_s = int(BASE.timestamp())
    result = await _history(
        {
            "history": [
                {"t": base_s - 1, "p": 0.1},
                {"t": base_s, "p": 0.2},
                {"t": base_s + 10, "p": 0.3},
            ]
        }
    )

    assert [point.price for point in result.points] == [0.2]
    assert result.coverage.queried_windows[0].start_utc == BASE - timedelta(seconds=1)
    assert result.coverage.queried_windows[-1].end_utc == BASE + timedelta(seconds=10)
    assert result.coverage.outside_window_rows == 2


@pytest.mark.asyncio
async def test_price_history_reconciles_widened_outside_keys_before_containment() -> None:
    base_s = int(BASE.timestamp())
    payload = {
        "history": [
            {"t": base_s - 1, "p": 0.1},
            {"t": base_s - 1, "p": 0.2},
            {"t": base_s + 1, "p": 0.3},
            {"t": base_s + 10, "p": 0.4},
            {"t": base_s + 10, "p": 0.4},
        ]
    }
    with pytest.raises(InvalidDataError, match="conflicting prices"):
        await _history(payload)

    result = await _history(payload, invalid_rows="report")
    assert [point.price for point in result.points] == [0.3]
    assert result.coverage.raw_rows == 5
    assert result.coverage.accepted_rows == 1
    assert result.coverage.rejected_rows == 2
    assert result.coverage.conflicting_rows == 2
    assert result.coverage.duplicate_rows == 0
    assert result.coverage.outside_window_rows == 2


@pytest.mark.asyncio
async def test_price_history_normalizes_utc_before_fold_ordering() -> None:
    berlin = ZoneInfo("Europe/Berlin")
    earlier_utc_later_wall = datetime(2024, 10, 27, 2, 45, tzinfo=berlin, fold=0)
    later_utc_earlier_wall = datetime(2024, 10, 27, 2, 30, tzinfo=berlin, fold=1)

    result = await _history(
        {"history": []},
        start=earlier_utc_later_wall,
        end=later_utc_earlier_wall,
    )
    assert result.coverage.requested_start_utc < result.coverage.requested_end_utc

    with pytest.raises(ValueError, match="UTC normalization"):
        await _history(
            {"history": []},
            start=later_utc_earlier_wall,
            end=earlier_utc_later_wall,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ({"instrument": PolymarketMarketRef("market")}, TypeError),
        ({"start": BASE.replace(tzinfo=None)}, ValueError),
        ({"end": BASE.replace(tzinfo=None)}, ValueError),
        ({"sampling_minutes": True}, TypeError),
        ({"sampling_minutes": 0}, ValueError),
        ({"max_points": True}, TypeError),
        ({"max_points": 0}, ValueError),
        ({"deadline_s": None}, TypeError),
        ({"deadline_s": math.inf}, ValueError),
        ({"invalid_rows": "ignore"}, ValueError),
    ],
)
async def test_price_history_rejects_invalid_inputs_before_io(
    changes: dict[str, object], error: type[Exception]
) -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"history": []})

    kwargs: dict[str, object] = {
        "instrument": PolymarketInstrumentRef("token"),
        "start": BASE,
        "end": BASE + timedelta(seconds=10),
        "sampling_minutes": 1,
    }
    kwargs.update(changes)
    async with AsyncClobClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(error):
            await client.get_price_history(**kwargs)  # type: ignore[arg-type]
    assert calls == 0


@pytest.mark.asyncio
async def test_price_history_strict_and_report_invalid_rows() -> None:
    base_s = int(BASE.timestamp())
    payload = {
        "history": [
            {"t": base_s + 1, "p": 0.2},
            {"t": base_s + 2, "p": True},
            {"t": base_s + 3, "p": "0.4"},
        ]
    }
    with pytest.raises(InvalidDataError, match="price"):
        await _history(payload)

    result = await _history(payload, invalid_rows="report")
    assert [point.price for point in result.points] == [0.2, 0.4]
    assert result.coverage.raw_rows == 3
    assert result.coverage.rejected_rows == 1
    assert result.issues[0].code == "invalid_row"
    assert result.issues[0].occurrence_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_row",
    [
        "not-an-object",
        {"p": 0.3},
        {"t": 1},
        {"t": True, "p": 0.3},
        {"t": 1.5, "p": 0.3},
        {"t": "1.5", "p": 0.3},
        {"t": 1, "p": "bad"},
        {"t": 1, "p": math.nan},
        {"t": 1, "p": math.inf},
        {"t": 1, "p": -0.01},
        {"t": 1, "p": 1.01},
    ],
)
async def test_price_history_malformed_row_matrix_preserves_adjacent_valids(
    invalid_row: object,
) -> None:
    base_s = int(BASE.timestamp())
    payload = {
        "history": [
            {"t": base_s + 1, "p": 0.2},
            invalid_row,
            {"t": base_s + 3, "p": 0.4},
        ]
    }
    with pytest.raises(InvalidDataError):
        await _history(payload, raw_json=True)

    result = await _history(payload, invalid_rows="report", raw_json=True)
    assert [point.price for point in result.points] == [0.2, 0.4]
    assert result.coverage.raw_rows == 3
    assert result.coverage.rejected_rows == 1


@pytest.mark.asyncio
async def test_price_history_huge_decimal_timestamp_obeys_row_policy() -> None:
    base_s = int(BASE.timestamp())
    payload = {
        "history": [
            {"t": base_s + 1, "p": 0.2},
            {"t": "9" * 5000, "p": 0.3},
            {"t": base_s + 2, "p": 0.4},
        ]
    }
    with pytest.raises(InvalidDataError, match="supported range"):
        await _history(payload)

    result = await _history(payload, invalid_rows="report")
    assert [point.price for point in result.points] == [0.2, 0.4]
    assert result.coverage.rejected_rows == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_rows", ["raise", "report"])
@pytest.mark.parametrize(
    "payload",
    [
        [],
        {},
        {"history": {}},
        {"market": "wrong", "history": []},
        {"market": "token", "asset_id": "other", "history": []},
        {"condition_id": "wrong-condition", "history": []},
        {"parent_market_id": "wrong-parent", "history": []},
    ],
)
async def test_price_history_envelope_and_identity_errors_are_always_fatal(
    payload: object,
    invalid_rows: str,
) -> None:
    instrument = PolymarketInstrumentRef(
        "token",
        market=PolymarketMarketRef("parent", condition_id="condition"),
    )
    with pytest.raises(InvalidDataError):
        await _history(payload, instrument=instrument, invalid_rows=invalid_rows)


@pytest.mark.asyncio
async def test_price_history_matching_identity_hints_are_retained() -> None:
    instrument = PolymarketInstrumentRef(
        "token",
        market=PolymarketMarketRef("parent", condition_id="condition"),
    )
    result = await _history(
        {
            "market": "token",
            "asset_id": "token",
            "conditionId": "condition",
            "parentMarketId": "parent",
            "history": [],
        },
        instrument=instrument,
    )
    assert result.provenance.observations[-1].response_identities == (
        "token_id=token",
        "condition_id=condition",
        "parent_market_id=parent",
    )


@pytest.mark.asyncio
async def test_price_history_equal_duplicates_collapse() -> None:
    base_s = int(BASE.timestamp())
    result = await _history(
        {
            "history": [
                {"t": base_s + 2, "p": 0.3},
                {"t": base_s + 1, "p": 0.2},
                {"t": base_s + 1, "p": 0.2},
            ]
        }
    )
    assert [point.price for point in result.points] == [0.2, 0.3]
    assert result.coverage.duplicate_rows == 1
    assert result.coverage.rejected_rows == 0


@pytest.mark.asyncio
async def test_price_history_report_removes_entire_conflicting_key_and_recurrences() -> None:
    base_s = int(BASE.timestamp())
    payload = {
        "history": [
            {"t": base_s + 1, "p": 0.2},
            {"t": base_s + 1, "p": 0.2},
            {"t": base_s + 1, "p": 0.3},
            {"t": base_s + 2, "p": 0.4},
            {"t": base_s + 1, "p": 0.2},
        ]
    }
    with pytest.raises(InvalidDataError, match="conflicting prices"):
        await _history(payload)

    result = await _history(payload, invalid_rows="report", max_points=1)
    assert [point.price for point in result.points] == [0.4]
    assert result.coverage.raw_rows == 5
    assert result.coverage.accepted_rows == 1
    assert result.coverage.rejected_rows == 4
    assert result.coverage.conflicting_rows == 4
    assert result.coverage.duplicate_rows == 0
    assert result.issues[0].occurrence_count == 4


@pytest.mark.asyncio
async def test_price_history_distinguishes_empty_and_all_rejected() -> None:
    empty = await _history({"history": []}, invalid_rows="report")
    rejected = await _history(
        {"history": [{"t": "bad", "p": 0.2}]}, invalid_rows="report"
    )

    assert empty.points == ()
    assert empty.coverage.raw_rows == 0
    assert empty.coverage.rejected_rows == 0
    assert rejected.points == ()
    assert rejected.coverage.raw_rows == 1
    assert rejected.coverage.rejected_rows == 1


@pytest.mark.asyncio
async def test_price_history_output_cap_raises_without_truncation() -> None:
    base_s = int(BASE.timestamp())
    with pytest.raises(ResultLimitExceededError, match="max_points=1"):
        await _history(
            {
                "history": [
                    {"t": base_s + 1, "p": 0.2},
                    {"t": base_s + 2, "p": 0.3},
                ]
            },
            max_points=1,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("termination", ["timeout", "cancel"])
async def test_price_history_termination_drains_transport_and_client_is_reusable(
    termination: str,
) -> None:
    calls = 0
    entered = asyncio.Event()
    drained = asyncio.Event()
    release = asyncio.Event()
    base_s = int(BASE.timestamp())

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 2:
            entered.set()
            try:
                await release.wait()
            finally:
                await asyncio.sleep(0)
                drained.set()
        return httpx.Response(200, json={"history": [{"t": base_s + 1, "p": 0.2}]})

    client = AsyncClobClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
    )
    await client.__aenter__()
    try:
        warm = await client.get_price_history(
            PolymarketInstrumentRef("token"),
            start=BASE,
            end=BASE + timedelta(seconds=10),
            sampling_minutes=1,
            deadline_s=1.0,
        )
        assert [point.price for point in warm.points] == [0.2]
        task = asyncio.create_task(
            client.get_price_history(
                PolymarketInstrumentRef("token"),
                start=BASE,
                end=BASE + timedelta(seconds=10),
                sampling_minutes=1,
                deadline_s=0.1 if termination == "timeout" else 5.0,
            )
        )
        await asyncio.wait_for(entered.wait(), timeout=2.0)
        if termination == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(OperationTimeoutError):
                await asyncio.wait_for(task, timeout=2.0)
        assert drained.is_set()
        reused = await client.get_price_history(
            PolymarketInstrumentRef("token"),
            start=BASE,
            end=BASE + timedelta(seconds=10),
            sampling_minutes=1,
            deadline_s=1.0,
        )
    finally:
        await client.close()
    assert [point.price for point in reused.points] == [0.2]
    assert calls == 3


@pytest.mark.asyncio
async def test_native_history_model_and_parameter_behavior_remain_unchanged() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"history": [{"t": 123, "p": 0.5}]})

    async with AsyncClobClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
    ) as client:
        native = await client.prices_history(
            "token", interval="1d", fidelity=5, start_ts=100, end_ts=200
        )

    assert isinstance(native, PriceHistory)
    assert native.history[0].t == 123
    assert len(requests) == 1
    assert "interval" not in requests[0].url.params
    assert requests[0].url.params["market"] == "token"
    assert requests[0].url.params["fidelity"] == "5"


@pytest.mark.asyncio
async def test_report_mode_does_not_weaken_native_history_model_validation() -> None:
    base_s = int(BASE.timestamp())
    payload = {
        "history": [
            {"t": base_s + 1, "p": 0.2},
            {"t": "bad", "p": 0.3},
            {"t": base_s + 3, "p": 0.4},
        ]
    }

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    async with AsyncClobClient(
        base_url="https://offline.invalid",
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(ValidationError):
            await client.prices_history("token", interval="1d")
        normalized = await client.get_price_history(
            PolymarketInstrumentRef("token"),
            start=BASE,
            end=BASE + timedelta(seconds=10),
            sampling_minutes=1,
            invalid_rows="report",
        )
    assert [point.price for point in normalized.points] == [0.2, 0.4]


@pytest.mark.asyncio
async def test_price_history_explicit_endpoint_ignores_poisoned_global_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def poisoned_config() -> None:
        raise AssertionError("global config must not be read")

    monkeypatch.setattr("pmkt.config.PmktConfig.from_env", poisoned_config)
    result = await _history({"history": []})
    assert result.points == ()


@pytest.mark.asyncio
async def test_price_history_issues_bound_examples_but_preserve_counts() -> None:
    result = await _history(
        {"history": [{"t": "bad", "p": 0.2} for _ in range(25)]},
        invalid_rows="report",
    )
    assert result.issues[0].occurrence_count == 25
    assert len(result.issues[0].examples) == 20


def test_price_history_duplicate_heavy_normalization_checks_expiry() -> None:
    clock_calls = 0

    def clock() -> float:
        nonlocal clock_calls
        clock_calls += 1
        return 0.0 if clock_calls == 1 else 1.0

    expiry = OperationExpiry(deadline_monotonic=1.0, _clock=clock)
    base_s = int(BASE.timestamp())
    observation = RequestObservation(
        request_id="history-test",
        venue="polymarket",
        data_scope="synthetic",
        transport_origin="caller_supplied",
        origin="https://offline.invalid",
        endpoint_template="/prices-history",
        effective_parameters=(),
        started_at_utc=BASE,
        received_at_utc=BASE,
        attempt_count=1,
        outcome="success",
        status_code=200,
    )

    with pytest.raises(OperationTimeoutError):
        normalize_clob_price_history(
            {"history": [{"t": base_s + 1, "p": 0.2}] * 1000},
            instrument=PolymarketInstrumentRef("token"),
            requested_start_utc=BASE,
            requested_end_utc=BASE + timedelta(seconds=10),
            queried_start_utc=BASE - timedelta(seconds=1),
            queried_end_utc=BASE + timedelta(seconds=10),
            sampling_minutes=1,
            max_points=1,
            invalid_rows="report",
            observation=observation,
            expiry=expiry,
        )
    assert clock_calls == 2


@pytest.mark.asyncio
async def test_price_history_lazy_table_conversions_and_empty_utc_schema() -> None:
    base_s = int(BASE.timestamp())
    result = await _history(
        {"history": [{"t": base_s + 1, "p": 0.25}]}
    )
    arrow = result.to_arrow()
    frame = result.to_pandas()

    assert arrow.schema.field("timestamp_utc").type.tz == "UTC"
    assert arrow.schema.metadata[b"dataset"] == b"clob_sampled_prices"
    assert frame.attrs["instrument_token_id"] == "token"
    assert str(frame["timestamp_utc"].dtype) == "datetime64[ns, UTC]"
    assert frame["price"].tolist() == [0.25]

    empty = await _history({"history": []})
    empty_arrow = empty.to_arrow()
    empty_frame = empty.to_pandas()
    assert empty_arrow.num_rows == 0
    assert empty_arrow.schema.field("timestamp_utc").type.tz == "UTC"
    assert str(empty_frame["timestamp_utc"].dtype) == "datetime64[ns, UTC]"
    assert str(empty_frame["price"].dtype) == "float64"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "requested,returned", [("0xABCDEF", "0xabcdef"), ("0xabcdef", "0xABCDEF")]
)
async def test_price_history_condition_identity_is_case_insensitive(
    requested, returned
):
    instrument = PolymarketInstrumentRef(
        "token", market=PolymarketMarketRef("gamma-id", condition_id=requested)
    )
    result = await _history(
        {"history": [], "condition_id": returned}, instrument=instrument
    )
    assert result.instrument == instrument
    assert f"condition_id={returned}" in result.provenance.observations[-1].response_identities
