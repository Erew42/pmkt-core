from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

import pmkt.resolution.cache as cache_module
from pmkt.data.canonical import market_resolution_row
from pmkt.data.registry import MARKET_RESOLUTION_COLUMNS
from pmkt.resolution.models import (
    CONFIDENCE_CANONICAL,
    CONFIDENCE_INCONSISTENT,
    CONFIDENCE_METADATA_ONLY,
    CONFIDENCE_PLATFORM_CONFIRMED,
    CONFIDENCE_UNAVAILABLE,
    RESOLVER_VERSION,
    RESULT_TYPE_BINARY,
    RESULT_TYPE_SCALAR,
    RESULT_TYPE_UNKNOWN,
    Payout,
    STATE_FINAL,
    STATE_INCONSISTENT,
    STATE_METADATA_ONLY,
    STATE_OPEN,
    STATE_UNAVAILABLE,
    ResolutionRecord,
)


class FakeGamma:
    async def market(self, market_id: str) -> dict[str, Any]:
        return {}


class FakeClob:
    async def clob_market_info(self, condition_id: str) -> dict[str, Any]:
        return {}


class FakeKalshiClient:
    def __init__(self, payloads: dict[str, dict[str, Any]] | None = None) -> None:
        self.payloads = payloads or {}

    async def market(self, ticker: str) -> dict[str, Any]:
        return self.payloads.get(ticker, {"ticker": ticker})

    async def historical_market(self, ticker: str) -> dict[str, Any]:
        return {"ticker": ticker}


def _write_polymarket(path: Path, rows: list[dict[str, Any]]) -> None:
    pd.DataFrame(
        rows,
        columns=[
            "schema_version",
            "market_id",
            "question",
            "condition_id",
            "outcome_labels_json",
            "outcome_prices_json",
            "uma_resolution_status",
            "close_time",
            "updated_time",
        ],
    ).to_parquet(path, index=False)


def _write_kalshi(path: Path, rows: list[dict[str, Any]]) -> None:
    pd.DataFrame(
        rows,
        columns=[
            "schema_version",
            "exchange",
            "market_key",
            "question",
            "status",
            "result",
            "settlement_value_dollars",
            "settlement_ts",
            "close_time",
            "updated_time",
        ],
    ).to_parquet(path, index=False)


def _write_existing(output_dir: Path, rows: list[dict[str, Any]]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=MARKET_RESOLUTION_COLUMNS).to_parquet(
        output_dir / "market_resolutions.parquet",
        index=False,
    )


def _resolution_row(
    *,
    platform: str,
    market_key: str,
    result: str = "yes",
    state: str = STATE_FINAL,
    confidence: str = CONFIDENCE_CANONICAL,
    resolver_version: str | None = RESOLVER_VERSION,
) -> dict[str, Any]:
    return market_resolution_row(
        platform=platform,
        market_key=market_key,
        input_identifier=market_key,
        resolution_state=state,
        result_type=RESULT_TYPE_BINARY if state == STATE_FINAL else RESULT_TYPE_UNKNOWN,
        confidence=confidence,
        canonical_source="test",
        result=result,
        winner=result if state == STATE_FINAL else None,
        payouts_json="[]",
        source_observations_json="[]",
        observed_at_utc="2026-01-01T00:00:00+00:00",
        resolver_version=resolver_version,
    )


def _final_record(platform: str, market_key: str, result: str) -> ResolutionRecord:
    return ResolutionRecord(
        platform=platform,
        market_key=market_key,
        input_identifier=market_key,
        resolution_state=STATE_FINAL,
        result_type=RESULT_TYPE_BINARY,
        confidence=CONFIDENCE_CANONICAL,
        canonical_source="test_resolver",
        result=result,
        winner=result,
        observed_at_utc="2026-01-02T00:00:00+00:00",
    )


def _metadata_record(platform: str, market_key: str) -> ResolutionRecord:
    return ResolutionRecord(
        platform=platform,
        market_key=market_key,
        input_identifier=market_key,
        resolution_state=STATE_METADATA_ONLY,
        result_type=RESULT_TYPE_UNKNOWN,
        confidence=CONFIDENCE_METADATA_ONLY,
        observed_at_utc="2026-01-02T00:00:00+00:00",
    )


def _unavailable_record(platform: str, market_key: str) -> ResolutionRecord:
    return ResolutionRecord(
        platform=platform,
        market_key=market_key,
        input_identifier=market_key,
        resolution_state=STATE_UNAVAILABLE,
        result_type=RESULT_TYPE_UNKNOWN,
        confidence=CONFIDENCE_UNAVAILABLE,
        observed_at_utc="2026-01-02T00:00:00+00:00",
    )


@pytest.mark.asyncio
async def test_cache_reuse_is_universe_and_resolver_version_aware(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    matches_path = tmp_path / "matches.parquet"
    output_dir = tmp_path / "out"
    _write_polymarket(
        polymarket_path,
        [
            {"schema_version": "polymarket_market_snapshot.v1", "market_id": "pm-keep", "question": "Keep?"},
            {"schema_version": "polymarket_market_snapshot.v1", "market_id": "pm-old", "question": "Old?"},
            {"schema_version": "polymarket_market_snapshot.v1", "market_id": "pm-out", "question": "Out?"},
        ],
    )
    _write_kalshi(kalshi_path, [])
    pd.DataFrame(
        [
            {"polymarket_market_key": "pm-keep"},
            {"polymarket_market_key": "pm-old"},
        ]
    ).to_parquet(matches_path, index=False)
    _write_existing(
        output_dir,
        [
            _resolution_row(platform="polymarket", market_key="pm-keep", result="yes"),
            _resolution_row(platform="polymarket", market_key="pm-out", result="yes"),
            _resolution_row(
                platform="polymarket",
                market_key="pm-old",
                result="yes",
                resolver_version="market_resolution_resolver.v0",
            ),
        ],
    )
    calls: list[str] = []

    class FakePolymarketResolver:
        def __init__(self, **_: Any) -> None:
            pass

        async def resolve(self, market_key: str, *, snapshot: dict[str, Any]) -> ResolutionRecord:
            calls.append(market_key)
            return _final_record("polymarket", market_key, "no")

    monkeypatch.setattr(cache_module, "PolymarketResolutionResolver", FakePolymarketResolver)

    await cache_module.resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        matches_path=matches_path,
        output_dir=output_dir,
        gamma_client=FakeGamma(),
        clob_client=FakeClob(),
        kalshi_client=FakeKalshiClient(),
    )

    written = pd.read_parquet(output_dir / "market_resolutions.parquet")
    assert set(written["market_key"]) == {"pm-keep", "pm-old"}
    assert calls == ["pm-old"]
    results = written.set_index("market_key")["result"].to_dict()
    assert results == {"pm-keep": "yes", "pm-old": "no"}


@pytest.mark.asyncio
async def test_cache_migrates_unsafe_v1_polymarket_final_to_corrected_metadata_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    _write_polymarket(
        polymarket_path,
        [
            {
                "schema_version": "polymarket_market_snapshot.v1",
                "market_id": "pm-bad",
                "question": "Bad old final?",
            }
        ],
    )
    _write_kalshi(kalshi_path, [])
    _write_existing(
        output_dir,
        [
            _resolution_row(
                platform="polymarket",
                market_key="pm-bad",
                result="yes",
                resolver_version="market_resolution_resolver.v1",
            )
        ],
    )
    calls: list[str] = []

    class FakePolymarketResolver:
        def __init__(self, **_: Any) -> None:
            pass

        async def resolve(self, market_key: str, *, snapshot: dict[str, Any]) -> ResolutionRecord:
            calls.append(market_key)
            return _metadata_record("polymarket", market_key)

    monkeypatch.setattr(cache_module, "PolymarketResolutionResolver", FakePolymarketResolver)

    await cache_module.resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        gamma_client=FakeGamma(),
        clob_client=FakeClob(),
        kalshi_client=FakeKalshiClient(),
    )

    [row] = pd.read_parquet(output_dir / "market_resolutions.parquet").to_dict("records")
    assert calls == ["pm-bad"]
    assert row["resolver_version"] == RESOLVER_VERSION
    assert row["resolution_state"] == STATE_METADATA_ONLY
    assert row["confidence"] == CONFIDENCE_METADATA_ONLY
    assert row["result"] is None
    assert row["winner"] is None


@pytest.mark.asyncio
async def test_cache_refresh_migrates_unsafe_v1_kalshi_final_to_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    _write_polymarket(polymarket_path, [])
    _write_kalshi(
        kalshi_path,
        [
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "exchange": "kalshi",
                "market_key": "KXBAD",
                "question": "Bad old final?",
            }
        ],
    )
    _write_existing(
        output_dir,
        [
            _resolution_row(
                platform="kalshi",
                market_key="KXBAD",
                result="yes",
                resolver_version="market_resolution_resolver.v1",
            )
        ],
    )

    class FakeKalshiResolver:
        def __init__(self, client: Any | None = None) -> None:
            pass

        async def resolve(self, market_key: str, *, snapshot: dict[str, Any]) -> ResolutionRecord:
            return _unavailable_record("kalshi", market_key)

    monkeypatch.setattr(cache_module, "KalshiResolutionResolver", FakeKalshiResolver)

    await cache_module.resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        refresh=True,
        gamma_client=FakeGamma(),
        clob_client=FakeClob(),
        kalshi_client=FakeKalshiClient(),
    )

    [row] = pd.read_parquet(output_dir / "market_resolutions.parquet").to_dict("records")
    assert row["resolver_version"] == RESOLVER_VERSION
    assert row["resolution_state"] == STATE_UNAVAILABLE
    assert row["confidence"] == CONFIDENCE_UNAVAILABLE
    assert row["result"] is None
    assert row["winner"] is None


@pytest.mark.asyncio
async def test_cache_migrates_unsafe_v1_conflicting_canonical_to_inconsistent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    _write_polymarket(polymarket_path, [])
    _write_kalshi(
        kalshi_path,
        [
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "exchange": "kalshi",
                "market_key": "KXCONFLICT",
                "question": "Conflict?",
            }
        ],
    )
    _write_existing(
        output_dir,
        [
            _resolution_row(
                platform="kalshi",
                market_key="KXCONFLICT",
                result="yes",
                resolver_version="market_resolution_resolver.v1",
            )
        ],
    )

    class FakeKalshiResolver:
        def __init__(self, client: Any | None = None) -> None:
            pass

        async def resolve(self, market_key: str, *, snapshot: dict[str, Any]) -> ResolutionRecord:
            return _final_record("kalshi", market_key, "no")

    monkeypatch.setattr(cache_module, "KalshiResolutionResolver", FakeKalshiResolver)

    await cache_module.resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        gamma_client=FakeGamma(),
        clob_client=FakeClob(),
        kalshi_client=FakeKalshiClient(),
    )

    [row] = pd.read_parquet(output_dir / "market_resolutions.parquet").to_dict("records")
    assert row["resolver_version"] == RESOLVER_VERSION
    assert row["resolution_state"] == STATE_INCONSISTENT
    assert row["confidence"] == CONFIDENCE_INCONSISTENT
    assert row["error_type"] == "MarketResolutionCacheConflict"
    assert "yes" in row["error_message"]
    assert "no" in row["error_message"]


def test_cache_carry_forward_observations_are_deduped_and_bounded() -> None:
    existing = _resolution_row(platform="kalshi", market_key="KXOBS", result="yes")
    existing["source_observations_json"] = [
        {"source": f"source-{index}", "confidence": CONFIDENCE_UNAVAILABLE}
        for index in range(40)
    ]
    attempted = _resolution_row(
        platform="kalshi",
        market_key="KXOBS",
        state=STATE_UNAVAILABLE,
        confidence=CONFIDENCE_UNAVAILABLE,
    )
    attempted["source_observations_json"] = [
        {"source": "source-39", "confidence": CONFIDENCE_UNAVAILABLE},
        {"source": "new-source", "confidence": CONFIDENCE_UNAVAILABLE},
    ]

    carried = cache_module._carry_forward_final(existing, attempted)
    observations = carried["source_observations_json"]

    assert len(observations) == 32
    assert observations[-1]["source"] == "new-source"
    assert "source-0" not in {observation["source"] for observation in observations}
    assert len({cache_module._stable_json(observation) for observation in observations}) == 32


@pytest.mark.asyncio
async def test_cache_duplicate_snapshots_choose_latest_timestamp(tmp_path: Path) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    _write_polymarket(polymarket_path, [])
    _write_kalshi(
        kalshi_path,
        [
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "exchange": "kalshi",
                "market_key": "KXDUP",
                "question": "Duplicate?",
                "status": "settled",
                "settlement_value_dollars": "1",
                "settlement_ts": "2026-01-02T00:00:00Z",
                "updated_time": "2026-01-02T00:00:00Z",
            },
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "exchange": "kalshi",
                "market_key": "KXDUP",
                "question": "Duplicate?",
                "status": "open",
                "updated_time": "2026-01-01T00:00:00Z",
            },
        ],
    )

    await cache_module.resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        gamma_client=FakeGamma(),
        clob_client=FakeClob(),
        kalshi_client=FakeKalshiClient(),
    )

    written = pd.read_parquet(output_dir / "market_resolutions.parquet")
    assert written.to_dict("records")[0]["market_key"] == "KXDUP"
    assert written.to_dict("records")[0]["resolution_state"] == STATE_METADATA_ONLY
    assert written.to_dict("records")[0]["confidence"] == CONFIDENCE_METADATA_ONLY
    assert written.to_dict("records")[0]["result"] == "yes"


@pytest.mark.asyncio
async def test_cache_duplicate_snapshots_do_not_use_close_time_as_freshness(
    tmp_path: Path,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    _write_polymarket(polymarket_path, [])
    _write_kalshi(
        kalshi_path,
        [
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "exchange": "kalshi",
                "market_key": "KXDUP",
                "question": "Duplicate?",
                "status": "open",
                "close_time": "2030-01-01T00:00:00Z",
                "updated_time": "2026-01-01T00:00:00Z",
            },
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "exchange": "kalshi",
                "market_key": "KXDUP",
                "question": "Duplicate?",
                "status": "settled",
                "settlement_value_dollars": "1",
                "settlement_ts": "2026-01-02T00:00:00Z",
                "close_time": "2026-01-01T00:00:00Z",
                "updated_time": "2026-01-02T00:00:00Z",
            },
        ],
    )

    await cache_module.resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        gamma_client=FakeGamma(),
        clob_client=FakeClob(),
        kalshi_client=FakeKalshiClient(),
    )

    written = pd.read_parquet(output_dir / "market_resolutions.parquet")
    row = written.to_dict("records")[0]
    assert row["market_key"] == "KXDUP"
    assert row["resolution_state"] == STATE_METADATA_ONLY
    assert row["confidence"] == CONFIDENCE_METADATA_ONLY
    assert row["result"] == "yes"


def test_cache_duplicate_snapshot_tie_breaker_ignores_close_time_without_freshness() -> None:
    rows = [
        {
            "market_key": "KXDUP",
            "question": "Duplicate?",
            "status": "open",
            "close_time": "2030-01-01T00:00:00Z",
            "payload_marker": "future-close",
        },
        {
            "market_key": "KXDUP",
            "question": "Duplicate?",
            "status": "settled",
            "close_time": "2026-01-01T00:00:00Z",
            "payload_marker": "stable-json-winner",
        },
    ]

    selected = cache_module._unique_market_rows(pd.DataFrame(rows), "market_key")
    swapped_close_times = [dict(rows[0], close_time=rows[1]["close_time"]), dict(rows[1], close_time=rows[0]["close_time"])]
    selected_after_close_time_swap = cache_module._unique_market_rows(
        pd.DataFrame(swapped_close_times),
        "market_key",
    )

    assert selected == [rows[1]]
    assert selected_after_close_time_swap == [swapped_close_times[1]]


@pytest.mark.asyncio
async def test_cache_duplicate_polymarket_snapshots_do_not_use_close_time_as_freshness(
    tmp_path: Path,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    _write_polymarket(
        polymarket_path,
        [
            {
                "schema_version": "polymarket_market_snapshot.v1",
                "market_id": "pm-dup",
                "question": "Duplicate?",
                "condition_id": "0xabc",
                "outcome_labels_json": ["yes", "no"],
                "status": "open",
                "close_time": "2030-01-01T00:00:00Z",
                "updated_time": "2026-01-01T00:00:00Z",
            },
            {
                "schema_version": "polymarket_market_snapshot.v1",
                "market_id": "pm-dup",
                "question": "Duplicate?",
                "condition_id": "0xabc",
                "outcome_labels_json": ["yes", "no"],
                "outcome_prices_json": ["1", "0"],
                "uma_resolution_status": "resolved",
                "close_time": "2026-01-01T00:00:00Z",
                "updated_time": "2026-01-02T00:00:00Z",
            },
        ],
    )
    _write_kalshi(kalshi_path, [])

    await cache_module.resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        gamma_client=FakeGamma(),
        clob_client=FakeClob(),
        kalshi_client=FakeKalshiClient(),
    )

    written = pd.read_parquet(output_dir / "market_resolutions.parquet")
    row = written.to_dict("records")[0]
    assert row["platform"] == "polymarket"
    assert row["market_key"] == "pm-dup"
    assert row["resolution_state"] == STATE_METADATA_ONLY
    assert row["confidence"] == CONFIDENCE_METADATA_ONLY
    assert row["result"] is None


@pytest.mark.asyncio
async def test_cache_refresh_carries_current_final_over_weak_downgrade(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    _write_polymarket(polymarket_path, [])
    _write_kalshi(
        kalshi_path,
        [
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "exchange": "kalshi",
                "market_key": "KXFINAL",
                "question": "Final?",
            }
        ],
    )
    _write_existing(output_dir, [_resolution_row(platform="kalshi", market_key="KXFINAL", result="yes")])

    class FakeKalshiResolver:
        def __init__(self, client: Any | None = None) -> None:
            pass

        async def resolve(self, market_key: str, *, snapshot: dict[str, Any]) -> ResolutionRecord:
            return ResolutionRecord(
                platform="kalshi",
                market_key=market_key,
                input_identifier=market_key,
                resolution_state=STATE_OPEN,
                confidence=CONFIDENCE_UNAVAILABLE,
                observed_at_utc="2026-01-02T00:00:00+00:00",
            )

    monkeypatch.setattr(cache_module, "KalshiResolutionResolver", FakeKalshiResolver)

    await cache_module.resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        refresh=True,
        gamma_client=FakeGamma(),
        clob_client=FakeClob(),
        kalshi_client=FakeKalshiClient(),
    )

    [row] = pd.read_parquet(output_dir / "market_resolutions.parquet").to_dict("records")
    assert row["resolution_state"] == STATE_FINAL
    assert row["confidence"] == CONFIDENCE_CANONICAL
    assert row["result"] == "yes"


@pytest.mark.asyncio
async def test_cache_refresh_carries_canonical_final_over_platform_confirmed_final(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    _write_polymarket(
        polymarket_path,
        [
            {
                "schema_version": "polymarket_market_snapshot.v1",
                "market_id": "pm-final",
                "question": "Final?",
            }
        ],
    )
    _write_kalshi(kalshi_path, [])
    _write_existing(
        output_dir,
        [_resolution_row(platform="polymarket", market_key="pm-final", result="yes")],
    )

    class FakePolymarketResolver:
        def __init__(self, **_: Any) -> None:
            pass

        async def resolve(self, market_key: str, *, snapshot: dict[str, Any]) -> ResolutionRecord:
            return ResolutionRecord(
                platform="polymarket",
                market_key=market_key,
                input_identifier=market_key,
                resolution_state=STATE_FINAL,
                result_type=RESULT_TYPE_BINARY,
                confidence=CONFIDENCE_PLATFORM_CONFIRMED,
                canonical_source="polymarket_clob",
                result="no",
                winner="no",
                observed_at_utc="2026-01-02T00:00:00+00:00",
            )

    monkeypatch.setattr(cache_module, "PolymarketResolutionResolver", FakePolymarketResolver)

    await cache_module.resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        refresh=True,
        gamma_client=FakeGamma(),
        clob_client=FakeClob(),
        kalshi_client=FakeKalshiClient(),
    )

    [row] = pd.read_parquet(output_dir / "market_resolutions.parquet").to_dict("records")
    assert row["resolution_state"] == STATE_FINAL
    assert row["confidence"] == CONFIDENCE_CANONICAL
    assert row["result"] == "yes"


@pytest.mark.asyncio
async def test_cache_refresh_marks_conflicting_canonical_finals_inconsistent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    _write_polymarket(polymarket_path, [])
    _write_kalshi(
        kalshi_path,
        [
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "exchange": "kalshi",
                "market_key": "KXCONFLICT",
                "question": "Conflict?",
            }
        ],
    )
    _write_existing(output_dir, [_resolution_row(platform="kalshi", market_key="KXCONFLICT", result="yes")])

    class FakeKalshiResolver:
        def __init__(self, client: Any | None = None) -> None:
            pass

        async def resolve(self, market_key: str, *, snapshot: dict[str, Any]) -> ResolutionRecord:
            return _final_record("kalshi", market_key, "no")

    monkeypatch.setattr(cache_module, "KalshiResolutionResolver", FakeKalshiResolver)

    await cache_module.resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        refresh=True,
        gamma_client=FakeGamma(),
        clob_client=FakeClob(),
        kalshi_client=FakeKalshiClient(),
    )

    [row] = pd.read_parquet(output_dir / "market_resolutions.parquet").to_dict("records")
    assert row["resolution_state"] == STATE_INCONSISTENT
    assert row["confidence"] == CONFIDENCE_INCONSISTENT
    assert row["error_type"] == "MarketResolutionCacheConflict"
    assert "yes" in row["error_message"]
    assert "no" in row["error_message"]


@pytest.mark.asyncio
async def test_cache_refresh_accepts_matching_canonical_terminal_forms(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    _write_polymarket(polymarket_path, [])
    _write_kalshi(
        kalshi_path,
        [
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "exchange": "kalshi",
                "market_key": "KXMATCH",
                "question": "Match?",
            }
        ],
    )
    existing = _resolution_row(platform="kalshi", market_key="KXMATCH", result="yes")
    existing["winner"] = None
    _write_existing(output_dir, [existing])

    class FakeKalshiResolver:
        def __init__(self, client: Any | None = None) -> None:
            pass

        async def resolve(self, market_key: str, *, snapshot: dict[str, Any]) -> ResolutionRecord:
            return _final_record("kalshi", market_key, "yes")

    monkeypatch.setattr(cache_module, "KalshiResolutionResolver", FakeKalshiResolver)

    await cache_module.resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        refresh=True,
        gamma_client=FakeGamma(),
        clob_client=FakeClob(),
        kalshi_client=FakeKalshiClient(),
    )

    [row] = pd.read_parquet(output_dir / "market_resolutions.parquet").to_dict("records")
    assert row["resolution_state"] == STATE_FINAL
    assert row["confidence"] == CONFIDENCE_CANONICAL
    assert row["result"] == "yes"
    assert pd.isna(row["error_type"])


@pytest.mark.asyncio
async def test_cache_refresh_accepts_equivalent_zero_scalar_terminals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    _write_polymarket(polymarket_path, [])
    _write_kalshi(
        kalshi_path,
        [
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "exchange": "kalshi",
                "market_key": "KXZERO",
                "question": "Zero?",
            }
        ],
    )
    existing = _resolution_row(platform="kalshi", market_key="KXZERO", result="0")
    existing["winner"] = None
    existing["result_type"] = RESULT_TYPE_SCALAR
    existing["settlement_value_dollars"] = "0"
    existing["payouts_json"] = [{"outcome": "scalar", "payout": "0"}]
    _write_existing(output_dir, [existing])

    class FakeKalshiResolver:
        def __init__(self, client: Any | None = None) -> None:
            pass

        async def resolve(self, market_key: str, *, snapshot: dict[str, Any]) -> ResolutionRecord:
            return ResolutionRecord(
                platform="kalshi",
                market_key=market_key,
                input_identifier=market_key,
                resolution_state=STATE_FINAL,
                result_type=RESULT_TYPE_SCALAR,
                confidence=CONFIDENCE_CANONICAL,
                canonical_source="test_resolver",
                result="0.0",
                settlement_value_dollars="0.0",
                payouts=[Payout(outcome="scalar", payout="0.0")],
                observed_at_utc="2026-01-02T00:00:00+00:00",
            )

    monkeypatch.setattr(cache_module, "KalshiResolutionResolver", FakeKalshiResolver)

    await cache_module.resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        refresh=True,
        gamma_client=FakeGamma(),
        clob_client=FakeClob(),
        kalshi_client=FakeKalshiClient(),
    )

    [row] = pd.read_parquet(output_dir / "market_resolutions.parquet").to_dict("records")
    assert row["resolution_state"] == STATE_FINAL
    assert row["confidence"] == CONFIDENCE_CANONICAL
    assert row["settlement_value_dollars"] == "0.0"
    assert pd.isna(row["error_type"])


@pytest.mark.asyncio
async def test_cache_refresh_preserves_existing_cache_conflict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    _write_polymarket(polymarket_path, [])
    _write_kalshi(
        kalshi_path,
        [
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "exchange": "kalshi",
                "market_key": "KXCONFLICT",
                "question": "Conflict?",
            }
        ],
    )
    _write_existing(
        output_dir,
        [
            {
                **_resolution_row(
                    platform="kalshi",
                    market_key="KXCONFLICT",
                    state=STATE_INCONSISTENT,
                    confidence=CONFIDENCE_INCONSISTENT,
                ),
                "error_type": "MarketResolutionCacheConflict",
                "error_message": "Existing canonical result yes conflicts with refreshed canonical result no",
            }
        ],
    )

    class FakeKalshiResolver:
        def __init__(self, client: Any | None = None) -> None:
            pass

        async def resolve(self, market_key: str, *, snapshot: dict[str, Any]) -> ResolutionRecord:
            return _final_record("kalshi", market_key, "no")

    monkeypatch.setattr(cache_module, "KalshiResolutionResolver", FakeKalshiResolver)

    await cache_module.resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        refresh=True,
        gamma_client=FakeGamma(),
        clob_client=FakeClob(),
        kalshi_client=FakeKalshiClient(),
    )

    [row] = pd.read_parquet(output_dir / "market_resolutions.parquet").to_dict("records")
    assert row["resolution_state"] == STATE_INCONSISTENT
    assert row["confidence"] == CONFIDENCE_INCONSISTENT
    assert row["error_type"] == "MarketResolutionCacheConflict"


@pytest.mark.asyncio
async def test_cache_resolver_failure_isolated_to_market(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    _write_polymarket(polymarket_path, [])
    _write_kalshi(
        kalshi_path,
        [
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "exchange": "kalshi",
                "market_key": "KXFAIL",
                "question": "Fail?",
            },
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "exchange": "kalshi",
                "market_key": "KXOK",
                "question": "Ok?",
            },
        ],
    )

    class FakeKalshiResolver:
        def __init__(self, client: Any | None = None) -> None:
            pass

        async def resolve(self, market_key: str, *, snapshot: dict[str, Any]) -> ResolutionRecord:
            if market_key == "KXFAIL":
                raise RuntimeError("boom")
            return _final_record("kalshi", market_key, "yes")

    monkeypatch.setattr(cache_module, "KalshiResolutionResolver", FakeKalshiResolver)

    summary = await cache_module.resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        gamma_client=FakeGamma(),
        clob_client=FakeClob(),
        kalshi_client=FakeKalshiClient(),
    )

    written = pd.read_parquet(output_dir / "market_resolutions.parquet").set_index("market_key")
    assert summary["row_count"] == 2
    assert written.loc["KXOK", "resolution_state"] == STATE_FINAL
    assert written.loc["KXFAIL", "resolution_state"] == "unavailable"
    assert written.loc["KXFAIL", "error_type"] == "RuntimeError"


@pytest.mark.asyncio
async def test_cache_inconsistent_record_is_not_collapsed_into_existing_final(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    polymarket_path = tmp_path / "polymarket.parquet"
    kalshi_path = tmp_path / "kalshi.parquet"
    output_dir = tmp_path / "out"
    _write_polymarket(polymarket_path, [])
    _write_kalshi(
        kalshi_path,
        [
            {
                "schema_version": "kalshi_market_snapshot.v1",
                "exchange": "kalshi",
                "market_key": "KXCONFLICT",
                "question": "Conflict?",
            }
        ],
    )
    _write_existing(output_dir, [_resolution_row(platform="kalshi", market_key="KXCONFLICT", result="yes")])

    class FakeKalshiResolver:
        def __init__(self, client: Any | None = None) -> None:
            pass

        async def resolve(self, market_key: str, *, snapshot: dict[str, Any]) -> ResolutionRecord:
            return ResolutionRecord(
                platform="kalshi",
                market_key=market_key,
                input_identifier=market_key,
                resolution_state=STATE_INCONSISTENT,
                confidence=CONFIDENCE_UNAVAILABLE,
                error_type="Conflict",
                error_message="conflicting sources",
                observed_at_utc="2026-01-02T00:00:00+00:00",
            )

    monkeypatch.setattr(cache_module, "KalshiResolutionResolver", FakeKalshiResolver)

    await cache_module.resolve_market_resolution_cache(
        polymarket_markets_path=polymarket_path,
        kalshi_markets_path=kalshi_path,
        output_dir=output_dir,
        refresh=True,
        gamma_client=FakeGamma(),
        clob_client=FakeClob(),
        kalshi_client=FakeKalshiClient(),
    )

    [row] = pd.read_parquet(output_dir / "market_resolutions.parquet").to_dict("records")
    assert row["resolution_state"] == STATE_INCONSISTENT
    assert row["error_type"] == "Conflict"
