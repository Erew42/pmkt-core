from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import math
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Callable,
    Iterable,
    Literal,
    Sequence,
)
from urllib.parse import quote, urlparse
from uuid import uuid4

import httpx
from pmkt.exchanges._requests import VenueRequests
from aiolimiter import AsyncLimiter

from pmkt.runtime import RequestPolicy
from pmkt._http import HttpClient
from pmkt.runtime import OperationExpiry
from pmkt.config import PmktConfig
from pmkt.data.canonical import KALSHI_MARKET_SNAPSHOT_COLUMNS
from pmkt.data.normalize_kalshi import (
    kalshi_market_matches_query_status,
    normalize_kalshi_market,
)
from pmkt.errors import (
    InvalidDataError,
    MarketNotFoundError,
    UnsupportedCapabilityError,
)
from pmkt.exchanges.kalshi._workflow import (
    KALSHI_MARKET_INTERPRETATION_ID,
    decode_kalshi_detail_envelope,
    decode_kalshi_markets_envelope,
    kalshi_book_identities,
    kalshi_detail_identities,
    kalshi_market_identity,
    kalshi_page_identities,
    normalize_kalshi_orderbook,
    normalize_kalshi_workflow_book,
    normalize_kalshi_workflow_market,
)
from pmkt.exchanges.kalshi._history import (
    CandlePayload,
    decode_kalshi_event_series,
    decode_kalshi_historical_cutoff,
    kalshi_candle_identities,
    kalshi_cutoff_identities,
    kalshi_event_identities,
    normalize_kalshi_candle_history,
    parse_kalshi_settlement_timestamp,
)
from pmkt.exchanges.read_auth import (
    ReadAuthHeaderProvider,
    ReadOnlyRequestError,
    headers_for_read,
)
from pmkt.records import (
    BookSnapshot,
    CandleHistoryResult,
    DataIssue,
    DataScope,
    DiscoveryReport,
    DiscoveryResult,
    DiscoveryStopReason,
    KalshiFilter,
    KalshiInstrumentRef,
    KalshiMarket,
    KalshiMarketRef,
    HistoryQueryWindow,
    RequestObservation,
)
from pmkt import __version__

if TYPE_CHECKING:
    import pandas as pd


KALSHI_DISCOVERY_TICKER_CHUNK_SIZE = 20
_MARKETS_ENDPOINT = "/markets"
_MARKETS_PARAMETER_ALLOWLIST = frozenset(
    {
        "limit",
        "cursor",
        "status",
        "event_ticker",
        "series_ticker",
        "tickers",
        "mve_filter",
    }
)
_CANDLE_PARAMETER_ALLOWLIST = frozenset(
    {"start_ts", "end_ts", "period_interval", "include_latest_before_start"}
)
# Adapter chunk size in elapsed periods (`end_ts - start_ts`), not candle count.
# Kalshi's single-market live and historical candlestick endpoints use inclusive
# `start_ts`/`end_ts` labels (candles ending on or after start, on or before end)
# and do not publish a numeric cap. The batch endpoint caps 10,000 candles across
# at most 100 tickers. A window of N elapsed periods can include N+1 inclusive
# end-labels, so 5,000 elapsed periods stays well under that published batch cap.
KALSHI_CANDLE_QUERY_ELAPSED_PERIODS_PER_REQUEST = 5_000


@dataclass
class _DiscoveryPartition:
    chunk_index: int
    tickers: tuple[str, ...] | None
    cursor: str | None = None
    returned_cursors: set[str] | None = None

    def __post_init__(self) -> None:
        if self.returned_cursors is None:
            self.returned_cursors = set()


def _normalize_tickers(tickers: str | Iterable[str] | None) -> str | None:
    if tickers is None:
        return None
    if isinstance(tickers, str):
        return tickers
    cleaned = [str(ticker).strip() for ticker in tickers if str(ticker).strip()]
    return ",".join(dict.fromkeys(cleaned)) or None


def _signed_path(base_url: str, endpoint_path: str) -> str:
    base_path = urlparse(base_url).path.rstrip("/")
    endpoint = "/" + endpoint_path.lstrip("/")
    if base_path and (endpoint == base_path or endpoint.startswith(base_path + "/")):
        return endpoint
    return f"{base_path}{endpoint}" if base_path else endpoint


def kalshi_markets_dataframe(markets: list[dict[str, Any]]) -> pd.DataFrame:
    import pandas as pd

    rows = [
        normalize_kalshi_market(market)
        for market in markets
        if isinstance(market, dict)
    ]
    df = pd.DataFrame(rows, columns=KALSHI_MARKET_SNAPSHOT_COLUMNS)
    if not df.empty and "market_key" in df.columns:
        df = df.sort_values("market_key").reset_index(drop=True)
    return df


def normalize_kalshi_event(event: dict[str, Any]) -> dict[str, Any]:
    ticker = event.get("event_ticker") or event.get("ticker")
    title = event.get("title") or event.get("name") or ticker
    status = event.get("status")
    category = (
        event.get("category")
        or event.get("series_category")
        or event.get("event_category")
    )
    return {
        "exchange": "kalshi",
        "event_ticker": str(ticker) if ticker is not None else None,
        "ticker": ticker,
        "title": title,
        "subtitle": event.get("subtitle") or event.get("sub_title"),
        "category": category,
        "series_ticker": event.get("series_ticker"),
        "status": status,
        "closed": status in {"closed", "settled"},
        "close_time": event.get("close_time") or event.get("expiration_time"),
        "open_time": event.get("open_time"),
        "updated_time": event.get("updated_time"),
        "raw_json": json.dumps(event, ensure_ascii=True, sort_keys=True, default=str),
    }


def kalshi_events_dataframe(events: list[dict[str, Any]]) -> pd.DataFrame:
    import pandas as pd

    rows = [
        normalize_kalshi_event(event) for event in events if isinstance(event, dict)
    ]
    df = pd.DataFrame(rows)
    if not df.empty and "event_ticker" in df.columns:
        df = df.sort_values("event_ticker").reset_index(drop=True)
    return df


class KalshiHttpClient(HttpClient):
    def __init__(
        self,
        *,
        base_url: str,
        auth: ReadAuthHeaderProvider | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_s: float = 10.0,
        max_attempts: int = 3,
        limiter: AsyncLimiter | None = None,
        request_policy: RequestPolicy | None = None,
    ) -> None:
        super().__init__(
            base_url=base_url,
            transport=transport,
            timeout_s=timeout_s,
            max_attempts=max_attempts,
            limiter=limiter,
            request_policy=request_policy,
        )
        self.header_provider = auth

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
        *,
        expiry: OperationExpiry | None = None,
        trace: Any | None = None,
    ) -> httpx.Response:
        if method.upper() != "GET":
            raise ReadOnlyRequestError(
                f"Kalshi market-data client is read-only; blocked {method.upper()}"
            )
        if self.header_provider is None:
            return await super()._request(
                method,
                path,
                params=params,
                json=json,
                headers=headers,
                expiry=expiry,
                trace=trace,
            )
        parsed_path = urlparse(path)
        if parsed_path.scheme or parsed_path.netloc:
            raise ReadOnlyRequestError(
                "authenticated Kalshi reads require a relative endpoint path"
            )

        auth_headers = headers_for_read(
            self.header_provider,
            method,
            _signed_path(self.base_url, path),
        )
        request_headers = {**(headers or {}), **auth_headers}
        return await super()._request(
            method,
            path,
            params=params,
            json=json,
            headers=request_headers,
            expiry=expiry,
            trace=trace,
        )


class AsyncKalshiClient:
    """Kalshi REST market-data client."""

    def __init__(
        self,
        base_url: str | None = None,
        *,
        auth: ReadAuthHeaderProvider | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        limiter: AsyncLimiter | None = None,
        request_policy: RequestPolicy | None = None,
        config: PmktConfig | None = None,
        timeout_s: float = 10.0,
        _utc_now: Callable[[], datetime] | None = None,
    ) -> None:
        self.base_url = (
            base_url
            if base_url is not None
            else config.resolved_kalshi_api_url
            if config is not None
            else PmktConfig().resolved_kalshi_api_url
        )
        self.header_provider = auth
        self.transport = transport
        self.limiter = limiter or AsyncLimiter(10, 1)
        self._utc_now = _utc_now or (lambda: datetime.now(timezone.utc))
        self._http = KalshiHttpClient(
            base_url=self.base_url,
            auth=self.header_provider,
            transport=self.transport,
            limiter=self.limiter,
            request_policy=request_policy,
            timeout_s=timeout_s,
        )
        self._requests = VenueRequests(self._http, venue="kalshi", service="kalshi")

    async def close(self) -> None:
        await self._http.close()

    def __enter__(self) -> "AsyncKalshiClient":
        raise RuntimeError("Use 'async with' for AsyncKalshiClient.")

    async def __aenter__(self) -> "AsyncKalshiClient":
        await self._http.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def get_market(
        self,
        *,
        market: KalshiMarketRef,
        source: Literal["live", "historical"] = "live",
        deadline_s: float = 30.0,
    ) -> KalshiMarket:
        """Fetch one normalized market from exactly the selected Kalshi dataset."""

        if not isinstance(market, KalshiMarketRef):
            raise TypeError("market must be a KalshiMarketRef")
        ticker = market.ticker
        if source not in ("live", "historical"):
            raise ValueError("source must be 'live' or 'historical'")
        expiry = OperationExpiry.bounded(deadline_s)
        observations: list[RequestObservation] = []
        result = await self._get_market_with_expiry(
            ticker=ticker,
            source=source,
            expiry=expiry,
            observations=observations,
        )
        expiry.checkpoint()
        return result

    async def _get_market_with_expiry(
        self,
        *,
        ticker: str,
        source: Literal["live", "historical"],
        expiry: OperationExpiry,
        observations: list[RequestObservation],
        expected_series_ticker: str | None = None,
    ) -> KalshiMarket:
        encoded_ticker = quote(ticker, safe="")
        if source == "live":
            path = f"/markets/{encoded_ticker}"
            template = "/markets/{ticker}"
            lookup_scope = (
                "Kalshi standard live market detail; historical archive was not checked"
            )
        else:
            path = f"/historical/markets/{encoded_ticker}"
            template = "/historical/markets/{ticker}"
            lookup_scope = "Kalshi historical market detail"
        try:
            payload, observation = await self._requests.request_json_observed(
                "GET",
                path,
                request_id=f"kalshi-detail-{uuid4().hex}",
                endpoint_template=template,
                effective_parameters=None,
                params=None,
                expiry=expiry,
                response_identities=lambda value: kalshi_detail_identities(
                    value,
                    requested_ticker=ticker,
                    expected_series_ticker=expected_series_ticker,
                ),
                record_observation=observations.append,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise MarketNotFoundError(
                    venue="kalshi", identifier=ticker, lookup_scope=lookup_scope
                ) from exc
            raise
        expiry.checkpoint()
        row = decode_kalshi_detail_envelope(payload)
        market = normalize_kalshi_workflow_market(row, observation=observation)
        expiry.checkpoint()
        return market

    async def discover_markets(
        self,
        *,
        filters: KalshiFilter,
        max_markets: int = 100,
        max_pages: int = 20,
        deadline_s: float = 60.0,
    ) -> DiscoveryResult[KalshiMarket]:
        """Discover a bounded set of normalized standard-dataset markets."""

        if not isinstance(filters, KalshiFilter):
            raise TypeError("filters must be a KalshiFilter")
        _require_positive_int(max_markets, "max_markets")
        _require_positive_int(max_pages, "max_pages")
        expiry = OperationExpiry.bounded(deadline_s)
        started = datetime.now(timezone.utc)
        requested_filters = _reported_filters(filters)
        requested_tickers = _deduplicate(filters.tickers or ())
        requested_ticker_set = frozenset(requested_tickers)
        selection_strategy = (
            "targeted_tickers" if filters.tickers is not None else "cursor_scan"
        )
        if filters.tickers == ():
            expiry.checkpoint()
            return DiscoveryResult(
                items=(),
                report=_kalshi_discovery_report(
                    started=started,
                    stop_reason="empty_selection",
                    filters=filters,
                    requested_filters=requested_filters,
                    pages_fetched=0,
                    rows_scanned=0,
                    unique_markets_seen=0,
                    duplicates_seen=0,
                    unknown_counts={},
                    data_scope=self._requests.source.data_scope,
                    observations=(),
                    issues=(),
                    selection_strategy=selection_strategy,
                    requested_selector_count=0,
                    queried_chunks=0,
                    traversal_complete=True,
                ),
            )

        chunks: tuple[tuple[str, ...] | None, ...]
        if requested_tickers:
            chunks = tuple(
                requested_tickers[index : index + KALSHI_DISCOVERY_TICKER_CHUNK_SIZE]
                for index in range(
                    0, len(requested_tickers), KALSHI_DISCOVERY_TICKER_CHUNK_SIZE
                )
            )
        else:
            chunks = (None,)
        partitions = deque(
            _DiscoveryPartition(index, chunk) for index, chunk in enumerate(chunks)
        )
        observations: list[RequestObservation] = []
        items: list[KalshiMarket] = []
        seen_tickers: set[str] = set()
        unknown_counts: dict[str, int] = {}
        issues: list[DataIssue] = []
        queried_chunks: set[int] = set()
        pages_fetched = 0
        rows_scanned = 0
        duplicates_seen = 0
        stop_reason: DiscoveryStopReason | None = None
        operation_id = uuid4().hex

        while partitions and stop_reason is None:
            if pages_fetched >= max_pages:
                stop_reason = "page_limit"
                break
            partition = partitions.popleft()
            params: dict[str, Any] = {
                "limit": 1000,
                "cursor": partition.cursor,
                # Passing None is deliberate: HttpClient omits the wire parameter and
                # avoids the native markets_page status="open" default.
                "status": filters.status,
                "event_ticker": filters.event_ticker,
                "series_ticker": filters.series_ticker,
                "tickers": ",".join(partition.tickers)
                if partition.tickers is not None
                else None,
                "mve_filter": filters.mve_filter,
            }
            effective_parameters = {
                key: value for key, value in params.items() if value is not None
            }
            payload, observation = await self._requests.request_json_observed(
                "GET",
                _MARKETS_ENDPOINT,
                request_id=f"kalshi-discovery-{operation_id}-{pages_fetched + 1}",
                endpoint_template=_MARKETS_ENDPOINT,
                effective_parameters=effective_parameters,
                params=params,
                expiry=expiry,
                response_identities=kalshi_page_identities,
                record_observation=observations.append,
            )
            expiry.checkpoint()
            rows, next_cursor = decode_kalshi_markets_envelope(payload)
            pages_fetched += 1
            queried_chunks.add(partition.chunk_index)
            if next_cursor is not None:
                assert partition.returned_cursors is not None
                if next_cursor in partition.returned_cursors:
                    raise InvalidDataError(
                        "Kalshi markets returned a repeated cursor within one chunk"
                    )
            for row in rows:
                expiry.checkpoint()
                ticker, _ = kalshi_market_identity(row)
                rows_scanned += 1
                if ticker in seen_tickers:
                    duplicates_seen += 1
                    continue
                seen_tickers.add(ticker)
                market = normalize_kalshi_workflow_market(
                    row,
                    observation=observation,
                    series_filter_evidence=filters.series_ticker,
                )
                issues.extend(market.issues)
                if not _kalshi_market_matches(
                    market,
                    filters,
                    requested_tickers=requested_ticker_set,
                    unknown_counts=unknown_counts,
                ):
                    continue
                items.append(market)
                if len(items) >= max_markets:
                    stop_reason = "result_limit"
                    break
            if stop_reason is not None:
                break
            if next_cursor is not None:
                assert partition.returned_cursors is not None
                partition.returned_cursors.add(next_cursor)
                partition.cursor = next_cursor
                partitions.append(partition)

        if stop_reason is None:
            stop_reason = "source_exhausted"
        expiry.checkpoint()
        report = _kalshi_discovery_report(
            started=started,
            stop_reason=stop_reason,
            filters=filters,
            requested_filters=requested_filters,
            pages_fetched=pages_fetched,
            rows_scanned=rows_scanned,
            unique_markets_seen=len(seen_tickers),
            duplicates_seen=duplicates_seen,
            unknown_counts=unknown_counts,
            data_scope=_combined_data_scope(
                observations, self._requests.source.data_scope
            ),
            observations=tuple(observations),
            issues=_aggregate_issues(issues),
            selection_strategy=selection_strategy,
            requested_selector_count=len(requested_tickers),
            queried_chunks=len(queried_chunks),
            traversal_complete=stop_reason == "source_exhausted",
        )
        expiry.checkpoint()
        return DiscoveryResult(items=tuple(items), report=report)

    async def get_book(
        self,
        instrument: KalshiInstrumentRef,
        *,
        depth: int | None = None,
        deadline_s: float = 30.0,
    ) -> BookSnapshot:
        """Fetch one strictly validated projected Kalshi outcome book."""

        if not isinstance(instrument, KalshiInstrumentRef):
            raise TypeError("instrument must be a KalshiInstrumentRef")
        if depth is not None:
            if isinstance(depth, bool) or not isinstance(depth, int):
                raise TypeError("depth must be an int or None")
            if depth <= 0:
                raise ValueError("depth must be positive")
        expiry = OperationExpiry.bounded(deadline_s)
        observations: list[RequestObservation] = []
        capability = await self._get_market_with_expiry(
            ticker=instrument.market.ticker,
            source="live",
            expiry=expiry,
            observations=observations,
            expected_series_ticker=instrument.market.series_ticker,
        )
        if capability.mapping_status == "inconsistent":
            raise InvalidDataError("Kalshi market capability evidence is inconsistent")
        if not capability.book_supported:
            raise UnsupportedCapabilityError(
                f"Kalshi market {instrument.market.ticker!r} is not a qualified binary book"
            )
        effective_market = (
            capability.ref
            if capability.ref.series_ticker is not None
            else instrument.market
        )
        effective_instrument = KalshiInstrumentRef(effective_market, instrument.side)
        expiry.checkpoint()
        encoded_ticker = quote(instrument.market.ticker, safe="")
        try:
            payload, _ = await self._requests.request_json_observed(
                "GET",
                f"/markets/{encoded_ticker}/orderbook",
                request_id=f"kalshi-book-{uuid4().hex}",
                endpoint_template="/markets/{ticker}/orderbook",
                effective_parameters=None,
                params=None,
                expiry=expiry,
                response_identities=lambda value: kalshi_book_identities(
                    value, instrument=effective_instrument
                ),
                record_observation=observations.append,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise MarketNotFoundError(
                    venue="kalshi",
                    identifier=instrument.market.ticker,
                    lookup_scope="Kalshi current order book",
                ) from exc
            raise
        expiry.checkpoint()
        result = normalize_kalshi_workflow_book(
            payload,
            instrument=effective_instrument,
            depth=depth,
            observations=observations,
            expiry=expiry,
        )
        expiry.checkpoint()
        return result

    async def get_candles(
        self,
        market: KalshiMarketRef,
        *,
        start: datetime,
        end: datetime,
        period_minutes: Literal[1, 60, 1440],
        source: Literal["auto", "live", "historical"] = "auto",
        max_candles: int = 100_000,
        deadline_s: float = 60.0,
        invalid_rows: Literal["raise", "report"] = "raise",
    ) -> CandleHistoryResult:
        """Fetch fully contained, completed candles for one Kalshi market."""

        if not isinstance(market, KalshiMarketRef):
            raise TypeError("market must be a KalshiMarketRef")
        start_utc, end_utc = _utc_history_bounds(start, end)
        if isinstance(period_minutes, bool) or not isinstance(period_minutes, int):
            raise TypeError("period_minutes must be an int")
        if period_minutes not in (1, 60, 1440):
            raise ValueError("period_minutes must be one of 1, 60, or 1440")
        if source not in ("auto", "live", "historical"):
            raise ValueError("source must be 'auto', 'live', or 'historical'")
        _require_positive_int(max_candles, "max_candles")
        if invalid_rows not in ("raise", "report"):
            raise ValueError("invalid_rows must be 'raise' or 'report'")
        expiry = OperationExpiry.bounded(deadline_s)
        frozen_now = self._utc_now()
        if not isinstance(frozen_now, datetime):
            raise TypeError("injected UTC clock must return a datetime")
        frozen_offset = frozen_now.utcoffset()
        if (
            frozen_now.tzinfo is None
            or frozen_offset is None
            or frozen_offset.total_seconds() != 0
        ):
            raise ValueError("injected UTC clock must return a UTC datetime")
        expiry.checkpoint()

        windows = _kalshi_candle_query_windows(
            start_utc,
            end_utc,
            period_minutes=period_minutes,
            expiry=expiry,
        )
        observations: list[RequestObservation] = []
        payloads: list[CandlePayload] = []
        queried_windows: list[HistoryQueryWindow] = []
        routing_flags: list[str] = []
        cutoff: datetime | None = None
        live_market: KalshiMarket | None = None
        routing_market: KalshiMarket | None = None
        live_series: str | None = None

        selected_source: Literal["live", "historical"]
        if source == "auto":
            cutoff = await self._historical_cutoff_with_expiry(
                expiry=expiry, observations=observations
            )
            try:
                live_market = await self._get_market_with_expiry(
                    ticker=market.ticker,
                    source="live",
                    expiry=expiry,
                    observations=observations,
                    expected_series_ticker=market.series_ticker,
                )
            except MarketNotFoundError:
                routing_market = await self._get_market_with_expiry(
                    ticker=market.ticker,
                    source="historical",
                    expiry=expiry,
                    observations=observations,
                    expected_series_ticker=market.series_ticker,
                )
                selected_source = "historical"
                routing_flags.append("live_metadata_absent_archive_verified")
            else:
                routing_market = live_market
                settlement = parse_kalshi_settlement_timestamp(
                    live_market.native_payload.get("settlement_ts")
                )
                if settlement is not None and settlement < cutoff:
                    selected_source = "historical"
                    routing_flags.append("settlement_before_archive_cutoff")
                else:
                    selected_source = "live"
                    routing_flags.append("settlement_after_archive_cutoff_or_unsettled")
        elif source == "live":
            selected_source = "live"
        else:
            selected_source = "historical"

        if selected_source == "live":
            if live_market is None:
                live_market = await self._get_market_with_expiry(
                    ticker=market.ticker,
                    source="live",
                    expiry=expiry,
                    observations=observations,
                    expected_series_ticker=market.series_ticker,
                )
            routing_market = live_market
            live_series = await self._verified_live_series(
                market=market,
                market_detail=live_market,
                expiry=expiry,
                observations=observations,
            )

        fetched, attempted, missing = await self._fetch_candle_dataset(
            market=market,
            dataset=selected_source,
            series_ticker=live_series,
            query_windows=windows,
            period_minutes=period_minutes,
            expiry=expiry,
            observations=observations,
        )
        payloads.extend(fetched)
        queried_windows.extend(attempted)

        if missing:
            if source != "auto":
                raise MarketNotFoundError(
                    venue="kalshi",
                    identifier=market.ticker,
                    lookup_scope=f"Kalshi {selected_source} candle dataset",
                )
            alternate: Literal["live", "historical"] = (
                "historical" if selected_source == "live" else "live"
            )
            if alternate == "live":
                if live_market is None:
                    live_market = await self._get_market_with_expiry(
                        ticker=market.ticker,
                        source="live",
                        expiry=expiry,
                        observations=observations,
                        expected_series_ticker=market.series_ticker,
                    )
                live_series = await self._verified_live_series(
                    market=market,
                    market_detail=live_market,
                    expiry=expiry,
                    observations=observations,
                )
                routing_market = live_market
            (
                alternate_payloads,
                alternate_windows,
                alternate_missing,
            ) = await self._fetch_candle_dataset(
                market=market,
                dataset=alternate,
                series_ticker=live_series if alternate == "live" else None,
                query_windows=windows,
                period_minutes=period_minutes,
                expiry=expiry,
                observations=observations,
            )
            queried_windows.extend(alternate_windows)
            if alternate_missing:
                raise MarketNotFoundError(
                    venue="kalshi",
                    identifier=market.ticker,
                    lookup_scope="Kalshi live and historical candle datasets",
                )
            payloads.extend(alternate_payloads)
            routing_flags.append("qualified_404_migration_fallback")

        expiry.checkpoint()
        result = normalize_kalshi_candle_history(
            payloads,
            market=market,
            requested_start_utc=start_utc,
            requested_end_utc=end_utc,
            period_minutes=period_minutes,
            requested_source=source,
            completed_through_utc=frozen_now,
            historical_cutoff_utc=cutoff,
            queried_windows=queried_windows,
            observations=observations,
            max_candles=max_candles,
            invalid_rows=invalid_rows,
            routing_flags=routing_flags,
            routing_market=routing_market,
            expiry=expiry,
        )
        expiry.checkpoint()
        return result

    async def _historical_cutoff_with_expiry(
        self,
        *,
        expiry: OperationExpiry,
        observations: list[RequestObservation],
    ) -> datetime:
        payload, observation = await self._historical_cutoff_payload(
            expiry=expiry, observations=observations
        )
        assert observation is not None
        return decode_kalshi_historical_cutoff(payload)

    async def _verified_live_series(
        self,
        *,
        market: KalshiMarketRef,
        market_detail: KalshiMarket,
        expiry: OperationExpiry,
        observations: list[RequestObservation],
    ) -> str:
        if market_detail.ref.series_ticker is not None:
            if (
                market.series_ticker is not None
                and market.series_ticker != market_detail.ref.series_ticker
            ):
                raise InvalidDataError("Kalshi market series evidence is inconsistent")
            return market_detail.ref.series_ticker
        event_ticker = market_detail.event_ticker
        if event_ticker is None:
            raise InvalidDataError(
                "Kalshi live candle routing requires verified event or series evidence"
            )
        encoded_event = quote(event_ticker, safe="")
        payload, _ = await self._requests.request_json_observed(
            "GET",
            f"/events/{encoded_event}",
            request_id=f"kalshi-candle-event-{uuid4().hex}",
            endpoint_template="/events/{event_ticker}",
            effective_parameters=None,
            params=None,
            expiry=expiry,
            response_identities=lambda value: kalshi_event_identities(
                value,
                event_ticker=event_ticker,
                expected_series_ticker=market.series_ticker,
            ),
            record_observation=observations.append,
        )
        return decode_kalshi_event_series(
            payload,
            event_ticker=event_ticker,
            expected_series_ticker=market.series_ticker,
        )

    async def _fetch_candle_dataset(
        self,
        *,
        market: KalshiMarketRef,
        dataset: Literal["live", "historical"],
        series_ticker: str | None,
        query_windows: Sequence[tuple[int, int]],
        period_minutes: Literal[1, 60, 1440],
        expiry: OperationExpiry,
        observations: list[RequestObservation],
    ) -> tuple[list[CandlePayload], list[HistoryQueryWindow], bool]:
        payloads: list[CandlePayload] = []
        attempted: list[HistoryQueryWindow] = []
        endpoint = (
            "/series/{series_ticker}/markets/{ticker}/candlesticks"
            if dataset == "live"
            else "/historical/markets/{ticker}/candlesticks"
        )
        for start_ts, end_ts in query_windows:
            expiry.checkpoint()
            attempted.append(
                HistoryQueryWindow(
                    start_utc=datetime.fromtimestamp(start_ts, tz=timezone.utc),
                    end_utc=datetime.fromtimestamp(end_ts, tz=timezone.utc),
                    dataset=dataset,
                    endpoint=endpoint,
                )
            )
            try:
                if dataset == "live":
                    if series_ticker is None:
                        raise RuntimeError(
                            "live candle fetch requires a verified series"
                        )
                    response_market = KalshiMarketRef(
                        market.ticker, series_ticker=series_ticker
                    )
                    payload, observation = await self._market_candlesticks_payload(
                        series_ticker=series_ticker,
                        ticker=market.ticker,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        period_interval=period_minutes,
                        include_latest_before_start=False,
                        market=response_market,
                        expiry=expiry,
                        observations=observations,
                    )
                else:
                    (
                        payload,
                        observation,
                    ) = await self._historical_market_candlesticks_payload(
                        ticker=market.ticker,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        period_interval=period_minutes,
                        market=market,
                        expiry=expiry,
                        observations=observations,
                    )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    return payloads, attempted, True
                raise
            assert observation is not None
            payloads.append(CandlePayload(payload, dataset, observation))
        return payloads, attempted, False

    @staticmethod
    def _validate_limit(limit: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise TypeError("limit must be an int")
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")

    async def markets_page(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        status: str | None = "open",
        event_ticker: str | None = None,
        series_ticker: str | None = None,
        tickers: str | Iterable[str] | None = None,
        mve_filter: str | None = None,
        min_close_ts: int | None = None,
        max_close_ts: int | None = None,
        min_created_ts: int | None = None,
        max_created_ts: int | None = None,
        min_updated_ts: int | None = None,
        max_updated_ts: int | None = None,
        min_settled_ts: int | None = None,
        max_settled_ts: int | None = None,
        expiry: OperationExpiry | None = None,
    ) -> dict[str, Any]:
        self._validate_limit(limit)
        params = {
            "limit": limit,
            "cursor": cursor,
            "status": status,
            "event_ticker": event_ticker,
            "series_ticker": series_ticker,
            "tickers": _normalize_tickers(tickers),
            "mve_filter": mve_filter,
            "min_close_ts": min_close_ts,
            "max_close_ts": max_close_ts,
            "min_created_ts": min_created_ts,
            "max_created_ts": max_created_ts,
            "min_updated_ts": min_updated_ts,
            "max_updated_ts": max_updated_ts,
            "min_settled_ts": min_settled_ts,
            "max_settled_ts": max_settled_ts,
        }
        data = await self._http.request_json(
            "GET", "/markets", params=params, expiry=expiry
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def market(
        self, ticker: str, *, expiry: OperationExpiry | None = None
    ) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET",
            f"/markets/{quote(ticker, safe='')}",
            expiry=expiry,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        market = data.get("market")
        if isinstance(market, dict):
            return market
        return data

    async def historical_market(
        self, ticker: str, *, expiry: OperationExpiry | None = None
    ) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET",
            f"/historical/markets/{quote(ticker, safe='')}",
            expiry=expiry,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        market = data.get("market")
        if isinstance(market, dict):
            return market
        return data

    async def historical_markets_page(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        event_ticker: str | None = None,
        series_ticker: str | None = None,
        tickers: str | Iterable[str] | None = None,
        mve_filter: str | None = None,
        expiry: OperationExpiry | None = None,
    ) -> dict[str, Any]:
        """Fetch one page of markets from Kalshi's historical archive."""
        self._validate_limit(limit)
        normalized_tickers = _normalize_tickers(tickers)
        if tickers is not None and normalized_tickers is None:
            raise ValueError("tickers must not be empty")
        selectors = (event_ticker, series_ticker, normalized_tickers, mve_filter)
        if sum(value is not None for value in selectors) > 1:
            raise ValueError("historical market filters are mutually exclusive")
        if mve_filter is not None and mve_filter != "exclude":
            raise ValueError("historical mve_filter must be 'exclude'")
        for name, value in (
            ("event_ticker", event_ticker),
            ("series_ticker", series_ticker),
            ("tickers", normalized_tickers),
        ):
            if value is not None:
                _require_nonempty_string(value, name)
        data = await self._http.request_json(
            "GET",
            "/historical/markets",
            params={
                "limit": limit,
                "cursor": cursor,
                "event_ticker": event_ticker,
                "series_ticker": series_ticker,
                "tickers": normalized_tickers,
                "mve_filter": mve_filter,
            },
            expiry=expiry,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def iter_historical_markets(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        event_ticker: str | None = None,
        series_ticker: str | None = None,
        tickers: str | Iterable[str] | None = None,
        mve_filter: str | None = None,
        max_pages: int | None = None,
        expiry: OperationExpiry | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield archived markets until the cursor ends or max_pages is reached."""
        if tickers is not None:
            tickers = _normalize_tickers(tickers)
            if tickers is None:
                raise ValueError("tickers must not be empty")
        seen_cursors = {cursor} if cursor else set()
        pages = 0
        while max_pages is None or pages < max_pages:
            page = await self.historical_markets_page(
                limit=limit,
                cursor=cursor,
                event_ticker=event_ticker,
                series_ticker=series_ticker,
                tickers=tickers,
                mve_filter=mve_filter,
                expiry=expiry,
            )
            pages += 1
            markets = page.get("markets")
            if not isinstance(markets, list):
                raise TypeError("Expected response field 'markets' to be a list")
            for market in markets:
                if isinstance(market, dict):
                    yield market
            next_cursor = str(page.get("cursor") or "").strip() or None
            if next_cursor is None:
                break
            if next_cursor in seen_cursors:
                raise RuntimeError("Kalshi historical markets cursor repeated")
            seen_cursors.add(next_cursor)
            cursor = next_cursor

    async def iter_markets(
        self,
        *,
        limit: int = 100,
        status: str | None = "open",
        max_pages: int | None = None,
        expiry: OperationExpiry | None = None,
        **params: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        cursor = params.pop("cursor", None)
        if "tickers" in params:
            # Normalize once so a one-shot iterable filters every page.
            params["tickers"] = _normalize_tickers(params["tickers"])
        seen_cursors = {cursor} if cursor else set()
        pages = 0
        while True:
            if max_pages is not None and pages >= max_pages:
                break
            page = await self.markets_page(
                limit=limit, cursor=cursor, status=status, **params, expiry=expiry
            )
            pages += 1
            markets = page.get("markets")
            if not isinstance(markets, list):
                raise TypeError("Expected response field 'markets' to be a list")
            for market in markets:
                if isinstance(market, dict):
                    yield market
            next_cursor = str(page.get("cursor") or "").strip() or None
            if next_cursor is None:
                break
            if next_cursor in seen_cursors:
                raise RuntimeError("Kalshi markets cursor repeated")
            seen_cursors.add(next_cursor)
            cursor = next_cursor

    async def events_page(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        status: str | None = None,
        series_ticker: str | None = None,
        with_nested_markets: bool | None = None,
        expiry: OperationExpiry | None = None,
    ) -> dict[str, Any]:
        self._validate_limit(limit)
        data = await self._http.request_json(
            "GET",
            "/events",
            params={
                "limit": limit,
                "cursor": cursor,
                "status": status,
                "series_ticker": series_ticker,
                "with_nested_markets": with_nested_markets,
            },
            expiry=expiry,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def iter_events(
        self,
        *,
        limit: int = 100,
        status: str | None = None,
        max_pages: int | None = None,
        expiry: OperationExpiry | None = None,
        **params: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        cursor = params.pop("cursor", None)
        pages = 0
        while True:
            if max_pages is not None and pages >= max_pages:
                break
            page = await self.events_page(
                limit=limit, cursor=cursor, status=status, **params, expiry=expiry
            )
            pages += 1
            events = page.get("events")
            if not isinstance(events, list) or not events:
                break
            for event in events:
                if isinstance(event, dict):
                    yield event
            cursor = page.get("cursor") or None
            if not cursor:
                break

    async def orderbook(
        self,
        ticker: str,
        *,
        depth: int | None = None,
        expiry: OperationExpiry | None = None,
    ) -> dict[str, Any]:
        params = {"depth": depth}
        data = await self._http.request_json(
            "GET", f"/markets/{ticker}/orderbook", params=params, expiry=expiry
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def normalized_orderbook(
        self,
        ticker: str,
        *,
        depth: int | None = None,
        expiry: OperationExpiry | None = None,
    ) -> dict[str, Any]:
        data = await self.orderbook(ticker, depth=depth, expiry=expiry)
        return normalize_kalshi_orderbook(data, market_ticker=ticker)

    async def market_candlesticks(
        self,
        series_ticker: str,
        ticker: str,
        *,
        start_ts: int,
        end_ts: int,
        period_interval: int,
        include_latest_before_start: bool | None = None,
        expiry: OperationExpiry | None = None,
    ) -> dict[str, Any]:
        data, _ = await self._market_candlesticks_payload(
            series_ticker=series_ticker,
            ticker=ticker,
            start_ts=start_ts,
            end_ts=end_ts,
            period_interval=period_interval,
            include_latest_before_start=include_latest_before_start,
            expiry=expiry,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def _market_candlesticks_payload(
        self,
        *,
        series_ticker: str,
        ticker: str,
        start_ts: int,
        end_ts: int,
        period_interval: int,
        include_latest_before_start: bool | None,
        market: KalshiMarketRef | None = None,
        expiry: OperationExpiry | None = None,
        observations: list[RequestObservation] | None = None,
    ) -> tuple[object, RequestObservation | None]:
        encoded_series = quote(series_ticker, safe="")
        encoded_ticker = quote(ticker, safe="")
        path = f"/series/{encoded_series}/markets/{encoded_ticker}/candlesticks"
        params = {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "period_interval": period_interval,
            "include_latest_before_start": include_latest_before_start,
        }
        if observations is None:
            return await self._http.request_json("GET", path, params=params, expiry=expiry), None
        if market is None:
            raise RuntimeError("observed candle fetch requires workflow context")
        effective_parameters = {
            key: value for key, value in params.items() if value is not None
        }
        data, observation = await self._requests.request_json_observed(
            "GET",
            path,
            request_id=f"kalshi-candles-live-{uuid4().hex}",
            endpoint_template="/series/{series_ticker}/markets/{ticker}/candlesticks",
            effective_parameters=effective_parameters,
            params=params,
            expiry=expiry,
            response_identities=lambda value: kalshi_candle_identities(
                value, market=market
            ),
            record_observation=observations.append,
        )
        return data, observation

    async def batch_market_candlesticks(
        self,
        market_tickers: str | Iterable[str],
        *,
        start_ts: int,
        end_ts: int,
        period_interval: int,
        include_latest_before_start: bool | None = None,
        expiry: OperationExpiry | None = None,
    ) -> dict[str, Any]:
        tickers = _normalize_tickers(market_tickers)
        if not tickers:
            raise ValueError("market_tickers must contain at least one ticker")
        data = await self._http.request_json(
            "GET",
            "/markets/candlesticks",
            params={
                "market_tickers": tickers,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "period_interval": period_interval,
                "include_latest_before_start": include_latest_before_start,
            },
            expiry=expiry,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def historical_market_candlesticks(
        self,
        ticker: str,
        *,
        start_ts: int,
        end_ts: int,
        period_interval: int,
        expiry: OperationExpiry | None = None,
    ) -> dict[str, Any]:
        data, _ = await self._historical_market_candlesticks_payload(
            ticker=ticker,
            start_ts=start_ts,
            end_ts=end_ts,
            period_interval=period_interval,
            expiry=expiry,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def _historical_market_candlesticks_payload(
        self,
        *,
        ticker: str,
        start_ts: int,
        end_ts: int,
        period_interval: int,
        market: KalshiMarketRef | None = None,
        expiry: OperationExpiry | None = None,
        observations: list[RequestObservation] | None = None,
    ) -> tuple[object, RequestObservation | None]:
        encoded_ticker = quote(ticker, safe="")
        path = f"/historical/markets/{encoded_ticker}/candlesticks"
        params = {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "period_interval": period_interval,
        }
        if observations is None:
            return await self._http.request_json("GET", path, params=params, expiry=expiry), None
        if market is None:
            raise RuntimeError("observed candle fetch requires workflow context")
        data, observation = await self._requests.request_json_observed(
            "GET",
            path,
            request_id=f"kalshi-candles-historical-{uuid4().hex}",
            endpoint_template="/historical/markets/{ticker}/candlesticks",
            effective_parameters=params,
            params=params,
            expiry=expiry,
            response_identities=lambda value: kalshi_candle_identities(
                value, market=market
            ),
            record_observation=observations.append,
        )
        return data, observation

    async def historical_cutoff(
        self, *, expiry: OperationExpiry | None = None
    ) -> dict[str, Any]:
        data, _ = await self._historical_cutoff_payload(expiry=expiry)
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def _historical_cutoff_payload(
        self,
        *,
        expiry: OperationExpiry | None = None,
        observations: list[RequestObservation] | None = None,
    ) -> tuple[object, RequestObservation | None]:
        if observations is None:
            return await self._http.request_json("GET", "/historical/cutoff", expiry=expiry), None
        data, observation = await self._requests.request_json_observed(
            "GET",
            "/historical/cutoff",
            request_id=f"kalshi-candle-cutoff-{uuid4().hex}",
            endpoint_template="/historical/cutoff",
            effective_parameters=None,
            params=None,
            expiry=expiry,
            response_identities=kalshi_cutoff_identities,
            record_observation=observations.append,
        )
        return data, observation

    async def series(
        self,
        series_ticker: str,
        *,
        include_volume: bool | None = None,
        expiry: OperationExpiry | None = None,
    ) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET",
            f"/series/{series_ticker}",
            params={"include_volume": include_volume},
            expiry=expiry,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def series_list(
        self,
        *,
        category: str | None = None,
        tags: str | None = None,
        include_product_metadata: bool | None = None,
        include_volume: bool | None = None,
        min_updated_ts: int | None = None,
        expiry: OperationExpiry | None = None,
    ) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET",
            "/series",
            params={
                "category": category,
                "tags": tags,
                "include_product_metadata": include_product_metadata,
                "include_volume": include_volume,
                "min_updated_ts": min_updated_ts,
            },
            expiry=expiry,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def trades(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        ticker: str | None = None,
        min_ts: int | None = None,
        max_ts: int | None = None,
        expiry: OperationExpiry | None = None,
    ) -> dict[str, Any]:
        self._validate_limit(limit)
        data = await self._http.request_json(
            "GET",
            "/markets/trades",
            params={
                "limit": limit,
                "cursor": cursor,
                "ticker": ticker,
                "min_ts": min_ts,
                "max_ts": max_ts,
            },
            expiry=expiry,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def historical_trades(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        ticker: str | None = None,
        min_ts: int | None = None,
        max_ts: int | None = None,
        is_block_trade: bool | None = None,
        expiry: OperationExpiry | None = None,
    ) -> dict[str, Any]:
        self._validate_limit(limit)
        data = await self._http.request_json(
            "GET",
            "/historical/trades",
            params={
                "limit": limit,
                "cursor": cursor,
                "ticker": ticker,
                "min_ts": min_ts,
                "max_ts": max_ts,
                "is_block_trade": is_block_trade,
            },
            expiry=expiry,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data


def _require_nonempty_string(value: object, name: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    if not value.strip():
        raise ValueError(f"{name} must not be empty")


def _require_positive_int(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int")
    if value <= 0:
        raise ValueError(f"{name} must be positive")


def _utc_history_bounds(start: object, end: object) -> tuple[datetime, datetime]:
    normalized: list[datetime] = []
    for name, value in (("start", start), ("end", end)):
        if not isinstance(value, datetime):
            raise TypeError(f"{name} must be a datetime")
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must be timezone-aware")
        normalized.append(value.astimezone(timezone.utc))
    start_utc, end_utc = normalized
    if start_utc >= end_utc:
        raise ValueError("start must precede end after UTC normalization")
    return start_utc, end_utc


def _kalshi_candle_query_windows(
    start_utc: datetime,
    end_utc: datetime,
    *,
    period_minutes: int,
    elapsed_periods_per_request: int | None = None,
    expiry: OperationExpiry | None = None,
) -> tuple[tuple[int, int], ...]:
    """Build inclusive-label requests of at most N elapsed periods.

    Adjacent chunks overlap at one end-label (`chunk_end` becomes the next
    `start_ts`), so a max-sized window of N elapsed periods may contain N+1
    inclusive candle labels.
    """

    if elapsed_periods_per_request is None:
        elapsed_periods_per_request = KALSHI_CANDLE_QUERY_ELAPSED_PERIODS_PER_REQUEST
    _require_positive_int(elapsed_periods_per_request, "elapsed_periods_per_request")
    query_start = math.floor(start_utc.timestamp())
    query_end = math.ceil(end_utc.timestamp())
    if query_end <= query_start:
        query_end = query_start + 1
    span = period_minutes * 60 * elapsed_periods_per_request
    windows: list[tuple[int, int]] = []
    cursor = query_start
    while cursor < query_end:
        if expiry is not None:
            expiry.checkpoint()
        chunk_end = min(query_end, cursor + span)
        windows.append((cursor, chunk_end))
        if chunk_end == query_end:
            break
        cursor = chunk_end
    return tuple(windows)


def _deduplicate(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(values))


def _reported_filters(filters: KalshiFilter) -> tuple[tuple[str, object], ...]:
    values: tuple[tuple[str, object | None], ...] = (
        ("tickers", filters.tickers),
        ("event_ticker", filters.event_ticker),
        ("series_ticker", filters.series_ticker),
        ("status", filters.status),
        ("mve_filter", filters.mve_filter),
        ("question_contains", filters.question_contains),
        ("has_instruments", filters.has_instruments),
    )
    return tuple((name, value) for name, value in values if value is not None)


def _server_filter_names(filters: KalshiFilter) -> tuple[str, ...]:
    return tuple(
        name
        for name, value in (
            ("tickers", filters.tickers),
            ("event_ticker", filters.event_ticker),
            ("series_ticker", filters.series_ticker),
            ("status", filters.status),
            ("mve_filter", filters.mve_filter),
        )
        if value is not None
    )


def _local_filter_names(filters: KalshiFilter) -> tuple[str, ...]:
    return tuple(
        name
        for name, value in (
            ("tickers", filters.tickers),
            ("event_ticker", filters.event_ticker),
            ("status", filters.status),
            ("question_contains", filters.question_contains),
            ("has_instruments", filters.has_instruments),
        )
        if value is not None
    )


def _source_scope(filters: KalshiFilter) -> str:
    status = filters.status or "all_statuses"
    mve = filters.mve_filter or "mve_included"
    return f"kalshi_standard_markets_{status}_{mve}"


def _kalshi_market_matches(
    market: KalshiMarket,
    filters: KalshiFilter,
    *,
    requested_tickers: frozenset[str],
    unknown_counts: dict[str, int],
) -> bool:
    matches = True
    if filters.tickers is not None and market.ref.ticker not in requested_tickers:
        matches = False
    if filters.event_ticker is not None:
        if market.event_ticker is None:
            _increment(unknown_counts, "event_ticker")
            matches = False
        elif market.event_ticker != filters.event_ticker:
            matches = False
    if filters.series_ticker is not None:
        if market.ref.series_ticker is None:
            _increment(unknown_counts, "series_ticker")
        elif market.ref.series_ticker != filters.series_ticker:
            matches = False
    if filters.status is not None:
        if market.status is None:
            _increment(unknown_counts, "status")
            matches = False
        elif not kalshi_market_matches_query_status(market.status, filters.status):
            matches = False
    if filters.question_contains is not None:
        if market.title is None:
            _increment(unknown_counts, "question_contains")
            matches = False
        elif filters.question_contains.casefold() not in market.title.casefold():
            matches = False
    if filters.has_instruments is not None:
        if market.mapping_status == "mapped":
            has_instruments: bool | None = bool(market.instruments)
        elif market.mapping_status == "empty":
            has_instruments = False
        else:
            _increment(unknown_counts, "has_instruments")
            has_instruments = None
        if has_instruments is None or has_instruments is not filters.has_instruments:
            matches = False
    return matches


def _increment(counts: dict[str, int], name: str) -> None:
    counts[name] = counts.get(name, 0) + 1


def _combined_data_scope(
    observations: Sequence[RequestObservation], fallback: DataScope
) -> DataScope:
    scopes = {observation.data_scope for observation in observations}
    if not scopes:
        return fallback
    if len(scopes) == 1:
        return next(iter(scopes))
    return "unknown"


def _aggregate_issues(issues: Sequence[DataIssue]) -> tuple[DataIssue, ...]:
    grouped: dict[tuple[str, str], list[DataIssue]] = {}
    for issue in issues:
        grouped.setdefault((issue.code, issue.severity), []).append(issue)
    result: list[DataIssue] = []
    for (code, severity), values in grouped.items():
        examples: list[str] = []
        for value in values:
            for example in value.examples:
                located = "; ".join(
                    part
                    for part in (
                        f"request_id={value.request_id}" if value.request_id else "",
                        value.row_locator or "",
                        f"field={value.field_locator}" if value.field_locator else "",
                        example,
                    )
                    if part
                )[:512]
                if len(examples) < 20 and located not in examples:
                    examples.append(located)
        result.append(
            DataIssue(
                code=code,
                severity=severity,  # type: ignore[arg-type]
                occurrence_count=sum(value.occurrence_count for value in values),
                examples=tuple(examples),
            )
        )
    return tuple(result)


def _kalshi_discovery_report(
    *,
    started: datetime,
    stop_reason: DiscoveryStopReason,
    filters: KalshiFilter,
    requested_filters: tuple[tuple[str, object], ...],
    pages_fetched: int,
    rows_scanned: int,
    unique_markets_seen: int,
    duplicates_seen: int,
    unknown_counts: dict[str, int],
    data_scope: DataScope,
    observations: tuple[RequestObservation, ...],
    issues: tuple[DataIssue, ...],
    selection_strategy: str,
    requested_selector_count: int,
    queried_chunks: int,
    traversal_complete: bool,
) -> DiscoveryReport:
    return DiscoveryReport(
        started_at_utc=started,
        finished_at_utc=datetime.now(timezone.utc),
        stop_reason=stop_reason,
        pages_fetched=pages_fetched,
        rows_scanned=rows_scanned,
        unique_markets_seen=unique_markets_seen,
        duplicates_seen=duplicates_seen,
        requested_filters=requested_filters,
        applied_server_filters=_server_filter_names(filters),
        applied_local_filters=_local_filter_names(filters),
        unknown_filter_counts=tuple(sorted(unknown_counts.items())),
        source_scope=_source_scope(filters),
        data_scope=data_scope,
        interpretation_id=KALSHI_MARKET_INTERPRETATION_ID,
        package_version=__version__,
        observations=observations,
        issues=issues,
        endpoints=(_MARKETS_ENDPOINT,),
        pagination_strategy="opaque_cursor_round_robin",
        ordering="server_defined",
        selection_strategy=selection_strategy,
        requested_selector_count=requested_selector_count,
        queried_chunks=queried_chunks,
        traversal_complete=traversal_complete,
    )


__all__ = [
    "AsyncKalshiClient",
    "AsyncKalshiClient",
    "kalshi_events_dataframe",
    "kalshi_markets_dataframe",
    "normalize_kalshi_event",
    "normalize_kalshi_market",
    "normalize_kalshi_orderbook",
]
