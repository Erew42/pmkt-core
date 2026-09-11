from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, AsyncIterator, Iterable, Literal, Sequence
from urllib.parse import quote, urlparse
from uuid import uuid4

import httpx
from aiolimiter import AsyncLimiter

from pmkt._http import HttpClient, RequestPolicy
from pmkt._operation import OperationExpiry
from pmkt.config import PmktConfig, get_config
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
from pmkt.exchanges.read_auth import (
    ReadAuthHeaderProvider,
    ReadOnlyRequestError,
    headers_for_read,
)
from pmkt.records import (
    BookSnapshot,
    DataIssue,
    DataScope,
    DiscoveryReport,
    DiscoveryResult,
    DiscoveryStopReason,
    KalshiFilter,
    KalshiInstrumentRef,
    KalshiMarket,
    RequestObservation,
)
from pmkt import __version__

if TYPE_CHECKING:
    import pandas as pd


KALSHI_DISCOVERY_TICKER_CHUNK_SIZE = 20
_MARKETS_ENDPOINT = "/markets"
_MARKETS_PARAMETER_ALLOWLIST = frozenset(
    {"limit", "cursor", "status", "event_ticker", "series_ticker", "tickers", "mve_filter"}
)


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

    rows = [normalize_kalshi_market(market) for market in markets if isinstance(market, dict)]
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

    rows = [normalize_kalshi_event(event) for event in events if isinstance(event, dict)]
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
        max_retries: int = 3,
        limiter: AsyncLimiter | None = None,
        request_policy: RequestPolicy | None = None,
    ) -> None:
        super().__init__(
            base_url=base_url,
            transport=transport,
            timeout_s=timeout_s,
            max_retries=max_retries,
            limiter=limiter,
            request_policy=request_policy,
            source_venue="kalshi",
            source_service="kalshi",
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
    ) -> None:
        self.base_url = (
            base_url
            if base_url is not None
            else config.resolved_kalshi_api_url
            if config is not None
            else get_config().resolved_kalshi_api_url
        )
        self.header_provider = auth
        self.transport = transport
        self.limiter = limiter or AsyncLimiter(10, 1)
        self._http = KalshiHttpClient(
            base_url=self.base_url,
            auth=self.header_provider,
            transport=self.transport,
            limiter=self.limiter,
            request_policy=request_policy,
            timeout_s=timeout_s,
        )

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
        ticker: str,
        source: Literal["live", "historical"] = "live",
        deadline_s: float = 30.0,
    ) -> KalshiMarket:
        """Fetch one normalized market from exactly the selected Kalshi dataset."""

        _require_nonempty_string(ticker, "ticker")
        if source not in ("live", "historical"):
            raise ValueError("source must be 'live' or 'historical'")
        expiry = OperationExpiry.bounded(deadline_s)
        observations: list[RequestObservation] = []
        market = await self._get_market_with_expiry(
            ticker=ticker,
            source=source,
            expiry=expiry,
            observations=observations,
        )
        expiry.checkpoint()
        return market

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
            payload, observation = await self._http.request_json_observed(
                "GET",
                path,
                request_id=f"kalshi-detail-{uuid4().hex}",
                endpoint_template=template,
                parameter_allowlist=(),
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
                    data_scope=self._http.source.data_scope,
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
            payload, observation = await self._http.request_json_observed(
                "GET",
                _MARKETS_ENDPOINT,
                request_id=f"kalshi-discovery-{operation_id}-{pages_fetched + 1}",
                endpoint_template=_MARKETS_ENDPOINT,
                parameter_allowlist=_MARKETS_PARAMETER_ALLOWLIST,
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
            data_scope=_combined_data_scope(observations, self._http.source.data_scope),
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
            payload, _ = await self._http.request_json_observed(
                "GET",
                f"/markets/{encoded_ticker}/orderbook",
                request_id=f"kalshi-book-{uuid4().hex}",
                endpoint_template="/markets/{ticker}/orderbook",
                parameter_allowlist=(),
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
        data = await self._http.request_json("GET", "/markets", params=params)
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def market(self, ticker: str) -> dict[str, Any]:
        data = await self._http.request_json("GET", f"/markets/{ticker}", params=None)
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        market = data.get("market")
        if isinstance(market, dict):
            return market
        return data

    async def historical_market(self, ticker: str) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET",
            f"/historical/markets/{ticker}",
            params=None,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        market = data.get("market")
        if isinstance(market, dict):
            return market
        return data

    async def iter_markets(
        self,
        *,
        limit: int = 100,
        status: str | None = "open",
        max_pages: int | None = None,
        **params: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        cursor = params.pop("cursor", None)
        seen_cursors = {cursor} if cursor else set()
        pages = 0
        while True:
            if max_pages is not None and pages >= max_pages:
                break
            page = await self.markets_page(
                limit=limit,
                cursor=cursor,
                status=status,
                **params,
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
        **params: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        cursor = params.pop("cursor", None)
        pages = 0
        while True:
            if max_pages is not None and pages >= max_pages:
                break
            page = await self.events_page(
                limit=limit,
                cursor=cursor,
                status=status,
                **params,
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

    async def orderbook(self, ticker: str, *, depth: int | None = None) -> dict[str, Any]:
        params = {"depth": depth}
        data = await self._http.request_json(
            "GET",
            f"/markets/{ticker}/orderbook",
            params=params,
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def normalized_orderbook(
        self,
        ticker: str,
        *,
        depth: int | None = None,
    ) -> dict[str, Any]:
        data = await self.orderbook(ticker, depth=depth)
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
    ) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET",
            f"/series/{series_ticker}/markets/{ticker}/candlesticks",
            params={
                "start_ts": start_ts,
                "end_ts": end_ts,
                "period_interval": period_interval,
                "include_latest_before_start": include_latest_before_start,
            },
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def batch_market_candlesticks(
        self,
        market_tickers: str | Iterable[str],
        *,
        start_ts: int,
        end_ts: int,
        period_interval: int,
        include_latest_before_start: bool | None = None,
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
    ) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET",
            f"/historical/markets/{ticker}/candlesticks",
            params={
                "start_ts": start_ts,
                "end_ts": end_ts,
                "period_interval": period_interval,
            },
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def historical_cutoff(self) -> dict[str, Any]:
        data = await self._http.request_json("GET", "/historical/cutoff")
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def series(
        self,
        series_ticker: str,
        *,
        include_volume: bool | None = None,
    ) -> dict[str, Any]:
        data = await self._http.request_json(
            "GET",
            f"/series/{series_ticker}",
            params={"include_volume": include_volume},
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


KalshiClient = AsyncKalshiClient


__all__ = [
    "AsyncKalshiClient",
    "KalshiClient",
    "kalshi_events_dataframe",
    "kalshi_markets_dataframe",
    "normalize_kalshi_event",
    "normalize_kalshi_market",
    "normalize_kalshi_orderbook",
]
