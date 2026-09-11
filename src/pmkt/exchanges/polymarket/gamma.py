from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Sequence
from urllib.parse import quote
from uuid import uuid4

import httpx
from aiolimiter import AsyncLimiter
from pydantic import TypeAdapter

from pmkt._http import HttpClient, RequestPolicy
from pmkt._operation import OperationExpiry
from pmkt import __version__
from pmkt.errors import InvalidDataError, MarketNotFoundError
from pmkt.models import Event, Market
from pmkt.pagination import normalize_polymarket_cursor, polymarket_cursor_stop_reason
from pmkt.records import (
    DataScope,
    DataIssue,
    DiscoveryReport,
    DiscoveryResult,
    DiscoveryStopReason,
    PolymarketFilter,
    PolymarketMarket,
    RequestObservation,
)

from pmkt.exchanges.polymarket._workflow import (
    POLYMARKET_MARKET_INTERPRETATION_ID,
    decode_gamma_keyset_envelope,
    gamma_detail_identities,
    gamma_market_identity,
    gamma_page_identities,
    normalize_gamma_market,
)


from pmkt.config import PmktConfig, get_config


POLYMARKET_DISCOVERY_CONDITION_CHUNK_SIZE = 20
_KEYSET_ENDPOINT = "/markets/keyset"
_KEYSET_PARAMETER_ALLOWLIST = frozenset(
    {"limit", "after_cursor", "closed", "tag_id", "related_tags", "condition_ids"}
)


@dataclass
class _DiscoveryPartition:
    chunk_index: int
    condition_ids: tuple[str, ...] | None
    closed: bool
    cursor: str | None = None
    returned_cursors: set[str] | None = None

    def __post_init__(self) -> None:
        if self.returned_cursors is None:
            self.returned_cursors = set()


class AsyncGammaClient:
    """Gamma API client (discovery endpoints)."""

    def __init__(
        self,
        base_url: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        limiter: AsyncLimiter | None = None,
        request_policy: RequestPolicy | None = None,
        *,
        config: PmktConfig | None = None,
        timeout_s: float = 10.0,
    ) -> None:
        self.base_url = (
            base_url
            if base_url is not None
            else config.gamma_api_url
            if config is not None
            else get_config().gamma_api_url
        )
        self.transport = transport
        # Default to 10 requests per second if not provided
        self.limiter = limiter or AsyncLimiter(10, 1)
        self._http = HttpClient(
            base_url=self.base_url,
            transport=self.transport,
            limiter=self.limiter,
            request_policy=request_policy,
            timeout_s=timeout_s,
            source_venue="polymarket",
            source_service="gamma",
        )

    async def close(self) -> None:
        await self._http.close()

    def __enter__(self) -> "AsyncGammaClient":
        raise RuntimeError("Use 'async with' for AsyncGammaClient.")

    async def __aenter__(self) -> "AsyncGammaClient":
        await self._http.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    def _validate_pagination(self, limit: int, offset: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise TypeError("limit must be an int")
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        if isinstance(offset, bool) or not isinstance(offset, int):
            raise TypeError("offset must be an int")
        if offset < 0:
            raise ValueError("offset must be >= 0")

    @staticmethod
    def _validate_keyset_limit(limit: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise TypeError("limit must be an int")
        if limit < 1 or limit > 100:
            raise ValueError("keyset limit must be between 1 and 100")

    async def market(self, market_id: str | int) -> dict[str, Any]:
        """Fetch one Gamma market by id, preserving raw fields for resolution joins."""
        data = await self._http.request_json(
            "GET", f"/markets/{market_id}", params=None
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        return data

    async def market_with_events(self, market_id: str | int) -> dict[str, Any]:
        """Fetch one market and attach its Gamma event metadata.

        Gamma's single-market endpoint omits ``events``. The filtered list
        endpoint includes it, but requires the correct closed-state filter.
        Resolve the state from the detail response, then require exactly one
        matching list row before returning a combined payload.
        """

        key = str(market_id)
        payload = await self.market(market_id)
        rows = await self._http.request_json(
            "GET",
            "/markets",
            params={"id": key, "closed": bool(payload.get("closed"))},
        )
        if (
            not isinstance(rows, list)
            or len(rows) != 1
            or not isinstance(rows[0], dict)
        ):
            raise ValueError(
                f"expected exactly one filtered Gamma market row for {key}, got {rows!r}"
            )
        returned_key = str(rows[0].get("id") or "")
        if returned_key != key:
            raise ValueError(
                f"filtered Gamma market key mismatch: requested {key}, got {returned_key}"
            )
        result = dict(payload)
        result["events"] = rows[0].get("events")
        return result

    async def get_market(
        self, *, market_id: str, deadline_s: float = 30.0
    ) -> PolymarketMarket:
        """Fetch and strictly normalize one Gamma market by its native ID."""

        _require_nonempty_string(market_id, "market_id")
        expiry = OperationExpiry.bounded(deadline_s)
        observations: list[RequestObservation] = []
        request_id = f"gamma-detail-{uuid4().hex}"
        try:
            payload, observation = await self._http.request_json_observed(
                "GET",
                f"/markets/{quote(market_id, safe='')}",
                request_id=request_id,
                endpoint_template="/markets/{market_id}",
                parameter_allowlist=(),
                effective_parameters=None,
                params=None,
                expiry=expiry,
                response_identities=lambda value: gamma_detail_identities(
                    value, requested_market_id=market_id
                ),
                record_observation=observations.append,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise MarketNotFoundError(
                    venue="polymarket",
                    identifier=market_id,
                    lookup_scope="Gamma current market detail",
                ) from exc
            raise
        expiry.checkpoint()
        if observation.received_at_utc is None:
            raise InvalidDataError("Gamma detail observation has no receive time")
        assert isinstance(payload, dict)
        market = normalize_gamma_market(
            payload,
            observation=observation,
        )
        expiry.checkpoint()
        return market

    async def discover_markets(
        self,
        *,
        filters: PolymarketFilter | None = None,
        max_markets: int = 100,
        max_pages: int = 20,
        deadline_s: float = 60.0,
    ) -> DiscoveryResult[PolymarketMarket]:
        """Discover a bounded set of normalized Gamma markets."""

        selected_filters = filters if filters is not None else PolymarketFilter()
        if not isinstance(selected_filters, PolymarketFilter):
            raise TypeError("filters must be a PolymarketFilter or None")
        _require_positive_int(max_markets, "max_markets")
        _require_positive_int(max_pages, "max_pages")
        expiry = OperationExpiry.bounded(deadline_s)
        started = datetime.now(timezone.utc)
        requested_filters = _reported_filters(selected_filters)
        requested_ids = _deduplicate(selected_filters.condition_ids or ())
        requested_id_set = frozenset(requested_ids)
        selection_strategy = (
            "targeted_condition_ids" if selected_filters.condition_ids is not None else "keyset_scan"
        )
        if selected_filters.condition_ids == ():
            expiry.checkpoint()
            report = _discovery_report(
                started=started,
                stop_reason="empty_selection",
                pages_fetched=0,
                rows_scanned=0,
                unique_markets_seen=0,
                duplicates_seen=0,
                requested_filters=requested_filters,
                server_filters=_server_filter_names(selected_filters),
                local_filters=_local_filter_names(selected_filters),
                unknown_counts={},
                source_scope=_source_scope(selected_filters),
                data_scope=self._http.source.data_scope,
                observations=(),
                issues=(),
                selection_strategy=selection_strategy,
                requested_selector_count=0,
                queried_chunks=0,
                traversal_complete=True,
            )
            return DiscoveryResult(items=(), report=report)

        chunks: tuple[tuple[str, ...] | None, ...]
        if requested_ids:
            chunks = tuple(
                requested_ids[index : index + POLYMARKET_DISCOVERY_CONDITION_CHUNK_SIZE]
                for index in range(
                    0, len(requested_ids), POLYMARKET_DISCOVERY_CONDITION_CHUNK_SIZE
                )
            )
        else:
            chunks = (None,)
        lifecycle = (
            (False, True) if selected_filters.closed is None else (selected_filters.closed,)
        )
        partitions = deque(
            _DiscoveryPartition(index, chunk, closed)
            for index, chunk in enumerate(chunks)
            for closed in lifecycle
        )

        observations: list[RequestObservation] = []
        items: list[PolymarketMarket] = []
        seen_market_ids: set[str] = set()
        unknown_counts: dict[str, int] = {}
        issues: list[DataIssue] = []
        queried_chunk_indexes: set[int] = set()
        pages_fetched = 0
        rows_scanned = 0
        duplicates_seen = 0
        operation_id = uuid4().hex
        stop_reason: DiscoveryStopReason | None = None

        while partitions and stop_reason is None:
            if pages_fetched >= max_pages:
                stop_reason = "page_limit"
                break
            partition = partitions.popleft()
            params: dict[str, Any] = {
                "limit": 100,
                "after_cursor": partition.cursor,
                "closed": partition.closed,
                "tag_id": [selected_filters.tag_id]
                if selected_filters.tag_id is not None
                else None,
                "related_tags": selected_filters.related_tags,
                "condition_ids": list(partition.condition_ids)
                if partition.condition_ids is not None
                else None,
            }
            effective_parameters = {
                key: value for key, value in params.items() if value is not None
            }
            request_id = f"gamma-discovery-{operation_id}-{pages_fetched + 1}"
            payload, observation = await self._http.request_json_observed(
                "GET",
                _KEYSET_ENDPOINT,
                request_id=request_id,
                endpoint_template=_KEYSET_ENDPOINT,
                parameter_allowlist=_KEYSET_PARAMETER_ALLOWLIST,
                effective_parameters=effective_parameters,
                params=params,
                expiry=expiry,
                response_identities=gamma_page_identities,
                record_observation=observations.append,
            )
            expiry.checkpoint()
            rows, next_cursor = decode_gamma_keyset_envelope(payload)
            pages_fetched += 1
            queried_chunk_indexes.add(partition.chunk_index)
            if observation.received_at_utc is None:
                raise InvalidDataError("Gamma page observation has no receive time")
            if next_cursor is not None:
                assert partition.returned_cursors is not None
                if next_cursor in partition.returned_cursors:
                    raise InvalidDataError(
                        "Gamma keyset returned a repeated cursor within one partition"
                    )

            for row in rows:
                expiry.checkpoint()
                market_id, _ = gamma_market_identity(row)
                rows_scanned += 1
                if market_id in seen_market_ids:
                    duplicates_seen += 1
                    continue
                seen_market_ids.add(market_id)
                market = normalize_gamma_market(
                    row,
                    observation=observation,
                )
                issues.extend(market.issues)
                if not _market_matches(
                    market,
                    selected_filters,
                    requested_condition_ids=requested_id_set,
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
        traversal_complete = stop_reason == "source_exhausted"
        report = _discovery_report(
            started=started,
            stop_reason=stop_reason,
            pages_fetched=pages_fetched,
            rows_scanned=rows_scanned,
            unique_markets_seen=len(seen_market_ids),
            duplicates_seen=duplicates_seen,
            requested_filters=requested_filters,
            server_filters=_server_filter_names(selected_filters),
            local_filters=_local_filter_names(selected_filters),
            unknown_counts=unknown_counts,
            source_scope=_source_scope(selected_filters),
            data_scope=_combined_data_scope(observations, self._http.source.data_scope),
            observations=tuple(observations),
            issues=_aggregate_issues(issues),
            selection_strategy=selection_strategy,
            requested_selector_count=len(requested_ids),
            queried_chunks=len(queried_chunk_indexes),
            traversal_complete=traversal_complete,
        )
        expiry.checkpoint()
        return DiscoveryResult(items=tuple(items), report=report)

    async def markets_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        closed: bool | None = None,
        tag_id: str | None = None,
        related_tags: bool | None = None,
        exclude_tag_id: str | None = None,
    ) -> list[Market]:
        """Fetch one page from /markets using limit/offset pagination."""
        self._validate_pagination(limit, offset)
        data = await self.markets_raw_page(
            limit=limit,
            offset=offset,
            closed=closed,
            tag_id=tag_id,
            related_tags=related_tags,
            exclude_tag_id=exclude_tag_id,
        )
        return TypeAdapter(list[Market]).validate_python(data)

    async def markets_raw_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        closed: bool | None = None,
        tag_id: str | None = None,
        related_tags: bool | None = None,
        exclude_tag_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch one page while preserving decoded venue payload fields."""
        self._validate_pagination(limit, offset)
        data = await self._http.request_json(
            "GET",
            "/markets",
            params={
                "limit": limit,
                "offset": offset,
                "closed": closed,
                "tag_id": tag_id,
                "related_tags": related_tags,
                "exclude_tag_id": exclude_tag_id,
            },
        )
        if not isinstance(data, list) or any(not isinstance(row, dict) for row in data):
            raise TypeError(f"Expected list[dict], got {type(data)}")
        return data

    async def markets_keyset_page(
        self,
        *,
        limit: int = 100,
        after_cursor: str | None = None,
        closed: bool | None = None,
        tag_id: str | None = None,
        related_tags: bool | None = None,
        exclude_tag_id: str | None = None,
        condition_ids: Sequence[str] | None = None,
        order: str | None = None,
        ascending: bool | None = None,
    ) -> dict[str, Any]:
        """Fetch one page from /markets/keyset using cursor pagination."""
        self._validate_keyset_limit(limit)
        data = await self.markets_keyset_raw_page(
            limit=limit,
            after_cursor=after_cursor,
            closed=closed,
            tag_id=tag_id,
            related_tags=related_tags,
            exclude_tag_id=exclude_tag_id,
            condition_ids=condition_ids,
            order=order,
            ascending=ascending,
        )
        return {
            **data,
            "markets": TypeAdapter(list[Market]).validate_python(data["markets"]),
        }

    async def markets_keyset_raw_page(
        self,
        *,
        limit: int = 100,
        after_cursor: str | None = None,
        closed: bool | None = None,
        tag_id: str | None = None,
        related_tags: bool | None = None,
        exclude_tag_id: str | None = None,
        condition_ids: Sequence[str] | None = None,
        order: str | None = None,
        ascending: bool | None = None,
    ) -> dict[str, Any]:
        """Fetch a keyset page while preserving decoded venue payload fields."""
        self._validate_keyset_limit(limit)
        data = await self._http.request_json(
            "GET",
            "/markets/keyset",
            params={
                "limit": limit,
                "after_cursor": after_cursor,
                "closed": closed,
                "tag_id": tag_id,
                "related_tags": related_tags,
                "exclude_tag_id": exclude_tag_id,
                "condition_ids": list(condition_ids) if condition_ids else None,
                "order": order,
                "ascending": ascending,
            },
        )
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict, got {type(data)}")
        markets = data.get("markets")
        if not isinstance(markets, list) or any(
            not isinstance(row, dict) for row in markets
        ):
            raise TypeError("Expected keyset response field 'markets' to be list[dict]")
        return data

    async def events_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        closed: bool | None = None,
        tag_id: str | None = None,
        related_tags: bool | None = None,
        exclude_tag_id: str | None = None,
        order: str | None = None,
        ascending: bool | None = None,
    ) -> list[Event]:
        """Fetch one page from /events using limit/offset pagination."""
        self._validate_pagination(limit, offset)
        params = {
            "limit": limit,
            "offset": offset,
            "closed": closed,
            "tag_id": tag_id,
            "related_tags": related_tags,
            "exclude_tag_id": exclude_tag_id,
            "order": order,
            "ascending": ascending,
        }
        data = await self._http.request_json("GET", "/events", params=params)
        if not isinstance(data, list):
            raise TypeError(f"Expected list, got {type(data)}")
        return TypeAdapter(list[Event]).validate_python(data)

    async def iter_markets(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        closed: bool | None = None,
        tag_id: str | None = None,
        related_tags: bool | None = None,
        exclude_tag_id: str | None = None,
    ) -> AsyncIterator[Market]:
        """Iterate over /markets until pagination exhausts."""
        current_offset = offset
        while True:
            page = await self.markets_page(
                limit=limit,
                offset=current_offset,
                closed=closed,
                tag_id=tag_id,
                related_tags=related_tags,
                exclude_tag_id=exclude_tag_id,
            )
            if not page:
                break
            for market in page:
                yield market
            if len(page) < limit:
                break
            current_offset += limit

    async def iter_markets_keyset(
        self,
        *,
        limit: int = 100,
        after_cursor: str | None = None,
        closed: bool | None = None,
        tag_id: str | None = None,
        related_tags: bool | None = None,
        exclude_tag_id: str | None = None,
        condition_ids: Sequence[str] | None = None,
        order: str | None = None,
        ascending: bool | None = None,
    ) -> AsyncIterator[Market]:
        """Iterate over /markets/keyset until cursor exhaustion."""
        cursor = after_cursor
        while True:
            page = await self.markets_keyset_page(
                limit=limit,
                after_cursor=cursor,
                closed=closed,
                tag_id=tag_id,
                related_tags=related_tags,
                exclude_tag_id=exclude_tag_id,
                condition_ids=condition_ids,
                order=order,
                ascending=ascending,
            )
            markets = page["markets"]
            if not markets:
                break
            for market in markets:
                yield market
            next_cursor = normalize_polymarket_cursor(page.get("next_cursor"))
            if polymarket_cursor_stop_reason(next_cursor, previous_cursor=cursor):
                break
            cursor = next_cursor

    async def iter_events(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        closed: bool | None = None,
        tag_id: str | None = None,
        related_tags: bool | None = None,
        exclude_tag_id: str | None = None,
        order: str | None = None,
        ascending: bool | None = None,
    ) -> AsyncIterator[Event]:
        """Iterate over /events until pagination exhausts."""
        current_offset = offset
        while True:
            page = await self.events_page(
                limit=limit,
                offset=current_offset,
                closed=closed,
                tag_id=tag_id,
                related_tags=related_tags,
                exclude_tag_id=exclude_tag_id,
                order=order,
                ascending=ascending,
            )
            if not page:
                break
            for event in page:
                yield event
            if len(page) < limit:
                break
            current_offset += limit


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


def _reported_filters(filters: PolymarketFilter) -> tuple[tuple[str, object], ...]:
    values: tuple[tuple[str, object | None], ...] = (
        ("condition_ids", filters.condition_ids),
        ("closed", filters.closed),
        ("tag_id", filters.tag_id),
        ("related_tags", filters.related_tags),
        ("question_contains", filters.question_contains),
        ("outcome_count", filters.outcome_count),
        ("has_instruments", filters.has_instruments),
    )
    return tuple((name, value) for name, value in values if value is not None)


def _server_filter_names(filters: PolymarketFilter) -> tuple[str, ...]:
    names = ["closed"]
    for name, value in (
        ("condition_ids", filters.condition_ids),
        ("tag_id", filters.tag_id),
        ("related_tags", filters.related_tags),
    ):
        if value is not None:
            names.append(name)
    return tuple(names)


def _local_filter_names(filters: PolymarketFilter) -> tuple[str, ...]:
    names: list[str] = []
    for name, value in (
        ("condition_ids", filters.condition_ids),
        ("closed", filters.closed),
        ("question_contains", filters.question_contains),
        ("outcome_count", filters.outcome_count),
        ("has_instruments", filters.has_instruments),
    ):
        if value is not None:
            names.append(name)
    return tuple(names)


def _source_scope(filters: PolymarketFilter) -> str:
    if filters.closed is None:
        return "gamma_keyset_closed_false_then_true"
    return f"gamma_keyset_closed_{str(filters.closed).lower()}"


def _market_matches(
    market: PolymarketMarket,
    filters: PolymarketFilter,
    *,
    requested_condition_ids: frozenset[str],
    unknown_counts: dict[str, int],
) -> bool:
    matches = True
    if filters.condition_ids is not None:
        if market.ref.condition_id is None:
            _increment(unknown_counts, "condition_ids")
            matches = False
        elif market.ref.condition_id not in requested_condition_ids:
            matches = False
    if filters.closed is not None:
        if market.closed is None:
            _increment(unknown_counts, "closed")
            matches = False
        elif market.closed is not filters.closed:
            matches = False
    if filters.question_contains is not None:
        if market.question is None:
            _increment(unknown_counts, "question_contains")
            matches = False
        elif filters.question_contains.casefold() not in market.question.casefold():
            matches = False
    if filters.outcome_count is not None:
        if not market.outcome_labels_valid:
            _increment(unknown_counts, "outcome_count")
            matches = False
        elif len(market.outcome_labels) != filters.outcome_count:
            matches = False
    if filters.has_instruments is not None:
        if market.mapping_status == "mapped":
            has_instruments = bool(market.instruments)
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


def _discovery_report(
    *,
    started: datetime,
    stop_reason: DiscoveryStopReason,
    pages_fetched: int,
    rows_scanned: int,
    unique_markets_seen: int,
    duplicates_seen: int,
    requested_filters: tuple[tuple[str, object], ...],
    server_filters: tuple[str, ...],
    local_filters: tuple[str, ...],
    unknown_counts: dict[str, int],
    source_scope: str,
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
        applied_server_filters=server_filters,
        applied_local_filters=local_filters,
        unknown_filter_counts=tuple(sorted(unknown_counts.items())),
        source_scope=source_scope,
        data_scope=data_scope,
        interpretation_id=POLYMARKET_MARKET_INTERPRETATION_ID,
        package_version=__version__,
        observations=observations,
        issues=issues,
        endpoints=(_KEYSET_ENDPOINT,),
        pagination_strategy="opaque_cursor_round_robin",
        ordering="server_defined",
        selection_strategy=selection_strategy,
        requested_selector_count=requested_selector_count,
        queried_chunks=queried_chunks,
        traversal_complete=traversal_complete,
    )

GammaClient = AsyncGammaClient
