from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Mapping, Sequence

import httpx
from pmkt.runtime import OperationExpiry
from aiolimiter import AsyncLimiter

from pmkt.runtime import RequestPolicy
from pmkt._http import HttpClient
from pmkt.config import PmktConfig


MAX_OPEN_INTEREST_MARKETS = 25
DEFAULT_DATA_API_MAX_RATE = 10
DEFAULT_DATA_API_PERIOD_SECONDS = 1
MAX_V2_PAGE_SIZE = 1000
_WALLET_RE = re.compile(r"0x[0-9a-fA-F]{40}\Z")
_CONDITION_RE = re.compile(r"0x[0-9a-fA-F]{64}\Z")


def normalize_condition_ids(condition_ids: Sequence[object]) -> tuple[str, ...]:
    requested: list[str] = []
    seen: set[str] = set()
    for value in condition_ids:
        token = str(value).strip()
        if not token or token in seen:
            continue
        seen.add(token)
        requested.append(token)
    if not requested:
        raise ValueError("At least one Polymarket condition ID is required.")
    if len(requested) > MAX_OPEN_INTEREST_MARKETS:
        raise ValueError(
            f"Polymarket /oi accepts at most {MAX_OPEN_INTEREST_MARKETS} markets per request."
        )
    return tuple(requested)


@dataclass(frozen=True)
class PolymarketOpenInterestBatch:
    requested_keys: tuple[str, ...]
    values: dict[str, Decimal]
    omitted_keys: tuple[str, ...]
    response_keys: tuple[str, ...]

    @property
    def value_coverage_complete(self) -> bool:
        return not self.omitted_keys

    @property
    def coverage_rate(self) -> float:
        return len(self.values) / len(self.requested_keys)


def normalize_polymarket_open_interest(
    requested_condition_ids: Sequence[object],
    payload: object,
) -> PolymarketOpenInterestBatch:
    requested = normalize_condition_ids(requested_condition_ids)
    if not isinstance(payload, list):
        raise TypeError("Polymarket /oi response must be a list.")

    requested_set = set(requested)
    values: dict[str, Decimal] = {}
    response_keys: list[str] = []
    for row in payload:
        if not isinstance(row, Mapping):
            raise TypeError("Polymarket /oi response rows must be objects.")
        market = str(row.get("market") or "").strip()
        if not market:
            raise ValueError("Polymarket /oi response row lacks market.")
        if market not in requested_set:
            raise ValueError(f"Unexpected Polymarket /oi market: {market}")
        if market in values:
            raise ValueError(f"Duplicate Polymarket /oi market: {market}")
        raw_value = row.get("value")
        if isinstance(raw_value, bool):
            raise ValueError(f"Invalid Polymarket /oi value for {market}: {raw_value}")
        try:
            value = Decimal(str(raw_value))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(
                f"Invalid Polymarket /oi value for {market}: {raw_value}"
            ) from exc
        if not value.is_finite() or value < 0:
            raise ValueError(f"Invalid Polymarket /oi value for {market}: {raw_value}")
        values[market] = value
        response_keys.append(market)

    omitted = tuple(key for key in requested if key not in values)
    return PolymarketOpenInterestBatch(
        requested_keys=requested,
        values=values,
        omitted_keys=omitted,
        response_keys=tuple(response_keys),
    )


@dataclass(frozen=True)
class PolymarketPosition:
    """One Data API v2 position, retaining its source lifecycle status."""

    wallet: str
    condition_id: str
    token_id: str
    status: str
    current_size: Decimal
    total_size: Decimal | None
    outcome: str | None
    avg_price: Decimal | None
    current_value: Decimal | None
    realized_pnl: Decimal | None
    unrealized_pnl: Decimal | None


@dataclass(frozen=True)
class PolymarketWalletTrade:
    """A wallet-attributed Data API v2 trade (not a CLOB fill)."""

    wallet: str
    condition_id: str
    token_id: str
    side: str
    size: Decimal
    price: Decimal
    timestamp_s: int
    transaction_hash: str
    outcome: str | None


@dataclass(frozen=True)
class PolymarketParticipant:
    wallet: str
    current_positions: tuple[PolymarketPosition, ...]
    past_positions: tuple[PolymarketPosition, ...]


@dataclass(frozen=True)
class PolymarketMarketParticipants:
    condition_id: str
    participants: tuple[PolymarketParticipant, ...]
    current_pages: int
    past_pages: int
    current_next_cursor: str | None
    past_next_cursor: str | None
    started_at_utc: datetime
    completed_at_utc: datetime

    @property
    def complete(self) -> bool:
        return self.current_next_cursor is None and self.past_next_cursor is None


@dataclass(frozen=True)
class PolymarketWalletHistory:
    wallet: str
    trades: tuple[PolymarketWalletTrade, ...]
    current_positions: tuple[PolymarketPosition, ...]
    past_positions: tuple[PolymarketPosition, ...]
    trade_pages: int
    current_pages: int
    past_pages: int
    trades_next_cursor: str | None
    current_next_cursor: str | None
    past_next_cursor: str | None
    started_at_utc: datetime
    completed_at_utc: datetime

    @property
    def complete(self) -> bool:
        return (
            self.trades_next_cursor is None
            and self.current_next_cursor is None
            and self.past_next_cursor is None
        )


@dataclass(frozen=True)
class _DataPage:
    rows: tuple[dict[str, Any], ...]
    next_cursor: str | None


def _identifier(value: str, *, name: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValueError(f"{name} must be a 0x-prefixed hexadecimal identifier")
    return value.lower()


def _positive_int(value: int, *, name: str, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be at most {maximum}")
    return value


def _text(row: Mapping[str, Any], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Data API v2 row lacks {key}")
    return value


def _optional_text(row: Mapping[str, Any], key: str) -> str | None:
    value = row.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Data API v2 {key} must be a string or null")
    return value


def _decimal(row: Mapping[str, Any], key: str, *, required: bool = False) -> Decimal | None:
    value = row.get(key)
    if value is None:
        if required:
            raise ValueError(f"Data API v2 row lacks {key}")
        return None
    if isinstance(value, bool):
        raise ValueError(f"Data API v2 {key} must be numeric")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Data API v2 {key} must be numeric") from exc
    if not result.is_finite():
        raise ValueError(f"Data API v2 {key} must be finite")
    return result


def _position(row: Mapping[str, Any], *, wallet: str | None, condition_id: str | None) -> PolymarketPosition:
    found_wallet = _identifier(_text(row, "proxy_wallet"), name="proxy_wallet", pattern=_WALLET_RE)
    found_condition = _identifier(_text(row, "condition_id"), name="condition_id", pattern=_CONDITION_RE)
    if wallet is not None and found_wallet != wallet:
        raise ValueError("Data API v2 position wallet differs from the request")
    if condition_id is not None and found_condition != condition_id:
        raise ValueError("Data API v2 position condition differs from the request")
    current_size = _decimal(row, "current_size", required=True)
    assert current_size is not None
    if current_size < 0:
        raise ValueError("Data API v2 current_size must be nonnegative")
    return PolymarketPosition(
        wallet=found_wallet,
        condition_id=found_condition,
        token_id=_text(row, "token_id"),
        status=_text(row, "status"),
        current_size=current_size,
        total_size=_decimal(row, "total_size"),
        outcome=_optional_text(row, "outcome"),
        avg_price=_decimal(row, "avg_price"),
        current_value=_decimal(row, "current_value"),
        realized_pnl=_decimal(row, "realized_pnl"),
        unrealized_pnl=_decimal(row, "unrealized_pnl"),
    )


def _trade(row: Mapping[str, Any], *, wallet: str) -> PolymarketWalletTrade:
    found_wallet = _identifier(_text(row, "proxy_wallet"), name="proxy_wallet", pattern=_WALLET_RE)
    if found_wallet != wallet:
        raise ValueError("Data API v2 trade wallet differs from the request")
    size = _decimal(row, "size", required=True)
    price = _decimal(row, "price", required=True)
    assert size is not None and price is not None
    if size <= 0 or not 0 <= price <= 1:
        raise ValueError("Data API v2 trade size or price is invalid")
    timestamp = row.get("timestamp")
    if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp < 0:
        raise ValueError("Data API v2 trade timestamp must be epoch seconds")
    return PolymarketWalletTrade(
        wallet=found_wallet,
        condition_id=_identifier(_text(row, "condition_id"), name="condition_id", pattern=_CONDITION_RE),
        token_id=_text(row, "token_id"),
        side=_text(row, "side"),
        size=size,
        price=price,
        timestamp_s=timestamp,
        transaction_hash=_text(row, "transaction_hash"),
        outcome=_optional_text(row, "outcome"),
    )


class AsyncPolymarketDataClient:
    """Public Polymarket Data API client for activity endpoints."""

    def __init__(
        self,
        base_url: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        limiter: AsyncLimiter | None = None,
        *,
        config: PmktConfig | None = None,
        timeout_s: float = 30.0,
        request_policy: RequestPolicy | None = None,
    ) -> None:
        self.base_url = (
            base_url
            if base_url is not None
            else config.polymarket_data_api_url
            if config is not None
            else PmktConfig().polymarket_data_api_url
        )
        self.limiter = limiter or AsyncLimiter(
            DEFAULT_DATA_API_MAX_RATE,
            DEFAULT_DATA_API_PERIOD_SECONDS,
        )
        self._http = HttpClient(
            base_url=self.base_url,
            transport=transport,
            limiter=self.limiter,
            timeout_s=timeout_s,
            request_policy=request_policy,
        )

    async def __aenter__(self) -> "AsyncPolymarketDataClient":
        await self._http.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        await self._http.close()

    async def open_interest_page(
        self, condition_ids: Sequence[object], *, expiry: OperationExpiry | None = None
    ) -> list[dict[str, Any]]:
        requested = normalize_condition_ids(condition_ids)
        payload = await self._http.request_json(
            "GET", "/oi", params={"market": ",".join(requested)}, expiry=expiry
        )
        if not isinstance(payload, list) or any(
            not isinstance(row, dict) for row in payload
        ):
            raise TypeError("Polymarket /oi response must be list[dict].")
        return payload

    async def _v2_page(
        self, path: str, params: dict[str, Any], *, expiry: OperationExpiry | None
    ) -> _DataPage:
        response = await self._http.request_response("GET", path, params=params, expiry=expiry)
        response.raise_for_status()
        if expiry is not None:
            expiry.checkpoint()
        payload = json.loads(response.content, parse_float=Decimal)
        if expiry is not None:
            expiry.checkpoint()
        if not isinstance(payload, dict):
            raise TypeError(f"{path} response must be an object")
        rows = payload.get("data")
        pagination = payload.get("pagination")
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise TypeError(f"{path} data must be a list of objects")
        if not isinstance(pagination, dict):
            raise TypeError(f"{path} pagination must be an object")
        has_more = pagination.get("has_more")
        next_cursor = pagination.get("next_cursor")
        if not isinstance(has_more, bool):
            raise TypeError(f"{path} pagination.has_more must be boolean")
        if has_more:
            if not isinstance(next_cursor, str) or not next_cursor:
                raise ValueError(f"{path} has_more requires next_cursor")
        elif next_cursor is not None:
            raise ValueError(f"{path} terminal page has next_cursor")
        return _DataPage(tuple(rows), next_cursor)

    async def positions_page(
        self,
        *,
        condition_id: str | None = None,
        wallet: str | None = None,
        status: str = "OPEN",
        page_size: int = 1000,
        cursor: str | None = None,
        expiry: OperationExpiry | None = None,
    ) -> tuple[tuple[PolymarketPosition, ...], str | None]:
        """Read one current or exited-position page for a wallet or one market."""
        if condition_id is None and wallet is None:
            raise ValueError("condition_id or wallet is required")
        condition = (
            _identifier(condition_id, name="condition_id", pattern=_CONDITION_RE)
            if condition_id is not None else None
        )
        user = (
            _identifier(wallet, name="wallet", pattern=_WALLET_RE)
            if wallet is not None else None
        )
        if status not in ("OPEN", "CLOSED"):
            raise ValueError("status must be OPEN or CLOSED")
        _positive_int(page_size, name="page_size", maximum=MAX_V2_PAGE_SIZE)
        if cursor is not None and (not isinstance(cursor, str) or not cursor):
            raise ValueError("cursor must be a nonempty string or None")
        page = await self._v2_page(
            "/v2/positions",
            {
                "condition": condition,
                "user": user,
                "status": status,
                "limit": page_size,
                "cursor": cursor,
                "filter_type": "TOKENS",
                "filter_amount": 0,
                "include_archived": True if status == "OPEN" else None,
            },
            expiry=expiry,
        )
        positions = tuple(
            _position(row, wallet=user, condition_id=condition) for row in page.rows
        )
        if expiry is not None:
            expiry.checkpoint()
        return positions, page.next_cursor

    async def trades_page(
        self,
        *,
        wallet: str,
        page_size: int = 1000,
        cursor: str | None = None,
        expiry: OperationExpiry | None = None,
    ) -> tuple[tuple[PolymarketWalletTrade, ...], str | None]:
        """Read one wallet trade page from the full-history feed."""
        user = _identifier(wallet, name="wallet", pattern=_WALLET_RE)
        _positive_int(page_size, name="page_size", maximum=MAX_V2_PAGE_SIZE)
        if cursor is not None and (not isinstance(cursor, str) or not cursor):
            raise ValueError("cursor must be a nonempty string or None")
        page = await self._v2_page(
            "/v2/trades",
            {"user": user, "full_history": True, "limit": page_size, "cursor": cursor},
            expiry=expiry,
        )
        trades = tuple(_trade(row, wallet=user) for row in page.rows)
        if expiry is not None:
            expiry.checkpoint()
        return trades, page.next_cursor

    async def _scan_positions(
        self,
        *,
        condition_id: str | None,
        wallet: str | None,
        status: str,
        page_size: int,
        max_pages: int,
        expiry: OperationExpiry,
    ) -> tuple[tuple[PolymarketPosition, ...], int, str | None]:
        rows: list[PolymarketPosition] = []
        seen_cursors: set[str] = set()
        cursor: str | None = None
        for page_count in range(1, max_pages + 1):
            page_rows, next_cursor = await self.positions_page(
                condition_id=condition_id,
                wallet=wallet,
                status=status,
                page_size=page_size,
                cursor=cursor,
                expiry=expiry,
            )
            rows.extend(page_rows)
            if next_cursor is None:
                return tuple(rows), page_count, None
            if next_cursor in seen_cursors:
                raise ValueError("Data API v2 repeated a positions cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return tuple(rows), max_pages, cursor

    async def _scan_trades(
        self,
        *,
        wallet: str,
        page_size: int,
        max_pages: int,
        expiry: OperationExpiry,
    ) -> tuple[tuple[PolymarketWalletTrade, ...], int, str | None]:
        rows: list[PolymarketWalletTrade] = []
        seen_cursors: set[str] = set()
        cursor: str | None = None
        for page_count in range(1, max_pages + 1):
            page_rows, next_cursor = await self.trades_page(
                wallet=wallet, page_size=page_size, cursor=cursor, expiry=expiry
            )
            rows.extend(page_rows)
            if next_cursor is None:
                return tuple(rows), page_count, None
            if next_cursor in seen_cursors:
                raise ValueError("Data API v2 repeated a trades cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return tuple(rows), max_pages, cursor

    async def market_participants(
        self,
        condition_id: str,
        *,
        page_size: int = 1000,
        max_pages_per_status: int = 20,
        deadline_s: float = 120.0,
    ) -> PolymarketMarketParticipants:
        """Find current and exited positions in one Polymarket condition.

        The two status walks are not an atomic historical snapshot. A retained
        cursor means the corresponding walk stopped at the caller's page cap.
        """
        condition = _identifier(condition_id, name="condition_id", pattern=_CONDITION_RE)
        _positive_int(page_size, name="page_size", maximum=MAX_V2_PAGE_SIZE)
        _positive_int(max_pages_per_status, name="max_pages_per_status")
        started = datetime.now(timezone.utc)
        expiry = OperationExpiry.bounded(deadline_s)
        current, current_pages, current_cursor = await self._scan_positions(
            condition_id=condition, wallet=None, status="OPEN", page_size=page_size,
            max_pages=max_pages_per_status, expiry=expiry,
        )
        past, past_pages, past_cursor = await self._scan_positions(
            condition_id=condition, wallet=None, status="CLOSED", page_size=page_size,
            max_pages=max_pages_per_status, expiry=expiry,
        )
        by_wallet: dict[str, tuple[list[PolymarketPosition], list[PolymarketPosition]]] = {}
        for position in current:
            by_wallet.setdefault(position.wallet, ([], []))[0].append(position)
        for position in past:
            by_wallet.setdefault(position.wallet, ([], []))[1].append(position)
        participants = tuple(
            PolymarketParticipant(wallet, tuple(positions[0]), tuple(positions[1]))
            for wallet, positions in sorted(by_wallet.items())
        )
        expiry.checkpoint()
        return PolymarketMarketParticipants(
            condition, participants, current_pages, past_pages,
            current_cursor, past_cursor, started, datetime.now(timezone.utc),
        )

    async def wallet_history(
        self,
        wallet: str,
        *,
        page_size: int = 1000,
        max_pages_per_feed: int = 20,
        deadline_s: float = 120.0,
    ) -> PolymarketWalletHistory:
        """Read a wallet's full trade feed and its current and exited positions."""
        user = _identifier(wallet, name="wallet", pattern=_WALLET_RE)
        _positive_int(page_size, name="page_size", maximum=MAX_V2_PAGE_SIZE)
        _positive_int(max_pages_per_feed, name="max_pages_per_feed")
        started = datetime.now(timezone.utc)
        expiry = OperationExpiry.bounded(deadline_s)
        trades, trade_pages, trades_cursor = await self._scan_trades(
            wallet=user, page_size=page_size, max_pages=max_pages_per_feed, expiry=expiry
        )
        current, current_pages, current_cursor = await self._scan_positions(
            condition_id=None, wallet=user, status="OPEN", page_size=page_size,
            max_pages=max_pages_per_feed, expiry=expiry,
        )
        past, past_pages, past_cursor = await self._scan_positions(
            condition_id=None, wallet=user, status="CLOSED", page_size=page_size,
            max_pages=max_pages_per_feed, expiry=expiry,
        )
        expiry.checkpoint()
        return PolymarketWalletHistory(
            user, trades, current, past, trade_pages, current_pages, past_pages,
            trades_cursor, current_cursor, past_cursor, started, datetime.now(timezone.utc),
        )


__all__ = [
    "AsyncPolymarketDataClient",
    "DEFAULT_DATA_API_MAX_RATE",
    "DEFAULT_DATA_API_PERIOD_SECONDS",
    "MAX_OPEN_INTEREST_MARKETS",
    "MAX_V2_PAGE_SIZE",
    "PolymarketMarketParticipants",
    "PolymarketOpenInterestBatch",
    "PolymarketParticipant",
    "PolymarketPosition",
    "PolymarketWalletHistory",
    "PolymarketWalletTrade",
    "normalize_condition_ids",
    "normalize_polymarket_open_interest",
]
