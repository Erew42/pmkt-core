"""Strict private decoding for Kalshi public REST workflows."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Literal, Sequence

from pmkt import __version__
from pmkt.runtime import OperationExpiry
from pmkt.data.kalshi_quotes import KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT
from pmkt.data.normalize_kalshi import normalize_kalshi_market_status
from pmkt.data.prices import complement_probability
from pmkt.data.types import parse_float
from pmkt.errors import InvalidDataError
from pmkt.records import (
    ResultProvenance,
    RawResponseEvidence,
    BookLevel,
    BookSnapshot,
    DataIssue,
    KalshiInstrumentRef,
    KalshiMarket,
    KalshiMarketRef,
    RequestObservation,
    DataScope,
)


KALSHI_MARKET_INTERPRETATION_ID = "kalshi_market_metadata.v1"
KALSHI_BOOK_INTERPRETATION_ID = "kalshi_orderbook_fp_book.v1"
_MISSING = object()


def _levels_to_map(
    levels: Any, *, expiry: OperationExpiry | None = None
) -> dict[float, float]:
    """Retain the compatible permissive native normalizer's last-price-wins policy."""

    parsed: dict[float, float] = {}
    if not isinstance(levels, list):
        return parsed
    for index, level in enumerate(levels):
        if expiry is not None and index % 256 == 0:
            expiry.checkpoint()
        if isinstance(level, dict):
            price = parse_float(level.get("price") or level.get("price_dollars"))
            size = parse_float(
                level.get("size") or level.get("count") or level.get("count_fp")
            )
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price = parse_float(level[0])
            size = parse_float(level[1])
        else:
            continue
        if price is None or size is None or size <= 0:
            continue
        parsed[price] = size
    return parsed


def normalize_kalshi_orderbook(
    payload: dict[str, Any],
    *,
    market_ticker: str | None = None,
    market_id: str | None = None,
    _expiry: OperationExpiry | None = None,
) -> dict[str, Any]:
    """Compatibly normalize Kalshi bid-only YES/NO ladders into quote fields."""

    raw_book = payload.get("orderbook_fp")
    book = raw_book if isinstance(raw_book, dict) else payload
    yes_levels = _levels_to_map(
        book.get("yes_dollars_fp") or book.get("yes_dollars") or book.get("yes") or [],
        expiry=_expiry,
    )
    no_levels = _levels_to_map(
        book.get("no_dollars_fp") or book.get("no_dollars") or book.get("no") or [],
        expiry=_expiry,
    )
    if _expiry is not None:
        _expiry.checkpoint()
    yes_bid = max(yes_levels) if yes_levels else None
    no_bid = max(no_levels) if no_levels else None
    yes_ask = complement_probability(no_bid)
    no_ask = complement_probability(yes_bid)
    yes_bid_size = yes_levels.get(yes_bid) if yes_bid is not None else None
    no_bid_size = no_levels.get(no_bid) if no_bid is not None else None
    mid = (
        (yes_bid + yes_ask) / 2.0
        if yes_bid is not None and yes_ask is not None
        else None
    )
    spread = yes_ask - yes_bid if yes_bid is not None and yes_ask is not None else None
    return {
        "exchange": "kalshi",
        "market_ticker": market_ticker,
        "market_id": market_id,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "yes_bid_source": "direct" if yes_bid is not None else "missing",
        "yes_ask_source": "complement_derived" if no_bid is not None else "missing",
        "no_bid_source": "direct" if no_bid is not None else "missing",
        "no_ask_source": "complement_derived" if yes_bid is not None else "missing",
        "yes_bid_size": yes_bid_size,
        "yes_ask_size": no_bid_size,
        "no_bid_size": no_bid_size,
        "no_ask_size": yes_bid_size,
        "mid": mid,
        "spread": spread,
        "depth": len(yes_levels) + len(no_levels),
        "yes_bid_depth": len(yes_levels),
        "no_bid_depth": len(no_levels),
        "yes_levels": [
            {"price": price, "size": size}
            for price, size in sorted(yes_levels.items(), reverse=True)
        ],
        "no_levels": [
            {"price": price, "size": size}
            for price, size in sorted(no_levels.items(), reverse=True)
        ],
    }


def kalshi_market_identity(payload: object) -> tuple[str, str | None]:
    if not isinstance(payload, dict):
        raise InvalidDataError("Kalshi market must be an object")
    ticker = _required_alias_identifier(
        payload, "ticker", "market_ticker", label="ticker"
    )
    series_ticker = _optional_identifier(
        payload.get("series_ticker", _MISSING), "series ticker"
    )
    return ticker, series_ticker


def kalshi_page_identities(payload: object) -> tuple[str, ...]:
    rows, _ = decode_kalshi_markets_envelope(payload)
    identities: list[str] = []
    for row in rows:
        ticker, series_ticker = kalshi_market_identity(row)
        identities.append(f"ticker={ticker}")
        if series_ticker is not None:
            identities.append(f"series_ticker={series_ticker}")
    return tuple(identities)


def kalshi_detail_identities(
    payload: object,
    *,
    requested_ticker: str,
    expected_series_ticker: str | None = None,
) -> tuple[str, ...]:
    row = decode_kalshi_detail_envelope(payload)
    ticker, series_ticker = kalshi_market_identity(row)
    if ticker != requested_ticker:
        raise InvalidDataError(
            f"Kalshi detail ticker mismatch: requested {requested_ticker!r}, received {ticker!r}"
        )
    if (
        expected_series_ticker is not None
        and series_ticker is not None
        and series_ticker != expected_series_ticker
    ):
        raise InvalidDataError(
            "Kalshi detail series mismatch: "
            f"expected {expected_series_ticker!r}, received {series_ticker!r}"
        )
    result = [f"ticker={ticker}"]
    if series_ticker is not None:
        result.append(f"series_ticker={series_ticker}")
    return tuple(result)


def decode_kalshi_detail_envelope(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise InvalidDataError("Kalshi detail response must be an object")
    if "market" not in payload:
        return payload
    market = payload["market"]
    if not isinstance(market, dict):
        raise InvalidDataError("Kalshi detail field 'market' must be an object")
    return market


def decode_kalshi_markets_envelope(
    payload: object,
) -> tuple[list[dict[str, Any]], str | None]:
    if not isinstance(payload, dict):
        raise InvalidDataError("Kalshi markets response must be an object")
    markets = payload.get("markets")
    if not isinstance(markets, list) or any(
        not isinstance(row, dict) for row in markets
    ):
        raise InvalidDataError(
            "Kalshi markets field 'markets' must be an array of objects"
        )
    if "cursor" not in payload:
        raise InvalidDataError("Kalshi markets response is missing cursor")
    cursor_value = payload["cursor"]
    if cursor_value is None or cursor_value == "":
        cursor = None
    elif isinstance(cursor_value, str):
        cursor = cursor_value
    else:
        raise InvalidDataError("Kalshi cursor must be a string when supplied")
    return markets, cursor


def normalize_kalshi_workflow_market(
    payload: dict[str, Any],
    *,
    observation: RequestObservation,
    series_filter_evidence: str | None = None,
) -> KalshiMarket:
    observed_at = observation.received_at_utc
    if observed_at is None:
        raise InvalidDataError("Kalshi market observation has no receive time")
    ticker, payload_series = kalshi_market_identity(payload)
    series_ticker: str | None
    issues: tuple[DataIssue, ...]
    if (
        payload_series is not None
        and series_filter_evidence is not None
        and payload_series != series_filter_evidence
    ):
        mapping_status: Literal["mapped", "unknown", "inconsistent"] = "inconsistent"
        series_ticker = payload_series
        type_value = _normalized_market_type(payload)[1]
        issues = (
            _issue(
                "inconsistent_mapping",
                request_id=observation.request_id,
                ticker=ticker,
                field="series_ticker",
                detail=(
                    f"response series {payload_series!r} contradicts selected series "
                    f"{series_filter_evidence!r}"
                ),
            ),
        )
    else:
        # A successful server-side series filter is request evidence, not a
        # returned parent identity. Keep enrichment only when the row supplies it.
        series_ticker = payload_series
        type_state, type_value = _normalized_market_type(payload)
        issues_list: list[DataIssue] = []
        if type_state == "binary":
            mapping_status = "mapped"
        elif type_state == "missing" or type_state == "unsupported":
            mapping_status = "unknown"
            if type_state == "unsupported":
                issues_list.append(
                    _issue(
                        "unsupported_market_type",
                        request_id=observation.request_id,
                        ticker=ticker,
                        field="market_type",
                        detail=f"unsupported Kalshi market type {type_value!r}",
                        severity="warning",
                    )
                )
        else:
            mapping_status = "inconsistent"
            issues_list.append(
                _issue(
                    "inconsistent_mapping",
                    request_id=observation.request_id,
                    ticker=ticker,
                    field="market_type",
                    detail="conflicting or malformed Kalshi market type evidence",
                )
            )
        issues = tuple(issues_list)

    ref = KalshiMarketRef(ticker, series_ticker=series_ticker)
    instruments = (
        (KalshiInstrumentRef(ref, "yes"), KalshiInstrumentRef(ref, "no"))
        if mapping_status == "mapped"
        else ()
    )
    title = _optional_text(payload.get("title", _MISSING))
    return KalshiMarket(
        ref=ref,
        title=title,
        question=title,
        observed_at_utc=observed_at,
        observation=observation,
        interpretation_id=KALSHI_MARKET_INTERPRETATION_ID,
        package_version=__version__,
        instruments=instruments,
        mapping_status=mapping_status,
        book_supported=mapping_status == "mapped",
        market_type=type_value,
        status=normalize_kalshi_market_status(payload.get("status")),
        event_ticker=_optional_identifier(
            payload.get("event_ticker", _MISSING), "event ticker"
        ),
        open_time=_optional_text(payload.get("open_time", _MISSING)),
        close_time=_optional_text(payload.get("close_time", _MISSING)),
        expected_expiration_time=_optional_text(
            payload.get("expected_expiration_time", _MISSING)
        ),
        expiration_time=_optional_text(payload.get("expiration_time", _MISSING)),
        created_time=_optional_text(payload.get("created_time", _MISSING)),
        updated_time=_optional_text(payload.get("updated_time", _MISSING)),
        settlement_ts=_optional_text(payload.get("settlement_ts", _MISSING)),
        issues=issues,
        native_payload=deepcopy(payload),
    )


def kalshi_book_identities(
    payload: object, *, instrument: KalshiInstrumentRef
) -> tuple[str, ...]:
    if not isinstance(payload, dict):
        raise InvalidDataError("Kalshi order book response must be an object")
    ticker = _optional_alias_identifier(
        payload, "ticker", "market_ticker", label="ticker"
    )
    series_ticker = _optional_identifier(
        payload.get("series_ticker", _MISSING), "series ticker"
    )
    if ticker is not None and ticker != instrument.market.ticker:
        raise InvalidDataError(
            f"Kalshi book ticker mismatch: requested {instrument.market.ticker!r}, received {ticker!r}"
        )
    if (
        series_ticker is not None
        and instrument.market.series_ticker is not None
        and series_ticker != instrument.market.series_ticker
    ):
        raise InvalidDataError(
            "Kalshi book series mismatch: "
            f"expected {instrument.market.series_ticker!r}, received {series_ticker!r}"
        )
    identities: list[str] = []
    if ticker is not None:
        identities.append(f"ticker={ticker}")
    if series_ticker is not None:
        identities.append(f"series_ticker={series_ticker}")
    return tuple(identities)


def normalize_kalshi_workflow_book(
    payload: object,
    *,
    instrument: KalshiInstrumentRef,
    depth: int | None,
    observations: Sequence[RequestObservation],
    expiry: OperationExpiry,
) -> BookSnapshot:
    kalshi_book_identities(payload, instrument=instrument)
    assert isinstance(payload, dict)
    raw_book = payload.get("orderbook_fp", _MISSING)
    if not isinstance(raw_book, dict):
        raise InvalidDataError("Kalshi book field 'orderbook_fp' must be an object")
    unqualified_aliases = tuple(
        name
        for name in ("yes", "no", "yes_dollars_fp", "no_dollars_fp")
        if name in raw_book
    )
    if unqualified_aliases:
        raise InvalidDataError(
            "unqualified Kalshi ladder fields are not supported: "
            + ", ".join(unqualified_aliases)
        )
    raw_yes = _strict_dollar_ladder(raw_book, "yes_dollars", expiry=expiry)
    raw_no = _strict_dollar_ladder(raw_book, "no_dollars", expiry=expiry)
    expiry.checkpoint()

    # Validated ladders use the native last-positive-price-wins policy.
    yes_bids = tuple(
        {level.price: level for level in raw_yes if level.quantity > 0}.values()
    )
    no_bids = tuple(
        {level.price: level for level in raw_no if level.quantity > 0}.values()
    )
    yes_asks = tuple(
        BookLevel(price=_required_complement(level.price), quantity=level.quantity)
        for level in no_bids
    )
    no_asks = tuple(
        BookLevel(price=_required_complement(level.price), quantity=level.quantity)
        for level in yes_bids
    )
    yes_bids = tuple(sorted(yes_bids, key=lambda level: level.price, reverse=True))
    no_bids = tuple(sorted(no_bids, key=lambda level: level.price, reverse=True))
    yes_asks = tuple(sorted(yes_asks, key=lambda level: level.price))
    no_asks = tuple(sorted(no_asks, key=lambda level: level.price))
    expiry.checkpoint()

    if instrument.side == "yes":
        bids, asks = yes_bids, yes_asks
        native_bid_count, native_ask_count = len(raw_yes), len(raw_no)
    else:
        bids, asks = no_bids, no_asks
        native_bid_count, native_ask_count = len(raw_no), len(raw_yes)
    pre_trim_bids, pre_trim_asks = bids, asks
    returned_bids = bids if depth is None else bids[:depth]
    returned_asks = asks if depth is None else asks[:depth]

    flags: set[str] = set()
    if not pre_trim_bids:
        flags.add("empty_bid")
    if not pre_trim_asks:
        flags.add("empty_ask")
    if pre_trim_bids and pre_trim_asks:
        spread = pre_trim_asks[0].price - pre_trim_bids[0].price
        if spread < 0:
            flags.update(("crossed_book", "negative_spread"))
        elif spread == 0:
            flags.add("crossed_book")
    book_observation = observations[-1]
    return BookSnapshot(
        provenance=ResultProvenance(
            observations=tuple(observations),
            interpretation_id=KALSHI_BOOK_INTERPRETATION_ID,
            package_version=__version__,
            raw_responses=(RawResponseEvidence(book_observation.request_id, payload),),
        ),
        instrument=instrument,
        bids=returned_bids,
        asks=returned_asks,
        quantity_unit="contracts",
        exchange_timestamp_utc=None,
        endpoint="/markets/{ticker}/orderbook",
        source_scope="kalshi_current_orderbook_fp",
        data_scope=_combined_scope(observations),
        quote_normalization_policy=KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT,
        valid_state=not flags,
        quality_flags=tuple(sorted(flags)),
        bid_provenance="direct" if pre_trim_bids else "missing",
        ask_provenance="complement_derived" if pre_trim_asks else "missing",
        native_bid_count=native_bid_count,
        native_ask_count=native_ask_count,
        pre_trim_bid_count=len(pre_trim_bids),
        pre_trim_ask_count=len(pre_trim_asks),
        returned_bid_count=len(returned_bids),
        returned_ask_count=len(returned_asks),
    )


def _normalized_market_type(
    payload: dict[str, Any],
) -> tuple[Literal["binary", "missing", "unsupported", "malformed"], str | None]:
    present = [
        (name, payload[name]) for name in ("market_type", "type") if name in payload
    ]
    if not present or all(value is None for _, value in present):
        return "missing", None
    normalized: list[str] = []
    for _, value in present:
        if not isinstance(value, str) or not value.strip():
            return "malformed", None
        normalized.append(value.strip().casefold())
    if len(set(normalized)) != 1:
        return "malformed", None
    market_type = normalized[0]
    return (
        ("binary", market_type)
        if market_type == "binary"
        else ("unsupported", market_type)
    )


def _strict_dollar_ladder(
    book: dict[str, Any], name: str, *, expiry: OperationExpiry
) -> list[BookLevel]:
    if name not in book:
        return []
    values = book[name]
    if not isinstance(values, list):
        raise InvalidDataError(f"Kalshi book field {name!r} must be an array")
    levels: list[BookLevel] = []
    for index, value in enumerate(values):
        if index % 256 == 0:
            expiry.checkpoint()
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            raise InvalidDataError(
                f"Kalshi {name} level {index} must be [price, quantity]"
            )
        price = _finite_number(value[0], f"Kalshi {name} level {index} price")
        quantity = _finite_number(value[1], f"Kalshi {name} level {index} quantity")
        if not 0 <= price <= 1:
            raise InvalidDataError(
                f"Kalshi {name} level {index} price must be in [0, 1]"
            )
        if quantity < 0:
            raise InvalidDataError(
                f"Kalshi {name} level {index} quantity must be nonnegative"
            )
        levels.append(BookLevel(price, quantity))
    return levels


def _finite_number(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise InvalidDataError(f"{label} must be numeric")
    try:
        result = float(value)
    except (OverflowError, TypeError, ValueError) as exc:
        raise InvalidDataError(f"{label} must be numeric") from exc
    if result != result or result in (float("inf"), float("-inf")):
        raise InvalidDataError(f"{label} must be finite")
    return result


def _required_complement(price: float) -> float:
    result = complement_probability(price)
    assert result is not None
    return result


def _required_alias_identifier(
    payload: dict[str, Any], first: str, second: str, *, label: str
) -> str:
    value = _optional_alias_identifier(payload, first, second, label=label)
    if value is None:
        raise InvalidDataError(f"missing Kalshi {label}")
    return value


def _optional_alias_identifier(
    payload: dict[str, Any], first: str, second: str, *, label: str
) -> str | None:
    first_value = _optional_identifier(payload.get(first, _MISSING), label)
    second_value = _optional_identifier(payload.get(second, _MISSING), label)
    if first in payload and second in payload and first_value != second_value:
        raise InvalidDataError(f"conflicting Kalshi {label} aliases")
    return first_value if first in payload else second_value


def _optional_identifier(value: object, label: str) -> str | None:
    if value is _MISSING or value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise InvalidDataError(f"Kalshi {label} must be a nonempty string")
    return value


def _optional_text(value: object) -> str | None:
    if value is _MISSING or value is None:
        return None
    return value if isinstance(value, str) else None


def _issue(
    code: str,
    *,
    request_id: str,
    ticker: str,
    field: str,
    detail: str,
    severity: Literal["warning", "error"] = "error",
) -> DataIssue:
    return DataIssue(
        code=code,
        severity=severity,
        request_id=request_id,
        row_locator=f"ticker={ticker}",
        field_locator=field,
        examples=(detail[:512],),
    )


def _combined_scope(observations: Sequence[RequestObservation]) -> DataScope:
    scopes = {observation.data_scope for observation in observations}
    return next(iter(scopes)) if len(scopes) == 1 else "unknown"


__all__ = [
    "KALSHI_BOOK_INTERPRETATION_ID",
    "KALSHI_MARKET_INTERPRETATION_ID",
    "decode_kalshi_detail_envelope",
    "decode_kalshi_markets_envelope",
    "kalshi_book_identities",
    "kalshi_detail_identities",
    "kalshi_market_identity",
    "kalshi_page_identities",
    "normalize_kalshi_orderbook",
    "normalize_kalshi_workflow_book",
    "normalize_kalshi_workflow_market",
]
