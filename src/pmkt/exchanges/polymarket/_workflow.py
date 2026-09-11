"""Strict private decoding for Polymarket public REST workflows."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
import math
from typing import Any, Literal, Sequence

from pmkt import __version__
from pmkt._operation import OperationExpiry
from pmkt.errors import InvalidDataError
from pmkt.records import (
    BookLevel,
    BookSnapshot,
    DataIssue,
    PolymarketInstrumentRef,
    PolymarketMarket,
    PolymarketMarketRef,
    RequestObservation,
)


POLYMARKET_MARKET_INTERPRETATION_ID = "polymarket_gamma_market.v1"
POLYMARKET_CLOB_BOOK_INTERPRETATION_ID = "polymarket_clob_book.v1"


def gamma_market_identity(payload: object) -> tuple[str, str | None]:
    """Validate and return Gamma's market ID and optional condition ID."""

    if not isinstance(payload, dict):
        raise InvalidDataError("Gamma market must be an object")
    market_id = _required_alias_identifier(payload, "id", "market_id", label="market ID")
    condition_id = _optional_alias_identifier(
        payload, "conditionId", "condition_id", label="condition ID"
    )
    return market_id, condition_id


def gamma_page_identities(payload: object) -> tuple[str, ...]:
    markets, _ = decode_gamma_keyset_envelope(payload)
    identities: list[str] = []
    for row in markets:
        market_id, condition_id = gamma_market_identity(row)
        identities.append(f"market_id={market_id}")
        if condition_id is not None:
            identities.append(f"condition_id={condition_id}")
    return tuple(identities)


def gamma_detail_identities(
    payload: object, *, requested_market_id: str
) -> tuple[str, ...]:
    market_id, condition_id = gamma_market_identity(payload)
    if market_id != requested_market_id:
        raise InvalidDataError(
            f"Gamma detail identity mismatch: requested {requested_market_id!r}, "
            f"received {market_id!r}"
        )
    identities = [f"market_id={market_id}"]
    if condition_id is not None:
        identities.append(f"condition_id={condition_id}")
    return tuple(identities)


def decode_gamma_keyset_envelope(
    payload: object,
) -> tuple[list[dict[str, Any]], str | None]:
    if not isinstance(payload, dict):
        raise InvalidDataError("Gamma keyset response must be an object")
    markets = payload.get("markets")
    if not isinstance(markets, list) or any(not isinstance(row, dict) for row in markets):
        raise InvalidDataError("Gamma keyset field 'markets' must be an array of objects")
    cursor_value = payload.get("next_cursor")
    if cursor_value is None:
        cursor = None
    elif isinstance(cursor_value, str):
        cursor = cursor_value or None
    else:
        raise InvalidDataError("Gamma next_cursor must be a string when supplied")
    return markets, cursor


def normalize_gamma_market(
    payload: dict[str, Any],
    *,
    observation: RequestObservation,
) -> PolymarketMarket:
    observed_at_utc = observation.received_at_utc
    if observed_at_utc is None:
        raise InvalidDataError("Gamma market observation has no receive time")
    request_id = observation.request_id
    market_id, condition_id = gamma_market_identity(payload)
    question_value = payload.get("question")
    question = question_value if isinstance(question_value, str) else None

    issues: list[DataIssue] = []
    labels_state, labels = _strict_string_array(payload.get("outcomes", _MISSING))
    tokens_state, tokens, tokens_alias_conflict = _strict_string_array_alias(
        payload, "clobTokenIds", "clob_token_ids"
    )

    mapping_status: Literal["mapped", "empty", "unknown", "inconsistent"]
    instruments: tuple[PolymarketInstrumentRef, ...] = ()
    if (
        labels_state == "malformed"
        or tokens_state == "malformed"
        or (tokens_state == "valid" and len(set(tokens)) != len(tokens))
    ):
        mapping_status = "inconsistent"
    elif labels_state == "missing" or tokens_state == "missing":
        mapping_status = "unknown"
    elif not labels and not tokens:
        mapping_status = "empty"
    elif not labels or not tokens or len(labels) != len(tokens) or len(set(tokens)) != len(tokens):
        mapping_status = "inconsistent"
    else:
        mapping_status = "mapped"
        market_ref = PolymarketMarketRef(market_id, condition_id=condition_id)
        instruments = tuple(
            PolymarketInstrumentRef(token, market=market_ref, outcome_index=index)
            for index, token in enumerate(tokens)
        )

    if mapping_status == "inconsistent":
        reasons: list[str] = []
        if tokens_alias_conflict:
            reasons.append("conflicting clobTokenIds aliases")
        if labels_state == "malformed":
            reasons.append("malformed outcomes")
        if tokens_state == "malformed":
            reasons.append("malformed token IDs")
        if labels_state == tokens_state == "valid" and len(labels) != len(tokens):
            reasons.append("outcome/token length mismatch")
        if tokens_state == "valid" and len(set(tokens)) != len(tokens):
            reasons.append("duplicate token ID")
        issues.append(
            _issue(
                "inconsistent_mapping",
                request_id=request_id,
                market_id=market_id,
                field="outcomes/clobTokenIds",
                detail="; ".join(reasons) or "inconsistent outcome/token mapping",
            )
        )

    outcome_prices, price_issue = _normalize_outcome_prices(
        payload, expected_count=len(labels) if labels_state == "valid" else None
    )
    if price_issue is not None:
        issues.append(
            _issue(
                "invalid_outcome_prices",
                request_id=request_id,
                market_id=market_id,
                field="outcomePrices",
                detail=price_issue,
            )
        )

    ref = PolymarketMarketRef(market_id, condition_id=condition_id)
    closed = payload.get("closed")
    closed_value = closed if isinstance(closed, bool) else None
    enable_order_book, capability_issue = _book_capability(payload)
    if capability_issue is not None:
        issues.append(
            _issue(
                "invalid_book_capability",
                request_id=request_id,
                market_id=market_id,
                field="enableOrderBook",
                detail=capability_issue,
            )
        )
    book_supported = mapping_status == "mapped" and enable_order_book is not False
    return PolymarketMarket(
        ref=ref,
        question=question,
        observed_at_utc=observed_at_utc,
        observation=observation,
        interpretation_id=POLYMARKET_MARKET_INTERPRETATION_ID,
        package_version=__version__,
        instruments=instruments,
        outcome_labels=labels if labels_state == "valid" else (),
        outcome_labels_valid=labels_state == "valid",
        outcome_prices=outcome_prices,
        mapping_status=mapping_status,
        book_supported=book_supported,
        closed=closed_value,
        start_date=_optional_text_alias(payload, "startDate", "start_date"),
        end_date=_optional_text_alias(payload, "endDate", "end_date"),
        created_at=_optional_text_alias(payload, "createdAt", "created_at"),
        updated_at=_optional_text_alias(payload, "updatedAt", "updated_at"),
        issues=tuple(issues),
        native_payload=deepcopy(payload),
    )


def clob_book_identities(
    payload: object, *, instrument: PolymarketInstrumentRef
) -> tuple[str, ...]:
    if not isinstance(payload, dict):
        raise InvalidDataError("CLOB book response must be an object")
    token_id = _optional_alias_identifier(
        payload, "asset_id", "assetId", label="CLOB token ID"
    )
    condition_id = _optional_identifier(payload.get("market", _MISSING), "condition ID")
    if token_id is not None and token_id != instrument.token_id:
        raise InvalidDataError(
            f"CLOB book token mismatch: requested {instrument.token_id!r}, "
            f"received {token_id!r}"
        )
    expected_condition = (
        instrument.market.condition_id if instrument.market is not None else None
    )
    if (
        condition_id is not None
        and expected_condition is not None
        and condition_id != expected_condition
    ):
        raise InvalidDataError(
            f"CLOB book condition mismatch: expected {expected_condition!r}, "
            f"received {condition_id!r}"
        )
    identities: list[str] = []
    if token_id is not None:
        identities.append(f"token_id={token_id}")
    if condition_id is not None:
        identities.append(f"condition_id={condition_id}")
    return tuple(identities)


def normalize_clob_book(
    payload: object,
    *,
    instrument: PolymarketInstrumentRef,
    depth: int | None,
    observation: RequestObservation,
    expiry: OperationExpiry,
) -> BookSnapshot:
    clob_book_identities(payload, instrument=instrument)
    assert isinstance(payload, dict)
    raw_bids = _required_ladder(payload, "bids")
    raw_asks = _required_ladder(payload, "asks")
    bids = _strict_book_levels(raw_bids, side="bid", expiry=expiry)
    asks = _strict_book_levels(raw_asks, side="ask", expiry=expiry)
    expiry.checkpoint()
    pre_trim_bids = tuple(sorted((level for level in bids if level.quantity > 0), key=lambda x: x.price, reverse=True))
    pre_trim_asks = tuple(sorted((level for level in asks if level.quantity > 0), key=lambda x: x.price))
    expiry.checkpoint()
    returned_bids = pre_trim_bids if depth is None else pre_trim_bids[:depth]
    returned_asks = pre_trim_asks if depth is None else pre_trim_asks[:depth]

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

    exchange_timestamp = _clob_timestamp(payload.get("timestamp", _MISSING))
    return BookSnapshot(
        instrument=instrument,
        bids=returned_bids,
        asks=returned_asks,
        quantity_unit="shares",
        exchange_timestamp_utc=exchange_timestamp,
        endpoint="/book",
        source_scope="clob_current_book",
        data_scope=observation.data_scope,
        observation=observation,
        interpretation_id=POLYMARKET_CLOB_BOOK_INTERPRETATION_ID,
        package_version=__version__,
        valid_state=not flags,
        quality_flags=tuple(sorted(flags)),
        bid_provenance="direct" if pre_trim_bids else "missing",
        ask_provenance="direct" if pre_trim_asks else "missing",
        native_bid_count=len(raw_bids),
        native_ask_count=len(raw_asks),
        pre_trim_bid_count=len(pre_trim_bids),
        pre_trim_ask_count=len(pre_trim_asks),
        returned_bid_count=len(returned_bids),
        returned_ask_count=len(returned_asks),
        native_payload=deepcopy(payload),
    )


_MISSING = object()


def _strict_string_array(
    value: object,
) -> tuple[Literal["missing", "valid", "malformed"], tuple[str, ...]]:
    if value is _MISSING or value is None:
        return "missing", ()
    decoded = value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return "malformed", ()
    if not isinstance(decoded, list):
        return "malformed", ()
    if any(not isinstance(item, str) or not item.strip() for item in decoded):
        return "malformed", ()
    return "valid", tuple(decoded)


def _strict_string_array_alias(
    payload: dict[str, Any], first: str, second: str
) -> tuple[
    Literal["missing", "valid", "malformed"], tuple[str, ...], bool
]:
    first_state, first_values = _strict_string_array(payload.get(first, _MISSING))
    second_state, second_values = _strict_string_array(payload.get(second, _MISSING))
    if first not in payload:
        return second_state, second_values, False
    if second not in payload:
        return first_state, first_values, False
    if first_state == second_state and first_values == second_values:
        return first_state, first_values, False
    return "malformed", (), True


def _normalize_outcome_prices(
    payload: dict[str, Any], *, expected_count: int | None
) -> tuple[tuple[float, ...] | None, str | None]:
    first_present = "outcomePrices" in payload
    second_present = "outcome_prices" in payload
    first = _decode_outcome_price_value(
        payload.get("outcomePrices", _MISSING), expected_count=expected_count
    )
    second = _decode_outcome_price_value(
        payload.get("outcome_prices", _MISSING), expected_count=expected_count
    )
    if first_present and second_present:
        if first != second:
            return None, "conflicting outcomePrices aliases"
        return first
    return first if first_present else second


def _decode_outcome_price_value(
    value: object, *, expected_count: int | None
) -> tuple[tuple[float, ...] | None, str | None]:
    if value is _MISSING or value is None:
        return None, None
    decoded = value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return None, "outcomePrices is not a JSON array"
    if not isinstance(decoded, list):
        return None, "outcomePrices is not an array"
    prices: list[float] = []
    for item in decoded:
        if isinstance(item, bool) or not isinstance(item, (str, int, float)):
            return None, "outcomePrices contains a nonnumeric value"
        try:
            price = float(item)
        except (OverflowError, TypeError, ValueError):
            return None, "outcomePrices contains a nonnumeric value"
        if not math.isfinite(price) or not 0 <= price <= 1:
            return None, "outcomePrices contains a value outside [0, 1]"
        prices.append(price)
    if expected_count is None or len(prices) != expected_count:
        return None, "outcomePrices does not align with valid outcomes"
    return tuple(prices), None


def _required_ladder(payload: dict[str, Any], name: str) -> list[object]:
    value = payload.get(name, _MISSING)
    if not isinstance(value, list):
        raise InvalidDataError(f"CLOB book field {name!r} must be an array")
    return value


def _strict_book_levels(
    values: Sequence[object],
    *,
    side: Literal["bid", "ask"],
    expiry: OperationExpiry,
) -> tuple[BookLevel, ...]:
    result: list[BookLevel] = []
    for index, value in enumerate(values):
        if index % 256 == 0:
            expiry.checkpoint()
        if not isinstance(value, dict) or "price" not in value or "size" not in value:
            raise InvalidDataError(f"CLOB {side} level {index} must contain price and size")
        price = _finite_number(value["price"], f"CLOB {side} level {index} price")
        quantity = _finite_number(value["size"], f"CLOB {side} level {index} size")
        if not 0 <= price <= 1:
            raise InvalidDataError(f"CLOB {side} level {index} price must be in [0, 1]")
        if quantity < 0:
            raise InvalidDataError(f"CLOB {side} level {index} size must be nonnegative")
        result.append(BookLevel(price=price, quantity=quantity))
    return tuple(result)


def _finite_number(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise InvalidDataError(f"{label} must be numeric")
    try:
        result = float(value)
    except (OverflowError, TypeError, ValueError) as exc:
        raise InvalidDataError(f"{label} must be numeric") from exc
    if not math.isfinite(result):
        raise InvalidDataError(f"{label} must be finite")
    return result


def _clob_timestamp(value: object) -> datetime | None:
    if value is _MISSING or value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise InvalidDataError("CLOB timestamp must be epoch milliseconds")
    text = str(value)
    if not text.isascii() or not text.isdecimal():
        raise InvalidDataError("CLOB timestamp must be epoch milliseconds")
    try:
        return datetime.fromtimestamp(int(text) / 1000.0, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise InvalidDataError("CLOB timestamp is outside the supported range") from exc


def _mapping_alias_value(
    payload: dict[str, Any], first: str, second: str
) -> tuple[object, bool]:
    has_first = first in payload
    has_second = second in payload
    if has_first and has_second and payload[first] != payload[second]:
        return _MISSING, True
    if has_first:
        return payload[first], False
    if has_second:
        return payload[second], False
    return _MISSING, False


def _book_capability(payload: dict[str, Any]) -> tuple[bool | None, str | None]:
    first_present = "enableOrderBook" in payload
    second_present = "enable_order_book" in payload
    first = payload.get("enableOrderBook")
    second = payload.get("enable_order_book")
    if first_present and (not isinstance(first, bool)):
        return False, "enableOrderBook must be boolean"
    if second_present and (not isinstance(second, bool)):
        return False, "enable_order_book must be boolean"
    if first_present and second_present and first is not second:
        return False, "conflicting enableOrderBook aliases"
    if first_present:
        return first, None
    if second_present:
        return second, None
    return None, None


def _alias_value(payload: dict[str, Any], first: str, second: str) -> object:
    value, conflict = _mapping_alias_value(payload, first, second)
    return _MISSING if conflict else value


def _required_alias_identifier(
    payload: dict[str, Any], first: str, second: str, *, label: str
) -> str:
    value, conflict = _mapping_alias_value(payload, first, second)
    if conflict:
        raise InvalidDataError(f"conflicting {label} aliases")
    result = _optional_identifier(value, label)
    if result is None:
        raise InvalidDataError(f"missing {label}")
    return result


def _optional_alias_identifier(
    payload: dict[str, Any], first: str, second: str, *, label: str
) -> str | None:
    value, conflict = _mapping_alias_value(payload, first, second)
    if conflict:
        raise InvalidDataError(f"conflicting {label} aliases")
    return _optional_identifier(value, label)


def _optional_identifier(value: object, label: str) -> str | None:
    if value is _MISSING or value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise InvalidDataError(f"{label} must be a nonempty string")
    return value


def _optional_text_alias(
    payload: dict[str, Any], first: str, second: str
) -> str | None:
    value = _alias_value(payload, first, second)
    return value if isinstance(value, str) else None


def _issue(
    code: str,
    *,
    request_id: str,
    market_id: str,
    field: str,
    detail: str,
) -> DataIssue:
    return DataIssue(
        code=code,
        severity="warning",
        request_id=request_id,
        row_locator=f"market_id={market_id}",
        field_locator=field,
        examples=(detail[:512],),
    )


__all__ = [
    "POLYMARKET_CLOB_BOOK_INTERPRETATION_ID",
    "POLYMARKET_MARKET_INTERPRETATION_ID",
    "clob_book_identities",
    "decode_gamma_keyset_envelope",
    "gamma_detail_identities",
    "gamma_market_identity",
    "gamma_page_identities",
    "normalize_clob_book",
    "normalize_gamma_market",
]
