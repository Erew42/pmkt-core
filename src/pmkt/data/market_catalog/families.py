"""Catalog native and operational family classification."""

from __future__ import annotations

import re
from typing import Any


from .fs import _quote_sql
from .types import (
    KALSHI_CATALOG_NATIVE_FAMILIES,
    KALSHI_LEGACY_MVE_PREFIX,
)


POLYMARKET_RECURRING_FORMS = ("updown", "up-or-down")


POLYMARKET_RECURRING_DURATIONS = ("5m", "15m", "4h")


POLYMARKET_RECURRING_EPOCH_DIGITS = (9, 12)


_PM_FORM_PATTERN = "|".join(re.escape(value) for value in POLYMARKET_RECURRING_FORMS)


_PM_DURATION_PATTERN = "|".join(
    re.escape(value) for value in POLYMARKET_RECURRING_DURATIONS
)


_PM_GENERATED = re.compile(
    rf"(?:^|-)(?:{_PM_FORM_PATTERN})-({_PM_DURATION_PATTERN})-"
    rf"(\d{{{POLYMARKET_RECURRING_EPOCH_DIGITS[0]},"
    rf"{POLYMARKET_RECURRING_EPOCH_DIGITS[1]}}})$",
    re.IGNORECASE,
)


def native_family_for_legacy_kalshi(
    market_key: Any, *, native_family: Any = None
) -> tuple[str, str]:
    explicit = str(native_family or "").strip().casefold()
    if explicit in KALSHI_CATALOG_NATIVE_FAMILIES:
        return explicit, "partition_provenance"
    key = str(market_key or "").strip().upper()
    if not key:
        return "family_unknown", "ambiguous_legacy_evidence"
    if key.startswith(KALSHI_LEGACY_MVE_PREFIX):
        return "kalshi_mve", "legacy_ticker_compat"
    return "kalshi_conventional", "legacy_ticker_compat"


def polymarket_operational_family(slug: Any) -> str:
    match = _PM_GENERATED.search(str(slug or "").strip())
    if match is None:
        return "polymarket_conventional"
    return f"polymarket_updown_{match.group(1).casefold()}"


def _kalshi_family_sql(market_key_sql: str, *, filename_sql: str | None = None) -> str:
    explicit_cases = ""
    if filename_sql is not None:
        normalized_filename = (
            f"replace(lower(CAST({filename_sql} AS VARCHAR)), chr(92), '/')"
        )
        explicit_cases = "".join(
            f"WHEN {normalized_filename} LIKE "
            f"{_quote_sql(f'%/native_family={family}/%')} THEN {_quote_sql(family)} "
            for family in KALSHI_CATALOG_NATIVE_FAMILIES
        )
    normalized_key = f"upper(trim(CAST({market_key_sql} AS VARCHAR)))"
    return (
        "CASE "
        f"{explicit_cases}"
        f"WHEN {market_key_sql} IS NULL OR trim(CAST({market_key_sql} AS VARCHAR)) = '' "
        "THEN 'family_unknown' "
        f"WHEN starts_with({normalized_key}, {_quote_sql(KALSHI_LEGACY_MVE_PREFIX)}) "
        "THEN 'kalshi_mve' "
        "ELSE 'kalshi_conventional' END"
    )


def _kalshi_family_provenance_sql(
    market_key_sql: str, *, filename_sql: str | None = None
) -> str:
    explicit_case = ""
    if filename_sql is not None:
        normalized_filename = (
            f"replace(lower(CAST({filename_sql} AS VARCHAR)), chr(92), '/')"
        )
        predicates = " OR ".join(
            f"{normalized_filename} LIKE {_quote_sql(f'%/native_family={family}/%')}"
            for family in KALSHI_CATALOG_NATIVE_FAMILIES
        )
        explicit_case = f"WHEN {predicates} THEN 'partition_provenance' "
    return (
        "CASE "
        f"{explicit_case}"
        f"WHEN {market_key_sql} IS NULL OR trim(CAST({market_key_sql} AS VARCHAR)) = '' "
        "THEN 'ambiguous_legacy_evidence' "
        "ELSE 'legacy_ticker_compat' END"
    )


def _polymarket_operational_family_sql(slug_sql: str) -> str:
    minimum_digits, maximum_digits = POLYMARKET_RECURRING_EPOCH_DIGITS
    form_pattern = "|".join(POLYMARKET_RECURRING_FORMS)
    cases = []
    for duration in POLYMARKET_RECURRING_DURATIONS:
        pattern = (
            rf"(?i)(^|-)({form_pattern})-{duration}-"
            rf"[0-9]{{{minimum_digits},{maximum_digits}}}$"
        )
        cases.append(
            f"WHEN regexp_matches(coalesce(CAST({slug_sql} AS VARCHAR), ''), "
            f"{_quote_sql(pattern)}) THEN {_quote_sql(f'polymarket_updown_{duration}')}"
        )
    return "CASE " + " ".join(cases) + " ELSE 'polymarket_conventional' END"
