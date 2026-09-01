from __future__ import annotations

import ast
import json
from collections.abc import Iterable, Mapping
from typing import Any

TOKEN_LIST_KEYS = (
    "clobTokenIds",
    "clob_token_ids",
    "outcomeTokenIds",
    "outcome_token_ids",
    "outcomeTokens",
    "outcome_tokens",
    "token_ids",
    "tokens",
)
TOKEN_VALUE_KEYS = ("token_id", "tokenId", "tokenID", "id", "token")
POLYMARKET_TOKEN_COLUMNS = ("token_ids", "clob_token_ids", "token_id")


def normalize_token_value(value: Any) -> str | None:
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.startswith("[") or raw.startswith("{"):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return raw
            return normalize_token_value(parsed)
        return raw
    if isinstance(value, int):
        return str(value)
    return None


def extract_token_ids(payload: dict[str, Any]) -> list[str]:
    tokens: list[str] = []
    seen: set[str] = set()

    def add(token: str | None) -> None:
        if not token or token in seen:
            return
        seen.add(token)
        tokens.append(token)

    def scan(container: Any) -> None:
        if isinstance(container, dict):
            for key in TOKEN_LIST_KEYS:
                if key in container:
                    scan(container[key])
            for key in TOKEN_VALUE_KEYS:
                if key in container:
                    add(normalize_token_value(container[key]))
            outcomes = container.get("outcomes")
            if isinstance(outcomes, list):
                for outcome in outcomes:
                    if isinstance(outcome, dict):
                        for key in TOKEN_VALUE_KEYS:
                            if key in outcome:
                                add(normalize_token_value(outcome[key]))
            markets = container.get("markets")
            if isinstance(markets, list):
                for market in markets:
                    scan(market)
        elif isinstance(container, list):
            for item in container:
                scan(item)
        elif isinstance(container, str):
            raw = container.strip()
            if raw.startswith("[") or raw.startswith("{"):
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    add(raw or None)
                else:
                    scan(parsed)
            else:
                add(raw or None)
        else:
            add(normalize_token_value(container))

    scan(payload)
    return tokens


def flatten_token_ids(value: Any, *, dedupe: bool = True) -> list[str]:
    """Flatten parquet, JSON, and list-like token cells into token strings."""
    tokens: list[str] = []
    seen: set[str] = set()

    def add(token: Any) -> None:
        text = str(token).strip()
        if not text or (dedupe and text in seen):
            return
        seen.add(text)
        tokens.append(text)

    def scan(item: Any) -> None:
        if item is None:
            return
        if isinstance(item, str):
            text = item.strip()
            if not text:
                return
            if text[0] in "[{(":
                parsed = parse_serialized_container(text)
                if parsed is not None:
                    scan(parsed)
                    return
            add(text)
            return
        if isinstance(item, bytes):
            add(item.decode("utf-8", errors="replace"))
            return
        if not isinstance(item, (str, bytes)) and hasattr(item, "tolist"):
            scan(item.tolist())
            return
        if isinstance(item, Mapping):
            for key in POLYMARKET_TOKEN_COLUMNS:
                if key in item:
                    scan(item[key])
            return
        if isinstance(item, Iterable):
            for nested in item:
                scan(nested)
            return
        try:
            if bool(item != item):
                return
        except (TypeError, ValueError):
            pass
        add(item)

    scan(value)
    return tokens


def parse_serialized_container(text: str) -> Any | None:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    try:
        return ast.literal_eval(text)
    except (SyntaxError, ValueError):
        return None


__all__ = [
    "POLYMARKET_TOKEN_COLUMNS",
    "TOKEN_LIST_KEYS",
    "TOKEN_VALUE_KEYS",
    "extract_token_ids",
    "flatten_token_ids",
    "normalize_token_value",
    "parse_serialized_container",
]
