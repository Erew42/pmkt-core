from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_DATA_DIR = Path(__file__).resolve().parent / "taxonomy_data"


def taxonomy_data_dir() -> Path:
    return _DATA_DIR


def _load_mapping(filename: str) -> dict[str, str]:
    path = _DATA_DIR / filename
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in payload.items()
    ):
        raise ValueError(f"{filename} must contain a JSON object of string values")
    return dict(payload)


def _load_string_set(filename: str) -> set[str]:
    path = _DATA_DIR / filename
    payload: Any = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not all(isinstance(value, str) for value in payload):
        raise ValueError(f"{filename} must contain a JSON array of strings")
    return set(payload)


def _load_string_list(filename: str) -> tuple[str, ...]:
    path = _DATA_DIR / filename
    payload: Any = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not all(isinstance(value, str) for value in payload):
        raise ValueError(f"{filename} must contain a JSON array of strings")
    return tuple(payload)


def _load_pattern_pairs(filename: str) -> tuple[tuple[str, str], ...]:
    path = _DATA_DIR / filename
    payload: Any = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"{filename} must contain a JSON array")
    pairs: list[tuple[str, str]] = []
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError(f"{filename} entries must be JSON objects")
        category = item.get("category")
        pattern = item.get("pattern")
        if not isinstance(category, str) or not isinstance(pattern, str):
            raise ValueError(f"{filename} entries require string category and pattern")
        pairs.append((category, pattern))
    return tuple(pairs)


BROAD_CATEGORY_ALIASES = _load_mapping("broad_category_aliases.json")
TOKEN_ALIASES = _load_mapping("token_aliases.json")
GENERIC_MARKET_TOKENS = _load_string_set("generic_market_tokens.json")
KALSHI_SPORT_PREFIXES = _load_string_list("kalshi_sport_prefixes.json")
SCAN_CATEGORY_ALIASES = _load_mapping("scan_category_aliases.json")
SCAN_CATEGORY_PATTERNS = _load_pattern_pairs("scan_category_patterns.json")


__all__ = [
    "BROAD_CATEGORY_ALIASES",
    "GENERIC_MARKET_TOKENS",
    "KALSHI_SPORT_PREFIXES",
    "SCAN_CATEGORY_ALIASES",
    "SCAN_CATEGORY_PATTERNS",
    "TOKEN_ALIASES",
    "taxonomy_data_dir",
]
