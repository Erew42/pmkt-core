from __future__ import annotations

import re
from typing import Any

from pmkt.text.taxonomy import TOKEN_ALIASES

_TEXT_RE = re.compile(r"[^a-z0-9 ]+")
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "of",
    "on",
    "or",
    "the",
    "to",
    "will",
    "with",
}


def normalize_text(value: Any) -> str:
    text = "" if value is None else str(value).lower()
    text = text.replace("&", " and ")
    text = _TEXT_RE.sub(" ", text)
    return " ".join(TOKEN_ALIASES.get(token, token) for token in text.split())


def token_set(value: Any, *, stopwords: set[str] | None = None) -> set[str]:
    blocked = _STOPWORDS if stopwords is None else stopwords
    return {
        token
        for token in normalize_text(value).split()
        if len(token) > 1 and token not in blocked
    }


__all__ = ["normalize_text", "token_set"]
