import re
import logging
from typing import Any

logger = logging.getLogger(__name__)


def extract_bounds_heuristic(question: str) -> dict[str, Any]:
    """
    Provides a resilient NLP-lite fallback for extracting numerical bounds from market questions.
    This bypasses rigid regex trees in grouping.py for a more flexible, forgiving semantic search.
    In the future, this can be replaced by an LLM call or a tiny local model using Pydantic extraction.
    """
    # Quick, simple fallback heuristics
    question = question.lower().replace(",", "")

    # Try finding exact numerical ranges explicitly stated with "between X and Y"
    between_match = re.search(r"between\s+\$?([\d\.]+)\s+(?:and|to)\s+\$?([\d\.]+)", question)
    if between_match:
        try:
            return {
                "kind": "range",
                "low": float(between_match.group(1)),
                "high": float(between_match.group(2))
            }
        except ValueError:
            pass

    # Try finding >= or "or more"
    gte_match = re.search(r"([\d\.]+)\s*(?:\+|or more|and up)", question)
    if gte_match:
        try:
            return {
                "kind": "gte",
                "low": float(gte_match.group(1)),
                "high": None
            }
        except ValueError:
            pass

    # Try finding < or "under" or "less than"
    lt_match = re.search(r"(?:under|less than|<)\s*([\d\.]+)", question)
    if lt_match:
        try:
            return {
                "kind": "lt",
                "low": None,
                "high": float(lt_match.group(1))
            }
        except ValueError:
            pass

    return {}
