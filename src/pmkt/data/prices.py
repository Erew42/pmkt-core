from __future__ import annotations


def complement_probability(price: float | None) -> float | None:
    if price is None:
        return None
    return round(1.0 - price, 10)


def is_probability(value: float | None) -> bool:
    return value is not None and 0.0 <= value <= 1.0


__all__ = ["complement_probability", "is_probability"]
