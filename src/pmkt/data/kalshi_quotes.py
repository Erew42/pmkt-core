from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from pmkt.data.prices import complement_probability

KALSHI_QUOTE_NORMALIZATION_POLICY_LEGACY = "kalshi_quote_normalization.v1"
KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT = "kalshi_quote_normalization.v2"
_SUPPORTED_POLICIES = frozenset(
    {
        KALSHI_QUOTE_NORMALIZATION_POLICY_LEGACY,
        KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT,
    }
)


def resolve_kalshi_quote_normalization_policy(value: Any) -> str:
    """Resolve persisted policy, treating only an absent value as legacy.

    Captures created before this policy existed have no version in their adapter
    settings. An empty or unknown value is different from an old missing value:
    it is malformed evidence and must not silently select replay semantics.
    """

    if value is None:
        return KALSHI_QUOTE_NORMALIZATION_POLICY_LEGACY
    if not isinstance(value, str) or value not in _SUPPORTED_POLICIES:
        raise ValueError(f"unsupported Kalshi quote-normalization policy: {value!r}")
    return value


def resolve_kalshi_use_yes_price(adapter_settings: Mapping[str, Any]) -> bool:
    """Resolve the persisted Kalshi wire-price mode exactly.

    Older capture run states predate ``use_yes_price`` and therefore use the
    historical default. Once the field is present it is evidence, not a truthy
    configuration value: accepting strings or integers here would silently
    reconstruct the same bytes under different venue semantics.
    """

    if "use_yes_price" not in adapter_settings:
        return True
    value = adapter_settings["use_yes_price"]
    if not isinstance(value, bool):
        raise ValueError(
            "Kalshi adapter setting 'use_yes_price' must be a boolean when present; "
            f"got {value!r}"
        )
    return value


@dataclass(frozen=True)
class KalshiQuoteProjection:
    use_yes_price: bool
    quote_normalization_policy: str
    yes_bid: float | None
    yes_ask: float | None
    no_bid: float | None
    no_ask: float | None
    yes_bid_size: float | None
    yes_ask_size: float | None
    no_bid_size: float | None
    no_ask_size: float | None
    yes_bid_source: str
    yes_ask_source: str
    no_bid_source: str
    no_ask_source: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "use_yes_price": self.use_yes_price,
            "quote_normalization_policy": self.quote_normalization_policy,
            "yes_bid": self.yes_bid,
            "yes_ask": self.yes_ask,
            "no_bid": self.no_bid,
            "no_ask": self.no_ask,
            "yes_bid_size": self.yes_bid_size,
            "yes_ask_size": self.yes_ask_size,
            "no_bid_size": self.no_bid_size,
            "no_ask_size": self.no_ask_size,
            "yes_bid_source": self.yes_bid_source,
            "yes_ask_source": self.yes_ask_source,
            "no_bid_source": self.no_bid_source,
            "no_ask_source": self.no_ask_source,
        }


def project_kalshi_quotes(
    *,
    yes_bid: float | None,
    no_ladder_price: float | None,
    yes_bid_size: float | None,
    no_ladder_size: float | None,
    use_yes_price: bool,
    policy_version: str = KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT,
) -> KalshiQuoteProjection:
    """Project Kalshi's two bid ladders into explicit YES and NO quotes.

    With ``use_yes_price=True``, the NO ladder is transmitted on the YES-price
    scale: its best (lowest) value is a direct YES ask, while the displayed NO
    bid is its complement. With ``False``, the same ladder is a direct NO bid
    and the YES ask is complement-derived. The opposite outcome's ask is always
    the complement of the YES bid. Sizes follow the originating ladder.
    """

    policy = resolve_kalshi_quote_normalization_policy(policy_version)
    yes_ask = (
        no_ladder_price
        if use_yes_price
        else complement_probability(no_ladder_price)
    )
    no_bid = (
        complement_probability(yes_ask) if use_yes_price else no_ladder_price
    )
    no_ask = complement_probability(yes_bid)

    if policy == KALSHI_QUOTE_NORMALIZATION_POLICY_LEGACY:
        yes_ask_derived = False
        no_bid_derived = False
        no_ask_derived = False
    else:
        yes_ask_derived = not use_yes_price
        no_bid_derived = use_yes_price
        no_ask_derived = True

    return KalshiQuoteProjection(
        use_yes_price=bool(use_yes_price),
        quote_normalization_policy=policy,
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        yes_bid_size=yes_bid_size,
        yes_ask_size=no_ladder_size,
        no_bid_size=no_ladder_size,
        no_ask_size=yes_bid_size,
        yes_bid_source=_quote_source(yes_bid, derived=False),
        yes_ask_source=_quote_source(yes_ask, derived=yes_ask_derived),
        no_bid_source=_quote_source(no_bid, derived=no_bid_derived),
        no_ask_source=_quote_source(no_ask, derived=no_ask_derived),
    )


def _quote_source(value: float | None, *, derived: bool) -> str:
    if value is None:
        return "missing"
    return "complement_derived" if derived else "direct"


__all__ = [
    "KALSHI_QUOTE_NORMALIZATION_POLICY_CURRENT",
    "KALSHI_QUOTE_NORMALIZATION_POLICY_LEGACY",
    "KalshiQuoteProjection",
    "project_kalshi_quotes",
    "resolve_kalshi_quote_normalization_policy",
    "resolve_kalshi_use_yes_price",
]
