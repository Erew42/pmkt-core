from __future__ import annotations

from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic.alias_generators import to_camel


def _coerce_numeric_string(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return str(value)
    return value


class CamelCaseModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="allow",
    )


class Event(CamelCaseModel):
    id: str
    ticker: Optional[str] = None
    slug: Optional[str] = None
    title: str
    description: Optional[str] = None
    start_date: Optional[str] = None
    start_time: Optional[str] = None
    end_date: Optional[str] = None
    event_date: Optional[str] = None
    active: Optional[bool] = None
    closed: Optional[bool] = None
    archived: Optional[bool] = None
    new: Optional[bool] = None
    featured: Optional[bool] = None
    restricted: Optional[bool] = None
    liquidity: Optional[float] = None
    volume: Optional[float] = None
    open_interest: Optional[float] = None
    sort_by: Optional[str] = None
    category: Optional[str] = None
    creation_date: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    competitive: Optional[float] = None
    volume_24hr: Optional[float] = Field(None, alias="volume24hr")
    markets: List[Market] = Field(default_factory=list)

    @field_validator("id", mode="before")
    @classmethod
    def _normalize_id(cls, value: Any) -> Any:
        return _coerce_numeric_string(value)


class Market(CamelCaseModel):
    id: str
    question: str
    condition_id: Optional[str] = None
    slug: Optional[str] = None
    twitter_card_image: Optional[str] = None
    start_date: Optional[str] = None
    start_date_iso: Optional[str] = Field(None, alias="startDateIso")
    end_date: Optional[str] = None
    end_date_iso: Optional[str] = Field(None, alias="endDateIso")
    category: Optional[str] = None
    liquidity: Optional[str] = None
    image: Optional[str] = None
    icon: Optional[str] = None
    description: Optional[str] = None
    outcomes: Optional[str] = None
    outcome_prices: Optional[str] = None
    volume: Optional[str] = None
    volume_num: Optional[float] = None
    volume_24hr: Optional[float] = Field(None, alias="volume24hr")
    volume_1wk: Optional[float] = Field(None, alias="volume1wk")
    volume_1mo: Optional[float] = Field(None, alias="volume1mo")
    volume_1yr: Optional[float] = Field(None, alias="volume1yr")
    active: Optional[bool] = None
    market_type: Optional[str] = None
    closed: Optional[bool] = None
    market_maker_address: Optional[str] = None
    updated_by: Optional[int] = None
    created_at: Optional[str] = None
    accepting_orders_timestamp: Optional[str] = Field(
        None,
        alias="acceptingOrdersTimestamp",
    )
    event_start_time: Optional[str] = Field(None, alias="eventStartTime")
    game_start_time: Optional[str] = Field(None, alias="gameStartTime")
    updated_at: Optional[str] = None
    closed_time: Optional[str] = None
    archived: Optional[bool] = None
    restricted: Optional[bool] = None
    clob_token_ids: Optional[str] = None
    liquidity_num: Optional[float] = None
    market_cap: Optional[float] = Field(None, alias="marketCap")
    open_interest: Optional[float] = Field(None, alias="openInterest")
    best_bid: Optional[float] = Field(None, alias="bestBid")
    best_ask: Optional[float] = Field(None, alias="bestAsk")
    last_trade_price: Optional[float] = Field(None, alias="lastTradePrice")
    events: List[Event] = Field(default_factory=list)

    @field_validator("id", "condition_id", mode="before")
    @classmethod
    def _normalize_ids(cls, value: Any) -> Any:
        return _coerce_numeric_string(value)


class Order(CamelCaseModel):
    price: str
    size: str


class OrderBook(CamelCaseModel):
    hash: str
    market: Optional[str] = None
    asset_id: Optional[str] = None
    timestamp: Optional[str] = None
    min_order_size: Optional[str] = None
    tick_size: Optional[str] = None
    last_trade_price: Optional[str] = None
    neg_risk: Optional[bool] = None
    bids: List[Order] = Field(default_factory=list)
    asks: List[Order] = Field(default_factory=list)


class PriceHistoryPoint(CamelCaseModel):
    t: int
    p: float


class PriceHistory(CamelCaseModel):
    history: List[PriceHistoryPoint] = Field(default_factory=list)


# Update forward refs
Event.model_rebuild()
Market.model_rebuild()
OrderBook.model_rebuild()
PriceHistory.model_rebuild()
