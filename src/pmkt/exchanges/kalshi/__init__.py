from __future__ import annotations

from importlib import import_module
from typing import Any

_LAZY_MODULES: dict[str, str] = {
    "client": "pmkt.exchanges.kalshi.client",
    "order_book_stream": "pmkt.exchanges.kalshi.order_book_stream",
    "ws": "pmkt.exchanges.kalshi.ws",
}

_LAZY_ATTRS: dict[str, tuple[str, str]] = {
    "AsyncKalshiClient": ("pmkt.exchanges.kalshi.client", "AsyncKalshiClient"),
    "AsyncKalshiWebSocketClient": (
        "pmkt.exchanges.kalshi.ws",
        "AsyncKalshiWebSocketClient",
    ),
    "DEFAULT_KALSHI_ORDER_BOOK_STREAM_ROOT": (
        "pmkt.exchanges.kalshi.order_book_stream",
        "DEFAULT_KALSHI_ORDER_BOOK_STREAM_ROOT",
    ),
    "KalshiBookSnapshot": ("pmkt.exchanges.kalshi.ws", "KalshiBookSnapshot"),
    "KalshiClient": ("pmkt.exchanges.kalshi.client", "KalshiClient"),
    "KalshiConnectFactory": ("pmkt.exchanges.kalshi.ws", "KalshiConnectFactory"),
    "KalshiOrderBookState": ("pmkt.exchanges.kalshi.ws", "KalshiOrderBookState"),
    "KalshiSubscriptionAck": ("pmkt.exchanges.kalshi.ws", "KalshiSubscriptionAck"),
    "KalshiTickerUpdate": ("pmkt.exchanges.kalshi.ws", "KalshiTickerUpdate"),
    "KalshiWebSocketError": ("pmkt.exchanges.kalshi.ws", "KalshiWebSocketError"),
    "RawFrameCallback": ("pmkt.exchanges.kalshi.ws", "RawFrameCallback"),
    "apply_kalshi_orderbook_message": (
        "pmkt.exchanges.kalshi.ws",
        "apply_kalshi_orderbook_message",
    ),
    "decode_kalshi_messages": ("pmkt.exchanges.kalshi.ws", "decode_kalshi_messages"),
    "kalshi_events_dataframe": ("pmkt.exchanges.kalshi.client", "kalshi_events_dataframe"),
    "kalshi_markets_dataframe": (
        "pmkt.exchanges.kalshi.client",
        "kalshi_markets_dataframe",
    ),
    "kalshi_orderbook_subscription_payload": (
        "pmkt.exchanges.kalshi.ws",
        "kalshi_orderbook_subscription_payload",
    ),
    "kalshi_public_subscription_payload": (
        "pmkt.exchanges.kalshi.ws",
        "kalshi_public_subscription_payload",
    ),
    "normalize_kalshi_event": ("pmkt.exchanges.kalshi.client", "normalize_kalshi_event"),
    "normalize_kalshi_market": (
        "pmkt.exchanges.kalshi.client",
        "normalize_kalshi_market",
    ),
    "normalize_kalshi_orderbook": (
        "pmkt.exchanges.kalshi.client",
        "normalize_kalshi_orderbook",
    ),
    "normalize_market_tickers": ("pmkt.exchanges.kalshi.ws", "normalize_market_tickers"),
    "parse_kalshi_subscription_ack": (
        "pmkt.exchanges.kalshi.ws",
        "parse_kalshi_subscription_ack",
    ),
    "parse_kalshi_ticker_update": (
        "pmkt.exchanges.kalshi.ws",
        "parse_kalshi_ticker_update",
    ),
    "parse_kalshi_websocket_error": (
        "pmkt.exchanges.kalshi.ws",
        "parse_kalshi_websocket_error",
    ),
    "stream_kalshi_order_book_data": (
        "pmkt.exchanges.kalshi.order_book_stream",
        "stream_kalshi_order_book_data",
    ),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY_MODULES:
        module = import_module(_LAZY_MODULES[name])
        globals()[name] = module
        return module
    if name in _LAZY_ATTRS:
        module_name, attr_name = _LAZY_ATTRS[name]
        value = getattr(import_module(module_name), attr_name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_LAZY_MODULES) | set(_LAZY_ATTRS))


__all__ = list(_LAZY_ATTRS)
