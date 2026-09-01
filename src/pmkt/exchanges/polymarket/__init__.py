from __future__ import annotations

from importlib import import_module
from typing import Any

_LAZY_MODULES: dict[str, str] = {
    "clob": "pmkt.exchanges.polymarket.clob",
    "data_api": "pmkt.exchanges.polymarket.data_api",
    "gamma": "pmkt.exchanges.polymarket.gamma",
    "order_book_stream": "pmkt.exchanges.polymarket.order_book_stream",
    "subgraph": "pmkt.exchanges.polymarket.subgraph",
    "ws": "pmkt.exchanges.polymarket.ws",
}

_LAZY_ATTRS: dict[str, tuple[str, str]] = {
    "AsyncClobClient": ("pmkt.exchanges.polymarket.clob", "AsyncClobClient"),
    "AsyncPolymarketDataClient": (
        "pmkt.exchanges.polymarket.data_api",
        "AsyncPolymarketDataClient",
    ),
    "DEFAULT_DATA_API_MAX_RATE": (
        "pmkt.exchanges.polymarket.data_api",
        "DEFAULT_DATA_API_MAX_RATE",
    ),
    "DEFAULT_DATA_API_PERIOD_SECONDS": (
        "pmkt.exchanges.polymarket.data_api",
        "DEFAULT_DATA_API_PERIOD_SECONDS",
    ),
    "AsyncGammaClient": ("pmkt.exchanges.polymarket.gamma", "AsyncGammaClient"),
    "AsyncMarketWebSocketClient": (
        "pmkt.exchanges.polymarket.ws",
        "AsyncMarketWebSocketClient",
    ),
    "AsyncSubgraphClient": ("pmkt.exchanges.polymarket.subgraph", "AsyncSubgraphClient"),
    "AsyncWebSocketClient": ("pmkt.exchanges.polymarket.ws", "AsyncWebSocketClient"),
    "ClobClient": ("pmkt.exchanges.polymarket.clob", "ClobClient"),
    "DEFAULT_ORDER_BOOK_STREAM_ROOT": (
        "pmkt.exchanges.polymarket.order_book_stream",
        "DEFAULT_ORDER_BOOK_STREAM_ROOT",
    ),
    "GammaClient": ("pmkt.exchanges.polymarket.gamma", "GammaClient"),
    "MarketBookState": ("pmkt.exchanges.polymarket.ws", "MarketBookState"),
    "MarketStreamSnapshot": ("pmkt.exchanges.polymarket.ws", "MarketStreamSnapshot"),
    "SubgraphClient": ("pmkt.exchanges.polymarket.subgraph", "SubgraphClient"),
    "WebSocketProtocolError": ("pmkt.exchanges.polymarket.ws", "WebSocketProtocolError"),
    "apply_market_message": ("pmkt.exchanges.polymarket.ws", "apply_market_message"),
    "collect_market_snapshots": ("pmkt.exchanges.polymarket.ws", "collect_market_snapshots"),
    "decode_market_messages": ("pmkt.exchanges.polymarket.ws", "decode_market_messages"),
    "market_operation_payload": ("pmkt.exchanges.polymarket.ws", "market_operation_payload"),
    "market_subscription_payload": (
        "pmkt.exchanges.polymarket.ws",
        "market_subscription_payload",
    ),
    "stream_order_book_data": (
        "pmkt.exchanges.polymarket.order_book_stream",
        "stream_order_book_data",
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
