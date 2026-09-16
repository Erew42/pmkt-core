"""Public kalshi recording entrypoint; see docs/stream_recording_contract.md."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Literal, Sequence

from pmkt.exchanges.read_auth import ReadAuthHeaderProvider
from pmkt.streaming.recording import RecordingOptions
from pmkt.streaming.recording_feed import record_feed

DEFAULT_KALSHI_ORDER_BOOK_STREAM_ROOT = Path("generated/kalshi_order_book_streams")


async def stream_kalshi_order_book_data(
    market_tickers: Sequence[str], *,
    mode: Literal["topbook", "full"] = "full",
    depth_check_interval_s: float | None = 10.0,
    depth_on_best_price_change: bool = False,
    raw_messages: bool = False,
    output_root: str | Path = "generated/kalshi_order_book_streams",
    run_name: str | None = None,
    duration_s: float = 300.0,
    max_messages: int | None = None,
    max_reconnects: int = 3,
    ws_url: str | None = None,
    websocket_max_size_bytes: int | None = None,
    websocket_max_queue_frames: int | None = None,
    connect_factory: Callable[..., Any] | None = None,
    auth: ReadAuthHeaderProvider | None = None,
) -> dict[str, Any]:
    """Record changed topbooks and optional full snapshots into SQLite/Parquet.

    ``None`` disables periodic depth checks when the best-price trigger is on.
    Historical storage-profile arguments and outputs have been retired.
    """
    options = RecordingOptions(mode, depth_check_interval_s, depth_on_best_price_change, raw_messages)
    return await record_feed(
        market_tickers, venue="kalshi", options=options,
        output_root=output_root, run_name=run_name, duration_s=duration_s,
        max_messages=max_messages, max_reconnects=max_reconnects,
        ws_url=ws_url, connect_factory=connect_factory,
        websocket_max_size_bytes=websocket_max_size_bytes,
        websocket_max_queue_frames=websocket_max_queue_frames,
        auth=auth,
    )
