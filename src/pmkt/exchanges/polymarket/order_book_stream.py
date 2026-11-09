"""Public polymarket recording entrypoint; see docs/stream_recording_contract.md."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Literal, Sequence, cast

from pmkt.streaming.recording import RecordingOptions, _UNSET_DEPTH_CHECK_INTERVAL
from pmkt.streaming.recording_feed import record_feed

DEFAULT_ORDER_BOOK_STREAM_ROOT = Path("generated/order_book_streams")


async def stream_order_book_data(
    asset_ids: Sequence[str], *,
    mode: Literal["topbook", "full"] | None = None,
    depth_check_interval_s: float | None = cast(Any, _UNSET_DEPTH_CHECK_INTERVAL),
    depth_on_best_price_change: bool | None = None,
    raw_messages: bool | None = None,
    output_root: str | Path = "generated/order_book_streams",
    run_name: str | None = None,
    duration_s: float | None = None,
    max_messages: int | None = None,
    max_reconnects: int = 3,
    ws_url: str | None = None,
    websocket_max_size_bytes: int | None = None,
    websocket_max_queue_frames: int | None = None,
    connect_factory: Callable[..., Any] | None = None,
    heartbeat_interval: float | None = 10.0,
    custom_feature_enabled: bool = True,
) -> dict[str, Any]:
    """Record changed topbooks and optional full snapshots into SQLite/Parquet.

    Omitted or explicit ``None`` for ``mode``, ``depth_on_best_price_change``,
    ``raw_messages`` and ``duration_s`` inherits the owner default; explicit
    ``depth_check_interval_s=None`` instead disables periodic depth checks when
    the best-price trigger is on.
    Historical storage-profile arguments and outputs have been retired.
    """
    option_overrides: dict[str, Any] = {
        name: value for name, value in {
            "mode": mode,
            "depth_on_best_price_change": depth_on_best_price_change,
            "raw_messages": raw_messages,
        }.items() if value is not None
    }
    if depth_check_interval_s is not _UNSET_DEPTH_CHECK_INTERVAL:
        option_overrides["depth_check_interval_s"] = depth_check_interval_s
    options = RecordingOptions(**option_overrides)
    feed_overrides: dict[str, Any] = {}
    if duration_s is not None:
        feed_overrides["duration_s"] = duration_s
    return await record_feed(
        asset_ids, venue="polymarket", options=options,
        output_root=output_root, run_name=run_name, **feed_overrides,
        max_messages=max_messages, max_reconnects=max_reconnects,
        ws_url=ws_url, connect_factory=connect_factory,
        websocket_max_size_bytes=websocket_max_size_bytes,
        websocket_max_queue_frames=websocket_max_queue_frames,
        heartbeat_interval=heartbeat_interval, custom_feature_enabled=custom_feature_enabled,
    )
