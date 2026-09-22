"""Live recording plus explicitly requested historical/diagnostic helpers."""
from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    **dict.fromkeys(("RuntimeFeedProjectionRecorder", "StreamDatasetSpec", "StreamSinkSet", "StreamRunOutputs", "validate_parquet_rotation_config"), "collector"),
    **dict.fromkeys(("CliImportTimingResult", "CliImportTimingSpec", "FakeWebsocketReplayConfig", "FakeWebsocketReplayReport", "default_cli_import_timing_specs", "measure_cli_import_timing", "pr15_fake_websocket_replay_config", "run_fake_websocket_load_replay"), "measurement"),
    **dict.fromkeys(("FeedPreflightReport", "FeedRecoveryAction", "FeedShardHealth", "LiveFeedSupervisor", "SubscriptionPlanValidation", "SubscriptionPlanValidator"), "supervisor"),
    "RecordingOptions": "recording",
    "export_recording": "recording_store",
}
__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(f"pmkt.streaming.{_EXPORTS[name]}"), name)
    globals()[name] = value
    return value
