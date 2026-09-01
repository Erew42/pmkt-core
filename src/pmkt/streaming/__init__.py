from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from pmkt.streaming.collector import (
    RuntimeFeedProjectionRecorder,
    StreamDatasetSpec,
    StreamSinkSet,
    StreamRunOutputs,
    validate_parquet_rotation_config,
)
from pmkt.streaming.measurement import (
    CliImportTimingResult,
    CliImportTimingSpec,
    FakeWebsocketReplayConfig,
    FakeWebsocketReplayReport,
    default_cli_import_timing_specs,
    measure_cli_import_timing,
    pr15_fake_websocket_replay_config,
    run_fake_websocket_load_replay,
)
if TYPE_CHECKING:
    from pmkt.streaming.supervisor import (
        FeedPreflightReport,
        FeedRecoveryAction,
        FeedShardHealth,
        LiveFeedSupervisor,
        SubscriptionPlanValidation,
        SubscriptionPlanValidator,
    )


_SUPERVISOR_EXPORTS = frozenset(
    {
        "FeedPreflightReport",
        "FeedRecoveryAction",
        "FeedShardHealth",
        "LiveFeedSupervisor",
        "SubscriptionPlanValidation",
        "SubscriptionPlanValidator",
    }
)


def __getattr__(name: str) -> Any:
    """Load the optional pandas-backed supervisor only when it is requested."""
    if name not in _SUPERVISOR_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module("pmkt.streaming.supervisor")
    value = getattr(module, name)
    globals()[name] = value
    return value

__all__ = [
    "CliImportTimingResult",
    "CliImportTimingSpec",
    "FakeWebsocketReplayConfig",
    "FakeWebsocketReplayReport",
    "FeedPreflightReport",
    "FeedRecoveryAction",
    "FeedShardHealth",
    "LiveFeedSupervisor",
    "RuntimeFeedProjectionRecorder",
    "StreamDatasetSpec",
    "StreamSinkSet",
    "StreamRunOutputs",
    "SubscriptionPlanValidation",
    "SubscriptionPlanValidator",
    "default_cli_import_timing_specs",
    "measure_cli_import_timing",
    "pr15_fake_websocket_replay_config",
    "run_fake_websocket_load_replay",
    "validate_parquet_rotation_config",
]
