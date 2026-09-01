from __future__ import annotations

from typing import Sequence

from pmkt.data.registry import (
    CAPTURE_INSTRUMENT_EVIDENCE_SCHEMA_VERSION,
    BOOK_TAPE_CONTROL_SCHEMA_VERSION,
    BOOK_TAPE_EVENT_SCHEMA_VERSION,
    BOOK_TAPE_LEVEL_SCHEMA_VERSION,
    DEPTH_SCHEMA_VERSION,
    FEED_HEALTH_SCHEMA_VERSION,
    STREAM_LIFECYCLE_SCHEMA_VERSION,
    TOPBOOK_SCHEMA_VERSION,
    TRADE_SCHEMA_VERSION,
    arrow_schema,
    get_table_spec,
)
from pmkt.streaming.collector import StreamDatasetSpec
from pmkt.streaming.profiles import DatasetRole


def _registered_spec(
    role: DatasetRole,
    filename: str,
    schema_version: str,
    *,
    manifest_schema_key: str | None = None,
) -> StreamDatasetSpec:
    return StreamDatasetSpec(
        file_key=role.value,
        filename=filename,
        schema=arrow_schema(get_table_spec(schema_version)),
        manifest_schema_key=manifest_schema_key or role.value,
        schema_version=schema_version,
        role=role.value,
    )


CANONICAL_PROFILE_DATASETS = (
    _registered_spec(
        DatasetRole.TOPBOOK_MAIN,
        "topbook_v1.parquet",
        TOPBOOK_SCHEMA_VERSION,
        manifest_schema_key="topbook",
    ),
    _registered_spec(
        DatasetRole.TOPBOOK_CHECKPOINT,
        "topbook_checkpoints.parquet",
        TOPBOOK_SCHEMA_VERSION,
        manifest_schema_key="topbook_checkpoint",
    ),
    _registered_spec(
        DatasetRole.DEPTH_MAIN,
        "depth_v1.parquet",
        DEPTH_SCHEMA_VERSION,
        manifest_schema_key="depth",
    ),
    _registered_spec(
        DatasetRole.TAPE_EVENT,
        "book_tape_event.parquet",
        BOOK_TAPE_EVENT_SCHEMA_VERSION,
    ),
    _registered_spec(
        DatasetRole.TAPE_LEVEL,
        "book_tape_level.parquet",
        BOOK_TAPE_LEVEL_SCHEMA_VERSION,
    ),
    _registered_spec(
        DatasetRole.TAPE_CONTROL,
        "book_tape_control.parquet",
        BOOK_TAPE_CONTROL_SCHEMA_VERSION,
    ),
    _registered_spec(DatasetRole.TRADE, "trades.parquet", TRADE_SCHEMA_VERSION),
    _registered_spec(
        DatasetRole.LIFECYCLE,
        "stream_lifecycle.parquet",
        STREAM_LIFECYCLE_SCHEMA_VERSION,
    ),
    _registered_spec(
        DatasetRole.HEALTH,
        "feed_health.parquet",
        FEED_HEALTH_SCHEMA_VERSION,
        manifest_schema_key="feed_health",
    ),
    _registered_spec(
        DatasetRole.INSTRUMENT_EVIDENCE,
        "capture_instrument_evidence.parquet",
        CAPTURE_INSTRUMENT_EVIDENCE_SCHEMA_VERSION,
    ),
)


def merge_profile_dataset_specs(
    venue_specs: Sequence[StreamDatasetSpec],
) -> tuple[StreamDatasetSpec, ...]:
    """Combine venue-specific raw/legacy schemas with shared registered roles."""
    by_role = {
        spec.role: spec for spec in CANONICAL_PROFILE_DATASETS if spec.role is not None
    }
    unroled: list[StreamDatasetSpec] = []
    for spec in venue_specs:
        if spec.role is None:
            unroled.append(spec)
            continue
        by_role.setdefault(spec.role, spec)
    return (*unroled, *(by_role[role] for role in sorted(by_role)))


__all__ = ["CANONICAL_PROFILE_DATASETS", "merge_profile_dataset_specs"]
