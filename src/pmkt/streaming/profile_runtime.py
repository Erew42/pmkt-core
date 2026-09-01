from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from pmkt.streaming.collector import StreamDatasetSpec
from pmkt.streaming.durability import DurableCaptureCoordinator
from pmkt.streaming.durability_settings import CaptureDurabilitySettings
from pmkt.streaming.profiles import DatasetRole, StorageProfileSelection
from pmkt.streaming.recovery_contracts import RunStateV1
from pmkt.streaming.storage_backends import (
    CaptureCoordinator,
    CaptureStorageBackend,
    CaptureStorageSettings,
)


class CoordinatorSink:
    def __init__(self, coordinator: CaptureCoordinator, role: str) -> None:
        self.coordinator = coordinator
        self.role = role

    async def write(self, row: Mapping[str, Any]) -> None:
        self.coordinator.add(self.role, row)


@dataclass
class ProfileCaptureRuntime:
    selection: StorageProfileSelection
    coordinator: CaptureCoordinator
    sinks: Mapping[str, CoordinatorSink]
    specs_by_role: Mapping[str, StreamDatasetSpec]

    async def __aenter__(self) -> Mapping[str, CoordinatorSink]:
        return self.sinks

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            await self.force_finalize_async()

    def force_finalize(self) -> None:
        self.coordinator.finalize_segments()

    async def force_finalize_async(self) -> None:
        from pmkt.streaming.sqlite_durability import SQLiteCaptureCoordinator

        if isinstance(self.coordinator, SQLiteCaptureCoordinator):
            # Capture acknowledgement remains the inline SQLite transaction.
            # Only clean-shutdown sealing, Parquet promotion, and full semantic
            # validation move off the collector event loop.
            await asyncio.to_thread(self.coordinator.finalize_segments)
            return
        self.coordinator.finalize_segments()

    def mark_finalized(self) -> None:
        self.coordinator.mark_finalized()

    def manifest_profile(self, *, terminal_completeness: str) -> dict[str, Any]:
        payload = self.selection.to_manifest_mapping()
        payload.update(
            {
                "successfully_committed_roles": sorted(
                    self.coordinator.committed_roles
                ),
                "terminal_completeness": terminal_completeness,
            }
        )
        return payload


def create_profile_runtime(
    *,
    run_dir: Path,
    selection: StorageProfileSelection,
    specs: Sequence[StreamDatasetSpec],
    shard_plan: Mapping[str, Any],
    adapter_settings_by_venue: Mapping[str, Mapping[str, Any]],
    started_at_utc: str,
    durability_settings: CaptureDurabilitySettings,
    storage_backend: CaptureStorageBackend | str = (
        CaptureStorageBackend.PARQUET_SEGMENTS
    ),
) -> ProfileCaptureRuntime:
    specs_by_role = {str(spec.role): spec for spec in specs if spec.role is not None}
    parquet_roles = selection.enabled_roles - {DatasetRole.RAW_JSONL}
    if set(specs_by_role) != {role.value for role in parquet_roles}:
        raise ValueError(
            "profile runtime specs must exactly match enabled parquet roles"
        )
    paths = {role: specs_by_role[role].filename for role in specs_by_role}
    schema_versions = {
        role: (specs_by_role[role].schema_version or f"legacy.{role}.v1")
        for role in specs_by_role
    }
    external_roles: set[str] = set()
    if DatasetRole.RAW_JSONL in selection.enabled_roles:
        paths[DatasetRole.RAW_JSONL.value] = "raw_events.jsonl"
        schema_versions[DatasetRole.RAW_JSONL.value] = "legacy.raw_jsonl.v1"
        external_roles.add(DatasetRole.RAW_JSONL.value)
    storage_settings = CaptureStorageSettings.for_backend(storage_backend)
    if (
        storage_settings.backend is CaptureStorageBackend.SQLITE_WAL
        and external_roles
    ):
        raise ValueError(
            "sqlite_wal_v1 does not yet support raw_jsonl; disable the raw sidecar "
            "or use parquet_segments"
        )
    state = RunStateV1(
        run_id=run_dir.name,
        profile_name=selection.definition.name,
        profile_version=selection.definition.profile_version,
        expected_role_paths=paths,
        storage_profile=selection.to_manifest_mapping(),
        shard_plan=dict(shard_plan),
        started_at_utc=started_at_utc,
        adapter_settings_by_venue=adapter_settings_by_venue,
        capture_durability=durability_settings.to_mapping(),
        capture_storage=storage_settings.to_mapping(),
    )
    coordinator_class: type[DurableCaptureCoordinator]
    if storage_settings.backend is CaptureStorageBackend.SQLITE_WAL:
        from pmkt.streaming.sqlite_durability import SQLiteCaptureCoordinator

        coordinator_class = SQLiteCaptureCoordinator
    else:
        coordinator_class = DurableCaptureCoordinator
    coordinator = coordinator_class(
        run_dir=run_dir,
        run_state=state,
        role_schema_versions=schema_versions,
        role_schemas={role: spec.schema for role, spec in specs_by_role.items()},
        external_file_roles=sorted(external_roles),
        segment_row_limit=durability_settings.effective_segment_rows,
        commit_interval_seconds=durability_settings.effective_segment_seconds,
        durability_settings=durability_settings,
    )
    sinks = {
        spec.file_key: CoordinatorSink(coordinator, role)
        for role, spec in specs_by_role.items()
    }
    return ProfileCaptureRuntime(selection, coordinator, sinks, specs_by_role)


__all__ = ["CoordinatorSink", "ProfileCaptureRuntime", "create_profile_runtime"]
