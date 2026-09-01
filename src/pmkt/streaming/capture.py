from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from pmkt.streaming.collector import StreamRunOutputs, StreamSinkSet
from pmkt.streaming.profiles import DatasetRole, StorageProfileSelection


@dataclass(frozen=True)
class CaptureWriteIntent:
    role: DatasetRole
    row: Mapping[str, Any]


@dataclass(frozen=True)
class TapeBatchIntent:
    event: Mapping[str, Any]
    levels: tuple[Mapping[str, Any], ...]

    @classmethod
    def materialize(
        cls,
        *,
        event: Mapping[str, Any],
        levels: Sequence[Mapping[str, Any]],
    ) -> "TapeBatchIntent":
        frozen_levels = tuple(
            dict(row)
            for row in sorted(
                levels,
                key=lambda row: (
                    str(row.get("source_side") or ""),
                    str(row.get("price_key") or ""),
                    int(row.get("level_ordinal") or 0),
                ),
            )
        )
        return cls(event=dict(event), levels=frozen_levels)


@dataclass(frozen=True)
class CaptureCompleteness:
    required_roles: frozenset[DatasetRole]
    enabled_roles: frozenset[DatasetRole]
    committed_roles: frozenset[DatasetRole]
    row_counts: Mapping[str, int]
    close_error: str | None = None

    @property
    def missing_required_roles(self) -> frozenset[DatasetRole]:
        return self.required_roles - self.committed_roles

    @property
    def complete(self) -> bool:
        return self.close_error is None and not self.missing_required_roles


class CaptureRouter:
    """Profile-aware sink router shared by venue stream orchestration shells.

    Venue modules submit canonical capture intents. The router never parses wire
    payloads or owns venue book semantics.
    """

    def __init__(
        self,
        *,
        selection: StorageProfileSelection,
        outputs: StreamRunOutputs,
    ) -> None:
        self.selection = selection
        self.outputs = outputs
        specs_by_role = outputs.dataset_specs_by_role
        expected = selection.enabled_roles - {DatasetRole.RAW_JSONL}
        missing = {role for role in expected if role.value not in specs_by_role}
        if missing:
            joined = ", ".join(sorted(role.value for role in missing))
            raise ValueError(f"enabled capture roles have no dataset spec: {joined}")
        unexpected = {
            DatasetRole(role)
            for role in specs_by_role
            if DatasetRole(role) not in expected
        }
        if unexpected:
            joined = ", ".join(sorted(role.value for role in unexpected))
            raise ValueError(f"outputs contain disabled capture roles: {joined}")
        self._specs_by_role = {
            DatasetRole(role): spec for role, spec in specs_by_role.items()
        }
        self._sink_context: StreamSinkSet | None = None
        self._sinks: StreamSinkSet | None = None
        self._row_counts: Counter[str] = Counter()
        self._committed_roles: set[DatasetRole] = set()
        self._close_error: str | None = None

    async def __aenter__(self) -> "CaptureRouter":
        self._sink_context = self.outputs.open_sinks()
        self._sinks = await self._sink_context.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        context = self._sink_context
        if context is None:
            return
        try:
            await context.__aexit__(exc_type, exc, tb)
        except Exception as close_exc:
            self._close_error = f"{type(close_exc).__name__}: {close_exc}"
            raise
        else:
            if exc_type is None:
                self._committed_roles.update(self._specs_by_role)
        finally:
            self._sinks = None

    async def write(self, intent: CaptureWriteIntent) -> bool:
        role = intent.role
        if role not in self.selection.enabled_roles:
            return False
        if role is DatasetRole.RAW_JSONL:
            raise ValueError("raw JSONL is a line-file role, not a parquet sink intent")
        sinks = self._require_open_sinks()
        spec = self._specs_by_role.get(role)
        if spec is None:
            if role in self.selection.required_roles:
                raise RuntimeError(f"mandatory capture role {role.value!r} is unavailable")
            return False
        await sinks[spec.file_key].write(dict(intent.row))
        self._row_counts[role.value] += 1
        return True

    async def write_tape_batch(self, batch: TapeBatchIntent) -> bool:
        event_enabled = DatasetRole.TAPE_EVENT in self.selection.enabled_roles
        level_enabled = DatasetRole.TAPE_LEVEL in self.selection.enabled_roles
        if event_enabled != level_enabled:
            raise RuntimeError("tape event and level roles must be enabled together")
        if not event_enabled:
            return False
        for level in batch.levels:
            await self.write(CaptureWriteIntent(DatasetRole.TAPE_LEVEL, level))
        await self.write(CaptureWriteIntent(DatasetRole.TAPE_EVENT, batch.event))
        return True

    async def write_topbook(self, row: Mapping[str, Any]) -> bool:
        return await self.write(CaptureWriteIntent(DatasetRole.TOPBOOK_MAIN, row))

    async def write_checkpoint(self, row: Mapping[str, Any]) -> bool:
        return await self.write(
            CaptureWriteIntent(DatasetRole.TOPBOOK_CHECKPOINT, row)
        )

    async def write_trade(self, row: Mapping[str, Any]) -> bool:
        return await self.write(CaptureWriteIntent(DatasetRole.TRADE, row))

    async def write_lifecycle(self, row: Mapping[str, Any]) -> bool:
        return await self.write(CaptureWriteIntent(DatasetRole.LIFECYCLE, row))

    async def write_health(self, row: Mapping[str, Any]) -> bool:
        return await self.write(CaptureWriteIntent(DatasetRole.HEALTH, row))

    async def write_control(self, row: Mapping[str, Any]) -> bool:
        return await self.write(CaptureWriteIntent(DatasetRole.TAPE_CONTROL, row))

    def completeness(self) -> CaptureCompleteness:
        required_dataset_roles = self.selection.required_roles - {
            DatasetRole.RAW_JSONL
        }
        committed = set(self._committed_roles)
        if DatasetRole.RAW_JSONL in self.selection.enabled_roles and self.outputs.include_raw_jsonl:
            committed.add(DatasetRole.RAW_JSONL)
        return CaptureCompleteness(
            required_roles=frozenset(required_dataset_roles),
            enabled_roles=self.selection.enabled_roles,
            committed_roles=frozenset(committed),
            row_counts=dict(sorted(self._row_counts.items())),
            close_error=self._close_error,
        )

    def _require_open_sinks(self) -> StreamSinkSet:
        if self._sinks is None:
            raise RuntimeError("capture router is not open")
        return self._sinks


__all__ = [
    "CaptureCompleteness",
    "CaptureRouter",
    "CaptureWriteIntent",
    "TapeBatchIntent",
]
