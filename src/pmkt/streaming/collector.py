from __future__ import annotations

from contextlib import AsyncExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Protocol, Sequence

from pmkt.data.io import StreamingParquetSink, read_parquet_segment_manifest


@dataclass(frozen=True)
class StreamDatasetSpec:
    file_key: str
    filename: str
    schema: Any
    manifest_schema_key: str | None = None
    schema_version: str | None = None
    role: str | None = None

    @property
    def include_in_run_manifest(self) -> bool:
        return self.manifest_schema_key is not None


class StreamSinkSet:
    def __init__(
        self,
        specs: Sequence[StreamDatasetSpec],
        paths: Mapping[str, Path],
        *,
        parquet_segment_rows: int | None,
        parquet_segment_seconds: float | None,
    ) -> None:
        self._specs = tuple(specs)
        self._paths = dict(paths)
        self._parquet_segment_rows = parquet_segment_rows
        self._parquet_segment_seconds = parquet_segment_seconds
        self._stack = AsyncExitStack()
        self._sinks: dict[str, StreamingParquetSink] = {}

    async def __aenter__(self) -> "StreamSinkSet":
        for spec in self._specs:
            self._sinks[spec.file_key] = await self._stack.enter_async_context(
                StreamingParquetSink(
                    self._paths[spec.file_key],
                    spec.schema,
                    segment_rows=self._parquet_segment_rows,
                    segment_seconds=self._parquet_segment_seconds,
                )
            )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self._stack.__aexit__(exc_type, exc, tb)

    def __getitem__(self, key: str) -> StreamingParquetSink:
        return self._sinks[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._sinks)

    def items(self):
        return self._sinks.items()


class RuntimeFeedProjectionRecorder(Protocol):
    def record_feed_projection(
        self,
        *,
        feed_health_rows: Sequence[Mapping[str, Any]],
        topbooks: Iterable[Mapping[str, Any]],
        observed_at_utc: str,
    ) -> None: ...

    def record_recovery_actions(
        self,
        *,
        actions: Iterable[Any],
        observed_at_utc: str,
    ) -> None: ...


class StreamRunOutputs:
    def __init__(
        self,
        *,
        run_dir: str | Path,
        datasets: Sequence[StreamDatasetSpec],
        raw_jsonl_name: str = "raw_events.jsonl",
        manifest_name: str = "manifest.json",
        include_raw_jsonl: bool = True,
        parquet_segment_rows: int | None,
        parquet_segment_seconds: float | None,
    ) -> None:
        validate_parquet_rotation_config(
            parquet_segment_rows=parquet_segment_rows,
            parquet_segment_seconds=parquet_segment_seconds,
        )
        self.run_dir = Path(run_dir)
        self.datasets = tuple(datasets)
        self.raw_jsonl_path = self.run_dir / raw_jsonl_name
        self.manifest_path = self.run_dir / manifest_name
        self.include_raw_jsonl = bool(include_raw_jsonl)
        self.parquet_segment_rows = parquet_segment_rows
        self.parquet_segment_seconds = parquet_segment_seconds
        self._paths = {
            spec.file_key: self.run_dir / spec.filename for spec in self.datasets
        }

    def path(self, file_key: str) -> Path:
        return self._paths[file_key]

    @property
    def files(self) -> dict[str, str]:
        files = {key: str(path) for key, path in self._paths.items()}
        if self.include_raw_jsonl:
            files = {"raw_events_jsonl": str(self.raw_jsonl_path), **files}
        return files

    @property
    def dataset_specs_by_role(self) -> dict[str, StreamDatasetSpec]:
        result: dict[str, StreamDatasetSpec] = {}
        for spec in self.datasets:
            if spec.role is None:
                continue
            if spec.role in result:
                raise ValueError(f"duplicate stream dataset role {spec.role!r}")
            result[spec.role] = spec
        return result

    @property
    def dataset_paths(self) -> dict[str, str]:
        return {
            spec.file_key: str(self._paths[spec.file_key])
            for spec in self.datasets
            if spec.include_in_run_manifest
        }

    @property
    def schema_versions(self) -> dict[str, str]:
        versions: dict[str, str] = {}
        for spec in self.datasets:
            if not spec.include_in_run_manifest:
                continue
            if not spec.schema_version:
                raise ValueError(f"{spec.file_key} is missing schema_version")
            assert spec.manifest_schema_key is not None
            versions[spec.manifest_schema_key] = spec.schema_version
        return versions

    def parquet_segment_manifests(self) -> dict[str, Any]:
        manifests: dict[str, Any] = {}
        for spec in self.datasets:
            payload = read_parquet_segment_manifest(self._paths[spec.file_key])
            if payload is not None:
                manifests[spec.file_key] = payload
        return manifests

    def open_sinks(self) -> StreamSinkSet:
        return StreamSinkSet(
            self.datasets,
            self._paths,
            parquet_segment_rows=self.parquet_segment_rows,
            parquet_segment_seconds=self.parquet_segment_seconds,
        )


def validate_parquet_rotation_config(
    *,
    parquet_segment_rows: int | None,
    parquet_segment_seconds: float | None,
) -> None:
    if parquet_segment_rows is not None and parquet_segment_rows <= 0:
        raise ValueError("parquet_segment_rows must be > 0 when provided")
    if parquet_segment_seconds is not None and parquet_segment_seconds <= 0:
        raise ValueError("parquet_segment_seconds must be > 0 when provided")


__all__ = [
    "RuntimeFeedProjectionRecorder",
    "StreamDatasetSpec",
    "StreamSinkSet",
    "StreamRunOutputs",
    "validate_parquet_rotation_config",
]
