from __future__ import annotations

import csv
import json
import os
import time
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence, TextIO, runtime_checkable


RECOMMENDED_PARQUET_SEGMENT_ROWS = 50_000
RECOMMENDED_PARQUET_SEGMENT_SECONDS = 60.0
# Compatibility aliases for callers that imported the old names. Stream
# commands now default to single-file parquet; these values are opt-in settings.
DEFAULT_PARQUET_SEGMENT_ROWS = RECOMMENDED_PARQUET_SEGMENT_ROWS
DEFAULT_PARQUET_SEGMENT_SECONDS = RECOMMENDED_PARQUET_SEGMENT_SECONDS
PARQUET_SEGMENT_MANIFEST_NAME = "_segments.json"


def _prepare_output_path(
    path: str | Path,
    *,
    allow_existing_directory: bool = False,
) -> Path:
    output = Path(path)
    if output.exists() and output.is_dir() and not allow_existing_directory:
        raise IsADirectoryError(f"Output path is a directory: {output}")
    if output.parent.exists() and not output.parent.is_dir():
        raise NotADirectoryError(f"Output parent is not a directory: {output.parent}")
    output.parent.mkdir(parents=True, exist_ok=True)
    return output


def parquet_segment_manifest_path(path: str | Path) -> Path:
    return Path(path) / PARQUET_SEGMENT_MANIFEST_NAME


def read_parquet_segment_manifest(path: str | Path) -> dict[str, Any] | None:
    manifest_path = parquet_segment_manifest_path(path)
    if not manifest_path.exists():
        return None
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"segment manifest root must be a JSON object: {manifest_path}")
    return payload


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(path.name + ".tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, indent=2, sort_keys=True))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp_path, path)


@runtime_checkable
class Sink(Protocol):
    async def write(self, item: Any) -> None:
        ...

    async def close(self) -> None:
        ...

    async def __aenter__(self) -> "Sink":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


class JsonlSink:
    def __init__(self, path: str | Path):
        self.path = _prepare_output_path(path)
        self._file: TextIO | None = None

    async def write(self, item: Any) -> None:
        if self._file is None:
            self._file = self.path.open("a", encoding="utf-8")
        output_file = self._file
        assert output_file is not None

        # Handle Pydantic models
        if hasattr(item, "model_dump_json"):
            line = item.model_dump_json()
        elif hasattr(item, "json"):  # Older pydantic or other objects
            line = item.json()
        elif isinstance(item, dict):
            line = json.dumps(item)
        else:
            raise ValueError(f"Cannot serialize {type(item)}")

        output_file.write(line + "\n")

    async def close(self) -> None:
        if self._file:
            self._file.close()
            self._file = None

    async def __aenter__(self) -> "JsonlSink":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


class CsvSink:
    def __init__(self, path: str | Path, columns: Sequence[str]):
        self.path = _prepare_output_path(path)
        self.columns = list(columns)
        self._file: TextIO | None = None
        self._writer: Any | None = None

    async def write(self, item: dict[str, Any] | list[Any]) -> None:
        if self._file is None:
            write_header = not self.path.exists()
            self._file = self.path.open("a", encoding="utf-8", newline="")
            self._writer = csv.writer(self._file)
            if write_header:
                self._writer.writerow(self.columns)
        writer = self._writer
        assert writer is not None

        row: list[Any]
        if isinstance(item, dict):
            row = []
            for col in self.columns:
                row.append(item.get(col))
        elif isinstance(item, (list, tuple)):
            row = list(item)
        else:
            raise ValueError(f"Expected dict or list/tuple, got {type(item)}")

        writer.writerow(row)

    async def close(self) -> None:
        if self._file:
            self._file.close()
            self._file = None
            self._writer = None

    async def __aenter__(self) -> "CsvSink":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


class ParquetSink:
    def __init__(self, path: str | Path, columns: Sequence[str], flush_interval: int = 1000):
        self.path = _prepare_output_path(path)
        self.columns = list(columns)
        self.flush_interval = flush_interval
        self._rows: list[dict[str, Any]] = []
        self._writer: Any | None = None
        self._schema: Any | None = None
        self._temp_path: Path | None = None

    async def write(self, item: dict[str, Any]) -> None:
        self._rows.append(item)
        if len(self._rows) >= self.flush_interval:
            self._flush()

    def _flush(self) -> None:
        if not self._rows:
            return

        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        df = pd.DataFrame(self._rows, columns=self.columns)
        table = pa.Table.from_pandas(df, preserve_index=False)

        if self._writer is None:
            if self.path.exists():
                self._temp_path = self.path.with_suffix(self.path.suffix + ".tmp")
                if self._temp_path.exists():
                    self._temp_path.unlink()

                existing = pq.ParquetFile(self.path)
                existing_rows = existing.metadata.num_rows if existing.metadata else 0
                self._schema = table.schema if existing_rows == 0 else existing.schema_arrow
                self._writer = pq.ParquetWriter(self._temp_path, self._schema)
                if existing_rows > 0:
                    for idx in range(existing.num_row_groups):
                        self._writer.write_table(existing.read_row_group(idx))
            else:
                self._schema = table.schema
                self._writer = pq.ParquetWriter(self.path, self._schema)

        if self._schema is not None and table.schema != self._schema:
            # Basic cast if schema evolved or types differ slightly.
            table = table.cast(self._schema)

        writer = self._writer
        assert writer is not None
        writer.write_table(table)
        self._rows.clear()

    async def close(self) -> None:
        self._flush()
        if self._writer:
            self._writer.close()
            self._writer = None

        # If we wrote to a temp file, replace original
        if self._temp_path:
            self._temp_path.replace(self.path)
            self._temp_path = None
        elif self._writer is None and not self.path.exists():
            # Create empty parquet if nothing wrote.
            import pandas as pd
            pd.DataFrame(columns=self.columns).to_parquet(self.path, index=False)

    async def __aenter__(self) -> "ParquetSink":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


def _rows_to_arrow_table(rows: Sequence[dict[str, Any]], schema: Any) -> Any:
    import pyarrow as pa

    arrays = [
        pa.array(
            [row.get(field.name) for row in rows],
            type=field.type,
        )
        for field in schema
    ]
    return pa.Table.from_arrays(arrays, schema=schema)


def _empty_arrow_table(schema: Any) -> Any:
    import pyarrow as pa

    arrays = [pa.array([], type=field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def append_parquet_segment(
    path: str | Path,
    schema: Any,
    rows: Sequence[dict[str, Any]],
    *,
    segment_prefix: str = "part",
) -> dict[str, Any]:
    """Append one complete parquet segment to an existing dataset directory."""
    import pyarrow.parquet as pq

    dataset_path = _prepare_output_path(path, allow_existing_directory=True)
    if dataset_path.exists() and not dataset_path.is_dir():
        raise NotADirectoryError(f"Segmented parquet path is not a directory: {dataset_path}")
    dataset_path.mkdir(parents=True, exist_ok=True)
    manifest = read_parquet_segment_manifest(dataset_path) or {}
    raw_rotation = manifest.get("rotation")
    rotation: Mapping[str, Any] = (
        raw_rotation if isinstance(raw_rotation, Mapping) else {}
    )
    completed = _completed_segments_from_manifest(manifest)
    next_index = _next_segment_index(completed)
    final_path = dataset_path / f"{segment_prefix}-{next_index:06d}.parquet"
    while final_path.exists():
        next_index += 1
        final_path = dataset_path / f"{segment_prefix}-{next_index:06d}.parquet"
    temp_path = dataset_path / f"._{segment_prefix}-{next_index:06d}.parquet.tmp"
    if temp_path.exists():
        temp_path.unlink()
    table = _rows_to_arrow_table(rows, schema) if rows else _empty_arrow_table(schema)
    pq.write_table(table, temp_path)
    os.replace(temp_path, final_path)
    completed.append(
        {
            "index": next_index,
            "path": final_path.name,
            "row_count": len(rows),
            "completed_at_epoch_seconds": time.time(),
        }
    )
    payload = _segment_manifest_payload(
        dataset_path=dataset_path,
        status="closed",
        segment_prefix=segment_prefix,
        segment_rows=_optional_int(rotation.get("rows")),
        segment_seconds=_optional_float(rotation.get("seconds")),
        completed_segments=completed,
        incomplete_segments=[],
    )
    _write_json_atomic(parquet_segment_manifest_path(dataset_path), payload)
    return payload


def _completed_segments_from_manifest(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    value = manifest.get("completed_segments")
    if not isinstance(value, list):
        return []
    completed: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            completed.append(dict(item))
    return completed


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _next_segment_index(completed_segments: Sequence[Mapping[str, Any]]) -> int:
    indices: list[int] = []
    for segment in completed_segments:
        try:
            indices.append(int(segment.get("index", -1)))
        except (TypeError, ValueError):
            continue
    return max(indices, default=-1) + 1


def _segment_manifest_payload(
    *,
    dataset_path: Path,
    status: str,
    segment_prefix: str,
    segment_rows: int | None,
    segment_seconds: float | None,
    completed_segments: Sequence[Mapping[str, Any]],
    incomplete_segments: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    completed = [dict(segment) for segment in completed_segments]
    incomplete = [dict(segment) for segment in incomplete_segments]
    return {
        "format": "pmkt.parquet_segments.v1",
        "dataset_path": str(dataset_path),
        "status": status,
        "updated_at_epoch_seconds": time.time(),
        "segment_prefix": segment_prefix,
        "rotation": {
            "rows": segment_rows,
            "seconds": segment_seconds,
        },
        "row_count": sum(int(segment.get("row_count") or 0) for segment in completed),
        "completed_segments": completed,
        "incomplete_segments": incomplete,
    }


class StreamingParquetSink:
    """Write parquet row groups incrementally using an explicit Arrow schema."""

    def __init__(
        self,
        path: str | Path,
        schema: Any,
        flush_interval: int = 1000,
        *,
        overwrite: bool = True,
        segment_rows: int | None = None,
        segment_seconds: float | None = None,
        segment_prefix: str = "part",
    ):
        if flush_interval <= 0:
            raise ValueError("flush_interval must be > 0")
        if segment_rows is not None and segment_rows <= 0:
            raise ValueError("segment_rows must be > 0 when provided")
        if segment_seconds is not None and segment_seconds <= 0:
            raise ValueError("segment_seconds must be > 0 when provided")
        self.segment_rows = segment_rows
        self.segment_seconds = segment_seconds
        self.segment_prefix = segment_prefix
        self.path = _prepare_output_path(
            path,
            allow_existing_directory=self._segmented,
        )
        self.schema = schema
        self.flush_interval = (
            min(flush_interval, segment_rows)
            if segment_rows is not None
            else flush_interval
        )
        self.overwrite = overwrite
        self._rows: list[dict[str, Any]] = []
        self._writer: Any | None = None
        self._started = False
        self._completed_segments: list[dict[str, Any]] = []
        self._active_segment_index: int | None = None
        self._active_segment_rows = 0
        self._active_segment_started_at_epoch: float | None = None
        self._active_segment_started_at_monotonic: float | None = None
        self._active_temp_path: Path | None = None
        self._active_final_path: Path | None = None

    @property
    def _segmented(self) -> bool:
        return self.segment_rows is not None or self.segment_seconds is not None

    async def write(self, item: dict[str, Any]) -> None:
        self._ensure_started()
        if self._segmented and self._should_rotate_for_time():
            self._finalize_active_segment()
        self._rows.append(item)
        if len(self._rows) >= self.flush_interval:
            self._flush()

    def _ensure_started(self) -> None:
        if self._started:
            return
        if not self.overwrite and self.path.exists():
            raise FileExistsError(f"{self.path} already exists")
        if self._segmented:
            self._prepare_segment_directory()
            self._write_segment_manifest(status="open")
        elif self.overwrite and self.path.exists():
            self.path.unlink()
        self._started = True

    def _prepare_segment_directory(self) -> None:
        if self.path.exists() and self.path.is_file():
            self.path.unlink()
        self.path.mkdir(parents=True, exist_ok=True)
        if not self.overwrite:
            return
        for child in self.path.iterdir():
            if child.is_dir():
                raise IsADirectoryError(
                    f"Segmented parquet output contains a nested directory: {child}"
                )
            child.unlink()

    def _flush(self) -> None:
        if not self._rows:
            return
        if self._segmented:
            self._flush_segmented()
        else:
            self._flush_single_file()

    def _flush_single_file(self) -> None:
        import pyarrow.parquet as pq

        if self._writer is None:
            self._writer = pq.ParquetWriter(self.path, self.schema)
        table = _rows_to_arrow_table(self._rows, self.schema)
        self._writer.write_table(table)
        self._rows.clear()

    def _flush_segmented(self) -> None:
        pending = list(self._rows)
        self._rows.clear()
        while pending:
            self._ensure_active_segment()
            if self._should_rotate_for_time() and self._active_segment_rows > 0:
                self._finalize_active_segment()
                continue
            row_budget = len(pending)
            if self.segment_rows is not None:
                remaining = self.segment_rows - self._active_segment_rows
                if remaining <= 0:
                    self._finalize_active_segment()
                    continue
                row_budget = min(row_budget, remaining)
            chunk = pending[:row_budget]
            del pending[:row_budget]
            table = _rows_to_arrow_table(chunk, self.schema)
            writer = self._writer
            assert writer is not None
            writer.write_table(table)
            self._active_segment_rows += len(chunk)
            self._write_segment_manifest(status="open")
            if (
                self.segment_rows is not None
                and self._active_segment_rows >= self.segment_rows
            ):
                self._finalize_active_segment()

    def _ensure_active_segment(self) -> None:
        if self._writer is not None:
            return
        import pyarrow.parquet as pq

        index = len(self._completed_segments)
        final_path = self.path / f"{self.segment_prefix}-{index:06d}.parquet"
        while final_path.exists():
            index += 1
            final_path = self.path / f"{self.segment_prefix}-{index:06d}.parquet"
        temp_path = self.path / f"._{self.segment_prefix}-{index:06d}.parquet.tmp"
        if temp_path.exists():
            temp_path.unlink()
        self._active_segment_index = index
        self._active_segment_rows = 0
        self._active_segment_started_at_epoch = time.time()
        self._active_segment_started_at_monotonic = time.monotonic()
        self._active_temp_path = temp_path
        self._active_final_path = final_path
        self._writer = pq.ParquetWriter(temp_path, self.schema)
        self._write_segment_manifest(status="open")

    def _finalize_active_segment(self) -> None:
        writer = self._writer
        if writer is None:
            return
        writer.close()
        self._writer = None
        temp_path = self._active_temp_path
        final_path = self._active_final_path
        if temp_path is None or final_path is None:
            raise RuntimeError("active parquet segment paths are not initialized")
        os.replace(temp_path, final_path)
        self._completed_segments.append(
            {
                "index": int(self._active_segment_index or 0),
                "path": final_path.name,
                "row_count": self._active_segment_rows,
                "started_at_epoch_seconds": self._active_segment_started_at_epoch,
                "completed_at_epoch_seconds": time.time(),
            }
        )
        self._active_segment_index = None
        self._active_segment_rows = 0
        self._active_segment_started_at_epoch = None
        self._active_segment_started_at_monotonic = None
        self._active_temp_path = None
        self._active_final_path = None
        self._write_segment_manifest(status="open")

    def _should_rotate_for_time(self) -> bool:
        if self.segment_seconds is None:
            return False
        if self._writer is None or self._active_segment_started_at_monotonic is None:
            return False
        return (
            time.monotonic() - self._active_segment_started_at_monotonic
        ) >= self.segment_seconds

    def _incomplete_segments(self) -> list[dict[str, Any]]:
        if self._active_segment_index is None or self._active_temp_path is None:
            return []
        return [
            {
                "index": self._active_segment_index,
                "path": self._active_temp_path.name,
                "row_count": self._active_segment_rows,
                "started_at_epoch_seconds": self._active_segment_started_at_epoch,
            }
        ]

    def _write_segment_manifest(self, *, status: str) -> None:
        if not self._segmented:
            return
        payload = _segment_manifest_payload(
            dataset_path=self.path,
            status=status,
            segment_prefix=self.segment_prefix,
            segment_rows=self.segment_rows,
            segment_seconds=self.segment_seconds,
            completed_segments=self._completed_segments,
            incomplete_segments=self._incomplete_segments(),
        )
        _write_json_atomic(parquet_segment_manifest_path(self.path), payload)

    async def close(self) -> None:
        self._ensure_started()
        self._flush()
        if self._segmented:
            if self._writer is None and not self._completed_segments:
                self._ensure_active_segment()
                writer = self._writer
                assert writer is not None
                writer.write_table(_empty_arrow_table(self.schema))
            self._finalize_active_segment()
            self._write_segment_manifest(status="closed")
            return
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        elif not self.path.exists():
            import pyarrow.parquet as pq

            pq.write_table(_empty_arrow_table(self.schema), self.path)

    async def __aenter__(self) -> "StreamingParquetSink":
        self._ensure_started()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()
