from __future__ import annotations

import contextlib
import os
import tempfile
from pathlib import Path
from typing import Any

from pmkt.data.registry import arrow_schema, get_table_spec
from pmkt.data.validation import convert_frame_strict


class AtomicParquetWriter:
    """Write validated page frames into one atomically published Parquet file."""

    def __init__(self, path: Path, *, schema_version: str) -> None:
        import pyarrow.parquet as pq

        self.path = path
        self.spec = get_table_spec(schema_version)
        self.schema = arrow_schema(self.spec)
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temp_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
        )
        os.close(descriptor)
        self.temp_path = Path(temp_name)
        self.writer = pq.ParquetWriter(self.temp_path, self.schema)
        self.row_count = 0
        self._closed = False

    def append(self, frame: Any) -> None:
        import pyarrow as pa

        if self._closed:
            raise RuntimeError("Parquet stream is already closed")
        converted = convert_frame_strict(frame, self.spec)
        table = pa.Table.from_pandas(
            converted,
            schema=self.schema,
            preserve_index=False,
            safe=True,
        )
        self.writer.write_table(table)
        self.row_count += len(converted)

    def finish(self) -> Path:
        if self._closed:
            return self.path
        try:
            self.writer.close()
            self._closed = True
            with self.temp_path.open("r+b") as handle:
                os.fsync(handle.fileno())
            os.replace(self.temp_path, self.path)
        except BaseException:
            self._closed = True
            with contextlib.suppress(OSError):
                self.temp_path.unlink(missing_ok=True)
            raise
        return self.path

    def abort(self) -> None:
        if not self._closed:
            with contextlib.suppress(Exception):
                self.writer.close()
            self._closed = True
        with contextlib.suppress(OSError):
            self.temp_path.unlink(missing_ok=True)
