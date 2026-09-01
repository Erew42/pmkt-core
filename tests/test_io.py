import pandas as pd
import pyarrow as pa
import pytest

from pmkt.data.io import (
    StreamingParquetSink,
    ParquetSink,
    read_parquet_segment_manifest,
)


pytestmark = pytest.mark.asyncio


async def test_parquet_sink_creates_empty_file_with_columns(tmp_path) -> None:
    path = tmp_path / "empty.parquet"

    async with ParquetSink(path, columns=["token_id", "mid"]):
        pass

    df = pd.read_parquet(path)
    assert df.empty
    assert df.columns.tolist() == ["token_id", "mid"]


async def test_parquet_sink_fills_missing_columns(tmp_path) -> None:
    path = tmp_path / "metrics.parquet"

    async with ParquetSink(path, columns=["token_id", "mid"], flush_interval=1) as sink:
        await sink.write({"token_id": "token-1"})

    df = pd.read_parquet(path)
    assert df["token_id"].tolist() == ["token-1"]
    assert pd.isna(df.loc[0, "mid"])


async def test_parquet_sink_appends_to_preexisting_empty_file(tmp_path) -> None:
    path = tmp_path / "metrics.parquet"

    async with ParquetSink(path, columns=["token_id", "mid"]):
        pass
    async with ParquetSink(path, columns=["token_id", "mid"], flush_interval=1) as sink:
        await sink.write({"token_id": "token-1", "mid": 0.4})

    df = pd.read_parquet(path)
    assert df["token_id"].tolist() == ["token-1"]
    assert df["mid"].tolist() == [0.4]


async def test_parquet_sink_preserves_existing_rows_when_appending(tmp_path) -> None:
    path = tmp_path / "metrics.parquet"

    async with ParquetSink(path, columns=["token_id", "mid"], flush_interval=1) as sink:
        await sink.write({"token_id": "token-1", "mid": 0.4})
    async with ParquetSink(path, columns=["token_id", "mid"], flush_interval=1) as sink:
        await sink.write({"token_id": "token-2", "mid": 0.6})

    df = pd.read_parquet(path)
    assert df["token_id"].tolist() == ["token-1", "token-2"]
    assert df["mid"].tolist() == [0.4, 0.6]


async def test_parquet_sink_rejects_directory_output(tmp_path) -> None:
    with pytest.raises(IsADirectoryError, match="Output path is a directory"):
        ParquetSink(tmp_path, columns=["token_id"])


async def test_streaming_parquet_sink_overwrite_false_rejects_existing_path(
    tmp_path,
) -> None:
    path = tmp_path / "metrics.parquet"
    pd.DataFrame([{"value": 1}]).to_parquet(path, index=False)
    schema = pa.schema([("value", pa.int64())])

    with pytest.raises(FileExistsError, match="already exists"):
        async with StreamingParquetSink(path, schema, overwrite=False):
            pass

    assert pd.read_parquet(path)["value"].tolist() == [1]


async def test_streaming_parquet_sink_writes_readable_segments(tmp_path) -> None:
    path = tmp_path / "metrics.parquet"
    schema = pa.schema([("value", pa.int64())])

    async with StreamingParquetSink(
        path,
        schema,
        flush_interval=1,
        segment_rows=2,
    ) as sink:
        for value in range(5):
            await sink.write({"value": value})

    assert path.is_dir()
    df = pd.read_parquet(path)
    manifest = read_parquet_segment_manifest(path)
    assert df["value"].tolist() == [0, 1, 2, 3, 4]
    assert manifest is not None
    assert [item["row_count"] for item in manifest["completed_segments"]] == [2, 2, 1]
    assert manifest["incomplete_segments"] == []


async def test_streaming_parquet_sink_manifest_tracks_incomplete_segment(
    tmp_path,
) -> None:
    path = tmp_path / "metrics.parquet"
    schema = pa.schema([("value", pa.int64())])

    async with StreamingParquetSink(
        path,
        schema,
        flush_interval=1,
        segment_rows=10,
    ) as sink:
        await sink.write({"value": 1})
        manifest = read_parquet_segment_manifest(path)
        assert manifest is not None
        assert manifest["completed_segments"] == []
        assert manifest["incomplete_segments"][0]["row_count"] == 1

    closed_manifest = read_parquet_segment_manifest(path)
    assert closed_manifest is not None
    assert closed_manifest["status"] == "closed"
    assert closed_manifest["completed_segments"][0]["row_count"] == 1
    assert closed_manifest["incomplete_segments"] == []


async def test_streaming_parquet_sink_empty_segment_preserves_schema(tmp_path) -> None:
    path = tmp_path / "empty.parquet"
    schema = pa.schema([("token_id", pa.string()), ("mid", pa.float64())])

    async with StreamingParquetSink(path, schema, segment_rows=2):
        pass

    df = pd.read_parquet(path)
    assert df.empty
    assert df.columns.tolist() == ["token_id", "mid"]
