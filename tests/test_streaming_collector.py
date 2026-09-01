import pandas as pd
import pyarrow as pa
import pytest

from pmkt.streaming.collector import StreamDatasetSpec, StreamRunOutputs


pytestmark = pytest.mark.asyncio


def _specs():
    return (
        StreamDatasetSpec(
            "events_parquet",
            "events.parquet",
            pa.schema([("sequence", pa.int64())]),
        ),
        StreamDatasetSpec(
            "topbook_v1_parquet",
            "topbook_v1.parquet",
            pa.schema([("schema_version", pa.string()), ("instrument_id", pa.string())]),
            manifest_schema_key="topbook",
            schema_version="topbook.v1",
        ),
    )


async def test_stream_run_outputs_opens_segmented_sink_set(tmp_path) -> None:
    outputs = StreamRunOutputs(
        run_dir=tmp_path / "run",
        datasets=_specs(),
        parquet_segment_rows=1,
        parquet_segment_seconds=None,
    )

    async with outputs.open_sinks() as sinks:
        await sinks["events_parquet"].write({"sequence": 1})
        await sinks["topbook_v1_parquet"].write(
            {"schema_version": "topbook.v1", "instrument_id": "token-1"}
        )

    assert pd.read_parquet(outputs.path("events_parquet"))["sequence"].tolist() == [1]
    topbook = pd.read_parquet(outputs.path("topbook_v1_parquet"))
    assert topbook["instrument_id"].tolist() == ["token-1"]
    assert outputs.dataset_paths == {
        "topbook_v1_parquet": str(tmp_path / "run" / "topbook_v1.parquet")
    }
    assert outputs.schema_versions == {"topbook": "topbook.v1"}
    assert set(outputs.parquet_segment_manifests()) == {
        "events_parquet",
        "topbook_v1_parquet",
    }


async def test_stream_run_outputs_rejects_bad_rotation_config(tmp_path) -> None:
    with pytest.raises(ValueError, match="parquet_segment_rows must be > 0"):
        StreamRunOutputs(
            run_dir=tmp_path / "run",
            datasets=_specs(),
            parquet_segment_rows=0,
            parquet_segment_seconds=None,
        )

    with pytest.raises(ValueError, match="parquet_segment_seconds must be > 0"):
        StreamRunOutputs(
            run_dir=tmp_path / "run",
            datasets=_specs(),
            parquet_segment_rows=None,
            parquet_segment_seconds=0,
        )
