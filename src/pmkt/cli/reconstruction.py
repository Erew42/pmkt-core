from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path
from typing import Annotated, Mapping, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import typer

from pmkt.data.book_reconstruction import (
    BookTapeReconstructionResult,
)
from pmkt.data.book_reconstruction_streaming import (
    DEFAULT_RECONSTRUCTION_BATCH_ROWS,
    stream_reconstruct_book_tape,
)
from pmkt.data.registry import (
    DEPTH_SCHEMA_VERSION,
    TOPBOOK_SCHEMA_VERSION,
    arrow_schema,
    get_table_spec,
)
from pmkt.data.storage.parquet import write_parquet
from pmkt.data.validation import coerce_frame, validate_frame
from pmkt.streaming.durability import write_json_atomic_fsync

PARITY_MISMATCH_REPORT_SUFFIX = ".parity-mismatch.json"


def _parity_mismatch_report_path(destination: Path) -> Path:
    return destination.with_name(destination.name + PARITY_MISMATCH_REPORT_SUFFIX)



def publish_book_tape_reconstruction(
    result: BookTapeReconstructionResult,
    out_dir: str | Path,
) -> Mapping[str, str]:
    """Publish validated reconstruction outputs as one atomic directory."""
    if result.report.get("status") != "success":
        raise ValueError(
            "cannot publish reconstruction outputs with parity discrepancies"
        )
    destination = Path(out_dir).resolve()
    if destination.exists():
        raise FileExistsError(
            f"reconstruction output directory already exists: {destination}"
        )
    topbooks = coerce_frame(result.topbooks, TOPBOOK_SCHEMA_VERSION)
    depths = coerce_frame(result.depths, DEPTH_SCHEMA_VERSION)
    for frame, schema_version in (
        (topbooks, TOPBOOK_SCHEMA_VERSION),
        (depths, DEPTH_SCHEMA_VERSION),
    ):
        validation = validate_frame(
            frame,
            schema_version,
            strict=True,
        )
        if not validation.ok:
            raise ValueError("; ".join(validation.errors))

    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(
            prefix=f".{destination.name}.staging-",
            dir=destination.parent,
        )
    )
    output_names = {
        "topbook": "reconstructed_topbook.parquet",
        "depth": "reconstructed_depth.parquet",
        "report": "book_tape_reconstruction_report.v1.json",
    }
    final_outputs = {key: str(destination / name) for key, name in output_names.items()}
    try:
        write_parquet(
            topbooks,
            staging / output_names["topbook"],
            overwrite=False,
            schema=TOPBOOK_SCHEMA_VERSION,
            validate=True,
            strict=True,
        )
        write_parquet(
            depths,
            staging / output_names["depth"],
            overwrite=False,
            schema=DEPTH_SCHEMA_VERSION,
            validate=True,
            strict=True,
        )
        report = {
            **result.report,
            "publication": {
                "atomic_directory_publication": True,
                "destination": str(destination),
            },
            "outputs": final_outputs,
        }
        write_json_atomic_fsync(
            staging / output_names["report"],
            report,
        )
        staging.replace(destination)
    except BaseException:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
        raise
    return final_outputs


def publish_streamed_book_tape_reconstruction(
    manifest: str | Path,
    out_dir: str | Path,
    *,
    venue_book_id: str | None = None,
    batch_rows: int = DEFAULT_RECONSTRUCTION_BATCH_ROWS,
) -> Mapping[str, str]:
    """Reconstruct and publish bounded Arrow batches atomically."""
    destination = Path(out_dir).resolve()
    mismatch_report_path = _parity_mismatch_report_path(destination)
    if destination.exists():
        raise FileExistsError(
            f"reconstruction output directory already exists: {destination}"
        )
    if mismatch_report_path.exists():
        raise FileExistsError(
            "reconstruction parity-mismatch report already exists: "
            f"{mismatch_report_path}"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(
            prefix=f".{destination.name}.staging-",
            dir=destination.parent,
        )
    )
    output_names = {
        "topbook": "reconstructed_topbook.parquet",
        "depth": "reconstructed_depth.parquet",
        "report": "book_tape_reconstruction_report.v1.json",
    }
    final_outputs = {key: str(destination / name) for key, name in output_names.items()}
    stream = None
    writers: dict[str, pq.ParquetWriter] = {}
    schemas = {
        "topbook": arrow_schema(get_table_spec(TOPBOOK_SCHEMA_VERSION)),
        "depth": arrow_schema(get_table_spec(DEPTH_SCHEMA_VERSION)),
    }
    try:
        stream = stream_reconstruct_book_tape(
            manifest,
            venue_book_id=venue_book_id,
            batch_rows=batch_rows,
        )
        for batch in stream:
            for role, record_batch in (
                ("topbook", batch.topbooks),
                ("depth", batch.depths),
            ):
                if not len(record_batch):
                    continue
                writer = writers.get(role)
                if writer is None:
                    writer = pq.ParquetWriter(
                        staging / output_names[role],
                        schemas[role],
                    )
                    writers[role] = writer
                writer.write_batch(record_batch)
        for writer in writers.values():
            writer.close()
        writers.clear()
        for role in ("topbook", "depth"):
            path = staging / output_names[role]
            if not path.exists():
                schema = schemas[role]
                pq.write_table(
                    pa.Table.from_arrays(
                        [pa.array([], type=field.type) for field in schema],
                        schema=schema,
                    ),
                    path,
                )
        report = stream.report
        if report.get("status") != "success":
            mismatch_report = {
                **report,
                "publication": {
                    "atomic_directory_publication": False,
                    "destination": str(destination),
                    "incremental_parquet_writer": True,
                    "batch_rows": batch_rows,
                    "reason": "parity_mismatch",
                },
                "outputs": {},
            }
            write_json_atomic_fsync(mismatch_report_path, mismatch_report)
            raise ValueError(
                "cannot publish reconstruction outputs with parity discrepancies; "
                f"diagnostic report: {mismatch_report_path}"
            )
        published_report = {
            **report,
            "publication": {
                "atomic_directory_publication": True,
                "destination": str(destination),
                "incremental_parquet_writer": True,
                "batch_rows": batch_rows,
            },
            "outputs": final_outputs,
        }
        write_json_atomic_fsync(
            staging / output_names["report"],
            published_report,
        )
        staging.replace(destination)
    except BaseException:
        for writer in writers.values():
            writer.close()
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        if stream is not None:
            stream.close()
    return final_outputs


def reconstruct_book_tape_cmd(
    manifest: Annotated[
        Path,
        typer.Option(
            "--manifest",
            exists=True,
            dir_okay=False,
            resolve_path=True,
        ),
    ],
    out_dir: Annotated[Path, typer.Option("--out-dir")],
    venue_book_id: Annotated[
        Optional[str],
        typer.Option("--venue-book-id"),
    ] = None,
    batch_rows: Annotated[
        int,
        typer.Option("--batch-rows", min=1),
    ] = DEFAULT_RECONSTRUCTION_BATCH_ROWS,
) -> None:
    """Reconstruct committed book tape for research and audit use only."""
    outputs = publish_streamed_book_tape_reconstruction(
        manifest,
        out_dir,
        venue_book_id=venue_book_id,
        batch_rows=batch_rows,
    )
    typer.echo(json.dumps(outputs, sort_keys=True))


__all__ = [
    "publish_book_tape_reconstruction",
    "publish_streamed_book_tape_reconstruction",
    "reconstruct_book_tape_cmd",
]
