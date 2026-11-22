use crate::{arrow::ArrowStream, engine_error};
use pmkt_engine::{
    capture::inspect_session,
    emit::{QuoteBatchBuilder, Version, reconstruct_session},
};
use pyo3::{exceptions::PyValueError, prelude::*, types::PyDict};
use std::path::PathBuf;

/// Verify the sealed chunks and return the session's counts, bounds and gaps.
#[pyfunction]
pub fn inspect_raw_capture(py: Python<'_>, path: PathBuf) -> PyResult<Bound<'_, PyDict>> {
    let report = py.detach(|| inspect_session(path)).map_err(engine_error)?;
    let result = PyDict::new(py);
    let mut gaps = Vec::new();
    for gap in report.chunk_gaps {
        let item = PyDict::new(py);
        item.set_item("missing_frame_count", gap.missing_frame_count())?;
        item.set_item("first_missing", gap.first_missing)?;
        item.set_item("last_missing", gap.last_missing)?;
        item.set_item("next_chunk", gap.next_chunk)?;
        item.set_item("previous_last_frame_seq", gap.previous_last_frame_seq)?;
        item.set_item("next_first_frame_seq", gap.next_first_frame_seq)?;
        gaps.push(item);
    }
    result.set_item("chunk_gaps", gaps)?;
    result.set_item("incomplete_files", report.incomplete_files)?;
    result.set_item("sealed_chunk_count", report.sealed_chunk_count)?;
    result.set_item("compressed_bytes", report.compressed_bytes)?;
    result.set_item("frame_count", report.frame_count)?;
    result.set_item("venue", report.venue.map(|v| v.as_str()))?;
    result.set_item("recording_id", report.recording_id)?;
    result.set_item("first_received_utc", report.first_received_utc)?;
    result.set_item("last_received_utc", report.last_received_utc)?;
    result.set_item("connection_generations", report.connection_generations)?;
    result.set_item("frame_type_counts", report.frame_type_counts)?;
    result.set_item("direction_counts", report.direction_counts)?;
    result.set_item("local_event_counts", report.local_event_counts)?;
    Ok(result)
}

/// Retain both tables' batches until verified EOF. target_rows controls batch size.
#[pyfunction]
#[pyo3(signature = (path, *, version=2, target_rows=8192))]
pub fn reconstruct_raw_capture(
    py: Python<'_>,
    path: PathBuf,
    version: u8,
    target_rows: usize,
) -> PyResult<Bound<'_, PyDict>> {
    let version = match version {
        1 => Version::V1,
        2 => Version::V2,
        _ => return Err(PyValueError::new_err("version must be 1 or 2")),
    };
    let (top, depth) = py
        .detach(|| -> pmkt_engine::Result<_> {
            let mut top = Vec::new();
            let mut depth = Vec::new();
            reconstruct_session(path, version, target_rows, |batches| {
                top.push(batches.topbook);
                depth.push(batches.depth);
                Ok(())
            })?;
            if top.is_empty() {
                let empty = QuoteBatchBuilder::new(version).finish()?;
                top.push(empty.topbook);
                depth.push(empty.depth);
            }
            Ok((top, depth))
        })
        .map_err(engine_error)?;
    let result = PyDict::new(py);
    result.set_item(
        "topbook_rows",
        top.iter().map(|b| b.num_rows()).sum::<usize>(),
    )?;
    result.set_item(
        "depth_rows",
        depth.iter().map(|b| b.num_rows()).sum::<usize>(),
    )?;
    result.set_item("topbook", Py::new(py, ArrowStream::new(top))?)?;
    result.set_item("depth", Py::new(py, ArrowStream::new(depth))?)?;
    Ok(result)
}
