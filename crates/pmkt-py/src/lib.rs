mod arrow;
mod book;
mod capture;
mod feed;
mod merge;

use pyo3::{exceptions::PyValueError, prelude::*};

fn venue(name: &str) -> PyResult<pmkt_engine::Venue> {
    match name {
        "polymarket" => Ok(pmkt_engine::Venue::Polymarket),
        "kalshi" => Ok(pmkt_engine::Venue::Kalshi),
        _ => Err(PyValueError::new_err(format!(
            "unsupported venue: {name:?}"
        ))),
    }
}

fn engine_error(error: pmkt_engine::Error) -> PyErr {
    match error {
        pmkt_engine::Error::Io(err) => err.into(),
        other => PyValueError::new_err(other.to_string()),
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<arrow::ArrowStream>()?;
    module.add_class::<feed::ShardFeed>()?;
    module.add_class::<book::RecordingBooks>()?;
    module.add_class::<book::BookCheck>()?;
    module.add_class::<merge::CopyMerge>()?;
    module.add("MergeError", module.py().get_type::<merge::MergeError>())?;
    module.add_function(wrap_pyfunction!(book::book_integrity, module)?)?;
    module.add_function(wrap_pyfunction!(book::update_local_book, module)?)?;
    module.add_function(wrap_pyfunction!(merge::message_key, module)?)?;
    module.add_function(wrap_pyfunction!(merge::snapshot_class, module)?)?;
    module.add_function(wrap_pyfunction!(merge::conflated_subscription, module)?)?;
    module.add_function(wrap_pyfunction!(capture::inspect_raw_capture, module)?)?;
    module.add_function(wrap_pyfunction!(capture::reconstruct_raw_capture, module)?)?;
    module.add_function(wrap_pyfunction!(capture::merge_raw_captures, module)?)?;
    Ok(())
}
