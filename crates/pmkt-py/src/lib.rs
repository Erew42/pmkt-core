mod arrow;
mod capture;
mod feed;

use pyo3::{exceptions::PyValueError, prelude::*};

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
    module.add_function(wrap_pyfunction!(capture::inspect_raw_capture, module)?)?;
    module.add_function(wrap_pyfunction!(capture::reconstruct_raw_capture, module)?)?;
    module.add_function(wrap_pyfunction!(capture::merge_raw_captures, module)?)?;
    Ok(())
}
