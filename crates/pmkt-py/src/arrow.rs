use arrow_array::{RecordBatch, RecordBatchIterator, ffi_stream::FFI_ArrowArrayStream};
use pyo3::{exceptions::PyValueError, prelude::*, types::PyCapsule};

/// A single transfer of complete engine batches through the Arrow C Stream interface.
#[pyclass(module = "pmkt._native")]
pub struct ArrowStream {
    batches: Option<Vec<RecordBatch>>,
}

impl ArrowStream {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches: Some(batches),
        }
    }
}

fn export_batches(batches: Vec<RecordBatch>) -> FFI_ArrowArrayStream {
    // Reconstruction supplies a schema-bearing empty batch for an empty capture.
    let schema = batches[0].schema();
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    FFI_ArrowArrayStream::new(Box::new(reader))
}

#[pymethods]
impl ArrowStream {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &mut self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        // Schema conversion is optional in the protocol. Export the native schema.
        let _ = requested_schema;
        let batches = self
            .batches
            .take()
            .ok_or_else(|| PyValueError::new_err("stream already consumed"))?;
        // The capsule owns the stream. Arrow clears its release callback on transfer.
        PyCapsule::new(
            py,
            export_batches(batches),
            Some(c"arrow_array_stream".into()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array, ffi_stream::ArrowArrayStreamReader};
    use std::sync::Arc;

    #[test]
    fn c_stream_shares_the_engine_value_buffer() {
        let values = Int64Array::from(vec![11, 23]);
        let address = values.values().as_ptr();
        let batch = RecordBatch::try_from_iter([("v", Arc::new(values) as ArrayRef)]).unwrap();
        let stream = export_batches(vec![batch]);
        let mut reader = ArrowArrayStreamReader::try_new(stream).unwrap();
        let imported = reader.next().unwrap().unwrap();
        let array = imported
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(array.values().as_ptr(), address);
    }
}
