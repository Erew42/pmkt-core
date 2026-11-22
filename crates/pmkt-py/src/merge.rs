use crate::{book::json, engine_error, venue};
use pmkt_engine::{
    Error,
    merge::{self, MergeOptions, MergeRun, Occurrence, Origin, Stream},
};
use pyo3::{prelude::*, types::PyIterator};

pyo3::create_exception!(_native, MergeError, pyo3::exceptions::PyValueError);

type Input = (
    (String, String),
    String,
    u64,
    u64,
    Option<usize>,
    i64,
    String,
    bool,
);
type Key = (String, String, Option<i64>, Option<i64>, Vec<u8>);

#[pyfunction]
pub fn message_key(venue_name: &str, message: &str) -> PyResult<Key> {
    let value = json(message)?;
    let venue = if value.is_object() {
        venue(venue_name)?
    } else {
        pmkt_engine::Venue::Polymarket
    };
    let key = merge::Key::new(venue, &value);
    Ok((key.kind, key.scope, key.venue_ms, key.sent, key.id.to_vec()))
}

#[pyfunction]
pub fn snapshot_class(kind: &str, reconnect: bool, message: &str) -> PyResult<&'static str> {
    Ok(merge::snapshot_class(kind, reconnect, &json(message)?))
}

#[pyfunction]
pub fn conflated_subscription(raw: &str) -> bool {
    merge::conflated_subscription(raw)
}

struct InputError(PyErr);
impl From<Error> for InputError {
    fn from(error: Error) -> Self {
        Self(MergeError::new_err(error.to_string()))
    }
}

struct Source(Py<PyIterator>);
impl Iterator for Source {
    type Item = Result<Occurrence, InputError>;

    fn next(&mut self) -> Option<Self::Item> {
        Python::attach(|py| {
            self.0.bind(py).clone().next().map(|item| {
                let (
                    (copy, connection),
                    session,
                    generation,
                    frame_seq,
                    position,
                    received_us,
                    message,
                    reconnect_snapshot,
                ) = item
                    .and_then(|item| item.extract::<Input>())
                    .map_err(InputError)?;
                Ok(Occurrence {
                    origin: Origin {
                        stream: Stream::new(copy, connection),
                        session,
                        generation,
                        frame_seq,
                        position,
                        received_us,
                        received_utc: String::new(),
                        recording_id: String::new(),
                        received_monotonic_ns: 0,
                        use_yes_price: true,
                        coverage_period: 0,
                        coverage_group: 0,
                        subscription: None,
                        book_command: None,
                    },
                    message: json(&message).map_err(InputError)?,
                    reconnect_snapshot,
                })
            })
        })
    }
}

#[pyclass(module = "pmkt._native")]
pub struct CopyMerge(MergeRun<Source>);

#[pymethods]
impl CopyMerge {
    #[new]
    fn new(
        venue_name: &str,
        sources: Vec<Py<PyIterator>>,
        lateness_ms: i64,
        untimed_match_ms: i64,
        retention_ms: i64,
        strict_order: bool,
        preferred_copies: Vec<String>,
    ) -> PyResult<Self> {
        let engine = merge::CopyMerge::new(
            venue(venue_name)?,
            MergeOptions {
                lateness_ms,
                untimed_match_ms,
                retention_ms,
                strict_order,
                preferred_copies,
            },
        )
        .map_err(engine_error)?;
        engine
            .stream(sources.into_iter().map(Source))
            .map(Self)
            .map_err(|e| e.0)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<String>> {
        self.0
            .next_message()
            .map_err(|e| e.0)?
            .map(|message| serde_json::to_string(&message))
            .transpose()
            .map_err(|error| engine_error(error.into()))
    }

    fn stats(&self) -> PyResult<String> {
        serde_json::to_string(self.0.stats()).map_err(|error| engine_error(error.into()))
    }
}
