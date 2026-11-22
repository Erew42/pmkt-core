use crate::engine_error;
use pmkt_engine::{
    ScaledDecimal, Venue,
    feed::{self, FeedTimeouts, Observation, PublicBook, PublicBookHealth},
    health::Flags,
};
use pyo3::{exceptions::PyValueError, prelude::*, types::PyDict};

fn flags(values: Flags) -> Vec<&'static str> {
    values.into_iter().map(|flag| flag.as_str()).collect()
}

// Decimal strings retain every price level, including adjacent sub-float prices.
fn levels(values: Vec<(ScaledDecimal, ScaledDecimal)>) -> Vec<(String, String)> {
    values
        .into_iter()
        .map(|(p, q)| (p.to_string(), q.to_string()))
        .collect()
}

fn book(py: Python<'_>, value: PublicBook) -> PyResult<Bound<'_, PyDict>> {
    let result = PyDict::new(py);
    result.set_item("venue", value.venue.as_str())?;
    result.set_item("instrument", value.instrument)?;
    result.set_item("market_id", value.market_id)?;
    result.set_item("shard_id", value.shard_id)?;
    result.set_item("generation", value.generation)?;
    result.set_item("bids", levels(value.bids))?;
    result.set_item("asks", levels(value.asks))?;
    result.set_item("received_monotonic_ns", value.received_monotonic_ns)?;
    result.set_item(
        "source_timestamp_json",
        value.source_timestamp.value.map(|v| v.to_string()),
    )?;
    result.set_item(
        "source_timestamp_milliseconds",
        value.source_timestamp.milliseconds,
    )?;
    result.set_item("quality_flags", flags(value.quality_flags))?;
    result.set_item("metadata_revision", value.metadata_revision)?;
    result.set_item("tick_size", value.tick_size.map(|d| d.to_string()))?;
    result.set_item(
        "minimum_order_size",
        value.minimum_order_size.map(|d| d.to_string()),
    )?;
    Ok(result)
}

fn health(py: Python<'_>, value: PublicBookHealth) -> PyResult<Bound<'_, PyDict>> {
    let result = PyDict::new(py);
    result.set_item("venue", value.venue.as_str())?;
    result.set_item("instrument", value.instrument)?;
    result.set_item("shard_id", value.shard_id)?;
    result.set_item("generation", value.generation)?;
    result.set_item("connection_state", value.connection_state)?;
    result.set_item("usable", value.usable)?;
    result.set_item("reason", value.reason)?;
    result.set_item("liveness_age_ns", value.liveness_age_ns)?;
    result.set_item("book_update_age_ns", value.book_update_age_ns)?;
    result.set_item("reconnect_count", value.reconnect_count)?;
    result.set_item("error_count", value.error_count)?;
    result.set_item("last_error", value.last_error)?;
    result.set_item("quality_flags", flags(value.quality_flags))?;
    result.set_item("metadata_revision", value.metadata_revision)?;
    Ok(result)
}

fn observation(py: Python<'_>, value: Observation) -> PyResult<Bound<'_, PyDict>> {
    let result = PyDict::new(py);
    result.set_item("book", value.book.map(|b| book(py, b)).transpose()?)?;
    result.set_item("health", health(py, value.health)?)?;
    Ok(result)
}

/// Synchronous read-only shard state. The caller supplies transport and clocks.
#[pyclass(module = "pmkt._native")]
pub struct ShardFeed(feed::ShardFeed);

#[pymethods]
impl ShardFeed {
    #[new]
    #[pyo3(signature = (venue, shard_id, instruments, *, liveness_ns=None, snapshot_ns=None))]
    fn new(
        venue: &str,
        shard_id: String,
        instruments: Vec<String>,
        liveness_ns: Option<u64>,
        snapshot_ns: Option<u64>,
    ) -> PyResult<Self> {
        let venue = match venue {
            "polymarket" => Venue::Polymarket,
            "kalshi" => Venue::Kalshi,
            _ => return Err(PyValueError::new_err("unknown venue")),
        };
        let defaults = FeedTimeouts::default();
        let timeouts = FeedTimeouts {
            liveness_ns: liveness_ns.unwrap_or(defaults.liveness_ns),
            snapshot_ns: snapshot_ns.unwrap_or(defaults.snapshot_ns),
        };
        feed::ShardFeed::new(venue, shard_id, instruments, timeouts)
            .map(Self)
            .map_err(engine_error)
    }

    #[pyo3(signature = (generation, now_ns, *, command_id=None))]
    fn connected(&mut self, generation: u64, now_ns: u64, command_id: Option<i64>) -> PyResult<()> {
        self.0
            .connected(generation, command_id, now_ns)
            .map_err(engine_error)
    }

    fn disconnected(&mut self, generation: u64, reason: &str) -> PyResult<()> {
        self.0
            .disconnected(generation, reason)
            .map_err(engine_error)
    }

    fn heartbeat(&mut self, generation: u64, now_ns: u64) -> PyResult<()> {
        self.0.heartbeat(generation, now_ns).map_err(engine_error)
    }

    fn begin_requests(&mut self, generation: u64, now_ns: u64) -> PyResult<Vec<String>> {
        self.0
            .begin_requests(generation, now_ns)
            .map_err(engine_error)
    }

    fn finish_requests(&mut self, generation: u64, ids: Vec<String>, now_ns: u64) -> PyResult<()> {
        self.0
            .finish_requests(generation, &ids, now_ns)
            .map_err(engine_error)
    }

    fn poll_books(&mut self, now_ns: u64) {
        self.0.poll_books(now_ns);
    }

    fn resolved(&self, id: &str) -> bool {
        self.0.resolved(id)
    }

    fn transport(&self) -> Option<(u64, Option<i64>)> {
        self.0.transport()
    }

    #[pyo3(signature = (shard_id, generation, frame, now_ns, *, detach=true))]
    fn ingest<'py>(
        &mut self,
        py: Python<'py>,
        shard_id: &str,
        generation: u64,
        frame: &[u8],
        now_ns: u64,
        detach: bool,
    ) -> PyResult<Bound<'py, pyo3::types::PyList>> {
        if !detach {
            return pyo3::types::PyList::new(
                py,
                self.0
                    .ingest_updates(shard_id, generation, frame, now_ns)
                    .map_err(engine_error)?,
            );
        }
        let observations = self
            .0
            .ingest(shard_id, generation, frame, now_ns)
            .map_err(engine_error)?
            .into_iter()
            .map(|item| observation(py, item))
            .collect::<PyResult<Vec<_>>>()?;
        pyo3::types::PyList::new(py, observations)
    }

    fn health<'py>(
        &mut self,
        py: Python<'py>,
        instrument: &str,
        now_ns: u64,
    ) -> PyResult<Bound<'py, PyDict>> {
        health(py, self.0.health(instrument, now_ns).map_err(engine_error)?)
    }

    fn observation<'py>(
        &mut self,
        py: Python<'py>,
        instrument: &str,
        now_ns: u64,
    ) -> PyResult<Bound<'py, PyDict>> {
        observation(
            py,
            self.0
                .observation(instrument, now_ns)
                .map_err(engine_error)?,
        )
    }

    fn close(&mut self) {
        self.0.close();
    }
}
