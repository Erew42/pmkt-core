use crate::engine_error;
use pmkt_engine::{
    ScaledDecimal, Venue,
    feed::{self, FeedTimeouts, Observation, PublicBook, PublicBookHealth},
    health::Flags,
};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyDict, PyTuple},
};
use std::collections::{HashMap, VecDeque};

fn flags(values: &Flags) -> Vec<&'static str> {
    values.iter().map(|flag| flag.as_str()).collect()
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
    result.set_item("quality_flags", flags(&value.quality_flags))?;
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
    result.set_item("quality_flags", flags(&value.quality_flags))?;
    result.set_item("metadata_revision", value.metadata_revision)?;
    Ok(result)
}

fn observation(py: Python<'_>, value: Observation) -> PyResult<Bound<'_, PyDict>> {
    let result = PyDict::new(py);
    result.set_item("book", value.book.map(|b| book(py, b)).transpose()?)?;
    result.set_item("health", health(py, value.health)?)?;
    Ok(result)
}

// Most levels survive a delta. Share only their immutable Python representation.
// Evict one old pair per miss to bound both memory and work on each read.
#[derive(Default)]
struct DecimalLevels {
    pairs: HashMap<(ScaledDecimal, ScaledDecimal), Py<PyTuple>>,
    insertion_order: VecDeque<(ScaledDecimal, ScaledDecimal)>,
}

impl DecimalLevels {
    const CAPACITY: usize = 4096;

    fn side<'py>(
        &mut self,
        py: Python<'py>,
        decimal: &Bound<'py, PyAny>,
        levels: impl ExactSizeIterator<Item = (ScaledDecimal, ScaledDecimal)>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let mut result = Vec::with_capacity(levels.len());
        for pair in levels {
            if let Some(existing) = self.pairs.get(&pair) {
                result.push(existing.clone_ref(py));
                continue;
            }
            let price = decimal.call1((pair.0.to_string(),))?;
            let size = decimal.call1((pair.1.to_string(),))?;
            let level = (price, size).into_pyobject(py)?.unbind();
            if self.pairs.len() == Self::CAPACITY {
                let oldest = self.insertion_order.pop_front().expect("full cache");
                self.pairs.remove(&oldest);
            }
            self.insertion_order.push_back(pair);
            self.pairs.insert(pair, level.clone_ref(py));
            result.push(level);
        }
        PyTuple::new(py, result)
    }
}

/// Synchronous read-only shard state. The caller supplies transport and clocks.
#[pyclass(module = "pmkt._native")]
pub struct ShardFeed {
    shard: feed::ShardFeed,
    decimal: Py<PyAny>,
    decimal_levels: DecimalLevels,
}

#[pymethods]
impl ShardFeed {
    #[new]
    #[pyo3(signature = (venue, shard_id, instruments, *, liveness_ns=None, snapshot_ns=None, polymarket_socket_liveness=false))]
    fn new(
        py: Python<'_>,
        venue: &str,
        shard_id: String,
        instruments: Vec<String>,
        liveness_ns: Option<u64>,
        snapshot_ns: Option<u64>,
        polymarket_socket_liveness: bool,
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
        let mut shard =
            feed::ShardFeed::new(venue, shard_id, instruments, timeouts).map_err(engine_error)?;
        if polymarket_socket_liveness {
            shard = shard.with_polymarket_socket_liveness();
        }
        Ok(Self {
            shard,
            decimal: py.import("decimal")?.getattr("Decimal")?.unbind(),
            decimal_levels: DecimalLevels::default(),
        })
    }

    #[pyo3(signature = (generation, now_ns, *, command_id=None))]
    fn connected(&mut self, generation: u64, now_ns: u64, command_id: Option<i64>) -> PyResult<()> {
        self.shard
            .connected(generation, command_id, now_ns)
            .map_err(engine_error)
    }

    fn disconnected(&mut self, generation: u64, reason: &str) -> PyResult<()> {
        self.shard
            .disconnected(generation, reason)
            .map_err(engine_error)
    }

    fn heartbeat(&mut self, generation: u64, now_ns: u64) -> PyResult<()> {
        self.shard
            .heartbeat(generation, now_ns)
            .map_err(engine_error)
    }

    fn begin_requests(&mut self, generation: u64, now_ns: u64) -> PyResult<Vec<String>> {
        self.shard
            .begin_requests(generation, now_ns)
            .map_err(engine_error)
    }

    fn finish_requests(&mut self, generation: u64, ids: Vec<String>, now_ns: u64) -> PyResult<()> {
        self.shard
            .finish_requests(generation, &ids, now_ns)
            .map_err(engine_error)
    }

    fn poll_books(&mut self, now_ns: u64) {
        self.shard.poll_books(now_ns);
    }

    fn resolved(&self, id: &str) -> bool {
        self.shard.resolved(id)
    }

    fn transport(&self) -> Option<(u64, Option<i64>)> {
        self.shard.transport()
    }

    fn poll_transport(&mut self, now_ns: u64) -> Option<String> {
        self.shard.poll_transport(now_ns).map(str::to_owned)
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
                self.shard
                    .ingest_updates(shard_id, generation, frame, now_ns)
                    .map_err(engine_error)?,
            );
        }
        let observations = self
            .shard
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
        health(
            py,
            self.shard
                .health(instrument, now_ns)
                .map_err(engine_error)?,
        )
    }

    fn read_book<'py>(
        &mut self,
        py: Python<'py>,
        instrument: &str,
        now_ns: u64,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        let Some(value) = self
            .shard
            .read_book(instrument, now_ns)
            .map_err(engine_error)?
        else {
            return Ok(None);
        };
        let decimal = self.decimal.bind(py);
        let result = PyDict::new(py);
        result.set_item("market_id", value.book.market())?;
        result.set_item("generation", value.generation)?;
        result.set_item("bids", self.decimal_levels.side(py, decimal, value.bids())?)?;
        result.set_item("asks", self.decimal_levels.side(py, decimal, value.asks())?)?;
        result.set_item("received_monotonic_ns", value.received_monotonic_ns)?;
        result.set_item(
            "source_timestamp_json",
            value.source_timestamp.value.as_ref().map(|v| v.to_string()),
        )?;
        result.set_item("quality_flags", flags(value.book.flags()))?;
        result.set_item("metadata_revision", value.metadata_revision)?;
        Ok(Some(result))
    }

    fn observation<'py>(
        &mut self,
        py: Python<'py>,
        instrument: &str,
        now_ns: u64,
    ) -> PyResult<Bound<'py, PyDict>> {
        observation(
            py,
            self.shard
                .observation(instrument, now_ns)
                .map_err(engine_error)?,
        )
    }

    fn close(&mut self) {
        self.shard.close();
    }
}
