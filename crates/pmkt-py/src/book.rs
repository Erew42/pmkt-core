use crate::{engine_error, venue};
use pmkt_engine::{
    Book, BookMode, Venue, VenueBooks,
    book::UpdateKind,
    health::{Flags, Integrity},
    wire::{Action, Event, Ladder, decode},
};
use pyo3::{prelude::*, types::PyDict};
use serde_json::Value;
use std::collections::BTreeMap;

pub(crate) fn json(text: &str) -> PyResult<Value> {
    serde_json::from_str(text).map_err(|error| engine_error(error.into()))
}

pub(crate) fn flags(values: &Flags) -> Vec<&'static str> {
    values.iter().map(|flag| flag.as_str()).collect()
}

pub(crate) fn parse_flags(values: Vec<String>) -> Flags {
    values
        .into_iter()
        .filter_map(|value| serde_json::from_value(Value::String(value)).ok())
        .collect()
}

fn levels(values: &Ladder) -> Vec<(String, String)> {
    values
        .iter()
        .map(|(p, q)| (p.to_string(), q.to_string()))
        .collect()
}

#[pyfunction]
pub fn book_integrity(initialized: bool, quality_flags: Vec<String>) -> bool {
    let count = quality_flags.len();
    let flags = parse_flags(quality_flags);
    flags.len() == count && Integrity::from_observation(initialized, &flags) == Integrity::Intact
}

#[pyclass(module = "pmkt._native")]
pub struct RecordingBooks(VenueBooks);

#[pymethods]
impl RecordingBooks {
    #[new]
    fn new(venue_name: &str, instruments: Vec<String>) -> PyResult<Self> {
        let mode = match venue(venue_name)? {
            Venue::Polymarket => BookMode::Polymarket,
            Venue::Kalshi => BookMode::kalshi(),
        };
        Ok(Self(VenueBooks::new(mode, instruments)))
    }

    fn reset(&mut self) {
        self.0.reset();
    }

    fn resolved(&self, instrument: &str) -> bool {
        self.0.resolved(instrument)
    }

    fn apply<'py>(
        &mut self,
        py: Python<'py>,
        message: &str,
        count: u64,
    ) -> PyResult<Vec<Bound<'py, PyDict>>> {
        self.0
            .apply(&json(message)?, count)
            .map_err(engine_error)?
            .into_iter()
            .map(|update| {
                let b = self.0.get(&update.instrument).expect("touched book");
                let (first, second) = b.native_levels();
                let result = PyDict::new(py);
                result.set_item("instrument", b.instrument())?;
                result.set_item("market", b.market())?;
                result.set_item("first", levels(first))?;
                result.set_item("second", levels(second))?;
                result.set_item("initialized", b.initialized())?;
                result.set_item("intact", b.integrity() == Integrity::Intact)?;
                result.set_item("valid", b.valid_state())?;
                result.set_item("flags", flags(b.flags()))?;
                result.set_item(
                    "timestamp_json",
                    b.timestamp().value.as_ref().map(Value::to_string),
                )?;
                result.set_item("milliseconds", b.timestamp().milliseconds)?;
                result.set_item("tick", b.tick_size().map(|d| d.to_string()))?;
                result.set_item("minimum", b.minimum_size().map(|d| d.to_string()))?;
                result.set_item("source_update", update.kind != UpdateKind::Invalidated)?;
                Ok(result)
            })
            .collect()
    }

    fn recovery_due(&mut self, count: u64, now_ns: u64) -> bool {
        self.0.recovery_due(count, now_ns)
    }

    fn complementary_due(&self, count: u64, now_ns: u64) -> bool {
        self.0.complementary.due(count, now_ns)
    }

    fn next_deadline_ns(&self) -> Option<u64> {
        self.0.complementary.next_deadline_ns()
    }
}

#[pyclass(module = "pmkt._native")]
pub struct BookCheck {
    mode: BookMode,
    books: BTreeMap<String, Book>,
}

#[pymethods]
impl BookCheck {
    #[new]
    fn new(venue_name: &str) -> PyResult<Self> {
        let mode = match venue(venue_name)? {
            Venue::Polymarket => BookMode::Polymarket,
            Venue::Kalshi => BookMode::kalshi(),
        };
        Ok(Self {
            mode,
            books: BTreeMap::new(),
        })
    }

    /// One row per checkpoint. None counts a first checkpoint with no baseline.
    fn apply(&mut self, message: &str) -> PyResult<Vec<(String, Option<usize>)>> {
        let value = json(message)?;
        let Ok(Event::Books(updates)) = decode(self.mode.venue(), &value) else {
            return Ok(Vec::new());
        };
        let mut comparisons = Vec::new();
        for update in updates {
            let book = self
                .books
                .entry(update.instrument.clone())
                .or_insert_with(|| Book::new(self.mode, &update.instrument));
            if let Action::Snapshot(snapshot) = &update.action {
                let (first, second) = book.native_levels();
                let difference = book.initialized().then(|| {
                    differing_levels(first, &snapshot.first.levels)
                        + differing_levels(second, &snapshot.second.levels)
                });
                comparisons.push((update.instrument.clone(), difference));
            }
            if update.action.changes_depth() {
                book.apply_update(&update);
            }
        }
        Ok(comparisons)
    }
}

fn differing_levels(rebuilt: &Ladder, stated: &Ladder) -> usize {
    rebuilt
        .iter()
        .filter(|(p, q)| stated.get(p) != Some(q))
        .count()
        + stated.keys().filter(|p| !rebuilt.contains_key(p)).count()
}
