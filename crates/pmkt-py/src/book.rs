use crate::{engine_error, venue};
use pmkt_engine::{
    Book, BookMode, ScaledDecimal, Venue, VenueBooks,
    book::{BookState, QuotePolicy, UpdateKind},
    health::{Flags, Integrity},
    wire::{Action, Event, Ladder, Sequence, decode, snapshot_spelling},
};
use pyo3::{exceptions::PyValueError, prelude::*, types::PyDict};
use serde_json::Value;
use std::collections::BTreeMap;

pub(crate) fn json(text: &str) -> PyResult<Value> {
    serde_json::from_str(text).map_err(|error| engine_error(error.into()))
}

fn flags(values: &Flags) -> Vec<&'static str> {
    values.iter().map(|flag| flag.as_str()).collect()
}

fn parse_flags(values: Vec<String>) -> Flags {
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

fn decimal(value: Option<&str>) -> Option<ScaledDecimal> {
    value.and_then(|s| s.parse().ok())
}

#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub struct LocalState {
    initialized: bool,
    flags: Vec<String>,
    first: Vec<(String, String)>,
    second: Vec<(String, String)>,
    sid: Option<i64>,
    seq: Option<i64>,
    hash: Option<String>,
    tick: Option<String>,
}

fn ladder(values: Vec<(String, String)>) -> PyResult<Ladder> {
    values
        .into_iter()
        .map(|(p, q)| {
            let parse = |s: &str| {
                s.parse().map_err(|e: pmkt_engine::decimal::DecimalError| {
                    PyValueError::new_err(e.to_string())
                })
            };
            Ok((parse(&p)?, parse(&q)?))
        })
        .collect()
}

#[pyfunction]
pub fn book_integrity(initialized: bool, quality_flags: Vec<String>) -> bool {
    let count = quality_flags.len();
    let flags = parse_flags(quality_flags);
    flags.len() == count && Integrity::from_observation(initialized, &flags) == Integrity::Intact
}

/// Adapt the public mutable WS dataclasses without making a second state store.
#[pyfunction]
#[pyo3(signature = (venue_name, instrument, state, message, *, use_yes_price=true, quotes=None))]
pub fn update_local_book<'py>(
    py: Python<'py>,
    venue_name: &str,
    instrument: &str,
    state: LocalState,
    message: &str,
    use_yes_price: bool,
    quotes: Option<(Option<&str>, Option<&str>)>,
) -> PyResult<Bound<'py, PyDict>> {
    let venue = venue(venue_name)?;
    let mode = match venue {
        Venue::Polymarket => BookMode::Polymarket,
        Venue::Kalshi => BookMode::Kalshi {
            use_yes_price,
            policy: QuotePolicy::Current,
        },
    };
    let mut book = Book::new(mode, instrument);
    book.restore(BookState {
        levels: (ladder(state.first)?, ladder(state.second)?),
        initialized: state.initialized,
        flags: parse_flags(state.flags),
        sequence: Sequence {
            sid: state.sid,
            seq: state.seq,
        },
        hash: state.hash,
        tick: decimal(state.tick.as_deref()),
    });
    let message = json(message)?;
    let event = decode(venue, &message).map_err(engine_error)?;
    let mut snapshot = false;
    match event {
        Event::Books(updates) => {
            for update in updates {
                snapshot |= update.action.is_snapshot();
                book.apply_update(&update);
            }
        }
        Event::Lifecycle {
            market,
            timestamp,
            resolved,
            ..
        } => {
            book.apply_update(&pmkt_engine::wire::Update {
                instrument: instrument.into(),
                market,
                timestamp,
                sequence: None,
                action: Action::Lifecycle { resolved },
            });
        }
        _ => {}
    }
    let (bid, ask) = if let Some((bid, ask)) = quotes {
        let (mut bid, mut ask) = (decimal(bid), decimal(ask));
        if message["event_type"] == "best_bid_ask" {
            bid = ScaledDecimal::from_json(&message["best_bid"]).ok().or(bid);
            ask = ScaledDecimal::from_json(&message["best_ask"]).ok().or(ask);
        }
        if message["event_type"] != "market_resolved" {
            book.refresh_quotes(bid, ask);
        }
        (bid, ask)
    } else {
        (book.best_native_bid(), book.best_right().map(|q| q.price))
    };
    let (first, second) = book.native_levels();
    let [first, second] = if snapshot {
        snapshot_spelling(venue, &message).map(|side| {
            let spelling = |value: &Value| {
                value
                    .as_str()
                    .map_or_else(|| value.to_string(), str::to_owned)
            };
            side.into_values()
                .map(|(p, q)| (spelling(p), spelling(q)))
                .collect()
        })
    } else {
        [levels(first), levels(second)]
    };
    let result = PyDict::new(py);
    result.set_item("first", first)?;
    result.set_item("second", second)?;
    result.set_item("bid", bid.map(|p| p.to_string()))?;
    result.set_item("ask", ask.map(|p| p.to_string()))?;
    result.set_item("initialized", book.initialized())?;
    result.set_item("valid", book.valid_state())?;
    result.set_item("flags", flags(book.flags()))?;
    result.set_item("hash", book.venue_hash())?;
    result.set_item("tick", book.tick_size().map(|d| d.to_string()))?;
    result.set_item("sid", book.sequence().sid)?;
    result.set_item("seq", book.sequence().seq)?;
    Ok(result)
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
