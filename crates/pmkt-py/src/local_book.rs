//! The public state objects expose writable Python dictionaries. Keep their depth there
//! and use the same single-price transition as the native managed and recording books.
use crate::book::{flags, parse_flags};
use pmkt_engine::{
    Book, BookMode, ScaledDecimal as D, Venue,
    book::{BookState, QuotePolicy},
    wire::{self, Action, Sequence, Side, kalshi, polymarket},
};
use pyo3::{
    exceptions::PyValueError,
    intern,
    prelude::*,
    sync::PyOnceLock,
    types::{PyBool, PyDict, PyFloat, PyInt, PyList, PySet, PyString, PyTuple, PyType},
};
use serde_json::Value;
use std::{cell::RefCell, collections::HashMap};

type Object<'py> = Bound<'py, PyAny>;
type Top<'py> = (Option<Object<'py>>, Option<Object<'py>>);

fn decimal_type(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    static DECIMAL: PyOnceLock<Py<PyType>> = PyOnceLock::new();
    DECIMAL.import(py, "decimal", "Decimal")
}

/// Reuse frequently repeated immutable prices and sizes. A Decimal computes its
/// hash on first lookup, so reusing a key also avoids rehashing it at every tick.
fn py_decimal(py: Python<'_>, value: D) -> PyResult<Object<'_>> {
    thread_local! {
        static VALUES: RefCell<HashMap<D, Py<PyAny>>> = RefCell::default();
    }
    VALUES.with(|cache| {
        if let Some(obj) = cache.borrow().get(&value) {
            return Ok(obj.clone_ref(py).into_bound(py));
        }
        let obj = decimal_type(py)?.call1((value.to_string(),))?;
        let mut cache = cache.borrow_mut();
        if cache.len() == 4096 {
            cache.clear();
        }
        cache.insert(value, obj.clone().unbind());
        Ok(obj)
    })
}

fn scaled(obj: &Object<'_>) -> Option<D> {
    if obj.is_none() {
        return None;
    }
    if let Ok(s) = obj.cast::<PyString>() {
        return s.to_str().ok()?.parse().ok();
    }
    obj.str().ok()?.to_str().ok()?.parse().ok()
}

fn field<'py>(dict: &Bound<'py, PyDict>, names: &[&str]) -> PyResult<Option<Object<'py>>> {
    for name in names {
        if let Some(v) = dict.get_item(*name)? {
            return Ok(Some(v));
        }
    }
    Ok(None)
}

fn text(obj: &Object<'_>) -> PyResult<Option<String>> {
    if obj.is_instance_of::<PyString>() {
        return obj.extract().map(Some);
    }
    if !obj.is_instance_of::<PyBool>()
        && (obj.is_instance_of::<PyInt>()
            || obj.is_instance_of::<PyFloat>()
            || obj.is_instance(decimal_type(obj.py())?)?)
    {
        let token = obj.str()?.to_str()?.to_owned();
        return Ok(Some(
            token
                .parse::<serde_json::Number>()
                .map_or_else(|_| token, |n| n.to_string()),
        ));
    }
    Ok(None)
}

fn previous_hash(dict: &Bound<'_, PyDict>) -> PyResult<Option<String>> {
    static KEYS: PyOnceLock<[Py<PyString>; 4]> = PyOnceLock::new();
    let py = dict.py();
    let keys = KEYS.get_or_init(py, || {
        polymarket::PREVIOUS_HASH_FIELDS.map(|name| PyString::intern(py, name).unbind())
    });
    for name in keys {
        if let Some(value) = dict.get_item(name.bind(py))?
            && let Some(hash) = text(&value)?
        {
            return Ok(Some(hash));
        }
    }
    Ok(None)
}

/// Snapshots carry arbitrarily nested pairs or objects. Convert once on full
/// replacement, preserving the spelling of Decimal and float tokens.
fn snapshot_value(obj: &Object<'_>, remaining_depth: usize) -> PyResult<Value> {
    // Match the JSON decoder's recursion bound for caller-supplied containers.
    let child_depth = remaining_depth
        .checked_sub(1)
        .ok_or_else(|| PyValueError::new_err("recursion limit exceeded"))?;
    if let Ok(dict) = obj.cast::<PyDict>() {
        return dict
            .iter()
            .map(|(k, v)| Ok((k.extract()?, snapshot_value(&v, child_depth)?)))
            .collect::<PyResult<_>>()
            .map(Value::Object);
    }
    if let Ok(list) = obj.cast::<PyList>() {
        return list
            .iter()
            .map(|v| snapshot_value(&v, child_depth))
            .collect::<PyResult<_>>()
            .map(Value::Array);
    }
    if let Ok(tuple) = obj.cast::<PyTuple>() {
        return tuple
            .iter()
            .map(|v| snapshot_value(&v, child_depth))
            .collect::<PyResult<_>>()
            .map(Value::Array);
    }
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if obj.is_instance_of::<PyBool>() {
        return obj.extract().map(Value::Bool);
    }
    if obj.is_instance_of::<PyString>() {
        return obj.extract().map(Value::String);
    }
    let token = obj.str()?.to_str()?.to_owned();
    Ok(token
        .parse()
        .map_or_else(|_| Value::String(token), Value::Number))
}

struct Ladders<'py> {
    first: Bound<'py, PyDict>,
    second: Bound<'py, PyDict>,
}

impl<'py> Ladders<'py> {
    fn load(state: &Object<'py>, venue: Venue) -> PyResult<Self> {
        let (a, b) = side_names(venue);
        Ok(Self {
            first: state.getattr(a)?.cast_into()?,
            second: state.getattr(b)?.cast_into()?,
        })
    }

    fn replace(state: &Object<'py>, venue: Venue, body: &Value) -> PyResult<Self> {
        let py = state.py();
        let decimal = decimal_type(py)?;
        let [first, second] = wire::snapshot_spelling(venue, body).map(|levels| -> PyResult<_> {
            let dict = PyDict::new(py);
            for (p, q) in levels.into_values() {
                let spelling = |value: &Value| {
                    value
                        .as_str()
                        .map_or_else(|| value.to_string(), str::to_owned)
                };
                dict.set_item(
                    decimal.call1((spelling(p),))?,
                    decimal.call1((spelling(q),))?,
                )?;
            }
            Ok(dict)
        });
        let (first, second) = (first?, second?);
        let (a, b) = side_names(venue);
        state.setattr(a, &first)?;
        state.setattr(b, &second)?;
        Ok(Self { first, second })
    }

    fn side(&self, side: Side) -> &Bound<'py, PyDict> {
        match side {
            Side::First => &self.first,
            Side::Second => &self.second,
        }
    }

    fn apply(&self, book: &mut Book, action: &Action, sequence: Option<Sequence>) -> PyResult<()> {
        let py = self.first.py();
        let prior = if let Some((side, p)) = action.relative_level() {
            self.side(side)
                .get_item(py_decimal(py, p)?)?
                .map(|q| scaled(&q).ok_or_else(|| PyValueError::new_err("invalid decimal")))
                .transpose()?
        } else {
            None
        };
        if let Some((side, p, q)) = book.observe_local(action, sequence, prior) {
            let side = self.side(side);
            let price = py_decimal(py, p)?;
            let current = side.get_item(&price)?;
            if let Some(q) = q {
                let size = py_decimal(py, q)?;
                if let Some(old) = current
                    && old.eq(&size)?
                {
                    return Ok(());
                }
                side.set_item(&price, size)?;
            } else if current.is_some() {
                side.del_item(&price)?;
            }
        }
        Ok(())
    }

    fn top(&self, use_min: bool) -> PyResult<Top<'py>> {
        static MIN: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
        static MAX: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
        let py = self.first.py();
        let max = MAX.import(py, "builtins", "max")?;
        let best = |side: &Bound<'py, PyDict>, func: &Object<'py>| {
            if side.is_empty() {
                Ok(None)
            } else {
                func.call1((side,)).map(Some)
            }
        };
        Ok((
            best(&self.first, max)?,
            best(
                &self.second,
                if use_min {
                    MIN.import(py, "builtins", "min")?
                } else {
                    max
                },
            )?,
        ))
    }
}

fn side_names(venue: Venue) -> (&'static str, &'static str) {
    match venue {
        Venue::Polymarket => ("bids", "asks"),
        Venue::Kalshi => ("yes_bids", "no_bids"),
    }
}

fn evidence(state: &Object<'_>) -> PyResult<BookState> {
    let py = state.py();
    let flags = state
        .getattr(intern!(py, "quality_flags"))?
        .cast_into::<PySet>()?;
    Ok(BookState {
        initialized: state
            .getattr(intern!(py, "initial_snapshot_received"))?
            .extract()?,
        flags: parse_flags(
            flags
                .iter()
                .map(|flag| flag.extract())
                .collect::<PyResult<_>>()?,
        ),
        ..Default::default()
    })
}

fn publish(state: &Object<'_>, book: &Book) -> PyResult<()> {
    let py = state.py();
    state.setattr(
        intern!(py, "quality_flags"),
        PySet::new(py, flags(book.flags()))?,
    )?;
    state.setattr(intern!(py, "initial_snapshot_received"), book.initialized())?;
    state.setattr(intern!(py, "valid_state"), book.valid_state())
}

#[pyfunction]
#[pyo3(signature=(state,event_type,message,change=None))]
pub fn update_market_book(
    state: &Object<'_>,
    event_type: &str,
    message: &Bound<'_, PyDict>,
    change: Option<&Bound<'_, PyDict>>,
) -> PyResult<()> {
    let py = state.py();
    let mut prior = evidence(state)?;
    prior.hash = state.getattr(intern!(py, "last_hash"))?.extract()?;
    let mut book = Book::from_state(BookMode::Polymarket, "", prior);
    let (bid, ask) = match event_type {
        "book" | "price_change" => {
            if !state.getattr(intern!(py, "asset_id"))?.is_truthy()? {
                return Err(PyValueError::new_err(if event_type == "book" {
                    "missing_book_identity"
                } else {
                    "malformed_price_changes"
                }));
            }
            let (ladders, action) = if event_type == "book" {
                let body = snapshot_value(message.as_any(), 128)?;
                (
                    Ladders::replace(state, Venue::Polymarket, &body)?,
                    Action::Snapshot(wire::snapshot(Venue::Polymarket, &body)),
                )
            } else {
                let change = change.ok_or_else(|| PyValueError::new_err("missing price change"))?;
                let side = change
                    .get_item(intern!(py, "side"))?
                    .and_then(|v| v.extract::<String>().ok())
                    .unwrap_or_default();
                let hash = change
                    .get_item(intern!(py, "hash"))?
                    .as_ref()
                    .map(text)
                    .transpose()?
                    .flatten();
                let prev = match previous_hash(change)? {
                    Some(v) => Some(v),
                    None => previous_hash(message)?,
                };
                let action = polymarket::absolute(
                    &side,
                    change
                        .get_item(intern!(py, "price"))?
                        .as_ref()
                        .and_then(scaled),
                    change
                        .get_item(intern!(py, "size"))?
                        .as_ref()
                        .and_then(scaled),
                    hash,
                    prev,
                );
                (
                    Ladders::load(state, Venue::Polymarket)?,
                    Action::Absolute(action),
                )
            };
            ladders.apply(&mut book, &action, None)?;
            let (b, a) = ladders.top(true)?;
            (b.as_ref().and_then(scaled), a.as_ref().and_then(scaled))
        }
        _ => {
            let mut bid = scaled(&state.getattr(intern!(py, "best_bid"))?);
            let mut ask = scaled(&state.getattr(intern!(py, "best_ask"))?);
            match event_type {
                "tick_size_change" => {
                    let tick = message.get_item("new_tick_size")?.as_ref().and_then(scaled);
                    book.observe_local(&Action::Tick(tick), None, None);
                    state.setattr(
                        "tick_size",
                        tick.map(|v| v.to_string().parse::<f64>().expect("finite decimal")),
                    )?;
                }
                "new_market" | "market_resolved" => {
                    book.observe_local(
                        &Action::Lifecycle {
                            resolved: event_type == "market_resolved",
                        },
                        None,
                        None,
                    );
                }
                "best_bid_ask" => {
                    bid = message
                        .get_item("best_bid")?
                        .as_ref()
                        .and_then(scaled)
                        .or(bid);
                    ask = message
                        .get_item("best_ask")?
                        .as_ref()
                        .and_then(scaled)
                        .or(ask);
                }
                _ => {}
            }
            (bid, ask)
        }
    };
    if event_type != "market_resolved" {
        book.refresh_quotes(bid, ask);
    }
    let convert = |v: Option<D>| v.map(|p| py_decimal(py, p)).transpose();
    state.setattr(intern!(py, "best_bid"), convert(bid)?)?;
    state.setattr(intern!(py, "best_ask"), convert(ask)?)?;
    state.setattr(intern!(py, "last_hash"), book.venue_hash())?;
    publish(state, &book)
}

#[pyfunction]
#[pyo3(signature=(state,event_type,body,sid,seq))]
pub fn update_kalshi_book<'py>(
    state: &Object<'py>,
    event_type: &str,
    body: &Bound<'py, PyDict>,
    sid: &Object<'_>,
    seq: &Object<'_>,
) -> PyResult<Top<'py>> {
    let py = state.py();
    let use_yes_price = state.getattr(intern!(py, "use_yes_price"))?.extract()?;
    let mut prior = evidence(state)?;
    prior.sequence = Sequence {
        sid: state.getattr(intern!(py, "sid"))?.extract()?,
        seq: state.getattr(intern!(py, "last_seq"))?.extract()?,
    };
    let mut book = Book::from_state(
        BookMode::Kalshi {
            use_yes_price,
            policy: QuotePolicy::Current,
        },
        "",
        prior,
    );
    let (ladders, action) = if event_type == "orderbook_snapshot" {
        let body = snapshot_value(body.as_any(), 128)?;
        (
            Ladders::replace(state, Venue::Kalshi, &body)?,
            Action::Snapshot(wire::snapshot(Venue::Kalshi, &body)),
        )
    } else {
        let side = body
            .get_item(intern!(py, "side"))?
            .and_then(|v| v.extract::<String>().ok())
            .unwrap_or_default();
        let action = kalshi::relative(
            &side,
            field(body, &kalshi::DELTA_PRICE_FIELDS)?
                .as_ref()
                .and_then(scaled),
            field(body, &kalshi::DELTA_SIZE_FIELDS)?
                .as_ref()
                .and_then(scaled),
        );
        (Ladders::load(state, Venue::Kalshi)?, action)
    };
    // The wire decoder reads JSON integer tokens with as_i64; an oversized
    // venue sequence supplies the same missing-sequence evidence as null.
    let sequence = Sequence {
        sid: sid.extract::<i64>().ok(),
        seq: seq.extract::<i64>().ok(),
    };
    ladders.apply(&mut book, &action, Some(sequence))?;
    let (bid, right) = ladders.top(use_yes_price)?;
    book.refresh_native_quotes(
        bid.as_ref().and_then(scaled),
        right.as_ref().and_then(scaled),
    );
    publish(state, &book)?;
    state.setattr(intern!(py, "sid"), book.sequence().sid)?;
    state.setattr(intern!(py, "last_seq"), book.sequence().seq)?;
    Ok((bid, right))
}
