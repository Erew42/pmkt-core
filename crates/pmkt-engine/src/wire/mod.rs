//! Preserve the input spelling of JSON numbers and batch elements until normalization.
pub mod kalshi;
pub mod polymarket;

use crate::{Error, Result, Venue, decimal::ScaledDecimal as D};
use serde_json::{Value, value::RawValue};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

pub type Ladder = BTreeMap<D, D>;

/// Subscription evidence available up to this frame in one captured connection.
#[derive(Default)]
pub(crate) struct RecordedSubscription {
    pub instruments: Option<Arc<BTreeSet<String>>>,
    pub kalshi_basis: Option<bool>,
    /// Kalshi command id of the order-book subscription; other channels are separate commands.
    pub book_command: Option<i64>,
}
impl RecordedSubscription {
    pub fn record(&mut self, venue: Venue, command: &Value) -> Result<()> {
        let ids = match venue {
            Venue::Polymarket => command["assets_ids"].as_array(),
            Venue::Kalshi => {
                let Some(basis) = kalshi::subscription_basis(command)? else {
                    return Ok(());
                };
                self.kalshi_basis = Some(basis);
                self.book_command = command["id"].as_i64();
                command["params"]["market_tickers"].as_array()
            }
        };
        if let Some(ids) = ids {
            let instruments = Arc::make_mut(self.instruments.get_or_insert_with(Arc::default));
            instruments.extend(ids.iter().filter_map(Value::as_str).map(str::to_owned));
        }
        Ok(())
    }
}

/// An error names the command it rejects. A rejected side channel (trade, lifecycle)
/// leaves books intact; any other error means the book subscription is unreliable.
pub(crate) fn rejects_books(error: &Value, book_command: Option<i64>) -> bool {
    error["id"]
        .as_i64()
        .zip(book_command)
        .is_none_or(|(rejected, book)| rejected == book)
}

#[derive(Debug)]
pub struct WireMessage<'a> {
    pub text: &'a str,
    pub position: Option<usize>,
    pub value: Value,
}

/// Unlike live ingress this accepts JSON scalars. Raw capture consumers can distinguish
/// a valid null from unparsed text and keep an exact substring for each batch element.
pub fn messages(bytes: &[u8]) -> Result<Vec<WireMessage<'_>>> {
    let text = std::str::from_utf8(bytes).map_err(|e| Error::Invalid(e.to_string()))?;
    if text.trim_start().starts_with('[') {
        serde_json::from_str::<Vec<&RawValue>>(text)?
            .into_iter()
            .enumerate()
            .map(|(position, raw)| {
                Ok(WireMessage {
                    text: raw.get(),
                    position: Some(position),
                    value: serde_json::from_str(raw.get())?,
                })
            })
            .collect()
    } else {
        Ok(vec![WireMessage {
            text,
            position: None,
            value: serde_json::from_str(text)?,
        }])
    }
}

pub(crate) fn live_messages(bytes: &[u8]) -> Result<Vec<WireMessage<'_>>> {
    let trimmed = bytes.trim_ascii();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case(b"PING")
        || trimmed.eq_ignore_ascii_case(b"PONG")
    {
        return Ok(Vec::new());
    }
    let messages = messages(bytes)?;
    if messages.iter().any(|m| !m.value.is_object()) {
        return Err(Error::Invalid("malformed_frame".into()));
    }
    Ok(messages)
}

pub(crate) fn text(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}
pub(crate) fn first<'a>(object: &'a Value, names: &[&str]) -> Option<&'a Value> {
    names.iter().find_map(|key| object.get(key))
}
fn decimal(v: Option<&Value>) -> Option<D> {
    v.and_then(|v| D::from_json(v).ok())
}
fn price(v: Option<&Value>) -> Option<D> {
    decimal(v).filter(|d| d.is_probability())
}

#[derive(Clone, Debug)]
pub struct SnapshotSide {
    pub levels: Ladder,
    pub malformed: bool,
}
fn levels(value: Option<&Value>, allow_missing: bool) -> SnapshotSide {
    let mut levels = Ladder::new();
    let malformed = visit_levels(value, allow_missing, |price, size, _, _| {
        levels.insert(price, size);
    });
    SnapshotSide { levels, malformed }
}
fn visit_levels<'a>(
    value: Option<&'a Value>,
    allow_missing: bool,
    mut on_level: impl FnMut(D, D, &'a Value, &'a Value),
) -> bool {
    let Some(items) = value.and_then(Value::as_array) else {
        return !(allow_missing && value.is_none_or(Value::is_null));
    };
    let mut malformed = false;
    for level in items {
        let (p, q) = if let Some(pair) = level.as_array().filter(|a| a.len() == 2) {
            (pair.first(), pair.get(1))
        } else {
            (
                first(level, &["price", "price_dollars"]),
                first(level, &["size", "count", "count_fp"]),
            )
        };
        match (price(p), decimal(q)) {
            (Some(price), Some(size)) if size > D::ZERO => {
                on_level(
                    price,
                    size,
                    p.expect("parsed price"),
                    q.expect("parsed size"),
                );
            }
            (Some(_), Some(size)) if size == D::ZERO => {}
            _ => malformed = true,
        }
    }
    malformed
}

fn snapshot_sides(venue: Venue, body: &Value) -> [Option<&Value>; 2] {
    match venue {
        Venue::Polymarket => [body.get("bids"), body.get("asks")],
        Venue::Kalshi => [
            first(body, &["yes_dollars_fp", "yes_dollars", "yes"]),
            first(body, &["no_dollars_fp", "no_dollars", "no"]),
        ],
    }
}

type SnapshotSpelling<'a> = BTreeMap<D, (&'a Value, &'a Value)>;

/// Keep the legacy Decimal spelling of accepted snapshot levels at the Python boundary.
pub fn snapshot_spelling(venue: Venue, message: &Value) -> [SnapshotSpelling<'_>; 2] {
    let body = match venue {
        Venue::Polymarket => message,
        Venue::Kalshi => kalshi::body(message),
    };
    snapshot_sides(venue, body).map(|side| {
        let mut spelling = SnapshotSpelling::new();
        visit_levels(side, true, |price, _, p, q| {
            spelling
                .entry(price)
                .and_modify(|pair| pair.1 = q)
                .or_insert((p, q));
        });
        spelling
    })
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SourceTime {
    pub value: Option<Value>,
    pub milliseconds: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum Metadata {
    #[default]
    Absent,
    Value(D),
    Malformed,
}
impl Metadata {
    fn read(v: Option<&Value>) -> Self {
        if v.is_none_or(Value::is_null) {
            return Self::Absent;
        }
        decimal(v)
            .filter(|d| *d > D::ZERO)
            .map_or(Self::Malformed, Self::Value)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Sequence {
    pub sid: Option<i64>,
    pub seq: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub first: SnapshotSide,
    pub second: SnapshotSide,
    pub hash: Option<String>,
    pub tick: Metadata,
    pub minimum: Metadata,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    First,
    Second,
}

#[derive(Clone, Debug)]
pub struct Change {
    pub parent_timestamp: Option<String>,
    pub side: Option<Side>,
    pub price: Option<D>,
    pub size: Option<D>,
    pub previous_hash: Option<String>,
    pub hash: Option<String>,
    pub reported_bid: Option<D>,
    pub reported_ask: Option<D>,
    pub corrective_side: bool,
}

#[derive(Clone, Debug)]
pub enum Action {
    Snapshot(Snapshot),
    Absolute(Change),
    Relative {
        side: Option<Side>,
        price: Option<D>,
        delta: Option<D>,
    },
    Tick(Option<D>),
    Lifecycle {
        resolved: bool,
    },
}
impl Action {
    pub fn is_snapshot(&self) -> bool {
        matches!(self, Self::Snapshot(_))
    }
    pub fn changes_depth(&self) -> bool {
        matches!(
            self,
            Self::Snapshot(_) | Self::Absolute(_) | Self::Relative { .. }
        )
    }
}

#[derive(Clone, Debug)]
pub struct Update {
    pub instrument: String,
    pub market: Option<String>,
    pub timestamp: SourceTime,
    pub sequence: Option<Sequence>,
    pub action: Action,
}

#[derive(Debug)]
pub enum Event {
    Books(Vec<Update>),
    Lifecycle {
        market: Option<String>,
        instruments: Vec<String>,
        resolved: bool,
        timestamp: SourceTime,
    },
    Subscribed {
        command_id: Option<i64>,
        sid: Option<i64>,
        book_channel: bool,
    },
    SubscriptionError,
    Other,
}
pub fn decode(venue: Venue, value: &Value) -> Result<Event> {
    match venue {
        Venue::Polymarket => polymarket::decode(value),
        Venue::Kalshi => kalshi::decode(value),
    }
}
