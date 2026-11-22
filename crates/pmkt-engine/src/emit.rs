//! Public quote tables. Conversion to binary64 happens only at this output boundary.
mod reconstruct;
pub use reconstruct::{reconstruct_merged, reconstruct_session};

use crate::{
    Error, Result, ScaledDecimal as D, Venue,
    book::{Book, Quote},
    health::{Flags, Integrity},
    wire::SourceTime,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    builder::{ListBuilder, StringBuilder},
};
use arrow_schema::{Field, Schema};
use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Version {
    V1,
    V2,
}
impl Version {
    fn label(self, depth: bool) -> &'static str {
        match (self, depth) {
            (Self::V1, false) => "topbook.v1",
            (Self::V1, true) => "depth.v1",
            (Self::V2, false) => "topbook.v2",
            (Self::V2, true) => "depth.v2",
        }
    }
}
#[derive(Debug)]
pub struct QuoteBatches {
    pub topbook: RecordBatch,
    pub depth: RecordBatch,
}
/// Causal coordinates belong to the producer. The raw-event reference identifies a
/// single message (including its array position) in a received frame.
#[derive(Clone, Copy, Debug)]
pub struct Receive<'a> {
    pub run_id: Option<&'a str>,
    pub source: Option<&'a str>,
    pub utc: &'a str,
    pub monotonic_ns: Option<i64>,
    pub local_sequence: Option<i64>,
    pub raw_event_ref: Option<&'a str>,
    pub quote_age_ms: Option<i64>,
}

fn binary(value: D) -> f64 {
    value
        .to_string()
        .parse()
        .expect("representable finite decimal")
}
fn utc_text(dt: DateTime<Utc>) -> Option<String> {
    if !(1..=9999).contains(&dt.year()) {
        return None;
    }
    let nanos = dt.timestamp_subsec_nanos();
    let precision = if nanos == 0 {
        SecondsFormat::Secs
    } else if nanos.is_multiple_of(1000) {
        SecondsFormat::Micros
    } else {
        SecondsFormat::Nanos
    };
    Some(dt.to_rfc3339_opts(precision, false))
}
fn exchange_time(t: &SourceTime) -> Option<String> {
    let value = t.value.as_ref()?;
    if let Some(s) = value.as_str() {
        let mut s = s.trim().to_owned();
        if s.is_ascii() && s.len() >= 19 {
            if let Some(offset) = s[10..].rfind(['+', '-']) {
                match s.len() - (10 + offset) {
                    3 => s.push_str(":00"),
                    5 => s.insert(13 + offset, ':'),
                    _ => {}
                }
            }
            if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
                return utc_text(dt.to_utc());
            }
        }
    }
    let seconds = binary(D::from_json(value).ok()?) / if t.milliseconds { 1000.0 } else { 1.0 };
    // Match datetime.fromtimestamp: round the fractional second to microseconds.
    let whole = seconds.trunc() as i64;
    let fractional = ((seconds - seconds.trunc()) * 1_000_000.0).round_ties_even() as i64;
    utc_text(DateTime::from_timestamp_micros(
        whole.checked_mul(1_000_000)?.checked_add(fractional)?,
    )?)
}
struct Common {
    run: Option<String>,
    venue: Venue,
    market: String,
    source: Option<String>,
    utc: String,
    mono: Option<i64>,
    exchange_ts: Option<String>,
    local: Option<i64>,
    seq: Option<i64>,
    sid: Option<i64>,
    hash: Option<String>,
    raw_ref: Option<String>,
    age: Option<i64>,
    tick: Option<f64>,
    minimum: Option<f64>,
    flags: Flags,
    intact: bool,
    valid: bool,
}
struct Base {
    common: Arc<Common>,
    instrument: String,
    outcome: Option<&'static str>,
}
struct Top {
    base: Arc<Base>,
    bid: Option<Quote>,
    ask: Option<Quote>,
    bid_px: Option<f64>,
    ask_px: Option<f64>,
    mid: Option<f64>,
    spread: Option<f64>,
    bps: Option<f64>,
}
struct Depth {
    base: Arc<Base>,
    side: &'static str,
    level: i64,
    price: f64,
    size: f64,
    cumulative: f64,
}
pub struct QuoteBatchBuilder {
    version: Version,
    tops: Vec<Top>,
    depths: Vec<Depth>,
}
impl QuoteBatchBuilder {
    pub fn new(version: Version) -> Self {
        Self {
            version,
            tops: vec![],
            depths: vec![],
        }
    }
    pub fn row_count(&self) -> usize {
        self.tops.len() + self.depths.len()
    }
    /// Emit both Kalshi outcomes, with native depth, ordered as in canonical WS tables.
    pub fn push(&mut self, book: &Book, receive: Receive<'_>, include_depth: bool) -> Result<()> {
        let common = Arc::new(Common {
            run: receive.run_id.map(str::to_owned),
            venue: book.mode().venue(),
            market: book
                .market()
                .filter(|s| !s.is_empty())
                .unwrap_or(book.instrument())
                .into(),
            source: receive.source.map(str::to_owned),
            utc: receive.utc.into(),
            mono: receive.monotonic_ns,
            local: receive.local_sequence,
            exchange_ts: exchange_time(book.timestamp()),
            seq: book.sequence().seq,
            sid: book.sequence().sid,
            hash: book.venue_hash().map(str::to_owned),
            raw_ref: receive.raw_event_ref.map(str::to_owned),
            age: receive.quote_age_ms,
            tick: book.tick_size().map(binary),
            minimum: book.minimum_size().map(binary),
            flags: book.flags().clone(),
            intact: book.integrity() == Integrity::Intact,
            valid: book.valid_state(),
        });
        let (first, second) = book.native_levels();
        // A failed cumulative sum must leave the builder usable for the next observation.
        let mut tops = vec![];
        let mut depths = vec![];
        for q in book.quotes() {
            let base = Arc::new(Base {
                common: Arc::clone(&common),
                instrument: q.outcome.map_or_else(
                    || book.instrument().into(),
                    |o| format!("{}:{o}", book.instrument()),
                ),
                outcome: q.outcome,
            });
            let bid_px = q.bid.map(|q| binary(q.price));
            let ask_px = q.ask.map(|q| binary(q.price));
            let pair = bid_px.zip(ask_px);
            let mid = pair.map(|(b, a)| (a + b) / 2.0);
            let spread = pair.map(|(b, a)| a - b);
            let bps = spread
                .zip(mid.filter(|m| *m > 0.0))
                .map(|(s, m)| s / m * 10_000.0);
            tops.push(Top {
                base: base.clone(),
                bid: q.bid,
                ask: q.ask,
                bid_px,
                ask_px,
                mid,
                spread,
                bps,
            });
            if !include_depth || !book.initialized() {
                continue;
            }
            let mut append = |side, levels: &mut dyn Iterator<Item = (&D, &D)>| -> Result<()> {
                let mut total = D::ZERO;
                for (index, (&p, &qty)) in levels.enumerate() {
                    total = total
                        .checked_add(qty)
                        .map_err(|e| Error::Invalid(e.to_string()))?;
                    depths.push(Depth {
                        base: base.clone(),
                        side,
                        level: index as i64,
                        price: binary(p),
                        size: binary(qty),
                        cumulative: binary(total),
                    });
                }
                Ok(())
            };
            match q.outcome {
                Some("YES") => append("yes", &mut first.iter().rev())?,
                Some("NO") => append("no", &mut second.iter().rev())?,
                _ => {
                    append("bid", &mut first.iter().rev())?;
                    append("ask", &mut second.iter())?;
                }
            }
        }
        self.tops.extend(tops);
        self.depths.extend(depths);
        Ok(())
    }
    pub fn finish(self) -> Result<QuoteBatches> {
        Ok(QuoteBatches {
            topbook: top_batch(self.version, &self.tops)?,
            depth: depth_batch(self.version, &self.depths)?,
        })
    }
}

struct Columns {
    fields: Vec<Field>,
    arrays: Vec<ArrayRef>,
}
impl Columns {
    fn add(&mut self, name: &str, nullable: bool, array: impl Array + 'static) {
        self.fields
            .push(Field::new(name, array.data_type().clone(), nullable));
        self.arrays.push(Arc::new(array));
    }
    fn finish(self) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(self.fields)),
            self.arrays,
        )?)
    }
}
fn strings<'a>(v: impl Iterator<Item = Option<&'a str>>) -> StringArray {
    v.collect()
}
fn ints(v: impl Iterator<Item = Option<i64>>) -> Int64Array {
    v.collect()
}
fn floats(v: impl Iterator<Item = Option<f64>>) -> Float64Array {
    v.collect()
}
fn flags<'a>(values: impl Iterator<Item = &'a Flags>) -> arrow_array::ListArray {
    let mut list = ListBuilder::new(StringBuilder::new());
    for flags in values {
        for flag in flags {
            list.values().append_value(flag.as_str());
        }
        list.append(true);
    }
    list.finish()
}
fn prefix(version: Version, depth: bool, rows: &[&Base]) -> Columns {
    let mut c = Columns {
        fields: vec![],
        arrays: vec![],
    };
    c.add(
        "schema_version",
        false,
        strings(rows.iter().map(|_| Some(version.label(depth)))),
    );
    c.add(
        "collector_run_id",
        true,
        strings(rows.iter().map(|r| r.common.run.as_deref())),
    );
    c.add(
        "exchange",
        false,
        strings(rows.iter().map(|r| Some(r.common.venue.as_str()))),
    );
    c.add(
        "venue_market_id",
        true,
        strings(rows.iter().map(|r| Some(r.common.market.as_str()))),
    );
    c.add(
        "instrument_id",
        false,
        strings(rows.iter().map(|r| Some(r.instrument.as_str()))),
    );
    c.add("outcome", true, strings(rows.iter().map(|r| r.outcome)));
    c.add(
        "source",
        true,
        strings(rows.iter().map(|r| r.common.source.as_deref())),
    );
    c.add(
        "received_at_utc",
        false,
        strings(rows.iter().map(|r| Some(r.common.utc.as_str()))),
    );
    if !depth {
        c.add(
            "received_at_monotonic_ns",
            true,
            ints(rows.iter().map(|r| r.common.mono)),
        );
    }
    c.add(
        "exchange_ts_utc",
        true,
        strings(rows.iter().map(|r| r.common.exchange_ts.as_deref())),
    );
    c.add(
        "local_sequence",
        true,
        ints(rows.iter().map(|r| r.common.local)),
    );
    c.add(
        "venue_sequence",
        true,
        ints(rows.iter().map(|r| r.common.seq)),
    );
    c.add("venue_sid", true, ints(rows.iter().map(|r| r.common.sid)));
    c.add(
        "book_hash",
        true,
        strings(rows.iter().map(|r| r.common.hash.as_deref())),
    );
    c
}
fn quote_source(q: Option<Quote>) -> &'static str {
    match q {
        None => "missing",
        Some(q) if q.derived => "complement_derived",
        Some(_) => "direct",
    }
}
fn top_batch(version: Version, rows: &[Top]) -> Result<RecordBatch> {
    let mut c = prefix(
        version,
        false,
        &rows.iter().map(|r| r.base.as_ref()).collect::<Vec<_>>(),
    );
    c.add(
        "best_bid_dollars",
        true,
        floats(rows.iter().map(|r| r.bid_px)),
    );
    c.add(
        "best_ask_dollars",
        true,
        floats(rows.iter().map(|r| r.ask_px)),
    );
    c.add("mid_dollars", true, floats(rows.iter().map(|r| r.mid)));
    c.add(
        "spread_dollars",
        true,
        floats(rows.iter().map(|r| r.spread)),
    );
    c.add("spread_bps", true, floats(rows.iter().map(|r| r.bps)));
    c.add(
        "bid_size_contracts",
        true,
        floats(rows.iter().map(|r| r.bid.map(|q| binary(q.size)))),
    );
    c.add(
        "ask_size_contracts",
        true,
        floats(rows.iter().map(|r| r.ask.map(|q| binary(q.size)))),
    );
    c.add(
        "best_bid_source",
        true,
        strings(rows.iter().map(|r| Some(quote_source(r.bid)))),
    );
    c.add(
        "best_ask_source",
        true,
        strings(rows.iter().map(|r| Some(quote_source(r.ask)))),
    );
    c.add(
        "tick_size_dollars",
        true,
        floats(rows.iter().map(|r| r.base.common.tick)),
    );
    c.add(
        "min_order_size_contracts",
        true,
        floats(rows.iter().map(|r| r.base.common.minimum)),
    );
    c.add(
        "quote_age_ms",
        true,
        ints(rows.iter().map(|r| r.base.common.age)),
    );
    c.add(
        "valid_state",
        false,
        BooleanArray::from_iter(rows.iter().map(|r| Some(r.base.common.valid))),
    );
    c.add(
        "quality_flags",
        true,
        flags(rows.iter().map(|r| &r.base.common.flags)),
    );
    c.add(
        "raw_event_ref",
        true,
        strings(rows.iter().map(|r| r.base.common.raw_ref.as_deref())),
    );
    if version == Version::V2 {
        c.add(
            "book_integrity_valid",
            false,
            BooleanArray::from_iter(rows.iter().map(|r| Some(r.base.common.intact))),
        );
    }
    c.finish()
}
fn depth_batch(version: Version, rows: &[Depth]) -> Result<RecordBatch> {
    let mut c = prefix(
        version,
        true,
        &rows.iter().map(|r| r.base.as_ref()).collect::<Vec<_>>(),
    );
    c.add("side", false, strings(rows.iter().map(|r| Some(r.side))));
    c.add(
        "level_index",
        false,
        ints(rows.iter().map(|r| Some(r.level))),
    );
    c.add(
        "price_dollars",
        true,
        floats(rows.iter().map(|r| Some(r.price))),
    );
    c.add(
        "size_contracts",
        true,
        floats(rows.iter().map(|r| Some(r.size))),
    );
    c.add(
        "cumulative_size_contracts",
        true,
        floats(rows.iter().map(|r| Some(r.cumulative))),
    );
    c.add(
        "is_delta",
        true,
        BooleanArray::from_iter(rows.iter().map(|_| Some(false))),
    );
    c.add(
        "valid_state",
        false,
        BooleanArray::from_iter(rows.iter().map(|r| Some(r.base.common.valid))),
    );
    c.add(
        "quality_flags",
        true,
        flags(rows.iter().map(|r| &r.base.common.flags)),
    );
    if version == Version::V2 {
        c.add(
            "book_integrity_valid",
            false,
            BooleanArray::from_iter(rows.iter().map(|r| Some(r.base.common.intact))),
        );
    }
    c.finish()
}
