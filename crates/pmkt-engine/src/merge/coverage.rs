//! Receive-time coverage of a single copy/connection/session. No book state lives here.
use super::{Occurrence, Origin, Stream};
use crate::{
    Error, Result, Venue,
    capture::{CaptureItem, RawFrame},
    health::Sequences,
    wire,
};
use chrono::DateTime;
use serde_json::Value;
use std::collections::BTreeSet;

#[derive(serde::Serialize, Clone, Copy, Debug, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum StartReason {
    SessionStart,
    NewGeneration,
    AfterGap,
}
#[derive(serde::Serialize, Clone, Copy, Debug, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EndReason {
    NewGeneration,
    TransportError,
    DiskPauseGap,
    ChunkGap,
    #[serde(rename = "seq_gap")]
    SequenceGap,
    SessionEnd,
}
#[derive(serde::Serialize, Clone, Debug)]
pub struct Interval {
    pub generation: u64,
    pub start_us: i64,
    pub end_us: i64,
    pub start_reason: StartReason,
    pub end_reason: EndReason,
}
#[derive(Debug)]
enum Window {
    Closed(StartReason),
    Open {
        start: i64,
        last: i64,
        before: Option<i64>,
        reason: StartReason,
    },
}
#[derive(serde::Serialize, Default, Debug)]
pub struct CoverageCounts {
    pub chunk_gaps: u64,
    pub sequence_gaps: u64,
    pub seq_gap_markers: u64,
    pub disk_pause_markers: u64,
    pub transport_errors: u64,
    pub unparsed_text_frames: u64,
}
#[derive(serde::Serialize, Debug)]
pub struct SessionCoverage {
    pub stream: Stream,
    pub session: String,
    pub intervals: Vec<Interval>,
    pub counts: CoverageCounts,
}
#[derive(Debug)]
pub struct CoverageTracker {
    window: Window,
    generation: Option<u64>,
    frame_us: Option<i64>,
    seqs: Sequences,
    report: SessionCoverage,
}
impl CoverageTracker {
    pub fn new(stream: Stream, session: impl Into<String>) -> Self {
        Self {
            window: Window::Closed(StartReason::SessionStart),
            generation: None,
            frame_us: None,
            seqs: Sequences::default(),
            report: SessionCoverage {
                stream,
                session: session.into(),
                intervals: vec![],
                counts: CoverageCounts::default(),
            },
        }
    }
    pub fn frame(&mut self, us: i64, generation: u64, kind: &str, direction: &str) {
        if self.generation != Some(generation) {
            if self.generation.is_some() {
                self.end(EndReason::NewGeneration, None);
                self.window = Window::Closed(StartReason::NewGeneration);
            }
            self.generation = Some(generation);
            self.seqs.clear();
        }
        if direction == "local" {
            match kind {
                "transport_error" => {
                    self.report.counts.transport_errors += 1;
                    self.end(EndReason::TransportError, None);
                }
                "disk_pause_gap" => {
                    self.report.counts.disk_pause_markers += 1;
                    self.end(EndReason::DiskPauseGap, None);
                    self.window = Window::Closed(StartReason::AfterGap);
                    self.seqs.clear();
                }
                "seq_gap" => self.report.counts.seq_gap_markers += 1,
                _ => {}
            }
        } else if direction.starts_with("received") {
            self.frame_us = Some(us);
            match &mut self.window {
                Window::Closed(reason) => {
                    self.window = Window::Open {
                        start: us,
                        last: us,
                        before: None,
                        reason: *reason,
                    }
                }
                Window::Open { last, before, .. } => {
                    *before = Some(*last);
                    *last = (*last).max(us);
                }
            }
        }
    }
    pub fn sequence(&mut self, sid: i64, seq: i64) {
        if self.seqs.gap(sid, seq) {
            self.report.counts.sequence_gaps += 1;
            if let (Window::Open { before, start, .. }, Some(us)) = (&self.window, self.frame_us) {
                self.end(EndReason::SequenceGap, Some(before.unwrap_or(*start)));
                self.window = Window::Open {
                    start: us,
                    last: us,
                    before: None,
                    reason: StartReason::AfterGap,
                };
            }
        }
    }
    pub fn chunk_gap(&mut self) {
        self.report.counts.chunk_gaps += 1;
        self.end(EndReason::ChunkGap, None);
        self.window = Window::Closed(StartReason::AfterGap);
        self.seqs.clear();
    }
    fn end(&mut self, reason: EndReason, end: Option<i64>) {
        if let Window::Open {
            start,
            last,
            reason: start_reason,
            ..
        } = self.window
        {
            self.report.intervals.push(Interval {
                generation: self.generation.unwrap(),
                start_us: start,
                end_us: end.unwrap_or(last),
                start_reason,
                end_reason: reason,
            });
            self.window = Window::Closed(StartReason::AfterGap);
        }
    }
    pub fn finish(mut self) -> SessionCoverage {
        self.end(EndReason::SessionEnd, None);
        self.report
    }
}

pub fn uncovered_intervals(
    intervals: impl IntoIterator<Item = (i64, i64)>,
    start: i64,
    end: i64,
) -> Result<Vec<(i64, i64)>> {
    if end < start {
        return Err(Error::Invalid("end precedes start".into()));
    }
    let mut spans: Vec<_> = intervals.into_iter().collect();
    spans.sort_unstable();
    let mut cursor = start;
    let mut holes = vec![];
    for (begin, finish) in spans {
        if cursor >= end {
            break;
        }
        if begin > cursor {
            holes.push((cursor, begin.min(end)));
        }
        cursor = cursor.max(finish);
    }
    if cursor < end {
        holes.push((cursor, end));
    }
    Ok(holes)
}
pub(crate) fn received_us(s: &str) -> Result<i64> {
    DateTime::parse_from_rfc3339(s)
        .map(|v| v.timestamp_micros())
        .map_err(|e| Error::Invalid(format!("invalid receive timestamp {s}: {e}")))
}
fn conflated(raw: &str) -> bool {
    let Ok(v) = serde_json::from_str::<Value>(raw) else {
        return raw.contains("update_interval_ms");
    };
    match &v["params"]["update_interval_ms"] {
        Value::Null | Value::Bool(false) => false,
        Value::Number(n) => {
            n.as_str().parse::<crate::ScaledDecimal>().ok() != Some(crate::ScaledDecimal::ZERO)
        }
        _ => true,
    }
}
pub struct SessionOccurrences<I> {
    source: I,
    tracker: CoverageTracker,
    generation: Option<u64>,
    books: BTreeSet<String>,
    use_yes_price: bool,
    pending: std::vec::IntoIter<Occurrence>,
    done: bool,
}
impl<I: Iterator<Item = Result<CaptureItem>>> SessionOccurrences<I> {
    pub fn new(stream: Stream, session: impl Into<String>, source: I) -> Self {
        Self {
            source,
            tracker: CoverageTracker::new(stream, session),
            generation: None,
            books: BTreeSet::new(),
            use_yes_price: true,
            pending: vec![].into_iter(),
            done: false,
        }
    }
    pub fn finish(self) -> SessionCoverage {
        self.tracker.finish()
    }
    fn frame(&mut self, f: RawFrame) -> Result<()> {
        let us = received_us(&f.received_utc)?;
        if self.generation != Some(f.connection_generation) || f.frame_type == "subscription" {
            self.books.clear();
            self.generation = Some(f.connection_generation);
        }
        if f.frame_type == "subscription" && f.venue == Venue::Kalshi && conflated(&f.payload_raw) {
            return Err(Error::Invalid(
                "conflated Kalshi subscription (update_interval_ms)".into(),
            ));
        }
        if f.venue == Venue::Kalshi
            && f.frame_type == "subscription"
            && f.direction == "sent"
            && let Ok(command) = serde_json::from_str::<Value>(&f.payload_raw)
            && let Some(basis) = wire::kalshi::subscription_basis(&command)?
        {
            self.use_yes_price = basis;
        }
        self.tracker
            .frame(us, f.connection_generation, &f.frame_type, &f.direction);
        if f.direction != "received" || f.frame_type != "text" {
            return Ok(());
        }
        let Ok(messages) = wire::messages(f.payload_raw.as_bytes()) else {
            self.tracker.report.counts.unparsed_text_frames += 1;
            return Ok(());
        };
        let mut events = Vec::with_capacity(messages.len());
        for m in messages {
            let v = m.value;
            let asset = match f.venue {
                Venue::Kalshi => {
                    if let Some((sid, seq)) = v["sid"].as_i64().zip(v["seq"].as_i64()) {
                        self.tracker.sequence(sid, seq);
                    }
                    (v["type"] == "orderbook_snapshot" && v["msg"].is_object())
                        .then(|| &v["msg"]["market_ticker"])
                }
                Venue::Polymarket => (v["event_type"] == "book").then(|| &v["asset_id"]),
            };
            let reconnect_snapshot = asset.is_some_and(|a| self.books.insert(a.to_string()));
            events.push(Occurrence {
                origin: Origin {
                    stream: self.tracker.report.stream.clone(),
                    session: self.tracker.report.session.clone(),
                    generation: f.connection_generation,
                    frame_seq: f.local_frame_seq,
                    position: m.position,
                    received_us: us,
                    received_utc: f.received_utc.clone(),
                    recording_id: f.recording_id.clone(),
                    received_monotonic_ns: f.received_monotonic_ns,
                    use_yes_price: self.use_yes_price,
                },
                message: v,
                reconnect_snapshot,
            });
        }
        self.pending = events.into_iter();
        Ok(())
    }
    fn step(&mut self) -> Result<Option<Occurrence>> {
        loop {
            if let Some(o) = self.pending.next() {
                return Ok(Some(o));
            }
            match self.source.next().transpose()? {
                None => return Ok(None),
                Some(CaptureItem::Gap(_)) => self.tracker.chunk_gap(),
                Some(CaptureItem::Frame(f)) => self.frame(f)?,
            }
        }
    }
}
impl<I: Iterator<Item = Result<CaptureItem>>> Iterator for SessionOccurrences<I> {
    type Item = Result<Occurrence>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let result = self.step();
        self.done = !matches!(result, Ok(Some(_)));
        result.transpose()
    }
}
