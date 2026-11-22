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
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

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
    WsClose,
    ChunkGap,
    #[serde(rename = "seq_gap")]
    SequenceGap,
    SessionEnd,
}
impl EndReason {
    pub(crate) fn transport_boundary(kind: &str, direction: &str) -> Option<Self> {
        match (kind, direction) {
            ("ws_close", _) => Some(Self::WsClose),
            ("disk_pause_gap", "local") => Some(Self::DiskPauseGap),
            ("transport_error", "local") => Some(Self::TransportError),
            _ => None,
        }
    }
}
#[derive(serde::Serialize, Clone, Debug)]
pub struct Interval {
    pub generation: u64,
    pub start_us: i64,
    pub end_us: i64,
    pub start_reason: StartReason,
    pub end_reason: EndReason,
}
/// Coordinates in the verified, unpooled receipt coverage of one recording.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReceiptPeriod {
    pub(super) source: usize,
    pub(super) interval: Option<usize>,
}
impl ReceiptPeriod {
    pub(crate) fn bridges(self, sessions: &[SessionCoverage], received: &Origin) -> bool {
        let Some(id) = self.interval else {
            return false;
        };
        let Some(source) = sessions.get(self.source) else {
            return false;
        };
        let Ok(pos) = source.interval_ids.binary_search(&id) else {
            return false;
        };
        let Some(span) = source.intervals.get(pos) else {
            return false;
        };
        source.stream != received.stream
            && (span.start_us..=span.end_us).contains(&received.received_us)
    }
}
#[derive(Debug)]
enum Window {
    Closed(StartReason),
    Open {
        id: usize,
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
    #[serde(skip)]
    pub(crate) interval_ids: Vec<usize>,
    #[serde(skip)]
    pub(crate) interval_instruments: Vec<Option<Arc<BTreeSet<String>>>>,
    #[serde(skip)]
    pub(crate) interval_first_book_us: Vec<BTreeMap<String, i64>>,
    pub counts: CoverageCounts,
}
#[derive(Debug)]
pub struct CoverageTracker {
    window: Window,
    generation: Option<u64>,
    frame_us: Option<i64>,
    next_interval_id: usize,
    pending_book_from: Option<usize>,
    current_instruments: Option<Arc<BTreeSet<String>>>,
    current_first_book_us: BTreeMap<String, i64>,
    seqs: Sequences,
    report: SessionCoverage,
}
impl CoverageTracker {
    pub fn new(stream: Stream, session: impl Into<String>) -> Self {
        Self {
            window: Window::Closed(StartReason::SessionStart),
            generation: None,
            frame_us: None,
            next_interval_id: 0,
            pending_book_from: None,
            current_instruments: None,
            current_first_book_us: BTreeMap::new(),
            seqs: Sequences::default(),
            report: SessionCoverage {
                stream,
                session: session.into(),
                intervals: vec![],
                interval_ids: vec![],
                interval_instruments: vec![],
                interval_first_book_us: vec![],
                counts: CoverageCounts::default(),
            },
        }
    }
    fn open(&mut self, us: i64, reason: StartReason) {
        let id = self.next_interval_id;
        self.next_interval_id += 1;
        self.current_first_book_us.clear();
        self.window = Window::Open {
            id,
            start: us,
            last: us,
            before: None,
            reason,
        };
    }
    pub(crate) fn current_interval_id(&self) -> Option<usize> {
        match self.window {
            Window::Open { id, .. } => Some(id),
            Window::Closed(_) => None,
        }
    }
    pub(crate) fn set_instruments(&mut self, instruments: Option<Arc<BTreeSet<String>>>) {
        self.current_instruments = instruments;
    }
    pub(crate) fn note_book_observation(&mut self, instrument: &str, us: i64) {
        if matches!(self.window, Window::Open { .. })
            && !self.current_first_book_us.contains_key(instrument)
        {
            self.current_first_book_us.insert(instrument.to_owned(), us);
        }
    }
    pub(crate) fn record_boundary_counts(&mut self, kind: &str, direction: &str) {
        if let Some(reason) = EndReason::transport_boundary(kind, direction) {
            self.report.counts.transport_errors += u64::from(reason == EndReason::TransportError);
            self.report.counts.disk_pause_markers += u64::from(reason == EndReason::DiskPauseGap);
        } else if direction == "local" && kind == "seq_gap" {
            self.report.counts.seq_gap_markers += 1;
        }
    }
    pub(crate) fn begin_generation(&mut self, generation: u64) {
        if self.generation != Some(generation) {
            if self.generation.is_some() {
                self.end(EndReason::NewGeneration, None);
                self.window = Window::Closed(StartReason::NewGeneration);
            }
            self.generation = Some(generation);
            self.seqs.clear();
        }
        self.pending_book_from = None;
    }
    pub(crate) fn mark_book_pending(&mut self) {
        self.pending_book_from
            .get_or_insert(self.report.intervals.len());
    }
    pub(crate) fn confirm_books(&mut self) {
        self.pending_book_from = None;
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
        if direction.starts_with("received") {
            self.frame_us = Some(us);
            match &mut self.window {
                Window::Closed(reason) => {
                    let reason = *reason;
                    self.open(us, reason);
                }
                Window::Open { last, before, .. } => {
                    *before = Some(*last);
                    *last = (*last).max(us);
                }
            }
        }
        // A received close is evidence up to the close frame itself.
        if let Some(reason) = EndReason::transport_boundary(kind, direction) {
            self.report.counts.transport_errors += u64::from(reason == EndReason::TransportError);
            self.report.counts.disk_pause_markers += u64::from(reason == EndReason::DiskPauseGap);
            self.end(reason, None);
            if reason != EndReason::TransportError {
                self.window = Window::Closed(StartReason::AfterGap);
                self.seqs.clear();
            }
        } else if direction == "local" && kind == "seq_gap" {
            self.report.counts.seq_gap_markers += 1;
        }
    }
    pub fn sequence(&mut self, sid: i64, seq: i64) {
        if self.seqs.gap(sid, seq) {
            self.report.counts.sequence_gaps += 1;
            if let (Window::Open { before, start, .. }, Some(us)) = (&self.window, self.frame_us) {
                self.end(EndReason::SequenceGap, Some(before.unwrap_or(*start)));
                self.open(us, StartReason::AfterGap);
            }
        }
    }
    pub fn chunk_gap(&mut self) {
        self.report.counts.chunk_gaps += 1;
        self.end(EndReason::ChunkGap, None);
        self.window = Window::Closed(StartReason::AfterGap);
        self.seqs.clear();
    }
    pub(crate) fn reject_books(&mut self) {
        if let Some(from) = self.pending_book_from.take() {
            self.report.intervals.truncate(from);
            self.report.interval_ids.truncate(from);
            self.report.interval_instruments.truncate(from);
            self.report.interval_first_book_us.truncate(from);
        } else {
            self.end(EndReason::TransportError, None);
        }
        self.current_first_book_us.clear();
        self.window = Window::Closed(StartReason::AfterGap);
        self.seqs.clear();
    }
    fn end(&mut self, reason: EndReason, end: Option<i64>) {
        if let Window::Open {
            id,
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
            self.report.interval_ids.push(id);
            self.report
                .interval_instruments
                .push(self.current_instruments.clone());
            self.report
                .interval_first_book_us
                .push(std::mem::take(&mut self.current_first_book_us));
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
pub fn conflated_subscription(raw: &str) -> bool {
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
pub(crate) fn confirms_books(venue: Venue, value: &Value, book_command: Option<i64>) -> bool {
    if matches!(wire::decode(venue, value), Ok(wire::Event::Books(_))) {
        return true;
    }
    venue == Venue::Kalshi
        && value["type"] == "subscribed"
        && (value["msg"]["channel"] == "orderbook_delta"
            || value["id"]
                .as_i64()
                .zip(book_command)
                .is_some_and(|(ack, book)| ack == book))
}

/// Annotates occurrences with receipt coverage. Quote replay can observe this
/// evidence without using a coverage boundary as a book-state transition.
pub(crate) struct SessionEvidence {
    pub(crate) coverage_group: usize,
    tracker: CoverageTracker,
    generation: Option<u64>,
    last_received_us: Option<i64>,
    books: BTreeSet<String>,
}
impl SessionEvidence {
    pub(crate) fn new(stream: Stream, session: impl Into<String>) -> Self {
        Self {
            coverage_group: 0,
            tracker: CoverageTracker::new(stream, session),
            generation: None,
            last_received_us: None,
            books: BTreeSet::new(),
        }
    }
    pub(crate) fn finish(self) -> SessionCoverage {
        self.tracker.finish()
    }
    pub(crate) fn chunk_gap(&mut self) {
        self.tracker.chunk_gap();
    }
    pub(crate) fn begin_generation(&mut self, generation: u64) {
        self.tracker.begin_generation(generation);
    }
    pub(crate) fn set_instruments(&mut self, instruments: Option<Arc<BTreeSet<String>>>) {
        self.tracker.set_instruments(instruments);
    }
    pub(crate) fn mark_book_pending(&mut self) {
        self.tracker.mark_book_pending();
    }
    pub(crate) fn confirm_books(&mut self) {
        self.tracker.confirm_books();
    }
    pub(crate) fn frame(
        &mut self,
        f: &RawFrame,
        messages: Option<&[wire::WireMessage<'_>]>,
        record_coverage: bool,
        awaiting_book_confirm: bool,
    ) -> Result<()> {
        let us = received_us(&f.received_utc)?;
        if f.direction.starts_with("received") {
            self.last_received_us = Some(us);
        }
        if self.generation != Some(f.connection_generation) || f.frame_type == "subscription" {
            self.books.clear();
            self.generation = Some(f.connection_generation);
        }
        if !record_coverage {
            self.tracker.reject_books();
            return Ok(());
        }
        if awaiting_book_confirm {
            self.tracker
                .record_boundary_counts(&f.frame_type, &f.direction);
            return Ok(());
        }
        self.tracker
            .frame(us, f.connection_generation, &f.frame_type, &f.direction);
        let Some(messages) = messages else {
            self.tracker.report.counts.unparsed_text_frames += 1;
            return Ok(());
        };
        // CoverageTracker puts a sequence loss between the previous frame and
        // this one, even when a later array element exposes it. Assign the
        // same resulting local interval to every occurrence of this frame.
        if f.venue == Venue::Kalshi {
            for m in messages {
                if let Some((sid, seq)) = m.value["sid"].as_i64().zip(m.value["seq"].as_i64()) {
                    self.tracker.sequence(sid, seq);
                }
            }
        }
        for m in messages {
            if let Ok(wire::Event::Books(updates)) = wire::decode(f.venue, &m.value) {
                for u in updates {
                    self.tracker.note_book_observation(&u.instrument, us);
                }
            }
        }
        Ok(())
    }
    pub(crate) fn occurrence(
        &mut self,
        f: &RawFrame,
        subscription: &wire::RecordedSubscription,
        m: wire::WireMessage<'_>,
    ) -> Occurrence {
        let v = m.value;
        let asset = match f.venue {
            Venue::Kalshi => (v["type"] == "orderbook_snapshot" && v["msg"].is_object())
                .then(|| &v["msg"]["market_ticker"]),
            Venue::Polymarket => (v["event_type"] == "book").then(|| &v["asset_id"]),
        };
        let reconnect_snapshot = asset.is_some_and(|a| self.books.insert(a.to_string()));
        Occurrence {
            origin: Origin {
                stream: self.tracker.report.stream.clone(),
                session: self.tracker.report.session.clone(),
                generation: f.connection_generation,
                frame_seq: f.local_frame_seq,
                position: m.position,
                received_us: self.last_received_us.expect("received text frame"),
                received_utc: f.received_utc.clone(),
                recording_id: f.recording_id.clone(),
                received_monotonic_ns: f.received_monotonic_ns,
                use_yes_price: subscription.kalshi_basis.unwrap_or(true),
                subscription: subscription.instruments.clone(),
                book_command: subscription.book_command,
                coverage_period: self.tracker.current_interval_id(),
                coverage_group: self.coverage_group,
            },
            message: v,
            reconnect_snapshot,
        }
    }
}

pub struct SessionOccurrences<I> {
    pub(crate) evidence: SessionEvidence,
    source: I,
    subscription: wire::RecordedSubscription,
    observed_books: BTreeSet<String>,
    last_generation: Option<u64>,
    saw_subscription: bool,
    book_confirmed: bool,
    awaiting_book_confirm: bool,
    book_rejected: bool,
    pending: std::vec::IntoIter<Occurrence>,
    done: bool,
}
impl<I: Iterator<Item = Result<CaptureItem>>> SessionOccurrences<I> {
    pub fn new(stream: Stream, session: impl Into<String>, source: I) -> Self {
        Self {
            evidence: SessionEvidence::new(stream, session),
            source,
            subscription: wire::RecordedSubscription::default(),
            observed_books: BTreeSet::new(),
            last_generation: None,
            saw_subscription: false,
            book_confirmed: false,
            awaiting_book_confirm: false,
            book_rejected: false,
            pending: vec![].into_iter(),
            done: false,
        }
    }
    pub fn finish(self) -> SessionCoverage {
        self.evidence.finish()
    }
    pub(crate) fn subscription(&self) -> Option<std::sync::Arc<BTreeSet<String>>> {
        self.subscription.instruments.clone()
    }
    pub(crate) fn observed_books(&self) -> BTreeSet<String> {
        self.observed_books.clone()
    }
    pub(crate) fn side_only_subscription(&self) -> bool {
        self.saw_subscription && self.subscription.instruments.is_none()
    }
    fn step(&mut self) -> Result<Option<Occurrence>> {
        loop {
            if let Some(o) = self.pending.next() {
                return Ok(Some(o));
            }
            let Some(item) = self.source.next().transpose()? else {
                return Ok(None);
            };
            let CaptureItem::Frame(f) = item else {
                self.evidence.chunk_gap();
                continue;
            };
            if self.last_generation != Some(f.connection_generation) {
                self.last_generation = Some(f.connection_generation);
                self.book_rejected = false;
                self.book_confirmed = false;
                self.awaiting_book_confirm = false;
                self.evidence.begin_generation(f.connection_generation);
            }
            if f.frame_type == "subscription"
                && f.venue == Venue::Kalshi
                && conflated_subscription(&f.payload_raw)
            {
                return Err(Error::Invalid(
                    "conflated Kalshi subscription (update_interval_ms)".into(),
                ));
            }
            if f.frame_type == "subscription" && f.direction == "sent" {
                self.saw_subscription = true;
                if let Ok(command) = serde_json::from_str::<Value>(&f.payload_raw)
                    && self
                        .subscription
                        .record(f.venue, f.connection_generation, &command)?
                {
                    self.evidence
                        .set_instruments(self.subscription.instruments.clone());
                    self.book_rejected = false;
                    if !self.book_confirmed {
                        if f.venue == Venue::Kalshi {
                            self.awaiting_book_confirm = true;
                        } else {
                            self.evidence.mark_book_pending();
                        }
                    }
                }
            }
            let messages = if f.frame_type == "text" && f.direction == "received" {
                wire::messages(f.payload_raw.as_bytes()).ok()
            } else {
                Some(vec![])
            };
            if let Some(msgs) = messages.as_deref() {
                for m in msgs {
                    if confirms_books(f.venue, &m.value, self.subscription.book_command) {
                        self.book_confirmed = true;
                        self.awaiting_book_confirm = false;
                        self.evidence.confirm_books();
                    }
                    match wire::decode(f.venue, &m.value) {
                        Ok(wire::Event::SubscriptionError)
                            if wire::rejects_books(&m.value, self.subscription.book_command) =>
                        {
                            self.book_rejected = true;
                            self.awaiting_book_confirm = false;
                        }
                        Ok(wire::Event::Books(updates)) => {
                            for u in updates {
                                self.observed_books.insert(u.instrument);
                            }
                        }
                        _ => {}
                    }
                }
            }
            self.evidence.frame(
                &f,
                messages.as_deref(),
                !self.book_rejected,
                self.awaiting_book_confirm,
            )?;
            self.pending = messages
                .into_iter()
                .flatten()
                .map(|m| self.evidence.occurrence(&f, &self.subscription, m))
                .collect::<Vec<_>>()
                .into_iter();
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
