use super::{QuoteBatchBuilder, QuoteBatches, Receive, Version, utc_text};
use crate::{
    Error, Result, Venue,
    book::{BookMode, SequenceCheck, VenueBooks},
    capture::{CaptureItem, SealedChunk, SessionChunks, list_session_chunks},
    health::Integrity,
    merge::{
        CopyMerge, EndReason, MergeOptions, MergeStats, MergedMessage, SessionCoverage,
        SessionOccurrences, Stream, received_us, uncovered_intervals,
    },
    wire::{self, Event},
};
use chrono::DateTime;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};

// Pin the manifests once. A single chunk may have an archived predecessor.
enum Input {
    Chunk(SealedChunk),
    Session(SessionChunks),
}
type CaptureFrames = Box<dyn Iterator<Item = Result<CaptureItem>>>;
impl Input {
    fn open(path: &Path) -> Result<Self> {
        if path.is_file() {
            Ok(Self::Chunk(SealedChunk::open(path)?))
        } else {
            Ok(Self::Session(list_session_chunks(path)?))
        }
    }
    fn venue(&self) -> Option<Venue> {
        match self {
            Self::Chunk(chunk) => Some(chunk.manifest.venue),
            Self::Session(session) => session.chunks.first().map(|chunk| chunk.manifest.venue),
        }
    }
    fn frames(&self) -> Result<CaptureFrames> {
        match self {
            Self::Chunk(chunk) => Ok(Box::new(
                chunk.frames(true)?.map(|f| f.map(CaptureItem::Frame)),
            )),
            Self::Session(session) => Ok(Box::new(session.clone().read(true))),
        }
    }
}

struct Receipt<'a> {
    utc: &'a str,
    us: i64,
    monotonic_ns: u64,
    /// The clock for the corrective-delta deadline. A single session reads its own
    /// monotonic clock; a merged stream needs one clock shared by its recorders.
    clock_ns: u64,
    sequence: u64,
    recording_id: &'a str,
}

struct ReconstructDriver<F> {
    books: VenueBooks,
    count: u64,
    last_utc: BTreeMap<String, i64>,
    batch: QuoteBatchBuilder,
    target_rows: usize,
    emit: F,
}
impl<F: FnMut(QuoteBatches) -> Result<()>> ReconstructDriver<F> {
    fn new(venue: Venue, version: Version, target_rows: usize, emit: F) -> Self {
        let mode = match venue {
            Venue::Polymarket => BookMode::Polymarket,
            Venue::Kalshi => BookMode::kalshi(),
        };
        Self {
            books: VenueBooks::new(mode, std::iter::empty::<String>()),
            count: 0,
            last_utc: BTreeMap::new(),
            batch: QuoteBatchBuilder::new(version),
            target_rows,
            emit,
        }
    }
    fn message<'a>(
        &mut self,
        message: &Value,
        frame: &Receipt<'_>,
        position: Option<usize>,
        subscriptions: impl Iterator<Item = &'a BTreeSet<String>> + Clone,
        book_command: Option<i64>,
        sequence_check: SequenceCheck,
    ) -> Result<()> {
        self.count += 1;
        let venue = self.books.mode().venue();
        for id in self
            .books
            .complementary
            .expired_instruments(self.count, frame.clock_ns)
        {
            self.books.reset_book(&id);
        }
        let event = wire::decode(venue, message)?;
        if matches!(event, Event::SubscriptionError) {
            if wire::rejects_books(message, book_command) {
                self.books.reset();
            }
            return Ok(());
        }
        let discover = subscriptions.clone().next().is_none();
        let subscribed = |id: &str| subscriptions.clone().any(|ids| ids.contains(id));
        match &event {
            Event::Books(updates) => {
                for u in updates {
                    if (discover || subscribed(&u.instrument))
                        && self.books.get(&u.instrument).is_none()
                    {
                        self.books.subscribe(u.instrument.clone());
                    }
                }
            }
            Event::Lifecycle {
                instruments,
                resolved,
                ..
            } => {
                for id in instruments {
                    if subscribed(id) {
                        self.books.subscribe(id.clone());
                    } else if discover && *resolved {
                        // Broadcast lifecycle messages do not establish a book subscription.
                        // Keep terminal evidence if a later book identifies this asset.
                        self.books.remember_resolution(id);
                    }
                }
            }
            _ => {}
        }
        let sequence = i64::try_from(frame.sequence)
            .map_err(|_| Error::Invalid("frame sequence exceeds int64".into()))?;
        let mono = i64::try_from(frame.monotonic_ns)
            .map_err(|_| Error::Invalid("receive clock exceeds int64".into()))?;
        let mut reference = format!("{}:{sequence}", frame.recording_id);
        if let Some(i) = position {
            reference.push_str(&format!(":{i}"));
        }
        for item in self.books.apply_event(event, self.count, sequence_check) {
            let book = self.books.get(&item.instrument).expect("touched book");
            // The top and all depth rows of one observation share one adjusted clock.
            let adjusted;
            let (clock, utc) = match self.last_utc.get(&item.instrument) {
                Some(prev) if *prev >= frame.us => {
                    let clock = prev
                        .checked_add(1)
                        .ok_or_else(|| Error::Invalid("receive timestamp overflow".into()))?;
                    adjusted = DateTime::from_timestamp_micros(clock)
                        .and_then(utc_text)
                        .ok_or_else(|| Error::Invalid("receive timestamp out of range".into()))?;
                    (clock, adjusted.as_str())
                }
                _ => (frame.us, frame.utc),
            };
            self.last_utc.insert(item.instrument.clone(), clock);
            self.batch.push(
                book,
                Receive {
                    run_id: Some(frame.recording_id),
                    source: Some("ws"),
                    utc,
                    monotonic_ns: Some(mono),
                    local_sequence: Some(sequence),
                    raw_event_ref: Some(&reference),
                    quote_age_ms: None,
                },
                true,
            )?;
            if venue == Venue::Polymarket
                && book.integrity() == Integrity::Impaired
                && !self
                    .books
                    .complementary
                    .defer(&item.instrument, self.count, frame.clock_ns)
            {
                self.books.reset_book(&item.instrument);
            }
        }
        if self.batch.row_count() >= self.target_rows {
            let empty = QuoteBatchBuilder::new(self.batch.version);
            (self.emit)(std::mem::replace(&mut self.batch, empty).finish()?)?;
        }
        Ok(())
    }
    fn finish(mut self) -> Result<()> {
        if self.batch.row_count() > 0 {
            (self.emit)(self.batch.finish()?)?;
        }
        Ok(())
    }
}

/// Merge redundant sessions in venue order and keep the winning frame's original
/// causal coordinates. A receive-time hole breaks continuity only if no copy spans it.
pub fn reconstruct_merged(
    venue: Venue,
    mut sessions: Vec<(String, String, PathBuf)>,
    version: Version,
    target_rows: usize,
    emit: impl FnMut(QuoteBatches) -> Result<()>,
    mut on_message: impl FnMut(&MergedMessage) -> Result<()>,
) -> Result<(MergeStats, Vec<SessionCoverage>)> {
    if target_rows == 0 {
        return Err(Error::Invalid("batch row target must be positive".into()));
    }
    sessions.sort();
    let multiple = sessions.len() > 1;
    let mut streams = BTreeMap::<_, Vec<_>>::new();
    let mut coverage = Vec::new();
    for (copy, connection, path) in sessions {
        let input = Input::open(&path)?;
        if input.venue().is_some_and(|v| v != venue) {
            return Err(Error::Invalid(
                "cannot merge sessions from different venues".into(),
            ));
        }
        let session = match input {
            Input::Chunk(_) => path.parent().unwrap_or(&path),
            Input::Session(_) => &path,
        };
        let name = session.file_name().and_then(|p| p.to_str()).unwrap_or("");
        let stream = Stream::new(copy, connection);
        let occurrences = || -> Result<_> {
            Ok(SessionOccurrences::new(
                stream.clone(),
                name,
                input.frames()?,
            ))
        };
        // Venue ordering can emit before a different copy has finished proving
        // coverage. Scan the same sealed inputs first, retaining only intervals.
        if multiple {
            let mut scan = occurrences()?;
            scan.try_for_each(|item| item.map(drop))?;
            coverage.push(scan.finish());
        }
        let source = occurrences()?;
        streams.entry(stream).or_default().push(source);
    }
    if multiple {
        let spans = coverage
            .iter()
            .flat_map(|c| &c.intervals)
            .map(|i| (i.start_us, i.end_us));
        if let (Some(start), Some(end)) = (
            spans.clone().map(|i| i.0).min(),
            spans.clone().map(|i| i.1).max(),
        ) {
            let holes = uncovered_intervals(spans, start, end)?;
            for (source, report) in streams.values_mut().flatten().zip(&coverage) {
                source.coverage_periods = report
                    .intervals
                    .iter()
                    .map(|i| holes.partition_point(|&(_, end)| end <= i.start_us))
                    .collect();
            }
        }
    }
    let mut driver = ReconstructDriver::new(venue, version, target_rows, emit);
    let mut period = None;
    let mut kalshi_basis = None;
    let mut subscriptions = BTreeMap::new();
    let stats = CopyMerge::new(venue, MergeOptions::default())?.merge(
        streams
            .values_mut()
            .map(|sessions| sessions.iter_mut().flatten()),
        |m| {
            on_message(&m)?;
            let origin = m.origin();
            // The recorders' monotonic clocks are unrelated host clocks; the UTC receive
            // time already orders the merge, so it also times the corrective-delta deadline.
            let clock_ns = u64::try_from(origin.received_us)
                .ok()
                .and_then(|us| us.checked_mul(1_000))
                .ok_or_else(|| Error::Invalid("receive timestamp out of range".into()))?;
            if period
                .replace(origin.coverage_period)
                .is_some_and(|old| old != origin.coverage_period)
            {
                driver.books.reset();
            }
            // Register the whole declared shard, including silent instruments
            // whose first delta may follow a gap. Origins share the same set
            // until a new subscription command changes it.
            for o in &m.occurrences {
                if let Some(ids) = &o.subscription
                    && subscriptions
                        .get(&o.stream)
                        .is_none_or(|prev| !Arc::ptr_eq(prev, ids))
                {
                    for id in ids.iter() {
                        driver.books.subscribe(id.clone());
                    }
                    subscriptions.insert(o.stream.clone(), Arc::clone(ids));
                }
            }
            if venue == Venue::Kalshi
                && matches!(m.kind.as_str(), "orderbook_snapshot" | "orderbook_delta")
            {
                for o in &m.occurrences {
                    if *kalshi_basis.get_or_insert(o.use_yes_price) != o.use_yes_price {
                        return Err(Error::Invalid(
                            "cannot merge Kalshi books on different price bases".into(),
                        ));
                    }
                }
                driver.books.set_kalshi_price_mode(origin.use_yes_price);
            }
            driver.message(
                &m.message,
                &Receipt {
                    utc: &origin.received_utc,
                    us: origin.received_us,
                    monotonic_ns: origin.received_monotonic_ns,
                    clock_ns,
                    sequence: origin.frame_seq,
                    recording_id: &origin.recording_id,
                },
                origin.position,
                m.occurrences
                    .iter()
                    .filter_map(|o| o.subscription.as_deref()),
                origin.book_command,
                SequenceCheck::SourceCoverage,
            )
        },
    )?;
    driver.finish()?;
    Ok((
        stats,
        streams
            .into_values()
            .flatten()
            .map(SessionOccurrences::finish)
            .collect(),
    ))
}

/// Reconstruct one verified raw session in file order. The sink receives complete
/// book observations in bounded batches (a single deep book can exceed target_rows).
/// Successful return certifies the EOF checks on every published chunk.
pub fn reconstruct_session(
    path: impl AsRef<Path>,
    version: Version,
    target_rows: usize,
    emit: impl FnMut(QuoteBatches) -> Result<()>,
) -> Result<()> {
    if target_rows == 0 {
        return Err(Error::Invalid("batch row target must be positive".into()));
    }
    let input = Input::open(path.as_ref())?;
    let Some(venue) = input.venue() else {
        return Ok(());
    };
    let mut driver = ReconstructDriver::new(venue, version, target_rows, emit);
    let mut generation = None;
    let mut subscription = wire::RecordedSubscription::default();
    for item in input.frames()? {
        let f = match item? {
            CaptureItem::Gap(_) => {
                driver.books.reset();
                continue;
            }
            CaptureItem::Frame(f) => f,
        };
        if generation
            .replace(f.connection_generation)
            .is_some_and(|g| g != f.connection_generation)
        {
            driver.books.reset();
        }
        if f.frame_type == "subscription" && f.direction == "sent" {
            let command: Value = serde_json::from_str(&f.payload_raw)?;
            subscription.record(venue, &command)?;
            if let Some(basis) = subscription.kalshi_basis {
                driver.books.set_kalshi_price_mode(basis);
            }
            if let Some(ids) = &subscription.instruments {
                for id in ids.iter() {
                    driver.books.subscribe(id.clone());
                }
            }
            continue;
        }
        if EndReason::transport_boundary(&f.frame_type, &f.direction).is_some() {
            driver.books.reset();
            continue;
        }
        if f.direction == "received" && f.frame_type == "text" {
            let receipt = Receipt {
                utc: &f.received_utc,
                us: received_us(&f.received_utc)?,
                monotonic_ns: f.received_monotonic_ns,
                clock_ns: f.received_monotonic_ns,
                sequence: f.local_frame_seq,
                recording_id: &f.recording_id,
            };
            for m in wire::live_messages(f.payload_raw.as_bytes())? {
                driver.message(
                    &m.value,
                    &receipt,
                    m.position,
                    subscription.instruments.as_deref().into_iter(),
                    subscription.book_command,
                    SequenceCheck::Connection,
                )?;
            }
        }
    }
    driver.finish()
}
