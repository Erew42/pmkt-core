use super::{QuoteBatchBuilder, QuoteBatches, Receive, Version, utc_text};
use crate::{
    Error, Result, Venue,
    book::{Book, BookMode, SequenceCheck, UpdateKind, VenueBooks},
    capture::{CaptureItem, RawFrame, SealedChunk, SessionChunks, list_session_chunks},
    health::Integrity,
    merge::{
        CopyMerge, EndReason, MergeOptions, MergeStats, MergedMessage, Origin, ReceiptPeriod,
        SessionCoverage, SessionEvidence, SessionOccurrences, Stream, received_us,
        recorded_message, uncovered_intervals,
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

// The latest applied depth update per token and the original recorder intervals
// which saw it. Metadata may advance the book timestamp without changing its depth.
struct MergedDepth {
    coverage: Arc<[SessionCoverage]>,
    receipts: Vec<ReceiptPeriod>,
    latest: BTreeMap<String, (i64, Vec<ReceiptPeriod>)>,
}

impl MergedDepth {
    fn retained(&self, books: &VenueBooks, event: &Event, m: &MergedMessage) -> Option<String> {
        if !m.late || !m.reconnect_snapshot {
            return None;
        }
        let Event::Books(updates) = event else {
            return None;
        };
        let [u] = updates.as_slice() else { return None };
        let book = books.get(&u.instrument)?;
        if !u.action.is_snapshot()
            || book.integrity() != Integrity::Intact
            || book.market()? != u.market.as_deref()?
        {
            return None;
        }
        let (latest, periods) = self.latest.get(&u.instrument)?;
        (m.venue_ts_ms? < *latest
            && periods
                .iter()
                .any(|p| p.bridges(&self.coverage, m.origin())))
        .then(|| u.instrument.clone())
    }

    fn record(&mut self, book: &Book, at: Option<i64>) {
        // VenueBooks keeps the token's established market across resets. A reset
        // leaves the ladder awaiting a snapshot; a fresh baseline replaces its proof.
        let instrument = book.instrument();
        let Some(at) =
            at.filter(|_| book.integrity() == Integrity::Intact && book.market().is_some())
        else {
            self.latest.remove(instrument);
            return;
        };
        if let Some((stamp, periods)) = self.latest.get_mut(instrument) {
            *stamp = at;
            periods.clone_from(&self.receipts);
        } else {
            self.latest
                .insert(instrument.to_owned(), (at, self.receipts.clone()));
        }
    }
}

struct MergedCoverage {
    coverage: Arc<[SessionCoverage]>,
    subscribed: Vec<Option<Arc<BTreeSet<String>>>>,
    observed: Vec<BTreeSet<String>>,
}

impl MergedCoverage {
    fn covered_by_other(&self, instrument: &str, origin: &Origin) -> bool {
        self.coverage
            .iter()
            .zip(&self.subscribed)
            .zip(&self.observed)
            .any(|((session, ids), obs)| {
                session.stream != origin.stream
                    && (ids.as_deref().is_some_and(|set| set.contains(instrument))
                        || obs.contains(instrument))
                    && session
                        .intervals
                        .iter()
                        .any(|span| (span.start_us..=span.end_us).contains(&origin.received_us))
            })
    }

    fn restart_for_stream(
        &self,
        origin: &Origin,
        books: &mut VenueBooks,
        skip_covered: bool,
    ) -> bool {
        let mut touched = false;
        for (session, obs) in self.coverage.iter().zip(&self.observed) {
            if session.stream == origin.stream {
                for id in obs {
                    touched = true;
                    if !skip_covered || !self.covered_by_other(id, origin) {
                        books.restart(id);
                    }
                }
            }
        }
        touched
    }
}

struct ReconstructDriver<F> {
    books: VenueBooks,
    merged_depth: Option<MergedDepth>,
    merged_coverage: Option<MergedCoverage>,
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
            merged_depth: None,
            merged_coverage: None,
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
        merged: Option<&MergedMessage>,
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
                if let Some(m) = merged {
                    let origin = m.origin();
                    if let Some(ids) = origin.subscription.as_deref() {
                        for id in ids {
                            if self
                                .merged_coverage
                                .as_ref()
                                .is_none_or(|cov| !cov.covered_by_other(id, origin))
                            {
                                self.books.restart(id);
                            }
                        }
                    } else if let Some(cov) = &self.merged_coverage {
                        cov.restart_for_stream(origin, &mut self.books, true);
                    } else {
                        self.books.reset();
                    }
                } else {
                    self.books.reset();
                }
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
        let retained = self
            .merged_depth
            .as_ref()
            .and_then(|depth| depth.retained(&self.books, &event, merged?));
        let touched = if retained.is_some() {
            // The older checkpoint observes the current book at a new receipt.
            Vec::new()
        } else {
            let check = if merged.is_some() {
                SequenceCheck::SourceCoverage
            } else {
                SequenceCheck::Connection
            };
            self.books.apply_event(event, self.count, check)
        };
        let applied = touched.into_iter().map(|item| {
            (
                item.instrument,
                matches!(item.kind, UpdateKind::Snapshot | UpdateKind::Delta),
            )
        });
        for (instrument, changed_depth) in applied.chain(retained.map(|id| (id, false))) {
            let book = self.books.get(&instrument).expect("touched book");
            if changed_depth && let (Some(depth), Some(m)) = (&mut self.merged_depth, merged) {
                depth.record(book, m.venue_ts_ms);
            }
            // The top and all depth rows of one observation share one adjusted clock.
            let adjusted;
            let (clock, utc) = match self.last_utc.get(&instrument) {
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
            self.last_utc.insert(instrument.clone(), clock);
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
                    .defer(&instrument, self.count, frame.clock_ns)
            {
                self.books.reset_book(&instrument);
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

/// Replay one session in file order, or merge redundant sessions in venue order.
/// Keep the winning frame's original causal coordinates. A receive-time hole breaks continuity only if no copy spans it.
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
    let mut streams = BTreeMap::<_, Vec<_>>::new();
    let mut coverage = Vec::new();
    let mut subscribed = Vec::new();
    let mut observed = Vec::new();
    let singleton = sessions.len() == 1;
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
        if singleton {
            let mut source = RecordedSource {
                evidence: SessionEvidence::new(stream, name),
                stats: MergeStats::default(),
                latest_us: i64::MIN,
                on_message: &mut on_message,
            };
            replay_file(&input, venue, version, target_rows, emit, Some(&mut source))?;
            return Ok((source.stats, vec![source.evidence.finish()]));
        }
        let occurrences = || -> Result<_> {
            Ok(SessionOccurrences::new(
                stream.clone(),
                name,
                input.frames()?,
            ))
        };
        // Venue ordering can emit before a different copy has finished proving
        // coverage. Scan the same sealed inputs first, retaining only intervals.
        let mut scan = occurrences()?;
        scan.try_for_each(|item| item.map(drop))?;
        subscribed.push(scan.subscription());
        observed.push(scan.observed_books());
        coverage.push(scan.finish());
        let source = occurrences()?;
        streams.entry(stream).or_default().push(source);
    }
    let pooled = {
        // A copy bridges a hole only for copies of the same declared subscription,
        // or undeclared copies of the same connection without disjoint books.
        let mut declared = BTreeMap::<&BTreeSet<String>, Vec<usize>>::new();
        let mut undeclared = BTreeMap::<&str, Vec<usize>>::new();
        for (source, ids) in subscribed.iter().enumerate() {
            match ids.as_deref() {
                Some(set) if !set.is_empty() => declared.entry(set).or_default().push(source),
                _ => undeclared
                    .entry(coverage[source].stream.connection.as_str())
                    .or_default()
                    .push(source),
            }
        }
        let mut groups: Vec<Vec<usize>> = declared.into_values().collect();
        for members in undeclared.into_values() {
            let non_empty: Vec<&BTreeSet<String>> = members
                .iter()
                .map(|&s| &observed[s])
                .filter(|set| !set.is_empty())
                .collect();
            let mismatched = non_empty
                .first()
                .is_some_and(|first| non_empty[1..].iter().any(|other| other != first));
            if mismatched {
                groups.extend(members.into_iter().map(|s| vec![s]));
            } else {
                groups.push(members);
            }
        }
        let mut periods = vec![(0, vec![]); coverage.len()];
        for (group, members) in groups.into_iter().enumerate() {
            let spans: Vec<_> = members
                .iter()
                .flat_map(|&s| &coverage[s].intervals)
                .map(|i| (i.start_us, i.end_us))
                .collect();
            let (Some(start), Some(end)) = (
                spans.iter().map(|s| s.0).min(),
                spans.iter().map(|s| s.1).max(),
            ) else {
                continue;
            };
            let holes = uncovered_intervals(spans, start, end)?;
            for source in members {
                let hole_count = |start_us| holes.partition_point(|&(_, end)| end <= start_us);
                periods[source] = (
                    group,
                    coverage[source]
                        .intervals
                        .iter()
                        .map(|i| hole_count(i.start_us))
                        .collect(),
                );
            }
        }
        periods
    };
    // During ordering each source is its own coverage group. Pool only when
    // applying the merged stream to the books, retaining the recorder evidence
    // needed to place a cached checkpoint across another recorder's reconnect.
    for (index, source) in streams.values_mut().flatten().enumerate() {
        source.evidence.coverage_group = index;
    }
    let mut driver = ReconstructDriver::new(venue, version, target_rows, emit);
    let mut periods = BTreeMap::new();
    let mut kalshi_basis = None;
    let mut subscriptions = BTreeMap::new();
    let mut merger = CopyMerge::new(venue, MergeOptions::default())?;
    merger.verified_sessions = coverage.into();
    driver.merged_coverage = Some(MergedCoverage {
        coverage: Arc::clone(&merger.verified_sessions),
        subscribed,
        observed,
    });
    if venue == Venue::Polymarket {
        driver.merged_depth = Some(MergedDepth {
            coverage: Arc::clone(&merger.verified_sessions),
            receipts: Vec::new(),
            latest: BTreeMap::new(),
        });
    }
    let stats = merger.merge(
        streams
            .values_mut()
            .map(|sessions| sessions.iter_mut().flatten()),
        |mut m| {
            if let Some(depth) = &mut driver.merged_depth {
                depth.receipts.clear();
                depth
                    .receipts
                    .extend(m.occurrences.iter().map(Origin::receipt_period));
            }
            let mut origin_covered = false;
            for (index, o) in m.occurrences.iter_mut().enumerate() {
                let (group, periods) = &pooled[o.coverage_group];
                if let Some(&period) = periods.get(o.coverage_period) {
                    o.coverage_group = *group;
                    o.coverage_period = period;
                    if index == 0 {
                        origin_covered = true;
                    }
                }
            }
            on_message(&m)?;
            let origin = m.origin();
            // A hole in the receipt of one subscription restarts only its instruments.
            if origin_covered
                && periods
                    .insert(origin.coverage_group, origin.coverage_period)
                    .is_some_and(|old| old != origin.coverage_period)
            {
                match &origin.subscription {
                    Some(ids) => ids.iter().for_each(|id| driver.books.restart(id)),
                    None => {
                        let scoped = driver.merged_coverage.as_ref().is_some_and(|cov| {
                            cov.restart_for_stream(origin, &mut driver.books, false)
                        });
                        if !scoped {
                            driver.books.reset();
                        }
                    }
                }
            }
            // The recorders' monotonic clocks are unrelated host clocks; the UTC receive
            // time already orders the merge, so it also times the corrective-delta deadline.
            let clock_ns = u64::try_from(origin.received_us)
                .ok()
                .and_then(|us| us.checked_mul(1_000))
                .ok_or_else(|| Error::Invalid("receive timestamp out of range".into()))?;
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
                Some(&m),
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
    replay_file(&input, venue, version, target_rows, emit, None)
}

struct RecordedSource<'a> {
    evidence: SessionEvidence,
    stats: MergeStats,
    latest_us: i64,
    on_message: &'a mut dyn FnMut(&MergedMessage) -> Result<()>,
}

impl RecordedSource<'_> {
    fn publish(
        &mut self,
        f: &RawFrame,
        subscription: &wire::RecordedSubscription,
        m: wire::WireMessage<'_>,
    ) -> Result<Value> {
        let o = self.evidence.occurrence(f, subscription, m);
        self.stats.clock_steps_back += u64::from(o.origin.received_us < self.latest_us);
        self.latest_us = self.latest_us.max(o.origin.received_us);
        let message = recorded_message(f.venue, o, self.latest_us.div_euclid(1000));
        self.stats.occurrences += 1;
        self.stats.messages += 1;
        self.stats.untimed_messages += u64::from(message.venue_ts_ms.is_none());
        (self.on_message)(&message)?;
        Ok(message.message)
    }
}

fn replay_file(
    input: &Input,
    venue: Venue,
    version: Version,
    target_rows: usize,
    emit: impl FnMut(QuoteBatches) -> Result<()>,
    mut source: Option<&mut RecordedSource<'_>>,
) -> Result<()> {
    let mut driver = ReconstructDriver::new(venue, version, target_rows, emit);
    let mut generation = None;
    let mut subscription = wire::RecordedSubscription::default();
    let mut book_rejected = false;
    for item in input.frames()? {
        let item = item?;
        let CaptureItem::Frame(f) = &item else {
            driver.books.reset();
            if let Some(s) = &mut source {
                s.evidence.chunk_gap();
            }
            continue;
        };
        let received_text = f.direction == "received" && f.frame_type == "text";
        let messages = if received_text {
            wire::recorded_messages(f.payload_raw.as_bytes())?
        } else {
            Some(vec![])
        };
        if generation
            .replace(f.connection_generation)
            .is_some_and(|g| g != f.connection_generation)
        {
            book_rejected = false;
            driver.books.reset();
        }
        if f.frame_type == "subscription" && f.direction == "sent" {
            let command: Value = serde_json::from_str(&f.payload_raw)?;
            if subscription.record(venue, &command)? {
                book_rejected = false;
            }
            if let Some(basis) = subscription.kalshi_basis {
                driver.books.set_kalshi_price_mode(basis);
            }
            if let Some(ids) = &subscription.instruments {
                for id in ids.iter() {
                    driver.books.subscribe(id.clone());
                }
            }
        }
        if received_text
            && messages.as_deref().is_some_and(|msgs| {
                msgs.iter().any(|m| {
                    matches!(wire::decode(venue, &m.value), Ok(Event::SubscriptionError))
                        && wire::rejects_books(&m.value, subscription.book_command)
                })
            })
        {
            book_rejected = true;
        }
        if let Some(s) = &mut source {
            s.evidence.frame(f, messages.as_deref(), !book_rejected)?;
        }
        if f.frame_type == "subscription" && f.direction == "sent" {
            continue;
        } else if EndReason::transport_boundary(&f.frame_type, &f.direction).is_some() {
            driver.books.reset();
        } else if received_text {
            let receipt = Receipt {
                utc: &f.received_utc,
                us: received_us(&f.received_utc)?,
                monotonic_ns: f.received_monotonic_ns,
                clock_ns: f.received_monotonic_ns,
                sequence: f.local_frame_seq,
                recording_id: &f.recording_id,
            };
            for m in messages.into_iter().flatten() {
                let position = m.position;
                let value = match &mut source {
                    Some(s) => s.publish(f, &subscription, m)?,
                    None => m.value,
                };
                driver.message(
                    &value,
                    &receipt,
                    position,
                    subscription.instruments.as_deref().into_iter(),
                    subscription.book_command,
                    None,
                )?;
            }
        }
    }
    driver.finish()
}
