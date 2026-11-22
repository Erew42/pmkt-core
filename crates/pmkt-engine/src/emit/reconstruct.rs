use super::{QuoteBatchBuilder, QuoteBatches, Receive, Version, utc_text};
use crate::{
    Error, Result, Venue,
    book::{Book, BookMode, SequenceCheck, UpdateKind, VenueBooks},
    capture::{CaptureItem, RawFrame, SealedChunk, SessionChunks, list_session_chunks},
    health::Integrity,
    merge::{
        CopyMerge, EndReason, MergeOptions, MergeStats, MergedMessage, Origin, ReceiptPeriod,
        SessionCoverage, SessionEvidence, SessionOccurrences, Stream, confirms_books, received_us,
        recorded_message,
    },
    wire::{self, Event},
};
use chrono::DateTime;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
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
    venue: Venue,
    coverage: Arc<[SessionCoverage]>,
    undeclared_instruments: Vec<BTreeSet<String>>,
}

impl MergedCoverage {
    fn interval_covers(&self, source: usize, idx: usize, instrument: &str) -> bool {
        let session = &self.coverage[source];
        match session.interval_instruments[idx].as_deref() {
            Some(set) if !set.is_empty() => set.contains(instrument),
            _ => self.undeclared_instruments[source].contains(instrument),
        }
    }

    fn covered_by_other(&self, instrument: &str, origin: &Origin) -> bool {
        self.coverage.iter().enumerate().any(|(source, session)| {
            session.stream != origin.stream
                && session.intervals.iter().enumerate().any(|(idx, span)| {
                    if !self.interval_covers(source, idx, instrument)
                        || !(span.start_us..=span.end_us).contains(&origin.received_us)
                    {
                        return false;
                    }
                    let declared = session.interval_instruments[idx]
                        .as_deref()
                        .is_some_and(|set| !set.is_empty());
                    if self.venue == Venue::Kalshi && declared {
                        session.interval_first_book_us[idx]
                            .get(instrument)
                            .is_some_and(|&first| first <= origin.received_us)
                    } else {
                        true
                    }
                })
        })
    }

    fn source_instruments_for(&self, origin: &Origin) -> BTreeSet<String> {
        if let Some(ids) = origin.subscription.as_deref()
            && !ids.is_empty()
        {
            return ids.clone();
        }
        self.undeclared_instruments
            .get(origin.coverage_group)
            .cloned()
            .unwrap_or_default()
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
                    if let Some(cov) = &self.merged_coverage {
                        for id in cov.source_instruments_for(origin) {
                            if !cov.covered_by_other(&id, origin) {
                                self.books.restart(&id);
                            }
                        }
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
    let mut side_only = Vec::new();
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
        side_only.push(scan.side_only_subscription());
        coverage.push(scan.finish());
        let source = occurrences()?;
        streams.entry(stream).or_default().push(source);
    }
    let undeclared_instruments = {
        let mut per_source: Vec<BTreeSet<String>> = observed.clone();
        // A pure control-only copy with no subscription command can still bridge
        // an undeclared peer on the same connection when every undeclared peer
        // on that connection observed the same non-empty instrument set.
        let mut undeclared_by_conn = BTreeMap::<&str, Vec<usize>>::new();
        for (source, ids) in subscribed.iter().enumerate() {
            if ids.as_deref().is_none_or(BTreeSet::is_empty) {
                undeclared_by_conn
                    .entry(coverage[source].stream.connection.as_str())
                    .or_default()
                    .push(source);
            }
        }
        for members in undeclared_by_conn.into_values() {
            let non_empty: Vec<&BTreeSet<String>> = members
                .iter()
                .map(|&s| &observed[s])
                .filter(|set| !set.is_empty())
                .collect();
            if let Some(&first) = non_empty.first()
                && non_empty[1..].iter().all(|&other| other == first)
            {
                for &s in &members {
                    if observed[s].is_empty() && !side_only[s] {
                        per_source[s] = first.clone();
                    }
                }
            }
        }
        per_source
    };
    // Compute rollback- and sequence-gap-safe cross-stream bridging spans per
    // (source, interval). An interval whose start rolled back at or before a
    // prior interval's end on the same source, or that immediately follows a
    // proven wire sequence gap, cannot bridge cross-stream by UTC alone;
    // continuity across that break requires a shared witness on the post-gap
    // message itself.
    let (effective_spans, sorted_spans) = {
        let mut effective = Vec::with_capacity(coverage.len());
        let mut sorted = Vec::with_capacity(coverage.len());
        for session in &coverage {
            let mut spans = Vec::with_capacity(session.intervals.len());
            let mut active = Vec::new();
            let mut prev_max_end = i64::MIN;
            for (idx, span) in session.intervals.iter().enumerate() {
                let after_seq_gap =
                    idx > 0 && session.intervals[idx - 1].end_reason == EndReason::SequenceGap;
                let eff = if span.start_us > prev_max_end && !after_seq_gap {
                    let bounds = (span.start_us, span.end_us);
                    active.push((span.start_us, span.end_us, idx));
                    Some(bounds)
                } else {
                    None
                };
                prev_max_end = prev_max_end.max(span.end_us);
                spans.push(eff);
            }
            effective.push(spans);
            sorted.push(active);
        }
        (effective, sorted)
    };
    let inst_bridge_span = |coverage: &[SessionCoverage],
                            undeclared: &[BTreeSet<String>],
                            source: usize,
                            idx: usize,
                            inst: &str|
     -> Option<(i64, i64)> {
        let session = coverage.get(source)?;
        let declared = session.interval_instruments[idx]
            .as_deref()
            .filter(|set| !set.is_empty());
        let ids: &BTreeSet<String> = declared.unwrap_or(&undeclared[source]);
        if !ids.contains(inst) {
            return None;
        }
        let base_span = effective_spans[source][idx]?;
        if venue == Venue::Kalshi && declared.is_some() {
            let &first_us = session.interval_first_book_us[idx].get(inst)?;
            let start = base_span.0.max(first_us);
            (start <= base_span.1).then_some((start, base_span.1))
        } else {
            Some(base_span)
        }
    };
    type Witness = (usize, usize, i64);
    let intervals_connected = |coverage: &[SessionCoverage],
                               undeclared: &[BTreeSet<String>],
                               inst: &str,
                               prior_witnesses: &[Witness],
                               u: Witness,
                               v: Witness|
     -> bool {
        if u.0 == v.0 && u.1 == v.1 {
            return true;
        }
        let Some((u_lo, u_hi)) = inst_bridge_span(coverage, undeclared, u.0, u.1, inst) else {
            return false;
        };
        let Some((v_lo, v_hi)) = inst_bridge_span(coverage, undeclared, v.0, v.1, inst) else {
            return false;
        };
        let u_a = u_lo.max(u.2);
        let u_b = u_hi;
        let v_a = v_lo;
        let v_b = v_hi.min(v.2);
        if u_a > u_b || v_a > v_b {
            return false;
        }
        let u_ended_at_prev =
            u.2 >= u_hi && coverage[u.0].intervals[u.1].end_reason != EndReason::SessionEnd;
        if coverage[u.0].stream != coverage[v.0].stream
            && !u_ended_at_prev
            && u_a <= v_b
            && v_a <= u_b
        {
            return true;
        }
        let need_start_le = u_b.min(v_b);
        let need_end_ge = u_a.max(v_a);
        for (src_w, peers) in sorted_spans.iter().enumerate() {
            let stream_w = &coverage[src_w].stream;
            if stream_w == &coverage[u.0].stream || stream_w == &coverage[v.0].stream {
                continue;
            }
            let pos = peers.partition_point(|&(_, end_us, _)| end_us < need_end_ge);
            if let Some(&(_, _, idx_w)) = peers.get(pos)
                && let Some((mut w_lo, w_hi)) =
                    inst_bridge_span(coverage, undeclared, src_w, idx_w, inst)
            {
                if let Some(w_prev) = prior_witnesses
                    .iter()
                    .find(|w| w.0 == src_w && w.1 == idx_w)
                {
                    if w_prev.2 >= w_hi
                        && coverage[src_w].intervals[idx_w].end_reason != EndReason::SessionEnd
                    {
                        continue;
                    }
                    w_lo = w_lo.max(w_prev.2);
                }
                if w_lo <= need_start_le && w_hi >= need_end_ge {
                    return true;
                }
            }
        }
        false
    };
    // Each source keeps its own index in verified_sessions so per-interval
    // coverage classes and ReceiptPeriod stay aligned across the merge.
    for (index, source) in streams.values_mut().flatten().enumerate() {
        source.evidence.coverage_group = index;
    }
    let mut driver = ReconstructDriver::new(venue, version, target_rows, emit);
    let mut interval_seen_us: Vec<Vec<Option<i64>>> = coverage
        .iter()
        .map(|c| vec![None; c.intervals.len()])
        .collect();
    let mut instrument_states: HashMap<String, (Vec<Witness>, u64)> = HashMap::new();
    let mut kalshi_basis = None;
    let mut subscriptions = BTreeMap::new();
    let mut merger = CopyMerge::new(venue, MergeOptions::default())?;
    merger.verified_sessions = coverage.into();
    merger.collect_repeat_witnesses = true;
    driver.merged_coverage = Some(MergedCoverage {
        venue,
        coverage: Arc::clone(&merger.verified_sessions),
        undeclared_instruments,
    });
    if venue == Venue::Polymarket {
        driver.merged_depth = Some(MergedDepth {
            coverage: Arc::clone(&merger.verified_sessions),
            receipts: Vec::new(),
            latest: BTreeMap::new(),
        });
    }
    let mut covered_occurrences: Vec<Witness> = Vec::new();
    let mut msg_instruments: Vec<String> = Vec::new();
    let mut msg_seq = 0_u64;
    let mut run = merger.stream(
        streams
            .values_mut()
            .map(|sessions| sessions.iter_mut().flatten()),
    )?;
    while let Some(m) = run.next_message()? {
        msg_seq += 1;
        // Propagate late deduplicated repeats so a peer whose copy was suppressed
        // as repeats_after_emit still witnesses the baseline it received.
        for (instruments, orig_seq, rep_origin) in run.drain_repeat_witnesses() {
            if let Some(id) = rep_origin.coverage_period
                && let Some(cov) = &driver.merged_coverage
                && let Some(session) = cov.coverage.get(rep_origin.coverage_group)
                && let Ok(pos) = session.interval_ids.binary_search(&id)
            {
                for inst in &instruments {
                    if let Some((witnesses, last_reset_seq)) = instrument_states.get_mut(inst)
                        && *last_reset_seq <= orig_seq
                        && !witnesses
                            .iter()
                            .any(|w| w.0 == rep_origin.coverage_group && w.1 == pos)
                    {
                        witnesses.push((rep_origin.coverage_group, pos, rep_origin.received_us));
                    }
                }
            }
        }
        if let Some(depth) = &mut driver.merged_depth {
            depth.receipts.clear();
            depth
                .receipts
                .extend(m.occurrences.iter().map(Origin::receipt_period));
        }
        on_message(&m)?;
        let origin = m.origin();
        covered_occurrences.clear();
        if let Some(cov) = &driver.merged_coverage {
            for o in &m.occurrences {
                if let Some(id) = o.coverage_period
                    && let Some(session) = cov.coverage.get(o.coverage_group)
                    && let Ok(pos) = session.interval_ids.binary_search(&id)
                    && !covered_occurrences
                        .iter()
                        .any(|c| c.0 == o.coverage_group && c.1 == pos)
                {
                    covered_occurrences.push((o.coverage_group, pos, o.received_us));
                }
            }
        }
        // Track continuity strictly per targeted instrument in O(1) for the
        // common same-interval case, without scanning quiet instruments or
        // letting unrelated assets narrow another asset's witnesses.
        if !covered_occurrences.is_empty()
            && let Some(cov) = &driver.merged_coverage
        {
            msg_instruments.clear();
            wire::visit_message_instruments(venue, &m.message, |inst| {
                if !msg_instruments.iter().any(|s| s == inst) {
                    msg_instruments.push(inst.to_owned());
                }
            });
            for inst in &msg_instruments {
                if let Some((witnesses, last_reset_seq)) = instrument_states.get_mut(inst) {
                    let connected = witnesses.iter().any(|&u| {
                        covered_occurrences
                            .iter()
                            .any(|&v| u.0 == v.0 && u.1 == v.1)
                    }) || witnesses.iter().any(|&u| {
                        covered_occurrences.iter().any(|&v| {
                            intervals_connected(
                                &cov.coverage,
                                &cov.undeclared_instruments,
                                inst,
                                witnesses,
                                u,
                                v,
                            )
                        })
                    });
                    if !connected {
                        driver.books.restart(inst);
                        *last_reset_seq = msg_seq;
                        witnesses.clear();
                        witnesses.extend_from_slice(&covered_occurrences);
                    } else if m.late && m.reconnect_snapshot {
                        // A late cached reconnect snapshot anchored before later messages on an
                        // uninterrupted witness must not evict that witness from `witnesses`.
                        witnesses.retain(|&u| {
                            inst_bridge_span(
                                &cov.coverage,
                                &cov.undeclared_instruments,
                                u.0,
                                u.1,
                                inst,
                            )
                            .is_some_and(|(_, u_hi)| {
                                covered_occurrences.iter().any(|&v| u_hi >= v.2)
                            })
                        });
                        for &v in &covered_occurrences {
                            if !witnesses.iter().any(|w| w.0 == v.0 && w.1 == v.1) {
                                witnesses.push(v);
                            }
                        }
                    } else {
                        witnesses.clear();
                        witnesses.extend_from_slice(&covered_occurrences);
                    }
                } else {
                    let mut crossed_prior_gap = false;
                    'prior: for (src, seen) in interval_seen_us.iter().enumerate() {
                        for (pos, seen_us) in seen.iter().enumerate() {
                            let Some(t_prev) = *seen_us else {
                                continue;
                            };
                            let same_source_prior =
                                covered_occurrences.iter().any(|&v| v.0 == src && pos < v.1);
                            let prior_covers = if same_source_prior {
                                let session = &cov.coverage[src];
                                let declared = session.interval_instruments[pos]
                                    .as_deref()
                                    .filter(|set| !set.is_empty());
                                declared
                                    .unwrap_or(&cov.undeclared_instruments[src])
                                    .contains(inst)
                            } else if let Some((s_lo, _)) = inst_bridge_span(
                                &cov.coverage,
                                &cov.undeclared_instruments,
                                src,
                                pos,
                                inst,
                            ) {
                                covered_occurrences.iter().any(|&v| s_lo <= v.2)
                            } else {
                                false
                            };
                            if !prior_covers {
                                continue;
                            }
                            let u = (src, pos, t_prev);
                            if !covered_occurrences.iter().any(|&v| {
                                intervals_connected(
                                    &cov.coverage,
                                    &cov.undeclared_instruments,
                                    inst,
                                    &[u],
                                    u,
                                    v,
                                )
                            }) {
                                crossed_prior_gap = true;
                                break 'prior;
                            }
                        }
                    }
                    let reset_seq = if crossed_prior_gap {
                        driver.books.restart(inst);
                        msg_seq
                    } else {
                        0
                    };
                    instrument_states
                        .insert(inst.clone(), (covered_occurrences.clone(), reset_seq));
                }
            }
            for &(src, pos, us) in &covered_occurrences {
                interval_seen_us[src][pos] =
                    Some(interval_seen_us[src][pos].map_or(us, |prev| prev.max(us)));
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
        )?;
    }
    let stats = run.into_stats();
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
    let mut book_confirmed = false;
    let mut awaiting_book_confirm = false;
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
            book_confirmed = false;
            awaiting_book_confirm = false;
            book_rejected = false;
            if let Some(s) = &mut source {
                s.evidence.begin_generation(f.connection_generation);
            }
            driver.books.reset();
        }
        if f.frame_type == "subscription" && f.direction == "sent" {
            let command: Value = serde_json::from_str(&f.payload_raw)?;
            if subscription.record(venue, f.connection_generation, &command)? {
                if let Some(s) = &mut source {
                    s.evidence.set_instruments(subscription.instruments.clone());
                }
                book_rejected = false;
                if !book_confirmed {
                    if venue == Venue::Kalshi {
                        awaiting_book_confirm = true;
                    } else if let Some(s) = &mut source {
                        s.evidence.mark_book_pending();
                    }
                }
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
        if received_text && let Some(msgs) = messages.as_deref() {
            for m in msgs {
                if confirms_books(venue, &m.value, subscription.book_command) {
                    book_confirmed = true;
                    awaiting_book_confirm = false;
                    if let Some(s) = &mut source {
                        s.evidence.confirm_books();
                    }
                }
                if matches!(wire::decode(venue, &m.value), Ok(Event::SubscriptionError))
                    && wire::rejects_books(&m.value, subscription.book_command)
                {
                    book_rejected = true;
                    awaiting_book_confirm = false;
                }
            }
        }
        if let Some(s) = &mut source {
            s.evidence.frame(
                f,
                messages.as_deref(),
                !book_rejected,
                awaiting_book_confirm,
            )?;
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
