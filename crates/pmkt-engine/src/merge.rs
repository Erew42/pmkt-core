//! Bounded union of redundant raw streams. Coverage is evidence of receipt only.
mod coverage;
mod identity;
mod order;
pub(crate) use coverage::received_us;
pub use coverage::{
    CoverageCounts, CoverageTracker, EndReason, Interval, SessionCoverage, SessionOccurrences,
    StartReason, uncovered_intervals,
};

use crate::{Error, Result, Venue};
use identity::ContentId;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(serde::Serialize, Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct Stream {
    pub copy: String,
    pub connection: String,
}
impl Stream {
    pub fn new(copy: impl Into<String>, connection: impl Into<String>) -> Self {
        Self {
            copy: copy.into(),
            connection: connection.into(),
        }
    }
}
#[derive(serde::Serialize, Clone, Debug, Eq, PartialEq)]
pub struct Origin {
    pub stream: Stream,
    pub session: String,
    pub generation: u64,
    pub frame_seq: u64,
    pub position: Option<usize>,
    pub received_us: i64,
    pub received_utc: String,
    pub recording_id: String,
    pub received_monotonic_ns: u64,
    pub use_yes_price: bool,
}
impl Origin {
    fn rank(&self) -> (i64, &Stream, &str, u64, Option<usize>) {
        (
            self.received_us,
            &self.stream,
            &self.session,
            self.frame_seq,
            self.position,
        )
    }
}
#[derive(Debug, Clone)]
pub struct Occurrence {
    pub origin: Origin,
    pub message: Value,
    pub reconnect_snapshot: bool,
}
#[derive(serde::Serialize, Debug)]
pub struct MergedMessage {
    pub kind: String,
    pub scope: String,
    pub venue_ts_ms: Option<i64>,
    pub order_ms: i64,
    pub late: bool,
    pub checkpoint: bool,
    pub reconnect_snapshot: bool,
    pub message: Value,
    pub content_digest: String,
    pub copies: BTreeSet<String>,
    pub occurrences: Vec<Origin>,
}
impl MergedMessage {
    pub fn origin(&self) -> &Origin {
        &self.occurrences[0]
    }
    pub fn snapshot_class(&self) -> Option<&'static str> {
        if !self.checkpoint {
            return None;
        }
        if self.reconnect_snapshot {
            return Some("reconnect");
        }
        let (body, fields): (&Value, &[&str]) = if self.kind == "book" {
            (&self.message, &["bids", "asks"])
        } else {
            (
                &self.message["msg"],
                &[
                    "yes_dollars_fp",
                    "yes_dollars",
                    "yes",
                    "no_dollars_fp",
                    "no_dollars",
                    "no",
                ],
            )
        };
        if body.is_object()
            && !fields
                .iter()
                .any(|s| body[*s].as_array().is_some_and(|v| !v.is_empty()))
        {
            Some("empty")
        } else {
            Some("later")
        }
    }
}
#[derive(Debug, Clone)]
pub struct MergeOptions {
    pub lateness_ms: i64,
    pub untimed_match_ms: i64,
    pub retention_ms: i64,
    pub strict_order: bool,
    pub preferred_copies: Vec<String>,
}
impl Default for MergeOptions {
    fn default() -> Self {
        Self {
            lateness_ms: 120_000,
            untimed_match_ms: 5_000,
            retention_ms: 900_000,
            strict_order: false,
            preferred_copies: vec![],
        }
    }
}
#[derive(serde::Serialize, Debug)]
pub struct OrderConflict {
    pub scope: String,
    pub order_ms: i64,
    pub origin: Origin,
    pub book_changes: bool,
}
#[derive(serde::Serialize, Debug, Default)]
pub struct MergeStats {
    pub occurrences: u64,
    pub messages: u64,
    pub repeats_merged: u64,
    pub repeats_after_emit: u64,
    pub untimed_messages: u64,
    pub late_messages: u64,
    pub future_messages: u64,
    pub late_by_copy: BTreeMap<String, u64>,
    pub clock_steps_back: u64,
    pub max_pending_occurrences: usize,
    pub snapshots_by_clock: u64,
    pub snapshot_clock_fallbacks: BTreeMap<&'static str, u64>,
    pub clock_regressions: u64,
    pub book_order_conflicts: u64,
    pub other_order_conflicts: u64,
    pub conflict_examples: Vec<OrderConflict>,
    pub ambiguous_choices: u64,
}
fn ms(value: &Value) -> Option<i64> {
    value.as_i64().filter(|t| *t >= 0).or_else(|| {
        value
            .as_str()
            .filter(|s| s.bytes().all(|c| c.is_ascii_digit()))
            .and_then(|s| s.parse().ok())
    })
}
struct Key {
    kind: String,
    scope: String,
    venue_ms: Option<i64>,
    sent: Option<i64>,
    id: ContentId,
}
impl Key {
    fn new(venue: Venue, v: &Value) -> Self {
        let (kind, scope, venue_ms, sent) = match venue {
            Venue::Polymarket => (
                &v["event_type"],
                if v["market"].as_str().is_some_and(|s| !s.is_empty()) {
                    &v["market"]
                } else {
                    &v["asset_id"]
                },
                ms(&v["timestamp"]),
                None,
            ),
            Venue::Kalshi => {
                let body = &v["msg"];
                (
                    &v["type"],
                    if body["market_ticker"]
                        .as_str()
                        .is_some_and(|s| !s.is_empty())
                    {
                        &body["market_ticker"]
                    } else {
                        &body["event_ticker"]
                    },
                    ms(&body["ts_ms"]),
                    ms(&v["sending_ts_ms"]),
                )
            }
        };
        Self {
            kind: kind.as_str().unwrap_or("").into(),
            scope: scope.as_str().unwrap_or("").into(),
            venue_ms,
            sent,
            id: identity::digest(venue, v),
        }
    }
}
fn checkpoint(venue: Venue, kind: &str) -> bool {
    matches!(
        (venue, kind),
        (Venue::Polymarket, "book") | (Venue::Kalshi, "orderbook_snapshot")
    )
}
fn book_change(venue: Venue, kind: &str) -> bool {
    checkpoint(venue, kind)
        || matches!(
            (venue, kind),
            (Venue::Kalshi, "orderbook_delta") | (Venue::Polymarket, "price_change")
        )
}
#[derive(Clone, Copy, Debug)]
struct BookClock {
    epoch: u64,
    value: u64,
}
struct Entry {
    key: Key,
    message: Value,
    origins: Vec<(usize, Origin)>,
    reconnect: bool,
    clock: Option<BookClock>,
    // Sending/receive time used for untimed matching. Clock placement never alters it.
    at: Option<i64>,
}
impl Entry {
    fn new(o: Occurrence, key: Key, index: usize, clock: Option<BookClock>) -> Self {
        Self {
            key,
            message: o.message,
            origins: vec![(index, o.origin)],
            reconnect: o.reconnect_snapshot,
            clock,
            at: None,
        }
    }
    fn attach(&mut self, o: Occurrence, index: usize, clock: Option<BookClock>) {
        self.reconnect |= o.reconnect_snapshot;
        if o.origin.rank() < self.origins[0].1.rank() {
            self.message = o.message;
            if clock.is_some() {
                self.clock = clock;
            }
            self.origins.insert(0, (index, o.origin));
        } else {
            self.origins.push((index, o.origin));
        }
    }
    fn has_stream(&self, s: &Stream) -> bool {
        self.origins.iter().any(|(_, o)| &o.stream == s)
    }
}
#[derive(Default)]
struct Group {
    counts: HashMap<(Stream, ContentId), usize>,
    timed: BTreeMap<(ContentId, usize), Entry>,
    untimed: BTreeSet<usize>,
}
type GroupKey = (i64, String);
struct Remembered {
    at: i64,
    streams: BTreeSet<Stream>,
}

pub struct CopyMerge {
    venue: Venue,
    options: MergeOptions,
    stats: MergeStats,
    groups: BTreeMap<GroupKey, Group>,
    untimed: BTreeMap<usize, Entry>,
    by_digest: HashMap<ContentId, BTreeSet<usize>>,
    clocked: HashMap<String, BTreeSet<usize>>,
    market_epochs: HashMap<String, u64>,
    stream_clocks: HashMap<(Stream, String), BookClock>,
    last_clock: HashMap<String, (BookClock, bool)>,
    emitted_timed: HashMap<ContentId, i64>,
    emitted_untimed: HashMap<ContentId, BTreeMap<usize, Remembered>>,
    expiry: BTreeMap<i64, Vec<(ContentId, Option<usize>)>>,
    pending: usize,
}
impl CopyMerge {
    pub fn new(venue: Venue, options: MergeOptions) -> Result<Self> {
        if options.lateness_ms <= 0
            || options.untimed_match_ms < 0
            || options.retention_ms < 0
            || options
                .preferred_copies
                .iter()
                .collect::<BTreeSet<_>>()
                .len()
                != options.preferred_copies.len()
        {
            return Err(Error::Invalid(
                "invalid merge windows or duplicate preferred copy".into(),
            ));
        }
        Ok(Self {
            venue,
            options,
            stats: MergeStats::default(),
            groups: BTreeMap::new(),
            untimed: BTreeMap::new(),
            by_digest: HashMap::new(),
            clocked: HashMap::new(),
            market_epochs: HashMap::new(),
            stream_clocks: HashMap::new(),
            last_clock: HashMap::new(),
            emitted_timed: HashMap::new(),
            emitted_untimed: HashMap::new(),
            expiry: BTreeMap::new(),
            pending: 0,
        })
    }
    /// Interleave one lookahead per input. The sink runs as each timestamp clears
    /// the watermark; neither the inputs nor the merged output need fit in RAM.
    pub fn merge<I: IntoIterator<Item = Result<Occurrence>>>(
        mut self,
        streams: impl IntoIterator<Item = I>,
        mut emit: impl FnMut(MergedMessage) -> Result<()>,
    ) -> Result<MergeStats> {
        let mut next = BTreeMap::new();
        let mut unique = BTreeSet::new();
        for source in streams {
            let mut iter = source.into_iter();
            if let Some(o) = iter.next().transpose()? {
                if !unique.insert(o.origin.stream.clone()) {
                    return Err(Error::Invalid("duplicate copy/connection stream".into()));
                }
                next.insert((o.origin.received_us, o.origin.stream.clone()), (o, iter));
            }
        }
        let mut index = 0;
        while let Some(((received, stream), (o, mut rest))) = next.pop_first() {
            if let Some(f) = rest.next().transpose()? {
                if f.origin.stream != stream {
                    return Err(Error::Invalid(
                        "copy/connection changed inside input stream".into(),
                    ));
                }
                if f.origin.received_us < received {
                    self.stats.clock_steps_back += 1;
                }
                next.insert((received.max(f.origin.received_us), stream), (f, rest));
            }
            let received = received.div_euclid(1000);
            let watermark = received.saturating_sub(self.options.lateness_ms);
            self.flush(Some(watermark), &mut emit)?;
            self.expire(watermark);
            if let Some(row) = self.add(o, index, received, watermark) {
                emit(row)?;
            }
            index += 1;
        }
        self.flush(None, &mut emit)?;
        Ok(self.stats)
    }
    fn clock(&mut self, key: &Key, o: &Occurrence) -> (Option<BookClock>, bool) {
        if self.venue != Venue::Kalshi || !book_change(self.venue, &key.kind) {
            return (None, false);
        }
        let Some(value) = o.message["msg"]["clock"].as_u64() else {
            return (None, false);
        };
        let stream = (o.origin.stream.clone(), key.scope.clone());
        let latest = self.market_epochs.get(&key.scope).copied().unwrap_or(0);
        let previous = self.stream_clocks.get(&stream);
        let mut epoch = previous.map_or(latest, |c| c.epoch);
        let reset = value > 0 && previous.is_some_and(|c| value < c.value);
        if reset {
            epoch = (epoch + 1).max(latest);
            self.market_epochs.insert(key.scope.clone(), epoch);
        }
        let clock = BookClock { epoch, value };
        if value > 0 {
            self.stream_clocks.insert(stream, clock);
        }
        (Some(clock), reset)
    }
    fn snapshot_clock(
        &mut self,
        scope: &str,
        clock: Option<BookClock>,
        reset: bool,
    ) -> Option<BookClock> {
        let reason = match clock {
            None => "missing",
            Some(c) if c.value == 0 => "zero",
            Some(c)
                if reset
                    || self
                        .last_clock
                        .get(scope)
                        .is_some_and(|(old, _)| old.epoch == c.epoch && c.value < old.value) =>
            {
                "decreased"
            }
            Some(_) => {
                self.stats.snapshots_by_clock += 1;
                return clock;
            }
        };
        *self
            .stats
            .snapshot_clock_fallbacks
            .entry(reason)
            .or_default() += 1;
        None
    }
    fn grow(&mut self) {
        self.pending += 1;
        self.stats.max_pending_occurrences = self.stats.max_pending_occurrences.max(self.pending);
    }
    fn add(
        &mut self,
        o: Occurrence,
        index: usize,
        received: i64,
        watermark: i64,
    ) -> Option<MergedMessage> {
        self.stats.occurrences += 1;
        let key = Key::new(self.venue, &o.message);
        let (clock, reset) = self.clock(&key, &o);
        let at = key.venue_ms.or(key.sent).unwrap_or(received);
        let id = key.id;
        if key.venue_ms.is_none() {
            let tolerance = self.options.untimed_match_ms as u64;
            if let Some(matches) = self.by_digest.get(&id)
                && let Some(&i) = matches.iter().find(|i| {
                    let e = &self.untimed[i];
                    !e.has_stream(&o.origin.stream) && at.abs_diff(e.at.unwrap()) <= tolerance
                })
            {
                let e = self.untimed.get_mut(&i).unwrap();
                e.attach(o, index, None);
                let old = e.at.unwrap();
                if watermark <= at && at < old {
                    e.at = Some(at);
                    self.unschedule(&(old, key.scope.clone()), i);
                    self.groups
                        .entry((at, key.scope))
                        .or_default()
                        .untimed
                        .insert(i);
                }
                self.grow();
                return None;
            }
            if let Some(records) = self.emitted_untimed.get_mut(&id)
                && let Some(r) = records.values_mut().find(|r| {
                    !r.streams.contains(&o.origin.stream) && at.abs_diff(r.at) <= tolerance
                })
            {
                r.streams.insert(o.origin.stream);
                self.stats.repeats_after_emit += 1;
                return None;
            }
        } else if self.emitted_timed.contains_key(&id) {
            self.stats.repeats_after_emit += 1;
            return None;
        }
        let future = at > received.saturating_add(self.options.lateness_ms);
        if at < watermark || future {
            let mut e = Entry::new(o, key, index, clock);
            if e.key.venue_ms.is_none() {
                e.at = Some(at);
                self.stats.untimed_messages += 1;
            }
            self.remember(&e, watermark.max(at));
            self.stats.future_messages += u64::from(future);
            self.stats.late_messages += 1;
            *self
                .stats
                .late_by_copy
                .entry(e.origins[0].1.stream.copy.clone())
                .or_default() += 1;
            return Some(self.message(e, watermark, true));
        }
        let group_key = (at, key.scope.clone());
        if key.venue_ms.is_some() {
            let g = self.groups.entry(group_key).or_default();
            let repeat = g.counts.entry((o.origin.stream.clone(), id)).or_default();
            let node = (id, *repeat);
            *repeat += 1;
            if let Some(e) = g.timed.get_mut(&node) {
                e.attach(o, index, clock);
            } else {
                g.timed.insert(node, Entry::new(o, key, index, clock));
            }
        } else {
            let mut e = Entry::new(o, key, index, clock);
            e.at = Some(at);
            if self.venue == Venue::Kalshi && e.key.kind == "orderbook_snapshot" {
                e.clock = self.snapshot_clock(&e.key.scope, clock, reset);
                if e.clock.is_some() {
                    self.clocked
                        .entry(e.key.scope.clone())
                        .or_default()
                        .insert(index);
                }
            }
            self.groups
                .entry(group_key)
                .or_default()
                .untimed
                .insert(index);
            self.untimed.insert(index, e);
            self.by_digest.entry(id).or_default().insert(index);
        }
        self.grow();
        None
    }
    fn unschedule(&mut self, key: &GroupKey, i: usize) {
        if let Some(g) = self.groups.get_mut(key) {
            g.untimed.remove(&i);
            if g.timed.is_empty() && g.untimed.is_empty() {
                self.groups.remove(key);
            }
        }
    }
    fn remember(&mut self, e: &Entry, until: i64) {
        let id = e.key.id;
        let record = if let Some(at) = e.at {
            // Late untimed copies match records in emission order, even when
            // a clock moved a later-received snapshot into an earlier batch.
            let i = self.stats.messages as usize;
            let streams = e.origins.iter().map(|(_, o)| o.stream.clone()).collect();
            self.emitted_untimed
                .entry(id)
                .or_default()
                .insert(i, Remembered { at, streams });
            Some(i)
        } else {
            self.emitted_timed.insert(id, until);
            None
        };
        self.expiry.entry(until).or_default().push((id, record));
    }
    fn expire(&mut self, watermark: i64) {
        let limit = watermark.saturating_sub(self.options.retention_ms);
        while self
            .expiry
            .first_key_value()
            .is_some_and(|(t, _)| *t < limit)
        {
            let (t, list) = self.expiry.pop_first().unwrap();
            for (id, i) in list {
                if let Some(i) = i {
                    if let Some(matches) = self.emitted_untimed.get_mut(&id) {
                        matches.remove(&i);
                        if matches.is_empty() {
                            self.emitted_untimed.remove(&id);
                        }
                    }
                } else if self.emitted_timed.get(&id) == Some(&t) {
                    self.emitted_timed.remove(&id);
                }
            }
        }
    }
    fn flush(
        &mut self,
        limit: Option<i64>,
        emit: &mut impl FnMut(MergedMessage) -> Result<()>,
    ) -> Result<()> {
        while self
            .groups
            .first_key_value()
            .is_some_and(|((time, _), _)| limit.is_none_or(|l| *time < l))
        {
            let ((time, scope), g) = self.groups.pop_first().unwrap();
            let mut entries: Vec<_> = g.timed.into_values().collect();
            let mut batch = g.untimed;
            if let Some(held) = self.clocked.get(&scope) {
                let mut newest = BTreeMap::new();
                for c in entries
                    .iter()
                    .filter(|e| e.key.kind == "orderbook_delta")
                    .chain(
                        batch
                            .iter()
                            .map(|i| &self.untimed[i])
                            .filter(|e| e.key.kind == "orderbook_snapshot"),
                    )
                    .filter_map(|e| e.clock)
                {
                    let max = newest.entry(c.epoch).or_insert(c.value);
                    *max = (*max).max(c.value);
                }
                batch.extend(held.iter().copied().filter(|i| {
                    let c = self.untimed[i].clock.unwrap();
                    newest.get(&c.epoch).is_some_and(|value| *value > c.value)
                }));
            }
            for i in batch {
                let e = self.untimed.remove(&i).unwrap();
                let id = e.key.id;
                let at = e.at.unwrap();
                if at != time {
                    self.unschedule(&(at, scope.clone()), i);
                }
                if let Some(matches) = self.by_digest.get_mut(&id) {
                    matches.remove(&i);
                    if matches.is_empty() {
                        self.by_digest.remove(&id);
                    }
                }
                if let Some(held) = self.clocked.get_mut(&scope) {
                    held.remove(&i);
                    if held.is_empty() {
                        self.clocked.remove(&scope);
                    }
                }
                entries.push(e);
            }
            let order = order::resolve(
                self.venue,
                &self.options,
                &mut self.stats,
                &entries,
                time,
                &scope,
            )?;
            let mut slots = entries.into_iter().map(Some).collect::<Vec<_>>();
            for node in order {
                let e = slots[node].take().unwrap();
                self.pending -= e.origins.len();
                self.stats.untimed_messages += u64::from(e.at.is_some());
                self.remember(&e, e.at.unwrap_or(time).max(time));
                emit(self.message(e, time, false))?;
            }
        }
        Ok(())
    }
    fn message(&mut self, mut e: Entry, order_ms: i64, late: bool) -> MergedMessage {
        if self.venue == Venue::Kalshi
            && let Some(c) = e.clock
        {
            if e.key.kind == "orderbook_delta" {
                if self
                    .last_clock
                    .get(&e.key.scope)
                    .is_some_and(|(old, snap)| {
                        c.value < old.value || (*snap && c.value == old.value)
                    })
                {
                    self.stats.clock_regressions += 1;
                }
                self.last_clock.insert(e.key.scope.clone(), (c, false));
            } else if e.key.kind == "orderbook_snapshot" && e.at.is_some() && !late {
                self.last_clock.insert(e.key.scope.clone(), (c, true));
            }
        }
        e.origins.sort_by(|(_, a), (_, b)| a.rank().cmp(&b.rank()));
        let copies = e
            .origins
            .iter()
            .map(|(_, o)| o.stream.copy.clone())
            .collect();
        self.stats.messages += 1;
        self.stats.repeats_merged += (e.origins.len() - 1) as u64;
        MergedMessage {
            checkpoint: checkpoint(self.venue, &e.key.kind),
            kind: e.key.kind,
            scope: e.key.scope,
            venue_ts_ms: e.key.venue_ms,
            order_ms,
            late,
            reconnect_snapshot: e.reconnect,
            message: e.message,
            content_digest: e.key.id.iter().map(|b| format!("{b:02x}")).collect(),
            copies,
            occurrences: e.origins.into_iter().map(|(_, o)| o).collect(),
        }
    }
}
