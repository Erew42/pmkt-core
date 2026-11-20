//! Venue-native ladders and exact quote projection.
use crate::{
    Venue,
    decimal::ScaledDecimal as D,
    health::{ComplementaryRecovery, Flags, Integrity, QualityFlag as F, Sequences},
    wire::*,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum QuotePolicy {
    Legacy,
    #[default]
    Current,
}
impl QuotePolicy {
    pub fn parse(value: Option<&str>) -> crate::Result<Self> {
        match value {
            None | Some("kalshi_quote_normalization.v1") => Ok(Self::Legacy),
            Some("kalshi_quote_normalization.v2") => Ok(Self::Current),
            _ => Err(crate::Error::Invalid(
                "unsupported Kalshi quote-normalization policy".into(),
            )),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BookMode {
    Polymarket,
    Kalshi {
        use_yes_price: bool,
        policy: QuotePolicy,
    },
}
impl BookMode {
    pub const fn kalshi() -> Self {
        Self::Kalshi {
            use_yes_price: true,
            policy: QuotePolicy::Current,
        }
    }
    pub const fn venue(self) -> Venue {
        match self {
            Self::Polymarket => Venue::Polymarket,
            Self::Kalshi { .. } => Venue::Kalshi,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Quote {
    pub price: D,
    pub size: D,
    pub derived: bool,
}
impl Quote {
    fn complement(self, derived: bool) -> Self {
        Self {
            price: self.price.complement().expect("validated binary price"),
            derived,
            ..self
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct OutcomeQuotes {
    pub outcome: Option<&'static str>,
    pub bid: Option<Quote>,
    pub ask: Option<Quote>,
}

#[derive(Debug)]
pub struct Book {
    mode: BookMode,
    instrument: String,
    market: Option<String>,
    first: Ladder,
    second: Ladder,
    integrity: Integrity,
    flags: Flags,
    pub(crate) sequence: Sequence,
    pub(crate) timestamp: SourceTime,
    pub(crate) hash: Option<String>,
    tick: Option<D>,
    minimum: Option<D>,
}
impl Book {
    pub fn new(mode: BookMode, instrument: impl Into<String>) -> Self {
        Self {
            mode,
            instrument: instrument.into(),
            market: None,
            first: Ladder::new(),
            second: Ladder::new(),
            integrity: Integrity::AwaitingSnapshot,
            flags: [F::NoInitialSnapshot].into(),
            sequence: Sequence::default(),
            timestamp: SourceTime::default(),
            hash: None,
            tick: None,
            minimum: None,
        }
    }
    pub fn mode(&self) -> BookMode {
        self.mode
    }
    pub fn instrument(&self) -> &str {
        &self.instrument
    }
    pub fn market(&self) -> Option<&str> {
        self.market.as_deref()
    }
    pub fn integrity(&self) -> Integrity {
        self.integrity
    }
    pub fn initialized(&self) -> bool {
        self.integrity != Integrity::AwaitingSnapshot
    }
    pub fn valid_state(&self) -> bool {
        self.integrity == Integrity::Intact && self.flags.is_empty()
    }
    pub fn flags(&self) -> &Flags {
        &self.flags
    }
    pub fn native_levels(&self) -> (&Ladder, &Ladder) {
        (&self.first, &self.second)
    }
    pub fn tick_size(&self) -> Option<D> {
        self.tick
    }
    pub fn minimum_size(&self) -> Option<D> {
        self.minimum
    }
    pub fn sequence(&self) -> Sequence {
        self.sequence
    }
    pub fn timestamp(&self) -> &SourceTime {
        &self.timestamp
    }
    pub fn venue_hash(&self) -> Option<&str> {
        self.hash.as_deref()
    }
    pub(crate) fn best_native_bid(&self) -> Option<D> {
        self.first.last_key_value().map(|(p, _)| *p)
    }
    fn best_right(&self) -> Option<Quote> {
        let level = if matches!(
            self.mode,
            BookMode::Kalshi {
                use_yes_price: false,
                ..
            }
        ) {
            self.second.last_key_value()
        } else {
            self.second.first_key_value()
        };
        level.map(|(&price, &size)| Quote {
            price,
            size,
            derived: false,
        })
    }
    pub fn quotes(&self) -> Vec<OutcomeQuotes> {
        let bid = self.first.last_key_value().map(|(&price, &size)| Quote {
            price,
            size,
            derived: false,
        });
        let right = self.best_right();
        match self.mode {
            BookMode::Polymarket => vec![OutcomeQuotes {
                outcome: None,
                bid,
                ask: right,
            }],
            BookMode::Kalshi {
                use_yes_price,
                policy,
            } => {
                let derived = policy == QuotePolicy::Current;
                let (ask, no_bid) = if use_yes_price {
                    (right, right.map(|q| q.complement(derived)))
                } else {
                    (right.map(|q| q.complement(derived)), right)
                };
                vec![
                    OutcomeQuotes {
                        outcome: Some("YES"),
                        bid,
                        ask,
                    },
                    OutcomeQuotes {
                        outcome: Some("NO"),
                        bid: no_bid,
                        ask: bid.map(|q| q.complement(derived)),
                    },
                ]
            }
        }
    }
    fn refresh(&mut self) {
        self.flags.retain(|f| !f.dynamic());
        let bid = self.best_native_bid();
        let ask = self.best_right().map(|q| {
            if matches!(
                self.mode,
                BookMode::Kalshi {
                    use_yes_price: false,
                    ..
                }
            ) {
                q.complement(true).price
            } else {
                q.price
            }
        });
        if bid.is_none() {
            self.flags.insert(F::EmptyBid);
        }
        if ask.is_none() {
            self.flags.insert(F::EmptyAsk);
        }
        if let Some((bid, ask)) = bid.zip(ask) {
            if bid > ask {
                self.flags.extend([F::CrossedBook, F::NegativeSpread]);
            } else if bid == ask && self.mode == BookMode::Polymarket {
                self.flags.insert(F::CrossedBook);
            }
        }
        if self.initialized() {
            self.integrity = if self.flags.iter().all(|f| {
                matches!(
                    f,
                    F::EmptyBid | F::EmptyAsk | F::NewMarket | F::MarketResolved
                )
            }) {
                Integrity::Intact
            } else {
                Integrity::Impaired
            };
        }
    }
    fn fault(&mut self, flag: F) {
        self.flags.insert(flag);
        if self.initialized() {
            self.integrity = Integrity::Impaired;
        }
    }
    fn apply_sequence(&mut self, s: Sequence) {
        let (Some(sid), Some(seq)) = (s.sid, s.seq) else {
            self.flags.insert(F::MissingSequence);
            return;
        };
        if self.sequence.sid.get_or_insert(sid) != &sid {
            self.flags.insert(F::SidChanged);
        }
        if self.sequence.seq.is_some_and(|last| seq <= last) {
            self.flags.insert(F::SeqGap);
        }
        self.sequence.seq = Some(seq);
    }
    fn side_mut(&mut self, side: Side) -> &mut Ladder {
        match side {
            Side::First => &mut self.first,
            Side::Second => &mut self.second,
        }
    }
    fn apply(&mut self, update: &Update) {
        if let Some(m) = &update.market {
            self.market = Some(m.clone());
        }
        self.timestamp = update.timestamp.clone();
        if update.action.is_snapshot() {
            self.flags.clear();
            self.integrity = Integrity::Intact;
            self.sequence = Sequence::default();
        } else if matches!(update.action, Action::Absolute(_) | Action::Relative { .. })
            && !self.initialized()
        {
            self.flags.insert(F::DeltaBeforeSnapshot);
        }
        if let Some(s) = update.sequence {
            self.apply_sequence(s);
        }
        match &update.action {
            Action::Snapshot(s) => {
                self.first.clone_from(&s.first.levels);
                self.second.clone_from(&s.second.levels);
                if s.first.malformed || s.second.malformed {
                    self.flags.insert(F::MalformedBook);
                }
                if s.hash.is_some() {
                    self.hash.clone_from(&s.hash);
                }
                for (field, target) in [(s.tick, &mut self.tick), (s.minimum, &mut self.minimum)] {
                    match field {
                        Metadata::Malformed => {
                            self.flags.insert(F::MalformedBook);
                        }
                        Metadata::Value(v) => *target = Some(v),
                        Metadata::Absent => {}
                    }
                }
            }
            Action::Absolute(c) => {
                if let (Some(side), Some(p), Some(q)) =
                    (c.side, c.price, c.size.filter(|q| *q >= D::ZERO))
                {
                    let levels = self.side_mut(side);
                    if q == D::ZERO {
                        levels.remove(&p);
                    } else {
                        levels.insert(p, q);
                    }
                } else {
                    self.flags.insert(F::MalformedBook);
                }
                if c.previous_hash
                    .as_ref()
                    .zip(self.hash.as_ref())
                    .is_some_and(|(prev, last)| prev != last)
                {
                    self.flags.insert(F::HashMismatch);
                }
                if c.hash.is_some() {
                    self.hash.clone_from(&c.hash);
                }
            }
            Action::Relative { side, price, delta } => {
                if let (Some(side), Some(p), Some(delta)) = (*side, *price, *delta) {
                    let levels = self.side_mut(side);
                    match levels
                        .get(&p)
                        .copied()
                        .unwrap_or(D::ZERO)
                        .checked_add(delta)
                    {
                        Ok(q) if q > D::ZERO => {
                            levels.insert(p, q);
                        }
                        Ok(q) => {
                            levels.remove(&p);
                            if q < D::ZERO {
                                self.flags.insert(F::MalformedBook);
                            }
                        }
                        Err(_) => {
                            self.flags.insert(F::MalformedBook);
                        }
                    }
                } else {
                    self.flags.insert(F::MalformedBook);
                }
            }
            Action::Tick(tick) => self.tick = *tick,
            Action::Lifecycle { resolved } => {
                self.flags.insert(if *resolved {
                    F::MarketResolved
                } else {
                    F::NewMarket
                });
                if *resolved {
                    return;
                }
            }
        }
        self.refresh();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpdateKind {
    Snapshot,
    Delta,
    Metadata,
    Lifecycle,
    Invalidated,
}
#[derive(Clone, Debug)]
pub struct Touched {
    pub instrument: String,
    pub kind: UpdateKind,
    identity: IdentityEvidence,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IdentityEvidence {
    Accepted,
    Changed,
    AwaitingEstablishedSnapshot,
}
impl Touched {
    pub(crate) fn identity(&self) -> IdentityEvidence {
        self.identity
    }
}

/// One subscription's book engine. Identity and terminal resolution survive ladder resets.
pub struct VenueBooks {
    recorder_flags: bool,
    mode: BookMode,
    books: BTreeMap<String, Book>,
    market_ids: BTreeMap<String, String>,
    conflicts: BTreeSet<String>,
    resolved: BTreeSet<String>,
    sequences: Sequences,
    pub complementary: ComplementaryRecovery,
}
impl VenueBooks {
    pub fn new(mode: BookMode, instruments: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let mut books = Self {
            recorder_flags: true,
            mode,
            books: BTreeMap::new(),
            market_ids: BTreeMap::new(),
            conflicts: BTreeSet::new(),
            resolved: BTreeSet::new(),
            sequences: Sequences::default(),
            complementary: ComplementaryRecovery::default(),
        };
        for id in instruments {
            books.subscribe(id.into());
        }
        books
    }
    // Managed health keeps terminal resolution independently of the latest
    // observation. A later snapshot replaces that observation's quality flags.
    pub(crate) fn managed(
        mode: BookMode,
        instruments: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let mut books = Self::new(mode, instruments);
        books.recorder_flags = false;
        books
    }
    pub fn subscribe(&mut self, id: String) {
        self.books
            .entry(id.clone())
            .or_insert_with(|| Book::new(self.mode, id));
    }
    pub fn get(&self, id: &str) -> Option<&Book> {
        self.books.get(id)
    }
    pub fn mode(&self) -> BookMode {
        self.mode
    }
    pub(crate) fn set_kalshi_price_mode(&mut self, use_yes_price: bool) {
        if let BookMode::Kalshi { policy, .. } = self.mode {
            let mode = BookMode::Kalshi {
                policy,
                use_yes_price,
            };
            if mode != self.mode {
                self.mode = mode;
                self.new_generation();
            }
        }
    }
    pub(crate) fn follows(&self, sid: i64, seq: i64) -> bool {
        self.sequences.follows(sid, seq)
    }
    pub fn instruments(&self) -> impl Iterator<Item = &str> {
        self.books.keys().map(String::as_str)
    }
    pub fn resolved(&self, id: &str) -> bool {
        self.resolved.contains(id)
    }
    pub fn reset_book(&mut self, id: &str) {
        if let Some(book) = self.books.get_mut(id) {
            *book = Book::new(self.mode, id);
        }
        self.complementary.forget(id);
    }
    pub fn reset(&mut self) {
        self.clear_connection(true);
    }
    /// After publishing the current observation, ask whether the recorder needs
    /// fresh baselines. Check complementary.due before applying later messages.
    pub fn recovery_due(&mut self, count: u64, now_ns: u64) -> bool {
        self.books.iter().any(|(id, b)| {
            self.conflicts.contains(id)
                || (b.integrity() == Integrity::Impaired
                    && (self.mode != BookMode::Polymarket
                        || !self.complementary.defer(id, count, now_ns)))
        }) || self.complementary.due(count, now_ns)
    }
    pub(crate) fn new_generation(&mut self) {
        self.clear_connection(false);
    }
    fn clear_connection(&mut self, retain_metadata: bool) {
        for (id, book) in &mut self.books {
            let mut fresh = Book::new(self.mode, id);
            if retain_metadata {
                fresh.market = book.market.take();
                fresh.tick = book.tick;
                fresh.minimum = book.minimum;
                fresh.flags.insert(F::Reconnect);
                if self.conflicts.contains(id) {
                    fresh.flags.insert(F::MarketIdentityChanged);
                }
                if self.resolved.contains(id) {
                    fresh.flags.insert(F::MarketResolved);
                }
            }
            *book = fresh;
        }
        self.sequences.clear();
        self.complementary = ComplementaryRecovery::default();
    }

    /// Apply one decoded message atomically at its batch position. A price_change may
    /// contain several changes to the same asset; publish only its final ladder.
    pub fn apply(
        &mut self,
        message: &serde_json::Value,
        count: u64,
    ) -> crate::Result<Vec<Touched>> {
        Ok(self.apply_event(decode(self.mode.venue(), message)?, count))
    }
    pub(crate) fn apply_event(&mut self, event: Event, count: u64) -> Vec<Touched> {
        let updates = match event {
            Event::Books(updates) => updates,
            Event::Lifecycle {
                market,
                instruments,
                resolved,
                timestamp,
            } => self
                .books
                .keys()
                .filter(|id| {
                    instruments.contains(id)
                        || market
                            .as_ref()
                            .is_some_and(|m| self.market_ids.get(*id) == Some(m))
                })
                .map(|instrument| Update {
                    instrument: instrument.clone(),
                    market: self
                        .market_ids
                        .get(instrument)
                        .cloned()
                        .or_else(|| market.clone()),
                    timestamp: timestamp.clone(),
                    sequence: None,
                    action: Action::Lifecycle { resolved },
                })
                .collect(),
            _ => return Vec::new(),
        };
        let mut order = Vec::new();
        let mut affected = BTreeMap::new();
        let mut gap = None;
        for u in &updates {
            let Some(book) = self.books.get_mut(&u.instrument) else {
                continue;
            };
            let id = &u.instrument;
            let (_, last, evidence) = affected.entry(id).or_insert_with(|| {
                order.push(id.clone());
                (
                    book.integrity() == Integrity::Intact,
                    u,
                    IdentityEvidence::Accepted,
                )
            });
            *last = u;
            if let Some(s) = u.sequence
                && (s
                    .sid
                    .zip(s.seq)
                    .is_none_or(|(sid, seq)| self.sequences.gap(sid, seq)))
            {
                gap = Some(s.sid);
            }
            let lifecycle = matches!(u.action, Action::Lifecycle { .. });
            // An established native id can only be verified by the incoming snapshot.
            if !lifecycle
                && let Some(known) = self.market_ids.get(id)
                && (u.market.as_ref().is_some_and(|m| m != known)
                    || (self.conflicts.contains(id)
                        && (!u.action.is_snapshot() || u.market.as_ref() != Some(known))))
            {
                *book = Book::new(self.mode, id);
                *evidence = if u.market.as_ref().is_some_and(|m| m != known) {
                    IdentityEvidence::Changed
                } else {
                    IdentityEvidence::AwaitingEstablishedSnapshot
                };
                book.market = Some(known.clone());
                book.fault(F::MarketIdentityChanged);
                self.conflicts.insert(id.clone());
                self.complementary.forget(id);
                continue;
            }
            if let Some(m) = &u.market {
                self.market_ids
                    .entry(id.clone())
                    .or_insert_with(|| m.clone());
            }
            if matches!(u.action, Action::Lifecycle { resolved: true }) {
                self.resolved.insert(id.clone());
            }
            book.apply(u);
            if u.action.is_snapshot() && book.integrity() == Integrity::Intact {
                self.conflicts.remove(id);
            }
            if self.recorder_flags && self.conflicts.contains(id) {
                book.fault(F::MarketIdentityChanged);
            }
            if self.recorder_flags && self.resolved.contains(id) {
                book.flags.insert(F::MarketResolved);
            }
        }
        let mut touched: Vec<_> = order
            .into_iter()
            .map(|id| {
                let (previous, last, identity) = affected[&id];
                let (kind, change) = match &last.action {
                    Action::Snapshot(_) => (UpdateKind::Snapshot, None),
                    Action::Absolute(c) => (UpdateKind::Delta, Some(c)),
                    Action::Relative { .. } => (UpdateKind::Delta, None),
                    Action::Tick(_) => (UpdateKind::Metadata, None),
                    Action::Lifecycle { .. } => (UpdateKind::Lifecycle, None),
                };
                if self.mode == BookMode::Polymarket {
                    self.complementary.observe(
                        &self.books[&id],
                        previous,
                        change,
                        kind == UpdateKind::Snapshot,
                        count,
                    );
                }
                Touched {
                    identity,
                    instrument: id,
                    kind,
                }
            })
            .collect();
        if let Some(sid) = gap {
            for (id, sibling) in &mut self.books {
                if sid.is_none() || sibling.sequence.sid == sid {
                    sibling.fault(F::SeqGap);
                    if !affected.contains_key(id) {
                        touched.push(Touched {
                            instrument: id.clone(),
                            kind: UpdateKind::Invalidated,
                            identity: IdentityEvidence::Accepted,
                        });
                    }
                }
            }
            touched.sort_by(|a, b| a.instrument.cmp(&b.instrument));
        }
        touched
    }
}
