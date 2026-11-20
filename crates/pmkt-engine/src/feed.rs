//! Synchronous shard state for a transport owned by the caller.
use crate::{
    Error, Result, Venue,
    book::{BookMode, IdentityEvidence, SequenceCheck, Touched, UpdateKind, VenueBooks},
    decimal::ScaledDecimal,
    health::{
        CORRECTIVE_DELTA_GRACE_NS, Flags, Integrity, QualityFlag as F, Recovery, RecoveryReason,
    },
    wire::{self, Event, SourceTime},
};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug)]
pub struct FeedTimeouts {
    pub liveness_ns: u64,
    pub snapshot_ns: u64,
}
impl Default for FeedTimeouts {
    fn default() -> Self {
        Self {
            liveness_ns: 30_000_000_000,
            snapshot_ns: 30_000_000_000,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicBook {
    pub venue: Venue,
    pub instrument: String,
    pub market_id: Option<String>,
    pub shard_id: String,
    pub generation: u64,
    pub bids: Vec<(ScaledDecimal, ScaledDecimal)>,
    pub asks: Vec<(ScaledDecimal, ScaledDecimal)>,
    pub received_monotonic_ns: u64,
    pub source_timestamp: SourceTime,
    pub quality_flags: Flags,
    pub metadata_revision: u64,
    pub tick_size: Option<ScaledDecimal>,
    pub minimum_order_size: Option<ScaledDecimal>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicBookHealth {
    pub venue: Venue,
    pub instrument: String,
    pub shard_id: String,
    pub generation: u64,
    pub connection_state: &'static str,
    pub usable: bool,
    pub reason: Option<String>,
    pub liveness_age_ns: Option<u64>,
    pub book_update_age_ns: Option<u64>,
    pub reconnect_count: u64,
    pub error_count: u64,
    pub last_error: Option<String>,
    pub quality_flags: Flags,
    pub metadata_revision: u64,
}
#[derive(Clone, Debug)]
pub struct Observation {
    pub book: Option<PublicBook>,
    pub health: PublicBookHealth,
}

#[derive(Debug)]
enum Subscription {
    Polymarket,
    AwaitingAck(i64),
    Kalshi(i64),
}
#[derive(Debug)]
enum Connection {
    Initialized,
    Disconnected(String),
    Connected {
        since: u64,
        alive: u64,
        sub: Subscription,
    },
    Closed,
}

pub struct ShardFeed {
    id: String,
    venue: Venue,
    generation: Option<u64>,
    timeouts: FeedTimeouts,
    initial_deadline_checked: bool,
    connection: Connection,
    books: VenueBooks,
    count: u64,
    updates: BTreeMap<String, (u64, SourceTime)>,
    revisions: BTreeMap<String, u64>,
    recovery: BTreeMap<String, Recovery>,
    reconnect_count: u64,
    error_count: u64,
    last_error: Option<String>,
}
impl ShardFeed {
    pub fn new(
        venue: Venue,
        shard_id: impl Into<String>,
        instruments: impl IntoIterator<Item = impl Into<String>>,
        timeouts: FeedTimeouts,
    ) -> Result<Self> {
        if timeouts.liveness_ns == 0 || timeouts.snapshot_ns == 0 {
            return Err(Error::Invalid("feed timeouts must be positive".into()));
        }
        let mode = match venue {
            Venue::Polymarket => BookMode::Polymarket,
            Venue::Kalshi => BookMode::kalshi(),
        };
        Ok(Self {
            id: shard_id.into(),
            venue,
            generation: None,
            timeouts,
            initial_deadline_checked: false,
            connection: Connection::Initialized,
            books: VenueBooks::managed(mode, instruments),
            count: 0,
            updates: BTreeMap::new(),
            revisions: BTreeMap::new(),
            recovery: BTreeMap::new(),
            reconnect_count: 0,
            error_count: 0,
            last_error: None,
        })
    }
    pub fn connected(
        &mut self,
        generation: u64,
        command_id: Option<i64>,
        now_ns: u64,
    ) -> Result<()> {
        if self.generation.is_some_and(|old| generation <= old)
            || matches!(self.connection, Connection::Closed)
        {
            return Err(Error::Invalid("stale or closed connection".into()));
        }
        let sub = match self.venue {
            Venue::Polymarket => Subscription::Polymarket,
            Venue::Kalshi => Subscription::AwaitingAck(
                command_id
                    .ok_or_else(|| Error::Invalid("missing subscription command id".into()))?,
            ),
        };
        if self.generation.is_some() {
            self.reconnect_count += 1;
        }
        self.reset();
        self.revisions.clear();
        self.count = 0;
        self.initial_deadline_checked = false;
        self.generation = Some(generation);
        self.connection = Connection::Connected {
            since: now_ns,
            alive: now_ns,
            sub,
        };
        Ok(())
    }
    fn reset(&mut self) {
        self.books.new_generation();
        self.recovery.clear();
        self.updates.clear();
    }
    fn invalidate(&mut self, reason: &str) {
        self.reset();
        self.connection = Connection::Disconnected(reason.into());
        self.error_count += 1;
        self.last_error = Some(reason.into());
    }
    pub fn disconnected(&mut self, generation: u64, reason: &str) -> Result<()> {
        self.check_generation(generation)?;
        self.invalidate(reason);
        Ok(())
    }
    pub fn close(&mut self) {
        self.reset();
        self.connection = Connection::Closed;
    }
    fn check_generation(&self, generation: u64) -> Result<()> {
        if self.generation == Some(generation)
            && matches!(self.connection, Connection::Connected { .. })
        {
            Ok(())
        } else {
            Err(Error::Invalid("inactive connection generation".into()))
        }
    }
    fn check_liveness(&mut self, now: u64) {
        if let Connection::Connected {
            since,
            alive,
            ref sub,
        } = self.connection
        {
            if now.saturating_sub(alive) > self.timeouts.liveness_ns {
                self.invalidate("liveness_expired");
            } else if matches!(sub, Subscription::AwaitingAck(_))
                && now.saturating_sub(since) >= self.timeouts.snapshot_ns
            {
                self.invalidate("subscription_ack_timeout");
            }
        }
    }
    pub fn heartbeat(&mut self, generation: u64, now_ns: u64) -> Result<()> {
        self.check_generation(generation)?;
        self.check_liveness(now_ns);
        self.check_generation(generation)?;
        if let Connection::Connected { alive, .. } = &mut self.connection {
            *alive = now_ns;
        }
        Ok(())
    }
    fn deadlines(&mut self, now: u64) {
        for (id, phase) in &mut self.recovery {
            if phase.tick(now, self.timeouts.snapshot_ns) {
                self.books.reset_book(id);
                self.updates.remove(id);
            }
        }
    }
    pub fn poll_books(&mut self, now: u64) {
        self.deadlines(now);
        if !self.initial_deadline_checked
            && let Connection::Connected { since, .. } = self.connection
            && now.saturating_sub(since) >= self.timeouts.snapshot_ns
        {
            self.initial_deadline_checked = true;
            for id in self.books.instruments() {
                if !self.books.get(id).expect("subscribed book").initialized()
                    && !self.books.resolved(id)
                {
                    self.recovery
                        .entry(id.into())
                        .or_insert(Recovery::Pending(RecoveryReason::SnapshotTimeout));
                }
            }
        }
    }
    /// Starts one batch of resnapshot requests. Keep receiving while the caller sends.
    pub fn begin_requests(&mut self, generation: u64, now_ns: u64) -> Result<Vec<String>> {
        self.check_generation(generation)?;
        self.check_liveness(now_ns);
        self.check_generation(generation)?;
        self.poll_books(now_ns);
        if matches!(
            self.connection,
            Connection::Connected {
                sub: Subscription::AwaitingAck(_),
                ..
            }
        ) {
            return Ok(vec![]);
        }
        let mut ids = vec![];
        for (id, phase) in &mut self.recovery {
            if let Recovery::Pending(reason) = *phase {
                *phase = Recovery::Sending {
                    reason,
                    answered: false,
                };
                self.books.reset_book(id);
                self.updates.remove(id);
                ids.push(id.clone());
            }
        }
        Ok(ids)
    }
    pub fn finish_requests(&mut self, generation: u64, ids: &[String], now_ns: u64) -> Result<()> {
        self.check_generation(generation)?;
        self.check_liveness(now_ns);
        self.check_generation(generation)?;
        for id in ids {
            if let Some(Recovery::Sending { reason, answered }) = self.recovery.get(id).copied() {
                if answered {
                    self.recovery.remove(id);
                } else {
                    self.books.reset_book(id);
                    self.updates.remove(id);
                    self.recovery.insert(
                        id.clone(),
                        Recovery::Waiting {
                            reason,
                            until: now_ns.saturating_add(self.timeouts.snapshot_ns),
                        },
                    );
                }
            }
        }
        Ok(())
    }
    pub fn ingest(
        &mut self,
        shard_id: &str,
        generation: u64,
        bytes: &[u8],
        now_ns: u64,
    ) -> Result<Vec<Observation>> {
        self.ingest_with(shard_id, generation, bytes, now_ns, |feed, id| {
            feed.observation(id, now_ns)
        })
    }
    /// Apply the frame without detaching ladders. Consumers invalidate cached reads by ID.
    pub fn ingest_updates(
        &mut self,
        shard_id: &str,
        generation: u64,
        bytes: &[u8],
        now_ns: u64,
    ) -> Result<Vec<String>> {
        self.ingest_with(shard_id, generation, bytes, now_ns, |_, id| {
            Ok(id.to_owned())
        })
    }
    pub fn resolved(&self, id: &str) -> bool {
        self.books.resolved(id)
    }
    pub fn transport(&self) -> Option<(u64, Option<i64>)> {
        match self.connection {
            Connection::Connected { alive, ref sub, .. } => Some((
                alive,
                match sub {
                    Subscription::Kalshi(sid) => Some(*sid),
                    _ => None,
                },
            )),
            _ => None,
        }
    }
    fn ingest_with<T>(
        &mut self,
        shard_id: &str,
        generation: u64,
        bytes: &[u8],
        now_ns: u64,
        project: impl FnMut(&mut Self, &str) -> Result<T>,
    ) -> Result<Vec<T>> {
        if shard_id != self.id {
            return Err(Error::Invalid("wrong shard".into()));
        }
        self.check_generation(generation)?;
        self.check_liveness(now_ns);
        self.check_generation(generation)?;
        let result = self.apply_frame(bytes, now_ns, project);
        if let Err(error) = &result {
            self.invalidate(&error.to_string());
        }
        result
    }
    fn apply_frame<T>(
        &mut self,
        bytes: &[u8],
        now: u64,
        mut project: impl FnMut(&mut Self, &str) -> Result<T>,
    ) -> Result<Vec<T>> {
        let messages =
            wire::live_messages(bytes).map_err(|_| Error::Invalid("malformed_frame".into()))?;
        if self.venue == Venue::Polymarket
            && let Connection::Connected { alive, .. } = &mut self.connection
        {
            *alive = now;
        }
        let mut observations = vec![];
        for m in messages {
            self.count += 1;
            self.deadlines(now);
            let event = wire::decode(self.venue, &m.value)?;
            if matches!(event, Event::SubscriptionError) {
                return Err(Error::Invalid("subscription_error".into()));
            }
            if self.venue == Venue::Kalshi {
                let Connection::Connected { sub, alive, .. } = &mut self.connection else {
                    unreachable!()
                };
                match &event {
                    Event::Subscribed {
                        command_id,
                        sid,
                        book_channel,
                    } => {
                        let match_id = matches!(sub,Subscription::AwaitingAck(want) if Some(*want)==*command_id);
                        if !match_id || !book_channel || sid.is_none_or(|id| id < 0) {
                            return Err(Error::Invalid("invalid_subscription_ack".into()));
                        }
                        *sub = Subscription::Kalshi(sid.unwrap());
                        continue;
                    }
                    Event::Books(updates) => {
                        let update = &updates[0];
                        let s = update.sequence.unwrap_or_default();
                        let (Some(sid), Some(seq)) = (s.sid, s.seq) else {
                            return Err(Error::Invalid("invalid_book_identity_or_sequence".into()));
                        };
                        if !matches!(sub,Subscription::Kalshi(expected) if *expected==sid)
                            || seq < 0
                            || m.value["msg"]["market_ticker"].as_str()
                                != Some(update.instrument.as_str())
                            || self.books.get(&update.instrument).is_none()
                        {
                            return Err(Error::Invalid("invalid_book_identity_or_sequence".into()));
                        }
                        if !self.books.follows(sid, seq) {
                            return Err(Error::Invalid("subscription_sequence_gap".into()));
                        }
                        *alive = now;
                    }
                    _ => continue,
                }
            }
            for item in self
                .books
                .apply_event(event, self.count, SequenceCheck::Connection)
            {
                self.observe(&item, now);
                observations.push(project(self, &item.instrument)?);
            }
        }
        Ok(observations)
    }
    fn observe(&mut self, item: &Touched, now: u64) {
        let id = &item.instrument;
        if item.kind == UpdateKind::Metadata {
            *self.revisions.entry(id.clone()).or_default() += 1;
        }
        if item.identity() != IdentityEvidence::Accepted {
            self.books.reset_book(id);
            self.updates.remove(id);
            if item.identity() == IdentityEvidence::Changed && !self.books.resolved(id) {
                if let Some(Recovery::Sending { reason, answered }) = self.recovery.get_mut(id) {
                    *reason = RecoveryReason::Identity;
                    *answered = false;
                } else {
                    self.recovery
                        .insert(id.clone(), Recovery::Pending(RecoveryReason::Identity));
                }
            }
            return;
        }
        let book = self.books.get(id).expect("touched subscribed book");
        let intact = book.integrity() == Integrity::Intact;
        let crossed = book
            .flags()
            .iter()
            .all(|f| matches!(f, F::CrossedBook | F::NegativeSpread));
        if matches!(item.kind, UpdateKind::Snapshot | UpdateKind::Delta) {
            self.updates
                .insert(id.clone(), (now, book.timestamp().clone()));
        }
        if self.books.resolved(id) {
            self.recovery.remove(id);
            return;
        }
        match self.recovery.get_mut(id) {
            Some(Recovery::Sending { answered, .. }) => {
                if !intact {
                    *answered = false;
                } else if item.kind == UpdateKind::Snapshot {
                    *answered = true;
                }
            }
            Some(phase)
                if intact
                    && (item.kind == UpdateKind::Snapshot
                        || (matches!(phase, Recovery::Grace { .. })
                            && item.kind == UpdateKind::Delta)) =>
            {
                self.recovery.remove(id);
            }
            Some(phase @ Recovery::Grace { .. }) if !crossed => {
                *phase = Recovery::Pending(RecoveryReason::Integrity);
                self.books.reset_book(id);
                // The malformed depth update was received now, even though its
                // ladder and source timestamp were discarded before publication.
                if matches!(item.kind, UpdateKind::Snapshot | UpdateKind::Delta) {
                    self.updates
                        .insert(id.clone(), (now, SourceTime::default()));
                } else {
                    self.updates.remove(id);
                }
            }
            None if !intact => {
                let grace = self.venue == Venue::Polymarket
                    && item.kind == UpdateKind::Delta
                    && book.initialized()
                    && crossed;
                self.recovery.insert(
                    id.clone(),
                    if grace {
                        Recovery::Grace {
                            until: now.saturating_add(CORRECTIVE_DELTA_GRACE_NS),
                        }
                    } else {
                        Recovery::Pending(RecoveryReason::Integrity)
                    },
                );
            }
            _ => {}
        }
    }
    pub fn health(&mut self, id: &str, now_ns: u64) -> Result<PublicBookHealth> {
        self.check_liveness(now_ns);
        self.poll_books(now_ns);
        let book = self
            .books
            .get(id)
            .ok_or_else(|| Error::Invalid(format!("unknown instrument {id}")))?;
        let (connection_state, reason, age) = match &self.connection {
            Connection::Initialized => ("initialized", Some("not_started"), None),
            Connection::Closed => ("closed", Some("closed"), None),
            Connection::Disconnected(r) => ("disconnected", Some(r.as_str()), None),
            Connection::Connected { alive, .. } => {
                let reason = if self.books.resolved(id) {
                    Some(F::MarketResolved.as_str())
                } else if let Some(phase) = self.recovery.get(id) {
                    Some(phase.reason().as_str())
                } else {
                    match book.integrity() {
                        Integrity::Intact => None,
                        Integrity::Impaired => Some(RecoveryReason::Integrity.as_str()),
                        Integrity::AwaitingSnapshot => Some(F::NoInitialSnapshot.as_str()),
                    }
                };
                ("connected", reason, Some(now_ns.saturating_sub(*alive)))
            }
        };
        Ok(PublicBookHealth {
            venue: self.venue,
            instrument: id.into(),
            shard_id: self.id.clone(),
            generation: self.generation.unwrap_or(0),
            connection_state,
            usable: reason.is_none(),
            reason: reason.map(str::to_owned),
            liveness_age_ns: age,
            book_update_age_ns: self.updates.get(id).map(|(t, _)| now_ns.saturating_sub(*t)),
            reconnect_count: self.reconnect_count,
            error_count: self.error_count,
            last_error: self.last_error.clone(),
            quality_flags: book.flags().clone(),
            metadata_revision: self.revisions.get(id).copied().unwrap_or(0),
        })
    }
    pub fn observation(&mut self, id: &str, now_ns: u64) -> Result<Observation> {
        let health = self.health(id, now_ns)?;
        let detached = if health.usable {
            let b = self.books.get(id).expect("known instrument");
            let (left, right) = b.native_levels();
            let (received, timestamp) = &self.updates[id];
            Some(PublicBook {
                venue: self.venue,
                instrument: id.into(),
                market_id: b.market().map(str::to_owned),
                shard_id: self.id.clone(),
                generation: health.generation,
                bids: left.iter().rev().map(|(&p, &s)| (p, s)).collect(),
                asks: right.iter().map(|(&p, &s)| (p, s)).collect(),
                received_monotonic_ns: *received,
                source_timestamp: timestamp.clone(),
                quality_flags: health.quality_flags.clone(),
                metadata_revision: health.metadata_revision,
                tick_size: b.tick_size(),
                minimum_order_size: b.minimum_size(),
            })
        } else {
            None
        };
        Ok(Observation {
            book: detached,
            health,
        })
    }
}
