//! State and deadlines are driven exclusively by caller-provided monotonic nanoseconds.
use crate::decimal::ScaledDecimal as D;
use crate::wire::Change;
use std::collections::{BTreeMap, BTreeSet};

/// Ordered by published spelling. This is the vocabulary for book evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualityFlag {
    CrossedBook,
    DeltaBeforeSnapshot,
    EmptyAsk,
    EmptyBid,
    HashMismatch,
    MalformedBook,
    MarketIdentityChanged,
    MarketResolved,
    MissingSequence,
    NegativeSpread,
    NewMarket,
    NoInitialSnapshot,
    Reconnect,
    SeqGap,
    SidChanged,
}
impl QualityFlag {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CrossedBook => "crossed_book",
            Self::DeltaBeforeSnapshot => "delta_before_snapshot",
            Self::EmptyAsk => "empty_ask",
            Self::EmptyBid => "empty_bid",
            Self::HashMismatch => "hash_mismatch",
            Self::MalformedBook => "malformed_book",
            Self::MarketIdentityChanged => "market_identity_changed",
            Self::MarketResolved => "market_resolved",
            Self::MissingSequence => "missing_sequence",
            Self::NegativeSpread => "negative_spread",
            Self::NewMarket => "new_market",
            Self::NoInitialSnapshot => "no_initial_snapshot",
            Self::Reconnect => "reconnect",
            Self::SeqGap => "seq_gap",
            Self::SidChanged => "sid_changed",
        }
    }
    pub(crate) fn dynamic(self) -> bool {
        matches!(
            self,
            Self::CrossedBook | Self::NegativeSpread | Self::EmptyAsk | Self::EmptyBid
        )
    }
}
pub type Flags = BTreeSet<QualityFlag>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Integrity {
    AwaitingSnapshot,
    Impaired,
    Intact,
}

impl Integrity {
    pub fn from_observation(initialized: bool, flags: &Flags) -> Self {
        if !initialized {
            Self::AwaitingSnapshot
        } else if flags.iter().all(|f| {
            matches!(
                f,
                QualityFlag::EmptyBid
                    | QualityFlag::EmptyAsk
                    | QualityFlag::NewMarket
                    | QualityFlag::MarketResolved
            )
        }) {
            Self::Intact
        } else {
            Self::Impaired
        }
    }
}

/// Subscription sequence spans instruments. Forward jumps in a single instrument
/// alone do not establish a gap.
#[derive(Default, Debug)]
pub struct Sequences(BTreeMap<i64, i64>);
impl Sequences {
    pub fn follows(&self, sid: i64, seq: i64) -> bool {
        self.0
            .get(&sid)
            .is_none_or(|last| last.checked_add(1) == Some(seq))
    }
    pub fn gap(&mut self, sid: i64, seq: i64) -> bool {
        let gap = !self.follows(sid, seq);
        self.0.insert(sid, seq);
        gap
    }
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

pub const CORRECTIVE_DELTA_GRACE_NS: u64 = 250_000_000;
pub const MAX_CORRECTIVE_FOLLOWUP_MESSAGES: u64 = 16;

#[derive(Debug)]
struct Candidate {
    timestamp: String,
    hash: String,
    count: u64,
    deadline: Option<u64>,
}

/// Recorder's narrow exception for two complementary updates with one venue hash.
/// Grace starts when recovery is first considered, after any synchronous writes.
#[derive(Default, Debug)]
pub struct ComplementaryRecovery {
    pending: BTreeMap<String, Candidate>,
}
impl ComplementaryRecovery {
    pub(crate) fn observe(
        &mut self,
        book: &crate::book::Book,
        previously_intact: bool,
        change: Option<&Change>,
        snapshot: bool,
        count: u64,
    ) {
        let instrument = book.instrument();
        if book.integrity() == Integrity::Intact || snapshot {
            self.pending.remove(instrument);
        }
        let Some(change) = change else { return };
        let timestamp = change.parent_timestamp.as_deref().unwrap_or("");
        let hash = change.hash.as_deref().unwrap_or("");
        let locked_only =
            book.flags().len() == 1 && book.flags().contains(&QualityFlag::CrossedBook);
        if self
            .pending
            .get(instrument)
            .is_some_and(|p| p.timestamp != timestamp || p.hash != hash || !locked_only)
        {
            self.pending.remove(instrument);
        }
        if previously_intact
            && locked_only
            && change.corrective_side
            && !hash.is_empty()
            && !timestamp.is_empty()
            && change.price.is_some()
            && book.best_native_bid() == change.price
            && change
                .reported_bid
                .zip(change.reported_ask)
                .is_some_and(|(bid, ask)| bid < ask)
            && change.size.is_some_and(|v| v > D::ZERO)
        {
            self.pending
                .entry(instrument.to_owned())
                .or_insert_with(|| Candidate {
                    timestamp: timestamp.into(),
                    hash: hash.into(),
                    count,
                    deadline: None,
                });
        }
    }
    pub fn defer(&mut self, instrument: &str, count: u64, now_ns: u64) -> bool {
        let Some(p) = self.pending.get_mut(instrument) else {
            return false;
        };
        p.deadline
            .get_or_insert_with(|| now_ns.saturating_add(CORRECTIVE_DELTA_GRACE_NS));
        if Self::expired(p, count, now_ns) {
            self.pending.remove(instrument);
            false
        } else {
            true
        }
    }
    fn expired(p: &Candidate, count: u64, now_ns: u64) -> bool {
        p.deadline.is_some_and(|deadline| now_ns >= deadline)
            || count.saturating_sub(p.count) >= MAX_CORRECTIVE_FOLLOWUP_MESSAGES
    }
    pub fn due(&self, count: u64, now_ns: u64) -> bool {
        self.pending
            .values()
            .any(|p| Self::expired(p, count, now_ns))
    }
    pub fn next_deadline_ns(&self) -> Option<u64> {
        self.pending.values().filter_map(|p| p.deadline).min()
    }
    pub(crate) fn expired_instruments(&self, count: u64, now_ns: u64) -> Vec<String> {
        self.pending
            .iter()
            .filter(|(_, p)| Self::expired(p, count, now_ns))
            .map(|(id, _)| id.clone())
            .collect()
    }
    pub(crate) fn forget(&mut self, instrument: &str) {
        self.pending.remove(instrument);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryReason {
    Integrity,
    Crossed,
    SnapshotTimeout,
    Identity,
    RecoveryTimeout,
}
impl RecoveryReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Integrity => "book_integrity_failed",
            Self::Crossed => QualityFlag::CrossedBook.as_str(),
            Self::SnapshotTimeout => "snapshot_timeout",
            Self::Identity => QualityFlag::MarketIdentityChanged.as_str(),
            Self::RecoveryTimeout => "recovery_timeout",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Recovery {
    Grace {
        until: u64,
    },
    Pending(RecoveryReason),
    Sending {
        reason: RecoveryReason,
        answered: bool,
    },
    Waiting {
        reason: RecoveryReason,
        until: u64,
    },
    Cooldown {
        reason: RecoveryReason,
        until: u64,
    },
}
impl Recovery {
    pub fn reason(self) -> RecoveryReason {
        match self {
            Self::Grace { .. } => RecoveryReason::Crossed,
            Self::Pending(r)
            | Self::Sending { reason: r, .. }
            | Self::Waiting { reason: r, .. }
            | Self::Cooldown { reason: r, .. } => r,
        }
    }
    /// True when old depth must be discarded before a later frame is accepted.
    pub(crate) fn tick(&mut self, now: u64, timeout: u64) -> bool {
        match *self {
            Self::Grace { until } if now >= until => {
                *self = Self::Pending(RecoveryReason::Integrity);
                true
            }
            Self::Waiting { reason, until } if now >= until => {
                let reason = if reason == RecoveryReason::SnapshotTimeout {
                    reason
                } else {
                    RecoveryReason::RecoveryTimeout
                };
                *self = Self::Cooldown {
                    reason,
                    until: now.saturating_add(timeout.max(30_000_000_000)),
                };
                true
            }
            Self::Cooldown { reason, until } if now >= until => {
                *self = Self::Pending(reason);
                false
            }
            _ => false,
        }
    }
}
