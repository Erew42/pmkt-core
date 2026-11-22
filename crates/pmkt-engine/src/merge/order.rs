//! Order evidence inside one (millisecond, market) group.
use super::{
    Entry, MergeOptions, MergeStats, OrderConflict, Origin, Stream, book_change, checkpoint,
};
use crate::{Error, Result, Venue};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
};

struct Constraints {
    successors: Vec<BTreeSet<usize>>,
    degree: Vec<usize>,
}
impl Constraints {
    fn new(n: usize) -> Self {
        Self {
            successors: vec![BTreeSet::new(); n],
            degree: vec![0; n],
        }
    }
    fn add(&mut self, before: usize, after: usize) -> bool {
        if self.successors[before].contains(&after) {
            return true;
        }
        // Only a path back to before would close a cycle.
        let mut stack = vec![after];
        let mut seen = BTreeSet::new();
        while let Some(node) = stack.pop() {
            if node == before {
                return false;
            }
            if seen.insert(node) {
                stack.extend(&self.successors[node]);
            }
        }
        self.successors[before].insert(after);
        self.degree[after] += 1;
        true
    }
}
pub(super) fn resolve(
    venue: Venue,
    options: &MergeOptions,
    stats: &mut MergeStats,
    entries: &[Entry],
    time: i64,
    scope: &str,
) -> Result<Vec<usize>> {
    if entries.len() == 1 {
        return Ok(vec![0]);
    }
    let mut streams: BTreeMap<&Stream, Vec<(usize, usize, &Origin)>> = BTreeMap::new();
    for (node, e) in entries.iter().enumerate() {
        for (index, origin) in &e.origins {
            streams
                .entry(&origin.stream)
                .or_default()
                .push((*index, node, origin));
        }
    }
    let mut ranked: Vec<_> = streams.into_iter().collect();
    ranked.sort_by_key(|(s, seq)| {
        (
            Reverse(seq.len()),
            options
                .preferred_copies
                .iter()
                .position(|c| c == &s.copy)
                .unwrap_or(usize::MAX),
            *s,
        )
    });
    let mut rank = vec![(usize::MAX, usize::MAX); entries.len()];
    for (r, (_, seq)) in ranked.iter_mut().enumerate() {
        seq.sort_unstable_by_key(|(index, ..)| *index);
        for (pos, (_, node, _)) in seq.iter().enumerate() {
            rank[*node] = rank[*node].min((r, pos));
        }
    }
    let mut graph = Constraints::new(entries.len());
    let snapshots: Vec<_> = entries
        .iter()
        .enumerate()
        .filter(|(_, e)| e.at.is_some() && e.key.kind == "orderbook_snapshot")
        .filter_map(|(i, e)| e.clock.map(|c| (i, c)))
        .collect();
    // An included delta precedes its snapshot; a greater clock follows it.
    for &(snap, c) in &snapshots {
        for (i, e) in entries.iter().enumerate() {
            if e.key.kind == "orderbook_delta"
                && let Some(d) = e.clock
                && d.epoch == c.epoch
            {
                let (a, b) = if d.value <= c.value {
                    (i, snap)
                } else {
                    (snap, i)
                };
                graph.add(a, b);
            }
        }
        for &(other, d) in &snapshots {
            if c.epoch == d.epoch && c.value < d.value {
                graph.add(snap, other);
            }
        }
    }
    let is_book = |i: usize| book_change(venue, &entries[i].key.kind);
    for books_only in [true, false] {
        for (_, seq) in &ranked {
            let mut previous = None;
            for &(_, node, origin) in seq {
                if books_only && !is_book(node) {
                    continue;
                }
                if let Some(p) = previous
                    && (books_only || !(is_book(p) && is_book(node)))
                    && !graph.add(p, node)
                {
                    if options.strict_order {
                        return Err(Error::Invalid(format!(
                            "streams disagree on order for {scope} at {time}, origin {origin:?}"
                        )));
                    }
                    if books_only {
                        stats.book_order_conflicts += 1;
                    } else {
                        stats.other_order_conflicts += 1;
                    }
                    if stats.conflict_examples.len() < 20 {
                        stats.conflict_examples.push(OrderConflict {
                            scope: scope.into(),
                            order_ms: time,
                            origin: origin.clone(),
                            book_changes: books_only,
                        });
                    }
                }
                previous = Some(node);
            }
        }
    }
    let priority = |i: usize| Reverse((checkpoint(venue, &entries[i].key.kind), rank[i], i));
    let mut ready: BinaryHeap<_> = graph
        .degree
        .iter()
        .enumerate()
        .filter(|(_, n)| **n == 0)
        .map(|(i, _)| priority(i))
        .collect();
    let mut order = Vec::with_capacity(entries.len());
    while let Some(Reverse((_, _, node))) = ready.pop() {
        stats.ambiguous_choices += u64::from(!ready.is_empty());
        order.push(node);
        for &after in &graph.successors[node] {
            graph.degree[after] -= 1;
            if graph.degree[after] == 0 {
                ready.push(priority(after));
            }
        }
    }
    Ok(order)
}

/// A reconnect can deliver a cached checkpoint after a later change stamped
/// in the same millisecond. Move only that checkpoint, and only when a second
/// recorder identifies its position in an uninterrupted sequence for the token.
pub(super) fn place_cached_books(
    order: &mut [usize],
    entries: &[Entry],
    sessions: &[super::SessionCoverage],
) {
    for (snap, entry) in entries.iter().enumerate() {
        if entry.reconnect
            && entry.key.kind == "book"
            && entry.key.venue_ms.is_some()
            && let Some((old, new)) = cached_position(order, entries, sessions, snap)
        {
            order[new..=old].rotate_right(1);
        }
    }
}

fn cached_position(
    order: &[usize],
    entries: &[Entry],
    sessions: &[super::SessionCoverage],
    snap: usize,
) -> Option<(usize, usize)> {
    let checkpoint = &entries[snap];
    let [(_, received)] = checkpoint.origins.as_slice() else {
        return None;
    };
    let asset = checkpoint.message["asset_id"].as_str()?;
    let hash = checkpoint.message["hash"]
        .as_str()
        .filter(|h| !h.is_empty())?;

    // A hash identifies an asset's state, not a frame. Keep multi-asset
    // envelopes atomic. A repeated anchor or several transitions for this
    // asset in one envelope do not identify a unique insertion point.
    let mut changes = Vec::new();
    let mut anchor = None;
    for (pos, &node) in order.iter().enumerate() {
        let entry = &entries[node];
        if node != snap && entry.key.kind == "book" && entry.message["asset_id"] == asset {
            return None;
        }
        if entry.key.kind != "price_change" {
            continue;
        }
        let mut legs = entry.message["price_changes"]
            .as_array()?
            .iter()
            .filter(|l| l["asset_id"] == asset);
        if let Some(leg) = legs.next() {
            let change_hash = leg["hash"].as_str().filter(|h| !h.is_empty())?;
            if legs.next().is_some() || entry.key.venue_ms != checkpoint.key.venue_ms {
                return None;
            }
            if change_hash == hash && anchor.replace(changes.len()).is_some() {
                return None;
            }
            changes.push((pos, entry));
        }
    }
    let anchor = anchor?;
    let (next, later_change) = *changes.get(anchor + 1)?;
    let old = order.iter().position(|&n| n == snap)?;
    if old <= next {
        return None;
    }

    // Only reception across a broken connection may be superseded. Keep all
    // ordering evidence from the checkpoint's own uninterrupted interval.
    if order[next..old].iter().any(|&n| {
        entries[n]
            .origins
            .iter()
            .any(|(_, o)| o.receipt_period() == received.receipt_period())
    }) {
        return None;
    }
    let anchor_change = changes[anchor].1;
    let proven = anchor_change.origins.iter().any(|(_, before)| {
        before.receipt_period().bridges(sessions, received)
            && later_change.origins.iter().any(|(_, after)| {
                before.receipt_period() == after.receipt_period()
                    && (before.frame_seq, before.position) < (after.frame_seq, after.position)
                    && after.received_us <= received.received_us
            })
    });
    proven.then_some((old, next))
}
