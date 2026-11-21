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
