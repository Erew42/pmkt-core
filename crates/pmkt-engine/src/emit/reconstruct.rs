use super::{QuoteBatchBuilder, QuoteBatches, Receive, Version, utc_text};
use crate::merge::{
    CopyMerge, MergeOptions, MergeStats, MergedMessage, SessionCoverage, SessionOccurrences, Stream,
};
use crate::{
    Error, Result, Venue,
    book::{BookMode, VenueBooks},
    capture::{CaptureItem, SealedChunk, list_session_chunks},
    health::Integrity,
    merge::received_us,
    wire::{self, Event},
};
use chrono::DateTime;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

// A sidecar-verified individual chunk can be replayed even when its predecessor
// is archived. Session inputs additionally verify inter-chunk links and gaps.
type CaptureFrames = Box<dyn Iterator<Item = Result<CaptureItem>>>;
fn input(path: &Path) -> Result<(Option<Venue>, CaptureFrames)> {
    if path.is_file() {
        let chunk = SealedChunk::open(path)?;
        return Ok((
            Some(chunk.manifest.venue),
            Box::new(chunk.frames(true)?.map(|f| f.map(CaptureItem::Frame))),
        ));
    }
    let chunks = list_session_chunks(path)?;
    Ok((
        chunks.chunks.first().map(|c| c.manifest.venue),
        Box::new(chunks.read(true)),
    ))
}

/// Merge redundant sessions in venue order and keep the winning frame's original
/// receive coordinates. Each input is one independently captured connection.
pub fn reconstruct_merged(
    venue: Venue,
    mut sessions: Vec<(String, String, PathBuf)>,
    version: Version,
    target_rows: usize,
    mut emit: impl FnMut(QuoteBatches) -> Result<()>,
    mut on_message: impl FnMut(&MergedMessage) -> Result<()>,
) -> Result<(MergeStats, Vec<SessionCoverage>)> {
    if target_rows == 0 {
        return Err(Error::Invalid("batch row target must be positive".into()));
    }
    sessions.sort();
    let mut streams = BTreeMap::<_, Vec<_>>::new();
    for (copy, connection, path) in sessions {
        let (source_venue, frames) = input(&path)?;
        if source_venue.is_some_and(|v| v != venue) {
            return Err(Error::Invalid(
                "cannot merge sessions from different venues".into(),
            ));
        }
        let session = if path.is_file() {
            path.parent().unwrap_or(&path)
        } else {
            &path
        };
        let name = session.file_name().and_then(|p| p.to_str()).unwrap_or("");
        let stream = Stream::new(copy, connection);
        streams
            .entry(stream.clone())
            .or_default()
            .push(SessionOccurrences::new(stream, name, frames));
    }
    let mode = match venue {
        Venue::Polymarket => BookMode::Polymarket,
        Venue::Kalshi => BookMode::kalshi(),
    };
    let mut books = VenueBooks::new(mode, std::iter::empty::<String>());
    let mut batch = QuoteBatchBuilder::new(version);
    let mut count = 0;
    let mut kalshi_basis = None;
    let stats = CopyMerge::new(venue, MergeOptions::default())?.merge(
        streams
            .values_mut()
            .map(|sessions| sessions.iter_mut().flatten()),
        |m| {
            count += 1;
            on_message(&m)?;
            if venue == Venue::Kalshi
                && matches!(m.kind.as_str(), "orderbook_snapshot" | "orderbook_delta")
            {
                for origin in &m.occurrences {
                    let basis = origin.use_yes_price;
                    if *kalshi_basis.get_or_insert(basis) != basis {
                        return Err(Error::Invalid(
                            "cannot merge Kalshi books on different price bases".into(),
                        ));
                    }
                }
                books.set_kalshi_price_mode(m.origin().use_yes_price);
            }
            let event = wire::decode(venue, &m.message)?;
            if let Event::Books(updates) = &event {
                for update in updates {
                    books.subscribe(update.instrument.clone());
                }
            }
            let origin = m.origin();
            let seq = i64::try_from(origin.frame_seq)
                .map_err(|_| Error::Invalid("frame sequence exceeds int64".into()))?;
            let mono = i64::try_from(origin.received_monotonic_ns)
                .map_err(|_| Error::Invalid("receive clock exceeds int64".into()))?;
            let mut reference = format!("{}:{seq}", origin.recording_id);
            if let Some(position) = origin.position {
                reference.push_str(&format!(":{position}"));
            }
            let receive = Receive {
                run_id: Some(&origin.recording_id),
                source: Some("ws"),
                utc: &origin.received_utc,
                monotonic_ns: Some(mono),
                local_sequence: Some(seq),
                raw_event_ref: Some(&reference),
                quote_age_ms: None,
            };
            for item in books.apply_event(event, count) {
                batch.push(
                    books.get(&item.instrument).expect("subscribed book"),
                    receive,
                    true,
                )?;
            }
            if batch.row_count() >= target_rows {
                emit(std::mem::replace(&mut batch, QuoteBatchBuilder::new(version)).finish()?)?;
            }
            Ok(())
        },
    )?;
    if batch.row_count() > 0 {
        emit(batch.finish()?)?;
    }
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
    mut emit: impl FnMut(QuoteBatches) -> Result<()>,
) -> Result<()> {
    if target_rows == 0 {
        return Err(Error::Invalid("batch row target must be positive".into()));
    }
    let (venue, frames) = input(path.as_ref())?;
    let Some(venue) = venue else {
        return Ok(());
    };
    let mode = match venue {
        Venue::Polymarket => BookMode::Polymarket,
        Venue::Kalshi => BookMode::kalshi(),
    };
    let mut books = VenueBooks::new(mode, std::iter::empty::<String>());
    let mut generation = None;
    let mut subscription_known = false;
    let mut batch = QuoteBatchBuilder::new(version);
    let mut count = 0;
    let mut last_utc: BTreeMap<String, i64> = BTreeMap::new();
    for item in frames {
        let f = match item? {
            CaptureItem::Gap(_) => {
                books.reset();
                continue;
            }
            CaptureItem::Frame(f) => f,
        };
        if generation != Some(f.connection_generation) {
            if generation.is_some() {
                books.reset();
            }
            generation = Some(f.connection_generation);
        }
        if f.frame_type == "subscription" && f.direction == "sent" {
            let command: Value = serde_json::from_str(&f.payload_raw)?;
            let instruments = match f.venue {
                Venue::Polymarket => command["assets_ids"].as_array(),
                Venue::Kalshi => {
                    let Some(mode) = wire::kalshi::subscription_basis(&command)? else {
                        continue;
                    };
                    books.set_kalshi_price_mode(mode);
                    command["params"]["market_tickers"].as_array()
                }
            };
            if let Some(ids) = instruments {
                for id in ids.iter().filter_map(Value::as_str) {
                    books.subscribe(id.to_owned());
                }
                subscription_known = true;
            }
            continue;
        }
        if (f.direction == "local"
            && matches!(f.frame_type.as_str(), "transport_error" | "disk_pause_gap"))
            || f.frame_type == "ws_close"
        {
            books.reset();
            continue;
        }
        if f.direction != "received" || f.frame_type != "text" {
            continue;
        }
        let us = received_us(&f.received_utc)?;
        let sequence = i64::try_from(f.local_frame_seq)
            .map_err(|_| Error::Invalid("frame sequence exceeds int64".into()))?;
        let mono = i64::try_from(f.received_monotonic_ns)
            .map_err(|_| Error::Invalid("receive clock exceeds int64".into()))?;
        for m in wire::live_messages(f.payload_raw.as_bytes())? {
            count += 1;
            // Only a recorded transport boundary invalidates unrelated books.
            // A silent corrective deadline belongs to its affected instrument.
            for id in books
                .complementary
                .expired_instruments(count, f.received_monotonic_ns)
            {
                books.reset_book(&id);
            }
            let event = wire::decode(f.venue, &m.value)?;
            if matches!(event, Event::SubscriptionError) {
                books.reset();
                continue;
            }
            if !subscription_known && let Event::Books(updates) = &event {
                for u in updates {
                    if books.get(&u.instrument).is_none() {
                        books.subscribe(u.instrument.clone());
                    }
                }
            }
            let reference = match m.position {
                Some(i) => format!("{}:{sequence}:{i}", f.recording_id),
                None => format!("{}:{sequence}", f.recording_id),
            };
            let touched = books.apply_event(event, count);
            let mut reset = vec![];
            for item in touched {
                let book = books.get(&item.instrument).unwrap();
                // Keep one causal receive coordinate for this book's top and depth rows.
                let clock = match last_utc.get(&item.instrument) {
                    Some(prev) if *prev >= us => prev
                        .checked_add(1)
                        .ok_or_else(|| Error::Invalid("receive timestamp overflow".into()))?,
                    _ => us,
                };
                let utc = if clock == us {
                    f.received_utc.clone()
                } else {
                    DateTime::from_timestamp_micros(clock)
                        .and_then(utc_text)
                        .ok_or_else(|| Error::Invalid("receive timestamp out of range".into()))?
                };
                let receive = Receive {
                    run_id: Some(&f.recording_id),
                    source: Some("ws"),
                    utc: &utc,
                    monotonic_ns: Some(mono),
                    local_sequence: Some(sequence),
                    raw_event_ref: Some(&reference),
                    // Capture records receipt, but not when the live quote was observed.
                    quote_age_ms: None,
                };
                batch.push(book, receive, true)?;
                last_utc.insert(item.instrument.clone(), clock);
                if f.venue == Venue::Polymarket
                    && book.integrity() == Integrity::Impaired
                    && !books
                        .complementary
                        .defer(&item.instrument, count, f.received_monotonic_ns)
                {
                    reset.push(item.instrument);
                }
            }
            for id in reset {
                books.reset_book(&id);
            }
            if batch.row_count() >= target_rows {
                emit(std::mem::replace(&mut batch, QuoteBatchBuilder::new(version)).finish()?)?;
            }
        }
    }
    if batch.row_count() > 0 {
        emit(batch.finish()?)?;
    }
    Ok(())
}
