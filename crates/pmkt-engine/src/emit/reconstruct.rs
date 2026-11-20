use super::{QuoteBatchBuilder, QuoteBatches, Receive, Version, utc_text};
use crate::{
    Error, Result, Venue,
    book::{BookMode, VenueBooks},
    capture::{CaptureItem, list_session_chunks},
    health::Integrity,
    merge::received_us,
    wire::{self, Event},
};
use chrono::DateTime;
use serde_json::Value;
use std::{collections::BTreeMap, path::Path};

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
    let chunks = list_session_chunks(path)?;
    let Some(first) = chunks.chunks.first() else {
        return Ok(());
    };
    let mode = match first.manifest.venue {
        Venue::Polymarket => BookMode::Polymarket,
        Venue::Kalshi => BookMode::kalshi(),
    };
    let mut books = VenueBooks::new(mode, std::iter::empty::<String>());
    let mut generation = None;
    let mut subscription_known = false;
    let mut batch = QuoteBatchBuilder::new(version);
    let mut count = 0;
    let mut last_utc: BTreeMap<String, i64> = BTreeMap::new();
    for item in chunks.read(true) {
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
                    let params = &command["params"];
                    if !params["channels"]
                        .as_array()
                        .is_some_and(|cs| cs.iter().any(|c| c == "orderbook_delta"))
                    {
                        continue;
                    }
                    let mode = params
                        .get("use_yes_price")
                        .map(|v| {
                            v.as_bool().ok_or_else(|| {
                                Error::Invalid("use_yes_price must be boolean".into())
                            })
                        })
                        .transpose()?
                        .unwrap_or(true);
                    books.set_kalshi_price_mode(mode);
                    params["market_tickers"].as_array()
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
