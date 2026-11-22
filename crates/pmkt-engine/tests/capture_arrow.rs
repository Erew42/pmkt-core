use arrow_array::{
    Array, BooleanArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
};
use arrow_schema::DataType;
mod capture_fixture;
use capture_fixture::{column, manifest, name, raw, root, seal, sidecar};
use pmkt_engine::{
    BookMode, Venue, VenueBooks,
    capture::{CaptureItem, SealedChunk, inspect_session, list_session_chunks, read_session},
    emit::{
        QuoteBatchBuilder, QuoteBatches, Receive, Version, reconstruct_merged, reconstruct_session,
    },
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
fn revise(root: &Path, i: u64, f: impl FnOnce(&mut Value)) {
    let p = manifest(root, i);
    let mut m: Value = serde_json::from_slice(&fs::read(&p).unwrap()).unwrap();
    f(&mut m);
    fs::write(p, m.to_string()).unwrap();
}
#[test]
fn verifies_links_and_reports_all_incomplete_artifacts_and_gaps() {
    let root = root();
    let first = seal(
        &root,
        1,
        &[
            raw(1, 1, "kalshi", json!({})),
            raw(2, 1, "kalshi", json!({})),
        ],
        None,
    );
    seal(&root, 2, &[raw(3, 2, "kalshi", json!({}))], Some(&first));
    seal(&root, 4, &[raw(6, 2, "kalshi", json!({}))], Some("missing"));
    for file in [
        "chunk_000003.jsonl.gz",
        "chunk_000005.jsonl.gz.manifest.json",
        "chunk_000006.jsonl.gz.part",
        "READY.json.tmp",
    ] {
        fs::write(root.join(file), b"").unwrap();
    }
    let discovery = list_session_chunks(&root).unwrap();
    assert_eq!(discovery.incomplete.len(), 4);
    assert_eq!(discovery.chunks.len(), 3);
    assert_eq!(
        discovery.chunks[0].manifest.extra["future_field"],
        "retained"
    );
    let events = discovery
        .read(true)
        .collect::<pmkt_engine::Result<Vec<_>>>()
        .unwrap();
    let CaptureItem::Gap(g) = &events[3] else {
        panic!("missing marker before following frame")
    };
    assert_eq!(
        (g.first_missing, g.last_missing, g.missing_frame_count()),
        (3, 3, 2)
    );
    assert!(matches!(&events[4],CaptureItem::Frame(f) if f.local_frame_seq==6));
    let inspection = inspect_session(&root).unwrap();
    assert_eq!(inspection.frame_count, 4);
    assert_eq!(inspection.chunk_gaps.len(), 1);
    revise(&root, 2, |m| m["previous_chunk_sha256"] = json!("wrong"));
    assert!(
        inspect_session(&root)
            .unwrap_err()
            .to_string()
            .contains("adjacent")
    );
}
#[test]
fn sha_size_checked_before_first_frame_and_endpoints_at_eof() {
    for (field, val) in [
        ("sha256", json!("bad")),
        ("size_bytes", json!(0)),
        ("frame_count", json!(3)),
        ("last_frame_seq", json!(7)),
    ] {
        let root = root();
        seal(&root, 1, &[raw(1, 1, "polymarket", json!({}))], None);
        revise(&root, 1, |m| m[field] = val);
        let mut events = read_session(&root, true).unwrap();
        if matches!(field, "frame_count" | "last_frame_seq") {
            assert!(events.next().unwrap().is_ok());
        }
        assert!(events.next().unwrap().is_err());
        assert!(events.next().is_none());
        assert_eq!(
            read_session(&root, false)
                .unwrap()
                .collect::<pmkt_engine::Result<Vec<_>>>()
                .unwrap()
                .len(),
            1
        );
    }
}
#[test]
fn missing_prefix_invalid_predecessor_and_duplicate_seq_are_checked() {
    let path = root();
    seal(&path, 3, &[raw(4, 1, "kalshi", json!({}))], Some("absent"));
    let items = read_session(&path, true)
        .unwrap()
        .collect::<pmkt_engine::Result<Vec<_>>>()
        .unwrap();
    assert!(
        matches!(&items[0],CaptureItem::Gap(g) if g.first_missing==1 && g.last_missing==2 && g.missing_frame_count()==3)
    );
    revise(&path, 3, |m| m["previous_chunk_sha256"] = Value::Null);
    assert!(inspect_session(&path).is_err());
    revise(&path, 3, |m| {
        m.as_object_mut().unwrap().remove("previous_chunk_sha256");
    });
    assert!(list_session_chunks(&path).is_err());
    let path = root();
    seal(
        &path,
        1,
        &[
            raw(1, 1, "kalshi", json!({})),
            raw(1, 1, "kalshi", json!({})),
        ],
        None,
    );
    assert!(
        inspect_session(&path)
            .unwrap_err()
            .to_string()
            .contains("consecutive")
    );
}

#[test]
fn four_arrow_schemas_follow_published_columns_dtypes_and_nullability() {
    let dictionary = include_str!("../../../docs/data_dictionary.md");
    for version in [Version::V1, Version::V2] {
        let batches = QuoteBatchBuilder::new(version).finish().unwrap();
        for (table, batch) in [("topbook", batches.topbook), ("depth", batches.depth)] {
            let marker = format!("`{table}.v1` columns:");
            let source = dictionary
                .split_once(&marker)
                .unwrap()
                .1
                .split_once("```text")
                .unwrap()
                .1
                .split_once("```")
                .unwrap()
                .0;
            let mut names: Vec<_> = source.split(',').map(str::trim).collect();
            if version == Version::V2 {
                names.push("book_integrity_valid");
            }
            let schema = batch.schema();
            assert_eq!(schema.fields.len(), names.len());
            for (name, f) in names.iter().zip(schema.fields.iter()) {
                assert_eq!(*name, f.name());
                let required = matches!(
                    *name,
                    "schema_version"
                        | "exchange"
                        | "instrument_id"
                        | "received_at_utc"
                        | "valid_state"
                        | "book_integrity_valid"
                        | "side"
                        | "level_index"
                );
                assert_ne!(f.is_nullable(), required, "{table}/{name}");
                let dtype = if name.ends_with("_dollars")
                    || name.ends_with("_contracts")
                    || *name == "spread_bps"
                {
                    DataType::Float64
                } else {
                    match *name {
                        "level_index"
                        | "local_sequence"
                        | "venue_sequence"
                        | "venue_sid"
                        | "quote_age_ms"
                        | "received_at_monotonic_ns" => DataType::Int64,
                        "is_delta" | "valid_state" | "book_integrity_valid" => DataType::Boolean,
                        "quality_flags" => DataType::List(Arc::new(arrow_schema::Field::new(
                            "item",
                            DataType::Utf8,
                            true,
                        ))),
                        _ => DataType::Utf8,
                    }
                };
                assert_eq!(&dtype, f.data_type(), "{table}/{name}");
            }
        }
    }
}
#[test]
fn raw_to_arrow_gap_invalidation_and_fresh_snapshot_coordinates() {
    let root = root();
    let snap = |seq| json!({"type":"orderbook_snapshot","sid":1,"seq":seq,"msg":{"market_id":"M","market_ticker":"K","yes":[["0.4","2"]],"no":[["0.7","4"]],"ts_ms":1735689600100i64}});
    seal(&root, 1, &[raw(1, 1, "kalshi", snap(1))], None);
    let d = json!({"type":"orderbook_delta","sid":1,"seq":4,"msg":{"market_ticker":"K","side":"yes","price":"0.4","delta":"2"}});
    seal(
        &root,
        3,
        &[raw(4, 1, "kalshi", json!([d, snap(5)]))],
        Some("absent"),
    );
    let mut batches = vec![];
    reconstruct_session(&root, Version::V2, 1, |batch| {
        batches.push(batch);
        Ok(())
    })
    .unwrap();
    assert_eq!(batches.len(), 3);
    let mut times = vec![];
    for (i, b) in batches.iter().enumerate() {
        assert_eq!(b.topbook.num_rows(), 2);
        assert_eq!(b.depth.num_rows(), if i == 1 { 0 } else { 2 });
        assert_eq!(
            column::<BooleanArray>(&b.topbook, "book_integrity_valid").value(0),
            i != 1
        );
        assert_eq!(
            column::<StringArray>(&b.topbook, "instrument_id").value(1),
            "K:NO"
        );
        times.push(column::<StringArray>(&b.topbook, "received_at_utc").value(0));
        for utc in column::<StringArray>(&b.depth, "received_at_utc") {
            assert_eq!(utc, Some(times[i]));
        }
        let reference = ["run:1", "run:4:0", "run:4:1"][i];
        for raw_ref in column::<StringArray>(&b.topbook, "raw_event_ref") {
            assert_eq!(raw_ref, Some(reference));
        }
        assert_eq!(
            column::<Int64Array>(&b.topbook, "local_sequence").value(0),
            if i == 0 { 1 } else { 4 }
        );
    }
    assert_eq!(
        times,
        [
            "2025-01-01T00:00:00+00:00",
            "2025-01-01T00:00:00.000001+00:00",
            "2025-01-01T00:00:00.000002+00:00"
        ]
    );
    let top = &batches[2].topbook;
    assert_eq!(
        column::<StringArray>(top, "best_bid_source").value(1),
        "complement_derived"
    );
    assert_eq!(
        column::<Float64Array>(top, "best_bid_dollars").value(1),
        0.3
    );
    assert_eq!(
        column::<Float64Array>(top, "ask_size_contracts").value(1),
        2.0
    );
    assert_eq!(
        column::<StringArray>(top, "exchange_ts_utc").value(0),
        "2025-01-01T00:00:00.100000+00:00"
    );
}

fn assert_book_health(batch: &RecordBatch, expected: &[&str], valid: bool, intact: bool) {
    for row in 0..batch.num_rows() {
        let flags = column::<ListArray>(batch, "quality_flags").value(row);
        let flags = flags.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            flags.iter().map(Option::unwrap).collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            column::<BooleanArray>(batch, "valid_state").value(row),
            valid
        );
        assert_eq!(
            column::<BooleanArray>(batch, "book_integrity_valid").value(row),
            intact
        );
    }
}

fn merged_poly(frames: &[Value], covering_copy: &[Value]) -> Vec<QuoteBatches> {
    let mut sessions = vec![];
    for (copy, data) in [("a", frames), ("b", covering_copy)] {
        if !data.is_empty() {
            let path = root();
            seal(&path, 1, data, None);
            sessions.push((copy.into(), "conn".into(), path));
        }
    }
    let mut batches = vec![];
    reconstruct_merged(
        Venue::Polymarket,
        sessions,
        Version::V2,
        1,
        |b| {
            batches.push(b);
            Ok(())
        },
        |_| Ok(()),
    )
    .unwrap();
    batches
}

fn poly_snapshot(ms: i64) -> Value {
    json!({"event_type":"book","market":"M","asset_id":"P","timestamp":ms,
           "bids":[["0.4","3"]],"asks":[["0.6","2"]]})
}

#[test]
fn merged_control_only_copy_can_bridge_a_transport_hole() {
    let ms = 1735689600000;
    let book = raw(1, 1, "polymarket", poly_snapshot(ms));
    let mut gap = raw(2, 1, "polymarket", json!({}));
    gap["frame_type"] = json!("disk_pause_gap");
    gap["direction"] = json!("local");
    let mut change = raw(
        3,
        1,
        "polymarket",
        json!({
            "event_type":"price_change","market":"M","timestamp":ms+1,
            "price_changes":[{"asset_id":"P","side":"BUY","price":"0.4","size":"5"}]
        }),
    );
    change["received_utc"] = json!("2025-01-01T00:00:01+00:00");
    let mut pong = book.clone();
    pong["frame_type"] = json!("ws_pong");
    pong["direction"] = json!("received_control_base64");
    let mut later = pong.clone();
    later["local_frame_seq"] = json!(2);
    later["received_utc"] = change["received_utc"].clone();
    let primary = [book, gap, change];
    for (other, valid) in [(vec![], false), (vec![pong, later], true)] {
        let batches = merged_poly(&primary, &other);
        assert_eq!(batches.len(), 2);
        let after = &batches[1];
        assert_eq!(
            column::<BooleanArray>(&after.topbook, "valid_state").value(0),
            valid
        );
        assert_eq!(after.depth.num_rows(), if valid { 2 } else { 0 });
        if !valid {
            assert_book_health(
                &after.topbook,
                &[
                    "delta_before_snapshot",
                    "empty_ask",
                    "no_initial_snapshot",
                    "reconnect",
                ],
                false,
                false,
            );
        }
    }
}

#[test]
fn merged_observations_bump_the_clock_across_batches_and_clock_regressions() {
    let ms = 1735689600000;
    let mut first = raw(
        1,
        1,
        "polymarket",
        json!([poly_snapshot(ms), poly_snapshot(ms + 1)]),
    );
    first["received_utc"] = json!("2025-01-01T00:00:00.500000+00:00");
    let last = raw(2, 1, "polymarket", poly_snapshot(ms + 2));
    let batches = merged_poly(&[first, last], &[]);
    assert_eq!(batches.len(), 3);
    for (i, batch) in batches.iter().enumerate() {
        let utc = format!("2025-01-01T00:00:00.{:06}+00:00", 500000 + i);
        assert_eq!(
            column::<StringArray>(&batch.topbook, "received_at_utc").value(0),
            utc
        );
        assert_eq!(batch.depth.num_rows(), 2);
        assert!(
            column::<StringArray>(&batch.depth, "received_at_utc")
                .iter()
                .all(|t| t == Some(&utc))
        );
    }
}

#[test]
fn merged_resolution_before_first_snapshot_is_terminal() {
    let ms = 1735689600000;
    let resolved = raw(
        1,
        1,
        "polymarket",
        json!({
            "event_type":"market_resolved","market":"M","assets_ids":["P"],"timestamp":ms
        }),
    );
    let book = raw(2, 1, "polymarket", poly_snapshot(ms + 1));
    let batches = merged_poly(&[resolved, book], &[]);
    assert_eq!(batches.len(), 1);
    assert_book_health(&batches[0].topbook, &["market_resolved"], false, true);
    assert_book_health(&batches[0].depth, &["market_resolved"], false, true);
}

#[test]
fn a_clock_rollback_cannot_move_a_post_gap_delta_into_the_old_ladder() {
    let ms = 1735689600000;
    let snapshot = raw(1, 1, "polymarket", poly_snapshot(ms));
    let mut gap = raw(2, 1, "polymarket", json!({}));
    gap["frame_type"] = json!("transport_error");
    gap["direction"] = json!("local");
    let delta = |seq, timestamp| {
        raw(
            seq,
            1,
            "polymarket",
            json!({
                "event_type":"price_change","asset_id":"P","timestamp":timestamp,
                "price_changes":[{"asset_id":"P","side":"BUY","price":"0.4","size":"5"}]
            }),
        )
    };
    let mut reopened = delta(3, ms + 2);
    reopened["received_utc"] = json!("2025-01-01T00:00:01+00:00");
    let rollback = delta(4, ms + 1);
    let mut pong = snapshot.clone();
    pong["frame_type"] = json!("ws_pong");
    pong["direction"] = json!("received_control_base64");
    let batches = merged_poly(&[snapshot, gap, reopened, rollback], &[pong]);
    let rows: Vec<_> = batches.iter().map(|b| b.depth.num_rows()).collect();
    assert_eq!(rows, [2, 0, 0]);
}

#[test]
fn subscribed_but_silent_books_record_the_same_gap_in_both_replays() {
    let path = root();
    let mut sub = raw(1, 1, "polymarket", json!({"assets_ids":["P","silent"]}));
    sub["direction"] = json!("sent");
    sub["frame_type"] = json!("subscription");
    let ms = 1735689600000_i64;
    let first = raw(2, 1, "polymarket", poly_snapshot(ms));
    let mut gap = raw(3, 1, "polymarket", json!({}));
    gap["direction"] = json!("local");
    gap["frame_type"] = json!("transport_error");
    let mut delta = raw(
        4,
        1,
        "polymarket",
        json!({"event_type":"price_change", "market":"M","timestamp":ms+1,
        "price_changes":[{"asset_id":"silent","price":".3","size":"2","side":"BUY"}]}),
    );
    delta["received_utc"] = json!("2025-01-01T00:00:01+00:00");
    seal(&path, 1, &[sub, first, gap, delta], None);
    let mut file = vec![];
    reconstruct_session(&path, Version::V2, 1, |b| {
        file.push(b.topbook);
        Ok(())
    })
    .unwrap();
    assert_book_health(
        &file[1],
        &[
            "delta_before_snapshot",
            "empty_ask",
            "no_initial_snapshot",
            "reconnect",
        ],
        false,
        false,
    );
    let mut merged = vec![];
    reconstruct_merged(
        Venue::Polymarket,
        vec![("a".into(), "c".into(), path)],
        Version::V2,
        1,
        |b| {
            merged.push(b.topbook);
            Ok(())
        },
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(merged, file);
}

fn kalshi_sequence_source(
    copy: &str,
    sid: i64,
    offset: usize,
    micros: [i64; 3],
    venue_ms: [i64; 3],
) -> (String, String, PathBuf) {
    let path = root();
    let mut frames = vec![];
    for (i, (micros, ms)) in micros.into_iter().zip(venue_ms).enumerate() {
        let sequence = offset + i + 1;
        let ms = 1_735_689_600_000 + ms;
        let msg = if i == 0 {
            json!({"type":"orderbook_snapshot","sid":sid,"seq":sequence,
                    "sending_ts_ms":ms,"msg":{"market_ticker":"K","clock":1,"yes":[[".3","1"]],"no":[[".6","1"]]}})
        } else {
            json!({"type":"orderbook_delta","sid":sid,"seq":sequence,"sending_ts_ms":ms+1,"msg":{"market_ticker":"K",
                    "clock":i+1,"ts_ms":ms,"side":"yes","price":".3","delta":"1"}})
        };
        let mut f = raw(i as u64 + 1, 1, "kalshi", msg);
        f["received_utc"] = json!(format!("2025-01-01T00:00:00.{micros:06}+00:00"));
        frames.push(f);
    }
    seal(&path, 1, &frames, None);
    (copy.into(), "conn".into(), path)
}

fn assert_kalshi_sequence_rows(sessions: Vec<(String, String, PathBuf)>, coords: [(i64, i64); 3]) {
    let mut expected = coords.into_iter().enumerate();
    reconstruct_merged(
        Venue::Kalshi,
        sessions,
        Version::V2,
        1,
        |batch| {
            let (i, (sid, seq)) = expected.next().unwrap();
            for rows in [&batch.topbook, &batch.depth] {
                assert_eq!(rows.num_rows(), 2);
                assert_book_health(rows, &[], true, true);
                assert_eq!(column::<Int64Array>(rows, "venue_sid").values(), &[sid; 2]);
                assert_eq!(
                    column::<Int64Array>(rows, "venue_sequence").values(),
                    &[seq; 2]
                );
            }
            assert_eq!(
                column::<Float64Array>(&batch.depth, "size_contracts").value(0),
                (i + 1) as f64
            );
            Ok(())
        },
        |_| Ok(()),
    )
    .unwrap();
    assert!(expected.next().is_none());
}

#[test]
fn alternating_kalshi_copies_keep_healthy_ladders_and_winning_coordinates() {
    let times = [0, 10, 20];
    let a = kalshi_sequence_source("a", 7, 100, [0, 11000, 20000], times);
    let b = kalshi_sequence_source("b", 81, 0, [1000, 10000, 21000], times);
    assert_kalshi_sequence_rows(vec![a, b], [(7, 101), (81, 2), (7, 103)]);
}

#[test]
fn venue_order_cannot_create_a_gap_in_a_continuous_kalshi_copy() {
    let a = kalshi_sequence_source("a", 7, 100, [0, 11000, 20000], [0, 20, 10]);
    assert_kalshi_sequence_rows(vec![a.clone()], [(7, 101), (7, 102), (7, 103)]);
    let b = kalshi_sequence_source("b", 81, 0, [1000, 12000, 21000], [0, 20, 10]);
    assert_kalshi_sequence_rows(vec![a, b], [(7, 101), (7, 103), (7, 102)]);
}

#[test]
fn a_gap_inside_one_frame_resets_before_its_first_merged_message() {
    let a = root();
    let start = raw(
        1,
        1,
        "kalshi",
        json!({"type":"orderbook_snapshot","sid":7,"seq":1,
        "msg":{"market_ticker":"K","ts_ms":100,"yes":[[".3","1"]],"no":[[".6","1"]]}}),
    );
    let delta = |seq| {
        json!({"type":"orderbook_delta","sid":7,"seq":seq,
        "msg":{"market_ticker":"K","ts_ms":100+seq,"side":"yes","price":".3","delta":"1"}})
    };
    let mut batched = raw(2, 1, "kalshi", json!([delta(2), delta(4)]));
    batched["received_utc"] = json!("2025-01-01T00:00:01+00:00");
    seal(&a, 1, &[start.clone(), batched], None);
    let b = root();
    let mut pong = start;
    pong["direction"] = json!("received_control_base64");
    pong["frame_type"] = json!("ws_pong");
    seal(&b, 1, &[pong], None);
    let mut sessions = vec![("a".into(), "c".into(), a)];
    for copies in 1..=2 {
        if copies == 2 {
            sessions.push(("b".into(), "c".into(), b.clone()));
        }
        let mut depth = Vec::new();
        reconstruct_merged(
            Venue::Kalshi,
            sessions.clone(),
            Version::V2,
            1,
            |batch| {
                depth.push(batch.depth.num_rows());
                Ok(())
            },
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(depth, if copies == 1 { [2, 2, 2] } else { [2, 0, 0] });
    }
}

#[test]
fn a_single_kalshi_source_keeps_depth_and_flags_the_sequence_jump() {
    let frames: Vec<_> = [1, 2, 4, 5, 6]
        .into_iter()
        .enumerate()
        .map(|(i, seq)| {
            let msg = if i == 0 {
                json!({"type":"orderbook_snapshot","sid":1,"seq":seq,"msg":{"market_ticker":"K",
                "yes_dollars_fp":[[".3","3"],[".2","4"]],"no_dollars_fp":[[".7","5"],[".8","3"]]}})
            } else {
                json!({"type":"orderbook_delta","sid":1,"seq":seq,"msg":{"market_ticker":"K",
                "side":"yes","price_dollars":".3","delta_fp":"1"}})
            };
            timed(i as u64 + 1, "kalshi", i as i64, i as u64, msg)
        })
        .collect();
    let mut observations = 0;
    let (stats, coverage) = reconstruct_merged(
        Venue::Kalshi,
        vec![session("a", &frames)],
        Version::V2,
        1,
        |batch| {
            let valid = observations < 2;
            for rows in [&batch.topbook, &batch.depth] {
                assert_book_health(rows, if valid { &[] } else { &["seq_gap"] }, valid, valid);
            }
            assert_eq!(batch.depth.num_rows(), 4);
            observations += 1;
            Ok(())
        },
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(observations, 5);
    assert_eq!(stats.messages, 5);
    assert_eq!(coverage[0].counts.sequence_gaps, 1);
}

#[test]
fn a_cached_reconnect_snapshot_precedes_its_deltas_in_a_single_recording() {
    let ms = 1_735_689_600_000;
    let mut other = poly_snapshot(ms + 10);
    other["asset_id"] = json!("other");
    let change = json!({"event_type":"price_change","market":"M","timestamp":ms+20,
        "price_changes":[{"asset_id":"P","side":"SELL","price":"0.9","size":"100"}]});
    // The new connection supplies a cached book timestamped before traffic
    // on the old one. Its untouched 0.6 ask must survive the 0.9 update.
    let frames = [
        raw(1, 1, "polymarket", other),
        raw(2, 2, "polymarket", poly_snapshot(ms + 1)),
        raw(3, 2, "polymarket", change),
    ];
    let batches = merged_poly(&frames, &[]);
    let last = batches.last().unwrap();
    assert_book_health(&last.topbook, &[], true, true);
    assert_eq!(
        column::<Float64Array>(&last.topbook, "best_ask_dollars").value(0),
        0.6
    );
    assert_eq!(last.depth.num_rows(), 3);
}

#[test]
fn one_recording_keeps_venue_time_inversions_and_repeats_after_the_watermark() {
    let ms = 1_735_689_600_000;
    let first = poly_snapshot(ms);
    let mut second = poly_snapshot(ms - 50);
    second["bids"] = json!([["0.5", "1"]]);
    let mut late_repeat = raw(3, 1, "polymarket", first.clone());
    late_repeat["received_utc"] = json!("2025-01-01T00:03:00+00:00");
    let frames = [
        raw(1, 1, "polymarket", first),
        raw(2, 1, "polymarket", second),
        late_repeat,
    ];
    let mut refs = vec![];
    let mut bids = vec![];
    let (stats, _) = reconstruct_merged(
        Venue::Polymarket,
        vec![session("a", &frames)],
        Version::V2,
        1,
        |batch| {
            bids.push(column::<Float64Array>(&batch.topbook, "best_bid_dollars").value(0));
            Ok(())
        },
        |m| {
            refs.push(m.origin().frame_seq);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(refs, [1, 2, 3]);
    assert_eq!(bids, [0.4, 0.5, 0.4]);
    assert_eq!((stats.messages, stats.repeats_after_emit), (3, 0));
}

#[test]
fn file_order_evidence_precedes_each_observation_across_a_receipt_clock_rollback() {
    let ms = 1_735_689_600_000;
    let book = poly_snapshot(ms);
    let frames = [
        timed(1, "polymarket", 30_000, 1, book.clone()),
        timed(2, "polymarket", 10_000, 2, json!([book, book])),
        timed(3, "polymarket", 20_000, 3, book),
    ];
    let messages = std::cell::Cell::new(0);
    let mut batches = 0;
    let (stats, _) = reconstruct_merged(
        Venue::Polymarket,
        vec![session("a", &frames)],
        Version::V2,
        1,
        |_| {
            batches += 1;
            assert_eq!(batches, messages.get());
            Ok(())
        },
        |msg| {
            assert_eq!(msg.order_ms, ms + 30);
            assert_eq!(msg.venue_ts_ms, Some(ms));
            messages.set(messages.get() + 1);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(batches, 4);
    assert_eq!(stats.clock_steps_back, 3);
}

#[test]
fn one_conflated_kalshi_session_can_be_replayed_without_ordering_across_copies() {
    let sub = json!({"params":{"channels":["orderbook_delta"],"market_tickers":["K"],"update_interval_ms":500}});
    let book = json!({"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"K",
        "yes_dollars_fp":[[".3","3"]],"no_dollars_fp":[[".7","5"]]}});
    let frames = [
        kind(raw(1, 1, "kalshi", sub), "subscription", "sent"),
        raw(2, 1, "kalshi", book),
    ];
    let a = session("a", &frames);
    let mut single = vec![];
    reconstruct_session(&a.2, Version::V2, 1, |b| {
        single.push(b.topbook);
        Ok(())
    })
    .unwrap();
    assert_eq!(merged_topbooks(Venue::Kalshi, vec![a.clone()]), single);
    assert!(
        reconstruct_merged(
            Venue::Kalshi,
            vec![a, session("b", &frames)],
            Version::V2,
            1,
            |_| Ok(()),
            |_| Ok(())
        )
        .unwrap_err()
        .to_string()
        .contains("conflated")
    );
}

#[test]
fn kalshi_outcomes_share_native_book_health() {
    let path = root();
    let snapshot = |seq, no: Value| {
        raw(
            seq,
            1,
            "kalshi",
            json!({
                "type":"orderbook_snapshot","sid":1,"seq":seq,
                "msg":{"market_ticker":"K","yes_dollars_fp":[["0.40","3"]],"no_dollars_fp":no}
            }),
        )
    };
    seal(
        &path,
        1,
        &[
            snapshot(1, json!([["0.40", "2"]])),
            snapshot(2, json!([["0.39", "2"]])),
            snapshot(3, json!([])),
        ],
        None,
    );
    let mut batches = vec![];
    reconstruct_session(&path, Version::V2, 1, |b| {
        batches.push(b);
        Ok(())
    })
    .unwrap();
    let cases: [(&[&str], bool, bool, usize); 3] = [
        (&[], true, true, 2),
        (&["crossed_book", "negative_spread"], false, false, 2),
        (&["empty_ask"], false, true, 1),
    ];
    assert_eq!(batches.len(), cases.len());
    for (batch, (flags, valid, intact, depth_rows)) in batches.iter().zip(cases) {
        assert_eq!(batch.topbook.num_rows(), 2);
        assert_eq!(batch.depth.num_rows(), depth_rows);
        assert_book_health(&batch.topbook, flags, valid, intact);
        assert_book_health(&batch.depth, flags, valid, intact);
    }
}

#[test]
fn polymarket_replay_emits_depth_once_initialized_and_has_no_live_quote_age() {
    let path = root();
    let delta = json!({"event_type":"price_change","price_changes":[
        {"asset_id":"P","side":"BUY","price":"0.4","size":"3"}
    ]});
    let snapshot =
        json!({"event_type":"book","asset_id":"P","bids":[["0.4","3"]],"asks":[["0.6","2"]]});
    let tick = json!({"event_type":"tick_size_change","asset_id":"P","new_tick_size":"0.01"});
    seal(
        &path,
        1,
        &[raw(1, 1, "polymarket", json!([delta, snapshot, tick]))],
        None,
    );
    let mut batches = vec![];
    reconstruct_session(&path, Version::V2, 1, |b| {
        batches.push(b);
        Ok(())
    })
    .unwrap();
    assert_eq!(batches.len(), 3);
    assert_book_health(
        &batches[0].topbook,
        &["delta_before_snapshot", "empty_ask", "no_initial_snapshot"],
        false,
        false,
    );
    for (i, b) in batches.iter().enumerate() {
        assert_eq!(b.depth.num_rows(), if i == 0 { 0 } else { 2 });
        assert_eq!(b.topbook.num_rows(), 1);
        assert!(column::<Int64Array>(&b.topbook, "quote_age_ms").is_null(0));
    }
}
#[test]
fn one_sided_and_empty_native_ladders_have_no_synthetic_depth() {
    let mut books = VenueBooks::new(BookMode::Polymarket, ["A"]);
    books.apply(&json!({"event_type":"book","asset_id":"A","asks":[],"bids":[["0.31","1.2"],["0.32","3.2"]]}),1).unwrap();
    let mut b = QuoteBatchBuilder::new(Version::V1);
    b.push(
        books.get("A").unwrap(),
        Receive {
            run_id: None,
            source: None,
            utc: "2025-01-01T00:00:00Z",
            monotonic_ns: None,
            local_sequence: None,
            raw_event_ref: None,
            quote_age_ms: None,
        },
        true,
    )
    .unwrap();
    let b = b.finish().unwrap();
    assert!(column::<Float64Array>(&b.topbook, "best_ask_dollars").is_null(0));
    assert_eq!(
        column::<StringArray>(&b.topbook, "best_ask_source").value(0),
        "missing"
    );
    assert_eq!(
        column::<ListArray>(&b.topbook, "quality_flags").value_length(0),
        1
    );
    assert_eq!(b.depth.num_rows(), 2);
    assert_eq!(
        column::<Float64Array>(&b.depth, "cumulative_size_contracts").value(1),
        4.4
    );
    assert_eq!(
        column::<Float64Array>(&b.depth, "price_dollars").value(0),
        0.32
    );
}

#[test]
fn kalshi_reconstruction_reads_subscription_price_basis_and_limits_instruments() {
    let path = root();
    let mut sub = raw(
        1,
        1,
        "kalshi",
        json!({"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":["K"],"use_yes_price":false}}),
    );
    sub["direction"] = json!("sent");
    sub["frame_type"] = json!("subscription");
    let snap = |ticker| json!({"type":"orderbook_snapshot","sid":2,"seq":1,"msg":{"market_ticker":ticker,"yes":[["0.4","2"]],"no":[["0.35","3"]]}});
    seal(
        &path,
        1,
        &[
            sub,
            raw(2, 1, "kalshi", snap("J")),
            raw(3, 1, "kalshi", snap("K")),
        ],
        None,
    );
    let mut count = 0;
    reconstruct_session(&path, Version::V2, 16, |b| {
        count += b.topbook.num_rows();
        assert_eq!(
            column::<Float64Array>(&b.topbook, "best_ask_dollars").value(0),
            0.65
        );
        assert_eq!(
            column::<StringArray>(&b.topbook, "best_ask_source").value(0),
            "complement_derived"
        );
        assert_eq!(
            column::<StringArray>(&b.topbook, "instrument_id").value(1),
            "K:NO"
        );
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 2);
}
#[test]
fn reconstructs_published_venue_sample_envelopes() {
    for (venue, frames, run) in [
        ("polymarket", 205, "a51260f6-b1e2-4c79-bb41-82926544d227"),
        ("kalshi", 203, "8b84542b-f29f-4361-9fac-29b65913f55a"),
    ] {
        let root = root();
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
            "../../tests/fixtures/raw_capture/{venue}_00.jsonl.gz"
        ));
        fs::copy(fixture, root.join(name(1))).unwrap();
        sidecar(&root, 1, venue, run, 1..=frames, None);
        let pair = SealedChunk::open(&root.join(name(1))).unwrap();
        assert_eq!(
            pair.frames(true)
                .unwrap()
                .collect::<pmkt_engine::Result<Vec<_>>>()
                .unwrap()
                .len(),
            frames as usize
        );
        let mut rows = 0;
        let mut intact = 0;
        reconstruct_session(&root, Version::V2, 400, |b| {
            rows += b.topbook.num_rows();
            intact += column::<BooleanArray>(&b.topbook, "book_integrity_valid").true_count();
            Ok(())
        })
        .unwrap();
        assert!(rows > 0);
        if venue == "kalshi" {
            assert_eq!(intact, 0, "mid-session sample contains no Kalshi baseline");
        } else {
            assert!(intact > 0);
        }
    }
}

// A frame of one recorded connection, `micros` after the fixture epoch, stamped
// by a host whose monotonic clock reads `mono_ns`.
fn timed(seq: u64, venue: &str, micros: i64, mono_ns: u64, msg: Value) -> Value {
    let mut f = raw(seq, 1, venue, msg);
    f["received_utc"] = json!(format!("2025-01-01T00:00:00.{micros:06}+00:00"));
    f["received_monotonic_ns"] = json!(mono_ns);
    f
}

fn kind(mut frame: Value, frame_type: &str, direction: &str) -> Value {
    frame["frame_type"] = json!(frame_type);
    frame["direction"] = json!(direction);
    frame
}

fn session(copy: &str, frames: &[Value]) -> (String, String, PathBuf) {
    let path = root();
    seal(&path, 1, frames, None);
    (copy.into(), "conn".into(), path)
}

fn merged_topbooks(venue: Venue, sessions: Vec<(String, String, PathBuf)>) -> Vec<RecordBatch> {
    let mut rows = vec![];
    reconstruct_merged(
        venue,
        sessions,
        Version::V2,
        1,
        |b| {
            rows.push(b.topbook);
            Ok(())
        },
        |_| Ok(()),
    )
    .unwrap();
    rows
}

// The last published top-of-book row of one instrument: (valid, best bid, best ask).
fn last_quote(rows: &[RecordBatch], instrument: &str) -> (bool, Option<f64>, Option<f64>) {
    let b = rows
        .iter()
        .rev()
        .find(|b| column::<StringArray>(b, "instrument_id").value(0) == instrument)
        .unwrap();
    let price = |name: &str| {
        let c = column::<Float64Array>(b, name);
        (!c.is_null(0)).then(|| c.value(0))
    };
    (
        column::<BooleanArray>(b, "valid_state").value(0),
        price("best_bid_dollars"),
        price("best_ask_dollars"),
    )
}

fn poly_book(asset: &str, market: &str) -> Value {
    json!({"event_type":"book","market":market,"asset_id":asset,"timestamp":"1700000000000","hash":"h1",
        "bids":[{"price":"0.4","size":"1"}],"asks":[{"price":"0.5","size":"3"},{"price":"0.7","size":"4"}]})
}

#[test]
fn a_copy_that_never_subscribed_to_an_instrument_cannot_cover_its_gap() {
    let subscribe = |micros, asset: &str| {
        let sub = timed(1, "polymarket", micros, 1, json!({"assets_ids":[asset]}));
        kind(sub, "subscription", "sent")
    };
    let change = json!({"event_type":"price_change","market":"M","timestamp":"1700000000001",
        "price_changes":[{"asset_id":"X","side":"BUY","price":"0.42","size":"2"}]});
    let x = [
        subscribe(10, "X"),
        timed(2, "polymarket", 100, 2, poly_book("X", "M")),
        kind(
            timed(3, "polymarket", 1200, 3, json!({})),
            "disk_pause_gap",
            "local",
        ),
        timed(4, "polymarket", 3000, 4, change),
    ];
    // Healthy and continuous, but subscribed to a different instrument.
    let y = [
        subscribe(9, "Y"),
        timed(2, "polymarket", 90, 2, poly_book("Y", "N")),
        kind(
            timed(3, "polymarket", 3100, 3, json!({})),
            "ws_pong",
            "received_control_base64",
        ),
    ];
    let alone = merged_topbooks(Venue::Polymarket, vec![session("a", &x)]);
    assert_eq!(last_quote(&alone, "X"), (false, Some(0.42), None));
    let with_y = merged_topbooks(Venue::Polymarket, vec![session("a", &x), session("b", &y)]);
    assert_eq!(last_quote(&with_y, "X"), (false, Some(0.42), None));
    // Y was never interrupted, so the other instrument's gap must not touch it.
    assert_eq!(last_quote(&with_y, "Y"), (true, Some(0.4), Some(0.5)));
}

#[test]
fn the_corrective_delta_deadline_ignores_the_other_recorders_monotonic_clock() {
    let change = |side, size| {
        json!({"event_type":"price_change","market":"M","timestamp":"1700000000001",
            "price_changes":[{"asset_id":"X","side":side,"price":"0.5","size":size,
                "hash":"h2","best_bid":"0.5","best_ask":"0.7"}]})
    };
    let messages = [poly_book("X", "M"), change("BUY", "2"), change("SELL", "0")];
    // Two hosts: the second one's monotonic clock started two hours earlier.
    let host = |copy, micros: [i64; 3], offset_ns: u64| {
        let frames: Vec<_> = (1..)
            .zip(micros.into_iter().zip(&messages))
            .map(|(seq, (us, m))| {
                let mono = 1_000_000_000 + us as u64 * 1000 + offset_ns;
                timed(seq, "polymarket", us, mono, m.clone())
            })
            .collect();
        session(copy, &frames)
    };
    let a = host("a", [100, 1100, 2000], 0);
    let b = host("b", [200, 1200, 1900], 7_200_000_000_000);
    // The winning copy changes to b for the corrective delta, 0.8 ms after the crossing.
    for sources in [vec![a.clone()], vec![b.clone()], vec![a, b]] {
        let rows = merged_topbooks(Venue::Polymarket, sources);
        assert_eq!(last_quote(&rows, "X"), (true, Some(0.5), Some(0.7)));
    }
}

#[test]
fn one_recording_uses_its_monotonic_corrective_deadline_across_a_wall_clock_step() {
    let delta = |side, size| {
        json!({"event_type":"price_change","market":"M","timestamp":"1700000000001","price_changes":[{
        "asset_id":"X","side":side,"price":"0.5","size":size,
        "hash":"h2","best_bid":"0.5","best_ask":"0.7"}]})
    };
    let frames = [
        timed(1, "polymarket", 0, 0, poly_book("X", "M")),
        timed(2, "polymarket", 1_000, 1_000_000, delta("BUY", "2")),
        timed(3, "polymarket", 901_000, 2_000_000, delta("SELL", "0")),
    ];
    let rows = merged_topbooks(Venue::Polymarket, vec![session("a", &frames)]);
    assert_eq!(last_quote(&rows, "X"), (true, Some(0.5), Some(0.7)));
}

fn kalshi_channel_session(error: Option<Value>) -> Vec<Value> {
    // Recorder commands: 1 book, 2 trade, 3 lifecycle.
    let command = |id: u64, channel: &str| {
        let mut params = json!({"channels":[channel],"market_tickers":["K"]});
        if channel == "orderbook_delta" {
            params["use_yes_price"] = json!(true);
        }
        let msg = json!({"id":id,"cmd":"subscribe","params":params});
        kind(
            timed(id, "kalshi", id as i64, id, msg),
            "subscription",
            "sent",
        )
    };
    let book = json!({"type":"orderbook_snapshot","sid":7,"seq":1,
        "msg":{"market_ticker":"K","yes_dollars_fp":[["0.40","10"]],"no_dollars_fp":[["0.60","20"]]}});
    let delta = json!({"type":"orderbook_delta","sid":7,"seq":2,
        "msg":{"market_ticker":"K","side":"yes","price_dollars":"0.40","delta_fp":"2"}});
    let mut frames = vec![
        command(1, "orderbook_delta"),
        command(2, "trade"),
        command(3, "market_lifecycle_v2"),
        timed(4, "kalshi", 1000, 1000, book),
    ];
    frames.extend(error.map(|e| timed(5, "kalshi", 2000, 2000, e)));
    frames.push(timed(6, "kalshi", 3000, 3000, delta));
    for (i, f) in frames.iter_mut().enumerate() {
        f["local_frame_seq"] = json!(i as u64 + 1);
    }
    frames
}

fn kalshi_yes_row(batches: &[RecordBatch]) -> (bool, f64) {
    let b = batches
        .iter()
        .rev()
        .find(|b| column::<StringArray>(b, "outcome").value(0) == "YES")
        .unwrap();
    (
        column::<BooleanArray>(b, "valid_state").value(0),
        column::<Float64Array>(b, "bid_size_contracts").value(0),
    )
}

#[test]
fn only_an_error_on_the_book_subscription_resets_kalshi_depth() {
    let error = |id: Option<u64>| {
        let mut e = json!({"type":"error","msg":{"code":9,"msg":"Unknown channel"}});
        if let Some(id) = id {
            e["id"] = json!(id);
        }
        e
    };
    // (error, depth survives): the trade and lifecycle commands are 2 and 3.
    for (error, survives) in [
        (None, true),
        (Some(error(Some(3))), true),
        (Some(error(Some(2))), true),
        (Some(error(Some(1))), false),
        (Some(error(None)), false),
    ] {
        let label = format!("{error:?}");
        let frames = kalshi_channel_session(error);
        let expected = if survives { (true, 12.0) } else { (false, 2.0) };
        let path = root();
        seal(&path, 1, &frames, None);
        let mut single = vec![];
        reconstruct_session(&path, Version::V2, 1, |b| {
            single.push(b.topbook);
            Ok(())
        })
        .unwrap();
        assert_eq!(kalshi_yes_row(&single), expected, "single, error {label}");
        let merged = merged_topbooks(Venue::Kalshi, vec![("a".into(), "c".into(), path)]);
        assert_eq!(kalshi_yes_row(&merged), expected, "merged, error {label}");
    }
}

#[test]
fn archived_subscriptions_do_not_pool_coverage_across_shards() {
    let dir_a = root();
    let dir_b = root();
    let book_a = json!({"event_type":"book","asset_id":"a","market":"market-a","timestamp":"1700000000001",
        "bids":[{"price":"0.30","size":"7"},{"price":"0.20","size":"3"}],"asks":[{"price":"0.60","size":"10"}]});
    let change_a = json!({"event_type":"price_change","market":"market-a","timestamp":"1700000000003",
        "price_changes":[{"asset_id":"a","side":"SELL","price":"0.60","size":"5"}]});
    let book_b = json!({"event_type":"book","asset_id":"b","market":"market-b","timestamp":"1700000000000",
        "bids":[{"price":"0.30","size":"7"}],"asks":[{"price":"0.60","size":"10"}]});
    let change_b = json!({"event_type":"price_change","market":"market-b","timestamp":"1700000000004",
        "price_changes":[{"asset_id":"b","side":"BUY","price":"0.20","size":"1"}]});

    // Shard A has an archived subscription (chunk 1 removed) and a missing chunk 3.
    let sub_a = kind(
        timed(
            1,
            "polymarket",
            1,
            1,
            json!({"type":"market","assets_ids":["a"]}),
        ),
        "subscription",
        "sent",
    );
    let sub_b = kind(
        timed(
            1,
            "polymarket",
            1,
            1,
            json!({"type":"market","assets_ids":["b"]}),
        ),
        "subscription",
        "sent",
    );
    let full_a = root();
    let full_b = root();
    let sha1_a = seal(&full_a, 1, &[sub_a], None);
    let sha2_a = seal(
        &full_a,
        2,
        &[timed(2, "polymarket", 1000, 1000, book_a)],
        Some(&sha1_a),
    );
    let sha3_a = seal(
        &full_a,
        3,
        &[timed(
            3,
            "polymarket",
            2000,
            2000,
            json!({"event_type":"price_change","market":"market-a","timestamp":"1700000000002",
                "price_changes":[{"asset_id":"a","side":"BUY","price":"0.30","size":"0"}]}),
        )],
        Some(&sha2_a),
    );
    seal(
        &full_a,
        4,
        &[timed(4, "polymarket", 3000, 3000, change_a)],
        Some(&sha3_a),
    );
    let sha1_b = seal(&full_b, 1, &[sub_b], None);
    seal(
        &full_b,
        2,
        &[
            timed(2, "polymarket", 100, 100, book_b),
            timed(3, "polymarket", 4000, 4000, change_b),
        ],
        Some(&sha1_b),
    );
    for idx in [2_u32, 4] {
        for ext in ["jsonl.gz", "jsonl.gz.manifest.json"] {
            let name = format!("chunk_{idx:06}.{ext}");
            std::fs::copy(full_a.join(&name), dir_a.join(&name)).unwrap();
        }
    }
    for ext in ["jsonl.gz", "jsonl.gz.manifest.json"] {
        let name = format!("chunk_000002.{ext}");
        std::fs::copy(full_b.join(&name), dir_b.join(&name)).unwrap();
    }

    let merged = merged_topbooks(
        Venue::Polymarket,
        vec![
            ("A".into(), "shard-A".into(), dir_a),
            ("B".into(), "shard-B".into(), dir_b),
        ],
    );
    let post_gap = merged
        .iter()
        .find(|b| {
            column::<StringArray>(b, "instrument_id").value(0) == "a"
                && column::<StringArray>(b, "raw_event_ref")
                    .value(0)
                    .ends_with(":4")
        })
        .expect("post-gap observation for token a");
    assert!(
        !column::<BooleanArray>(post_gap, "valid_state").value(0),
        "unrelated shard with archived subscription must not mask chunk gap"
    );
    let healthy_b = merged
        .iter()
        .find(|b| {
            column::<StringArray>(b, "instrument_id").value(0) == "b"
                && column::<StringArray>(b, "raw_event_ref")
                    .value(0)
                    .ends_with(":3")
        })
        .expect("healthy shard b update");
    assert!(
        column::<BooleanArray>(healthy_b, "valid_state").value(0),
        "shard a's chunk gap must not reset unrelated healthy shard b"
    );
}

#[test]
fn rejected_kalshi_book_subscription_does_not_mask_peer_seq_gap() {
    let sub_book = |ticker: &str| {
        kind(
            timed(
                1,
                "kalshi",
                0,
                0,
                json!({"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":[ticker],"use_yes_price":true}}),
            ),
            "subscription",
            "sent",
        )
    };
    let sub_trade = kind(
        timed(
            2,
            "kalshi",
            10,
            10,
            json!({"id":2,"cmd":"subscribe","params":{"channels":["trade"],"market_tickers":["X"]}}),
        ),
        "subscription",
        "sent",
    );
    let book = json!({"type":"orderbook_snapshot","sid":7,"seq":1,"sending_ts_ms":1700000000002i64,
        "msg":{"market_ticker":"X","yes_dollars_fp":[["0.40","10"]],"no_dollars_fp":[["0.60","20"]]}});
    let delta_gap = json!({"type":"orderbook_delta","sid":7,"seq":3,"sending_ts_ms":1700000000005i64,
        "msg":{"ts_ms":1700000000005i64,"market_ticker":"X","side":"yes","price_dollars":"0.40","delta_fp":"2"}});
    let error = json!({"id":1,"type":"error","msg":{"code":9,"msg":"Unknown channel"}});
    let trade = json!({"type":"trade","sid":8,"seq":1,"sending_ts_ms":1700000000007i64,
        "msg":{"market_ticker":"X","ts_ms":1700000000007i64,"yes_price_dollars":"0.4","count_fp":"1","taker_side":"yes"}});

    let dir_a = root();
    let dir_b = root();
    seal(
        &dir_a,
        1,
        &[
            sub_book("X"),
            timed(2, "kalshi", 2000, 2000, book),
            timed(3, "kalshi", 5000, 5000, delta_gap),
        ],
        None,
    );
    seal(
        &dir_b,
        1,
        &[
            sub_book("X"),
            sub_trade.clone(),
            timed(3, "kalshi", 1000, 1000, error.clone()),
            timed(4, "kalshi", 7000, 7000, trade),
        ],
        None,
    );

    let dir_control = root();
    seal(
        &dir_control,
        1,
        &[
            sub_book("X"),
            sub_trade,
            timed(3, "kalshi", 1000, 1000, error),
        ],
        None,
    );
    let control = merged_topbooks(
        Venue::Kalshi,
        vec![
            ("a".into(), "conn".into(), dir_a.clone()),
            ("short".into(), "conn".into(), dir_control),
        ],
    );
    let merged = merged_topbooks(
        Venue::Kalshi,
        vec![
            ("a".into(), "conn".into(), dir_a),
            ("b".into(), "conn".into(), dir_b),
        ],
    );
    assert!(!kalshi_yes_row(&control).0);
    assert_eq!(
        kalshi_yes_row(&merged),
        kalshi_yes_row(&control),
        "rejected book subscription with active trade channel must not mask peer seq gap"
    );
}

#[test]
fn rejected_kalshi_book_subscription_does_not_reset_unrelated_market() {
    let sub_book = |seq: u64, us: i64, ticker: &str| {
        kind(
            timed(
                seq,
                "kalshi",
                us,
                us as u64,
                json!({"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":[ticker],"use_yes_price":true}}),
            ),
            "subscription",
            "sent",
        )
    };
    let book = json!({"type":"orderbook_snapshot","sid":7,"seq":1,"sending_ts_ms":1700000000001i64,
        "msg":{"market_ticker":"X","yes_dollars_fp":[["0.40","10"]],"no_dollars_fp":[["0.60","20"]]}});
    let delta = json!({"type":"orderbook_delta","sid":7,"seq":2,"sending_ts_ms":1700000000005i64,
        "msg":{"ts_ms":1700000000005i64,"market_ticker":"X","side":"yes","price_dollars":"0.40","delta_fp":"2"}});
    let error = json!({"id":1,"type":"error","msg":{"code":9,"msg":"Unknown channel"}});

    let dir_a = root();
    let dir_b = root();
    seal(
        &dir_a,
        1,
        &[
            sub_book(1, 0, "X"),
            timed(2, "kalshi", 1000, 1000, book),
            timed(3, "kalshi", 5000, 5000, delta),
        ],
        None,
    );
    seal(
        &dir_b,
        1,
        &[
            sub_book(1, 2000, "Y"),
            timed(2, "kalshi", 3000, 3000, error),
        ],
        None,
    );

    let merged = merged_topbooks(
        Venue::Kalshi,
        vec![
            ("a".into(), "conn".into(), dir_a),
            ("b".into(), "conn".into(), dir_b),
        ],
    );
    assert_eq!(
        kalshi_yes_row(&merged),
        (true, 12.0),
        "rejection of unrelated market Y must not reset healthy market X"
    );
}

#[test]
fn delayed_book_rejection_and_future_reconnect_cannot_corrupt_coverage() {
    let sub_book = |seq: u64, us: i64, generation: u64| {
        let mut f = kind(
            timed(
                seq,
                "kalshi",
                us,
                us as u64,
                json!({"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":["X"],"use_yes_price":true}}),
            ),
            "subscription",
            "sent",
        );
        f["connection_generation"] = json!(generation);
        f
    };
    let sub_trade = |seq: u64, us: i64| {
        kind(
            timed(
                seq,
                "kalshi",
                us,
                us as u64,
                json!({"id":2,"cmd":"subscribe","params":{"channels":["trade"],"market_tickers":["X"]}}),
            ),
            "subscription",
            "sent",
        )
    };
    let with_gen = |mut f: Value, generation: u64| {
        f["connection_generation"] = json!(generation);
        f
    };
    let snap = |ms: i64| {
        json!({"type":"orderbook_snapshot","sid":7,"seq":1,"sending_ts_ms":1700000000000i64 + ms,
            "msg":{"market_ticker":"X","yes_dollars_fp":[["0.40","10"]],"no_dollars_fp":[["0.60","20"]]}})
    };
    let delta = |ms: i64, seq: i64| {
        json!({"type":"orderbook_delta","sid":7,"seq":seq,"sending_ts_ms":1700000000000i64 + ms,
            "msg":{"ts_ms":1700000000000i64 + ms,"market_ticker":"X","side":"yes","price_dollars":"0.40","delta_fp":"2"}})
    };
    let side_ack = json!({"id":2,"type":"subscribed","msg":{"channel":"trade","sid":8}});
    let book_ack = json!({"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":7}});
    let error = json!({"id":1,"type":"error","msg":{"code":9,"msg":"Unknown channel"}});
    let trade = |ms: i64| {
        json!({"type":"trade","sid":8,"seq":1,"sending_ts_ms":1700000000000i64 + ms,
            "msg":{"market_ticker":"X","ts_ms":1700000000000i64 + ms,"yes_price_dollars":"0.4","count_fp":"1","taker_side":"yes"}})
    };

    // Case 1: side ack + trade arrive BEFORE the book rejection at 7ms.
    let dir_gapped = root();
    let dir_pending = root();
    seal(
        &dir_gapped,
        1,
        &[
            sub_book(1, 0, 1),
            timed(2, "kalshi", 2000, 2000, snap(2)),
            timed(3, "kalshi", 5000, 5000, delta(5, 3)),
        ],
        None,
    );
    seal(
        &dir_pending,
        1,
        &[
            sub_book(1, 0, 1),
            sub_trade(2, 0),
            timed(3, "kalshi", 1000, 1000, side_ack),
            timed(4, "kalshi", 6000, 6000, trade(6)),
            timed(5, "kalshi", 7000, 7000, error.clone()),
        ],
        None,
    );
    let merged_pending = merged_topbooks(
        Venue::Kalshi,
        vec![
            ("a".into(), "conn".into(), dir_gapped),
            ("b".into(), "conn".into(), dir_pending),
        ],
    );
    assert!(
        !kalshi_yes_row(&merged_pending).0,
        "side frames before delayed book rejection must not certify peer seq gap"
    );

    // Case 2: rejected copy emits a post-rejection trade at 3ms, then reconnects at 9-11ms.
    let dir_good = root();
    let dir_recovered = root();
    seal(
        &dir_good,
        1,
        &[
            sub_book(1, 0, 1),
            timed(2, "kalshi", 2000, 2000, snap(2)),
            timed(3, "kalshi", 5000, 5000, delta(5, 2)),
        ],
        None,
    );
    seal(
        &dir_recovered,
        1,
        &[
            sub_book(1, 0, 1),
            sub_trade(2, 0),
            timed(3, "kalshi", 1000, 1000, error),
            timed(4, "kalshi", 3000, 3000, trade(3)),
            sub_book(5, 9000, 2),
            with_gen(timed(6, "kalshi", 10000, 10000, book_ack), 2),
            with_gen(timed(7, "kalshi", 11000, 11000, snap(11)), 2),
        ],
        None,
    );
    let merged_recovered = merged_topbooks(
        Venue::Kalshi,
        vec![
            ("good".into(), "conn".into(), dir_good),
            ("recovered".into(), "conn".into(), dir_recovered),
        ],
    );
    let good_delta = merged_recovered
        .iter()
        .find(|b| {
            column::<StringArray>(b, "instrument_id").value(0) == "X:YES"
                && column::<StringArray>(b, "raw_event_ref")
                    .value(0)
                    .ends_with(":3")
        })
        .expect("healthy delta from good");
    assert!(column::<BooleanArray>(good_delta, "valid_state").value(0));
    assert_eq!(
        column::<Float64Array>(good_delta, "bid_size_contracts").value(0),
        12.0
    );
}

#[test]
fn gapped_trade_or_overlapping_stream_does_not_reset_continuously_covered_book() {
    let sub_book = |ticker: &str| {
        kind(
            timed(
                1,
                "kalshi",
                0,
                0,
                json!({"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":[ticker],"use_yes_price":true}}),
            ),
            "subscription",
            "sent",
        )
    };
    let sub_trade = kind(
        timed(
            1,
            "kalshi",
            0,
            0,
            json!({"id":2,"cmd":"subscribe","params":{"channels":["trade"],"market_tickers":["Y"]}}),
        ),
        "subscription",
        "sent",
    );
    let snap = |ticker: &str, ms: i64, seq: i64| {
        json!({"type":"orderbook_snapshot","sid":7,"seq":seq,"sending_ts_ms":1700000000000i64 + ms,
            "msg":{"market_ticker":ticker,"yes_dollars_fp":[["0.40","10"]],"no_dollars_fp":[["0.60","20"]]}})
    };
    let delta = |ticker: &str, ms: i64, seq: i64| {
        json!({"type":"orderbook_delta","sid":7,"seq":seq,"sending_ts_ms":1700000000000i64 + ms,
            "msg":{"ts_ms":1700000000000i64 + ms,"market_ticker":ticker,"side":"yes","price_dollars":"0.40","delta_fp":"2"}})
    };
    let trade = |ms: i64, seq: i64| {
        json!({"type":"trade","sid":8,"seq":seq,"sending_ts_ms":1700000000000i64 + ms,
            "msg":{"market_ticker":"Y","ts_ms":1700000000000i64 + ms,"yes_price_dollars":"0.4","count_fp":"1","taker_side":"yes"}})
    };
    let gap = |seq: u64, us: i64| {
        kind(
            timed(seq, "kalshi", us, us as u64, json!({})),
            "disk_pause_gap",
            "local",
        )
    };

    // Control: gapped trade-only stream must not reset healthy book stream.
    let dir_book = root();
    let dir_trade = root();
    seal(
        &dir_book,
        1,
        &[
            sub_book("X"),
            timed(2, "kalshi", 1000, 1000, snap("X", 1, 1)),
            timed(3, "kalshi", 8000, 8000, delta("X", 8, 2)),
        ],
        None,
    );
    seal(
        &dir_trade,
        1,
        &[
            sub_trade,
            timed(2, "kalshi", 2000, 2000, trade(2, 1)),
            gap(3, 3000),
            timed(4, "kalshi", 5000, 5000, trade(5, 2)),
        ],
        None,
    );
    let merged_trade = merged_topbooks(
        Venue::Kalshi,
        vec![
            ("book".into(), "book-a".into(), dir_book),
            ("trades".into(), "trade-b".into(), dir_trade),
        ],
    );
    assert_eq!(kalshi_yes_row(&merged_trade), (true, 12.0));

    // Partial overlap: archived shard {X, Z} gaps, while shard {Y, X} continuously covers X.
    let dir_good = root();
    let dir_overlap = root();
    seal(
        &dir_good,
        1,
        &[
            timed(1, "kalshi", 1000, 1000, snap("Y", 1, 1)),
            timed(2, "kalshi", 2000, 2000, snap("X", 2, 2)),
            timed(3, "kalshi", 8000, 8000, delta("X", 8, 3)),
        ],
        None,
    );
    seal(
        &dir_overlap,
        1,
        &[
            timed(1, "kalshi", 1000, 1000, snap("X", 1, 1)),
            timed(2, "kalshi", 2000, 2000, snap("Z", 2, 2)),
            gap(3, 3000),
            timed(4, "kalshi", 5000, 5000, delta("Z", 5, 4)),
        ],
        None,
    );
    let merged_overlap = merged_topbooks(
        Venue::Kalshi,
        vec![
            ("good".into(), "same".into(), dir_good),
            ("other".into(), "same".into(), dir_overlap),
        ],
    );
    assert_eq!(kalshi_yes_row(&merged_overlap), (true, 12.0));
}

#[test]
fn declared_polymarket_overlap_survives_snapshot_receipt_skew() {
    let sub = |ids: &[&str]| {
        kind(
            timed(
                1,
                "polymarket",
                0,
                0,
                json!({"type":"market","assets_ids":ids}),
            ),
            "subscription",
            "sent",
        )
    };
    let snap = json!({
        "event_type": "book",
        "asset_id": "X",
        "market": "M",
        "timestamp": "1735689600001",
        "hash": "h1",
        "bids": [{"price": "0.40", "size": "10"}],
        "asks": [{"price": "0.60", "size": "20"}],
    });
    let delta = |ms: i64, size: &str| {
        json!({
            "event_type": "price_change",
            "market": "M",
            "timestamp": (1735689600000i64 + ms).to_string(),
            "price_changes": [{"asset_id": "X", "price": "0.40", "size": size, "side": "BUY"}],
        })
    };
    let gap = kind(
        timed(3, "polymarket", 2000, 2000, json!({})),
        "disk_pause_gap",
        "local",
    );

    let dir_broken = root();
    seal(
        &dir_broken,
        1,
        &[
            sub(&["X", "Y"]),
            timed(2, "polymarket", 1000, 1000, snap.clone()),
            gap,
            timed(4, "polymarket", 5000, 5000, delta(5, "12")),
        ],
        None,
    );
    for offset in [0_i64, 1, 100, -1] {
        let dir_healthy = root();
        seal(
            &dir_healthy,
            1,
            &[
                sub(&["X", "Z"]),
                timed(
                    2,
                    "polymarket",
                    1000 + offset,
                    (1000 + offset) as u64,
                    snap.clone(),
                ),
                timed(3, "polymarket", 3000, 3000, delta(3, "11")),
                timed(4, "polymarket", 5100, 5100, delta(5, "12")),
            ],
            None,
        );
        let merged = merged_topbooks(
            Venue::Polymarket,
            vec![
                ("broken".into(), "c".into(), dir_broken.clone()),
                ("healthy".into(), "c".into(), dir_healthy),
            ],
        );
        let last = merged.last().expect("final topbook");
        assert!(
            column::<BooleanArray>(last, "valid_state").value(0),
            "offset {offset} should stay valid"
        );
        assert_eq!(
            column::<Float64Array>(last, "bid_size_contracts").value(0),
            12.0
        );
        assert_eq!(
            column::<Float64Array>(last, "best_ask_dollars").value(0),
            0.6
        );
    }
}

#[test]
fn kalshi_side_traffic_before_book_ack_cannot_certify_peer_seq_gap() {
    let sub_book = kind(
        timed(
            1,
            "kalshi",
            0,
            0,
            json!({"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":["X"],"use_yes_price":true}}),
        ),
        "subscription",
        "sent",
    );
    let sub_trade = kind(
        timed(
            2,
            "kalshi",
            0,
            0,
            json!({"id":2,"cmd":"subscribe","params":{"channels":["trade"],"market_tickers":["X"]}}),
        ),
        "subscription",
        "sent",
    );
    let snap = |ms: i64, size: &str| {
        json!({"type":"orderbook_snapshot","sid":7,"seq":1,"sending_ts_ms":1700000000000i64 + ms,
            "msg":{"market_ticker":"X","yes_dollars_fp":[["0.40",size]],"no_dollars_fp":[["0.60","20"]]}})
    };
    let delta = |ms: i64, seq: i64| {
        json!({"type":"orderbook_delta","sid":7,"seq":seq,"sending_ts_ms":1700000000000i64 + ms,
            "msg":{"ts_ms":1700000000000i64 + ms,"market_ticker":"X","side":"yes","price_dollars":"0.40","delta_fp":"2"}})
    };
    let side_ack = json!({"id":2,"type":"subscribed","msg":{"channel":"trade","sid":8}});
    let book_ack = json!({"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":7}});
    let trade = json!({"type":"trade","sid":8,"seq":1,"sending_ts_ms":1700000000006i64,
        "msg":{"market_ticker":"X","ts_ms":1700000000006i64,"yes_price_dollars":"0.4","count_fp":"1","taker_side":"yes"}});

    let dir_a = root();
    seal(
        &dir_a,
        1,
        &[
            sub_book.clone(),
            timed(2, "kalshi", 2000, 2000, snap(2, "10")),
            timed(3, "kalshi", 5000, 5000, delta(5, 3)),
        ],
        None,
    );
    let dir_delayed = root();
    seal(
        &dir_delayed,
        1,
        &[
            sub_book.clone(),
            sub_trade.clone(),
            timed(3, "kalshi", 1000, 1000, side_ack.clone()),
            timed(4, "kalshi", 6000, 6000, trade.clone()),
            timed(5, "kalshi", 8000, 8000, book_ack),
            timed(6, "kalshi", 9000, 9000, snap(9, "17")),
        ],
        None,
    );
    let merged_delayed = merged_topbooks(
        Venue::Kalshi,
        vec![
            ("a".into(), "c".into(), dir_a.clone()),
            ("delayed".into(), "c".into(), dir_delayed),
        ],
    );
    let a_row = merged_delayed
        .iter()
        .find(|b| {
            column::<StringArray>(b, "instrument_id").value(0) == "X:YES"
                && column::<StringArray>(b, "raw_event_ref")
                    .value(0)
                    .ends_with(":3")
        })
        .expect("seq gap row");
    assert!(!column::<BooleanArray>(a_row, "valid_state").value(0));

    let dir_dropped = root();
    seal(
        &dir_dropped,
        1,
        &[
            sub_book,
            sub_trade,
            timed(3, "kalshi", 1000, 1000, side_ack),
            timed(4, "kalshi", 6000, 6000, trade),
            kind(
                timed(5, "kalshi", 7000, 7000, json!({})),
                "transport_error",
                "local",
            ),
        ],
        None,
    );
    let merged_dropped = merged_topbooks(
        Venue::Kalshi,
        vec![
            ("a".into(), "c".into(), dir_a),
            ("dropped".into(), "c".into(), dir_dropped),
        ],
    );
    assert!(!kalshi_yes_row(&merged_dropped).0);
}

#[test]
fn receipt_clock_rollback_cannot_bridge_own_gap_in_merged_replay() {
    let sub_book = kind(
        timed(
            1,
            "kalshi",
            0,
            0,
            json!({"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":["X"],"use_yes_price":true}}),
        ),
        "subscription",
        "sent",
    );
    let sub_trade = kind(
        timed(
            1,
            "kalshi",
            0,
            0,
            json!({"id":2,"cmd":"subscribe","params":{"channels":["trade"],"market_tickers":["X"]}}),
        ),
        "subscription",
        "sent",
    );
    let snap = json!({"type":"orderbook_snapshot","sid":7,"seq":1,"sending_ts_ms":1700000000001i64,
        "msg":{"market_ticker":"X","yes_dollars_fp":[["0.40","10"]],"no_dollars_fp":[["0.60","20"]]}});
    let delta = |ms: i64, seq: i64, qty: &str| {
        json!({"type":"orderbook_delta","sid":7,"seq":seq,"sending_ts_ms":1700000000000i64 + ms,
            "msg":{"ts_ms":1700000000000i64 + ms,"market_ticker":"X","side":"yes","price_dollars":"0.40","delta_fp":qty}})
    };
    let side_ack = json!({"id":2,"type":"subscribed","msg":{"channel":"trade","sid":8}});

    let dir_a = root();
    seal(
        &dir_a,
        1,
        &[
            sub_book,
            timed(2, "kalshi", 1000, 1000, snap),
            timed(3, "kalshi", 5000, 5000, delta(5, 2, "1")),
            timed(4, "kalshi", 3000, 6000, delta(7, 4, "2")),
            timed(5, "kalshi", 8000, 8000, delta(8, 5, "1")),
        ],
        None,
    );
    let dir_peer = root();
    seal(
        &dir_peer,
        1,
        &[sub_trade, timed(2, "kalshi", 1000, 1000, side_ack)],
        None,
    );
    let merged = merged_topbooks(
        Venue::Kalshi,
        vec![
            ("a".into(), "c".into(), dir_a),
            ("peer".into(), "other".into(), dir_peer),
        ],
    );
    assert!(
        !kalshi_yes_row(&merged).0,
        "rollback overlap on one stream must not hide its own seq gap"
    );
}

#[test]
fn ended_peer_on_rollback_and_kalshi_ack_before_snapshot_cannot_certify_gap() {
    let sub_book = |tickers: &[&str]| {
        kind(
            timed(
                1,
                "kalshi",
                0,
                0,
                json!({"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":tickers,"use_yes_price":true}}),
            ),
            "subscription",
            "sent",
        )
    };
    let snap = |ticker: &str, ms: i64, seq: i64, size: &str| {
        json!({"type":"orderbook_snapshot","sid":7,"seq":seq,"sending_ts_ms":1735689600000i64 + ms,
            "msg":{"market_ticker":ticker,"yes_dollars_fp":[["0.40",size]],"no_dollars_fp":[["0.60","20"]]}})
    };
    let delta = |ticker: &str, ms: i64, seq: i64, qty: &str| {
        json!({"type":"orderbook_delta","sid":7,"seq":seq,"sending_ts_ms":1735689600000i64 + ms,
            "msg":{"ts_ms":1735689600000i64 + ms,"market_ticker":ticker,"side":"yes","price_dollars":"0.40","delta_fp":qty}})
    };
    let book_ack = json!({"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":7}});

    // Case 1: peer ended at seq=2 (5ms); A skips seq=3 with 2ms rollback on seq=4.
    let dir_a = root();
    let dir_ended = root();
    seal(
        &dir_a,
        1,
        &[
            sub_book(&["X"]),
            timed(2, "kalshi", 1000, 1000, snap("X", 1, 1, "10")),
            timed(3, "kalshi", 5000, 5000, delta("X", 5, 2, "1")),
            timed(4, "kalshi", 3000, 6000, delta("X", 7, 4, "2")),
            timed(5, "kalshi", 8000, 8000, delta("X", 8, 5, "1")),
        ],
        None,
    );
    seal(
        &dir_ended,
        1,
        &[
            sub_book(&["X"]),
            timed(2, "kalshi", 1000, 1000, snap("X", 1, 1, "10")),
            timed(3, "kalshi", 5000, 5000, delta("X", 5, 2, "1")),
            kind(
                timed(4, "kalshi", 5100, 5100, json!({})),
                "transport_error",
                "local",
            ),
        ],
        None,
    );
    let merged_ended = merged_topbooks(
        Venue::Kalshi,
        vec![
            ("a".into(), "c".into(), dir_a),
            ("b".into(), "c".into(), dir_ended),
        ],
    );
    assert!(
        !kalshi_yes_row(&merged_ended).0,
        "peer that ended at seq 2 must not bridge rollback gap"
    );

    // Case 2: peer acked {X, Y} at 1ms and received Y at 6ms, but X snapshot only at 9ms.
    let dir_gap = root();
    let dir_early = root();
    seal(
        &dir_gap,
        1,
        &[
            sub_book(&["X"]),
            timed(2, "kalshi", 2000, 2000, snap("X", 2, 1, "10")),
            timed(3, "kalshi", 5000, 5000, delta("X", 5, 3, "2")),
        ],
        None,
    );
    seal(
        &dir_early,
        1,
        &[
            sub_book(&["X", "Y"]),
            timed(2, "kalshi", 1000, 1000, book_ack),
            timed(3, "kalshi", 6000, 6000, snap("Y", 6, 1, "10")),
            timed(4, "kalshi", 9000, 9000, snap("X", 9, 2, "17")),
        ],
        None,
    );
    let merged_early = merged_topbooks(
        Venue::Kalshi,
        vec![
            ("gapped".into(), "c".into(), dir_gap),
            ("peer".into(), "c".into(), dir_early),
        ],
    );
    let gapped_row = merged_early
        .iter()
        .find(|b| {
            column::<StringArray>(b, "instrument_id").value(0) == "X:YES"
                && column::<StringArray>(b, "raw_event_ref")
                    .value(0)
                    .ends_with(":3")
        })
        .expect("gapped X delta");
    assert!(
        !column::<BooleanArray>(gapped_row, "valid_state").value(0),
        "Kalshi channel ack must not certify X before peer's first X snapshot"
    );
}

#[test]
fn unrelated_y_interleave_and_late_duplicate_preserve_healthy_polymarket_book() {
    let sub = |ids: &[&str]| {
        kind(
            timed(
                1,
                "polymarket",
                0,
                0,
                json!({"type":"market","assets_ids":ids}),
            ),
            "subscription",
            "sent",
        )
    };
    let snap = |asset: &str, ts_ms: i64| {
        json!({
            "event_type": "book",
            "asset_id": asset,
            "market": "M",
            "timestamp": ts_ms.to_string(),
            "hash": format!("h{asset}{ts_ms}"),
            "bids": [{"price": "0.40", "size": "10"}],
            "asks": [{"price": "0.60", "size": "20"}],
        })
    };
    let delta = |ms: i64, size: &str| {
        json!({
            "event_type": "price_change",
            "market": "M",
            "timestamp": (1735689600000i64 + ms).to_string(),
            "price_changes": [{"asset_id": "X", "price": "0.40", "size": size, "side": "BUY"}],
        })
    };

    // Case 1: unrelated Y snapshot on retiring {X, Y} shard must not erase healthy {X, Z} witness.
    let dir_a = root();
    let dir_b = root();
    seal(
        &dir_a,
        1,
        &[
            sub(&["X", "Y"]),
            timed(2, "polymarket", 1000, 1000, snap("X", 1735689600001)),
            timed(3, "polymarket", 2000, 2000, snap("Y", 1735689600002)),
            kind(
                timed(4, "polymarket", 2500, 2500, json!({})),
                "transport_error",
                "local",
            ),
        ],
        None,
    );
    seal(
        &dir_b,
        1,
        &[
            sub(&["X", "Z"]),
            timed(2, "polymarket", 3000, 3000, snap("X", 1735689600001)),
            timed(3, "polymarket", 6000, 6000, delta(6, "12")),
        ],
        None,
    );
    let merged_interleave = merged_topbooks(
        Venue::Polymarket,
        vec![
            ("a".into(), "c".into(), dir_a),
            ("b".into(), "c".into(), dir_b),
        ],
    );
    let last_x = merged_interleave
        .iter()
        .rfind(|b| column::<StringArray>(b, "instrument_id").value(0) == "X")
        .expect("final X row");
    assert!(column::<BooleanArray>(last_x, "valid_state").value(0));
    assert_eq!(
        column::<Float64Array>(last_x, "best_ask_dollars").value(0),
        0.6
    );

    // Case 2: 130s-old cached snapshot received 2ms apart (repeats_after_emit) preserves healthy B.
    let dir_la = root();
    let dir_lb = root();
    seal(
        &dir_la,
        1,
        &[
            sub(&["X", "Y"]),
            timed(
                2,
                "polymarket",
                1000,
                1000,
                snap("X", 1735689600000 - 130000),
            ),
            kind(
                timed(3, "polymarket", 2000, 2000, json!({})),
                "transport_error",
                "local",
            ),
        ],
        None,
    );
    seal(
        &dir_lb,
        1,
        &[
            sub(&["X", "Z"]),
            timed(
                2,
                "polymarket",
                3000,
                3000,
                snap("X", 1735689600000 - 130000),
            ),
            timed(3, "polymarket", 6000, 6000, delta(6, "12")),
        ],
        None,
    );
    let merged_late = merged_topbooks(
        Venue::Polymarket,
        vec![
            ("a".into(), "c".into(), dir_la),
            ("b".into(), "c".into(), dir_lb),
        ],
    );
    let last_late = merged_late.last().expect("final late X row");
    assert!(column::<BooleanArray>(last_late, "valid_state").value(0));
    assert_eq!(
        column::<Float64Array>(last_late, "best_ask_dollars").value(0),
        0.6
    );
}

#[test]
fn multihop_ended_peers_and_same_class_polymarket_interleave_are_exact() {
    let sub_k = kind(
        timed(
            1,
            "kalshi",
            0,
            0,
            json!({"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":["X"],"use_yes_price":true}}),
        ),
        "subscription",
        "sent",
    );
    let ksnap = |ms: i64, seq: i64, clock: i64| {
        json!({"type":"orderbook_snapshot","sid":7,"seq":seq,"sending_ts_ms":1735689600000i64 + ms,
            "msg":{"market_ticker":"X","clock":clock,"market_id":"m-1","yes_dollars_fp":[["0.40","10"]],"no_dollars_fp":[["0.60","20"]]}})
    };
    let kdelta = |ms: i64, seq: i64, qty: &str, clock: i64| {
        json!({"type":"orderbook_delta","sid":7,"seq":seq,"sending_ts_ms":1735689600000i64 + ms,
            "msg":{"ts_ms":1735689600000i64 + ms,"market_ticker":"X","clock":clock,"market_id":"m-1","side":"yes","price_dollars":"0.40","delta_fp":qty}})
    };
    let dir_a = root();
    let dir_c3 = root();
    let dir_early = root();
    let dir_late = root();
    let dir_full = root();
    seal(
        &dir_a,
        1,
        &[
            sub_k.clone(),
            timed(2, "kalshi", 1000, 1000, ksnap(1, 1, 4100001)),
            timed(3, "kalshi", 5000, 5000, kdelta(5, 2, "1", 4100002)),
            timed(4, "kalshi", 3000, 6000, kdelta(7, 4, "2", 4100004)),
            timed(5, "kalshi", 8000, 8000, kdelta(8, 5, "1", 4100005)),
        ],
        None,
    );
    seal(
        &dir_c3,
        1,
        &[
            sub_k.clone(),
            timed(2, "kalshi", 4000, 4000, ksnap(1, 1, 4100001)),
            timed(3, "kalshi", 8000, 8000, kdelta(5, 2, "1", 4100002)),
            kind(
                timed(4, "kalshi", 8100, 8100, json!({})),
                "transport_error",
                "local",
            ),
        ],
        None,
    );
    seal(
        &dir_early,
        1,
        &[
            sub_k.clone(),
            timed(2, "kalshi", 2000, 2000, ksnap(-2, 1, 4099988)),
            kind(
                timed(3, "kalshi", 7000, 7000, json!({})),
                "ws_pong",
                "received_control_base64",
            ),
            kind(
                timed(4, "kalshi", 7100, 7100, json!({})),
                "transport_error",
                "local",
            ),
        ],
        None,
    );
    seal(
        &dir_late,
        1,
        &[
            sub_k.clone(),
            timed(2, "kalshi", 6000, 6000, ksnap(-1, 1, 4099989)),
            kind(
                timed(3, "kalshi", 9000, 9000, json!({})),
                "ws_pong",
                "received_control_base64",
            ),
            kind(
                timed(4, "kalshi", 9100, 9100, json!({})),
                "transport_error",
                "local",
            ),
        ],
        None,
    );
    seal(
        &dir_full,
        1,
        &[
            sub_k,
            timed(2, "kalshi", 1000, 1000, ksnap(1, 1, 4100001)),
            timed(3, "kalshi", 5000, 5000, kdelta(5, 2, "1", 4100002)),
            timed(4, "kalshi", 6000, 6000, kdelta(6, 3, "5", 4100003)),
            timed(5, "kalshi", 7000, 7000, kdelta(7, 4, "2", 4100004)),
            timed(6, "kalshi", 8000, 8000, kdelta(8, 5, "1", 4100005)),
        ],
        None,
    );
    let with_c3 = merged_topbooks(
        Venue::Kalshi,
        vec![
            ("a".into(), "c".into(), dir_a.clone()),
            ("c3".into(), "c".into(), dir_c3),
        ],
    );
    assert!(!kalshi_yes_row(&with_c3).0);
    let with_multihop = merged_topbooks(
        Venue::Kalshi,
        vec![
            ("a".into(), "c".into(), dir_a.clone()),
            ("early".into(), "c".into(), dir_early),
            ("late".into(), "c".into(), dir_late),
        ],
    );
    assert!(!kalshi_yes_row(&with_multihop).0);
    let with_full = merged_topbooks(
        Venue::Kalshi,
        vec![
            ("a".into(), "c".into(), dir_a),
            ("full".into(), "c".into(), dir_full),
        ],
    );
    assert_eq!(kalshi_yes_row(&with_full), (true, 19.0));

    // Same-class Polymarket {X, Y, Z} interleave on A must not narrow B's witness for X.
    let sub_p = kind(
        timed(
            1,
            "polymarket",
            0,
            0,
            json!({"type":"market","assets_ids":["X","Y","Z"]}),
        ),
        "subscription",
        "sent",
    );
    let psnap = |asset: &str, ts_ms: i64| {
        json!({
            "event_type": "book",
            "asset_id": asset,
            "market": "M",
            "timestamp": ts_ms.to_string(),
            "hash": format!("h{asset}{ts_ms}"),
            "bids": [{"price": "0.40", "size": "10"}],
            "asks": [{"price": "0.60", "size": "20"}],
        })
    };
    let pdelta = |assets: &[(&str, &str)], ms: i64| {
        let changes: Vec<_> = assets
            .iter()
            .map(|(id, size)| json!({"asset_id": id, "price": "0.40", "size": size, "side": "BUY"}))
            .collect();
        json!({
            "event_type": "price_change",
            "market": "M",
            "timestamp": (1735689600000i64 + ms).to_string(),
            "price_changes": changes,
        })
    };
    let dir_pa = root();
    let dir_pb = root();
    seal(
        &dir_pa,
        1,
        &[
            sub_p.clone(),
            timed(2, "polymarket", 200, 200, psnap("Y", 1735689600000)),
            timed(3, "polymarket", 400, 400, psnap("Z", 1735689600000)),
            timed(4, "polymarket", 1000, 1000, psnap("X", 1735689600001)),
            timed(
                5,
                "polymarket",
                2000,
                2000,
                pdelta(&[("Y", "40"), ("Z", "60")], 2),
            ),
            kind(
                timed(6, "polymarket", 2900, 2900, json!({})),
                "transport_error",
                "local",
            ),
        ],
        None,
    );
    seal(
        &dir_pb,
        1,
        &[
            sub_p,
            timed(2, "polymarket", 3000, 3000, psnap("X", 1735689600001)),
            timed(3, "polymarket", 6000, 6000, pdelta(&[("X", "12")], 6)),
        ],
        None,
    );
    let merged_p = merged_topbooks(
        Venue::Polymarket,
        vec![
            ("a".into(), "c".into(), dir_pa),
            ("b".into(), "c".into(), dir_pb),
        ],
    );
    let last_x = merged_p
        .iter()
        .rfind(|b| column::<StringArray>(b, "instrument_id").value(0) == "X")
        .expect("final X row");
    assert!(column::<BooleanArray>(last_x, "valid_state").value(0));
    assert_eq!(
        column::<Float64Array>(last_x, "best_ask_dollars").value(0),
        0.6
    );
    assert_eq!(
        column::<Float64Array>(last_x, "bid_size_contracts").value(0),
        12.0
    );
}
