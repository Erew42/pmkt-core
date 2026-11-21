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
