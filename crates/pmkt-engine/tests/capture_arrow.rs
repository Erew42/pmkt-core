use arrow_array::{
    Array, BooleanArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
};
use arrow_schema::DataType;
use flate2::{Compression, write::GzEncoder};
use pmkt_engine::{
    BookMode, VenueBooks,
    capture::{CaptureItem, SealedChunk, inspect_session, list_session_chunks, read_session},
    emit::{QuoteBatchBuilder, Receive, Version, reconstruct_session},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

fn root() -> PathBuf {
    // Each case owns fresh files under the ignored workspace build tree.
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/capture-fixtures");
    fs::create_dir_all(&base).unwrap();
    tempfile::tempdir_in(base).unwrap().keep()
}
fn raw(seq: u64, generation: u64, venue: &str, msg: Value) -> Value {
    json!({"schema":format!("{venue}.public_raw_frame.v1"),"venue":venue,"recording_id":"run","connection_generation":generation,
        "local_frame_seq":seq,"received_utc":"2025-01-01T00:00:00+00:00","received_monotonic_ns":seq*1_000_000,
        "frame_type":"text","direction":"received","payload_raw":msg.to_string()})
}
fn name(i: u64) -> String {
    format!("chunk_{i:06}.jsonl.gz")
}
fn manifest(root: &Path, i: u64) -> PathBuf {
    root.join(format!("{}.manifest.json", name(i)))
}
fn sidecar(
    root: &Path,
    i: u64,
    venue: &str,
    run: &str,
    range: std::ops::RangeInclusive<u64>,
    prev: Option<&str>,
) -> String {
    let bytes = fs::read(root.join(name(i))).unwrap();
    let sha = format!("{:x}", Sha256::digest(&bytes));
    let m = json!({"schema":format!("{venue}.public_raw_chunk.v1"),"venue":venue,"recording_id":run,"filename":name(i),"size_bytes":bytes.len(),
        "sha256":sha,"frame_count":range.end()-range.start()+1,"first_frame_seq":range.start(),"last_frame_seq":range.end(),"previous_chunk_sha256":prev,
        "future_field":"retained"});
    fs::write(manifest(root, i), m.to_string()).unwrap();
    sha
}
fn seal(root: &Path, i: u64, frames: &[Value], prev: Option<&str>) -> String {
    let mut gz = GzEncoder::new(
        fs::File::create(root.join(name(i))).unwrap(),
        Compression::fast(),
    );
    for f in frames {
        writeln!(gz, "{f}").unwrap();
    }
    gz.finish().unwrap();
    sidecar(
        root,
        i,
        frames[0]["venue"].as_str().unwrap(),
        "run",
        frames[0]["local_frame_seq"].as_u64().unwrap()
            ..=frames.last().unwrap()["local_frame_seq"].as_u64().unwrap(),
        prev,
    )
}
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
fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap()
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
