use pmkt_engine::{
    BookMode, Result, Venue, VenueBooks,
    capture::{CaptureItem, RawFrame},
    health::{Integrity, QualityFlag as F},
    merge::{
        CopyMerge, EndReason, MergeOptions, MergeStats, MergedMessage, Occurrence, Origin,
        SessionOccurrences, StartReason, Stream, uncovered_intervals,
    },
};
use serde_json::{Value, json};

#[test]
fn two_copy_real_samples_match_python_merge_rows() {
    use flate2::read::GzDecoder;
    use sha2::{Digest, Sha256};
    use std::io::{BufRead, BufReader, Write};
    // Oracle: copy_merge.CopyMerge at 1286258. Feed B omits every seventh
    // frame and receives its remaining frames 5.1ms after feed A.
    let cases = [
        (
            Venue::Polymarket,
            include_bytes!("../../../tests/fixtures/raw_capture/polymarket_00.jsonl.gz").as_slice(),
            174,
            "f2b3ef0680d6fddcaa221a2988f8c2d3bf19b944adb21ab9280099faa5ab3fb6",
        ),
        (
            Venue::Kalshi,
            include_bytes!("../../../tests/fixtures/raw_capture/kalshi_00.jsonl.gz").as_slice(),
            173,
            "a1f1b767cb649d58ca85fb54df2649045e907ed501f9779796c01ae53c3f1cc0",
        ),
    ];
    for (venue, gz, repeats, expected) in cases {
        let frames = BufReader::new(GzDecoder::new(gz))
            .lines()
            .map(|line| Ok(CaptureItem::Frame(serde_json::from_str(&line?)?)));
        let a = SessionOccurrences::new(Stream::new("a", "conn"), "session", frames)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let b = a
            .iter()
            .filter(|o| o.origin.frame_seq % 7 != 0)
            .cloned()
            .map(|mut o| {
                o.origin.stream.copy = "b".into();
                o.origin.received_us += 5100;
                o
            })
            .collect();
        let (rows, stats) = merge(venue, vec![a, b], MergeOptions::default()).unwrap();
        assert_eq!((stats.messages, stats.repeats_merged), (202, repeats));
        let mut h = Sha256::new();
        for m in rows {
            let origin = m.origin();
            writeln!(
                h,
                "{}/{}/{}/{}/{}/{}/{}",
                m.order_ms,
                m.content_digest,
                m.occurrences.len(),
                origin.frame_seq,
                origin
                    .position
                    .map_or_else(|| "None".into(), |p| p.to_string()),
                m.snapshot_class().unwrap_or("None"),
                u8::from(m.late)
            )
            .unwrap();
        }
        assert_eq!(format!("{:x}", h.finalize()), expected);
    }
}

fn delta(clock: u64, ts: i64, seq: i64) -> Value {
    json!({"type":"orderbook_delta","sid":1,"seq":seq,"sending_ts_ms":ts+1,"msg":{"clock":clock,"market_ticker":"K","ts_ms":ts,"side":"yes","price_dollars":"0.4","delta_fp":"1"}})
}
fn snap(clock: Option<u64>, sent: i64, seq: i64) -> Value {
    let mut body = json!({"market_ticker":"K","yes_dollars_fp":[["0.4","10"]]});
    if let Some(c) = clock {
        body["clock"] = json!(c);
    }
    json!({"type":"orderbook_snapshot","sid":1,"seq":seq,"sending_ts_ms":sent,"msg":body})
}
fn occurrence(copy: &str, t: i64, m: Value, seq: u64) -> Occurrence {
    Occurrence {
        origin: Origin {
            stream: Stream::new(copy, "conn"),
            session: "session".into(),
            generation: 1,
            frame_seq: seq,
            position: None,
            received_us: t * 1000,
            received_utc: String::new(),
            recording_id: "session".into(),
            received_monotonic_ns: 0,
            use_yes_price: true,
            coverage_period: 0,
            subscription: None,
            book_command: None,
        },
        message: m,
        reconnect_snapshot: false,
    }
}
fn stream(copy: &str, items: Vec<(i64, Value)>) -> Vec<Occurrence> {
    items
        .into_iter()
        .enumerate()
        .map(|(i, (t, m))| occurrence(copy, t, m, i as u64 + 1))
        .collect()
}
fn merge(
    venue: Venue,
    streams: Vec<Vec<Occurrence>>,
    options: MergeOptions,
) -> Result<(Vec<MergedMessage>, MergeStats)> {
    let mut rows = vec![];
    let stats = CopyMerge::new(venue, options)?.merge(
        streams.into_iter().map(|s| s.into_iter().map(Ok)),
        |m| {
            rows.push(m);
            Ok(())
        },
    )?;
    Ok((rows, stats))
}
#[test]
fn coverage_reopens_on_offending_frame_while_books_require_a_new_snapshot() {
    let mut b = VenueBooks::new(BookMode::kalshi(), ["K", "J"]);
    let mut messages = [
        snap(Some(10), 10, 1),
        snap(Some(11), 20, 2),
        delta(12, 30, 4),
        delta(13, 40, 5),
        snap(Some(14), 50, 6),
    ];
    messages[1]["msg"]["market_ticker"] = json!("J");
    let items = messages.iter().enumerate().map(|(i, m)| {
        Ok(CaptureItem::Frame(RawFrame {
            schema: "kalshi.public_raw_frame.v1".into(),
            venue: Venue::Kalshi,
            recording_id: "session".into(),
            connection_generation: 1,
            local_frame_seq: i as u64 + 1,
            received_utc: format!("1970-01-01T00:00:00.{:06}+00:00", (i + 1) * 1000),
            received_monotonic_ns: (i as u64 + 1) * 1_000_000,
            frame_type: "text".into(),
            direction: "received".into(),
            payload_raw: m.to_string(),
        }))
    });
    let mut occurrences = SessionOccurrences::new(Stream::new("a", "c"), "s", items);
    for o in &mut occurrences {
        let o = o.unwrap();
        let seq = o.origin.frame_seq;
        b.apply(&o.message, seq).unwrap();
        let book = b.get("K").unwrap();
        if (3..=4).contains(&seq) {
            assert_eq!(book.integrity(), Integrity::Impaired);
            assert!(book.flags().contains(&F::SeqGap));
        }
        if seq == 5 {
            assert_eq!(book.integrity(), Integrity::Intact);
            assert_eq!(b.get("J").unwrap().integrity(), Integrity::Impaired);
        }
    }
    let cov = occurrences.finish();
    assert_eq!(cov.counts.sequence_gaps, 1);
    assert_eq!(cov.intervals.len(), 2);
    assert_eq!(
        (
            cov.intervals[0].start_us,
            cov.intervals[0].end_us,
            cov.intervals[0].end_reason
        ),
        (1000, 2000, EndReason::SequenceGap)
    );
    assert_eq!(
        (
            cov.intervals[1].start_us,
            cov.intervals[1].end_us,
            cov.intervals[1].start_reason
        ),
        (3000, 5000, StartReason::AfterGap)
    );
    assert_eq!(
        uncovered_intervals(
            cov.intervals.iter().map(|i| (i.start_us, i.end_us)),
            1000,
            5000
        )
        .unwrap(),
        [(2000, 3000)]
    );
}
#[test]
fn reconnect_clock_epoch_prevents_earlier_epoch_from_pulling_new_snapshot_forward() {
    for post_delta in [false, true] {
        let mut frames = stream(
            "a",
            vec![
                (91, snap(Some(100), 90, 1)),
                (111, delta(101, 100, 2)),
                (201, snap(Some(1), 200, 1)),
                (211, delta(2, 205, 2)),
                (221, snap(Some(4), 220, 1)),
            ],
        );
        frames[2].origin.generation = 2;
        frames[3].origin.generation = 2;
        frames[4].origin.generation = 3;
        if post_delta {
            let mut o = occurrence("a", 226, delta(5, 216, 2), 6);
            o.origin.generation = 3;
            frames.push(o);
        }
        let (rows, stats) = merge(Venue::Kalshi, vec![frames], MergeOptions::default()).unwrap();
        let mut expected = vec![
            (100, 90),
            (101, 100),
            (1, 200),
            (2, 205),
            (4, if post_delta { 216 } else { 220 }),
        ];
        if post_delta {
            expected.push((5, 216));
        }
        assert_eq!(
            rows.iter()
                .map(|r| (r.message["msg"]["clock"].as_i64().unwrap(), r.order_ms))
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(stats.snapshot_clock_fallbacks["decreased"], 1);
    }
}
#[test]
fn positive_snapshot_includes_equal_clock_and_precedes_greater_clock_two_copies() {
    let repeats = delta(6, 105, 2);
    let greater = delta(7, 105, 4);
    let a = stream(
        "a",
        vec![
            (100, delta(5, 100, 1)),
            (105, repeats.clone()),
            (106, repeats.clone()),
            (107, greater.clone()),
        ],
    );
    let mut copy = repeats;
    copy["seq"] = json!(50);
    copy["sid"] = json!(19);
    copy["sending_ts_ms"] = json!(108);
    let mut b = stream(
        "b",
        vec![(108, copy), (111, snap(Some(6), 110, 51)), (115, greater)],
    );
    b[1].reconnect_snapshot = true;
    let (rows, stats) = merge(Venue::Kalshi, vec![b, a], MergeOptions::default()).unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| (r.kind.as_str(), r.order_ms))
            .collect::<Vec<_>>(),
        [
            ("orderbook_delta", 100),
            ("orderbook_delta", 105),
            ("orderbook_delta", 105),
            ("orderbook_snapshot", 105),
            ("orderbook_delta", 105)
        ]
    );
    assert_eq!(rows[3].snapshot_class(), Some("reconnect"));
    assert_eq!(rows[1].origin().stream.copy, "a");
    assert_eq!(rows[1].occurrences.len(), 2);
    assert_eq!(rows[2].occurrences.len(), 1);
    assert_eq!(stats.repeats_merged, 2);
}
#[test]
fn zero_missing_and_decreased_clocks_fall_back_to_sending_time() {
    let (rows, stats) = merge(
        Venue::Kalshi,
        vec![stream(
            "a",
            vec![
                (10, delta(10, 9, 1)),
                (20, snap(Some(0), 18, 2)),
                (30, snap(None, 28, 3)),
                (40, snap(Some(3), 38, 4)),
            ],
        )],
        MergeOptions::default(),
    )
    .unwrap();
    assert_eq!(
        rows.iter().map(|r| r.order_ms).collect::<Vec<_>>(),
        [9, 18, 28, 38]
    );
    for reason in ["decreased", "zero", "missing"] {
        assert_eq!(stats.snapshot_clock_fallbacks[reason], 1);
    }
}
#[test]
fn delayed_copy_adopts_latest_market_epoch_only_on_its_own_decrease() {
    // A sees two resets before B observes even the first. B's next reset must
    // join epoch two, avoiding both an extra epoch and a stale epoch one.
    let a = stream(
        "a",
        vec![
            (10, delta(100, 10, 1)),
            (20, delta(1, 20, 2)),
            (30, delta(80, 30, 3)),
            (40, delta(2, 40, 4)),
            (61, snap(Some(8), 60, 5)),
        ],
    );
    let b = stream(
        "b",
        vec![
            (11, delta(100, 10, 1)),
            (45, delta(3, 45, 2)),
            (58, delta(9, 50, 3)),
        ],
    );
    let (rows, _) = merge(Venue::Kalshi, vec![a, b], MergeOptions::default()).unwrap();
    let clocks = rows
        .iter()
        .map(|r| (r.message["msg"]["clock"].as_i64().unwrap(), r.order_ms))
        .collect::<Vec<_>>();
    assert_eq!(
        clocks,
        [
            (100, 10),
            (1, 20),
            (80, 30),
            (2, 40),
            (3, 45),
            (8, 50),
            (9, 50)
        ]
    );
}
#[test]
fn book_order_beats_trade_edges_and_strict_mode_rejects_conflict() {
    let event =
        |kind, label| json!({"event_type":kind,"market":"M","timestamp":"100","label":label});
    let x = event("price_change", "x");
    let y = event("price_change", "y");
    let t = event("last_trade_price", "t");
    let streams = vec![
        stream("a", vec![(100, x.clone()), (101, t.clone())]),
        stream("b", vec![(100, t), (101, y.clone())]),
        stream("c", vec![(100, y), (101, x)]),
    ];
    let (rows, stats) = merge(Venue::Polymarket, streams.clone(), MergeOptions::default()).unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| r.message["label"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["y", "x", "t"]
    );
    assert_eq!(stats.other_order_conflicts, 1);
    assert_eq!(stats.book_order_conflicts, 0);
    assert!(
        merge(
            Venue::Polymarket,
            streams,
            MergeOptions {
                strict_order: true,
                ..Default::default()
            }
        )
        .is_err()
    );
}
#[test]
fn late_untimed_repeat_retention_starts_at_sending_time_after_clock_placement() {
    let s = snap(Some(5), 150, 1);
    let last = delta(7, 300, 3);
    let a = stream("a", vec![(160, s.clone()), (400, last.clone())]);
    let mut s2 = s;
    s2["sending_ts_ms"] = json!(151);
    let b = stream("b", vec![(105, delta(6, 100, 1)), (215, s2), (401, last)]);
    let (rows, stats) = merge(
        Venue::Kalshi,
        vec![a, b],
        MergeOptions {
            lateness_ms: 100,
            retention_ms: 10,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        rows.iter().map(|r| r.order_ms).collect::<Vec<_>>(),
        [100, 100, 300]
    );
    assert_eq!(stats.repeats_after_emit, 2);
}
#[test]
fn identity_matches_python_sorted_ascii_json_and_does_not_round_forwarded_bytes() {
    let wire = r#"{"msg":{"z":1.000000000000000000001,"a":"€😀"},"type":"test","sid":4,"sending_ts_ms":101,"seq":10}"#;
    let v = serde_json::from_str(wire).unwrap();
    let (rows, _) = merge(
        Venue::Kalshi,
        vec![stream("a", vec![(102, v)])],
        MergeOptions::default(),
    )
    .unwrap();
    assert_eq!(
        rows[0].message["msg"]["z"].to_string(),
        "1.000000000000000000001"
    );
    assert_eq!(rows[0].content_digest, "69da6e4bccf22e308b7c83d0c8163262");
}
