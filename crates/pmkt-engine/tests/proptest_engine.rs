//! Seeded generated cases for the public book, capture, merge and live boundaries.
mod capture_fixture;

use arrow_array::{BooleanArray, StringArray};
use capture_fixture::{column, name, raw, root, seal};
use chrono::{DateTime, SecondsFormat};
use num_bigint::BigInt;
use pmkt_engine::{
    BookMode, Result, ScaledDecimal as D, ShardFeed, Venue, VenueBooks,
    book::QuotePolicy,
    capture::{CaptureItem, inspect_session, read_session},
    emit::{Version, reconstruct_merged, reconstruct_session},
    feed::FeedTimeouts,
    health::{Integrity, QualityFlag as F},
    merge::{CopyMerge, EndReason, MergeOptions, SessionOccurrences, Stream},
};
use proptest::{prelude::*, test_runner::RngSeed};
use serde_json::{Value, json};
use std::fs;

const MIDNIGHT_MS: i64 = 1_735_689_600_000;

fn config(cases: u32) -> ProptestConfig {
    ProptestConfig {
        cases,
        rng_seed: RngSeed::Fixed(0x5052_360a),
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

fn utc(us: i64) -> String {
    DateTime::from_timestamp_micros(us)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Micros, false)
}

fn kalshi_book(id: &str, sid: i64, seq: i64, yes: &str, no: &str) -> Value {
    json!({"type":"orderbook_snapshot","sid":sid,"seq":seq,"msg":{
        "market_ticker":id,"yes_dollars_fp":[[yes,"1.2345"]],"no_dollars_fp":[[no,"2.3456"]]
    }})
}

fn kalshi_delta(id: &str, sid: i64, seq: i64) -> Value {
    json!({"type":"orderbook_delta","sid":sid,"seq":seq,"msg":{
        "market_ticker":id,"side":"yes","price_dollars":"0.3","delta_fp":"0.001"
    }})
}

fn poly_book(id: &str) -> Value {
    json!({"event_type":"book","asset_id":id,"market":"M","hash":"initial","timestamp":"100",
        "bids":[{"price":"0.301","size":"1.2345"}],"asks":[{"price":"0.6","size":"2.3456"}]})
}

fn change(id: &str, side: &str, price: &str, size: &str) -> Value {
    json!({"asset_id":id,"side":side,"price":price,"size":size,
        "hash":"next","best_bid":"0.6","best_ask":"0.7"})
}

fn changes(updates: &[Value]) -> Value {
    json!({"event_type":"price_change","market":"M","timestamp":"101","price_changes":updates})
}

fn stream(
    copy: &str,
    packets: &[(i64, Value)],
) -> SessionOccurrences<impl Iterator<Item = Result<CaptureItem>>> {
    let frames = packets
        .iter()
        .enumerate()
        .map(|(i, (ms, msg))| {
            let mut f = raw(i as u64 + 1, 1, "kalshi", msg.clone());
            f["received_utc"] = json!(utc(ms * 1000));
            Ok(CaptureItem::Frame(serde_json::from_value(f)?))
        })
        .collect::<Vec<_>>();
    SessionOccurrences::new(Stream::new(copy, "conn"), "session", frames.into_iter())
}

proptest! {
    #![proptest_config(config(512))]

    #[test]
    fn kalshi_gap_on_snapshot_or_delta(
        first in 20_i64..100_000,
        sid in 1_i64..2_000,
        on_snapshot in any::<bool>(),
        jump in prop_oneof![Just(0_i64), -12_i64..0, 2_i64..12],
        followups in 1_i64..8,
    ) {
        let mut books = VenueBooks::new(BookMode::kalshi(), ["a", "b", "c"]);
        let mut packets = vec![
            (1, kalshi_book("a", sid, first, ".3", ".6")),
            (2, kalshi_book("b", sid, first + 1, ".3", ".6")),
            (3, kalshi_delta("c", sid + 1, 10)), // A new SID has no sequence predecessor.
        ];
        let gap_seq = first + 1 + jump;
        let bad = if on_snapshot {
            kalshi_book("a", sid, gap_seq, ".4", ".6")
        } else {
            kalshi_delta("a", sid, gap_seq)
        };
        packets.push((4, bad));
        for j in 1..=followups {
            packets.push((4 + j, kalshi_delta("a", sid, gap_seq + j)));
        }
        packets.push((
            5 + followups,
            kalshi_book("a", sid, gap_seq + followups + 1, ".3", ".6"),
        ));
        let mut source = stream("a", &packets);
        for (index, item) in source.by_ref().enumerate() {
            let touched = books.apply(&item.unwrap().message, index as u64).unwrap();
            if index == 2 {
                prop_assert_eq!(touched.len(), 1);
                prop_assert_eq!(
                    books.get("c").unwrap().integrity(),
                    Integrity::AwaitingSnapshot
                );
                prop_assert!(books.get("a").unwrap().valid_state());
            } else if index == 3 {
                let ids: Vec<_> = touched.iter().map(|t| t.instrument.as_str()).collect();
                prop_assert_eq!(ids, ["a", "b"]);
            }
            if index >= 3 {
                prop_assert!(books.get("b").unwrap().flags().contains(&F::SeqGap));
                prop_assert!(!books.get("c").unwrap().flags().contains(&F::SeqGap));
                let last = index + 1 == packets.len();
                prop_assert_eq!(books.get("a").unwrap().valid_state(), last);
                prop_assert_eq!(books.get("a").unwrap().flags().contains(&F::SeqGap), !last);
            }
        }
        let coverage = source.finish();
        prop_assert_eq!(coverage.counts.sequence_gaps, 1);
        prop_assert_eq!(coverage.intervals.len(), 2);
        prop_assert_eq!(coverage.intervals[0].end_reason, EndReason::SequenceGap);
        prop_assert_eq!(coverage.intervals[1].start_us, 4_000);
    }

    #[test]
    fn kalshi_native_legs_and_complements(
        bid in 2_i128..9998,
        offset in prop_oneof![Just(0_i128), Just(-1_i128), Just(1_i128)],
        yes_basis in any::<bool>(),
    ) {
        let ask = bid + offset;
        let native_no = if yes_basis { ask } else { 10_000 - ask };
        let price = |x| D::new(x, 4).unwrap();
        let mut books = VenueBooks::new(
            BookMode::Kalshi {
                use_yes_price: yes_basis,
                policy: QuotePolicy::Current,
            },
            ["k"],
        );
        books
            .apply(
                &kalshi_book(
                    "k",
                    1,
                    1,
                    &price(bid).to_string(),
                    &price(native_no).to_string(),
                ),
                1,
            )
            .unwrap();
        let book = books.get("k").unwrap();
        prop_assert_eq!(book.valid_state(), bid <= ask);
        prop_assert_eq!(book.flags().contains(&F::CrossedBook), bid > ask);
        let (yes, no) = book.native_levels();
        prop_assert_eq!(yes[&price(bid)], price(12345));
        prop_assert_eq!(no[&price(native_no)], price(23456));
        let q = book.quotes();
        prop_assert_eq!(q[0].ask.unwrap().price, price(ask));
        prop_assert_eq!(q[1].bid.unwrap().price, price(10_000 - ask));
        prop_assert_eq!(q[1].ask.unwrap().price, price(10_000 - bid));
    }

    #[test]
    fn kalshi_epochs_and_repeat_multiplicity(
        day in 0_i64..365,
        high in 100_i64..50_000,
        c in 2_i64..90,
        repeats_a in 1_usize..7,
        repeats_b in 1_usize..7,
    ) {
        let day = MIDNIGHT_MS + day * 86_400_000;
        let delta = |clock, ms| {
            let mut m = kalshi_delta("a", 1, 1);
            m["msg"]["clock"] = json!(clock);
            m["msg"]["ts_ms"] = json!(day + ms);
            m
        };
        let snap = |clock: Option<i64>, ms| {
            let mut m = kalshi_book("a", 1, 1, ".3", ".6");
            if let Some(v) = clock {
                m["msg"]["clock"] = json!(v);
            }
            m["sending_ts_ms"] = json!(day + ms);
            m
        };
        let copies = |n: usize| {
            (0..n)
                .map(|i| (day - 20 + i as i64, delta(high, -20)))
                .collect::<Vec<_>>()
        };
        let mut a = copies(repeats_a);
        let mut b = copies(repeats_b);
        // Only A resets, without reconnecting, after midnight. B's old high
        // clock cannot pull C to an earlier time in the new epoch.
        a.extend([
            (day + 1, snap(Some(1), 1)),
            (day + 5, delta(c, 5)),
            (day + 11, snap(Some(c), 10)),
            (day + 12, delta(c + 1, 7)),
            (day + 20, snap(Some(0), 20)),
            (day + 30, snap(None, 30)),
        ]);
        b.push((day + 3, delta(high + 1, 3)));
        for s in [&mut a, &mut b] {
            for (i, (_, m)) in s.iter_mut().enumerate() {
                m["seq"] = json!(i + 1);
            }
        }
        let mut rows = vec![];
        let stats = CopyMerge::new(Venue::Kalshi, MergeOptions::default())
            .unwrap()
            .merge([stream("a", &a), stream("b", &b)], |m| {
                rows.push(m);
                Ok(())
            })
            .unwrap();
        let n = repeats_a.max(repeats_b);
        prop_assert_eq!(rows.len(), n + 7);
        prop_assert!(rows[..n].iter().all(|r| r.message["msg"]["clock"] == high));
        prop_assert_eq!(stats.repeats_merged, repeats_a.min(repeats_b) as u64);
        prop_assert_eq!(
            rows[..n].iter().map(|r| r.occurrences.len()).sum::<usize>(),
            repeats_a + repeats_b
        );
        let times: Vec<_> = rows[n..]
            .iter()
            .map(|r| (r.order_ms - day, r.checkpoint))
            .collect();
        prop_assert_eq!(
            times,
            [
                (1, true),
                (3, false),
                (5, false),
                (7, true),
                (7, false),
                (20, true),
                (30, true)
            ]
        );
        for reason in ["decreased", "missing", "zero"] {
            prop_assert_eq!(stats.snapshot_clock_fallbacks[reason], 1);
        }
    }

    #[test]
    fn poly_atomic_changes_tick_and_hash(
        size in 1_i128..50_000_000,
        digits in 3_u32..16,
        tick in 1_i128..99,
    ) {
        let mut books = VenueBooks::new(BookMode::Polymarket, ["a", "b"]);
        for id in ["a", "b"] {
            books.apply(&poly_book(id), 1).unwrap();
        }
        let qty = D::new(size, digits).unwrap();
        let packet = changes(&[
            change("b", "SELL", "0.8", "6"),
            change("a", "BUY", ".302", "9"),
            change("a", "BUY", ".302", &qty.to_string()),
            change("a", "BUY", ".301", "0"),
        ]);
        let touched = books.apply(&packet, 2).unwrap();
        prop_assert_eq!(
            touched
                .iter()
                .map(|t| t.instrument.as_str())
                .collect::<Vec<_>>(),
            ["b", "a"]
        );
        books.apply(&json!({"event_type":"tick_size_change","asset_id":"a","new_tick_size":D::new(tick,2).unwrap().to_string()}),3).unwrap();
        let book = books.get("a").unwrap();
        prop_assert_eq!(book.tick_size(), Some(D::new(tick, 2).unwrap()));
        let bids = book.native_levels().0;
        prop_assert_eq!(bids.len(), 1);
        prop_assert_eq!(bids[&D::new(302, 3).unwrap()], qty);
        let mut bad = change("a", "BUY", ".302", "0");
        bad["previous_hash"] = json!("wrong");
        books.apply(&changes(&[bad]), 4).unwrap();
        prop_assert!(books.get("a").unwrap().flags().contains(&F::HashMismatch));
        prop_assert!(books.get("b").unwrap().valid_state());
    }

    #[test]
    fn decimal_rounding_and_addition_match_unbounded_integers(
        a in prop_oneof![any::<i128>(),Just(i128::MIN), Just(i128::MAX)],
        s in 0_u32..=38,
        b in 1_i128..=i128::MAX,
        t in 0_u32..=38,
    ) {
        let lhs = D::new(a, s).unwrap();
        let step = D::new(b, t).unwrap();
        let scale = s.max(t);
        let ten = BigInt::from(10);
        let x = BigInt::from(a) * ten.pow(scale - s);
        let y = BigInt::from(b) * ten.pow(scale - t);
        let remainder = ((&x % &y) + &y) % &y;
        let floor = &x - &remainder;
        let ceil = if remainder == BigInt::ZERO {
            floor.clone()
        } else {
            &floor + &y
        };
        for (got, expected) in [
            (lhs.floor_to(step), floor),
            (lhs.ceil_to(step), ceil),
            (lhs.checked_add(step), x + y),
        ] {
            // Canonicalize using decimal text, independently of ScaledDecimal's
            // alignment, arithmetic, parser and storage bounds.
            let digits = expected.to_string();
            let zeros = (digits.len() - digits.trim_end_matches('0').len()).min(scale as usize);
            let reduced = &expected / ten.pow(zeros as u32);
            match i128::try_from(reduced) {
                Ok(c) => prop_assert_eq!(got.unwrap(), D::new(c, scale - zeros as u32).unwrap()),
                Err(_) => prop_assert!(got.is_err()),
            }
        }
    }
}

fn send(shard: &mut ShardFeed, event: &Value, ns: u64) {
    shard
        .ingest_updates("s", 1, &serde_json::to_vec(event).unwrap(), ns)
        .unwrap();
}

fn whole_shard(shard: &mut ShardFeed, now: u64, reasons: [Option<&str>; 3]) {
    for (id, reason) in ["a", "b", "c"].into_iter().zip(reasons) {
        let read = shard
            .read_book(id, now)
            .unwrap()
            .map(|b| (b.bids().collect::<Vec<_>>(), b.asks().collect::<Vec<_>>()));
        assert_eq!(read.is_some(), reason.is_none(), "{id} at {now}");
        let o = shard.observation(id, now).unwrap();
        assert_eq!(o.health.reason.as_deref(), reason, "{id} at {now}");
        assert_eq!(o.health.usable, reason.is_none());
        assert_eq!(o.book.is_some(), o.health.usable);
        if let Some(b) = o.book {
            assert_eq!(b.instrument, id);
            assert!(b.bids.windows(2).all(|w| w[0].0 > w[1].0));
            assert!(b.asks.windows(2).all(|w| w[0].0 < w[1].0));
            assert!(b.bids.iter().chain(&b.asks).all(|(_, q)| *q > D::ZERO));
            assert_eq!(b.generation, o.health.generation);
            assert_eq!(b.metadata_revision, o.health.metadata_revision);
            assert_eq!(read.unwrap(), (b.bids, b.asks));
        }
    }
}

proptest! {
    #![proptest_config(config(128))]

    #[test]
    fn live_kalshi_sequence_fault_is_shard_wide(
        first in 10_i64..100_000,
        wrong_sid in any::<bool>(),
        tenths in 3_u8..10,
    ) {
        let mut shard = ShardFeed::new(
            Venue::Kalshi,
            "s",
            ["a", "b", "c"],
            FeedTimeouts {
                liveness_ns: 10_000,
                snapshot_ns: 1_000,
            },
        )
        .unwrap();
        shard.connected(1, Some(11), 0).unwrap();
        let missing = Some("no_initial_snapshot");
        whole_shard(&mut shard, 0, [missing; 3]);
        send(
            &mut shard,
            &json!({"type":"subscribed","id":11,"msg":{"channel":"orderbook_delta","sid":7}}),
            1,
        );
        whole_shard(&mut shard, 1, [missing; 3]);
        let mut a = kalshi_book("a", 7, first, ".3", ".6");
        // Aligning this integer to tenths overflows i128 by three. Subtracting
        // at least 0.3 leaves a representable result, including i128::MAX.
        let large = i128::MAX / 10 + 1;
        a["msg"]["yes_dollars_fp"][0][1] = json!(large.to_string());
        send(&mut shard, &a, 2);
        whole_shard(&mut shard, 2, [None, missing, missing]);
        send(&mut shard, &kalshi_book("b", 7, first + 1, ".3", ".6"), 3);
        whole_shard(&mut shard, 3, [None, None, missing]);
        let mut d = kalshi_delta("a", 7, first + 2);
        d["msg"]["delta_fp"] = json!(format!("-0.{tenths}"));
        send(&mut shard, &d, 4);
        whole_shard(&mut shard, 4, [None, None, missing]);
        let book = shard.observation("a", 4).unwrap().book.unwrap();
        let expected = D::new(i128::MAX - i128::from(tenths - 3), 1).unwrap();
        prop_assert_eq!(book.bids[0].1, expected);
        whole_shard(&mut shard, 1_000, [None, None, Some("snapshot_timeout")]);
        d["sid"] = json!(if wrong_sid { 8 } else { 7 });
        let err = shard
            .ingest_updates("s", 1, &serde_json::to_vec(&d).unwrap(), 1_001)
            .unwrap_err();
        let reason = if wrong_sid {
            "invalid_book_identity_or_sequence"
        } else {
            "subscription_sequence_gap"
        };
        prop_assert_eq!(err.to_string(), reason);
        whole_shard(&mut shard, 1_001, [Some(reason); 3]);
    }

    #[test]
    fn live_grace_and_whole_shard_deadlines(
        start in 0_u64..1_000_000_000,
        followups in 14_usize..20,
        late in any::<bool>(),
    ) {
        let mut shard = ShardFeed::new(
            Venue::Polymarket,
            "s",
            ["a", "b", "c"],
            FeedTimeouts {
                liveness_ns: 4_000_000_000,
                snapshot_ns: 1_000_000_000,
            },
        )
        .unwrap();
        shard.connected(1, None, start).unwrap();
        let missing = Some("no_initial_snapshot");
        let crossed = Some("crossed_book");
        let integrity = Some("book_integrity_failed");
        let timeout = Some("snapshot_timeout");
        for (i, id) in ["a", "b"].into_iter().enumerate() {
            send(&mut shard, &poly_book(id), start);
            whole_shard(
                &mut shard,
                start,
                [None, if i == 0 { missing } else { None }, missing],
            );
        }
        send(
            &mut shard,
            &changes(&[change("a", "BUY", ".6", "2")]),
            start,
        );
        whole_shard(&mut shard, start, [crossed, None, missing]);
        for _ in 0..followups {
            send(&mut shard, &json!({"event_type":"last_trade_price"}), start);
            whole_shard(&mut shard, start, [crossed, None, missing]);
        }
        let time = start + 250_000_000 - u64::from(!late);
        whole_shard(
            &mut shard,
            time,
            [if late { integrity } else { crossed }, None, missing],
        );
        send(&mut shard, &changes(&[change("a", "BUY", ".6", "0")]), time);
        whole_shard(
            &mut shard,
            time,
            [if late { integrity } else { None }, None, missing],
        );
        let time = start + 1_000_000_000;
        whole_shard(
            &mut shard,
            time,
            [if late { integrity } else { None }, None, timeout],
        );
        let requests = shard.begin_requests(1, time).unwrap();
        prop_assert_eq!(
            requests.iter().map(String::as_str).collect::<Vec<_>>(),
            if late { vec!["a", "c"] } else { vec!["c"] }
        );
        shard.finish_requests(1, &requests, time).unwrap();
        whole_shard(
            &mut shard,
            time + 1_000_000_000,
            [
                if late { Some("recovery_timeout") } else { None },
                None,
                timeout,
            ],
        );
        whole_shard(
            &mut shard,
            time + 4_000_000_001,
            [Some("liveness_expired"); 3],
        );
    }
}

proptest! {
    // Bound filesystem work separately from the in-memory generators.
    #![proptest_config(config(24))]

    #[test]
    fn self_merge_replays_every_topbook_and_depth_column_in_file_order(
        operations in prop::collection::vec((0_u8..7, 0_i64..10, -4_i64..5, any::<bool>()), 5..30),
        batch_size in 1_usize..16,
        v2 in any::<bool>(),
    ) {
        let version = if v2 { Version::V2 } else { Version::V1 };
        for venue in [Venue::Polymarket, Venue::Kalshi] {
            let mut previous = match venue {
                Venue::Polymarket => poly_book("a"),
                Venue::Kalshi => kalshi_book("a", 1, 1, ".3", ".6"),
            };
            let mut frames = vec![raw(1, 1, venue.as_str(), previous.clone())];
            let mut generation = 1;
            let mut sequence = 1;
            for &(action, book_clock, jitter, other) in &operations {
                let id = if other { "b" } else { "a" };
                let seq = frames.len() as u64 + 1;
                let ms = MIDNIGHT_MS + jitter;
                if action == 0 {
                    generation += 1;
                    sequence = 0;
                }
                sequence += if action == 1 { 3 } else { 1 };
                let snapshot = action == 2 || action == 0;
                let mut msg = match venue {
                    Venue::Kalshi => {
                        let mut msg = if snapshot {
                            kalshi_book(id, 1, sequence, ".3", ".6")
                        } else {
                            kalshi_delta(id, 1, sequence)
                        };
                        msg["sending_ts_ms"] = json!(ms);
                        msg["msg"]["clock"] = json!(book_clock);
                        if !snapshot { msg["msg"]["ts_ms"] = json!(ms); }
                        msg
                    }
                    Venue::Polymarket => {
                        let mut msg = if snapshot {
                            poly_book(id)
                        } else {
                            changes(&[change(id, "BUY", ".6", if other { "0" } else { "2" })])
                        };
                        msg["timestamp"] = json!(ms.to_string());
                        msg
                    }
                };
                if action == 3 { msg = previous.clone(); }
                let payload = if action == 4 { json!([previous, msg]) } else { msg.clone() };
                let mut frame = raw(seq, generation, venue.as_str(), payload);
                if action == 5 {
                    frame["frame_type"] = json!("transport_error");
                    frame["direction"] = json!("local");
                } else {
                    previous = msg;
                }
                frame["received_utc"] = json!(utc(ms * 1000 + book_clock));
                frame["received_monotonic_ns"] = json!(book_clock * 100_000_000);
                frames.push(frame);
            }
            let path = root();
            seal(&path, 1, &frames, None);
            let mut expected = vec![];
            reconstruct_session(&path, version, batch_size, |b| {
                expected.push((b.topbook, b.depth));
                Ok(())
            }).unwrap();
            let mut actual = vec![];
            reconstruct_merged(venue, vec![("a".into(), "conn".into(), path)], version, batch_size, |b| {
                actual.push((b.topbook, b.depth));
                Ok(())
            }, |_| Ok(())).unwrap();
            prop_assert_eq!(actual, expected);
        }
    }

    #[test]
    fn file_order_corrective_limits(
        unrelated in 13_usize..18,
        ns in 249_999_998_u64..250_000_003,
    ) {
        let path = root();
        let mut packets = vec![poly_book("a"), changes(&[change("a", "BUY", ".6", "2")])];
        packets.extend((0..unrelated).map(|_| json!({"event_type":"last_trade_price"})));
        packets.push(changes(&[change("a", "BUY", ".6", "0")]));
        let last = packets.len() - 1;
        let frames = packets
            .into_iter()
            .enumerate()
            .map(|(i, m)| {
                let mut f = raw(i as u64 + 1, 1, "polymarket", m);
                f["received_monotonic_ns"] = json!(if i == last { ns } else { 0 });
                f
            })
            .collect::<Vec<_>>();
        seal(&path, 1, &frames, None);
        let mut health = vec![];
        reconstruct_session(&path, Version::V2, 1, |b| {
            health.extend(column::<BooleanArray>(&b.topbook, "book_integrity_valid").iter());
            Ok(())
        })
        .unwrap();
        prop_assert_eq!(
            health,
            [
                Some(true),
                Some(false),
                Some(unrelated + 1 < 16 && ns < 250_000_000)
            ]
        );
    }

    #[test]
    fn verified_chunks_across_midnight(
        missing in 1_u64..8,
        truncate in 1_usize..12,
        duplicate in any::<bool>(),
    ) {
        let path = root();
        let mut first = raw(1, 1, "polymarket", poly_book("a"));
        first["received_utc"] = json!(utc(MIDNIGHT_MS * 1000 - 1));
        seal(&path, 1, &[first], None);
        let next = missing + 2;
        let mut second = raw(next, 1, "polymarket", poly_book("a"));
        second["received_utc"] = json!(utc(MIDNIGHT_MS * 1000));
        let mut third = second.clone();
        third["local_frame_seq"] = json!(if duplicate { next } else { next + 1 });
        seal(&path, next, &[second, third], Some("archived"));
        let gz = fs::read(path.join(name(1))).unwrap();
        fs::write(
            path.join("chunk_000002.jsonl.gz.part"),
            &gz[..gz.len() - truncate],
        )
        .unwrap();
        fs::write(
            path.join("chunk_000002.jsonl.gz.manifest.json.tmp"),
            b"{\"sha256\":",
        )
        .unwrap();
        let mut items = read_session(&path, true).unwrap();
        prop_assert!(matches!(
            items.next().unwrap().unwrap(),
            CaptureItem::Frame(_)
        ));
        let CaptureItem::Gap(gap) = items.next().unwrap().unwrap() else {
            panic!("missing in-band gap")
        };
        prop_assert_eq!(
            (
                gap.first_missing,
                gap.last_missing,
                gap.missing_frame_count()
            ),
            (2, 1 + missing, missing)
        );
        prop_assert!(matches!(
            items.next().unwrap().unwrap(),
            CaptureItem::Frame(_)
        ));
        if duplicate {
            prop_assert!(items.next().unwrap().is_err());
            prop_assert!(inspect_session(&path).is_err());
        } else {
            let report = inspect_session(&path).unwrap();
            prop_assert_eq!(report.frame_count, 3);
            prop_assert_eq!(report.incomplete_files.len(), 2);
            prop_assert_eq!(report.first_received_utc, Some(utc(MIDNIGHT_MS * 1000 - 1)));
            prop_assert_eq!(report.last_received_utc, Some(utc(MIDNIGHT_MS * 1000)));
        }
    }

    #[test]
    fn collision_keys_and_millisecond_rounding(
        backstep in 1_i64..100_000,
        ms in 1_i64..200_000,
        above_half in any::<bool>(),
        batch_size in 1_usize..9,
    ) {
        let a = root();
        let b = root();
        let before_midnight = MIDNIGHT_MS * 1000 - 1;
        let mut snap = poly_book("a");
        snap["timestamp"] = json!(format!("{ms}.000{}", if above_half { 501 } else { 499 }));
        let mut first = raw(1, 1, "polymarket", json!([snap, snap]));
        first["received_utc"] = json!(utc(before_midnight));
        let mut after = snap.clone();
        after["timestamp"] = json!((ms + 1).to_string());
        let mut second = raw(2, 1, "polymarket", after);
        second["received_utc"] = json!(utc(before_midnight - backstep));
        seal(&a, 1, &[first, second], None);
        // An independent observation on B shares A's wall time but has a
        // different exchange time. Repeated snapshots on A are real wire events.
        let mut other = snap;
        other["timestamp"] = json!((ms + 2).to_string());
        let mut frame = raw(1, 1, "polymarket", other);
        frame["received_utc"] = json!(utc(before_midnight));
        seal(&b, 1, &[frame], None);
        for merged in [false, true] {
            let mut top = vec![];
            let mut depth = vec![];
            let mut exchange = vec![];
            let mut gather = |batch: pmkt_engine::emit::QuoteBatches| {
                for (target, rows, field) in [
                    (&mut top, &batch.topbook, "received_at_utc"),
                    (&mut depth, &batch.depth, "received_at_utc"),
                    (&mut exchange, &batch.topbook, "exchange_ts_utc"),
                ] {
                    target.extend(
                        column::<StringArray>(rows, field)
                            .iter()
                            .map(|s| s.unwrap().to_owned()),
                    );
                }
                Ok(())
            };
            if merged {
                reconstruct_merged(
                    Venue::Polymarket,
                    vec![
                        ("a".into(), "c".into(), a.clone()),
                        ("b".into(), "c".into(), b.clone()),
                    ],
                    Version::V2,
                    batch_size,
                    &mut gather,
                    |_| Ok(()),
                )
                .unwrap();
            } else {
                reconstruct_session(&a, Version::V2, batch_size, &mut gather).unwrap();
            }
            let count = if merged { 4 } else { 3 };
            // The fractional venue timestamp has no integer merge-order key;
            // inspect its event independently, irrespective of merged ordering.
            let exact = DateTime::from_timestamp_micros(ms * 1000 + i64::from(above_half)).unwrap();
            let expected_exchange = exact.to_rfc3339_opts(
                if exact.timestamp_subsec_micros() == 0 {
                    SecondsFormat::Secs
                } else {
                    SecondsFormat::Micros
                },
                false,
            );
            prop_assert_eq!(
                exchange.iter().filter(|&t| t == &expected_exchange).count(),
                2
            );
            prop_assert_eq!(top.len(), count);
            prop_assert_eq!(depth.len(), count * 2);
            for (i, t) in top.iter().enumerate() {
                prop_assert_eq!(&depth[2 * i..2 * i + 2], [t.as_str(), t.as_str()]);
                if i > 0 {
                    prop_assert!(top[i - 1] < *t);
                }
            }
            if !merged {
                let instants: Vec<_> = top
                    .iter()
                    .map(|s| DateTime::parse_from_rfc3339(s).unwrap().timestamp_micros())
                    .collect();
                prop_assert_eq!(
                    instants,
                    [before_midnight, before_midnight + 1, before_midnight + 2]
                );
            }
        }
    }
}
