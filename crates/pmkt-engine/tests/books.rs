use pmkt_engine::{
    BookMode, ScaledDecimal as D, ShardFeed, Venue, VenueBooks,
    book::QuotePolicy,
    feed::FeedTimeouts,
    health::{CORRECTIVE_DELTA_GRACE_NS as GRACE, Integrity, QualityFlag as F},
    wire,
};
use serde_json::{Value, json};

fn d(s: &str) -> D {
    s.parse().unwrap()
}
fn apply(b: &mut VenueBooks, venue: Venue, v: Value, count: u64) {
    let bytes = v.to_string();
    let frame = wire::messages(bytes.as_bytes()).unwrap().pop().unwrap();
    for update in b.apply(&frame.value, count).unwrap() {
        assert_eq!(b.get(&update.instrument).unwrap().mode().venue(), venue);
    }
}
fn snapshot() -> Value {
    json!({"event_type":"book","market":"M","asset_id":"X","timestamp":"100",
    "bids":[["0.4","1"],["0.3","2"]],"asks":[["0.5","3"],["0.7","4"]],"hash":"h","tick_size":"0.1","min_order_size":"2"})
}
fn delta(side: &str, p: &str, q: &str) -> Value {
    json!({"event_type":"price_change","market":"M","timestamp":"101",
        "price_changes":[{"asset_id":"X","side":side,"price":p,"size":q,"best_bid":"0.4","best_ask":"0.6","hash":"h2"}]})
}
fn poly_feed() -> ShardFeed {
    let mut feed =
        ShardFeed::new(Venue::Polymarket, "s", ["X", "Y"], FeedTimeouts::default()).unwrap();
    feed.connected(1, None, 0).unwrap();
    feed
}
fn ingest(feed: &mut ShardFeed, v: Value, now: u64) {
    feed.ingest("s", 1, v.to_string().as_bytes(), now).unwrap();
}
#[test]
fn raw_batch_slices_preserve_exact_numbers_and_spacing() {
    let bytes = br#" [  {"p":0.123456789012345678901230} , null,  [1, 2] ] "#;
    let batch = wire::messages(bytes).unwrap();
    assert_eq!(batch[0].text, r#"{"p":0.123456789012345678901230}"#);
    assert_eq!(
        D::from_json(&batch[0].value["p"]).unwrap(),
        d(".12345678901234567890123")
    );
    assert_eq!(batch[1].position, Some(1));
    assert!(batch[1].value.is_null());
    assert_eq!(batch[2].text, "[1, 2]");
}
#[test]
fn poly_deltas_delete_exactly_one_level_and_tick_metadata_never_rekeys() {
    let mut feed = poly_feed();
    let raw=br#"{"event_type":"book","asset_id":"X","market":"M","timestamp":"101","bids":[[0.40000000000000000001,1],[0.40000000000000000002,2]],"asks":[["0.5","4"]]}"#;
    let original = feed.ingest("s", 1, raw, 5).unwrap().remove(0).book.unwrap();
    assert_eq!(original.bids[0], (d(".40000000000000000002"), d("2")));
    let tick = json!({"event_type":"tick_size_change","asset_id":"X","new_tick_size":"0.01","timestamp":"105"});
    ingest(&mut feed, tick.clone(), 8);
    ingest(&mut feed, tick, 9);
    let b = feed.observation("X", 10).unwrap().book.unwrap();
    assert_eq!(b.bids, original.bids);
    assert_eq!(b.metadata_revision, 2);
    assert_eq!(b.tick_size, Some(d(".01")));
    assert_eq!(b.received_monotonic_ns, 5);
    assert_eq!(b.source_timestamp.value, Some(json!("101")));
    ingest(&mut feed, delta("BUY", "0.40000000000000000002", "0"), 11);
    assert_eq!(
        feed.observation("X", 11).unwrap().book.unwrap().bids,
        [(d(".40000000000000000001"), d("1"))]
    );
    assert_eq!(original.bids.len(), 2);
}
#[test]
fn late_correction_latches_but_in_window_managed_correction_restores_book() {
    for (delay, usable) in [(GRACE - 1, true), (GRACE, false)] {
        let mut feed = poly_feed();
        ingest(&mut feed, snapshot(), 1);
        ingest(&mut feed, delta("BUY", "0.5", "2"), 2);
        assert_eq!(
            feed.health("X", 2).unwrap().reason.as_deref(),
            Some("crossed_book")
        );
        ingest(&mut feed, delta("SELL", "0.5", "0"), 2 + delay);
        assert_eq!(feed.health("X", 2 + delay).unwrap().usable, usable);
        if !usable {
            assert_eq!(
                feed.health("X", 2 + delay).unwrap().reason.as_deref(),
                Some("book_integrity_failed")
            );
            ingest(&mut feed, snapshot(), 3 + delay);
            assert!(feed.health("X", 3 + delay).unwrap().usable);
        }
    }
}
#[test]
fn recorder_grace_starts_at_decision_and_bounds_unrelated_messages() {
    let mut books = VenueBooks::new(BookMode::Polymarket, ["X"]);
    apply(&mut books, Venue::Polymarket, snapshot(), 1);
    apply(&mut books, Venue::Polymarket, delta("BUY", "0.5", "2"), 2);
    assert_eq!(books.complementary.next_deadline_ns(), None);
    assert!(books.complementary.defer("X", 2, 4_000_000_000));
    assert!(!books.complementary.due(17, 4_000_000_000 + GRACE - 1));
    assert!(books.complementary.due(18, 4_000_000_001));
    assert!(books.complementary.due(2, 4_000_000_000 + GRACE));
    let mut wrong = delta("BUY", "0.5", "2");
    wrong["price_changes"][0]["hash"] = json!("different");
    apply(&mut books, Venue::Polymarket, wrong, 3);
    assert!(!books.complementary.defer("X", 3, 9_000_000_000));
}
#[test]
fn recovery_send_barrier_and_generation_guard() {
    let mut f = poly_feed();
    let mut bad = snapshot();
    bad["bids"] = json!("oops");
    ingest(&mut f, bad, 1);
    let ids = f.begin_requests(1, 2).unwrap();
    assert_eq!(ids, ["X"]);
    ingest(&mut f, snapshot(), 3);
    assert!(!f.health("X", 3).unwrap().usable);
    f.finish_requests(1, &ids, 4).unwrap();
    assert!(f.health("X", 4).unwrap().usable);
    f.connected(2, None, 100).unwrap();
    assert!(
        f.ingest("s", 1, snapshot().to_string().as_bytes(), 101)
            .is_err()
    );
    assert!(f.finish_requests(1, &ids, 101).is_err());
    assert!(!f.health("X", 101).unwrap().usable);
    assert!(f.ingest("other", 2, b"PING", 102).is_err());
}

#[test]
fn waiting_identity_request_keeps_its_deadline_across_anonymous_deltas() {
    let mut f = poly_feed();
    ingest(&mut f, snapshot(), 0);
    let mut wrong = delta("BUY", ".2", "9");
    wrong["market"] = json!("wrong");
    ingest(&mut f, wrong, 1);
    let request = f.begin_requests(1, 2).unwrap();
    assert_eq!(request, ["X"]);
    f.finish_requests(1, &request, 3).unwrap();
    let mut anonymous = delta("BUY", ".2", "2");
    anonymous.as_object_mut().unwrap().remove("market");
    ingest(&mut f, anonymous, 4);
    assert!(f.begin_requests(1, 5).unwrap().is_empty());
    f.heartbeat(1, 25_000_000_003).unwrap();
    assert_eq!(
        f.health("X", 30_000_000_003).unwrap().reason.as_deref(),
        Some("recovery_timeout")
    );
    ingest(&mut f, snapshot(), 30_000_000_004);
    assert!(f.health("X", 30_000_000_004).unwrap().usable);
}

#[test]
fn kalshi_ack_timeout_and_failed_send_answer_cannot_publish_partial_depth() {
    let mut k = ShardFeed::new(
        Venue::Kalshi,
        "s",
        ["X"],
        FeedTimeouts {
            liveness_ns: 500,
            snapshot_ns: 50,
        },
    )
    .unwrap();
    assert_eq!(k.health("X", 0).unwrap().connection_state, "initialized");
    k.connected(1, Some(1), 0).unwrap();
    assert_eq!(
        k.health("X", 50).unwrap().reason.as_deref(),
        Some("subscription_ack_timeout")
    );
    let mut f = poly_feed();
    let mut bad = snapshot();
    bad["asks"] = json!(false);
    ingest(&mut f, bad.clone(), 1);
    let request = f.begin_requests(1, 2).unwrap();
    ingest(&mut f, snapshot(), 3);
    ingest(&mut f, bad, 4);
    f.finish_requests(1, &request, 5).unwrap();
    assert!(!f.health("X", 5).unwrap().usable);
    ingest(&mut f, delta("BUY", "0.4", "1"), 6);
    assert!(!f.health("X", 6).unwrap().usable);
    ingest(&mut f, snapshot(), 7);
    assert!(f.health("X", 7).unwrap().usable);
}
#[test]
fn liveness_timeout_precedes_late_heartbeat_and_one_sided_books_are_usable() {
    let mut f = poly_feed();
    let mut one = snapshot();
    one["asks"] = json!([]);
    ingest(&mut f, one, 2);
    let h = f.health("X", 30_000_000_002).unwrap();
    assert!(h.usable);
    assert!(h.quality_flags.contains(&F::EmptyAsk));
    assert!(f.heartbeat(1, 30_000_000_003).is_err());
    let h = f.health("X", 30_000_000_003).unwrap();
    assert!(!h.usable);
    assert_eq!(h.reason.as_deref(), Some("liveness_expired"));
}
#[test]
fn established_identity_and_resolution_survive_recovery() {
    let mut f = poly_feed();
    ingest(&mut f, snapshot(), 1);
    let mut wrong = delta("BUY", ".2", "9");
    wrong["market"] = json!("wrong");
    ingest(&mut f, wrong, 2);
    assert_eq!(
        f.health("X", 2).unwrap().reason.as_deref(),
        Some("market_identity_changed")
    );
    let mut anonymous = snapshot();
    anonymous.as_object_mut().unwrap().remove("market");
    ingest(&mut f, anonymous, 3);
    assert!(!f.health("X", 3).unwrap().usable);
    ingest(&mut f, snapshot(), 4);
    assert!(f.health("X", 4).unwrap().usable);
    ingest(
        &mut f,
        json!({"event_type":"market_resolved","market":"M"}),
        5,
    );
    f.connected(2, None, 6).unwrap();
    f.ingest("s", 2, snapshot().to_string().as_bytes(), 7)
        .unwrap();
    assert_eq!(
        f.health("X", 7).unwrap().reason.as_deref(),
        Some("market_resolved")
    );
}
#[test]
fn malformed_batch_and_hash_mismatch_fail_closed() {
    let mut f = poly_feed();
    ingest(&mut f, snapshot(), 1);
    assert!(
        f.ingest("s", 1, br#"[{"event_type":"book","asset_id":"X"},3]"#, 2)
            .is_err()
    );
    assert_eq!(f.health("X", 2).unwrap().connection_state, "disconnected");
    let mut b = VenueBooks::new(BookMode::Polymarket, ["X"]);
    apply(&mut b, Venue::Polymarket, snapshot(), 1);
    let mut c = delta("BUY", "0.4", "2");
    c["price_changes"][0]["prev_hash"] = json!("wrong");
    apply(&mut b, Venue::Polymarket, c, 2);
    apply(&mut b, Venue::Polymarket, delta("BUY", "0.4", "1"), 3);
    assert!(b.get("X").unwrap().flags().contains(&F::HashMismatch));
    apply(&mut b, Venue::Polymarket, snapshot(), 4);
    assert!(b.get("X").unwrap().valid_state());
}
#[test]
fn kalshi_golden_wire_mode_projection_and_signed_deltas() {
    for bytes in [
        include_str!("../../../tests/fixtures/kalshi_orderbook_yes_price.json"),
        include_str!("../../../tests/fixtures/kalshi_orderbook_no_price.json"),
    ] {
        let t: Value = serde_json::from_str(bytes).unwrap();
        let mode = BookMode::Kalshi {
            use_yes_price: t["use_yes_price"].as_bool().unwrap(),
            policy: QuotePolicy::Current,
        };
        let mut books = VenueBooks::new(mode, ["KXTRANSCRIPT"]);
        for (i, m) in t["messages"].as_array().unwrap().iter().enumerate() {
            apply(&mut books, Venue::Kalshi, m.clone(), i as u64);
        }
        let b = books.get("KXTRANSCRIPT").unwrap();
        assert!(b.valid_state());
        for q in b.quotes() {
            let expect = &t["expected"][q.outcome.unwrap()];
            for (name, quote) in [("bid", q.bid.unwrap()), ("ask", q.ask.unwrap())] {
                assert_eq!(
                    quote.price,
                    D::from_json(&expect[format!("best_{name}_dollars")]).unwrap()
                );
                assert_eq!(
                    quote.size,
                    D::from_json(&expect[format!("{name}_size_contracts")]).unwrap()
                );
                assert_eq!(
                    quote.derived,
                    expect[format!("best_{name}_source")] == "complement_derived"
                );
            }
        }
        let mut delta = t["messages"][1].clone();
        let p = D::from_json(&delta["msg"]["price_dollars"]).unwrap();
        delta["msg"]["delta_fp"] = json!(format!("-{}", b.native_levels().1[&p]));
        delta["seq"] = json!(3);
        apply(&mut books, Venue::Kalshi, delta.clone(), 3);
        assert!(
            !books
                .get("KXTRANSCRIPT")
                .unwrap()
                .native_levels()
                .1
                .contains_key(&p)
        );
        delta["seq"] = json!(4);
        apply(&mut books, Venue::Kalshi, delta, 4);
        assert!(
            books
                .get("KXTRANSCRIPT")
                .unwrap()
                .flags()
                .contains(&F::MalformedBook)
        );
    }
    assert!(QuotePolicy::parse(Some("")).is_err());
    assert_eq!(QuotePolicy::parse(None).unwrap(), QuotePolicy::Legacy);
}
#[test]
fn kalshi_ack_is_bound_to_command_and_sid_and_sequence_is_shard_wide() {
    let mut f = ShardFeed::new(Venue::Kalshi, "s", ["X", "Y"], FeedTimeouts::default()).unwrap();
    f.connected(1, Some(7), 0).unwrap();
    ingest(
        &mut f,
        json!({"type":"subscribed","id":7,"msg":{"sid":12,"channel":"orderbook_delta"}}),
        1,
    );
    let snapshot = |ticker, seq| json!({"type":"orderbook_snapshot","sid":12,"seq":seq,"msg":{"market_ticker":ticker,"yes":[["0.2","3"]]}});
    ingest(&mut f, snapshot("X", 1), 2);
    ingest(&mut f, snapshot("Y", 2), 3);
    ingest(
        &mut f,
        json!({"type":"orderbook_delta","sid":12,"seq":3,"msg":{"market_ticker":"X","side":"yes","price":"0.2","delta":"1"}}),
        4,
    );
    assert_eq!(
        f.observation("X", 4).unwrap().book.unwrap().bids[0].1,
        d("4")
    );
    assert!(
        f.ingest("s", 1, snapshot("Y", 5).to_string().as_bytes(), 5)
            .is_err()
    );
    for id in ["X", "Y"] {
        assert_eq!(
            f.health(id, 5).unwrap().reason.as_deref(),
            Some("subscription_sequence_gap")
        );
    }
    f.connected(2, Some(8), 6).unwrap();
    assert!(
        f.ingest(
            "s",
            2,
            br#"{"type":"subscribed","id":7,"msg":{"sid":12,"channel":"orderbook_delta"}}"#,
            7
        )
        .is_err()
    );
}
#[test]
fn malformed_first_present_ladder_cannot_fall_back_and_empty_is_intact() {
    let mut b = VenueBooks::new(BookMode::kalshi(), ["X"]);
    let mut m = json!({"type":"orderbook_snapshot","sid":0,"seq":1,"msg":{"market_ticker":"X","yes_dollars_fp":"bad","yes":[["0.4","1"]]}});
    apply(&mut b, Venue::Kalshi, m.clone(), 1);
    let book = b.get("X").unwrap();
    assert!(book.native_levels().0.is_empty());
    assert_eq!(book.integrity(), Integrity::Impaired);
    m["msg"]["yes_dollars_fp"] = json!([]);
    m["seq"] = json!(2);
    apply(&mut b, Venue::Kalshi, m, 2);
    assert_eq!(b.get("X").unwrap().integrity(), Integrity::Intact);
    assert!(!b.get("X").unwrap().valid_state());
}
