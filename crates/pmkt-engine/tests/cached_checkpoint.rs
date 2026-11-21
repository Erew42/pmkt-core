mod capture_fixture;

use arrow_array::{BooleanArray, Float64Array, StringArray};
use capture_fixture::{column, raw, root, seal};
use chrono::{DateTime, SecondsFormat};
use pmkt_engine::{
    Venue,
    emit::{Version, reconstruct_merged},
};
use serde_json::{Value, json};

const MS: i64 = 1_735_689_600_010;
const ANCHOR: &str = "da70f6a2795e39c7d5aef94fd5d8841cf1915947";
const NEXT: &str = "75928a5a0e694188c66b5e86afb8fed66ef457c4";

#[derive(Clone, Copy)]
enum Witness {
    Continuous,
    Interrupted {
        marker: &'static str,
        before_b: bool,
    },
    RepeatedHash,
}

struct Observations {
    // Intact venue envelopes at the tied timestamp, with all recording coordinates.
    messages: Vec<(Value, Vec<String>)>,
    // Target token's causal reference, validity, best ask and size at .478.
    quotes: Vec<(String, bool, f64, Option<f64>)>,
}

fn append(
    frames: &mut Vec<Value>,
    copy: &str,
    us: i64,
    generation: u64,
    kind: &str,
    direction: &str,
    msg: &Value,
) {
    let mut f = raw(
        frames.len() as u64 + 1,
        generation,
        "polymarket",
        msg.clone(),
    );
    f["recording_id"] = json!(copy);
    f["received_utc"] = json!(
        DateTime::from_timestamp_micros(us)
            .unwrap()
            .to_rfc3339_opts(SecondsFormat::Micros, false)
    );
    f["frame_type"] = json!(kind);
    f["direction"] = json!(direction);
    frames.push(f);
}

fn book(stamp: i64, hash: &str, bid: (&str, &str), asks: &[(&str, &str)]) -> Value {
    let asks: Vec<_> = asks
        .iter()
        .map(|(p, q)| json!({"price":p,"size":q}))
        .collect();
    json!({"event_type":"book","asset_id":"X","market":"M","timestamp":stamp.to_string(),"hash":hash,
           "bids":[{"price":bid.0,"size":bid.1}],"asks":asks})
}

fn leg(asset: &str, side: &str, price: &str, size: &str, hash: &str) -> Value {
    json!({"asset_id":asset,"side":side,"price":price,"size":size,"hash":hash})
}

fn change(stamp: i64, legs: Value) -> Value {
    json!({"event_type":"price_change","market":"M","timestamp":stamp.to_string(),"price_changes":legs})
}

fn replay(witness: Witness) -> Observations {
    // In vm3b.pm00 the reconnect snapshot matched A (the .502 ask) even though
    // both recorders had already received B (the .478 ask) in the same millisecond.
    let bid = ("0.032", "39.19");
    let initial = book(MS - 10, "initial", bid, &[("0.06", "100")]);
    let cached = book(MS, ANCHOR, bid, &[("0.06", "100"), ("0.502", "20")]);
    let a = change(
        MS,
        json!([
            leg("X", "SELL", "0.502", "20", ANCHOR),
            leg("Y", "BUY", "0.498", "20", "YA")
        ]),
    );
    let b = change(
        MS,
        json!([
            leg("Y", "BUY", "0.522", "634.95", "YB"),
            leg("X", "SELL", "0.478", "634.95", NEXT)
        ]),
    );
    // Removing the old best exposes a stale level immediately if B was erased.
    let c = change(MS + 90, json!([leg("X", "SELL", "0.06", "0", "C")]));
    let unrelated = json!({"event_type":"best_bid_ask","market":"other","asset_id":"Z","best_bid":"0.2","best_ask":"0.3","timestamp":(MS+1).to_string()});
    let sub = json!({"assets_ids":["X","Y"]});
    let mut sessions = vec![];
    for (copy, skew) in [("continuous", 0), ("reconnect", 1)] {
        let mut frames = vec![];
        let mut push = |us: i64, generation, kind: &str, direction: &str, msg: &Value| {
            append(
                &mut frames,
                copy,
                (MS - 10) * 1000 + us + skew,
                generation,
                kind,
                direction,
                msg,
            );
        };
        push(100, 1, "subscription", "sent", &sub);
        push(1_000, 1, "text", "received", &initial);
        if matches!(witness, Witness::RepeatedHash) {
            let other = change(MS, json!([leg("X", "SELL", "0.750", "1", ANCHOR)]));
            push(10_000, 1, "text", "received", &other);
        }
        push(11_000, 1, "text", "received", &a);
        let marker = |before_b| match witness {
            Witness::Interrupted {
                marker,
                before_b: at,
            } if before_b == at => (
                marker,
                if marker == "ws_close" {
                    "received_control_base64"
                } else {
                    "local"
                },
            ),
            _ => ("text", "received"),
        };
        if copy == "continuous" {
            let (kind, dir) = marker(true);
            push(12_000, 1, kind, dir, &unrelated);
        }
        push(13_000, 1, "text", "received", &b);
        if copy == "continuous" {
            let (kind, dir) = marker(false);
            push(20_000, 1, kind, dir, &unrelated);
            // The witness can resume within the same generation after a local loss.
            push(30_000, 1, "ws_pong", "received_control_base64", &json!(""));
            push(110_000, 1, "text", "received", &c);
        } else {
            push(40_000, 1, "transport_error", "local", &json!({}));
            push(50_000, 2, "subscription", "sent", &sub);
            push(60_000, 2, "text", "received", &json!([unrelated, cached]));
            push(110_000, 2, "text", "received", &c);
        }
        let path = root();
        seal(&path, 1, &frames, None);
        sessions.push((copy.to_owned(), "pm00".to_owned(), path));
    }
    let mut result = Observations {
        messages: vec![],
        quotes: vec![],
    };
    reconstruct_merged(
        Venue::Polymarket,
        sessions,
        Version::V2,
        1,
        |batch| {
            let t = &batch.topbook;
            let d = &batch.depth;
            for i in 0..t.num_rows() {
                if column::<StringArray>(t, "instrument_id").value(i) != "X" {
                    continue;
                }
                let level = (0..d.num_rows()).find(|&j| {
                    column::<StringArray>(d, "instrument_id").value(j) == "X"
                        && column::<Float64Array>(d, "price_dollars").value(j) == 0.478
                });
                result.quotes.push((
                    column::<StringArray>(t, "raw_event_ref").value(i).into(),
                    column::<BooleanArray>(t, "valid_state").value(i),
                    column::<Float64Array>(t, "best_ask_dollars").value(i),
                    level.map(|j| column::<Float64Array>(d, "size_contracts").value(j)),
                ));
            }
            Ok(())
        },
        |m| {
            if m.scope == "M" && m.venue_ts_ms == Some(MS) {
                let origins = m
                    .occurrences
                    .iter()
                    .map(|o| {
                        let mut text = format!("{}:{}", o.recording_id, o.frame_seq);
                        if let Some(i) = o.position {
                            text.push_str(&format!(":{i}"))
                        }
                        text
                    })
                    .collect();
                result.messages.push((m.message.clone(), origins));
            }
            Ok(())
        },
    )
    .unwrap();
    result
}

#[test]
fn cached_checkpoint_precedes_the_next_proven_change_to_its_token() {
    let r = replay(Witness::Continuous);
    assert_eq!(
        r.quotes,
        [
            ("continuous:2".into(), true, 0.06, None),
            ("continuous:3".into(), true, 0.06, None),
            ("reconnect:7:1".into(), true, 0.06, None),
            ("continuous:5".into(), true, 0.06, Some(634.95)),
            ("continuous:8".into(), true, 0.478, Some(634.95)),
        ]
    );
    let [(a, oa), (s, os), (b, ob)] = r.messages.as_slice() else {
        panic!("expected both atomic changes and their checkpoint")
    };
    assert_eq!(oa, &["continuous:3", "reconnect:3"]);
    assert_eq!(os, &["reconnect:7:1"]);
    assert_eq!(ob, &["continuous:5", "reconnect:4"]);
    let leg_hashes = |m: &Value| {
        m["price_changes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|leg| leg["hash"].clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(leg_hashes(a), [ANCHOR, "YA"]);
    assert_eq!(s["hash"], ANCHOR);
    assert_eq!(leg_hashes(b), ["YB", NEXT]);
}

#[test]
fn a_cached_hash_needs_an_uninterrupted_witness_and_an_unambiguous_anchor() {
    for marker in ["ws_close", "disk_pause_gap"] {
        for before_b in [true, false] {
            let r = replay(Witness::Interrupted { marker, before_b });
            assert_eq!(r.messages.len(), 3);
            assert_eq!(
                r.messages.last().unwrap().0["event_type"],
                "book",
                "{marker}, before B = {before_b}"
            );
        }
    }
    let r = replay(Witness::RepeatedHash);
    assert_eq!(r.messages.len(), 4);
    assert_eq!(r.messages.last().unwrap().0["event_type"], "book");
}

// On vm1b a cached snapshot arrived eleven minutes after both recorders saw a
// newer absolute change to .994. The snapshot is one venue millisecond older.
const LATEST: i64 = 1_793_842_996_347;
const OLD_HASH: &str = "0ed489d359e7d2aad0e2314d6e9f16882bbd6e00";
const NEW_HASH: &str = "973cfa188be276b8d0b5e05d113fe8e6a0772fd8";

fn late_checkpoint(witness_gap: bool, cached_ms: i64) -> (f64, String, String) {
    let bid = ("0.047", "20");
    let asks = [("0.11", "32.9"), ("0.994", "833.33")];
    let initial = book(LATEST - 1_010, "initial", bid, &asks);
    let newer = change(
        LATEST,
        json!([leg("X", "SELL", "0.994", "839.26", NEW_HASH)]),
    );
    let cached = json!([book(cached_ms, OLD_HASH, bid, &asks)]);
    let expose = change(
        LATEST + 689_990,
        json!([leg("X", "SELL", "0.11", "0", "after")]),
    );
    let sub = json!({"assets_ids":["X"]});
    let mut sessions = vec![];
    for (copy, skew) in [("vm1b", 0), ("vm3b", 40)] {
        let mut frames = vec![];
        let mut push = |ms: i64, gen_no, kind: &str, dir: &str, msg: &Value| {
            append(
                &mut frames,
                copy,
                (LATEST + ms) * 1000 + skew,
                gen_no,
                kind,
                dir,
                msg,
            );
        };
        push(-2_000, 1, "subscription", "sent", &sub);
        push(-1_000, 1, "text", "received", &initial);
        push(10, 1, "text", "received", &newer);
        if copy == "vm1b" {
            // A tick updates the book's exchange time but supplies no new depth.
            let tick = json!({"event_type":"tick_size_change","asset_id":"X","market":"M","timestamp":(LATEST + 100).to_string(),"new_tick_size":"0.001"});
            push(110, 1, "text", "received", &tick);
            push(1_000, 1, "transport_error", "local", &json!({}));
            push(680_000, 2, "subscription", "sent", &sub);
            push(686_900, 2, "text", "received", &cached);
        } else if witness_gap {
            push(200, 1, "disk_pause_gap", "local", &json!({}));
            push(300, 1, "ws_pong", "received_control_base64", &json!(""));
        }
        let generation = if copy == "vm1b" { 2 } else { 1 };
        push(690_000, generation, "text", "received", &expose);
        let path = root();
        seal(&path, 1, &frames, None);
        sessions.push((copy.to_owned(), "pm00".to_owned(), path));
    }
    let mut seen = false;
    let mut snapshot = None;
    let mut exposed = None;
    reconstruct_merged(
        Venue::Polymarket,
        sessions,
        Version::V2,
        1,
        |batch| {
            let t = &batch.topbook;
            let d = &batch.depth;
            let h = column::<StringArray>(t, "book_hash").value(0);
            if column::<StringArray>(t, "raw_event_ref").value(0) == "vm1b:7:0" {
                assert!(column::<BooleanArray>(t, "valid_state").value(0));
                let j = (0..d.num_rows())
                    .find(|&j| column::<Float64Array>(d, "price_dollars").value(j) == 0.994)
                    .unwrap();
                snapshot = Some((
                    column::<Float64Array>(d, "size_contracts").value(j),
                    h.to_owned(),
                    column::<StringArray>(t, "exchange_ts_utc")
                        .value(0)
                        .to_owned(),
                ));
            } else if h == "after" {
                assert!(column::<BooleanArray>(t, "valid_state").value(0));
                assert_eq!(
                    column::<Float64Array>(t, "best_ask_dollars").value(0),
                    0.994
                );
                exposed = Some(column::<Float64Array>(t, "ask_size_contracts").value(0));
            }
            Ok(())
        },
        |m| {
            if m.message["hash"] == OLD_HASH {
                assert!(m.late && m.reconnect_snapshot);
                assert_eq!(m.origin().received_us, (LATEST + 686_900) * 1000);
                assert_eq!(m.message["asks"][1]["size"], "833.33");
                assert_eq!(m.venue_ts_ms, Some(cached_ms));
                seen = true;
            }
            Ok(())
        },
    )
    .unwrap();
    assert!(seen);
    let snapshot = snapshot.unwrap();
    assert_eq!(exposed, Some(snapshot.0));
    snapshot
}

#[test]
fn late_reconnect_snapshot_observes_the_newer_continuously_covered_book() {
    let (size, hash, exchange_ts) = late_checkpoint(false, LATEST - 1);
    assert_eq!(size, 839.26);
    assert_eq!(hash, NEW_HASH);
    assert_eq!(exchange_ts, "2026-11-05T01:43:16.447000+00:00"); // intervening tick
}

#[test]
fn late_reconnect_snapshot_applies_after_a_witness_gap_or_a_newer_tick_alone() {
    for (gap, stamp) in [(true, LATEST - 1), (false, LATEST + 50)] {
        let (size, hash, _) = late_checkpoint(gap, stamp);
        assert_eq!(size, 833.33);
        assert_eq!(hash, OLD_HASH);
    }
}
