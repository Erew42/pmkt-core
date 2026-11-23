//! One Criterion element is one input frame. Fixture preparation is outside the timer.
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use pmkt_engine::{
    BookMode, ShardFeed, Venue, VenueBooks,
    emit::{QuoteBatchBuilder, Receive, Version},
    feed::FeedTimeouts,
    wire,
};
use serde_json::{Value, json};
use std::hint::black_box;

#[path = "inputs/capture.rs"]
mod capture;

fn snapshot(venue: Venue, levels: u32, instrument: &str) -> Value {
    let ladder = |first: u32, step: i32| -> Vec<_> {
        (0..levels)
            .map(|i| {
                let price = first as i32 + i as i32 * step;
                [format!("0.{price:03}"), (i + 1).to_string()]
            })
            .collect()
    };
    let bids = ladder(400, -1);
    match venue {
        Venue::Polymarket => json!({
            "event_type":"book","market":"M","asset_id":instrument,"timestamp":"1710000000000",
            "bids":bids, "asks":ladder(600, 1), "hash":"snapshot"
        }),
        // With use_yes_price, NO levels are expressed on the YES price scale.
        Venue::Kalshi => json!({"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{
            "market_ticker":instrument,"yes_dollars_fp":bids,"no_dollars_fp":ladder(600, 1)
        }}),
    }
}

fn changes(prices: impl IntoIterator<Item = String>, size: &str) -> Value {
    let changes: Vec<_> = prices
        .into_iter()
        .map(|price| {
            json!({
                "asset_id":"X","side":"BUY","price":price,"size":size
            })
        })
        .collect();
    json!({"event_type":"price_change","market":"M","timestamp":"1710000000001","price_changes":changes})
}

fn encode(v: &Value) -> Vec<u8> {
    serde_json::to_vec(v).unwrap()
}

fn decode_update_emit(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_update_emit");
    group.throughput(Throughput::Elements(1));
    let receive = Receive {
        run_id: Some("bench"),
        source: Some("ws"),
        utc: "2024-03-09T16:00:00.001000+00:00",
        monotonic_ns: Some(1_000_000),
        local_sequence: Some(2),
        raw_event_ref: Some("bench/1/2"),
        quote_age_ms: Some(0),
    };
    for mode in [BookMode::Polymarket, BookMode::kalshi()] {
        let venue = mode.venue();
        for levels in [1, 50, 200] {
            let seed = snapshot(venue, levels, "X");
            let delta = match venue {
                Venue::Polymarket => changes(["0.400".into()], "8.5"),
                Venue::Kalshi => json!({"type":"orderbook_delta","sid":1,"seq":2,"msg":{
                    "market_ticker":"X","ts_ms":1710000000001_i64,"side":"yes","price_dollars":"0.400","delta_fp":"8.5"
                }}),
            };
            let delta_name = if levels == 1 {
                venue.as_str().to_owned()
            } else {
                format!("{}_delta_{levels}", venue.as_str())
            };
            let mut cases = vec![(delta_name, encode(&delta), true)];
            if levels > 1 {
                cases.push((
                    format!("{}_snapshot_{levels}", venue.as_str()),
                    encode(&seed),
                    false,
                ));
                if venue == Venue::Polymarket {
                    let batch = changes((0..16).map(|i| format!("0.{:03}", 400 - i)), "8.5");
                    cases.push((
                        format!("polymarket_16_changes_{levels}"),
                        encode(&batch),
                        true,
                    ));
                }
            }
            for (name, payload, seeded) in cases {
                group.bench_function(name, |b| {
                    b.iter_batched_ref(
                        || {
                            let mut books = VenueBooks::new(mode, ["X"]);
                            if seeded {
                                books.apply(&seed, 1).unwrap();
                            }
                            books
                        },
                        |books| {
                            let mut batch = QuoteBatchBuilder::new(Version::V2);
                            for message in wire::messages(black_box(&payload)).unwrap() {
                                for update in books.apply(&message.value, 2).unwrap() {
                                    batch
                                        .push(books.get(&update.instrument).unwrap(), receive, true)
                                        .unwrap();
                                }
                            }
                            black_box(batch.finish().unwrap())
                        },
                        BatchSize::LargeInput,
                    )
                });
            }
        }
    }
    group.finish();
}

fn poly_feed(instruments: usize) -> ShardFeed {
    let ids: Vec<_> = std::iter::once("X".to_owned())
        .chain((1..instruments).map(|i| format!("quiet-{i}")))
        .collect();
    let mut feed =
        ShardFeed::new(Venue::Polymarket, "bench", &ids, FeedTimeouts::default()).unwrap();
    feed.connected(1, None, 0).unwrap();
    for id in ids {
        feed.ingest_updates("bench", 1, &encode(&snapshot(Venue::Polymarket, 1, &id)), 1)
            .unwrap();
    }
    feed
}

fn shard_feed(c: &mut Criterion) {
    let mut group = c.benchmark_group("shard_feed");
    group.throughput(Throughput::Elements(1));
    let delta = ["2", "3"].map(|size| encode(&changes(["0.400".into()], size)));
    for instruments in [1, 256, 512] {
        let mut feed = poly_feed(instruments);
        let mut now = 1;
        group.bench_function(BenchmarkId::new("ingest_read", instruments), |b| {
            b.iter(|| {
                now += 1;
                let ids = feed
                    .ingest_updates("bench", 1, black_box(&delta[now as usize % 2]), now)
                    .unwrap();
                for id in ids {
                    black_box(
                        feed.read_book(&id, now)
                            .unwrap()
                            .unwrap()
                            .bids()
                            .next()
                            .unwrap(),
                    );
                }
            })
        });
    }
    let mut feed = poly_feed(1);
    let mut now = 1;
    group.bench_function("ingest_detached", |b| {
        b.iter(|| {
            now += 1;
            black_box(
                feed.ingest("bench", 1, &delta[now as usize % 2], now)
                    .unwrap(),
            )
        })
    });
    group.finish();

    let mut group = c.benchmark_group("recovery");
    group.throughput(Throughput::Elements(2));
    let crossed = changes(["0.600".into()], "2");
    let correction = changes(["0.600".into()], "0");
    let mut mismatch = changes(["0.400".into()], "2");
    mismatch["price_changes"][0]["previous_hash"] = json!("unexpected");
    for (id, bad, good, reason) in [
        (
            "crossed_corrective_delta",
            crossed,
            correction,
            "crossed_book",
        ),
        (
            "hash_mismatch_snapshot",
            mismatch,
            snapshot(Venue::Polymarket, 1, "X"),
            "book_integrity_failed",
        ),
    ] {
        let (bad, good) = (encode(&bad), encode(&good));
        let mut probe = poly_feed(1);
        probe.ingest_updates("bench", 1, &bad, 2).unwrap();
        assert_eq!(
            probe.health("X", 2).unwrap().reason.as_deref(),
            Some(reason)
        );
        probe.ingest_updates("bench", 1, &good, 3).unwrap();
        assert!(probe.health("X", 3).unwrap().usable);
        group.bench_function(id, |b| {
            b.iter_batched_ref(
                || poly_feed(1),
                |feed| {
                    black_box(feed.ingest_updates("bench", 1, &bad, 2).unwrap());
                    black_box(feed.ingest_updates("bench", 1, &good, 3).unwrap())
                },
                BatchSize::LargeInput,
            )
        });
    }
    group.finish();
}

criterion_group!(benches, decode_update_emit, shard_feed, capture::benchmark);
criterion_main!(benches);
