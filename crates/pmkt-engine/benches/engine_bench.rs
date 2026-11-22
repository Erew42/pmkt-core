use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use pmkt_engine::{
    BookMode, VenueBooks,
    emit::{QuoteBatchBuilder, Receive, Version},
    wire,
};
use std::hint::black_box;

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_update_emit");
    group.throughput(Throughput::Elements(1));
    let poly_snapshot=br#"{"event_type":"book","market":"M","asset_id":"X","timestamp":"1710000000000","bids":[["0.4","12.5"]],"asks":[["0.6","3"]]}"#;
    let poly_delta=br#"{"event_type":"price_change","market":"M","timestamp":"1710000000001","price_changes":[{"asset_id":"X","side":"BUY","price":"0.42","size":"8.50"}]}"#;
    let kalshi_snapshot=br#"{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"X","yes_dollars_fp":[["0.4","12.5"]],"no_dollars_fp":[["0.6","3"]]}}"#;
    let kalshi_delta=br#"{"type":"orderbook_delta","sid":1,"seq":2,"msg":{"market_ticker":"X","ts_ms":1710000000001,"side":"yes","price_dollars":"0.42","delta_fp":"8.50"}}"#;
    for (mode, snapshot, delta) in [
        (
            BookMode::Polymarket,
            poly_snapshot.as_slice(),
            poly_delta.as_slice(),
        ),
        (
            BookMode::kalshi(),
            kalshi_snapshot.as_slice(),
            kalshi_delta.as_slice(),
        ),
    ] {
        let venue = mode.venue();
        let seed = wire::messages(snapshot).unwrap().pop().unwrap().value;
        group.bench_function(venue.as_str(), |b| {
            b.iter_batched(
                || {
                    let mut books = VenueBooks::new(mode, ["X"]);
                    books.apply(&seed, 1).unwrap();
                    books
                },
                |mut books| {
                    let mut batch = QuoteBatchBuilder::new(Version::V2);
                    let receive = Receive {
                        run_id: Some("bench"),
                        source: Some("ws"),
                        utc: "2024-03-09T16:00:00.001000+00:00",
                        monotonic_ns: Some(1_000_000),
                        local_sequence: Some(2),
                        raw_event_ref: Some("bench/1/2"),
                        quote_age_ms: Some(0),
                    };
                    for message in wire::messages(black_box(delta)).unwrap() {
                        for update in books.apply(&message.value, 2).unwrap() {
                            batch
                                .push(books.get(&update.instrument).unwrap(), receive, true)
                                .unwrap();
                        }
                    }
                    black_box(batch.finish().unwrap())
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}
criterion_group!(benches, bench);
criterion_main!(benches);
