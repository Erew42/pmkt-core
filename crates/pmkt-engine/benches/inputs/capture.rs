//! Both timed paths stream the supplied session. The fixture samples lack publisher
//! sidecars, so wrap their original bytes in temporary, verified chunk pairs once.
use criterion::{BenchmarkId, Criterion, Throughput};
use flate2::read::MultiGzDecoder;
use pmkt_engine::{
    Result, Venue,
    capture::{CaptureItem, RawFrame, SealedChunk, list_session_chunks, read_session},
    emit::{Version, reconstruct_session},
    merge::{CopyMerge, MergeOptions, SessionOccurrences, Stream},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    env, fs,
    hint::black_box,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

fn wrap_fixture(root: &Path, venue: Venue, bytes: &[u8]) -> PathBuf {
    let dir = root.join(venue.as_str());
    fs::create_dir(&dir).unwrap();
    let frames: Vec<RawFrame> = BufReader::new(MultiGzDecoder::new(bytes))
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect();
    let filename = "chunk_000001.jsonl.gz";
    let manifest = json!({
        "schema": format!("{}.public_raw_chunk.v1", venue.as_str()), "venue": venue.as_str(),
        "recording_id": frames[0].recording_id, "filename":filename, "size_bytes":bytes.len(),
        "sha256":format!("{:x}", Sha256::digest(bytes)), "frame_count":frames.len(),
        "first_frame_seq":frames[0].local_frame_seq, "last_frame_seq":frames.last().unwrap().local_frame_seq,
        "previous_chunk_sha256":null
    });
    fs::write(dir.join(filename), bytes).unwrap();
    fs::write(
        dir.join(format!("{filename}.manifest.json")),
        manifest.to_string(),
    )
    .unwrap();
    dir
}

fn capture_metadata(path: &Path) -> (Venue, u64) {
    let chunks = if path.is_file() {
        vec![SealedChunk::open(path).unwrap()]
    } else {
        let session = list_session_chunks(path).unwrap();
        assert!(
            session.incomplete.is_empty(),
            "session contains incomplete or unsealed chunks"
        );
        session.chunks
    };
    assert!(
        !chunks.is_empty(),
        "no sealed capture chunks in {}",
        path.display()
    );
    let venue = chunks[0].manifest.venue;
    let count = chunks.iter().map(|c| c.manifest.frame_count).sum();
    assert!(count > 0, "empty capture");
    (venue, count)
}

fn frames(path: &Path) -> Box<dyn Iterator<Item = Result<CaptureItem>>> {
    if path.is_file() {
        let iter = SealedChunk::open(path).unwrap().frames(true).unwrap();
        Box::new(iter.map(|f| f.map(CaptureItem::Frame)))
    } else {
        Box::new(read_session(path, true).unwrap())
    }
}

fn capture(c: &mut Criterion, path: &Path, fixture: bool) {
    let (venue, nframes) = capture_metadata(path);
    let mut group = c.benchmark_group("capture");
    // Includes chunk verification, decompression, wire decode, VenueBooks updates,
    // and materialization of the topbook.v2 and depth.v2 Arrow batches.
    group.throughput(Throughput::Elements(nframes));
    group.bench_function(
        BenchmarkId::new("reconstruct_session", venue.as_str()),
        |b| {
            b.iter(|| {
                reconstruct_session(path, Version::V2, 4096, |batch| {
                    black_box(batch);
                    Ok(())
                })
                .unwrap()
            })
        },
    );
    // Two independent verified readers of the same capture. The primary loses
    // every seventh text frame; the backup supplies its messages 5.1 ms later.
    group.throughput(Throughput::Elements(nframes * 2));
    group.bench_function(
        BenchmarkId::new("copy_merge_gap_fill", venue.as_str()),
        |b| {
            b.iter(|| {
                let streams = ["primary", "backup"].map(|copy| {
                    SessionOccurrences::new(Stream::new(copy, "bench"), "bench", frames(path))
                        .filter_map(move |result| match result {
                            Ok(o) if copy == "primary" && o.origin.frame_seq % 7 == 0 => None,
                            Ok(mut o) if copy == "backup" => {
                                o.origin.received_us += 5100;
                                Some(Ok(o))
                            }
                            other => Some(other),
                        })
                });
                let mut backup_only = 0;
                let stats = CopyMerge::new(venue, MergeOptions::default())
                    .unwrap()
                    .merge(streams, |message| {
                        if message.copies.len() == 1 && message.copies.contains("backup") {
                            backup_only += 1;
                        }
                        black_box(message);
                        Ok(())
                    })
                    .unwrap();
                if fixture {
                    assert!(stats.repeats_merged > 0 && backup_only > 0);
                    assert_eq!(stats.messages, 202);
                }
                black_box(stats)
            })
        },
    );
    group.finish();
}

pub fn benchmark(c: &mut Criterion) {
    if let Some(path) = env::var_os("PMKT_BENCH_DATA_DIR") {
        capture(c, Path::new(&path), false);
    } else {
        let tmp = tempfile::tempdir().unwrap();
        for (venue, bytes) in [
            (
                Venue::Polymarket,
                include_bytes!("../../../../tests/fixtures/raw_capture/polymarket_00.jsonl.gz")
                    .as_slice(),
            ),
            (
                Venue::Kalshi,
                include_bytes!("../../../../tests/fixtures/raw_capture/kalshi_00.jsonl.gz")
                    .as_slice(),
            ),
        ] {
            let path = wrap_fixture(tmp.path(), venue, bytes);
            capture(c, &path, true);
        }
    }
}
