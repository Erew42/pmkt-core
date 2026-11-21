use arrow_array::RecordBatch;
use flate2::{Compression, write::GzEncoder};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

pub fn root() -> PathBuf {
    // Each case owns fresh files under the ignored workspace build tree.
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/capture-fixtures");
    fs::create_dir_all(&base).unwrap();
    tempfile::tempdir_in(base).unwrap().keep()
}
pub fn raw(seq: u64, generation: u64, venue: &str, msg: Value) -> Value {
    json!({"schema":format!("{venue}.public_raw_frame.v1"),"venue":venue,"recording_id":"run","connection_generation":generation,
        "local_frame_seq":seq,"received_utc":"2025-01-01T00:00:00+00:00","received_monotonic_ns":seq*1_000_000,
        "frame_type":"text","direction":"received","payload_raw":msg.to_string()})
}
pub fn name(i: u64) -> String {
    format!("chunk_{i:06}.jsonl.gz")
}
pub fn manifest(root: &Path, i: u64) -> PathBuf {
    root.join(format!("{}.manifest.json", name(i)))
}
pub fn sidecar(
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
pub fn seal(root: &Path, i: u64, frames: &[Value], prev: Option<&str>) -> String {
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
pub fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap()
}
