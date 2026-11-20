//! Streaming reads of immutable, published gzip/manifest pairs.
use crate::{Error, Result, Venue};
use flate2::read::MultiGzDecoder;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, BufRead, BufReader, Seek},
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Deserialize)]
pub struct RawFrame {
    pub schema: String,
    pub venue: Venue,
    pub recording_id: String,
    pub connection_generation: u64,
    pub local_frame_seq: u64,
    pub received_utc: String,
    pub received_monotonic_ns: u64,
    pub frame_type: String,
    pub direction: String,
    pub payload_raw: String,
}
fn required_link<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Option<String>, D::Error> {
    Option::deserialize(d)
}
#[derive(Clone, Debug, Deserialize)]
pub struct Manifest {
    pub schema: String,
    pub venue: Venue,
    pub recording_id: String,
    pub filename: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub frame_count: u64,
    pub first_frame_seq: u64,
    pub last_frame_seq: u64,
    #[serde(deserialize_with = "required_link")]
    pub previous_chunk_sha256: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}
#[derive(Clone, Debug)]
pub struct SealedChunk {
    pub path: PathBuf,
    pub index: u64,
    pub manifest: Manifest,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkGap {
    pub first_missing: u64,
    pub last_missing: u64,
    pub next_chunk: PathBuf,
    pub previous_last_frame_seq: Option<u64>,
    pub next_first_frame_seq: u64,
}
impl ChunkGap {
    pub fn missing_frame_count(&self) -> u64 {
        self.next_first_frame_seq
            .saturating_sub(self.previous_last_frame_seq.unwrap_or(0))
            .saturating_sub(1)
    }
}
#[derive(Clone, Debug)]
pub enum CaptureItem {
    Frame(RawFrame),
    Gap(ChunkGap),
}
#[derive(Debug)]
pub struct SessionChunks {
    pub chunks: Vec<SealedChunk>,
    pub incomplete: Vec<PathBuf>,
}

fn failure(path: &Path, msg: impl std::fmt::Display) -> Error {
    Error::Invalid(format!("{}: {msg}", path.display()))
}
fn chunk_digits(name: &str) -> Option<&str> {
    name.strip_prefix("chunk_")?
        .strip_suffix(".jsonl.gz")
        .filter(|digits| digits.len() >= 6 && digits.bytes().all(|c| c.is_ascii_digit()))
}
impl SealedChunk {
    pub fn open(path: &Path) -> Result<Self> {
        let name = path.file_name().and_then(|p| p.to_str()).unwrap_or("");
        let index = chunk_digits(name)
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|i| *i > 0 && name == format!("chunk_{i:06}.jsonl.gz"))
            .ok_or_else(|| failure(path, "invalid published chunk filename"))?;
        let sidecar = path.with_file_name(format!("{name}.manifest.json"));
        let manifest: Manifest = serde_json::from_reader(BufReader::new(
            File::open(&sidecar).map_err(|e| failure(&sidecar, e))?,
        ))
        .map_err(|e| failure(&sidecar, e))?;
        if manifest.schema != format!("{}.public_raw_chunk.v1", manifest.venue.as_str())
            || manifest.filename != name
        {
            return Err(failure(&sidecar, "schema or filename mismatch"));
        }
        Ok(Self {
            path: path.into(),
            index,
            manifest,
        })
    }
    pub fn frames(&self, verify: bool) -> Result<ChunkReader> {
        let mut file = File::open(&self.path).map_err(|e| failure(&self.path, e))?;
        if verify {
            let mut digest = Sha256::new();
            let bytes = io::copy(&mut file, &mut digest)?;
            if bytes != self.manifest.size_bytes {
                return Err(failure(&self.path, "size_bytes mismatch"));
            }
            if format!("{:x}", digest.finalize()) != self.manifest.sha256 {
                return Err(failure(&self.path, "sha256 mismatch"));
            }
            file.rewind()?;
        }
        Ok(ChunkReader {
            source: BufReader::new(MultiGzDecoder::new(file)),
            chunk: self.clone(),
            verify,
            done: false,
            count: 0,
            first: None,
            last: None,
            line: String::new(),
        })
    }
}
pub fn list_session_chunks(root: impl AsRef<Path>) -> Result<SessionChunks> {
    let root = root.as_ref();
    let mut files = BTreeSet::new();
    for entry in
        fs::read_dir(root).map_err(|e| failure(root, format!("cannot list session: {e}")))?
    {
        let entry = entry?;
        if entry.path().is_file() {
            files.insert(entry.file_name().to_string_lossy().into_owned());
        }
    }
    let mut chunks = vec![];
    let mut incomplete = vec![];
    for name in &files {
        if chunk_digits(name).is_some() {
            if files.contains(&format!("{name}.manifest.json")) {
                chunks.push(SealedChunk::open(&root.join(name))?);
            } else {
                incomplete.push(root.join(name));
            }
        } else if name.ends_with(".tmp")
            || name.ends_with(".part")
            || name
                .strip_suffix(".manifest.json")
                .is_some_and(|base| chunk_digits(base).is_some() && !files.contains(base))
        {
            incomplete.push(root.join(name));
        }
    }
    chunks.sort_by_key(|chunk| chunk.index);
    Ok(SessionChunks { chunks, incomplete })
}
pub struct ChunkReader {
    source: BufReader<MultiGzDecoder<File>>,
    chunk: SealedChunk,
    verify: bool,
    done: bool,
    count: u64,
    first: Option<u64>,
    last: Option<u64>,
    line: String,
}
impl ChunkReader {
    fn next_frame(&mut self) -> Result<Option<RawFrame>> {
        self.line.clear();
        let path = &self.chunk.path;
        let m = &self.chunk.manifest;
        if self
            .source
            .read_line(&mut self.line)
            .map_err(|e| failure(path, e))?
            == 0
        {
            if self.verify
                && (self.count != m.frame_count
                    || self.first != Some(m.first_frame_seq)
                    || self.last != Some(m.last_frame_seq))
            {
                return Err(failure(
                    path,
                    "frame_count mismatch or frame sequence endpoint mismatch",
                ));
            }
            return Ok(None);
        }
        self.count += 1;
        let frame: RawFrame = serde_json::from_str(&self.line)
            .map_err(|e| failure(path, format!("line {}: {e}", self.count)))?;
        if self.verify {
            if frame.venue != m.venue
                || frame.recording_id != m.recording_id
                || frame.schema != format!("{}.public_raw_frame.v1", m.venue.as_str())
            {
                return Err(failure(
                    path,
                    format!("line {}: frame identity mismatch", self.count),
                ));
            }
            if self
                .last
                .is_some_and(|s| s.checked_add(1) != Some(frame.local_frame_seq))
            {
                return Err(failure(path, "local_frame_seq is not consecutive"));
            }
        }
        self.first.get_or_insert(frame.local_frame_seq);
        self.last = Some(frame.local_frame_seq);
        Ok(Some(frame))
    }
}
impl Iterator for ChunkReader {
    type Item = Result<RawFrame>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let result = self.next_frame();
        self.done = !matches!(result, Ok(Some(_)));
        result.transpose()
    }
}
fn link(
    previous: Option<&SealedChunk>,
    chunk: &SealedChunk,
    verify: bool,
) -> Result<Option<ChunkGap>> {
    let expected_index = previous
        .map_or(0, |p| p.index)
        .checked_add(1)
        .ok_or_else(|| failure(&chunk.path, "chunk index overflow"))?;
    let missing = chunk
        .index
        .checked_sub(expected_index)
        .ok_or_else(|| failure(&chunk.path, "chunks out of numeric order"))?;
    let m = &chunk.manifest;
    let last = previous.map(|p| p.manifest.last_frame_seq);
    if verify {
        if previous.is_some_and(|p| {
            p.manifest.venue != m.venue || p.manifest.recording_id != m.recording_id
        }) {
            return Err(failure(&chunk.path, "session identity mismatch"));
        }
        let expected = last
            .unwrap_or(0)
            .checked_add(1)
            .and_then(|s| s.checked_add(missing))
            .ok_or_else(|| failure(&chunk.path, "frame sequence overflow"))?;
        if missing > 0 {
            if m.previous_chunk_sha256.is_none() || m.first_frame_seq < expected {
                return Err(failure(
                    &chunk.path,
                    "invalid predecessor or sequence after missing chunks",
                ));
            }
        } else if m.previous_chunk_sha256.as_deref() != previous.map(|p| p.manifest.sha256.as_str())
            || m.first_frame_seq != expected
        {
            return Err(failure(
                &chunk.path,
                "adjacent chunk hash or sequence mismatch",
            ));
        }
    }
    Ok((missing > 0).then(|| ChunkGap {
        first_missing: expected_index,
        last_missing: chunk.index - 1,
        next_chunk: chunk.path.clone(),
        previous_last_frame_seq: last,
        next_first_frame_seq: m.first_frame_seq,
    }))
}
/// Exhaust the iterator for the last chunk's count and endpoint checks.
pub struct SessionReader {
    chunks: std::vec::IntoIter<SealedChunk>,
    previous: Option<SealedChunk>,
    pending: Option<SealedChunk>,
    frames: Option<ChunkReader>,
    verify: bool,
    done: bool,
}
impl SessionChunks {
    pub fn read(self, verify: bool) -> SessionReader {
        SessionReader {
            chunks: self.chunks.into_iter(),
            previous: None,
            pending: None,
            frames: None,
            verify,
            done: false,
        }
    }
}
pub fn read_session(path: impl AsRef<Path>, verify: bool) -> Result<SessionReader> {
    Ok(list_session_chunks(path)?.read(verify))
}
impl SessionReader {
    fn step(&mut self) -> Result<Option<CaptureItem>> {
        loop {
            if let Some(frames) = &mut self.frames
                && let Some(item) = frames.next()
            {
                return item.map(|f| Some(CaptureItem::Frame(f)));
            }
            self.frames = None;
            let chunk = if let Some(p) = self.pending.take() {
                p
            } else {
                let Some(chunk) = self.chunks.next() else {
                    return Ok(None);
                };
                if let Some(gap) = link(self.previous.as_ref(), &chunk, self.verify)? {
                    self.pending = Some(chunk);
                    return Ok(Some(CaptureItem::Gap(gap)));
                }
                chunk
            };
            self.frames = Some(chunk.frames(self.verify)?);
            self.previous = Some(chunk);
        }
    }
}
impl Iterator for SessionReader {
    type Item = Result<CaptureItem>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let result = self.step();
        self.done = !matches!(result, Ok(Some(_)));
        result.transpose()
    }
}

#[derive(Default, Debug)]
pub struct Inspection {
    pub incomplete_files: Vec<PathBuf>,
    pub chunk_gaps: Vec<ChunkGap>,
    pub sealed_chunk_count: usize,
    pub compressed_bytes: u64,
    pub frame_count: u64,
    pub venue: Option<Venue>,
    pub recording_id: Option<String>,
    pub first_received_utc: Option<String>,
    pub last_received_utc: Option<String>,
    pub connection_generations: BTreeSet<u64>,
    pub frame_type_counts: BTreeMap<String, u64>,
    pub direction_counts: BTreeMap<String, u64>,
    pub local_event_counts: BTreeMap<String, u64>,
}
pub fn inspect_session(path: impl AsRef<Path>) -> Result<Inspection> {
    let discovery = list_session_chunks(path)?;
    let mut result = Inspection {
        incomplete_files: discovery.incomplete.clone(),
        sealed_chunk_count: discovery.chunks.len(),
        compressed_bytes: discovery
            .chunks
            .iter()
            .try_fold(0u64, |total, c| total.checked_add(c.manifest.size_bytes))
            .ok_or_else(|| Error::Invalid("session byte count overflow".into()))?,
        ..Inspection::default()
    };
    for item in discovery.read(true) {
        match item? {
            CaptureItem::Gap(g) => result.chunk_gaps.push(g),
            CaptureItem::Frame(f) => {
                if result.frame_count == 0 {
                    result.first_received_utc = Some(f.received_utc.clone());
                    result.recording_id = Some(f.recording_id);
                    result.venue = Some(f.venue);
                }
                result.frame_count += 1;
                result.last_received_utc = Some(f.received_utc);
                result
                    .connection_generations
                    .insert(f.connection_generation);
                if f.direction == "local" {
                    *result
                        .local_event_counts
                        .entry(f.frame_type.clone())
                        .or_default() += 1;
                }
                *result.frame_type_counts.entry(f.frame_type).or_default() += 1;
                *result.direction_counts.entry(f.direction).or_default() += 1;
            }
        }
    }
    Ok(result)
}
