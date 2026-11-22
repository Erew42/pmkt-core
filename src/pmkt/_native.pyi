from collections.abc import Sequence
from os import PathLike
from pathlib import Path
from typing import Literal, NoReturn, TypedDict, final, overload

@final
class ArrowStream:
    """A one-shot Arrow C stream, consumable by a PyCapsule-aware reader."""
    def __new__(cls, _private: NoReturn, /) -> ArrowStream: ...
    def __arrow_c_stream__(self, requested_schema: object = None) -> object: ...

class _ChunkGap(TypedDict):
    first_missing: int
    last_missing: int
    next_chunk: Path
    previous_last_frame_seq: int | None
    next_first_frame_seq: int
    missing_frame_count: int

class _Inspection(TypedDict):
    incomplete_files: list[Path]
    chunk_gaps: list[_ChunkGap]
    sealed_chunk_count: int
    compressed_bytes: int
    frame_count: int
    venue: Literal["polymarket", "kalshi"] | None
    recording_id: str | None
    first_received_utc: str | None
    last_received_utc: str | None
    connection_generations: set[int]
    frame_type_counts: dict[str, int]
    direction_counts: dict[str, int]
    local_event_counts: dict[str, int]

class _Reconstruction(TypedDict):
    topbook: ArrowStream
    depth: ArrowStream
    topbook_rows: int
    depth_rows: int

def inspect_raw_capture(path: str | PathLike[str]) -> _Inspection: ...
def reconstruct_raw_capture(
    path: str | PathLike[str], *, version: Literal[1, 2] = 2, target_rows: int = 8192
) -> _Reconstruction: ...

class _MergedReconstruction(_Reconstruction):
    messages: list[str]
    summary: str

def merge_raw_captures(
    venue: str, sessions: Sequence[tuple[str, str, str | PathLike[str]]], *,
    version: Literal[1, 2] = 2, target_rows: int = 8192, include_messages: bool = False,
) -> _MergedReconstruction: ...

class _Snapshot(TypedDict):
    venue: Literal["polymarket", "kalshi"]
    instrument: str
    shard_id: str
    generation: int
    quality_flags: list[str]
    metadata_revision: int

class _Book(_Snapshot):
    market_id: str | None
    bids: list[tuple[str, str]]
    asks: list[tuple[str, str]]
    received_monotonic_ns: int
    source_timestamp_json: str | None
    source_timestamp_milliseconds: bool
    tick_size: str | None
    minimum_order_size: str | None

class _Health(_Snapshot):
    connection_state: Literal["initialized", "connected", "disconnected", "closed"]
    usable: bool
    reason: str | None
    liveness_age_ns: int | None
    book_update_age_ns: int | None
    reconnect_count: int
    error_count: int
    last_error: str | None

class _Observation(TypedDict):
    book: _Book | None
    health: _Health

@final
class ShardFeed:
    def __init__(
        self, venue: Literal["polymarket", "kalshi"], shard_id: str,
        instruments: Sequence[str], *, liveness_ns: int | None = None,
        snapshot_ns: int | None = None,
    ) -> None: ...
    def connected(self, generation: int, now_ns: int, *, command_id: int | None = None) -> None: ...
    def disconnected(self, generation: int, reason: str) -> None: ...
    def heartbeat(self, generation: int, now_ns: int) -> None: ...
    def begin_requests(self, generation: int, now_ns: int) -> list[str]: ...
    def finish_requests(self, generation: int, ids: Sequence[str], now_ns: int) -> None: ...
    def poll_books(self, now_ns: int) -> None: ...
    def resolved(self, id: str) -> bool: ...
    def transport(self) -> tuple[int, int | None] | None: ...
    @overload
    def ingest(self, shard_id: str, generation: int, frame: bytes, now_ns: int, *, detach: Literal[True] = True) -> list[_Observation]: ...
    @overload
    def ingest(self, shard_id: str, generation: int, frame: bytes, now_ns: int, *, detach: Literal[False]) -> list[str]: ...
    def health(self, instrument: str, now_ns: int) -> _Health: ...
    def observation(self, instrument: str, now_ns: int) -> _Observation: ...
    def close(self) -> None: ...
