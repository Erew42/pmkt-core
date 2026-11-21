from collections.abc import Iterator, Sequence
from decimal import Decimal
from os import PathLike
from pathlib import Path
from typing import Literal, NoReturn, TypedDict, final, overload

from pmkt.exchanges.kalshi.ws import KalshiOrderBookState
from pmkt.exchanges.polymarket.ws import MarketBookState

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

class _ManagedBookRead(TypedDict):
    market_id: str | None
    generation: int
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    received_monotonic_ns: int
    source_timestamp_json: str | None
    quality_flags: list[str]
    metadata_revision: int

@final
class ShardFeed:
    def __init__(
        self, venue: Literal["polymarket", "kalshi"], shard_id: str,
        instruments: Sequence[str], *, liveness_ns: int | None = None,
        snapshot_ns: int | None = None, polymarket_socket_liveness: bool = False,
    ) -> None: ...
    def connected(self, generation: int, now_ns: int, *, command_id: int | None = None) -> None: ...
    def disconnected(self, generation: int, reason: str) -> None: ...
    def heartbeat(self, generation: int, now_ns: int) -> None: ...
    def begin_requests(self, generation: int, now_ns: int) -> list[str]: ...
    def finish_requests(self, generation: int, ids: Sequence[str], now_ns: int) -> None: ...
    def poll_books(self, now_ns: int) -> None: ...
    def resolved(self, id: str) -> bool: ...
    def transport(self) -> tuple[int, int | None] | None: ...
    def poll_transport(self, now_ns: int) -> str | None: ...
    @overload
    def ingest(self, shard_id: str, generation: int, frame: bytes, now_ns: int, *, detach: Literal[True] = True) -> list[_Observation]: ...
    @overload
    def ingest(self, shard_id: str, generation: int, frame: bytes, now_ns: int, *, detach: Literal[False]) -> list[str]: ...
    def health(self, instrument: str, now_ns: int) -> _Health: ...
    def read_book(self, instrument: str, now_ns: int) -> _ManagedBookRead | None: ...
    def observation(self, instrument: str, now_ns: int) -> _Observation: ...
    def close(self) -> None: ...


def book_integrity(initialized: bool, quality_flags: Sequence[str]) -> bool: ...
def update_market_book(
    state: MarketBookState, event_type: str, message: dict[str, object], change: dict[str, object] | None = None,
) -> bool: ...
def update_kalshi_book(
    state: KalshiOrderBookState, event_type: str, body: dict[str, object], sid: int | None, seq: int | None,
) -> tuple[Decimal | None, Decimal | None]: ...

class _RecordingBook(TypedDict):
    instrument: str
    market: str | None
    first: list[tuple[str, str]]
    second: list[tuple[str, str]]
    initialized: bool
    intact: bool
    valid: bool
    flags: list[str]
    timestamp_json: str | None
    milliseconds: bool
    tick: str | None
    minimum: str | None
    source_update: bool

@final
class RecordingBooks:
    def __init__(self, venue_name: str, instruments: Sequence[str]) -> None: ...
    def reset(self) -> None: ...
    def resolved(self, instrument: str) -> bool: ...
    def apply(self, message: str, count: int) -> list[_RecordingBook]: ...
    def recovery_due(self, count: int, now_ns: int) -> bool: ...
    def complementary_due(self, count: int, now_ns: int) -> bool: ...
    def next_deadline_ns(self) -> int | None: ...

@final
class BookCheck:
    def __init__(self, venue_name: str) -> None: ...
    def apply(self, message: str) -> list[tuple[str, int | None]]: ...


_MergeInput = tuple[tuple[str, str], str, int, int, int | None, int, str, bool]

class MergeError(ValueError): ...

def snapshot_class(kind: str, reconnect: bool, message: str) -> str: ...
def conflated_subscription(raw: str) -> bool: ...

def message_key(venue_name: str, message: str) -> tuple[str, str, int | None, int | None, bytes]: ...

@final
class CopyMerge:
    def __init__(
        self, venue_name: str, sources: Sequence[Iterator[_MergeInput]],
        lateness_ms: int, untimed_match_ms: int, retention_ms: int,
        strict_order: bool, preferred_copies: Sequence[str],
    ) -> None: ...
    def __iter__(self) -> Iterator[str]: ...
    def __next__(self) -> str: ...
    def stats(self) -> str: ...
