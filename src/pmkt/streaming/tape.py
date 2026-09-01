from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence

from pmkt.data.canonical import (
    book_tape_control_row,
    book_tape_event_row,
    book_tape_level_row,
    canonical_fixed_decimal,
)
from pmkt.streaming.capture import TapeBatchIntent

TAPE_ENCODING_VERSION = "book-tape.v1"
TAPE_ID_VERSION = "capture-id.v1"
ADAPTER_ENCODING_VERSION = "venue-book-state.v1"
FAMILY_PRECEDENCE: Mapping[str, int] = {
    "invalidation_control": 0,
    "book_event": 1,
    "topbook": 2,
    "trade_lifecycle": 3,
    "recovery_control": 4,
    "health": 5,
}


def canonical_decimal(value: Any) -> str:
    """Return the non-exponent fixed-decimal identity for a numeric value."""
    return canonical_fixed_decimal(value)


def _canonicalize(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _canonicalize(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonicalize(item) for item in value]
    if isinstance(value, Decimal):
        return canonical_decimal(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical JSON does not support non-finite floats")
        return canonical_decimal(value)
    return value


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        _canonicalize(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def canonical_json(value: Any) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def semantic_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def versioned_id(
    name: str, values: Sequence[Any], *, version: str = TAPE_ID_VERSION
) -> str:
    return semantic_hash([name, version, *values])


def canonical_utc(value: datetime | str) -> str:
    if isinstance(value, str):
        candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            raise ValueError(f"invalid timestamp: {value!r}") from None
    else:
        parsed = value
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    parsed = parsed.astimezone(timezone.utc)
    return parsed.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass(frozen=True)
class CaptureCoordinate:
    collector_run_id: str
    shard_id: str
    received_at_utc: str
    received_at_monotonic_ns: int
    local_sequence: int
    subsequence: int

    def __post_init__(self) -> None:
        canonical_utc(self.received_at_utc)
        if self.received_at_monotonic_ns < 0 or self.local_sequence < 0:
            raise ValueError("capture coordinates must be nonnegative")
        if self.subsequence < 0:
            raise ValueError("subsequence must be nonnegative")


@dataclass(frozen=True)
class NativeBookLevel:
    source_side: str
    price: Any
    size_after_contracts: Any
    size_delta_contracts: Any | None = None
    level_ordinal: int = 0

    @property
    def price_key(self) -> str:
        return canonical_decimal(self.price)


def epoch_id(
    coordinate: CaptureCoordinate,
    *,
    venue_book_id: str,
    epoch_generation: int,
    encoding_version: str = TAPE_ENCODING_VERSION,
) -> str:
    if epoch_generation < 0:
        raise ValueError("epoch_generation must be nonnegative")
    return versioned_id(
        "book-epoch",
        [
            coordinate.collector_run_id,
            coordinate.shard_id,
            venue_book_id,
            epoch_generation,
            coordinate.local_sequence,
            coordinate.subsequence,
            encoding_version,
        ],
    )


def post_book_hash(
    *,
    venue: str,
    venue_book_id: str,
    levels: Iterable[NativeBookLevel],
    adapter_settings: Mapping[str, Any] | None = None,
    adapter_encoding_version: str = ADAPTER_ENCODING_VERSION,
) -> str:
    ordered = sorted(
        (
            level.source_side,
            level.price_key,
            canonical_decimal(level.size_after_contracts),
        )
        for level in levels
        if Decimal(canonical_decimal(level.size_after_contracts)) > 0
    )
    return semantic_hash(
        [
            venue,
            adapter_encoding_version,
            venue_book_id,
            dict(adapter_settings or {}),
            ordered,
        ]
    )


def _deduplicate_levels(
    levels: Iterable[NativeBookLevel],
) -> tuple[NativeBookLevel, ...]:
    collapsed: dict[tuple[str, str], NativeBookLevel] = {}
    for level in levels:
        if level.level_ordinal < 0:
            raise ValueError("level ordinal must be nonnegative")
        collapsed[(level.source_side, level.price_key)] = level
    return tuple(
        NativeBookLevel(
            source_side=level.source_side,
            price=level.price_key,
            size_after_contracts=level.size_after_contracts,
            size_delta_contracts=level.size_delta_contracts,
            level_ordinal=ordinal,
        )
        for ordinal, (_, level) in enumerate(sorted(collapsed.items()))
    )


def tape_level_payload_projection(
    levels: Iterable[Mapping[str, Any]],
) -> list[list[Any]]:
    ordered = sorted(
        levels,
        key=lambda row: (
            int(row.get("level_ordinal") or 0),
            str(row.get("source_side") or ""),
            str(row.get("price_key") or ""),
        ),
    )
    return [
        [
            str(row.get("source_side") or ""),
            str(row.get("price_key") or ""),
            canonical_decimal(row.get("size_after_contracts")),
            (
                canonical_decimal(row.get("size_delta_contracts"))
                if row.get("size_delta_contracts") is not None
                else None
            ),
            int(row.get("level_ordinal") or 0),
        ]
        for row in ordered
    ]


def recompute_tape_event_payload_hash(
    event: Mapping[str, Any], levels: Iterable[Mapping[str, Any]]
) -> str:
    excluded = {"event_id", "event_payload_hash", "raw_event_ref", "raw_event_hash"}
    header_projection = {
        key: value for key, value in event.items() if key not in excluded
    }
    return semantic_hash(
        {
            "header": header_projection,
            "levels": tape_level_payload_projection(levels),
        }
    )


def recompute_tape_event_id(
    event: Mapping[str, Any],
    *,
    shard_id: str,
    payload_hash: str | None = None,
) -> str:
    resolved_payload_hash = payload_hash or str(event.get("event_payload_hash") or "")
    return versioned_id(
        "book-tape-event",
        [
            str(event.get("collector_run_id") or ""),
            shard_id,
            str(event.get("venue_book_id") or ""),
            event.get("epoch_id"),
            int(event.get("local_sequence") or 0),
            int(event.get("subsequence") or 0),
            str(event.get("event_kind") or ""),
            resolved_payload_hash,
        ],
    )


def build_tape_batch(
    *,
    coordinate: CaptureCoordinate,
    venue: str,
    venue_market_id: str,
    venue_book_id: str,
    event_kind: str,
    epoch: str | None,
    levels: Iterable[NativeBookLevel],
    full_book_levels: Iterable[NativeBookLevel],
    allowed_source_sides: Sequence[str],
    valid_state: bool,
    reconstructible: bool,
    quality_flags: Iterable[str] = (),
    checkpoint_reason: str | None = None,
    exchange_at_utc: str | None = None,
    venue_sequence: Any | None = None,
    venue_sid: Any | None = None,
    raw_event_ref: str | None = None,
    raw_event_hash: str | None = None,
    adapter_settings: Mapping[str, Any] | None = None,
    encoding_version: str = TAPE_ENCODING_VERSION,
) -> TapeBatchIntent:
    if event_kind not in {"checkpoint", "delta"}:
        raise ValueError("event_kind must be checkpoint or delta")
    if event_kind == "checkpoint" and checkpoint_reason is None:
        raise ValueError("checkpoint_reason is required for checkpoints")
    if event_kind == "delta" and checkpoint_reason is not None:
        raise ValueError("checkpoint_reason must be absent for deltas")
    if reconstructible and epoch is None:
        raise ValueError("reconstructible events require an open epoch")
    unique_levels = _deduplicate_levels(levels)
    allowed = tuple(allowed_source_sides)
    if any(level.source_side not in allowed for level in unique_levels):
        raise ValueError("tape level uses an unsupported venue source side")
    side_counts = None
    if event_kind == "checkpoint":
        counts = {side: 0 for side in allowed}
        for level in unique_levels:
            counts[level.source_side] += 1
        side_counts = canonical_json(counts)
    header = book_tape_event_row(
        collector_run_id=coordinate.collector_run_id,
        event_id=None,
        venue=venue,
        venue_market_id=venue_market_id,
        venue_book_id=venue_book_id,
        epoch_id=epoch,
        event_kind=event_kind,
        checkpoint_reason=checkpoint_reason,
        received_at_utc=canonical_utc(coordinate.received_at_utc),
        received_at_monotonic_ns=coordinate.received_at_monotonic_ns,
        exchange_at_utc=canonical_utc(exchange_at_utc) if exchange_at_utc else None,
        local_sequence=coordinate.local_sequence,
        subsequence=coordinate.subsequence,
        venue_sequence=str(venue_sequence) if venue_sequence is not None else None,
        venue_sid=str(venue_sid) if venue_sid is not None else None,
        expected_level_row_count=len(unique_levels),
        side_counts_json=side_counts,
        post_book_hash=post_book_hash(
            venue=venue,
            venue_book_id=venue_book_id,
            levels=full_book_levels,
            adapter_settings=adapter_settings,
        ),
        valid_state=valid_state,
        reconstructible=reconstructible,
        quality_flags_json=canonical_json(sorted(set(quality_flags))),
        raw_event_ref=raw_event_ref,
        raw_event_hash=raw_event_hash,
        event_payload_hash=None,
        encoding_version=encoding_version,
    )
    level_identity_rows = [
        {
            "source_side": level.source_side,
            "price_key": level.price_key,
            "size_after_contracts": level.size_after_contracts,
            "size_delta_contracts": level.size_delta_contracts,
            "level_ordinal": level.level_ordinal,
        }
        for level in unique_levels
    ]
    payload_hash = recompute_tape_event_payload_hash(header, level_identity_rows)
    event_id = recompute_tape_event_id(
        header,
        shard_id=coordinate.shard_id,
        payload_hash=payload_hash,
    )
    header["event_id"] = event_id
    header["event_payload_hash"] = payload_hash
    rows = tuple(
        book_tape_level_row(
            collector_run_id=coordinate.collector_run_id,
            event_id=event_id,
            venue=venue,
            venue_book_id=venue_book_id,
            epoch_id=epoch,
            source_side=level.source_side,
            price_key=level.price_key,
            price_dollars=float(Decimal(level.price_key)),
            size_after_contracts=float(
                Decimal(canonical_decimal(level.size_after_contracts))
            ),
            size_delta_contracts=(
                float(Decimal(canonical_decimal(level.size_delta_contracts)))
                if level.size_delta_contracts is not None
                else None
            ),
            level_ordinal=level.level_ordinal,
        )
        for level in unique_levels
    )
    return TapeBatchIntent.materialize(event=header, levels=rows)


def build_control_row(
    *,
    coordinate: CaptureCoordinate,
    venue: str,
    venue_market_id: str,
    venue_book_id: str,
    control_type: str,
    reason: str,
    valid_after: bool,
    epoch: str | None = None,
    exchange_at_utc: str | None = None,
    venue_sequence: Any | None = None,
    evidence_role: str | None = None,
    evidence_id: str | None = None,
    quality_flags: Iterable[str] = (),
) -> dict[str, Any]:
    semantic_payload = [
        venue,
        venue_market_id,
        venue_book_id,
        control_type,
        reason,
        valid_after,
        epoch,
        evidence_role,
        evidence_id,
        sorted(set(quality_flags)),
    ]
    control_id = versioned_id(
        "book-tape-control",
        [
            coordinate.collector_run_id,
            coordinate.shard_id,
            venue_book_id,
            coordinate.local_sequence,
            coordinate.subsequence,
            semantic_hash(semantic_payload),
        ],
    )
    return book_tape_control_row(
        collector_run_id=coordinate.collector_run_id,
        control_id=control_id,
        venue=venue,
        venue_market_id=venue_market_id,
        venue_book_id=venue_book_id,
        epoch_id=epoch,
        control_type=control_type,
        reason=reason,
        valid_after=valid_after,
        received_at_utc=canonical_utc(coordinate.received_at_utc),
        received_at_monotonic_ns=coordinate.received_at_monotonic_ns,
        exchange_at_utc=canonical_utc(exchange_at_utc) if exchange_at_utc else None,
        local_sequence=coordinate.local_sequence,
        subsequence=coordinate.subsequence,
        venue_sequence=str(venue_sequence) if venue_sequence is not None else None,
        evidence_role=evidence_role,
        evidence_id=evidence_id,
        quality_flags_json=canonical_json(sorted(set(quality_flags))),
    )


def deterministic_merge_key(
    row: Mapping[str, Any], *, shard_id: str, family: str
) -> tuple[Any, ...]:
    return (
        str(row.get("received_at_utc") or ""),
        shard_id,
        int(row.get("local_sequence") or 0),
        int(row.get("subsequence") or 0),
        FAMILY_PRECEDENCE[family],
        str(
            row.get("event_id")
            or row.get("control_id")
            or row.get("lifecycle_event_id")
            or row.get("trade_id")
            or ""
        ),
    )


__all__ = [
    "ADAPTER_ENCODING_VERSION",
    "CaptureCoordinate",
    "FAMILY_PRECEDENCE",
    "NativeBookLevel",
    "TAPE_ENCODING_VERSION",
    "TAPE_ID_VERSION",
    "build_control_row",
    "build_tape_batch",
    "canonical_decimal",
    "canonical_json",
    "canonical_json_bytes",
    "canonical_utc",
    "deterministic_merge_key",
    "epoch_id",
    "post_book_hash",
    "recompute_tape_event_id",
    "recompute_tape_event_payload_hash",
    "tape_level_payload_projection",
    "semantic_hash",
    "versioned_id",
]
