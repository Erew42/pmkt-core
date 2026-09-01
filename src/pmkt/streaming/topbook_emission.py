from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping

from pmkt.streaming.profiles import (
    TOPBOOK_CHANGE_TRIGGER_VERSION,
    TOPBOOK_EXCLUDED_QUALITY_FLAGS,
    DatasetRole,
)
from pmkt.streaming.tape import canonical_decimal, semantic_hash

EXCLUDED_NON_STATE_QUALITY_FLAGS = TOPBOOK_EXCLUDED_QUALITY_FLAGS
TOPBOOK_CHECKPOINT_REASONS = frozenset(
    {"startup", "periodic", "reconnect", "resync", "terminal"}
)
TOPBOOK_BOUNDARY_REASONS = frozenset({"startup", "reconnect", "resync", "terminal"})


def topbook_state_fingerprint(row: Mapping[str, Any]) -> str:
    flags = sorted(
        flag
        for flag in _quality_flags(row.get("quality_flags"))
        if flag not in EXCLUDED_NON_STATE_QUALITY_FLAGS
    )
    projection = {
        "version": TOPBOOK_CHANGE_TRIGGER_VERSION,
        "best_bid_dollars": _decimal_or_none(row.get("best_bid_dollars")),
        "best_ask_dollars": _decimal_or_none(row.get("best_ask_dollars")),
        "bid_size_contracts": _decimal_or_none(row.get("bid_size_contracts")),
        "ask_size_contracts": _decimal_or_none(row.get("ask_size_contracts")),
        "valid_state": bool(row.get("valid_state")),
        "tick_size_dollars": _decimal_or_none(row.get("tick_size_dollars")),
        "min_order_size_contracts": _decimal_or_none(
            row.get("min_order_size_contracts")
        ),
        "best_bid_source": _text_or_none(row.get("best_bid_source")),
        "best_ask_source": _text_or_none(row.get("best_ask_source")),
        "quality_flags": flags,
    }
    return semantic_hash(projection)


def topbook_primary_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("exchange") or ""),
        str(row.get("instrument_id") or ""),
        str(row.get("received_at_utc") or ""),
    )


@dataclass(frozen=True)
class TopbookEmission:
    role: DatasetRole
    row: Mapping[str, Any]
    fingerprint: str
    reason: str


class TopbookEmissionTracker:
    """Route state changes and restatements into mutually exclusive topbook roles."""

    def __init__(self, *, checkpoint_interval_seconds: float = 300.0) -> None:
        if checkpoint_interval_seconds <= 0:
            raise ValueError("checkpoint_interval_seconds must be positive")
        self.checkpoint_interval_ns = int(checkpoint_interval_seconds * 1_000_000_000)
        self._fingerprints: dict[tuple[str, str], str] = {}
        self._latest_rows: dict[tuple[str, str], Mapping[str, Any]] = {}
        self._last_checkpoint_ns: dict[tuple[str, str], int] = {}
        self._main_keys: set[tuple[str, str, str]] = set()
        self._checkpoint_keys: set[tuple[str, str, str]] = set()
        self._last_received_at: dict[tuple[str, str], datetime] = {}

    def observe(
        self,
        row: Mapping[str, Any],
        *,
        now_monotonic_ns: int,
        restatement_reason: str | None = None,
        force_main: bool = False,
    ) -> TopbookEmission | None:
        if now_monotonic_ns < 0:
            raise ValueError("now_monotonic_ns must be nonnegative")
        exchange = str(row.get("exchange") or "")
        if not exchange:
            raise ValueError("topbook exchange is required")
        instrument = str(row.get("instrument_id") or "")
        if not instrument:
            raise ValueError("topbook instrument_id is required")
        identity = (exchange, instrument)
        if (
            restatement_reason is not None
            and restatement_reason not in TOPBOOK_CHECKPOINT_REASONS
        ):
            raise ValueError("unsupported topbook checkpoint reason")
        fingerprint = topbook_state_fingerprint(row)
        previous = self._fingerprints.get(identity)
        self._latest_rows[identity] = dict(row)
        # force_main supports dense storage profiles: emit a main row on every
        # observation while still uniquifying primary-key receive clocks.
        if previous is None or previous != fingerprint or force_main:
            self._fingerprints[identity] = fingerprint
            if previous is None:
                reason = "initial"
            elif previous != fingerprint:
                reason = "state_change"
            else:
                reason = "dense_restatement"
            emission = self._emit(
                DatasetRole.TOPBOOK_MAIN,
                row,
                fingerprint,
                reason,
            )
            if emission is not None:
                self._last_checkpoint_ns[identity] = now_monotonic_ns
            return emission
        due = (
            now_monotonic_ns - self._last_checkpoint_ns.get(identity, 0)
            >= self.checkpoint_interval_ns
        )
        if restatement_reason is None and not due:
            return None
        reason = restatement_reason or "periodic"
        emission = self._emit(DatasetRole.TOPBOOK_CHECKPOINT, row, fingerprint, reason)
        if emission is not None:
            self._last_checkpoint_ns[identity] = now_monotonic_ns
        return emission

    def due_restatements(
        self,
        *,
        now_monotonic_ns: int,
        received_at_utc: str,
        local_sequence: int,
    ) -> tuple[TopbookEmission, ...]:
        emissions: list[TopbookEmission] = []
        for identity in sorted(self._latest_rows):
            if (
                now_monotonic_ns - self._last_checkpoint_ns.get(identity, 0)
                < self.checkpoint_interval_ns
            ):
                continue
            row = dict(self._latest_rows[identity])
            row["received_at_utc"] = received_at_utc
            row["received_at_monotonic_ns"] = now_monotonic_ns
            row["local_sequence"] = local_sequence
            emission = self.observe(
                row,
                now_monotonic_ns=now_monotonic_ns,
                restatement_reason="periodic",
            )
            if emission is not None:
                emissions.append(emission)
        return tuple(emissions)

    def boundary_restatements(
        self,
        *,
        reason: str,
        now_monotonic_ns: int,
        received_at_utc: str,
        local_sequence: int,
    ) -> tuple[TopbookEmission, ...]:
        """Restate every observed instrument at one explicit capture boundary."""
        if reason not in TOPBOOK_BOUNDARY_REASONS:
            raise ValueError("unsupported topbook boundary reason")
        if now_monotonic_ns < 0:
            raise ValueError("now_monotonic_ns must be nonnegative")
        emissions: list[TopbookEmission] = []
        for identity in sorted(self._latest_rows):
            row = dict(self._latest_rows[identity])
            row["received_at_utc"] = received_at_utc
            row["received_at_monotonic_ns"] = now_monotonic_ns
            row["local_sequence"] = local_sequence
            emission = self.observe(
                row,
                now_monotonic_ns=now_monotonic_ns,
                restatement_reason=reason,
            )
            if emission is not None:
                emissions.append(emission)
        return tuple(emissions)

    def _emit(
        self,
        role: DatasetRole,
        row: Mapping[str, Any],
        fingerprint: str,
        reason: str,
    ) -> TopbookEmission | None:
        emitted_row = dict(row)
        key = topbook_primary_key(emitted_row)
        own = (
            self._main_keys
            if role is DatasetRole.TOPBOOK_MAIN
            else self._checkpoint_keys
        )
        other = (
            self._checkpoint_keys
            if role is DatasetRole.TOPBOOK_MAIN
            else self._main_keys
        )
        instrument_key = (key[0], key[1])
        received_at = _parse_received_at(key[2])
        previous_received_at = self._last_received_at.get(instrument_key)
        if role is DatasetRole.TOPBOOK_CHECKPOINT:
            if key in own or key in other:
                return None
            if previous_received_at is not None and received_at <= previous_received_at:
                return None
        elif (
            key in own
            or key in other
            or (
                previous_received_at is not None and received_at <= previous_received_at
            )
        ):
            if previous_received_at is not None and received_at <= previous_received_at:
                received_at = previous_received_at + timedelta(microseconds=1)
            while True:
                emitted_row["received_at_utc"] = _format_received_at(received_at)
                key = topbook_primary_key(emitted_row)
                if key not in own and key not in other:
                    break
                received_at += timedelta(microseconds=1)
        own.add(key)
        self._last_received_at[instrument_key] = received_at
        return TopbookEmission(role, emitted_row, fingerprint, reason)


def _quality_flags(value: Any) -> Iterable[str]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (
            flag.strip() for flag in value.replace(",", ";").split(";") if flag.strip()
        )
    if isinstance(value, Iterable):
        return (str(flag).strip() for flag in value if str(flag).strip())
    return ()


def _decimal_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, Decimal) and value.is_nan():
        return None
    return canonical_decimal(value)


def _text_or_none(value: Any) -> str | None:
    return None if value is None else str(value)


def _parse_received_at(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("topbook received_at_utc must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("topbook received_at_utc must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _format_received_at(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


__all__ = [
    "EXCLUDED_NON_STATE_QUALITY_FLAGS",
    "TOPBOOK_CHANGE_TRIGGER_VERSION",
    "TOPBOOK_BOUNDARY_REASONS",
    "TOPBOOK_CHECKPOINT_REASONS",
    "TopbookEmission",
    "TopbookEmissionTracker",
    "topbook_primary_key",
    "topbook_state_fingerprint",
]
