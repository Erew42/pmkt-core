from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

from pmkt.streaming.tape import semantic_hash

HEALTH_FINGERPRINT_VERSION = "feed-health-fingerprint.v1"
HEALTH_EMISSION_POLICY_VERSION = "feed-health-emission.v3"
DEFAULT_DETAIL_INTERVAL_SECONDS = 300.0
_FORCE_DETAIL_CAUSES = frozenset(
    {"connection", "startup", "reconnect", "gap", "error", "recovery", "terminal"}
)


def feed_health_fingerprint(row: Mapping[str, Any]) -> str:
    projection = {
        key: value
        for key, value in row.items()
        if key
        not in {
            "observed_at_utc",
            "local_sequence",
            "last_message_age_ms",
            "last_valid_book_age_ms",
            "instrument_state_json",
            "valid_book_count",
            "invalid_book_count",
        }
    }
    projection["quality_flags"] = sorted(_flags(row.get("quality_flags")))
    projection["instrument_state"] = _stable_instrument_state(
        row.get("instrument_state_json")
    )
    projection["version"] = HEALTH_FINGERPRINT_VERSION
    return semantic_hash(projection)


@dataclass(frozen=True)
class HealthEmission:
    row: Mapping[str, Any]
    detail_included: bool
    reason: str


@dataclass(frozen=True)
class _HealthEmissionUpdate:
    key: tuple[str, str]
    fingerprint: str
    blocked: bool
    emitted_at_ns: int
    reason: str
    encoded_bytes: int
    detail_included: bool
    detail_bytes: int


@dataclass
class PreparedHealthEmissions:
    """Health emissions whose emitter state advances only after acknowledgement."""

    emissions: tuple[HealthEmission, ...]
    _updates: tuple[_HealthEmissionUpdate, ...] = field(repr=False)
    _rows_evaluated: int = field(repr=False)
    _owner_token: object = field(repr=False)
    _generation: int = field(repr=False)
    _committed: bool = field(init=False, default=False, repr=False)


class SlimHealthEmitter:
    def __init__(
        self,
        *,
        interval_seconds: float = 10.0,
        detail_interval_seconds: float = DEFAULT_DETAIL_INTERVAL_SECONDS,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if detail_interval_seconds <= 0:
            raise ValueError("detail_interval_seconds must be positive")
        self.interval_ns = int(interval_seconds * 1_000_000_000)
        self.detail_interval_ns = int(detail_interval_seconds * 1_000_000_000)
        self._fingerprints: dict[tuple[str, str], str] = {}
        self._last_emitted_ns: dict[tuple[str, str], int] = {}
        self._last_detail_emitted_ns: dict[tuple[str, str], int] = {}
        self._blocked: dict[tuple[str, str], bool] = {}
        self._counts: Counter[str] = Counter()
        self._bytes: Counter[str] = Counter()
        self._detail_counts: Counter[str] = Counter()
        self._detail_bytes: Counter[str] = Counter()
        self._observe_calls = 0
        self._rows_evaluated = 0
        self._generation = 0
        self._commit_token = object()

    def due_shard_keys(
        self,
        shard_keys: Iterable[tuple[str, str]],
        *,
        now_monotonic_ns: int,
    ) -> tuple[tuple[str, str], ...]:
        if now_monotonic_ns < 0:
            raise ValueError("now_monotonic_ns must be nonnegative")
        due: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for venue, shard_id in shard_keys:
            key = (str(venue), str(shard_id))
            if not all(key) or key in seen:
                continue
            seen.add(key)
            last_emitted = self._last_emitted_ns.get(key)
            compact_due = (
                last_emitted is None
                or now_monotonic_ns - last_emitted >= self.interval_ns
            )
            last_detail_emitted = self._last_detail_emitted_ns.get(key)
            detail_due = self._blocked.get(key, False) and (
                last_detail_emitted is None
                or now_monotonic_ns - last_detail_emitted >= self.detail_interval_ns
            )
            if compact_due or detail_due:
                due.append(key)
        return tuple(sorted(due))

    def prepare(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        now_monotonic_ns: int,
        cause: str = "message",
        detail_provider: Callable[
            [tuple[str, str]], Mapping[str, Any]
        ]
        | None = None,
    ) -> PreparedHealthEmissions:
        if now_monotonic_ns < 0:
            raise ValueError("now_monotonic_ns must be nonnegative")
        emissions: list[HealthEmission] = []
        updates: list[_HealthEmissionUpdate] = []
        seen: set[tuple[str, str]] = set()
        rows_evaluated = 0
        for source in rows:
            key = (str(source.get("venue") or ""), str(source.get("shard_id") or ""))
            if not all(key) or key in seen:
                continue
            seen.add(key)
            rows_evaluated += 1
            fingerprint = feed_health_fingerprint(source)
            transition = self._fingerprints.get(key) != fingerprint
            due = (
                now_monotonic_ns - self._last_emitted_ns.get(key, -self.interval_ns)
                >= self.interval_ns
            )
            blocked = _is_blocked(source)
            detail_cause = _detail_cause(
                source,
                cause=cause,
                transition=transition,
                was_blocked=self._blocked.get(key, False),
            )
            detail_due = blocked and (
                now_monotonic_ns
                - self._last_detail_emitted_ns.get(key, -self.detail_interval_ns)
                >= self.detail_interval_ns
            )
            if detail_cause is None and detail_due:
                detail_cause = "blocked_periodic"
            forced = detail_cause is not None
            if not transition and not due and not forced:
                continue
            include_detail = forced
            row = dict(source)
            if include_detail and not row.get("instrument_state_json"):
                if detail_provider is not None:
                    detail_row = detail_provider(key)
                    detail_key = (
                        str(detail_row.get("venue") or ""),
                        str(detail_row.get("shard_id") or ""),
                    )
                    if detail_key != key:
                        raise ValueError(
                            "feed-health detail provider returned the wrong shard"
                        )
                    row["instrument_state_json"] = str(
                        detail_row.get("instrument_state_json") or ""
                    )
            elif not include_detail:
                row["instrument_state_json"] = ""
            reason = detail_cause or ("transition" if transition else "periodic")
            emission = HealthEmission(row, include_detail, reason)
            emissions.append(emission)
            encoded_bytes = len(
                json.dumps(
                    row,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
            )
            updates.append(
                _HealthEmissionUpdate(
                    key=key,
                    fingerprint=fingerprint,
                    blocked=blocked,
                    emitted_at_ns=now_monotonic_ns,
                    reason=reason,
                    encoded_bytes=encoded_bytes,
                    detail_included=include_detail,
                    detail_bytes=(
                        len(
                            str(row.get("instrument_state_json") or "").encode(
                                "utf-8"
                            )
                        )
                        if include_detail
                        else 0
                    ),
                )
            )
        return PreparedHealthEmissions(
            emissions=tuple(emissions),
            _updates=tuple(updates),
            _rows_evaluated=rows_evaluated,
            _owner_token=self._commit_token,
            _generation=self._generation,
        )

    def commit(self, prepared: PreparedHealthEmissions) -> None:
        if prepared._owner_token is not self._commit_token:
            raise ValueError("prepared health emissions belong to another emitter")
        if prepared._committed:
            raise ValueError("prepared health emissions were already committed")
        if prepared._generation != self._generation:
            raise RuntimeError("prepared health emissions are stale")
        self._observe_calls += 1
        self._rows_evaluated += prepared._rows_evaluated
        for update in prepared._updates:
            self._counts[update.reason] += 1
            self._bytes[update.reason] += update.encoded_bytes
            if update.detail_included:
                self._last_detail_emitted_ns[update.key] = update.emitted_at_ns
                self._detail_counts[update.reason] += 1
                self._detail_bytes[update.reason] += update.detail_bytes
            self._fingerprints[update.key] = update.fingerprint
            self._last_emitted_ns[update.key] = update.emitted_at_ns
            self._blocked[update.key] = update.blocked
        prepared._committed = True
        self._generation += 1

    def observe(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        now_monotonic_ns: int,
        cause: str = "message",
        detail_provider: Callable[
            [tuple[str, str]], Mapping[str, Any]
        ]
        | None = None,
    ) -> tuple[HealthEmission, ...]:
        prepared = self.prepare(
            rows,
            now_monotonic_ns=now_monotonic_ns,
            cause=cause,
            detail_provider=detail_provider,
        )
        self.commit(prepared)
        return prepared.emissions

    def manifest_metrics(self) -> dict[str, Any]:
        reasons = sorted(set(self._counts) | set(self._bytes))
        return {
            "policy_version": HEALTH_EMISSION_POLICY_VERSION,
            "detail_interval_seconds": self.detail_interval_ns / 1_000_000_000,
            "rows": sum(self._counts.values()),
            "bytes": sum(self._bytes.values()),
            "detail_rows": sum(self._detail_counts.values()),
            "detail_bytes": sum(self._detail_bytes.values()),
            "observe_calls": self._observe_calls,
            "rows_evaluated": self._rows_evaluated,
            "by_reason": {
                reason: {
                    "rows": self._counts[reason],
                    "bytes": self._bytes[reason],
                    "detail_rows": self._detail_counts[reason],
                    "detail_bytes": self._detail_bytes[reason],
                }
                for reason in reasons
            },
        }


def _detail_cause(
    row: Mapping[str, Any],
    *,
    cause: str,
    transition: bool,
    was_blocked: bool,
) -> str | None:
    if cause in _FORCE_DETAIL_CAUSES:
        return cause
    if not transition:
        return None
    flags = set(_flags(row.get("quality_flags")))
    if "sequence_gap" in flags:
        return "gap"
    if str(row.get("connection_state") or "") == "reconnecting":
        return "reconnect"
    blocked = _is_blocked(row)
    if blocked and not was_blocked:
        return "error"
    if not blocked and was_blocked:
        return "recovery"
    return None


def _is_blocked(row: Mapping[str, Any]) -> bool:
    flags = set(_flags(row.get("quality_flags")))
    instrument_count = int(row.get("instrument_count") or 0)
    covered_instruments = int(row.get("valid_instrument_count") or 0) + int(
        row.get("invalid_instrument_count") or 0
    )
    blocking_tokens = {
        "invalid_book",
        "invalid_instrument_books",
        "missing_instrument_books",
        "no_initial_snapshot",
        "sequence_gap",
        "stale_books",
        "stale_instrument_books",
        "stale_messages",
    }
    return (
        str(row.get("connection_state") or "") not in {"connected", "healthy"}
        or bool(flags & blocking_tokens)
        or covered_instruments < instrument_count
        or any(flag.startswith("error") for flag in flags)
        or int(row.get("invalid_instrument_count") or 0) > 0
        or int(row.get("missing_instrument_count") or 0) > 0
    )


def _stable_instrument_state(raw: Any) -> Any:
    if raw is None or raw == "":
        return []
    if isinstance(raw, str):
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            return {"malformed": True}
    else:
        decoded = raw
    if not isinstance(decoded, list):
        return {"malformed": True}
    stable = []
    for item in decoded:
        if not isinstance(item, Mapping):
            return {"malformed": True}
        stable.append(
            {
                key: sorted(_flags(value)) if key == "quality_flags" else value
                for key, value in item.items()
                if key
                not in {
                    "last_message_age_ms",
                    "last_valid_book_age_ms",
                    "valid_book_count",
                    "invalid_book_count",
                }
            }
        )
    return sorted(stable, key=lambda item: str(item.get("instrument") or ""))


def _flags(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item for item in value.replace(",", ";").split(";") if item]
    if isinstance(value, Iterable):
        return [str(item) for item in value if str(item)]
    return [str(value)]


__all__ = [
    "HEALTH_EMISSION_POLICY_VERSION",
    "HEALTH_FINGERPRINT_VERSION",
    "DEFAULT_DETAIL_INTERVAL_SECONDS",
    "HealthEmission",
    "PreparedHealthEmissions",
    "SlimHealthEmitter",
    "feed_health_fingerprint",
]
