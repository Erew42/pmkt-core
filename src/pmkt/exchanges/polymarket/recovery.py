"""Bounded recovery deferral for complementary Polymarket price deltas."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from pmkt.exchanges.book_integrity import finite_number
from pmkt.exchanges.polymarket.ws import MarketBookState


def _event_type(message: Mapping[str, Any]) -> str:
    return str(message.get("event_type") or message.get("type") or "")


def _changes_by_asset(message: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    if _event_type(message) != "price_change":
        return {}
    changes = message.get("price_changes")
    if not isinstance(changes, list):
        return {}
    return {
        str(change.get("asset_id") or ""): change
        for change in changes
        if isinstance(change, dict)
    }


@dataclass
class _PendingDelta:
    timestamp: str
    book_hash: str
    message_count: int
    deadline_ns: int | None = None


class ComplementaryDeltaRecovery:
    """Keep invalid evidence while allowing a bounded corrective update.

    The timer begins at the first recovery decision, after synchronous writes.
    A commit delay before that decision must not consume the receive opportunity.
    Neither unrelated traffic nor repeated locked updates renew either bound.
    """

    grace_ns = 250_000_000
    max_followup_messages = 16

    def __init__(self) -> None:
        self.pending: dict[str, _PendingDelta] = {}
        self.candidates = 0
        self.resolved = 0
        self.expired = 0

    @staticmethod
    def intact_changed_assets(
        message: Mapping[str, Any],
        states: Mapping[str, MarketBookState],
    ) -> set[str]:
        """Read pre-update integrity only for books touched by this delta.

        The collector calls this before applying the message. Scanning the
        subscription universe here would add work for every silent peer to
        every received message, including trades and heartbeat observations.
        """
        return {
            asset
            for asset in _changes_by_asset(message)
            if (state := states.get(asset)) is not None and state.book_integrity_valid
        }

    def observe(
        self,
        message: Mapping[str, Any],
        states: Mapping[str, MarketBookState],
        previously_intact: set[str],
        *,
        message_count: int,
    ) -> set[str]:
        """Return still-invalid books whose recovery delay was withdrawn."""
        event_type = _event_type(message)
        by_asset = _changes_by_asset(message)
        revoked: set[str] = set()
        for asset in list(self.pending):
            state = states.get(asset)
            change = by_asset.get(asset)
            if state is None or state.book_integrity_valid:
                self.pending.pop(asset)
                if state is not None:
                    self.resolved += 1
            elif str(message.get("asset_id") or "") == asset and event_type == "book":
                self.pending.pop(asset)
                revoked.add(asset)
            elif change is not None and (
                str(change.get("hash") or "") != self.pending[asset].book_hash
                or str(message.get("timestamp") or "") != self.pending[asset].timestamp
                or state.quality_flags != {"crossed_book"}
            ):
                self.pending.pop(asset)
                revoked.add(asset)
        if event_type != "price_change":
            return revoked
        for asset, change in by_asset.items():
            state = states.get(asset)
            if asset not in previously_intact or asset in self.pending or state is None:
                continue
            bid, ask = (
                finite_number(change.get("best_bid")),
                finite_number(change.get("best_ask")),
            )
            size, price = (
                finite_number(change.get("size")),
                finite_number(change.get("price")),
            )
            timestamp, book_hash = (
                str(message.get("timestamp") or ""),
                str(change.get("hash") or ""),
            )
            if (
                state.initial_snapshot_received
                and state.quality_flags == {"crossed_book"}
                and state.best_bid == state.best_ask == price
                and bid is not None
                and ask is not None
                and 0 <= bid < ask <= 1
                and size is not None
                and size > 0
                and change.get("side") in {"BUY", "SELL"}
                and timestamp
                and book_hash
            ):
                self.pending[asset] = _PendingDelta(timestamp, book_hash, message_count)
                self.candidates += 1
        return revoked

    def defer(self, asset: str, *, message_count: int, now_ns: int) -> bool:
        pending = self.pending.get(asset)
        if pending is None:
            return False
        if pending.deadline_ns is None:
            pending.deadline_ns = now_ns + self.grace_ns
        if self._expired(pending, message_count=message_count, now_ns=now_ns):
            self.pending.pop(asset)
            self.expired += 1
            return False
        return True

    def _expired(
        self, pending: _PendingDelta, *, message_count: int, now_ns: int
    ) -> bool:
        return (
            pending.deadline_ns is not None and now_ns >= pending.deadline_ns
        ) or message_count - pending.message_count >= self.max_followup_messages

    def recovery_due(self, *, message_count: int, now_ns: int) -> bool:
        """Check bounds before a later message can clear an invalid book."""
        return any(
            self._expired(pending, message_count=message_count, now_ns=now_ns)
            for pending in self.pending.values()
        )

    @property
    def next_deadline_ns(self) -> int | None:
        """Wake an idle collector when an already-granted delay expires."""
        return min(
            (
                pending.deadline_ns
                for pending in self.pending.values()
                if pending.deadline_ns is not None
            ),
            default=None,
        )

    def clear(self) -> None:
        self.pending.clear()

    def metrics(self) -> dict[str, Any]:
        return {
            "policy": "complementary-delta.v1",
            "grace_ms": self.grace_ns // 1_000_000,
            "max_followup_messages": self.max_followup_messages,
            "candidates": self.candidates,
            "resolved": self.resolved,
            "expired": self.expired,
            "pending": len(self.pending),
        }
