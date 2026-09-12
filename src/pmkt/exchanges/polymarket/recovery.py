"""Bounded recovery deferral for complementary Polymarket price deltas."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from pmkt.exchanges.book_integrity import finite_number
from pmkt.exchanges.polymarket.ws import MarketBookState


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

    def observe(
        self, message: Mapping[str, Any], states: Mapping[str, MarketBookState],
        previously_intact: set[str], *, message_count: int,
    ) -> None:
        changes = message.get("price_changes", [])
        if not isinstance(changes, list):
            changes = []
        by_asset = {str(c.get("asset_id") or ""): c for c in changes if isinstance(c, dict)}
        for asset in list(self.pending):
            state = states.get(asset)
            change = by_asset.get(asset)
            if state is None or state.book_integrity_valid:
                self.pending.pop(asset)
                if state is not None:
                    self.resolved += 1
            elif str(message.get("asset_id") or "") == asset and message.get("event_type") == "book":
                self.pending.pop(asset)
            elif change is not None and (
                str(change.get("hash") or "") != self.pending[asset].book_hash
                or str(message.get("timestamp") or "") != self.pending[asset].timestamp
                or state.quality_flags != {"crossed_book"}
            ):
                self.pending.pop(asset)
        if message.get("event_type") != "price_change":
            return
        for asset, change in by_asset.items():
            state = states.get(asset)
            if asset not in previously_intact or asset in self.pending or state is None:
                continue
            bid, ask = finite_number(change.get("best_bid")), finite_number(change.get("best_ask"))
            size, price = finite_number(change.get("size")), finite_number(change.get("price"))
            timestamp, book_hash = str(message.get("timestamp") or ""), str(change.get("hash") or "")
            if (state.initial_snapshot_received and state.quality_flags == {"crossed_book"}
                    and state.best_bid == state.best_ask == price
                    and bid is not None and ask is not None and 0 <= bid < ask <= 1
                    and size is not None and size > 0 and change.get("side") in {"BUY", "SELL"}
                    and timestamp and book_hash):
                self.pending[asset] = _PendingDelta(timestamp, book_hash, message_count)
                self.candidates += 1

    def defer(self, asset: str, *, message_count: int, now_ns: int) -> bool:
        pending = self.pending.get(asset)
        if pending is None:
            return False
        if pending.deadline_ns is None:
            pending.deadline_ns = now_ns + self.grace_ns
        if now_ns >= pending.deadline_ns or message_count - pending.message_count >= self.max_followup_messages:
            self.pending.pop(asset)
            self.expired += 1
            return False
        return True

    def clear(self) -> None:
        self.pending.clear()

    def metrics(self) -> dict[str, Any]:
        return {"policy": "complementary-delta.v1", "grace_ms": self.grace_ns // 1_000_000,
                "max_followup_messages": self.max_followup_messages, "candidates": self.candidates,
                "resolved": self.resolved, "expired": self.expired, "pending": len(self.pending)}
