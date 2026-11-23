"""Replay recorded public wire frames into a fresh managed-feed book, offline."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import Literal

from pmkt import _native

Ladder = tuple[tuple[Decimal, Decimal], ...]


class WireReplay:
    """Ingest recorded frames into one new native shard, as ManagedBookFeed does.

    ``frames`` are ``(received_monotonic_ns, raw_wire_bytes)`` pairs from a
    single connection generation, in receipt order. No transport is opened.
    Construction ingests every frame, so time it when measuring a replay.
    """

    def __init__(
        self,
        venue: Literal["polymarket", "kalshi"],
        subscriptions: Sequence[str],
        frames: Sequence[tuple[int, bytes]],
        *,
        command_id: int | None = None,
    ) -> None:
        if not frames:
            raise ValueError("a wire replay needs at least one frame")
        self._feed = _native.ShardFeed(venue, "replay", list(subscriptions))
        self._feed.connected(1, frames[0][0], command_id=command_id)
        for received_ns, frame in frames:
            self._feed.ingest("replay", 1, frame, received_ns, detach=False)
        self._now_ns = frames[-1][0]

    def book(self, instrument: str) -> tuple[Ladder, Ladder] | None:
        """Return (bids, asks) at the last frame, or None without a usable book.

        An instrument outside ``subscriptions`` raises ValueError.

        Kalshi ladders are on the YES price basis, as in PublicBook.
        """
        observed = self._feed.read_book(instrument, self._now_ns)
        if observed is None:
            return None
        return tuple(observed["bids"]), tuple(observed["asks"])
