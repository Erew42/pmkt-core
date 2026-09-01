"""Shared websocket transport limits for venue market-data clients.

These were previously left at the ``websockets`` defaults, which produced two
distinct failures:

``max_size`` (default 1 MiB)
    The initial book snapshot scales with the subscribed universe.  Measured
    against the live Polymarket market feed: 74 tokens -> 182 KiB, 200 -> 534
    KiB, 400 -> 1002 KiB (98% of the default), 600 -> 1.45 MiB, at which the
    server closes the connection with 1009 "message too big".  That imposed a
    hard ceiling of roughly 410 instruments per socket, past which the client
    reconnected indefinitely while capturing nothing.

``max_queue`` (default 16 frames)
    When the application stalls -- a synchronous commit blocking the event loop
    -- the inbound frame buffer crosses the high-water mark and ``websockets``
    applies transport flow control via ``pause_reading``/``resume_reading``.
    Those calls are the precondition for the unguarded ``_transport``
    dereference in CPython's ``asyncio/sslproto.py``.  A larger bound keeps
    ordinary commit latency off that path.

Raising these bounds is containment, not a fix for the underlying blocking; it
removes a hard functional ceiling and widens the margin before flow control
engages.  Memory is bounded by ``max_queue`` frames, so the queue is sized to a
reviewed default and may be raised only through explicit capture configuration.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Bounded defaults sized above the observed 1.45 MiB / 600-instrument snapshot
# while avoiding the much larger per-connection memory exposure of the
# temporary 64 MiB / 4096-frame containment values.
WS_MAX_SIZE_BYTES = 16 * 1024 * 1024

# Bounded backpressure. Higher limits remain available only through explicit
# configuration on the capture/client boundary.
WS_MAX_QUEUE_FRAMES = 64


@dataclass(frozen=True)
class WebSocketTransportSettings:
    """Validated per-connection websocket bounds."""

    max_size_bytes: int = WS_MAX_SIZE_BYTES
    max_queue_frames: int = WS_MAX_QUEUE_FRAMES

    def __post_init__(self) -> None:
        for name, value in (
            ("max_size_bytes", self.max_size_bytes),
            ("max_queue_frames", self.max_queue_frames),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")

    def as_connect_kwargs(self) -> dict[str, int]:
        return {
            "max_size": self.max_size_bytes,
            "max_queue": self.max_queue_frames,
        }

    def as_manifest_mapping(
        self,
        *,
        requested_max_size_bytes: int | None = None,
        requested_max_queue_frames: int | None = None,
    ) -> dict[str, dict[str, int | None]]:
        return {
            "requested": {
                "max_size_bytes": requested_max_size_bytes,
                "max_queue_frames": requested_max_queue_frames,
            },
            "effective": {
                "max_size_bytes": self.max_size_bytes,
                "max_queue_frames": self.max_queue_frames,
            },
        }


WS_TRANSPORT_LIMITS: dict[str, Any] = WebSocketTransportSettings().as_connect_kwargs()

# Both transport flow-control entry points in CPython's asyncio SSL layer
# dereference the protocol's transport without a None guard:
#
#   asyncio/sslproto.py:335   self._ssl_protocol._transport.pause_reading()
#   asyncio/sslproto.py:343   self._ssl_protocol._transport.resume_reading()
#
# while lines 458/504 set ``_transport = None`` on close.  If the connection is
# torn down between a pause and its resume, the call raises AttributeError.
_TRANSPORT_FLOW_CONTROL_ATTRS = frozenset({"pause_reading", "resume_reading"})


# The race can only occur inside the transport flow-control machinery. Requiring
# an originating frame from those modules stops an unrelated application
# AttributeError -- which may legitimately carry name="pause_reading" and
# obj=None -- from being swallowed as a reconnectable transport event.
_TRANSPORT_ORIGIN_MODULES = (
    "asyncio.sslproto",
    "asyncio.selector_events",
    "asyncio.proactor_events",
    "websockets.",
)
_TRANSPORT_ORIGIN_FILES = ("sslproto.py", "selector_events.py", "proactor_events.py")


def _raised_from_transport_stack(exc: BaseException) -> bool:
    """True when the traceback's innermost frames sit in the transport stack.

    Deliberately matches module/file names rather than line numbers, so upstream
    reformatting does not break detection.
    """
    tb = exc.__traceback__
    if tb is None:
        return False
    # Walk to the innermost frames; the raising site is what matters.
    frames = []
    while tb is not None:
        frames.append(tb.tb_frame)
        tb = tb.tb_next
    for frame in reversed(frames[-4:]):
        module = frame.f_globals.get("__name__", "")
        filename = frame.f_code.co_filename.replace("\\", "/").rsplit("/", 1)[-1]
        if module.startswith(_TRANSPORT_ORIGIN_MODULES) or (
            filename in _TRANSPORT_ORIGIN_FILES
        ):
            return True
    return False


def is_transport_teardown_race(exc: BaseException) -> bool:
    """True when an AttributeError is the asyncio SSL transport teardown race.

    Two independent conditions must hold:

    1. **Shape** -- an ``AttributeError`` for ``pause_reading``/``resume_reading``
       on a ``None`` receiver. Checked structurally via ``.name``/``.obj`` rather
       than by comparing rendered text, which was fragile in both directions: an
       upstream rewording silently turned a recoverable disconnect into a hard
       capture failure, and the original check covered only ``resume_reading``
       even though the ``pause_reading`` site races identically and, measured
       under load, fires more often.
    2. **Origin** -- the exception was raised from the asyncio/websockets
       transport stack. Without this, an unrelated application error of the same
       shape would be silently converted into a reconnect.
    """
    if not isinstance(exc, AttributeError):
        return False
    name = getattr(exc, "name", None)
    obj = getattr(exc, "obj", None)
    if name is not None:
        # Python 3.10+ populates .name/.obj on AttributeError.
        shape_matches = name in _TRANSPORT_FLOW_CONTROL_ATTRS and obj is None
    else:
        # Conservative fallback for interpreters that do not populate .name.
        text = str(exc)
        shape_matches = "NoneType" in text and any(
            attr in text for attr in _TRANSPORT_FLOW_CONTROL_ATTRS
        )
    return shape_matches and _raised_from_transport_stack(exc)


__all__ = [
    "WS_MAX_QUEUE_FRAMES",
    "WS_MAX_SIZE_BYTES",
    "WS_TRANSPORT_LIMITS",
    "WebSocketTransportSettings",
    "is_transport_teardown_race",
]
