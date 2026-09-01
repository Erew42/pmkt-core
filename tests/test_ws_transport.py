from __future__ import annotations

from collections.abc import Callable

import pytest

from pmkt.exchanges.ws_transport import is_transport_teardown_race


def _transport_error(attribute: str) -> AttributeError:
    namespace: dict[str, object] = {"__name__": "asyncio.sslproto"}
    source = (
        "def raise_transport_error():\n"
        "    transport = None\n"
        f"    transport.{attribute}()\n"
    )
    exec(compile(source, "sslproto.py", "exec"), namespace)
    raiser = namespace["raise_transport_error"]
    assert isinstance(raiser, Callable)
    try:
        raiser()
    except AttributeError as exc:
        return exc
    raise AssertionError("synthetic transport error did not raise")


@pytest.mark.parametrize("attribute", ["pause_reading", "resume_reading"])
def test_transport_teardown_race_requires_shape_and_transport_origin(
    attribute: str,
) -> None:
    assert is_transport_teardown_race(_transport_error(attribute))


def test_transport_teardown_race_rejects_matching_application_error() -> None:
    try:
        transport = None
        transport.pause_reading()  # type: ignore[union-attr]
    except AttributeError as exc:
        assert not is_transport_teardown_race(exc)
    else:
        raise AssertionError("application error did not raise")


def test_transport_teardown_race_rejects_exception_without_traceback() -> None:
    exc = AttributeError("'NoneType' object has no attribute 'resume_reading'")

    assert not is_transport_teardown_race(exc)


def test_transport_teardown_race_rejects_unrelated_attribute_error() -> None:
    assert not is_transport_teardown_race(AttributeError("application bug"))
