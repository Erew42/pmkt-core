from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from pmkt.data.types import parse_float

if TYPE_CHECKING:
    import pandas as pd


EpochUnit = Literal["seconds", "milliseconds", "auto"]
TimestampErrors = Literal["coerce", "raise"]

_EXPLICIT_OFFSET_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}"
    r"(?P<fraction>\.\d+)?"
    r"(?P<offset>Z|z|[+-]\d{2}(?::?\d{2})?)$"
)


def timestamp_seconds(value: Any, *, unit: EpochUnit = "auto") -> float | None:
    """Parse an epoch value and return seconds.

    Raw venue adapters should declare ``seconds`` or ``milliseconds`` whenever
    the source field has a fixed unit. ``auto`` preserves the historical
    seconds/milliseconds compatibility heuristic for source fields whose name
    and retained values do not establish one unit.
    """

    if unit not in {"seconds", "milliseconds", "auto"}:
        raise ValueError(f"unsupported epoch unit {unit!r}")
    parsed = parse_float(value)
    if parsed is None:
        return None
    if unit == "milliseconds":
        return parsed / 1000.0
    if unit == "seconds":
        return parsed
    return parsed / 1000.0 if abs(parsed) > 10_000_000_000 else parsed


def parse_utc_timestamp(value: Any) -> datetime | None:
    """Parse canonical explicit-offset text and normalize it to UTC.

    Numeric epochs deliberately do not belong to the canonical timestamp
    contract. They must be handled at a raw-source boundary with an explicit
    unit (or the documented compatibility heuristic).
    """

    if value is None:
        return None
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    match = _EXPLICIT_OFFSET_TIMESTAMP_RE.fullmatch(text)
    if match is None:
        return None
    offset = match.group("offset")
    if offset in {"Z", "z"}:
        text = text[: -len(offset)] + "+00:00"
    elif len(offset) == 3:
        text = text[: -len(offset)] + offset + ":00"
    elif len(offset) == 5 and ":" not in offset:
        text = text[: -len(offset)] + offset[:3] + ":" + offset[3:]
    fraction = match.group("fraction") or ""
    fraction_digits = len(fraction) - 1 if fraction else 0
    if 0 < fraction_digits <= 6:
        # Python 3.10's fromisoformat accepts only selected fractional-second
        # widths. RFC3339 permits any non-empty width, so normalize through
        # microseconds before parsing to keep behavior version-independent.
        normalized_fraction = "." + fraction[1:].ljust(6, "0")
        start, end = match.span("fraction")
        text = text[:start] + normalized_fraction + text[end:]
    try:
        if fraction_digits > 6:
            # Python 3.10 rejects precision beyond microseconds, while 3.11+
            # accepts the text but silently truncates it. Route explicitly to
            # pandas so the canonical result is version-independent.
            import pandas as pd

            parsed = pd.Timestamp(text)
        else:
            parsed = datetime.fromisoformat(text)
    except (ImportError, TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def isoformat_source_timestamp(
    value: Any,
    *,
    epoch_unit: EpochUnit,
) -> str | None:
    """Normalize a timestamp at a raw-source boundary.

    Explicit-offset text follows the canonical parser. Numeric input is
    interpreted only according to the source adapter's declared epoch policy.
    """

    parsed = parse_utc_timestamp(value)
    if parsed is not None:
        return parsed.isoformat()
    seconds = timestamp_seconds(value, unit=epoch_unit)
    if seconds is None:
        return None
    try:
        parsed = datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
    return parsed.isoformat()


def isoformat_utc(value: Any) -> str | None:
    """Compatibility wrapper for raw timestamps with an unknown epoch unit.

    New source adapters should call :func:`isoformat_source_timestamp` and name
    their unit. Canonical readers should call :func:`parse_utc_timestamp`.
    """

    return isoformat_source_timestamp(value, epoch_unit="auto")


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def parse_utc_timestamp_series(
    values: Any,
    *,
    errors: TimestampErrors = "coerce",
    field_name: str = "timestamp",
) -> "pd.Series":
    """Parse canonical timestamps with the same policy as the scalar path.

    Missing values remain ``NaT``. In ``raise`` mode, the first malformed
    non-null value reports its source field, row index, and exact value so an
    authoritative ingestion path cannot silently discard the row.
    """

    import pandas as pd

    if errors not in {"coerce", "raise"}:
        raise ValueError(f"unsupported timestamp error policy {errors!r}")
    source = values if isinstance(values, pd.Series) else pd.Series(values)
    parsed_values: list[datetime | None] = []
    for index, value in source.items():
        parsed = parse_utc_timestamp(value)
        if parsed is None and not _is_missing_scalar(value) and errors == "raise":
            raise ValueError(
                f"{field_name}: invalid explicit-offset timestamp "
                f"at index {index!r}: {value!r}"
            )
        parsed_values.append(parsed)
    converted = pd.to_datetime(parsed_values, utc=True, errors="coerce")
    if errors == "raise":
        for (index, value), parsed, converted_value in zip(
            source.items(), parsed_values, converted
        ):
            if parsed is not None and pd.isna(converted_value):
                raise ValueError(
                    f"{field_name}: timestamp outside the supported pandas range "
                    f"at index {index!r}: {value!r}"
                )
    return pd.Series(
        converted,
        index=source.index,
        name=source.name,
    )


def _is_missing_scalar(value: Any) -> bool:
    import pandas as pd

    if value is None:
        return True
    if not pd.api.types.is_scalar(value):
        return False
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


__all__ = [
    "EpochUnit",
    "isoformat_utc",
    "isoformat_source_timestamp",
    "parse_utc_timestamp",
    "parse_utc_timestamp_series",
    "timestamp_seconds",
    "utc_now_iso",
]
