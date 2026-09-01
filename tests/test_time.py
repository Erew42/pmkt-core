from __future__ import annotations

import pandas as pd
import pytest

from pmkt.data.time import (
    isoformat_source_timestamp,
    parse_utc_timestamp,
    parse_utc_timestamp_series,
    timestamp_seconds,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-06-15T00:00:00Z", "2026-06-15T00:00:00+00:00"),
        ("2026-06-15T02:00:00+02", "2026-06-15T00:00:00+00:00"),
        ("2026-06-15T02:00:00+0200", "2026-06-15T00:00:00+00:00"),
        ("2026-06-15 02:00:00+02:00", "2026-06-15T00:00:00+00:00"),
        ("2026-07-11T20:25:57.99Z", "2026-07-11T20:25:57.990000+00:00"),
        ("2026-06-15T00:00:00.001000z", "2026-06-15T00:00:00.001000+00:00"),
        (
            "2026-06-15T00:00:00.123456789Z",
            "2026-06-15T00:00:00.123456789+00:00",
        ),
    ],
)
def test_parse_utc_timestamp_accepts_explicit_offset_spellings(
    value: str,
    expected: str,
) -> None:
    parsed = parse_utc_timestamp(value)

    assert parsed is not None
    assert parsed.isoformat() == expected


@pytest.mark.parametrize(
    "value",
    [
        "2026-06-15T00:00:00",
        "",
        "not-a-timestamp",
        "1787392363567",
        1787392363567,
    ],
)
def test_parse_utc_timestamp_rejects_noncanonical_values(value: object) -> None:
    assert parse_utc_timestamp(value) is None


def test_parse_utc_timestamp_series_keeps_mixed_precision() -> None:
    # A Series mixing whole-second and fractional-second ISO timestamps must
    # parse both; the default pd.to_datetime(..., errors="coerce") would coerce
    # the fractional row to NaT under pandas >= 2.0.
    values = pd.Series(
        [
            "2026-06-15T00:00:00+00:00",
            "2026-06-15T00:00:00.001000+00:00",
        ]
    )

    parsed = parse_utc_timestamp_series(values)

    assert parsed.notna().all()
    assert parsed.iloc[1] == pd.Timestamp("2026-06-15T00:00:00.001000+00:00")


def test_parse_utc_timestamp_series_matches_scalar_offset_policy() -> None:
    values = pd.Series(
        [
            "2026-06-15T02:00:00+02",
            "2026-06-15T02:00:00+0200",
            "2026-06-15T02:00:00+02:00",
            "2026-06-15T00:00:00Z",
            pd.NA,
        ],
        index=[10, 11, 12, 13, 14],
        name="observed_at_utc",
    )

    parsed = parse_utc_timestamp_series(values, errors="raise")

    assert parsed.index.tolist() == values.index.tolist()
    assert parsed.name == values.name
    assert parsed.iloc[:4].eq(pd.Timestamp("2026-06-15T00:00:00+00:00")).all()
    assert pd.isna(parsed.iloc[4])


def test_parse_utc_timestamp_series_coerces_invalid_to_nat() -> None:
    parsed = parse_utc_timestamp_series(
        pd.Series(["not-a-timestamp", "2026-06-15T00:00:00+00:00"])
    )

    assert pd.isna(parsed.iloc[0])
    assert parsed.iloc[1] == pd.Timestamp("2026-06-15T00:00:00+00:00")


@pytest.mark.parametrize(
    "value",
    ["2026-06-15T00:00:00", "1787392363567", 1787392363567],
)
def test_parse_utc_timestamp_series_rejects_noncanonical_values(value: object) -> None:
    parsed = parse_utc_timestamp_series(pd.Series([value]))

    assert pd.isna(parsed.iloc[0])


def test_parse_utc_timestamp_series_raise_identifies_bad_row() -> None:
    values = pd.Series(
        ["2026-06-15T00:00:00Z", "naive-or-bad"],
        index=["good", "bad"],
    )

    with pytest.raises(
        ValueError,
        match=r"observed_at_utc:.*index 'bad': 'naive-or-bad'",
    ):
        parse_utc_timestamp_series(
            values,
            errors="raise",
            field_name="observed_at_utc",
        )


def test_parse_utc_timestamp_series_raise_identifies_out_of_range_row() -> None:
    with pytest.raises(
        ValueError,
        match=r"observed_at_utc:.*supported pandas range.*index 7: "
        r"'9999-12-31T23:59:59Z'",
    ):
        parse_utc_timestamp_series(
            pd.Series(["9999-12-31T23:59:59Z"], index=[7]),
            errors="raise",
            field_name="observed_at_utc",
        )


@pytest.mark.parametrize("errors", ["ignore", "warn"])
def test_parse_utc_timestamp_series_rejects_unknown_error_policy(errors: str) -> None:
    with pytest.raises(ValueError, match="unsupported timestamp error policy"):
        parse_utc_timestamp_series([], errors=errors)  # type: ignore[arg-type]


def test_source_timestamp_requires_an_epoch_policy() -> None:
    assert (
        isoformat_source_timestamp(1_787_392_363_567, epoch_unit="milliseconds")
        == "2026-08-22T09:52:43.567000+00:00"
    )
    assert (
        isoformat_source_timestamp(1_787_392_363.567, epoch_unit="seconds")
        == "2026-08-22T09:52:43.567000+00:00"
    )
    assert (
        isoformat_source_timestamp("2026-08-22T11:52:43+02", epoch_unit="seconds")
        == "2026-08-22T09:52:43+00:00"
    )


def test_timestamp_seconds_rejects_unknown_unit() -> None:
    with pytest.raises(ValueError, match="unsupported epoch unit"):
        timestamp_seconds(1, unit="minutes")  # type: ignore[arg-type]
