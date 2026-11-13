"""Tests for pure Assignment A normalization functions."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from solution.au_pipeline.normalize import (
    IST,
    NormalizationError,
    normalize_date,
    normalize_decimal,
    normalize_enum,
    normalize_header,
    normalize_phone,
    parse_timestamp,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("9876543210", "+919876543210"),
        ("+91 98765 43210", "+919876543210"),
        ("91-98765-43210", "+919876543210"),
        ("09876543210", "+919876543210"),
        ("(080) 7654-3210", "+918076543210"),
    ],
)
def test_phone_normalization(raw, expected):
    assert normalize_phone(raw) == expected


@pytest.mark.parametrize("raw", ["123456789", "5123456789", "12345678901", "9191234567890", "N/A", ""])
def test_invalid_phones_are_rejected(raw):
    with pytest.raises(NormalizationError):
        normalize_phone(raw)


@pytest.mark.parametrize(
    "raw",
    ["2026-12-01", "01/12/2026", "01-12-2026", "01-Dec-2026", "01-dEc-2026"],
)
def test_all_supported_dates_are_day_first(raw):
    assert normalize_date(raw).isoformat() == "2026-12-01"


@pytest.mark.parametrize("raw", ["31/02/2026", "2026-13-01", "2026-02-29", "1/12/2026", "01-December-2026", "soon"])
def test_impossible_or_nonconforming_dates_are_rejected(raw):
    with pytest.raises(NormalizationError):
        normalize_date(raw)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1234", Decimal("1234.00")),
        ("₹ 12,345.678", Decimal("12345.68")),
        ("Rs 1,234.50", Decimal("1234.50")),
        ("Rs. 9", Decimal("9.00")),
        ("INR 0.005", Decimal("0.01")),
        ("1,234,567.891", Decimal("1234567.89")),
    ],
)
def test_currency_decimal_normalization(raw, expected):
    assert normalize_decimal(raw) == expected


@pytest.mark.parametrize("raw", ["-1", "₹-0.01", "12,34", "1,23,456", "twelve", "1.2.3", "1e3", ""])
def test_negative_and_malformed_decimals_are_rejected(raw):
    with pytest.raises(NormalizationError):
        normalize_decimal(raw)


def test_enum_is_case_insensitive_and_returns_lowercase():
    assert normalize_enum(" HI ", ["en", "hi", "ta"]) == "hi"
    with pytest.raises(NormalizationError):
        normalize_enum("Hindi", ["en", "hi", "ta"])


def test_header_normalization_collapses_underscores_and_whitespace():
    assert normalize_header("  Policy__  Expiry ") == "policy expiry"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-10-05T13:20:21Z", datetime(2026, 10, 5, 13, 20, 21, tzinfo=timezone.utc)),
        ("2026-10-05T18:50:21+05:30", datetime(2026, 10, 5, 13, 20, 21, tzinfo=timezone.utc)),
        ("2026-10-05 18:50:21", datetime(2026, 10, 5, 13, 20, 21, tzinfo=timezone.utc)),
    ],
)
def test_timestamp_parses_to_utc(raw, expected):
    parsed = parse_timestamp(raw)
    assert parsed == expected
    assert parsed.tzinfo is timezone.utc


def test_timestamp_naive_means_ist_and_invalid_values_fail():
    assert parse_timestamp("2026-10-03 10:01:00") == datetime(
        2026, 10, 3, 4, 31, tzinfo=timezone.utc
    )
    assert IST.utcoffset(None).total_seconds() == 19800
    with pytest.raises(NormalizationError):
        parse_timestamp("2026-02-30T10:00:00Z")
    with pytest.raises(NormalizationError):
        parse_timestamp("2026-10-03T10:00Z")
