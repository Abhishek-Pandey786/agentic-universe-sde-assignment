"""Pure input normalization helpers for Assignment A."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Iterable


IST = timezone(timedelta(hours=5, minutes=30), name="IST")
UTC = timezone.utc
_HEADER_SEPARATORS = re.compile(r"[_\s]+")
_DATE_ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_DATE_DAY_FIRST = re.compile(r"^(\d{2})[/-](\d{2})[/-](\d{4})$")
_DATE_MONTH_NAME = re.compile(r"^(\d{2})-([A-Za-z]{3})-(\d{4})$")
_DECIMAL_NUMBER = re.compile(r"^[+-]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?$")
_CURRENCY_PREFIX = re.compile(r"^(?:₹|rs\.?|inr)\s*", re.IGNORECASE)
_TIMESTAMP = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(\.\d+)?"
    r"(Z|[+-]\d{2}:\d{2})?$"
)
_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


class NormalizationError(ValueError):
    """Raised when an input value does not match its specified format."""


def normalize_header(header: str) -> str:
    """Lowercase and trim a CSV header, collapsing runs of spaces/underscores."""
    if not isinstance(header, str):
        raise TypeError("header must be a string")
    return _HEADER_SEPARATORS.sub(" ", header.strip().lower())


def normalize_phone(value: str) -> str:
    """Normalize an Indian mobile number to ``+91XXXXXXXXXX``."""
    if not isinstance(value, str):
        raise TypeError("phone must be a string")
    digits = re.sub(r"[^0-9]", "", value)
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) != 10 or digits[0] not in "6789":
        raise NormalizationError("invalid Indian mobile number")
    return "+91" + digits


def normalize_date(value: str) -> date:
    """Parse only the four specified date formats, rejecting impossible dates."""
    if not isinstance(value, str):
        raise TypeError("date must be a string")
    text = value.strip()
    match = _DATE_ISO.fullmatch(text)
    if match:
        year, month, day = map(int, match.groups())
        return _make_date(year, month, day)
    match = _DATE_DAY_FIRST.fullmatch(text)
    if match:
        day, month, year = map(int, match.groups())
        return _make_date(year, month, day)
    match = _DATE_MONTH_NAME.fullmatch(text)
    if match:
        day, month_name, year = match.groups()
        month = _MONTHS.get(month_name.lower())
        if month is None:
            raise NormalizationError("invalid date month")
        return _make_date(int(year), month, int(day))
    raise NormalizationError("date must use YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, or DD-Mon-YYYY")


def _make_date(year: int, month: int, day: int) -> date:
    try:
        return date(year, month, day)
    except ValueError as exc:
        raise NormalizationError("impossible calendar date") from exc


def normalize_decimal(value: str) -> Decimal:
    """Parse a nonnegative amount and round to two places using half-up rounding."""
    if not isinstance(value, str):
        raise TypeError("decimal value must be a string")
    text = value.strip()
    text = _CURRENCY_PREFIX.sub("", text, count=1).strip()
    if not _DECIMAL_NUMBER.fullmatch(text):
        raise NormalizationError("invalid decimal amount")
    try:
        amount = Decimal(text.replace(",", ""))
        if not amount.is_finite():
            raise NormalizationError("decimal amount must be finite")
        if amount < 0:
            raise NormalizationError("decimal amount must be nonnegative")
        return amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except InvalidOperation as exc:
        raise NormalizationError("invalid decimal amount") from exc


def normalize_enum(value: str, allowed_values: Iterable[str]) -> str:
    """Match an enum ignoring case and return its lowercase spelling."""
    if not isinstance(value, str):
        raise TypeError("enum value must be a string")
    text = value.strip()
    if not text:
        raise NormalizationError("enum value is empty")
    allowed = tuple(allowed_values)
    matches = [candidate for candidate in allowed if candidate.casefold() == text.casefold()]
    if not matches:
        raise NormalizationError("value is not in the allowed enum values")
    if len(matches) > 1:
        raise ValueError("allowed enum values are ambiguous ignoring case")
    return matches[0].lower()


def parse_timestamp(value: str) -> datetime:
    """Parse supported event timestamps and return an aware UTC datetime.

    Naive timestamps are interpreted as Indian Standard Time. Accepted values
    use ISO date and seconds, optional fractional seconds, and either ``T`` or
    a space between date and time.
    """
    if not isinstance(value, str):
        raise TypeError("timestamp must be a string")
    text = value.strip()
    match = _TIMESTAMP.fullmatch(text)
    if not match:
        raise NormalizationError("invalid event timestamp")
    date_part, time_part, fraction, offset = match.groups()
    iso = f"{date_part}T{time_part}{fraction or ''}"
    if offset == "Z":
        iso += "+00:00"
    elif offset:
        iso += offset
    try:
        parsed = datetime.fromisoformat(iso)
    except ValueError as exc:
        raise NormalizationError("invalid event timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=IST)
    return parsed.astimezone(UTC)
