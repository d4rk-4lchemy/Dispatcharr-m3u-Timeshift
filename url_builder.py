"""Build provider catch-up URLs for standard M3U timeshift streams."""

import re
from datetime import datetime, timezone
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit


def parse_start_to_utc_epoch(start_timestamp):
    """Return UTC epoch seconds for Dispatcharr/XC/ISO catch-up timestamps."""
    dt = _parse_datetime(start_timestamp)
    if dt is None:
        raise ValueError("Invalid timestamp")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp())


def build_m3u_catchup_url(live_url, start_timestamp, param_name="utc"):
    """Append the UTC epoch start query parameter to *live_url*."""
    if not live_url:
        raise ValueError("Missing live URL")
    param_name = str(param_name or "utc")
    epoch = parse_start_to_utc_epoch(start_timestamp)
    parsed = urlsplit(live_url)
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key != param_name
    ]
    query.append((param_name, str(epoch)))
    return urlunsplit(
        parsed._replace(
            query=urlencode(query, doseq=True, quote_via=quote, safe=""),
        )
    )


def _parse_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value

    raw = str(value).strip()
    if not raw:
        return None

    if raw.isdigit():
        try:
            number = int(raw)
        except ValueError:
            return None
        if len(raw) == 13:
            number = number / 1000
        elif len(raw) != 10:
            return None
        return datetime.fromtimestamp(number, tz=timezone.utc)

    if "T" in raw:
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None

    normalized = _normalize_xc_timestamp(raw)
    if normalized is None:
        return None
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _normalize_xc_timestamp(raw):
    match = re.match(
        r"^(?P<date>\d{4}-\d{2}-\d{2})[:_ ]"
        r"(?P<hour>\d{2})[-:](?P<minute>\d{2})"
        r"(?:[-:](?P<second>\d{2}))?$",
        raw,
    )
    if not match:
        return None

    second = match.group("second") or "00"
    return (
        f"{match.group('date')}T"
        f"{match.group('hour')}:{match.group('minute')}:{second}"
    )


def is_within_catchup_days(start_timestamp, catchup_days, now=None):
    """Return whether *start_timestamp* is inside the configured archive window."""
    try:
        days = int(catchup_days or 0)
    except (TypeError, ValueError):
        return False
    if days <= 0:
        return False
    dt = _parse_datetime(start_timestamp)
    if dt is None:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    age_seconds = (now - dt).total_seconds()
    return 0 <= age_seconds <= days * 86400
