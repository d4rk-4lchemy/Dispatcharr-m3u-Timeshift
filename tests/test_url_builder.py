from datetime import datetime, timezone

import pytest

from url_builder import (
    build_m3u_catchup_url,
    is_within_catchup_days,
    parse_start_to_utc_epoch,
)


def test_build_url_without_query():
    assert build_m3u_catchup_url("http://example.test/live.ts", "2026-07-18:12-00") == (
        "http://example.test/live.ts?utc=1784376000"
    )


def test_build_url_with_query():
    assert build_m3u_catchup_url(
        "http://example.test/live.ts?token=abc", "2026-07-18T12:00:00Z"
    ) == "http://example.test/live.ts?token=abc&utc=1784376000"


def test_existing_parameter_is_replaced_and_fragment_is_preserved():
    assert build_m3u_catchup_url(
        "http://example.test/live.ts?utc=old&token=abc#player",
        "2026-07-18T12:00:00Z",
    ) == "http://example.test/live.ts?token=abc&utc=1784376000#player"


def test_custom_param_name():
    assert build_m3u_catchup_url(
        "http://example.test/live.ts", "2026-07-18 12:00:00", param_name="start time"
    ) == "http://example.test/live.ts?start%20time=1784376000"


def test_epoch_milliseconds():
    assert parse_start_to_utc_epoch("1784376000000") == 1784376000


def test_invalid_timestamp():
    with pytest.raises(ValueError):
        build_m3u_catchup_url("http://example.test/live.ts", "not-a-date")


def test_within_catchup_days():
    now = datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc)
    assert is_within_catchup_days("2026-07-18:12-00", 1, now=now)
    assert not is_within_catchup_days("2026-07-17:11-59", 1, now=now)
