import types

import pytest

from test_session_ownership import _hooks_module


@pytest.mark.parametrize("value", [None, "", "bad", 0, -1, "nan", "inf"])
def test_invalid_rate_uses_default(value):
    assert _hooks_module()._hls_archive_rate(value) == 5.0


@pytest.mark.parametrize("value", [1, 5, "2.5", 0.5])
def test_positive_rate_is_preserved(value):
    assert _hooks_module()._hls_archive_rate(value) == float(value)


@pytest.mark.parametrize("rate", [1, 5])
@pytest.mark.parametrize("content_length", [True, False])
@pytest.mark.parametrize("paced", [True, False])
@pytest.mark.parametrize("burst_seconds", [0, 7, 10, 15, 60])
def test_segment_delivery_timing(monkeypatch, rate, content_length, paced, burst_seconds):
    hooks = _hooks_module()
    clock = [100.0]
    monkeypatch.setattr(hooks.time, "time", lambda: clock[0])
    monkeypatch.setattr(hooks.time, "sleep", lambda delay: clock.__setitem__(0, clock[0] + delay))
    monkeypatch.setattr(hooks, "_heartbeat_hls_timeshift_stats", lambda **kwargs: None)
    requested_at = []
    closed = []

    class Response:
        status_code = 200
        headers = {"Content-Length": str(64 * 1024)} if content_length else {}

        def iter_content(self, chunk_size):
            payload = b"x" * (64 * 1024)
            for offset in range(0, len(payload), chunk_size):
                yield payload[offset:offset + chunk_size]

        def close(self):
            closed.append("response")

    class Session:
        def get(self, *args, **kwargs):
            requested_at.append(clock[0])
            return Response()

        def close(self):
            closed.append("session")

    monkeypatch.setattr(hooks.requests, "Session", Session)
    cleanup = []
    chunks = []
    for chunk in hooks._iter_hls_segments_as_ts(
        [{"url": "https://example.test/segment.ts", "duration": 10}] * 2,
        None,
        redis_client=None,
        timeshift_views=types.SimpleNamespace(),
        stats_channel_id="channel",
        client_id="client",
        virtual_channel_id="virtual",
        stream_generation="generation",
        pace_segments=paced,
        playback_rate=rate,
        burst_seconds=burst_seconds,
        reserved_profile_id=None,
        release_profile_slot=None,
        cleanup=cleanup.append,
    ):
        chunks.append((clock[0], chunk))

    def elapsed(media):
        if not paced:
            return 0
        fast = min(media, burst_seconds)
        return fast / rate + media - fast

    assert requested_at == pytest.approx([100, 100 + elapsed(10)])
    assert sum(len(chunk) for _, chunk in chunks) == 128 * 1024
    if paced and content_length:
        assert [at for at, _ in chunks] == pytest.approx(
            [100 + elapsed(media) for media in (0, 5, 10, 15)]
        )
    # Without Content-Length, only the gap before the next segment is paced.
    assert clock[0] == pytest.approx(100 + elapsed(20 if content_length else 10))
    assert closed == ["response", "response", "session"]
    assert len(cleanup) == 1


@pytest.mark.parametrize("value", [None, "", "bad", -1, "nan", "inf"])
def test_invalid_burst_uses_default(value):
    assert _hooks_module()._hls_archive_burst_seconds(value) == 60.0


@pytest.mark.parametrize("value", [0, 60, "120", 0.5])
def test_nonnegative_burst_is_preserved(value):
    assert _hooks_module()._hls_archive_burst_seconds(value) == float(value)


def test_new_request_with_same_session_gets_fresh_burst(monkeypatch):
    # Both calls use identical client/session identifiers, as after a seek.
    test_segment_delivery_timing(monkeypatch, 5, True, True, 7)
    test_segment_delivery_timing(monkeypatch, 5, True, True, 7)
