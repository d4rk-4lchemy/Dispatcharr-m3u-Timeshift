import importlib
import sys
import types
from pathlib import Path

import pytest


def _hooks_module():
    """Load hooks without requiring the full Django runtime for pure unit tests."""
    if "django.http" not in sys.modules:
        django = types.ModuleType("django")
        http = types.ModuleType("django.http")

        class Response:
            def __init__(self, *args, **kwargs):
                pass

        http.HttpResponse = Response
        http.HttpResponseBadRequest = Response
        http.HttpResponseForbidden = Response
        http.StreamingHttpResponse = Response
        django.http = http
        sys.modules["django"] = django
        sys.modules["django.http"] = http

    package_name = "m3u_timeshift_test_plugin"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(Path(__file__).resolve().parents[1])]
        sys.modules[package_name] = package
    return importlib.import_module(f"{package_name}.hooks")


class _Pipeline:
    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def setex(self, key, _ttl, value):
        self.commands.append((key, value))
        return self

    def execute(self):
        self.redis.values.update(self.commands)


class _Redis:
    def __init__(self):
        self.values = {}

    def get(self, key):
        return self.values.get(key)

    def delete(self, key):
        self.values.pop(key, None)

    def pipeline(self, transaction=False):
        return _Pipeline(self)

    def hgetall(self, key):
        return self.values.get(key, {})


class _Request:
    def __init__(self, session_id=None):
        self.GET = {"session_id": session_id} if session_id else {}


class _User:
    def __init__(self, user_id):
        self.id = user_id


class _Channel:
    id = 99
    uuid = "channel-99"


class _TimeshiftKeys:
    @staticmethod
    def api_session(session_id):
        return f"timeshift:session:{session_id}"


class _TimeshiftViews:
    TimeshiftRedisKeys = _TimeshiftKeys


def test_plugin_session_cannot_be_reused_by_another_user():
    hooks = _hooks_module()
    redis = _Redis()
    minted = iter(("session-a", "session-b"))
    resolve = lambda request, user: hooks._resolve_plugin_session_id(
        request=request,
        user=user,
        channel=_Channel(),
        client_ip="192.0.2.1",
        client_user_agent="pytest",
        redis_client=redis,
        timeshift_views=object(),
        make_stats_channel_id=lambda channel_id, session_id: f"{channel_id}_{session_id}",
        mint_session_id=lambda: next(minted),
    )

    owner = _User(1)
    other_user = _User(2)
    session_id = resolve(_Request(), owner)

    assert session_id == "session-a"
    assert resolve(_Request("session-a"), owner) == "session-a"
    assert resolve(_Request("session-a"), other_user) == "session-b"


def test_dispatcharr_api_session_is_preserved_for_its_owner_and_channel():
    hooks = _hooks_module()
    redis = _Redis()
    redis.values["timeshift:session:native-session"] = {
        "user_id": "1",
        "channel_uuid": "channel-99",
        "start": "2026-07-18:12-00",
    }

    session_id = hooks._resolve_plugin_session_id(
        request=_Request("native-session"),
        user=_User(1),
        channel=_Channel(),
        client_ip="192.0.2.1",
        client_user_agent="pytest",
        redis_client=redis,
        timeshift_views=_TimeshiftViews,
        make_stats_channel_id=lambda channel_id, session_id: f"{channel_id}_{session_id}",
        mint_session_id=lambda: "should-not-be-used",
    )

    assert session_id == "native-session"


def test_closing_an_unstarted_hls_iterator_runs_cleanup():
    hooks = _hooks_module()
    cleaned = []

    def stream():
        yield b"data"

    wrapped = hooks._CleanupOnCloseStream(stream(), lambda: cleaned.append(True))
    wrapped.close()

    assert cleaned == [True]


def test_hls_reuse_signal_does_not_stop_its_replacement_generation():
    hooks = _hooks_module()
    redis = _Redis()
    client_id = "session-a"
    stop_key = "timeshift:stop"
    redis.values[hooks._hls_stream_generation_key(client_id)] = "2"
    redis.values[stop_key] = "reuse"

    assert not hooks._hls_stop_requested(
        redis, stop_key, client_id=client_id, stream_generation=2,
    )
    assert stop_key not in redis.values
    assert hooks._hls_stop_requested(
        redis, stop_key, client_id=client_id, stream_generation=1,
    )


def test_native_handoff_stops_an_active_plugin_stream(
    monkeypatch: pytest.MonkeyPatch,
):
    hooks = _hooks_module()

    class Redis:
        def __init__(self):
            self.values = {hooks._hls_active_virtual_channel_key("session-a"): "old-vid"}

        def get(self, key):
            return self.values.get(key)

        def setex(self, key, _ttl, value):
            self.values[key] = value

        def delete(self, key):
            self.values.pop(key, None)

    redis = Redis()
    closed = []

    class Keys:
        @staticmethod
        def client_stop(virtual_channel_id, client_id):
            return f"stop:{virtual_channel_id}:{client_id}"

    apps = types.ModuleType("apps")
    apps.__path__ = []
    proxy = types.ModuleType("apps.proxy")
    proxy.__path__ = []
    live_proxy = types.ModuleType("apps.proxy.live_proxy")
    live_proxy.__path__ = []
    live_utils = types.ModuleType("apps.proxy.live_proxy.utils")
    live_utils.get_client_ip = lambda _request: "192.0.2.1"
    timeshift = types.ModuleType("apps.timeshift")
    timeshift.__path__ = []
    views = types.ModuleType("apps.timeshift.views")
    views.TimeshiftRedisKeys = Keys
    views._close_active_upstream = lambda vid, sid: closed.append((vid, sid))
    core = types.ModuleType("core")
    core.__path__ = []
    core_utils = types.ModuleType("core.utils")
    core_utils.RedisClient = types.SimpleNamespace(get_client=lambda: redis)
    apps.proxy = proxy
    proxy.live_proxy = live_proxy
    live_proxy.utils = live_utils
    apps.timeshift = timeshift
    timeshift.views = views
    core.utils = core_utils
    for name, module in {
        "apps": apps,
        "apps.proxy": proxy,
        "apps.proxy.live_proxy": live_proxy,
        "apps.proxy.live_proxy.utils": live_utils,
        "apps.timeshift": timeshift,
        "apps.timeshift.views": views,
        "core": core,
        "core.utils": core_utils,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setattr(hooks, "_plugin_session_is_owned_by", lambda *_args: True)
    monkeypatch.setattr(
        hooks, "_is_dispatcharr_api_session_owned_by", lambda *_args: False,
    )

    request = types.SimpleNamespace(
        GET={"session_id": "session-a"}, META={"HTTP_USER_AGENT": "pytest"},
    )
    hooks._handoff_plugin_stream_to_native(request, _User(1), _Channel())

    assert redis.values["stop:old-vid:session-a"] == "hop"
    assert hooks._hls_active_virtual_channel_key("session-a") not in redis.values
    assert closed == [("old-vid", "session-a")]


def test_hls_heartbeat_refreshes_a_native_api_session(monkeypatch: pytest.MonkeyPatch):
    hooks = _hooks_module()
    refreshed = []

    class Pipeline:
        def hset(self, *_args, **_kwargs):
            return self

        def expire(self, *_args, **_kwargs):
            return self

        def hincrby(self, *_args, **_kwargs):
            return self

        def execute(self):
            return []

    class Redis:
        def pipeline(self, transaction=False):
            assert transaction is False
            return Pipeline()

    class Keys:
        @staticmethod
        def clients(channel_id):
            return f"clients:{channel_id}"

        @staticmethod
        def client_metadata(channel_id, client_id):
            return f"client:{channel_id}:{client_id}"

        @staticmethod
        def channel_metadata(channel_id):
            return f"metadata:{channel_id}"

    class TimeshiftViews:
        TimeshiftRedisKeys = Keys
        ChannelMetadataField = types.SimpleNamespace(TOTAL_BYTES="total_bytes")
        CLIENT_TTL_SECONDS = 60

    redis = Redis()
    monkeypatch.setattr(
        hooks,
        "_touch_native_catchup_session",
        lambda client, session_id: refreshed.append((client, session_id)),
    )
    hooks._heartbeat_hls_timeshift_stats(
        timeshift_views=TimeshiftViews,
        redis_client=redis,
        stats_channel_id="99_session-a",
        client_id="session-a",
        virtual_channel_id="99_programme",
        bytes_delta=1,
    )

    assert refreshed == [(redis, "session-a")]


def test_expired_xc_stream_does_not_hide_a_later_eligible_m3u_stream(
    monkeypatch: pytest.MonkeyPatch,
):
    hooks = _hooks_module()

    class Account:
        def __init__(self, account_type):
            self.account_type = account_type

    class Stream:
        def __init__(self, account, days, properties):
            self.m3u_account = account
            self.catchup_days = days
            self.custom_properties = properties

    class Streams:
        def __init__(self, streams):
            self._streams = streams

        def filter(self, **_kwargs):
            return self

        def order_by(self, *_args):
            return self

        def select_related(self, *_args):
            return self

        def __iter__(self):
            return iter(self._streams)

    class Channel:
        streams = Streams(
            [
                Stream(Account("XC"), 1, {}),
                Stream(Account("STD"), 3, {hooks.PLUGIN_MARKER: True}),
            ]
        )

    monkeypatch.setattr(
        hooks,
        "is_within_catchup_days",
        lambda _timestamp, days: int(days) >= 2,
    )

    selected = hooks._first_plugin_m3u_before_native_xc(Channel(), "requested")

    assert selected is Channel.streams._streams[1]


def test_catchup_wrapper_tries_m3u_sources_after_failed_m3u_and_xc(
    monkeypatch: pytest.MonkeyPatch,
):
    hooks = _hooks_module()
    first_m3u = object()
    xc_stream = object()
    second_m3u = object()
    calls = []

    monkeypatch.setattr(hooks, "_refresh_runtime_state", lambda: True)
    monkeypatch.setattr(
        hooks,
        "_ordered_plugin_catchup_candidates",
        lambda *_args: [("m3u", first_m3u), ("xc", xc_stream), ("m3u", second_m3u)],
    )
    monkeypatch.setattr(hooks, "_handoff_plugin_stream_to_native", lambda *_args: None)

    def serve_m3u(_request, _user, _channel, stream, _timestamp, **_kwargs):
        calls.append(("m3u", stream))
        return types.SimpleNamespace(status_code=404)

    def serve_native(_request, _user, candidate_channel, *_args, **_kwargs):
        calls.append(("xc", None))
        assert list(candidate_channel.streams) == [xc_stream]
        return types.SimpleNamespace(status_code=502)

    monkeypatch.setattr(hooks, "_serve_plugin_m3u_stream", serve_m3u)
    monkeypatch.setattr(hooks, "_call_original", serve_native)

    response = hooks._serve_catchup_wrapper(
        object(), object(), object(), "2026-07-18:12-00",
    )

    assert response.status_code == 404
    assert calls == [("m3u", first_m3u), ("xc", None), ("m3u", second_m3u)]


def test_profile_reservation_retries_after_handing_off_plugin_stream(
    monkeypatch: pytest.MonkeyPatch,
):
    hooks = _hooks_module()
    active_key = hooks._hls_active_virtual_channel_key("session-a")

    class Redis:
        def __init__(self):
            self.values = {active_key: "old-vid"}

        def get(self, key):
            return self.values.get(key)

        def setex(self, key, _ttl, value):
            self.values[key] = value

    class Keys:
        @staticmethod
        def client_stop(virtual_channel_id, client_id):
            return f"stop:{virtual_channel_id}:{client_id}"

    closed = []
    timeshift_views = types.SimpleNamespace(
        TimeshiftRedisKeys=Keys,
        _close_active_upstream=lambda vid, sid: closed.append((vid, sid)),
    )
    profile = object()
    attempts = iter(((None, "capacity_full"), (profile, "reserved")))
    monkeypatch.setattr(
        hooks, "_reserve_m3u_profile_slot", lambda *_args: next(attempts),
    )

    reserved, status = hooks._reserve_m3u_profile_slot_with_handoff(
        m3u_account=object(),
        redis_client=Redis(),
        reserve_profile_slot=object(),
        timeshift_views=timeshift_views,
        session_id="session-a",
    )

    assert reserved is profile
    assert status == "reserved"
    assert closed == [("old-vid", "session-a")]


def test_disable_restores_native_hook_and_install_unwraps_a_prior_reload(
    monkeypatch: pytest.MonkeyPatch,
):
    hooks = _hooks_module()

    def native(*_args, **_kwargs):
        return "native"

    def stale_wrapper(*_args, **_kwargs):
        return "stale"

    setattr(stale_wrapper, hooks._HOOK_MARKER, native)
    apps = types.ModuleType("apps")
    apps.__path__ = []
    timeshift = types.ModuleType("apps.timeshift")
    timeshift.__path__ = []
    views = types.ModuleType("apps.timeshift.views")
    views._serve_catchup = stale_wrapper
    apps.timeshift = timeshift
    timeshift.views = views
    monkeypatch.setitem(sys.modules, "apps", apps)
    monkeypatch.setitem(sys.modules, "apps.timeshift", timeshift)
    monkeypatch.setitem(sys.modules, "apps.timeshift.views", views)

    hooks._installed = False
    hooks._original_serve_catchup = None
    hooks.install()

    assert views._serve_catchup is hooks._serve_catchup_wrapper
    assert hooks._original_serve_catchup is native

    hooks.disable()

    assert views._serve_catchup is native
