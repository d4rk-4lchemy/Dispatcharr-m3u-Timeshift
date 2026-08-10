"""Runtime hook for Dispatcharr catch-up playback."""

import logging
import threading
import time

import requests
from django.http import (
    HttpResponse,
    HttpResponseBadRequest,
    HttpResponseForbidden,
    StreamingHttpResponse,
)

from .hls import parse_hls_playlist
from .scanner import PLUGIN_MARKER, as_json_object, parse_timeshift_days
from .url_builder import build_m3u_catchup_url, is_within_catchup_days

logger = logging.getLogger(__name__)

_lock = threading.RLock()
_installed = False
_enabled = True
_settings = {}
_original_serve_catchup = None
_PLUGIN_SESSION_TTL_SECONDS = 6 * 60 * 60
_PLUGIN_KEY_PREFIX = "m3u_timeshift"
_HOOK_MARKER = "_m3u_timeshift_hook_original"
_PROFILE_HANDOFF_RETRY_SECONDS = 2.0
_PROFILE_HANDOFF_RETRY_INTERVAL = 0.05


def install():
    """Patch Dispatcharr's private catch-up function once per process."""
    global _installed, _original_serve_catchup
    with _lock:
        if _installed:
            return
        try:
            from apps.timeshift import views
        except Exception:
            logger.exception("M3U timeshift hook install failed")
            return
        current = views._serve_catchup
        # A plugin reload creates a new module with fresh globals.  Without an
        # marker, that new module would regard the previous module's wrapper as
        # the native function and create an ever-growing wrapper chain.
        previous_original = getattr(current, _HOOK_MARKER, None)
        if previous_original is not None:
            current = previous_original
            views._serve_catchup = current
        _original_serve_catchup = current
        setattr(_serve_catchup_wrapper, _HOOK_MARKER, _original_serve_catchup)
        views._serve_catchup = _serve_catchup_wrapper
        _installed = True
        logger.info("M3U timeshift hook installed")


def set_runtime_settings(settings):
    global _settings, _enabled
    with _lock:
        _settings = dict(settings or {})
        _enabled = bool(_settings.get("enabled", True))


def disable():
    global _enabled, _installed
    with _lock:
        _enabled = False
        # Restore only when this module still owns the hook.  A newer plugin
        # reload may already have installed its own wrapper.
        try:
            from apps.timeshift import views

            if views._serve_catchup is _serve_catchup_wrapper:
                views._serve_catchup = _original_serve_catchup
        except Exception:
            logger.debug("M3U timeshift hook restore failed", exc_info=True)
        _installed = False


def _serve_catchup_wrapper(request, user, channel, timestamp, client_duration_hint=None):
    if not _refresh_runtime_state():
        _handoff_plugin_stream_to_native(request, user, channel)
        return _call_original(request, user, channel, timestamp, client_duration_hint)

    candidates = _ordered_plugin_catchup_candidates(channel, timestamp)
    if not candidates:
        _handoff_plugin_stream_to_native(request, user, channel)
        return _call_original(request, user, channel, timestamp, client_duration_hint)

    # Dispatcharr's native selector only knows how to serve XC sources. Limit
    # each call to the current XC stream so a later XC source cannot leapfrog
    # an intervening standard-M3U fallback in this ordered walk.
    last_response = None
    for source_type, stream in candidates:
        if source_type == "xc":
            _handoff_plugin_stream_to_native(request, user, channel)
            response = _call_original(
                request,
                user,
                _SingleCatchupStreamChannel(channel, stream),
                timestamp,
                client_duration_hint,
            )
        else:
            response = _serve_plugin_m3u_stream(
                request, user, channel, stream, timestamp,
                client_duration_hint=client_duration_hint,
            )

        if getattr(response, "status_code", 500) < 400:
            return response
        # A byte-range rejection is about the client request, not this source;
        # trying another provider would incorrectly restart the archive.
        if (
            getattr(response, "timeshift_passthrough", False)
            or getattr(response, "status_code", None) == 416
        ):
            return response
        last_response = response

    if last_response is not None:
        return last_response
    _handoff_plugin_stream_to_native(request, user, channel)
    return _call_original(request, user, channel, timestamp, client_duration_hint)


def _handoff_plugin_stream_to_native(request, user, channel):
    """Stop this viewer's plugin stream before native catch-up takes over.

    Plugin streams do not have Dispatcharr pool entries, so native pool
    preemption cannot discover them.  Without this handoff an HLS bridge can
    keep downloading (and retain its profile slot) after a stream-order change
    or after the plugin is disabled.
    """
    try:
        from dispatcharr.utils import get_client_ip
        from apps.timeshift import views as timeshift_views
        from core.utils import RedisClient

        redis_client = RedisClient.get_client()
    except Exception:
        logger.debug("M3U timeshift native handoff setup failed", exc_info=True)
        return
    if redis_client is None:
        return

    requested = request.GET.get("session_id")
    candidates = []
    if requested and (
        _plugin_session_is_owned_by(redis_client, requested, user)
        or _is_dispatcharr_api_session_owned_by(
            redis_client, timeshift_views, requested, user, channel
        )
    ):
        candidates.append(requested)

    # Native fallback requests can omit session_id.  The plugin's fingerprint
    # key is scoped to the authenticated user, channel, IP, and user agent, so
    # it lets us safely find that viewer's prior plugin-minted session.
    try:
        remembered = _decode_redis_value(
            redis_client.get(
                _plugin_session_fingerprint_key(
                    user=user,
                    channel=channel,
                    client_ip=get_client_ip(request),
                    client_user_agent=request.META.get("HTTP_USER_AGENT", "") or "",
                )
            )
        )
    except Exception:
        remembered = None
    if (
        remembered
        and remembered not in candidates
        and _plugin_session_is_owned_by(redis_client, remembered, user)
    ):
        candidates.append(remembered)

    for session_id in candidates:
        active_key = _hls_active_virtual_channel_key(session_id)
        try:
            virtual_channel_id = _decode_redis_value(redis_client.get(active_key))
            if not virtual_channel_id:
                continue
            stop_key = timeshift_views.TimeshiftRedisKeys.client_stop(
                virtual_channel_id, session_id,
            )
            # ``hop`` is understood by both this plugin and Dispatcharr's
            # direct TS iterator.  Unlike ``reuse``, it is terminal for the
            # old response rather than a signal intended for its replacement.
            redis_client.setex(stop_key, 60, "hop")
            redis_client.delete(active_key)
            close_upstream = getattr(timeshift_views, "_close_active_upstream", None)
            if close_upstream is not None:
                close_upstream(virtual_channel_id, session_id)
            logger.debug(
                "M3U timeshift handed off plugin session %s to native playback",
                session_id,
            )
        except Exception:
            logger.debug("M3U timeshift native handoff failed", exc_info=True)


def _refresh_runtime_state():
    global _settings, _enabled
    try:
        from apps.plugins.models import PluginConfig

        cfg = PluginConfig.objects.filter(
            key__in=("m3u-timeshift", "m3u_timeshift")
        ).first()
    except Exception:
        return _enabled
    if cfg is None:
        return _enabled
    with _lock:
        _settings = dict(cfg.settings or {})
        _enabled = bool(cfg.enabled and _settings.get("enabled", True))
        return _enabled


def _first_plugin_m3u_before_native_xc(channel, timestamp):
    """Return the first eligible plugin M3U stream before an eligible XC stream.

    An XC stream whose declared archive window cannot cover *timestamp* must
    not hide a later standard-M3U stream that can.  Dispatcharr's native
    selector similarly deprioritises expired streams before attempting them.
    """
    for source_type, stream in _ordered_plugin_catchup_candidates(channel, timestamp):
        if source_type == "xc":
            return None
        return stream
    return None


def _ordered_plugin_catchup_candidates(channel, timestamp):
    """Return eligible plugin M3U and native XC candidates in stream order.

    Both source types are retained in their configured order. Each XC stream
    is passed to Dispatcharr's native implementation through a single-stream
    channel facade, while standard-M3U streams are attempted directly.
    """
    try:
        catchup_streams = (
            channel.streams.filter(is_catchup=True, m3u_account__is_active=True)
            .order_by("channelstream__order")
            .select_related("m3u_account", "m3u_account__user_agent")
        )
    except Exception:
        logger.exception("M3U timeshift stream lookup failed")
        return []

    candidates = []
    for stream in catchup_streams:
        account = stream.m3u_account
        if account is None:
            continue
        if getattr(account, "account_type", None) == "XC":
            try:
                xc_days = int(stream.catchup_days or 0)
            except (TypeError, ValueError):
                xc_days = 0
            # Unknown archive depth preserves Dispatcharr's native-first
            # behaviour. A known-expired XC source must not hide a later M3U
            # source that can still serve the requested programme.
            if xc_days <= 0 or is_within_catchup_days(timestamp, xc_days):
                candidates.append(("xc", stream))
            continue
        if _is_plugin_m3u_catchup_stream(stream) and is_within_catchup_days(
            timestamp, stream.catchup_days
        ):
            candidates.append(("m3u", stream))
    return candidates


class _SingleCatchupStreamCollection:
    """Small queryset-shaped collection used to constrain native XC fallback."""

    def __init__(self, stream):
        self._stream = stream

    def filter(self, **_kwargs):
        return self

    def order_by(self, *_args):
        return self

    def select_related(self, *_args):
        return self

    def __iter__(self):
        return iter((self._stream,))


class _SingleCatchupStreamChannel:
    """Delegate a channel while exposing exactly one native catch-up stream."""

    def __init__(self, channel, stream):
        self._channel = channel
        self.streams = _SingleCatchupStreamCollection(stream)

    def __getattr__(self, name):
        return getattr(self._channel, name)


def _is_plugin_m3u_catchup_stream(stream):
    props = as_json_object(stream.custom_properties)
    if props.get(PLUGIN_MARKER) is True:
        return True
    return parse_timeshift_days(props.get("timeshift"), _settings.get("max_days_cap", 30)) > 0


def _serve_plugin_m3u_stream(
    request, user, channel, stream, timestamp, client_duration_hint=None,
):
    try:
        from apps.channels.utils import is_catchup_enabled
        from apps.m3u.connection_pool import release_profile_slot, reserve_profile_slot
        from apps.proxy.utils import check_user_stream_limits
        from apps.proxy.live_proxy.url_utils import transform_url
        from apps.timeshift import views as timeshift_views
        from apps.timeshift.helpers import resolve_catchup_duration
        from apps.timeshift.redis_keys import (
            mint_session_id,
            stats_channel_id as make_stats_channel_id,
            virtual_channel_id as make_virtual_channel_id,
        )
        from core.utils import RedisClient
        from dispatcharr.utils import get_client_ip, network_access_allowed
    except Exception:
        logger.exception("M3U timeshift imports failed")
        return HttpResponseBadRequest("Cannot build timeshift URL")

    if not is_catchup_enabled(user=user):
        return timeshift_views._finalize_timeshift_response(
            HttpResponseForbidden("Catch-up is disabled")
        )

    if not network_access_allowed(request, "STREAMS"):
        return timeshift_views._finalize_timeshift_response(
            HttpResponseForbidden("Access denied")
        )

    redis_client = RedisClient.get_client()
    client_ip = get_client_ip(request)
    client_user_agent = request.META.get("HTTP_USER_AGENT", "") or ""

    session_id = _resolve_plugin_session_id(
        request=request,
        user=user,
        channel=channel,
        client_ip=client_ip,
        client_user_agent=client_user_agent,
        redis_client=redis_client,
        timeshift_views=timeshift_views,
        make_stats_channel_id=make_stats_channel_id,
        mint_session_id=mint_session_id,
    )
    # A supplied ID can be rejected when it belongs to another user (or is not
    # one of this plugin's sessions). Redirect whenever the resolver selected
    # a replacement so the client continues with the safe ID.
    if request.GET.get("session_id") != session_id:
        return timeshift_views._finalize_timeshift_response(
            timeshift_views._redirect_with_session(request, session_id)
        )

    safe_ts = str(timestamp).replace(":", "-").replace("/", "-")
    media_id = f"{channel.id}_{safe_ts}"
    virtual_channel_id = make_virtual_channel_id(channel.id, safe_ts, stream.id)
    stats_channel_id = make_stats_channel_id(channel.id, session_id)
    # Keep this direct provider path subject to the same single-viewer and
    # programme-hop rules as Dispatcharr's native XC catch-up path.
    timeshift_views._terminate_previous_timeshift_sessions(
        redis_client, user, channel.id, media_id, session_id,
    )
    if not check_user_stream_limits(user, session_id, media_id=media_id):
        return timeshift_views._finalize_timeshift_response(
            HttpResponseForbidden("Stream limit exceeded")
        )
    _touch_hls_timeshift_session_request(
        redis_client=redis_client,
        timeshift_views=timeshift_views,
        stats_channel_id=stats_channel_id,
        client_id=session_id,
    )
    range_header = request.META.get("HTTP_RANGE")
    user_agent = _resolve_user_agent(stream)
    debug = bool(_settings.get("debug_mode", False))

    reserved_profile, reservation_status = _reserve_m3u_profile_slot_with_handoff(
        m3u_account=stream.m3u_account,
        redis_client=redis_client,
        reserve_profile_slot=reserve_profile_slot,
        timeshift_views=timeshift_views,
        session_id=session_id,
    )
    if reserved_profile is None and reservation_status == "capacity_full":
        return timeshift_views._finalize_timeshift_response(
            HttpResponse("No available stream slot", status=503)
        )
    if reserved_profile is None and reservation_status in {
        "no_active_profiles", "lookup_error",
    }:
        return timeshift_views._finalize_timeshift_response(
            HttpResponse("No active M3U profile", status=503)
        )

    live_url = stream.url
    if reserved_profile is not None:
        live_url = transform_url(
            live_url or "",
            reserved_profile.search_pattern,
            reserved_profile.replace_pattern,
        )

    try:
        upstream_url = build_m3u_catchup_url(
            live_url,
            timestamp,
            param_name=_settings.get("utc_param_name", "utc"),
        )
    except ValueError:
        _release_reserved_profile(
            reserved_profile, redis_client, release_profile_slot
        )
        return timeshift_views._finalize_timeshift_response(
            HttpResponseBadRequest("Invalid timestamp")
        )

    playlist = _load_hls_playlist(upstream_url, user_agent)
    if playlist["ok"]:
        duration_minutes = resolve_catchup_duration(
            channel, timestamp, client_hint=client_duration_hint,
        )
        response = _serve_hls_archive_as_ts(
            upstream_url=upstream_url,
            playlist=playlist,
            user_agent=user_agent,
            channel=channel,
            stream=stream,
            timestamp=timestamp,
            duration_minutes=duration_minutes,
            user=user,
            client_id=session_id,
            client_ip=client_ip,
            client_user_agent=client_user_agent,
            redis_client=redis_client,
            timeshift_views=timeshift_views,
            virtual_channel_id=virtual_channel_id,
            stats_channel_id=stats_channel_id,
            reserved_profile=reserved_profile,
            release_profile_slot=release_profile_slot,
            range_header=range_header,
        )
        return timeshift_views._finalize_timeshift_response(response)

    # Native XC playback uses its pool to replace an earlier programme for the
    # same session. Standard M3U playback has no such pool, so claim the
    # active virtual channel explicitly before starting the direct TS stream.
    stream_generation = _begin_plugin_stream_generation(
        redis_client=redis_client,
        timeshift_views=timeshift_views,
        client_id=session_id,
        virtual_channel_id=virtual_channel_id,
    )

    def release_profile(**_kwargs):
        _clear_plugin_stream_generation(
            redis_client=redis_client,
            client_id=session_id,
            virtual_channel_id=virtual_channel_id,
            stream_generation=stream_generation,
        )
        _release_reserved_profile(
            reserved_profile, redis_client, release_profile_slot
        )

    try:
        response = timeshift_views._stream_from_provider(
            candidate_urls=[upstream_url],
            user_agent=user_agent,
            range_header=range_header,
            virtual_channel_id=virtual_channel_id,
            stats_channel_id=stats_channel_id,
            client_id=session_id,
            client_ip=client_ip,
            client_user_agent=client_user_agent,
            user=user,
            channel_display_name=channel.name,
            timestamp_utc=timestamp,
            channel_logo_id=getattr(channel, "logo_id", None),
            m3u_profile_id=getattr(reserved_profile, "id", None),
            debug=debug,
            account_id=getattr(stream, "m3u_account_id", None),
            redis_client=redis_client,
            release_cb=release_profile,
            pool_session_id=session_id,
            channel_id=channel.id,
            channel_uuid=channel.uuid,
            stats_stream_id=stream.id,
            stream_stats=stream.stream_stats,
            duration_minutes=None,
        )
    except Exception:
        release_profile()
        logger.exception("M3U timeshift Dispatcharr stream handoff failed")
        return timeshift_views._finalize_timeshift_response(
            HttpResponse("Dispatcharr stream handoff failed", status=502)
        )

    if response.status_code >= 400:
        release_profile()
    return response


def _serve_hls_archive_as_ts(
    *,
    upstream_url,
    playlist,
    user_agent,
    channel,
    stream,
    timestamp,
    duration_minutes,
    user,
    client_id,
    client_ip,
    client_user_agent,
    redis_client,
    timeshift_views,
    virtual_channel_id,
    stats_channel_id,
    reserved_profile,
    release_profile_slot,
    range_header,
):
    # Bridged HLS is a virtual, concatenated TS response. It has no stable
    # byte map without downloading every segment first, so a non-zero range
    # must not be answered from the start of the archive.
    if range_header and range_header.strip().lower() != "bytes=0-":
        _release_reserved_profile(
            reserved_profile, redis_client, release_profile_slot
        )
        response = HttpResponse("HLS catch-up byte seeking is not supported", status=416)
        response["Accept-Ranges"] = "none"
        response["Content-Range"] = "bytes */*"
        return response

    if playlist["encrypted"]:
        _release_reserved_profile(
            reserved_profile, redis_client, release_profile_slot
        )
        return HttpResponse("Encrypted HLS catch-up is not supported", status=502)

    if playlist["fragmented_mp4"]:
        _release_reserved_profile(
            reserved_profile, redis_client, release_profile_slot
        )
        return HttpResponse("Fragmented MP4 HLS catch-up is not supported", status=502)

    if playlist["byterange"]:
        _release_reserved_profile(
            reserved_profile, redis_client, release_profile_slot
        )
        return HttpResponse("Byte-range HLS catch-up is not supported", status=502)

    segments = _select_hls_segments(
        playlist["segments"], max_seconds=float(duration_minutes or 0) * 60.0,
    )
    if not segments:
        _release_reserved_profile(
            reserved_profile, redis_client, release_profile_slot
        )
        return HttpResponse("HLS playlist has no playable segments", status=404)

    stream_generation = _begin_plugin_stream_generation(
        redis_client=redis_client,
        timeshift_views=timeshift_views,
        client_id=client_id,
        virtual_channel_id=virtual_channel_id,
    )

    _register_hls_timeshift_stats(
        timeshift_views=timeshift_views,
        redis_client=redis_client,
        stats_channel_id=stats_channel_id,
        client_id=client_id,
        client_ip=client_ip,
        client_user_agent=client_user_agent,
        user=user,
        channel=channel,
        stream=stream,
        timestamp=timestamp,
        upstream_url=upstream_url,
        virtual_channel_id=virtual_channel_id,
        m3u_profile_id=getattr(reserved_profile, "id", None),
        duration_minutes=duration_minutes,
    )

    cleanup_state = {"done": False}

    def cleanup_hls_stream(bytes_delta=0):
        _cleanup_hls_archive_stream(
            bytes_delta=bytes_delta,
            cleanup_state=cleanup_state,
            redis_client=redis_client,
            timeshift_views=timeshift_views,
            stats_channel_id=stats_channel_id,
            client_id=client_id,
            virtual_channel_id=virtual_channel_id,
            stream_generation=stream_generation,
            reserved_profile_id=getattr(reserved_profile, "id", None),
            release_profile_slot=release_profile_slot,
        )

    stream_iter = _CleanupOnCloseStream(
        _iter_hls_segments_as_ts(
            segments,
            user_agent,
            redis_client=redis_client,
            timeshift_views=timeshift_views,
            stats_channel_id=stats_channel_id,
            client_id=client_id,
            virtual_channel_id=virtual_channel_id,
            stream_generation=stream_generation,
            pace_segments=bool(_settings.get("pace_hls_archive", True)),
            reserved_profile_id=getattr(reserved_profile, "id", None),
            release_profile_slot=release_profile_slot,
            cleanup=cleanup_hls_stream,
        ),
        lambda: cleanup_hls_stream(),
    )
    response = StreamingHttpResponse(
        stream_iter,
        content_type="video/mp2t",
        status=200,
    )
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    response["Accept-Ranges"] = "none"
    return response


class _CleanupOnCloseStream:
    """Ensure cleanup runs even when WSGI closes an unstarted generator."""

    def __init__(self, generator, cleanup):
        self._generator = generator
        self._cleanup = cleanup

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._generator)

    def close(self):
        try:
            self._generator.close()
        finally:
            self._cleanup()


def _reserve_m3u_profile_slot(m3u_account, redis_client, reserve_profile_slot):
    if m3u_account is None:
        return None, "unavailable"
    try:
        profiles = list(m3u_account.profiles.filter(is_active=True))
    except Exception:
        logger.exception("M3U timeshift profile lookup failed")
        return None, "lookup_error"
    if not profiles:
        return None, "no_active_profiles"
    default_profile = next((profile for profile in profiles if profile.is_default), None)
    ordered_profiles = ([default_profile] if default_profile else []) + [
        profile for profile in profiles if profile is not default_profile
    ]
    if redis_client is None:
        return ordered_profiles[0], "untracked"
    for profile in ordered_profiles:
        reserved, _count, reason = reserve_profile_slot(profile, redis_client)
        if reserved:
            return profile, "reserved"
        logger.info(
            "M3U timeshift profile %s unavailable: %s",
            getattr(profile, "id", None),
            reason or "capacity",
        )
    return None, "capacity_full"


def _reserve_m3u_profile_slot_with_handoff(
    *,
    m3u_account,
    redis_client,
    reserve_profile_slot,
    timeshift_views,
    session_id,
):
    """Reserve a profile, reclaiming this session's prior plugin stream first.

    Plugin streams intentionally do not create Dispatcharr pool entries, so a
    reconnect cannot use the native pool handoff.  With a one-stream profile,
    reserving before stopping the old response always returns capacity-full.
    Signal and close the old response, let its normal cleanup release the
    Dispatcharr profile counter, then retry the atomic reservation briefly.
    """
    reserved_profile, status = _reserve_m3u_profile_slot(
        m3u_account, redis_client, reserve_profile_slot,
    )
    if reserved_profile is not None or status != "capacity_full":
        return reserved_profile, status
    if not _handoff_plugin_stream_for_profile_retry(
        redis_client=redis_client,
        timeshift_views=timeshift_views,
        session_id=session_id,
    ):
        return reserved_profile, status

    deadline = time.monotonic() + _PROFILE_HANDOFF_RETRY_SECONDS
    while True:
        reserved_profile, status = _reserve_m3u_profile_slot(
            m3u_account, redis_client, reserve_profile_slot,
        )
        if reserved_profile is not None or status != "capacity_full":
            return reserved_profile, status
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return reserved_profile, status
        time.sleep(min(_PROFILE_HANDOFF_RETRY_INTERVAL, remaining))


def _handoff_plugin_stream_for_profile_retry(*, redis_client, timeshift_views, session_id):
    """Stop this plugin session so its normal cleanup can release its slot."""
    if redis_client is None or not session_id:
        return False
    active_key = _hls_active_virtual_channel_key(session_id)
    try:
        virtual_channel_id = _decode_redis_value(redis_client.get(active_key))
        if not virtual_channel_id:
            return False
        stop_key = timeshift_views.TimeshiftRedisKeys.client_stop(
            virtual_channel_id, session_id,
        )
        # ``hop`` is terminal for both the HLS bridge and Dispatcharr's direct
        # TS iterator.  ``reuse`` would be consumed by the still-current HLS
        # generation before a replacement generation has been installed.
        redis_client.setex(stop_key, 60, "hop")
        close_upstream = getattr(timeshift_views, "_close_active_upstream", None)
        if close_upstream is not None:
            close_upstream(virtual_channel_id, session_id)
        logger.debug(
            "M3U timeshift releasing active plugin session %s before profile retry",
            session_id,
        )
        return True
    except Exception:
        logger.debug("M3U timeshift profile handoff failed", exc_info=True)
        return False


def _release_reserved_profile(reserved_profile, redis_client, release_profile_slot):
    if reserved_profile is None or redis_client is None:
        return
    try:
        release_profile_slot(reserved_profile.id, redis_client)
    except Exception:
        logger.exception("M3U timeshift profile slot release failed")


def _register_hls_timeshift_stats(
    *,
    timeshift_views,
    redis_client,
    stats_channel_id,
    client_id,
    client_ip,
    client_user_agent,
    user,
    channel,
    stream,
    timestamp,
    upstream_url,
    virtual_channel_id,
    m3u_profile_id,
    duration_minutes,
):
    if redis_client is None:
        return
    try:
        keys = timeshift_views.TimeshiftRedisKeys
        fields = timeshift_views.ChannelMetadataField
        ttl = timeshift_views.CLIENT_TTL_SECONDS
        now = str(time.time())
        client_set_key = keys.clients(stats_channel_id)
        client_key = keys.client_metadata(stats_channel_id, client_id)
        metadata_key = keys.channel_metadata(stats_channel_id)
        _cancel_hls_stats_disconnect_grace(
            timeshift_views=timeshift_views,
            redis_client=redis_client,
            stats_channel_id=stats_channel_id,
            client_id=client_id,
        )
        try:
            existing_connected_at = redis_client.hget(client_key, "connected_at")
            existing_init_time = redis_client.hget(metadata_key, fields.INIT_TIME)
        except Exception:
            existing_connected_at = None
            existing_init_time = None
        existing_connected_at = _decode_redis_value(existing_connected_at)
        existing_init_time = _decode_redis_value(existing_init_time)

        client_payload = {
            "user_agent": client_user_agent or "unknown",
            "ip_address": client_ip,
            "connected_at": existing_connected_at or now,
            "last_active": now,
            "user_id": str(user.id) if user is not None else "0",
            "username": user.username if user is not None else "unknown",
            "programme_start": str(timestamp),
            "position_anchor_at": now,
            "programme_vid": virtual_channel_id,
        }
        metadata_payload = {
            fields.STATE: timeshift_views.ChannelState.ACTIVE,
            fields.INIT_TIME: existing_init_time or now,
            fields.CHANNEL_ID: str(channel.id),
            fields.CHANNEL_UUID: str(channel.uuid),
            fields.CHANNEL_NAME: channel.name or "Catch-up",
            fields.STREAM_NAME: f"Catch-up @ {timestamp} UTC",
            fields.URL: _redact_url(upstream_url),
            fields.STREAM_ID: str(stream.id),
            fields.STREAM_TYPE: "hls-timeshift",
        }
        if getattr(channel, "logo_id", None) is not None:
            metadata_payload[fields.LOGO_ID] = str(channel.logo_id)
        if m3u_profile_id is not None:
            metadata_payload[fields.M3U_PROFILE] = str(m3u_profile_id)

        # Keep HLS catch-up consistent with Dispatcharr's native XC/TS path:
        # seed the session metadata from the Stream.stream_stats JSON. This
        # uses stats collected during earlier playback and/or M3U attributes
        # merged by scanner.py; it does not probe the upstream stream.
        try:
            from apps.timeshift.stats import seed_stream_stats_metadata

            seed_stream_stats_metadata(
                redis_client,
                metadata_key,
                metadata_payload,
                stats_stream_id=stream.id,
                stream_stats=stream.stream_stats,
            )
        except Exception:
            logger.debug("M3U HLS stream stats seeding failed", exc_info=True)

        pipe = redis_client.pipeline(transaction=False)
        pipe.hset(client_key, mapping=client_payload)
        pipe.expire(client_key, ttl)
        pipe.sadd(client_set_key, client_id)
        pipe.expire(client_set_key, ttl)
        pipe.hset(metadata_key, mapping=metadata_payload)
        pipe.expire(metadata_key, ttl)
        pipe.execute()
        try:
            timeshift_views._trigger_timeshift_stats_update(redis_client)
        except Exception:
            logger.debug("M3U timeshift stats broadcast failed", exc_info=True)
    except Exception:
        logger.exception("M3U timeshift stats registration failed")


_HLS_PROBE_BYTES = 16 * 1024
_HLS_PLAYLIST_MAX_BYTES = 2 * 1024 * 1024
_HLS_MASTER_MAX_DEPTH = 3


def _load_hls_playlist(url, user_agent):
    """Load a bounded HLS media playlist, following master variants.

    Non-HLS responses are deliberately not read beyond a small probe so a
    catch-up MPEG-TS archive can be handed to Dispatcharr's streaming path.
    """
    current_url = url
    for _depth in range(_HLS_MASTER_MAX_DEPTH + 1):
        document = _load_hls_document(current_url, user_agent)
        if not document["ok"]:
            return document
        playlist = parse_hls_playlist(document["text"], document["url"])
        if not playlist["master"]:
            return playlist
        if not playlist["variants"]:
            return {"ok": False, "error": "HLS master playlist has no variants"}
        variant = max(playlist["variants"], key=lambda item: item["bandwidth"])
        current_url = variant["url"]
    return {"ok": False, "error": "HLS master playlist nesting is too deep"}


def _load_hls_document(url, user_agent):
    headers = {"Accept-Encoding": "identity"}
    if user_agent:
        headers["User-Agent"] = user_agent
    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=(10, 20),
            allow_redirects=True,
            stream=True,
        )
    except requests.RequestException as exc:
        return {"ok": False, "error": str(exc), "error_type": type(exc).__name__}

    try:
        if response.status_code != 200:
            return {
                "ok": False,
                "http_status": response.status_code,
                "content_type": response.headers.get("Content-Type"),
            }
        chunks = []
        size = 0
        is_hls = None
        for chunk in response.iter_content(chunk_size=_HLS_PROBE_BYTES):
            if not chunk:
                continue
            chunks.append(chunk)
            size += len(chunk)
            if is_hls is None:
                probe = b"".join(chunks).lstrip(b"\xef\xbb\xbf\r\n \t")
                is_hls = probe.startswith(b"#EXTM3U")
                if not is_hls:
                    return {"ok": False, "not_hls": True}
            if size > _HLS_PLAYLIST_MAX_BYTES:
                return {"ok": False, "error": "HLS playlist is too large"}
        if not chunks:
            return {"ok": False, "not_hls": True}
        try:
            text = b"".join(chunks).decode(response.encoding or "utf-8")
        except UnicodeDecodeError:
            return {"ok": False, "not_hls": True}
        return {"ok": True, "text": text, "url": getattr(response, "url", url)}
    finally:
        response.close()


def _select_hls_segments(segments, max_seconds):
    if max_seconds <= 0:
        return list(segments)
    selected = []
    total = 0.0
    for segment in segments:
        selected.append(segment)
        total += float(segment.get("duration") or 0.0)
        if total >= max_seconds:
            break
    return selected


def _iter_hls_segments_as_ts(
    segments,
    user_agent,
    *,
    redis_client,
    timeshift_views,
    stats_channel_id,
    client_id,
    virtual_channel_id,
    stream_generation,
    pace_segments,
    reserved_profile_id,
    release_profile_slot,
    cleanup,
):
    session = requests.Session()
    headers = {"Accept-Encoding": "identity"}
    if user_agent:
        headers["User-Agent"] = user_agent
    stop_key = None
    if redis_client is not None:
        try:
            stop_key = timeshift_views.TimeshiftRedisKeys.client_stop(
                virtual_channel_id, client_id
            )
        except Exception:
            stop_key = None
    bytes_since_heartbeat = 0
    last_heartbeat = time.time()
    try:
        for index, segment in enumerate(segments):
            if _hls_stop_requested(
                redis_client,
                stop_key,
                client_id=client_id,
                stream_generation=stream_generation,
            ):
                break
            segment_started_at = time.time()
            url = segment["url"]
            try:
                response = session.get(
                    url,
                    headers=headers,
                    stream=True,
                    timeout=(10, 30),
                    allow_redirects=True,
                )
            except requests.RequestException as exc:
                logger.debug(
                    "M3U timeshift segment request failed at index %s: %s",
                    index, type(exc).__name__,
                )
                break
            try:
                if response.status_code not in (200, 206):
                    logger.debug(
                        "M3U timeshift segment HTTP status %s at index %s",
                        response.status_code, index,
                    )
                    break
                segment_duration = float(segment.get("duration") or 0.0)
                segment_content_length = _safe_int(response.headers.get("Content-Length"))
                segment_bytes_sent = 0
                chunk_size = 32 * 1024 if pace_segments else 256 * 1024
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        yield chunk
                        segment_bytes_sent += len(chunk)
                        bytes_since_heartbeat += len(chunk)
                        now = time.time()
                        if now - last_heartbeat >= 5:
                            _heartbeat_hls_timeshift_stats(
                                timeshift_views=timeshift_views,
                                redis_client=redis_client,
                                stats_channel_id=stats_channel_id,
                                client_id=client_id,
                                virtual_channel_id=virtual_channel_id,
                                bytes_delta=bytes_since_heartbeat,
                            )
                            bytes_since_heartbeat = 0
                            last_heartbeat = now
                        if (
                            pace_segments
                            and segment_duration > 0
                            and segment_content_length
                            and segment_content_length > 0
                        ):
                            target_elapsed = min(
                                segment_duration,
                                segment_duration
                                * (segment_bytes_sent / float(segment_content_length)),
                            )
                            stopped, last_heartbeat, bytes_since_heartbeat = (
                                _pace_hls_until(
                                    deadline=segment_started_at + target_elapsed,
                                    pending_bytes=bytes_since_heartbeat,
                                    redis_client=redis_client,
                                    stop_key=stop_key,
                                    timeshift_views=timeshift_views,
                                    stats_channel_id=stats_channel_id,
                                    client_id=client_id,
                                    virtual_channel_id=virtual_channel_id,
                                    stream_generation=stream_generation,
                                    last_heartbeat=last_heartbeat,
                                )
                            )
                            if stopped:
                                return
                    if _hls_stop_requested(
                        redis_client,
                        stop_key,
                        client_id=client_id,
                        stream_generation=stream_generation,
                    ):
                        return
            finally:
                response.close()
            if (
                pace_segments
                and index < len(segments) - 1
                and not segment_content_length
            ):
                stopped, last_heartbeat, bytes_since_heartbeat = _pace_hls_segment(
                    segment_duration=float(segment.get("duration") or 0.0),
                    segment_started_at=segment_started_at,
                    pending_bytes=bytes_since_heartbeat,
                    redis_client=redis_client,
                    stop_key=stop_key,
                    timeshift_views=timeshift_views,
                    stats_channel_id=stats_channel_id,
                    client_id=client_id,
                    virtual_channel_id=virtual_channel_id,
                    stream_generation=stream_generation,
                    last_heartbeat=last_heartbeat,
                )
                if stopped:
                    break
    finally:
        cleanup(bytes_since_heartbeat)
        session.close()


def _cleanup_hls_archive_stream(
    *,
    bytes_delta,
    cleanup_state,
    redis_client,
    timeshift_views,
    stats_channel_id,
    client_id,
    virtual_channel_id,
    stream_generation,
    reserved_profile_id,
    release_profile_slot,
):
    """Finish one HLS response exactly once, including an unstarted response."""
    if cleanup_state["done"]:
        return
    cleanup_state["done"] = True
    if bytes_delta > 0:
        _heartbeat_hls_timeshift_stats(
            timeshift_views=timeshift_views,
            redis_client=redis_client,
            stats_channel_id=stats_channel_id,
            client_id=client_id,
            virtual_channel_id=virtual_channel_id,
            bytes_delta=bytes_delta,
        )
    if _is_current_plugin_stream_generation(
        redis_client=redis_client,
        client_id=client_id,
        stream_generation=stream_generation,
    ):
        _schedule_hls_stats_disconnect_grace(
            timeshift_views=timeshift_views,
            redis_client=redis_client,
            stats_channel_id=stats_channel_id,
            client_id=client_id,
        )
    else:
        logger.debug("M3U timeshift stats unregister skipped for superseded stream")
    _clear_plugin_stream_generation(
        redis_client=redis_client,
        client_id=client_id,
        virtual_channel_id=virtual_channel_id,
        stream_generation=stream_generation,
    )
    if reserved_profile_id is not None and redis_client is not None:
        try:
            release_profile_slot(reserved_profile_id, redis_client)
        except Exception:
            logger.exception("M3U timeshift profile slot release failed")


def _pace_hls_segment(
    *,
    segment_duration,
    segment_started_at,
    pending_bytes,
    redis_client,
    stop_key,
    timeshift_views,
    stats_channel_id,
    client_id,
    virtual_channel_id,
    stream_generation,
    last_heartbeat,
):
    remaining = float(segment_duration or 0.0) - (time.time() - segment_started_at)
    return _pace_hls_until(
        deadline=time.time() + max(remaining, 0.0),
        pending_bytes=pending_bytes,
        redis_client=redis_client,
        stop_key=stop_key,
        timeshift_views=timeshift_views,
        stats_channel_id=stats_channel_id,
        client_id=client_id,
        virtual_channel_id=virtual_channel_id,
        stream_generation=stream_generation,
        last_heartbeat=last_heartbeat,
    )


def _pace_hls_until(
    *,
    deadline,
    pending_bytes,
    redis_client,
    stop_key,
    timeshift_views,
    stats_channel_id,
    client_id,
    virtual_channel_id,
    stream_generation,
    last_heartbeat,
):
    heartbeat_at = last_heartbeat
    while time.time() < deadline:
        if _hls_stop_requested(
            redis_client,
            stop_key,
            client_id=client_id,
            stream_generation=stream_generation,
        ):
            if pending_bytes > 0:
                _heartbeat_hls_timeshift_stats(
                    timeshift_views=timeshift_views,
                    redis_client=redis_client,
                    stats_channel_id=stats_channel_id,
                    client_id=client_id,
                    virtual_channel_id=virtual_channel_id,
                    bytes_delta=pending_bytes,
                )
                pending_bytes = 0
            return True, heartbeat_at, pending_bytes
        now = time.time()
        if now - heartbeat_at >= 5:
            _heartbeat_hls_timeshift_stats(
                timeshift_views=timeshift_views,
                redis_client=redis_client,
                stats_channel_id=stats_channel_id,
                client_id=client_id,
                virtual_channel_id=virtual_channel_id,
                bytes_delta=pending_bytes,
            )
            pending_bytes = 0
            heartbeat_at = now
        time.sleep(min(0.25, max(0.0, deadline - now)))
    return False, heartbeat_at, pending_bytes


def _heartbeat_hls_timeshift_stats(
    *,
    timeshift_views,
    redis_client,
    stats_channel_id,
    client_id,
    virtual_channel_id,
    bytes_delta,
):
    if redis_client is None:
        return
    try:
        keys = timeshift_views.TimeshiftRedisKeys
        client_set_key = keys.clients(stats_channel_id)
        client_key = keys.client_metadata(stats_channel_id, client_id)
        metadata_key = keys.channel_metadata(stats_channel_id)
        pipe = redis_client.pipeline(transaction=False)
        pipe.hset(client_key, "last_active", str(time.time()))
        pipe.expire(client_key, timeshift_views.CLIENT_TTL_SECONDS)
        pipe.expire(client_set_key, timeshift_views.CLIENT_TTL_SECONDS)
        if bytes_delta > 0:
            pipe.hincrby(
                metadata_key,
                timeshift_views.ChannelMetadataField.TOTAL_BYTES,
                bytes_delta,
            )
        pipe.expire(metadata_key, timeshift_views.CLIENT_TTL_SECONDS)
        pipe.expire(_hls_active_virtual_channel_key(client_id), _PLUGIN_SESSION_TTL_SECONDS)
        pipe.expire(_hls_stream_generation_key(client_id), _PLUGIN_SESSION_TTL_SECONDS)
        pipe.execute()
        _touch_native_catchup_session(redis_client, client_id)
    except Exception:
        logger.debug("M3U timeshift stats heartbeat failed", exc_info=True)


def _unregister_hls_timeshift_stats(
    *, timeshift_views, redis_client, stats_channel_id, client_id,
):
    if redis_client is None:
        return
    try:
        timeshift_views._unregister_stats_client(redis_client, stats_channel_id, client_id)
        timeshift_views._trigger_timeshift_stats_update(redis_client)
    except Exception:
        logger.debug("M3U timeshift stats unregister failed", exc_info=True)


def _touch_native_catchup_session(redis_client, session_id):
    """Extend a native API session's sliding TTL while HLS data is flowing.

    Plugin-minted session IDs have no corresponding native record, so
    ``touch_catchup_session`` is intentionally a no-op for them.
    """
    if redis_client is None or not session_id:
        return
    try:
        from apps.timeshift.sessions import touch_catchup_session

        touch_catchup_session(session_id, redis_client=redis_client)
    except Exception:
        logger.debug("M3U HLS native session refresh failed", exc_info=True)


def _schedule_hls_stats_disconnect_grace(
    *, timeshift_views, redis_client, stats_channel_id, client_id,
):
    if redis_client is None:
        return
    scheduler = getattr(timeshift_views, "_schedule_stats_disconnect_grace", None)
    if scheduler is None:
        _unregister_hls_timeshift_stats(
            timeshift_views=timeshift_views,
            redis_client=redis_client,
            stats_channel_id=stats_channel_id,
            client_id=client_id,
        )
        return
    try:
        scheduler(redis_client, stats_channel_id, client_id)
    except Exception:
        logger.debug("M3U timeshift stats grace scheduling failed", exc_info=True)
        _unregister_hls_timeshift_stats(
            timeshift_views=timeshift_views,
            redis_client=redis_client,
            stats_channel_id=stats_channel_id,
            client_id=client_id,
        )


def _cancel_hls_stats_disconnect_grace(
    *, timeshift_views, redis_client, stats_channel_id, client_id,
):
    if redis_client is None:
        return
    cancel = getattr(timeshift_views, "_cancel_stats_disconnect_grace", None)
    if cancel is None:
        return
    try:
        cancel(redis_client, stats_channel_id, client_id)
    except Exception:
        logger.debug("M3U timeshift stats grace cancel failed", exc_info=True)


def _touch_hls_timeshift_session_request(
    *, redis_client, timeshift_views, stats_channel_id, client_id,
):
    if redis_client is None:
        return
    touch = getattr(timeshift_views, "_touch_stats_on_session_request", None)
    if touch is not None:
        try:
            channel_id = str(stats_channel_id).split("_", 1)[0]
            touch(redis_client, channel_id, client_id)
            return
        except Exception:
            logger.debug("M3U timeshift native stats touch failed", exc_info=True)
    _cancel_hls_stats_disconnect_grace(
        timeshift_views=timeshift_views,
        redis_client=redis_client,
        stats_channel_id=stats_channel_id,
        client_id=client_id,
    )


def _hls_stop_requested(
    redis_client, stop_key, *, client_id=None, stream_generation=None,
):
    if redis_client is None or not stop_key:
        return False
    try:
        # A replacement HLS request leaves the old stream's ``reuse`` signal
        # in Redis until either generator observes it.  The replacement must
        # not consume its own signal; the per-session generation makes the
        # old generator stop even when the new one clears that key first.
        if (
            client_id is not None
            and stream_generation is not None
            and not _is_current_plugin_stream_generation(
                redis_client=redis_client,
                client_id=client_id,
                stream_generation=stream_generation,
            )
        ):
            return True
        stop_value = _decode_redis_value(redis_client.get(stop_key))
        if stop_value is not None:
            redis_client.delete(stop_key)
            if stop_value == "reuse":
                return False
            return True
    except Exception:
        return False
    return False


def _resolve_plugin_session_id(
    *,
    request,
    user,
    channel,
    client_ip,
    client_user_agent,
    redis_client,
    timeshift_views,
    make_stats_channel_id,
    mint_session_id,
):
    requested = request.GET.get("session_id")
    key = _plugin_session_fingerprint_key(
        user=user,
        channel=channel,
        client_ip=client_ip,
        client_user_agent=client_user_agent,
    )
    remembered = _decode_redis_value(redis_client.get(key)) if redis_client else None
    if requested:
        # Without Redis there is no shared plugin session state to validate or
        # protect. Preserve the caller's ID to avoid an endless redirect loop
        # while Dispatcharr is operating in its degraded mode.
        if redis_client is None:
            return requested
        # Dispatcharr has already authenticated and resolved native API
        # playback sessions before this hook runs.  Preserve a session only
        # when its Redis record binds it to this same user and channel; a
        # random caller-supplied session ID still falls through to replacement.
        if _is_dispatcharr_api_session_owned_by(
            redis_client, timeshift_views, requested, user, channel
        ):
            return requested
        if _plugin_session_is_owned_by(redis_client, requested, user):
            _remember_plugin_session(redis_client, key, requested, user)
            return requested

        # Do not allow a client to attach to an arbitrary existing session.
        # This includes owner-less IDs from older plugin versions: replacing
        # them is safer than allowing a possible cross-user takeover.
        session_id = mint_session_id()
        _remember_plugin_session(redis_client, key, session_id, user)
        return session_id

    if remembered and _plugin_session_is_reusable(
        redis_client=redis_client,
        timeshift_views=timeshift_views,
        make_stats_channel_id=make_stats_channel_id,
        channel=channel,
        session_id=remembered,
    ) and _plugin_session_is_owned_by(redis_client, remembered, user):
        _remember_plugin_session(redis_client, key, remembered, user)
        return remembered

    session_id = mint_session_id()
    _remember_plugin_session(redis_client, key, session_id, user)
    return session_id


def _plugin_session_fingerprint_key(*, user, channel, client_ip, client_user_agent):
    user_id = getattr(user, "id", "0") or "0"
    channel_id = getattr(channel, "id", "0") or "0"
    fingerprint = f"{user_id}|{channel_id}|{client_ip or ''}|{client_user_agent or ''}"
    import hashlib

    digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()
    return f"{_PLUGIN_KEY_PREFIX}:session:{digest}"


def _remember_plugin_session(redis_client, key, session_id, user):
    if redis_client is None or not key or not session_id:
        return
    try:
        user_id = str(getattr(user, "id", "0") or "0")
        pipe = redis_client.pipeline(transaction=False)
        pipe.setex(key, _PLUGIN_SESSION_TTL_SECONDS, session_id)
        pipe.setex(
            _plugin_session_owner_key(session_id),
            _PLUGIN_SESSION_TTL_SECONDS,
            user_id,
        )
        pipe.execute()
    except Exception:
        logger.debug("M3U timeshift session fingerprint store failed", exc_info=True)


def _plugin_session_owner_key(session_id):
    return f"{_PLUGIN_KEY_PREFIX}:session_owner:{session_id}"


def _is_dispatcharr_api_session_owned_by(
    redis_client, timeshift_views, session_id, user, channel,
):
    """Validate a native API playback session without accepting arbitrary IDs."""
    if redis_client is None or not session_id:
        return False
    try:
        key = timeshift_views.TimeshiftRedisKeys.api_session(session_id)
        record = redis_client.hgetall(key)
        if not record:
            return False
        record = {
            _decode_redis_value(key): _decode_redis_value(value)
            for key, value in record.items()
        }
        return (
            str(record.get("user_id") or "")
            == str(getattr(user, "id", "") or "")
            and str(record.get("channel_uuid") or "")
            == str(getattr(channel, "uuid", "") or "")
            and bool(record.get("start"))
        )
    except Exception:
        logger.debug("M3U timeshift API session validation failed", exc_info=True)
        return False


def _plugin_session_is_owned_by(redis_client, session_id, user):
    """Return whether *session_id* was minted for *user* by this plugin."""
    if redis_client is None or not session_id:
        return False
    try:
        owner_id = _decode_redis_value(
            redis_client.get(_plugin_session_owner_key(session_id))
        )
        return owner_id is not None and str(owner_id) == str(
            getattr(user, "id", "0") or "0"
        )
    except Exception:
        logger.debug("M3U timeshift session ownership check failed", exc_info=True)
        return False


def _plugin_session_is_reusable(
    *, redis_client, timeshift_views, make_stats_channel_id, channel, session_id,
):
    if redis_client is None or not session_id:
        return False
    try:
        keys = timeshift_views.TimeshiftRedisKeys
        stats_channel_id = make_stats_channel_id(channel.id, session_id)
        client_key = keys.client_metadata(stats_channel_id, session_id)
        grace_key = keys.stats_grace(stats_channel_id, session_id)
        pool_key = keys.pool(session_id)
        return bool(
            redis_client.exists(client_key)
            or redis_client.exists(grace_key)
            or redis_client.exists(pool_key)
        )
    except Exception:
        logger.debug("M3U timeshift reusable session check failed", exc_info=True)
        return False


def _begin_plugin_stream_generation(
    *, redis_client, timeshift_views, client_id, virtual_channel_id,
):
    if redis_client is None:
        return None
    active_key = _hls_active_virtual_channel_key(client_id)
    generation_key = _hls_stream_generation_key(client_id)
    try:
        previous_vid = _decode_redis_value(redis_client.get(active_key))
        # Direct M3U playback has no native timeshift pool to multiplex sibling
        # requests. Replace any in-flight plugin response for this session,
        # including one for the same programme, so it cannot consume another
        # provider/profile slot indefinitely.
        if previous_vid:
            stop_key = timeshift_views.TimeshiftRedisKeys.client_stop(
                previous_vid, client_id,
            )
            redis_client.setex(stop_key, 60, "reuse")
        stream_generation = redis_client.incr(generation_key)
        pipe = redis_client.pipeline(transaction=False)
        pipe.expire(generation_key, _PLUGIN_SESSION_TTL_SECONDS)
        pipe.setex(active_key, _PLUGIN_SESSION_TTL_SECONDS, virtual_channel_id)
        pipe.execute()
        return int(stream_generation)
    except Exception:
        logger.debug("M3U timeshift stream generation claim failed", exc_info=True)
        return None


def _is_current_plugin_stream_generation(*, redis_client, client_id, stream_generation):
    if redis_client is None or stream_generation is None:
        return True
    try:
        current = _decode_redis_value(redis_client.get(_hls_stream_generation_key(client_id)))
        return str(current) == str(stream_generation)
    except Exception:
        return True


def _clear_plugin_stream_generation(
    *, redis_client, client_id, virtual_channel_id, stream_generation,
):
    if redis_client is None or stream_generation is None:
        return
    try:
        active_key = _hls_active_virtual_channel_key(client_id)
        current_vid = _decode_redis_value(redis_client.get(active_key))
        current_generation = _decode_redis_value(
            redis_client.get(_hls_stream_generation_key(client_id))
        )
        if (
            str(current_generation) == str(stream_generation)
            and current_vid == virtual_channel_id
        ):
            redis_client.delete(active_key)
    except Exception:
        logger.debug("M3U timeshift generation cleanup failed", exc_info=True)


def _hls_active_virtual_channel_key(client_id):
    return f"{_PLUGIN_KEY_PREFIX}:active_vid:{client_id}"


def _hls_stream_generation_key(client_id):
    return f"{_PLUGIN_KEY_PREFIX}:generation:{client_id}"


def _decode_redis_value(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _resolve_user_agent(stream):
    account = getattr(stream, "m3u_account", None)
    if not account:
        return None
    try:
        ua = account.get_user_agent()
    except Exception:
        ua = getattr(account, "user_agent", None)
    return getattr(ua, "user_agent", None) if ua else None


def _call_original(request, user, channel, timestamp, client_duration_hint):
    if _original_serve_catchup is None:
        return HttpResponseBadRequest("Timeshift hook is not initialized")
    return _original_serve_catchup(
        request,
        user,
        channel,
        timestamp,
        client_duration_hint=client_duration_hint,
    )


def _redact_url(url):
    if not url or "://" not in url:
        return url
    scheme, rest = url.split("://", 1)
    if "@" in rest:
        rest = rest.split("@", 1)[1]
    host = rest.split("/", 1)[0]
    return f"{scheme}://{host}/..."
