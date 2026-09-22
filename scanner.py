"""Scan standard M3U streams for timeshift and source stream metadata."""

import json
import logging
import re

logger = logging.getLogger(__name__)

PLUGIN_MARKER = "m3u_timeshift_plugin"
# Records the archive attributes written by this plugin.  The source importer
# replaces ``custom_properties`` on refresh, so this marker is intentionally
# absent when ``tv_archive`` values came directly from an M3U provider.
PLUGIN_ARCHIVE_VALUES_MARKER = "m3u_timeshift_plugin_archive_values"
BATCH_SIZE = 500

_STREAM_STATS_ALIASES = {
    "resolution": (
        "resolution",
        "video_resolution",
        "tvg_resolution",
        "video_res",
        "res",
    ),
    "source_fps": (
        "source_fps",
        "fps",
        "frame_rate",
        "framerate",
        "tvg_fps",
    ),
    "video_codec": (
        "video_codec",
        "vcodec",
        "video_codec_name",
    ),
    "audio_codec": (
        "audio_codec",
        "acodec",
        "audio_codec_name",
    ),
    "audio_channels": (
        "audio_channels",
        "channels",
        "channel_layout",
        "audio_channel_layout",
    ),
}

_VIDEO_CODEC_ALIASES = {
    "avc": "h264",
    "avc1": "h264",
    "h.264": "h264",
    "x264": "h264",
    "h.265": "hevc",
}


def _normalise_attribute_key(value):
    return str(value).strip().lower().replace("-", "_")


def as_json_object(value):
    """Return a mutable mapping for JSONField values, including legacy strings."""
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        if isinstance(parsed, dict):
            return dict(parsed)
    return {}


def _normalise_stream_stat_value(stat_name, value):
    if value is None:
        return None

    value = str(value).strip()
    if not value:
        return None

    if stat_name == "resolution":
        match = re.search(r"(?<!\d)(\d{3,5})\s*[xX]\s*(\d{3,5})(?!\d)", value)
        if match:
            return f"{int(match.group(1))}x{int(match.group(2))}"
        if re.fullmatch(r"\d{3,4}p", value, re.IGNORECASE):
            return value.lower()
        return value

    if stat_name == "source_fps":
        fps_value = value.lower().removesuffix("fps").strip()
        try:
            if "/" in fps_value:
                numerator, denominator = fps_value.split("/", 1)
                parsed = float(numerator) / float(denominator)
            else:
                parsed = float(fps_value)
        except (TypeError, ValueError, ZeroDivisionError):
            return None
        return parsed if parsed > 0 else None

    value = value.lower()
    if stat_name == "video_codec":
        return _VIDEO_CODEC_ALIASES.get(value, value)
    return value


def merge_m3u_stream_stats(custom_properties, existing_stats=None):
    """Merge advertised M3U stream metadata into previously measured stats.

    M3U attributes are provider-advertised values, while ``existing_stats`` may
    contain values collected during an earlier live playback. Explicit current
    M3U attributes update the matching keys; unrelated previously measured keys
    are preserved. No network probing is performed here.
    """
    stats = dict(existing_stats or {})
    properties = custom_properties if isinstance(custom_properties, dict) else {}

    sources = [properties]
    for nested_key in ("stream_stats", "stream_info"):
        nested = properties.get(nested_key)
        if isinstance(nested, dict):
            sources.insert(0, nested)

    values = {}
    for source in sources:
        for key, value in source.items():
            values[_normalise_attribute_key(key)] = value

    for stat_name, aliases in _STREAM_STATS_ALIASES.items():
        for alias in aliases:
            if alias not in values:
                continue
            normalised = _normalise_stream_stat_value(stat_name, values[alias])
            if normalised is not None:
                stats[stat_name] = normalised
                break

    return stats


def parse_timeshift_days(value, max_days_cap=30):
    """Parse a provider timeshift value into positive archive days or zero."""
    try:
        days = int(str(value).strip())
    except (TypeError, ValueError):
        return 0
    if days <= 0:
        return 0
    try:
        cap = int(max_days_cap or 30)
    except (TypeError, ValueError):
        cap = 30
    return min(days, max(1, cap))


def m3u_timeshift_days(properties, max_days_cap=30):
    """Return archive days advertised by either supported M3U marker.

    ``timeshift`` is the original provider attribute. Some providers use
    ``tvg-rec`` for the same purpose, so accept it as a fallback while keeping
    ``timeshift`` authoritative when both attributes are present.
    """
    if not isinstance(properties, dict):
        return 0
    for attribute in ("timeshift", "tvg-rec", "tvg_rec"):
        days = parse_timeshift_days(properties.get(attribute), max_days_cap)
        if days > 0:
            return days
    return 0


def native_m3u_archive_days(properties):
    """Return the provider-advertised native M3U archive depth, if any.

    Dispatcharr imports these fields itself.  They are deliberately not capped
    by this plugin because they did not originate from ``timeshift``.
    """
    if not isinstance(properties, dict):
        return 0
    if str(properties.get("tv_archive", "0")).strip() not in {"1", "True"}:
        return 0
    try:
        days = int(str(properties.get("tv_archive_duration", 0)).strip())
    except (TypeError, ValueError):
        return 0
    return max(days, 0)


def clear_plugin_timeshift_properties(properties):
    """Remove only archive attributes known to have been plugin-generated.

    Older plugin versions did not save ownership of the two native attributes.
    In that ambiguous case, retain them: preserving an M3U provider's metadata
    is safer than deleting it.
    """
    properties.pop(PLUGIN_MARKER, None)
    managed_values = properties.pop(PLUGIN_ARCHIVE_VALUES_MARKER, None)
    if not isinstance(managed_values, dict):
        return
    if (
        properties.get("tv_archive") == managed_values.get("tv_archive")
        and properties.get("tv_archive_duration")
        == managed_values.get("tv_archive_duration")
    ):
        properties.pop("tv_archive", None)
        properties.pop("tv_archive_duration", None)


def scan_timeshift_streams(account_id=None, settings=None):
    """Mark standard M3U streams with positive timeshift as catch-up capable."""
    settings = settings or {}
    max_days_cap = settings.get("max_days_cap", 30)

    from django.db import transaction
    from apps.channels.models import Channel, ChannelStream, Stream
    from apps.m3u.models import M3UAccount

    account_filter = {
        "m3u_account__isnull": False,
        "m3u_account__is_active": True,
    }
    if hasattr(M3UAccount, "Types"):
        account_filter["m3u_account__account_type"] = M3UAccount.Types.STADNARD
    else:
        account_filter["m3u_account__account_type"] = "STD"
    if account_id:
        account_filter["m3u_account_id"] = account_id

    qs = (
        Stream.objects.filter(**account_filter)
        .only(
            "id",
            "custom_properties",
            "stream_stats",
            "stream_stats_updated_at",
            "is_catchup",
            "catchup_days",
            "m3u_account_id",
        )
        .order_by("id")
    )

    scanned = 0
    catchup = 0
    cleared = 0
    changed = []

    with transaction.atomic():
        from django.utils import timezone

        # Channel membership can change without modifying the stream row (for
        # example, when an administrator links an already-scanned stream to a
        # channel).  Retain the scanned account scope so an otherwise no-op
        # scan still repairs the derived Channel catch-up fields.
        scanned_account_ids = set(
            qs.values_list("m3u_account_id", flat=True).distinct()
        )

        for stream in qs.iterator(chunk_size=BATCH_SIZE):
            scanned += 1
            props = as_json_object(stream.custom_properties)
            current_stats = as_json_object(stream.stream_stats)
            merged_stats = merge_m3u_stream_stats(props, current_stats)
            stats_changed = merged_stats != current_stats
            if stats_changed:
                stream.stream_stats = merged_stats
                stream.stream_stats_updated_at = timezone.now()

            days = m3u_timeshift_days(props, max_days_cap)
            was_marked = props.get(PLUGIN_MARKER) is True

            if days > 0:
                props["tv_archive"] = "1"
                props["tv_archive_duration"] = str(days)
                props[PLUGIN_MARKER] = True
                props[PLUGIN_ARCHIVE_VALUES_MARKER] = {
                    "tv_archive": "1",
                    "tv_archive_duration": str(days),
                }
                catchup += 1
                if (
                    stream.is_catchup is not True
                    or stream.catchup_days != days
                    or stream.custom_properties != props
                    or stats_changed
                ):
                    stream.is_catchup = True
                    stream.catchup_days = days
                    stream.custom_properties = props
                    changed.append(stream)
            elif was_marked:
                clear_plugin_timeshift_properties(props)
                # A refreshed playlist can still advertise Dispatcharr's
                # native archive fields after its ``timeshift`` attribute was
                # removed.  Keep those fields and their derived flags intact.
                native_days = native_m3u_archive_days(props)
                stream.is_catchup = native_days > 0
                stream.catchup_days = native_days
                stream.custom_properties = props
                changed.append(stream)
                cleared += 1
            elif stats_changed:
                changed.append(stream)

            if len(changed) >= BATCH_SIZE:
                Stream.objects.bulk_update(
                    changed,
                    [
                        "is_catchup",
                        "catchup_days",
                        "custom_properties",
                        "stream_stats",
                        "stream_stats_updated_at",
                    ],
                    batch_size=BATCH_SIZE,
                )
                changed.clear()

        if changed:
            Stream.objects.bulk_update(
                changed,
                [
                    "is_catchup",
                    "catchup_days",
                    "custom_properties",
                    "stream_stats",
                    "stream_stats_updated_at",
                ],
                batch_size=BATCH_SIZE,
            )

        channel_ids = _channel_ids_for_scanned_accounts(
            ChannelStream, scanned_account_ids
        )
        updated_channels = _rollup_channels(Channel, channel_ids)

    result = {
        "status": "ok",
        "scanned_streams": scanned,
        "catchup_streams": catchup,
        "cleared_streams": cleared,
        "updated_channels": updated_channels,
    }
    logger.info("M3U timeshift scan complete: %s", result)
    return result


def _channel_ids_for_scanned_accounts(ChannelStream, account_ids):
    """Return channels linked to any account included in this scan."""
    if not account_ids:
        return set()
    return set(
        ChannelStream.objects.filter(stream__m3u_account_id__in=account_ids)
        .values_list("channel_id", flat=True)
        .distinct()
    )


def _rollup_channels(Channel, channel_ids):
    if not channel_ids:
        return 0

    updated = 0
    channels = (
        Channel.objects.filter(id__in=channel_ids)
        .prefetch_related("streams__m3u_account")
        .order_by("id")
    )
    changed = []
    for channel in channels.iterator(chunk_size=BATCH_SIZE):
        active_catchup_streams = [
            stream
            for stream in channel.streams.all()
            if stream.is_catchup
            and stream.m3u_account is not None
            and stream.m3u_account.is_active
        ]
        is_catchup = bool(active_catchup_streams)
        catchup_days = max(
            [int(stream.catchup_days or 0) for stream in active_catchup_streams],
            default=0,
        )
        if channel.is_catchup != is_catchup or channel.catchup_days != catchup_days:
            channel.is_catchup = is_catchup
            channel.catchup_days = catchup_days
            changed.append(channel)
            updated += 1
        if len(changed) >= BATCH_SIZE:
            Channel.objects.bulk_update(
                changed, ["is_catchup", "catchup_days"], batch_size=BATCH_SIZE
            )
            changed.clear()
    if changed:
        Channel.objects.bulk_update(
            changed, ["is_catchup", "catchup_days"], batch_size=BATCH_SIZE
        )
    return updated
