from scanner import (
    _channel_ids_for_scanned_accounts,
    as_json_object,
    clear_plugin_timeshift_properties,
    merge_m3u_stream_stats,
    native_m3u_archive_days,
    parse_timeshift_days,
    PLUGIN_ARCHIVE_VALUES_MARKER,
    PLUGIN_MARKER,
)


def test_positive_timeshift_values():
    assert parse_timeshift_days("1") == 1
    assert parse_timeshift_days("5") == 5


def test_cap_is_applied():
    assert parse_timeshift_days("60", max_days_cap=30) == 30


def test_non_positive_and_invalid_values_are_zero():
    assert parse_timeshift_days("0") == 0
    assert parse_timeshift_days("-2") == 0
    assert parse_timeshift_days("abc") == 0
    assert parse_timeshift_days(None) == 0


def test_m3u_attributes_are_mapped_to_stream_stats():
    stats = merge_m3u_stream_stats(
        {
            "tvg-resolution": "1920x1080",
            "fps": "30000/1001",
            "video-codec": "H.264",
            "audio_codec": "AAC",
            "audio-channels": "stereo",
        }
    )

    assert stats == {
        "resolution": "1920x1080",
        "source_fps": 30000 / 1001,
        "video_codec": "h264",
        "audio_codec": "aac",
        "audio_channels": "stereo",
    }


def test_existing_playback_stats_are_preserved_for_unadvertised_fields():
    stats = merge_m3u_stream_stats(
        {"resolution": "1280x720"},
        {
            "resolution": "1920x1080",
            "video_bitrate": 5000,
            "pixel_format": "yuv420p",
        },
    )

    assert stats["resolution"] == "1280x720"
    assert stats["video_bitrate"] == 5000
    assert stats["pixel_format"] == "yuv420p"


def test_legacy_jsonfield_strings_are_normalised_to_mappings():
    assert as_json_object('{"timeshift": "3"}') == {"timeshift": "3"}
    assert as_json_object('["not", "an", "object"]') == {}
    assert as_json_object("not json") == {}


def test_clearing_plugin_metadata_keeps_provider_archive_fields():
    properties = {
        "timeshift": "0",
        "tv_archive": "1",
        "tv_archive_duration": "7",
        PLUGIN_MARKER: True,
    }

    clear_plugin_timeshift_properties(properties)

    assert properties == {
        "timeshift": "0",
        "tv_archive": "1",
        "tv_archive_duration": "7",
    }
    assert native_m3u_archive_days(properties) == 7


def test_clearing_owned_plugin_archive_fields_removes_them():
    properties = {
        "tv_archive": "1",
        "tv_archive_duration": "3",
        PLUGIN_MARKER: True,
        PLUGIN_ARCHIVE_VALUES_MARKER: {
            "tv_archive": "1",
            "tv_archive_duration": "3",
        },
    }

    clear_plugin_timeshift_properties(properties)

    assert properties == {}


def test_rollup_scope_includes_channels_for_unchanged_scanned_streams():
    class ChannelStreamManager:
        def __init__(self):
            self.filter_kwargs = None

        def filter(self, **kwargs):
            self.filter_kwargs = kwargs
            return self

        def values_list(self, *_args, **_kwargs):
            return self

        def distinct(self):
            return [10, 11]

    manager = ChannelStreamManager()

    class ChannelStream:
        objects = manager

    assert _channel_ids_for_scanned_accounts(ChannelStream, {7}) == {10, 11}
    assert manager.filter_kwargs == {"stream__m3u_account_id__in": {7}}
