from hls import expand_segment_url, parse_hls_playlist


def test_cmaf_hls_playlist_is_identified_as_unsupported():
    playlist = parse_hls_playlist(
        "#EXTM3U\n"
        "#EXT-X-MAP:URI=\"init.mp4\"\n"
        "#EXTINF:2.0,\n"
        "segment-1.m4s\n",
        "https://example.test/archive/index.m3u8",
    )

    assert playlist["fragmented_mp4"] is True


def test_m4s_segment_is_identified_without_an_initialization_map():
    playlist = parse_hls_playlist(
        "#EXTM3U\n#EXTINF:2.0,\nsegment-1.m4s?token=abc\n",
        "https://example.test/archive/index.m3u8",
    )

    assert playlist["fragmented_mp4"] is True


def test_transport_stream_hls_playlist_remains_supported():
    playlist = parse_hls_playlist(
        "#EXTM3U\n#EXTINF:2.0,\nsegment-1.ts\n",
        "https://example.test/archive/index.m3u8",
    )

    assert playlist["fragmented_mp4"] is False


def test_encrypted_hls_is_not_mistaken_for_method_none_in_a_key_uri():
    playlist = parse_hls_playlist(
        "#EXTM3U\n"
        '#EXT-X-KEY:METHOD=AES-128,URI="https://keys.example.test/key?method=none"\n'
        "#EXTINF:2.0,\nsegment-1.ts\n",
        "https://example.test/archive/index.m3u8",
    )

    assert playlist["encrypted"] is True


def test_master_playlist_uses_bandwidth_when_it_is_the_first_attribute():
    playlist = parse_hls_playlist(
        "#EXTM3U\n"
        "#EXT-X-STREAM-INF:BANDWIDTH=800000\n"
        "low.m3u8\n"
        "#EXT-X-STREAM-INF:BANDWIDTH=2000000\n"
        "high.m3u8\n",
        "https://example.test/archive/master.m3u8",
    )

    assert [variant["bandwidth"] for variant in playlist["variants"]] == [800000, 2000000]


def test_segment_query_inherits_unoverridden_master_parameters():
    assert expand_segment_url(
        "https://example.test/archive/index.m3u8?token=secret&utc=123",
        "segment.ts?part=1",
    ) == "https://example.test/archive/segment.ts?token=secret&utc=123&part=1"


def test_absolute_segment_inherits_master_parameters_and_can_override_them():
    assert expand_segment_url(
        "https://example.test/archive/index.m3u8?token=secret&utc=123",
        "https://cdn.example.test/segment.ts?token=segment-token",
    ) == "https://cdn.example.test/segment.ts?utc=123&token=segment-token"
