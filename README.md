# M3U Timeshift

Version: `0.5.0`

Dispatcharr plugin that adds catch-up support for standard M3U playlists using
the `timeshift` or `tvg-rec` EXTINF attribute and propagates available stream
metadata.

Example:

```m3u
#EXTINF:0 tvg-id="ch123" tvg-name="Channel 1" timeshift="3", Channel 1
http://url.net/ch123/mono.m3u8?token=abc
```

Positive numeric `timeshift` or `tvg-rec` values mean archive days. `timeshift`
takes precedence when both are present. `0`, negative, missing, and non-numeric
values are treated as no catch-up.

For catch-up playback the plugin appends the programme start as a UTC epoch
query parameter:

```text
http://url.net/ch123/mono.m3u8?token=abc&utc=1784376000
```

If the live URL has no query string, `?utc=...` is used instead.

## Installation

Install the packaged plugin zip, or place this directory under Dispatcharr's
plugin directory, usually:

```text
/data/plugins/m3u-timeshift
```

Enable the plugin in Dispatcharr and run the scan action once, or refresh an M3U
account when `Scan after M3U refresh` is enabled. The scan processes active
standard M3U accounts only.

## Stream Ordering

Playback respects Dispatcharr channel stream order. If a standard M3U catch-up
stream with a positive `timeshift` value appears before an XC catch-up stream,
the M3U stream is used. If an XC catch-up stream appears first, Dispatcharr's
native XC behavior is used.

## Settings

- `enabled`: enable scanner and playback hook checks.
- `debug_mode`: add diagnostic logs with redacted URLs.
- `utc_param_name`: query parameter name, default `utc`.
- `max_days_cap`: maximum imported archive days, default `30`.
- `scan_on_m3u_refresh`: scan automatically after `m3u_refresh` events.
- `pace_hls_archive`: throttle bridged HLS archive output using `#EXTINF`
  durations and `hls_archive_rate`, default `true`. Disable to send as fast as
  the source and client allow.
- `hls_archive_rate`: seconds of video sent per real second during the initial
  burst, default `5`.
  Set `1` for real-time delivery. Applies to new playback requests when pacing
  is enabled; invalid or non-positive values fall back to `5`. With a segment
  `Content-Length`, output is paced in 32 KiB chunks; otherwise the segment is
  sent immediately and the plugin waits before starting the next one.
  A 10-second segment targets 2 seconds at 5x, subject to source/client speed.
  This does not change playback speed or create material unavailable upstream.
  Active Connections may end before playback finishes because the player has
  buffered the remaining material. Direct MPEG-TS streams are unaffected.
- `hls_archive_burst_seconds`: initial burst budget in seconds of material,
  default `60`. After this budget, output returns to 1x. At 5x the first
  60 seconds target 12 real seconds, then each subsequent media second takes
  one real second. Set `0` to use 1x from the start. Invalid or negative values
  fall back to `60`. The budget resets for every new playback request,
  including seeks via a new timestamp, even when the session ID is reused.
  Byte Range support is unchanged: bridged HLS accepts only `bytes=0-`;
  other byte ranges return HTTP 416. With `Content-Length`, a burst boundary
  inside a segment is handled proportionally in the chunk pacing. Without it,
  pacing remains a pause between whole segments. Disabling `pace_hls_archive`
  disables both the burst limit and the subsequent 1x pacing.

## Active Connections

MPEG-TS catch-up playback is handed to Dispatcharr's normal provider streaming
path. HLS catch-up playback is bridged by the plugin and registered in
Dispatcharr's timeshift stats so it appears in Active Connections and
`/proxy/stats/`. Seek/reconnect requests reuse the same effective session where
possible, even when Dispatcharr minted a fresh API `session_id` for the new
request. The plugin updates the programme position on the existing stats entry
and uses Dispatcharr's delayed disconnect grace so short seek gaps do not emit a
false `Catch-up ended` event.

## Stream Metadata

When the M3U entry advertises stream metadata, the scanner maps supported values
into `Stream.stream_stats`. Existing stats from earlier live playback are
preserved for fields not advertised by the M3U entry. No separate upstream
probing is performed. MPEG-TS and HLS catch-up sessions use the same stored
stats.

Supported metadata fields and aliases:

- resolution: `resolution`, `video_resolution`, `tvg_resolution`, `video_res`,
  `res`
- frame rate: `source_fps`, `fps`, `frame_rate`, `framerate`, `tvg_fps`
- video codec: `video_codec`, `vcodec`, `video_codec_name`
- audio codec: `audio_codec`, `acodec`, `audio_codec_name`
- audio channels: `audio_channels`, `channels`, `channel_layout`,
  `audio_channel_layout`

## URL Handling

The plugin does not persist playback URLs, provider responses, tokens, or
catch-up request diagnostics to local plugin files. Runtime errors are left to
Dispatcharr's normal logging path and should avoid including full upstream URLs.

When a positive `timeshift` or `tvg-rec` value is found, the scanner writes
Dispatcharr's native M3U archive fields (`tv_archive=1` and
`tv_archive_duration=<days>`) and marks them as plugin-managed. If a later scan
no longer sees either attribute, only plugin-managed archive fields are removed.
Provider-supplied native archive fields are preserved.

## Limitations

The playback hook depends on Dispatcharr's private catch-up function. Dispatcharr
updates can require plugin updates. Provider-specific catch-up URL formats other
than a UTC epoch query parameter are not implemented.

HLS archive playlists are bridged to MPEG-TS by fetching listed `.ts` segments
server-side. Master playlists are followed to the highest-bandwidth variant up
to a small nesting limit. Encrypted, byte-range, and fragmented MP4/CMAF HLS
playlists (including `.m4s` segments) are not supported. Because a bridged
playlist does not have a stable byte map, downstream HLS archive byte seeks
return `416` instead of incorrectly restarting playback at the beginning.
