"""Dependency-free helpers for inspecting HLS archive playlists."""

import re
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlparse


def parse_hls_playlist(text, base_url):
    """Parse the supported subset of an HLS playlist.

    Fragmented MP4/CMAF media is detected so callers can reject it instead of
    incorrectly concatenating fragments into an MPEG-TS response.
    """
    segments = []
    variants = []
    pending_duration = None
    pending_variant_bandwidth = None
    encrypted = False
    byterange = False
    fragmented_mp4 = False
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#EXT-X-KEY"):
            method = _hls_attribute(line.partition(":")[2], "METHOD")
            if (method or "").upper() != "NONE":
                encrypted = True
            continue
        if line.startswith("#EXT-X-BYTERANGE"):
            byterange = True
            continue
        if line.startswith("#EXT-X-MAP"):
            fragmented_mp4 = True
            continue
        if line.startswith("#EXT-X-STREAM-INF:"):
            match = re.search(r"(?:^|[:,])BANDWIDTH=(\d+)(?:,|$)", line)
            pending_variant_bandwidth = int(match.group(1)) if match else 0
            continue
        if line.startswith("#EXTINF:"):
            value = line.split(":", 1)[1].split(",", 1)[0].strip()
            try:
                pending_duration = max(float(value), 0.0)
            except (TypeError, ValueError):
                pending_duration = 0.0
            continue
        if line.startswith("#"):
            continue
        if urlparse(line).path.lower().endswith(".m4s"):
            fragmented_mp4 = True
        if pending_variant_bandwidth is not None:
            variants.append(
                {
                    "url": expand_segment_url(base_url, line),
                    "bandwidth": pending_variant_bandwidth,
                }
            )
            pending_variant_bandwidth = None
            continue
        segments.append(
            {
                "url": expand_segment_url(base_url, line),
                "duration": pending_duration if pending_duration is not None else 0.0,
            }
        )
        pending_duration = None

    return {
        "ok": True,
        "encrypted": encrypted,
        "byterange": byterange,
        "fragmented_mp4": fragmented_mp4,
        "segments": segments,
        "master": bool(variants),
        "variants": variants,
    }


def _hls_attribute(attributes, name):
    """Return one unquoted or quoted HLS attribute value, if present."""
    match = re.search(
        rf'(?:^|,)\s*{re.escape(name)}\s*=\s*(?:"([^"]*)"|([^,\s]*))',
        attributes,
        re.IGNORECASE,
    )
    if match is None:
        return None
    return (match.group(1) if match.group(1) is not None else match.group(2)).strip()


def expand_segment_url(base_url, segment_uri):
    """Resolve a segment URI and inherit unoverridden playlist query parameters."""
    parsed_base = urlparse(base_url)
    base_query_values = dict(parse_qsl(parsed_base.query, keep_blank_values=True))
    token_value = base_query_values.get("token")
    if token_value:
        segment_uri = segment_uri.replace("{token}", token_value)
        segment_uri = segment_uri.replace("%7Btoken%7D", quote(token_value, safe=""))

    segment_url = urljoin(base_url, segment_uri)
    if not parsed_base.query:
        return segment_url

    parsed_resolved = urlparse(segment_url)
    segment_query = parse_qsl(parsed_resolved.query, keep_blank_values=True)
    segment_keys = {key for key, _value in segment_query}
    inherited_query = [
        (key, value)
        for key, value in parse_qsl(parsed_base.query, keep_blank_values=True)
        if key not in segment_keys
    ]
    if not inherited_query:
        return segment_url
    return parsed_resolved._replace(
        query=urlencode(inherited_query + segment_query, doseq=True, quote_via=quote, safe="")
    ).geturl()
