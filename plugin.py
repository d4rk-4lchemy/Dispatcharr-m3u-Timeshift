"""Dispatcharr entry point for the M3U Timeshift plugin."""

import json
import logging
from pathlib import Path

from . import hooks
from .scanner import scan_timeshift_streams

logger = logging.getLogger(__name__)


def _manifest():
    try:
        with Path(__file__).with_name("plugin.json").open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


class Plugin:
    def __init__(self):
        manifest = _manifest()
        self.name = manifest.get("name", "M3U Timeshift")
        self.version = manifest.get("version", "")
        self.description = manifest.get("description", "")
        self.author = manifest.get("author", "")
        self.fields = manifest.get("fields", [])
        self.actions = manifest.get("actions", [])
        hooks.install()

    def run(self, action, params=None, context=None):
        params = params or {}
        context = context or {}
        settings = context.get("settings") or {}
        hooks.set_runtime_settings(settings)

        if action == "stop":
            hooks.disable()
            return {"status": "ok", "message": "M3U timeshift hooks disabled"}

        if not settings.get("enabled", True):
            return {"status": "disabled"}

        if action == "scan_after_m3u_refresh":
            if not settings.get("scan_on_m3u_refresh", True):
                return {"status": "skipped", "reason": "scan_on_m3u_refresh disabled"}
            account_id = _extract_account_id(params)
            if account_id is None:
                account_id = _extract_account_id_from_name(params)
            return scan_timeshift_streams(account_id=account_id, settings=settings)

        if action == "scan":
            account_id = _extract_account_id(params)
            return scan_timeshift_streams(account_id=account_id, settings=settings)

        return {"status": "unknown_action", "action": action}

    def stop(self, context=None):
        hooks.disable()


def _extract_account_id(params):
    payload = params.get("payload") if isinstance(params, dict) else None
    candidates = []
    if isinstance(params, dict):
        candidates.extend(
            [
                params.get("account_id"),
                params.get("m3u_account_id"),
                params.get("account"),
            ]
        )
    if isinstance(payload, dict):
        candidates.extend(
            [
                payload.get("account_id"),
                payload.get("m3u_account_id"),
                payload.get("account"),
                payload.get("m3u_account"),
            ]
        )
    for candidate in candidates:
        if candidate in (None, ""):
            continue
        try:
            return int(candidate)
        except (TypeError, ValueError):
            continue
    return None


def _extract_account_id_from_name(params):
    """Resolve Dispatcharr's ``m3u_refresh`` event account name to its ID."""
    payload = params.get("payload") if isinstance(params, dict) else None
    candidates = []
    if isinstance(params, dict):
        candidates.append(params.get("account_name"))
    if isinstance(payload, dict):
        candidates.append(payload.get("account_name"))

    for account_name in candidates:
        if not isinstance(account_name, str) or not account_name.strip():
            continue
        try:
            from apps.m3u.models import M3UAccount

            return M3UAccount.objects.filter(name=account_name.strip()).values_list(
                "id", flat=True
            ).first()
        except Exception:
            logger.exception("Could not resolve refreshed M3U account name")
            return None
    return None
