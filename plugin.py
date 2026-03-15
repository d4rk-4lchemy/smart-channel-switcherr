"""
Smart Channel Switcherr plugin.

Monkey-patches apps.proxy.ts_proxy.views.generate_stream_url to insert
an optional delay before the FIRST stream source selection per request.
Retries within the same request (same greenlet) are not delayed.

If Smart Delay is enabled, the plugin only waits when the target channel's
first eligible M3U account is already at its concurrent limit, based on the
same Redis-backed channel metadata scan used by /proxy/ts/status.

Install: copy this directory to /data/plugins/wait-for-selection/
"""
import logging

logger = logging.getLogger(__name__)

_PATCH_MARKER = "_wait_for_selection_patched"
_ORIGINAL_FN_ATTR = "_wait_for_selection_original_fn"
_PLUGIN_KEY = "wait-for-selection"
_OCCUPYING_CHANNEL_STATES = {
    "active",
    "buffering",
    "connecting",
    "initializing",
    "stopping",
    "waiting_for_clients",
}


class Plugin:
    name = "Smart Channel Switcherr"
    version = "1.0.0"
    description = (
        "Delays stream source selection to allow recently-stopped channels "
        "to fully release their provider before a new assignment runs. "
        "Optional Smart Delay only waits when the target provider is already "
        "at its concurrent limit."
    )
    fields = []
    actions = []

    def __init__(self):
        self._original_fn = None
        self._proxy_views = None
        self._apply_patch()

    # ------------------------------------------------------------------
    # Patch management
    # ------------------------------------------------------------------

    def _apply_patch(self):
        try:
            import apps.proxy.ts_proxy.views as proxy_views
        except ImportError:
            logger.warning(
                "[wait-for-selection] Cannot import apps.proxy.ts_proxy.views; "
                "plugin will have no effect."
            )
            return

        current = proxy_views.generate_stream_url
        original = _unwrap_original_generate_stream_url(current)

        if getattr(current, _PATCH_MARKER, False):
            logger.info(
                "[wait-for-selection] Replacing existing generate_stream_url patch."
            )

        try:
            from gevent.local import local as _local
        except ImportError:
            import threading

            _local = threading.local
            logger.warning(
                "[wait-for-selection] gevent not found; falling back to "
                "threading.local(). Per-request delay behaviour may differ."
            )

        _state = _local()

        def _patched(channel_id):
            if not getattr(_state, "delay_applied", False):
                _state.delay_applied = True
                settings = _get_settings()
                wait = _get_wait_seconds(settings)
                if wait > 0 and _should_apply_delay(channel_id, settings):
                    _sleep_before_selection(wait, channel_id)
            return original(channel_id)

        setattr(_patched, _PATCH_MARKER, True)
        setattr(_patched, _ORIGINAL_FN_ATTR, original)
        self._original_fn = original
        self._proxy_views = proxy_views
        proxy_views.generate_stream_url = _patched
        logger.info("[wait-for-selection] Patch applied to generate_stream_url.")

    def _remove_patch(self):
        views = self._proxy_views
        original = self._original_fn
        if not views or not original:
            return
        current = getattr(views, "generate_stream_url", None)
        if current is not None and getattr(current, _PATCH_MARKER, False):
            views.generate_stream_url = original
            logger.info("[wait-for-selection] Patch removed from generate_stream_url.")
        self._original_fn = None
        self._proxy_views = None

    # ------------------------------------------------------------------
    # Plugin lifecycle
    # ------------------------------------------------------------------

    def run(self, action_id, params, context):
        return {"status": "ok", "message": "No actions defined for this plugin."}

    def stop(self, context=None):
        self._remove_patch()


# ------------------------------------------------------------------
# Delay helpers
# ------------------------------------------------------------------

def _sleep_before_selection(wait: float, channel_id):
    try:
        import gevent

        logger.debug(
            "[wait-for-selection] Sleeping %.2fs before stream selection for %s",
            wait,
            channel_id,
        )
        gevent.sleep(wait)
    except ImportError:
        import time

        time.sleep(wait)


def _unwrap_original_generate_stream_url(fn):
    original = getattr(fn, _ORIGINAL_FN_ATTR, None)
    if callable(original):
        return original

    closure = getattr(fn, "__closure__", None) or ()
    for cell in closure:
        value = getattr(cell, "cell_contents", None)
        if callable(value) and value is not fn:
            return value

    return fn


# ------------------------------------------------------------------
# Settings helpers
# ------------------------------------------------------------------

def _get_settings() -> dict:
    try:
        from apps.plugins.models import PluginConfig

        cfg = PluginConfig.objects.get(key=_PLUGIN_KEY, enabled=True)
        return cfg.settings or {}
    except Exception:
        return {}


def _get_wait_seconds(settings=None) -> float:
    settings = settings if settings is not None else _get_settings()
    try:
        return max(0.0, float(settings.get("wait_seconds", 2)))
    except Exception:
        return 2.0


def _get_smart_delay_enabled(settings=None) -> bool:
    settings = settings if settings is not None else _get_settings()
    return _coerce_bool(settings.get("smart_delay", True))


def _coerce_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


# ------------------------------------------------------------------
# Smart-delay decision helpers
# ------------------------------------------------------------------

def _should_apply_delay(channel_id, settings=None) -> bool:
    smart_delay = _get_smart_delay_enabled(settings)
    wait_seconds = _get_wait_seconds(settings)
    if not smart_delay:
        return True

    try:
        decision = _build_smart_delay_decision(channel_id)
    except Exception:
        _log_smart_delay_event(
            channel_id,
            wait_seconds,
            smart_delay,
            should_delay=True,
            reason="evaluation_error",
        )
        logger.exception(
            "[wait-for-selection] Smart Delay evaluation failed for %s; "
            "falling back to applying delay.",
            channel_id,
        )
        return True

    if not decision:
        _log_smart_delay_event(
            channel_id,
            wait_seconds,
            smart_delay,
            should_delay=False,
            reason="no_candidate",
        )
        logger.debug(
            "[wait-for-selection] Smart Delay could not find an eligible target "
            "stream/account for %s; skipping delay.",
            channel_id,
        )
        return False

    _log_smart_delay_event(
        channel_id,
        wait_seconds,
        smart_delay,
        should_delay=decision.get("should_delay", False),
        reason=decision.get("reason"),
        decision=decision,
    )

    logger.debug(
        "[wait-for-selection] Smart Delay decision for %s: stream=%s account=%s "
        "active=%s limit=%s delay=%s",
        channel_id,
        decision.get("stream_name") or decision.get("stream_id"),
        decision.get("account_name") or decision.get("account_id"),
        decision.get("active_count"),
        decision.get("limit"),
        decision.get("should_delay"),
    )
    return decision.get("should_delay", False)


def _build_smart_delay_decision(target_id):
    target = _get_target_object(target_id)
    candidate = _get_primary_candidate(target)
    if not candidate:
        return None

    limit = candidate["limit"]
    if limit <= 0:
        candidate.update(
            {
                "active_count": 0,
                "active_channels": [],
                "reason": "unlimited",
                "should_delay": False,
            }
        )
        return candidate

    active_channels = _get_active_channels_for_account(candidate["account_id"])
    active_count = len(active_channels)
    candidate.update(
        {
            "active_count": active_count,
            "active_channels": active_channels,
            "reason": "at_or_over_limit" if active_count >= limit else "below_limit",
            "should_delay": active_count >= limit,
        }
    )
    return candidate


def _get_target_object(target_id):
    from apps.proxy.ts_proxy.url_utils import get_stream_object

    return get_stream_object(target_id)


def _get_primary_candidate(target):
    from apps.channels.models import Channel, Stream

    if isinstance(target, Stream):
        return _build_stream_candidate(target)

    if isinstance(target, Channel):
        for stream in (
            target.streams.all()
            .select_related("m3u_account")
            .order_by("channelstream__order")
        ):
            candidate = _build_stream_candidate(stream)
            if candidate:
                candidate["channel_id"] = str(target.uuid)
                candidate["channel_name"] = target.name
                return candidate

    return None


def _build_stream_candidate(stream):
    m3u_account = getattr(stream, "m3u_account", None)
    if not m3u_account or not m3u_account.is_active:
        return None

    profile = _get_default_active_profile(m3u_account)
    if not profile:
        return None

    return {
        "stream_id": stream.id,
        "stream_name": stream.name,
        "account_id": m3u_account.id,
        "account_name": m3u_account.name,
        "profile_id": profile.id,
        "profile_name": profile.name,
        "limit": _resolve_stream_limit(m3u_account, profile),
    }


def _get_default_active_profile(m3u_account):
    return m3u_account.profiles.filter(is_active=True, is_default=True).first()


def _resolve_stream_limit(m3u_account, profile) -> int:
    account_limit = int(getattr(m3u_account, "max_streams", 0) or 0)
    profile_limit = int(getattr(profile, "max_streams", 0) or 0)
    return account_limit if account_limit > 0 else profile_limit


def _get_active_channels_for_account(account_id):
    from apps.channels.models import Stream

    active_channels = _collect_active_channel_statuses()
    stream_ids = [item["stream_id"] for item in active_channels]
    if not stream_ids:
        return []

    streams_by_id = {
        stream.id: stream
        for stream in Stream.objects.filter(id__in=stream_ids).select_related(
            "m3u_account"
        )
    }

    matches = []
    for item in active_channels:
        stream = streams_by_id.get(item["stream_id"])
        if not stream or stream.m3u_account_id != account_id:
            continue
        matches.append(item)

    return matches


def _collect_active_channel_statuses():
    from apps.proxy.ts_proxy.channel_status import ChannelStatus
    from apps.proxy.ts_proxy.server import ProxyServer

    proxy_server = ProxyServer.get_instance()
    redis_client = getattr(proxy_server, "redis_client", None)
    if not redis_client:
        return []

    channel_pattern = "ts_proxy:channel:*:metadata"
    active_channels = []
    cursor = 0

    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=channel_pattern, count=100)
        for key in keys:
            channel_id = _extract_channel_id(key)
            if not channel_id:
                continue

            channel_info = ChannelStatus.get_basic_channel_info(channel_id)
            if not channel_info:
                continue

            state = channel_info.get("state")
            stream_id = channel_info.get("stream_id")
            if state not in _OCCUPYING_CHANNEL_STATES or not stream_id:
                continue

            active_channels.append(
                {
                    "channel_id": channel_id,
                    "state": state,
                    "stream_id": int(stream_id),
                    "stream_name": channel_info.get("stream_name"),
                }
            )

        if cursor == 0:
            break

    return active_channels


def _extract_channel_id(key) -> str:
    if isinstance(key, bytes):
        key = key.decode("utf-8")

    parts = key.split(":")
    if len(parts) != 4:
        return ""

    if parts[0] != "ts_proxy" or parts[1] != "channel" or parts[3] != "metadata":
        return ""

    return parts[2]


def _log_smart_delay_event(
    channel_id,
    wait_seconds,
    smart_delay,
    should_delay,
    reason,
    decision=None,
):
    try:
        from core.utils import log_system_event

        decision = decision or {}
        log_system_event(
            "wait_for_selection_decision",
            channel_id=decision.get("channel_id") or channel_id,
            channel_name=decision.get("channel_name"),
            wait_seconds=wait_seconds,
            smart_delay=smart_delay,
            should_delay=should_delay,
            reason=reason,
            target_stream_id=decision.get("stream_id"),
            target_stream_name=decision.get("stream_name"),
            account_id=decision.get("account_id"),
            account_name=decision.get("account_name"),
            active_count=decision.get("active_count"),
            limit=decision.get("limit"),
        )
    except Exception:
        logger.exception(
            "[wait-for-selection] Failed to log smart-delay system event for %s",
            channel_id,
        )
