"""
Smart Channel Switcherr plugin.

Monkey-patches apps.proxy.ts_proxy.views.stream_ts and
apps.proxy.ts_proxy.views.generate_stream_url to insert an optional delay
before the FIRST stream source selection per request. Retries within the
same request (same greenlet) are not delayed.

If Smart Delay is enabled, the plugin only waits when the target channel's
first eligible M3U account is already at its concurrent limit, based on the
same Redis-backed channel metadata scan used by /proxy/ts/status, and the
request comes from the same client (IP + User-Agent) that already has an
active connection on that account.

Install: copy this directory to /data/plugins/smart-channel-switcherr/
"""
from functools import wraps
import logging

logger = logging.getLogger(__name__)

_GENERATE_PATCH_MARKER = "_smart_channel_switcherr_generate_patched"
_ORIGINAL_GENERATE_FN_ATTR = "_smart_channel_switcherr_original_generate_fn"
_STREAM_TS_PATCH_MARKER = "_smart_channel_switcherr_stream_ts_patched"
_ORIGINAL_STREAM_TS_ATTR = "_smart_channel_switcherr_original_stream_ts"
_PLUGIN_KEY = "smart-channel-switcherr"

# States that mean a channel is holding an active provider connection.
_OCCUPYING_CHANNEL_STATES = {
    "active",
    "buffering",
    "connecting",
    "initializing",
    "stopping",
    "waiting_for_clients",
}


def _build_request_state():
    try:
        from gevent.local import local as _local
    except ImportError:
        import threading

        _local = threading.local
        logger.warning(
            "[smart-channel-switcherr] gevent not found; falling back to "
            "threading.local(). Per-request delay behaviour may differ."
        )

    return _local()


_REQUEST_STATE = _build_request_state()


class Plugin:
    name = "Smart Channel Switcherr"
    description = (
        "Delays stream source selection to allow recently-stopped channels "
        "to fully release their provider before a new assignment runs. "
        "Optional Smart Delay only waits when the target provider is already "
        "at its concurrent limit for the same client."
    )
    fields = []
    actions = []

    def __init__(self):
        self._original_generate_stream_url = None
        self._original_stream_ts = None
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
                "[smart-channel-switcherr] Cannot import apps.proxy.ts_proxy.views; "
                "plugin will have no effect."
            )
            return

        current_generate = proxy_views.generate_stream_url
        current_stream_ts = proxy_views.stream_ts
        original_generate = _unwrap_original(
            current_generate, _ORIGINAL_GENERATE_FN_ATTR
        )
        original_stream_ts = _unwrap_original(
            current_stream_ts, _ORIGINAL_STREAM_TS_ATTR
        )

        if getattr(current_generate, _GENERATE_PATCH_MARKER, False):
            logger.info(
                "[smart-channel-switcherr] Replacing existing generate_stream_url patch."
            )
        if getattr(current_stream_ts, _STREAM_TS_PATCH_MARKER, False):
            logger.warning(
                "[smart-channel-switcherr] Replacing existing stream_ts patch."
            )

        @wraps(original_stream_ts)
        def _patched_stream_ts(request, channel_id, *args, **kwargs):
            previous_state = _snapshot_request_state()
            client_ip, client_user_agent = _extract_request_client_identity(request)
            _set_request_state(
                delay_applied=False,
                client_ip=client_ip,
                client_user_agent=client_user_agent,
            )
            try:
                return original_stream_ts(request, channel_id, *args, **kwargs)
            finally:
                _restore_request_state(previous_state)

        @wraps(original_generate)
        def _patched_generate_stream_url(channel_id, *args, **kwargs):
            if not getattr(_REQUEST_STATE, "delay_applied", False):
                _REQUEST_STATE.delay_applied = True
                settings = _get_settings()
                wait = _get_wait_seconds(settings)
                if wait > 0 and _should_apply_delay(channel_id, settings):
                    _sleep_before_selection(wait, channel_id)
            return original_generate(channel_id, *args, **kwargs)

        setattr(_patched_stream_ts, _STREAM_TS_PATCH_MARKER, True)
        setattr(_patched_stream_ts, _ORIGINAL_STREAM_TS_ATTR, original_stream_ts)
        setattr(_patched_generate_stream_url, _GENERATE_PATCH_MARKER, True)
        setattr(
            _patched_generate_stream_url,
            _ORIGINAL_GENERATE_FN_ATTR,
            original_generate,
        )
        self._original_stream_ts = original_stream_ts
        self._original_generate_stream_url = original_generate
        self._proxy_views = proxy_views
        proxy_views.stream_ts = _patched_stream_ts
        proxy_views.generate_stream_url = _patched_generate_stream_url
        logger.info(
            "[smart-channel-switcherr] Patches applied to stream_ts and "
            "generate_stream_url."
        )

    def _remove_patch(self):
        views = self._proxy_views
        if not views:
            return

        current_generate = getattr(views, "generate_stream_url", None)
        current_stream_ts = getattr(views, "stream_ts", None)
        if current_generate is not None and getattr(
            current_generate, _GENERATE_PATCH_MARKER, False
        ):
            views.generate_stream_url = self._original_generate_stream_url
            logger.info("[smart-channel-switcherr] Patch removed from generate_stream_url.")
        if current_stream_ts is not None and getattr(
            current_stream_ts, _STREAM_TS_PATCH_MARKER, False
        ):
            views.stream_ts = self._original_stream_ts
            logger.info("[smart-channel-switcherr] Patch removed from stream_ts.")
        self._original_generate_stream_url = None
        self._original_stream_ts = None
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
            "[smart-channel-switcherr] Sleeping %.2fs before stream selection for %s",
            wait,
            channel_id,
        )
        gevent.sleep(wait)
    except ImportError:
        import time

        time.sleep(wait)


def _unwrap_original(fn, original_attr):
    original = getattr(fn, original_attr, None)
    if callable(original):
        return original

    closure = getattr(fn, "__closure__", None) or ()
    for cell in closure:
        value = getattr(cell, "cell_contents", None)
        if callable(value) and value is not fn:
            return value

    return fn


def _snapshot_request_state():
    return {
        "delay_applied": getattr(_REQUEST_STATE, "delay_applied", None),
        "client_ip": getattr(_REQUEST_STATE, "client_ip", None),
        "client_user_agent": getattr(_REQUEST_STATE, "client_user_agent", None),
    }


def _set_request_state(delay_applied, client_ip, client_user_agent):
    _REQUEST_STATE.delay_applied = delay_applied
    _REQUEST_STATE.client_ip = client_ip
    _REQUEST_STATE.client_user_agent = client_user_agent


def _restore_request_state(previous_state):
    for attr, value in previous_state.items():
        if value is None:
            try:
                delattr(_REQUEST_STATE, attr)
            except AttributeError:
                pass
        else:
            setattr(_REQUEST_STATE, attr, value)


def _extract_request_client_identity(request):
    try:
        from apps.proxy.ts_proxy.utils import get_client_ip

        client_ip = _normalize_client_value(get_client_ip(request))
    except Exception:
        client_ip = ""

    client_user_agent = ""
    meta = getattr(request, "META", {}) or {}
    for header in ["HTTP_USER_AGENT", "User-Agent", "user-agent"]:
        if header in meta:
            client_user_agent = _normalize_client_value(meta.get(header))
            if client_user_agent:
                break

    return client_ip, client_user_agent


def _get_request_client_identity():
    return {
        "client_ip": _normalize_client_value(getattr(_REQUEST_STATE, "client_ip", "")),
        "client_user_agent": _normalize_client_value(
            getattr(_REQUEST_STATE, "client_user_agent", "")
        ),
    }


def _normalize_client_value(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


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
            "[smart-channel-switcherr] Smart Delay evaluation failed for %s; "
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
            "[smart-channel-switcherr] Smart Delay could not find an eligible target "
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
        "[smart-channel-switcherr] Smart Delay decision for %s: stream=%s account=%s "
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
    from apps.proxy.ts_proxy.url_utils import get_stream_object

    target = get_stream_object(target_id)
    candidate = _get_primary_candidate(target)
    if not candidate:
        return None

    limit = candidate["limit"]
    if limit <= 0:
        candidate.update(
            {
                "active_count": 0,
                "active_channels": [],
                "matching_client_found": False,
                "reason": "unlimited",
                "should_delay": False,
            }
        )
        return candidate

    request_client = _get_request_client_identity()
    candidate.update(request_client)
    if not request_client["client_ip"] or not request_client["client_user_agent"]:
        candidate.update(
            {
                "active_count": 0,
                "active_channels": [],
                "matching_client_found": False,
                "reason": "missing_request_client_identity",
                "should_delay": False,
            }
        )
        return candidate

    active_channels = _get_active_channels_for_account(candidate["account_id"])
    active_count = len(active_channels)
    matching_channel = _find_matching_client_channel(
        active_channels,
        request_client["client_ip"],
        request_client["client_user_agent"],
    )
    candidate.update(
        {
            "active_count": active_count,
            "active_channels": active_channels,
            "matching_client_found": bool(matching_channel),
            "matching_channel_id": matching_channel.get("channel_id")
            if matching_channel
            else None,
            "matching_stream_id": matching_channel.get("stream_id")
            if matching_channel
            else None,
            "reason": _build_delay_reason(active_count, limit, matching_channel),
            "should_delay": active_count >= limit and bool(matching_channel),
        }
    )
    return candidate


def _build_delay_reason(active_count, limit, matching_channel) -> str:
    if active_count < limit:
        return "below_limit"
    if not matching_channel:
        return "no_matching_client"
    return "at_or_over_limit_same_client"


def _get_primary_candidate(target):
    from apps.channels.models import Channel, Stream

    if isinstance(target, Stream):
        return _build_stream_candidate(target)

    if isinstance(target, Channel):
        # prefetch_related avoids an extra query per stream for profile lookup
        streams = (
            target.streams.all()
            .select_related("m3u_account")
            .prefetch_related("m3u_account__profiles")
            .order_by("channelstream__order")
        )
        for stream in streams:
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
    # max_streams can be None (DB nullable), so `or 0` normalises None → 0.
    # Account-level limit takes precedence; profile limit is the fallback.
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

    return [
        item
        for item in active_channels
        if (s := streams_by_id.get(item["stream_id"])) and s.m3u_account_id == account_id
    ]


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
            stream_id_raw = channel_info.get("stream_id")
            if state not in _OCCUPYING_CHANNEL_STATES or not stream_id_raw:
                continue

            # FIX: guard against non-numeric stream_id values from Redis
            try:
                stream_id = int(stream_id_raw)
            except (ValueError, TypeError):
                logger.warning(
                    "[smart-channel-switcherr] Non-numeric stream_id %r in metadata for "
                    "channel %s; skipping.",
                    stream_id_raw,
                    channel_id,
                )
                continue

            active_channels.append(
                {
                    "channel_id": channel_id,
                    "state": state,
                    "stream_id": stream_id,
                    "stream_name": channel_info.get("stream_name"),
                    "clients": _extract_channel_clients(channel_info.get("clients")),
                }
            )

        if cursor == 0:
            break

    return active_channels


def _extract_channel_clients(clients):
    normalized_clients = []
    for client in clients or []:
        client_ip = _normalize_client_value(client.get("ip_address"))
        client_user_agent = _normalize_client_value(client.get("user_agent"))
        if not client_ip and not client_user_agent:
            continue
        normalized_clients.append(
            {
                "ip_address": client_ip,
                "user_agent": client_user_agent,
            }
        )
    return normalized_clients


def _find_matching_client_channel(active_channels, client_ip, client_user_agent):
    for channel in active_channels:
        for client in channel.get("clients", []):
            if (
                client.get("ip_address") == client_ip
                and client.get("user_agent") == client_user_agent
            ):
                return channel
    return None


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
            request_client_ip=decision.get("client_ip"),
            request_client_user_agent=decision.get("client_user_agent"),
            matching_client_found=decision.get("matching_client_found"),
            matching_channel_id=decision.get("matching_channel_id"),
        )
    except Exception:
        logger.exception(
            "[smart-channel-switcherr] Failed to log smart-delay system event for %s",
            channel_id,
        )
