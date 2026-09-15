from __future__ import annotations

from typing import Any

from astrbot.api import logger

try:
    from .user_stats import (
        _build_user_usage_window,
        _normalize_user_group_id,
        _sanitize_user_id,
    )
except ImportError:  # pragma: no cover - compatible with direct module loading.
    from user_stats import (  # type: ignore[no-redef]
        _build_user_usage_window,
        _normalize_user_group_id,
        _sanitize_user_id,
    )


class UserLimitMixin:
    """Per-user daily token limit checks for QQ group LLM requests.

    Tiers are resolved via ``SponsorMixin``:
    - super admin: no limit (usage only recorded, never blocked);
    - sponsor: uses ``sponsor_daily_token_limit`` (``-1`` = unlimited);
    - normal: uses ``user_daily_token_limit`` (``-1`` = unlimited).
    """

    def _user_daily_limit(self) -> int:
        try:
            return max(-1, int(self._config_value("user_daily_token_limit")))
        except (TypeError, ValueError):
            return -1

    def _user_daily_limit_enabled(self) -> bool:
        # User-scope limiting is active when either user or sponsor tier has a limit.
        return self._user_daily_limit() >= 0 or self._sponsor_daily_limit() >= 0

    async def _user_usage_total_for_event(
        self,
        event: Any,
        user_id: str | None = None,
        nickname: str | None = None,
    ) -> dict[str, Any] | None:
        if not self._user_daily_limit_enabled():
            return None
        if not self._is_enabled():
            return None
        if not self._is_qq_group_event(event):
            return None

        get_event_group_id = getattr(self, "_event_group_id", None)
        group_id = _normalize_user_group_id(
            get_event_group_id(event)
            if callable(get_event_group_id)
            else event.get_group_id()
        )
        if not group_id or group_id not in self._limited_groups():
            return None

        if user_id is None:
            user_id = _sanitize_user_id(self._event_user_id(event))
        else:
            user_id = _sanitize_user_id(user_id)
        if not user_id:
            return None

        limit, tier = self._user_effective_limit(user_id)
        if limit is None:
            # Super admin, or this tier is set to unlimited (-1).
            return None

        stats = await self._maybe_sync_user_stats(force=True, group_id=group_id)
        window = _build_user_usage_window(self._config_value("refresh_time"))
        group_data = stats.get("groups", {}).get(group_id, {})
        if not isinstance(group_data, dict):
            group_data = {}
        realtime_totals = await self._query_user_totals_for_group(
            group_id,
            window,
            group_data,
        )
        stored_totals = self._stored_user_totals_for_group(stats, group_id, window)
        totals = self._combine_user_totals(realtime_totals, stored_totals)
        return {
            "group_id": group_id,
            "user_id": user_id,
            "tier": tier,
            "nickname": nickname or self._event_user_name(event) or user_id,
            "used": max(0, int(totals.get(user_id, 0) or 0)),
            "limit": limit,
            "window": window,
        }

    async def _should_block_user_daily_limit(self, event: Any) -> dict[str, Any] | None:
        limit_state = await self._user_usage_total_for_event(event)
        if not limit_state:
            return None
        limit = int(limit_state["limit"])
        used = int(limit_state["used"])
        if used < limit:
            return None
        return limit_state

    async def _block_user_daily_limit_if_needed(self, event: Any, stage: str) -> bool:
        limit_state = await self._should_block_user_daily_limit(event)
        if not limit_state:
            return False
        event.stop_event()
        logger.info(
            "Blocked user LLM request by per-user token limit: "
            "stage=%s group=%s user=%s tier=%s used=%s limit=%s",
            stage,
            limit_state["group_id"],
            limit_state["user_id"],
            limit_state.get("tier", "normal"),
            limit_state["used"],
            limit_state["limit"],
        )
        await self._send_user_limit_message(event, limit_state)
        return True