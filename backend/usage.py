from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any

from quart import request
from sqlmodel import col, func, select

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.provider import ProviderRequest
from astrbot.core.db.po import ProviderStat
from astrbot.core.message.message_event_result import MessageChain
from astrbot.core.platform.message_type import MessageType


try:
    from .config import (
    PLUGIN_NAME,
    LEGACY_PLUGIN_NAME,
    GROUP_REMARKS_FILE,
    GROUP_LIMITS_FILE,
    PLUGIN_DATA_FILES,
    MAX_GROUP_REMARK_LENGTH,
    GROUP_SETTING_DAILY_LIMIT,
    GROUP_SETTING_ONLY_AT_BOT,
    GROUP_SETTING_CONTEXT_LIMIT_05,
    GROUP_SETTING_PROMPT_CACHE_KEY_STRATEGY,
    GROUP_SETTING_DEEPSEEK_CACHE_STRATEGY,
    GROUP_SETTINGS_CONFIG_BACKUP_KEY,
    TOKEN_LIMIT_CONTEXT_RATIO,
    TOKEN_LIMIT_CONTEXT_TRIM_RATIO,
    TOKEN_LIMIT_CONTEXT_COMPRESS_THRESHOLD,
    TOKEN_LIMIT_CONTEXT_FALLBACK_TURNS,
    TOKEN_LIMIT_TEMP_PROVIDER_PREFIX,
    TOKEN_LIMIT_OPENAI_CACHE_RETENTION_MODELS,
    TOKEN_LIMIT_PROMPT_CACHE_ANCHOR_VERSION,
    TOKEN_LIMIT_PROMPT_CACHE_ANCHOR_LINES,
    TOKEN_LIMIT_PROMPT_CACHE_PREFIX_TARGET,
    TOKEN_LIMIT_IMAGE_TOKEN_ESTIMATE,
    TOKEN_LIMIT_AUDIO_TOKEN_ESTIMATE,
    OVER_LIMIT_STOP,
    OVER_LIMIT_FALLBACK,
    TOKEN_FIELDS_SUM,
    CONFIG_SCHEMA,
    UsageWindow,
    TOKEN_LIMIT_PROMPT_CACHE_ANCHOR,
    _ok,
    _error,
    _split_group_values,
    _normalize_group_id,
    _escape_like,
    _format_tokens,
    _format_context_limit_tokens,
    _parse_refresh_time,
    _local_timezone,
    _build_usage_window,
    StarTools,
    )
except ImportError:  # pragma: no cover - compatible with direct module loading.
    from config import (
    PLUGIN_NAME,
    LEGACY_PLUGIN_NAME,
    GROUP_REMARKS_FILE,
    GROUP_LIMITS_FILE,
    PLUGIN_DATA_FILES,
    MAX_GROUP_REMARK_LENGTH,
    GROUP_SETTING_DAILY_LIMIT,
    GROUP_SETTING_ONLY_AT_BOT,
    GROUP_SETTING_CONTEXT_LIMIT_05,
    GROUP_SETTING_PROMPT_CACHE_KEY_STRATEGY,
    GROUP_SETTING_DEEPSEEK_CACHE_STRATEGY,
    GROUP_SETTINGS_CONFIG_BACKUP_KEY,
    TOKEN_LIMIT_CONTEXT_RATIO,
    TOKEN_LIMIT_CONTEXT_TRIM_RATIO,
    TOKEN_LIMIT_CONTEXT_COMPRESS_THRESHOLD,
    TOKEN_LIMIT_CONTEXT_FALLBACK_TURNS,
    TOKEN_LIMIT_TEMP_PROVIDER_PREFIX,
    TOKEN_LIMIT_OPENAI_CACHE_RETENTION_MODELS,
    TOKEN_LIMIT_PROMPT_CACHE_ANCHOR_VERSION,
    TOKEN_LIMIT_PROMPT_CACHE_ANCHOR_LINES,
    TOKEN_LIMIT_PROMPT_CACHE_PREFIX_TARGET,
    TOKEN_LIMIT_IMAGE_TOKEN_ESTIMATE,
    TOKEN_LIMIT_AUDIO_TOKEN_ESTIMATE,
    OVER_LIMIT_STOP,
    OVER_LIMIT_FALLBACK,
    TOKEN_FIELDS_SUM,
    CONFIG_SCHEMA,
    UsageWindow,
    TOKEN_LIMIT_PROMPT_CACHE_ANCHOR,
    _ok,
    _error,
    _split_group_values,
    _normalize_group_id,
    _escape_like,
    _format_tokens,
    _format_context_limit_tokens,
    _parse_refresh_time,
    _local_timezone,
    _build_usage_window,
    StarTools,
    )



class UsageMixin:

    def _umo_candidates_for_group(self, group_id: str) -> list[str]:
        candidates = []
        for platform_id in self._qq_platform_ids():
            candidates.append(f"{platform_id}:{MessageType.GROUP_MESSAGE.value}:{group_id}")
        return candidates


    def _unique_session_like_patterns(self, group_id: str) -> list[str]:
        escaped_group_id = _escape_like(group_id)
        return [
            f"{_escape_like(platform_id)}:{MessageType.GROUP_MESSAGE.value}:%{escaped_group_id}%"
            for platform_id in self._qq_platform_ids()
        ]


    async def _query_usage_for_group(
        self,
        group_id: str,
        window: UsageWindow,
        provider_id: str | None = None,
        exclude_provider_id: str | None = None,
    ) -> tuple[int, dict[str, int]]:
        db = self.context.get_db()
        umo_candidates = self._umo_candidates_for_group(group_id)
        filters = [
            ProviderStat.agent_type == "internal",
            ProviderStat.created_at >= window.start_utc,
            ProviderStat.created_at < window.end_utc,
        ]
        if provider_id:
            filters.append(ProviderStat.provider_id == provider_id)
        if exclude_provider_id:
            filters.append(ProviderStat.provider_id != exclude_provider_id)

        if bool(self._config_value("match_unique_session")):
            umo_filter = ProviderStat.umo.in_(umo_candidates)
            for pattern in self._unique_session_like_patterns(group_id):
                umo_filter = umo_filter | ProviderStat.umo.like(
                    pattern,
                    escape="\\",
                )
            filters.append(umo_filter)
        else:
            filters.append(ProviderStat.umo.in_(umo_candidates))

        async with db.get_db() as session:
            hourly: dict[str, int] = {}
            total = 0
            database_url = str(getattr(db, "DATABASE_URL", "") or "").lower()
            if "sqlite" in database_url:
                bucket_expr = func.strftime(
                    "%Y-%m-%dT%H:00:00+00:00",
                    ProviderStat.created_at,
                )
                rows_result = await session.execute(
                    select(
                        bucket_expr.label("bucket"),
                        func.coalesce(func.sum(TOKEN_FIELDS_SUM), 0).label("tokens"),
                    )
                    .where(*filters)
                    .group_by(bucket_expr)
                    .order_by(bucket_expr.asc())
                )
                for bucket, tokens in rows_result.all():
                    if not bucket:
                        continue
                    bucket_utc = datetime.fromisoformat(str(bucket))
                    if bucket_utc.tzinfo is None:
                        bucket_utc = bucket_utc.replace(tzinfo=timezone.utc)
                    else:
                        bucket_utc = bucket_utc.astimezone(timezone.utc)
                    bucket_local = bucket_utc.astimezone(window.start_local.tzinfo)
                    normalized_tokens = int(tokens or 0)
                    hourly[bucket_local.isoformat()] = normalized_tokens
                    total += normalized_tokens
            else:
                total_result = await session.execute(
                    select(func.coalesce(func.sum(TOKEN_FIELDS_SUM), 0)).where(*filters)
                )
                total = int(total_result.scalar_one() or 0)

                rows_result = await session.execute(
                    select(ProviderStat.created_at, TOKEN_FIELDS_SUM.label("tokens"))
                    .where(*filters)
                    .order_by(col(ProviderStat.created_at).asc())
                )
                for created_at, tokens in rows_result.all():
                    created_at_utc = (
                        created_at.replace(tzinfo=timezone.utc)
                        if created_at.tzinfo is None
                        else created_at.astimezone(timezone.utc)
                    )
                    bucket = created_at_utc.astimezone(window.start_local.tzinfo).replace(
                        minute=0,
                        second=0,
                        microsecond=0,
                    )
                    key = bucket.isoformat()
                    hourly[key] = hourly.get(key, 0) + int(tokens or 0)
        return total, hourly


    async def _query_split_usage_for_group(
        self,
        group_id: str,
        window: UsageWindow,
        fallback_provider_id: str,
    ) -> tuple[int, int, dict[str, int]]:
        if not fallback_provider_id:
            primary_used, hourly = await self._query_usage_for_group(group_id, window)
            return primary_used, 0, hourly

        primary_used, hourly = await self._query_usage_for_group(
            group_id,
            window,
            exclude_provider_id=fallback_provider_id,
        )
        fallback_used, _ = await self._query_usage_for_group(
            group_id,
            window,
            provider_id=fallback_provider_id,
        )
        return primary_used, fallback_used, hourly


    @staticmethod
    def _build_limit_state(
        limit: int,
        policy: dict[str, Any],
        primary_used: int,
        fallback_used: int,
        fallback_configured: bool,
    ) -> dict[str, Any]:
        used = primary_used + fallback_used
        action = str(policy.get("action") or OVER_LIMIT_STOP)
        fallback_token_limit = max(0, int(policy.get("fallback_token_limit") or 0))
        hard_limit = limit
        effective_limit = limit
        using_fallback = False
        stopped = False
        status = "normal"

        if limit > 0:
            if action == OVER_LIMIT_FALLBACK and fallback_configured:
                hard_limit = limit + fallback_token_limit
                if used >= limit:
                    effective_limit = hard_limit
                    if used >= hard_limit:
                        stopped = True
                        status = "stopped"
                    else:
                        using_fallback = True
                        status = "fallback"
            elif used >= limit:
                stopped = True
                status = "stopped"

        percent = (
            0
            if effective_limit <= 0
            else min(100, round((used / effective_limit) * 100, 2))
        )
        return {
            "used": used,
            "hard_limit": hard_limit,
            "effective_limit": effective_limit,
            "percent": percent,
            "using_fallback": using_fallback,
            "stopped": stopped,
            "status": status,
        }


    async def _build_event_limit_context(
        self,
        event: AstrMessageEvent,
    ) -> dict[str, Any] | None:
        if not self._is_enabled():
            return None
        if not self._is_qq_group_event(event):
            return None

        group_id = self._event_group_id(event)
        if not group_id or group_id not in self._limited_groups():
            return None

        limit = self._daily_limit_for_group(group_id)
        if limit <= 0:
            return None

        window = _build_usage_window(self._config_value("refresh_time"))
        policy = self._over_limit_policy()
        fallback_provider_id = (
            str(policy["fallback_provider_id"])
            if policy["action"] == OVER_LIMIT_FALLBACK
            else ""
        )
        fallback_token_limit = int(policy["fallback_token_limit"])
        fallback_configured = bool(
            policy["action"] == OVER_LIMIT_FALLBACK and fallback_token_limit > 0
        )
        fallback_provider_valid = bool(
            fallback_provider_id and self._fallback_provider_exists(fallback_provider_id)
        )
        primary_used, fallback_used, _ = await self._query_split_usage_for_group(
            group_id,
            window,
            fallback_provider_id,
        )
        limit_state = self._build_limit_state(
            limit,
            policy,
            primary_used,
            fallback_used,
            fallback_configured,
        )
        return {
            "group_id": group_id,
            "limit": limit,
            "window": window,
            "policy": policy,
            "fallback_provider_id": fallback_provider_id,
            "fallback_token_limit": fallback_token_limit,
            "fallback_configured": fallback_configured,
            "fallback_provider_valid": fallback_provider_valid,
            "primary_used": primary_used,
            "fallback_used": fallback_used,
            "limit_state": limit_state,
        }


    async def _build_usage_payload(self) -> dict[str, Any]:
        groups = self._limited_groups()
        global_limit = self._daily_limit()
        group_settings = self._load_group_settings()
        group_limits = {
            group_id: int(settings[GROUP_SETTING_DAILY_LIMIT])
            for group_id, settings in group_settings.items()
            if GROUP_SETTING_DAILY_LIMIT in settings
        }
        policy = self._over_limit_policy()
        fallback_provider_id = (
            str(policy["fallback_provider_id"])
            if policy["action"] == OVER_LIMIT_FALLBACK
            else ""
        )
        fallback_token_limit = int(policy["fallback_token_limit"])
        fallback_configured = bool(
            policy["action"] == OVER_LIMIT_FALLBACK and fallback_token_limit > 0
        )
        fallback_enabled = bool(
            fallback_provider_id and self._fallback_provider_exists(fallback_provider_id)
        )
        window = _build_usage_window(self._config_value("refresh_time"))
        remarks = self._load_group_remarks()
        group_names = self._load_group_names()
        items = []
        for group_id in groups:
            limit = self._daily_limit_for_group(group_id, group_limits)
            has_custom_limit = group_id in group_limits
            only_at_bot_llm = self._group_only_at_bot_llm(group_id, group_settings)
            context_limit_05 = self._group_context_limit_05(group_id, group_settings)
            prompt_cache_key_strategy = self._group_prompt_cache_key_strategy(
                group_id,
                group_settings,
            )
            deepseek_cache_strategy = self._group_deepseek_cache_strategy(
                group_id,
                group_settings,
            )
            context_limit_tokens = self._group_context_limit_tokens(
                group_id,
                group_settings,
                group_limits,
            )
            primary_used, fallback_used, hourly = await self._query_split_usage_for_group(
                group_id,
                window,
                fallback_provider_id,
            )
            limit_state = self._build_limit_state(
                limit,
                policy,
                primary_used,
                fallback_used,
                fallback_configured,
            )
            used = int(limit_state["used"])
            effective_limit = int(limit_state["effective_limit"])
            hard_limit = int(limit_state["hard_limit"])
            items.append(
                {
                    "group_id": group_id,
                    "group_name": group_names.get(group_id, ""),
                    "remark": remarks.get(group_id, ""),
                    "used_tokens": used,
                    "primary_used_tokens": primary_used,
                    "fallback_used_tokens": fallback_used,
                    "limit_tokens": effective_limit,
                    "hard_limit_tokens": hard_limit,
                    "primary_limit_tokens": limit,
                    "global_limit_tokens": global_limit,
                    "custom_limit_tokens": group_limits.get(group_id),
                    "has_custom_limit": has_custom_limit,
                    "only_at_bot_llm": only_at_bot_llm,
                    "context_limit_05": context_limit_05,
                    "prompt_cache_key_strategy": prompt_cache_key_strategy,
                    "deepseek_cache_strategy": deepseek_cache_strategy,
                    "context_limit_tokens": context_limit_tokens,
                    "context_limit_display": (
                        _format_context_limit_tokens(context_limit_tokens)
                        if context_limit_05
                        else _format_context_limit_tokens(
                            max(1, int(limit * TOKEN_LIMIT_CONTEXT_RATIO))
                        )
                    ),
                    "fallback_limit_tokens": fallback_token_limit,
                    "used_display": _format_tokens(used),
                    "limit_display": _format_tokens(effective_limit),
                    "hard_limit_display": _format_tokens(hard_limit),
                    "primary_limit_display": _format_tokens(limit),
                    "global_limit_display": _format_tokens(global_limit),
                    "custom_limit_display": (
                        _format_tokens(group_limits[group_id]) if has_custom_limit else ""
                    ),
                    "fallback_limit_display": _format_tokens(fallback_token_limit),
                    "percent": limit_state["percent"],
                    "limited": bool(limit_state["stopped"]),
                    "using_fallback": bool(
                        limit_state["using_fallback"] and fallback_enabled
                    ),
                    "fallback_unavailable": bool(
                        limit_state["using_fallback"] and not fallback_enabled
                    ),
                    "stopped": bool(limit_state["stopped"]),
                    "status": limit_state["status"],
                    "fallback_provider_id": fallback_provider_id,
                    "hourly": hourly,
                }
            )

        return {
            "enabled": self._is_enabled(),
            "groups": items,
            "remarks": remarks,
            "group_limits": group_limits,
            "group_settings": group_settings,
            "global_daily_token_limit": global_limit,
            "global_daily_token_limit_display": _format_tokens(global_limit),
            "over_limit_policy": {
                **policy,
                "fallback_configured": fallback_configured,
                "fallback_enabled": fallback_enabled,
            },
            "window": {
                "start": window.start_local.isoformat(),
                "end": window.end_local.isoformat(),
                "refresh_time": str(self._config_value("refresh_time") or "00:00"),
            },
        }
