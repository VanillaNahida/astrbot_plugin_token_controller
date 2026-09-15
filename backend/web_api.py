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



class WebApiMixin:

    async def api_get_config(self) -> dict:
        return _ok(
            {
                "config": self._serialize_config(),
                "schema": self._config_schema_for_page(),
            }
        )


    async def api_save_config(self) -> dict:
        payload = await request.get_json(silent=True)
        if not isinstance(payload, dict):
            return _error("请求体必须是 JSON 对象。")
        raw_config = payload.get("config", payload)
        if not isinstance(raw_config, dict):
            return _error("config 必须是 JSON 对象。")
        try:
            next_config = self._sanitize_config(raw_config)
        except ValueError as exc:
            return _error(str(exc))
        group_settings_backup = self._load_group_settings()
        self.config.clear()
        self.config.update(next_config)
        self._set_group_settings_config_backup(
            group_settings_backup,
            save_config=False,
        )
        save_config = getattr(self.config, "save_config", None)
        if callable(save_config):
            save_config()
        await self._maybe_sync_history_stats(force=True)
        await self._maybe_sync_user_stats(force=True)
        return _ok(
            {
                "config": self._serialize_config(),
                "schema": self._config_schema_for_page(),
            }
        )


    async def api_get_usage(self) -> dict:
        try:
            await self._maybe_sync_history_stats()
            await self._maybe_sync_user_stats()
            try:
                await self._refresh_group_names_from_adapter(force=True)
            except Exception as exc:
                logger.info("Failed to refresh QQ group names for usage: %s", exc)
            return _ok(await self._build_usage_payload())
        except Exception as exc:
            logger.error("获取 QQ 群 token 用量失败: %s", exc, exc_info=True)
            return _error(f"获取用量失败: {exc}")


    async def api_get_managed_groups(self) -> dict:
        """列出全部可限流的 QQ 群（含群名），并标出当前已限流项。"""
        group_items = await self._all_groups_from_adapter()
        limited_ids = set(self._limited_groups())
        names = self._load_group_names()
        remarks = self._load_group_remarks()

        def _build(item_group_id: str, limited: bool) -> dict[str, Any]:
            return {
                "group_id": item_group_id,
                "group_name": str(item_group_id and names.get(item_group_id, "") or ""),
                "remark": str(item_group_id and remarks.get(item_group_id, "") or ""),
                "limited": bool(limited),
            }

        groups: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in group_items:
            item_group_id = _normalize_group_id(item.get("group_id"))
            if not item_group_id or item_group_id in seen:
                continue
            seen.add(item_group_id)
            groups.append(_build(item_group_id, item_group_id in limited_ids))
        # 确保已配置但当前可能不在适配器群列表中的限额群仍出现在弹窗中。
        for item_group_id in self._limited_groups():
            if item_group_id not in seen:
                seen.add(item_group_id)
                groups.append(_build(item_group_id, True))
        groups.sort(key=lambda item: (item["group_name"] or item["group_id"]))
        return _ok(
            {
                "groups": groups,
                "limited_groups": self._limited_groups(),
            }
        )


    async def api_save_limited_groups(self) -> dict:
        payload = await request.get_json(silent=True)
        if not isinstance(payload, dict):
            return _error("请求体必须是 JSON 对象。")
        next_groups = self._normalize_config_list(payload.get("groups"))
        self.config["limited_groups"] = next_groups
        self._save_config_preserving_group_settings()
        try:
            await self._refresh_group_names_from_adapter(force=True)
        except Exception as exc:
            logger.info("Failed to refresh group names after saving limits: %s", exc)
        return _ok({"limited_groups": self._limited_groups()})


    async def api_get_providers(self) -> dict:
        return _ok({"providers": self._provider_options()})


    async def api_get_remarks(self) -> dict:
        return _ok({"remarks": self._load_group_remarks()})


    async def api_save_remark(self) -> dict:
        payload = await request.get_json(silent=True)
        if not isinstance(payload, dict):
            return _error("Request body must be a JSON object.")

        group_id = _normalize_group_id(payload.get("group_id"))
        if not group_id:
            return _error("group_id is required.")

        remarks = self._load_group_remarks()
        remark = self._sanitize_group_remark(payload.get("remark"))
        if remark:
            remarks[group_id] = remark
        else:
            remarks.pop(group_id, None)

        try:
            self._save_group_remarks(remarks)
        except ValueError as exc:
            return _error(str(exc))

        return _ok({"group_id": group_id, "remark": remark, "remarks": remarks})


    async def api_get_group_settings(self) -> dict:
        group_id = _normalize_group_id(request.args.get("group_id"))
        if not group_id:
            return _error("group_id is required.")

        global_limit = self._daily_limit()
        group_settings = self._load_group_settings()
        group_limits = {
            item_group_id: int(settings[GROUP_SETTING_DAILY_LIMIT])
            for item_group_id, settings in group_settings.items()
            if GROUP_SETTING_DAILY_LIMIT in settings
        }
        has_custom_limit = group_id in group_limits
        effective_limit = self._daily_limit_for_group(group_id, group_limits)
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
        return _ok(
            {
                "group_id": group_id,
                "daily_token_limit": effective_limit,
                "daily_token_limit_display": _format_tokens(effective_limit),
                "global_daily_token_limit": global_limit,
                "global_daily_token_limit_display": _format_tokens(global_limit),
                "has_custom_limit": has_custom_limit,
                "custom_daily_token_limit": (
                    group_limits[group_id] if has_custom_limit else None
                ),
                "custom_daily_token_limit_display": (
                    _format_tokens(group_limits[group_id]) if has_custom_limit else ""
                ),
                "only_at_bot_llm": only_at_bot_llm,
                "context_limit_05": context_limit_05,
                "prompt_cache_key_strategy": prompt_cache_key_strategy,
                "deepseek_cache_strategy": deepseek_cache_strategy,
                "context_limit_tokens": context_limit_tokens,
                "context_limit_display": _format_context_limit_tokens(
                    context_limit_tokens
                    if context_limit_05
                    else max(1, int(effective_limit * TOKEN_LIMIT_CONTEXT_RATIO))
                ),
            }
        )


    async def api_save_group_settings(self) -> dict:
        payload = await request.get_json(silent=True)
        if not isinstance(payload, dict):
            return _error("Request body must be a JSON object.")

        group_id = _normalize_group_id(payload.get("group_id"))
        if not group_id:
            return _error("group_id is required.")

        group_settings = self._load_group_settings()
        group_limits = {
            item_group_id: int(settings[GROUP_SETTING_DAILY_LIMIT])
            for item_group_id, settings in group_settings.items()
            if GROUP_SETTING_DAILY_LIMIT in settings
        }
        if bool(payload.get("reset")):
            group_settings.setdefault(group_id, {}).pop(GROUP_SETTING_DAILY_LIMIT, None)
            if not group_settings[group_id]:
                group_settings.pop(group_id, None)
            try:
                self._save_group_settings(group_settings)
            except ValueError as exc:
                return _error(str(exc))

            group_limits.pop(group_id, None)
            global_limit = self._daily_limit()
            return _ok(
                {
                    "group_id": group_id,
                    "daily_token_limit": global_limit,
                    "daily_token_limit_display": _format_tokens(global_limit),
                    "global_daily_token_limit": global_limit,
                    "global_daily_token_limit_display": _format_tokens(global_limit),
                    "has_custom_limit": False,
                    "custom_daily_token_limit": None,
                    "custom_daily_token_limit_display": "",
                    "group_limits": group_limits,
                    "group_settings": group_settings,
                    "only_at_bot_llm": self._group_only_at_bot_llm(
                        group_id,
                        group_settings,
                    ),
                    "context_limit_05": self._group_context_limit_05(
                        group_id,
                        group_settings,
                    ),
                    "prompt_cache_key_strategy": self._group_prompt_cache_key_strategy(
                        group_id,
                        group_settings,
                    ),
                    "deepseek_cache_strategy": self._group_deepseek_cache_strategy(
                        group_id,
                        group_settings,
                    ),
                    "context_limit_tokens": self._group_context_limit_tokens(
                        group_id,
                        group_settings,
                        group_limits,
                    ),
                    "context_limit_display": _format_context_limit_tokens(
                        max(1, int(global_limit * TOKEN_LIMIT_CONTEXT_RATIO))
                    ),
                }
            )

        current_group_settings = dict(group_settings.get(group_id, {}))
        if "daily_token_limit" in payload:
            try:
                next_daily_limit = max(
                    0,
                    int(payload.get("daily_token_limit") or 0),
                )
            except (TypeError, ValueError):
                return _error("daily_token_limit must be an integer.")
            if (
                GROUP_SETTING_DAILY_LIMIT not in current_group_settings
                and next_daily_limit == self._daily_limit()
            ):
                current_group_settings.pop(GROUP_SETTING_DAILY_LIMIT, None)
            else:
                current_group_settings[GROUP_SETTING_DAILY_LIMIT] = next_daily_limit
        if "only_at_bot_llm" in payload:
            current_group_settings[GROUP_SETTING_ONLY_AT_BOT] = bool(
                payload.get("only_at_bot_llm")
            )
            if not current_group_settings[GROUP_SETTING_ONLY_AT_BOT]:
                current_group_settings.pop(GROUP_SETTING_ONLY_AT_BOT, None)
        if "context_limit_05" in payload:
            current_group_settings[GROUP_SETTING_CONTEXT_LIMIT_05] = bool(
                payload.get("context_limit_05")
            )
            if not current_group_settings[GROUP_SETTING_CONTEXT_LIMIT_05]:
                current_group_settings.pop(GROUP_SETTING_CONTEXT_LIMIT_05, None)
        if "prompt_cache_key_strategy" in payload:
            current_group_settings[GROUP_SETTING_PROMPT_CACHE_KEY_STRATEGY] = bool(
                payload.get("prompt_cache_key_strategy")
            )
            if not current_group_settings[GROUP_SETTING_PROMPT_CACHE_KEY_STRATEGY]:
                current_group_settings.pop(
                    GROUP_SETTING_PROMPT_CACHE_KEY_STRATEGY,
                    None,
                )
        if "deepseek_cache_strategy" in payload:
            current_group_settings[GROUP_SETTING_DEEPSEEK_CACHE_STRATEGY] = bool(
                payload.get("deepseek_cache_strategy")
            )
            if not current_group_settings[GROUP_SETTING_DEEPSEEK_CACHE_STRATEGY]:
                current_group_settings.pop(
                    GROUP_SETTING_DEEPSEEK_CACHE_STRATEGY,
                    None,
                )

        if current_group_settings:
            group_settings[group_id] = current_group_settings
        else:
            group_settings.pop(group_id, None)
        try:
            self._save_group_settings(group_settings)
        except ValueError as exc:
            return _error(str(exc))

        global_limit = self._daily_limit()
        group_limits = {
            item_group_id: int(settings[GROUP_SETTING_DAILY_LIMIT])
            for item_group_id, settings in group_settings.items()
            if GROUP_SETTING_DAILY_LIMIT in settings
        }
        has_custom_limit = group_id in group_limits
        effective_limit = self._daily_limit_for_group(group_id, group_limits)
        context_limit_05 = self._group_context_limit_05(group_id, group_settings)
        context_limit_tokens = self._group_context_limit_tokens(
            group_id,
            group_settings,
            group_limits,
        )
        return _ok(
            {
                "group_id": group_id,
                "daily_token_limit": effective_limit,
                "daily_token_limit_display": _format_tokens(effective_limit),
                "global_daily_token_limit": global_limit,
                "global_daily_token_limit_display": _format_tokens(global_limit),
                "has_custom_limit": has_custom_limit,
                "custom_daily_token_limit": (
                    group_limits[group_id] if has_custom_limit else None
                ),
                "custom_daily_token_limit_display": (
                    _format_tokens(group_limits[group_id]) if has_custom_limit else ""
                ),
                "group_limits": group_limits,
                "group_settings": group_settings,
                "only_at_bot_llm": self._group_only_at_bot_llm(
                    group_id,
                    group_settings,
                ),
                "prompt_cache_key_strategy": self._group_prompt_cache_key_strategy(
                    group_id,
                    group_settings,
                ),
                "deepseek_cache_strategy": self._group_deepseek_cache_strategy(
                    group_id,
                    group_settings,
                ),
                "context_limit_05": context_limit_05,
                "context_limit_tokens": context_limit_tokens,
                "context_limit_display": _format_context_limit_tokens(
                    context_limit_tokens
                    if context_limit_05
                    else max(1, int(effective_limit * TOKEN_LIMIT_CONTEXT_RATIO))
                ),
            }
        )
