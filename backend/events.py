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
    from .sponsor import TIER_SUPER_ADMIN
except ImportError:  # pragma: no cover - compatible with direct module loading.
    from sponsor import TIER_SUPER_ADMIN  # type: ignore[no-redef]


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



class EventMixin:

    @staticmethod
    def _event_get_extra(event: AstrMessageEvent, key: str) -> Any:
        get_extra = getattr(event, "get_extra", None)
        if not callable(get_extra):
            return None
        try:
            return get_extra(key)
        except Exception:
            return None


    @staticmethod
    def _event_truthy_attr(event: AstrMessageEvent, name: str) -> bool | None:
        value = getattr(event, name, None)
        if value is None:
            return None
        if callable(value):
            try:
                return bool(value())
            except TypeError:
                return None
            except Exception:
                return False
        return bool(value)


    @staticmethod
    def _event_self_id(event: AstrMessageEvent) -> str:
        get_self_id = getattr(event, "get_self_id", None)
        if callable(get_self_id):
            try:
                self_id = str(get_self_id() or "").strip()
                if self_id:
                    return self_id
            except Exception:
                pass

        self_id = str(getattr(event, "self_id", "") or "").strip()
        if self_id:
            return self_id

        message_obj = getattr(event, "message_obj", None)
        return str(getattr(message_obj, "self_id", "") or "").strip()


    @staticmethod
    def _event_group_id(event: AstrMessageEvent) -> str:
        def scalar_group_id(value: Any) -> str:
            if isinstance(value, (str, int, float)):
                return _normalize_group_id(value)
            return ""

        message_obj = getattr(event, "message_obj", None)
        for source in (message_obj, event):
            if source is None:
                continue
            for name in ("group_id", "room_id"):
                value = getattr(source, name, None)
                group_id = scalar_group_id(value)
                if group_id:
                    return group_id
            group_obj = getattr(source, "group", None)
            group_id = scalar_group_id(group_obj)
            if group_id:
                return group_id
            if isinstance(group_obj, dict):
                for name in ("group_id", "id", "uin"):
                    group_id = scalar_group_id(group_obj.get(name))
                    if group_id:
                        return group_id
            elif group_obj is not None:
                for name in ("group_id", "id", "uin"):
                    group_id = scalar_group_id(getattr(group_obj, name, None))
                    if group_id:
                        return group_id
        get_group_id = getattr(event, "get_group_id", None)
        if callable(get_group_id):
            try:
                group_id = _normalize_group_id(get_group_id())
                if group_id:
                    return group_id
            except Exception:
                pass
        umo = str(getattr(event, "unified_msg_origin", "") or "").strip()
        if umo:
            group_id = _normalize_group_id(umo.rsplit(":", 1)[-1])
            if group_id:
                return group_id
        return ""


    @staticmethod
    def _event_group_name(event: AstrMessageEvent) -> str:
        message_obj = getattr(event, "message_obj", None)
        group = getattr(message_obj, "group", None)
        if group is None:
            group = getattr(event, "group", None)
        name = ""
        if isinstance(group, dict):
            for key in ("group_name", "name", "title"):
                value = group.get(key)
                if isinstance(value, str) and value.strip():
                    name = value
                    break
        elif group is not None:
            for attr in ("group_name", "name", "title"):
                value = getattr(group, attr, None)
                if isinstance(value, str) and value.strip():
                    name = value
                    break
        return name.strip()


    @staticmethod
    def _message_component_type(item: Any) -> str:
        item_type = getattr(item, "type", "")
        item_type_value = getattr(item_type, "value", item_type)
        item_type_name = getattr(item_type, "name", "")
        values = {
            str(item_type_value or "").lower(),
            str(item_type_name or "").lower(),
            item.__class__.__name__.lower(),
        }
        return "at" if {"at", "componenttype.at"} & values else ""


    @staticmethod
    def _message_component_targets(item: Any) -> list[str]:
        targets = []
        for name in ("qq", "user_id", "id", "target", "uin"):
            value = getattr(item, name, None)
            if value is not None:
                targets.append(str(value).strip())

        for dump_name in ("model_dump", "dict"):
            dump = getattr(item, dump_name, None)
            if not callable(dump):
                continue
            try:
                data = dump()
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            for name in ("qq", "user_id", "id", "target", "uin"):
                value = data.get(name)
                if value is not None:
                    targets.append(str(value).strip())
        return [target for target in targets if target]


    @staticmethod
    def _iter_message_items(event: AstrMessageEvent) -> list[Any]:
        candidates = []
        message_obj = getattr(event, "message_obj", None)
        if message_obj is not None:
            candidates.extend(
                [
                    getattr(message_obj, "message", None),
                    getattr(message_obj, "chain", None),
                ]
            )

        for getter_name in ("get_messages", "get_message_chain", "get_message"):
            getter = getattr(event, getter_name, None)
            if not callable(getter):
                continue
            try:
                candidates.append(getter())
            except Exception:
                continue

        items = []
        for candidate in candidates:
            if isinstance(candidate, (list, tuple)):
                items.extend(candidate)
            else:
                chain = getattr(candidate, "chain", None)
                message = getattr(candidate, "message", None)
                if isinstance(chain, (list, tuple)):
                    items.extend(chain)
                if isinstance(message, (list, tuple)):
                    items.extend(message)
        return items


    def _event_has_at_bot(self, event: AstrMessageEvent) -> bool:
        for name in ("is_at_bot", "is_at_self", "is_at"):
            value = self._event_truthy_attr(event, name)
            if value:
                return True

        self_id = self._event_self_id(event)
        for item in self._iter_message_items(event):
            if self._message_component_type(item) != "at":
                continue
            targets = self._message_component_targets(item)
            if not self_id or not targets:
                return True
            if any(target == self_id for target in targets):
                return True

        for getter_name in ("get_message_str", "get_raw_message"):
            getter = getattr(event, getter_name, None)
            if not callable(getter):
                continue
            try:
                text = str(getter() or "")
            except Exception:
                continue
            if "[CQ:at" in text or "<at" in text.lower():
                return not self_id or self_id in text

        message_obj = getattr(event, "message_obj", None)
        raw_message = str(getattr(message_obj, "raw_message", "") or "")
        if "[CQ:at" in raw_message or "<at" in raw_message.lower():
            return not self_id or self_id in raw_message
        return False


    def _is_wake_word_invocation(self, event: AstrMessageEvent) -> bool:
        if self._event_has_at_bot(event):
            return False

        for key in (
            "is_wake_command",
            "wake_command",
            "wake_word",
            "wake_prefix_matched",
            "is_at_or_wake_command",
        ):
            extra_value = self._event_get_extra(event, key)
            if extra_value:
                return True

        for name in (
            "is_wake_command",
            "wake_command",
            "wake_word",
            "wake_prefix_matched",
            "is_at_or_wake_command",
        ):
            value = self._event_truthy_attr(event, name)
            if value:
                return True
        return False


    def _should_block_wake_word_invocation(
        self,
        event: AstrMessageEvent,
        limit_context: dict[str, Any],
    ) -> bool:
        policy = limit_context["policy"]
        if not bool(policy.get("block_wake_words_after_limit")):
            return False
        limit = int(limit_context["limit"])
        used = int(limit_context["limit_state"]["used"])
        if not (limit > 0 and used >= limit and self._is_wake_word_invocation(event)):
            return False
        # 超级管理员豁免：超管的关键字唤醒不受“超限后不再响应唤醒词”拦截。
        if self._user_tier(self._event_user_id(event)) == TIER_SUPER_ADMIN:
            return False
        return True


    def _should_block_group_only_at_bot_invocation(
        self,
        event: AstrMessageEvent,
    ) -> str | None:
        if not self._is_enabled():
            return None
        if not self._is_qq_group_event(event):
            return None
        group_id = self._event_group_id(event)
        if not group_id or group_id not in self._limited_groups():
            return None
        if not self._group_only_at_bot_llm(group_id):
            return None
        if not self._is_wake_word_invocation(event):
            return None
        # 超级管理员豁免：“仅 @bot”策略不拦超管的关键字唤醒。
        if self._user_tier(self._event_user_id(event)) == TIER_SUPER_ADMIN:
            return None
        return group_id


    def _block_group_only_at_bot_invocation_if_needed(
        self,
        event: AstrMessageEvent,
        stage: str,
    ) -> bool:
        group_id = self._should_block_group_only_at_bot_invocation(event)
        if not group_id:
            return False
        event.stop_event()
        logger.info(
            "Blocked wake-word invocation by group @bot-only policy: "
            "stage=%s group=%s",
            stage,
            group_id,
        )
        return True


    def _is_qq_group_event(self, event: AstrMessageEvent) -> bool:
        if event.get_message_type() != MessageType.GROUP_MESSAGE:
            return False
        platform_names = self._qq_platform_names()
        return not platform_names or event.get_platform_name() in platform_names
