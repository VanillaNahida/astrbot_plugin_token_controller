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



class ProviderStrategyMixin:

    def _provider_id_for_context_limit(
        self,
        event: AstrMessageEvent,
        limit_context: dict[str, Any],
    ) -> str:
        limit_state = limit_context.get("limit_state", {})
        if (
            limit_state.get("status") == "fallback"
            and limit_context.get("fallback_provider_valid")
        ):
            return str(limit_context.get("fallback_provider_id") or "").strip()
        selected_provider_id = str(
            self._event_get_extra(event, "selected_provider") or ""
        ).strip()
        if (
            selected_provider_id
            and not selected_provider_id.startswith(TOKEN_LIMIT_TEMP_PROVIDER_PREFIX)
        ):
            return selected_provider_id
        try:
            provider = self.context.get_using_provider(
                umo=getattr(event, "unified_msg_origin", None),
            )
        except Exception:
            provider = None
        provider_config = getattr(provider, "provider_config", None)
        if isinstance(provider_config, dict):
            provider_id = str(provider_config.get("id") or "").strip()
            if provider_id:
                return provider_id
        try:
            provider_meta = provider.meta()
            return str(getattr(provider_meta, "id", "") or "").strip()
        except Exception:
            return ""


    def _cleanup_temp_context_provider(self, event: AstrMessageEvent) -> None:
        temp_provider_id = self._event_get_extra(
            event,
            "token_limit_temp_context_provider_id",
        )
        if not temp_provider_id:
            event.set_extra("token_limit_context_provider_origin", "")
            event.set_extra("token_limit_context_tokens", 0)
            event.set_extra("token_limit_prompt_cache_key", "")
            event.set_extra("token_limit_prompt_cache_kind", "")
            event.set_extra("token_limit_prompt_cache_active", False)
            event.set_extra("token_limit_context_provider_had_selected", False)
            return
        provider_manager = getattr(self.context, "provider_manager", None)
        inst_map = getattr(provider_manager, "inst_map", None)
        provider_insts = getattr(provider_manager, "provider_insts", None)
        if isinstance(inst_map, dict):
            temp_provider = inst_map.pop(str(temp_provider_id), None)
        else:
            temp_provider = None
        if isinstance(provider_insts, list) and temp_provider in provider_insts:
            provider_insts.remove(temp_provider)
        selected_provider_id = str(
            self._event_get_extra(event, "selected_provider") or ""
        )
        origin_provider_id = str(
            self._event_get_extra(event, "token_limit_context_provider_origin")
            or ""
        )
        had_selected_provider = bool(
            self._event_get_extra(event, "token_limit_context_provider_had_selected")
        )
        if selected_provider_id == str(temp_provider_id):
            event.set_extra(
                "selected_provider",
                origin_provider_id if had_selected_provider else "",
            )
        event.set_extra("token_limit_temp_context_provider_id", "")
        event.set_extra("token_limit_context_provider_origin", "")
        event.set_extra("token_limit_context_tokens", 0)
        event.set_extra("token_limit_prompt_cache_key", "")
        event.set_extra("token_limit_prompt_cache_kind", "")
        event.set_extra("token_limit_prompt_cache_active", False)
        event.set_extra("token_limit_context_provider_had_selected", False)


    def _set_temp_context_provider_limit(
        self,
        event: AstrMessageEvent,
        context_tokens: int,
    ) -> bool:
        temp_provider_id = str(
            self._event_get_extra(event, "token_limit_temp_context_provider_id") or ""
        )
        if not temp_provider_id:
            return False
        provider_manager = getattr(self.context, "provider_manager", None)
        inst_map = getattr(provider_manager, "inst_map", None)
        if not isinstance(inst_map, dict):
            return False
        provider = inst_map.get(temp_provider_id)
        if provider is None:
            return False
        provider_config = getattr(provider, "provider_config", None)
        if not isinstance(provider_config, dict):
            return False

        effective_context = max(1, int(context_tokens))
        provider_config["max_context_tokens"] = effective_context
        try:
            provider.max_context_tokens = effective_context
        except Exception:
            pass
        event.set_extra("token_limit_context_tokens", effective_context)
        return True


    async def _cleanup_temp_context_provider_later(
        self,
        event: AstrMessageEvent,
        temp_provider_id: str,
    ) -> None:
        await asyncio.sleep(60)
        current_temp_provider_id = self._event_get_extra(
            event,
            "token_limit_temp_context_provider_id",
        )
        if str(current_temp_provider_id or "") == temp_provider_id:
            self._cleanup_temp_context_provider(event)


    @staticmethod
    def _provider_model_name(provider: Any, provider_config: dict[str, Any]) -> str:
        for key in ("model", "model_name"):
            model = str(provider_config.get(key) or "").strip()
            if model:
                return model
        get_model = getattr(provider, "get_model", None)
        if callable(get_model):
            try:
                return str(get_model() or "").strip()
            except Exception:
                return ""
        return ""


    @staticmethod
    def _provider_type_name(provider: Any, provider_config: dict[str, Any]) -> str:
        for key in ("type", "provider_type"):
            value = str(provider_config.get(key) or "").strip()
            if value:
                return value
        try:
            meta = provider.meta()
            return str(getattr(meta, "type", "") or "").strip()
        except Exception:
            return ""


    @staticmethod
    def _provider_api_base(provider_config: dict[str, Any]) -> str:
        for key in ("api_base", "base_url", "openai_api_base"):
            value = str(provider_config.get(key) or "").strip()
            if value:
                return value
        return ""


    def _provider_supports_prompt_cache_key(
        self,
        provider: Any,
        provider_config: dict[str, Any],
    ) -> bool:
        provider_type = self._provider_type_name(provider, provider_config).lower()
        if provider_type == "openai_chat_completion":
            return True
        module_name = str(getattr(provider.__class__, "__module__", "") or "").lower()
        return "openai" in module_name and "provider" in module_name


    @staticmethod
    def _model_is_gpt(model: str) -> bool:
        normalized_model = model.lower().strip()
        if not normalized_model:
            return False
        return (
            normalized_model.startswith("gpt-")
            or normalized_model.startswith("chatgpt-")
            or "/gpt-" in normalized_model
            or "/chatgpt-" in normalized_model
            or ":gpt-" in normalized_model
            or ":chatgpt-" in normalized_model
        )


    @staticmethod
    def _model_is_deepseek(model: str) -> bool:
        normalized_model = model.lower().strip()
        if not normalized_model:
            return False
        return (
            normalized_model.startswith("deepseek")
            or "/deepseek" in normalized_model
            or ":deepseek" in normalized_model
        )


    def _provider_is_deepseek(
        self,
        provider: Any,
        provider_config: dict[str, Any],
        model: str,
    ) -> bool:
        if self._model_is_deepseek(model):
            return True
        api_base = self._provider_api_base(provider_config).lower()
        if "deepseek" in api_base:
            return True
        provider_type = self._provider_type_name(provider, provider_config).lower()
        return "deepseek" in provider_type


    def _provider_supports_prompt_cache_retention(
        self,
        provider: Any,
        provider_config: dict[str, Any],
        model: str,
    ) -> bool:
        if not self._provider_supports_prompt_cache_key(provider, provider_config):
            return False
        api_base = self._provider_api_base(provider_config).lower().rstrip("/")
        if api_base and "api.openai.com" not in api_base:
            return False
        normalized_model = model.lower()
        return any(
            normalized_model.startswith(prefix)
            for prefix in TOKEN_LIMIT_OPENAI_CACHE_RETENTION_MODELS
        )


    @staticmethod
    def _prompt_cache_key_for_group(
        group_id: str,
        provider_id: str,
        model: str,
    ) -> str:
        raw_key = f"{group_id}|{provider_id}|{model}"
        digest = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()[:24]
        return f"tl-{digest}"


    def _prompt_cache_extra_body(
        self,
        group_id: str,
        provider_id: str,
        provider: Any,
        provider_config: dict[str, Any],
    ) -> dict[str, Any]:
        model = self._provider_model_name(provider, provider_config)
        if not self._model_is_gpt(model):
            return {}
        cache_key = self._prompt_cache_key_for_group(group_id, provider_id, model)
        extra_body: dict[str, Any] = {"prompt_cache_key": cache_key}
        if self._provider_supports_prompt_cache_retention(
            provider,
            provider_config,
            model,
        ):
            extra_body["prompt_cache_retention"] = "24h"
        return extra_body


    def _apply_context_limit_provider_if_needed(
        self,
        event: AstrMessageEvent,
        limit_context: dict[str, Any],
    ) -> None:
        group_id = str(limit_context.get("group_id") or "")
        group_settings = self._load_group_settings()
        group_limits = {
            item_group_id: int(settings[GROUP_SETTING_DAILY_LIMIT])
            for item_group_id, settings in group_settings.items()
            if GROUP_SETTING_DAILY_LIMIT in settings
        }
        context_limit_tokens = self._group_context_limit_tokens(
            group_id,
            group_settings,
            group_limits,
        )
        gpt_prompt_cache_enabled = self._group_prompt_cache_key_strategy(
            group_id,
            group_settings,
        )
        deepseek_cache_enabled = self._group_deepseek_cache_strategy(
            group_id,
            group_settings,
        )
        if (
            context_limit_tokens <= 0
            and not gpt_prompt_cache_enabled
            and not deepseek_cache_enabled
        ):
            self._cleanup_temp_context_provider(event)
            return

        previous_selected_provider_id = str(
            self._event_get_extra(event, "selected_provider") or ""
        ).strip()
        provider_id = self._provider_id_for_context_limit(event, limit_context)
        if not provider_id:
            return
        provider = self.context.get_provider_by_id(provider_id)
        if provider is None or not hasattr(provider, "text_chat"):
            return
        provider_config = getattr(provider, "provider_config", None)
        if not isinstance(provider_config, dict):
            return
        prompt_cache_extra: dict[str, Any] = {}
        prompt_cache_key = ""
        prompt_cache_kind = ""
        model = self._provider_model_name(provider, provider_config)
        if gpt_prompt_cache_enabled and self._model_is_gpt(model):
            if self._provider_supports_prompt_cache_key(provider, provider_config):
                prompt_cache_extra = self._prompt_cache_extra_body(
                    group_id,
                    provider_id,
                    provider,
                    provider_config,
                )
                if prompt_cache_extra:
                    prompt_cache_key = str(prompt_cache_extra.get("prompt_cache_key") or "")
                    prompt_cache_kind = "gpt"
            if not prompt_cache_extra and context_limit_tokens <= 0:
                return
        elif (
            deepseek_cache_enabled
            and self._provider_is_deepseek(provider, provider_config, model)
        ):
            prompt_cache_key = self._prompt_cache_key_for_group(
                group_id,
                provider_id,
                model,
            )
            prompt_cache_kind = "deepseek"

        if context_limit_tokens <= 0 and not prompt_cache_kind:
            self._cleanup_temp_context_provider(event)
            return

        if context_limit_tokens <= 0 and not prompt_cache_extra:
            self._cleanup_temp_context_provider(event)
            event.set_extra("token_limit_prompt_cache_key", prompt_cache_key)
            event.set_extra("token_limit_prompt_cache_kind", prompt_cache_kind)
            event.set_extra("token_limit_prompt_cache_active", True)
            logger.debug(
                "Apply prompt cache anchor for group=%s provider=%s kind=%s",
                group_id,
                provider_id,
                prompt_cache_kind,
            )
            return

        self._cleanup_temp_context_provider(event)
        temp_provider = copy.copy(provider)
        temp_config = dict(provider_config)
        temp_provider_id = (
            f"{TOKEN_LIMIT_TEMP_PROVIDER_PREFIX}{group_id}_{provider_id}_{id(event)}"
        )
        if context_limit_tokens > 0:
            temp_config["max_context_tokens"] = context_limit_tokens
        if prompt_cache_extra:
            custom_extra_body = temp_config.get("custom_extra_body")
            merged_extra_body = (
                dict(custom_extra_body) if isinstance(custom_extra_body, dict) else {}
            )
            merged_extra_body.update(prompt_cache_extra)
            temp_config["custom_extra_body"] = merged_extra_body
        temp_provider.provider_config = temp_config
        if context_limit_tokens > 0:
            try:
                temp_provider.max_context_tokens = context_limit_tokens
            except Exception:
                pass

        provider_manager = getattr(self.context, "provider_manager", None)
        inst_map = getattr(provider_manager, "inst_map", None)
        provider_insts = getattr(provider_manager, "provider_insts", None)
        if not isinstance(inst_map, dict):
            return
        inst_map[temp_provider_id] = temp_provider
        if isinstance(provider_insts, list):
            provider_insts.append(temp_provider)

        event.set_extra("selected_provider", temp_provider_id)
        event.set_extra("token_limit_temp_context_provider_id", temp_provider_id)
        event.set_extra("token_limit_context_provider_origin", provider_id)
        event.set_extra("token_limit_context_tokens", context_limit_tokens)
        event.set_extra("token_limit_prompt_cache_key", prompt_cache_key)
        event.set_extra("token_limit_prompt_cache_kind", prompt_cache_kind)
        event.set_extra("token_limit_prompt_cache_active", bool(prompt_cache_kind))
        event.set_extra(
            "token_limit_context_provider_had_selected",
            bool(previous_selected_provider_id),
        )
        asyncio.create_task(
            self._cleanup_temp_context_provider_later(event, temp_provider_id)
        )
        log_method = logger.info if context_limit_tokens > 0 else logger.debug
        log_method(
            "Apply temporary provider overrides for group=%s provider=%s context=%s prompt_cache=%s retention=%s",
            group_id,
            provider_id,
            context_limit_tokens,
            prompt_cache_kind or "none",
            str(prompt_cache_extra.get("prompt_cache_retention") or ""),
        )


    @staticmethod
    def _context_limit_trim_budget(tokens: int) -> int:
        return max(1, int(max(1, tokens) * TOKEN_LIMIT_CONTEXT_TRIM_RATIO))


    @staticmethod
    def _context_limit_compress_threshold(tokens: int) -> int:
        return max(1, int(max(1, tokens) * TOKEN_LIMIT_CONTEXT_COMPRESS_THRESHOLD))


    @staticmethod
    def _context_limit_for_tokens(tokens: int) -> int:
        return max(
            1,
            math.ceil(max(1, tokens) / TOKEN_LIMIT_CONTEXT_COMPRESS_THRESHOLD) + 128,
        )


    @staticmethod
    def _estimate_text_tokens(value: Any) -> int:
        text = str(value or "")
        chinese_count = sum(1 for char in text if "\u4e00" <= char <= "\u9fff")
        other_count = len(text) - chinese_count
        return int(chinese_count * 0.6 + other_count * 0.3)


    def _estimate_content_tokens(self, content: Any) -> int:
        if isinstance(content, str):
            return self._estimate_text_tokens(content)
        if isinstance(content, list):
            total = 0
            for item in content:
                if isinstance(item, dict):
                    item_type = str(item.get("type") or "")
                    if item_type in {"image_url", "image"}:
                        total += TOKEN_LIMIT_IMAGE_TOKEN_ESTIMATE
                    elif item_type in {"audio_url", "input_audio", "audio"}:
                        total += TOKEN_LIMIT_AUDIO_TOKEN_ESTIMATE
                    elif "text" in item:
                        total += self._estimate_text_tokens(item.get("text"))
                    else:
                        total += self._estimate_text_tokens(
                            json.dumps(item, ensure_ascii=False)
                        )
                    continue
                total += self._estimate_text_tokens(item)
            return total
        if isinstance(content, dict):
            return self._estimate_text_tokens(json.dumps(content, ensure_ascii=False))
        return self._estimate_text_tokens(content)


    def _estimate_request_messages_tokens(
        self,
        req: ProviderRequest,
        history_messages: list[Any],
    ) -> int:
        total = 0
        if getattr(req, "system_prompt", None):
            total += self._estimate_text_tokens(req.system_prompt)
        for message in history_messages:
            if isinstance(message, dict):
                total += self._estimate_content_tokens(message.get("content", ""))
                if message.get("tool_calls"):
                    total += self._estimate_text_tokens(
                        json.dumps(message.get("tool_calls"), ensure_ascii=False)
                    )
            else:
                total += self._estimate_text_tokens(message)
        if getattr(req, "prompt", None):
            total += self._estimate_text_tokens(req.prompt)
        for part in getattr(req, "extra_user_content_parts", None) or []:
            if hasattr(part, "text"):
                total += self._estimate_text_tokens(getattr(part, "text", ""))
            elif isinstance(part, dict):
                total += self._estimate_content_tokens(part)
            else:
                total += self._estimate_text_tokens(part)
        total += (
            len(getattr(req, "image_urls", None) or [])
            * TOKEN_LIMIT_IMAGE_TOKEN_ESTIMATE
        )
        total += (
            len(getattr(req, "audio_urls", None) or [])
            * TOKEN_LIMIT_AUDIO_TOKEN_ESTIMATE
        )
        return total


    @staticmethod
    def _drop_oldest_context_turn(messages: list[Any]) -> tuple[list[Any], int]:
        if not messages:
            return messages, 0

        first_user_index = 0
        for index, message in enumerate(messages):
            if isinstance(message, dict) and message.get("role") == "user":
                first_user_index = index
                break

        next_user_index: int | None = None
        for index in range(first_user_index + 1, len(messages)):
            message = messages[index]
            if isinstance(message, dict) and message.get("role") == "user":
                next_user_index = index
                break

        drop_until = next_user_index if next_user_index is not None else len(messages)
        return messages[drop_until:], drop_until


    @staticmethod
    def _count_context_turns(messages: list[Any]) -> int:
        return sum(
            1
            for message in messages
            if isinstance(message, dict) and message.get("role") == "user"
        )


    @staticmethod
    def _keep_recent_context_turns(messages: list[Any], turns: int) -> list[Any]:
        if turns <= 0 or not messages:
            return []
        user_indexes = [
            index
            for index, message in enumerate(messages)
            if isinstance(message, dict) and message.get("role") == "user"
        ]
        if len(user_indexes) <= turns:
            return list(messages)
        return list(messages[user_indexes[-turns] :])


    @staticmethod
    def _clear_request_conversation_token_usage(req: ProviderRequest) -> None:
        conversation = getattr(req, "conversation", None)
        if conversation is not None and hasattr(conversation, "token_usage"):
            try:
                conversation.token_usage = 0
            except Exception:
                pass


    def _trim_provider_request_context_if_needed(
        self,
        event: AstrMessageEvent,
        req: ProviderRequest,
        limit_context: dict[str, Any],
    ) -> None:
        group_id = str(limit_context.get("group_id") or "")
        context_limit_tokens = self._group_context_limit_tokens(group_id)
        if context_limit_tokens <= 0:
            return
        self._clear_request_conversation_token_usage(req)
        raw_history_messages = getattr(req, "contexts", None)
        history_messages = (
            raw_history_messages if isinstance(raw_history_messages, list) else []
        )
        budget = self._context_limit_trim_budget(context_limit_tokens)
        compress_threshold = self._context_limit_compress_threshold(
            context_limit_tokens
        )
        before_tokens = self._estimate_request_messages_tokens(req, history_messages)
        if before_tokens <= budget:
            return

        trimmed_messages = list(history_messages)
        removed_turns = 0
        while trimmed_messages:
            next_messages, removed_count = self._drop_oldest_context_turn(
                trimmed_messages
            )
            if removed_count <= 0 or len(next_messages) == len(trimmed_messages):
                break
            trimmed_messages = next_messages
            removed_turns += 1
            if self._estimate_request_messages_tokens(req, trimmed_messages) <= budget:
                break

        if len(trimmed_messages) == len(history_messages):
            after_tokens = before_tokens
        else:
            after_tokens = self._estimate_request_messages_tokens(
                req,
                trimmed_messages,
            )

        effective_context_tokens = context_limit_tokens
        raised_context = False
        if after_tokens > compress_threshold:
            fallback_messages = self._keep_recent_context_turns(
                history_messages,
                TOKEN_LIMIT_CONTEXT_FALLBACK_TURNS,
            )
            fallback_tokens = self._estimate_request_messages_tokens(
                req,
                fallback_messages,
            )
            trimmed_messages = fallback_messages
            after_tokens = fallback_tokens
            removed_turns = max(
                0,
                self._count_context_turns(history_messages)
                - self._count_context_turns(trimmed_messages),
            )
            if after_tokens > compress_threshold:
                effective_context_tokens = self._context_limit_for_tokens(after_tokens)
                raised_context = self._set_temp_context_provider_limit(
                    event,
                    effective_context_tokens,
                )

        if len(trimmed_messages) != len(history_messages):
            req.contexts = trimmed_messages
        if len(trimmed_messages) != len(history_messages) or raised_context:
            logger.info(
                "Token limit context trim group=%s configured=%s effective=%s tokens=%s->%s budget=%s removed_turns=%s kept_turns=%s raised=%s",
                group_id,
                context_limit_tokens,
                effective_context_tokens,
                before_tokens,
                after_tokens,
                budget,
                removed_turns,
                self._count_context_turns(trimmed_messages),
                raised_context,
            )


    def _apply_prompt_cache_anchor_if_needed(
        self,
        event: AstrMessageEvent,
        req: ProviderRequest,
        limit_context: dict[str, Any],
    ) -> None:
        if not bool(self._event_get_extra(event, "token_limit_prompt_cache_active")):
            return
        group_id = str(limit_context.get("group_id") or "")
        cache_key = str(self._event_get_extra(event, "token_limit_prompt_cache_key") or "")
        if not group_id or not cache_key:
            return
        current_prompt = str(getattr(req, "system_prompt", "") or "")
        if TOKEN_LIMIT_PROMPT_CACHE_ANCHOR in current_prompt:
            return
        req.system_prompt = f"{TOKEN_LIMIT_PROMPT_CACHE_ANCHOR}\n{current_prompt}"
        estimated_tokens = self._estimate_text_tokens(TOKEN_LIMIT_PROMPT_CACHE_ANCHOR)
        if estimated_tokens < TOKEN_LIMIT_PROMPT_CACHE_PREFIX_TARGET:
            logger.debug(
                "Prompt cache anchor estimate is below target: group=%s key=%s estimated=%s target=%s",
                group_id,
                cache_key,
                estimated_tokens,
                TOKEN_LIMIT_PROMPT_CACHE_PREFIX_TARGET,
            )
