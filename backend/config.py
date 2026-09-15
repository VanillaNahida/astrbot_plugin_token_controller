from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone, tzinfo
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.core.db.po import ProviderStat
from astrbot.core.message.message_event_result import MessageChain

try:
    from astrbot.api.star import Context, Star, StarTools
except ImportError:  # pragma: no cover - kept for older AstrBot builds.
    from astrbot.api.star import Context, Star

    StarTools = None  # type: ignore[assignment]


PLUGIN_NAME = "astrbot_plugin_token_controller"
LEGACY_PLUGIN_NAME = "astrbot_plugin_token_limit"
GROUP_REMARKS_FILE = "group_remarks.json"
GROUP_LIMITS_FILE = "group_limits.json"
GROUP_NAMES_FILE = "group_names.json"
USER_NAMES_FILE = "user_names.json"
PLUGIN_DATA_FILES = (
    GROUP_REMARKS_FILE,
    GROUP_LIMITS_FILE,
    GROUP_NAMES_FILE,
    USER_NAMES_FILE,
    "history_usage.json",
    "user_usage_48h.json",
)
MAX_GROUP_REMARK_LENGTH = 64
MAX_GROUP_NAME_LENGTH = 96
MAX_USER_NAME_LENGTH = 64
GROUP_SETTING_DAILY_LIMIT = "daily_token_limit"
GROUP_SETTING_ONLY_AT_BOT = "only_at_bot_llm"
GROUP_SETTING_CONTEXT_LIMIT_05 = "context_limit_05"
GROUP_SETTING_PROMPT_CACHE_KEY_STRATEGY = "prompt_cache_key_strategy"
GROUP_SETTING_DEEPSEEK_CACHE_STRATEGY = "deepseek_cache_strategy"
GROUP_SETTINGS_CONFIG_BACKUP_KEY = "_plugin_page_group_settings"
TOKEN_LIMIT_CONTEXT_RATIO = 0.005
TOKEN_LIMIT_CONTEXT_TRIM_RATIO = 0.8
TOKEN_LIMIT_CONTEXT_COMPRESS_THRESHOLD = 0.82
TOKEN_LIMIT_CONTEXT_FALLBACK_TURNS = 3
TOKEN_LIMIT_TEMP_PROVIDER_PREFIX = "__token_limit_context__"
TOKEN_LIMIT_OPENAI_CACHE_RETENTION_MODELS = ("gpt-5", "gpt-4.1")
TOKEN_LIMIT_PROMPT_CACHE_ANCHOR_VERSION = "v1"
TOKEN_LIMIT_PROMPT_CACHE_ANCHOR_LINES = 128
TOKEN_LIMIT_PROMPT_CACHE_PREFIX_TARGET = 1400
TOKEN_LIMIT_IMAGE_TOKEN_ESTIMATE = 765
TOKEN_LIMIT_AUDIO_TOKEN_ESTIMATE = 500
OVER_LIMIT_STOP = "stop_llm"
OVER_LIMIT_FALLBACK = "fallback_provider"
TOKEN_FIELDS_SUM = (
    ProviderStat.token_input_other
    + ProviderStat.token_input_cached
    + ProviderStat.token_output
)


CONFIG_SCHEMA: dict[str, dict[str, Any]] = {
    "enabled": {
        "description": "启用插件",
        "type": "bool",
        "hint": "关闭后不统计限流状态，也不会拦截任何 LLM 请求。",
        "default": True,
    },
    "limited_groups": {
        "description": "需要限流的 QQ 群聊列表",
        "type": "list",
        "hint": "填写 QQ 群号。原生 WebUI 可逐项填写；也兼容逗号、空格或换行分隔的字符串。新加入的群号会从加入时刻开始写入历史 token 用量统计；之后即使从列表移除也会继续统计。",
        "default": [],
    },
    "daily_token_limit": {
        "description": "单个群聊每日用量上限",
        "type": "int",
        "hint": "单位为 token。当前统计窗口内群聊总用量到达上限后，根据“用量超限时的措施”停止调用 LLM 或切换到回退模型。",
        "default": 1000000,
    },
    "user_daily_token_limit": {
        "description": "单个用户每日用量上限",
        "type": "int",
        "hint": "单位为 token。仅统计单个群聊内单个用户当前刷新周期的用量；设置为 -1 时不限制普通用户用量。超过上限后阻断该用户在该群内发起的 LLM 请求，并向其发送提示。",
        "default": -1,
    },
    "sponsor_daily_token_limit": {
        "description": "赞助用户每日用量上限",
        "type": "int",
        "hint": "单位为 token。赞助用户（位于“赞助用户列表”）当日用量达到该上限后阻断；设置为 -1 时不限制赞助用户用量。",
        "default": -1,
    },
    "sponsor_users": {
        "description": "赞助用户列表（QQ 号）",
        "type": "list",
        "hint": "赞助用户按“赞助用户每日用量上限”限流。原生 WebUI 可逐项填写；也兼容逗号、空格或换行分隔的字符串。可在 WebUI“赞助用户管理”弹窗或使用命令 /sponsor add 维护。",
        "default": [],
    },
    "super_admin_users": {
        "description": "超级管理员列表（QQ 号）",
        "type": "list",
        "hint": "超级管理员仅记录用量不做限流，任意用量都不会被阻断。原生 WebUI 可逐项填写；也兼容逗号、空格或换行分隔的字符串。可在 WebUI“赞助用户管理”弹窗或使用命令 /sponsor super 维护。",
        "default": [],
    },
    "user_limit_message": {
        "description": "单用户超限提示",
        "type": "text",
        "hint": "普通或赞助用户当日用量达到上限时发送到群的提示。可用变量：{nickname}、{user_id}、{used}、{limit}。",
        "default": "{nickname}，您当日的对话额度已用尽，达到{limit}，请明日再来。",
    },
    "send_user_limit_message": {
        "description": "发送单用户超限提示",
        "type": "bool",
        "hint": "关闭后仍会阻断普通/赞助用户超限请求，但不向其发送提示消息。",
        "default": True,
    },
    "over_limit_policy": {
        "description": "用量超限时的措施",
        "type": "object",
        "hint": "配置群聊达到每日用量上限后的处理方式。",
        "default": {
            "action": OVER_LIMIT_STOP,
            "fallback_provider_id": "",
            "fallback_token_limit": 0,
            "block_wake_words_after_limit": False,
        },
        "items": {
            "action": {
                "description": "处理方式",
                "type": "string",
                "hint": "选择“停止调用 LLM”时，原始模型用量超限后直接拦截；选择“回退到其他模型”时，会改用回退供应商继续生成回复。",
                "default": OVER_LIMIT_STOP,
                "options": [OVER_LIMIT_STOP, OVER_LIMIT_FALLBACK],
                "option_labels": ["停止调用 LLM", "回退到其他模型"],
            },
            "fallback_provider_id": {
                "description": "回退的模型供应商",
                "type": "string",
                "hint": "填写 AstrBot 模型供应商 ID。Plugin Page 会提供当前已加载的聊天模型供应商下拉选择。",
                "default": "",
            },
            "fallback_token_limit": {
                "description": "回退模型的用量上限",
                "type": "int",
                "hint": "单位为 token。选择回退时，硬上限为“每日用量上限 + 回退模型的用量上限”；当前统计窗口内群聊总用量到达硬上限后停止调用。",
                "default": 0,
            },
            "block_wake_words_after_limit": {
                "description": "超限后不再响应唤醒词",
                "type": "bool",
                "hint": "启用后，群聊当前窗口用量达到“单个群聊每日用量上限”时，使用唤醒词触发 bot 的事件会被阻断；@bot 仍可继续触发，但仍遵守回退模型或停止响应规则。",
                "default": False,
            },
        },
    },
    "refresh_time": {
        "description": "用量刷新时间",
        "type": "string",
        "hint": "每天按本机时区在该时间切换统计窗口，格式 HH:MM，例如 00:00 或 04:30。",
        "default": "00:00",
    },
    "qq_platform_names": {
        "description": "QQ 平台适配器名称",
        "type": "list",
        "hint": "只有这些平台类型的群聊会被限流。默认覆盖 aiocqhttp、qq_official 和 qq_official_webhook。",
        "default": ["aiocqhttp", "qq_official", "qq_official_webhook"],
    },
    "match_unique_session": {
        "description": "兼容会话隔离统计",
        "type": "bool",
        "hint": "开启后统计当前窗口和历史用量时，会匹配常见 unique_session 形式，例如 用户ID_群号。",
        "default": True,
    },
    "block_message": {
        "description": "超限拦截提示",
        "type": "text",
        "hint": "达到停止调用条件时发送到群聊。可用变量：{group_id}、{used}、{limit}、{refresh_time}、{window_start}、{window_end}。",
        "default": "本群今日 LLM token 用量已达上限（{used}/{limit}），将在 {refresh_time} 后恢复。",
    },
    "send_block_message": {
        "description": "发送拦截提示",
        "type": "bool",
        "hint": "关闭后只拦截 LLM 请求，不向群内发送提示消息。",
        "default": True,
    },
}


@dataclass(frozen=True)
class UsageWindow:
    start_local: datetime
    end_local: datetime
    start_utc: datetime
    end_utc: datetime


def _ok(data: dict | list | None = None, message: str | None = None) -> dict:
    return {"status": "ok", "message": message, "data": data or {}}


def _error(message: str) -> dict:
    return {"status": "error", "message": message, "data": {}}


def _split_group_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (int, float)):
        return [str(int(value))]
    if isinstance(value, str):
        return [item for item in re.split(r"[\s,;，；]+", value.strip()) if item]
    if isinstance(value, list):
        groups: list[str] = []
        for item in value:
            groups.extend(_split_group_values(item))
        return groups
    return []


def _normalize_group_id(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")


def _format_tokens(value: int) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f} M"
    if value >= 1_000:
        return f"{value / 1_000:.2f} K"
    return str(value)


def _format_context_limit_tokens(value: int) -> str:
    value = max(1, int(value or 0))
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    return f"{max(1, round(value / 1_000))}K"


def _prompt_cache_anchor_text() -> str:
    lines = [
        "<token_limit_prompt_cache_anchor>",
        f"version={TOKEN_LIMIT_PROMPT_CACHE_ANCHOR_VERSION}",
        "purpose=stable_prefix_for_prompt_cache",
    ]
    for index in range(TOKEN_LIMIT_PROMPT_CACHE_ANCHOR_LINES):
        lines.append(f"anchor_line_{index:03d}=stable_cache_prefix_no_instruction")
    lines.append("</token_limit_prompt_cache_anchor>")
    return "\n".join(lines)


TOKEN_LIMIT_PROMPT_CACHE_ANCHOR = _prompt_cache_anchor_text()


def _parse_refresh_time(value: Any) -> time:
    raw = str(value or "00:00").strip()
    match = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", raw)
    if not match:
        return time(hour=0, minute=0)
    return time(hour=int(match.group(1)), minute=int(match.group(2)))


def _local_timezone() -> tzinfo:
    tz = datetime.now().astimezone().tzinfo
    return tz or timezone.utc


def _build_usage_window(refresh_time: Any) -> UsageWindow:
    local_tz = _local_timezone()
    now_local = datetime.now(local_tz)
    parsed_time = _parse_refresh_time(refresh_time)
    today_refresh = datetime.combine(now_local.date(), parsed_time, tzinfo=local_tz)
    if now_local >= today_refresh:
        start_local = today_refresh
    else:
        start_local = today_refresh - timedelta(days=1)
    end_local = start_local + timedelta(days=1)
    return UsageWindow(
        start_local=start_local,
        end_local=end_local,
        start_utc=start_local.astimezone(timezone.utc),
        end_utc=end_local.astimezone(timezone.utc),
    )


class ConfigMixin:

    def _config_value(self, key: str) -> Any:
        if key in self.config:
            return self.config[key]
        return CONFIG_SCHEMA[key].get("default")


    def _limited_groups(self) -> list[str]:
        seen: set[str] = set()
        groups: list[str] = []
        for item in _split_group_values(self._config_value("limited_groups")):
            group_id = _normalize_group_id(item)
            if group_id and group_id not in seen:
                seen.add(group_id)
                groups.append(group_id)
        return groups


    def _qq_platform_names(self) -> set[str]:
        values = _split_group_values(self._config_value("qq_platform_names"))
        return {item.strip() for item in values if item.strip()}


    def _qq_platform_ids(self) -> set[str]:
        configured_names = self._qq_platform_names()
        platform_ids: set[str] = set()
        platform_manager = getattr(self.context, "platform_manager", None)
        platform_insts = getattr(platform_manager, "platform_insts", []) or []
        for platform in platform_insts:
            meta = platform.meta()
            meta_name = getattr(meta, "name", "")
            meta_id = getattr(meta, "id", "")
            if not configured_names or meta_name in configured_names:
                if meta_id:
                    platform_ids.add(str(meta_id))
                if meta_name:
                    platform_ids.add(str(meta_name))
        if not platform_ids:
            platform_ids.update(configured_names)
        return platform_ids


    def _daily_limit(self) -> int:
        try:
            return max(0, int(self._config_value("daily_token_limit") or 0))
        except (TypeError, ValueError):
            return 0


    def _over_limit_policy(self) -> dict[str, Any]:
        raw_policy = self._config_value("over_limit_policy")
        default_policy = CONFIG_SCHEMA["over_limit_policy"]["default"]
        policy = dict(default_policy)
        if isinstance(raw_policy, dict):
            policy.update(raw_policy)

        action = str(policy.get("action") or OVER_LIMIT_STOP).strip()
        if action not in {OVER_LIMIT_STOP, OVER_LIMIT_FALLBACK}:
            action = OVER_LIMIT_STOP

        try:
            fallback_token_limit = max(
                0,
                int(policy.get("fallback_token_limit") or 0),
            )
        except (TypeError, ValueError):
            fallback_token_limit = 0

        return {
            "action": action,
            "fallback_provider_id": str(policy.get("fallback_provider_id") or "").strip(),
            "fallback_token_limit": fallback_token_limit,
            "block_wake_words_after_limit": bool(
                policy.get("block_wake_words_after_limit", False)
            ),
        }


    def _fallback_provider_id(self) -> str:
        policy = self._over_limit_policy()
        if policy["action"] != OVER_LIMIT_FALLBACK:
            return ""
        return str(policy["fallback_provider_id"] or "").strip()


    def _fallback_token_limit(self) -> int:
        return int(self._over_limit_policy()["fallback_token_limit"])


    def _fallback_provider_exists(self, provider_id: str) -> bool:
        if not provider_id:
            return False
        get_provider_by_id = getattr(self.context, "get_provider_by_id", None)
        if not callable(get_provider_by_id):
            return False
        provider = get_provider_by_id(provider_id)
        return provider is not None and hasattr(provider, "text_chat")


    def _is_enabled(self) -> bool:
        return bool(self._config_value("enabled"))


    def _serialize_config(self) -> dict[str, Any]:
        return {key: self._config_value(key) for key in CONFIG_SCHEMA}


    def _provider_options(self) -> list[dict[str, str]]:
        providers = []
        seen_provider_ids: set[str] = set()
        get_all_providers = getattr(self.context, "get_all_providers", None)
        provider_insts = get_all_providers() if callable(get_all_providers) else []
        for provider in provider_insts or []:
            if not hasattr(provider, "text_chat"):
                continue
            try:
                meta = provider.meta()
            except Exception:
                continue
            provider_id = str(getattr(meta, "id", "") or "").strip()
            if not provider_id or provider_id in seen_provider_ids:
                continue
            seen_provider_ids.add(provider_id)
            get_model = getattr(provider, "get_model", None)
            model = str(
                getattr(meta, "model", "")
                or (get_model() if callable(get_model) else "")
                or ""
            ).strip()
            provider_type = str(getattr(meta, "type", "") or "").strip()
            label_parts = [provider_id]
            detail = " / ".join(item for item in (provider_type, model) if item)
            if detail:
                label_parts.append(detail)
            providers.append(
                {
                    "id": provider_id,
                    "label": " - ".join(label_parts),
                    "type": provider_type,
                    "model": model,
                }
            )
        return providers


    def _config_schema_for_page(self) -> dict[str, dict[str, Any]]:
        schema = json.loads(json.dumps(CONFIG_SCHEMA, ensure_ascii=False))
        provider_options = self._provider_options()
        fallback_meta = schema["over_limit_policy"]["items"]["fallback_provider_id"]
        fallback_meta["options"] = [item["id"] for item in provider_options]
        fallback_meta["option_labels"] = [item["label"] for item in provider_options]
        return schema


    def _sanitize_config(self, raw_config: dict[str, Any]) -> dict[str, Any]:
        next_config = self._serialize_config()
        if "enabled" in raw_config:
            next_config["enabled"] = bool(raw_config["enabled"])
        if "limited_groups" in raw_config:
            next_config["limited_groups"] = self._normalize_config_list(
                raw_config["limited_groups"]
            )
        if "daily_token_limit" in raw_config:
            try:
                next_config["daily_token_limit"] = max(
                    0, int(raw_config["daily_token_limit"])
                )
            except (TypeError, ValueError):
                raise ValueError("单个群聊每日用量上限必须是整数。") from None
        if "user_daily_token_limit" in raw_config:
            try:
                next_config["user_daily_token_limit"] = max(
                    -1,
                    int(raw_config["user_daily_token_limit"]),
                )
            except (TypeError, ValueError):
                raise ValueError("单个用户每日用量上限必须是整数。") from None
        if "sponsor_daily_token_limit" in raw_config:
            try:
                next_config["sponsor_daily_token_limit"] = max(
                    -1,
                    int(raw_config["sponsor_daily_token_limit"]),
                )
            except (TypeError, ValueError):
                raise ValueError("赞助用户每日用量上限必须是整数。") from None
        if "sponsor_users" in raw_config:
            next_config["sponsor_users"] = self._normalize_config_list(
                raw_config["sponsor_users"]
            )
        if "super_admin_users" in raw_config:
            next_config["super_admin_users"] = self._normalize_config_list(
                raw_config["super_admin_users"]
            )
        if "user_limit_message" in raw_config:
            next_config["user_limit_message"] = str(raw_config["user_limit_message"] or "")
        if "send_user_limit_message" in raw_config:
            next_config["send_user_limit_message"] = bool(
                raw_config["send_user_limit_message"]
            )
        if "over_limit_policy" in raw_config:
            next_config["over_limit_policy"] = self._sanitize_over_limit_policy(
                raw_config["over_limit_policy"]
            )
        if "refresh_time" in raw_config:
            raw_time = str(raw_config["refresh_time"] or "").strip()
            if not re.fullmatch(r"([01]?\d|2[0-3]):[0-5]\d", raw_time):
                raise ValueError("用量刷新时间格式必须是 HH:MM。")
            parsed = _parse_refresh_time(raw_time)
            next_config["refresh_time"] = f"{parsed.hour:02d}:{parsed.minute:02d}"
        if "qq_platform_names" in raw_config:
            next_config["qq_platform_names"] = self._normalize_config_list(
                raw_config["qq_platform_names"]
            )
        if "match_unique_session" in raw_config:
            next_config["match_unique_session"] = bool(
                raw_config["match_unique_session"]
            )
        if "block_message" in raw_config:
            next_config["block_message"] = str(raw_config["block_message"] or "")
        if "send_block_message" in raw_config:
            next_config["send_block_message"] = bool(raw_config["send_block_message"])
        return next_config


    def _sanitize_over_limit_policy(self, raw_policy: Any) -> dict[str, Any]:
        if not isinstance(raw_policy, dict):
            raw_policy = {}

        action = str(raw_policy.get("action") or OVER_LIMIT_STOP).strip()
        if action not in {OVER_LIMIT_STOP, OVER_LIMIT_FALLBACK}:
            raise ValueError("用量超限时的措施必须是“停止调用 LLM”或“回退到其他模型”。")

        fallback_provider_id = str(raw_policy.get("fallback_provider_id") or "").strip()
        try:
            fallback_token_limit = max(
                0,
                int(raw_policy.get("fallback_token_limit") or 0),
            )
        except (TypeError, ValueError):
            raise ValueError("回退模型的用量上限必须是整数。") from None

        if action == OVER_LIMIT_FALLBACK:
            if not fallback_provider_id:
                raise ValueError("选择“回退到其他模型”时必须配置回退的模型供应商。")
            if not self._fallback_provider_exists(fallback_provider_id):
                raise ValueError(f"未找到回退模型供应商：{fallback_provider_id}")

        return {
            "action": action,
            "fallback_provider_id": fallback_provider_id,
            "fallback_token_limit": fallback_token_limit,
            "block_wake_words_after_limit": bool(
                raw_policy.get("block_wake_words_after_limit", False)
            ),
        }


    @staticmethod
    def _normalize_config_list(value: Any) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for item in _split_group_values(value):
            text = _normalize_group_id(item)
            if text and text not in seen:
                seen.add(text)
                result.append(text)
        return result
