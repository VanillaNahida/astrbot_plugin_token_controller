from __future__ import annotations

from typing import Any

from quart import request

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent
from astrbot.core.message.message_event_result import MessageChain

try:
    from .config import (
        _ok,
        _error,
        _format_tokens,
        _split_group_values,
        _normalize_group_id,
    )
except ImportError:  # pragma: no cover - compatible with direct module loading.
    from config import (  # type: ignore[no-redef]
        _ok,
        _error,
        _format_tokens,
        _split_group_values,
        _normalize_group_id,
    )

try:
    from .user_stats import (
        _build_user_usage_window,
        _message_component_data,
        _message_component_type,
        _normalize_user_group_id,
        _sanitize_user_id,
    )
except ImportError:  # pragma: no cover - compatible with direct module loading.
    from user_stats import (  # type: ignore[no-redef]
        _build_user_usage_window,
        _message_component_data,
        _message_component_type,
        _normalize_user_group_id,
        _sanitize_user_id,
    )


SPONSOR_DAILY_TOKEN_LIMIT_KEY = "sponsor_daily_token_limit"
SPONSOR_USERS_KEY = "sponsor_users"
SUPER_ADMIN_USERS_KEY = "super_admin_users"
USER_LIMIT_MESSAGE_KEY = "user_limit_message"
SEND_USER_LIMIT_MESSAGE_KEY = "send_user_limit_message"

TIER_SUPER_ADMIN = "super_admin"
TIER_SPONSOR = "sponsor"
TIER_NORMAL = "normal"


class SponsorMixin:
    """Three-tier per-user daily token limits and sponsor/super-admin management."""

    # ---- list accessors -------------------------------------------------

    def _sponsor_users(self) -> list[str]:
        return self._normalize_config_list(self._config_value(SPONSOR_USERS_KEY))

    def _super_admin_users(self) -> list[str]:
        return self._normalize_config_list(self._config_value(SUPER_ADMIN_USERS_KEY))

    def _sponsor_daily_limit(self) -> int:
        try:
            return max(-1, int(self._config_value(SPONSOR_DAILY_TOKEN_LIMIT_KEY)))
        except (TypeError, ValueError):
            return -1

    # ---- tier helpers ---------------------------------------------------

    def _user_tier(self, user_id: str) -> str:
        normalized_user_id = _normalize_group_id(user_id)
        if normalized_user_id in self._super_admin_users():
            return TIER_SUPER_ADMIN
        if normalized_user_id in self._sponsor_users():
            return TIER_SPONSOR
        return TIER_NORMAL

    def _user_effective_limit(self, user_id: str) -> tuple[int | None, str]:
        """Return (limit_or_None, tier). None means no limit for that user."""
        tier = self._user_tier(user_id)
        if tier == TIER_SUPER_ADMIN:
            return None, tier
        if tier == TIER_SPONSOR:
            limit = self._sponsor_daily_limit()
            if limit < 0:
                return None, tier
            return limit, tier
        user_limit = self._user_daily_limit()
        if user_limit < 0:
            return None, tier
        return user_limit, tier

    # ---- config write helpers --------------------------------------------

    def _set_sponsor_users(self, users: list[str]) -> None:
        self.config[SPONSOR_USERS_KEY] = self._normalize_config_list(users)

    def _set_super_admin_users(self, users: list[str]) -> None:
        self.config[SUPER_ADMIN_USERS_KEY] = self._normalize_config_list(users)

    def _save_config_preserving_group_settings(self) -> None:
        group_settings_backup = self._load_group_settings()
        self._set_group_settings_config_backup(group_settings_backup, save_config=False)
        config_save = getattr(self.config, "save_config", None)
        if callable(config_save):
            config_save()

    # ---- user limit message ----------------------------------------------

    def _format_user_limit_message(
        self,
        context: dict[str, Any],
    ) -> str:
        template = str(self._config_value(USER_LIMIT_MESSAGE_KEY) or "")
        if not template:
            return ""
        replacements = {
            "{nickname}": str(context.get("nickname") or ""),
            "{user_id}": str(context.get("user_id") or ""),
            "{used}": _format_tokens(int(context.get("used") or 0)),
            "{limit}": _format_tokens(int(context.get("limit") or 0)),
        }
        message = template
        for token, value in replacements.items():
            message = message.replace(token, value)
        return message

    async def _send_user_limit_message(
        self,
        event: AstrMessageEvent,
        context: dict[str, Any],
    ) -> None:
        if not bool(self._config_value(SEND_USER_LIMIT_MESSAGE_KEY)):
            return
        message = self._format_user_limit_message(context)
        if not message:
            return
        try:
            await event.send(MessageChain().message(message))
        except Exception as exc:  # pragma: no cover - defensive.
            logger.warning("Failed to send per-user token limit message: %s", exc)

    # ---- chat commands -----------------------------------------------------

    async def _sponsor_mutate(
        self,
        event: AstrMessageEvent,
        action: str,
        target: str,
        user_id: str,
    ) -> None:
        normalized_user_id = _normalize_group_id(user_id)
        if not normalized_user_id:
            # 支持在群内用艾特（@成员）代替 QQ 号指定目标用户。
            at_ids = self._event_at_target_ids(event)
            if at_ids:
                normalized_user_id = _normalize_group_id(at_ids[0])
        if not normalized_user_id:
            yield event.plain_result(
                "用法：/sponsor {add|remove|super|unsuper} <QQ号 或 @群成员>"
            )
            return
        if target not in {SPONSOR_USERS_KEY, SUPER_ADMIN_USERS_KEY}:
            yield event.plain_result("未知的目标分组。")
            return
        current = (
            self._sponsor_users()
            if target == SPONSOR_USERS_KEY
            else self._super_admin_users()
        )
        if action == "add":
            if normalized_user_id in current:
                yield event.plain_result(f"用户 {normalized_user_id} 已在目标分组中。")
                return
            current = current + [normalized_user_id]
        elif action == "remove":
            if normalized_user_id not in current:
                yield event.plain_result(f"用户 {normalized_user_id} 不在目标分组中。")
                return
            current = [item for item in current if item != normalized_user_id]
        else:
            yield event.plain_result("未知的操作。")
            return

        if target == SPONSOR_USERS_KEY:
            self._set_sponsor_users(current)
        else:
            self._set_super_admin_users(current)
        self._save_config_preserving_group_settings()
        group_label = "赞助用户" if target == SPONSOR_USERS_KEY else "超级管理员"
        yield event.plain_result(
            f"已将用户 {normalized_user_id} 加入「{group_label}」分组。"
            if action == "add"
            else f"已将用户 {normalized_user_id} 移出「{group_label}」分组。"
        )

    def _user_name_map(self, user_ids: list[str]) -> dict[str, str]:
        names = self._load_user_names()
        return {
            user_id: names.get(user_id, "")
            for user_id in user_ids
            if user_id
        }

    @staticmethod
    def _message_at_target(item: Any) -> str:
        """从一条 At 消息组件中解析目标 QQ 号。"""
        data = _message_component_data(item)
        nested_data = data.get("data") if isinstance(data, dict) else None
        for obj in (nested_data, data, item):
            if isinstance(obj, dict):
                for name in ("qq", "id", "user_id", "target"):
                    value = obj.get(name)
                    if value not in (None, ""):
                        return str(value)
            else:
                for name in ("qq", "id", "user_id", "target"):
                    value = getattr(obj, name, None)
                    if value not in (None, ""):
                        return str(value)
        target = getattr(item, "target_id", None)
        if callable(target):
            try:
                resolved = str(target() or "")
            except Exception:
                resolved = ""
            if resolved and resolved != "all":
                return resolved
        return ""

    def _event_at_target_ids(self, event: Any) -> list[str]:
        """提取消息里被 @（艾特）的群成员 QQ 号列表（跳过 @全体与机器人自身）。"""
        bot_id = _sanitize_user_id(self._event_self_id(event))
        target_ids: list[str] = []
        seen: set[str] = set()
        for item in self._iter_event_message_items(event):
            if _message_component_type(item) != "at":
                continue
            raw_target = _message_at_target(item)
            target_id = _sanitize_user_id(raw_target)
            if not target_id or target_id == "all" or target_id in seen:
                continue
            if bot_id and target_id == bot_id:
                continue
            seen.add(target_id)
            target_ids.append(target_id)
        return target_ids

    # ---- user quota query --------------------------------------------------

    async def _user_quota_query(self, event: AstrMessageEvent):
        user_id = _sanitize_user_id(self._event_user_id(event))
        if not user_id:
            yield event.plain_result("无法识别你的 QQ 账号，请稍后重试。")
            return
        nickname = self._event_user_name(event) or user_id
        tier = self._user_tier(user_id)
        tier_label = {
            TIER_SUPER_ADMIN: "超级管理员",
            TIER_SPONSOR: "赞助用户",
            TIER_NORMAL: "普通用户",
        }.get(tier, tier)
        limit, _effective_tier = self._user_effective_limit(user_id)
        lines = [
            f"昵称：{nickname}（{user_id}）",
            f"身份：{tier_label}",
        ]
        if limit is None:
            lines.append("额度：无限制（不阻断）")
            yield event.plain_result("\n".join(lines))
            return
        lines.append(f"总额度：{_format_tokens(limit)}")
        limit_state = await self._user_usage_total_for_event(event)
        if not limit_state:
            lines.append("当前未启用本群用户额度统计。")
            yield event.plain_result("\n".join(lines))
            return
        used = max(0, int(limit_state.get("used") or 0))
        lines.append(f"已用：{_format_tokens(used)}")
        lines.append(f"剩余：{_format_tokens(max(0, limit - used))}")
        yield event.plain_result("\n".join(lines))

    # ---- web api -----------------------------------------------------------

    async def api_get_sponsors(self) -> dict:
        sponsors = self._sponsor_users()
        super_admins = self._super_admin_users()
        return _ok(
            {
                "sponsor_users": sponsors,
                "super_admin_users": super_admins,
                "sponsor_user_names": self._user_name_map(sponsors),
                "super_admin_user_names": self._user_name_map(super_admins),
                "sponsor_daily_token_limit": self._sponsor_daily_limit(),
                "user_daily_token_limit": self._user_daily_limit(),
            }
        )

    async def api_get_user_name(self) -> dict:
        user_id = _normalize_group_id(request.args.get("user_id"))
        if not user_id:
            return _error("user_id is required.")
        nickname = await self._resolve_user_nickname(user_id)
        return _ok({"user_id": user_id, "nickname": nickname})

    async def api_save_sponsors(self) -> dict:
        payload = await request.get_json(silent=True)
        if not isinstance(payload, dict):
            return _error("请求体必须是 JSON 对象。")

        changed = False

        if isinstance(payload.get("sponsor_users"), (list, str)):
            self._set_sponsor_users(payload.get("sponsor_users"))
            changed = True
        if isinstance(payload.get("super_admin_users"), (list, str)):
            self._set_super_admin_users(payload.get("super_admin_users"))
            changed = True

        action = str(payload.get("action") or "").strip()
        target = str(payload.get("target") or "").strip()
        user_id = _normalize_group_id(payload.get("user_id"))
        if action in {"add", "remove"} and target in {
            SPONSOR_USERS_KEY,
            SUPER_ADMIN_USERS_KEY,
        }:
            current = (
                self._sponsor_users()
                if target == SPONSOR_USERS_KEY
                else self._super_admin_users()
            )
            if action == "add" and user_id and user_id not in current:
                current = current + [user_id]
                changed = True
            elif action == "remove" and user_id in current:
                current = [item for item in current if item != user_id]
                changed = True
            if target == SPONSOR_USERS_KEY:
                self._set_sponsor_users(current)
            else:
                self._set_super_admin_users(current)

        if not changed:
            return _error("没有需要更新的赞助用户配置。")

        self._save_config_preserving_group_settings()
        return _ok(
            {
                "sponsor_users": self._sponsor_users(),
                "super_admin_users": self._super_admin_users(),
            }
        )