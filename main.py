from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.provider import ProviderRequest
from astrbot.core.message.message_event_result import MessageChain

try:
    from astrbot.api.star import Context, Star, StarTools
except ImportError:  # pragma: no cover - kept for older AstrBot builds.
    from astrbot.api.star import Context, Star

    StarTools = None  # type: ignore[assignment]

try:
    from .backend.config import ConfigMixin, PLUGIN_NAME, _format_tokens
    from .backend.events import EventMixin
    from .backend.group_settings import GroupSettingsMixin
    from .backend.history_stats import HistoryStatsMixin
    from .backend.provider_strategy import ProviderStrategyMixin
    from .backend.sponsor import (
        SPONSOR_USERS_KEY,
        SUPER_ADMIN_USERS_KEY,
        SPONSOR_DAILY_TOKEN_LIMIT_KEY,
        SponsorMixin,
    )
    from .backend.usage import UsageMixin
    from .backend.user_limits import UserLimitMixin
    from .backend.user_stats import UserStatsMixin
    from .backend.web_api import WebApiMixin
except ImportError:  # pragma: no cover - compatible with direct module loading.
    from backend.config import ConfigMixin, PLUGIN_NAME, _format_tokens
    from backend.events import EventMixin
    from backend.group_settings import GroupSettingsMixin
    from backend.history_stats import HistoryStatsMixin
    from backend.provider_strategy import ProviderStrategyMixin
    from backend.sponsor import (
        SPONSOR_USERS_KEY,
        SUPER_ADMIN_USERS_KEY,
        SPONSOR_DAILY_TOKEN_LIMIT_KEY,
        SponsorMixin,
    )
    from backend.usage import UsageMixin
    from backend.user_limits import UserLimitMixin
    from backend.user_stats import UserStatsMixin
    from backend.web_api import WebApiMixin



class Main(
    SponsorMixin,
    WebApiMixin,
    UsageMixin,
    EventMixin,
    ProviderStrategyMixin,
    GroupSettingsMixin,
    ConfigMixin,
    UserLimitMixin,
    UserStatsMixin,
    HistoryStatsMixin,
    Star,
):



    def __init__(self, context: Context, config: dict | None = None) -> None:
        super().__init__(context)
        self.context = context
        self.config = config if config is not None else {}
        self.group_remarks_path = self._resolve_group_remarks_path()
        self.group_limits_path = self._resolve_group_limits_path()
        self.group_names_path = self._resolve_group_names_path()
        self.user_names_path = self._resolve_user_names_path()
        self.history_stats_path = self._resolve_history_stats_path()
        self.user_stats_path = self._resolve_user_stats_path()
        self._history_last_sync_attempt: datetime | None = None
        self._history_sync_lock = asyncio.Lock()
        self._group_names_refresh_task: asyncio.Task | None = None
        self._user_last_sync_attempts: dict[str, datetime] = {}
        self._user_sync_lock = asyncio.Lock()
        self._ensure_history_tracking_for_current_groups()
        self._ensure_user_tracking_for_current_groups()
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/config",
            self.api_get_config,
            ["GET"],
            "获取 QQ 群 token 限流插件配置",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/config",
            self.api_save_config,
            ["POST"],
            "保存 QQ 群 token 限流插件配置",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/usage",
            self.api_get_usage,
            ["GET"],
            "获取 QQ 群 token 用量统计",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/history",
            self.api_get_history,
            ["GET"],
            "获取 QQ 群 token 历史用量统计",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/user-usage",
            self.api_get_user_usage,
            ["GET"],
            "获取 QQ 群今日用户 token 用量统计",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/providers",
            self.api_get_providers,
            ["GET"],
            "获取可用于回退的 LLM 模型供应商列表",
        )

        self.context.register_web_api(
            f"/{PLUGIN_NAME}/remarks",
            self.api_get_remarks,
            ["GET"],
            "Get QQ group remarks",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/remarks",
            self.api_save_remark,
            ["POST"],
            "Save QQ group remark",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/group-settings",
            self.api_get_group_settings,
            ["GET"],
            "Get QQ group personalized settings",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/group-settings",
            self.api_save_group_settings,
            ["POST"],
            "Save QQ group personalized settings",
        )

        self.context.register_web_api(
            f"/{PLUGIN_NAME}/sponsors",
            self.api_get_sponsors,
            ["GET"],
            "获取赞助用户与超级管理员列表",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/sponsors",
            self.api_save_sponsors,
            ["POST"],
            "保存赞助用户与超级管理员列表",
        )

        self.context.register_web_api(
            f"/{PLUGIN_NAME}/managed-groups",
            self.api_get_managed_groups,
            ["GET"],
            "获取可限流的 QQ 群聊列表（含群名）",
        )
        self.context.register_web_api(
            f"/{PLUGIN_NAME}/limited-groups",
            self.api_save_limited_groups,
            ["POST"],
            "保存限流 QQ 群聊配置",
        )

        self.context.register_web_api(
            f"/{PLUGIN_NAME}/user-name",
            self.api_get_user_name,
            ["GET"],
            "通过 QQ 号获取用户昵称（OneBot API，带缓存）",
        )





    # ---- sponsor management commands -----------------------------------

    @filter.command_group("sponsor")
    def sponsor_group(self):
        """赞助用户管理指令组"""
        pass

    @sponsor_group.command("add")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def sponsor_add(self, event: AstrMessageEvent, user_id: str = "") -> None:
        """将一个 QQ 号添加为赞助用户"""
        async for result in self._sponsor_mutate(
            event, "add", SPONSOR_USERS_KEY, user_id
        ):
            yield result
        event.stop_event()

    @sponsor_group.command("添加")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def sponsor_add_zh(self, event: AstrMessageEvent, user_id: str = "") -> None:
        async for result in self._sponsor_mutate(
            event, "add", SPONSOR_USERS_KEY, user_id
        ):
            yield result
        event.stop_event()

    @sponsor_group.command("remove")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def sponsor_remove(self, event: AstrMessageEvent, user_id: str = "") -> None:
        """将一个 QQ 号从赞助用户中移除"""
        async for result in self._sponsor_mutate(
            event, "remove", SPONSOR_USERS_KEY, user_id
        ):
            yield result
        event.stop_event()

    @sponsor_group.command("移除")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def sponsor_remove_zh(self, event: AstrMessageEvent, user_id: str = "") -> None:
        async for result in self._sponsor_mutate(
            event, "remove", SPONSOR_USERS_KEY, user_id
        ):
            yield result
        event.stop_event()

    @sponsor_group.command("super")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def sponsor_super(self, event: AstrMessageEvent, user_id: str = "") -> None:
        """将一个 QQ 号添加为超级管理员"""
        async for result in self._sponsor_mutate(
            event, "add", SUPER_ADMIN_USERS_KEY, user_id
        ):
            yield result
        event.stop_event()

    @sponsor_group.command("超级")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def sponsor_super_zh(self, event: AstrMessageEvent, user_id: str = "") -> None:
        async for result in self._sponsor_mutate(
            event, "add", SUPER_ADMIN_USERS_KEY, user_id
        ):
            yield result
        event.stop_event()

    @sponsor_group.command("unsuper")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def sponsor_unsuper(self, event: AstrMessageEvent, user_id: str = "") -> None:
        """将一个 QQ 号从超级管理员中移除"""
        async for result in self._sponsor_mutate(
            event, "remove", SUPER_ADMIN_USERS_KEY, user_id
        ):
            yield result
        event.stop_event()

    @sponsor_group.command("普通")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def sponsor_unsuper_zh(self, event: AstrMessageEvent, user_id: str = "") -> None:
        async for result in self._sponsor_mutate(
            event, "remove", SUPER_ADMIN_USERS_KEY, user_id
        ):
            yield result
        event.stop_event()

    @sponsor_group.command("list")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def sponsor_list(self, event: AstrMessageEvent) -> None:
        """查看当前赞助用户与超级管理员列表"""
        sponsors = self._sponsor_users()
        super_admins = self._super_admin_users()
        sponsor_names = self._user_name_map(sponsors)
        super_names = self._user_name_map(super_admins)
        sponsor_line = "、".join(
            f"{sponsor_names.get(user_id) or ''}[{user_id}]"
            if sponsor_names.get(user_id)
            else f"{user_id}"
            for user_id in sponsors
        )
        super_line = "、".join(
            f"{super_names.get(user_id) or ''}[{user_id}]"
            if super_names.get(user_id)
            else f"{user_id}"
            for user_id in super_admins
        )
        lines = [
            "赞助用户分组：",
            sponsor_line if sponsors else "（空）",
            "超级管理员分组：",
            super_line if super_admins else "（空）",
        ]
        yield event.plain_result("\n".join(lines))
        event.stop_event()

    @sponsor_group.command("列表")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def sponsor_list_zh(self, event: AstrMessageEvent) -> None:
        async for result in self.sponsor_list(event):
            yield result
        event.stop_event()

    @sponsor_group.command("limit")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def sponsor_limit(
        self,
        event: AstrMessageEvent,
        value: str = "",
    ) -> None:
        """设置或查看赞助用户每日用量上限（-1 表示不限）"""
        value = str(value or "").strip()
        if not value:
            limit = self._sponsor_daily_limit()
            yield event.plain_result(
                f"赞助用户每日用量上限：{_format_tokens(limit) if limit >= 0 else '不限'}（-1 表示不限）"
            )
            event.stop_event()
            return
        try:
            new_limit = int(value)
        except (TypeError, ValueError):
            yield event.plain_result("用法：/sponsor limit <每日上限>，-1 表示不限。")
            event.stop_event()
            return
        if new_limit < -1:
            yield event.plain_result("每日上限不能小于 -1。")
            event.stop_event()
            return
        self.config[SPONSOR_DAILY_TOKEN_LIMIT_KEY] = new_limit
        self._save_config_preserving_group_settings()
        yield event.plain_result(
            f"赞助用户每日用量上限已更新为：{_format_tokens(new_limit) if new_limit >= 0 else '不限'}。"
        )
        event.stop_event()

    @sponsor_group.command("限额")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def sponsor_limit_zh(
        self,
        event: AstrMessageEvent,
        value: str = "",
    ) -> None:
        async for result in self.sponsor_limit(event, value):
            yield result
        event.stop_event()

    @filter.command("quota")
    async def quota(self, event: AstrMessageEvent, user_id: str = "") -> None:
        """查询当日额度；不传参查自己，携带 QQ 号或 @成员时查询对方"""
        async for result in self._user_quota_query(event, user_id):
            yield result
        event.stop_event()

    @filter.command("额度")
    async def quota_zh(self, event: AstrMessageEvent, user_id: str = "") -> None:
        async for result in self._user_quota_query(event, user_id):
            yield result
        event.stop_event()

    async def initialize(self) -> None:
        self._start_history_background_sync()
        self._start_user_background_sync()
        self._group_names_refresh_task = asyncio.create_task(
            self._background_refresh_group_names()
        )



    async def _background_refresh_group_names(self) -> None:
        # Adapters may still be connecting at startup, so retry a few times.
        for _ in range(8):
            try:
                if await self._refresh_group_names_from_adapter() > 0:
                    return
            except Exception as exc:
                logger.warning(
                    "Failed to refresh QQ group names via adapter: %s",
                    exc,
                )
            await asyncio.sleep(15)

    async def terminate(self) -> None:
        if self._group_names_refresh_task is not None:
            self._group_names_refresh_task.cancel()
            self._group_names_refresh_task = None
        await self._stop_history_background_sync()
        await self._stop_user_background_sync()





    @filter.on_waiting_llm_request(priority=1000)
    async def on_waiting_llm_request(self, event: AstrMessageEvent) -> None:
        if self._block_group_only_at_bot_invocation_if_needed(
            event,
            "waiting_llm_request",
        ):
            return
        self._remember_user_usage_event(event)
        if await self._block_user_daily_limit_if_needed(event, "waiting_llm_request"):
            return
        await self._maybe_sync_history_stats()
        await self._maybe_sync_user_stats()
        limit_context = await self._build_event_limit_context(event)
        if not limit_context:
            self._cleanup_temp_context_provider(event)
            return

        self._cleanup_temp_context_provider(event)
        if self._should_block_wake_word_invocation(event, limit_context):
            event.stop_event()
            logger.info(
                "Blocked wake-word invocation after token limit: group=%s used=%s limit=%s",
                limit_context["group_id"],
                limit_context["limit_state"]["used"],
                limit_context["limit"],
            )
            return

        limit_state = limit_context["limit_state"]
        if limit_state["status"] != "fallback":
            if limit_state["status"] == "normal":
                self._apply_context_limit_provider_if_needed(event, limit_context)
            return

        fallback_provider_id = str(limit_context["fallback_provider_id"] or "")
        if not limit_context["fallback_provider_valid"]:
            self._apply_context_limit_provider_if_needed(event, limit_context)
            logger.warning(
                "群 %s 当前窗口 token=%s 已进入回退区间，但回退模型供应商 %s 不可用；本次将交由 AstrBot 使用当前可用供应商。",
                limit_context["group_id"],
                limit_state["used"],
                fallback_provider_id or "<empty>",
            )
            return

        event.set_extra("selected_provider", fallback_provider_id)
        event.set_extra("token_limit_selected_provider", fallback_provider_id)
        self._apply_context_limit_provider_if_needed(event, limit_context)
        logger.info(
            "群 %s 当前窗口 token=%s 已达每日上限 %s，未达硬上限 %s，本次预先切换到回退模型供应商 %s。",
            limit_context["group_id"],
            limit_state["used"],
            limit_context["limit"],
            limit_state["hard_limit"],
            fallback_provider_id,
        )





    @filter.on_llm_request(priority=1000)
    async def on_llm_request(
        self,
        event: AstrMessageEvent,
        req: ProviderRequest,
    ) -> None:
        conversation_id = (
            req.conversation.cid
            if getattr(req, "conversation", None) is not None
            else None
        )
        self._remember_user_usage_event(
            event,
            conversation_id=conversation_id,
            prompt=getattr(req, "prompt", None),
        )
        if self._block_group_only_at_bot_invocation_if_needed(event, "llm_request"):
            self._cleanup_temp_context_provider(event)
            return
        if await self._block_user_daily_limit_if_needed(event, "llm_request"):
            self._cleanup_temp_context_provider(event)
            return
        limit_context = await self._build_event_limit_context(event)
        if not limit_context:
            self._cleanup_temp_context_provider(event)
            return

        if self._should_block_wake_word_invocation(event, limit_context):
            self._cleanup_temp_context_provider(event)
            event.stop_event()
            logger.info(
                "Blocked wake-word LLM request after token limit: group=%s used=%s limit=%s",
                limit_context["group_id"],
                limit_context["limit_state"]["used"],
                limit_context["limit"],
            )
            return

        group_id = limit_context["group_id"]
        limit = int(limit_context["limit"])
        window = limit_context["window"]
        limit_state = limit_context["limit_state"]
        used = int(limit_state["used"])
        stop_limit = int(limit_state["hard_limit"])

        if limit_state["status"] in {"normal", "fallback"}:
            self._apply_prompt_cache_anchor_if_needed(event, req, limit_context)
            self._trim_provider_request_context_if_needed(event, req, limit_context)
        self._cleanup_temp_context_provider(event)

        if limit_state["status"] == "normal":
            return

        if limit_state["status"] == "fallback":
            fallback_provider_id = str(limit_context["fallback_provider_id"] or "")
            if limit_context["fallback_provider_valid"]:
                event.set_extra("selected_provider", fallback_provider_id)

            selected_provider = event.get_extra("token_limit_selected_provider")
            if selected_provider:
                logger.debug(
                    "群 %s 已预先切换到回退模型供应商 %s。",
                    group_id,
                    selected_provider,
                )
            elif limit_context["fallback_provider_valid"]:
                logger.warning(
                    "群 %s 已进入回退区间，但 provider 已在 LLM 请求钩子前完成选择；请确认 on_waiting_llm_request 钩子已启用。",
                    group_id,
                )
            else:
                logger.warning(
                    "群 %s 已进入回退区间，但回退模型供应商 %s 不可用；本次交由 AstrBot 当前可用供应商处理。",
                    group_id,
                    limit_context["fallback_provider_id"] or "<empty>",
                )
            return

        if bool(self._config_value("send_block_message")):
            message_template = str(self._config_value("block_message") or "")
            message = message_template.format(
                group_id=group_id,
                used=_format_tokens(used),
                limit=_format_tokens(stop_limit),
                refresh_time=self._config_value("refresh_time"),
                window_start=window.start_local.strftime("%Y-%m-%d %H:%M"),
                window_end=window.end_local.strftime("%Y-%m-%d %H:%M"),
            )
            if message:
                await event.send(MessageChain().message(message))

        event.stop_event()
        logger.info(
            "已拦截群 %s 的 LLM 请求：当前窗口 token=%s，上限=%s",
            group_id,
            used,
            stop_limit,
        )


