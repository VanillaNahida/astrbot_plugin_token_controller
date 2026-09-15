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
    GROUP_NAMES_FILE,
    USER_NAMES_FILE,
    PLUGIN_DATA_FILES,
    MAX_GROUP_REMARK_LENGTH,
    MAX_GROUP_NAME_LENGTH,
    MAX_USER_NAME_LENGTH,
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
    GROUP_NAMES_FILE,
    USER_NAMES_FILE,
    PLUGIN_DATA_FILES,
    MAX_GROUP_REMARK_LENGTH,
    MAX_GROUP_NAME_LENGTH,
    MAX_USER_NAME_LENGTH,
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



class GroupSettingsMixin:

    def _resolve_group_remarks_path(self) -> Path:
        if StarTools is not None:
            try:
                data_dir = StarTools.get_data_dir(PLUGIN_NAME)
                self._copy_legacy_data_files(data_dir)
                return data_dir / GROUP_REMARKS_FILE
            except Exception as exc:
                logger.warning("Failed to get plugin data dir; using plugin dir: %s", exc)
        return Path(__file__).resolve().with_name(GROUP_REMARKS_FILE)


    def _copy_legacy_data_files(self, data_dir: Path) -> None:
        if LEGACY_PLUGIN_NAME == PLUGIN_NAME:
            return
        legacy_data_dir = data_dir.with_name(LEGACY_PLUGIN_NAME)
        if legacy_data_dir == data_dir or not legacy_data_dir.exists():
            return
        try:
            data_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            logger.warning("Failed to prepare plugin data dir for migration: %s", exc)
            return
        for file_name in PLUGIN_DATA_FILES:
            source = legacy_data_dir / file_name
            target = data_dir / file_name
            if target.exists() or not source.is_file():
                continue
            try:
                target.write_bytes(source.read_bytes())
            except Exception as exc:
                logger.warning(
                    "Failed to copy legacy plugin data file %s: %s",
                    file_name,
                    exc,
                )


    def _resolve_group_limits_path(self) -> Path:
        remarks_path = getattr(self, "group_remarks_path", None)
        if isinstance(remarks_path, Path):
            return remarks_path.with_name(GROUP_LIMITS_FILE)
        return Path(__file__).resolve().with_name(GROUP_LIMITS_FILE)


    def _load_group_remarks(self) -> dict[str, str]:
        if not self.group_remarks_path.exists():
            return {}
        try:
            raw_data = json.loads(self.group_remarks_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Failed to read QQ group remarks: %s", exc)
            return {}
        if not isinstance(raw_data, dict):
            return {}

        remarks: dict[str, str] = {}
        for group_id, remark in raw_data.items():
            normalized_group_id = _normalize_group_id(group_id)
            normalized_remark = self._sanitize_group_remark(remark)
            if normalized_group_id and normalized_remark:
                remarks[normalized_group_id] = normalized_remark
        return remarks


    def _save_group_remarks(self, remarks: dict[str, str]) -> None:
        try:
            self.group_remarks_path.parent.mkdir(parents=True, exist_ok=True)
            self.group_remarks_path.write_text(
                json.dumps(remarks, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            raise ValueError(f"保存 QQ 群备注失败: {exc}") from exc


    @staticmethod
    def _sanitize_group_remark(value: Any) -> str:
        return str(value or "").strip()[:MAX_GROUP_REMARK_LENGTH]


    def _resolve_group_names_path(self) -> Path:
        remarks_path = getattr(self, "group_remarks_path", None)
        if isinstance(remarks_path, Path):
            return remarks_path.with_name(GROUP_NAMES_FILE)
        return Path(__file__).resolve().with_name(GROUP_NAMES_FILE)


    def _load_group_names(self) -> dict[str, str]:
        names_path = getattr(self, "group_names_path", None)
        if not isinstance(names_path, Path) or not names_path.exists():
            return {}
        try:
            raw_data = json.loads(names_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Failed to read QQ group names: %s", exc)
            return {}
        if not isinstance(raw_data, dict):
            return {}
        names: dict[str, str] = {}
        for group_id, name in raw_data.items():
            normalized_group_id = _normalize_group_id(group_id)
            normalized_name = str(name or "").strip()[:MAX_GROUP_NAME_LENGTH]
            if normalized_group_id and normalized_name:
                names[normalized_group_id] = normalized_name
        return names


    def _save_group_names(self, names: dict[str, str]) -> None:
        try:
            names_path = getattr(self, "group_names_path", None)
            if not isinstance(names_path, Path):
                return
            names_path.parent.mkdir(parents=True, exist_ok=True)
            names_path.write_text(
                json.dumps(names, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning("Failed to save QQ group names: %s", exc)


    def _ensure_group_name(self, group_id: str, group_name: str) -> None:
        normalized_group_id = _normalize_group_id(group_id)
        normalized_name = str(group_name or "").strip()[:MAX_GROUP_NAME_LENGTH]
        if not normalized_group_id or not normalized_name:
            return
        names = self._load_group_names()
        if names.get(normalized_group_id) == normalized_name:
            return
        names[normalized_group_id] = normalized_name
        self._save_group_names(names)


    def _adapter_call_actions(self) -> list[Any]:
        sources = []
        platform_manager = getattr(self, "context", None)
        platform_manager = getattr(platform_manager, "platform_manager", None)
        platform_insts = getattr(platform_manager, "platform_insts", None)
        for platform in platform_insts or []:
            bot = getattr(platform, "bot", None)
            call_action = getattr(bot, "call_action", None)
            if callable(call_action):
                sources.append(call_action)
        return sources


    async def _refresh_group_names_from_adapter(
        self,
        group_ids: list[str] | None = None,
        force: bool = False,
    ) -> int:
        """Fetch missing (or all, when ``force``) QQ group names from the adapter.

        Uses ``get_group_info(group_id=...)`` which returns ``group_name``.
        ``force`` also re-queries groups that already have a cached name.
        Returns the number of names newly filled.
        """
        if group_ids is None:
            group_ids = self._limited_groups()
        cached = self._load_group_names()
        if force:
            requested = [group_id for group_id in group_ids if _normalize_group_id(group_id)]
        else:
            requested = [
                group_id
                for group_id in group_ids
                if group_id not in cached
            ]
        if not requested:
            return 0
        sources = self._adapter_call_actions()
        if not sources:
            return 0
        updated = 0
        for call_action in sources:
            for group_id in requested:
                name = ""
                try:
                    info = await call_action(
                        "get_group_info",
                        group_id=group_id,
                    )
                except Exception as exc:
                    logger.warning(
                        "Failed to fetch group name via adapter for %s: %s",
                        group_id,
                        exc,
                    )
                    continue
                if isinstance(info, dict):
                    name = str(info.get("group_name") or "").strip()
                if name:
                    self._ensure_group_name(group_id, name)
                    updated += 1
        return updated


    async def _all_groups_from_adapter(self) -> list[dict[str, str]]:
        """Fetch the full QQ group list from the platform adapter (OneBot API).

        Uses ``get_group_list`` which returns items with ``group_id`` and
        ``group_name``. Newly seen group names are persisted via
        ``_ensure_group_name``. Returns a list of ``{"group_id", "group_name"}``.
        """
        sources = self._adapter_call_actions()
        if not sources:
            return []
        groups: list[dict[str, str]] = []
        seen: set[str] = set()
        for call_action in sources:
            try:
                raw = await call_action("get_group_list")
            except Exception as exc:
                logger.warning(
                    "Failed to fetch QQ group list via adapter: %s",
                    exc,
                )
                continue
            items = raw
            if isinstance(raw, dict) and isinstance(raw.get("data"), list):
                items = raw["data"]
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                group_id = _normalize_group_id(item.get("group_id"))
                group_name = str(item.get("group_name") or "").strip()
                if not group_id or group_id in seen:
                    continue
                seen.add(group_id)
                if group_name:
                    self._ensure_group_name(group_id, group_name)
                groups.append({"group_id": group_id, "group_name": group_name})
        return groups


    def _resolve_user_names_path(self) -> Path:
        remarks_path = getattr(self, "group_remarks_path", None)
        if isinstance(remarks_path, Path):
            return remarks_path.with_name(USER_NAMES_FILE)
        return Path(__file__).resolve().with_name(USER_NAMES_FILE)


    def _load_user_names(self) -> dict[str, str]:
        names_path = getattr(self, "user_names_path", None)
        if not isinstance(names_path, Path) or not names_path.exists():
            return {}
        try:
            raw_data = json.loads(names_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Failed to read QQ user names: %s", exc)
            return {}
        if not isinstance(raw_data, dict):
            return {}
        names: dict[str, str] = {}
        for user_id, name in raw_data.items():
            normalized_user_id = _normalize_group_id(user_id)
            normalized_name = str(name or "").strip()[:MAX_USER_NAME_LENGTH]
            if normalized_user_id and normalized_name:
                names[normalized_user_id] = normalized_name
        return names


    def _save_user_names(self, names: dict[str, str]) -> None:
        try:
            names_path = getattr(self, "user_names_path", None)
            if not isinstance(names_path, Path):
                return
            names_path.parent.mkdir(parents=True, exist_ok=True)
            names_path.write_text(
                json.dumps(names, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning("Failed to save QQ user names: %s", exc)


    def _ensure_user_name(self, user_id: str, user_name: str) -> str:
        normalized_user_id = _normalize_group_id(user_id)
        normalized_name = str(user_name or "").strip()[:MAX_USER_NAME_LENGTH]
        if not normalized_user_id:
            return ""
        if not normalized_name:
            return self._load_user_names().get(normalized_user_id, "")
        names = self._load_user_names()
        if names.get(normalized_user_id) == normalized_name:
            return normalized_name
        names[normalized_user_id] = normalized_name
        self._save_user_names(names)
        return normalized_name


    async def _resolve_user_nickname(self, user_id: str) -> str:
        normalized_user_id = _normalize_group_id(user_id)
        if not normalized_user_id:
            return ""
        cached = self._load_user_names().get(normalized_user_id, "")
        if cached:
            return cached
        for call_action in self._adapter_call_actions():
            try:
                info = await call_action(
                    "get_stranger_info",
                    user_id=int(normalized_user_id),
                )
            except Exception as exc:
                logger.warning(
                    "Failed to fetch nickname via adapter for %s: %s",
                    normalized_user_id,
                    exc,
                )
                continue
            if isinstance(info, dict):
                name = str(info.get("nickname") or "").strip()
                if name:
                    return self._ensure_user_name(normalized_user_id, name)
        return cached


    def _normalize_group_settings_data(
        self,
        raw_data: Any,
    ) -> dict[str, dict[str, Any]]:
        if isinstance(raw_data, str):
            try:
                raw_data = json.loads(raw_data)
            except (TypeError, ValueError):
                return {}
        if not isinstance(raw_data, dict):
            return {}

        settings: dict[str, dict[str, Any]] = {}
        for group_id, value in raw_data.items():
            normalized_group_id = _normalize_group_id(group_id)
            if not normalized_group_id:
                continue
            if isinstance(value, dict):
                item: dict[str, Any] = {}
                if GROUP_SETTING_DAILY_LIMIT in value:
                    try:
                        item[GROUP_SETTING_DAILY_LIMIT] = max(
                            0,
                            int(value.get(GROUP_SETTING_DAILY_LIMIT) or 0),
                        )
                    except (TypeError, ValueError):
                        pass
                if bool(value.get(GROUP_SETTING_ONLY_AT_BOT, False)):
                    item[GROUP_SETTING_ONLY_AT_BOT] = True
                if bool(value.get(GROUP_SETTING_CONTEXT_LIMIT_05, False)):
                    item[GROUP_SETTING_CONTEXT_LIMIT_05] = True
                if bool(value.get(GROUP_SETTING_PROMPT_CACHE_KEY_STRATEGY, False)):
                    item[GROUP_SETTING_PROMPT_CACHE_KEY_STRATEGY] = True
                if bool(value.get(GROUP_SETTING_DEEPSEEK_CACHE_STRATEGY, False)):
                    item[GROUP_SETTING_DEEPSEEK_CACHE_STRATEGY] = True
                if item:
                    settings[normalized_group_id] = item
                continue
            try:
                settings[normalized_group_id] = {
                    GROUP_SETTING_DAILY_LIMIT: max(0, int(value or 0))
                }
            except (TypeError, ValueError):
                continue
        return settings


    @staticmethod
    def _merge_group_settings(
        base: dict[str, dict[str, Any]],
        extra: dict[str, dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        merged = {group_id: dict(settings) for group_id, settings in base.items()}
        for group_id, settings in extra.items():
            item = dict(merged.get(group_id, {}))
            item.update(settings)
            if item:
                merged[group_id] = item
        return merged


    def _read_group_settings_file(self, path: Path) -> dict[str, dict[str, Any]]:
        if not path.exists():
            return {}
        try:
            raw_data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning(
                "Failed to read QQ group personalized settings from %s: %s",
                path,
                exc,
            )
            return {}
        return self._normalize_group_settings_data(raw_data)


    def _write_group_settings_file(
        self,
        settings: dict[str, dict[str, Any]],
    ) -> None:
        normalized_settings = self._normalize_group_settings_data(settings)
        self.group_limits_path.parent.mkdir(parents=True, exist_ok=True)
        self.group_limits_path.write_text(
            json.dumps(normalized_settings, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


    def _load_group_settings_config_backup(self) -> dict[str, dict[str, Any]]:
        try:
            raw_data = (
                self.config.get(GROUP_SETTINGS_CONFIG_BACKUP_KEY)
                if hasattr(self.config, "get")
                else (
                    self.config[GROUP_SETTINGS_CONFIG_BACKUP_KEY]
                    if GROUP_SETTINGS_CONFIG_BACKUP_KEY in self.config
                    else None
                )
            )
        except Exception:
            return {}
        return self._normalize_group_settings_data(raw_data)


    def _set_group_settings_config_backup(
        self,
        settings: dict[str, dict[str, Any]],
        *,
        save_config: bool = True,
    ) -> bool:
        normalized_settings = self._normalize_group_settings_data(settings)
        try:
            current_settings = self._load_group_settings_config_backup()
            changed = current_settings != normalized_settings
            if changed:
                if normalized_settings:
                    self.config[GROUP_SETTINGS_CONFIG_BACKUP_KEY] = normalized_settings
                else:
                    self.config.pop(GROUP_SETTINGS_CONFIG_BACKUP_KEY, None)
            if changed and save_config:
                config_save = getattr(self.config, "save_config", None)
                if callable(config_save):
                    config_save()
            return True
        except Exception as exc:
            logger.warning("Failed to save group settings config backup: %s", exc)
            return False


    def _group_settings_fallback_paths(self) -> list[Path]:
        paths: list[Path] = []
        legacy_path = (
            self.group_limits_path.parent.with_name(LEGACY_PLUGIN_NAME)
            / GROUP_LIMITS_FILE
        )
        plugin_dir_path = Path(__file__).resolve().with_name(GROUP_LIMITS_FILE)
        for path in (legacy_path, plugin_dir_path):
            if path != self.group_limits_path and path not in paths:
                paths.append(path)
        return paths


    def _load_group_settings(self) -> dict[str, dict[str, Any]]:
        primary_exists = self.group_limits_path.exists()
        config_backup_settings = self._load_group_settings_config_backup()
        settings: dict[str, dict[str, Any]] = {}
        settings = self._merge_group_settings(settings, config_backup_settings)
        primary_settings = self._read_group_settings_file(self.group_limits_path)
        settings = self._merge_group_settings(settings, primary_settings)

        if not primary_exists and not settings:
            for path in self._group_settings_fallback_paths():
                fallback_settings = self._read_group_settings_file(path)
                if fallback_settings:
                    settings = self._merge_group_settings(settings, fallback_settings)

        if settings:
            if settings != primary_settings:
                try:
                    self._write_group_settings_file(settings)
                except Exception as exc:
                    logger.warning(
                        "Failed to backfill group settings file from backup: %s",
                        exc,
                    )
            self._set_group_settings_config_backup(settings)
        return settings


    def _save_group_settings(self, settings: dict[str, dict[str, Any]]) -> None:
        normalized_settings = self._normalize_group_settings_data(settings)
        file_error: Exception | None = None
        try:
            self._write_group_settings_file(normalized_settings)
        except Exception as exc:
            file_error = exc

        backup_saved = self._set_group_settings_config_backup(normalized_settings)
        if file_error is not None:
            if backup_saved:
                logger.warning(
                    "Failed to save group settings file; config backup was updated: %s",
                    file_error,
                )
                return
            raise ValueError(f"保存 QQ 群个性化配置失败: {file_error}") from file_error


    def _load_group_limits(self) -> dict[str, int]:
        limits: dict[str, int] = {}
        for group_id, settings in self._load_group_settings().items():
            if GROUP_SETTING_DAILY_LIMIT in settings:
                limits[group_id] = max(0, int(settings[GROUP_SETTING_DAILY_LIMIT] or 0))
        return limits


    def _save_group_limits(self, limits: dict[str, int]) -> None:
        current_settings = self._load_group_settings()
        next_settings: dict[str, dict[str, Any]] = {}
        for group_id, settings in current_settings.items():
            item: dict[str, Any] = {}
            if bool(settings.get(GROUP_SETTING_ONLY_AT_BOT, False)):
                item[GROUP_SETTING_ONLY_AT_BOT] = True
            if bool(settings.get(GROUP_SETTING_CONTEXT_LIMIT_05, False)):
                item[GROUP_SETTING_CONTEXT_LIMIT_05] = True
            if bool(settings.get(GROUP_SETTING_PROMPT_CACHE_KEY_STRATEGY, False)):
                item[GROUP_SETTING_PROMPT_CACHE_KEY_STRATEGY] = True
            if bool(settings.get(GROUP_SETTING_DEEPSEEK_CACHE_STRATEGY, False)):
                item[GROUP_SETTING_DEEPSEEK_CACHE_STRATEGY] = True
            if item:
                next_settings[group_id] = item
        for group_id, limit in limits.items():
            normalized_group_id = _normalize_group_id(group_id)
            if not normalized_group_id:
                continue
            item = dict(next_settings.get(normalized_group_id, {}))
            item[GROUP_SETTING_DAILY_LIMIT] = max(0, int(limit or 0))
            next_settings[normalized_group_id] = item
        self._save_group_settings(next_settings)


    def _daily_limit_for_group(
        self,
        group_id: str,
        group_limits: dict[str, int] | None = None,
    ) -> int:
        normalized_group_id = _normalize_group_id(group_id)
        limits = group_limits if group_limits is not None else self._load_group_limits()
        if normalized_group_id in limits:
            return max(0, int(limits[normalized_group_id] or 0))
        return self._daily_limit()


    def _group_only_at_bot_llm(
        self,
        group_id: str,
        group_settings: dict[str, dict[str, Any]] | None = None,
    ) -> bool:
        normalized_group_id = _normalize_group_id(group_id)
        settings = (
            group_settings
            if group_settings is not None
            else self._load_group_settings()
        )
        return bool(
            settings.get(normalized_group_id, {}).get(
                GROUP_SETTING_ONLY_AT_BOT,
                False,
            )
        )


    def _group_context_limit_05(
        self,
        group_id: str,
        group_settings: dict[str, dict[str, Any]] | None = None,
    ) -> bool:
        normalized_group_id = _normalize_group_id(group_id)
        settings = (
            group_settings
            if group_settings is not None
            else self._load_group_settings()
        )
        return bool(
            settings.get(normalized_group_id, {}).get(
                GROUP_SETTING_CONTEXT_LIMIT_05,
                False,
            )
        )


    def _group_prompt_cache_key_strategy(
        self,
        group_id: str,
        group_settings: dict[str, dict[str, Any]] | None = None,
    ) -> bool:
        normalized_group_id = _normalize_group_id(group_id)
        settings = (
            group_settings
            if group_settings is not None
            else self._load_group_settings()
        )
        return bool(
            settings.get(normalized_group_id, {}).get(
                GROUP_SETTING_PROMPT_CACHE_KEY_STRATEGY,
                False,
            )
        )


    def _group_deepseek_cache_strategy(
        self,
        group_id: str,
        group_settings: dict[str, dict[str, Any]] | None = None,
    ) -> bool:
        normalized_group_id = _normalize_group_id(group_id)
        settings = (
            group_settings
            if group_settings is not None
            else self._load_group_settings()
        )
        return bool(
            settings.get(normalized_group_id, {}).get(
                GROUP_SETTING_DEEPSEEK_CACHE_STRATEGY,
                False,
            )
        )


    def _group_context_limit_tokens(
        self,
        group_id: str,
        group_settings: dict[str, dict[str, Any]] | None = None,
        group_limits: dict[str, int] | None = None,
    ) -> int:
        if not self._group_context_limit_05(group_id, group_settings):
            return 0
        limit = self._daily_limit_for_group(group_id, group_limits)
        return max(1, int(limit * TOKEN_LIMIT_CONTEXT_RATIO))
