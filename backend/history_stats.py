from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any

from quart import request

from astrbot.api import logger


HISTORY_STATS_FILE = "history_usage.json"
HISTORY_STATS_VERSION = 1
HISTORY_SYNC_OVERLAP = timedelta(hours=2)
HISTORY_SYNC_MIN_INTERVAL = timedelta(minutes=5)
HISTORY_BACKGROUND_SYNC_INTERVAL = 3600
HISTORY_DEFAULT_TOP_LIMIT = 10
HISTORY_RANGE_KEYS = {"24h", "7d", "30d", "all"}
MONTH_ABBR = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


@dataclass(frozen=True)
class HistoryUsageWindow:
    start_local: datetime
    end_local: datetime
    start_utc: datetime
    end_utc: datetime


def _history_ok(data: dict | list | None = None, message: str | None = None) -> dict:
    return {"status": "ok", "message": message, "data": data or {}}


def _history_error(message: str) -> dict:
    return {"status": "error", "message": message, "data": {}}


def _normalize_history_group_id(value: Any) -> str:
    text = str(value or "").strip()
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text


def _format_history_tokens(value: int, decimals: int = 2) -> str:
    normalized = max(0, int(value or 0))
    precision = max(0, int(decimals))
    if normalized >= 1_000_000:
        return f"{normalized / 1_000_000:.{precision}f} M"
    if normalized >= 1_000:
        return f"{normalized / 1_000:.{precision}f} K"
    return str(normalized)


def _local_timezone() -> tzinfo:
    tz = datetime.now().astimezone().tzinfo
    return tz or timezone.utc


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _hour_start(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _month_key(value: datetime) -> str:
    local_value = value.astimezone(_local_timezone())
    return f"{local_value.year:04d}-{local_value.month:02d}"


def _hour_label(value: datetime) -> str:
    return value.astimezone(_local_timezone()).strftime("%H:00")


def _hour_range_label(start: datetime, end: datetime) -> str:
    if start == end:
        return _hour_label(start)
    local_tz = _local_timezone()
    start_local = start.astimezone(local_tz)
    end_local = end.astimezone(local_tz)
    return f"{start_local:%H}-{end_local:%H}"


def _month_abbr(month: int) -> str:
    if 1 <= month <= 12:
        return MONTH_ABBR[month - 1]
    return f"{month:02d}"


def _day_range_label(start, end, use_month_name: bool = False) -> str:
    if use_month_name:
        if start == end:
            return f"{_month_abbr(start.month)} {start:%d}"
        if start.month == end.month:
            return f"{_month_abbr(start.month)} {start:%d}-{end:%d}"
        return f"{_month_abbr(start.month)} {start:%d}-{_month_abbr(end.month)} {end:%d}"
    if start == end:
        return start.strftime("%m-%d")
    if start.month == end.month:
        return f"{start:%m-%d}-{end:%d}"
    return f"{start:%m-%d}-{end:%m-%d}"


class HistoryStatsMixin:
    """Persistent hourly token usage history based on AstrBot ProviderStat."""

    def _resolve_history_stats_path(self) -> Path:
        remarks_path = getattr(self, "group_remarks_path", None)
        if isinstance(remarks_path, Path):
            return remarks_path.with_name(HISTORY_STATS_FILE)
        return Path(__file__).resolve().parents[1] / HISTORY_STATS_FILE

    def _load_history_stats(self) -> dict[str, Any]:
        path = getattr(self, "history_stats_path", None)
        if not isinstance(path, Path) or not path.exists():
            return self._empty_history_stats()
        try:
            raw_data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Failed to read token history stats: %s", exc)
            return self._empty_history_stats()
        return self._sanitize_history_stats(raw_data)

    def _save_history_stats(self, data: dict[str, Any]) -> None:
        path = getattr(self, "history_stats_path", None)
        if not isinstance(path, Path):
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(data, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning("Failed to save token history stats: %s", exc)

    @staticmethod
    def _empty_history_stats() -> dict[str, Any]:
        return {"version": HISTORY_STATS_VERSION, "groups": {}}

    def _sanitize_history_stats(self, raw_data: Any) -> dict[str, Any]:
        if not isinstance(raw_data, dict):
            return self._empty_history_stats()
        groups_raw = raw_data.get("groups")
        if not isinstance(groups_raw, dict):
            groups_raw = {}

        groups: dict[str, dict[str, Any]] = {}
        for raw_group_id, raw_group in groups_raw.items():
            group_id = _normalize_history_group_id(raw_group_id)
            if not group_id or not isinstance(raw_group, dict):
                continue

            tracked_since = _parse_datetime(raw_group.get("tracked_since")) or _now_utc()
            last_synced_at = _parse_datetime(raw_group.get("last_synced_at"))
            raw_hours = raw_group.get("hours")
            hours: dict[str, int] = {}
            if isinstance(raw_hours, dict):
                for raw_bucket, raw_tokens in raw_hours.items():
                    bucket = _parse_datetime(raw_bucket)
                    if bucket is None:
                        continue
                    try:
                        tokens = max(0, int(raw_tokens or 0))
                    except (TypeError, ValueError):
                        continue
                    if tokens > 0:
                        hours[_iso_utc(_hour_start(bucket))] = tokens

            groups[group_id] = {
                "tracked_since": _iso_utc(tracked_since),
                "last_synced_at": _iso_utc(last_synced_at) if last_synced_at else "",
                "total_tokens": sum(hours.values()),
                "hours": hours,
            }

        return {"version": HISTORY_STATS_VERSION, "groups": groups}

    def _ensure_history_tracking_for_current_groups(
        self,
        data: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        stats = data if data is not None else self._load_history_stats()
        groups = stats.setdefault("groups", {})
        changed = False
        limited_groups = self._limited_groups()
        self._history_group_signature = tuple(limited_groups)
        now_iso = _iso_utc(_now_utc())
        for group_id in limited_groups:
            if group_id in groups:
                continue
            groups[group_id] = {
                "tracked_since": now_iso,
                "last_synced_at": "",
                "total_tokens": 0,
                "hours": {},
            }
            changed = True
        if data is None and changed:
            self._save_history_stats(stats)
        self._history_cached_stats = stats
        return stats, changed

    async def _history_background_sync_loop(self) -> None:
        while True:
            try:
                await self._maybe_sync_history_stats(force=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Token history background sync failed: %s", exc)
            await asyncio.sleep(HISTORY_BACKGROUND_SYNC_INTERVAL)

    def _start_history_background_sync(self) -> None:
        existing_task = getattr(self, "_history_background_task", None)
        if existing_task is not None and not existing_task.done():
            return
        try:
            self._history_background_task = asyncio.create_task(
                self._history_background_sync_loop(),
                name="token_limit_history_sync",
            )
        except RuntimeError as exc:
            logger.debug("Token history background sync is not started: %s", exc)

    async def _stop_history_background_sync(self) -> None:
        task = getattr(self, "_history_background_task", None)
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        finally:
            self._history_background_task = None

    async def _maybe_sync_history_stats(self, force: bool = False) -> dict[str, Any]:
        sync_lock = getattr(self, "_history_sync_lock", None)
        if sync_lock is None:
            sync_lock = asyncio.Lock()
            self._history_sync_lock = sync_lock
        async with sync_lock:
            return await self._sync_history_stats_locked(force)

    async def _sync_history_stats_locked(self, force: bool) -> dict[str, Any]:
        now = _now_utc()
        current_signature = tuple(self._limited_groups())
        last_attempt = getattr(self, "_history_last_sync_attempt", None)
        last_signature = getattr(self, "_history_group_signature", None)
        if (
            not force
            and isinstance(last_attempt, datetime)
            and now - last_attempt < HISTORY_SYNC_MIN_INTERVAL
            and current_signature == last_signature
        ):
            cached_stats = getattr(self, "_history_cached_stats", None)
            if isinstance(cached_stats, dict):
                return cached_stats

        stats, changed = self._ensure_history_tracking_for_current_groups()

        self._history_last_sync_attempt = now
        synced = False
        for group_id, group_data in list(stats.get("groups", {}).items()):
            if not isinstance(group_data, dict):
                continue
            try:
                group_synced = await self._sync_history_group(group_id, group_data, now)
                synced = synced or group_synced
            except Exception as exc:
                logger.warning(
                    "Failed to sync token history for group %s: %s",
                    group_id,
                    exc,
                    exc_info=True,
                )

        if changed or synced:
            self._save_history_stats(stats)
        self._history_cached_stats = stats
        return stats

    async def _sync_history_group(
        self,
        group_id: str,
        group_data: dict[str, Any],
        now: datetime,
    ) -> bool:
        tracked_since = _parse_datetime(group_data.get("tracked_since"))
        if tracked_since is None:
            tracked_since = now
            group_data["tracked_since"] = _iso_utc(tracked_since)

        last_synced = _parse_datetime(group_data.get("last_synced_at")) or tracked_since
        overlap_start = last_synced - HISTORY_SYNC_OVERLAP
        if overlap_start <= tracked_since:
            query_start = tracked_since
        else:
            aligned_start = _hour_start(overlap_start)
            query_start = tracked_since if aligned_start < tracked_since else aligned_start
        query_end = now
        if query_end <= query_start:
            return False

        local_tz = _local_timezone()
        window = HistoryUsageWindow(
            start_local=query_start.astimezone(local_tz),
            end_local=query_end.astimezone(local_tz),
            start_utc=query_start.astimezone(timezone.utc),
            end_utc=query_end.astimezone(timezone.utc),
        )
        _, hourly = await self._query_usage_for_group(group_id, window)

        hours = group_data.setdefault("hours", {})
        if not isinstance(hours, dict):
            hours = {}
            group_data["hours"] = hours

        remove_start = _hour_start(query_start)
        removed_tokens = 0
        for bucket_key in list(hours.keys()):
            bucket_time = _parse_datetime(bucket_key)
            if bucket_time is None:
                removed_tokens += int(hours.pop(bucket_key, 0) or 0)
                continue
            if remove_start <= bucket_time < query_end:
                removed_tokens += int(hours.pop(bucket_key, 0) or 0)

        added_tokens = 0
        for bucket_key, tokens in hourly.items():
            bucket_time = _parse_datetime(bucket_key)
            if bucket_time is None:
                continue
            normalized_bucket = _iso_utc(_hour_start(bucket_time))
            normalized_tokens = max(0, int(tokens or 0))
            if normalized_tokens > 0:
                hours[normalized_bucket] = normalized_tokens
                added_tokens += normalized_tokens

        has_stored_total = "total_tokens" in group_data
        try:
            old_total = int(group_data.get("total_tokens") or 0)
        except (TypeError, ValueError):
            has_stored_total = False
            old_total = 0
        if has_stored_total:
            group_data["total_tokens"] = max(0, old_total - removed_tokens + added_tokens)
        else:
            group_data["total_tokens"] = sum(
                max(0, int(value or 0)) for value in hours.values()
            )
        group_data["last_synced_at"] = _iso_utc(query_end)
        return True

    async def api_get_history(self) -> dict:
        try:
            group_id = _normalize_history_group_id(request.args.get("group_id"))
            range_key = str(request.args.get("range") or "24h").strip()
            if range_key not in HISTORY_RANGE_KEYS:
                range_key = "24h"
            try:
                top_limit = max(
                    1,
                    min(30, int(request.args.get("limit") or HISTORY_DEFAULT_TOP_LIMIT)),
                )
            except (TypeError, ValueError):
                top_limit = HISTORY_DEFAULT_TOP_LIMIT

            stats = await self._maybe_sync_history_stats(force=True)
            usage_payload = await self._build_usage_payload()
            groups = self._history_dropdown_groups(usage_payload)
            known_groups = stats.get("groups", {})
            selected_group_id = group_id if group_id in known_groups else ""
            range_total_tokens = 0
            range_total_display = ""
            if selected_group_id:
                bars = self._history_group_bars(
                    known_groups[selected_group_id],
                    range_key,
                    _now_utc(),
                )
                range_total_tokens = sum(int(bar.get("tokens") or 0) for bar in bars)
                range_total_display = _format_history_tokens(range_total_tokens)
            else:
                bars = self._history_top_bars(stats, top_limit)

            return _history_ok(
                {
                    "groups": groups,
                    "selected_group_id": selected_group_id,
                    "range": range_key,
                    "bars": bars,
                    "range_total_tokens": range_total_tokens,
                    "range_total_display": range_total_display,
                    "synced_at": _iso_utc(_now_utc()),
                    "top_limit": top_limit,
                }
            )
        except Exception as exc:
            logger.error("Failed to build token history response: %s", exc, exc_info=True)
            return _history_error(f"获取历史用量失败: {exc}")

    def _history_dropdown_groups(self, usage_payload: dict[str, Any]) -> list[dict[str, Any]]:
        groups = []
        for item in usage_payload.get("groups", []) or []:
            group_id = _normalize_history_group_id(item.get("group_id"))
            if not group_id:
                continue
            status = "normal"
            if item.get("stopped"):
                status = "stopped"
            elif item.get("using_fallback"):
                status = "fallback"
            groups.append(
                {
                    "group_id": group_id,
                    "group_name": str(item.get("group_name") or ""),
                    "remark": str(item.get("remark") or ""),
                    "daily_tokens": int(item.get("used_tokens") or 0),
                    "daily_display": str(item.get("used_display") or "0"),
                    "status": status,
                }
            )
        return groups

    def _history_top_bars(
        self,
        stats: dict[str, Any],
        top_limit: int,
    ) -> list[dict[str, Any]]:
        remarks = self._load_group_remarks()
        group_names = self._load_group_names()
        groups = stats.get("groups", {})
        rows = []
        if isinstance(groups, dict):
            for group_id, group_data in groups.items():
                if not isinstance(group_data, dict):
                    continue
                try:
                    tokens = int(group_data.get("total_tokens") or 0)
                except (TypeError, ValueError):
                    tokens = 0
                rows.append((str(group_id), max(0, tokens)))
        rows.sort(key=lambda item: item[1], reverse=True)
        return [
            self._history_bar(
                label=(
                    f"{group_names[group_id]}（{group_id}）"
                    if group_names.get(group_id)
                    else (
                        f"{group_id}（{remarks[group_id]}）"
                        if remarks.get(group_id)
                        else group_id
                    )
                ),
                tokens=tokens,
                group_id=group_id,
            )
            for group_id, tokens in rows[:top_limit]
        ]

    def _history_group_bars(
        self,
        group_data: dict[str, Any],
        range_key: str,
        now: datetime,
    ) -> list[dict[str, Any]]:
        hours = group_data.get("hours")
        if not isinstance(hours, dict):
            hours = {}

        if range_key == "24h":
            return self._history_recent_hour_bars(
                hours,
                now,
                24,
                display_decimals=1,
            )
        if range_key == "7d":
            return self._history_recent_day_bars(hours, now, 7)
        if range_key == "30d":
            return self._history_recent_day_bars(
                hours,
                now,
                30,
                bucket_days=3,
                display_decimals=1,
                month_name_labels=True,
            )
        return self._history_all_bars(hours)

    def _history_recent_hour_bars(
        self,
        hours: dict[str, Any],
        now: datetime,
        count: int,
        bucket_hours: int = 1,
        display_decimals: int = 2,
    ) -> list[dict[str, Any]]:
        end_hour = _hour_start(now)
        start_hour = end_hour - timedelta(hours=count - 1)
        values = self._history_hour_values(hours)
        bars = []
        step = max(1, int(bucket_hours or 1))
        for index in range(0, count, step):
            bucket_start = start_hour + timedelta(hours=index)
            bucket_end = min(
                bucket_start + timedelta(hours=step - 1),
                end_hour,
            )
            tokens = 0
            cursor = bucket_start
            while cursor <= bucket_end:
                tokens += values.get(_iso_utc(cursor), 0)
                cursor += timedelta(hours=1)
            bars.append(
                self._history_bar(
                    _hour_range_label(bucket_start, bucket_end),
                    tokens,
                    bucket=_iso_utc(bucket_start),
                    display_decimals=display_decimals,
                )
            )
        return bars

    def _history_recent_day_bars(
        self,
        hours: dict[str, Any],
        now: datetime,
        count: int,
        bucket_days: int = 1,
        display_decimals: int = 2,
        month_name_labels: bool = False,
    ) -> list[dict[str, Any]]:
        local_tz = _local_timezone()
        today = now.astimezone(local_tz).date()
        first_day = today - timedelta(days=count - 1)
        totals = {first_day + timedelta(days=index): 0 for index in range(count)}
        for bucket_time, tokens in self._history_iter_hours(hours):
            bucket_day = bucket_time.astimezone(local_tz).date()
            if bucket_day in totals:
                totals[bucket_day] += tokens

        bars = []
        step = max(1, int(bucket_days or 1))
        for index in range(0, count, step):
            bucket_start = first_day + timedelta(days=index)
            bucket_end = min(bucket_start + timedelta(days=step - 1), today)
            bucket_tokens = 0
            cursor = bucket_start
            while cursor <= bucket_end:
                bucket_tokens += totals.get(cursor, 0)
                cursor += timedelta(days=1)
            bars.append(
                self._history_bar(
                    _day_range_label(
                        bucket_start,
                        bucket_end,
                        use_month_name=month_name_labels,
                    ),
                    bucket_tokens,
                    bucket=bucket_start.isoformat(),
                    display_decimals=display_decimals,
                )
            )
        return bars

    def _history_all_bars(self, hours: dict[str, Any]) -> list[dict[str, Any]]:
        buckets = list(self._history_iter_hours(hours))
        if not buckets:
            return []

        first = min(bucket for bucket, _ in buckets)
        last = max(bucket for bucket, _ in buckets)
        if last - first <= timedelta(days=31):
            totals: dict[str, int] = {}
            for bucket_time, tokens in buckets:
                key = bucket_time.astimezone(_local_timezone()).date().isoformat()
                totals[key] = totals.get(key, 0) + tokens
            return [
                self._history_bar(label=key[5:], tokens=tokens, bucket=key)
                for key, tokens in sorted(totals.items())
            ]

        monthly: dict[str, int] = {}
        for bucket_time, tokens in buckets:
            key = _month_key(bucket_time)
            monthly[key] = monthly.get(key, 0) + tokens
        return [
            self._history_bar(label=key, tokens=tokens, bucket=key)
            for key, tokens in sorted(monthly.items())
        ]

    @staticmethod
    def _history_hour_values(hours: dict[str, Any]) -> dict[str, int]:
        values: dict[str, int] = {}
        for bucket_key, raw_tokens in hours.items():
            bucket = _parse_datetime(bucket_key)
            if bucket is None:
                continue
            try:
                tokens = max(0, int(raw_tokens or 0))
            except (TypeError, ValueError):
                continue
            values[_iso_utc(_hour_start(bucket))] = tokens
        return values

    @staticmethod
    def _history_iter_hours(hours: dict[str, Any]):
        for bucket_key, raw_tokens in hours.items():
            bucket = _parse_datetime(bucket_key)
            if bucket is None:
                continue
            try:
                tokens = max(0, int(raw_tokens or 0))
            except (TypeError, ValueError):
                continue
            yield _hour_start(bucket), tokens

    @staticmethod
    def _history_bar(
        label: str,
        tokens: int,
        bucket: str = "",
        group_id: str = "",
        display_decimals: int = 2,
    ) -> dict[str, Any]:
        normalized_tokens = max(0, int(tokens or 0))
        return {
            "label": re.sub(r"\s+", " ", str(label or "")).strip(),
            "tokens": normalized_tokens,
            "display": _format_history_tokens(normalized_tokens, display_decimals),
            "bucket": bucket,
            "group_id": group_id,
        }
