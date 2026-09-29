"""多来源原始事件的归一化与校验。

支持的来源格式：
- ``app-json``   ：结构化 JSON 对象，字段为 id/ts/user/action/category/detail
- ``legacy``     ：旧系统 JSON 对象，字段名为 uid/time/op/who/klass
- ``syslog``     ：文本行，``id=.. ts=.. user=.. action=.. category=..`` 键值对

校验失败时抛出 :class:`EventRejected`，并携带可区分的
:class:`RejectionReason`，绝不静默丢弃。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .models import AuditEvent, EventCategory, RejectionReason


class EventRejected(Exception):
    """事件被拒绝时抛出，携带可区分的原因。"""

    def __init__(self, reason: RejectionReason, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


# 各来源字段名 -> 统一模型字段名
_FIELD_MAPS: dict[str, dict[str, str]] = {
    "app-json": {
        "id": "event_id",
        "ts": "timestamp",
        "user": "actor",
        "action": "action",
        "category": "category",
        "detail": "detail",
    },
    "legacy": {
        "uid": "event_id",
        "time": "timestamp",
        "who": "actor",
        "op": "action",
        "klass": "category",
        "extra": "detail",
    },
    "syslog": {
        "id": "event_id",
        "ts": "timestamp",
        "user": "actor",
        "action": "action",
        "category": "category",
    },
}

_REQUIRED = ("event_id", "timestamp", "actor", "action", "category")


def _parse_syslog(raw: str) -> dict[str, Any]:
    """把 ``k=v`` 键值对文本解析成 dict；无法解析抛 INVALID_FORMAT。"""
    fields: dict[str, Any] = {}
    for token in raw.split():
        if "=" not in token:
            raise EventRejected(
                RejectionReason.INVALID_FORMAT,
                f"syslog 片段无法解析为 k=v: {token!r}",
            )
        k, v = token.split("=", 1)
        fields[k] = v
    if not fields:
        raise EventRejected(RejectionReason.INVALID_FORMAT, "空的 syslog 事件")
    return fields


def _parse_timestamp(value: Any) -> datetime:
    """接受 ISO 8601 字符串或 epoch 秒；失败抛 INVALID_TIMESTAMP。"""
    if isinstance(value, bool):
        raise EventRejected(RejectionReason.INVALID_TIMESTAMP, f"时间戳类型非法: {value!r}")
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except (OverflowError, OSError, ValueError) as exc:
            raise EventRejected(
                RejectionReason.INVALID_TIMESTAMP, f"epoch 时间戳不可解析: {value!r}"
            ) from exc
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise EventRejected(RejectionReason.INVALID_TIMESTAMP, "时间戳为空字符串")
        try:
            ts = datetime.fromisoformat(text)
        except ValueError as exc:
            raise EventRejected(
                RejectionReason.INVALID_TIMESTAMP, f"时间戳格式不可解析: {value!r}"
            ) from exc
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    raise EventRejected(
        RejectionReason.INVALID_TIMESTAMP, f"时间戳类型不支持: {type(value).__name__}"
    )


def _parse_category(value: Any) -> EventCategory:
    if not isinstance(value, str):
        raise EventRejected(
            RejectionReason.INVALID_FIELD_TYPE,
            f"category 应为字符串，实际为 {type(value).__name__}",
        )
    try:
        return EventCategory(value.strip().lower())
    except ValueError as exc:
        raise EventRejected(
            RejectionReason.UNKNOWN_CATEGORY, f"未知事件类别: {value!r}"
        ) from exc


def normalize_event(raw: Any, source: str) -> AuditEvent:
    """把某个来源的原始事件归一化为 AuditEvent；不合法时抛 EventRejected。"""
    if source not in _FIELD_MAPS:
        raise EventRejected(
            RejectionReason.INVALID_FORMAT, f"未知来源: {source!r}"
        )

    # 1) 解析原始报文为字段 dict
    if source == "syslog":
        if not isinstance(raw, str):
            raise EventRejected(
                RejectionReason.INVALID_FORMAT,
                f"syslog 事件应为字符串，实际为 {type(raw).__name__}",
            )
        fields = _parse_syslog(raw)
    elif isinstance(raw, dict):
        fields = dict(raw)
    else:
        raise EventRejected(
            RejectionReason.INVALID_FORMAT,
            f"{source} 事件应为 JSON 对象，实际为 {type(raw).__name__}",
        )

    # 2) 字段名映射到统一模型
    mapped: dict[str, Any] = {}
    for src_key, dst_key in _FIELD_MAPS[source].items():
        if src_key in fields:
            mapped[dst_key] = fields[src_key]

    # 3) 必填字段检查
    for name in _REQUIRED:
        if name not in mapped or mapped[name] is None:
            raise EventRejected(
                RejectionReason.MISSING_FIELD, f"必填字段缺失: {name}"
            )

    # 4) 字段类型检查
    for name in ("event_id", "actor", "action"):
        if not isinstance(mapped[name], str) or not mapped[name].strip():
            raise EventRejected(
                RejectionReason.INVALID_FIELD_TYPE,
                f"字段 {name} 应为非空字符串，实际为 {mapped[name]!r}",
            )
    detail = mapped.get("detail") or {}
    if not isinstance(detail, dict):
        raise EventRejected(
            RejectionReason.INVALID_FIELD_TYPE,
            f"字段 detail 应为对象，实际为 {type(detail).__name__}",
        )

    # 5) 时间戳与类别
    timestamp = _parse_timestamp(mapped["timestamp"])
    category = _parse_category(mapped["category"])

    return AuditEvent(
        event_id=mapped["event_id"].strip(),
        timestamp=timestamp,
        source=source,
        category=category,
        actor=mapped["actor"].strip(),
        action=mapped["action"].strip(),
        detail=detail,
    )
