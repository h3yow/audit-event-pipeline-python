"""统一审计事件模型与拒绝原因定义。"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class EventCategory(str, Enum):
    SECURITY = "security"
    ACCESS = "access"
    SYSTEM = "system"
    DATA = "data"


class RejectionReason(str, Enum):
    MISSING_FIELD = "missing_field"        # 必填字段缺失
    INVALID_FORMAT = "invalid_format"      # 原始报文无法解析
    INVALID_FIELD_TYPE = "invalid_field_type"  # 字段类型/取值不合法
    UNKNOWN_CATEGORY = "unknown_category"  # 类别无法映射
    INVALID_TIMESTAMP = "invalid_timestamp"  # 时间戳缺失或不可解析


@dataclass(frozen=True)
class AuditEvent:
    """归一化后的统一审计事件。"""

    event_id: str
    timestamp: datetime
    source: str
    category: EventCategory
    actor: str
    action: str
    detail: dict[str, Any] = field(default_factory=dict)
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "category": self.category.value,
            "actor": self.actor,
            "action": self.action,
            "detail": self.detail,
            "schema_version": self.schema_version,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AuditEvent":
        ts = datetime.fromisoformat(data["timestamp"])
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return cls(
            event_id=data["event_id"],
            timestamp=ts,
            source=data["source"],
            category=EventCategory(data["category"]),
            actor=data["actor"],
            action=data["action"],
            detail=dict(data.get("detail") or {}),
            schema_version=int(data.get("schema_version", 1)),
        )

    @classmethod
    def from_json(cls, line: str) -> "AuditEvent":
        return cls.from_dict(json.loads(line))


@dataclass(frozen=True)
class RejectedEvent:
    """被拒绝的原始事件及其可区分的拒绝原因。"""

    raw: Any
    source: str
    reason: RejectionReason
    message: str
    rejected_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw": self.raw,
            "source": self.source,
            "reason": self.reason.value,
            "message": self.message,
            "rejected_at": self.rejected_at.isoformat(),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, default=str)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RejectedEvent":
        return cls(
            raw=data["raw"],
            source=data["source"],
            reason=RejectionReason(data["reason"]),
            message=data["message"],
            rejected_at=datetime.fromisoformat(data["rejected_at"]),
        )

    @classmethod
    def from_json(cls, line: str) -> "RejectedEvent":
        return cls.from_dict(json.loads(line))
