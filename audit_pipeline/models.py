"""统一审计事件模型及跨模块共享的类型定义。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class EventCategory(str, Enum):
    """审计事件大类，决定分发标签与留存策略。"""

    LOGIN = "login"
    DATA_ACCESS = "data_access"
    PERMISSION_CHANGE = "permission_change"
    SYSTEM_OP = "system_op"
    UNKNOWN = "unknown"


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class RejectCode(str, Enum):
    """事件被拒原因，需可区分、可统计。"""

    MISSING_FIELD = "missing_field"
    INVALID_FORMAT = "invalid_format"
    UNKNOWN_SOURCE = "unknown_source"
    UNKNOWN_CATEGORY = "unknown_category"
    ILLEGAL_TIMESTAMP = "illegal_timestamp"


class DeliveryStatus(str, Enum):
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"


class FailureCode(str, Enum):
    SINK_UNAVAILABLE = "sink_unavailable"
    RETRY_EXHAUSTED = "retry_exhausted"


@dataclass(frozen=True)
class AuditEvent:
    """归一化后的统一审计事件模型。"""

    event_id: str
    source: str
    category: EventCategory
    severity: Severity
    actor: str
    action: str
    occurred_at: datetime
    received_at: datetime
    raw: dict = field(default_factory=dict)


@dataclass(frozen=True)
class RejectedEvent:
    source: str
    raw: dict | list | str | int | float | bool | None
    reason: RejectCode
    detail: str
    rejected_at: datetime


@dataclass(frozen=True)
class NormalizationResult:
    accepted: AuditEvent | None
    rejected: RejectedEvent | None

    @property
    def ok(self) -> bool:
        return self.accepted is not None


@dataclass(frozen=True)
class RoutingRule:
    """按事件类别路由到下游接收方。"""

    name: str
    categories: frozenset[EventCategory]
    min_severity: Severity = Severity.INFO


@dataclass(frozen=True)
class DeliveryRecord:
    event_id: str
    sink: str
    status: DeliveryStatus
    attempts: int
    failure_code: FailureCode | None
    last_error: str | None
    updated_at: datetime
