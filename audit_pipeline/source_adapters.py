"""不同事件来源的原始格式适配器。

当前内置两个来源：

* ``web_app`` —— Web 应用直报，snake_case 字段、ISO8601 时间、
  点分式 ``type``（如 ``user.login``）。
* ``gateway`` —— 网关上报，嵌套/异形字段名、斜杠日期时间格式、
  大写枚举（如 ``LOGIN`` / ``INFO``）。

适配器只负责把单一来源的原始 payload 解析为
``NormalizationResult``；字段缺失与格式非法必须以不同
``RejectCode`` 拒绝，不能悄悄丢弃。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from audit_pipeline.clock import Clock, utc_now
from audit_pipeline.models import (
    AuditEvent,
    EventCategory,
    NormalizationResult,
    RejectedEvent,
    RejectCode,
    Severity,
)

# web_app 的 type -> 统一类别；未命中精确映射时再按点分前缀归类
_WEB_TYPE: dict[str, EventCategory] = {
    "user.login": EventCategory.LOGIN,
    "user.logout": EventCategory.LOGIN,
}
_WEB_TYPE_PREFIX: dict[str, EventCategory] = {
    "data": EventCategory.DATA_ACCESS,
    "role": EventCategory.PERMISSION_CHANGE,
    "system": EventCategory.SYSTEM_OP,
}

# gateway 的 event_type -> 统一类别
_GATEWAY_CATEGORY: dict[str, EventCategory] = {
    "LOGIN": EventCategory.LOGIN,
    "LOGOUT": EventCategory.LOGIN,
    "DATA_QUERY": EventCategory.DATA_ACCESS,
    "DATA_EXPORT": EventCategory.DATA_ACCESS,
    "PERM_GRANT": EventCategory.PERMISSION_CHANGE,
    "PERM_REVOKE": EventCategory.PERMISSION_CHANGE,
    "SYS_RESTART": EventCategory.SYSTEM_OP,
}

_SEVERITY_MAP: dict[str, Severity] = {
    "info": Severity.INFO,
    "warning": Severity.WARNING,
    "warn": Severity.WARNING,
    "critical": Severity.CRITICAL,
    "fatal": Severity.CRITICAL,
}


def _reject(
    source: str,
    raw: Any,
    reason: RejectCode,
    detail: str,
    clock: Clock,
) -> NormalizationResult:
    return NormalizationResult(
        accepted=None,
        rejected=RejectedEvent(
            source=source,
            raw=raw,
            reason=reason,
            detail=detail,
            rejected_at=clock(),
        ),
    )


def _accept(event: AuditEvent) -> NormalizationResult:
    return NormalizationResult(accepted=event, rejected=None)


def _parse_iso8601(value: str) -> datetime:
    """解析 web_app 的 ISO8601 时间（容忍结尾 Z）。"""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_gateway_time(value: str) -> datetime:
    """解析 gateway 的 ``YYYY/MM/DD HH:MM:SS`` 本地式时间（按 UTC 处理）。"""
    return datetime.strptime(value.strip(), "%Y/%m/%d %H:%M:%S").replace(
        tzinfo=timezone.utc
    )


def _coerce_severity(value: Any) -> Severity | None:
    if not isinstance(value, str):
        return None
    return _SEVERITY_MAP.get(value.strip().lower())


class SourceAdapter:
    """来源适配器基类：负责解析单一来源格式。"""

    source_name: str = ""

    def parse(self, raw: Any, received_at: datetime) -> NormalizationResult:
        raise NotImplementedError

    # -- 共用校验工具 -------------------------------------------------

    def _bad_shape(
        self, raw: Any, clock: Clock
    ) -> NormalizationResult | None:
        if not isinstance(raw, dict):
            return _reject(
                self.source_name,
                raw,
                RejectCode.INVALID_FORMAT,
                f"payload 必须是 JSON 对象，实际为 {type(raw).__name__}",
                clock,
            )
        return None

    def _require_str(
        self,
        raw: dict,
        key: str,
        clock: Clock,
    ) -> tuple[str | None, NormalizationResult | None]:
        if key not in raw:
            return None, _reject(
                self.source_name,
                raw,
                RejectCode.MISSING_FIELD,
                f"缺少必填字段: {key}",
                clock,
            )
        value = raw[key]
        if not isinstance(value, str) or not value.strip():
            return None, _reject(
                self.source_name,
                raw,
                RejectCode.INVALID_FORMAT,
                f"字段 {key} 必须是非空字符串，实际为 {value!r}",
                clock,
            )
        return value, None

    def _build_event(
        self,
        event_id: str,
        category: EventCategory,
        severity: Severity,
        actor: str,
        action: str,
        occurred_at: datetime,
        received_at: datetime,
        raw: dict,
    ) -> NormalizationResult:
        return _accept(
            AuditEvent(
                event_id=event_id,
                source=self.source_name,
                category=category,
                severity=severity,
                actor=actor,
                action=action,
                occurred_at=occurred_at,
                received_at=received_at,
                raw=raw,
            )
        )


class WebAppAdapter(SourceAdapter):
    """Web 应用来源：``{event_id,type,severity,user,ts[,data]}``。"""

    source_name = "web_app"

    def parse(self, raw: Any, received_at: datetime) -> NormalizationResult:
        clock: Clock = lambda: received_at

        bad = self._bad_shape(raw, clock)
        if bad is not None:
            return bad

        event_id, rej = self._require_str(raw, "event_id", clock)
        if rej is not None:
            return rej
        type_name, rej = self._require_str(raw, "type", clock)
        if rej is not None:
            return rej
        actor, rej = self._require_str(raw, "user", clock)
        if rej is not None:
            return rej

        if "ts" not in raw:
            return _reject(
                self.source_name, raw, RejectCode.MISSING_FIELD,
                "缺少必填字段: ts", clock,
            )
        try:
            occurred_at = _parse_iso8601(raw["ts"])
        except (TypeError, ValueError):
            return _reject(
                self.source_name, raw, RejectCode.ILLEGAL_TIMESTAMP,
                f"ts 不是合法 ISO8601 时间: {raw['ts']!r}", clock,
            )

        severity = _coerce_severity(raw.get("severity", "info"))
        if severity is None:
            return _reject(
                self.source_name, raw, RejectCode.INVALID_FORMAT,
                f"severity 取值不合法: {raw.get('severity')!r}", clock,
            )

        prefix = type_name.split(".", 1)[0]
        category = _WEB_TYPE.get(type_name) or _WEB_TYPE_PREFIX.get(prefix)
        if category is None:
            return _reject(
                self.source_name, raw, RejectCode.UNKNOWN_CATEGORY,
                f"无法从 type={type_name!r} 识别事件类别", clock,
            )

        return self._build_event(
            event_id, category, severity, actor, type_name,
            occurred_at, received_at, raw,
        )


class GatewayAdapter(SourceAdapter):
    """网关来源：``{id,event_type,level,subject,op,@timestamp}``。"""

    source_name = "gateway"

    def parse(self, raw: Any, received_at: datetime) -> NormalizationResult:
        clock: Clock = lambda: received_at

        bad = self._bad_shape(raw, clock)
        if bad is not None:
            return bad

        event_id, rej = self._require_str(raw, "id", clock)
        if rej is not None:
            return rej
        event_type, rej = self._require_str(raw, "event_type", clock)
        if rej is not None:
            return rej
        actor, rej = self._require_str(raw, "subject", clock)
        if rej is not None:
            return rej
        action, rej = self._require_str(raw, "op", clock)
        if rej is not None:
            return rej

        ts_key = "@timestamp"
        if ts_key not in raw:
            return _reject(
                self.source_name, raw, RejectCode.MISSING_FIELD,
                f"缺少必填字段: {ts_key}", clock,
            )
        ts_value = raw[ts_key]
        if not isinstance(ts_value, str):
            return _reject(
                self.source_name, raw, RejectCode.ILLEGAL_TIMESTAMP,
                f"{ts_key} 必须是字符串，实际为 {type(ts_value).__name__}",
                clock,
            )
        try:
            occurred_at = _parse_gateway_time(ts_value)
        except ValueError:
            return _reject(
                self.source_name, raw, RejectCode.ILLEGAL_TIMESTAMP,
                f"{ts_key} 不是合法 'YYYY/MM/DD HH:MM:SS' 时间: {ts_value!r}",
                clock,
            )

        severity = _coerce_severity(raw.get("level", "info"))
        if severity is None:
            return _reject(
                self.source_name, raw, RejectCode.INVALID_FORMAT,
                f"level 取值不合法: {raw.get('level')!r}", clock,
            )

        category = _GATEWAY_CATEGORY.get(event_type.strip().upper())
        if category is None:
            return _reject(
                self.source_name, raw, RejectCode.UNKNOWN_CATEGORY,
                f"无法识别的 event_type: {event_type!r}", clock,
            )

        return self._build_event(
            event_id, category, severity, actor, action,
            occurred_at, received_at, raw,
        )
