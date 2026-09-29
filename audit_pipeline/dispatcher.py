"""分发：规则路由 + 按下游去重 + 失败缓冲重试。

状态以 (event_id, sink) 为键持久化在 ``LocalStorage``：

* ``delivered`` —— 已送达；重复投递同一事件到同一下游时直接跳过，
  下游不重复计数、不重复告警；
* ``pending``   —— 下游暂时写不进去，已缓冲，等待 ``retry_pending``；
* ``failed``    —— 达到最大尝试次数仍失败（RETRY_EXHAUSTED），
  事件与失败原因都保留在库里，不会静默丢失。
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from audit_pipeline.clock import Clock, utc_now
from audit_pipeline.errors import SinkUnavailableError
from audit_pipeline.models import (
    AuditEvent,
    DeliveryRecord,
    DeliveryStatus,
    FailureCode,
    RoutingRule,
    Severity,
)
from audit_pipeline.sinks import Sink
from audit_pipeline.storage import LocalStorage

logger = logging.getLogger("audit_pipeline.dispatcher")

_SEVERITY_ORDER = {
    Severity.INFO: 0,
    Severity.WARNING: 1,
    Severity.CRITICAL: 2,
}


class Dispatcher:
    def __init__(
        self,
        storage: LocalStorage,
        sinks: dict[str, Sink],
        rules: Sequence[RoutingRule],
        clock: Clock = utc_now,
        max_attempts: int = 3,
    ) -> None:
        self._storage = storage
        self._sinks = sinks
        self._rules = list(rules)
        self._clock = clock
        self._max_attempts = max_attempts

    # -- 路由 ---------------------------------------------------------

    def routes_for(self, event: AuditEvent) -> list[str]:
        matched: list[str] = []
        for rule in self._rules:
            if event.category not in rule.categories:
                continue
            if _SEVERITY_ORDER[event.severity] < _SEVERITY_ORDER[
                rule.min_severity
            ]:
                continue
            if rule.name in self._sinks:
                matched.append(rule.name)
        return matched

    # -- 分发 ---------------------------------------------------------

    def dispatch(self, event: AuditEvent) -> list[DeliveryRecord]:
        targets = self.routes_for(event)
        logger.info(
            "分发判定 event_id=%s category=%s severity=%s 原始=%r 命中下游=%s",
            event.event_id, event.category.value, event.severity.value,
            event.raw, targets,
        )
        if not targets:
            logger.info(
                "判定依据: 规则 %s 均未命中 event_id=%s，不分发",
                [(r.name, sorted(c.value for c in r.categories)) for r in self._rules],
                event.event_id,
            )

        results: list[DeliveryRecord] = []
        for sink_name in targets:
            existing = self._storage.get_delivery(event.event_id, sink_name)
            if existing is not None and existing.status is DeliveryStatus.DELIVERED:
                logger.info(
                    "判定依据: event_id=%s -> sink=%s 已于 attempts=%d 送达，"
                    "重复投递跳过（不重复计数/告警）",
                    event.event_id, sink_name, existing.attempts,
                )
                results.append(existing)
                continue
            results.append(self._attempt(event, sink_name))
        return results

    def retry_pending(self) -> list[DeliveryRecord]:
        """重试所有 pending（以及未耗尽次数的 failed）投递。"""
        results: list[DeliveryRecord] = []
        for record in self._storage.list_deliveries():
            if record.status is DeliveryStatus.DELIVERED:
                continue
            if record.attempts >= self._max_attempts:
                continue
            event = self._storage.get_event(record.event_id)
            if event is None:
                continue
            results.append(self._attempt(event, record.sink))
        return results

    # -- 内部 ---------------------------------------------------------

    def _attempt(self, event: AuditEvent, sink_name: str) -> DeliveryRecord:
        sink = self._sinks[sink_name]
        previous = self._storage.get_delivery(event.event_id, sink_name)
        attempts = (previous.attempts if previous else 0) + 1
        try:
            sink.deliver(event)
        except SinkUnavailableError as exc:
            exhausted = attempts >= self._max_attempts
            record = DeliveryRecord(
                event_id=event.event_id,
                sink=sink_name,
                status=DeliveryStatus.FAILED if exhausted
                else DeliveryStatus.PENDING,
                attempts=attempts,
                failure_code=FailureCode.RETRY_EXHAUSTED if exhausted
                else FailureCode.SINK_UNAVAILABLE,
                last_error=str(exc),
                updated_at=self._clock(),
            )
            logger.info(
                "判定依据: event_id=%s -> sink=%s 第 %d 次写入失败(%s)，状态=%s",
                event.event_id, sink_name, attempts, exc, record.status.value,
            )
        else:
            record = DeliveryRecord(
                event_id=event.event_id,
                sink=sink_name,
                status=DeliveryStatus.DELIVERED,
                attempts=attempts,
                failure_code=None,
                last_error=None,
                updated_at=self._clock(),
            )
            logger.info(
                "判定依据: event_id=%s -> sink=%s 第 %d 次尝试送达",
                event.event_id, sink_name, attempts,
            )
        self._storage.upsert_delivery(record)
        return record
