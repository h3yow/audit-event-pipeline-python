"""分发：重复投递幂等、下游暂时不可用缓冲重试、耗尽留状态。"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest

from audit_pipeline.dispatcher import Dispatcher
from audit_pipeline.models import (
    AuditEvent,
    DeliveryStatus,
    EventCategory,
    RoutingRule,
    Severity,
)
from audit_pipeline.sinks import LocalFileSink
from audit_pipeline.storage import LocalStorage


NOW = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)


def _event(
    event_id: str = "e-1",
    category: EventCategory = EventCategory.SYSTEM_OP,
    severity: Severity = Severity.CRITICAL,
) -> AuditEvent:
    return AuditEvent(
        event_id=event_id,
        source="gateway",
        category=category,
        severity=severity,
        actor="root",
        action="restart",
        occurred_at=NOW - timedelta(minutes=1),
        received_at=NOW,
        raw={"id": event_id, "event_type": "SYS_RESTART",
             "subject": "root", "op": "restart",
             "@timestamp": "2026/09/29 11:59:00", "level": "CRITICAL"},
    )


@pytest.fixture
def harness(tmp_path):
    storage = LocalStorage()
    siem = LocalFileSink("siem", tmp_path / "siem.jsonl")
    alerter = LocalFileSink("alerter", tmp_path / "alerter.jsonl")
    rules = [
        RoutingRule(
            "siem",
            frozenset(EventCategory),
        ),
        RoutingRule(
            "alerter",
            frozenset({EventCategory.SYSTEM_OP,
                       EventCategory.PERMISSION_CHANGE}),
            Severity.CRITICAL,
        ),
    ]
    dispatcher = Dispatcher(
        storage,
        {"siem": siem, "alerter": alerter},
        rules,
        clock=lambda: NOW,
        max_attempts=3,
    )
    return storage, siem, alerter, dispatcher


def test_routing_respects_category_and_severity(harness):
    storage, siem, alerter, dispatcher = harness
    login = _event("l-1", EventCategory.LOGIN, Severity.INFO)
    storage.save_event(login)

    records = dispatcher.dispatch(login)
    by_sink = {r.sink: r.status for r in records}
    assert by_sink == {"siem": DeliveryStatus.DELIVERED}
    assert alerter.delivered_count == 0


def test_duplicate_delivery_not_double_counted_or_alerted(harness):
    storage, siem, alerter, dispatcher = harness
    event = _event()
    storage.save_event(event)

    first = dispatcher.dispatch(event)
    assert {r.sink: r.status for r in first} == {
        "siem": DeliveryStatus.DELIVERED,
        "alerter": DeliveryStatus.DELIVERED,
    }
    second = dispatcher.dispatch(event)
    third = dispatcher.dispatch(event)

    assert all(r.status is DeliveryStatus.DELIVERED for r in second + third)
    assert siem.delivered_count == 1
    assert alerter.delivered_count == 1
    assert alerter.alert_count == 1  # CRITICAL 不重复告警
    assert len(siem.read_lines()) == 1
    assert len(alerter.read_lines()) == 1
    # 投递记录仍是同一条，attempts 不增加
    rec = storage.get_delivery("e-1", "alerter")
    assert rec.attempts == 1


def test_sink_outage_buffered_then_retried_and_succeeds(harness):
    storage, siem, alerter, dispatcher = harness
    event = _event()
    storage.save_event(event)

    # siem 暂时不可用：事件必须缓冲为 pending，不能丢
    siem.set_available(False)
    records = dispatcher.dispatch(event)
    siem_rec = next(r for r in records if r.sink == "siem")
    assert siem_rec.status is DeliveryStatus.PENDING
    assert siem_rec.failure_code.value == "sink_unavailable"
    assert siem.delivered_count == 0

    # alerter 正常，只收一次
    assert alerter.delivered_count == 1

    # 下游恢复前重试：仍失败，attempts 累积但事件还在
    retried = dispatcher.retry_pending()
    siem_retry = next(r for r in retried if r.sink == "siem")
    assert siem_retry.status is DeliveryStatus.PENDING
    assert siem_retry.attempts == 2
    assert storage.get_event("e-1") is not None

    # 下游恢复：缓冲事件送达，只计一次
    siem.set_available(True)
    recovered = dispatcher.retry_pending()
    siem_ok = next(r for r in recovered if r.sink == "siem")
    assert siem_ok.status is DeliveryStatus.DELIVERED
    assert siem.delivered_count == 1
    assert len(siem.read_lines()) == 1

    # 恢复后再重复投递，不重复计数
    dispatcher.dispatch(event)
    assert siem.delivered_count == 1


def test_retry_exhausted_marked_failed_but_event_retained(harness):
    storage, siem, alerter, dispatcher = harness
    event = _event()
    storage.save_event(event)
    siem.set_available(False)

    dispatcher.dispatch(event)       # attempts=1
    dispatcher.retry_pending()       # attempts=2
    dispatcher.retry_pending()       # attempts=3 -> 耗尽
    exhausted = dispatcher.retry_pending()

    rec = storage.get_delivery("e-1", "siem")
    assert rec.status is DeliveryStatus.FAILED
    assert rec.attempts == 3
    assert rec.failure_code.value == "retry_exhausted"
    assert rec.last_error
    # 事件没有被丢
    assert storage.get_event("e-1") is not None
    # 耗尽后 retry_pending 不再刷尝试次数
    assert exhausted == []


def test_logs_show_raw_event_and_dispatch_basis(harness, caplog):
    storage, siem, alerter, dispatcher = harness
    event = _event()
    storage.save_event(event)
    with caplog.at_level(logging.INFO):
        dispatcher.dispatch(event)
        dispatcher.dispatch(event)  # 重复
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "原始=" in text
    assert "命中下游" in text
    assert "重复投递跳过" in text
