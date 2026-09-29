"""分发：路由、重复投递去重、下游异常时缓冲与重试。"""
import logging
from datetime import datetime, timedelta, timezone

import pytest

from audit_pipeline import AuditEvent, EventCategory

logger = logging.getLogger("audit_tests.dispatch")

NOW = datetime(2026, 9, 29, 12, 0, 0, tzinfo=timezone.utc)


def make_event(event_id: str, category: EventCategory) -> AuditEvent:
    return AuditEvent(
        event_id=event_id,
        timestamp=NOW,
        source="test",
        category=category,
        actor="tester",
        action="do",
    )


def test_routing_by_category(dispatcher, sinks):
    sec = make_event("sec-1", EventCategory.SECURITY)
    acc = make_event("acc-1", EventCategory.ACCESS)
    r1 = dispatcher.dispatch(sec)
    r2 = dispatcher.dispatch(acc)
    logger.info("事件 %s 路由结果: delivered=%s", sec.event_id, r1.delivered)
    logger.info("事件 %s 路由结果: delivered=%s", acc.event_id, r2.delivered)
    assert r1.delivered == ("security", "ops")   # security 类 -> 两个下游
    assert r2.delivered == ("security",)         # access 类 -> 只去 security
    assert sinks["security"].count() == 2
    assert sinks["ops"].count() == 1


def test_duplicate_delivery_is_deduplicated(dispatcher, sinks):
    event = make_event("dup-1", EventCategory.SECURITY)
    first = dispatcher.dispatch(event)
    second = dispatcher.dispatch(event)
    third = dispatcher.dispatch(event)
    logger.info(
        "重复投递判定: 第1次 delivered=%s dedup=%s | 第2次 dedup=%s | 第3次 dedup=%s",
        first.delivered, first.deduplicated, second.deduplicated, third.deduplicated,
    )
    assert first.delivered == ("security", "ops")
    assert second.deduplicated and third.deduplicated
    # 下游不重复计数
    assert sinks["security"].count() == 1
    assert sinks["ops"].count() == 1


def test_sink_outage_buffers_then_recovers(dispatcher, sinks):
    sinks["ops"].available = False  # 模拟下游暂时不可用
    event = make_event("out-1", EventCategory.SECURITY)
    result = dispatcher.dispatch(event)
    logger.info(
        "下游异常时判定: delivered=%s buffered=%s",
        result.delivered, result.buffered,
    )
    assert result.delivered == ("security",)
    assert result.buffered == ("ops",)
    assert dispatcher.buffered_count() == 1  # 事件没丢，进了缓冲

    # 宕机期间同一事件重复投递：不重复占位、不重复写
    again = dispatcher.dispatch(event)
    assert dispatcher.buffered_count() == 1
    assert sinks["security"].count() == 1

    # 下游恢复后重试，缓冲清空且只补投一次
    sinks["ops"].available = True
    dispatcher.flush_buffer()
    logger.info("恢复后 flush: ops 事件数=%d", sinks["ops"].count())
    assert dispatcher.buffered_count() == 0
    assert sinks["ops"].count() == 1

    # 再 flush / 再投递都不会产生重复
    dispatcher.flush_buffer()
    dispatcher.dispatch(event)
    assert sinks["ops"].count() == 1
    assert sinks["security"].count() == 1


def test_buffer_survives_dispatcher_restart(sinks, tmp_path, dispatcher):
    """缓冲落盘：新 Dispatcher 实例（模拟重启）仍能重试。"""
    from audit_pipeline import Dispatcher, RoutingRule

    sinks["ops"].available = False
    event = make_event("restart-1", EventCategory.SYSTEM)
    dispatcher.dispatch(event)
    assert dispatcher.buffered_count() == 1

    sinks["ops"].available = True
    new_dispatcher = Dispatcher(
        sinks,
        rules=[RoutingRule(EventCategory.SYSTEM, ("ops",))],
        buffer_path=tmp_path / "buffer.jsonl",
    )
    new_dispatcher.flush_buffer()
    logger.info("重启后 flush: ops 事件数=%d", sinks["ops"].count())
    assert sinks["ops"].count() == 1
    assert new_dispatcher.buffered_count() == 0


def test_end_to_end_ingest_with_outage(pipeline, sinks):
    raw = {"id": "e2e-1", "ts": "2026-09-29T10:00:00Z", "user": "alice",
           "action": "login", "category": "access"}
    sinks["security"].available = False
    result = pipeline.ingest(raw, "app-json")
    logger.info(
        "端到端判定: accepted=%s buffered=%s",
        result.accepted, result.dispatch.buffered,
    )
    assert result.accepted
    assert result.dispatch.buffered == ("security",)

    sinks["security"].available = True
    pipeline.flush_buffer()
    assert sinks["security"].count() == 1
