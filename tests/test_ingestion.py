"""采集与归一化：多来源格式统一、拒绝原因可区分、日志含原始事件与依据。"""
import logging

import pytest

from audit_pipeline import (
    EventCategory,
    EventRejected,
    RejectionReason,
    normalize_event,
)

logger = logging.getLogger("audit_tests.ingestion")


def test_normalize_app_json():
    raw = {
        "id": "a-1",
        "ts": "2026-09-29T08:00:00+00:00",
        "user": "alice",
        "action": "login",
        "category": "access",
        "detail": {"ip": "10.0.0.1"},
    }
    event = normalize_event(raw, "app-json")
    logger.info("原始事件: %s -> 归一化: %s", raw, event)
    assert event.event_id == "a-1"
    assert event.category is EventCategory.ACCESS
    assert event.source == "app-json"
    assert event.detail == {"ip": "10.0.0.1"}


def test_normalize_legacy_field_mapping():
    raw = {"uid": "l-1", "time": 1790000000, "who": "bob", "op": "export", "klass": "data"}
    event = normalize_event(raw, "legacy")
    logger.info("原始事件: %s -> 归一化: %s", raw, event)
    assert event.event_id == "l-1"
    assert event.actor == "bob"
    assert event.action == "export"
    assert event.category is EventCategory.DATA
    assert event.timestamp.tzinfo is not None


def test_normalize_syslog_kv():
    raw = "id=s-1 ts=2026-09-29T09:30:00Z user=carol action=restart category=system"
    event = normalize_event(raw, "syslog")
    logger.info("原始事件: %s -> 归一化: %s", raw, event)
    assert event.event_id == "s-1"
    assert event.category is EventCategory.SYSTEM


@pytest.mark.parametrize(
    ("raw", "source", "reason"),
    [
        # 必填字段缺失（缺 category）
        (
            {"id": "x", "ts": "2026-09-29T00:00:00Z", "user": "u", "action": "a"},
            "app-json",
            RejectionReason.MISSING_FIELD,
        ),
        # 原始报文无法解析
        ("这不是键值对", "syslog", RejectionReason.INVALID_FORMAT),
        (["not", "a", "dict"], "app-json", RejectionReason.INVALID_FORMAT),
        # 字段类型不合法（id 不是字符串）
        (
            {"id": 123, "ts": "2026-09-29T00:00:00Z", "user": "u", "action": "a", "category": "access"},
            "app-json",
            RejectionReason.INVALID_FIELD_TYPE,
        ),
        # 未知类别
        (
            {"id": "x", "ts": "2026-09-29T00:00:00Z", "user": "u", "action": "a", "category": "weird"},
            "app-json",
            RejectionReason.UNKNOWN_CATEGORY,
        ),
        # 时间戳不可解析
        (
            {"id": "x", "ts": "not-a-time", "user": "u", "action": "a", "category": "access"},
            "app-json",
            RejectionReason.INVALID_TIMESTAMP,
        ),
    ],
)
def test_rejection_reasons_are_distinguishable(raw, source, reason):
    with pytest.raises(EventRejected) as exc_info:
        normalize_event(raw, source)
    logger.info(
        "原始事件: %s | 来源: %s | 拒绝原因: %s | 依据: %s",
        raw, source, exc_info.value.reason.value, exc_info.value.message,
    )
    assert exc_info.value.reason is reason


def test_rejected_events_land_in_dead_letter_store(pipeline):
    bad = {"id": "x", "user": "bob"}  # 缺 ts/action/category
    result = pipeline.ingest(bad, "app-json")
    logger.info("原始事件: %s | 拒绝原因: %s", bad, result.rejection.reason.value)
    assert not result.accepted
    dead = pipeline._dead_letters.load_all()
    assert len(dead) == 1
    assert dead[0].reason is RejectionReason.MISSING_FIELD
    assert dead[0].raw == bad  # 原始报文完整留档，未静默丢弃
