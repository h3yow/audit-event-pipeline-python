"""留存与清理：按类别保留、策略变更可复现、清理动作全部留痕。"""
import logging
from datetime import datetime, timedelta, timezone

from audit_pipeline import (
    AuditEvent,
    EventCategory,
    EventStore,
    PurgeLog,
    RetentionPolicy,
    evaluate_event,
    run_retention,
)

logger = logging.getLogger("audit_tests.retention")

NOW = datetime(2026, 9, 29, 12, 0, 0, tzinfo=timezone.utc)

POLICY_V1 = RetentionPolicy(
    version=1,
    retention_days={
        EventCategory.SECURITY: 365,
        EventCategory.ACCESS: 90,
        EventCategory.SYSTEM: 30,
        EventCategory.DATA: 180,
    },
)
# 策略调整：system 从 30 天缩短为 7 天，access 从 90 天缩短为 30 天
POLICY_V2 = RetentionPolicy(
    version=2,
    retention_days={
        EventCategory.SECURITY: 365,
        EventCategory.ACCESS: 30,
        EventCategory.SYSTEM: 7,
        EventCategory.DATA: 180,
    },
)


def make_event(event_id: str, category: EventCategory, age_days: float) -> AuditEvent:
    return AuditEvent(
        event_id=event_id,
        timestamp=NOW - timedelta(days=age_days),
        source="test",
        category=category,
        actor="tester",
        action="do",
    )


def test_per_category_retention():
    cases = [
        ("sec", EventCategory.SECURITY, 300, True),   # 365 天内
        ("acc", EventCategory.ACCESS, 100, False),    # 超 90 天
        ("sys", EventCategory.SYSTEM, 10, True),      # 30 天内
        ("dat", EventCategory.DATA, 200, False),      # 超 180 天
    ]
    for event_id, category, age, expect_keep in cases:
        event = make_event(event_id, category, age)
        decision = evaluate_event(event, POLICY_V1, NOW)
        logger.info(
            "事件 %s (类别=%s, 已存=%d天) | 判定: %s | 依据: %s",
            event_id, category.value, age,
            "保留" if decision.keep else "清理", decision.reason,
        )
        assert decision.keep is expect_keep
        assert decision.policy_version == 1


def test_uncovered_category_defaults_to_keep():
    policy = RetentionPolicy(version=9, retention_days={EventCategory.SECURITY: 365})
    event = make_event("uncovered", EventCategory.SYSTEM, 9999)
    decision = evaluate_event(event, policy, NOW)
    logger.info("未覆盖类别判定: keep=%s | 依据: %s", decision.keep, decision.reason)
    assert decision.keep  # 保守默认：策略未覆盖的类别不删


def test_policy_change_is_reproducible():
    """同一事件在新旧策略下各自判定一致，历史判定可复现。"""
    event = make_event("repro", EventCategory.SYSTEM, 20)
    d_v1 = evaluate_event(event, POLICY_V1, NOW)
    d_v2 = evaluate_event(event, POLICY_V2, NOW)
    logger.info("策略 v1 判定: keep=%s | 依据: %s", d_v1.keep, d_v1.reason)
    logger.info("策略 v2 判定: keep=%s | 依据: %s", d_v2.keep, d_v2.reason)
    assert d_v1.keep is True    # v1: 30 天内保留
    assert d_v2.keep is False   # v2: 超 7 天清理
    # 复现：同样的输入再算一遍，结果完全一致
    assert evaluate_event(event, POLICY_V1, NOW) == d_v1
    assert evaluate_event(event, POLICY_V2, NOW) == d_v2


def test_run_retention_purges_and_logs_everything(tmp_path):
    store = EventStore(tmp_path / "events.jsonl")
    purge_log = PurgeLog(tmp_path / "purge.jsonl")
    events = [
        make_event("keep-1", EventCategory.SECURITY, 100),
        make_event("purge-1", EventCategory.SYSTEM, 60),
        make_event("purge-2", EventCategory.ACCESS, 120),
        make_event("keep-2", EventCategory.DATA, 30),
    ]
    for e in events:
        store.append(e)

    decisions = run_retention(store, POLICY_V1, NOW, purge_log)
    for d in decisions:
        logger.info(
            "留存判定: event=%s action=%s 依据=%s",
            d.event_id, "保留" if d.keep else "清理", d.reason,
        )

    # 状态一致：库里只剩应保留的
    assert [e.event_id for e in store.load_all()] == ["keep-1", "keep-2"]

    # 清理留痕：每条事件一条判定记录 + 一条运行汇总
    entries = purge_log.load_all()
    assert len(entries) == len(events) + 1
    purged = [e for e in entries if e["action"] == "purged"]
    assert {e["event_id"] for e in purged} == {"purge-1", "purge-2"}
    assert all(e["policy_version"] == 1 and e["reason"] for e in purged)
    summary = entries[-1]
    assert summary["action"] == "run_summary"
    assert summary["kept"] == 2 and summary["purged"] == 2


def test_retention_after_policy_change_applies_to_existing_events(tmp_path):
    """策略调整后对库里已有事件重新判定，且两次运行都有日志。"""
    store = EventStore(tmp_path / "events.jsonl")
    purge_log = PurgeLog(tmp_path / "purge.jsonl")
    store.append(make_event("sys-20d", EventCategory.SYSTEM, 20))

    run_retention(store, POLICY_V1, NOW, purge_log)
    assert [e.event_id for e in store.load_all()] == ["sys-20d"]  # v1 下保留

    run_retention(store, POLICY_V2, NOW, purge_log)
    assert store.load_all() == []  # v2 下到期清理

    entries = purge_log.load_all()
    v1 = [e for e in entries if e.get("policy_version") == 1 and e["action"] == "kept"]
    v2 = [e for e in entries if e.get("policy_version") == 2 and e["action"] == "purged"]
    logger.info("v1 留痕: %s", v1)
    logger.info("v2 留痕: %s", v2)
    assert len(v1) == 1 and len(v2) == 1
    assert v1[0]["event_id"] == v2[0]["event_id"] == "sys-20d"
