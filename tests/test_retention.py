"""留存：分类别保留时长、策略变更后按绑定版本复现、清理必留痕。"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest

from audit_pipeline.models import AuditEvent, EventCategory, Severity
from audit_pipeline.retention import RetentionPolicy, RetentionService
from audit_pipeline.storage import LocalStorage
from audit_pipeline.errors import PolicyError


NOW = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)


def _event(event_id: str, category: EventCategory, age_days: int) -> AuditEvent:
    return AuditEvent(
        event_id=event_id,
        source="web_app",
        category=category,
        severity=Severity.INFO,
        actor="alice",
        action="do",
        occurred_at=NOW - timedelta(days=age_days),
        received_at=NOW,
        raw={"event_id": event_id, "type": category.value,
             "user": "alice", "ts": (NOW - timedelta(days=age_days)).isoformat()},
    )


@pytest.fixture
def service():
    storage = LocalStorage()
    return storage, RetentionService(storage, clock=lambda: NOW)


def test_category_specific_retention_differ(service):
    storage, rs = service
    rs.apply_policy(
        RetentionPolicy(
            1,
            {"login": 30, "data_access": 90,
             "permission_change": 365, "system_op": 180},
            default_days=60,
            created_at=NOW, effective_at=NOW,
        )
    )
    old_login = _event("login-old", EventCategory.LOGIN, 40)
    old_data = _event("data-old", EventCategory.DATA_ACCESS, 40)
    storage.save_event(old_login, policy_version=1)
    storage.save_event(old_data, policy_version=1)

    # 同为 40 天：login(30) 过期，data_access(90) 保留
    assert rs.is_expired(old_login, NOW) is True
    assert rs.is_expired(old_data, NOW) is False


def test_policy_change_does_not_change_existing_events_judgement(service):
    storage, rs = service
    # v1: login 保留 30 天
    rs.apply_policy(
        RetentionPolicy(1, {"login": 30}, default_days=60,
                        created_at=NOW, effective_at=NOW)
    )
    login_40d = _event("login-40", EventCategory.LOGIN, 40)
    storage.save_event(login_40d, policy_version=1)
    assert rs.is_expired(login_40d, NOW) is True

    # v2: login 大幅延长到 365 天 —— 对已有事件不溯及既往
    rs.apply_policy(
        RetentionPolicy(2, {"login": 365}, default_days=60,
                        created_at=NOW + timedelta(days=1),
                        effective_at=NOW + timedelta(days=1))
    )
    assert rs.is_expired(login_40d, NOW) is True

    # 缩短策略同样不影响绑定旧版本的事件
    login_40d_new = _event("login-40-new", EventCategory.LOGIN, 40)
    storage.save_event(login_40d_new, policy_version=2)
    # 同一时刻、同样年龄的两条事件，仅因绑定版本不同判定不同
    assert rs.is_expired(login_40d, NOW) is True
    assert rs.is_expired(login_40d_new, NOW) is False

    # v3: data_access 从 90 缩到 30
    rs.apply_policy(
        RetentionPolicy(3, {"login": 365, "data_access": 30},
                        default_days=60, created_at=NOW + timedelta(days=2),
                        effective_at=NOW + timedelta(days=2))
    )
    data_bound_v1 = _event("data-v1", EventCategory.DATA_ACCESS, 60)
    storage.save_event(data_bound_v1, policy_version=1)  # v1 无 data 规则 -> default 60
    data_bound_v3 = _event("data-v3", EventCategory.DATA_ACCESS, 60)
    storage.save_event(data_bound_v3, policy_version=3)  # v3 data=30
    assert rs.is_expired(data_bound_v1, NOW) is False
    assert rs.is_expired(data_bound_v3, NOW) is True


def test_cleanup_soft_deletes_and_leaves_audit_trail(service):
    storage, rs = service
    rs.apply_policy(
        RetentionPolicy(1, {"login": 30, "data_access": 90},
                        default_days=60, created_at=NOW, effective_at=NOW)
    )
    expired = _event("old", EventCategory.LOGIN, 100)
    retained = _event("young", EventCategory.DATA_ACCESS, 10)
    storage.save_event(expired, policy_version=1)
    storage.save_event(retained, policy_version=1)

    result = rs.cleanup_expired(NOW)
    assert result.deleted == ["old"]
    assert result.skipped == ["young"]

    # 默认列表已看不到，但物理行仍在（软删除，可追溯）
    visible_ids = {e.event_id for e in storage.list_events()}
    assert visible_ids == {"young"}
    assert storage.get_event("old") is not None

    # 清理痕迹：每条删除都有记录，含依据与策略版本
    trail = storage.list_cleanup_audit()
    assert len(trail) == 1
    entry = trail[0]
    assert entry["event_id"] == "old"
    assert entry["policy_version"] == 1
    assert "保留 30 天" in entry["reason"]
    assert entry["deleted_at"] == NOW

    # 再清理一次：已清理事件不会重复产生痕迹
    second = rs.cleanup_expired(NOW)
    assert second.deleted == []
    assert len(storage.list_cleanup_audit()) == 1


def test_policy_version_must_be_monotonic(service):
    storage, rs = service
    rs.apply_policy(
        RetentionPolicy(1, {"login": 30}, default_days=60,
                        created_at=NOW, effective_at=NOW)
    )
    with pytest.raises(PolicyError):
        rs.apply_policy(
            RetentionPolicy(1, {"login": 10}, default_days=60,
                            created_at=NOW, effective_at=NOW)
        )


def test_expiry_logs_raw_event_and_basis(service, caplog):
    storage, rs = service
    rs.apply_policy(
        RetentionPolicy(1, {"login": 30}, default_days=60,
                        created_at=NOW, effective_at=NOW)
    )
    event = _event("logged", EventCategory.LOGIN, 40)
    storage.save_event(event, policy_version=1)
    with caplog.at_level(logging.INFO):
        rs.is_expired(event, NOW)
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "原始=" in text
    assert "绑定策略 v1" in text
    assert "保留天数=30" in text
