"""留存策略与清理：按类别保留、策略版本化、清理留痕。

- 不同类别的事件保留时长不同，由 :class:`RetentionPolicy` 按类别配置。
- 策略带版本号；:func:`evaluate_event` 是 (事件, 策略, 当前时间) 的纯函数，
  策略调整后仍可用旧版本策略复现当时对已有事件的判定。
- :func:`run_retention` 的每一条判定（保留或清理）都写入 :class:`PurgeLog`，
  绝不无记录删除。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .models import AuditEvent, EventCategory
from .storage import EventStore, PurgeLog


@dataclass(frozen=True)
class RetentionPolicy:
    """按类别定义保留天数；version 用于复现历史判定。"""

    version: int
    retention_days: dict[EventCategory, int]

    def days_for(self, category: EventCategory) -> int | None:
        return self.retention_days.get(category)


@dataclass(frozen=True)
class PurgeDecision:
    """对单条事件的留存判定，可复现、可解释。"""

    event_id: str
    keep: bool
    reason: str
    policy_version: int


def evaluate_event(event: AuditEvent, policy: RetentionPolicy, now: datetime) -> PurgeDecision:
    """按策略判定一条事件应保留还是清理（纯函数，可复现）。"""
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    days = policy.days_for(event.category)
    if days is None:
        return PurgeDecision(
            event_id=event.event_id,
            keep=True,
            reason=f"策略 v{policy.version} 未覆盖类别 {event.category.value}，默认保留",
            policy_version=policy.version,
        )
    age_days = (now - event.timestamp).total_seconds() / 86400
    if age_days <= days:
        return PurgeDecision(
            event_id=event.event_id,
            keep=True,
            reason=(
                f"类别 {event.category.value} 保留 {days} 天，"
                f"事件已存 {age_days:.2f} 天，未到期"
            ),
            policy_version=policy.version,
        )
    return PurgeDecision(
        event_id=event.event_id,
        keep=False,
        reason=(
            f"类别 {event.category.value} 保留 {days} 天，"
            f"事件已存 {age_days:.2f} 天，已到期"
        ),
        policy_version=policy.version,
    )


def run_retention(
    store: EventStore,
    policy: RetentionPolicy,
    now: datetime,
    purge_log: PurgeLog,
) -> list[PurgeDecision]:
    """对事件库执行留存清理，所有判定与删除动作写入清理日志。"""
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    events = store.load_all()
    decisions = [evaluate_event(e, policy, now) for e in events]
    kept = [e for e, d in zip(events, decisions) if d.keep]

    for event, decision in zip(events, decisions):
        purge_log.record({
            "run_at": now.isoformat(),
            "policy_version": policy.version,
            "event_id": event.event_id,
            "category": event.category.value,
            "action": "kept" if decision.keep else "purged",
            "reason": decision.reason,
        })
    purged = sum(1 for d in decisions if not d.keep)
    purge_log.record({
        "run_at": now.isoformat(),
        "policy_version": policy.version,
        "action": "run_summary",
        "total": len(events),
        "kept": len(kept),
        "purged": purged,
    })
    if purged:
        store.rewrite(kept)
    return decisions
