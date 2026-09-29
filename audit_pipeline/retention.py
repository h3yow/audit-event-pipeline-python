"""留存与清理：策略版本化，判定可复现，删除必留痕。

设计要点：

* ``RetentionPolicy`` 带单调递增的版本号，规则为
  ``{category: 保留天数}``，另含 ``default_days`` 兜底；
* 事件**入库时**记录当前生效策略版本（``events.policy_version``）。
  之后策略无论怎么改，该事件的过期判定始终用绑定版本的规则，
  保证"策略改过之后对已有事件的判定可复现"；
* 绑定版本缺失（如早于策略机制的存量事件）回退到最新策略，
  并在判定依据中显式说明；
* 清理前先把事件关键信息写入 ``cleanup_audit``，再软删除
  （``retained=0``）。事件不会物理消失，任何清理动作都有可查痕迹。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from audit_pipeline.clock import Clock, utc_now
from audit_pipeline.errors import PolicyError
from audit_pipeline.models import AuditEvent
from audit_pipeline.storage import LocalStorage

logger = logging.getLogger("audit_pipeline.retention")


@dataclass(frozen=True)
class RetentionPolicy:
    version: int
    retention_days: dict[str, int]
    default_days: int
    created_at: datetime = field(default_factory=utc_now)
    effective_at: datetime = field(default_factory=utc_now)

    def validate(self) -> None:
        if self.version < 1:
            raise PolicyError("策略版本号必须 >= 1")
        if self.default_days < 0:
            raise PolicyError("default_days 不能为负")
        for category, days in self.retention_days.items():
            if days < 0:
                raise PolicyError(f"类别 {category} 的保留天数不能为负: {days}")


@dataclass(frozen=True)
class CleanupResult:
    deleted: list[str]
    skipped: list[str]
    details: list[str]


class RetentionService:
    def __init__(self, storage: LocalStorage, clock: Clock = utc_now) -> None:
        self._storage = storage
        self._clock = clock

    def apply_policy(self, policy: RetentionPolicy) -> RetentionPolicy:
        """登记一个新版本策略。版本必须单调递增，不可覆盖历史版本。"""
        policy.validate()
        current = self._storage.latest_policy_version()
        if current is not None and policy.version <= current:
            raise PolicyError(
                f"策略版本必须递增：当前最新 v{current}，收到 v{policy.version}"
            )
        self._storage.save_policy(
            version=policy.version,
            rules={**policy.retention_days, "__default__": policy.default_days},
            created_at=policy.created_at,
            effective_at=policy.effective_at,
        )
        logger.info(
            "留存策略生效 v%d 规则=%s default_days=%d",
            policy.version, policy.retention_days, policy.default_days,
        )
        return policy

    def current_version(self) -> int | None:
        return self._storage.latest_policy_version()

    def _resolve_policy(
        self, event: AuditEvent
    ) -> tuple[int, dict[str, int], str]:
        """返回 (用于判定的版本号, 规则, 判定依据说明)。"""
        bound_version = self._storage.event_policy_version(event.event_id)
        if bound_version is not None:
            policy = self._storage.load_policy(bound_version)
            if policy is not None:
                return bound_version, policy["rules"], (
                    f"event_id={event.event_id} 入库时绑定策略 v{bound_version}，"
                    f"即使后续策略变更也按该版本判定"
                )
        latest = self._storage.latest_policy_version()
        if latest is None:
            raise PolicyError("尚未登记任何留存策略，无法判定过期")
        policy = self._storage.load_policy(latest)
        return latest, policy["rules"], (
            f"event_id={event.event_id} 未绑定历史策略版本，"
            f"回退按最新策略 v{latest} 判定"
        )

    def is_expired(
        self, event: AuditEvent, at: datetime | None = None
    ) -> bool:
        at = at or self._clock()
        version, rules, basis = self._resolve_policy(event)
        days = rules.get(event.category.value, rules["__default__"])
        cutoff = at - timedelta(days=days)
        expired = event.occurred_at < cutoff
        logger.info(
            "留存判定 event_id=%s category=%s occurred_at=%s 原始=%r "
            "依据=%s；保留天数=%d 截止线=%s -> %s",
            event.event_id, event.category.value,
            event.occurred_at.isoformat(), event.raw, basis, days,
            cutoff.isoformat(), "过期" if expired else "保留",
        )
        return expired

    def cleanup_expired(
        self, at: datetime | None = None
    ) -> CleanupResult:
        """清理全部已过期事件；先写 cleanup_audit 再软删除。"""
        at = at or self._clock()
        deleted: list[str] = []
        skipped: list[str] = []
        details: list[str] = []
        for event in self._storage.list_events(retained_only=True):
            version, rules, basis = self._resolve_policy(event)
            days = rules.get(event.category.value, rules["__default__"])
            if self.is_expired(event, at):
                self._storage.record_cleanup(
                    policy_version=version,
                    event_id=event.event_id,
                    source=event.source,
                    category=event.category.value,
                    occurred_at=event.occurred_at,
                    reason=f"按策略 v{version} 保留 {days} 天，"
                           f"occurred_at={event.occurred_at.isoformat()} 已过期",
                    deleted_at=at,
                )
                self._storage.soft_delete_event(event.event_id)
                deleted.append(event.event_id)
                detail = f"{event.event_id}: 按绑定 v{version} 保留{days}天 -> 已清理并留痕"
                logger.info("清理动作 %s", detail)
            else:
                skipped.append(event.event_id)
                detail = f"{event.event_id}: 按绑定 v{version} 保留{days}天 -> 仍在留存期"
            details.append(detail)
        return CleanupResult(deleted=deleted, skipped=skipped, details=details)
