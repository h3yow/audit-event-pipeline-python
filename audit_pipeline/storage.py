"""本地持久化（sqlite 标准库实现）。

五张表对应五类状态，保证重复投递、重试与留存判定可复现：

* ``events`` —— 已接受的审计事件（按 event_id 幂等去重）；
* ``rejected_events`` —— 被拒事件及其区分化原因，绝不静默丢弃；
* ``delivery_state`` —— (event_id, sink) 维度的投递状态与尝试次数；
* ``retention_policies`` —— 留存策略版本（判定按事件记录时的版本可复现）；
* ``cleanup_audit`` —— 每次清理动作的可查痕迹（删除前留快照）。
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from audit_pipeline.models import (
    AuditEvent,
    DeliveryRecord,
    DeliveryStatus,
    EventCategory,
    FailureCode,
    RejectedEvent,
    Severity,
)


def _dt(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _dt_parse(text: str) -> datetime:
    return datetime.fromisoformat(text)


class LocalStorage:
    def __init__(self, path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._init_schema()

    # -- 建表 ---------------------------------------------------------

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
                event_id     TEXT PRIMARY KEY,
                source       TEXT NOT NULL,
                category     TEXT NOT NULL,
                severity     TEXT NOT NULL,
                actor        TEXT NOT NULL,
                action       TEXT NOT NULL,
                occurred_at  TEXT NOT NULL,
                received_at  TEXT NOT NULL,
                raw          TEXT NOT NULL,
                policy_version INTEGER,
                retained     INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS rejected_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                source      TEXT NOT NULL,
                raw         TEXT NOT NULL,
                reason      TEXT NOT NULL,
                detail      TEXT NOT NULL,
                rejected_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS delivery_state (
                event_id     TEXT NOT NULL,
                sink         TEXT NOT NULL,
                status       TEXT NOT NULL,
                attempts     INTEGER NOT NULL,
                failure_code TEXT,
                last_error   TEXT,
                updated_at   TEXT NOT NULL,
                PRIMARY KEY (event_id, sink)
            );

            CREATE TABLE IF NOT EXISTS retention_policies (
                version      INTEGER PRIMARY KEY,
                rules_json   TEXT NOT NULL,
                created_at   TEXT NOT NULL,
                effective_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cleanup_audit (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                policy_version INTEGER NOT NULL,
                event_id       TEXT NOT NULL,
                source         TEXT NOT NULL,
                category       TEXT NOT NULL,
                occurred_at    TEXT NOT NULL,
                reason         TEXT NOT NULL,
                deleted_at     TEXT NOT NULL
            );
            """
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.commit()
        self._conn.close()

    # -- 事件 ---------------------------------------------------------

    def save_event(self, event: AuditEvent, policy_version: int | None = None) -> bool:
        """幂等保存。返回 True 表示首次写入，False 表示重复 event_id。"""
        cur = self._conn.execute(
            """
            INSERT OR IGNORE INTO events
                (event_id, source, category, severity, actor, action,
                 occurred_at, received_at, raw, policy_version, retained)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                event.event_id, event.source, event.category.value,
                event.severity.value, event.actor, event.action,
                _dt(event.occurred_at), _dt(event.received_at),
                json.dumps(event.raw, ensure_ascii=False, sort_keys=True),
                policy_version,
            ),
        )
        self._conn.commit()
        return cur.rowcount == 1

    def get_event(self, event_id: str) -> AuditEvent | None:
        row = self._conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        return self._row_to_event(row) if row else None

    def list_events(self, retained_only: bool = True) -> list[AuditEvent]:
        sql = "SELECT * FROM events"
        if retained_only:
            sql += " WHERE retained = 1"
        sql += " ORDER BY occurred_at"
        return [self._row_to_event(r) for r in self._conn.execute(sql)]

    def event_policy_version(self, event_id: str) -> int | None:
        row = self._conn.execute(
            "SELECT policy_version FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        return row["policy_version"] if row else None

    def delete_event(self, event_id: str) -> None:
        self._conn.execute("DELETE FROM events WHERE event_id = ?", (event_id,))
        self._conn.commit()

    def soft_delete_event(self, event_id: str) -> None:
        """留存清理使用：置 retained=0，事件行保留以便追溯。"""
        self._conn.execute(
            "UPDATE events SET retained = 0 WHERE event_id = ?", (event_id,)
        )
        self._conn.commit()

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> AuditEvent:
        return AuditEvent(
            event_id=row["event_id"],
            source=row["source"],
            category=EventCategory(row["category"]),
            severity=Severity(row["severity"]),
            actor=row["actor"],
            action=row["action"],
            occurred_at=_dt_parse(row["occurred_at"]),
            received_at=_dt_parse(row["received_at"]),
            raw=json.loads(row["raw"]),
        )

    # -- 拒绝 ---------------------------------------------------------

    def save_rejection(self, rejected: RejectedEvent) -> None:
        self._conn.execute(
            """
            INSERT INTO rejected_events (source, raw, reason, detail, rejected_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                rejected.source,
                json.dumps(rejected.raw, ensure_ascii=False, sort_keys=True),
                rejected.reason.value,
                rejected.detail,
                _dt(rejected.rejected_at),
            ),
        )
        self._conn.commit()

    def list_rejections(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM rejected_events ORDER BY id"
        ).fetchall()
        return [
            {
                "source": r["source"],
                "raw": json.loads(r["raw"]),
                "reason": r["reason"],
                "detail": r["detail"],
                "rejected_at": _dt_parse(r["rejected_at"]),
            }
            for r in rows
        ]

    # -- 投递状态 -----------------------------------------------------

    def upsert_delivery(self, record: DeliveryRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO delivery_state
                (event_id, sink, status, attempts, failure_code,
                 last_error, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id, sink) DO UPDATE SET
                status = excluded.status,
                attempts = excluded.attempts,
                failure_code = excluded.failure_code,
                last_error = excluded.last_error,
                updated_at = excluded.updated_at
            """,
            (
                record.event_id, record.sink, record.status.value,
                record.attempts,
                record.failure_code.value if record.failure_code else None,
                record.last_error, _dt(record.updated_at),
            ),
        )
        self._conn.commit()

    def get_delivery(self, event_id: str, sink: str) -> DeliveryRecord | None:
        row = self._conn.execute(
            "SELECT * FROM delivery_state WHERE event_id = ? AND sink = ?",
            (event_id, sink),
        ).fetchone()
        return self._row_to_delivery(row) if row else None

    def list_deliveries(
        self, status: DeliveryStatus | None = None
    ) -> list[DeliveryRecord]:
        if status is None:
            rows = self._conn.execute(
                "SELECT * FROM delivery_state ORDER BY event_id, sink"
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM delivery_state WHERE status = ? "
                "ORDER BY event_id, sink",
                (status.value,),
            ).fetchall()
        return [self._row_to_delivery(r) for r in rows]

    @staticmethod
    def _row_to_delivery(row: sqlite3.Row) -> DeliveryRecord:
        return DeliveryRecord(
            event_id=row["event_id"],
            sink=row["sink"],
            status=DeliveryStatus(row["status"]),
            attempts=row["attempts"],
            failure_code=FailureCode(row["failure_code"])
            if row["failure_code"]
            else None,
            last_error=row["last_error"],
            updated_at=_dt_parse(row["updated_at"]),
        )

    # -- 留存策略版本 -------------------------------------------------

    def save_policy(
        self,
        version: int,
        rules: dict[str, int],
        created_at: datetime,
        effective_at: datetime,
    ) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO retention_policies
                (version, rules_json, created_at, effective_at)
            VALUES (?, ?, ?, ?)
            """,
            (version, json.dumps(rules, sort_keys=True),
             _dt(created_at), _dt(effective_at)),
        )
        self._conn.commit()

    def load_policy(self, version: int) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM retention_policies WHERE version = ?", (version,)
        ).fetchone()
        if row is None:
            return None
        return {
            "version": row["version"],
            "rules": json.loads(row["rules_json"]),
            "created_at": _dt_parse(row["created_at"]),
            "effective_at": _dt_parse(row["effective_at"]),
        }

    def latest_policy_version(self) -> int | None:
        row = self._conn.execute(
            "SELECT MAX(version) AS v FROM retention_policies"
        ).fetchone()
        return row["v"]

    # -- 清理痕迹 -----------------------------------------------------

    def record_cleanup(
        self,
        policy_version: int,
        event_id: str,
        source: str,
        category: str,
        occurred_at: datetime,
        reason: str,
        deleted_at: datetime,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO cleanup_audit
                (policy_version, event_id, source, category,
                 occurred_at, reason, deleted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                policy_version, event_id, source, category,
                _dt(occurred_at), reason, _dt(deleted_at),
            ),
        )
        self._conn.commit()

    def list_cleanup_audit(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM cleanup_audit ORDER BY id"
        ).fetchall()
        entries: list[dict[str, Any]] = []
        for r in rows:
            entry = dict(r)
            entry["occurred_at"] = _dt_parse(entry["occurred_at"])
            entry["deleted_at"] = _dt_parse(entry["deleted_at"])
            entries.append(entry)
        return entries
