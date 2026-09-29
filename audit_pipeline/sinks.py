"""下游接收方：全部以本地存储模拟。

``LocalFileSink`` 把每条送达事件以 JSONL 追加写入本地文件，
并维护送达计数 / 已送达 event_id 集合 / 告警计数（CRITICAL
视为触发告警），供测试核对"不重复计数、不重复告警"。

故障注入方式：

* ``set_available(False)`` —— 持续不可用；
* ``fail_times(n)`` —— 接下来 n 次投递抛 ``SinkUnavailableError``，
  随后自动恢复，用于模拟"下游暂时写不进去"。
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from audit_pipeline.errors import SinkUnavailableError
from audit_pipeline.models import AuditEvent, Severity

logger = logging.getLogger("audit_pipeline.sink")


class Sink:
    name: str = ""

    def deliver(self, event: AuditEvent) -> None:
        raise NotImplementedError

    @property
    def available(self) -> bool:
        return True


class LocalFileSink(Sink):
    """以本地 JSONL 文件模拟下游接收方。"""

    def __init__(self, name: str, path: str | Path) -> None:
        self.name = name
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)
        self._available = True
        self._remaining_failures = 0
        self.delivered_count = 0
        self.alert_count = 0
        self.delivered_ids: set[str] = set()

    # -- 故障注入 -----------------------------------------------------

    def set_available(self, available: bool) -> None:
        self._available = available
        logger.info("sink=%s 可用性设置为 %s", self.name, available)

    def fail_times(self, n: int) -> None:
        self._remaining_failures = n

    @property
    def available(self) -> bool:
        return self._available and self._remaining_failures == 0

    # -- 投递 ---------------------------------------------------------

    def deliver(self, event: AuditEvent) -> None:
        if not self._available:
            raise SinkUnavailableError(f"sink={self.name} 当前不可用")
        if self._remaining_failures > 0:
            self._remaining_failures -= 1
            raise SinkUnavailableError(
                f"sink={self.name} 注入瞬时故障，剩余 {self._remaining_failures} 次"
            )

        record = {
            "event_id": event.event_id,
            "source": event.source,
            "category": event.category.value,
            "severity": event.severity.value,
            "actor": event.actor,
            "action": event.action,
            "occurred_at": event.occurred_at.isoformat(),
        }
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

        self.delivered_count += 1
        self.delivered_ids.add(event.event_id)
        if event.severity is Severity.CRITICAL:
            self.alert_count += 1
        logger.info(
            "sink=%s 写入成功 event_id=%s delivered_count=%d alert_count=%d",
            self.name, event.event_id, self.delivered_count, self.alert_count,
        )

    def read_lines(self) -> list[dict]:
        if not self.path.exists():
            return []
        with self.path.open(encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]
