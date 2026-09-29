"""事件分发：路由规则、下游 sink、重试缓冲与去重。

- 路由：按事件类别匹配 :class:`RoutingRule`，一个类别可发往多个下游。
- 去重：以 (下游, event_id) 为准，下游已含该事件则跳过，不重复写入，
  因此重复投递不会导致下游重复计数或重复告警。
- 缓冲：下游暂时不可写（抛 :class:`SinkUnavailable`）时，事件进入本地
  缓冲文件，:meth:`Dispatcher.flush_buffer` 负责后续重试，绝不直接丢弃。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .models import AuditEvent, EventCategory


class Sink(Protocol):
    """下游接收方接口。"""

    name: str

    def write(self, event: AuditEvent) -> None:
        ...

    def contains(self, event_id: str) -> bool:
        ...


class SinkUnavailable(Exception):
    """下游暂时不可写时由 sink 抛出。"""


@dataclass(frozen=True)
class RoutingRule:
    """按类别把事件路由到指定下游。"""

    category: EventCategory
    sink_names: tuple[str, ...]


@dataclass(frozen=True)
class DispatchResult:
    event_id: str
    delivered: tuple[str, ...]   # 本次成功写入的下游
    buffered: tuple[str, ...]    # 写不进去、已进缓冲的下游
    deduplicated: bool           # 是否因重复投递被整体跳过


class LocalFileSink:
    """用本地 JSONL 文件模拟的下游接收方。

    ``available`` 置为 False 可模拟下游暂时不可用；写入前按 event_id
    去重，保证重复投递不会产生重复记录。
    """

    def __init__(self, name: str, path: Path) -> None:
        self.name = name
        self.path = Path(path)
        self.available = True

    def _event_ids(self) -> set[str]:
        if not self.path.exists():
            return set()
        ids: set[str] = set()
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                ids.add(json.loads(line)["event_id"])
        return ids

    def contains(self, event_id: str) -> bool:
        return event_id in self._event_ids()

    def write(self, event: AuditEvent) -> None:
        if not self.available:
            raise SinkUnavailable(f"下游 {self.name} 暂时不可用")
        if self.contains(event.event_id):
            return  # 幂等：重复投递不重复写
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(event.to_json() + "\n")

    def count(self) -> int:
        return len(self._event_ids())


class Dispatcher:
    """按规则分发事件；下游失败时缓冲重试；按 event_id 去重。"""

    def __init__(
        self,
        sinks: dict[str, Sink],
        rules: list[RoutingRule],
        buffer_path: Path,
    ) -> None:
        self._sinks = dict(sinks)
        self._rules = list(rules)
        self._buffer_path = Path(buffer_path)

    # ---- 路由 ----

    def _targets(self, event: AuditEvent) -> list[Sink]:
        names: list[str] = []
        for rule in self._rules:
            if rule.category is event.category:
                names.extend(rule.sink_names)
        seen: set[str] = set()
        targets: list[Sink] = []
        for name in names:
            if name not in seen and name in self._sinks:
                seen.add(name)
                targets.append(self._sinks[name])
        return targets

    # ---- 缓冲 ----

    def _buffer_entries(self) -> list[dict]:
        if not self._buffer_path.exists():
            return []
        return [
            json.loads(ln)
            for ln in self._buffer_path.read_text(encoding="utf-8").splitlines()
            if ln.strip()
        ]

    def _write_buffer(self, entries: list[dict]) -> None:
        self._buffer_path.parent.mkdir(parents=True, exist_ok=True)
        if not entries:
            if self._buffer_path.exists():
                self._buffer_path.unlink()
            return
        tmp = self._buffer_path.with_suffix(self._buffer_path.suffix + ".tmp")
        tmp.write_text(
            "".join(json.dumps(e, ensure_ascii=False, sort_keys=True) + "\n" for e in entries),
            encoding="utf-8",
        )
        tmp.replace(self._buffer_path)

    def _enqueue(self, sink_name: str, event: AuditEvent) -> None:
        entries = self._buffer_entries()
        if any(
            e["sink"] == sink_name and e["event"]["event_id"] == event.event_id
            for e in entries
        ):
            return  # 已在缓冲中，不重复占位
        entries.append({"sink": sink_name, "event": event.to_dict()})
        self._write_buffer(entries)

    # ---- 分发与重试 ----

    def dispatch(self, event: AuditEvent) -> DispatchResult:
        delivered: list[str] = []
        buffered: list[str] = []
        for sink in self._targets(event):
            if sink.contains(event.event_id):
                continue  # 下游已有此事件：去重跳过
            try:
                sink.write(event)
                delivered.append(sink.name)
            except SinkUnavailable:
                self._enqueue(sink.name, event)
                buffered.append(sink.name)
        deduplicated = not delivered and not buffered
        return DispatchResult(
            event_id=event.event_id,
            delivered=tuple(delivered),
            buffered=tuple(buffered),
            deduplicated=deduplicated,
        )

    def flush_buffer(self) -> None:
        """重试缓冲区里的事件；仍失败的留在缓冲中等待下次重试。"""
        remaining: list[dict] = []
        for entry in self._buffer_entries():
            sink = self._sinks.get(entry["sink"])
            event = AuditEvent.from_dict(entry["event"])
            if sink is None:
                remaining.append(entry)  # 未知下游：保留，等待人工处理
                continue
            if sink.contains(event.event_id):
                continue  # 已送达（可能由其他路径写入），丢弃缓冲副本
            try:
                sink.write(event)
            except SinkUnavailable:
                remaining.append(entry)
        self._write_buffer(remaining)

    def buffered_count(self) -> int:
        return len(self._buffer_entries())
