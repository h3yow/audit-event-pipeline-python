"""本地文件存储：事件库、拒件库、清理日志。

全部使用 JSONL（每行一条 JSON）追加写入，模拟本地持久化；
读取时逐行解析，坏行不会让整个库不可用。
"""
from __future__ import annotations

import json
from pathlib import Path

from .models import AuditEvent, RejectedEvent


def _append_line(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def _read_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]


class EventStore:
    """以 JSONL 追加方式保存已接受事件的本地事件库。"""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def append(self, event: AuditEvent) -> None:
        _append_line(self.path, event.to_json())

    def load_all(self) -> list[AuditEvent]:
        return [AuditEvent.from_json(ln) for ln in _read_lines(self.path)]

    def rewrite(self, events: list[AuditEvent]) -> None:
        """整体重写事件库（留存清理后调用）。"""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(
            "".join(e.to_json() + "\n" for e in events), encoding="utf-8"
        )
        tmp.replace(self.path)


class DeadLetterStore:
    """保存被拒绝事件及原因的本地拒件库。"""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def append(self, rejected: RejectedEvent) -> None:
        _append_line(self.path, rejected.to_json())

    def load_all(self) -> list[RejectedEvent]:
        return [RejectedEvent.from_json(ln) for ln in _read_lines(self.path)]


class PurgeLog:
    """记录每一次清理动作的审计日志（只追加，不删除）。"""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def record(self, entry: dict) -> None:
        _append_line(
            self.path, json.dumps(entry, ensure_ascii=False, sort_keys=True, default=str)
        )

    def load_all(self) -> list[dict]:
        return [json.loads(ln) for ln in _read_lines(self.path)]
