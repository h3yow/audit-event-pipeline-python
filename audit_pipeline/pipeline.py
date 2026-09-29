"""端到端流水线：采集归一化 -> 入库 -> 分发，拒绝进拒件库。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .dispatch import Dispatcher, DispatchResult
from .ingestion import EventRejected, normalize_event
from .models import RejectedEvent
from .storage import DeadLetterStore, EventStore


@dataclass(frozen=True)
class IngestResult:
    """单条原始事件的处理结果。"""

    accepted: bool
    event_id: str | None
    rejection: RejectedEvent | None
    dispatch: DispatchResult | None


class AuditPipeline:
    """把采集、入库、分发、拒件记录串起来的本地流水线。"""

    def __init__(
        self,
        store: EventStore,
        dead_letters: DeadLetterStore,
        dispatcher: Dispatcher,
    ) -> None:
        self._store = store
        self._dead_letters = dead_letters
        self._dispatcher = dispatcher

    def ingest(self, raw: Any, source: str) -> IngestResult:
        """处理一条原始事件：归一化 -> 入库 -> 分发；失败则记录拒件。"""
        try:
            event = normalize_event(raw, source)
        except EventRejected as exc:
            rejected = RejectedEvent(
                raw=raw,
                source=source,
                reason=exc.reason,
                message=exc.message,
                rejected_at=datetime.now(timezone.utc),
            )
            self._dead_letters.append(rejected)
            return IngestResult(
                accepted=False, event_id=None, rejection=rejected, dispatch=None
            )
        self._store.append(event)
        dispatch = self._dispatcher.dispatch(event)
        return IngestResult(
            accepted=True, event_id=event.event_id, rejection=None, dispatch=dispatch
        )

    def flush_buffer(self) -> None:
        """重试分发缓冲（下游恢复后调用）。"""
        self._dispatcher.flush_buffer()
