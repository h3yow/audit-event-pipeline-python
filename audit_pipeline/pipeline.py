"""流水线门面：串联归一化 -> 持久化 -> 分发 ->（可选）留存。

``ingest`` 是唯一入口，保证：

* 非法事件落 ``rejected_events``，带区分化拒绝原因；
* 合法事件按 event_id 幂等入库，重复投递不会重复分发计数；
* 入库时绑定当前留存策略版本，供后续过期判定复现。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum

from audit_pipeline.models import DeliveryRecord, NormalizationResult
from audit_pipeline.normalizer import Normalizer
from audit_pipeline.retention import RetentionService
from audit_pipeline.storage import LocalStorage
from audit_pipeline.dispatcher import Dispatcher

logger = logging.getLogger("audit_pipeline.pipeline")


class IngestStatus(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


@dataclass
class IngestResult:
    status: IngestStatus
    normalization: NormalizationResult
    deliveries: list[DeliveryRecord] = field(default_factory=list)


class AuditPipeline:
    def __init__(
        self,
        storage: LocalStorage,
        normalizer: Normalizer,
        dispatcher: Dispatcher,
        retention: RetentionService | None = None,
    ) -> None:
        self._storage = storage
        self._normalizer = normalizer
        self._dispatcher = dispatcher
        self._retention = retention

    def ingest(self, source: str, raw: object) -> IngestResult:
        logger.info("收到事件 source=%s 原始事件=%r", source, raw)
        result = self._normalizer.normalize(source, raw)

        if not result.ok:
            self._storage.save_rejection(result.rejected)
            logger.info(
                "事件被拒已留档 source=%s reason=%s",
                source, result.rejected.reason.value,
            )
            return IngestResult(
                status=IngestStatus.REJECTED,
                normalization=result,
            )

        event = result.accepted
        policy_version = (
            self._retention.current_version() if self._retention else None
        )
        first_seen = self._storage.save_event(
            event, policy_version=policy_version
        )
        if not first_seen:
            logger.info(
                "重复投递 event_id=%s：事件已存在，跳过分发（下游不重复计数）",
                event.event_id,
            )
            return IngestResult(
                status=IngestStatus.DUPLICATE,
                normalization=result,
            )

        deliveries = self._dispatcher.dispatch(event)
        return IngestResult(
            status=IngestStatus.ACCEPTED,
            normalization=result,
            deliveries=deliveries,
        )

    def retry_pending(self) -> list[DeliveryRecord]:
        return self._dispatcher.retry_pending()
