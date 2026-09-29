"""事件归一化入口：按来源分派适配器。

* 未注册来源 -> ``RejectCode.UNKNOWN_SOURCE``；
* 适配器内部按 ``missing_field / invalid_format /
  illegal_timestamp / unknown_category`` 区分拒绝原因；
* 每次判定都打 INFO 日志，包含原始事件与判定依据，单测输出可直接核对。
"""

from __future__ import annotations

import logging
from typing import Any

from audit_pipeline.clock import Clock, utc_now
from audit_pipeline.models import NormalizationResult, RejectedEvent, RejectCode
from audit_pipeline.source_adapters import (
    GatewayAdapter,
    SourceAdapter,
    WebAppAdapter,
)

logger = logging.getLogger("audit_pipeline.normalizer")


class Normalizer:
    def __init__(
        self,
        adapters: dict[str, SourceAdapter] | None = None,
        clock: Clock = utc_now,
    ) -> None:
        if adapters is None:
            adapters = {
                WebAppAdapter.source_name: WebAppAdapter(),
                GatewayAdapter.source_name: GatewayAdapter(),
            }
        self._adapters = adapters
        self._clock = clock

    def normalize(self, source: str, raw: Any) -> NormalizationResult:
        logger.info("归一化判定 | source=%s 原始事件=%r", source, raw)
        adapter = self._adapters.get(source)
        if adapter is None:
            result = NormalizationResult(
                accepted=None,
                rejected=RejectedEvent(
                    source=source,
                    raw=raw,
                    reason=RejectCode.UNKNOWN_SOURCE,
                    detail=f"未注册的事件来源: {source!r}",
                    rejected_at=self._clock(),
                ),
            )
            logger.info(
                "判定依据: source=%s 未在注册表 %s 中，拒绝原因=%s",
                source, sorted(self._adapters), result.rejected.reason.value,
            )
            return result

        result = adapter.parse(raw, self._clock())
        if result.ok:
            event = result.accepted
            logger.info(
                "判定依据: 来源适配器=%s 解析成功 event_id=%s category=%s severity=%s -> 接受",
                source, event.event_id, event.category.value, event.severity.value,
            )
        else:
            logger.info(
                "判定依据: 来源适配器=%s 拒绝原因=%s 详情=%s",
                source, result.rejected.reason.value, result.rejected.detail,
            )
        return result
