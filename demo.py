"""本地端到端演示：不依赖任何外部服务。

运行::

    uv run python demo.py --data-dir ./demo_data

流程：
1. 在 ``data_dir/spool.jsonl`` 生成本地事件来源（两种异构格式）；
2. 逐行采集归一化，非法事件按原因拒掉并留档；
3. 向两个本地文件下游分发，先注入一次下游不可用，再重试；
4. 登记 v1 留存策略，再改为 v2，演示已有事件按绑定版本判定；
5. 执行清理，打印 cleanup_audit 痕迹。
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from audit_pipeline.dispatcher import Dispatcher
from audit_pipeline.logging_setup import setup_logging
from audit_pipeline.models import (
    DeliveryStatus,
    EventCategory,
    RoutingRule,
    Severity,
)
from audit_pipeline.normalizer import Normalizer
from audit_pipeline.pipeline import AuditPipeline
from audit_pipeline.retention import RetentionPolicy, RetentionService
from audit_pipeline.sinks import LocalFileSink
from audit_pipeline.storage import LocalStorage


def build_spool(path: Path) -> None:
    if path.exists():
        return
    rows = [
        {"_source": "web_app", "event_id": "w-001", "type": "user.login",
         "user": "alice", "ts": "2026-09-28T08:00:00Z", "severity": "info"},
        {"_source": "gateway", "id": "g-001", "event_type": "SYS_RESTART",
         "level": "CRITICAL", "subject": "root", "op": "restart",
         "@timestamp": "2026/09/28 09:00:00"},
        {"_source": "gateway", "id": "g-002", "event_type": "DATA_EXPORT",
         "level": "warning", "subject": "bob", "op": "export",
         "@timestamp": "2026/09/28 10:00:00"},
        # 非法：缺 event_id
        {"_source": "web_app", "type": "user.login", "user": "x",
         "ts": "2026-09-28T08:00:00Z"},
        # 非法：时间格式不合法
        {"_source": "gateway", "id": "g-bad", "event_type": "LOGIN",
         "subject": "x", "op": "login", "@timestamp": "yesterday"},
    ]
    path.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows),
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="./demo_data")
    args = parser.parse_args()

    setup_logging("INFO")
    base = Path(args.data_dir)
    base.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    spool = base / "spool.jsonl"
    build_spool(spool)

    storage = LocalStorage(str(base / "pipeline.db"))
    siem = LocalFileSink("siem", base / "siem.jsonl")
    alerter = LocalFileSink("alerter", base / "alerter.jsonl")
    dispatcher = Dispatcher(
        storage,
        {"siem": siem, "alerter": alerter},
        [
            RoutingRule("siem", frozenset(EventCategory)),
            RoutingRule(
                "alerter",
                frozenset({EventCategory.SYSTEM_OP,
                           EventCategory.PERMISSION_CHANGE}),
                Severity.WARNING,
            ),
        ],
        max_attempts=3,
    )
    retention = RetentionService(storage)
    retention.apply_policy(
        RetentionPolicy(
            1,
            {"login": 30, "data_access": 90,
             "permission_change": 365, "system_op": 180},
            default_days=60,
        )
    )
    pipeline = AuditPipeline(storage, Normalizer(), dispatcher, retention)

    print("== 1. 采集本地 spool ==")
    for line in spool.read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        source = record.pop("_source")
        result = pipeline.ingest(source, record)
        print(f"  -> {result.status.value}")

    print("== 2. 注入 siem 不可用后重试 ==")
    siem.set_available(False)
    # 对尚未送达的记录演示缓冲（首批采集时 siem 正常，这里重放 g-002 无效，
    # 故改用一条新事件演示故障路径）
    failing = {
        "_source": "web_app", "event_id": "w-002", "type": "role.grant",
        "user": "carol", "ts": (now - timedelta(minutes=5)).isoformat(),
        "severity": "warning",
    }
    source = failing.pop("_source")
    result = pipeline.ingest(source, failing)
    states = {r.sink: r.status.value for r in result.deliveries}
    print(f"  故障期间投递状态: {states}")
    siem.set_available(True)
    recovered = pipeline.retry_pending()
    print(f"  重试结果: {[ (r.sink, r.status.value) for r in recovered ]}")

    print("== 3. 重复投递 w-001 ==")
    dup = pipeline.ingest(
        "web_app",
        {"event_id": "w-001", "type": "user.login", "user": "alice",
         "ts": "2026-09-28T08:00:00Z", "severity": "info"},
    )
    print(f"  -> {dup.status.value}（siem 计数仍为 {siem.delivered_count}）")

    print("== 4. 策略变更 v1 -> v2（演示版本绑定，不实际清理新事件）==")
    retention.apply_policy(
        RetentionPolicy(
            2,
            {"login": 7, "data_access": 30,
             "permission_change": 90, "system_op": 30},
            default_days=30,
        )
    )
    for event in storage.list_events():
        version = storage.event_policy_version(event.event_id)
        print(f"  {event.event_id} 绑定策略 v{version}")

    print("== 5. 清理：时间快进 400 天（演示锚点 2026-09-29 + 400d）==")
    anchor = datetime(2026, 9, 29, tzinfo=timezone.utc) + timedelta(days=400)
    cleanup = retention.cleanup_expired(anchor)
    print(f"  删除: {cleanup.deleted} 保留: {cleanup.skipped}")
    for entry in storage.list_cleanup_audit():
        print(f"  清理痕迹: {entry['event_id']} 按v{entry['policy_version']} "
              f"原因: {entry['reason']}")

    print("== 汇总 ==")
    print(f"  接受事件: {len(storage.list_events())} "
          f"拒绝留档: {len(storage.list_rejections())} "
          f"投递记录: {len(storage.list_deliveries())}")
    reasons: dict[str, int] = {}
    for r in storage.list_rejections():
        reasons[r["reason"]] = reasons.get(r["reason"], 0) + 1
    print(f"  拒绝原因分布: {reasons}")
    failed = storage.list_deliveries(DeliveryStatus.FAILED)
    print(f"  最终失败(未丢): {[(d.event_id, d.sink, d.failure_code.value) for d in failed]}")
    storage.close()


if __name__ == "__main__":
    main()
