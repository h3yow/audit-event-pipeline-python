"""本地演示：采集 -> 归一化 -> 分发（含下游故障与重试）-> 留存清理。

运行：uv run python main.py
所有状态写入 ./data/ 下的 JSONL 文件，可直接查看核对。
"""
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from audit_pipeline import (
    AuditPipeline,
    Dispatcher,
    EventCategory,
    EventStore,
    DeadLetterStore,
    LocalFileSink,
    PurgeLog,
    RetentionPolicy,
    RoutingRule,
    run_retention,
)

DATA = Path("data")


def main() -> None:
    if DATA.exists():
        shutil.rmtree(DATA)

    sinks = {
        "security": LocalFileSink("security", DATA / "sink_security.jsonl"),
        "ops": LocalFileSink("ops", DATA / "sink_ops.jsonl"),
    }
    dispatcher = Dispatcher(
        sinks,
        rules=[
            RoutingRule(EventCategory.SECURITY, ("security", "ops")),
            RoutingRule(EventCategory.ACCESS, ("security",)),
            RoutingRule(EventCategory.SYSTEM, ("ops",)),
            RoutingRule(EventCategory.DATA, ("ops",)),
        ],
        buffer_path=DATA / "buffer.jsonl",
    )
    pipeline = AuditPipeline(
        store=EventStore(DATA / "events.jsonl"),
        dead_letters=DeadLetterStore(DATA / "dead_letters.jsonl"),
        dispatcher=dispatcher,
    )

    now = datetime.now(timezone.utc)
    raw_events = [
        ({"id": "e1", "ts": now.isoformat(), "user": "alice", "action": "login",
          "category": "access"}, "app-json"),
        ({"uid": "e2", "time": (now - timedelta(hours=1)).timestamp(), "who": "bob",
          "op": "export", "klass": "data"}, "legacy"),
        ("id=e3 ts=" + now.isoformat() + " user=carol action=restart category=system", "syslog"),
        ({"id": "e1", "ts": now.isoformat(), "user": "alice", "action": "login",
          "category": "access"}, "app-json"),  # 重复投递
        ({"id": "bad-1", "user": "mallory"}, "app-json"),  # 缺字段，应被拒
    ]

    print("== 1. 采集与分发（ops 下游先宕机） ==")
    sinks["ops"].available = False
    for raw, source in raw_events:
        result = pipeline.ingest(raw, source)
        if result.accepted:
            d = result.dispatch
            print(f"  接受 {result.event_id}: delivered={d.delivered} "
                  f"buffered={d.buffered} dedup={d.deduplicated}")
        else:
            r = result.rejection
            print(f"  拒绝 [{r.reason.value}] {r.message} | raw={r.raw}")

    print("== 2. ops 恢复，重试缓冲 ==")
    sinks["ops"].available = True
    pipeline.flush_buffer()
    for name, sink in sinks.items():
        print(f"  下游 {name}: {sink.count()} 条")

    print("== 3. 留存清理（system 保留 0 天，演示清理留痕） ==")
    policy = RetentionPolicy(
        version=1,
        retention_days={
            EventCategory.SECURITY: 365,
            EventCategory.ACCESS: 90,
            EventCategory.SYSTEM: 0,
            EventCategory.DATA: 180,
        },
    )
    decisions = run_retention(
        EventStore(DATA / "events.jsonl"), policy, now, PurgeLog(DATA / "purge_log.jsonl")
    )
    for d in decisions:
        print(f"  {d.event_id}: {'保留' if d.keep else '清理'} | {d.reason}")

    print(f"== 完成，状态文件在 {DATA}/ ==")
    for p in sorted(DATA.glob("*.jsonl")):
        print(f"  {p}")


if __name__ == "__main__":
    main()
