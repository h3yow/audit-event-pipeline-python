"""端到端：采集(本地文件模拟来源) -> 归一化 -> 分发 -> 留存。"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from audit_pipeline.dispatcher import Dispatcher
from audit_pipeline.models import (
    DeliveryStatus,
    EventCategory,
    RoutingRule,
)
from audit_pipeline.normalizer import Normalizer
from audit_pipeline.pipeline import AuditPipeline, IngestStatus
from audit_pipeline.retention import RetentionPolicy, RetentionService
from audit_pipeline.sinks import LocalFileSink
from audit_pipeline.storage import LocalStorage


NOW = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)


def read_spool(path: Path) -> list[tuple[str, dict]]:
    """本地存储模拟事件来源：每行 JSONL，含 _source 标记。"""
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            record = json.loads(line)
            out.append((record.pop("_source"), record))
    return out


@pytest.fixture
def app(tmp_path):
    storage = LocalStorage()
    siem = LocalFileSink("siem", tmp_path / "downstream" / "siem.jsonl")
    alerter = LocalFileSink("alerter", tmp_path / "downstream" / "alerter.jsonl")
    rules = [
        RoutingRule("siem", frozenset(EventCategory)),
        RoutingRule(
            "alerter",
            frozenset({EventCategory.SYSTEM_OP,
                       EventCategory.PERMISSION_CHANGE}),
        ),
    ]
    dispatcher = Dispatcher(
        storage, {"siem": siem, "alerter": alerter}, rules,
        clock=lambda: NOW, max_attempts=3,
    )
    retention = RetentionService(storage, clock=lambda: NOW)
    retention.apply_policy(
        RetentionPolicy(
            1,
            {"login": 30, "data_access": 90,
             "permission_change": 365, "system_op": 180},
            default_days=60, created_at=NOW, effective_at=NOW,
        )
    )
    pipeline = AuditPipeline(
        storage, Normalizer(clock=lambda: NOW), dispatcher, retention
    )
    return pipeline, storage, siem, alerter


def test_end_to_end_happy_path(app, tmp_path):
    pipeline, storage, siem, alerter = app
    spool = tmp_path / "spool.jsonl"
    spool.write_text(
        "\n".join(
            [
                json.dumps({"_source": "web_app", "event_id": "w1",
                            "type": "user.login", "user": "alice",
                            "ts": "2026-09-29T08:00:00Z",
                            "severity": "info"}),
                json.dumps({"_source": "gateway", "id": "g1",
                            "event_type": "SYS_RESTART", "level": "CRITICAL",
                            "subject": "root", "op": "restart",
                            "@timestamp": "2026/09/29 08:00:00"}),
            ]
        ),
        encoding="utf-8",
    )

    results = [pipeline.ingest(src, raw) for src, raw in read_spool(spool)]
    assert all(r.status is IngestStatus.ACCEPTED for r in results)
    assert {e.event_id for e in storage.list_events()} == {"w1", "g1"}
    assert siem.delivered_count == 2
    assert alerter.delivered_count == 1
    assert alerter.alert_count == 1
    # 入库时绑定了策略 v1
    assert storage.event_policy_version("w1") == 1


def test_end_to_end_duplicate_delivery(app):
    pipeline, storage, siem, alerter = app
    raw = {"event_id": "dup1", "type": "user.login", "user": "alice",
           "ts": "2026-09-29T08:00:00Z"}

    first = pipeline.ingest("web_app", raw)
    second = pipeline.ingest("web_app", dict(raw))  # 同 event_id 重复投递

    assert first.status is IngestStatus.ACCEPTED
    assert second.status is IngestStatus.DUPLICATE
    assert siem.delivered_count == 1
    assert alerter.delivered_count == 0  # login 不路由 alerter
    assert len(storage.list_events()) == 1
    assert storage.list_deliveries()[0].status is DeliveryStatus.DELIVERED


def test_end_to_end_invalid_events_distinguished_and_archived(app):
    pipeline, storage, siem, alerter = app

    r_missing = pipeline.ingest(
        "web_app", {"type": "user.login", "user": "a",
                    "ts": "2026-09-29T08:00:00Z"}
    )
    r_bad_ts = pipeline.ingest(
        "gateway", {"id": "x", "event_type": "LOGIN", "subject": "a",
                    "op": "x", "@timestamp": "bad"}
    )
    r_unknown = pipeline.ingest("iot", {"foo": 1})

    assert r_missing.status is IngestStatus.REJECTED
    assert r_bad_ts.status is IngestStatus.REJECTED
    assert r_unknown.status is IngestStatus.REJECTED

    rejections = storage.list_rejections()
    reasons = {r["reason"] for r in rejections}
    assert reasons == {"missing_field", "illegal_timestamp", "unknown_source"}
    # 被拒事件原始 payload 完整保留，没有被悄悄丢掉
    assert rejections[0]["raw"]["type"] == "user.login"
    assert siem.delivered_count == 0


def test_end_to_end_outage_retry_then_policy_change(app):
    pipeline, storage, siem, alerter = app

    # 下游全挂：投递缓冲
    siem.set_available(False)
    alerter.set_available(False)
    result = pipeline.ingest(
        "gateway",
        {"id": "g9", "event_type": "SYS_RESTART", "level": "CRITICAL",
         "subject": "root", "op": "restart",
         "@timestamp": "2026/09/29 08:00:00"},
    )
    assert result.status is IngestStatus.ACCEPTED
    assert all(r.status is DeliveryStatus.PENDING for r in result.deliveries)
    assert siem.delivered_count == 0 and alerter.delivered_count == 0

    # 恢复后重试：每条恰好送达一次
    siem.set_available(True)
    alerter.set_available(True)
    recovered = pipeline.retry_pending()
    assert {r.sink: r.status for r in recovered} == {
        "siem": DeliveryStatus.DELIVERED,
        "alerter": DeliveryStatus.DELIVERED,
    }
    assert siem.delivered_count == 1
    assert alerter.alert_count == 1

    # 策略从 v1(system_op=180) 变更为 v2(system_op=10)：
    # 已有事件 g9 绑定 v1，判定不随策略改变
    pipeline._retention.apply_policy(
        RetentionPolicy(
            2, {"login": 30, "data_access": 90,
                "permission_change": 365, "system_op": 10},
            default_days=60,
            created_at=NOW + timedelta(days=1),
            effective_at=NOW + timedelta(days=1),
        )
    )
    event_g9 = storage.get_event("g9")
    assert storage.event_policy_version("g9") == 1
    assert pipeline._retention.is_expired(event_g9, NOW) is False

    # 模拟 200 天后清理：g9 按 v1 的 180 天过期
    future = NOW + timedelta(days=200)
    cleanup = pipeline._retention.cleanup_expired(future)
    assert cleanup.deleted == ["g9"]
    trail = storage.list_cleanup_audit()
    assert trail[0]["policy_version"] == 1
    assert "v1" in trail[0]["reason"]


def test_end_to_end_logs_raw_and_basis(app, caplog):
    pipeline, storage, siem, alerter = app
    with caplog.at_level(logging.INFO):
        pipeline.ingest(
            "web_app",
            {"event_id": "log1", "type": "data.query", "user": "zoe",
             "ts": "2026-09-29T09:00:00Z"},
        )
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "原始事件=" in text
    assert "log1" in text
