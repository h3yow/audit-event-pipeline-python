"""归一化：字段缺失/格式非法按不同原因拒绝；未知来源可区分。"""

from __future__ import annotations

import logging

from audit_pipeline.models import RejectCode
from audit_pipeline.normalizer import Normalizer


def test_web_app_accept_and_normalize():
    n = Normalizer()
    result = n.normalize(
        "web_app",
        {
            "event_id": "w-1",
            "type": "user.login",
            "user": "alice",
            "ts": "2026-09-29T08:00:00Z",
            "severity": "info",
        },
    )
    assert result.ok
    e = result.accepted
    assert e.event_id == "w-1"
    assert e.source == "web_app"
    assert e.category.value == "login"
    assert e.actor == "alice"


def test_gateway_heterogeneous_fields_accepted():
    n = Normalizer()
    result = n.normalize(
        "gateway",
        {
            "id": "g-1",
            "event_type": "DATA_EXPORT",
            "level": "Critical",
            "subject": "bob",
            "op": "export_customers",
            "@timestamp": "2026/09/29 08:30:00",
        },
    )
    assert result.ok
    e = result.accepted
    assert e.category.value == "data_access"
    assert e.severity.value == "critical"


def test_missing_field_distinct_from_bad_format():
    n = Normalizer()

    missing = n.normalize(
        "web_app",
        {"type": "user.login", "user": "alice", "ts": "2026-09-29T08:00:00Z"},
    )
    assert not missing.ok
    assert missing.rejected.reason is RejectCode.MISSING_FIELD
    assert "event_id" in missing.rejected.detail

    bad_format = n.normalize(
        "web_app",
        {"event_id": "w-2", "type": "user.login", "user": 123,
         "ts": "2026-09-29T08:00:00Z"},
    )
    assert not bad_format.ok
    assert bad_format.rejected.reason is RejectCode.INVALID_FORMAT

    illegal_ts = n.normalize(
        "gateway",
        {"id": "g-2", "event_type": "LOGIN", "subject": "bob",
         "op": "login", "@timestamp": "not-a-time"},
    )
    assert not illegal_ts.ok
    assert illegal_ts.rejected.reason is RejectCode.ILLEGAL_TIMESTAMP

    unknown_category = n.normalize(
        "web_app",
        {"event_id": "w-3", "type": "mystery.event", "user": "alice",
         "ts": "2026-09-29T08:00:00Z"},
    )
    assert not unknown_category.ok
    assert unknown_category.rejected.reason is RejectCode.UNKNOWN_CATEGORY

    unknown_source = n.normalize("iot_sensor", {"x": 1})
    assert not unknown_source.ok
    assert unknown_source.rejected.reason is RejectCode.UNKNOWN_SOURCE

    reasons = {
        missing.rejected.reason, bad_format.rejected.reason,
        illegal_ts.rejected.reason, unknown_category.rejected.reason,
        unknown_source.rejected.reason,
    }
    assert len(reasons) == 5


def test_non_object_payload_rejected_not_dropped():
    n = Normalizer()
    result = n.normalize("web_app", ["not", "an", "object"])
    assert not result.ok
    assert result.rejected.reason is RejectCode.INVALID_FORMAT
    assert result.rejected.raw == ["not", "an", "object"]


def test_logs_include_raw_event_and_basis(caplog):
    n = Normalizer()
    raw = {"event_id": "w-9", "type": "data.query", "user": "carol",
           "ts": "2026-09-29T09:00:00Z"}
    with caplog.at_level(logging.INFO):
        n.normalize("web_app", raw)
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "原始事件=" in text
    assert "w-9" in text
    assert "判定依据" in text
