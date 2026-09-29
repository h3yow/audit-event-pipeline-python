"""统一模型基础行为。"""

from datetime import datetime, timezone

from audit_pipeline.models import (
    AuditEvent,
    DeliveryRecord,
    DeliveryStatus,
    EventCategory,
    NormalizationResult,
    RejectedEvent,
    RejectCode,
    Severity,
)


def test_event_is_frozen_value_object():
    e = AuditEvent(
        event_id="1", source="s", category=EventCategory.LOGIN,
        severity=Severity.INFO, actor="a", action="act",
        occurred_at=datetime(2026, 9, 29, tzinfo=timezone.utc),
        received_at=datetime(2026, 9, 29, tzinfo=timezone.utc),
    )
    try:
        e.actor = "b"  # type: ignore[misc]
    except AttributeError:
        pass
    else:
        raise AssertionError("AuditEvent 应为冻结对象")


def test_normalization_result_ok_flag():
    accepted = NormalizationResult(
        accepted=AuditEvent(
            event_id="1", source="s", category=EventCategory.LOGIN,
            severity=Severity.INFO, actor="a", action="act",
            occurred_at=datetime(2026, 9, 29, tzinfo=timezone.utc),
            received_at=datetime(2026, 9, 29, tzinfo=timezone.utc),
        ),
        rejected=None,
    )
    rejected = NormalizationResult(
        accepted=None,
        rejected=RejectedEvent(
            source="s", raw={}, reason=RejectCode.MISSING_FIELD,
            detail="x", rejected_at=datetime(2026, 9, 29, tzinfo=timezone.utc),
        ),
    )
    assert accepted.ok is True
    assert rejected.ok is False


def test_delivery_status_values():
    assert DeliveryStatus.PENDING.value == "pending"
    assert DeliveryStatus.DELIVERED.value == "delivered"
    assert DeliveryStatus.FAILED.value == "failed"
    rec = DeliveryRecord(
        event_id="1", sink="s", status=DeliveryStatus.DELIVERED,
        attempts=1, failure_code=None, last_error=None,
        updated_at=datetime(2026, 9, 29, tzinfo=timezone.utc),
    )
    assert rec.failure_code is None
