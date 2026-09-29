import logging
from datetime import datetime, timezone

import pytest

from audit_pipeline import (
    AuditPipeline,
    Dispatcher,
    EventCategory,
    EventStore,
    DeadLetterStore,
    LocalFileSink,
    RoutingRule,
)

logger = logging.getLogger("audit_tests")


@pytest.fixture()
def now():
    return datetime(2026, 9, 29, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def sinks(tmp_path):
    return {
        "security": LocalFileSink("security", tmp_path / "sink_security.jsonl"),
        "ops": LocalFileSink("ops", tmp_path / "sink_ops.jsonl"),
    }


@pytest.fixture()
def dispatcher(sinks, tmp_path):
    return Dispatcher(
        sinks,
        rules=[
            RoutingRule(EventCategory.SECURITY, ("security", "ops")),
            RoutingRule(EventCategory.ACCESS, ("security",)),
            RoutingRule(EventCategory.SYSTEM, ("ops",)),
            RoutingRule(EventCategory.DATA, ("ops",)),
        ],
        buffer_path=tmp_path / "buffer.jsonl",
    )


@pytest.fixture()
def pipeline(dispatcher, tmp_path):
    return AuditPipeline(
        store=EventStore(tmp_path / "events.jsonl"),
        dead_letters=DeadLetterStore(tmp_path / "dead_letters.jsonl"),
        dispatcher=dispatcher,
    )
