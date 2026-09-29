"""可注入的时钟，保证时间相关逻辑可测、可复现。"""

from collections.abc import Callable
from datetime import datetime, timezone


Clock = Callable[[], datetime]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
