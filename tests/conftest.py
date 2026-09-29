"""测试公共配置：打开 INFO 日志，使每次判定的原始事件与依据可在单测输出中看到。"""

import logging

import pytest

from audit_pipeline.logging_setup import setup_logging


@pytest.fixture(autouse=True)
def _enable_pipeline_logging(caplog):
    setup_logging("INFO")
    caplog.set_level(logging.INFO, logger="audit_pipeline")
    yield
