"""日志配置：单测中需打印原始事件与判定依据。"""

import logging

_DEFAULT_FORMAT = "%(asctime)s %(levelname)s %(name)s | %(message)s"


def setup_logging(level: str | int = "INFO") -> None:
    """配置 audit_pipeline 命名空间日志（幂等，可重复调用）。

    pytest 运行时 root 上已挂捕获 handler，保持 propagate=True 即可同时
    被 caplog 与 ``--log-cli-level`` 捕获；独立脚本（如 demo.py）运行时
    root 没有 handler，则补一个 StreamHandler 输出到 stderr。
    """
    root = logging.getLogger("audit_pipeline")
    root.setLevel(level)
    root.propagate = True
    if not logging.getLogger().handlers and not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT))
        root.addHandler(handler)
