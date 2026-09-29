"""流水线自定义异常。"""


class PipelineError(Exception):
    """流水线错误基类。"""


class UnknownSourceError(PipelineError):
    """事件来源未注册。"""


class SinkUnavailableError(PipelineError):
    """下游接收方暂时不可写。"""


class PolicyError(PipelineError):
    """留存策略错误。"""
