"""结构化日志模块"""
import uuid
import logging
import structlog
from contextvars import ContextVar
from typing import Optional

trace_id_var: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)


def get_trace_id() -> str:
    tid = trace_id_var.get()
    if tid is None:
        tid = str(uuid.uuid4())[:12]
        trace_id_var.set(tid)
    return tid


def set_trace_id(tid: str):
    trace_id_var.set(tid)


def _add_trace_id(logger, method_name, event_dict):
    event_dict["trace_id"] = get_trace_id()
    return event_dict

def configure_logging(log_level: str = "INFO"):
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _add_trace_id,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str = __name__):
    return structlog.get_logger(name)