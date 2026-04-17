import json
import logging
import sys
from datetime import datetime, timezone

# Attributs standards du LogRecord Python — on les exclut pour ne garder que les champs custom
_STANDARD_ATTRS = frozenset(
    {
        "args",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "message",
        "module",
        "msecs",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
        "taskName",
    }
)


class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Ajoute les champs custom passés via extra={...}
        log_entry.update(
            {k: v for k, v in record.__dict__.items() if k not in _STANDARD_ATTRS}
        )

        return json.dumps(log_entry, ensure_ascii=False)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Returns a logger that writes JSON to stdout.

    Usage:
        logger = get_logger(__name__)
        logger.info("Job started", extra={"extra": {"job": "bronze_orders", "rows": 99441}})
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False

    return logger
