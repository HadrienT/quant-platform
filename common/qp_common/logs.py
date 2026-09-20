"""One JSON object per line on stdout — what Loki ingests without parsing rules.

High-cardinality values (event_id, username, request_id) belong in the log LINE,
never in a Loki label: the collector labels by container only (blueprint WP 03).
"""

import json
import logging
import sys
from datetime import datetime, timezone


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        doc = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        extra = getattr(record, "fields", None)
        if extra:
            doc.update(extra)
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str)


def setup(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())


def fields(**kw: object) -> dict[str, dict[str, object]]:
    """logger.info("msg", extra=fields(batch=12))"""
    return {"fields": kw}
