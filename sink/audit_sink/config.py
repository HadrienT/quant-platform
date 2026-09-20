"""Configuration from the environment (12-factor); every value has a safe default."""

import os
from dataclasses import dataclass, field

# qm.audit.*, qm.dataquality.*, qm.assistant.* and qm.http.access.v1 — NOT qm.dlq.v1
# (the sink's own dead-letter topic must never feed back into it).
DEFAULT_TOPICS_REGEX = r"^(qm\.(audit|dataquality|assistant)\..+|qm\.http\.access\.v1)$"


@dataclass(frozen=True)
class Config:
    bootstrap: str = "kafka:9092"
    group_id: str = "audit-sink"
    topics_regex: str = DEFAULT_TOPICS_REGEX
    batch_size: int = 500
    batch_timeout_s: float = 1.0
    metrics_port: int = 9108
    log_level: str = "INFO"
    # Test hook (crash test, exercise 3): sleep AFTER the database commit and BEFORE
    # the offset commit — the exact window where a crash forces a re-read.
    debug_delay_s: float = 0.0
    db: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Config":
        e = dict(os.environ if env is None else env)
        return cls(
            bootstrap=e.get("KAFKA_BOOTSTRAP", cls.bootstrap),
            group_id=e.get("SINK_GROUP_ID", cls.group_id),
            topics_regex=e.get("SINK_TOPICS_REGEX", DEFAULT_TOPICS_REGEX),
            batch_size=int(e.get("SINK_BATCH_SIZE", cls.batch_size)),
            batch_timeout_s=int(e.get("SINK_BATCH_TIMEOUT_MS", "1000")) / 1000,
            metrics_port=int(e.get("SINK_METRICS_PORT", cls.metrics_port)),
            log_level=e.get("LOG_LEVEL", cls.log_level),
            debug_delay_s=int(e.get("SINK_DEBUG_DELAY_MS", "0")) / 1000,
            db={
                "host": e.get("PGHOST", "qm-audit"),
                "port": e.get("PGPORT", "5432"),
                "dbname": e.get("PGDATABASE", "qm_audit"),
                "user": e.get("PGUSER", "audit_writer"),
                "password": e["PGPASSWORD"] if "PGPASSWORD" in e else "",
                "connect_timeout": e.get("PGCONNECT_TIMEOUT", "5"),
            },
        )
