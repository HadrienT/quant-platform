"""Configuration from the environment; every value has a safe default."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    bootstrap: str = "kafka:9092"
    group_id: str = "data-quality"
    topics: tuple[str, ...] = ("qm.dataquality.fallback.v1", "qm.audit.valuation.v1")
    batch_size: int = 500
    batch_timeout_s: float = 1.0
    metrics_port: int = 9109
    schema_registry_url: str = ""
    window_s: int = 900
    log_level: str = "INFO"
    # Test hooks (crash test): pause AFTER processing a batch and BEFORE committing it,
    # and log every processed event_id so "no loss" can be counted from the logs.
    debug_delay_s: float = 0.0
    log_events: bool = False

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Config":
        e = dict(os.environ if env is None else env)
        return cls(
            bootstrap=e.get("KAFKA_BOOTSTRAP", cls.bootstrap),
            group_id=e.get("DQ_GROUP_ID", cls.group_id),
            topics=tuple(
                t for t in e.get("DQ_TOPICS", ",".join(cls.topics)).split(",") if t
            ),
            batch_size=int(e.get("DQ_BATCH_SIZE", cls.batch_size)),
            batch_timeout_s=int(e.get("DQ_BATCH_TIMEOUT_MS", "1000")) / 1000,
            metrics_port=int(e.get("DQ_METRICS_PORT", cls.metrics_port)),
            schema_registry_url=e.get("SCHEMA_REGISTRY_URL", ""),
            window_s=int(e.get("DQ_WINDOW_S", cls.window_s)),
            log_level=e.get("LOG_LEVEL", cls.log_level),
            debug_delay_s=int(e.get("DQ_DEBUG_DELAY_MS", "0")) / 1000,
            log_events=e.get("DQ_LOG_EVENTS", "0") == "1",
        )
