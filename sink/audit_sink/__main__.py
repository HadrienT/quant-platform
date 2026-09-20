"""Entry point: python -m audit_sink"""

import logging

from confluent_kafka import Consumer, Producer

from qp_common import logs, metrics
from qp_common.lifecycle import GracefulStop

from .config import Config
from .store import Store
from .worker import Worker

log = logging.getLogger("audit_sink")


def main() -> None:
    cfg = Config.from_env()
    logs.setup(cfg.log_level)
    stop = GracefulStop()
    stop.install()
    metrics.serve(cfg.metrics_port)

    consumer = Consumer(
        {
            "bootstrap.servers": cfg.bootstrap,
            "group.id": cfg.group_id,
            # Offsets are committed by hand, AFTER the database commit.
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "cooperative-sticky",
            "session.timeout.ms": 30000,
            # New topics matching the regex are noticed within 30 s.
            "topic.metadata.refresh.interval.ms": 30000,
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": cfg.bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "zstd",
            "linger.ms": 20,
        }
    )
    store = Store(cfg.db)
    try:
        Worker(cfg, consumer, producer, store, stop).run()
    finally:
        store.close()
        producer.flush(10)
        consumer.close()  # leaves the group cleanly: partitions move at once
        log.info("sink stopped")


if __name__ == "__main__":
    main()
