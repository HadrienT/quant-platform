"""Entry point: python -m dq"""

import logging

from confluent_kafka import Consumer

from qp_common import logs, metrics
from qp_common.lifecycle import GracefulStop

from .aggregate import Aggregator
from .config import Config
from .worker import Worker

log = logging.getLogger("dq")


def main() -> None:
    cfg = Config.from_env()
    logs.setup(cfg.log_level)
    stop = GracefulStop()
    stop.install()
    aggregator = Aggregator(cfg.window_s)
    metrics.serve(cfg.metrics_port)

    consumer = Consumer(
        {
            "bootstrap.servers": cfg.bootstrap,
            # Its own group: independent of the audit sink (neither slows the other).
            "group.id": cfg.group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "cooperative-sticky",
            "session.timeout.ms": 30000,
        }
    )
    try:
        Worker(cfg, consumer, aggregator, stop).run()
    finally:
        consumer.close()
        log.info("data-quality stopped")


if __name__ == "__main__":
    main()
