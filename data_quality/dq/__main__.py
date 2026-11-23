"""Entry point: python -m dq"""

import logging
import sys

from confluent_kafka import Consumer

from qp_common import logs, metrics
from qp_common.lifecycle import GracefulStop
from qp_common.logs import fields
from qp_common.watchdog import ConsumerStuck
from qp_common.wire import Decoder, RegistryClient

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
    stuck = False
    try:
        registry = (
            RegistryClient(cfg.schema_registry_url) if cfg.schema_registry_url else None
        )
        Worker(cfg, consumer, aggregator, stop, decoder=Decoder(registry)).run()
    except ConsumerStuck as exc:
        # Exit non-zero so Docker restarts a fresh process (issue #9).
        log.error("consumer stuck outside its group", extra=fields(error=str(exc)))
        stuck = True
    finally:
        # A consumer that lost its coordinator may block in close(): skip it.
        if not stuck:
            consumer.close()
        log.info("data-quality stopped")
    if stuck:
        sys.exit(3)


if __name__ == "__main__":
    main()
