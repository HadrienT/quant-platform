"""Entry point: python -m audit_sink"""

import logging
import sys

from confluent_kafka import Consumer, Producer

from qp_common import logs, metrics
from qp_common.lifecycle import GracefulStop
from qp_common.logs import fields
from qp_common.watchdog import ConsumerStuck
from qp_common.wire import Decoder, RegistryClient

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
    registry = (
        RegistryClient(cfg.schema_registry_url) if cfg.schema_registry_url else None
    )
    stuck = False
    try:
        Worker(cfg, consumer, producer, store, stop, decoder=Decoder(registry)).run()
    except ConsumerStuck as exc:
        # Exit non-zero so Docker restarts a fresh process (issue #9). Nothing is
        # pending: offsets are only committed after processing, so the new process
        # resumes from the last commit and at worst re-reads one batch.
        log.error("consumer stuck outside its group", extra=fields(error=str(exc)))
        stuck = True
    finally:
        store.close()
        producer.flush(10)
        # A consumer that lost its coordinator may block in close(): skip it, the
        # broker drops the member when its session times out.
        if not stuck:
            consumer.close()  # leaves the group cleanly: partitions move at once
        log.info("sink stopped")
    if stuck:
        sys.exit(3)


if __name__ == "__main__":
    main()
