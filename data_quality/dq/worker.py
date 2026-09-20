"""consume → aggregate → commit, with a manual commit AFTER the batch is processed.

Same discipline as the audit sink: a crash before the commit re-reads the batch, so
every event is processed AT LEAST once and none is lost. Unlike the sink there is no
idempotent store: the aggregates are in-memory counters, which reset on restart
(Prometheus handles counter resets) — an event re-read after a crash can be counted
twice in the new incarnation, which is harmless for a monitoring signal and is why
this consumer never feeds anything that must be exact (the audit trail does).
"""

import logging
import time
from collections.abc import Callable
from typing import Any

from confluent_kafka import KafkaError, KafkaException, TopicPartition

from qp_common.envelope import EnvelopeError, parse_envelope
from qp_common.lifecycle import GracefulStop
from qp_common.logs import fields

from .aggregate import SKIPPED, Aggregator
from .config import Config

log = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        cfg: Config,
        consumer: Any,
        aggregator: Aggregator,
        stop: GracefulStop,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._cfg = cfg
        self._consumer = consumer
        self._agg = aggregator
        self._stop = stop
        self._sleep = sleep
        self._uncommitted: dict[tuple[str, int], int] = {}

    def run(self) -> None:
        self._consumer.subscribe(
            list(self._cfg.topics), on_assign=self._on_assign, on_revoke=self._on_revoke
        )
        log.info("data-quality started", extra=fields(topics=list(self._cfg.topics)))
        while not self._stop.requested:
            msgs = self._consumer.consume(
                num_messages=self._cfg.batch_size, timeout=self._cfg.batch_timeout_s
            )
            if msgs:
                self.process_batch(msgs)
        log.info("data-quality stopping")

    def process_batch(self, msgs: list[Any]) -> None:
        offsets: dict[tuple[str, int], int] = {}
        for msg in msgs:
            err = msg.error()
            if err is not None:
                self._on_consume_error(err)
                continue
            key = (msg.topic(), msg.partition())
            offsets[key] = max(offsets.get(key, -1), msg.offset())
            try:
                env = parse_envelope(msg.value())
            except EnvelopeError:
                # The audit sink dead-letters invalid envelopes; this consumer only counts them.
                SKIPPED.labels("invalid_envelope").inc()
                continue
            self._agg.observe(msg.topic(), env)
            if self._cfg.log_events:
                log.info("processed", extra=fields(event_id=env.event_id))

        if self._cfg.debug_delay_s:
            self._sleep(self._cfg.debug_delay_s)
        self._commit(offsets)

    def _commit(self, offsets: dict[tuple[str, int], int]) -> None:
        for key, offset in offsets.items():
            self._uncommitted[key] = max(self._uncommitted.get(key, -1), offset)
        self._commit_uncommitted()

    def _commit_uncommitted(self) -> None:
        if not self._uncommitted:
            return
        to_commit = [
            TopicPartition(topic, part, offset + 1)
            for (topic, part), offset in self._uncommitted.items()
        ]
        try:
            self._consumer.commit(offsets=to_commit, asynchronous=False)
        except KafkaException as exc:
            log.warning(
                "offset commit failed; batch will be re-read",
                extra=fields(error=str(exc)),
            )
            return
        self._uncommitted.clear()

    def _on_consume_error(self, err: KafkaError) -> None:
        if err.fatal():
            raise KafkaException(err)
        log.warning("consumer error", extra=fields(error=str(err), code=err.code()))

    def _on_assign(self, _consumer: Any, partitions: list[Any]) -> None:
        log.info("partitions assigned", extra=fields(count=len(partitions)))

    def _on_revoke(self, _consumer: Any, partitions: list[Any]) -> None:
        self._commit_uncommitted()  # hand partitions back with nothing pending
        log.info("partitions revoked", extra=fields(count=len(partitions)))
