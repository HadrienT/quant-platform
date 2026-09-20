"""The consume → validate → insert → commit loop.

Per batch, in this order:
  1. classify each message: valid envelope, or rejected (permanent, per message);
  2. one database transaction for the valid ones (idempotent insert);
  3. rejected messages go to qm.dlq.v1 and the producer flush is awaited;
  4. ONLY THEN commit the offsets to Kafka.

A crash anywhere before step 4 means the batch is re-read: the insert is idempotent
so nothing is duplicated (at-least-once + idempotent sink = effectively once,
ADR-005). Transient failures (database down, broker down) retry with backoff and
never commit and never dead-letter.
"""

import logging
import time
from collections.abc import Callable
from typing import Any, TypeVar

from confluent_kafka import KafkaError, KafkaException, TopicPartition
from prometheus_client import Counter, Gauge, Histogram

from qp_common.dlq import DLQ_TOPIC, Rejected, dlq_headers
from qp_common.envelope import EnvelopeError, parse_envelope
from qp_common.errors import Shutdown, TransientError
from qp_common.lifecycle import GracefulStop
from qp_common.logs import fields
from qp_common.retry import retry_transient
from qp_common.wire import Decoder

from .config import Config
from .store import InsertResult, Record, Store

log = logging.getLogger(__name__)

T = TypeVar("T")

EVENTS = Counter("qp_sink_events_total", "Messages handled, by outcome", ["result"])
BATCHES = Counter("qp_sink_batches_total", "Batches committed")
TRANSIENT = Counter(
    "qp_sink_transient_errors_total", "Transient failures retried", ["component"]
)
LAST_SUCCESS = Gauge(
    "qp_sink_last_success_timestamp_seconds", "Unix time of the last committed batch"
)
BATCH_SECONDS = Histogram("qp_sink_batch_seconds", "Time to process one batch")


class Worker:
    def __init__(
        self,
        cfg: Config,
        consumer: Any,
        producer: Any,
        store: Store,
        stop: GracefulStop,
        sleep: Callable[[float], None] = time.sleep,
        decoder: Decoder | None = None,
    ) -> None:
        self._cfg = cfg
        self._consumer = consumer
        self._producer = producer
        self._store = store
        self._stop = stop
        self._sleep = sleep
        self._decoder = decoder or Decoder(None)  # JSON only unless a registry is given
        # Offsets processed but whose Kafka commit failed; retried on the next
        # commit and before partitions are handed back on a rebalance.
        self._uncommitted: dict[tuple[str, int], int] = {}

    # ── main loop ────────────────────────────────────────────────────────────
    def run(self) -> None:
        self._consumer.subscribe(
            [self._cfg.topics_regex],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        log.info("sink started", extra=fields(topics=self._cfg.topics_regex))
        try:
            while not self._stop.requested:
                msgs = self._consumer.consume(
                    num_messages=self._cfg.batch_size,
                    timeout=self._cfg.batch_timeout_s,
                )
                if msgs:
                    self.process_batch(msgs)
        except Shutdown:
            log.info("stop requested during a retry: leaving without committing")
        log.info("sink stopping")

    # ── one batch ────────────────────────────────────────────────────────────
    def process_batch(self, msgs: list[Any]) -> None:
        started = time.monotonic()
        records: list[Record] = []
        rejected: list[Rejected] = []
        offsets: dict[tuple[str, int], int] = {}

        for msg in msgs:
            err = msg.error()
            if err is not None:
                self._on_consume_error(err)
                continue
            key = (msg.topic(), msg.partition())
            offsets[key] = max(offsets.get(key, -1), msg.offset())
            try:
                # A registry outage is transient: retry, never dead-letter the message.
                env = self._retry(
                    "registry", lambda m=msg: parse_envelope(m.value(), self._decoder)
                )
            except EnvelopeError as exc:
                rejected.append(self._reject(msg, str(exc)))
            else:
                records.append(Record(env, msg.topic(), msg.partition(), msg.offset()))

        result: InsertResult = self._retry("db", lambda: self._store.insert(records))
        for rec, reason in result.rejected:
            rejected.append(
                Rejected(rec.topic, rec.partition, rec.offset, None, None, reason)
            )
        rejected = self._with_original_values(rejected, msgs)
        if rejected:
            self._retry("dlq", lambda: self._publish_dlq(rejected))

        EVENTS.labels("inserted").inc(result.inserted)
        EVENTS.labels("duplicate").inc(result.duplicates)
        EVENTS.labels("rejected").inc(len(rejected))

        if self._cfg.debug_delay_s:
            self._sleep(self._cfg.debug_delay_s)

        self._commit(offsets)
        BATCHES.inc()
        LAST_SUCCESS.set_to_current_time()
        BATCH_SECONDS.observe(time.monotonic() - started)
        log.info(
            "batch committed",
            extra=fields(
                messages=len(msgs),
                inserted=result.inserted,
                duplicates=result.duplicates,
                rejected=len(rejected),
            ),
        )

    # ── helpers ──────────────────────────────────────────────────────────────
    @staticmethod
    def _reject(msg: Any, error: str) -> Rejected:
        return Rejected(
            msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.value(), error
        )

    @staticmethod
    def _with_original_values(
        rejected: list[Rejected], msgs: list[Any]
    ) -> list[Rejected]:
        """Rows the database refused only carry a reason; re-attach the raw message."""
        by_position = {
            (m.topic(), m.partition(), m.offset()): m for m in msgs if m.error() is None
        }
        out = []
        for item in rejected:
            if item.value is None:
                msg = by_position[(item.topic, item.partition, item.offset)]
                item = Rejected(
                    item.topic,
                    item.partition,
                    item.offset,
                    msg.key(),
                    msg.value(),
                    item.error,
                )
            out.append(item)
        return out

    def _retry(self, component: str, action: Callable[[], T]) -> T:
        def on_error(exc: TransientError, delay: float) -> None:
            TRANSIENT.labels(component).inc()
            log.error(
                "transient %s failure, retrying without commit",
                component,
                extra=fields(error=str(exc), retry_in_s=round(delay, 1)),
            )

        return retry_transient(action, self._stop, on_error)

    def _publish_dlq(self, rejected: list[Rejected]) -> None:
        failures: list[str] = []

        def delivered(err: Any, _msg: Any) -> None:
            if err is not None:
                failures.append(str(err))

        try:
            for item in rejected:
                self._producer.produce(
                    DLQ_TOPIC,
                    key=item.topic.encode(),
                    value=item.value if item.value is not None else b"",
                    headers=dlq_headers(item, self._cfg.group_id),
                    on_delivery=delivered,
                )
            remaining = self._producer.flush(30)
        except (KafkaException, BufferError) as exc:
            raise TransientError(f"dlq produce failed: {exc}") from exc
        if remaining or failures:
            raise TransientError(
                f"dlq delivery incomplete: {remaining} pending, errors={failures[:1]}"
            )
        for item in rejected:
            log.warning(
                "message sent to the DLQ",
                extra=fields(
                    source_topic=item.topic,
                    source_partition=item.partition,
                    source_offset=item.offset,
                    error=item.error,
                ),
            )

    def _commit(self, offsets: dict[tuple[str, int], int]) -> None:
        for key, offset in offsets.items():
            self._uncommitted[key] = max(self._uncommitted.get(key, -1), offset)
        self._commit_uncommitted()

    def _commit_uncommitted(self) -> None:
        if not self._uncommitted:
            return
        to_commit = [
            TopicPartition(topic, part, offset + 1)  # committed = NEXT offset to read
            for (topic, part), offset in self._uncommitted.items()
        ]
        try:
            self._consumer.commit(offsets=to_commit, asynchronous=False)
        except KafkaException as exc:
            # Not fatal: the batch will be re-read, and the insert is idempotent.
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

    # ── rebalance callbacks ──────────────────────────────────────────────────
    def _on_assign(self, _consumer: Any, partitions: list[Any]) -> None:
        log.info(
            "partitions assigned",
            extra=fields(partitions=[f"{p.topic}[{p.partition}]" for p in partitions]),
        )

    def _on_revoke(self, _consumer: Any, partitions: list[Any]) -> None:
        # Commit anything still pending BEFORE handing the partitions back.
        self._commit_uncommitted()
        log.info(
            "partitions revoked",
            extra=fields(partitions=[f"{p.topic}[{p.partition}]" for p in partitions]),
        )
