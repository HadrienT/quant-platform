import re

import pytest

from audit_sink.config import DEFAULT_TOPICS_REGEX, Config
from audit_sink.worker import Shutdown

from conftest import TRANSIENT, msg


def test_commit_comes_after_the_database_commit(parts, journal):
    worker, *_ = parts
    worker.process_batch([msg(10), msg(11), msg(12)])
    assert journal.calls == [
        ("insert", [10, 11, 12]),
        # committed offset = LAST processed + 1 (the next one to read)
        ("commit", [("qm.audit.auth.v1", 0, 13)]),
    ]


def test_commit_covers_every_partition_in_the_batch(parts, journal):
    worker, *_ = parts
    worker.process_batch([msg(5, part=0), msg(7, part=1), msg(6, part=0)])
    assert journal.calls[-1] == (
        "commit",
        [("qm.audit.auth.v1", 0, 7), ("qm.audit.auth.v1", 1, 8)],
    )


def test_poison_message_goes_to_the_dlq_and_the_batch_moves_on(parts, journal):
    worker, _, producer, store, *_ = parts
    worker.process_batch([msg(1), msg(2, value=b"{broken"), msg(3)])

    assert store.inserted_batches == [[1, 3]]
    [(topic, key, value, headers)] = producer.sent
    assert topic == "qm.dlq.v1" and key == b"qm.audit.auth.v1" and value == b"{broken"
    assert headers["dlq.source.offset"] == b"2"
    assert headers["dlq.consumer.group"] == b"audit-sink"
    assert b"UTF-8 JSON" in headers["dlq.error"]
    # the poison message's offset is committed too: no infinite loop on it
    assert journal.calls[-1] == ("commit", [("qm.audit.auth.v1", 0, 4)])


def test_db_transient_error_retries_without_dlq_and_without_commit(parts, journal):
    worker, _, producer, store, _, sleeps = parts
    store.failures = [TRANSIENT, TRANSIENT]
    worker.process_batch([msg(1), msg(2)])

    assert producer.sent == []  # never dead-letter during an outage
    assert len(sleeps) == 2  # one backoff per transient failure
    assert journal.calls == [
        ("insert", [1, 2]),
        ("commit", [("qm.audit.auth.v1", 0, 3)]),
    ]


def test_backoff_grows_between_transient_retries(parts):
    worker, _, _, store, _, sleeps = parts
    store.failures = [TRANSIENT, TRANSIENT, TRANSIENT]
    worker.process_batch([msg(1)])
    assert len(sleeps) == 3 and sleeps[0] < sleeps[1] < sleeps[2]


def test_stop_during_a_retry_leaves_without_committing(parts, journal):
    worker, _, _, store, stop, _ = parts
    store.failures = [TRANSIENT]
    stop.request()  # SIGTERM arrives while the database is down
    with pytest.raises(Shutdown):
        worker.process_batch([msg(1)])
    assert journal.calls == []  # nothing committed: the batch is simply re-read later


def test_dlq_failure_retries_only_the_dlq_and_does_not_reinsert(parts, journal):
    worker, _, producer, store, *_ = parts
    producer.flush_remaining = [1, 0]  # first flush: 1 message still pending
    worker.process_batch([msg(1), msg(2, value=b"{broken")])

    assert store.inserted_batches == [[1]]  # inserted exactly once
    assert [c for c in journal.calls if c[0] == "dlq"] == [("dlq", "qm.dlq.v1")] * 2
    assert journal.calls[-1][0] == "commit"


def test_row_refused_by_the_database_is_dead_lettered_with_its_original_bytes(
    parts, journal
):
    worker, _, producer, store, *_ = parts
    bad = msg(2)
    store.reject_ids = {"0198f2c4-7b1e-7a3d-9f10-000000000002"}
    worker.process_batch([msg(1), bad, msg(3)])

    [(_, _, value, headers)] = producer.sent
    assert value == bad.value()
    assert b"DataError" in headers["dlq.error"]
    assert store.inserted_batches == [[1, 3]]


def test_failed_kafka_commit_is_retried_with_the_next_batch(parts, journal):
    worker, consumer, *_ = parts
    consumer.fail_commits = 1
    worker.process_batch([msg(1)])  # commit fails: swallowed, batch will be re-read
    assert [c for c in journal.calls if c[0] == "commit"] == []
    worker.process_batch([msg(2)])
    assert journal.calls[-1] == ("commit", [("qm.audit.auth.v1", 0, 3)])


def test_revoke_commits_what_is_still_pending(parts, journal):
    worker, consumer, *_ = parts
    consumer.fail_commits = 1
    worker.process_batch([msg(1)])
    worker._on_revoke(consumer, [])
    assert journal.calls[-1] == ("commit", [("qm.audit.auth.v1", 0, 2)])


def test_topic_regex_matches_the_contract_and_never_the_dlq():
    rx = re.compile(DEFAULT_TOPICS_REGEX)
    for ok in [
        "qm.audit.valuation.v1",
        "qm.audit.auth.v1",
        "qm.dataquality.fallback.v1",
        "qm.http.access.v1",
        "qm.assistant.chat.v1",
    ]:
        assert rx.match(ok), ok
    for no in [
        "qm.dlq.v1",
        "qm.http.other.v1",
        "other.audit.x.v1",
        "__consumer_offsets",
    ]:
        assert not rx.match(no), no


def test_config_reads_the_environment():
    cfg = Config.from_env(
        {
            "PGPASSWORD": "s",
            "SINK_BATCH_SIZE": "20",
            "SINK_BATCH_TIMEOUT_MS": "250",
            "SINK_DEBUG_DELAY_MS": "100",
        }
    )
    assert (cfg.batch_size, cfg.batch_timeout_s, cfg.debug_delay_s) == (20, 0.25, 0.1)
    assert cfg.db["user"] == "audit_writer" and cfg.db["password"] == "s"


class _GroupLostConsumer:
    """Holds partitions for a few polls, then loses them for good (issue #9)."""

    def __init__(self, clock, polls_with_partitions):
        self.clock = clock
        self.polls_left = polls_with_partitions

    def subscribe(self, topics, on_assign, on_revoke):
        pass

    def consume(self, num_messages, timeout):
        self.clock.now += timeout
        self.polls_left -= 1
        return []

    def assignment(self):
        return ["p"] * 11 if self.polls_left > 0 else []


def test_run_exits_when_the_consumer_never_gets_its_partitions_back(parts):
    _, _, producer, store, stop, _ = parts
    from audit_sink.worker import Worker
    from qp_common.watchdog import AssignmentWatchdog, ConsumerStuck

    class Clock:
        now = 0.0

        def __call__(self):
            return self.now

    clock = Clock()
    consumer = _GroupLostConsumer(clock, polls_with_partitions=50)
    worker = Worker(
        Config(batch_timeout_s=1.0),
        consumer,
        producer,
        store,
        stop,
        watchdog=AssignmentWatchdog(120, clock),
    )
    with pytest.raises(ConsumerStuck):
        worker.run()
    # 50 polls with partitions, then the bound (120 s at 1 s per poll) plus one
    assert 170 <= clock.now <= 172


def test_max_unassigned_is_configurable():
    assert Config.from_env({}).max_unassigned_s == 120
    assert Config.from_env({"SINK_MAX_UNASSIGNED_S": "30"}).max_unassigned_s == 30
