import pytest

from dq.aggregate import FALLBACK_TOPIC, Aggregator
from dq.config import Config
from dq.worker import Worker
from qp_common.lifecycle import GracefulStop

from dq_fakes import Clock, FakeConsumer, FakeMsg, raw
from prometheus_client import REGISTRY


@pytest.fixture
def parts():
    consumer = FakeConsumer()
    events: list = []
    worker = Worker(
        Config(),
        consumer,
        Aggregator(900, Clock()),
        GracefulStop(),
        sleep=events.append,
    )
    return worker, consumer


def msg(offset, value=None, part=0):
    return FakeMsg(
        FALLBACK_TOPIC,
        part,
        offset,
        value or raw("data.fallback", {"kind": "default_rate"}, offset),
    )


def counted() -> float:
    return REGISTRY.get_sample_value("qp_dq_fallbacks_total", {"kind": "default_rate"})


def test_batch_is_processed_then_committed_at_last_offset_plus_one(parts):
    worker, consumer = parts
    before = counted()
    worker.process_batch([msg(3), msg(4), msg(5)])
    assert counted() == before + 3
    assert consumer.commits == [[(FALLBACK_TOPIC, 0, 6)]]


def test_invalid_message_is_skipped_but_its_offset_is_committed(parts):
    worker, consumer = parts
    s0 = REGISTRY.get_sample_value(
        "qp_dq_skipped_total", {"reason": "invalid_envelope"}
    )
    before = counted()
    worker.process_batch([msg(1), msg(2, value=b"{broken"), msg(3)])
    assert counted() == before + 2
    assert (
        REGISTRY.get_sample_value("qp_dq_skipped_total", {"reason": "invalid_envelope"})
        == s0 + 1
    )
    assert consumer.commits == [
        [(FALLBACK_TOPIC, 0, 4)]
    ]  # never loops on a bad message


def test_failed_commit_is_retried_with_the_next_batch_and_on_revoke(parts):
    worker, consumer = parts
    consumer.fail_commits = 1
    worker.process_batch([msg(1)])
    assert consumer.commits == []
    worker.process_batch([msg(2)])
    assert consumer.commits == [[(FALLBACK_TOPIC, 0, 3)]]

    consumer.fail_commits = 1
    worker.process_batch([msg(3)])
    worker._on_revoke(consumer, [])
    assert consumer.commits[-1] == [(FALLBACK_TOPIC, 0, 4)]


def test_config_reads_the_environment():
    cfg = Config.from_env(
        {
            "DQ_TOPICS": "a,b",
            "DQ_BATCH_SIZE": "10",
            "DQ_DEBUG_DELAY_MS": "100",
            "DQ_LOG_EVENTS": "1",
        }
    )
    assert cfg.topics == ("a", "b")
    assert (cfg.batch_size, cfg.debug_delay_s, cfg.log_events) == (10, 0.1, True)
    assert Config().group_id == "data-quality"  # distinct from the audit sink's group
