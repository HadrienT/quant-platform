"""Fakes standing in for Kafka and Postgres, so the worker's rules run without Docker."""

import json
from dataclasses import dataclass, field

import pytest
from confluent_kafka import KafkaException

from audit_sink.config import Config
from audit_sink.store import InsertResult, Record, TransientError
from qp_common.lifecycle import GracefulStop

EVENT = {
    "event_id": "0198f2c4-7b1e-7a3d-9f10-2a6c5e8d4b71",
    "type": "data.fallback",
    "version": 1,
    "occurred_at": "2026-09-19T17:03:11.482Z",
    "request_id": None,
    "trace_id": None,
    "username": None,
    "producer": {"service": "test"},
    "payload": {"kind": "default_rate"},
}


def event_bytes(n: int) -> bytes:
    return json.dumps(
        {**EVENT, "event_id": f"0198f2c4-7b1e-7a3d-9f10-{n:012x}"}
    ).encode()


@dataclass
class FakeMsg:
    _topic: str
    _partition: int
    _offset: int
    _value: bytes | None
    _key: bytes | None = b"k"

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def key(self):
        return self._key

    def error(self):
        return None


def msg(offset: int, value: bytes | None = None, topic="qm.audit.auth.v1", part=0):
    return FakeMsg(topic, part, offset, event_bytes(offset) if value is None else value)


@dataclass
class Journal:
    """One ordered log of everything with a side effect, to assert ORDER."""

    calls: list = field(default_factory=list)


class FakeConsumer:
    def __init__(self, journal: Journal):
        self.journal = journal
        self.fail_commits = 0

    def commit(self, offsets, asynchronous):
        assert asynchronous is False
        if self.fail_commits:
            self.fail_commits -= 1
            raise KafkaException("commit failed")
        self.journal.calls.append(
            ("commit", sorted((o.topic, o.partition, o.offset) for o in offsets))
        )


class FakeProducer:
    def __init__(self, journal: Journal):
        self.journal = journal
        self.flush_remaining = []  # successive return values of flush()
        self.sent = []

    def produce(self, topic, key, value, headers, on_delivery):
        self.sent.append((topic, key, value, dict(headers)))
        self.journal.calls.append(("dlq", topic))
        on_delivery(None, None)

    def flush(self, timeout):
        return self.flush_remaining.pop(0) if self.flush_remaining else 0


class FakeStore:
    def __init__(self, journal: Journal):
        self.journal = journal
        self.failures = []  # exceptions raised by successive insert() calls
        self.inserted_batches = []
        self.reject_ids: set[str] = set()

    def insert(self, records):
        if self.failures:
            raise self.failures.pop(0)
        good = [r for r in records if r.envelope.event_id not in self.reject_ids]
        bad = [
            (r, "DataError: unsupported Unicode escape")
            for r in records
            if r.envelope.event_id in self.reject_ids
        ]
        self.inserted_batches.append([r.offset for r in good])
        self.journal.calls.append(("insert", [r.offset for r in good]))
        return InsertResult(len(good), 0, bad)


@pytest.fixture
def journal():
    return Journal()


@pytest.fixture
def parts(journal):
    stop = GracefulStop()
    sleeps: list[float] = []

    def fast_sleep(seconds: float) -> bool:
        """Record the backoff instead of waiting; honour a stop request like the real one."""
        sleeps.append(seconds)
        return stop.requested

    stop.sleep = fast_sleep
    consumer, producer, store = (
        FakeConsumer(journal),
        FakeProducer(journal),
        FakeStore(journal),
    )
    from audit_sink.worker import Worker

    worker = Worker(Config(), consumer, producer, store, stop, sleep=sleeps.append)
    return worker, consumer, producer, store, stop, sleeps


TRANSIENT = TransientError("connection refused")
