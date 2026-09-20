"""Envelope and Kafka fakes for the data-quality tests (no conftest: the sink has one)."""

import json
from dataclasses import dataclass

from confluent_kafka import KafkaException

from qp_common.envelope import Envelope, parse_envelope


def envelope(type_: str, payload: dict, n: int = 1) -> Envelope:
    return parse_envelope(
        json.dumps(
            {
                "event_id": f"0198f2c4-7b1e-7a3d-9f10-{n:012x}",
                "type": type_,
                "version": 1,
                "occurred_at": "2026-09-19T17:03:11.482Z",
                "request_id": None,
                "trace_id": None,
                "username": None,
                "producer": {"service": "test"},
                "payload": payload,
            }
        ).encode()
    )


def raw(type_: str, payload: dict, n: int = 1) -> bytes:
    e = envelope(type_, payload, n)
    return json.dumps(
        {
            "event_id": e.event_id,
            "type": e.type,
            "version": 1,
            "occurred_at": "2026-09-19T17:03:11.482Z",
            "request_id": None,
            "trace_id": None,
            "username": None,
            "producer": {"service": "test"},
            "payload": payload,
        }
    ).encode()


@dataclass
class FakeMsg:
    _topic: str
    _partition: int
    _offset: int
    _value: bytes

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def error(self):
        return None


class FakeConsumer:
    def __init__(self):
        self.commits = []
        self.fail_commits = 0

    def commit(self, offsets, asynchronous):
        assert asynchronous is False
        if self.fail_commits:
            self.fail_commits -= 1
            raise KafkaException("commit failed")
        self.commits.append(sorted((o.topic, o.partition, o.offset) for o in offsets))


class Clock:
    def __init__(self, now: float = 1_000_000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now
