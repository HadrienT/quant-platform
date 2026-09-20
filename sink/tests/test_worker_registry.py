"""The sink with Avro messages: a registry outage retries, an unknown schema dead-letters."""

import io

import fastavro
import pytest

from audit_sink.config import Config
from audit_sink.worker import Worker
from qp_common.envelope import EnvelopeError
from qp_common.errors import TransientError
from qp_common.lifecycle import GracefulStop
from qp_common.wire import Decoder

from conftest import FakeConsumer, FakeMsg, FakeProducer, FakeStore, Journal

SCHEMA = fastavro.parse_schema(
    {
        "type": "record",
        "name": "Event",
        "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "type", "type": "string"},
            {"name": "version", "type": "int"},
            {"name": "occurred_at", "type": "string"},
            {"name": "request_id", "type": ["null", "string"], "default": None},
            {"name": "trace_id", "type": ["null", "string"], "default": None},
            {"name": "username", "type": ["null", "string"], "default": None},
            {
                "name": "producer",
                "type": {
                    "type": "record",
                    "name": "P",
                    "fields": [{"name": "service", "type": "string"}],
                },
            },
            {
                "name": "payload",
                "type": {
                    "type": "record",
                    "name": "Pl",
                    "fields": [{"name": "kind", "type": "string"}],
                },
            },
        ],
    }
)


def avro_bytes(schema_id: int, n: int) -> bytes:
    body = io.BytesIO()
    fastavro.schemaless_writer(
        body,
        SCHEMA,
        {
            "event_id": f"0198f2c4-7b1e-7a3d-9f10-{n:012x}",
            "type": "data.fallback",
            "version": 1,
            "occurred_at": "2026-09-19T17:03:11.482Z",
            "request_id": None,
            "trace_id": None,
            "username": None,
            "producer": {"service": "t"},
            "payload": {"kind": "default_rate"},
        },
    )
    return b"\x00" + schema_id.to_bytes(4, "big") + body.getvalue()


class Registry:
    def __init__(self, fail_first=0):
        self.fail_first = fail_first

    def schema_by_id(self, schema_id):
        if self.fail_first:
            self.fail_first -= 1
            raise TransientError("registry down")
        if schema_id != 1:
            raise EnvelopeError(f"unknown schema id {schema_id} in the registry")
        return SCHEMA


@pytest.fixture
def parts():
    journal = Journal()
    stop = GracefulStop()
    waits: list[float] = []
    stop.sleep = lambda s: waits.append(s) or stop.requested
    consumer, producer, store = (
        FakeConsumer(journal),
        FakeProducer(journal),
        FakeStore(journal),
    )

    def make(registry):
        return Worker(
            Config(), consumer, producer, store, stop, decoder=Decoder(registry)
        )

    return make, journal, producer, store, waits


def msg(offset, value):
    return FakeMsg("qm.audit.valuation.v1", 0, offset, value)


def test_avro_messages_are_inserted_like_json_ones(parts):
    make, journal, producer, store, _ = parts
    make(Registry()).process_batch([msg(1, avro_bytes(1, 1)), msg(2, avro_bytes(1, 2))])
    assert store.inserted_batches == [[1, 2]] and producer.sent == []


def test_registry_outage_retries_without_dead_lettering_or_committing_early(parts):
    make, journal, producer, store, waits = parts
    make(Registry(fail_first=2)).process_batch([msg(1, avro_bytes(1, 1))])
    assert len(waits) == 2  # backed off twice
    assert producer.sent == []  # NOT dead-lettered during the outage
    assert store.inserted_batches == [[1]]
    assert journal.calls[-1][0] == "commit"


def test_unknown_schema_id_goes_to_the_dlq(parts):
    make, journal, producer, store, _ = parts
    make(Registry()).process_batch(
        [msg(1, avro_bytes(1, 1)), msg(2, avro_bytes(42, 2))]
    )
    assert store.inserted_batches == [[1]]
    [(topic, _, _, headers)] = producer.sent
    assert topic == "qm.dlq.v1" and b"unknown schema id 42" in headers["dlq.error"]


def test_json_and_avro_coexist_in_one_batch(parts):
    make, journal, producer, store, _ = parts
    from conftest import event_bytes

    make(Registry()).process_batch([msg(1, event_bytes(1)), msg(2, avro_bytes(1, 2))])
    assert store.inserted_batches == [[1, 2]]
