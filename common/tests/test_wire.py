import io
import json
import urllib.error
from datetime import datetime, timezone
from decimal import Decimal

import fastavro
import pytest

from qp_common import wire
from qp_common.envelope import EnvelopeError, parse_envelope
from qp_common.errors import TransientError
from qp_common.wire import Decoder, RegistryClient

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
                    "name": "Producer",
                    "fields": [{"name": "service", "type": "string"}],
                },
            },
            {
                "name": "payload",
                "type": {
                    "type": "record",
                    "name": "Payload",
                    "fields": [
                        {"name": "product", "type": "string"},
                        {"name": "npv", "type": "double"},
                    ],
                },
            },
        ],
    }
)

EVENT = {
    "event_id": "0198f2c4-7b1e-7a3d-9f10-2a6c5e8d4b71",
    "type": "pricing.valuation",
    "version": 1,
    "occurred_at": "2026-09-19T17:03:11.482Z",
    "request_id": None,
    "trace_id": None,
    "username": "hadrien",
    "producer": {"service": "quant-modeling-api"},
    "payload": {"product": "autocall", "npv": 0.9713},
}


def framed(schema_id: int, record: dict, schema=SCHEMA) -> bytes:
    body = io.BytesIO()
    fastavro.schemaless_writer(body, schema, record)
    return b"\x00" + schema_id.to_bytes(4, "big") + body.getvalue()


class FakeRegistry:
    def __init__(self, schemas=None, failures=0):
        self.schemas = schemas if schemas is not None else {7: SCHEMA}
        self.failures = failures
        self.calls = 0

    def schema_by_id(self, schema_id):
        self.calls += 1
        if self.failures:
            self.failures -= 1
            raise TransientError("registry down")
        if schema_id not in self.schemas:
            raise EnvelopeError(f"unknown schema id {schema_id} in the registry")
        return self.schemas[schema_id]


def test_json_messages_still_decode_without_any_registry():
    assert Decoder(None).decode(json.dumps(EVENT).encode()) == EVENT


def test_avro_message_decodes_to_the_same_dict_as_its_json_twin():
    assert Decoder(FakeRegistry()).decode(framed(7, EVENT)) == EVENT


def test_avro_event_passes_envelope_validation_and_keeps_the_payload():
    env = parse_envelope(framed(7, EVENT), Decoder(FakeRegistry()))
    assert env.username == "hadrien" and env.payload == {
        "product": "autocall",
        "npv": 0.9713,
    }


def test_unknown_schema_id_is_permanent():
    with pytest.raises(EnvelopeError, match="unknown schema id 99"):
        Decoder(FakeRegistry()).decode(framed(99, EVENT))


def test_registry_outage_is_transient_not_a_poison_message():
    with pytest.raises(TransientError):
        Decoder(FakeRegistry(failures=1)).decode(framed(7, EVENT))


def test_avro_without_a_configured_registry_stalls_instead_of_dead_lettering():
    with pytest.raises(TransientError, match="SCHEMA_REGISTRY_URL"):
        Decoder(None).decode(framed(7, EVENT))


def test_body_that_does_not_match_its_schema_is_permanent():
    with pytest.raises(EnvelopeError, match="does not match schema 7"):
        Decoder(FakeRegistry()).decode(b"\x00\x00\x00\x00\x07" + b"\xff\xff\xff")


def test_truncated_avro_message_is_permanent():
    with pytest.raises(EnvelopeError, match="truncated"):
        Decoder(FakeRegistry()).decode(b"\x00\x00")


def test_logical_types_are_normalised_to_plain_json():
    schema = fastavro.parse_schema(
        {
            "type": "record",
            "name": "T",
            "fields": [
                {
                    "name": "when",
                    "type": {"type": "long", "logicalType": "timestamp-millis"},
                },
                {"name": "raw", "type": "bytes"},
                {
                    "name": "amount",
                    "type": {
                        "type": "bytes",
                        "logicalType": "decimal",
                        "precision": 10,
                        "scale": 2,
                    },
                },
            ],
        }
    )
    record = {
        "when": datetime(2026, 9, 19, tzinfo=timezone.utc),
        "raw": b"\x01\x02",
        "amount": Decimal("12.34"),
    }
    out = Decoder(FakeRegistry({1: schema})).decode(framed(1, record, schema))
    assert out == {
        "when": "2026-09-19T00:00:00+00:00",
        "raw": "AQI=",
        "amount": "12.34",
    }
    json.dumps(out)  # would raise if anything were left un-serialisable


# ── RegistryClient over HTTP (urlopen stubbed) ───────────────────────────────
class Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_registry_client_caches_schemas_forever(monkeypatch):
    calls = []

    def urlopen(url, timeout):
        calls.append(url)
        return Resp(json.dumps({"schema": json.dumps(SCHEMA)}).encode())

    monkeypatch.setattr(wire.urllib.request, "urlopen", urlopen)
    client = RegistryClient("http://registry/apis/ccompat/v7/")
    client.schema_by_id(3)
    client.schema_by_id(3)
    assert calls == ["http://registry/apis/ccompat/v7/schemas/ids/3"]


@pytest.mark.parametrize(
    "error, expected",
    [
        (urllib.error.HTTPError("u", 404, "nf", {}, None), EnvelopeError),
        (urllib.error.HTTPError("u", 500, "boom", {}, None), TransientError),
        (urllib.error.HTTPError("u", 503, "down", {}, None), TransientError),
        (urllib.error.URLError("connection refused"), TransientError),
        (TimeoutError("slow"), TransientError),
    ],
)
def test_registry_client_maps_http_failures(monkeypatch, error, expected):
    def urlopen(url, timeout):
        raise error

    monkeypatch.setattr(wire.urllib.request, "urlopen", urlopen)
    with pytest.raises(expected):
        RegistryClient("http://registry").schema_by_id(1)
