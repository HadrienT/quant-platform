import json

import pytest

from qp_common.envelope import EnvelopeError, parse_envelope

VALID = {
    "event_id": "0198f2c4-7b1e-7a3d-9f10-2a6c5e8d4b71",
    "type": "pricing.valuation",
    "version": 1,
    "occurred_at": "2026-09-19T17:03:11.482Z",
    "request_id": "req_01J8ZK3V9Q",
    "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
    "username": "hadrien",
    "producer": {
        "service": "quant-modeling-api",
        "git_sha": "6e251ca",
        "lib_build": "a13",
    },
    "payload": {"anything": ["the", "producer", "wants"]},
}


def encode(**overrides) -> bytes:
    doc = {**VALID, **overrides}
    return json.dumps({k: v for k, v in doc.items() if v is not ...}).encode()


def test_valid_envelope_is_parsed():
    env = parse_envelope(encode())
    assert env.event_id == VALID["event_id"]
    assert (
        env.occurred_at.year == 2026
        and env.occurred_at.utcoffset().total_seconds() == 0
    )
    assert env.payload == VALID["payload"]


def test_anonymous_user_and_missing_trace_are_null_not_absent():
    env = parse_envelope(encode(username=None, trace_id=None, request_id=None))
    assert env.username is None and env.trace_id is None


def test_payload_is_not_interpreted():
    env = parse_envelope(encode(payload={"password": "hunter2", "nested": {"a": [1]}}))
    assert env.payload["nested"] == {"a": [1]}


@pytest.mark.parametrize(
    "raw",
    [None, b"", b"not json", b"\xff\xfe", b"[]", b'"a string"', b"null"],
)
def test_unreadable_messages_are_rejected(raw):
    with pytest.raises(EnvelopeError):
        parse_envelope(raw)


@pytest.mark.parametrize(
    "overrides, fragment",
    [
        ({"event_id": ...}, "event_id"),
        ({"event_id": "not-a-uuid"}, "event_id"),
        # a v4 UUID: valid UUID, but the contract says v7
        ({"event_id": "6f1c1f39-27d4-4f6e-9a62-0e1e6c1f4a10"}, "event_id"),
        ({"type": "NoDot"}, "type"),
        ({"version": 0}, "version"),
        ({"version": "1"}, "version"),
        ({"occurred_at": "yesterday"}, "occurred_at"),
        ({"occurred_at": "2026-09-19 17:03:11"}, "occurred_at"),
        ({"trace_id": "short"}, "trace_id"),
        ({"producer": {}}, "producer"),
        ({"payload": []}, "payload"),
        ({"payload": ...}, "payload"),
        ({"surprise": 1}, "surprise"),
    ],
)
def test_envelope_violations_name_the_field(overrides, fragment):
    with pytest.raises(EnvelopeError) as err:
        parse_envelope(encode(**overrides))
    assert fragment in str(err.value)


def test_impossible_calendar_date_is_rejected():
    with pytest.raises(EnvelopeError):
        parse_envelope(encode(occurred_at="2026-13-45T00:00:00Z"))
