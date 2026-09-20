"""Envelope validation (docs/contract.md §1).

Only the envelope is checked. The payload is the producer's; it is stored as is.
"""

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import resources
from typing import Any

from jsonschema import Draft202012Validator


class EnvelopeError(ValueError):
    """The message is not a valid event envelope: a permanent, per-message error."""


@dataclass(frozen=True)
class Envelope:
    event_id: str
    type: str
    version: int
    occurred_at: datetime
    request_id: str | None
    trace_id: str | None
    username: str | None
    producer: dict[str, Any]
    payload: dict[str, Any]


def _load_validator() -> Draft202012Validator:
    text = (resources.files(__package__) / "envelope.schema.json").read_text("utf-8")
    schema = json.loads(text)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


_VALIDATOR = _load_validator()


def _decode_json(raw: bytes | None) -> Any:
    if not raw:
        raise EnvelopeError("empty message")
    try:
        return json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EnvelopeError(f"not valid UTF-8 JSON: {exc}") from exc


def parse_envelope(raw: bytes | None, decoder: Any = None) -> Envelope:
    """Decode and validate one Kafka message value; raise EnvelopeError if invalid.

    `decoder` (qp_common.wire.Decoder) adds Avro support; without one the value is JSON.
    """
    doc = decoder.decode(raw) if decoder is not None else _decode_json(raw)

    errors = sorted(_VALIDATOR.iter_errors(doc), key=lambda e: list(e.absolute_path))
    if errors:
        first = errors[0]
        where = ".".join(str(p) for p in first.absolute_path) or "<root>"
        extra = f" (+{len(errors) - 1} more)" if len(errors) > 1 else ""
        raise EnvelopeError(f"envelope violation at {where}: {first.message}{extra}")

    try:
        occurred_at = datetime.fromisoformat(doc["occurred_at"])
    except ValueError as exc:
        raise EnvelopeError(f"occurred_at is not a real date: {exc}") from exc
    if occurred_at.tzinfo is None:
        occurred_at = occurred_at.replace(tzinfo=timezone.utc)

    return Envelope(
        event_id=doc["event_id"],
        type=doc["type"],
        version=doc["version"],
        occurred_at=occurred_at,
        request_id=doc["request_id"],
        trace_id=doc["trace_id"],
        username=doc["username"],
        producer=doc["producer"],
        payload=doc["payload"],
    )
