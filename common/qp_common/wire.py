"""Message wire formats: plain JSON, and Avro in the Confluent framing.

Confluent framing (what any Kafka Avro serializer produces):
    byte 0        magic byte 0x00
    bytes 1-4     schema id in the registry, big-endian
    bytes 5..     the Avro binary body, written with THAT schema

The sink stays agnostic: an Avro message is decoded to a dict with its writer
schema and then goes through the same envelope validation as a JSON one, and the
payload is stored as jsonb either way (WP 05). Both formats coexist on a topic,
which is what lets a producer migrate without a flag day.

Failure taxonomy — the same rule as everywhere in this platform:
  - a message that can never be read (unknown schema id, body not matching its
    schema, invalid JSON)  → EnvelopeError  → permanent → DLQ
  - the registry cannot be reached / answers 5xx → TransientError → retry, no DLQ
    (dead-lettering during a registry outage would empty the trail into the bin)
"""

import base64
import io
import json
import urllib.error
import urllib.request
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Protocol

import fastavro

from .envelope import EnvelopeError
from .errors import TransientError

MAGIC_BYTE = 0


class SchemaSource(Protocol):
    def schema_by_id(self, schema_id: int) -> Any: ...


class RegistryClient:
    """Minimal reader for the Confluent-compatible API (`/schemas/ids/{id}`).

    A schema id is immutable once assigned, so every answer is cached forever.
    """

    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        self._base = base_url.rstrip("/")
        self._timeout = timeout
        self._cache: dict[int, Any] = {}

    def schema_by_id(self, schema_id: int) -> Any:
        if schema_id in self._cache:
            return self._cache[schema_id]
        url = f"{self._base}/schemas/ids/{schema_id}"
        try:
            with urllib.request.urlopen(url, timeout=self._timeout) as resp:
                text = json.load(resp)["schema"]
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise EnvelopeError(
                    f"unknown schema id {schema_id} in the registry"
                ) from exc
            raise TransientError(
                f"registry answered HTTP {exc.code} for schema {schema_id}"
            ) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise TransientError(
                f"registry unreachable at {self._base}: {exc}"
            ) from exc
        parsed = fastavro.parse_schema(json.loads(text))
        self._cache[schema_id] = parsed
        return parsed


def _jsonable(value: Any) -> Any:
    """Avro logical types (dates, decimals, bytes) that json.dumps cannot write."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(value).decode()
    raise TypeError(f"not JSON serialisable: {type(value).__name__}")


class Decoder:
    """Turns a Kafka message value into a dict, whichever format it is in."""

    def __init__(self, registry: SchemaSource | None = None) -> None:
        self._registry = registry

    def decode(self, raw: bytes | None) -> Any:
        if not raw:
            raise EnvelopeError("empty message")
        if raw[0] == MAGIC_BYTE:
            return self._decode_avro(raw)
        try:
            return json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EnvelopeError(f"not valid UTF-8 JSON: {exc}") from exc

    def _decode_avro(self, raw: bytes) -> Any:
        if len(raw) < 5:
            raise EnvelopeError("truncated Avro message (no schema id)")
        if self._registry is None:
            # A configuration error, not a bad message: stall loudly rather than DLQ everything.
            raise TransientError(
                "received an Avro message but SCHEMA_REGISTRY_URL is not set"
            )
        schema_id = int.from_bytes(raw[1:5], "big")
        schema = self._registry.schema_by_id(schema_id)
        try:
            record = fastavro.schemaless_reader(io.BytesIO(raw[5:]), schema)
            # Normalise to plain JSON types now, so nothing downstream can trip on them.
            return json.loads(json.dumps(record, default=_jsonable))
        except (
            Exception
        ) as exc:  # noqa: BLE001 — fastavro raises many types for a corrupt body
            raise EnvelopeError(
                f"Avro body does not match schema {schema_id}: {exc}"
            ) from exc
