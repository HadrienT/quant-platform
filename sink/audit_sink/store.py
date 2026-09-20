"""The Postgres side of the sink.

Two kinds of failure, treated very differently:
  - a row the database REFUSES for what it contains (data / integrity errors):
    permanent and per-message → the caller sends that message to the DLQ;
  - anything else (connection lost, database restarting, missing privilege…):
    transient from the sink's point of view → TransientError, the caller backs off
    and retries WITHOUT committing and WITHOUT touching the DLQ. Sending events to
    the DLQ during a database outage would empty the audit trail into the bin.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass

import psycopg
from psycopg import errors as pg_errors
from psycopg.types.json import Jsonb

from qp_common.envelope import Envelope
from qp_common.errors import TransientError

log = logging.getLogger(__name__)

# Target-less ON CONFLICT: the primary key (event_id, occurred_at) is the only
# unique constraint, and the target form would require SELECT on top of INSERT
# (ADR-011). A re-delivered event is skipped: that is the idempotency.
INSERT_SQL = """
INSERT INTO audit.events (
    event_id, type, version, occurred_at, request_id, trace_id, username,
    producer, payload, src_topic, src_part, src_offset
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
"""


@dataclass(frozen=True)
class Record:
    envelope: Envelope
    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class InsertResult:
    inserted: int
    duplicates: int
    # Records the database refused for their content, with the reason.
    rejected: list[tuple[Record, str]]


def _params(rec: Record) -> tuple:
    e = rec.envelope
    return (
        e.event_id,
        e.type,
        e.version,
        e.occurred_at,
        e.request_id,
        e.trace_id,
        e.username,
        Jsonb(e.producer),
        Jsonb(e.payload),
        rec.topic,
        rec.partition,
        rec.offset,
    )


class Store:
    def __init__(self, conninfo: dict[str, str]) -> None:
        self._conninfo = conninfo
        self._conn: psycopg.Connection | None = None

    def _connection(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            # autocommit: every write goes through an explicit conn.transaction().
            self._conn = psycopg.connect(**self._conninfo, autocommit=True)
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None

    def insert(self, records: Sequence[Record]) -> InsertResult:
        """Insert a batch in ONE transaction; on a content error, isolate the bad rows."""
        if not records:
            return InsertResult(0, 0, [])
        try:
            return self._insert_batch(records)
        except (pg_errors.DataError, pg_errors.IntegrityError):
            log.warning("batch refused for its content; isolating the bad rows")
            return self._insert_isolating(records)
        except psycopg.Error as exc:
            self.close()  # never reuse a connection in an unknown state
            raise TransientError(f"{type(exc).__name__}: {exc}") from exc

    def _insert_batch(self, records: Sequence[Record]) -> InsertResult:
        conn = self._connection()
        with conn.transaction():
            with conn.cursor() as cur:
                cur.executemany(INSERT_SQL, [_params(r) for r in records])
                inserted = cur.rowcount
        return InsertResult(inserted, len(records) - inserted, [])

    def _insert_isolating(self, records: Sequence[Record]) -> InsertResult:
        conn = self._connection()
        inserted = 0
        rejected: list[tuple[Record, str]] = []
        try:
            for rec in records:
                try:
                    with conn.transaction():
                        with conn.cursor() as cur:
                            cur.execute(INSERT_SQL, _params(rec))
                            inserted += cur.rowcount
                except (pg_errors.DataError, pg_errors.IntegrityError) as exc:
                    rejected.append((rec, f"{type(exc).__name__}: {exc}"))
        except psycopg.Error as exc:
            self.close()
            raise TransientError(f"{type(exc).__name__}: {exc}") from exc
        return InsertResult(inserted, len(records) - inserted - len(rejected), rejected)
