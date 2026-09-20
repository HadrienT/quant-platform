"""Dead-letter queue: republish a rejected message as is, with the `dlq.*` headers
of the contract (docs/contract.md §2)."""

from dataclasses import dataclass

DLQ_TOPIC = "qm.dlq.v1"
_MAX_ERROR_LEN = 500


@dataclass(frozen=True)
class Rejected:
    """A message a consumer refuses permanently (never a transient failure)."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    error: str


def dlq_headers(item: Rejected, consumer_group: str) -> list[tuple[str, bytes]]:
    return [
        ("dlq.source.topic", item.topic.encode()),
        ("dlq.source.partition", str(item.partition).encode()),
        ("dlq.source.offset", str(item.offset).encode()),
        ("dlq.error", item.error[:_MAX_ERROR_LEN].encode("utf-8", "replace")),
        ("dlq.consumer.group", consumer_group.encode()),
    ]
