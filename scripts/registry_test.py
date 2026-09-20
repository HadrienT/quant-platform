"""Schema registry acceptance test (blueprint WP 05), against a RUNNING dev stack.

    .venv/bin/python scripts/registry_test.py

  1. smoke: the stock confluent-kafka client works against Apicurio's compat API
  2. an Avro event produced with the stock serializer is read by the sink and
     archived as jsonb, next to JSON events (both formats coexist)
  3. BACKWARD evolution: a field added WITH a default is accepted, and an OLD
     message is still readable by the NEW schema (the default fills in)
  4. a schema that breaks compatibility is REFUSED by the registry, naming the field
  5. size of the same event in JSON and in Avro

The test schemas are registered under the subject `qm.test.registry-value`, never
under a real topic's subject, so they cannot constrain a real producer.
"""

import json
import pathlib
import subprocess
import sys
import time
import uuid

import fastavro
from confluent_kafka import Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from gen_events import uuid7  # noqa: E402

SUBJECT = "qm.test.registry-value"
TOPIC = "qm.audit.valuation.v1"
PASS = FAIL = 0


def env(name: str, default: str) -> str:
    for line in (ROOT / ".env").read_text().splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].split("#")[0].strip()
    return default


def check(label: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ {label}")
    else:
        FAIL += 1
        print(f"  ✗ {label} {detail}", file=sys.stderr)


def load(name: str) -> str:
    return (ROOT / "schemas" / "examples" / name).read_text()


def sql(query: str) -> str:
    out = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "qm-audit",
            "psql",
            "-U",
            "qm_admin",
            "-d",
            "qm_audit",
            "-Atc",
            query,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.strip()


def wait_row(event_id: str, timeout: int = 60) -> str:
    for _ in range(timeout):
        payload = sql(
            f"SELECT payload::text FROM audit.events WHERE event_id = '{event_id}'"
        )
        if payload:
            return payload
        time.sleep(1)
    return ""


def event(tag: str, payload: dict) -> dict:
    return {
        "event_id": uuid7(),
        "type": "pricing.valuation",
        "version": 1,
        "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "request_id": f"req_{tag}",
        "trace_id": uuid.uuid4().hex,
        "username": tag,
        "producer": {
            "service": "registry_test.py",
            "git_sha": "test",
            "lib_build": "-",
        },
        "payload": payload,
    }


def main() -> int:
    tag = f"registry-{uuid.uuid4().hex[:6]}"
    base = f"http://127.0.0.1:{env('QP_REGISTRY_PORT', '8082')}/apis/ccompat/v7"
    client = SchemaRegistryClient({"url": base})
    producer = Producer(
        {
            "bootstrap.servers": f"127.0.0.1:{env('QP_KAFKA_PORT', '9094')}",
            "acks": "all",
        }
    )

    print("1. Smoke: the stock confluent-kafka client against Apicurio")
    v1, v2, v3 = (
        load(n)
        for n in (
            "valuation.v1.avsc",
            "valuation.v2-compatible.avsc",
            "valuation.v2-breaking.avsc",
        )
    )
    id1 = client.register_schema(SUBJECT, Schema(v1, "AVRO"))
    check("schema registered, id assigned", isinstance(id1, int) and id1 > 0)
    check(
        "re-registering the same schema is idempotent (same id)",
        client.register_schema(SUBJECT, Schema(v1, "AVRO")) == id1,
    )
    check("global compatibility is BACKWARD", client.get_compatibility() == "BACKWARD")

    def serializer(schema: str) -> AvroSerializer:
        return AvroSerializer(
            client,
            schema,
            # auto.register: what a normal producer does. (use.latest.version would
            # reuse the client's CACHED latest version, i.e. v1 after we registered v2.)
            conf={
                "auto.register.schemas": True,
                "subject.name.strategy": lambda ctx, record: SUBJECT,
            },
        )

    def send(record: dict, schema: str) -> None:
        value = serializer(schema)(
            record, SerializationContext(TOPIC, MessageField.VALUE)
        )
        producer.produce(TOPIC, key=tag.encode(), value=value)
        producer.flush(15)

    print("2. An Avro event reaches the audit trail; JSON keeps working")
    ev1 = event(tag, {"product": "autocall", "npv": 0.9713, "duration_ms": 412})
    send(ev1, v1)
    payload = wait_row(ev1["event_id"])
    check("Avro event archived by the sink", bool(payload))
    check(
        "payload stored as jsonb, unchanged",
        payload != "" and json.loads(payload) == ev1["payload"],
        payload,
    )
    ev_json = event(tag, {"product": "vanilla", "npv": 1.0, "duration_ms": 3})
    producer.produce(TOPIC, key=tag.encode(), value=json.dumps(ev_json).encode())
    producer.flush(15)
    check(
        "a JSON event on the same topic is archived too",
        bool(wait_row(ev_json["event_id"])),
    )

    print("3. BACKWARD evolution: a field WITH a default is accepted")
    check(
        "registry accepts the compatible schema",
        client.test_compatibility(SUBJECT, Schema(v2, "AVRO")),
    )
    id2 = client.register_schema(SUBJECT, Schema(v2, "AVRO"))
    check("registered as a new version with a new id", id2 != id1)
    old_body = fastavro_bytes(v1, ev1)
    reader = fastavro.schemaless_reader(
        old_body,
        fastavro.parse_schema(json.loads(v1)),
        fastavro.parse_schema(json.loads(v2)),
    )
    check(
        "an OLD message is readable with the NEW schema; the default fills in",
        reader["payload"]["mc_std_error"] is None,
    )
    ev2 = event(
        tag,
        {"product": "autocall", "npv": 0.97, "duration_ms": 9, "mc_std_error": 0.0006},
    )
    send(ev2, v2)
    payload2 = wait_row(ev2["event_id"])
    check(
        "a message in the new schema is archived with the new field",
        payload2 != "" and json.loads(payload2).get("mc_std_error") == 0.0006,
        payload2,
    )

    print("4. An incompatible schema is refused")
    check(
        "compatibility check says no",
        client.test_compatibility(SUBJECT, Schema(v3, "AVRO")) is False,
    )
    try:
        client.register_schema(SUBJECT, Schema(v3, "AVRO"))
        check(
            "registration of the breaking schema is refused",
            False,
            "(it was accepted!)",
        )
    except SchemaRegistryError as exc:
        check(
            "registration refused with HTTP 409",
            exc.http_status_code == 409,
            str(exc.http_status_code),
        )
        print(f"    registry says: {exc.error_message[:300]}")
        check(
            "the message names the offending field (model_name)",
            "model_name" in exc.error_message,
            exc.error_message[:300],
        )

    print("5. Size of the same event, JSON vs Avro (framed)")
    j = len(json.dumps(ev1, separators=(",", ":")).encode())
    a = len(serializer(v1)(ev1, SerializationContext(TOPIC, MessageField.VALUE)))
    print(
        f"    JSON {j} B · Avro {a} B → {a / j:.0%} of the JSON size; over 10 000 messages: {j * 10000 / 1e6:.2f} MB vs {a * 10000 / 1e6:.2f} MB"
    )
    check("Avro is smaller than JSON", a < j)

    print("6. The registry keeps its state across a restart (KafkaSQL)")
    compose("restart", "schema-registry")
    wait_registry_ready(base)
    fresh = SchemaRegistryClient({"url": base})
    check(
        "subject and schema ids survive the restart",
        SUBJECT in fresh.get_subjects() and fresh.get_schema(id1).schema_str != "",
    )

    print("7. Registry outage: the sink retries, nothing is dead-lettered")
    compose(
        "restart", "audit-sink"
    )  # empty schema cache: it must ask the registry again
    time.sleep(8)
    dlq_before = dlq_end_offset()
    compose("stop", "schema-registry")
    try:
        ev3 = event(tag, {"product": "vanilla", "npv": 2.0, "duration_ms": 5})
        producer.produce(TOPIC, key=tag.encode(), value=frame(id1, v1, ev3))
        producer.flush(15)
        time.sleep(15)
        check(
            "not archived while the registry is down",
            sql(
                f"SELECT count(*) FROM audit.events WHERE event_id = '{ev3['event_id']}'"
            )
            == "0",
        )
        check(
            "nothing was dead-lettered during the outage",
            dlq_end_offset() == dlq_before,
        )
    finally:
        compose("start", "schema-registry")
    wait_registry_ready(base)
    check("archived once the registry is back", bool(wait_row(ev3["event_id"], 120)))

    print(f"\n{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


def compose(*args: str) -> None:
    subprocess.run(
        ["docker", "compose", *args], cwd=ROOT, capture_output=True, check=True
    )


def wait_registry_ready(base: str, timeout: int = 120) -> None:
    import urllib.request

    for _ in range(timeout):
        try:
            urllib.request.urlopen(base + "/subjects", timeout=3)
            return
        except OSError:
            time.sleep(1)
    raise SystemExit("registry did not come back")


def frame(schema_id: int, schema_json: str, record: dict) -> bytes:
    return (
        b"\x00"
        + schema_id.to_bytes(4, "big")
        + fastavro_bytes(schema_json, record).getvalue()
    )


def dlq_end_offset() -> int:
    out = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "env",
            "KAFKA_HEAP_OPTS=-Xmx64m",
            "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--topic",
            "qm.dlq.v1",
            "--time",
            "-1",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return sum(int(line.split(":")[2]) for line in out.split() if line.count(":") == 2)


def fastavro_bytes(schema_json: str, record: dict):
    import io

    buf = io.BytesIO()
    fastavro.schemaless_writer(
        buf, fastavro.parse_schema(json.loads(schema_json)), record
    )
    buf.seek(0)
    return buf


if __name__ == "__main__":
    sys.exit(main())
