"""Lab probe (WP 06, exercise 2): a timestamped producer/consumer.

    probe.py produce TOPIC RATE_PER_S SECONDS
    probe.py consume TOPIC GROUP            (env STRATEGY, PROTOCOL)

`consume` prints one JSON line per message received (local wall-clock time, so that
lines from several consumers can be merged into one timeline) and one line per
rebalance callback. Env: KAFKA_BOOTSTRAP, STRATEGY (classic protocol: range |
cooperative-sticky), PROTOCOL (classic | consumer, the KIP-848 protocol).
"""

import json
import os
import signal
import sys
import time

from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka-1:9092")


def out(**kw):
    print(json.dumps({"t": round(time.time(), 4), **kw}), flush=True)


def produce(topic, rate, seconds):
    p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "1", "linger.ms": 5})
    n, start = 0, time.time()
    while time.time() - start < seconds:
        # 97 distinct keys: enough to reach every partition
        p.produce(topic, key=str(n % 97).encode(), value=str(n).encode())
        p.poll(0)
        n += 1
        time.sleep(1.0 / rate)
    p.flush(10)
    out(event="produced", n=n)


def consume(topic, group):
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    }
    protocol = os.environ.get("PROTOCOL", "classic")
    conf["group.protocol"] = protocol
    if protocol == "classic":
        conf["partition.assignment.strategy"] = os.environ.get("STRATEGY", "range")
        conf["session.timeout.ms"] = 10000
    c = Consumer(conf)

    def on_assign(_c, parts):
        out(event="assign", parts=sorted(p.partition for p in parts))

    def on_revoke(_c, parts):
        out(event="revoke", parts=sorted(p.partition for p in parts))

    # SIGTERM must end in c.close() (below): that is what makes the consumer LEAVE the group
    # at once. Without it `docker stop` looks exactly like a crash to the broker.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    c.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
    out(
        event="started",
        protocol=protocol,
        strategy=conf.get("partition.assignment.strategy"),
    )
    try:
        while True:
            m = c.poll(0.2)
            if m is not None and m.error() is None:
                out(p=m.partition(), o=m.offset())
    finally:
        c.close()


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == "produce":
        produce(sys.argv[2], float(sys.argv[3]), float(sys.argv[4]))
    else:
        consume(sys.argv[2], sys.argv[3])
