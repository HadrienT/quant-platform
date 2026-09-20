"""Print valid envelopes as `key|json` lines, ready for kafka-console-producer.

    python3 scripts/gen_events.py COUNT TAG [--type test.event] [--occurred-at ISO]

Every event gets a fresh UUID v7 and username TAG, so a test counts its own rows
(the audit table is append-only: test rows can never be cleaned up).
"""

import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone


def uuid7() -> str:
    ms = int(time.time() * 1000)
    rand = int.from_bytes(os.urandom(10), "big")
    value = (
        (ms << 80)
        | (0x7 << 76)
        | ((rand >> 68) << 64)
        | (0b10 << 62)
        | (rand & ((1 << 62) - 1))
    )
    return str(uuid.UUID(int=value))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("count", type=int)
    parser.add_argument("tag")
    parser.add_argument("--type", default="test.event")
    parser.add_argument("--occurred-at")
    args = parser.parse_args()

    occurred_at = args.occurred_at or (
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    )
    for i in range(args.count):
        event = {
            "event_id": uuid7(),
            "type": args.type,
            "version": 1,
            "occurred_at": occurred_at,
            "request_id": f"req_{i}",
            "trace_id": os.urandom(16).hex(),
            "username": args.tag,
            "producer": {
                "service": "gen_events.py",
                "git_sha": "test",
                "lib_build": "-",
            },
            "payload": {"i": i, "tag": args.tag},
        }
        print(f"{args.tag}|{json.dumps(event, separators=(',', ':'))}")


if __name__ == "__main__":
    main()
