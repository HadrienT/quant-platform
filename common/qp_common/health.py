"""Container healthcheck: healthy only while the consumer holds at least one partition.

    python -m qp_common.health <metrics port>

The previous check only fetched /metrics, which answers as long as the process lives:
a consumer stuck outside its group stayed "healthy" for a month (issue #9). This one
reads `qp_consumer_assigned_partitions` (set by the watchdog on every poll) and fails
when it is 0 or missing. A normal rebalance shows 0 for a few seconds; the compose
`retries` absorb it. Standard library only: it runs inside the consumer images.
"""

import sys
import urllib.request

METRIC = "qp_consumer_assigned_partitions"


def assigned_partitions(metrics_text: str) -> float | None:
    for line in metrics_text.splitlines():
        if line.startswith(METRIC + " ") or line.startswith(METRIC + "{"):
            return float(line.rsplit(" ", 1)[1])
    return None


def main(argv: list[str]) -> int:
    port = int(argv[1])
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=3) as r:
            text = r.read().decode()
    except OSError as exc:
        print(f"unhealthy: metrics endpoint unreachable: {exc}")
        return 1
    value = assigned_partitions(text)
    if not value:
        print(f"unhealthy: {METRIC} = {value}")
        return 1
    print(f"healthy: {value:.0f} partitions assigned")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
