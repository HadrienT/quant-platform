"""Per-partition consumption stalls around the death of one consumer.

    gaps.py KILL_EPOCH SURVIVOR.jsonl SURVIVOR.jsonl KILLED.jsonl

Each file holds one probe's output (one JSON line per message / rebalance event).
A merged timeline hides a rebalance, because partitions nobody lost keep flowing; so
this looks at every PARTITION on its own and reports the longest hole between two
consecutive messages that spans the moment of the kill:

  - partitions the killed consumer owned: how long until someone else served them;
  - partitions the survivors already owned: were THEY interrupted too?
    (yes = eager "stop the world" rebalance; no = cooperative / incremental)
"""

import json
import sys
from collections import defaultdict

kill = float(sys.argv[1])
*survivor_files, killed_file = sys.argv[2:]


def messages(path):
    for line in open(path):
        if line.startswith("{"):
            d = json.loads(line)
            if "o" in d:
                yield d["t"], d["p"]


killed_parts = {p for t, p in messages(killed_file) if kill - 10 <= t < kill}
times = defaultdict(list)
for path in (*survivor_files, killed_file):
    for t, p in messages(path):
        times[p].append(t)


def longest_stall(part):
    ts = sorted(times[part])
    spans = [b - a for a, b in zip(ts, ts[1:]) if b >= kill]
    if not spans:  # nothing arrived after the kill at all
        return float("inf")
    return max(spans)


nominal = sorted(
    b - a for p in times for a, b in zip(sorted(times[p]), sorted(times[p])[1:])
)
print(
    f"normal gap between messages of one partition (median): {nominal[len(nominal) // 2] * 1000:.0f} ms"
)
for label, parts in (
    ("owned by the killed consumer", sorted(killed_parts)),
    ("owned by the survivors", sorted(set(times) - killed_parts)),
):
    stalls = {p: longest_stall(p) for p in parts}
    worst = max(stalls.values())
    detail = ", ".join(f"p{p}={s:.1f}s" for p, s in stalls.items())
    print(f"partitions {label}: longest stall {worst:.1f} s   [{detail}]")
