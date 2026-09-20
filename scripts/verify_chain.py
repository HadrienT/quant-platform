"""Verify the audit trail's hash chain (WP 02, optional task 9).

    python3 scripts/verify_chain.py [TOPIC]        # needs the stack up

Recomputes the chain of every Kafka partition in the database (audit.verify_chain) and
prints, per partition, how many rows were checked and where the chain breaks, if it does.
Exit code 1 when any partition is broken. What it proves and what it cannot: see
migrations/0005_hash_chain.sql (consistency, not completeness; head/tail removal is not seen).
"""

import subprocess
import sys

topic = sys.argv[1] if len(sys.argv) > 1 else None
arg = "NULL" if topic is None else "'" + topic.replace("'", "''") + "'"
out = subprocess.run(
    [
        "docker",
        "compose",
        "exec",
        "-T",
        "qm-audit",
        "psql",
        "-U",
        "audit_reader",
        "-d",
        "qm_audit",
        "-X",
        "-At",
        "-F",
        "|",
        "-c",
        f"SELECT * FROM audit.verify_chain({arg})",
    ],
    capture_output=True,
    text=True,
)
if out.returncode != 0:
    print(out.stderr.strip(), file=sys.stderr)
    sys.exit(2)

broken = 0
total = 0
for line in out.stdout.splitlines():
    src_topic, part, rows, broken_at, reason = line.split("|")
    total += int(rows)
    if broken_at:
        broken += 1
        print(
            f"✗ {src_topic}[{part}]: chain broken at offset {broken_at} — {reason} ({rows} rows checked)"
        )
    else:
        print(f"✓ {src_topic}[{part}]: intact ({rows} rows)")
print(f"{total} chained rows checked, {broken} broken partition(s)")
sys.exit(1 if broken else 0)
