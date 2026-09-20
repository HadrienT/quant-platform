#!/usr/bin/env bash
# Memory under load: generates a steady flow of events for DURATION seconds while
# sampling `docker stats`, then prints the PEAK memory of each platform container
# against its limit. Sizing is measured, never guessed (CLAUDE.md).
#
#   scripts/measure_memory.sh [DURATION_S]      default 240
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib_test.sh
source scripts/lib_test.sh

DURATION="${1:-240}"
TAG="mem-$(openssl rand -hex 3)"
SAMPLES="$(mktemp)"
trap 'rm -f "$SAMPLES"' EXIT

echo "→ ${DURATION}s of load (a batch of 400 events every 3 s), sampling docker stats every 10 s"
end=$((SECONDS + DURATION))
next_sample=0
while ((SECONDS < end)); do
  produce_events 400 "$TAG"
  if ((SECONDS >= next_sample)); then
    mapfile -t names < <(docker compose ps --format '{{.Name}}')
    docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' "${names[@]}" >>"$SAMPLES" 2>/dev/null || true
    next_sample=$((SECONDS + 10))
  fi
  sleep 3
done

# MemUsage looks like "220.9MiB / 320MiB": convert to MiB, keep the max per container.
python3 - "$SAMPLES" <<'PY'
import re, sys
UNIT = {"B": 1 / 1048576, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024}
def mib(text):
    m = re.match(r"([\d.]+)\s*([KMG]?i?B)", text)
    return float(m.group(1)) * UNIT[m.group(2)]
peak, limit = {}, {}
for line in open(sys.argv[1]):
    name, rest = line.split(" ", 1)
    used, cap = (part.strip() for part in rest.split("/"))
    peak[name] = max(peak.get(name, 0), mib(used))
    limit[name] = mib(cap)
print(f"{'container':<18}{'peak MiB':>10}{'limit MiB':>11}{'peak/limit':>12}")
for name in sorted(peak):
    print(f"{name:<18}{peak[name]:>10.0f}{limit[name]:>11.0f}{peak[name] / limit[name]:>11.0%}")
print(f"{'TOTAL peak':<18}{sum(peak.values()):>10.0f}{sum(limit.values()):>11.0f}")
PY
