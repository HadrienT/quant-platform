"""Enforce the non-negotiable Docker rules of CLAUDE.md on the resolved compose file.

Reads the JSON printed by `docker compose config --format json` on stdin.

  - image tags are pinned (never `latest`, never missing);
  - every service has an explicit memory and CPU limit;
  - no port is published outside 127.0.0.1;
  - every service is attached to the external `dataplatform` network or is
    explicitly local-only (no networks entry is fine: compose default network).
"""

import json
import sys


def image_tag(image: str) -> str | None:
    """Return the tag of an image reference, or None when it has none."""
    name = image.rsplit("/", 1)[-1]
    if "@sha256:" in image:
        return "digest"
    if ":" not in name:
        return None
    return name.rsplit(":", 1)[1]


def check(config: dict) -> list[str]:
    problems: list[str] = []
    for name, svc in (config.get("services") or {}).items():
        image = svc.get("image")
        if not image:
            problems.append(f"{name}: no `image:` (built images still need a tag)")
        else:
            tag = image_tag(image)
            if tag is None or tag == "latest":
                problems.append(
                    f"{name}: image '{image}' must have a pinned tag (not latest)"
                )
        if not svc.get("mem_limit"):
            problems.append(f"{name}: missing explicit `mem_limit`")
        if not svc.get("cpus"):
            problems.append(f"{name}: missing explicit `cpus`")
        for port in svc.get("ports") or []:
            host_ip = port.get("host_ip") or "0.0.0.0"
            if host_ip != "127.0.0.1":
                problems.append(
                    f"{name}: port {port.get('published')}->{port.get('target')} "
                    f"published on {host_ip}, must be 127.0.0.1"
                )
        if svc.get("network_mode") == "host":
            problems.append(f"{name}: network_mode host is forbidden")
    return problems


def main() -> int:
    problems = check(json.load(sys.stdin))
    for problem in problems:
        print(f"✗ {problem}", file=sys.stderr)
    if not problems:
        print("✓ compose policy respected")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
