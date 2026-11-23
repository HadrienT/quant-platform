"""Group-membership watchdog: a consumer that holds no partition for too long exits.

Why exit instead of waiting: on 2026-10-20 the broker stalled for a few minutes,
dropped every group member, and both librdkafka consumers (audit-sink, data-quality)
got "Not coordinator", had their partitions revoked, and NEVER rejoined. The process
stayed up and healthy for a month while nothing was consumed (issue #9). The Java
client of the schema registry rejoined at once: the stuck state is client-side.

We do not try to repair librdkafka's state from the outside. A consumer that has been
subscribed but without any assigned partition for `max_unassigned_s` raises
`ConsumerStuck`; the entry point exits non-zero and Docker (`restart: unless-stopped`)
starts a fresh process, which joins the group from scratch. That is safe by
construction: offsets are committed only after processing (ADR-005), so a restart at
any moment re-reads at worst one batch, and the sink's insert is idempotent.

The bound must stay well above a normal rebalance (a few seconds) and the session
timeout (30 s): 120 s by default. It assumes ONE instance per group: a second
instance could legitimately hold zero partitions if there were fewer partitions than
members (not the case: one instance each, 4 to 11 partitions).
"""

import time
from collections.abc import Callable

from prometheus_client import Gauge

ASSIGNED = Gauge(
    "qp_consumer_assigned_partitions",
    "Partitions currently assigned to this consumer (0 = not consuming anything)",
)


class ConsumerStuck(Exception):
    """Subscribed, but no partition assigned for longer than the allowed bound."""


class AssignmentWatchdog:
    def __init__(
        self, max_unassigned_s: float, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self._max = max_unassigned_s
        self._clock = clock
        # Start counting at creation: the very first join is bounded too.
        self._unassigned_since: float | None = clock()

    def check(self, assigned: int) -> None:
        """Call once per poll with the current number of assigned partitions."""
        ASSIGNED.set(assigned)
        now = self._clock()
        if assigned > 0:
            self._unassigned_since = None
            return
        if self._unassigned_since is None:
            self._unassigned_since = now
        elif now - self._unassigned_since > self._max:
            raise ConsumerStuck(
                f"no partition assigned for {now - self._unassigned_since:.0f} s "
                f"(bound {self._max:.0f} s): exiting so the container restarts"
            )
