"""The group-membership watchdog and the healthcheck that reads its gauge (issue #9)."""

import pytest

from qp_common.health import assigned_partitions
from qp_common.watchdog import ASSIGNED, AssignmentWatchdog, ConsumerStuck


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_initial_join_is_bounded_too():
    clock = Clock()
    dog = AssignmentWatchdog(120, clock)
    clock.now += 119
    dog.check(0)  # still within the bound
    clock.now += 2
    with pytest.raises(ConsumerStuck, match="no partition assigned for 121 s"):
        dog.check(0)


def test_revoke_without_reassignment_raises_after_the_bound():
    """The 2026-10-20 incident: partitions revoked, never assigned again."""
    clock = Clock()
    dog = AssignmentWatchdog(120, clock)
    dog.check(11)
    clock.now += 3600  # an hour of normal consumption
    dog.check(11)
    clock.now += 1
    dog.check(0)  # revoked: the clock starts NOW, not at the last assignment
    clock.now += 120
    dog.check(0)
    clock.now += 1
    with pytest.raises(ConsumerStuck):
        dog.check(0)


def test_a_short_rebalance_resets_the_clock():
    clock = Clock()
    dog = AssignmentWatchdog(120, clock)
    for _ in range(10):  # repeated rebalances of 100 s each never add up
        dog.check(0)
        clock.now += 100
        dog.check(4)
    dog.check(0)


def test_gauge_follows_the_assignment():
    dog = AssignmentWatchdog(120, Clock())
    dog.check(7)
    assert ASSIGNED._value.get() == 7
    dog.check(0)
    assert ASSIGNED._value.get() == 0


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("# HELP x\nqp_consumer_assigned_partitions 11.0\n", 11.0),
        ("qp_consumer_assigned_partitions 0.0\n", 0.0),
        ("qp_consumer_assigned_partitions_other 3.0\n", None),
        ("", None),
    ],
)
def test_healthcheck_reads_the_gauge(text, expected):
    assert assigned_partitions(text) == expected
