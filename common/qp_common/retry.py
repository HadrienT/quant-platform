"""Exponential backoff with jitter, for TRANSIENT failures only."""

import random
from collections.abc import Callable


class Backoff:
    """1s, 2s, 4s… capped. reset() after a success."""

    def __init__(
        self,
        base: float = 1.0,
        cap: float = 30.0,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        self._base = base
        self._cap = cap
        self._jitter = jitter
        self._attempt = 0

    def next_delay(self) -> float:
        delay = min(self._cap, self._base * (2**self._attempt))
        self._attempt += 1
        # Up to 25% jitter so that several consumers do not retry in lockstep.
        return delay * (1 + 0.25 * self._jitter())

    def reset(self) -> None:
        self._attempt = 0

    @property
    def attempts(self) -> int:
        return self._attempt
