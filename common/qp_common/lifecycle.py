"""Graceful shutdown: SIGTERM/SIGINT ask the loop to finish its batch and exit."""

import signal
import threading
from types import FrameType


class GracefulStop:
    def __init__(self) -> None:
        self._event = threading.Event()

    def install(self) -> None:
        signal.signal(signal.SIGTERM, self._handle)
        signal.signal(signal.SIGINT, self._handle)

    def _handle(self, signum: int, _frame: FrameType | None) -> None:
        self._event.set()

    def request(self) -> None:
        self._event.set()

    @property
    def requested(self) -> bool:
        return self._event.is_set()

    def sleep(self, seconds: float) -> bool:
        """Sleep, waking early on a stop request. True if a stop was requested."""
        return self._event.wait(seconds)
