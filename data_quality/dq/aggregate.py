"""Turns envelopes into data-quality metrics.

Vocabulary of the desks for the quality of a market input (producer's payload):
  observed  read at the source and fresh
  stale     read, but too old
  proxied   replaced by a neighbouring input
  default   a fallback constant
A valuation is `degraded` when at least one of its inputs is NOT observed, and
`no_market_inputs` when it read none — a pricing on parameters the user typed
in (spot, vol…) says nothing about market-data quality, so it is neither clean
nor degraded (quant-modeling ADR-011 §4; issue #1).

Cardinality (CLAUDE.md principle 8): label values are drawn from CLOSED sets. A
`kind` or `status` the producer invents later maps to `other` / `unknown` instead
of minting a new time series; adding one is a reviewed change here.
"""

import time
from collections import deque
from collections.abc import Callable

from prometheus_client import Counter, Gauge

from qp_common.envelope import Envelope

# The producer's closed enumeration of fallback kinds (blueprint 18a).
KNOWN_KINDS = (
    "live_yfinance_chain",
    "live_yfinance_spot",
    "live_yfinance_dividend",
    "default_rate",
    "stale_data",
    "proxied_input",
)
INPUT_STATUSES = ("observed", "stale", "proxied", "default")

FALLBACK_TOPIC = "qm.dataquality.fallback.v1"
VALUATION_TOPIC = "qm.audit.valuation.v1"

FALLBACKS = Counter("qp_dq_fallbacks_total", "Fallback events, by kind", ["kind"])
VALUATIONS = Counter(
    "qp_dq_valuations_total",
    "Valuations: clean (every input observed), degraded (one is not), no_market_inputs (none read)",
    ["quality"],
)
INPUTS = Counter(
    "qp_dq_market_inputs_total", "Market inputs of valuations, by status", ["status"]
)
SKIPPED = Counter("qp_dq_skipped_total", "Messages not counted, by reason", ["reason"])
LAST_EVENT = Gauge(
    "qp_dq_last_event_timestamp_seconds",
    "Unix time at which the last event of a source topic was processed (a mute producer = this stops moving)",
    ["topic"],
)
WINDOW = Gauge(
    "qp_dq_fallbacks_window", "Fallbacks inside the sliding window, by kind", ["kind"]
)


def _init_series() -> None:
    """Create every series at 0 up front. A counter that first appears with value 1
    has no previous sample, so Prometheus' increase() reports 0 for that first event —
    an alert on it would never fire."""
    for kind in (*KNOWN_KINDS, "other"):
        FALLBACKS.labels(kind).inc(0)
    for quality in ("clean", "degraded", "no_market_inputs"):
        VALUATIONS.labels(quality).inc(0)
    for status in (*INPUT_STATUSES, "unknown"):
        INPUTS.labels(status).inc(0)
    for reason in ("invalid_envelope", "unexpected_type"):
        SKIPPED.labels(reason).inc(0)


class SlidingWindow:
    """Counts events per key over the last `seconds` (ingestion time)."""

    def __init__(self, seconds: int, clock: Callable[[], float] = time.time) -> None:
        self._seconds = seconds
        self._clock = clock
        self._events: deque[tuple[float, str]] = deque()

    def add(self, key: str) -> None:
        self._events.append((self._clock(), key))

    def count(self, key: str) -> int:
        self._evict()
        return sum(1 for _, k in self._events if k == key)

    def _evict(self) -> None:
        cutoff = self._clock() - self._seconds
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()


class Aggregator:
    def __init__(self, window_s: int, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self.window = SlidingWindow(window_s, clock)
        _init_series()
        for kind in (*KNOWN_KINDS, "other"):
            # Evaluated at scrape time: the window keeps sliding while nothing arrives.
            WINDOW.labels(kind).set_function(lambda k=kind: self.window.count(k))

    def observe(self, topic: str, env: Envelope) -> None:
        if topic == FALLBACK_TOPIC and env.type == "data.fallback":
            raw = env.payload.get("kind")
            kind = raw if raw in KNOWN_KINDS else "other"
            FALLBACKS.labels(kind).inc()
            self.window.add(kind)
        elif topic == VALUATION_TOPIC and env.type == "pricing.valuation":
            self._valuation(env)
        else:
            SKIPPED.labels("unexpected_type").inc()
            return
        LAST_EVENT.labels(topic).set(self._clock())

    def _valuation(self, env: Envelope) -> None:
        inputs = env.payload.get("market_inputs")
        if not isinstance(inputs, list) or not inputs:
            VALUATIONS.labels("no_market_inputs").inc()
            return
        degraded = False
        for item in inputs:
            status = item.get("status") if isinstance(item, dict) else None
            label = status if status in INPUT_STATUSES else "unknown"
            INPUTS.labels(label).inc()
            if label != "observed":
                degraded = True
        VALUATIONS.labels("degraded" if degraded else "clean").inc()
