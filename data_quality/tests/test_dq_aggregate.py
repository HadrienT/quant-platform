import pytest
from prometheus_client import REGISTRY

from dq.aggregate import (
    FALLBACK_TOPIC,
    VALUATION_TOPIC,
    Aggregator,
    SlidingWindow,
)

from dq_fakes import Clock, envelope


def value(name: str, **labels) -> float:
    v = REGISTRY.get_sample_value(name, labels)
    assert v is not None, f"series {name}{labels} does not exist"
    return v


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def agg(clock):
    return Aggregator(window_s=900, clock=clock)


def fallback(kind, n=1):
    return envelope("data.fallback", {"kind": kind}, n)


def valuation(statuses, n=1):
    return envelope(
        "pricing.valuation", {"market_inputs": [{"status": s} for s in statuses]}, n
    )


def test_every_series_exists_at_zero_before_any_event(agg):
    # otherwise increase() over the first event would be 0 and an alert would never fire
    assert value("qp_dq_fallbacks_total", kind="default_rate") >= 0
    assert value("qp_dq_valuations_total", quality="degraded") >= 0
    assert value("qp_dq_market_inputs_total", status="default") >= 0


def test_known_fallback_kind_is_counted_under_its_own_label(agg):
    before = value("qp_dq_fallbacks_total", kind="live_yfinance_spot")
    agg.observe(FALLBACK_TOPIC, fallback("live_yfinance_spot"))
    assert value("qp_dq_fallbacks_total", kind="live_yfinance_spot") == before + 1


def test_unknown_kind_cannot_mint_a_new_series(agg):
    before = value("qp_dq_fallbacks_total", kind="other")
    agg.observe(FALLBACK_TOPIC, fallback("something_the_producer_invented"))
    assert value("qp_dq_fallbacks_total", kind="other") == before + 1
    assert (
        REGISTRY.get_sample_value(
            "qp_dq_fallbacks_total", {"kind": "something_the_producer_invented"}
        )
        is None
    )


def test_valuation_is_degraded_when_any_input_is_not_observed(agg):
    d0 = value("qp_dq_valuations_total", quality="degraded")
    c0 = value("qp_dq_valuations_total", quality="clean")
    agg.observe(VALUATION_TOPIC, valuation(["observed", "observed"]))
    agg.observe(VALUATION_TOPIC, valuation(["observed", "default"]))
    agg.observe(VALUATION_TOPIC, valuation(["stale"]))
    agg.observe(VALUATION_TOPIC, valuation(["proxied", "observed"]))
    assert value("qp_dq_valuations_total", quality="clean") == c0 + 1
    assert value("qp_dq_valuations_total", quality="degraded") == d0 + 3


def test_input_statuses_are_counted_and_unknown_ones_are_bucketed(agg):
    u0 = value("qp_dq_market_inputs_total", status="unknown")
    agg.observe(VALUATION_TOPIC, valuation(["observed", "wat"]))
    assert value("qp_dq_market_inputs_total", status="unknown") == u0 + 1


def test_valuation_without_inputs_is_not_flagged_degraded(agg):
    s0 = value("qp_dq_skipped_total", reason="no_inputs")
    d0 = value("qp_dq_valuations_total", quality="degraded")
    agg.observe(VALUATION_TOPIC, envelope("pricing.valuation", {}))
    assert value("qp_dq_skipped_total", reason="no_inputs") == s0 + 1
    assert value("qp_dq_valuations_total", quality="degraded") == d0


def test_wrong_type_on_a_topic_is_skipped_not_counted(agg):
    s0 = value("qp_dq_skipped_total", reason="unexpected_type")
    agg.observe(FALLBACK_TOPIC, envelope("pricing.valuation", {}))
    assert value("qp_dq_skipped_total", reason="unexpected_type") == s0 + 1


def test_last_event_timestamp_tracks_each_source_topic(agg, clock):
    clock.now = 5000.0
    agg.observe(FALLBACK_TOPIC, fallback("default_rate"))
    clock.now = 6000.0
    agg.observe(VALUATION_TOPIC, valuation(["observed"]))
    assert value("qp_dq_last_event_timestamp_seconds", topic=FALLBACK_TOPIC) == 5000.0
    assert value("qp_dq_last_event_timestamp_seconds", topic=VALUATION_TOPIC) == 6000.0


def test_sliding_window_evicts_old_events():
    clock = Clock(0.0)
    w = SlidingWindow(60, clock)
    w.add("a")
    clock.now = 30
    w.add("a")
    w.add("b")
    assert (w.count("a"), w.count("b")) == (2, 1)
    clock.now = 70  # the first "a" (t=0) is now older than 60 s
    assert (w.count("a"), w.count("b")) == (1, 1)
    clock.now = 200
    assert (w.count("a"), w.count("b")) == (0, 0)


def test_window_gauge_slides_without_new_events(agg, clock):
    agg.observe(FALLBACK_TOPIC, fallback("stale_data"))
    assert value("qp_dq_fallbacks_window", kind="stale_data") == 1
    clock.now += 901  # nothing arrives; the gauge is evaluated at scrape time
    assert value("qp_dq_fallbacks_window", kind="stale_data") == 0
