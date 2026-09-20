from qp_common.dlq import Rejected, dlq_headers
from qp_common.retry import Backoff


def test_backoff_doubles_then_caps_and_resets():
    b = Backoff(base=1, cap=8, jitter=lambda: 0.0)
    assert [b.next_delay() for _ in range(6)] == [1, 2, 4, 8, 8, 8]
    b.reset()
    assert b.next_delay() == 1


def test_backoff_jitter_stays_within_25_percent():
    b = Backoff(base=4, cap=100, jitter=lambda: 1.0)
    assert b.next_delay() == 5.0


def test_dlq_headers_follow_the_contract():
    item = Rejected("qm.audit.auth.v1", 2, 41, b"k", b"{", "not valid UTF-8 JSON")
    headers = dict(dlq_headers(item, "audit-sink"))
    assert headers == {
        "dlq.source.topic": b"qm.audit.auth.v1",
        "dlq.source.partition": b"2",
        "dlq.source.offset": b"41",
        "dlq.error": b"not valid UTF-8 JSON",
        "dlq.consumer.group": b"audit-sink",
    }


def test_dlq_error_is_truncated():
    item = Rejected("t", 0, 0, None, b"", "x" * 5000)
    assert len(dict(dlq_headers(item, "g"))["dlq.error"]) == 500
