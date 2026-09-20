from wait_healthy import evaluate

ONESHOT = {"topics-init"}
EXPECTED = {"kafka", "topics-init", "grafana"}


def row(service, state="running", health="", code=0):
    return {"Service": service, "State": state, "Health": health, "ExitCode": code}


def test_everything_as_expected_is_ok():
    rows = [
        row("kafka", health="healthy"),
        row("grafana"),
        row("topics-init", "exited", code=0),
    ]
    assert evaluate(rows, ONESHOT, EXPECTED) == ("ok", [])


def test_a_finished_one_shot_job_is_success_not_failure():
    # the very case `docker compose up --wait` got wrong
    rows = [
        row("kafka", health="healthy"),
        row("grafana"),
        row("topics-init", "exited", code=0),
    ]
    assert evaluate(rows, ONESHOT, EXPECTED)[0] == "ok"


def test_a_failed_one_shot_job_fails_at_once():
    rows = [
        row("kafka", health="healthy"),
        row("grafana"),
        row("topics-init", "exited", code=1),
    ]
    state, details = evaluate(rows, ONESHOT, EXPECTED)
    assert state == "failed" and "code 1" in details[0]


def test_a_running_job_or_starting_service_is_pending():
    rows = [
        row("kafka", health="starting"),
        row("grafana"),
        row("topics-init", "running"),
    ]
    state, details = evaluate(rows, ONESHOT, EXPECTED)
    assert state == "pending" and len(details) == 2


def test_an_unhealthy_or_dead_long_running_service_fails():
    assert (
        evaluate(
            [
                row("kafka", health="unhealthy"),
                row("grafana"),
                row("topics-init", "exited"),
            ],
            ONESHOT,
            EXPECTED,
        )[0]
        == "failed"
    )
    assert (
        evaluate(
            [
                row("kafka", health="healthy"),
                row("grafana", "exited", code=137),
                row("topics-init", "exited"),
            ],
            ONESHOT,
            EXPECTED,
        )[0]
        == "failed"
    )


def test_a_missing_service_is_pending_not_ok():
    state, details = evaluate([row("kafka", health="healthy")], ONESHOT, EXPECTED)
    assert state == "pending" and any("not created" in d for d in details)
