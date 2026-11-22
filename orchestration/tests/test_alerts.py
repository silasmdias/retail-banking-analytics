"""Which alerts are delivered, where, and how often."""

from __future__ import annotations

import json

import pytest

from orchestration.alerts import (
    ALERTS_FILE,
    ERROR,
    WARNING,
    Alert,
    _post,
    dbt_test_note,
    deliver,
    root_cause,
)


def alert(alert_id: str = "a1", severity: str = ERROR, problems: tuple[str, ...] = ()) -> Alert:
    return Alert(
        alert_id=alert_id,
        raised_at="2026-02-03T07:00:00Z",
        severity=severity,
        source="landing/delivery: row_counts_match_control_file",
        summary="Row count differs from the control file: transactions_2026-01.csv",
        problems=problems,
        job="process_delivery",
        run_id="run-1",
    )


def recorded(alerts_dir) -> list[dict]:
    return [json.loads(line) for line in (alerts_dir / ALERTS_FILE).read_text().splitlines()]


def test_alerts_are_appended_to_a_file_one_per_line(tmp_path):
    assert deliver([alert("a1")], tmp_path) == [alert("a1")]
    assert deliver([alert("a2", WARNING)], tmp_path) == [alert("a2", WARNING)]
    assert [(row["alert_id"], row["severity"]) for row in recorded(tmp_path)] == [
        ("a1", "error"),
        ("a2", "warning"),
    ]


def test_the_same_alert_is_delivered_only_once(tmp_path):
    posted = []
    deliver(
        [alert("a1")], tmp_path, "https://chat.example/hook", post=lambda *args: posted.append(args)
    )
    again = deliver(
        [alert("a1"), alert("a2")],
        tmp_path,
        "https://chat.example/hook",
        post=lambda *args: posted.append(args),
    )
    assert again == [alert("a2")]
    assert len(recorded(tmp_path)) == 2
    assert len(posted) == 2


def test_nothing_is_posted_without_a_webhook(tmp_path):
    def post(url, text):
        raise AssertionError("no webhook was configured")

    assert deliver([alert()], tmp_path, webhook_url=None, post=post) == [alert()]


def test_a_webhook_gets_the_alert_as_text(tmp_path):
    posted = []
    deliver(
        [alert(problems=("first file", "second file"))],
        tmp_path,
        "https://chat.example/hook",
        post=lambda url, text: posted.append((url, text)),
    )
    [(url, text)] = posted
    assert url == "https://chat.example/hook"
    assert text.splitlines() == [
        "[ERROR] landing/delivery: row_counts_match_control_file",
        "Row count differs from the control file: transactions_2026-01.csv",
        "  - first file",
        "  - second file",
        "job process_delivery, run run-1",
    ]


def test_a_single_problem_is_not_repeated_under_the_summary():
    assert "  - " not in alert(problems=("only one",)).as_text()


def test_a_webhook_that_fails_does_not_lose_the_alert(tmp_path):
    logged = []

    def post(url, text):
        raise OSError("connection refused")

    assert deliver([alert()], tmp_path, "https://chat.example/hook", post=post, log=logged.append)
    assert len(recorded(tmp_path)) == 1
    assert "connection refused" in logged[0]


def test_only_http_urls_are_accepted_as_a_webhook():
    with pytest.raises(ValueError, match="http"):
        _post("file:///etc/hosts", "text")


def test_a_dbt_test_is_explained_in_the_words_of_the_dbt_project():
    limit = dbt_test_note("test.retail_banking.rejection_rate_within_limit.d14bd411dd")
    assert limit.startswith("More than 1% of a month's records rejected for a single reason")
    singular = dbt_test_note("test.retail_banking.assert_budget_vs_actual_reconciles_to_revenue")
    assert singular.startswith("Actual revenue in the budget report must equal revenue")
    assert "Debug:" in singular and "dagster" not in singular
    assert dbt_test_note("test.retail_banking.does_not_exist") == ""


class FakeError:
    def __init__(self, message: str, cause: FakeError | None = None) -> None:
        self.message = message
        self.cause = cause


class FakeFailure:
    def __init__(self, error: FakeError | None) -> None:
        self.error = error


def test_a_failed_step_is_reported_by_its_innermost_error():
    wrapped = FakeError("Error occurred while executing op", FakeError("KeyError: 'amount'\n"))
    assert root_cause(FakeFailure(wrapped)) == "KeyError: 'amount'"
    assert root_cause(FakeFailure(None)) == "The step failed without a message."


def test_a_dbt_failure_is_reported_in_the_words_of_dbt():
    message = (
        "The dbt CLI process with command\n\n`dbt build --select marts`\n\n"
        "failed with exit code `1`. Check the stdout in the Dagster compute logs.\n\n"
        "Errors parsed from dbt logs:\n\n"
        'Database Error in model dim_products\n  column "interchange" does not exist'
    )
    assert root_cause(FakeFailure(FakeError("wrapper", FakeError(message)))) == (
        'dbt reported: Database Error in model dim_products column "interchange" does not exist'
    )
