"""The two checks the orchestrator adds on top of the dbt tests."""

from __future__ import annotations

import dagster as dg

from orchestration.checks import (
    FRESHNESS_SOURCES,
    RejectionRate,
    find_rejection_drift,
    freshness_check_result,
)

KEY = dg.AssetKey(["core_banking", "transactions"])


def history(reason: str = "unknown_account", months: int = 8, rate: float = 0.001):
    """Months 2025-01 onwards with a steady rejection rate and 40 rejected records each."""
    return [
        RejectionRate("transactions", f"2025-{month:02d}", reason, 40, rate)
        for month in range(1, months + 1)
    ]


def test_a_steady_rate_is_not_drift():
    assert find_rejection_drift(history(), ["2025-08"]) == []


def test_a_rate_far_above_the_previous_six_months_is_drift():
    rows = history(months=7) + [
        RejectionRate("transactions", "2025-08", "unknown_account", 240, 0.006)
    ]
    assert find_rejection_drift(rows, ["2025-08"]) == [
        "transactions 2025-08, unknown_account: 0.60% rejected (240 records), "
        "6.0 times the 0.10% average of the previous 6 months"
    ]


def test_only_the_months_asked_for_are_evaluated():
    rows = history(months=7) + [
        RejectionRate("transactions", "2025-08", "unknown_account", 240, 0.006)
    ]
    assert find_rejection_drift(rows, ["2025-07"]) == []


def test_a_handful_of_records_is_noise_whatever_the_rate():
    rows = history(months=7) + [RejectionRate("fees", "2025-08", "missing_amount", 19, 0.02)]
    rows += [RejectionRate("fees", "2025-01", "unknown_account", 1, 0.001)]
    assert find_rejection_drift(rows, ["2025-08"]) == []


def test_a_reason_never_seen_before_is_drift_once_it_has_enough_records():
    rows = history(months=8) + [
        RejectionRate("transactions", "2025-08", "unreadable_date", 25, 0.0006)
    ]
    [problem] = find_rejection_drift(rows, ["2025-08"])
    assert problem.startswith("transactions 2025-08, unreadable_date: 0.06% rejected (25 records)")
    assert "up from the 0.00% average" in problem


def test_a_source_younger_than_six_months_is_not_evaluated():
    rows = history(months=5) + [
        RejectionRate("transactions", "2025-06", "unknown_account", 500, 0.05)
    ]
    assert find_rejection_drift(rows, ["2025-06"]) == []


def test_a_month_without_rejections_counts_as_zero_in_the_average():
    rows = [row for row in history(months=7) if row.record_month not in ("2025-03", "2025-04")]
    rows.append(RejectionRate("transactions", "2025-08", "unknown_account", 100, 0.0025))
    # Average of 2025-02 to 2025-07 is 4 months at 0.10% and 2 at zero: 0.067%.
    [problem] = find_rejection_drift(rows, ["2025-08"])
    assert "3.8 times the 0.07% average" in problem


def freshness(status: str, age_in_days: float) -> dict:
    return {
        "unique_id": "source.retail_banking.core_banking.transactions",
        "status": status,
        "max_loaded_at": "2026-01-05T08:00:00+00:00",
        "max_loaded_at_time_ago_in_s": age_in_days * 86400,
        "criteria": {
            "warn_after": {"count": 35, "period": "day"},
            "error_after": {"count": 45, "period": "day"},
        },
    }


def test_the_three_monthly_feeds_declare_freshness():
    assert sorted(key.to_user_string() for key in FRESHNESS_SOURCES.values()) == [
        "core_banking/account_balances",
        "core_banking/fees",
        "core_banking/transactions",
    ]


def test_a_recent_load_passes_the_freshness_check():
    result = freshness_check_result(freshness("pass", 0.1), KEY)
    assert result.passed
    assert result.description == "Last load 2.4 hours ago"


def test_a_feed_that_is_late_warns_and_one_that_stopped_fails():
    late = freshness_check_result(freshness("warn", 37), KEY)
    assert not late.passed and late.severity == dg.AssetCheckSeverity.WARN
    assert late.description == (
        "Last load 37.0 days ago; a new file is expected at least every 35 days"
    )
    stopped = freshness_check_result(freshness("error", 50), KEY)
    assert not stopped.passed and stopped.severity == dg.AssetCheckSeverity.ERROR
    assert stopped.description.endswith("at least every 45 days")


def test_a_source_dbt_could_not_measure_fails_the_check():
    result = freshness_check_result({"status": "runtime error", "error": "table not found"}, KEY)
    assert not result.passed and result.severity == dg.AssetCheckSeverity.ERROR
    assert "table not found" in result.description
