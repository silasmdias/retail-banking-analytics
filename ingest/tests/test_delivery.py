"""The landing folder scan, the delivery plan and the checks that guard the load."""

from __future__ import annotations

import csv

from generator.writer import HEADERS
from ingest.contract import FEEDS, MONTHLY
from ingest.delivery import (
    check_delivery_is_complete,
    check_headers_match_contract,
    check_monthly_volume_is_plausible,
    check_row_counts_match_control_file,
    plan_delivery,
    scan,
)
from ingest.load_raw import load_delivery
from ingest.tests.conftest import deliver, keep_rows, set_control_rows

OCTOBER = "core_banking/transactions/transactions_2023-10.csv"


def test_the_contract_describes_what_the_source_systems_send():
    assert {feed.table: feed.columns for feed in FEEDS} == HEADERS


def test_scan_reads_header_row_count_and_period_of_every_file(raw_dir):
    landed = scan(raw_dir)
    assert {file.relative_path for file in landed} == {
        path.relative_to(raw_dir).as_posix() for path in raw_dir.glob("*/*/*.csv")
    }
    for file in landed:
        with file.path.open(newline="") as handle:
            rows = list(csv.reader(handle))
        assert file.header == tuple(rows[0]) == file.feed.columns
        assert file.rows == len(rows) - 1
        assert file.period is not None
    periods = {file.period for file in landed if file.feed.cadence == MONTHLY}
    assert periods == {f"2023-{month:02d}" for month in range(1, 11)}


def test_a_file_without_a_final_line_break_is_counted_in_full(raw_dir):
    path = raw_dir / OCTOBER
    before = next(file.rows for file in scan(raw_dir) if file.relative_path == OCTOBER)
    path.write_text(path.read_text().rstrip("\n"))
    after = next(file.rows for file in scan(raw_dir) if file.relative_path == OCTOBER)
    assert after == before


def test_everything_is_pending_until_it_is_loaded(raw_dir, database):
    delivery = plan_delivery(raw_dir, database)
    assert delivery.pending == delivery.landed
    assert delivery.months == tuple(f"2023-{month:02d}" for month in range(1, 11))

    load_delivery(delivery, database)
    assert plan_delivery(raw_dir, database).pending == ()


def test_a_new_month_makes_only_new_and_changed_files_pending(raw_dir, database):
    load_delivery(plan_delivery(raw_dir, database), database)
    deliver(raw_dir, (2023, 11))

    delivery = plan_delivery(raw_dir, database)
    assert delivery.new_months == ("2023-11",)
    assert {file.relative_path for file in delivery.new_files} == {
        "core_banking/transactions/transactions_2023-11.csv",
        "core_banking/fees/fees_2023-11.csv",
        "core_banking/account_balances/account_balances_2023-11.csv",
    }
    # Snapshots and year-to-date files are replaced. October's fees come again
    # because one fee was waiting for a transaction that posted in November.
    assert {file.relative_path for file in delivery.changed_files} == {
        "core_banking/accounts/accounts.csv",
        "core_banking/customers/customers.csv",
        "core_banking/fees/fees_2023-10.csv",
        "finance/acquisition_spend/acquisition_spend_2023.csv",
        "finance/revenue_budget/revenue_budget_2023.csv",
        "treasury/funds_transfer_rates/funds_transfer_rates.csv",
    }
    assert delivery.months == ("2023-10", "2023-11")
    assert delivery.describe() == (
        "3 new files for 2023-11 and 6 files delivered again with changes"
    )


def test_the_fingerprint_changes_when_a_pending_file_is_delivered_again(raw_dir, database):
    first = plan_delivery(raw_dir, database).fingerprint
    assert plan_delivery(raw_dir, database).fingerprint == first
    keep_rows(raw_dir / OCTOBER, 5)
    assert plan_delivery(raw_dir, database).fingerprint != first


def test_a_clean_delivery_passes_every_check(raw_dir, database):
    delivery = plan_delivery(raw_dir, database)
    assert check_delivery_is_complete(delivery).passed
    assert check_headers_match_contract(delivery).passed
    assert check_row_counts_match_control_file(delivery).passed
    assert check_monthly_volume_is_plausible(delivery).passed


def test_a_missing_file_makes_the_delivery_incomplete(raw_dir, database):
    (raw_dir / "core_banking/account_balances/account_balances_2023-10.csv").unlink()
    outcome = check_delivery_is_complete(plan_delivery(raw_dir, database))
    assert not outcome.passed
    assert any("listed in the control file but was not delivered" in p for p in outcome.problems)
    assert "2023-10 has no account_balances file" in outcome.problems


def test_a_delivery_without_a_control_file_is_incomplete(raw_dir, database):
    (raw_dir / "_manifest.json").unlink()
    delivery = plan_delivery(raw_dir, database)
    assert not check_delivery_is_complete(delivery).passed
    assert check_row_counts_match_control_file(delivery).passed


def test_unlisted_and_misnamed_files_make_the_delivery_incomplete(raw_dir, database):
    folder = raw_dir / "core_banking/transactions"
    (folder / "transactions_2023-10.csv").rename(folder / "transactions_oct.csv")
    outcome = check_delivery_is_complete(plan_delivery(raw_dir, database))
    assert not outcome.passed
    problems = "\n".join(outcome.problems)
    assert "transactions_oct.csv was delivered but is not listed" in problems
    assert "does not follow the name transactions_YYYY-MM.csv" in problems


def test_a_renamed_column_fails_the_header_check(raw_dir, database):
    path = raw_dir / OCTOBER
    path.write_text(path.read_text().replace("posted_date", "posting_date", 1))
    outcome = check_headers_match_contract(plan_delivery(raw_dir, database))
    assert not outcome.passed
    assert outcome.problems == (f"{OCTOBER}: missing posted_date; unexpected posting_date",)


def test_the_header_check_ignores_files_that_are_already_loaded(raw_dir, database):
    load_delivery(plan_delivery(raw_dir, database), database)
    assert check_headers_match_contract(plan_delivery(raw_dir, database)).passed


def test_a_truncated_file_fails_the_control_count(raw_dir, database):
    declared = plan_delivery(raw_dir, database).control[OCTOBER]
    keep_rows(raw_dir / OCTOBER, declared // 2)
    outcome = check_row_counts_match_control_file(plan_delivery(raw_dir, database))
    assert not outcome.passed
    assert outcome.problems == (
        f"{OCTOBER}: control file says {declared:,} rows, the file has {declared // 2:,}",
    )


def test_a_small_extract_that_the_source_counted_correctly_fails_only_the_volume_check(
    raw_dir, database
):
    declared = plan_delivery(raw_dir, database).control[OCTOBER]
    keep_rows(raw_dir / OCTOBER, declared // 10)
    set_control_rows(raw_dir, OCTOBER, declared // 10)

    delivery = plan_delivery(raw_dir, database)
    assert check_row_counts_match_control_file(delivery).passed
    outcome = check_monthly_volume_is_plausible(delivery)
    assert not outcome.passed
    assert len(outcome.problems) == 1 and outcome.problems[0].startswith(OCTOBER)


def test_the_volume_check_skips_the_first_months_of_a_feed(raw_dir, database):
    """Early months grow too fast to compare: June is several times January to March."""
    first_six = [f"2023-{month:02d}" for month in range(1, 7)]
    for file in scan(raw_dir):
        if file.feed.cadence == MONTHLY and file.period not in first_six:
            file.path.unlink()
    path = raw_dir / "core_banking/transactions/transactions_2023-06.csv"
    keep_rows(path, 1)
    assert check_monthly_volume_is_plausible(plan_delivery(raw_dir, database)).passed
