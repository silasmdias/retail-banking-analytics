"""Properties the rest of the project relies on.

The simulations here are small (a few months, a fraction of the customers) so
the suite runs in a couple of seconds.
"""

from __future__ import annotations

import csv
import json

from generator import config as cfg
from generator.simulation import Settings, month_index, simulate
from generator.writer import HEADERS, write_dataset

FIRST_MONTH = month_index(2023, 1)


def small(**overrides) -> Settings:
    settings = {"start_month": FIRST_MONTH, "end_month": month_index(2023, 8), "scale": 0.15}
    return Settings(**(settings | overrides))


def all_rows(partitions: dict) -> list[tuple]:
    return [row for month in sorted(partitions) for row in partitions[month]]


def test_same_seed_gives_identical_data():
    first, second = simulate(small()), simulate(small())
    assert first.customers == second.customers
    assert first.accounts == second.accounts
    assert dict(first.transactions) == dict(second.transactions)
    assert dict(first.fees) == dict(second.fees)
    assert dict(first.revenue_budget) == dict(second.revenue_budget)


def test_different_seed_gives_different_data():
    assert simulate(small(seed=1)).customers != simulate(small(seed=2)).customers


def test_extending_the_end_month_does_not_rewrite_history():
    """A later run must only add files, so a scheduled load sees stable history."""
    short_end = month_index(2023, 6)
    short, extended = simulate(small(end_month=short_end)), simulate(small())
    for month in range(FIRST_MONTH, short_end + 1):
        assert extended.transactions[month] == short.transactions[month]
        assert extended.fees[month] == short.fees[month]
        assert extended.balances[month] == short.balances[month]
    assert extended.customers[: len(short.customers)] == short.customers
    assert len(extended.customers) > len(short.customers)

    def budget_through(data, last_month):
        last = f"{last_month // 12:04d}-{last_month % 12 + 1:02d}-01"
        return [row for row in all_rows(data.revenue_budget) if row[0] <= last]

    assert budget_through(extended, short_end) == budget_through(short, short_end)
    assert all_rows(extended.acquisition_spend)[: len(all_rows(short.acquisition_spend))] == (
        all_rows(short.acquisition_spend)
    )


def test_a_default_charges_off_every_credit_account_of_the_customer():
    data = simulate(Settings(start_month=FIRST_MONTH, end_month=month_index(2025, 12), scale=0.4))
    status_by_customer: dict[str, set[tuple[str, str]]] = {}
    for row in data.accounts:
        customer_id, product_id, closed_date, status = row[1], row[2], row[4], row[5]
        status_by_customer.setdefault(customer_id, set()).add((product_id, closed_date, status))
    defaulted = [
        rows for rows in status_by_customer.values() if any(r[2] == "charged_off" for r in rows)
    ]
    assert defaulted, "the simulation should produce at least one default"
    for rows in defaulted:
        default_date = next(r[1] for r in rows if r[2] == "charged_off")
        for product_id, closed_date, status in rows:
            is_credit = cfg.PRODUCTS[product_id].balance_side == "asset"
            if is_credit and closed_date == default_date:
                assert status == "charged_off"


def test_injected_problems_are_counted_exactly():
    data = simulate(small())
    transactions = all_rows(data.transactions)
    ids = [row[0] for row in transactions]
    assert len(ids) - len(set(ids)) == data.dirt_counts["duplicate_transaction"]
    unique_rows = set(transactions)
    assert sum(row[6] == "" for row in unique_rows) == data.dirt_counts["null_amount"]
    assert (
        sum(row[1].startswith("ACC-9") for row in unique_rows) == data.dirt_counts["orphan_account"]
    )
    assert sum("/" in row[2] for row in unique_rows) == data.dirt_counts["alternate_date_format"]


def test_dirt_can_be_switched_off():
    data = simulate(small(dirt=cfg.DirtRates.none()))
    assert not data.dirt_counts
    ids = [row[0] for row in all_rows(data.transactions)]
    assert len(ids) == len(set(ids))


def test_every_transaction_falls_inside_the_account_lifetime_when_clean():
    data = simulate(small(dirt=cfg.DirtRates.none()))
    opened = {row[0]: row[3] for row in data.accounts}
    closed = {row[0]: row[4] for row in data.accounts}
    for row in all_rows(data.transactions):
        account_id, transaction_date = row[1], row[2]
        assert transaction_date >= opened[account_id]
        assert not closed[account_id] or transaction_date <= closed[account_id]


def test_no_column_holds_personal_data():
    """The dataset is public: no names, contact details or document numbers."""
    forbidden = ("name", "email", "phone", "address", "document", "ssn", "tax_id", "birth_date")
    columns = {column for header in HEADERS.values() for column in header}
    columns.discard("product_name")
    assert not [column for column in columns if any(word in column for word in forbidden)]


def test_written_files_match_the_manifest(tmp_path):
    settings = small()
    manifest = write_dataset(simulate(settings), settings, tmp_path)
    assert json.loads((tmp_path / "_manifest.json").read_text()) == manifest
    for table, expected in manifest["rows_written"].items():
        files = list(tmp_path.glob(f"*/{table}/*.csv"))
        assert files, table
        written = 0
        for path in files:
            with path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.reader(handle))
            assert tuple(rows[0]) == HEADERS[table]
            written += len(rows) - 1
        assert written == expected
