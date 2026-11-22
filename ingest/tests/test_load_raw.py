"""The full refresh and the incremental load must leave the same warehouse."""

from __future__ import annotations

import duckdb
import pytest

from ingest.contract import FEEDS
from ingest.delivery import plan_delivery, scan
from ingest.ledger import read_ledger
from ingest.load_raw import load, load_delivery
from ingest.tests.conftest import deliver, keep_rows

OCTOBER = "core_banking/transactions/transactions_2023-10.csv"


def table_contents(database) -> dict[str, list[tuple]]:
    """Every raw table without the load timestamp, in a stable order."""
    with duckdb.connect(str(database), read_only=True) as connection:
        return {
            feed.relation: connection.execute(
                f"select * exclude (_loaded_at) from {feed.relation} order by all"
            ).fetchall()
            for feed in FEEDS
        }


def test_full_refresh_records_every_file_in_the_ledger(raw_dir, database):
    row_counts = load(raw_dir, database)
    ledger = read_ledger(database)
    landed = {file.relative_path: file for file in scan(raw_dir)}
    assert set(ledger) == set(landed)
    for relative_path, entry in ledger.items():
        assert entry.sha256 == landed[relative_path].sha256
        assert entry.loaded_rows == entry.file_rows == landed[relative_path].rows
    assert sum(entry.loaded_rows for entry in ledger.values()) == sum(row_counts.values())
    assert plan_delivery(raw_dir, database).pending == ()


def test_incremental_load_from_empty_equals_a_full_refresh(raw_dir, database, tmp_path):
    load(raw_dir, database)
    incremental = tmp_path / "incremental.duckdb"
    entries = load_delivery(plan_delivery(raw_dir, incremental), incremental, load_id="first")
    assert table_contents(incremental) == table_contents(database)
    assert {entry.load_id for entry in entries} == {"first"}
    assert set(read_ledger(incremental, load_id="first")) == set(read_ledger(database))


def test_loading_month_by_month_equals_a_full_refresh(raw_dir, database, tmp_path):
    """History in one go, then one more month, must match loading it all from scratch."""
    load_delivery(plan_delivery(raw_dir, database), database)
    deliver(raw_dir, (2023, 11))
    entries = load_delivery(plan_delivery(raw_dir, database), database)
    assert "core_banking/transactions/transactions_2023-11.csv" in {
        entry.relative_path for entry in entries
    }
    assert OCTOBER not in {entry.relative_path for entry in entries}

    from_scratch = tmp_path / "from_scratch.duckdb"
    load(raw_dir, from_scratch)
    assert table_contents(database) == table_contents(from_scratch)


def test_loading_twice_changes_nothing(raw_dir, database):
    load_delivery(plan_delivery(raw_dir, database), database)
    before = table_contents(database)
    assert load_delivery(plan_delivery(raw_dir, database), database) == []
    assert table_contents(database) == before


def test_a_file_delivered_again_replaces_its_own_rows(raw_dir, database):
    load_delivery(plan_delivery(raw_dir, database), database)
    keep_rows(raw_dir / OCTOBER, 7)

    delivery = plan_delivery(raw_dir, database)
    assert [file.relative_path for file in delivery.pending] == [OCTOBER]
    [entry] = load_delivery(delivery, database)
    assert entry.loaded_rows == 7

    with duckdb.connect(str(database), read_only=True) as connection:
        loaded = connection.execute(
            'select count(*) from "raw_core_banking"."transactions" '
            "where _source_file = 'transactions_2023-10.csv'"
        ).fetchone()[0]
        september = connection.execute(
            'select count(*) from "raw_core_banking"."transactions" '
            "where _source_file = 'transactions_2023-09.csv'"
        ).fetchone()[0]
    assert loaded == 7
    assert september > 7


def test_only_reloaded_rows_get_a_new_load_timestamp(raw_dir, database):
    load_delivery(plan_delivery(raw_dir, database), database, load_id="first")
    with duckdb.connect(str(database)) as connection:
        connection.execute(
            'update "raw_core_banking"."transactions" set _loaded_at = timestamp \'2020-01-01\''
        )
    keep_rows(raw_dir / OCTOBER, 7)
    load_delivery(plan_delivery(raw_dir, database), database, load_id="second")
    with duckdb.connect(str(database), read_only=True) as connection:
        fresh = connection.execute(
            'select distinct _source_file from "raw_core_banking"."transactions" '
            "where _loaded_at > timestamp '2020-01-01'"
        ).fetchall()
    assert fresh == [("transactions_2023-10.csv",)]


def test_a_failed_load_leaves_the_warehouse_untouched(raw_dir, database):
    load_delivery(plan_delivery(raw_dir, database), database)
    before = table_contents(database)
    ledger_before = read_ledger(database)

    deliver(raw_dir, (2023, 11))
    path = raw_dir / "core_banking/transactions/transactions_2023-11.csv"
    path.write_text(path.read_text().replace("posted_date", "posting_date", 1))

    with pytest.raises(duckdb.Error):
        load_delivery(plan_delivery(raw_dir, database), database)
    assert table_contents(database) == before
    assert read_ledger(database) == ledger_before
