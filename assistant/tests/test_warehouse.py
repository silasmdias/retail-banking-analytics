"""The copy of the marts: what a query can reach, and how it lives next to the pipeline."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import duckdb
import pytest

from assistant.tests.conftest import SECRET
from assistant.warehouse import Marts, QueryFailed, QueryRejected, WarehouseUnavailable

ADD_A_PRODUCT = "INSERT INTO marts.dim_products VALUES ('SAV_HY', 'High-Yield Savings', 'savings')"
COUNT_PRODUCTS = "SELECT count(*) FROM marts.dim_products"


# --------------------------------------------------------------------------- #
# What a query can reach
# --------------------------------------------------------------------------- #


def test_a_select_returns_json_safe_rows(marts: Marts) -> None:
    result = marts.query(
        "SELECT reporting_month, sales_channel, total_revenue "
        "FROM marts.revenue_by_product_channel ORDER BY total_revenue"
    )
    assert result.columns == ("reporting_month", "sales_channel", "total_revenue")
    assert result.rows[0] == ["2025-01-01", "branch", 100.5]
    assert not result.truncated


def test_a_result_is_cut_at_max_rows_and_says_so(marts: Marts) -> None:
    result = marts.query("SELECT * FROM marts.revenue_by_product_channel", max_rows=2)
    assert len(result.rows) == 2 and result.truncated


def test_the_copy_holds_the_marts_and_nothing_else(marts: Marts) -> None:
    result = marts.query("SELECT DISTINCT table_schema FROM information_schema.tables")
    assert result.rows == [["marts"]]
    assert marts.copy().row_counts == {
        "dim_products": 2,
        "fct_account_monthly_profitability": 2,
        "revenue_by_product_channel": 3,
    }
    assert marts.copy().column_types["revenue_by_product_channel"]["total_revenue"] == (
        "decimal(18,2)"
    )


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM raw_core_banking.customers",
        "SELECT * FROM staging.stg_core_banking__customers",
        "SELECT note FROM customers",
    ],
)
def test_raw_and_staging_tables_do_not_exist_for_the_server(marts: Marts, sql: str) -> None:
    with pytest.raises(QueryFailed, match="Only the marts can be queried") as error:
        marts.query(sql)
    assert "marts.dim_products" in str(error.value)


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO marts.dim_products VALUES ('X', 'X', 'X')",
        "UPDATE marts.dim_products SET product_name = 'X'",
        "DELETE FROM marts.dim_products",
        "DROP TABLE marts.dim_products",
        "CREATE TABLE marts.copy AS SELECT * FROM marts.dim_products",
        "COPY marts.dim_products TO 'products.csv'",
        "ATTACH 'other.duckdb' AS other",
        "PRAGMA database_size",
        "SET enable_external_access = true",
        "INSTALL httpfs",
        "CALL pragma_version()",
        "EXPORT DATABASE 'dump'",
        "EXPLAIN SELECT 1",
    ],
)
def test_anything_but_a_select_is_refused(marts: Marts, sql: str) -> None:
    with pytest.raises(QueryRejected, match="Only SELECT statements are allowed"):
        marts.query(sql)
    assert marts.query(COUNT_PRODUCTS).rows == [[2]]


def test_one_statement_per_call(marts: Marts) -> None:
    with pytest.raises(QueryRejected, match="one statement per call"):
        marts.query("SELECT 1; DROP TABLE marts.dim_products")
    with pytest.raises(QueryRejected, match="empty"):
        marts.query("  -- nothing here\n")
    assert marts.query(COUNT_PRODUCTS).rows == [[2]]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM read_csv('{file}')",
        "SELECT * FROM read_text('{file}')",
        "SELECT * FROM glob('{folder}/*')",
        "SELECT * FROM '{file}'",
    ],
)
def test_a_select_cannot_read_files(marts: Marts, warehouse_file: Path, sql: str) -> None:
    secret_file = warehouse_file.with_name("notes.csv")
    secret_file.write_text(f"note\n{SECRET}\n", encoding="utf-8")
    with pytest.raises((QueryRejected, QueryFailed)) as error:
        marts.query(sql.format(file=secret_file.as_posix(), folder=secret_file.parent.as_posix()))
    assert SECRET not in str(error.value)


def test_the_engine_refuses_even_when_the_statement_check_is_skipped(
    marts: Marts, warehouse_file: Path
) -> None:
    """The second fence: the database itself cannot reach outside the copy."""
    connection = marts.copy().connection
    with pytest.raises(duckdb.CatalogException):
        connection.execute("SELECT * FROM raw_core_banking.customers")
    with pytest.raises(duckdb.PermissionException):
        connection.execute(f"ATTACH '{warehouse_file.as_posix()}' AS source (READ_ONLY)")
    with pytest.raises(duckdb.PermissionException):
        connection.execute("COPY marts.dim_products TO 'products.csv'")
    with pytest.raises(duckdb.Error, match="locked"):
        connection.execute("SET enable_external_access = true")


def test_a_broken_query_returns_what_duckdb_said(marts: Marts) -> None:
    with pytest.raises(QueryFailed, match="could not parse"):
        marts.query("SELEC 1")
    with pytest.raises(QueryFailed, match="product_name"):  # DuckDB suggests the real column
        marts.query("SELECT product_nam FROM marts.dim_products")


def test_a_query_that_runs_too_long_is_stopped(marts: Marts) -> None:
    marts.timeout_seconds = 0.3
    with pytest.raises(QueryFailed, match="was stopped"):
        marts.query("SELECT count(*) FROM range(100000000000) a, range(1000) b")
    assert marts.query(COUNT_PRODUCTS).rows == [[2]]


# --------------------------------------------------------------------------- #
# Living next to the pipeline
# --------------------------------------------------------------------------- #


def test_the_server_does_not_hold_the_warehouse_file(marts: Marts, warehouse_file: Path) -> None:
    """After the copy, another process can open the file for writing, as dbt does."""
    marts.copy()
    write = "import sys, duckdb; duckdb.connect(sys.argv[1]).execute(sys.argv[2])"
    done = subprocess.run(
        [sys.executable, "-c", write, str(warehouse_file), ADD_A_PRODUCT],
        capture_output=True,
        text=True,
    )
    assert done.returncode == 0, done.stderr


def test_a_changed_warehouse_is_copied_again(marts: Marts, warehouse_file: Path) -> None:
    assert marts.query(COUNT_PRODUCTS).rows == [[2]]
    with duckdb.connect(str(warehouse_file)) as connection:
        connection.execute(ADD_A_PRODUCT)
    assert marts.query(COUNT_PRODUCTS).rows == [[3]]
    assert marts.notice is None


def test_while_the_pipeline_writes_the_last_copy_keeps_answering(
    marts: Marts, warehouse_file: Path, writer: Callable
) -> None:
    assert marts.query(COUNT_PRODUCTS).rows == [[2]]
    with writer(warehouse_file, ADD_A_PRODUCT):
        assert marts.query(COUNT_PRODUCTS).rows == [[2]]
        assert "being written by the pipeline" in marts.notice
    assert marts.query(COUNT_PRODUCTS).rows == [[3]]
    assert marts.notice is None


def test_a_warehouse_changed_moments_ago_is_left_alone(marts: Marts, warehouse_file: Path) -> None:
    """Between two steps of a run the file is free, but the run is not over."""
    assert marts.query(COUNT_PRODUCTS).rows == [[2]]
    marts.quiet_seconds = 60
    with duckdb.connect(str(warehouse_file)) as connection:
        connection.execute(ADD_A_PRODUCT)
    assert marts.query(COUNT_PRODUCTS).rows == [[2]]
    assert "is being updated" in marts.notice
    marts.quiet_seconds = 0
    assert marts.query(COUNT_PRODUCTS).rows == [[3]]


def test_with_no_copy_yet_a_writing_pipeline_is_reported(
    marts: Marts, warehouse_file: Path, writer: Callable
) -> None:
    with writer(warehouse_file, ADD_A_PRODUCT):
        with pytest.raises(WarehouseUnavailable, match="pipeline is writing"):
            marts.query(COUNT_PRODUCTS)
    assert marts.query(COUNT_PRODUCTS).rows == [[3]]


def test_a_missing_warehouse_says_how_to_build_it(tmp_path: Path) -> None:
    marts = Marts(tmp_path / "warehouse.duckdb", ["dim_products"])
    with pytest.raises(WarehouseUnavailable, match="make build"):
        marts.query("SELECT 1")


def test_a_warehouse_without_marts_says_how_to_build_them(tmp_path: Path) -> None:
    path = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE SCHEMA raw_core_banking")
    with pytest.raises(WarehouseUnavailable, match="no marts yet"):
        Marts(path, ["dim_products"], wait_seconds=0).query("SELECT 1")


def test_a_mart_that_is_not_built_yet_is_left_out(warehouse_file: Path) -> None:
    marts = Marts(warehouse_file, ["dim_products", "customer_lifetime_value"])
    assert list(marts.copy().row_counts) == ["dim_products"]
