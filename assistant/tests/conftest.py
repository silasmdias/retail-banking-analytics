"""A small warehouse and a small dbt manifest for every test, in a temp dir.

The warehouse has the same layers as the real one: a raw table, a staging view
and three marts. The raw table holds a value no test should ever get back.
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest

from assistant.catalog import Catalog, load_catalog
from assistant.warehouse import Marts

SECRET = "never-leaves-the-raw-layer"

WAREHOUSE_SQL = f"""
CREATE SCHEMA raw_core_banking;
CREATE TABLE raw_core_banking.customers AS
    SELECT 'C1' AS customer_id, '{SECRET}' AS note;

CREATE SCHEMA staging;
CREATE VIEW staging.stg_core_banking__customers AS SELECT * FROM raw_core_banking.customers;

CREATE SCHEMA marts;
CREATE TABLE marts.dim_products AS
    SELECT * FROM (VALUES
        ('CHK_STD', 'Everyday Checking', 'checking'),
        ('CC_STD', 'Classic Credit Card', 'credit_card')
    ) AS t(product_id, product_name, product_category);
CREATE TABLE marts.revenue_by_product_channel AS
    SELECT * FROM (VALUES
        (DATE '2025-01-01', 'CHK_STD', 'branch', 100.50::DECIMAL(18, 2)),
        (DATE '2025-01-01', 'CC_STD', 'online', 200.25::DECIMAL(18, 2)),
        (DATE '2025-02-01', 'CHK_STD', 'branch', 300.00::DECIMAL(18, 2))
    ) AS t(reporting_month, product_id, sales_channel, total_revenue);
CREATE TABLE marts.fct_account_monthly_profitability AS
    SELECT * FROM (VALUES
        ('A1', DATE '2024-11-01', 10.00::DECIMAL(18, 2)),
        ('A1', DATE '2025-02-01', 12.00::DECIMAL(18, 2))
    ) AS t(account_id, reporting_month, total_revenue);
"""


def _model(name: str, schema: str, area: str, description: str, columns: dict, parents=()) -> dict:
    return {
        "resource_type": "model",
        "package_name": "retail_banking",
        "name": name,
        "schema": schema,
        "fqn": ["retail_banking", "marts" if schema == "marts" else schema, area, name],
        "description": description,
        "columns": {
            column: {"name": column, "description": text, **({"data_type": kind} if kind else {})}
            for column, (text, kind) in columns.items()
        },
        "contract": {"enforced": name == "revenue_by_product_channel"},
        "depends_on": {"nodes": [f"model.retail_banking.{parent}" for parent in parents]},
    }


def _test(name: str, model: str, column: str, kwargs: dict | None = None) -> dict:
    return {
        "resource_type": "test",
        "package_name": "retail_banking",
        "name": f"{name}_{model}_{column}",
        "attached_node": f"model.retail_banking.{model}",
        "column_name": column,
        "test_metadata": {"name": name, "kwargs": kwargs or {}},
        "depends_on": {"nodes": [f"model.retail_banking.{model}"]},
    }


SALES_CHANNEL = "Channel through which a product was sold. Revenue is attributed to it."

MANIFEST = {
    "metadata": {"project_name": "retail_banking"},
    "nodes": {
        "model.retail_banking.stg_core_banking__customers": _model(
            "stg_core_banking__customers",
            "staging",
            "core_banking",
            "One row per customer, typed and deduplicated.",
            {"customer_id": ("Customer identifier.", None)},
        ),
        "model.retail_banking.dim_products": _model(
            "dim_products",
            "marts",
            "core",
            "One row per product.",
            {
                "product_id": ("Product identifier.", None),
                "product_name": ("Commercial name of the product.", None),
                "product_category": ("Product family.", None),
            },
        ),
        "model.retail_banking.revenue_by_product_channel": _model(
            "revenue_by_product_channel",
            "marts",
            "finance",
            "Monthly revenue by product and sales channel.\n\nSums to the account-level fact.",
            {
                "reporting_month": ("First day of the month.", "date"),
                "product_id": ("Product identifier.", "varchar"),
                "sales_channel": (SALES_CHANNEL, "varchar"),
                "total_revenue": ("Revenue in US dollars.", "decimal(18,2)"),
            },
            parents=["dim_products", "fct_account_monthly_profitability"],
        ),
        "model.retail_banking.fct_account_monthly_profitability": _model(
            "fct_account_monthly_profitability",
            "marts",
            "core",
            "One row per account and month.",
            {
                "account_id": ("Account identifier.", None),
                "reporting_month": ("First day of the month.", None),
                "total_revenue": ("Revenue in US dollars.", None),
            },
        ),
        "test.retail_banking.accepted": _test(
            "accepted_values",
            "dim_products",
            "product_category",
            {"values": ["checking", "credit_card"]},
        ),
        "test.retail_banking.unique": _test("unique", "dim_products", "product_id"),
        "test.retail_banking.not_null": _test("not_null", "dim_products", "product_id"),
    },
    "docs": {
        "doc.retail_banking.sales_channel": {
            "name": "sales_channel",
            "package_name": "retail_banking",
            "block_contents": SALES_CHANNEL,
        },
        "doc.retail_banking.contribution_margin": {
            "name": "contribution_margin",
            "package_name": "retail_banking",
            "block_contents": "Revenue minus credit losses minus cost to serve. "
            "Acquisition cost is excluded and reported separately as CAC.",
        },
        "doc.retail_banking.__overview__": {
            "name": "__overview__",
            "package_name": "retail_banking",
            "block_contents": "Project overview.",
        },
        "doc.dbt.__overview__": {
            "name": "__overview__",
            "package_name": "dbt",
            "block_contents": "dbt's own overview.",
        },
    },
}


@pytest.fixture
def warehouse_file(tmp_path: Path) -> Path:
    path = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute(WAREHOUSE_SQL)
    return path


@pytest.fixture
def manifest_file(tmp_path: Path) -> Path:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(MANIFEST), encoding="utf-8")
    return path


@pytest.fixture
def catalog(manifest_file: Path) -> Catalog:
    return load_catalog(manifest_file)


@pytest.fixture
def marts(warehouse_file: Path, catalog: Catalog) -> Marts:
    # No quiet period: a test changes the file and expects the next call to see it.
    return Marts(
        warehouse_file, [model.name for model in catalog.marts], quiet_seconds=0, wait_seconds=1
    )


WRITER = """
import sys, duckdb
connection = duckdb.connect(sys.argv[1])
connection.execute(sys.argv[2])
print("writing", flush=True)
sys.stdin.read()
connection.close()
"""


@pytest.fixture
def writer() -> Callable:
    """Another process that changes the warehouse and keeps it open, as a pipeline run does."""

    @contextmanager
    def hold(path: Path, sql: str) -> Iterator[None]:
        process = subprocess.Popen(
            [sys.executable, "-c", WRITER, str(path), sql],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            assert process.stdout.readline().strip() == "writing"
            yield
        finally:
            process.communicate("")

    return hold
