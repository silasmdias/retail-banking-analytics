"""End-to-end runs of the whole pipeline on a small dataset: Dagster, DuckDB and dbt.

The first load of 15 months runs once for the module. Each test starts from a
copy of that warehouse and landing folder and delivers April 2024 on top of it.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import dagster as dg
import duckdb
import pytest

from ingest.ledger import read_ledger
from orchestration.alerts import ALERTS_FILE, ERROR
from orchestration.demo import (
    cut_in_half,
    latest_published_month,
    point_to_unknown_accounts,
    transactions_file,
)
from orchestration.paths import Paths, paths
from orchestration.run_once import RunOutcome, process_pending
from orchestration.tests.conftest import deliver

VARIABLES = {"RAW_DIR": "raw", "WAREHOUSE_PATH": "warehouse.duckdb", "ALERTS_DIR": "alerts"}


def point_at(monkeypatch: pytest.MonkeyPatch, folder: Path) -> Paths:
    for variable, name in VARIABLES.items():
        monkeypatch.setenv(variable, str(folder / name))
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    return paths()


@pytest.fixture(scope="module")
def first_load(tmp_path_factory) -> tuple[Path, RunOutcome]:
    """January 2023 to March 2024, delivered and processed in one run."""
    folder = tmp_path_factory.mktemp("first_load")
    with pytest.MonkeyPatch.context() as monkeypatch:
        locations = point_at(monkeypatch, folder)
        deliver(locations.raw_dir, (2024, 3))
        with dg.DagsterInstance.ephemeral() as instance:
            outcome = process_pending(instance)
    return folder, outcome


@pytest.fixture
def workspace(first_load, tmp_path, monkeypatch) -> Paths:
    folder, _ = first_load
    shutil.copytree(folder, tmp_path / "workspace")
    return point_at(monkeypatch, tmp_path / "workspace")


@pytest.fixture
def instance():
    with dg.DagsterInstance.ephemeral() as instance:
        yield instance


def revenue(warehouse: Path) -> tuple:
    with duckdb.connect(str(warehouse), read_only=True) as connection:
        return connection.execute(
            'select count(*), sum(total_revenue) from "marts"."revenue_by_product_channel"'
        ).fetchone()


def test_the_first_delivery_loads_every_file_and_builds_the_marts(first_load, workspace):
    _, outcome = first_load
    assert outcome.success, outcome.report()
    assert set(outcome.steps.values()) == {"succeeded"}
    assert outcome.checks_failed == 0 and outcome.alerts == []
    assert latest_published_month(workspace.warehouse) == "2024-03"
    ledger = read_ledger(workspace.warehouse)
    assert set(ledger) == {file.relative_path for file in outcome.plan.landed}
    assert {entry.load_id for entry in ledger.values()} == {outcome.run_id}


def test_the_next_month_loads_only_new_and_changed_files(workspace, instance):
    deliver(workspace.raw_dir, (2024, 4))
    outcome = process_pending(instance)
    assert outcome.success, outcome.report()
    assert outcome.alerts == []
    assert outcome.plan.new_months == ("2024-04",)
    loaded = read_ledger(workspace.warehouse, load_id=outcome.run_id)
    assert set(loaded) == {file.relative_path for file in outcome.plan.pending}
    assert len(loaded) < len(outcome.plan.landed) / 3
    assert latest_published_month(workspace.warehouse) == "2024-04"
    assert not (workspace.alerts_dir / ALERTS_FILE).exists()


def test_broken_deliveries_stop_the_run_raise_an_alert_and_leave_the_marts_alone(
    workspace, instance
):
    revenue_before = revenue(workspace.warehouse)
    april = transactions_file(workspace.raw_dir, "2024-04")

    # A file cut short on the way: stopped before anything is loaded.
    deliver(workspace.raw_dir, (2024, 4))
    cut_in_half(april)
    truncated = process_pending(instance)
    assert not truncated.success
    assert truncated.steps["landing__delivery"] == "failed"
    assert {s for step, s in truncated.steps.items() if step != "landing__delivery"} == {"not run"}
    assert [(a.severity, a.source) for a in truncated.alerts if a.severity == ERROR] == [
        (ERROR, "landing/delivery: row_counts_match_control_file")
    ]
    assert "core_banking/transactions/transactions_2024-04.csv" not in read_ledger(
        workspace.warehouse
    )

    # A complete file with wrong content: loaded as sent, stopped before the marts.
    deliver(workspace.raw_dir, (2024, 4))
    point_to_unknown_accounts(april, one_in=20)
    wrong = process_pending(instance)
    assert not wrong.success
    assert wrong.steps["raw_tables"] == "succeeded"
    assert wrong.steps["dbt_validate_inputs"] == "failed"
    assert wrong.steps["dbt_build_marts"] == "not run"
    [alert] = wrong.alerts
    assert alert.severity == ERROR
    assert alert.source == "marts/dq_rejected_records: rejection_rate_within_limit"
    assert "More than 1% of a month's records rejected" in alert.summary

    assert latest_published_month(workspace.warehouse) == "2024-03"
    assert revenue(workspace.warehouse) == revenue_before

    on_file = [
        json.loads(line) for line in (workspace.alerts_dir / ALERTS_FILE).read_text().splitlines()
    ]
    assert [row["alert_id"] for row in on_file] == [
        a.alert_id for a in truncated.alerts + wrong.alerts
    ]

    # The corrected file: only it is loaded, and the month is published.
    deliver(workspace.raw_dir, (2024, 4))
    fixed = process_pending(instance)
    assert fixed.success, fixed.report()
    assert [file.path for file in fixed.plan.pending] == [april]
    assert fixed.alerts == []
    assert latest_published_month(workspace.warehouse) == "2024-04"
