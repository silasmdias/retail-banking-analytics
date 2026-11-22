"""How the pieces are wired: the order of the steps, the dbt split and the sensor."""

from __future__ import annotations

import json

import dagster as dg

from ingest.delivery import plan_delivery
from ingest.ledger import read_ledger
from ingest.load_raw import load_delivery
from orchestration import assets
from orchestration.assets import (
    dbt_build_marts,
    dbt_project,
    dbt_validate_inputs,
    delivery,
    raw_tables,
)
from orchestration.automation import delivery_job, new_delivery_sensor
from orchestration.definitions import defs
from orchestration.tests.conftest import deliver

MANIFEST = json.loads(dbt_project.manifest_path.read_text())


def test_the_definitions_load():
    dg.Definitions.validate_loadable(defs)


def test_every_dbt_model_and_seed_is_built_by_exactly_one_of_the_two_dbt_steps():
    nodes = [
        node["name"]
        for node in MANIFEST["nodes"].values()
        if node["resource_type"] in ("model", "seed")
    ]
    first = {key.path[-1] for key in dbt_validate_inputs.keys}
    second = {key.path[-1] for key in dbt_build_marts.keys}
    assert not first & second
    assert first | second == set(nodes)
    assert {"int_transactions_screened", "int_fees_screened", "dq_rejected_records"} <= first
    assert {key.path[0] for key in dbt_build_marts.keys} == {"intermediate", "marts"}


def test_every_dbt_test_is_a_check_except_the_two_that_compare_sources():
    tests = {node["name"] for node in MANIFEST["nodes"].values() if node["resource_type"] == "test"}
    checks = {key.name for key in dbt_validate_inputs.check_keys | dbt_build_marts.check_keys}
    assert checks <= tests
    # dagster-dbt cannot attach a test between two sources to an asset. dbt still
    # runs both, as warnings, when it validates the inputs.
    assert sorted(tests - checks) == [
        "source_relationships_core_banking_fees_account_id__account_id__source_core_banking_accounts_",
        "source_relationships_core_banking_transactions_account_id__account_id__source_core_banking_accounts_",
    ]


def test_the_marts_are_built_only_after_the_inputs_are_validated():
    job = defs.resolve_job_def(delivery_job.name)
    order = [node.name for node in job.nodes_in_topological_order]
    assert order.index("landing__delivery") < order.index("raw_tables")
    assert order.index("raw_tables") < order.index("dbt_validate_inputs")
    assert order.index("dbt_validate_inputs") < order.index("dbt_build_marts")
    dependencies = job.dependency_structure
    upstream = {
        handle.node_name
        for handles in dependencies.input_to_upstream_outputs_for_node("dbt_build_marts").values()
        for handle in handles
    }
    assert upstream == {"dbt_validate_inputs"}


def test_the_sensor_waits_for_the_control_file(workspace):
    workspace.raw_dir.mkdir(parents=True)
    result = new_delivery_sensor()
    assert isinstance(result, dg.SkipReason)
    assert "No control file" in result.skip_message


def test_the_sensor_asks_for_one_run_per_delivery_and_none_when_all_is_loaded(workspace):
    deliver(workspace.raw_dir, (2023, 4))
    first = new_delivery_sensor()
    assert isinstance(first, dg.RunRequest)
    assert first.run_key == plan_delivery(workspace.raw_dir, workspace.warehouse).fingerprint
    assert first.tags["delivery/months"] == "2023-01, 2023-02, 2023-03, 2023-04"
    # Asked again before the run, it gives the same key, so Dagster starts nothing new.
    assert new_delivery_sensor().run_key == first.run_key

    load_delivery(plan_delivery(workspace.raw_dir, workspace.warehouse), workspace.warehouse)
    assert isinstance(new_delivery_sensor(), dg.SkipReason)

    deliver(workspace.raw_dir, (2023, 5))
    second = new_delivery_sensor()
    assert isinstance(second, dg.RunRequest) and second.run_key != first.run_key


def loaded_files(workspace) -> int:
    return len(read_ledger(workspace.warehouse))


def test_the_load_takes_only_the_files_that_were_checked(workspace):
    deliver(workspace.raw_dir, (2023, 4))
    result = dg.materialize([delivery, raw_tables], raise_on_error=False)
    assert result.success
    assert loaded_files(workspace) == len(
        plan_delivery(workspace.raw_dir, workspace.warehouse).landed
    )


def test_the_load_refuses_to_start_without_a_checked_delivery(workspace):
    deliver(workspace.raw_dir, (2023, 4))
    result = dg.materialize([raw_tables], raise_on_error=False)
    assert not result.success
    assert loaded_files(workspace) == 0


def test_the_load_refuses_files_that_changed_after_the_check(workspace, monkeypatch):
    deliver(workspace.raw_dir, (2023, 4))
    april = workspace.raw_dir / "core_banking" / "fees" / "fees_2023-04.csv"
    calls = []

    def plan_then_tamper(raw_dir, database):
        """The first call is the check. Before the second, the load, a file is replaced."""
        if calls:
            april.write_text(
                april.read_text() + "FEE-LATE,ACC-000001-01,,overdraft,2023-04-30,34.00,posted\n"
            )
        calls.append(1)
        return plan_delivery(raw_dir, database)

    monkeypatch.setattr(assets, "plan_delivery", plan_then_tamper)
    result = dg.materialize([delivery, raw_tables], raise_on_error=False)
    assert not result.success
    assert len(calls) == 2
    assert loaded_files(workspace) == 0
