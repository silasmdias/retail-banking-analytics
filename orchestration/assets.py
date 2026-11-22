"""The assets of the pipeline, in the order a delivery flows through them.

    landing/delivery -> raw tables -> dbt: validate inputs -> dbt: build marts

1. ``delivery`` takes stock of the landing folder and checks the new files
   before anything is loaded. A failed blocking check stops the run here.
2. ``raw_tables`` loads the new and changed files into DuckDB and reconciles
   what was loaded against what was delivered.
3. ``dbt_validate_inputs`` runs the first part of the dbt project: sources,
   seeds, staging, record screening and the rejected-records report, with their
   tests. If a test fails, the run stops and the marts keep their last good state.
4. ``dbt_build_marts`` runs the rest: the remaining intermediate models and the marts.
"""

import shutil
import sys
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

from ingest.contract import FEEDS, Feed
from ingest.delivery import DELIVERY_CHECKS, Delivery, plan_delivery
from ingest.ledger import LedgerEntry
from ingest.load_raw import load_delivery
from orchestration.paths import DBT_PROJECT_DIR, paths

# --------------------------------------------------------------------------- #
# 1. The delivery
# --------------------------------------------------------------------------- #

DELIVERY_KEY = dg.AssetKey(["landing", "delivery"])


def _file_table(delivery: Delivery) -> str:
    if not delivery.pending:
        return "Nothing new."
    loaded = delivery.loaded
    lines = ["| File | Rows | Status |", "| --- | ---: | --- |"]
    for file in delivery.pending:
        status = "delivered again" if file.relative_path in loaded else "new"
        lines.append(f"| {file.relative_path} | {file.rows:,} | {status} |")
    return "\n".join(lines)


@dg.asset(
    key=DELIVERY_KEY,
    group_name="landing",
    kinds={"csv"},
    description=(
        "The files in the landing folder that the warehouse has not loaded yet: new files "
        "and files delivered again with different content. They are checked here, before "
        "the load."
    ),
    check_specs=[
        dg.AssetCheckSpec(
            check.name, asset=DELIVERY_KEY, description=check.description, blocking=check.blocking
        )
        for check in DELIVERY_CHECKS
    ],
)
def delivery(context: dg.AssetExecutionContext) -> Iterator[Any]:
    locations = paths()
    plan = plan_delivery(locations.raw_dir, locations.warehouse)
    context.log.info(f"Delivery: {plan.describe()}")
    yield dg.MaterializeResult(
        metadata={
            "summary": plan.describe(),
            "new_files": len(plan.new_files),
            "files_delivered_again": len(plan.changed_files),
            "months": ", ".join(plan.months) or "none",
            "fingerprint": plan.fingerprint,
            "files": dg.MetadataValue.md(_file_table(plan)),
        }
    )
    for check in DELIVERY_CHECKS:
        outcome = check.evaluate(plan)
        yield dg.AssetCheckResult(
            check_name=check.name,
            passed=outcome.passed,
            severity=dg.AssetCheckSeverity.ERROR if check.blocking else dg.AssetCheckSeverity.WARN,
            description=outcome.summary,
            metadata={"problems": list(outcome.problems)} if outcome.problems else {},
        )


# --------------------------------------------------------------------------- #
# 2. The raw tables
# --------------------------------------------------------------------------- #

LOAD_CHECK = "loaded_rows_match_delivery"


def raw_key(feed: Feed) -> dg.AssetKey:
    """Same key dagster-dbt gives the dbt source, so the lineage connects."""
    return dg.AssetKey([feed.system, feed.table])


def _load_check(feed: Feed, entries: list[LedgerEntry]) -> dg.AssetCheckResult:
    mismatches = [
        f"{entry.relative_path}: the file has {entry.file_rows:,} rows, "
        f"{entry.loaded_rows:,} were loaded"
        for entry in entries
        if entry.loaded_rows != entry.file_rows
    ]
    return dg.AssetCheckResult(
        asset_key=raw_key(feed),
        check_name=LOAD_CHECK,
        passed=not mismatches,
        description=(
            f"Loaded rows differ from the delivered files: {mismatches[0]}"
            if mismatches
            else f"{len(entries)} files loaded in full"
        ),
        metadata={"problems": mismatches} if mismatches else {},
    )


@dg.multi_asset(
    name="raw_tables",
    specs=[
        dg.AssetSpec(
            key=raw_key(feed),
            group_name="raw",
            kinds={"duckdb"},
            deps=[DELIVERY_KEY],
            description=(
                f"`{feed.schema}.{feed.table}`: the {feed.cadence} `{feed.table}` files, "
                "loaded as text with the file name and the load timestamp."
            ),
        )
        for feed in FEEDS
    ],
    check_specs=[
        dg.AssetCheckSpec(
            LOAD_CHECK,
            asset=raw_key(feed),
            blocking=True,
            description="Every file loaded in this run has as many rows in the table as "
            "it has in the file.",
        )
        for feed in FEEDS
    ],
)
def raw_tables(context: dg.AssetExecutionContext) -> Iterator[Any]:
    locations = paths()
    plan = plan_delivery(locations.raw_dir, locations.warehouse)
    entries = load_delivery(plan, locations.warehouse, load_id=context.run_id)
    context.log.info(f"Loaded {len(entries)} files ({sum(e.loaded_rows for e in entries):,} rows)")

    with duckdb.connect(str(locations.warehouse), read_only=True) as connection:
        for feed in FEEDS:
            loaded = [entry for entry in entries if entry.table_name == feed.table]
            try:
                total = connection.execute(f"select count(*) from {feed.relation}").fetchone()[0]
            except duckdb.CatalogException:
                total = 0
            yield dg.MaterializeResult(
                asset_key=raw_key(feed),
                metadata={
                    "dagster/row_count": total,
                    "files_loaded": len(loaded),
                    "rows_loaded": sum(entry.loaded_rows for entry in loaded),
                    "files": [entry.file_name for entry in loaded],
                },
            )
            yield _load_check(feed, loaded)


# --------------------------------------------------------------------------- #
# 3 and 4. The dbt project, in two steps
# --------------------------------------------------------------------------- #

# The Makefile installs the dbt packages once, so loading the definitions never
# needs the network.
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, install_deps=False)
dbt_project.prepare_if_dev()

if not dbt_project.manifest_path.exists():
    raise FileNotFoundError(
        f"{dbt_project.manifest_path} does not exist. Run `make manifest` to parse the "
        "dbt project first."
    )

# Everything that reads the raw tables and judges their quality. The marts are
# built only after this selection and its tests have passed.
INPUT_VALIDATION = " ".join(
    (
        "source:*",
        "resource_type:seed",
        "staging",
        "int_transactions_screened",
        "int_fees_screened",
        "dq_rejected_records",
    )
)


class LayerTranslator(DagsterDbtTranslator):
    """Groups dbt assets by layer: reference (seeds), staging, intermediate, marts."""

    def get_asset_spec(
        self, manifest: Mapping[str, Any], unique_id: str, project: DbtProject | None
    ) -> dg.AssetSpec:
        spec = super().get_asset_spec(manifest, unique_id, project)
        resource_type, _, _ = unique_id.partition(".")
        if resource_type == "seed":
            return spec.replace_attributes(group_name="reference")
        if resource_type == "model":
            layer = manifest["nodes"][unique_id]["fqn"][1]
            return spec.replace_attributes(group_name=layer)
        return spec


translator = LayerTranslator(
    settings=DagsterDbtTranslatorSettings(enable_source_tests_as_checks=True)
)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select=INPUT_VALIDATION,
    name="dbt_validate_inputs",
    project=dbt_project,
    dagster_dbt_translator=translator,
)
def dbt_validate_inputs(context: dg.AssetExecutionContext, dbt: DbtCliResource) -> Iterator[Any]:
    yield from dbt.cli(["build"], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    exclude=INPUT_VALIDATION,
    name="dbt_build_marts",
    project=dbt_project,
    dagster_dbt_translator=translator,
)
def dbt_build_marts(context: dg.AssetExecutionContext, dbt: DbtCliResource) -> Iterator[Any]:
    yield from dbt.cli(["build"], context=context).stream()


def dbt_resource() -> DbtCliResource:
    """Uses the dbt installed next to this Python, whatever the shell's PATH is."""
    beside_python = Path(sys.executable).with_name("dbt")
    executable = str(beside_python) if beside_python.exists() else shutil.which("dbt") or "dbt"
    return DbtCliResource(project_dir=dbt_project, dbt_executable=executable)
