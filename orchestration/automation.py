"""What starts a run and what happens after it: jobs, sensors and the schedule."""

import dagster as dg
import duckdb

from ingest.delivery import CONTROL_FILE, plan_delivery
from orchestration.alerts import notify_run
from orchestration.checks import source_freshness
from orchestration.paths import paths

delivery_job = dg.define_asset_job(
    "process_delivery",
    selection=dg.AssetSelection.all() | dg.AssetSelection.all_asset_checks(),
    description=(
        "Checks the files in the landing folder, loads the new ones, validates them with "
        "dbt and rebuilds the marts."
    ),
)

freshness_job = dg.define_asset_job(
    "check_source_freshness",
    selection=dg.AssetSelection.checks(source_freshness),
    description="Asks whether the monthly feeds are still arriving. Loads and builds nothing.",
)

# A delivery starts a run when it lands. This schedule covers the opposite case:
# nothing lands, so nothing runs, and only a clock can notice.
freshness_schedule = dg.ScheduleDefinition(
    name="daily_source_freshness",
    job=freshness_job,
    cron_schedule="0 7 * * *",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


@dg.sensor(
    job=delivery_job,
    minimum_interval_seconds=30,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description=(
        "Starts a run when the landing folder holds files the warehouse has not loaded, "
        "or files delivered again with different content."
    ),
)
def new_delivery_sensor() -> dg.RunRequest | dg.SkipReason:
    locations = paths()
    if not (locations.raw_dir / CONTROL_FILE).is_file():
        return dg.SkipReason(
            "No control file in the landing folder: nothing was delivered, or a delivery "
            "is still being written."
        )
    try:
        plan = plan_delivery(locations.raw_dir, locations.warehouse)
    except duckdb.IOException:
        return dg.SkipReason("The warehouse is in use by a running job. Will look again.")
    if not plan.pending:
        return dg.SkipReason("Every file in the landing folder is already loaded.")

    # The run key is the fingerprint of the pending files. A delivery that failed
    # its checks is not retried every 30 seconds; it runs again when a file changes.
    return dg.RunRequest(
        run_key=plan.fingerprint,
        tags={"delivery/months": ", ".join(plan.months), "delivery/files": str(len(plan.pending))},
    )


def _raise_alerts(context: dg.RunStatusSensorContext) -> None:
    for alert in notify_run(context.instance, context.dagster_run.run_id, log=context.log.warning):
        context.log.warning(alert.as_text())


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.FAILURE,
    name="alert_on_failed_run",
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="Raises an alert for every failed check and failed step of a run that failed.",
)
def alert_on_failed_run(context: dg.RunStatusSensorContext) -> None:
    _raise_alerts(context)


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    name="alert_on_warnings",
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="Raises an alert for the warnings of a run that finished.",
)
def alert_on_warnings(context: dg.RunStatusSensorContext) -> None:
    _raise_alerts(context)
