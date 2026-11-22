"""Everything Dagster loads: `dg dev` and the headless runner both start here."""

from __future__ import annotations

import dagster as dg

from orchestration.assets import (
    dbt_build_marts,
    dbt_resource,
    dbt_validate_inputs,
    delivery,
    raw_tables,
)
from orchestration.automation import (
    alert_on_failed_run,
    alert_on_warnings,
    delivery_job,
    freshness_job,
    freshness_schedule,
    new_delivery_sensor,
)
from orchestration.checks import rejection_rate_drift, source_freshness

defs = dg.Definitions(
    assets=[delivery, raw_tables, dbt_validate_inputs, dbt_build_marts],
    asset_checks=[source_freshness, rejection_rate_drift],
    jobs=[delivery_job, freshness_job],
    schedules=[freshness_schedule],
    sensors=[new_delivery_sensor, alert_on_failed_run, alert_on_warnings],
    resources={"dbt": dbt_resource()},
    # DuckDB accepts one writing process at a time, so the steps of a run execute
    # one after the other in a single process.
    executor=dg.in_process_executor,
)
