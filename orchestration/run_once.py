"""Processes whatever is in the landing folder, once, without the Dagster UI.

    python -m orchestration.run_once

It does in the foreground what `dg dev` does in the background: look at the
landing folder, run the `process_delivery` job and raise the alerts of that
run. Tests, the demos and CI use it. The exit code is 0 when the run succeeded.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import os
import sys
import time
from dataclasses import dataclass, field

import dagster as dg

from ingest.delivery import Delivery, plan_delivery
from orchestration.alerts import Alert, notify_run
from orchestration.automation import delivery_job
from orchestration.definitions import defs
from orchestration.paths import paths

# Dagster names a step after its asset or check key; these read better in a report.
STEP_LABELS = {
    "landing__delivery": "check the delivery",
    "raw_tables": "load the raw tables",
    "dbt_validate_inputs": "dbt: validate inputs",
    "source_freshness": "source freshness",
    "dbt_build_marts": "dbt: build marts",
    "marts__dq_rejected_records_rejection_rate_drift": "rejection rate drift",
}


@dataclass(frozen=True)
class RunOutcome:
    plan: Delivery
    run_id: str
    success: bool
    seconds: float
    steps: dict[str, str]  # step -> "succeeded", "failed" or "not run"
    checks_passed: int
    checks_warned: int
    checks_failed: int
    alerts: list[Alert] = field(default_factory=list)

    def report(self) -> str:
        lines = [
            f"Delivery: {self.plan.describe()}",
            f"Run {self.run_id[:8]} {'succeeded' if self.success else 'FAILED'} "
            f"in {self.seconds:.0f} seconds",
        ]
        labels = {step: STEP_LABELS.get(step, step) for step in self.steps}
        width = max(len(label) for label in labels.values())
        lines += [f"  {labels[step]:<{width}}  {status}" for step, status in self.steps.items()]
        lines.append(
            f"Checks: {self.checks_passed} passed, {self.checks_warned} warned, "
            f"{self.checks_failed} failed"
        )
        if not self.alerts:
            lines.append("Alerts: none")
        for alert in self.alerts:
            lines.append("Alert:")
            lines += [f"  {line}" for line in alert.as_text().splitlines()]
        return "\n".join(lines)


def get_instance() -> dg.DagsterInstance:
    """The instance in ``DAGSTER_HOME`` when set, so the UI shows these runs; else a throwaway."""
    if os.environ.get("DAGSTER_HOME"):
        return dg.DagsterInstance.get()
    return dg.DagsterInstance.ephemeral()


def _events(instance: dg.DagsterInstance, run_id: str, event_type: dg.DagsterEventType) -> list:
    records = []
    cursor = None
    while True:
        page = instance.get_records_for_run(run_id, cursor=cursor, of_type=event_type, limit=500)
        records += [record.event_log_entry.dagster_event for record in page.records]
        if not page.has_more:
            return records
        cursor = page.cursor


def process_pending(instance: dg.DagsterInstance, verbose: bool = False) -> RunOutcome:
    locations = paths()
    plan = plan_delivery(locations.raw_dir, locations.warehouse)
    job = defs.resolve_job_def(delivery_job.name)

    started = time.monotonic()
    # dbt's log lines reach stdout directly, so quiet mode has to swallow stdout as well.
    with contextlib.redirect_stdout(sys.stdout if verbose else io.StringIO()):
        result = job.execute_in_process(
            instance=instance,
            raise_on_error=False,
            tags={
                "delivery/months": ", ".join(plan.months),
                "delivery/files": str(len(plan.pending)),
            },
            run_config={
                "loggers": {
                    "console": {"config": {"log_level": "DEBUG" if verbose else "CRITICAL"}}
                }
            },
        )
    seconds = time.monotonic() - started
    run_id = result.run_id

    succeeded = {e.step_key for e in _events(instance, run_id, dg.DagsterEventType.STEP_SUCCESS)}
    failed = {e.step_key for e in _events(instance, run_id, dg.DagsterEventType.STEP_FAILURE)}
    steps = {
        node.name: "failed"
        if node.name in failed
        else "succeeded"
        if node.name in succeeded
        else "not run"
        for node in job.nodes_in_topological_order
    }

    evaluations = [
        event.event_specific_data
        for event in _events(instance, run_id, dg.DagsterEventType.ASSET_CHECK_EVALUATION)
    ]
    not_passed = [e for e in evaluations if not e.passed]
    warned = sum(e.severity == dg.AssetCheckSeverity.WARN for e in not_passed)

    return RunOutcome(
        plan=plan,
        run_id=run_id,
        success=result.success,
        seconds=seconds,
        steps=steps,
        checks_passed=len(evaluations) - len(not_passed),
        checks_warned=warned,
        checks_failed=len(not_passed) - warned,
        alerts=notify_run(instance, run_id),
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m orchestration.run_once",
        description="Check, load and build whatever is in the landing folder, once.",
    )
    parser.add_argument("--verbose", action="store_true", help="show the full Dagster and dbt logs")
    args = parser.parse_args()

    with get_instance() as instance:
        outcome = process_pending(instance, verbose=args.verbose)
    print(outcome.report())
    sys.exit(0 if outcome.success else 1)


if __name__ == "__main__":
    main()
