"""Checks that belong to the orchestrator, not to dbt.

dbt tests state what must always be true of the warehouse. The two checks here
need to know about time and about the delivery at hand, which dbt does not:

- ``source_freshness`` asks whether a monthly feed has stopped arriving. It runs
  ``dbt source freshness`` and turns each source's result into a check.
- ``rejection_rate_drift`` looks only at the months loaded in the current run
  and warns when a rejection reason is far more frequent than it used to be,
  before it reaches the 1% limit that fails the dbt test.
"""

import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any

import dagster as dg
import duckdb
from dagster_dbt import DbtCliResource, get_asset_key_for_model

from ingest.contract import MONTHLY_FEEDS
from ingest.ledger import read_ledger
from orchestration.assets import dbt_project, dbt_validate_inputs, translator
from orchestration.paths import paths

_manifest: Mapping[str, Any] = json.loads(dbt_project.manifest_path.read_text())

# --------------------------------------------------------------------------- #
# Source freshness
# --------------------------------------------------------------------------- #

FRESHNESS_CHECK = "source_freshness"


def _has_thresholds(source: Mapping[str, Any]) -> bool:
    freshness = source.get("freshness") or {}
    return bool(
        source.get("loaded_at_field")
        and any(
            (freshness.get(level) or {}).get("count") for level in ("warn_after", "error_after")
        )
    )


# dbt source unique_id -> asset key, for the sources that declare freshness thresholds.
FRESHNESS_SOURCES: dict[str, dg.AssetKey] = {
    unique_id: translator.get_asset_spec(_manifest, unique_id, dbt_project).key
    for unique_id, source in sorted(_manifest["sources"].items())
    if _has_thresholds(source)
}


def _age(seconds: float) -> str:
    return f"{seconds / 86400:.1f} days" if seconds >= 86400 else f"{seconds / 3600:.1f} hours"


def _threshold(criteria: Mapping[str, Any], level: str) -> str | None:
    rule = criteria.get(level) or {}
    return f"{rule['count']} {rule['period']}s" if rule.get("count") else None


def freshness_check_result(
    result: Mapping[str, Any], asset_key: dg.AssetKey
) -> dg.AssetCheckResult:
    """Turns one entry of dbt's ``sources.json`` into a check result."""
    status = result.get("status")
    if status not in ("pass", "warn", "error"):
        return dg.AssetCheckResult(
            asset_key=asset_key,
            check_name=FRESHNESS_CHECK,
            passed=False,
            description=f"dbt could not measure freshness: {result.get('error') or status}",
        )
    age = _age(result["max_loaded_at_time_ago_in_s"])
    criteria = result.get("criteria") or {}
    warn_after, error_after = (
        _threshold(criteria, "warn_after"),
        _threshold(criteria, "error_after"),
    )
    if status == "pass":
        description = f"Last load {age} ago"
    else:
        limit = error_after if status == "error" else warn_after
        description = f"Last load {age} ago; a new file is expected at least every {limit}"
    return dg.AssetCheckResult(
        asset_key=asset_key,
        check_name=FRESHNESS_CHECK,
        passed=status == "pass",
        severity=dg.AssetCheckSeverity.ERROR if status == "error" else dg.AssetCheckSeverity.WARN,
        description=description,
        metadata={
            "last_loaded_at": result["max_loaded_at"],
            "age_in_days": round(result["max_loaded_at_time_ago_in_s"] / 86400, 2),
            "warn_after": warn_after or "not set",
            "error_after": error_after or "not set",
        },
    )


@dg.multi_asset_check(
    name="source_freshness",
    specs=[
        dg.AssetCheckSpec(
            FRESHNESS_CHECK,
            asset=asset_key,
            description="The table received a new file recently enough, measured by "
            "`dbt source freshness` on `_loaded_at`.",
        )
        for asset_key in FRESHNESS_SOURCES.values()
    ],
)
def source_freshness(dbt: DbtCliResource) -> Iterator[dg.AssetCheckResult]:
    invocation = dbt.cli(["source", "freshness"], raise_on_error=False)
    invocation.wait()
    results = {r["unique_id"]: r for r in invocation.get_artifact("sources.json")["results"]}
    for unique_id, asset_key in FRESHNESS_SOURCES.items():
        yield freshness_check_result(
            results.get(unique_id, {"status": "missing from sources.json"}), asset_key
        )


# --------------------------------------------------------------------------- #
# Rejection rate drift
# --------------------------------------------------------------------------- #

DRIFT_HISTORY_MONTHS = 6
DRIFT_RATIO = 3.0
# Below this many rejected records a rate is mostly noise.
DRIFT_MINIMUM_RECORDS = 20

_report = _manifest["nodes"]["model.retail_banking.dq_rejected_records"]
REJECTED_RECORDS_RELATION = f'"{_report["schema"]}"."{_report["alias"] or _report["name"]}"'
REJECTED_RECORDS_KEY = get_asset_key_for_model([dbt_validate_inputs], "dq_rejected_records")


@dataclass(frozen=True)
class RejectionRate:
    """One row of ``dq_rejected_records``."""

    source_table: str
    record_month: str  # YYYY-MM
    rejection_reason: str
    rejected_records: int
    rejection_rate: float


def _months_before(month: str, count: int) -> list[str]:
    year, number = int(month[:4]), int(month[5:7])
    index = year * 12 + number - 1
    return [f"{(index - k) // 12:04d}-{(index - k) % 12 + 1:02d}" for k in range(1, count + 1)]


def find_rejection_drift(rows: Iterable[RejectionRate], months: Iterable[str]) -> list[str]:
    """Rejection reasons that, in the given months, ran far above their recent average.

    A reason drifts when it rejected at least ``DRIFT_MINIMUM_RECORDS`` records and
    its rate is more than ``DRIFT_RATIO`` times its average rate over the previous
    ``DRIFT_HISTORY_MONTHS`` months. A month without a row counts as a rate of zero,
    so a reason never seen before drifts as soon as it has enough records.
    """
    rows = list(rows)
    rate = {(r.source_table, r.rejection_reason, r.record_month): r.rejection_rate for r in rows}
    first_month = {}
    for row in rows:
        first_month[row.source_table] = min(
            row.record_month, first_month.get(row.source_table, row.record_month)
        )

    problems = []
    for month in sorted(set(months)):
        history = _months_before(month, DRIFT_HISTORY_MONTHS)
        for row in sorted(rows, key=lambda r: (r.source_table, r.rejection_reason)):
            if row.record_month != month or row.rejected_records < DRIFT_MINIMUM_RECORDS:
                continue
            if first_month[row.source_table] > history[-1]:
                continue  # the source is younger than the comparison window
            baseline = sum(
                rate.get((row.source_table, row.rejection_reason, past), 0.0) for past in history
            ) / len(history)
            if row.rejection_rate > DRIFT_RATIO * baseline:
                times = f"{row.rejection_rate / baseline:.1f} times" if baseline else "up from"
                problems.append(
                    f"{row.source_table} {month}, {row.rejection_reason}: "
                    f"{row.rejection_rate:.2%} rejected ({row.rejected_records:,} records), "
                    f"{times} the {baseline:.2%} average of the previous "
                    f"{DRIFT_HISTORY_MONTHS} months"
                )
    return problems


def _months_loaded_in(run_id: str) -> list[str]:
    monthly = {feed.table for feed in MONTHLY_FEEDS}
    entries = read_ledger(paths().warehouse, load_id=run_id).values()
    return sorted({e.period for e in entries if e.table_name in monthly and e.period})


@dg.asset_check(
    asset=REJECTED_RECORDS_KEY,
    name="rejection_rate_drift",
    description=(
        f"In the months loaded by this run, no rejection reason is more than "
        f"{DRIFT_RATIO:.0f} times its average rate of the previous {DRIFT_HISTORY_MONTHS} months."
    ),
)
def rejection_rate_drift(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    with duckdb.connect(str(paths().warehouse), read_only=True) as connection:
        rows = [
            RejectionRate(*row)
            for row in connection.execute(
                f"""
                select
                    source_table,
                    strftime(record_month, '%Y-%m'),
                    rejection_reason,
                    rejected_records,
                    cast(rejection_rate as double)
                from {REJECTED_RECORDS_RELATION}
                """
            ).fetchall()
        ]
    months = _months_loaded_in(context.run.run_id)
    if not months and rows:
        months = [max(row.record_month for row in rows)]  # a re-run: look at the latest month

    problems = find_rejection_drift(rows, months)
    scope = ", ".join(months) if len(months) <= 3 else f"{months[0]} to {months[-1]}"
    return dg.AssetCheckResult(
        passed=not problems,
        severity=dg.AssetCheckSeverity.WARN,
        description=(
            f"Rejections are rising: {problems[0]}"
            if problems
            else f"Rejection rates are in line with recent months ({scope or 'no months'})"
        ),
        metadata={"months_evaluated": scope or "none"}
        | ({"problems": problems} if problems else {}),
    )
