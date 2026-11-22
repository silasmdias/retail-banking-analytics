"""Turns the outcome of a run into alerts and delivers them.

What raises an alert:

- a failed check with error severity: the run stopped, somebody has to act;
- a failed check with warning severity that this project defines (volume,
  rejection drift, freshness): the run went on, somebody should look;
- a step that failed for any other reason.

A dbt test that only warns does not raise an alert. Those tests count source
problems the staging models already repair or reject, so they fail on every
run by design; the drift check is what notices when such a count changes.

Where alerts go: a line per alert in ``<alerts dir>/alerts.jsonl``, which needs
no account and no secret. If ``ALERT_WEBHOOK_URL`` is set, each new alert is
also posted there as ``{"text": ...}``, the format Slack and most chat tools accept.
"""

from __future__ import annotations

import hashlib
import json
import os
import urllib.request
from collections.abc import Callable, Iterable, Iterator
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dagster as dg

from orchestration.assets import dbt_build_marts, dbt_project, dbt_validate_inputs
from orchestration.paths import paths

ALERTS_FILE = "alerts.jsonl"
WEBHOOK_VARIABLE = "ALERT_WEBHOOK_URL"

ERROR, WARNING = "error", "warning"

_DBT_CHECKS = frozenset(dbt_validate_inputs.check_keys | dbt_build_marts.check_keys)
_DBT_TESTS = {
    unique_id: node
    for unique_id, node in json.loads(dbt_project.manifest_path.read_text())["nodes"].items()
    if node["resource_type"] == "test"
}


def dbt_test_note(unique_id: str | None) -> str:
    """What a dbt test means, in the words of the dbt project.

    Uses the test's description, or the comment that opens a singular test,
    which says what a failure means and where to look.
    """
    node = _DBT_TESTS.get(unique_id or "", {})
    if node.get("description"):
        return node["description"]
    comment = []
    for line in (node.get("raw_code") or "").splitlines():
        if not line.startswith("--"):
            break
        comment.append(line.removeprefix("--").strip())
    return " ".join(comment)


@dataclass(frozen=True)
class Alert:
    alert_id: str
    raised_at: str
    severity: str  # "error": the run stopped; "warning": it went on
    source: str  # the check or step that raised it
    summary: str
    problems: tuple[str, ...]
    job: str
    run_id: str

    def as_text(self) -> str:
        lines = [f"[{self.severity.upper()}] {self.source}", self.summary]
        if len(self.problems) > 1:  # a single problem is already in the summary
            lines += [f"  - {problem}" for problem in self.problems[:10]]
            if len(self.problems) > 10:
                lines.append(f"  - and {len(self.problems) - 10} more")
        lines.append(f"job {self.job}, run {self.run_id}")
        return "\n".join(lines)


def _alert(severity: str, source: str, summary: str, problems: Iterable[str], run: Any) -> Alert:
    identity = hashlib.sha256(f"{run.run_id}|{source}".encode()).hexdigest()[:12]
    return Alert(
        alert_id=identity,
        raised_at=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        severity=severity,
        source=source,
        summary=" ".join(summary.split()),
        problems=tuple(problems),
        job=run.job_name,
        run_id=run.run_id,
    )


def _events(
    instance: dg.DagsterInstance, run_id: str, event_type: dg.DagsterEventType
) -> Iterator[Any]:
    cursor = None
    while True:
        page = instance.get_records_for_run(run_id, cursor=cursor, of_type=event_type, limit=500)
        for record in page.records:
            yield record.event_log_entry.dagster_event
        if not page.has_more:
            return
        cursor = page.cursor


def _plain(value: Any) -> Any:
    return getattr(value, "value", value)


def _check_alert(evaluation: Any, run: Any) -> Alert | None:
    if evaluation.passed:
        return None
    is_error = evaluation.severity == dg.AssetCheckSeverity.ERROR
    key = evaluation.asset_check_key
    if not is_error and key in _DBT_CHECKS:
        return None

    problems = list(_plain(evaluation.metadata.get("problems")) or [])
    summary = evaluation.description
    if not summary and key in _DBT_CHECKS:
        failed_rows = _plain(evaluation.metadata.get("dagster_dbt/failed_row_count"))
        rows = "" if failed_rows is None else f" on {failed_rows:,} row{'s'[: failed_rows != 1]}"
        note = dbt_test_note(_plain(evaluation.metadata.get("unique_id")))
        summary = f"The dbt test failed{rows}. {note}".strip()
    return _alert(
        ERROR if is_error else WARNING,
        f"{key.asset_key.to_user_string()}: {key.name}",
        summary or "The check failed.",
        problems,
        run,
    )


def _root_cause(failure: Any) -> str:
    """First line of the innermost error: Dagster wraps the real one in its own."""
    error = failure.error
    while error is not None and error.cause is not None:
        error = error.cause
    message = (error.message if error else "").strip()
    return message.splitlines()[0][:500] if message else "The step failed without a message."


def alerts_for_run(instance: dg.DagsterInstance, run_id: str) -> list[Alert]:
    """Reads what a finished run recorded and decides what deserves an alert."""
    run = instance.get_run_by_id(run_id)
    if run is None:
        return []

    alerts = []
    steps_explained = set()
    for event in _events(instance, run_id, dg.DagsterEventType.ASSET_CHECK_EVALUATION):
        alert = _check_alert(event.event_specific_data, run)
        if alert:
            alerts.append(alert)
            if alert.severity == ERROR:
                steps_explained.add(event.step_key)

    for event in _events(instance, run_id, dg.DagsterEventType.STEP_FAILURE):
        if event.step_key in steps_explained:
            continue
        alerts.append(
            _alert(ERROR, f"step {event.step_key}", _root_cause(event.event_specific_data), (), run)
        )

    if run.status == dg.DagsterRunStatus.FAILURE and not any(a.severity == ERROR for a in alerts):
        alerts.append(
            _alert(ERROR, "run", "The run failed before any step reported an error.", (), run)
        )
    return alerts


def _post(url: str, text: str) -> None:
    if not url.startswith(("https://", "http://")):
        raise ValueError(f"{WEBHOOK_VARIABLE} must be an http(s) URL")
    request = urllib.request.Request(
        url,
        data=json.dumps({"text": text}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10):
        pass


def deliver(
    alerts: Iterable[Alert],
    alerts_dir: Path,
    webhook_url: str | None = None,
    post: Callable[[str, str], None] = _post,
    log: Callable[[str], None] = print,
) -> list[Alert]:
    """Records the alerts that are not on file yet and returns them.

    An alert is identified by its run and source, so evaluating the same run
    twice never notifies twice. A webhook that fails does not lose the alert:
    it is already in the file.
    """
    path = alerts_dir / ALERTS_FILE
    known = set()
    if path.exists():
        known = {json.loads(line)["alert_id"] for line in path.read_text().splitlines() if line}
    new = [alert for alert in alerts if alert.alert_id not in known]
    if not new:
        return []

    alerts_dir.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for alert in new:
            handle.write(json.dumps(asdict(alert)) + "\n")
    if webhook_url:
        for alert in new:
            try:
                post(webhook_url, alert.as_text())
            except Exception as error:  # a broken webhook must not break the run
                log(f"Could not post alert {alert.alert_id} to the webhook: {error}")
    return new


def notify_run(
    instance: dg.DagsterInstance, run_id: str, log: Callable[[str], None] = print
) -> list[Alert]:
    """Raises the alerts of a finished run. Safe to call more than once for the same run."""
    return deliver(
        alerts_for_run(instance, run_id),
        paths().alerts_dir,
        webhook_url=os.environ.get(WEBHOOK_VARIABLE) or None,
        log=log,
    )
