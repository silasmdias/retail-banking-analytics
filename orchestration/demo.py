"""Plays the source systems to show the pipeline at work, without the Dagster UI.

    python -m orchestration.demo new-month       the next month arrives, clean
    python -m orchestration.demo broken-month    the next month arrives broken, then fixed

Each command delivers the month after the latest one in the landing folder and
runs the pipeline on it, so they can be repeated and combined. With an empty
landing folder they first generate and load the default history.

Every step states what should happen and stops with an error if it does not,
which makes these demos end-to-end tests as well.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import dagster as dg
import duckdb

from generator.simulation import Dataset, Settings, month_label, parse_month, simulate
from generator.writer import write_dataset
from ingest.delivery import CONTROL_FILE, plan_delivery
from ingest.ledger import read_ledger
from orchestration.alerts import ALERTS_FILE, ERROR, WARNING
from orchestration.paths import paths
from orchestration.run_once import RunOutcome, get_instance, process_pending

REVENUE_MART = '"marts"."revenue_by_product_channel"'
REJECTED_RECORDS = '"marts"."dq_rejected_records"'


class DemoFailed(SystemExit):
    def __init__(self, message: str) -> None:
        super().__init__(f"\nThe demo did not behave as designed: {message}")


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise DemoFailed(message)


def heading(text: str) -> None:
    print(f"\n{text}\n{'-' * len(text)}")


# --------------------------------------------------------------------------- #
# The source systems
# --------------------------------------------------------------------------- #


def delivered_settings(raw_dir: Path) -> Settings | None:
    """Settings of the dataset in the landing folder, read from its control file."""
    path = raw_dir / CONTROL_FILE
    if not path.is_file():
        return None
    settings = json.loads(path.read_text())["settings"]
    return Settings(
        seed=settings["seed"],
        start_month=parse_month(settings["start_month"]),
        end_month=parse_month(settings["end_month"]),
        scale=settings["scale"],
    )


def transactions_file(raw_dir: Path, month: str) -> Path:
    return raw_dir / "core_banking" / "transactions" / f"transactions_{month}.csv"


def cut_in_half(path: Path) -> tuple[int, int]:
    """Keeps the first half of the records, as a transfer that stopped midway would."""
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    records = len(lines) - 1
    path.write_text("".join(lines[: 1 + records // 2]), encoding="utf-8")
    return records, records // 2


def point_to_unknown_accounts(path: Path, one_in: int) -> int:
    """Rewrites the account of every ``one_in``-th transaction to one that does not exist.

    The file keeps its row count, so it still agrees with the control file: this
    is a source that sent wrong data, not a file damaged on the way. A record the
    feed repeats is changed in every copy, so the result does not depend on which
    copy the staging model keeps.
    """
    with path.open(newline="", encoding="utf-8") as handle:
        header, *rows = list(csv.reader(handle))
    identifier, account = header.index("transaction_id"), header.index("account_id")
    distinct = list(dict.fromkeys(row[identifier] for row in rows))
    unknown = {
        transaction_id: f"ACC-8{position:05d}-01"
        for position, transaction_id in enumerate(distinct)
        if position % one_in == 0
    }
    for row in rows:
        if row[identifier] in unknown:
            row[account] = unknown[row[identifier]]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    return len(unknown)


# --------------------------------------------------------------------------- #
# What the warehouse shows
# --------------------------------------------------------------------------- #


def latest_published_month(warehouse: Path) -> str | None:
    """Latest month in the revenue mart, which is what a report reader sees."""
    if not warehouse.exists():
        return None
    with duckdb.connect(str(warehouse), read_only=True) as connection:
        try:
            [month] = connection.execute(
                f"select strftime(max(reporting_month), '%Y-%m') from {REVENUE_MART}"
            ).fetchone()
        except duckdb.CatalogException:
            return None
    return month


def unknown_account_rate(warehouse: Path, month: str) -> tuple[int, float]:
    with duckdb.connect(str(warehouse), read_only=True) as connection:
        row = connection.execute(
            f"""
            select rejected_records, cast(rejection_rate as double)
            from {REJECTED_RECORDS}
            where source_table = 'transactions'
                and rejection_reason = 'unknown_account'
                and strftime(record_month, '%Y-%m') = ?
            """,
            [month],
        ).fetchone()
    return row or (0, 0.0)


# --------------------------------------------------------------------------- #
# The demos
# --------------------------------------------------------------------------- #


class Demo:
    def __init__(self, instance: dg.DagsterInstance) -> None:
        self.instance = instance
        self.locations = paths()
        self._dataset: tuple[int, Dataset] | None = None

    def run(self) -> RunOutcome:
        outcome = process_pending(self.instance)
        print(outcome.report())
        return outcome

    def deliver(self, settings: Settings) -> None:
        """Writes the landing folder as the source systems would at the end of the month."""
        if self._dataset is None or self._dataset[0] != settings.end_month:
            self._dataset = (settings.end_month, simulate(settings))
        write_dataset(self._dataset[1], settings, self.locations.raw_dir)

    def published(self) -> str | None:
        return latest_published_month(self.locations.warehouse)

    def is_loaded(self, path: Path) -> bool:
        relative = path.relative_to(self.locations.raw_dir).as_posix()
        return relative in read_ledger(self.locations.warehouse)

    def ensure_history(self) -> Settings:
        """Makes sure there is a loaded history for the next month to build on."""
        settings = delivered_settings(self.locations.raw_dir)
        if settings is None:
            settings = Settings()
            heading(
                f"No data yet: the source systems deliver {month_label(settings.start_month)} "
                f"to {month_label(settings.end_month)}"
            )
            self.deliver(settings)
        elif plan_delivery(self.locations.raw_dir, self.locations.warehouse).pending:
            heading("The landing folder holds files that are not loaded yet")
        else:
            return settings
        outcome = self.run()
        expect(
            outcome.success,
            "the files already in the landing folder do not load cleanly. "
            "Run `make clean` to start over.",
        )
        return settings

    def next_month(self) -> tuple[Settings, str]:
        history = self.ensure_history()
        settings = Settings(
            seed=history.seed,
            start_month=history.start_month,
            end_month=history.end_month + 1,
            scale=history.scale,
        )
        return settings, month_label(settings.end_month)

    def new_month(self) -> None:
        settings, month = self.next_month()
        before = self.published()

        heading(f"{month} arrives")
        self.deliver(settings)
        outcome = self.run()
        expect(outcome.success, "a clean month should load and build")
        expect(not outcome.alerts, "a clean month should raise no alert")
        expect(self.published() == month, f"the marts should now reach {month}")
        loaded = len(outcome.plan.pending)
        on_disk = len(outcome.plan.landed)
        print(
            f"\nLoaded {loaded} of the {on_disk} files in the landing folder. "
            f"The marts went from {before} to {month}."
        )

    def broken_month(self) -> None:
        settings, month = self.next_month()
        before = self.published()
        transactions = transactions_file(self.locations.raw_dir, month)
        failed_rule = "marts/dq_rejected_records: rejection_rate_within_limit"
        drift_rule = "marts/dq_rejected_records: rejection_rate_drift"

        heading(f"1. {month} arrives with the transactions file cut in half")
        self.deliver(settings)
        declared, kept = cut_in_half(transactions)
        print(f"The control file declares {declared:,} transactions; the file holds {kept:,}.\n")
        outcome = self.run()
        expect(not outcome.success, "a truncated file should stop the run")
        expect(outcome.steps["raw_tables"] == "not run", "nothing should be loaded")
        expect(not self.is_loaded(transactions), "the truncated file should not be in the ledger")
        expect(
            any(
                a.severity == ERROR and a.source.endswith("row_counts_match_control_file")
                for a in outcome.alerts
            ),
            "the control count should raise an error alert",
        )
        expect(self.published() == before, "the marts should not change")
        print(f"\nStopped before the load. Nothing entered the warehouse; marts still at {before}.")

        heading("2. The file is sent again, complete, with 1 in 20 accounts unknown")
        self.deliver(settings)
        changed = point_to_unknown_accounts(transactions, one_in=20)
        print(
            f"{changed:,} transactions point to accounts that do not exist. The row count "
            "matches the control file.\n"
        )
        outcome = self.run()
        expect(not outcome.success, "a month with 5% of records rejected should stop the run")
        expect(outcome.steps["raw_tables"] == "succeeded", "the file should be loaded as sent")
        expect(outcome.steps["dbt_build_marts"] == "not run", "the marts should not be rebuilt")
        expect(
            any(a.severity == ERROR and a.source == failed_rule for a in outcome.alerts),
            "the rejection rate test should raise an error alert",
        )
        expect(self.published() == before, "the marts should not change")
        rejected, rate = unknown_account_rate(self.locations.warehouse, month)
        print(
            f"\nStopped after validation. dq_rejected_records shows {rejected:,} transactions "
            f"({rate:.1%}) with an unknown account in {month}; marts still at {before}."
        )

        heading("3. The file is sent again with 1 in 200 accounts unknown")
        self.deliver(settings)
        changed = point_to_unknown_accounts(transactions, one_in=200)
        print(f"{changed:,} transactions point to accounts that do not exist.\n")
        outcome = self.run()
        expect(outcome.success, "a rate under the 1% limit should not stop the run")
        expect(
            [(a.severity, a.source) for a in outcome.alerts] == [(WARNING, drift_rule)],
            "the drift check should raise exactly one warning",
        )
        expect(self.published() == month, f"the marts should now reach {month}")
        print("\nPublished with a warning: under the limit, but well above recent months.")

        heading("4. The source sends the corrected file")
        self.deliver(settings)
        outcome = self.run()
        expect(outcome.success, "the corrected file should load and build")
        expect(not outcome.alerts, "the corrected file should raise no alert")
        expect(
            [file.path for file in outcome.plan.pending] == [transactions],
            "only the corrected file should be loaded",
        )
        rejected, rate = unknown_account_rate(self.locations.warehouse, month)
        print(
            f"\nOnly the corrected file was loaded; it replaced its own rows. Unknown accounts "
            f"in {month} are back to {rate:.2%}. The marts reach {self.published()}."
        )

        print(f"\nThe alerts are in {self.locations.alerts_dir / ALERTS_FILE}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m orchestration.demo",
        description="Deliver the next month and run the pipeline on it.",
    )
    parser.add_argument("scenario", choices=("new-month", "broken-month"))
    args = parser.parse_args()

    with get_instance() as instance:
        demo = Demo(instance)
        if args.scenario == "new-month":
            demo.new_month()
        else:
            demo.broken_month()


if __name__ == "__main__":
    main()
