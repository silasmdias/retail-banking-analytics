"""Writes a simulated dataset as CSV files laid out like source-system extracts.

<output>/core_banking/customers/customers.csv              full snapshot
<output>/core_banking/accounts/accounts.csv                full snapshot
<output>/core_banking/products/products.csv                full snapshot
<output>/core_banking/transactions/transactions_YYYY-MM.csv   one file per posting month
<output>/core_banking/fees/fees_YYYY-MM.csv                   one file per month
<output>/core_banking/account_balances/account_balances_YYYY-MM.csv
<output>/treasury/funds_transfer_rates/funds_transfer_rates.csv
<output>/finance/acquisition_spend/acquisition_spend_YYYY.csv
<output>/finance/revenue_budget/revenue_budget_YYYY.csv
<output>/_manifest.json                                    control file, written last

The manifest doubles as the control file a source system sends with a batch: it
lists every file delivered and how many records each one holds, so the receiver
can tell a complete delivery from a truncated one. It is removed before the
files are rewritten and written again at the end, so its presence means the
delivery is complete.
"""

from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path

from generator import config as cfg
from generator.simulation import Dataset, Settings, month_label

SOURCE_SYSTEMS = ("core_banking", "treasury", "finance")

HEADERS = {
    "customers": (
        "customer_id",
        "signup_date",
        "segment",
        "region",
        "birth_year",
        "risk_band",
        "acquisition_channel",
        "is_internal_test",
    ),
    "accounts": (
        "account_id",
        "customer_id",
        "product_id",
        "opened_date",
        "closed_date",
        "account_status",
        "sales_channel",
        "annual_interest_rate",
        "credit_limit",
        "original_principal",
        "term_months",
    ),
    "products": (
        "product_id",
        "product_name",
        "product_category",
        "balance_side",
        "interchange_rate",
    ),
    "transactions": (
        "transaction_id",
        "account_id",
        "transaction_date",
        "posted_date",
        "transaction_type",
        "channel",
        "amount",
    ),
    "fees": (
        "fee_id",
        "account_id",
        "transaction_id",
        "fee_type",
        "fee_date",
        "amount",
        "fee_status",
    ),
    "account_balances": (
        "account_id",
        "balance_month",
        "average_balance",
        "interest_bearing_balance",
    ),
    "funds_transfer_rates": ("rate_month", "annual_rate"),
    "acquisition_spend": ("spend_month", "sales_channel", "acquisition_spend"),
    "revenue_budget": ("budget_month", "product_id", "sales_channel", "budget_revenue"),
}


MANIFEST_NAME = "_manifest.json"


def write_dataset(data: Dataset, settings: Settings, output_dir: Path) -> dict:
    """Replaces the contents of ``output_dir`` with the dataset and returns the manifest."""
    (output_dir / MANIFEST_NAME).unlink(missing_ok=True)
    for system in SOURCE_SYSTEMS:
        shutil.rmtree(output_dir / system, ignore_errors=True)

    rows_by_file: dict[str, int] = {}

    def _write(path: Path, table: str, rows: list[tuple]) -> int:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(HEADERS[table])
            writer.writerows(rows)
        rows_by_file[path.relative_to(output_dir).as_posix()] = len(rows)
        return len(rows)

    core, treasury, finance = (output_dir / system for system in SOURCE_SYSTEMS)
    counts = {
        "customers": _write(core / "customers" / "customers.csv", "customers", data.customers),
        "accounts": _write(core / "accounts" / "accounts.csv", "accounts", data.accounts),
        "products": _write(
            core / "products" / "products.csv",
            "products",
            [
                (
                    p.product_id,
                    p.product_name,
                    p.product_category,
                    p.balance_side,
                    f"{p.interchange_rate:.4f}",
                )
                for p in cfg.PRODUCTS.values()
            ],
        ),
        "funds_transfer_rates": _write(
            treasury / "funds_transfer_rates" / "funds_transfer_rates.csv",
            "funds_transfer_rates",
            data.funds_transfer_rates,
        ),
    }
    for table, partitions in (
        ("transactions", data.transactions),
        ("fees", data.fees),
        ("account_balances", data.balances),
    ):
        counts[table] = sum(
            _write(core / table / f"{table}_{month_label(month)}.csv", table, rows)
            for month, rows in sorted(partitions.items())
        )
    for table, partitions in (
        ("acquisition_spend", data.acquisition_spend),
        ("revenue_budget", data.revenue_budget),
    ):
        counts[table] = sum(
            _write(finance / table / f"{table}_{year}.csv", table, rows)
            for year, rows in sorted(partitions.items())
        )

    manifest = {
        "settings": {
            "seed": settings.seed,
            "start_month": month_label(settings.start_month),
            "end_month": month_label(settings.end_month),
            "scale": settings.scale,
        },
        "rows_written": counts,
        "files": dict(sorted(rows_by_file.items())),
        "data_quality_problems_injected": dict(sorted(data.dirt_counts.items())),
        "transactions_not_yet_posted": data.pending_late_arrivals,
        "revenue_control_totals": {
            name: round(value, 2) for name, value in sorted(data.revenue_truth_by_type.items())
        },
    }
    (output_dir / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest
