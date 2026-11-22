"""What the pipeline expects from each source feed.

The generator plays the source systems. This module is the receiving side of
the agreement: which tables arrive, how often, under which file names and with
which columns. The delivery checks compare what landed against it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

MONTHLY, YEARLY, SNAPSHOT = "monthly", "yearly", "snapshot"

_PERIOD_SUFFIX = {
    MONTHLY: r"_(\d{4}-(?:0[1-9]|1[0-2]))",
    YEARLY: r"_(\d{4})",
    SNAPSHOT: r"()",
}
_NAME_EXAMPLE = {
    MONTHLY: "{table}_YYYY-MM.csv",
    YEARLY: "{table}_YYYY.csv",
    SNAPSHOT: "{table}.csv",
}


@dataclass(frozen=True)
class Feed:
    """One source table and how it is delivered."""

    system: str
    table: str
    cadence: str  # MONTHLY and YEARLY add one file per period; SNAPSHOT replaces its file
    columns: tuple[str, ...]

    @property
    def directory(self) -> str:
        return f"{self.system}/{self.table}"

    @property
    def schema(self) -> str:
        return f"raw_{self.system}"

    @property
    def relation(self) -> str:
        return f'"{self.schema}"."{self.table}"'

    @property
    def expected_file_name(self) -> str:
        return _NAME_EXAMPLE[self.cadence].format(table=self.table)

    def period_of(self, file_name: str) -> str | None:
        """Month or year a file covers, "" for a snapshot, None for an unexpected name."""
        pattern = re.escape(self.table) + _PERIOD_SUFFIX[self.cadence] + r"\.csv"
        match = re.fullmatch(pattern, file_name)
        return match.group(1) if match else None


FEEDS = (
    Feed(
        "core_banking",
        "customers",
        SNAPSHOT,
        (
            "customer_id",
            "signup_date",
            "segment",
            "region",
            "birth_year",
            "risk_band",
            "acquisition_channel",
            "is_internal_test",
        ),
    ),
    Feed(
        "core_banking",
        "accounts",
        SNAPSHOT,
        (
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
    ),
    Feed(
        "core_banking",
        "products",
        SNAPSHOT,
        ("product_id", "product_name", "product_category", "balance_side", "interchange_rate"),
    ),
    Feed(
        "core_banking",
        "transactions",
        MONTHLY,
        (
            "transaction_id",
            "account_id",
            "transaction_date",
            "posted_date",
            "transaction_type",
            "channel",
            "amount",
        ),
    ),
    Feed(
        "core_banking",
        "fees",
        MONTHLY,
        ("fee_id", "account_id", "transaction_id", "fee_type", "fee_date", "amount", "fee_status"),
    ),
    Feed(
        "core_banking",
        "account_balances",
        MONTHLY,
        ("account_id", "balance_month", "average_balance", "interest_bearing_balance"),
    ),
    Feed("treasury", "funds_transfer_rates", SNAPSHOT, ("rate_month", "annual_rate")),
    Feed(
        "finance",
        "acquisition_spend",
        YEARLY,
        ("spend_month", "sales_channel", "acquisition_spend"),
    ),
    Feed(
        "finance",
        "revenue_budget",
        YEARLY,
        ("budget_month", "product_id", "sales_channel", "budget_revenue"),
    ),
)

MONTHLY_FEEDS = tuple(feed for feed in FEEDS if feed.cadence == MONTHLY)
