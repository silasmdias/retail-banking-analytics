"""Business parameters of the fictional bank.

These numbers shape the synthetic data: product catalog, pricing, customer mix,
behavior and the rate at which records are deliberately corrupted. They are
invented for this project and do not describe any real institution.
"""

from __future__ import annotations

from dataclasses import dataclass

# --------------------------------------------------------------------------- #
# Reference values
# --------------------------------------------------------------------------- #

SALES_CHANNELS = ("branch", "mobile_app", "online", "call_center", "partner")
SERVICING_CHANNELS = (
    "branch",
    "atm",
    "mobile_app",
    "online",
    "pos",
    "call_center",
    "automated",
)
SEGMENTS = ("mass_market", "emerging_affluent", "affluent")
REGIONS = {"north": 0.18, "south": 0.26, "east": 0.22, "west": 0.20, "central": 0.14}
RISK_BANDS = ("A", "B", "C", "D", "E")


@dataclass(frozen=True)
class Product:
    product_id: str
    product_name: str
    product_category: str
    balance_side: str  # "asset" (the bank lends) or "liability" (the bank owes)
    interchange_rate: float  # share of card purchase volume earned as interchange


PRODUCTS = {
    p.product_id: p
    for p in (
        Product("CHK_STD", "Everyday Checking", "checking", "liability", 0.0040),
        Product("SAV_HY", "High-Yield Savings", "savings", "liability", 0.0),
        Product("CC_STD", "Classic Credit Card", "credit_card", "asset", 0.0165),
        Product("CC_PREM", "Premium Credit Card", "credit_card", "asset", 0.0210),
        Product("PL_STD", "Personal Loan", "personal_loan", "asset", 0.0),
    )
}

# --------------------------------------------------------------------------- #
# Customer acquisition
# --------------------------------------------------------------------------- #

# New customers per month: BASE + GROWTH * months since the first month.
ACQUISITION_BASE = 95.0
ACQUISITION_GROWTH = 2.6
ACQUISITION_SEASONALITY = {
    1: 1.10,
    2: 1.00,
    3: 1.02,
    4: 0.98,
    5: 0.97,
    6: 0.95,
    7: 0.96,
    8: 1.00,
    9: 1.06,
    10: 1.03,
    11: 0.98,
    12: 0.85,
}

# Channel mix drifts from the first value to the second over three years.
CHANNEL_MIX_DRIFT = {
    "branch": (0.34, 0.18),
    "mobile_app": (0.24, 0.42),
    "online": (0.22, 0.20),
    "call_center": (0.08, 0.06),
    "partner": (0.12, 0.14),
}

SEGMENT_MIX = {
    "branch": {"mass_market": 0.58, "emerging_affluent": 0.28, "affluent": 0.14},
    "mobile_app": {"mass_market": 0.78, "emerging_affluent": 0.18, "affluent": 0.04},
    "online": {"mass_market": 0.66, "emerging_affluent": 0.25, "affluent": 0.09},
    "call_center": {"mass_market": 0.74, "emerging_affluent": 0.21, "affluent": 0.05},
    "partner": {"mass_market": 0.82, "emerging_affluent": 0.15, "affluent": 0.03},
}

RISK_MIX = {
    "branch": {"A": 0.32, "B": 0.32, "C": 0.22, "D": 0.11, "E": 0.03},
    "mobile_app": {"A": 0.22, "B": 0.29, "C": 0.27, "D": 0.16, "E": 0.06},
    "online": {"A": 0.27, "B": 0.31, "C": 0.24, "D": 0.13, "E": 0.05},
    "call_center": {"A": 0.20, "B": 0.28, "C": 0.28, "D": 0.17, "E": 0.07},
    "partner": {"A": 0.15, "B": 0.25, "C": 0.29, "D": 0.21, "E": 0.10},
}

FIRST_PRODUCT_MIX = {
    "branch": {"CHK_STD": 0.50, "SAV_HY": 0.14, "CC_STD": 0.12, "CC_PREM": 0.06, "PL_STD": 0.18},
    "mobile_app": {
        "CHK_STD": 0.52,
        "SAV_HY": 0.12,
        "CC_STD": 0.26,
        "CC_PREM": 0.04,
        "PL_STD": 0.06,
    },
    "online": {"CHK_STD": 0.34, "SAV_HY": 0.22, "CC_STD": 0.22, "CC_PREM": 0.08, "PL_STD": 0.14},
    "call_center": {
        "CHK_STD": 0.15,
        "SAV_HY": 0.05,
        "CC_STD": 0.45,
        "CC_PREM": 0.15,
        "PL_STD": 0.20,
    },
    "partner": {"CHK_STD": 0.10, "SAV_HY": 0.05, "CC_STD": 0.50, "CC_PREM": 0.05, "PL_STD": 0.30},
}

# Typical age and digital affinity (0 = branch-only, 1 = fully digital) by channel.
CHANNEL_AGE = {"branch": 47, "mobile_app": 31, "online": 38, "call_center": 44, "partner": 36}
CHANNEL_DIGITAL = {
    "branch": 0.30,
    "mobile_app": 0.80,
    "online": 0.65,
    "call_center": 0.35,
    "partner": 0.55,
}

SEGMENT_MONTHLY_INCOME = {"mass_market": 3200.0, "emerging_affluent": 6500.0, "affluent": 14000.0}

INTERNAL_TEST_CUSTOMER_RATE = 0.003

# Acquisition cost per new customer in the first year, and its yearly drift.
ACQUISITION_UNIT_COST = {
    "branch": 310.0,
    "mobile_app": 120.0,
    "online": 150.0,
    "call_center": 215.0,
    "partner": 185.0,
}
ACQUISITION_COST_DRIFT = {
    "branch": 1.04,
    "mobile_app": 0.94,
    "online": 0.98,
    "call_center": 1.03,
    "partner": 1.05,
}

# --------------------------------------------------------------------------- #
# Behavior
# --------------------------------------------------------------------------- #

# Monthly probability that a customer closes the whole relationship.
CHURN_BY_TENURE = ((3, 0.032), (12, 0.022), (24, 0.013), (10_000, 0.009))
CHURN_CHANNEL_FACTOR = {
    "branch": 0.75,
    "mobile_app": 1.25,
    "online": 1.00,
    "call_center": 1.15,
    "partner": 1.45,
}
CHURN_SEGMENT_FACTOR = {"mass_market": 1.10, "emerging_affluent": 0.90, "affluent": 0.70}
CHURN_PRODUCT_COUNT_FACTOR = {1: 1.00, 2: 0.60}
CHURN_MANY_PRODUCTS_FACTOR = 0.40

# Monthly probability of opening each product category not held yet.
CROSS_SELL_BASE = 0.012
CROSS_SELL_SEGMENT_FACTOR = {"mass_market": 1.0, "emerging_affluent": 1.25, "affluent": 1.6}
CROSS_SELL_CHANNEL_FACTOR = {
    "branch": 1.3,
    "mobile_app": 1.0,
    "online": 1.0,
    "call_center": 0.9,
    "partner": 0.8,
}
CROSS_SELL_SAME_CHANNEL_SHARE = 0.5

# Monthly probability that a credit account defaults and is charged off.
DEFAULT_HAZARD = {"A": 0.0003, "B": 0.0008, "C": 0.0020, "D": 0.0050, "E": 0.0110}
DEFAULT_REVOLVER_FACTOR = 1.5
DEFAULT_TRANSACTOR_FACTOR = 0.4
# No charge-offs in the first months of an account, then a linear ramp to the full hazard.
DEFAULT_SEASONING_START = 3
DEFAULT_SEASONING_RAMP = 9

LATE_PAYMENT_PROBABILITY = {"A": 0.010, "B": 0.025, "C": 0.050, "D": 0.090, "E": 0.150}

CARD_APR = {"A": 0.179, "B": 0.209, "C": 0.239, "D": 0.269, "E": 0.299}
LOAN_APR = {"A": 0.085, "B": 0.110, "C": 0.145, "D": 0.185, "E": 0.230}
REVOLVER_SHARE = {"A": 0.25, "B": 0.38, "C": 0.50, "D": 0.62, "E": 0.72}
SAVINGS_SPREAD_BELOW_FTP = 0.011
LOAN_TERMS_MONTHS = (24, 36, 48, 60)
LOAN_PREPAYMENT_PROBABILITY = 0.008
LOAN_ORIGINATION_FEE_RATE = 0.02
MAX_LOAN_PRINCIPAL = 80_000.0

CARD_SPEND_SEASONALITY = {
    1: 0.92,
    2: 0.93,
    3: 0.98,
    4: 1.00,
    5: 1.02,
    6: 1.03,
    7: 1.04,
    8: 1.02,
    9: 0.99,
    10: 1.01,
    11: 1.10,
    12: 1.25,
}

FEE_AMOUNTS = {
    "monthly_maintenance": 8.00,
    "atm_out_of_network": 3.00,
    "overdraft": 34.00,
    "wire_transfer": 25.00,
    "late_payment": 30.00,
    "annual_fee": 95.00,
}
MAINTENANCE_FEE_BALANCE_THRESHOLD = 1500.0
CASH_ADVANCE_FEE_RATE = 0.05
CASH_ADVANCE_FEE_MINIMUM = 10.00

# Share of fees that end up not being revenue.
FEE_WAIVED_SHARE = 0.04
FEE_REVERSED_SHARE = 0.02

# Funds transfer pricing: the internal annual rate treasury charges for funding
# assets and pays for deposits. Piecewise linear by months since 2023-01.
FTP_CURVE = ((0, 0.0440), (6, 0.0485), (17, 0.0485), (35, 0.0360), (59, 0.0330))

# --------------------------------------------------------------------------- #
# Deliberate data quality problems
# --------------------------------------------------------------------------- #

LATE_ARRIVAL_RATE = 0.005  # transactions posted 3 to 12 days after they happened


@dataclass(frozen=True)
class DirtRates:
    """Share of records hit by each problem. All zero when dirt is disabled."""

    duplicate_transaction: float = 0.004
    alternate_date_format: float = 0.010
    null_amount: float = 0.001
    orphan_account: float = 0.001
    before_account_opening: float = 0.001
    implausible_amount: float = 0.0002
    negative_amount: float = 0.002
    channel_label_variant: float = 0.04
    duplicate_fee: float = 0.003
    orphan_fee: float = 0.001
    null_fee_amount: float = 0.001
    fee_type_label_variant: float = 0.01
    duplicate_balance: float = 0.002
    duplicate_customer: float = 0.002
    segment_label_variant: float = 0.02
    null_segment: float = 0.01

    @classmethod
    def none(cls) -> DirtRates:
        return cls(**{name: 0.0 for name in cls.__dataclass_fields__})


# Messy spellings found in the source systems, keyed by the clean value.
CHANNEL_LABEL_VARIANTS = {
    "branch": ("Branch", "BRANCH", "branch ", "BR"),
    "mobile_app": ("Mobile App", "mobile", "MOBILE_APP", "mobile-app"),
    "online": ("Online", "web", "ONLINE", "internet banking"),
    "call_center": ("Call Center", "call-center", "phone"),
    "partner": ("Partner", "PARTNER", "referral partner"),
    "atm": ("ATM", "atm "),
    "pos": ("POS", "point of sale"),
    "automated": ("AUTO", "Automated", "system"),
}

# The finance team types channel names by hand in the budget workbook.
BUDGET_CHANNEL_LABELS = {
    "branch": "Branch",
    "mobile_app": "Mobile App",
    "online": "Online",
    "call_center": "Call Center",
    "partner": "Partner",
}

IMPLAUSIBLE_AMOUNT_RANGE = (1_000_000.0, 9_999_999.0)
