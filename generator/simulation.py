"""Simulates the fictional bank customer by customer, month by month.

Design notes
------------
* Every customer owns a random stream seeded by ``(seed, customer number)`` and
  is simulated forward in time. Extending the end month therefore appends new
  history without rewriting the months already generated, which is what a
  scheduled load expects.
* Data quality problems are injected while records are emitted, and each one is
  counted in ``Dataset.dirt_counts`` so the pipeline can be checked against the
  exact number of records it should have repaired or rejected.
* ``Dataset.revenue_truth`` accumulates the revenue of the clean records. It is
  the basis for the budget file and a control total for the dbt models.
"""

from __future__ import annotations

import calendar
import math
import random
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta

from generator import config as cfg

TREND_ANCHOR = 2023 * 12  # January 2023: origin of the channel-mix and FTP trends


# --------------------------------------------------------------------------- #
# Month arithmetic. A month is an integer: year * 12 + (month - 1).
# --------------------------------------------------------------------------- #


def month_index(year: int, month: int) -> int:
    return year * 12 + month - 1


def parse_month(text: str) -> int:
    year, month = text.split("-")
    return month_index(int(year), int(month))


def month_number(index: int) -> int:
    return index % 12 + 1


def month_start(index: int) -> date:
    return date(index // 12, month_number(index), 1)


def month_label(index: int) -> str:
    return f"{index // 12:04d}-{month_number(index):02d}"


def days_in_month(index: int) -> int:
    return calendar.monthrange(index // 12, month_number(index))[1]


def ftp_rate(month: int) -> float:
    """Annual funds transfer pricing rate for a month, from the configured curve."""
    position = month - TREND_ANCHOR
    points = cfg.FTP_CURVE
    if position <= points[0][0]:
        return points[0][1]
    for (x0, y0), (x1, y1) in zip(points, points[1:], strict=False):
        if position <= x1:
            return round(y0 + (y1 - y0) * (position - x0) / (x1 - x0), 4)
    return points[-1][1]


# --------------------------------------------------------------------------- #
# Containers
# --------------------------------------------------------------------------- #


@dataclass
class Settings:
    seed: int = 42
    start_month: int = month_index(2023, 1)
    end_month: int = month_index(2025, 12)
    scale: float = 1.0
    dirt: cfg.DirtRates = field(default_factory=cfg.DirtRates)


@dataclass
class Dataset:
    customers: list[tuple] = field(default_factory=list)
    accounts: list[tuple] = field(default_factory=list)
    transactions: dict[int, list[tuple]] = field(default_factory=lambda: defaultdict(list))
    fees: dict[int, list[tuple]] = field(default_factory=lambda: defaultdict(list))
    balances: dict[int, list[tuple]] = field(default_factory=lambda: defaultdict(list))
    funds_transfer_rates: list[tuple] = field(default_factory=list)
    acquisition_spend: dict[int, list[tuple]] = field(default_factory=lambda: defaultdict(list))
    revenue_budget: dict[int, list[tuple]] = field(default_factory=lambda: defaultdict(list))
    # (month, product_id, sales_channel) -> revenue of clean records
    revenue_truth: dict[tuple, float] = field(default_factory=lambda: defaultdict(float))
    revenue_truth_by_type: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    new_customers: dict[tuple, int] = field(default_factory=lambda: defaultdict(int))
    dirt_counts: Counter = field(default_factory=Counter)
    pending_late_arrivals: int = 0


@dataclass
class Account:
    number: int
    account_id: str
    product: cfg.Product
    sales_channel: str
    open_month: int
    open_day: int
    annual_rate: float = 0.0
    credit_limit: float | None = None
    original_principal: float | None = None
    term_months: int | None = None
    status: str = "open"
    closed_date: date | None = None
    # Behavior state, not exported.
    level: float = 0.0  # typical deposit balance
    biweekly_pay: bool = False
    pay_day: int = 1
    debit_frequency: float = 0.0
    atm_frequency: float = 0.0
    base_spend: float = 0.0
    average_ticket: float = 50.0
    revolver: bool = False
    utilization: float = 0.0
    revolving_balance: float = 0.0
    previous_spend: float = 0.0
    principal: float = 0.0
    installment: float = 0.0
    due_day: int = 1
    transaction_count: int = 0
    fee_count: int = 0

    @property
    def opened_date(self) -> date:
        return date(self.open_month // 12, month_number(self.open_month), self.open_day)


# --------------------------------------------------------------------------- #
# Random helpers
# --------------------------------------------------------------------------- #


def pick(rng: random.Random, weights: dict[str, float]) -> str:
    return rng.choices(list(weights), weights=list(weights.values()), k=1)[0]


def poisson(rng: random.Random, mean: float) -> int:
    if mean <= 0:
        return 0
    limit = math.exp(-mean)
    count, product = 0, rng.random()
    while product > limit:
        count += 1
        product *= rng.random()
    return count


def lognormal(rng: random.Random, sigma: float) -> float:
    """Multiplicative noise with mean 1."""
    return rng.lognormvariate(-sigma * sigma / 2, sigma)


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def first_dirt(draw: float, rates: tuple[tuple[str, float], ...]) -> str | None:
    """Maps one uniform draw to at most one of several mutually exclusive problems."""
    threshold = 0.0
    for name, rate in rates:
        threshold += rate
        if draw < threshold:
            return name
    return None


# --------------------------------------------------------------------------- #
# One customer
# --------------------------------------------------------------------------- #


class CustomerSimulation:
    def __init__(self, settings: Settings, data: Dataset, number: int, signup_month: int):
        self.settings = settings
        self.data = data
        self.number = number
        self.signup_month = signup_month
        self.rng = rng = random.Random(f"{settings.seed}:customer:{number}")
        self.customer_id = f"CUS-{number:06d}"
        self.accounts: list[Account] = []

        trend = clamp((signup_month - TREND_ANCHOR) / 35, 0.0, 1.0)
        mix = {c: a + (b - a) * trend for c, (a, b) in cfg.CHANNEL_MIX_DRIFT.items()}
        self.channel = pick(rng, mix)
        self.segment = pick(rng, cfg.SEGMENT_MIX[self.channel])
        self.region = pick(rng, cfg.REGIONS)
        self.risk_band = pick(rng, cfg.RISK_MIX[self.channel])
        age = int(clamp(rng.gauss(cfg.CHANNEL_AGE[self.channel], 11), 18, 82))
        self.birth_year = signup_month // 12 - age
        self.digital = clamp(
            cfg.CHANNEL_DIGITAL[self.channel] + (40 - age) / 120 + rng.gauss(0, 0.12), 0.02, 0.98
        )
        self.income = cfg.SEGMENT_MONTHLY_INCOME[self.segment] * lognormal(rng, 0.25)
        self.is_test = rng.random() < cfg.INTERNAL_TEST_CUSTOMER_RATE
        self.signup_day = rng.randint(1, days_in_month(signup_month))

        first_product = pick(rng, cfg.FIRST_PRODUCT_MIX[self.channel])
        if first_product == "CC_PREM" and self.segment == "mass_market":
            first_product = "CC_STD"
        self.open_account(first_product, signup_month, self.signup_day, self.channel)

    # -- lifecycle ---------------------------------------------------------- #

    def run(self) -> None:
        rng = self.rng
        for month in range(self.signup_month, self.settings.end_month + 1):
            if not any(a.status == "open" for a in self.accounts):
                break
            tenure = month - self.signup_month
            days = days_in_month(month)
            end_day: int | None = None
            defaulted: Account | None = None
            churned = False

            if tenure >= 1:
                open_accounts = [a for a in self.accounts if a.status == "open"]
                if rng.random() < self.churn_hazard(tenure, len(open_accounts)):
                    end_day, churned = rng.randint(1, days), True
                else:
                    for account in open_accounts:
                        if account.product.balance_side != "asset":
                            continue
                        if rng.random() < self.default_hazard(account, month):
                            end_day, defaulted = rng.randint(10, days), account
                            break
                    if end_day is None:
                        self.cross_sell(month, days)

            for account in [a for a in self.accounts if a.status == "open"]:
                first_day = account.open_day if account.open_month == month else 1
                last_day = end_day or days
                account.transaction_count = account.fee_count = 0
                self.account_month(
                    account,
                    month,
                    first_day,
                    last_day,
                    fraction=(last_day - first_day + 1) / days,
                    default=account is defaulted,
                    churn=churned,
                )

            if end_day is not None:
                closing = date(month // 12, month_number(month), end_day)
                for account in self.accounts:
                    if account.status == "open":
                        account.status = "charged_off" if account is defaulted else "closed"
                        account.closed_date = closing

    def churn_hazard(self, tenure: int, open_accounts: int) -> float:
        base = next(rate for limit, rate in cfg.CHURN_BY_TENURE if tenure <= limit)
        products = cfg.CHURN_PRODUCT_COUNT_FACTOR.get(open_accounts, cfg.CHURN_MANY_PRODUCTS_FACTOR)
        return (
            base
            * cfg.CHURN_CHANNEL_FACTOR[self.channel]
            * cfg.CHURN_SEGMENT_FACTOR[self.segment]
            * products
        )

    def default_hazard(self, account: Account, month: int) -> float:
        hazard = cfg.DEFAULT_HAZARD[self.risk_band]
        if account.product.product_category == "credit_card":
            factor = (
                cfg.DEFAULT_REVOLVER_FACTOR if account.revolver else cfg.DEFAULT_TRANSACTOR_FACTOR
            )
            hazard *= factor
        # A charge-off follows months of missed payments, so new accounts cannot default yet.
        months_open = month - account.open_month
        seasoning = clamp(
            (months_open - cfg.DEFAULT_SEASONING_START) / cfg.DEFAULT_SEASONING_RAMP, 0.0, 1.0
        )
        return hazard * seasoning

    def cross_sell(self, month: int, days: int) -> None:
        rng = self.rng
        held = {a.product.product_category for a in self.accounts if a.status == "open"}
        probability = (
            cfg.CROSS_SELL_BASE
            * cfg.CROSS_SELL_SEGMENT_FACTOR[self.segment]
            * cfg.CROSS_SELL_CHANNEL_FACTOR[self.channel]
        )
        for category in ("checking", "savings", "credit_card", "personal_loan"):
            if category in held or rng.random() >= probability:
                continue
            if category == "credit_card":
                premium = self.segment != "mass_market" and rng.random() < 0.35
                product_id = "CC_PREM" if premium else "CC_STD"
            else:
                product_id = {
                    "checking": "CHK_STD",
                    "savings": "SAV_HY",
                    "personal_loan": "PL_STD",
                }[category]
            if rng.random() < cfg.CROSS_SELL_SAME_CHANNEL_SHARE:
                sales_channel = self.channel
            else:
                sales_channel = pick(
                    rng, {"mobile_app": 0.45, "online": 0.25, "branch": 0.20, "call_center": 0.10}
                )
            self.open_account(product_id, month, rng.randint(1, days), sales_channel)

    def open_account(self, product_id: str, month: int, day: int, sales_channel: str) -> None:
        rng = self.rng
        product = cfg.PRODUCTS[product_id]
        number = len(self.accounts) + 1
        account = Account(
            number=number,
            account_id=f"ACC-{self.number:06d}-{number:02d}",
            product=product,
            sales_channel=sales_channel,
            open_month=month,
            open_day=day,
            due_day=rng.randint(1, 28),
        )
        category = product.product_category
        if category == "checking":
            account.level = 0.55 * self.income * lognormal(rng, 0.5)
            account.biweekly_pay = rng.random() < 0.45
            account.pay_day = rng.randint(1, 28)
            account.debit_frequency = (6 + 14 * self.digital) * rng.uniform(0.6, 1.4)
            account.atm_frequency = (0.6 + 3.0 * (1 - self.digital)) * rng.uniform(0.5, 1.5)
        elif category == "savings":
            account.annual_rate = round(
                max(0.005, ftp_rate(month) - cfg.SAVINGS_SPREAD_BELOW_FTP), 4
            )
            account.level = 2.5 * self.income * lognormal(rng, 0.7)
        elif category == "credit_card":
            account.annual_rate = cfg.CARD_APR[self.risk_band]
            risk_factor = {"A": 1.3, "B": 1.1, "C": 0.9, "D": 0.7, "E": 0.5}[self.risk_band]
            account.credit_limit = max(500.0, round(3 * self.income * risk_factor / 500) * 500.0)
            spend_share = 0.28 if product_id == "CC_PREM" else 0.18
            account.base_spend = spend_share * self.income * lognormal(rng, 0.35)
            account.average_ticket = rng.uniform(38, 85)
            account.revolver = rng.random() < cfg.REVOLVER_SHARE[self.risk_band]
            account.utilization = rng.uniform(0.25, 0.85)
        elif category == "personal_loan":
            account.annual_rate = round(
                cfg.LOAN_APR[self.risk_band] + rng.uniform(-0.005, 0.005), 4
            )
            principal = round(2.2 * self.income * lognormal(rng, 0.45) / 100) * 100.0
            principal = clamp(principal, 1000.0, cfg.MAX_LOAN_PRINCIPAL)
            term = rng.choice(cfg.LOAN_TERMS_MONTHS)
            monthly_rate = account.annual_rate / 12
            account.original_principal = account.principal = principal
            account.term_months = term
            account.installment = round(
                principal * monthly_rate / (1 - (1 + monthly_rate) ** -term), 2
            )
        self.accounts.append(account)

    # -- monthly activity by product ---------------------------------------- #

    def account_month(
        self,
        account: Account,
        month: int,
        first_day: int,
        last_day: int,
        *,
        fraction: float,
        default: bool,
        churn: bool,
    ) -> None:
        category = account.product.product_category
        if category == "checking":
            self.checking_month(account, month, first_day, last_day, fraction)
        elif category == "savings":
            self.savings_month(account, month, first_day, last_day, fraction)
        elif category == "credit_card":
            self.card_month(account, month, first_day, last_day, fraction, default, churn)
        else:
            self.loan_month(account, month, first_day, last_day, fraction, default, churn)

    def checking_month(
        self, account: Account, month: int, first_day: int, last_day: int, fraction: float
    ) -> None:
        rng = self.rng
        account.level *= math.exp(rng.gauss(0.003, 0.05))
        full_month_balance = max(20.0, account.level * lognormal(rng, 0.15))
        balance = full_month_balance * fraction
        self.emit_balance(account, month, balance, balance)

        def any_day() -> int:
            return rng.randint(first_day, last_day)

        days = days_in_month(month)
        pay_days = (15, days) if account.biweekly_pay else (min(account.pay_day, days),)
        for pay_day in pay_days:
            if first_day <= pay_day <= last_day:
                amount = self.income / len(pay_days) * rng.uniform(0.97, 1.03)
                self.emit_transaction(account, month, pay_day, "deposit", "automated", amount)

        online_share = 0.12 + 0.15 * self.digital
        for _ in range(poisson(rng, account.debit_frequency * fraction)):
            channel = "online" if rng.random() < online_share else "pos"
            amount = rng.lognormvariate(3.2, 0.8)
            self.emit_transaction(account, month, any_day(), "debit_purchase", channel, amount)

        for _ in range(poisson(rng, account.atm_frequency * fraction)):
            channel = "atm" if rng.random() < 0.88 else "branch"
            amount = float(rng.choice((20, 40, 60, 80, 100, 200)))
            day = any_day()
            transaction_id = self.emit_transaction(
                account, month, day, "withdrawal", channel, amount
            )
            if channel == "atm" and rng.random() < 0.22:
                self.emit_fee(
                    account, month, day, "atm_out_of_network", transaction_id=transaction_id
                )

        for _ in range(poisson(rng, 1.5 * fraction)):
            amount = rng.lognormvariate(5.0, 1.0)
            day = any_day()
            transaction_id = self.emit_transaction(
                account, month, day, "transfer_out", self.self_service_channel(), amount
            )
            if amount > 1500 and rng.random() < 0.30:
                self.emit_fee(account, month, day, "wire_transfer", transaction_id=transaction_id)

        for _ in range(poisson(rng, 0.8 * fraction)):
            amount = rng.lognormvariate(5.0, 0.9)
            self.emit_transaction(account, month, any_day(), "transfer_in", "automated", amount)

        for _ in range(poisson(rng, 2.5 * fraction)):
            channel = "automated" if rng.random() < 0.35 else self.self_service_channel()
            amount = rng.lognormvariate(4.4, 0.6)
            self.emit_transaction(account, month, any_day(), "bill_payment", channel, amount)

        for _ in range(poisson(rng, 0.4 * fraction)):
            amount = rng.lognormvariate(5.5, 0.8)
            self.emit_transaction(
                account, month, any_day(), "deposit", self.deposit_channel(), amount
            )

        charges_maintenance = (
            fraction >= 0.5
            and full_month_balance < cfg.MAINTENANCE_FEE_BALANCE_THRESHOLD
            and self.segment != "affluent"
        )
        if charges_maintenance:
            self.emit_fee(account, month, last_day, "monthly_maintenance")
        overdraft_probability = 0.22 if account.level < 500 else 0.02
        if rng.random() < overdraft_probability * fraction:
            self.emit_fee(account, month, any_day(), "overdraft")

    def savings_month(
        self, account: Account, month: int, first_day: int, last_day: int, fraction: float
    ) -> None:
        rng = self.rng
        account.level *= math.exp(rng.gauss(0.008, 0.035))
        balance = account.level * fraction
        self.emit_balance(account, month, balance, balance)
        for _ in range(poisson(rng, 0.9 * fraction)):
            amount = min(50_000.0, 0.08 * self.income * lognormal(rng, 0.6))
            day = rng.randint(first_day, last_day)
            self.emit_transaction(
                account, month, day, "deposit", self.self_service_channel(), amount
            )
        for _ in range(poisson(rng, 0.35 * fraction)):
            amount = rng.lognormvariate(5.3, 0.8)
            day = rng.randint(first_day, last_day)
            self.emit_transaction(
                account, month, day, "withdrawal", self.self_service_channel(), amount
            )

    def card_month(
        self,
        account: Account,
        month: int,
        first_day: int,
        last_day: int,
        fraction: float,
        default: bool,
        churn: bool,
    ) -> None:
        rng = self.rng
        season = cfg.CARD_SPEND_SEASONALITY[month_number(month)]
        target_spend = account.base_spend * season * lognormal(rng, 0.22) * fraction
        spend = 0.0
        if target_spend > 5:
            purchases = max(1, round(target_spend / account.average_ticket * rng.uniform(0.8, 1.2)))
            weights = [rng.random() + 0.2 for _ in range(purchases)]
            total_weight = sum(weights)
            online_share = 0.20 + 0.25 * self.digital
            for weight in weights:
                amount = round(target_spend * weight / total_weight, 2)
                channel = "online" if rng.random() < online_share else "pos"
                day = rng.randint(first_day, last_day)
                self.emit_transaction(account, month, day, "purchase", channel, amount)
                spend += amount

        if account.revolver and rng.random() < 0.03 * fraction:
            amount = float(rng.choice((100, 200, 300, 400, 500)))
            day = rng.randint(first_day, last_day)
            transaction_id = self.emit_transaction(
                account, month, day, "cash_advance", "atm", amount
            )
            fee = max(cfg.CASH_ADVANCE_FEE_MINIMUM, cfg.CASH_ADVANCE_FEE_RATE * amount)
            self.emit_fee(
                account, month, day, "cash_advance_fee", fee, transaction_id=transaction_id
            )

        previous_revolving = account.revolving_balance
        revolving = 0.0
        if account.revolver:
            target = account.utilization * account.credit_limit * 0.6
            noise = rng.gauss(0, 0.03 * account.credit_limit)
            revolving = max(0.0, previous_revolving + 0.3 * (target - previous_revolving) + noise)

        months_open = month - account.open_month
        if months_open >= 1 and not default:
            owed = account.previous_spend + previous_revolving
            payment = max(owed - revolving, 0.02 * owed)
            if payment >= 1:
                day = clamp(account.due_day, first_day, last_day)
                self.emit_transaction(
                    account, month, day, "payment", self.payment_channel(), payment
                )
                if rng.random() < cfg.LATE_PAYMENT_PROBABILITY[self.risk_band]:
                    self.emit_fee(account, month, day, "late_payment")

        if account.product.product_id == "CC_PREM" and months_open % 12 == 0:
            self.emit_fee(account, month, first_day, "annual_fee")

        average_revolving = (previous_revolving + revolving) / 2 * fraction
        self.emit_balance(account, month, 0.9 * spend + average_revolving, average_revolving)
        account.revolving_balance, account.previous_spend = revolving, spend

        outstanding = revolving + spend
        if default and outstanding > 0:
            self.emit_transaction(account, month, last_day, "charge_off", "automated", outstanding)
        elif churn and outstanding > 0:
            self.emit_transaction(
                account, month, last_day, "payment", self.payment_channel(), outstanding
            )

    def loan_month(
        self,
        account: Account,
        month: int,
        first_day: int,
        last_day: int,
        fraction: float,
        default: bool,
        churn: bool,
    ) -> None:
        rng = self.rng
        opening = account.principal
        closing = opening
        if month == account.open_month:
            self.emit_transaction(account, month, first_day, "disbursement", "automated", opening)
            fee = round(cfg.LOAN_ORIGINATION_FEE_RATE * opening, 2)
            self.emit_fee(account, month, first_day, "loan_origination", fee, always_posted=True)
        elif not default:
            interest = opening * account.annual_rate / 12
            payment = min(account.installment, opening + interest)
            day = min(last_day, account.due_day)
            self.emit_transaction(
                account, month, day, "loan_payment", self.payment_channel(), payment
            )
            closing = max(0.0, opening + interest - payment)
            if rng.random() < cfg.LATE_PAYMENT_PROBABILITY[self.risk_band]:
                self.emit_fee(account, month, day, "late_payment")
            if closing > 0.01 and rng.random() < cfg.LOAN_PREPAYMENT_PROBABILITY:
                day = rng.randint(day, last_day)
                self.emit_transaction(
                    account, month, day, "loan_payment", self.payment_channel(), closing
                )
                closing = 0.0
            if closing <= 0.01:
                closing = 0.0
                account.status = "closed"
                account.closed_date = date(month // 12, month_number(month), day)

        balance = (opening + closing) / 2 * fraction
        self.emit_balance(account, month, balance, balance)
        account.principal = closing

        if default and closing > 0:
            self.emit_transaction(account, month, last_day, "charge_off", "automated", closing)
        elif churn and account.status == "open" and closing > 0:
            self.emit_transaction(
                account, month, last_day, "loan_payment", self.payment_channel(), closing
            )
            account.principal = 0.0

    # -- channel preferences ------------------------------------------------ #

    def self_service_channel(self) -> str:
        d = self.digital
        return pick(
            self.rng,
            {
                "mobile_app": 0.25 + 0.60 * d,
                "online": 0.25,
                "branch": 0.25 * (1 - d) + 0.03,
                "call_center": 0.05 * (1 - d) + 0.01,
            },
        )

    def deposit_channel(self) -> str:
        d = self.digital
        return pick(
            self.rng, {"atm": 0.35, "branch": 0.35 * (1 - d) + 0.05, "mobile_app": 0.30 + 0.40 * d}
        )

    def payment_channel(self) -> str:
        return "automated" if self.rng.random() < 0.40 else self.self_service_channel()

    # -- record emission ---------------------------------------------------- #

    def emit_transaction(
        self,
        account: Account,
        month: int,
        day: int,
        transaction_type: str,
        channel: str,
        amount: float,
    ) -> str:
        """Writes one transaction and returns its id ("" when nothing was written)."""
        rng, data, dirt = self.rng, self.data, self.settings.dirt
        amount = round(amount, 2)
        if amount <= 0:
            return ""
        account.transaction_count += 1
        transaction_id = (
            f"TXN-{self.number:06d}{account.number:02d}-{month_label(month).replace('-', '')[2:]}"
            f"-{account.transaction_count:04d}"
        )
        transaction_date = date(month // 12, month_number(month), day)
        lag = rng.randint(0, 2)
        if rng.random() < cfg.LATE_ARRIVAL_RATE:
            lag = rng.randint(3, 12)
        posted_date = transaction_date + timedelta(days=lag)
        posted_month = month_index(posted_date.year, posted_date.month)

        problem = first_dirt(
            rng.random(),
            (
                ("duplicate_transaction", dirt.duplicate_transaction),
                ("alternate_date_format", dirt.alternate_date_format),
                ("null_amount", dirt.null_amount),
                ("orphan_account", dirt.orphan_account),
                ("before_account_opening", dirt.before_account_opening),
                ("implausible_amount", dirt.implausible_amount),
                ("negative_amount", dirt.negative_amount),
            ),
        )
        channel_variant = rng.random() < dirt.channel_label_variant
        variant_label = rng.choice(cfg.CHANNEL_LABEL_VARIANTS[channel])
        orphan_suffix = rng.randrange(100_000)
        backdate_days = rng.randint(5, 40)
        implausible = rng.uniform(*cfg.IMPLAUSIBLE_AMOUNT_RANGE)

        if posted_month > self.settings.end_month:
            data.pending_late_arrivals += 1
            return ""

        account_id = account.account_id
        date_text = transaction_date.isoformat()
        amount_text = f"{amount:.2f}"
        rejected = False
        if problem == "alternate_date_format":
            date_text = transaction_date.strftime("%m/%d/%Y")
        elif problem == "null_amount":
            amount_text, rejected = "", True
        elif problem == "orphan_account":
            account_id, rejected = f"ACC-9{orphan_suffix:05d}-01", True
        elif problem == "before_account_opening":
            date_text = (account.opened_date - timedelta(days=backdate_days)).isoformat()
            rejected = True
        elif problem == "implausible_amount":
            amount_text, rejected = f"{implausible:.2f}", True
        elif problem == "negative_amount":
            amount_text = f"{-amount:.2f}"
        if problem:
            data.dirt_counts[problem] += 1
        if channel_variant:
            channel = variant_label
            data.dirt_counts["channel_label_variant"] += 1

        row = (
            transaction_id,
            account_id,
            date_text,
            posted_date.isoformat(),
            transaction_type,
            channel,
            amount_text,
        )
        rows = data.transactions[posted_month]
        rows.append(row)
        if problem == "duplicate_transaction":
            rows.append(row)

        earns_interchange = transaction_type in ("purchase", "debit_purchase")
        if earns_interchange and not rejected:
            self.add_revenue(
                account, month, "interchange", amount * account.product.interchange_rate
            )
        return transaction_id

    def emit_fee(
        self,
        account: Account,
        month: int,
        day: int,
        fee_type: str,
        amount: float | None = None,
        *,
        transaction_id: str = "",
        always_posted: bool = False,
    ) -> None:
        rng, data, dirt = self.rng, self.data, self.settings.dirt
        amount = round(cfg.FEE_AMOUNTS[fee_type] if amount is None else amount, 2)
        account.fee_count += 1
        fee_id = (
            f"FEE-{self.number:06d}{account.number:02d}-{month_label(month).replace('-', '')[2:]}"
            f"-{account.fee_count:02d}"
        )
        status_draw = rng.random()
        status = "posted"
        if not always_posted:
            if status_draw < cfg.FEE_WAIVED_SHARE:
                status = "waived"
            elif status_draw < cfg.FEE_WAIVED_SHARE + cfg.FEE_REVERSED_SHARE:
                status = "reversed"

        problem = first_dirt(
            rng.random(),
            (
                ("duplicate_fee", dirt.duplicate_fee),
                ("orphan_fee", dirt.orphan_fee),
                ("null_fee_amount", dirt.null_fee_amount),
                ("fee_type_label_variant", dirt.fee_type_label_variant),
            ),
        )
        orphan_suffix = rng.randrange(100_000)

        account_id = account.account_id
        amount_text = f"{amount:.2f}"
        type_text = fee_type
        rejected = False
        if problem == "orphan_fee":
            account_id, rejected = f"ACC-9{orphan_suffix:05d}-01", True
        elif problem == "null_fee_amount":
            amount_text, rejected = "", True
        elif problem == "fee_type_label_variant":
            type_text = fee_type.replace("_", " ").title()
        if problem:
            data.dirt_counts[problem] += 1

        fee_date = date(month // 12, month_number(month), day)
        row = (
            fee_id,
            account_id,
            transaction_id,
            type_text,
            fee_date.isoformat(),
            amount_text,
            status,
        )
        rows = data.fees[month]
        rows.append(row)
        if problem == "duplicate_fee":
            rows.append(row)
        if status == "posted" and not rejected:
            self.add_revenue(account, month, "fees", amount)

    def emit_balance(
        self, account: Account, month: int, average_balance: float, interest_bearing_balance: float
    ) -> None:
        data = self.data
        average = round(average_balance, 2)
        interest_bearing = round(interest_bearing_balance, 2)
        row = (
            account.account_id,
            month_start(month).isoformat(),
            f"{average:.2f}",
            f"{interest_bearing:.2f}",
        )
        rows = data.balances[month]
        rows.append(row)
        if self.rng.random() < self.settings.dirt.duplicate_balance:
            rows.append(row)
            data.dirt_counts["duplicate_balance"] += 1

        customer_interest = interest_bearing * account.annual_rate / 12
        funds_transfer = average * ftp_rate(month) / 12
        if account.product.balance_side == "asset":
            net_interest = customer_interest - funds_transfer
        else:
            net_interest = funds_transfer - customer_interest
        self.add_revenue(account, month, "net_interest_income", net_interest)

    def add_revenue(self, account: Account, month: int, revenue_type: str, amount: float) -> None:
        if self.is_test:
            return
        key = (month, account.product.product_id, account.sales_channel)
        self.data.revenue_truth[key] += amount
        self.data.revenue_truth_by_type[revenue_type] += amount

    # -- export ------------------------------------------------------------- #

    def export(self) -> None:
        # A dedicated stream keeps the customer row identical however many months were simulated.
        self.export_rng = rng = random.Random(f"{self.settings.seed}:export:{self.number}")
        data, dirt = self.data, self.settings.dirt
        signup = date(self.signup_month // 12, month_number(self.signup_month), self.signup_day)

        segment = self.segment
        problem = first_dirt(
            rng.random(),
            (
                ("null_segment", dirt.null_segment),
                ("segment_label_variant", dirt.segment_label_variant),
            ),
        )
        variant = rng.choice((segment.upper(), segment.replace("_", " ").title(), f" {segment}"))
        if problem == "null_segment":
            segment = ""
        elif problem == "segment_label_variant":
            segment = variant
        if problem:
            data.dirt_counts[problem] += 1

        row = (
            self.customer_id,
            signup.isoformat(),
            segment,
            self.region,
            str(self.birth_year),
            self.risk_band,
            self.messy_channel(self.channel),
            "true" if self.is_test else "false",
        )
        data.customers.append(row)
        if rng.random() < dirt.duplicate_customer:
            data.customers.append(row)
            data.dirt_counts["duplicate_customer"] += 1

        for account in self.accounts:
            data.accounts.append(
                (
                    account.account_id,
                    self.customer_id,
                    account.product.product_id,
                    account.opened_date.isoformat(),
                    account.closed_date.isoformat() if account.closed_date else "",
                    account.status,
                    self.messy_channel(account.sales_channel),
                    f"{account.annual_rate:.4f}",
                    f"{account.credit_limit:.2f}" if account.credit_limit is not None else "",
                    f"{account.original_principal:.2f}"
                    if account.original_principal is not None
                    else "",
                    str(account.term_months) if account.term_months is not None else "",
                )
            )
        if not self.is_test:
            data.new_customers[(self.signup_month, self.channel)] += 1

    def messy_channel(self, channel: str) -> str:
        variant = self.export_rng.choice(cfg.CHANNEL_LABEL_VARIANTS[channel])
        if self.export_rng.random() < self.settings.dirt.channel_label_variant:
            self.data.dirt_counts["channel_label_variant"] += 1
            return variant
        return channel


# --------------------------------------------------------------------------- #
# The whole bank
# --------------------------------------------------------------------------- #


def simulate(settings: Settings) -> Dataset:
    data = Dataset()
    customer_number = 0
    for month in range(settings.start_month, settings.end_month + 1):
        rng = random.Random(f"{settings.seed}:acquisition:{month_label(month)}")
        expected = (
            (cfg.ACQUISITION_BASE + cfg.ACQUISITION_GROWTH * (month - TREND_ANCHOR))
            * cfg.ACQUISITION_SEASONALITY[month_number(month)]
            * rng.uniform(0.92, 1.08)
            * settings.scale
        )
        for _ in range(max(1, round(expected))):
            customer_number += 1
            customer = CustomerSimulation(settings, data, customer_number, month)
            customer.run()
            customer.export()

    for month in range(settings.start_month, settings.end_month + 1):
        data.funds_transfer_rates.append((month_start(month).isoformat(), f"{ftp_rate(month):.4f}"))
    _add_acquisition_spend(settings, data)
    _add_revenue_budget(settings, data)
    return data


def _add_acquisition_spend(settings: Settings, data: Dataset) -> None:
    """Monthly sales and marketing spend by channel, as finance would report it."""
    for month in range(settings.start_month, settings.end_month + 1):
        years_elapsed = max(0, month // 12 - TREND_ANCHOR // 12)
        for channel in cfg.SALES_CHANNELS:
            rng = random.Random(f"{settings.seed}:spend:{month_label(month)}:{channel}")
            unit_cost = (
                cfg.ACQUISITION_UNIT_COST[channel]
                * cfg.ACQUISITION_COST_DRIFT[channel] ** years_elapsed
            )
            spend = data.new_customers[(month, channel)] * unit_cost * rng.uniform(0.92, 1.08)
            data.acquisition_spend[month // 12].append(
                (month_start(month).isoformat(), channel, f"{spend:.2f}")
            )


def _add_revenue_budget(settings: Settings, data: Dataset) -> None:
    """Revenue plan by month, product and sales channel.

    The plan is the clean revenue distorted by a yearly bias per product and by
    monthly noise, so the budget-versus-actual report has real variances to show.
    One product and channel pair was never planned in 2023.
    """
    for (month, product_id, channel), revenue in sorted(data.revenue_truth.items()):
        year = month // 12
        if (product_id, channel, year) == ("SAV_HY", "partner", 2023):
            continue
        bias = random.Random(f"{settings.seed}:budget-bias:{product_id}:{year}").uniform(
            -0.10, 0.12
        )
        noise = random.Random(
            f"{settings.seed}:budget:{product_id}:{channel}:{month_label(month)}"
        ).uniform(-0.04, 0.04)
        budget = round(revenue * (1 + bias) * (1 + noise))
        data.revenue_budget[year].append(
            (
                month_start(month).isoformat(),
                product_id,
                cfg.BUDGET_CHANNEL_LABELS[channel],
                str(budget),
            )
        )
