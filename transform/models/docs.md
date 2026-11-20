{% docs __overview__ %}

# Retail banking analytics

dbt models over the synthetic data of a fictional retail bank. The project
answers three questions a retail banking finance team asks every month:

1. **Where does revenue come from?** `revenue_by_product_channel`
2. **What is a customer worth, and does acquisition pay back?**
   `customer_lifetime_value` and `customer_lifetime_value_curve`
3. **Are we on plan?** `budget_vs_actual`

Layers: `staging` types and cleans one source table at a time, `intermediate`
screens records and builds revenue and cost components, `marts` holds the
dimensional core and the finance outputs.

All data is synthetic. No real customer, account or institution is represented.

{% enddocs %}


{% docs sales_channel %}

Channel through which a product was sold: `branch`, `mobile_app`, `online`,
`call_center` or `partner`. Revenue is attributed to the channel that sold the
account, not to the channel where transactions later happen, because interest
and monthly fees have no transaction channel. For a customer, the acquisition
channel is the sales channel of the first account.

{% enddocs %}


{% docs servicing_channel %}

Channel where a transaction took place: `branch`, `atm`, `mobile_app`,
`online`, `pos`, `call_center` or `automated` (system postings such as direct
deposits and scheduled payments). It drives the cost to serve, since a branch
transaction costs far more than a mobile one. Not to be confused with the sales
channel.

{% enddocs %}


{% docs reporting_month %}

First day of the calendar month the figures refer to. Transactions count in the
month they happened (`transaction_date`), not the month they were posted.

{% enddocs %}


{% docs net_interest_income %}

Interest margin after funds transfer pricing (FTP), in US dollars.

* Lending products (asset side): interest charged to the customer minus the
  FTP charge for funding the balance.
* Deposit products (liability side): the FTP credit earned on the balance minus
  interest paid to the customer.

Monthly amounts use one twelfth of the annual rates. It can be negative: a card
paid in full every month earns no interest but its balance still has to be funded.

{% enddocs %}


{% docs fee_revenue %}

Fees kept by the bank, in US dollars. Only fees with status `posted` count.
Waived fees were never collected and reversed fees were refunded, so neither is
revenue.

{% enddocs %}


{% docs interchange_revenue %}

Share of card purchase volume paid to the bank by the card network, in US
dollars: purchase amount times the product's interchange rate. Applies to credit
card purchases and debit card purchases.

{% enddocs %}


{% docs total_revenue %}

`net_interest_income + fee_revenue + interchange_revenue`, in US dollars.

{% enddocs %}


{% docs credit_losses %}

Balances charged off when a credit account defaults, in US dollars, recognized
in the month of the charge-off. Recoveries after charge-off are not modeled.

{% enddocs %}


{% docs cost_to_serve %}

Direct cost of keeping and servicing accounts, in US dollars: a fixed monthly
servicing cost per open account (by product category) plus a unit cost for each
transaction (by servicing channel). Unit costs come from the
`product_servicing_costs` and `channel_transaction_costs` seeds.

{% enddocs %}


{% docs contribution_margin %}

`total_revenue - credit_losses - cost_to_serve`, in US dollars. It is the profit
measure used for customer lifetime value. It excludes acquisition cost (reported
separately as CAC), cost of capital, taxes and fixed overhead.

{% enddocs %}


{% docs cohort_month %}

First day of the month in which the customer was acquired (the month of
`signup_date`). Customers acquired in the same month form a cohort.

{% enddocs %}


{% docs months_since_acquisition %}

Whole months between the cohort month and the reporting month. `0` is the
acquisition month itself, so a customer acquired late in a month has a short
month 0.

{% enddocs %}


{% docs customer_lifetime_value %}

Customer lifetime value (CLV, also written LTV; not loan-to-value): the present
value of the contribution margin an acquired customer is expected to generate
over a fixed horizon.

**How it is computed, per acquisition cohort and channel**

1. *Realized part.* For months the cohort has already lived, the actual
   contribution margin of its customers.
2. *Projected part.* For the remaining months of the horizon, expected margin is
   `customers still active x margin per active customer`. Both come from what
   older cohorts of the same channel showed at the same age:
   * up to `clv_mature_age_months` (24), age-specific retention and margin;
   * beyond it, one pooled retention rate and one pooled margin per active
     customer, taken from all observed ages above 24 months, held constant.
3. *Discounting.* Each month is discounted at `clv_annual_discount_rate` (10% a
   year) back to the acquisition month.
4. *Per customer.* The total is divided by the customers acquired in the
   cohort, not by the customers still active, so churn lowers the value.

Horizon: `clv_horizon_months` (60 months).

**What it is not**

* It is not cumulative revenue: it is margin, after credit losses and cost to serve.
* It does not subtract acquisition cost. CAC is reported next to it, with the
  CLV to CAC ratio and the payback period.
* The projected part is an assumption, not an observation. With three years of
  history, months 37 to 60 are never observed for any cohort. `is_projected`
  and `projected_clv` keep the two parts apart.
* The projection ignores calendar effects such as changes in interest rates.

{% enddocs %}


{% docs customer_acquisition_cost %}

Customer acquisition cost (CAC): sales and marketing spend attributed to a sales
channel in a month, divided by the customers acquired through that channel in
that month. In US dollars per customer.

{% enddocs %}
