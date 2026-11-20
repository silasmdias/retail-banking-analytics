-- Rolling accounts up to customers must not change any total. Returns the
-- months where revenue or contribution margin differ between the two facts.
-- Debug: a difference means an account's customer is missing from
-- `dim_customers`, or a customer appears there more than once.

with

by_customer as (

    select
        reporting_month,
        sum(total_revenue) as total_revenue,
        sum(contribution_margin) as contribution_margin
    from {{ ref('fct_customer_monthly_profitability') }}
    group by 1

),

by_account as (

    select
        reporting_month,
        sum(total_revenue) as total_revenue,
        sum(contribution_margin) as contribution_margin
    from {{ ref('fct_account_monthly_profitability') }}
    group by 1

)

select
    coalesce(by_customer.reporting_month, by_account.reporting_month) as reporting_month,
    by_customer.total_revenue as customer_revenue,
    by_account.total_revenue as account_revenue,
    by_customer.contribution_margin as customer_margin,
    by_account.contribution_margin as account_margin
from by_customer
full outer join by_account
    on by_customer.reporting_month = by_account.reporting_month
where
    by_customer.total_revenue is distinct from by_account.total_revenue
    or by_customer.contribution_margin is distinct from by_account.contribution_margin
