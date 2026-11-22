-- Revenue reported by product and channel must equal the account-level fact,
-- month by month and to the cent. Returns the months where it does not.
-- Debug: a difference means rows were lost or duplicated in the joins of
-- `revenue_by_product_channel` (check `dim_products` for a missing product).

-- The `dagster` entry names the model this test guards, so the orchestrator
-- shows the result on that model.
{{ config(meta={'dagster': {'ref': {'name': 'revenue_by_product_channel'}}}) }}

with

reported as (

    select
        reporting_month,
        sum(total_revenue) as total_revenue
    from {{ ref('revenue_by_product_channel') }}
    group by 1

),

fact as (

    select
        reporting_month,
        sum(total_revenue) as total_revenue
    from {{ ref('fct_account_monthly_profitability') }}
    group by 1

)

select
    coalesce(reported.reporting_month, fact.reporting_month) as reporting_month,
    reported.total_revenue as reported_revenue,
    fact.total_revenue as fact_revenue
from reported
full outer join fact
    on reported.reporting_month = fact.reporting_month
where reported.total_revenue is distinct from fact.total_revenue
