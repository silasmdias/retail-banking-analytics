-- Actual revenue in the budget report must equal revenue by product and
-- channel, month by month and to the cent. Returns the months where it does not.
-- Debug: a difference means the full join in `budget_vs_actual` duplicated
-- rows; check `stg_finance__revenue_budget` for repeated plan lines.

with

budget_report as (

    select
        reporting_month,
        sum(actual_revenue) as actual_revenue
    from {{ ref('budget_vs_actual') }}
    where actual_revenue is not null
    group by 1

),

revenue as (

    select
        reporting_month,
        sum(total_revenue) as total_revenue
    from {{ ref('revenue_by_product_channel') }}
    group by 1

)

select
    coalesce(budget_report.reporting_month, revenue.reporting_month) as reporting_month,
    budget_report.actual_revenue,
    revenue.total_revenue
from budget_report
full outer join revenue
    on budget_report.reporting_month = revenue.reporting_month
where budget_report.actual_revenue is distinct from revenue.total_revenue
