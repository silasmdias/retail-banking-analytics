with

budget as (

    select * from {{ ref('stg_finance__revenue_budget') }}

),

actuals as (

    select * from {{ ref('revenue_by_product_channel') }}

),

products as (

    select * from {{ ref('dim_products') }}

),

combined as (

    -- Full join: a line can be planned and earn nothing, or earn revenue that was never planned.
    select
        coalesce(budget.budget_month, actuals.reporting_month) as reporting_month,
        coalesce(budget.product_id, actuals.product_id) as product_id,
        coalesce(budget.sales_channel, actuals.sales_channel) as sales_channel,
        budget.budget_revenue,
        actuals.total_revenue as actual_revenue
    from budget
    full outer join actuals
        on
            budget.budget_month = actuals.reporting_month
            and budget.product_id = actuals.product_id
            and budget.sales_channel = actuals.sales_channel

),

with_variance as (

    select
        *,
        coalesce(actual_revenue, 0) - coalesce(budget_revenue, 0) as variance_amount,
        sum(coalesce(budget_revenue, 0)) over year_to_date as budget_revenue_ytd,
        sum(coalesce(actual_revenue, 0)) over year_to_date as actual_revenue_ytd
    from combined
    window year_to_date as (
        partition by date_trunc('year', reporting_month), product_id, sales_channel
        order by reporting_month
        rows between unbounded preceding and current row
    )

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'with_variance.reporting_month', 'with_variance.product_id', 'with_variance.sales_channel'
        ]) }} as budget_vs_actual_id,
        with_variance.reporting_month,
        with_variance.product_id,
        products.product_name,
        with_variance.sales_channel,
        cast(with_variance.budget_revenue as decimal(18, 2)) as budget_revenue,
        cast(with_variance.actual_revenue as decimal(18, 2)) as actual_revenue,
        cast(with_variance.variance_amount as decimal(18, 2)) as variance_amount,
        cast(
            round(with_variance.variance_amount / nullif(with_variance.budget_revenue, 0), 4)
            as decimal(18, 4)
        ) as variance_pct,
        case
            when with_variance.budget_revenue is null then 'unbudgeted'
            when with_variance.actual_revenue is null then 'no_actuals'
            when abs(with_variance.variance_amount) <= 0.05 * abs(with_variance.budget_revenue)
                then 'on_plan'
            when with_variance.variance_amount > 0 then 'above_plan'
            else 'below_plan'
        end as variance_status,
        cast(with_variance.budget_revenue_ytd as decimal(18, 2)) as budget_revenue_ytd,
        cast(with_variance.actual_revenue_ytd as decimal(18, 2)) as actual_revenue_ytd,
        cast(with_variance.actual_revenue_ytd - with_variance.budget_revenue_ytd as decimal(18, 2))
            as variance_amount_ytd
    from with_variance
    inner join products
        on with_variance.product_id = products.product_id

)

select * from final
