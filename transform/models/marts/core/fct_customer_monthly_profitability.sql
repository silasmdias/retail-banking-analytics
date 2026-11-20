with

account_months as (

    select * from {{ ref('fct_account_monthly_profitability') }}

),

customers as (

    select * from {{ ref('dim_customers') }}

),

aggregated as (

    select
        customer_id,
        reporting_month,
        count(*) as active_accounts,
        sum(net_interest_income) as net_interest_income,
        sum(fee_revenue) as fee_revenue,
        sum(interchange_revenue) as interchange_revenue,
        sum(total_revenue) as total_revenue,
        sum(credit_losses) as credit_losses,
        sum(cost_to_serve) as cost_to_serve,
        sum(contribution_margin) as contribution_margin
    from account_months
    group by 1, 2

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['aggregated.customer_id', 'aggregated.reporting_month']) }}
            as customer_month_id,
        aggregated.customer_id,
        aggregated.reporting_month,
        customers.cohort_month,
        customers.acquisition_channel,
        cast(
            {{ dbt.datediff('customers.cohort_month', 'aggregated.reporting_month', 'month') }}
            as integer
        ) as months_since_acquisition,
        cast(aggregated.active_accounts as integer) as active_accounts,
        cast(aggregated.net_interest_income as decimal(18, 2)) as net_interest_income,
        cast(aggregated.fee_revenue as decimal(18, 2)) as fee_revenue,
        cast(aggregated.interchange_revenue as decimal(18, 2)) as interchange_revenue,
        cast(aggregated.total_revenue as decimal(18, 2)) as total_revenue,
        cast(aggregated.credit_losses as decimal(18, 2)) as credit_losses,
        cast(aggregated.cost_to_serve as decimal(18, 2)) as cost_to_serve,
        cast(aggregated.contribution_margin as decimal(18, 2)) as contribution_margin
    from aggregated
    inner join customers
        on aggregated.customer_id = customers.customer_id

)

select * from final
