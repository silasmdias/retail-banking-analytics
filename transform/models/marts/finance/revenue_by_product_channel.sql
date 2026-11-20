with

account_months as (

    select * from {{ ref('fct_account_monthly_profitability') }}

),

products as (

    select * from {{ ref('dim_products') }}

),

monthly as (

    select
        reporting_month,
        product_id,
        sales_channel,
        count(*) as active_accounts,
        sum(net_interest_income) as net_interest_income,
        sum(fee_revenue) as fee_revenue,
        sum(interchange_revenue) as interchange_revenue,
        sum(total_revenue) as total_revenue
    from account_months
    group by 1, 2, 3

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'monthly.reporting_month', 'monthly.product_id', 'monthly.sales_channel'
        ]) }} as revenue_by_product_channel_id,
        monthly.reporting_month,
        monthly.product_id,
        products.product_name,
        products.product_category,
        monthly.sales_channel,
        cast(monthly.active_accounts as integer) as active_accounts,
        cast(monthly.net_interest_income as decimal(18, 2)) as net_interest_income,
        cast(monthly.fee_revenue as decimal(18, 2)) as fee_revenue,
        cast(monthly.interchange_revenue as decimal(18, 2)) as interchange_revenue,
        cast(monthly.total_revenue as decimal(18, 2)) as total_revenue,
        cast(round(monthly.total_revenue / monthly.active_accounts, 2) as decimal(18, 2))
            as revenue_per_active_account,
        cast(prior_year.total_revenue as decimal(18, 2)) as total_revenue_prior_year
    from monthly
    inner join products
        on monthly.product_id = products.product_id
    left join monthly as prior_year
        on
            monthly.product_id = prior_year.product_id
            and monthly.sales_channel = prior_year.sales_channel
            and monthly.reporting_month = prior_year.reporting_month + interval 1 year

)

select * from final
