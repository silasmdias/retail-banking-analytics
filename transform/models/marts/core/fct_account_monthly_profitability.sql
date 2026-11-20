with

accounts as (

    select * from {{ ref('dim_accounts') }}

),

products as (

    select * from {{ ref('dim_products') }}

),

net_interest as (

    select * from {{ ref('int_net_interest_by_account_month') }}

),

fee_revenue as (

    select * from {{ ref('int_fee_revenue_by_account_month') }}

),

transaction_activity as (

    select * from {{ ref('int_transaction_activity_by_account_month') }}

),

account_months as (

    -- Every month in which an account had a balance, a fee or a transaction.
    select account_id, reporting_month from net_interest
    union
    select account_id, reporting_month from fee_revenue
    union
    select account_id, reporting_month from transaction_activity

),

joined as (

    select
        account_months.account_id,
        account_months.reporting_month,
        accounts.customer_id,
        accounts.product_id,
        accounts.sales_channel,
        net_interest.average_balance,
        net_interest.interest_bearing_balance,
        coalesce(transaction_activity.transaction_count, 0) as transaction_count,
        coalesce(net_interest.net_interest_income, 0) as net_interest_income,
        coalesce(fee_revenue.fee_revenue, 0) as fee_revenue,
        coalesce(transaction_activity.interchange_revenue, 0) as interchange_revenue,
        coalesce(transaction_activity.credit_losses, 0) as credit_losses,
        -- The fixed servicing cost applies to months in which the account was open.
        case
            when net_interest.account_id is not null then products.monthly_servicing_cost
            else 0
        end as servicing_cost,
        coalesce(transaction_activity.transaction_cost, 0) as transaction_cost
    from account_months
    inner join accounts
        on account_months.account_id = accounts.account_id
    inner join products
        on accounts.product_id = products.product_id
    left join net_interest
        on
            account_months.account_id = net_interest.account_id
            and account_months.reporting_month = net_interest.reporting_month
    left join fee_revenue
        on
            account_months.account_id = fee_revenue.account_id
            and account_months.reporting_month = fee_revenue.reporting_month
    left join transaction_activity
        on
            account_months.account_id = transaction_activity.account_id
            and account_months.reporting_month = transaction_activity.reporting_month

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['account_id', 'reporting_month']) }} as account_month_id,
        account_id,
        reporting_month,
        customer_id,
        product_id,
        sales_channel,
        average_balance,
        interest_bearing_balance,
        transaction_count,
        cast(net_interest_income as decimal(18, 2)) as net_interest_income,
        cast(fee_revenue as decimal(18, 2)) as fee_revenue,
        cast(interchange_revenue as decimal(18, 2)) as interchange_revenue,
        cast(net_interest_income + fee_revenue + interchange_revenue as decimal(18, 2)) as total_revenue,
        cast(credit_losses as decimal(18, 2)) as credit_losses,
        cast(servicing_cost + transaction_cost as decimal(18, 2)) as cost_to_serve,
        cast(
            net_interest_income + fee_revenue + interchange_revenue
            - credit_losses
            - servicing_cost
            - transaction_cost
            as decimal(18, 2)
        ) as contribution_margin
    from joined

)

select * from final
