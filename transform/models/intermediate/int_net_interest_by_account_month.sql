with

balances as (

    select * from {{ ref('stg_core_banking__account_balances') }}

),

accounts as (

    select * from {{ ref('stg_core_banking__accounts') }}

),

products as (

    select * from {{ ref('stg_core_banking__products') }}

),

funds_transfer_rates as (

    select * from {{ ref('stg_treasury__funds_transfer_rates') }}

),

monthly_interest as (

    select
        balances.account_id,
        balances.balance_month as reporting_month,
        products.balance_side,
        balances.average_balance,
        balances.interest_bearing_balance,
        accounts.annual_interest_rate,
        funds_transfer_rates.funds_transfer_rate,
        -- Interest charged to (asset) or paid to (liability) the customer.
        balances.interest_bearing_balance * accounts.annual_interest_rate / 12 as customer_interest,
        -- Internal charge for funding an asset, or credit for a deposit.
        balances.average_balance * funds_transfer_rates.funds_transfer_rate / 12 as funds_transfer_amount
    from balances
    inner join accounts
        on balances.account_id = accounts.account_id
    inner join products
        on accounts.product_id = products.product_id
    left join funds_transfer_rates
        on balances.balance_month = funds_transfer_rates.rate_month

),

final as (

    select
        account_id,
        reporting_month,
        average_balance,
        interest_bearing_balance,
        annual_interest_rate,
        funds_transfer_rate,
        cast(round(customer_interest, 2) as decimal(18, 2)) as customer_interest,
        cast(round(funds_transfer_amount, 2) as decimal(18, 2)) as funds_transfer_amount,
        cast(
            round(
                case balance_side
                    when 'asset' then customer_interest - funds_transfer_amount
                    when 'liability' then funds_transfer_amount - customer_interest
                end,
                2
            ) as decimal(18, 2)
        ) as net_interest_income
    from monthly_interest

)

select * from final
