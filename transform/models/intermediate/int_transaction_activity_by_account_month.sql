with

transactions as (

    select * from {{ ref('int_transactions_screened') }}
    where is_valid

),

products as (

    select * from {{ ref('stg_core_banking__products') }}

),

channel_costs as (

    select * from {{ ref('channel_transaction_costs') }}

),

final as (

    select
        transactions.account_id,
        cast(date_trunc('month', transactions.transaction_date) as date) as reporting_month,
        count(*) as transaction_count,
        cast(
            coalesce(
                sum(transactions.amount) filter (
                    where transactions.transaction_type in ('purchase', 'debit_purchase')
                ),
                0
            ) as decimal(18, 2)
        ) as card_purchase_volume,
        -- The bank earns a share of every card purchase from the card network.
        cast(
            round(
                coalesce(
                    sum(transactions.amount * products.interchange_rate) filter (
                        where transactions.transaction_type in ('purchase', 'debit_purchase')
                    ),
                    0
                ),
                2
            ) as decimal(18, 2)
        ) as interchange_revenue,
        -- Balance written off when a credit account defaults.
        cast(
            coalesce(
                sum(transactions.amount) filter (where transactions.transaction_type = 'charge_off'),
                0
            ) as decimal(18, 2)
        ) as credit_losses,
        cast(coalesce(sum(channel_costs.cost_per_transaction), 0) as decimal(18, 2)) as transaction_cost
    from transactions
    inner join products
        on transactions.product_id = products.product_id
    left join channel_costs
        on transactions.channel = channel_costs.channel
    group by 1, 2

)

select * from final
