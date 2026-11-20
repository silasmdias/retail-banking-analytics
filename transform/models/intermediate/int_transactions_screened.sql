with

transactions as (

    select * from {{ ref('stg_core_banking__transactions') }}

),

accounts as (

    select * from {{ ref('stg_core_banking__accounts') }}

),

screened as (

    select
        transactions.transaction_id,
        transactions.account_id,
        accounts.customer_id,
        accounts.product_id,
        transactions.transaction_date,
        transactions.posted_date,
        transactions.transaction_type,
        transactions.channel,
        transactions.amount,
        -- First rule that fails wins. A null reason means the record is usable.
        case
            when accounts.account_id is null
                then 'unknown_account'
            when transactions.amount is null
                then 'missing_amount'
            when transactions.transaction_date is null
                then 'unreadable_date'
            when transactions.transaction_date < accounts.opened_date
                then 'before_account_opening'
            when transactions.amount > {{ var('max_plausible_transaction_amount') }}
                then 'implausible_amount'
        end as rejection_reason
    from transactions
    left join accounts
        on transactions.account_id = accounts.account_id

)

select
    *,
    rejection_reason is null as is_valid
from screened
