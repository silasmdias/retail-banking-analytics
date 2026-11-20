with

fees as (

    select * from {{ ref('stg_core_banking__fees') }}

),

accounts as (

    select * from {{ ref('stg_core_banking__accounts') }}

),

screened as (

    select
        fees.fee_id,
        fees.account_id,
        accounts.customer_id,
        accounts.product_id,
        fees.transaction_id,
        fees.fee_type,
        fees.fee_date,
        fees.amount,
        fees.fee_status,
        -- First rule that fails wins. A null reason means the record is usable.
        case
            when accounts.account_id is null
                then 'unknown_account'
            when fees.amount is null
                then 'missing_amount'
        end as rejection_reason
    from fees
    left join accounts
        on fees.account_id = accounts.account_id

)

select
    *,
    rejection_reason is null as is_valid
from screened
