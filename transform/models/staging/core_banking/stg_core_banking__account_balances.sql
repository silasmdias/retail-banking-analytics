with

source as (

    select * from {{ source('core_banking', 'account_balances') }}

),

deduplicated as (

    -- The monthly balance extract occasionally repeats a row.
    select *
    from source
    qualify row_number() over (
        partition by account_id, balance_month
        order by _source_file, _loaded_at
    ) = 1

),

cleaned as (

    select
        account_id,
        cast(balance_month as date) as balance_month,
        cast(average_balance as decimal(18, 2)) as average_balance,
        cast(interest_bearing_balance as decimal(18, 2)) as interest_bearing_balance,
        _loaded_at
    from deduplicated

)

select * from cleaned
