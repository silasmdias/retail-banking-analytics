with

fees as (

    select * from {{ ref('int_fees_screened') }}

),

final as (

    select
        account_id,
        cast(date_trunc('month', fee_date) as date) as reporting_month,
        count(*) as fees_posted,
        cast(sum(amount) as decimal(18, 2)) as fee_revenue
    from fees
    where
        is_valid
        -- Waived fees were never collected and reversed fees were refunded.
        and fee_status = 'posted'
    group by 1, 2

)

select * from final
