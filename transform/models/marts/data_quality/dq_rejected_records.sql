with

transactions as (

    select
        'transactions' as source_table,
        cast(date_trunc('month', posted_date) as date) as record_month,
        rejection_reason
    from {{ ref('int_transactions_screened') }}

),

fees as (

    select
        'fees' as source_table,
        cast(date_trunc('month', fee_date) as date) as record_month,
        rejection_reason
    from {{ ref('int_fees_screened') }}

),

records as (

    select * from transactions
    union all
    select * from fees

),

monthly_totals as (

    select
        source_table,
        record_month,
        count(*) as total_records
    from records
    group by 1, 2

),

rejections as (

    select
        source_table,
        record_month,
        rejection_reason,
        count(*) as rejected_records
    from records
    where rejection_reason is not null
    group by 1, 2, 3

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'rejections.source_table', 'rejections.record_month', 'rejections.rejection_reason'
        ]) }} as rejected_records_id,
        rejections.source_table,
        rejections.record_month,
        rejections.rejection_reason,
        cast(rejections.rejected_records as integer) as rejected_records,
        cast(monthly_totals.total_records as integer) as total_records,
        cast(
            round(rejections.rejected_records / monthly_totals.total_records, 6) as decimal(9, 6)
        ) as rejection_rate
    from rejections
    inner join monthly_totals
        on
            rejections.source_table = monthly_totals.source_table
            and rejections.record_month = monthly_totals.record_month

)

select * from final
