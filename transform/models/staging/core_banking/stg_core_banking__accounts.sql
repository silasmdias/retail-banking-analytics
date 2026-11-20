with

source as (

    select * from {{ source('core_banking', 'accounts') }}

),

channel_mapping as (

    select * from {{ ref('channel_mapping') }}

),

cleaned as (

    select
        source.account_id,
        source.customer_id,
        source.product_id,
        cast(source.opened_date as date) as opened_date,
        cast(source.closed_date as date) as closed_date,
        source.account_status,
        channel_mapping.channel as sales_channel,
        cast(source.annual_interest_rate as decimal(9, 4)) as annual_interest_rate,
        cast(source.credit_limit as decimal(18, 2)) as credit_limit,
        cast(source.original_principal as decimal(18, 2)) as original_principal,
        cast(source.term_months as integer) as term_months,
        source._loaded_at
    from source
    left join channel_mapping
        on {{ channel_lookup_key('source.sales_channel') }} = channel_mapping.raw_channel

)

select * from cleaned
