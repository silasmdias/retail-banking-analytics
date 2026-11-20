with

source as (

    select * from {{ source('finance', 'acquisition_spend') }}

),

channel_mapping as (

    select * from {{ ref('channel_mapping') }}

),

cleaned as (

    select
        cast(source.spend_month as date) as spend_month,
        channel_mapping.channel as sales_channel,
        cast(source.acquisition_spend as decimal(18, 2)) as acquisition_spend
    from source
    left join channel_mapping
        on {{ channel_lookup_key('source.sales_channel') }} = channel_mapping.raw_channel

)

select * from cleaned
