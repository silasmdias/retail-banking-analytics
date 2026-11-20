with

source as (

    select * from {{ source('finance', 'revenue_budget') }}

),

channel_mapping as (

    select * from {{ ref('channel_mapping') }}

),

cleaned as (

    select
        cast(source.budget_month as date) as budget_month,
        source.product_id,
        -- Channel names are typed by hand in the planning workbook ("Mobile App").
        channel_mapping.channel as sales_channel,
        cast(source.budget_revenue as decimal(18, 2)) as budget_revenue
    from source
    left join channel_mapping
        on {{ channel_lookup_key('source.sales_channel') }} = channel_mapping.raw_channel

)

select * from cleaned
