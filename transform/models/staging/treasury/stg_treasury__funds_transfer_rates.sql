with

source as (

    select * from {{ source('treasury', 'funds_transfer_rates') }}

),

cleaned as (

    select
        cast(rate_month as date) as rate_month,
        cast(annual_rate as decimal(9, 4)) as funds_transfer_rate
    from source

)

select * from cleaned
