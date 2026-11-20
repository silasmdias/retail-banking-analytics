with

source as (

    select * from {{ source('core_banking', 'products') }}

),

cleaned as (

    select
        product_id,
        product_name,
        product_category,
        balance_side,
        cast(interchange_rate as decimal(9, 4)) as interchange_rate
    from source

)

select * from cleaned
