with

products as (

    select * from {{ ref('stg_core_banking__products') }}

),

servicing_costs as (

    select * from {{ ref('product_servicing_costs') }}

),

final as (

    select
        products.product_id,
        products.product_name,
        products.product_category,
        products.balance_side,
        products.interchange_rate,
        servicing_costs.monthly_servicing_cost
    from products
    left join servicing_costs
        on products.product_category = servicing_costs.product_category

)

select * from final
