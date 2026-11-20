with

accounts as (

    select * from {{ ref('stg_core_banking__accounts') }}

),

customers as (

    -- Joining to the customer dimension drops the accounts of internal test customers.
    select customer_id from {{ ref('dim_customers') }}

),

products as (

    select * from {{ ref('dim_products') }}

),

final as (

    select
        accounts.account_id,
        accounts.customer_id,
        accounts.product_id,
        products.product_category,
        accounts.sales_channel,
        accounts.opened_date,
        accounts.closed_date,
        accounts.account_status,
        accounts.annual_interest_rate,
        accounts.credit_limit,
        accounts.original_principal,
        accounts.term_months
    from accounts
    inner join customers
        on accounts.customer_id = customers.customer_id
    inner join products
        on accounts.product_id = products.product_id

)

select * from final
