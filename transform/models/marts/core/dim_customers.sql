with

customers as (

    select * from {{ ref('stg_core_banking__customers') }}
    -- Accounts opened by staff to test systems are not business.
    where not is_internal_test

),

accounts as (

    select * from {{ ref('stg_core_banking__accounts') }}

),

first_accounts as (

    select
        customer_id,
        product_id as first_product_id
    from accounts
    qualify row_number() over (
        partition by customer_id
        order by opened_date, account_id
    ) = 1

),

account_summary as (

    select
        customer_id,
        count(*) as accounts_opened,
        count(*) filter (where account_status = 'open') as open_accounts,
        count(*) filter (where account_status = 'charged_off') as charged_off_accounts,
        max(closed_date) as last_account_closed_date
    from accounts
    group by 1

),

final as (

    select
        customers.customer_id,
        customers.signup_date,
        cast(date_trunc('month', customers.signup_date) as date) as cohort_month,
        customers.acquisition_channel,
        customers.segment,
        customers.region,
        customers.birth_year,
        customers.risk_band,
        first_accounts.first_product_id,
        account_summary.accounts_opened,
        account_summary.open_accounts,
        case
            when account_summary.open_accounts > 0 then 'active'
            when account_summary.charged_off_accounts > 0 then 'charged_off'
            else 'churned'
        end as customer_status,
        case
            when account_summary.open_accounts = 0 then account_summary.last_account_closed_date
        end as relationship_end_date
    from customers
    inner join account_summary
        on customers.customer_id = account_summary.customer_id
    inner join first_accounts
        on customers.customer_id = first_accounts.customer_id

)

select * from final
