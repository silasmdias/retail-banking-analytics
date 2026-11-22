-- The balance extract must have one row for every month an account was open,
-- because those rows are what tells the models an account was active.
-- Returns the accounts with fewer balance months than their open period has.
-- Debug: list the account's rows in `stg_core_banking__account_balances` and
-- compare with its opened and closed dates; a gap means a monthly extract was
-- not delivered or was loaded incomplete.

-- The `dagster` entry names the model this test guards, so the orchestrator
-- shows the result on that model.
{{ config(meta={'dagster': {'ref': {'name': 'stg_core_banking__account_balances'}}}) }}

with

accounts as (

    select * from {{ ref('stg_core_banking__accounts') }}

),

balances as (

    select * from {{ ref('stg_core_banking__account_balances') }}

),

data_window as (

    -- The last month expected comes from the transaction feed as well, so a balance
    -- extract that never arrived for the latest month is noticed.
    select
        greatest(
            (select max(balance_month) from balances),
            (
                select cast(date_trunc('month', max(posted_date)) as date)
                from {{ ref('stg_core_banking__transactions') }}
            )
        ) as last_balance_month

),

expected as (

    select
        accounts.account_id,
        {{ dbt.datediff(
            "date_trunc('month', accounts.opened_date)",
            "least(coalesce(date_trunc('month', accounts.closed_date), data_window.last_balance_month), data_window.last_balance_month)",
            'month'
        ) }} + 1 as expected_months
    from accounts
    cross join data_window

),

delivered as (

    select
        account_id,
        count(*) as delivered_months
    from balances
    group by 1

)

select
    expected.account_id,
    expected.expected_months,
    coalesce(delivered.delivered_months, 0) as delivered_months
from expected
left join delivered
    on expected.account_id = delivered.account_id
where expected.expected_months != coalesce(delivered.delivered_months, 0)
