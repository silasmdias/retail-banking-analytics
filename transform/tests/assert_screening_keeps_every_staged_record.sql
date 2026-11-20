-- Screening flags records, it never drops or multiplies them. Returns a row
-- for each source whose screened row count differs from its staged row count.
-- Debug: a difference means the join to accounts fanned out; check that
-- `stg_core_banking__accounts` still has one row per account.

with

counts as (

    select
        'transactions' as source_table,
        (select count(*) from {{ ref('stg_core_banking__transactions') }}) as staged_records,
        (select count(*) from {{ ref('int_transactions_screened') }}) as screened_records

    union all

    select
        'fees' as source_table,
        (select count(*) from {{ ref('stg_core_banking__fees') }}) as staged_records,
        (select count(*) from {{ ref('int_fees_screened') }}) as screened_records

)

select *
from counts
where staged_records != screened_records
