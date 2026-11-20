with

source as (

    select * from {{ source('core_banking', 'fees') }}

),

deduplicated as (

    -- The fee feed occasionally delivers the same record twice.
    select *
    from source
    qualify row_number() over (
        partition by fee_id
        order by _source_file, _loaded_at
    ) = 1

),

cleaned as (

    select
        fee_id,
        account_id,
        transaction_id,
        {{ standardize_label('fee_type') }} as fee_type,
        cast(fee_date as date) as fee_date,
        cast(amount as decimal(18, 2)) as amount,
        fee_status,
        _source_file,
        _loaded_at
    from deduplicated

)

select * from cleaned
