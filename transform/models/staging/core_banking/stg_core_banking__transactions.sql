with

source as (

    select * from {{ source('core_banking', 'transactions') }}

),

channel_mapping as (

    select * from {{ ref('channel_mapping') }}

),

deduplicated as (

    -- The transaction feed occasionally delivers the same record twice.
    select *
    from source
    qualify row_number() over (
        partition by transaction_id
        order by _source_file, _loaded_at
    ) = 1

),

cleaned as (

    select
        deduplicated.transaction_id,
        deduplicated.account_id,
        {{ parse_source_date('deduplicated.transaction_date') }} as transaction_date,
        cast(deduplicated.posted_date as date) as posted_date,
        deduplicated.transaction_type,
        channel_mapping.channel,
        -- Direction is given by the transaction type. Part of the feed also
        -- sends a minus sign, so the sign is dropped here.
        abs(cast(deduplicated.amount as decimal(18, 2))) as amount,
        deduplicated._source_file,
        deduplicated._loaded_at
    from deduplicated
    left join channel_mapping
        on {{ channel_lookup_key('deduplicated.channel') }} = channel_mapping.raw_channel

)

select * from cleaned
