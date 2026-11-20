with

source as (

    select * from {{ source('core_banking', 'customers') }}

),

channel_mapping as (

    select * from {{ ref('channel_mapping') }}

),

deduplicated as (

    -- The customer snapshot occasionally repeats a row.
    select *
    from source
    qualify row_number() over (
        partition by customer_id
        order by _source_file, _loaded_at
    ) = 1

),

cleaned as (

    select
        deduplicated.customer_id,
        cast(deduplicated.signup_date as date) as signup_date,
        coalesce({{ standardize_label('deduplicated.segment') }}, 'unassigned') as segment,
        deduplicated.region,
        cast(deduplicated.birth_year as integer) as birth_year,
        deduplicated.risk_band,
        channel_mapping.channel as acquisition_channel,
        cast(deduplicated.is_internal_test as boolean) as is_internal_test,
        deduplicated._loaded_at
    from deduplicated
    left join channel_mapping
        on {{ channel_lookup_key('deduplicated.acquisition_channel') }} = channel_mapping.raw_channel

)

select * from cleaned
