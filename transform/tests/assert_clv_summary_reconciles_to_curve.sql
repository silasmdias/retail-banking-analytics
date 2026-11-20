-- The cohort summary must be the sum of its curve. Returns the cohorts whose
-- realized plus projected margin differs from the last point of the curve.
-- Debug: a difference means the join to acquisition spend in
-- `customer_lifetime_value` duplicated curve rows; check
-- `stg_finance__acquisition_spend` for a repeated month and channel.

with

summary as (

    select
        cohort_month,
        acquisition_channel,
        realized_discounted_margin + projected_discounted_margin as discounted_margin
    from {{ ref('customer_lifetime_value') }}

),

curve_end as (

    select
        cohort_month,
        acquisition_channel,
        cumulative_discounted_margin
    from {{ ref('customer_lifetime_value_curve') }}
    qualify row_number() over (
        partition by cohort_month, acquisition_channel
        order by months_since_acquisition desc
    ) = 1

)

select
    summary.cohort_month,
    summary.acquisition_channel,
    summary.discounted_margin,
    curve_end.cumulative_discounted_margin
from summary
inner join curve_end
    on
        summary.cohort_month = curve_end.cohort_month
        and summary.acquisition_channel = curve_end.acquisition_channel
where summary.discounted_margin != curve_end.cumulative_discounted_margin
