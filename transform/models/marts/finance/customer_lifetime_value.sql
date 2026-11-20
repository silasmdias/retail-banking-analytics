with

curve as (

    select * from {{ ref('customer_lifetime_value_curve') }}

),

acquisition_spend as (

    select * from {{ ref('stg_finance__acquisition_spend') }}

),

cohort_spend as (

    select
        curve.cohort_month,
        curve.acquisition_channel,
        curve.months_since_acquisition,
        curve.is_projected,
        curve.customers_acquired,
        curve.discounted_contribution_margin,
        curve.cumulative_contribution_margin,
        acquisition_spend.acquisition_spend
    from curve
    left join acquisition_spend
        on
            curve.cohort_month = acquisition_spend.spend_month
            and curve.acquisition_channel = acquisition_spend.sales_channel

),

summarized as (

    select
        cohort_month,
        acquisition_channel,
        max(customers_acquired) as customers_acquired,
        max(acquisition_spend) as acquisition_spend,
        count(*) filter (where not is_projected) as months_observed,
        coalesce(sum(discounted_contribution_margin) filter (where not is_projected), 0)
            as realized_discounted_margin,
        coalesce(sum(discounted_contribution_margin) filter (where is_projected), 0)
            as projected_discounted_margin,
        -- First age at which the cohort's margin, not discounted, has paid back what it cost to acquire.
        min(months_since_acquisition) filter (
            where cumulative_contribution_margin >= acquisition_spend
        ) as payback_age,
        min(months_since_acquisition) filter (where is_projected) as first_projected_age
    from cohort_spend
    group by 1, 2

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['cohort_month', 'acquisition_channel']) }}
            as customer_lifetime_value_id,
        cohort_month,
        acquisition_channel,
        cast(customers_acquired as integer) as customers_acquired,
        cast(months_observed as integer) as months_observed,
        cast(acquisition_spend as decimal(18, 2)) as acquisition_spend,
        cast(realized_discounted_margin as decimal(18, 2)) as realized_discounted_margin,
        cast(projected_discounted_margin as decimal(18, 2)) as projected_discounted_margin,
        cast(round(acquisition_spend / customers_acquired, 2) as decimal(18, 2))
            as customer_acquisition_cost,
        cast(round(realized_discounted_margin / customers_acquired, 2) as decimal(18, 2))
            as realized_clv,
        cast(round(projected_discounted_margin / customers_acquired, 2) as decimal(18, 2))
            as projected_clv,
        cast(
            round((realized_discounted_margin + projected_discounted_margin) / customers_acquired, 2)
            as decimal(18, 2)
        ) as customer_lifetime_value,
        cast(
            round(
                (realized_discounted_margin + projected_discounted_margin)
                / nullif(acquisition_spend, 0),
                2
            ) as decimal(18, 2)
        ) as clv_to_cac_ratio,
        -- Counted in months: payback during the acquisition month is 1.
        cast(payback_age + 1 as integer) as cac_payback_months,
        payback_age >= first_projected_age as is_payback_projected
    from summarized

)

select * from final
