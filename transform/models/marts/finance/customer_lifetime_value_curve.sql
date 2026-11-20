{%- set horizon_months = var('clv_horizon_months') -%}
{%- set mature_age_months = var('clv_mature_age_months') -%}
{%- set annual_discount_rate = var('clv_annual_discount_rate') -%}

with

customers as (

    select * from {{ ref('dim_customers') }}

),

customer_months as (

    select * from {{ ref('fct_customer_monthly_profitability') }}

),

ages as (

    -- 0, 1, ... up to the last month of the horizon.
    select cast(generated_number - 1 as integer) as months_since_acquisition
    from ({{ dbt_utils.generate_series(horizon_months) }}) as series

),

data_window as (

    select max(reporting_month) as last_observed_month
    from customer_months

),

cohorts as (

    select
        cohort_month,
        acquisition_channel,
        count(*) as customers_acquired
    from customers
    group by 1, 2

),

observed as (

    select
        cohort_month,
        acquisition_channel,
        months_since_acquisition,
        count(*) as active_customers,
        sum(contribution_margin) as contribution_margin
    from customer_months
    group by 1, 2, 3

),

cohort_ages as (

    -- One row per cohort, channel and age inside the horizon, whether already lived or not.
    select
        cohorts.cohort_month,
        cohorts.acquisition_channel,
        cohorts.customers_acquired,
        ages.months_since_acquisition,
        ages.months_since_acquisition > {{ dbt.datediff(
            'cohorts.cohort_month', 'data_window.last_observed_month', 'month'
        ) }} as is_projected,
        coalesce(observed.active_customers, 0) as observed_active_customers,
        coalesce(observed.contribution_margin, 0) as observed_contribution_margin
    from cohorts
    cross join data_window
    cross join ages
    left join observed
        on
            cohorts.cohort_month = observed.cohort_month
            and cohorts.acquisition_channel = observed.acquisition_channel
            and ages.months_since_acquisition = observed.months_since_acquisition

),

observed_with_previous as (

    select
        *,
        lag(observed_active_customers) over (
            partition by cohort_month, acquisition_channel
            order by months_since_acquisition
        ) as previous_active_customers
    from cohort_ages
    where not is_projected

),

age_profile as (

    -- What every cohort that already lived through an age shows for that age, by channel.
    select
        acquisition_channel,
        months_since_acquisition,
        sum(observed_active_customers) as active_customers,
        sum(previous_active_customers) as previous_active_customers,
        sum(observed_contribution_margin) as contribution_margin
    from observed_with_previous
    group by 1, 2

),

mature_profile as (

    -- Few cohorts are old enough to inform late ages, so those ages share one pooled rate.
    select
        acquisition_channel,
        sum(active_customers) / nullif(sum(previous_active_customers), 0) as retention_rate,
        sum(contribution_margin) / nullif(sum(active_customers), 0) as margin_per_active_customer
    from age_profile
    where months_since_acquisition > {{ mature_age_months }}
    group by 1

),

overall_profile as (

    -- Fallback for histories too short to have any mature age.
    select
        acquisition_channel,
        sum(active_customers) / nullif(sum(previous_active_customers), 0) as retention_rate,
        sum(contribution_margin) / nullif(sum(active_customers), 0) as margin_per_active_customer
    from age_profile
    where months_since_acquisition >= 1
    group by 1

),

assumptions as (

    select
        channels.acquisition_channel,
        ages.months_since_acquisition,
        coalesce(
            case
                when ages.months_since_acquisition <= {{ mature_age_months }}
                    then age_profile.active_customers / nullif(age_profile.previous_active_customers, 0)
            end,
            mature_profile.retention_rate,
            overall_profile.retention_rate
        ) as monthly_retention_rate,
        coalesce(
            case
                when ages.months_since_acquisition <= {{ mature_age_months }}
                    then age_profile.contribution_margin / nullif(age_profile.active_customers, 0)
            end,
            mature_profile.margin_per_active_customer,
            overall_profile.margin_per_active_customer
        ) as margin_per_active_customer
    from (select distinct acquisition_channel from cohorts) as channels
    cross join ages
    left join age_profile
        on
            channels.acquisition_channel = age_profile.acquisition_channel
            and ages.months_since_acquisition = age_profile.months_since_acquisition
    left join mature_profile
        on channels.acquisition_channel = mature_profile.acquisition_channel
    left join overall_profile
        on channels.acquisition_channel = overall_profile.acquisition_channel

),

survival as (

    select
        cohort_ages.*,
        assumptions.margin_per_active_customer,
        -- Customers still active in the last month the cohort has lived.
        last_value(
            case when not cohort_ages.is_projected then cohort_ages.observed_active_customers end
            ignore nulls
        ) over (
            partition by cohort_ages.cohort_month, cohort_ages.acquisition_channel
            order by cohort_ages.months_since_acquisition
            rows between unbounded preceding and current row
        ) as last_observed_active_customers,
        -- Running product of the assumed monthly retention rates since the last lived month.
        exp(
            sum(
                case
                    when cohort_ages.is_projected
                        then ln(greatest(assumptions.monthly_retention_rate, 0.000001))
                    else 0
                end
            ) over (
                partition by cohort_ages.cohort_month, cohort_ages.acquisition_channel
                order by cohort_ages.months_since_acquisition
                rows between unbounded preceding and current row
            )
        ) as survival_since_last_observation
    from cohort_ages
    inner join assumptions
        on
            cohort_ages.acquisition_channel = assumptions.acquisition_channel
            and cohort_ages.months_since_acquisition = assumptions.months_since_acquisition

),

monthly_values as (

    select
        cohort_month,
        acquisition_channel,
        months_since_acquisition,
        is_projected,
        customers_acquired,
        case
            when is_projected then last_observed_active_customers * survival_since_last_observation
            else observed_active_customers
        end as active_customers,
        case
            when is_projected
                then
                    last_observed_active_customers
                    * survival_since_last_observation
                    * margin_per_active_customer
            else observed_contribution_margin
        end as contribution_margin,
        power(1 + {{ annual_discount_rate }}, -months_since_acquisition / 12.0) as discount_factor
    from survival

),

rounded as (

    -- Round once here so every total downstream adds up to the cent.
    select
        cohort_month,
        acquisition_channel,
        months_since_acquisition,
        is_projected,
        customers_acquired,
        active_customers,
        discount_factor,
        cast(round(contribution_margin, 2) as decimal(18, 2)) as contribution_margin,
        cast(round(contribution_margin * discount_factor, 2) as decimal(18, 2))
            as discounted_contribution_margin
    from monthly_values

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'cohort_month', 'acquisition_channel', 'months_since_acquisition'
        ]) }} as customer_lifetime_value_curve_id,
        cohort_month,
        acquisition_channel,
        months_since_acquisition,
        is_projected,
        cast(customers_acquired as integer) as customers_acquired,
        cast(round(active_customers, 2) as decimal(18, 2)) as active_customers,
        cast(round(active_customers / customers_acquired, 4) as decimal(9, 4)) as retention_rate,
        contribution_margin,
        cast(round(discount_factor, 6) as decimal(9, 6)) as discount_factor,
        discounted_contribution_margin,
        cast(sum(contribution_margin) over cohort_to_date as decimal(18, 2))
            as cumulative_contribution_margin,
        cast(sum(discounted_contribution_margin) over cohort_to_date as decimal(18, 2))
            as cumulative_discounted_margin,
        cast(
            round(sum(discounted_contribution_margin) over cohort_to_date / customers_acquired, 2)
            as decimal(18, 2)
        ) as cumulative_clv
    from rounded
    window cohort_to_date as (
        partition by cohort_month, acquisition_channel
        order by months_since_acquisition
        rows between unbounded preceding and current row
    )

)

select * from final
