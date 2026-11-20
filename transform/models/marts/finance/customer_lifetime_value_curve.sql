{%- set horizon_months = var('clv_horizon_months') -%}
{%- set mature_age_months = var('clv_mature_age_months') -%}
{%- set annual_discount_rate = var('clv_annual_discount_rate') -%}
{%- set credibility = var('clv_credibility_customer_months') -%}
{%- set loss_credibility = var('clv_loss_credibility_customer_months') -%}

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
        sum(contribution_margin) as contribution_margin,
        coalesce(sum(credit_losses), 0) as credit_losses
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
        coalesce(observed.contribution_margin, 0) as observed_contribution_margin,
        coalesce(observed.credit_losses, 0) as observed_credit_losses
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

age_history as (

    -- What the cohorts that already lived through an age showed at that age, by channel.
    -- `previous_active_customers` are the customers who could have stayed. Margin is split
    -- into the part before credit losses, which is steady, and the losses, which are not.
    select
        acquisition_channel,
        months_since_acquisition,
        sum(observed_active_customers) as active_customers,
        sum(previous_active_customers) as previous_active_customers,
        sum(observed_contribution_margin + observed_credit_losses) as margin_before_losses,
        sum(observed_credit_losses) as credit_losses
    from observed_with_previous
    where months_since_acquisition >= 1
    group by 1, 2

),

mature_history as (

    -- Few cohorts are old enough to inform late ages one by one, so those ages are pooled.
    select
        acquisition_channel,
        sum(active_customers) as active_customers,
        sum(previous_active_customers) as previous_active_customers,
        sum(margin_before_losses) as margin_before_losses,
        sum(credit_losses) as credit_losses
    from age_history
    where months_since_acquisition > {{ mature_age_months }}
    group by 1

),

channel_evidence as (

    -- The history a channel has for each age it may need to project.
    select
        channels.acquisition_channel,
        ages.months_since_acquisition,
        coalesce(
            case
                when ages.months_since_acquisition <= {{ mature_age_months }}
                    then age_history.active_customers
                else mature_history.active_customers
            end,
            0
        ) as active_customers,
        coalesce(
            case
                when ages.months_since_acquisition <= {{ mature_age_months }}
                    then age_history.previous_active_customers
                else mature_history.previous_active_customers
            end,
            0
        ) as previous_active_customers,
        coalesce(
            case
                when ages.months_since_acquisition <= {{ mature_age_months }}
                    then age_history.margin_before_losses
                else mature_history.margin_before_losses
            end,
            0
        ) as margin_before_losses,
        coalesce(
            case
                when ages.months_since_acquisition <= {{ mature_age_months }}
                    then age_history.credit_losses
                else mature_history.credit_losses
            end,
            0
        ) as credit_losses
    from (select distinct acquisition_channel from cohorts) as channels
    cross join ages
    left join age_history
        on
            channels.acquisition_channel = age_history.acquisition_channel
            and ages.months_since_acquisition = age_history.months_since_acquisition
    left join mature_history
        on channels.acquisition_channel = mature_history.acquisition_channel
    where ages.months_since_acquisition >= 1

),

portfolio_evidence as (

    -- The same history for all channels together.
    select
        months_since_acquisition,
        sum(active_customers) / nullif(sum(previous_active_customers), 0) as monthly_retention_rate,
        sum(margin_before_losses) / nullif(sum(active_customers), 0)
            as margin_before_losses_per_active_customer,
        sum(credit_losses) / nullif(sum(active_customers), 0) as credit_losses_per_active_customer
    from channel_evidence
    group by 1

),

portfolio_overall as (

    -- Last resort for an age no cohort of any channel has reached: every age pooled.
    select
        sum(active_customers) / nullif(sum(previous_active_customers), 0) as monthly_retention_rate,
        sum(margin_before_losses) / nullif(sum(active_customers), 0)
            as margin_before_losses_per_active_customer,
        sum(credit_losses) / nullif(sum(active_customers), 0) as credit_losses_per_active_customer
    from age_history

),

portfolio_rates as (

    select
        portfolio_evidence.months_since_acquisition,
        coalesce(
            portfolio_evidence.monthly_retention_rate, portfolio_overall.monthly_retention_rate
        ) as monthly_retention_rate,
        coalesce(
            portfolio_evidence.margin_before_losses_per_active_customer,
            portfolio_overall.margin_before_losses_per_active_customer
        ) as margin_before_losses_per_active_customer,
        coalesce(
            portfolio_evidence.credit_losses_per_active_customer,
            portfolio_overall.credit_losses_per_active_customer
        ) as credit_losses_per_active_customer
    from portfolio_evidence
    cross join portfolio_overall

),

assumptions as (

    -- A channel's own history is blended with the all-channel figure in proportion to how
    -- much history it has: the blend adds a fixed number of notional customer-months at the
    -- all-channel rate. Credit losses are rare and large, so they get a much heavier
    -- blend than retention and margin before losses. One charge-off in a small channel
    -- then no longer decides the margin assumed for years of projection.
    select
        channel_evidence.acquisition_channel,
        channel_evidence.months_since_acquisition,
        coalesce(
            (
                channel_evidence.active_customers
                + {{ credibility }} * portfolio_rates.monthly_retention_rate
            ) / nullif(channel_evidence.previous_active_customers + {{ credibility }}, 0),
            portfolio_rates.monthly_retention_rate
        ) as monthly_retention_rate,
        coalesce(
            (
                channel_evidence.margin_before_losses
                + {{ credibility }} * portfolio_rates.margin_before_losses_per_active_customer
            ) / nullif(channel_evidence.active_customers + {{ credibility }}, 0),
            portfolio_rates.margin_before_losses_per_active_customer
        )
        - coalesce(
            (
                channel_evidence.credit_losses
                + {{ loss_credibility }} * portfolio_rates.credit_losses_per_active_customer
            ) / nullif(channel_evidence.active_customers + {{ loss_credibility }}, 0),
            portfolio_rates.credit_losses_per_active_customer
        ) as margin_per_active_customer
    from channel_evidence
    inner join portfolio_rates
        on channel_evidence.months_since_acquisition = portfolio_rates.months_since_acquisition

),

survival as (

    select
        cohort_ages.*,
        assumptions.margin_per_active_customer,
        -- Customers still active in the last month the cohort has lived.
        last_value(
            case when not cohort_ages.is_projected then cohort_ages.observed_active_customers end
            ignore nulls
        ) over cohort_to_date as last_observed_active_customers,
        -- Running product of the assumed monthly retention rates since the last lived month.
        exp(
            sum(
                case
                    when cohort_ages.is_projected
                        then ln(greatest(assumptions.monthly_retention_rate, 0.000001))
                    else 0
                end
            ) over cohort_to_date
        ) as survival_since_last_observation,
        -- A projected month without an assumption cannot be valued, nor can the months after it.
        sum(
            case
                when
                    cohort_ages.is_projected
                    and (
                        assumptions.monthly_retention_rate is null
                        or assumptions.margin_per_active_customer is null
                    )
                    then 1
                else 0
            end
        ) over cohort_to_date as missing_assumptions
    from cohort_ages
    left join assumptions
        on
            cohort_ages.acquisition_channel = assumptions.acquisition_channel
            and cohort_ages.months_since_acquisition = assumptions.months_since_acquisition
    window cohort_to_date as (
        partition by cohort_ages.cohort_month, cohort_ages.acquisition_channel
        order by cohort_ages.months_since_acquisition
        rows between unbounded preceding and current row
    )

),

monthly_values as (

    select
        cohort_month,
        acquisition_channel,
        months_since_acquisition,
        is_projected,
        customers_acquired,
        case
            when not is_projected then observed_active_customers
            when missing_assumptions = 0
                then last_observed_active_customers * survival_since_last_observation
        end as active_customers,
        case
            when not is_projected then observed_contribution_margin
            when missing_assumptions = 0
                then
                    last_observed_active_customers
                    * survival_since_last_observation
                    * margin_per_active_customer
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
