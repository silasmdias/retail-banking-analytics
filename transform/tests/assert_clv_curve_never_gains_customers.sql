-- A cohort only loses customers as it ages: nobody joins a cohort after the
-- acquisition month. Returns the cohort ages where active customers went up.
-- Debug: an increase in a lived month means a customer has activity after a
-- month with none (check their rows in `fct_customer_monthly_profitability`);
-- in a projected month it means an assumed retention rate above 100%.

with

curve as (

    select
        cohort_month,
        acquisition_channel,
        months_since_acquisition,
        active_customers,
        lag(active_customers) over (
            partition by cohort_month, acquisition_channel
            order by months_since_acquisition
        ) as previous_active_customers
    from {{ ref('customer_lifetime_value_curve') }}

)

select *
from curve
where active_customers > previous_active_customers + 0.01
