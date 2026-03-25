{{
  config(
    materialized='incremental',
    unique_key='earning_id',
    incremental_strategy= 'delete+insert',
    incremental_predicates=[
    "DBT_INTERNAL_DEST.reporting_month > date_trunc('month', current_date) - interval '2 month'"
    ]
  )
}}

with coach_coefficient as (
    select
        setting_value::float as coach_salary_percentage
    from {{ ref('manual_settings') }}
    where setting_key = 'coach_salary_percentage'
    limit 1
),
completed_classes as (
    select distinct
        s.class_id,
        cl.coach_id,
        date_trunc('month', class_date) as reporting_month
    from {{ ref('dim_schedule') }} s
    left join {{ ref('dim_classes') }} cl
    on s.class_id = cl.class_id
    where status = 'completed'
),
monthly_payments as (
    select
        p.coach_id,
        cc.reporting_month,
        sum(p.amount) as total_paid
    from {{ ref('fct_payments') }} p
    inner join completed_classes cc
    on p.class_id = cc.class_id and date_trunc('month', p.payment_date) = cc.reporting_month
    {% if is_incremental() %}
        where p.payment_date >= date_trunc('month', current_date) - interval '2 month'
            {% endif %}
    group by p.coach_id, cc.reporting_month
),
coach_earnings as (
    select
        {{ dbt_utils.generate_surrogate_key(['mp.coach_id', 'mp.reporting_month']) }} as earning_id,
        mp.reporting_month,
        c.full_name,
        total_paid,
        round(coalesce(total_paid,0) * (select coach_salary_percentage from coach_coefficient), 2) as salary
    from monthly_payments mp
    left join {{ ref('dim_coaches') }} c
    on mp.coach_id = c.coach_id
)
select * from coach_earnings