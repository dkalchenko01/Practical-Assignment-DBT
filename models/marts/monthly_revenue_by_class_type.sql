{{
  config(
    materialized='incremental',
    unique_key='revenue_id',
    incremental_strategy= 'delete+insert'
  )
}}

with completed_classes as (
    select distinct
        s.class_id,
        cl.class_type,
        date_trunc('month', class_date) as reporting_month
    from {{ ref('dim_schedule') }} s
    left join {{ ref('dim_classes') }} cl
    on s.class_id = cl.class_id
    where status = 'completed'
),
all_completed_counts as (
    select
        class_type,
        reporting_month,
        count(distinct class_id) as active_classes_count
    from completed_classes
    group by class_type, reporting_month
),
monthly_payments as (
    select
        cc.class_type,
        cc.reporting_month,
        coalesce(sum(p.amount), 0) as monthly_revenue
    from completed_classes cc
    left join {{ ref('fct_payments') }} p
    on p.class_id = cc.class_id and date_trunc('month', p.payment_date) = cc.reporting_month
    {% if is_incremental() %}
        where p.payment_date >= date_trunc('month', current_date) - interval '2 month'
            {% endif %}
    group by cc.class_type, cc.reporting_month
),
joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['mp.class_type', 'mp.reporting_month']) }} as revenue_id,
        mp.class_type,
        mp.reporting_month,
        mp.monthly_revenue,
        acc.active_classes_count,
        round(mp.monthly_revenue / nullif(acc.active_classes_count, 0), 2) as revenue_per_training
    from monthly_payments mp
    left join all_completed_counts acc
    on mp.class_type = acc.class_type and mp.reporting_month = acc.reporting_month
),
prev_month_comparison as (
    select
        *,
        lag(monthly_revenue) over(partition by class_type order by reporting_month) as prev_month_revenue,
        round(
            (monthly_revenue - lag(monthly_revenue) over (partition by class_type order by reporting_month))
            / nullif(lag(monthly_revenue) over (partition by class_type order by reporting_month), 0) * 100,
        2) as growth_pct
    from joined
)
select * from prev_month_comparison
