{{
  config(
    materialized='incremental',
    unique_key='enrollment_id',
    incremental_strategy= 'delete+insert'
  )
}}

with

{% if is_incremental() %}
     max_date as (
        select max(enrollment_date) as max_enrollment_date from {{ this }}
        ),
{% endif %}

enrollment_source as (
    select
        enrollment_id,
        dancer_full_name,
        class_name,
        enrollment_date,
        status
    from {{ ref('stg_enrollments') }}
             {% if is_incremental() %}
                cross join max_date
                where enrollment_date > max_enrollment_date
             {% endif %}
),

transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(['dancer_full_name', 'e.class_name', 'enrollment_date']) }} as enrollment_id,
        d.dancer_id,
        c.class_id,
        dt.date_id,
        enrollment_date,
        status
    from enrollment_source e
    left join {{ ref('dim_dancers') }} d
    on e.dancer_full_name = d.full_name
    left join {{ ref('dim_classes') }} c
    on e.class_name = c.class_name
    left join {{ ref('dim_dates') }} dt
    on e.enrollment_date = dt.date_full
)

select * from transformed