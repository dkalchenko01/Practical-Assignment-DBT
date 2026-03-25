{{
  config(
    materialized='incremental',
    unique_key='attendance_id',
    incremental_strategy= 'delete+insert'
  )
}}

with

{% if is_incremental() %}
     max_date as (
        select max(attendance_date) as max_attendance_date from {{ this }}
        ),
{% endif %}

attendance_source as (
    select
        attendance_id,
        dancer_full_name,
        class_name,
        attendance_date,
        status
    from {{ ref('stg_attendance') }}
    {% if is_incremental() %}
        cross join max_date
        where attendance_date > max_attendance_date
    {% endif %}
),
joined_dates_classes as (
    select
        sch.schedule_id,
        sch.date_id,
        d.date_full,
        c.class_name,
        c.class_id,
        c.coach_id
    from {{ ref('dim_schedule') }} sch
    left join {{ ref('dim_dates') }} d
    on sch.date_id = d.date_id
    left join {{ ref('dim_classes') }} c
    on sch.class_id = c.class_id
),

transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(['dancer_full_name', 'a.class_name', 'attendance_date']) }} as attendance_id,
        d.dancer_id,
        j.schedule_id,
        j.class_id,
        j.coach_id,
        d.team_id,
        j.date_id as attendance_date_id,
        a.attendance_date,
        status
    from attendance_source a
    left join joined_dates_classes j
    on a.class_name = j.class_name and a.attendance_date = j.date_full
    left join {{ ref('dim_dancers') }} d
    on a.dancer_full_name = d.full_name
)

select * from transformed