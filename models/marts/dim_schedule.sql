with schedule_source as (
    select class_name,
           class_date,
           start_time,
           end_time,
           room,
           status
    from {{ ref('stg_class_schedule') }}
),
transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(['s.class_name', 'class_date', 'start_time']) }} as schedule_id,
        c.class_id,
        d.date_id,
        s.class_date,
        s.start_time,
        s.end_time,
        r.room_id,
        s.status
    from schedule_source s
    left join {{ ref('dim_dates') }} d
    on s.class_date = d.date_full
    left join {{ ref('dim_rooms') }} r
    on s.room = r.room_name
    left join {{ ref('dim_classes') }} c
    on s.class_name = c.class_name
)

select * from transformed