with dancers as (
    select
        dancer_id,
        full_name,
        registration_date
    from {{ ref('dim_dancers') }}
),
attendace_statistics as (
    select
        dancer_id,
        count(attendance_id) as total_classes,
        sum(case when status = 'present' then 1 else 0 end) as attended_classes,
        min(attendance_date) as first_attended_date,
        max(attendance_date) as last_attended_date
    from {{ ref('fct_attendance') }}
    group by dancer_id
),
metrics as (
    select
        dancer_id,
        total_classes,
        attended_classes,
        coalesce(round(cast(attended_classes as float) / nullif(total_classes, 0) , 2), 0) as attendance_rate,
        case
            when attended_classes > 0 then datediff('day', first_attended_date, last_attended_date) + 1
            else 0
        end as active_lt_days
    from attendace_statistics
),
joined as (
    select
        d.dancer_id,
        d.full_name,
        d.registration_date,
        m.total_classes,
        m.attended_classes,
        m.attendance_rate,
        {{ get_attendance_category('m.attendance_rate') }} as attendance_category,
        m.active_lt_days,
        datediff('day', d.registration_date, current_date) as total_lt_days
    from dancers d
    left join metrics m
    on d.dancer_id = m.dancer_id
),
ranking as (
    select
        *,
        rank() over(order by attended_classes desc) as loyalty_rank
    from joined
)

select * from ranking