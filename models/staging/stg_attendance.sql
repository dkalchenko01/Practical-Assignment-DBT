with source as (

    select * from {{ ref('raw_attendance') }}

),
transformed as (
    select
        cast(attendance_id as integer) as attendance_id,
        trim(dancer_full_name) as dancer_full_name,
        trim(class_name) as class_name,
        cast(attendance_date as date) as attendance_date,
        trim(status) as status
    from source
)
select * from transformed