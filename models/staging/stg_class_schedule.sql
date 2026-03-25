with source as (

    select * from {{ ref('raw_class_schedule') }}

),
transformed as (
    select
        trim(class_name) as class_name,
        cast(class_date as date) as class_date,
        start_time,
        end_time,
        trim(room) as room,
        trim(status) as status
    from source
)
select * from transformed