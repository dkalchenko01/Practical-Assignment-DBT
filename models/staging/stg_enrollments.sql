with source as (

    select * from {{ ref('raw_enrollments') }}

),
transformed as (
    select
        cast(enrollment_id as integer) as enrollment_id,
        trim(dancer_full_name) as dancer_full_name,
        trim(class_name) as class_name,
        cast(enrollment_date as date) as enrollment_date,
        trim(status) as status,
        row_number() over(partition by trim(dancer_full_name), trim(class_name), cast(enrollment_date as date) order by cast(enrollment_id as integer)) as rn

    from source
)
select
    enrollment_id,
    dancer_full_name,
    class_name,
    enrollment_date,
    status
from transformed
where rn = 1