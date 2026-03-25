with source as (

    select * from {{ ref('raw_classes') }}

),
transformed as (
    select
        trim(name) as class_name,
        trim(dance_style) as dance_style,
        trim(difficulty_level) as difficulty_level,
        trim(coach_name) as coach_name,
        trim(class_type) as class_type,
        cast(created_at as date) as created_at
    from source
)
select * from transformed