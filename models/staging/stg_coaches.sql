with source as (

    select * from {{ ref('raw_coaches') }}

),
transformed as (
    select
        trim(full_name) as full_name,
        cast(birth_date as date) as birth_date,
        cast(hire_date as date) as hire_date,
        trim(specialization) as specialization,
        trim(gender) as gender,
        trim(cast(phone_number as varchar)) as phone_number,
        trim(cast(email as varchar)) as email
    from source
)
select * from transformed