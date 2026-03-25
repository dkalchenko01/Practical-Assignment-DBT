with coaches_source as (
    select full_name,
           birth_date,
           hire_date,
           specialization,
           gender,
           phone_number,
           email
    from {{ ref('stg_coaches') }}
),
transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(['full_name']) }} as coach_id,
        split_part(full_name, ' ', 1) as first_name,
        split_part(full_name, ' ', 2) as last_name,
        full_name,
        birth_date,
        {{ calculate_age('birth_date') }} as age,
        gender,
        phone_number,
        email
    from coaches_source
)
select * from transformed