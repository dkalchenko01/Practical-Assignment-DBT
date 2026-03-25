with source as (

    select * from {{ ref('raw_dancers') }}

),
transformed as (
    select
        trim(full_name) as full_name,
        cast(birth_date as date) as birth_date,
        trim(phone_number) as phone_number,
        cast(registration_date as date) as registration_date,
        trim(gender) as gender,
        trim(team_name) as team_name
    from source
)
select * from transformed