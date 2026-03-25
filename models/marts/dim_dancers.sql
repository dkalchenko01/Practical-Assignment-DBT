with dancers_source as (
    select
        full_name,
        birth_date,
        phone_number,
        registration_date,
        gender,
        team_name
    from {{ ref('stg_dancers') }}
),
transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(['full_name']) }} as dancer_id,
        split_part(d.full_name, ' ', 1) as first_name,
        split_part(d.full_name, ' ', 2) as last_name,
        d.full_name,
        d.birth_date,
        {{ calculate_age('d.birth_date') }} as age,
        d.registration_date,
        d.gender,
        d.phone_number,
        t.team_id
    from dancers_source d
    left join {{ ref('dim_teams') }} t
    on d.team_name = t.team_name
)
select * from transformed