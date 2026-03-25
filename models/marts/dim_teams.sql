with teams_source as (
    select distinct
           class_name,
           dance_style,
           difficulty_level,
           coach_name,
           created_at
    from {{ ref('stg_classes') }}
    where class_type = 'team'
),
transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(['class_name']) }} as team_id,
        t.class_name as team_name,
        t.dance_style,
        t.difficulty_level,
        t.created_at,
        c.coach_id
    from teams_source t
    left join {{ ref('dim_coaches') }} c
    on t.coach_name = c.full_name
)
select * from transformed