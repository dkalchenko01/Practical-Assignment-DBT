with classess_source as (
    select distinct
           class_name,
           dance_style,
           difficulty_level,
           coach_name,
           created_at,
           class_type
    from {{ ref('stg_classes') }}
),
transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(['class_name', 'class_type']) }} as class_id,
        cl.class_name,
        cl.dance_style,
        cl.difficulty_level,
        cl.created_at,
        cl.class_type,
        c.coach_id,
        t.team_id
    from classess_source cl
    left join {{ ref('dim_coaches') }} c
    on cl.coach_name = c.full_name
    left join {{ ref('dim_teams') }} t
    on cl.class_name = t.team_name
)
select * from transformed